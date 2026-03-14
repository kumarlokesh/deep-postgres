package storage

// Buffer manager (from src/backend/storage/buffer/).
//
// The buffer pool holds a fixed number of 8 KB page slots in memory.
// Each slot has a BufferDesc tracking which disk block it holds, its
// reference count (pin count), usage count, and dirty/valid flags.
//
// Replacement policy: clock-sweep, same as PostgreSQL's.
// A buffer can be evicted only when its pin count is 0 and its usage count
// has been decremented to 0 by the clock hand sweeping past it.
//
// Thread-safety note: this implementation is intentionally single-threaded
// (no locking). Concurrency will be added when building the smgr layer.

import "sync/atomic"

// BufferId is an index into the buffer pool array.
type BufferId = uint32

// InvalidBufferId is the sentinel for "no buffer".
const InvalidBufferId BufferId = ^uint32(0)

// ForkNumber identifies the relation fork.
// Matches PostgreSQL's ForkNumber (relpath.h).
type ForkNumber uint8

const (
	ForkMain ForkNumber = 0 // main data fork
	ForkFsm  ForkNumber = 1 // free space map
	ForkVm   ForkNumber = 2 // visibility map
	ForkInit ForkNumber = 3 // init fork
)

// BufferTag uniquely identifies a disk block: (relation, fork, block).
//
// Simplified from PostgreSQL's BufferTag (buf_internals.h) which also
// carries tablespace and database OIDs. Those are elided here; relation_id
// is sufficient for single-database experiments.
type BufferTag struct {
	RelationId Oid
	Fork       ForkNumber
	BlockNum   BlockNumber
}

// MainTag returns a BufferTag for the main fork.
func MainTag(relId Oid, block BlockNumber) BufferTag {
	return BufferTag{RelationId: relId, Fork: ForkMain, BlockNum: block}
}

// BufferStateFlags are bit flags on a buffer descriptor.
type BufferStateFlags uint32

const (
	BmValid        BufferStateFlags = 1 << 0 // buffer holds a valid page
	BmDirty        BufferStateFlags = 1 << 1 // page has been modified
	BmIOInProgress BufferStateFlags = 1 << 2 // I/O is in progress
)

// maxUsageCount is the ceiling for the clock-sweep usage counter.
// PostgreSQL uses 5.
const maxUsageCount = 5

// BufferDesc is the metadata record for one buffer slot.
// Matches PostgreSQL's BufferDesc (buf_internals.h).
type BufferDesc struct {
	Tag        BufferTag
	State      BufferStateFlags
	refcount   atomic.Uint32
	UsageCount uint8
}

func (d *BufferDesc) IsValid() bool  { return d.State&BmValid != 0 }
func (d *BufferDesc) IsDirty() bool  { return d.State&BmDirty != 0 }
func (d *BufferDesc) IsPinned() bool { return d.refcount.Load() > 0 }
func (d *BufferDesc) PinCount() uint32 { return d.refcount.Load() }

// Pin increments the reference count.
func (d *BufferDesc) Pin() { d.refcount.Add(1) }

// Unpin decrements the reference count. Panics if already zero.
func (d *BufferDesc) Unpin() {
	prev := d.refcount.Add(^uint32(0)) // subtract 1
	if prev == ^uint32(0) {             // wrapped (was 0)
		panic("unpin of buffer with zero pin count")
	}
}

// BufferPoolStats is returned by BufferPool.Stats().
type BufferPoolStats struct {
	Total  int
	Valid  int
	Dirty  int
	Pinned int
}

// BufferPool manages a fixed pool of page buffers with clock-sweep eviction.
type BufferPool struct {
	descriptors []BufferDesc
	pages       []*Page
	tagMap      map[BufferTag]BufferId
	clockHand   int
	numBuffers  int
}

// NewBufferPool creates a pool of numBuffers slots.
// Panics if numBuffers == 0.
func NewBufferPool(numBuffers int) *BufferPool {
	if numBuffers <= 0 {
		panic("buffer pool must have at least one buffer")
	}
	pool := &BufferPool{
		descriptors: make([]BufferDesc, numBuffers),
		pages:       make([]*Page, numBuffers),
		tagMap:      make(map[BufferTag]BufferId, numBuffers),
		numBuffers:  numBuffers,
	}
	for i := range pool.pages {
		pool.pages[i] = NewPage()
	}
	return pool
}

// NumBuffers returns the total number of buffer slots.
func (p *BufferPool) NumBuffers() int { return p.numBuffers }

// ReadBuffer returns the buffer ID for tag, pinning it.
// If the page is already in the pool, its existing slot is returned.
// Otherwise, a new slot is allocated via clock-sweep (possibly evicting a page).
//
// The caller must call UnpinBuffer when done with the buffer.
func (p *BufferPool) ReadBuffer(tag BufferTag) (BufferId, error) {
	// Fast path: already cached.
	if id, ok := p.tagMap[tag]; ok {
		desc := &p.descriptors[id]
		desc.Pin()
		desc.UsageCount = maxUsageCount
		return id, nil
	}

	// Allocate a slot.
	id, err := p.allocateBuffer()
	if err != nil {
		return InvalidBufferId, err
	}
	desc := &p.descriptors[id]

	// Remove stale tag mapping if the slot was previously occupied.
	if desc.IsValid() {
		delete(p.tagMap, desc.Tag)
	}

	// Flush dirty slot (disk write would happen here).
	if desc.IsDirty() {
		desc.State &^= BmDirty
	}

	// Initialise the slot.
	desc.Tag = tag
	desc.State = BmValid
	desc.UsageCount = maxUsageCount
	desc.Pin()
	p.pages[id].init()
	p.tagMap[tag] = id

	return id, nil
}

// allocateBuffer finds a victim buffer using clock sweep.
// Returns ErrBufferExhausted if every buffer is pinned.
//
// The clock hand advances one slot per iteration. Unpinned buffers with a
// non-zero usage count have their count decremented and are skipped; they
// become candidates on subsequent passes. This drains usage counts across
// up to maxUsageCount full sweeps before a victim emerges — identical to
// PostgreSQL's StrategyGetBuffer behaviour.
func (p *BufferPool) allocateBuffer() (BufferId, error) {
	// O(n) pre-check: reject immediately if every slot is pinned so the
	// sweep below doesn't spin forever.
	for i := range p.descriptors {
		if !p.descriptors[i].IsPinned() {
			goto sweep
		}
	}
	return InvalidBufferId, errBufferExhausted()

sweep:
	for {
		id := p.clockHand
		p.clockHand = (p.clockHand + 1) % p.numBuffers

		desc := &p.descriptors[id]
		if desc.IsPinned() {
			continue
		}
		if desc.UsageCount > 0 {
			desc.UsageCount--
			continue
		}
		return BufferId(id), nil
	}
}

// UnpinBuffer decrements the pin count of buffer id.
func (p *BufferPool) UnpinBuffer(id BufferId) error {
	if err := p.validateId(id); err != nil {
		return err
	}
	p.descriptors[id].Unpin()
	return nil
}

// GetPage returns a read-only reference to the page in buffer id.
// The buffer must be pinned by the caller.
func (p *BufferPool) GetPage(id BufferId) (*Page, error) {
	if err := p.validateId(id); err != nil {
		return nil, err
	}
	if !p.descriptors[id].IsPinned() {
		return nil, errBufferNotPinned(id)
	}
	return p.pages[id], nil
}

// GetPageForWrite returns a write reference to the page in buffer id and marks
// the buffer dirty. The buffer must be pinned.
func (p *BufferPool) GetPageForWrite(id BufferId) (*Page, error) {
	if err := p.validateId(id); err != nil {
		return nil, err
	}
	desc := &p.descriptors[id]
	if !desc.IsPinned() {
		return nil, errBufferNotPinned(id)
	}
	desc.State |= BmDirty
	return p.pages[id], nil
}

// MarkDirty marks buffer id as dirty without returning the page.
func (p *BufferPool) MarkDirty(id BufferId) error {
	if err := p.validateId(id); err != nil {
		return err
	}
	p.descriptors[id].State |= BmDirty
	return nil
}

// GetDescriptor returns a pointer to the BufferDesc for id.
func (p *BufferPool) GetDescriptor(id BufferId) (*BufferDesc, error) {
	if err := p.validateId(id); err != nil {
		return nil, err
	}
	return &p.descriptors[id], nil
}

// LookupBuffer returns the buffer ID for tag if it is in the pool, without
// pinning. Returns (InvalidBufferId, false) if not present.
func (p *BufferPool) LookupBuffer(tag BufferTag) (BufferId, bool) {
	id, ok := p.tagMap[tag]
	return id, ok
}

// FlushAll clears the dirty flag on every buffer (simulates writing to disk).
func (p *BufferPool) FlushAll() {
	for i := range p.descriptors {
		p.descriptors[i].State &^= BmDirty
	}
}

// Stats returns a snapshot of buffer pool utilisation.
func (p *BufferPool) Stats() BufferPoolStats {
	s := BufferPoolStats{Total: p.numBuffers}
	for i := range p.descriptors {
		d := &p.descriptors[i]
		if d.IsValid() {
			s.Valid++
		}
		if d.IsDirty() {
			s.Dirty++
		}
		if d.IsPinned() {
			s.Pinned++
		}
	}
	return s
}

func (p *BufferPool) validateId(id BufferId) error {
	if int(id) >= p.numBuffers {
		return errInvalidBufferId(id)
	}
	return nil
}
