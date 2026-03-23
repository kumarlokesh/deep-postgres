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

func (d *BufferDesc) IsValid() bool    { return d.State&BmValid != 0 }
func (d *BufferDesc) IsDirty() bool    { return d.State&BmDirty != 0 }
func (d *BufferDesc) IsPinned() bool   { return d.refcount.Load() > 0 }
func (d *BufferDesc) PinCount() uint32 { return d.refcount.Load() }

// Pin increments the reference count.
func (d *BufferDesc) Pin() { d.refcount.Add(1) }

// Unpin decrements the reference count. Panics if already zero.
func (d *BufferDesc) Unpin() {
	prev := d.refcount.Add(^uint32(0)) // subtract 1
	if prev == ^uint32(0) {            // wrapped (was 0)
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

// BufferTracer receives events from the buffer pool.
// Implement this interface to collect hit/miss/eviction metrics.
// The NoopBufferTracer provides a zero-cost default.
type BufferTracer interface {
	OnHit(tag BufferTag)
	OnMiss(tag BufferTag)
	OnEvict(tag BufferTag, dirty bool)
}

// NoopBufferTracer discards all events.
type NoopBufferTracer struct{}

func (NoopBufferTracer) OnHit(BufferTag)         {}
func (NoopBufferTracer) OnMiss(BufferTag)        {}
func (NoopBufferTracer) OnEvict(BufferTag, bool) {}

// relEntry binds a relation's file location to its storage manager.
type relEntry struct {
	node RelFileNode
	smgr StorageManager
}

// BufferPool manages a fixed pool of page buffers.  The replacement strategy
// is pluggable via EvictionPolicy; the default is ClockSweep.  An optional
// BufferTracer receives hit/miss/eviction events for instrumentation.
type BufferPool struct {
	descriptors []BufferDesc
	pages       []*Page
	tagMap      map[BufferTag]BufferId
	numBuffers  int
	rels        map[Oid]relEntry // relation registry for smgr-backed I/O
	policy      EvictionPolicy
	tracer      BufferTracer
}

// NewBufferPool creates a pool of numBuffers slots using ClockSweep eviction.
// Panics if numBuffers == 0.
func NewBufferPool(numBuffers int) *BufferPool {
	return NewBufferPoolWithPolicy(numBuffers, &ClockSweepPolicy{})
}

// NewBufferPoolWithPolicy creates a pool using the supplied EvictionPolicy.
// An optional tracer may be attached via pool.SetTracer after construction.
// Panics if numBuffers == 0.
func NewBufferPoolWithPolicy(numBuffers int, policy EvictionPolicy) *BufferPool {
	if numBuffers <= 0 {
		panic("buffer pool must have at least one buffer")
	}
	policy.Init(numBuffers)
	pool := &BufferPool{
		descriptors: make([]BufferDesc, numBuffers),
		pages:       make([]*Page, numBuffers),
		tagMap:      make(map[BufferTag]BufferId, numBuffers),
		numBuffers:  numBuffers,
		rels:        make(map[Oid]relEntry),
		policy:      policy,
		tracer:      NoopBufferTracer{},
	}
	for i := range pool.pages {
		pool.pages[i] = NewPage()
	}
	return pool
}

// SetTracer replaces the pool's BufferTracer.  Pass NoopBufferTracer{} to
// disable tracing.
func (p *BufferPool) SetTracer(t BufferTracer) { p.tracer = t }

// NumBuffers returns the total number of buffer slots.
func (p *BufferPool) NumBuffers() int { return p.numBuffers }

// RegisterRelation binds a relation OID to its physical location and storage
// manager.  After registration, ReadBuffer will load cache misses from disk
// and dirty evictions will be written back via smgr.
func (p *BufferPool) RegisterRelation(relId Oid, node RelFileNode, smgr StorageManager) {
	p.rels[relId] = relEntry{node: node, smgr: smgr}
}

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
		p.policy.Access(id, p.descriptors)
		p.tracer.OnHit(tag)
		return id, nil
	}

	p.tracer.OnMiss(tag)

	// Choose a victim via the eviction policy.
	// TagEvictionPolicy gets the incoming tag so ghost-list policies (e.g. ARC)
	// can make a more informed eviction decision.
	var id BufferId
	var ok bool
	if tep, isTag := p.policy.(TagEvictionPolicy); isTag {
		id, ok = tep.VictimForTag(tag, p.descriptors)
	} else {
		id, ok = p.policy.Victim(p.descriptors)
	}
	if !ok {
		return InvalidBufferId, errBufferExhausted()
	}
	desc := &p.descriptors[id]

	// Remove stale tag mapping if the slot was previously occupied.
	oldTag := desc.Tag
	if desc.IsValid() {
		delete(p.tagMap, oldTag)
	}

	// Flush dirty slot: write back to disk and notify tracer.
	if desc.IsDirty() {
		p.tracer.OnEvict(oldTag, true)
		p.writePage(oldTag, p.pages[id])
		desc.State &^= BmDirty
	} else if desc.IsValid() {
		p.tracer.OnEvict(oldTag, false)
	}

	// Notify tag-aware policy of the eviction (moves tag to ghost list).
	if desc.IsValid() {
		if tep, isTag := p.policy.(TagEvictionPolicy); isTag {
			tep.OnEvictTag(oldTag)
		}
	}

	// Initialise the slot.
	desc.Tag = tag
	desc.State = BmValid
	desc.UsageCount = 0
	desc.Pin()

	// Notify tag-aware policy of the new load; plain policies use Access.
	if tep, isTag := p.policy.(TagEvictionPolicy); isTag {
		tep.OnLoadTag(id, tag, p.descriptors)
	} else {
		p.policy.Access(id, p.descriptors)
	}

	// Load the page from disk if a storage manager is registered.
	// On a miss (block does not exist yet), fall back to a fresh empty page.
	if !p.readPage(tag, p.pages[id]) {
		p.pages[id].init()
	}

	p.tagMap[tag] = id

	return id, nil
}

// readPage fills page from disk via smgr.  Returns true on success.
func (p *BufferPool) readPage(tag BufferTag, page *Page) bool {
	entry, ok := p.rels[tag.RelationId]
	if !ok {
		return false
	}
	err := entry.smgr.Read(entry.node, tag.Fork, tag.BlockNum, page.data[:])
	return err == nil
}

// writePage flushes page to disk via smgr.  Silently ignored if no smgr is registered.
func (p *BufferPool) writePage(tag BufferTag, page *Page) {
	entry, ok := p.rels[tag.RelationId]
	if !ok {
		return
	}
	// Ignore write errors here; a real implementation would handle them.
	_ = entry.smgr.Write(entry.node, tag.Fork, tag.BlockNum, page.data[:])
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

// FlushAll writes all dirty buffers to disk (if smgr is registered) and
// clears their dirty flags.
func (p *BufferPool) FlushAll() {
	for i := range p.descriptors {
		desc := &p.descriptors[i]
		if desc.IsDirty() {
			p.writePage(desc.Tag, p.pages[i])
			desc.State &^= BmDirty
		}
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
