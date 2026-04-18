package storage

// Buffer manager (from src/backend/storage/buffer/).
//
// The buffer pool holds a fixed number of 8 KB page slots in memory.
// Each slot has a BufferDesc tracking which disk block it holds and its
// current state.
//
// State word (matches PostgreSQL's packed buf_internals.h layout):
//
//	bits  0-17: pin count  (max 262 143; BUF_REFCOUNT_MASK in PG)
//	bits 18-22: usage_count (max 31; capped at maxUsageCount=5; BUF_USAGECOUNT_MASK)
//	bit  23:    BM_DIRTY         - page has been modified
//	bit  24:    BM_VALID         - slot holds a valid page
//	bit  25:    BM_IO_IN_PROGRESS - smgr read/write in progress
//	bit  26:    BM_IO_ERROR      - previous I/O failed (reserved)
//	bit  27:    BM_PIN_COUNT_WAITER - one LockBufferForCleanup waiter registered
//
// Using a single atomic word for pin count + usage_count + flags lets
// them all be read in one load and updated with a single CAS - no
// secondary lock required.
//
// Content lock: a sync.RWMutex on each BufferDesc serialises page content
// access. Callers must hold a pin before acquiring the content lock (two-phase
// protocol: pin → lock content → read/write → unlock content → unpin).
//
// Replacement policy: clock-sweep, same as PostgreSQL's. A buffer can be
// evicted only when its pin count is 0 and its usage_count has been
// decremented to 0 by the clock hand sweeping past it.
//
// Thread-safety note: single-threaded by design (no goroutine synchronisation
// beyond the atomic state word). Concurrency will be added when building the
// smgr layer.

import "sync"

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

// ── Packed state word constants ───────────────────────────────────────────────

// bufRefcountMask covers bits 0-17 of the state word (pin count).
const bufRefcountMask = uint32(0x0003_FFFF)

// bufUsageShift / bufUsageMask cover bits 18-22 of the state word (usage_count).
const (
	bufUsageShift = 18
	bufUsageMask  = uint32(0x007C_0000) // bits 18-22 (5 bits, max 31)
	bufUsageOne   = uint32(1) << bufUsageShift
)

// maxUsageCount is the ceiling for the clock-sweep usage counter.
// Matches PostgreSQL's BM_MAX_USAGE_COUNT = 5 (buf_internals.h).
const maxUsageCount = uint32(5)

// Bit flags in the upper part of the state word.
const (
	BmDirty          = uint32(1) << 23 // page has been modified
	BmValid          = uint32(1) << 24 // slot holds a valid page
	BmIOInProgress   = uint32(1) << 25 // I/O in progress on this slot
	BmIOError        = uint32(1) << 26 // previous I/O failed (reserved)
	BmPinCountWaiter = uint32(1) << 27 // LockBufferForCleanup waiter present
)

// ── BufferLockMode ────────────────────────────────────────────────────────────

// BufferLockMode selects shared or exclusive content-level access.
// Matches PostgreSQL's BufferAccessStrategyType / BUFFER_LOCK_* macros.
type BufferLockMode uint8

const (
	// BufferLockShared allows concurrent reads.
	// Multiple goroutines may hold shared locks simultaneously.
	BufferLockShared BufferLockMode = 0

	// BufferLockExclusive is required for any page modification.
	// Exclusive excludes all other shared and exclusive holders.
	BufferLockExclusive BufferLockMode = 1
)

// ── BufferDesc ────────────────────────────────────────────────────────────────

// BufferDesc is the metadata record for one buffer slot.
// Matches PostgreSQL's BufferDesc (buf_internals.h).
//
// Two-phase locking discipline (mirroring PostgreSQL's LockBuffer semantics):
//  1. Acquire a pin: ReadBuffer / Pin → increments the pin count in state.
//  2. Acquire the content lock: LockBuffer(id, mode).
//  3. Read or write page content.
//  4. Release the content lock: UnlockBuffer(id, mode).
//  5. Release the pin: UnpinBuffer.
//
// The content lock must NOT be acquired without a prior pin. LockBuffer
// enforces this with a panic in debug builds.
type BufferDesc struct {
	Tag   BufferTag
	state sync.Mutex // guards access to the packed uint32 state word below
	// packed packs pin count + usage_count + flags into one value.
	// Access only via the helper methods; never read/write directly.
	packed uint32

	// contentLock serialises page content access.
	// Callers must hold a pin before acquiring this lock (see LockBuffer).
	contentLock sync.RWMutex

	// cleanupWaiter is reserved for LockBufferForCleanup semantics:
	// a 1 signals that one waiter needs all other pins to drain before
	// it can acquire exclusive access (used by VACUUM to reclaim pages).
	// TODO: wire up once vacuum needs pin-count-zero exclusion.
	cleanupWaiter uint32
}

// loadState returns the current packed state word.
// Caller must not hold d.state (used for reads that don't need consistency
// with writes - fine for single-threaded use).
func (d *BufferDesc) loadState() uint32 { return d.packed }

// ── Pin count ─────────────────────────────────────────────────────────────────

// Pin increments the pin count.
func (d *BufferDesc) Pin() {
	d.state.Lock()
	d.packed += 1 // refcount in bits 0-17; won't overflow into bit 18
	d.state.Unlock()
}

// Unpin decrements the pin count. Panics if already zero.
func (d *BufferDesc) Unpin() {
	d.state.Lock()
	rc := d.packed & bufRefcountMask
	if rc == 0 {
		d.state.Unlock()
		panic("unpin of buffer with zero pin count")
	}
	d.packed--
	d.state.Unlock()
}

// PinCount returns the current pin count.
func (d *BufferDesc) PinCount() uint32 { return d.loadState() & bufRefcountMask }

// IsPinned reports whether any caller holds a pin.
func (d *BufferDesc) IsPinned() bool { return d.PinCount() > 0 }

// ── Usage count ───────────────────────────────────────────────────────────────

// UsageCount returns the current usage count (0–maxUsageCount).
func (d *BufferDesc) UsageCount() uint8 {
	return uint8((d.loadState() & bufUsageMask) >> bufUsageShift)
}

// setUsageCount overwrites the usage count field. Package-internal; used by
// eviction policies and tests.
func (d *BufferDesc) setUsageCount(n uint8) {
	d.state.Lock()
	d.packed = (d.packed &^ bufUsageMask) | (uint32(n)<<bufUsageShift)&bufUsageMask
	d.state.Unlock()
}

// bumpUsageCount increments usage_count by 1, capped at maxUsageCount.
// Matches PostgreSQL's StrategyGetBuffer:
//
//	if (buf->usage_count < BM_MAX_USAGE_COUNT) buf->usage_count++;
func (d *BufferDesc) bumpUsageCount() {
	d.state.Lock()
	uc := (d.packed & bufUsageMask) >> bufUsageShift
	if uc < maxUsageCount {
		d.packed += bufUsageOne
	}
	d.state.Unlock()
}

// decUsageCount decrements usage_count by 1, floored at 0.
func (d *BufferDesc) decUsageCount() {
	d.state.Lock()
	if d.packed&bufUsageMask != 0 {
		d.packed -= bufUsageOne
	}
	d.state.Unlock()
}

// ── State flags ───────────────────────────────────────────────────────────────

// IsValid reports whether the slot holds a valid page.
func (d *BufferDesc) IsValid() bool { return d.loadState()&BmValid != 0 }

// IsDirty reports whether the page has unflushed modifications.
func (d *BufferDesc) IsDirty() bool { return d.loadState()&BmDirty != 0 }

// IsIOInProgress reports whether an smgr read/write is in progress.
func (d *BufferDesc) IsIOInProgress() bool { return d.loadState()&BmIOInProgress != 0 }

// setFlags atomically ORs mask into the state word.
func (d *BufferDesc) setFlags(mask uint32) {
	d.state.Lock()
	d.packed |= mask
	d.state.Unlock()
}

// clearFlags atomically clears mask bits from the state word.
func (d *BufferDesc) clearFlags(mask uint32) {
	d.state.Lock()
	d.packed &^= mask
	d.state.Unlock()
}

// ── BufferPoolStats ───────────────────────────────────────────────────────────

// BufferPoolStats is returned by BufferPool.Stats().
type BufferPoolStats struct {
	Total  int
	Valid  int
	Dirty  int
	Pinned int
}

// ── BufferTracer ──────────────────────────────────────────────────────────────

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

// ── BufferPool ────────────────────────────────────────────────────────────────

// relEntry binds a relation's file location to its storage manager.
type relEntry struct {
	node RelFileNode
	smgr StorageManager
}

// BufferPool manages a fixed pool of page buffers. The replacement strategy
// is pluggable via EvictionPolicy; the default is ClockSweep. An optional
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

// SetTracer replaces the pool's BufferTracer. Pass NoopBufferTracer{} to
// disable tracing.
func (p *BufferPool) SetTracer(t BufferTracer) { p.tracer = t }

// NumBuffers returns the total number of buffer slots.
func (p *BufferPool) NumBuffers() int { return p.numBuffers }

// RegisterRelation binds a relation OID to its physical location and storage
// manager. After registration, ReadBuffer will load cache misses from disk
// and dirty evictions will be written back via smgr.
func (p *BufferPool) RegisterRelation(relId Oid, node RelFileNode, smgr StorageManager) {
	p.rels[relId] = relEntry{node: node, smgr: smgr}
}

// ReadBuffer returns the buffer ID for tag, pinning it.
// If the page is already in the pool, its existing slot is returned.
// Otherwise, a new slot is allocated via the eviction policy (possibly
// evicting a page).
//
// The caller must call UnpinBuffer when done with the buffer.
//
// IO_IN_PROGRESS discipline: the slot is marked BM_IO_IN_PROGRESS for the
// duration of the smgr read. In a multi-threaded extension a second caller
// requesting the same block would wait on an io_in_progress condition variable
// until the first reader clears the flag. This single-threaded implementation
// sets and clears the flag correctly so the bit is always accurate.
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
		desc.clearFlags(BmDirty)
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
	// Reset packed word: clear everything, then set BmValid.
	desc.state.Lock()
	desc.packed = BmValid
	desc.state.Unlock()
	desc.Pin()

	// Notify tag-aware policy of the new load; plain policies use Access.
	if tep, isTag := p.policy.(TagEvictionPolicy); isTag {
		tep.OnLoadTag(id, tag, p.descriptors)
	} else {
		p.policy.Access(id, p.descriptors)
	}

	// Mark I/O in progress while loading from disk.
	desc.setFlags(BmIOInProgress)
	if !p.readPage(tag, p.pages[id]) {
		p.pages[id].init()
	}
	desc.clearFlags(BmIOInProgress)

	p.tagMap[tag] = id

	return id, nil
}

// readPage fills page from disk via smgr. Returns true on success.
func (p *BufferPool) readPage(tag BufferTag, page *Page) bool {
	entry, ok := p.rels[tag.RelationId]
	if !ok {
		return false
	}
	err := entry.smgr.Read(entry.node, tag.Fork, tag.BlockNum, page.data[:])
	return err == nil
}

// writePage flushes page to disk via smgr. Silently ignored if no smgr is registered.
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

// LockBuffer acquires the content lock on buffer id in the requested mode.
//
// Two-phase discipline: the caller must hold a pin on id before calling
// LockBuffer. Violating this ordering panics immediately so the bug is
// caught at the call site rather than manifesting as a data race later.
//
//	Correct usage:
//	  id, _ := pool.ReadBuffer(tag)          // acquires pin
//	  pool.LockBuffer(id, BufferLockShared)   // acquires content lock
//	  page, _ := pool.GetPage(id)
//	  ... read page ...
//	  pool.UnlockBuffer(id, BufferLockShared)
//	  pool.UnpinBuffer(id)
func (p *BufferPool) LockBuffer(id BufferId, mode BufferLockMode) error {
	if err := p.validateId(id); err != nil {
		return err
	}
	desc := &p.descriptors[id]
	if !desc.IsPinned() {
		panic("LockBuffer: caller must hold a pin before acquiring content lock")
	}
	switch mode {
	case BufferLockShared:
		desc.contentLock.RLock()
	case BufferLockExclusive:
		desc.contentLock.Lock()
	}
	return nil
}

// UnlockBuffer releases the content lock on buffer id.
// The mode must match the mode passed to the corresponding LockBuffer call.
func (p *BufferPool) UnlockBuffer(id BufferId, mode BufferLockMode) error {
	if err := p.validateId(id); err != nil {
		return err
	}
	desc := &p.descriptors[id]
	switch mode {
	case BufferLockShared:
		desc.contentLock.RUnlock()
	case BufferLockExclusive:
		desc.contentLock.Unlock()
	}
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
	desc.setFlags(BmDirty)
	return p.pages[id], nil
}

// MarkDirty marks buffer id as dirty without returning the page.
func (p *BufferPool) MarkDirty(id BufferId) error {
	if err := p.validateId(id); err != nil {
		return err
	}
	p.descriptors[id].setFlags(BmDirty)
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
			desc.clearFlags(BmDirty)
		}
	}
}

// InvalidateRange removes all pool entries for blocks [from, ∞) of relId/fork.
// The buffer must not be pinned; pinned buffers are skipped.
// Dirty buffers in the range are flushed to disk before invalidation.
//
// Use this before truncating a relation so the pool does not return stale pages
// for blocks that no longer exist on disk.
func (p *BufferPool) InvalidateRange(relId Oid, fork ForkNumber, from BlockNumber) {
	for i := range p.descriptors {
		desc := &p.descriptors[i]
		if !desc.IsValid() {
			continue
		}
		tag := desc.Tag
		if tag.RelationId != relId || tag.Fork != fork || tag.BlockNum < from {
			continue
		}
		if desc.IsPinned() {
			continue // cannot invalidate a pinned buffer
		}
		if desc.IsDirty() {
			p.writePage(tag, p.pages[i])
			desc.clearFlags(BmDirty)
		}
		delete(p.tagMap, tag)
		desc.Tag = BufferTag{}
		desc.state.Lock()
		desc.packed = 0 // clear valid, usage_count, refcount, all flags
		desc.state.Unlock()
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
