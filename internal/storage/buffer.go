package storage

// Buffer manager (from src/backend/storage/buffer/).
//
// The buffer pool holds a fixed number of 8 KB page slots in memory.
// Each slot has a BufferDesc tracking which disk block it holds and its
// current state.
//
// ── State word layout ────────────────────────────────────────────────────────
//
// BufferDesc.state is a single atomic.Uint32 packing three logical fields:
//
//	bits  0-17: pin count  (max 262 143; BUF_REFCOUNT_MASK in PG)
//	bits 18-22: usage_count (max 31; capped at maxUsageCount=5)
//	bit  23:    BM_DIRTY         - page has been modified
//	bit  24:    BM_VALID         - slot holds a valid page
//	bit  25:    BM_IO_IN_PROGRESS - smgr read/write in progress
//	bit  26:    BM_IO_ERROR      - previous I/O failed (reserved)
//	bit  27:    BM_PIN_COUNT_WAITER - LockBufferForCleanup waiter registered
//
// Packing all three into one word means any consistent snapshot of
// (refcount, usage_count, flags) requires only one atomic load - no secondary
// lock needed. Modifications use CompareAndSwap so concurrent updates to
// different sub-fields do not corrupt each other.
//
// ── Content lock ─────────────────────────────────────────────────────────────
//
// A sync.RWMutex on each BufferDesc serialises page content access.
// Two-phase discipline: acquire a pin, then lock content; release content
// lock before unpinning. LockBuffer panics if the pin is missing.
//
// ── Hash-table partitioning ──────────────────────────────────────────────────
//
// tagMap is a single Go map protected implicitly by the single-threaded
// design. PostgreSQL partitions its buffer hash table into 128 segments,
// each guarded by its own LWLock (BufMappingLock), specifically to avoid
// the single-lock bottleneck described in "30 Years of Buffer Manager
// Evolution" (Li 2022). Under concurrent access a single lock serialises
// every tag lookup, which becomes the dominant bottleneck at ≥8 cores.
// For this single-threaded research engine the simplification is intentional.
//
// ── IO_IN_PROGRESS coordination ──────────────────────────────────────────────
//
// When a miss begins loading a page from disk, the slot is marked
// BM_IO_IN_PROGRESS. In PostgreSQL a second goroutine requesting the same
// page would wait on the slot's ioCV condition variable until the first
// reader clears the flag; it would then re-check tagMap and, if the page
// is now cached, return the existing slot instead of loading again.
//
// This single-threaded engine sets and clears BM_IO_IN_PROGRESS correctly
// so the bit always reflects true I/O state. The condition-variable wait
// path is documented in the ioCV field of BufferDesc but is only triggered
// if a second concurrent caller finds BM_IO_IN_PROGRESS set - which cannot
// happen with a single goroutine.
//
// ── WAL write-back rule ───────────────────────────────────────────────────────
//
// Before writing a dirty page to disk during eviction, PostgreSQL calls
// XLogFlush(pd_lsn) to guarantee the WAL record for this page has been
// durably written before the modified data page. Violating this ordering
// would allow a page to reach disk whose changes are not yet in the WAL -
// a durability violation on crash recovery.
//
// BufferPool.walFlusher is an optional WALFlusher hook that is called with
// the page's pd_lsn whenever a dirty buffer is evicted. When nil (the
// default), dirty pages are written without a WAL flush check - acceptable
// for unit tests and in-memory-only workloads.  Wire it up once the WAL and
// buffer modules are integrated.
//
// Thread-safety note: single-threaded by design. The atomic state word and
// the contentLock RWMutex are present for correctness even in single-threaded
// use and will support goroutine-level concurrency without structural changes.

import (
	"sync"
	"sync/atomic"
)

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
// Matches PostgreSQL's BUFFER_LOCK_SHARE / BUFFER_LOCK_EXCLUSIVE macros.
type BufferLockMode uint8

const (
	// BufferLockShared allows concurrent reads.
	// Multiple goroutines may hold shared locks simultaneously.
	BufferLockShared BufferLockMode = 0

	// BufferLockExclusive is required for any page modification.
	// Exclusive excludes all other shared and exclusive holders.
	BufferLockExclusive BufferLockMode = 1
)

// ── WALFlusher ────────────────────────────────────────────────────────────────

// WALFlusher is called before a dirty buffer is written to disk to ensure the
// page's WAL record is durable before the data page reaches disk.
//
// In PostgreSQL this is XLogFlush(lsn) in xlog.c. The buffer manager calls
// it with the page's pd_lsn so the WAL module can flush all records up to
// (and including) that LSN before the smgr write proceeds.
//
// Set BufferPool.SetWALFlusher to wire up the WAL module. When nil, dirty
// pages are flushed without a WAL-durability check (acceptable for tests and
// in-memory-only workloads where crash recovery is not required).
type WALFlusher interface {
	// FlushUpTo ensures all WAL records with LSN ≤ lsn have been written to
	// stable storage. It is safe to call with lsn == 0 (no-op).
	FlushUpTo(lsn uint64) error
}

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
// enforces this with a panic.
type BufferDesc struct {
	Tag BufferTag

	// state packs pin count + usage_count + flags into a single atomic word.
	// All reads go through state.Load(); all writes use CAS or atomic OR/AND.
	// See the bit layout constants (bufRefcountMask, BmValid, ...) above.
	state atomic.Uint32

	// contentLock serialises page content access (shared read / exclusive write).
	// Callers must hold a pin before acquiring this lock.
	contentLock sync.RWMutex

	// ioCV is a condition variable that waiters block on when BM_IO_IN_PROGRESS
	// is set by a concurrent loader. In a single-threaded engine it is never
	// waited upon, but it is wired correctly so future goroutine-level
	// concurrency requires no structural changes.
	//
	// Usage pattern (multi-threaded extension):
	//   if BmIOInProgress set: ioCV.Wait() until cleared, then re-check tagMap.
	ioCV sync.Cond

	// cleanupWaiter is reserved for LockBufferForCleanup semantics:
	// 1 signals that one waiter needs all other pins to drain before it can
	// acquire exclusive access. Used by VACUUM for page-level reclamation.
	// TODO: wire up once VACUUM needs pin-count-zero exclusion.
	cleanupWaiter uint32
}

// ── Pin count ─────────────────────────────────────────────────────────────────

// Pin increments the pin count.
// Refcount lives in bits 0-17; adding 1 is safe as long as refcount < 262144.
func (d *BufferDesc) Pin() { d.state.Add(1) }

// Unpin decrements the pin count. Panics if already zero.
func (d *BufferDesc) Unpin() {
	for {
		s := d.state.Load()
		rc := s & bufRefcountMask
		if rc == 0 {
			panic("unpin of buffer with zero pin count")
		}
		if d.state.CompareAndSwap(s, s-1) {
			return
		}
	}
}

// PinCount returns the current pin count.
func (d *BufferDesc) PinCount() uint32 { return d.state.Load() & bufRefcountMask }

// IsPinned reports whether any caller holds a pin.
func (d *BufferDesc) IsPinned() bool { return d.PinCount() > 0 }

// ── Usage count ───────────────────────────────────────────────────────────────

// UsageCount returns the current usage count (0–maxUsageCount).
func (d *BufferDesc) UsageCount() uint8 {
	return uint8((d.state.Load() & bufUsageMask) >> bufUsageShift)
}

// setUsageCount overwrites the usage count field.
// Package-internal; used by eviction policies and tests.
func (d *BufferDesc) setUsageCount(n uint8) {
	for {
		s := d.state.Load()
		ns := (s &^ bufUsageMask) | (uint32(n)<<bufUsageShift)&bufUsageMask
		if d.state.CompareAndSwap(s, ns) {
			return
		}
	}
}

// bumpUsageCount increments usage_count by 1, capped at maxUsageCount.
// Matches PostgreSQL's StrategyGetBuffer:
//
//	if (buf->usage_count < BM_MAX_USAGE_COUNT) buf->usage_count++;
func (d *BufferDesc) bumpUsageCount() {
	for {
		s := d.state.Load()
		uc := (s & bufUsageMask) >> bufUsageShift
		if uc >= maxUsageCount {
			return // already at cap; no write needed
		}
		if d.state.CompareAndSwap(s, s+bufUsageOne) {
			return
		}
	}
}

// decUsageCount decrements usage_count by 1, floored at 0.
func (d *BufferDesc) decUsageCount() {
	for {
		s := d.state.Load()
		if s&bufUsageMask == 0 {
			return // already zero; no write needed
		}
		if d.state.CompareAndSwap(s, s-bufUsageOne) {
			return
		}
	}
}

// ── State flags ───────────────────────────────────────────────────────────────

// IsValid reports whether the slot holds a valid page.
func (d *BufferDesc) IsValid() bool { return d.state.Load()&BmValid != 0 }

// IsDirty reports whether the page has unflushed modifications.
func (d *BufferDesc) IsDirty() bool { return d.state.Load()&BmDirty != 0 }

// IsIOInProgress reports whether an smgr read/write is in progress.
func (d *BufferDesc) IsIOInProgress() bool { return d.state.Load()&BmIOInProgress != 0 }

// setFlags atomically ORs mask into the state word.
func (d *BufferDesc) setFlags(mask uint32) { d.state.Or(mask) }

// clearFlags atomically clears mask bits from the state word.
func (d *BufferDesc) clearFlags(mask uint32) { d.state.And(^mask) }

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
	// tagMap is the buffer hash table: tag → slot index.
	//
	// Simplification: a single map with no partitioning. PostgreSQL splits
	// its equivalent table into 128 partitions each guarded by its own
	// LWLock (NUM_BUFFER_PARTITIONS in buf_internals.h) to reduce lock
	// contention at many-core counts. With a single lock, every ReadBuffer
	// call serialises on tag lookup - the dominant bottleneck above ~8 cores.
	tagMap     map[BufferTag]BufferId
	numBuffers int
	rels       map[Oid]relEntry // relation registry for smgr-backed I/O
	policy     EvictionPolicy
	tracer     BufferTracer
	// walFlusher, if non-nil, is called with a page's pd_lsn before a dirty
	// eviction is written to disk. This enforces the WAL-before-data ordering
	// guarantee (write-ahead log rule). See WALFlusher interface above.
	walFlusher WALFlusher
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
		// Wire each ioCV to a fresh Mutex. Under goroutine-level concurrency a
		// waiter would call ioCV.Wait() after finding BM_IO_IN_PROGRESS set; the
		// loader calls ioCV.Broadcast() after clearing the flag.
		pool.descriptors[i].ioCV = sync.Cond{L: &sync.Mutex{}}
	}
	return pool
}

// SetTracer replaces the pool's BufferTracer. Pass NoopBufferTracer{} to
// disable tracing.
func (p *BufferPool) SetTracer(t BufferTracer) { p.tracer = t }

// SetWALFlusher wires up the WAL durability hook. When set, every dirty
// eviction calls flusher.FlushUpTo(pd_lsn) before writing the page to disk.
func (p *BufferPool) SetWALFlusher(flusher WALFlusher) { p.walFlusher = flusher }

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
// Concurrent-load safety: after a victim slot has been evicted but
// before the new page is loaded from disk, we re-check tagMap. Under
// concurrent access another goroutine may have loaded the same page into a
// different slot in the window between our cache-miss check and our eviction.
// The re-check catches this and returns the existing slot, avoiding a double
// load. This is the "check-after-eviction" pattern from PostgreSQL's
// BufferAlloc in bufmgr.c.
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

	// Evict the existing occupant.
	oldTag := desc.Tag
	if desc.IsValid() {
		delete(p.tagMap, oldTag)
	}
	if desc.IsDirty() {
		// WAL write-back rule: flush WAL up to the page's LSN before writing
		// the data page so the WAL record is always durable before the data.
		if p.walFlusher != nil {
			pageLSN := p.pages[id].LSN()
			if err := p.walFlusher.FlushUpTo(pageLSN); err != nil {
				return InvalidBufferId, err
			}
		}
		p.tracer.OnEvict(oldTag, true)
		p.writePage(oldTag, p.pages[id])
		desc.clearFlags(BmDirty)
	} else if desc.IsValid() {
		p.tracer.OnEvict(oldTag, false)
	}
	if desc.IsValid() {
		if tep, isTag := p.policy.(TagEvictionPolicy); isTag {
			tep.OnEvictTag(oldTag)
		}
	}

	// Check-after-eviction: another concurrent caller may have loaded
	// this tag into a different slot between our initial miss and now.
	// Re-check tagMap and return the existing slot if found.
	if id2, ok := p.tagMap[tag]; ok {
		// Restore evicted page state so the slot remains consistent.
		// (In this single-threaded engine the slot was already evicted above;
		// re-inserting it into tagMap is sufficient for correctness.)
		desc.Tag = oldTag
		if oldTag != (BufferTag{}) {
			p.tagMap[oldTag] = id
			desc.setFlags(BmValid)
		}
		d2 := &p.descriptors[id2]
		d2.Pin()
		p.policy.Access(id2, p.descriptors)
		p.tracer.OnHit(tag)
		return id2, nil
	}

	// Claim the slot.
	desc.Tag = tag
	desc.state.Store(BmValid) // reset: clear refcount, usage_count, all flags; set Valid
	desc.Pin()

	if tep, isTag := p.policy.(TagEvictionPolicy); isTag {
		tep.OnLoadTag(id, tag, p.descriptors)
	} else {
		p.policy.Access(id, p.descriptors)
	}

	// Mark I/O in progress while loading from disk.
	// Under goroutine-level concurrency, a second caller finding this flag set
	// on the same tag would call desc.ioCV.Wait() here until we broadcast below.
	desc.setFlags(BmIOInProgress)
	if !p.readPage(tag, p.pages[id]) {
		p.pages[id].init()
	}
	desc.clearFlags(BmIOInProgress)
	// Notify any concurrent waiters that I/O is complete.
	desc.ioCV.Broadcast()

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
// LockBuffer. Violating this ordering panics immediately.
//
//	id, _ := pool.ReadBuffer(tag)            // acquires pin
//	pool.LockBuffer(id, BufferLockShared)    // acquires content lock
//	page, _ := pool.GetPage(id)
//	... read page ...
//	pool.UnlockBuffer(id, BufferLockShared)
//	pool.UnpinBuffer(id)
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
// Pinned buffers are skipped. Dirty buffers are flushed before invalidation.
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
			continue
		}
		if desc.IsDirty() {
			p.writePage(tag, p.pages[i])
			desc.clearFlags(BmDirty)
		}
		delete(p.tagMap, tag)
		desc.Tag = BufferTag{}
		desc.state.Store(0) // clear valid, usage_count, refcount, all flags
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
