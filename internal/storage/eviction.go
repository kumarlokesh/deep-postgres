package storage

// EvictionPolicy is the pluggable interface for buffer replacement strategies.
//
// PostgreSQL uses clock-sweep (StrategyGetBuffer in freelist.c). This
// interface allows alternative policies - LRU, ARC, etc. - to be swapped in
// for experimentation without changing the rest of the buffer pool.
//
// Implementations must handle the case where Victim is called on a pool in
// which every buffer is pinned. In that case the implementation should
// return InvalidBufferId to signal exhaustion.
//
// Concurrency: single-threaded by design - no locking required.

// EvictionPolicy selects a victim buffer for replacement.
type EvictionPolicy interface {
	// Init is called once after the pool is created with the total number of
	// buffer slots.
	Init(numBuffers int)

	// Access is called every time a buffer is pinned (cache hit or new
	// allocation). The policy uses this to maintain its internal state (e.g.
	// incrementing usage count for clock-sweep, updating the LRU position).
	Access(id BufferId, descriptors []BufferDesc)

	// Victim returns the index of a buffer that may be evicted.
	// The pool will still verify that the returned buffer is unpinned before
	// evicting it (the policy's state may be stale).
	// Returns (InvalidBufferId, false) if no victim is available.
	Victim(descriptors []BufferDesc) (BufferId, bool)
}

// ── ClockSweep ────────────────────────────────────────────────────────────────

// ClockSweepPolicy implements the clock-sweep algorithm used by PostgreSQL.
//
// usage_count (0–maxUsageCount) is packed into bits 18-22 of each BufferDesc's
// atomic state word.  The clock hand sweeps through the ring:
//   - If the buffer is pinned: skip.
//   - If usage_count > 0: decrement and skip.
//   - If usage_count == 0 and unpinned: victim.
//
// Access calls bumpUsageCount which increments by 1 up to maxUsageCount.
// A page touched once survives 1 sweep; a page touched 5+ times survives 5.
type ClockSweepPolicy struct {
	clockHand  int
	numBuffers int
}

func (cs *ClockSweepPolicy) Init(numBuffers int) {
	cs.numBuffers = numBuffers
	cs.clockHand = 0
}

func (cs *ClockSweepPolicy) Access(id BufferId, descriptors []BufferDesc) {
	descriptors[id].bumpUsageCount()
}

func (cs *ClockSweepPolicy) Victim(descriptors []BufferDesc) (BufferId, bool) {
	// Pre-check: reject immediately if every slot is pinned.
	allPinned := true
	for i := range descriptors {
		if !descriptors[i].IsPinned() {
			allPinned = false
			break
		}
	}
	if allPinned {
		return InvalidBufferId, false
	}

	for {
		id := cs.clockHand
		cs.clockHand = (cs.clockHand + 1) % cs.numBuffers

		desc := &descriptors[id]
		if desc.IsPinned() {
			continue
		}
		if desc.UsageCount() > 0 {
			desc.decUsageCount()
			continue
		}
		return BufferId(id), true
	}
}

// ── LRU ───────────────────────────────────────────────────────────────────────

// LRUPolicy evicts the Least Recently Used buffer - the one that was accessed
// longest ago.
//
// The policy maintains a doubly-linked list of buffer IDs ordered from most
// recently used (head) to least recently used (tail). Access moves a buffer
// to the head; Victim returns the tail.
type LRUPolicy struct {
	order []BufferId // order[i] = buffer ID at position i (0 = MRU, n-1 = LRU)
	pos   []int      // pos[id] = current index of buffer id in order
	n     int
}

func (lru *LRUPolicy) Init(numBuffers int) {
	lru.n = numBuffers
	lru.order = make([]BufferId, numBuffers)
	lru.pos = make([]int, numBuffers)
	for i := range lru.order {
		lru.order[i] = BufferId(i)
		lru.pos[i] = i
	}
}

// Access moves id to the front (MRU position).
func (lru *LRUPolicy) Access(id BufferId, _ []BufferDesc) {
	cur := lru.pos[id]
	if cur == 0 {
		return // already MRU
	}
	// Shift everything left of cur one position right.
	copy(lru.order[1:cur+1], lru.order[0:cur])
	lru.order[0] = id
	// Update position map.
	for i := 0; i <= cur; i++ {
		lru.pos[lru.order[i]] = i
	}
}

// Victim returns the LRU (tail) buffer that is not pinned.
func (lru *LRUPolicy) Victim(descriptors []BufferDesc) (BufferId, bool) {
	// Walk from LRU end toward MRU end.
	for i := lru.n - 1; i >= 0; i-- {
		id := lru.order[i]
		if !descriptors[id].IsPinned() {
			return id, true
		}
	}
	return InvalidBufferId, false
}
