package storage

// ARCPolicy implements the Adaptive Replacement Cache algorithm.
//
// Reference: Megiddo & Modha, "ARC: A Self-Tuning, Low Overhead Replacement
// Cache" (USENIX FAST 2003).
//
// ARC maintains four lists:
//   T1 — pages seen exactly once recently (recency); LRU-ordered.
//   T2 — pages seen more than once (frequency); LRU-ordered.
//   B1 — ghost entries for recently evicted T1 pages (no data, tag only).
//   B2 — ghost entries for recently evicted T2 pages (no data, tag only).
//
// A target size p (0 ≤ p ≤ c) controls how much of the cache is reserved for
// T1; the rest goes to T2.  p adapts at runtime:
//   • A ghost hit in B1 → p increases (recency deserves more room).
//   • A ghost hit in B2 → p decreases (frequency deserves more room).
//
// Interface extension: ARCPolicy implements the optional TagEvictionPolicy
// interface so the buffer pool can pass the incoming page's tag to
// VictimForTag (needed by the REPLACE rule) and notify the policy of evictions
// and loads via OnEvictTag / OnLoadTag.

import "container/list"

// TagEvictionPolicy extends EvictionPolicy for policies that need tag-level
// visibility into the pool's miss/evict path (e.g. ARC ghost lists).
//
// When a buffer pool contains a TagEvictionPolicy:
//   - VictimForTag is called instead of Victim on cache misses.
//   - OnEvictTag is called with the outgoing tag before the slot is reused.
//   - OnLoadTag is called with (id, newTag) after the slot is reassigned.
type TagEvictionPolicy interface {
	EvictionPolicy
	VictimForTag(newTag BufferTag, descriptors []BufferDesc) (BufferId, bool)
	OnEvictTag(evictedTag BufferTag)
	OnLoadTag(id BufferId, newTag BufferTag, descriptors []BufferDesc)
}

// ── arcList ───────────────────────────────────────────────────────────────────

// arcList is an LRU-ordered doubly-linked list of BufferTags.
type arcList struct {
	lst  *list.List
	elem map[BufferTag]*list.Element
}

func newArcList() *arcList {
	return &arcList{lst: list.New(), elem: make(map[BufferTag]*list.Element)}
}

func (l *arcList) contains(tag BufferTag) bool { _, ok := l.elem[tag]; return ok }
func (l *arcList) len() int                    { return l.lst.Len() }

func (l *arcList) pushFront(tag BufferTag) {
	e := l.lst.PushFront(tag)
	l.elem[tag] = e
}

func (l *arcList) moveToFront(tag BufferTag) {
	if e, ok := l.elem[tag]; ok {
		l.lst.MoveToFront(e)
	}
}

// removeLRU removes the LRU (tail) entry.
func (l *arcList) removeLRU() {
	e := l.lst.Back()
	if e == nil {
		return
	}
	tag := e.Value.(BufferTag) //nolint:forcetypeassert
	l.lst.Remove(e)
	delete(l.elem, tag)
}

func (l *arcList) remove(tag BufferTag) {
	if e, ok := l.elem[tag]; ok {
		l.lst.Remove(e)
		delete(l.elem, tag)
	}
}

// ── ARCPolicy ─────────────────────────────────────────────────────────────────

// ARCPolicy is the ARC buffer replacement policy.
type ARCPolicy struct {
	c int // cache capacity (== numBuffers)
	p int // target size for T1 (0 ≤ p ≤ c)

	t1, t2 *arcList // resident lists (map to actual buffer slots)
	b1, b2 *arcList // ghost lists (tags of recently evicted pages)

	tagToSlot map[BufferTag]BufferId // tag → buffer slot (for resident pages)
	slotToTag map[BufferId]BufferTag // slot → tag
}

// Init initialises the policy for a pool of numBuffers slots.
func (a *ARCPolicy) Init(numBuffers int) {
	a.c = numBuffers
	a.p = 0
	a.t1 = newArcList()
	a.t2 = newArcList()
	a.b1 = newArcList()
	a.b2 = newArcList()
	a.tagToSlot = make(map[BufferTag]BufferId)
	a.slotToTag = make(map[BufferId]BufferTag)
}

// Access handles a cache HIT: the page is already in T1 or T2.
// Promote the page to T2 MRU (it has now been seen more than once).
func (a *ARCPolicy) Access(id BufferId, _ []BufferDesc) {
	tag, ok := a.slotToTag[id]
	if !ok {
		return
	}
	if a.t1.contains(tag) {
		a.t1.remove(tag)
		a.t2.pushFront(tag)
	} else if a.t2.contains(tag) {
		a.t2.moveToFront(tag)
	}
}

// Victim is the base EvictionPolicy method.  For ARC callers use VictimForTag.
// This fallback applies the REPLACE rule with p but without B2-membership check.
func (a *ARCPolicy) Victim(descriptors []BufferDesc) (BufferId, bool) {
	return a.replace(BufferTag{}, false, descriptors)
}

// VictimForTag selects a victim for a miss loading newTag.
// Knowing newTag allows the full REPLACE rule: if newTag ∈ B2, we prefer to
// evict from T1 even when |T1| == p.
func (a *ARCPolicy) VictimForTag(newTag BufferTag, descriptors []BufferDesc) (BufferId, bool) {
	inB2 := a.b2.contains(newTag)
	return a.replace(newTag, inB2, descriptors)
}

// OnEvictTag is called by the pool after it has selected a victim slot but
// before reusing it.  ARC moves the evicted tag to the appropriate ghost list.
func (a *ARCPolicy) OnEvictTag(evictedTag BufferTag) {
	if a.t1.contains(evictedTag) {
		a.t1.remove(evictedTag)
		// Cap ghost list: if B1 already has c entries, drop its LRU.
		if a.b1.len() >= a.c {
			a.b1.removeLRU()
		}
		a.b1.pushFront(evictedTag)
	} else if a.t2.contains(evictedTag) {
		a.t2.remove(evictedTag)
		if a.b2.len() >= a.c {
			a.b2.removeLRU()
		}
		a.b2.pushFront(evictedTag)
	}
	delete(a.tagToSlot, evictedTag)
	// slotToTag is updated in OnLoadTag when the slot is reused.
}

// OnLoadTag is called by the pool after assigning slot id to newTag.
// ARC processes the new page: ghost-hit in B1 or B2 adjusts p; otherwise
// the page joins T1.  Ghost-hits are promoted directly to T2.
func (a *ARCPolicy) OnLoadTag(id BufferId, newTag BufferTag, _ []BufferDesc) {
	// Update slot→tag mapping.
	delete(a.slotToTag, id) // remove old entry if any
	a.slotToTag[id] = newTag
	a.tagToSlot[newTag] = id

	switch {
	case a.b1.contains(newTag):
		// Ghost hit in B1 → increase p (recency needs more room).
		delta := 1
		if a.b2.len() > a.b1.len() {
			delta = a.b2.len() / a.b1.len()
		}
		a.p = min(a.p+delta, a.c)
		a.b1.remove(newTag)
		a.t2.pushFront(newTag)

	case a.b2.contains(newTag):
		// Ghost hit in B2 → decrease p (frequency needs more room).
		delta := 1
		if a.b1.len() > a.b2.len() {
			delta = a.b1.len() / a.b2.len()
		}
		a.p = max(a.p-delta, 0)
		a.b2.remove(newTag)
		a.t2.pushFront(newTag)

	default:
		// Cold miss → enter T1.
		a.t1.pushFront(newTag)
	}
}

// replace picks a victim slot using the ARC REPLACE rule.
// inB2 should be true when the incoming page is in B2 (alters the T1/T2 choice).
func (a *ARCPolicy) replace(
	_ BufferTag, inB2 bool, descriptors []BufferDesc,
) (BufferId, bool) {
	// Prefer a free (never-used) slot before applying the eviction rule.
	// ARC REPLACE is only meaningful when the pool is full (|T1|+|T2| == c).
	for i := range descriptors {
		if !descriptors[i].IsValid() && !descriptors[i].IsPinned() {
			return BufferId(i), true
		}
	}

	// Pool is full: apply REPLACE.
	// Prefer T1 if it has exceeded its target (or if the incoming page is in B2
	// and T1 is non-empty — to defend the frequency list).
	t1big := a.t1.len() > a.p
	t1ok := a.t1.len() > 0 && (t1big || inB2)

	if t1ok {
		if id, ok := a.lruSlot(a.t1, descriptors); ok {
			return id, true
		}
	}
	// Fall back to T2.
	if id, ok := a.lruSlot(a.t2, descriptors); ok {
		return id, true
	}
	// Last resort: scan unpinned buffers.
	for i := range descriptors {
		if !descriptors[i].IsPinned() {
			return BufferId(i), true
		}
	}
	return InvalidBufferId, false
}

// lruSlot returns the buffer slot for the LRU (tail) entry of lst, if it is
// unpinned. Returns (InvalidBufferId, false) if the slot is pinned or unknown.
func (a *ARCPolicy) lruSlot(lst *arcList, descriptors []BufferDesc) (BufferId, bool) {
	// Walk from LRU to MRU until we find an unpinned slot.
	e := lst.lst.Back()
	for e != nil {
		tag := e.Value.(BufferTag) //nolint:forcetypeassert
		if id, ok := a.tagToSlot[tag]; ok {
			if !descriptors[id].IsPinned() {
				return id, true
			}
		}
		e = e.Prev()
	}
	return InvalidBufferId, false
}
