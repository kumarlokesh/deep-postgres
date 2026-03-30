package storage

// Page-level VACUUM logic.
//
// PostgreSQL's VACUUM has two phases per heap page:
//
//  Phase 1 - pruning and freezing (heap_page_prune in heapam.c):
//    • HOT chain pruning: dead LP_NORMAL slots that have HEAP_HOT_UPDATED are
//      converted to LP_REDIRECT pointing at the live chain head.
//    • Dead HOT-only tuples (HEAP_ONLY_TUPLE, no index entry): LP_UNUSED.
//    • Regular dead tuples: LP_DEAD (to be cleaned after index vacuum).
//    • Old LP_DEAD slots from prior passes: LP_UNUSED.
//
//  Phase 2 - freezing (heap_freeze_tuple in heapam.c):
//    • Tuples whose xmin is committed and older than the freeze limit have
//      their xmin replaced with FrozenTransactionId (XID 2), making them
//      permanently visible and safe against XID wraparound.
//
// Simplifications in this research implementation:
//  • No index vacuum phase - regular dead tuples go directly to LP_UNUSED
//    instead of LP_DEAD, because we have no index entry removal step.
//  • No page compaction - pd_lower / pd_upper are not adjusted; freed tuple
//    data space is not reclaimed until a future VACUUM FULL (not implemented).

// VacuumOptions controls what a VacuumPage call will do.
type VacuumOptions struct {
	// OldestXmin is the horizon below which committed xmax values are dead to
	// all current and future snapshots. Obtain from TransactionManager.GlobalXmin.
	OldestXmin TransactionId

	// FreezeLimit: tuples with committed xmin < FreezeLimit have their xmin
	// replaced with FrozenTransactionId to prevent XID wraparound.
	// Set to 0 to disable freezing.
	FreezeLimit TransactionId
}

// VacuumPageStats records what VacuumPage did on one page.
type VacuumPageStats struct {
	TuplesRemoved int  // dead tuples reclaimed (LP_DEAD→LP_UNUSED or LP_NORMAL→LP_UNUSED)
	TuplesFrozen  int  // tuples whose xmin was replaced with FrozenTransactionId
	HOTPruned     int  // LP_NORMAL chain heads converted to LP_REDIRECT
	AllVisible    bool // true if every remaining tuple is visible to all snapshots
}

// VacuumPage prunes dead tuples and HOT chains on page and optionally freezes
// old xmin values. The page must have been obtained via GetPageForWrite so
// that changes are visible to the buffer pool.
//
// Returns stats and whether the page was modified.
func VacuumPage(page *Page, opts VacuumOptions, oracle TransactionOracle) (VacuumPageStats, bool) {
	var stats VacuumPageStats
	modified := false
	n := page.ItemCount()

	for i := 0; i < n; i++ {
		lp, err := page.GetItemId(i)
		if err != nil {
			continue
		}

		switch {
		case lp.IsUnused(), lp.IsRedirect():
			// Already free or already pruned - nothing to do.
			continue

		case lp.IsDead():
			// LP_DEAD from a prior pass (e.g. heap delete): reclaim the slot.
			// In a full implementation this happens only after the index entries
			// for this TID have been removed. We skip index vacuum, so we go
			// straight to LP_UNUSED.
			_ = page.SetItemIdUnused(i) //nolint:errcheck
			stats.TuplesRemoved++
			modified = true

		case lp.IsNormal():
			raw := page.Bytes()
			off := int(lp.Off())
			if off+HeapTupleHeaderSize > len(raw) {
				continue
			}
			hdr := decodeHeapTupleHeader(raw[off:])

			if tupleDeadToAll(&hdr, opts.OldestXmin, oracle) {
				modified = true
				if hdr.IsHotUpdated() {
					// HOT chain head: find the live successor and redirect to it.
					liveOff := hotChainLiveHead(page, hdr.TCtidOffset, opts.OldestXmin, oracle)
					if liveOff != InvalidOffsetNumber {
						_ = page.SetItemIdRedirect(i, liveOff) //nolint:errcheck
						stats.HOTPruned++
					} else {
						// Entire chain is dead.
						_ = page.SetItemIdUnused(i) //nolint:errcheck
						stats.TuplesRemoved++
					}
				} else {
					// Either a HOT-only version (no index entry → LP_UNUSED) or a
					// regular deleted tuple.  We reclaim both as LP_UNUSED for
					// simplicity (skipping the index-vacuum phase).
					_ = page.SetItemIdUnused(i) //nolint:errcheck
					stats.TuplesRemoved++
				}
				continue
			}

			// Tuple is alive: consider freezing it.
			if shouldFreeze(&hdr, opts.FreezeLimit, oracle) {
				hdr.TXmin = FrozenTransactionId
				if err := page.UpdateTupleHeader(i, &hdr); err == nil {
					stats.TuplesFrozen++
					modified = true
				}
			}
		}
	}

	// After the cleanup pass, check if every remaining tuple is visible to
	// all current and future snapshots. If so, the caller can mark the page
	// in the VisibilityMap and skip per-tuple MVCC checks on future scans.
	stats.AllVisible = PageIsAllVisible(page, opts.OldestXmin, oracle)

	return stats, modified
}

// ── Page compaction ───────────────────────────────────────────────────────────

// CompactPage rewrites page in-place, packing all LP_NORMAL tuples tightly
// from pd_special downward (PostgreSQL's PageRepairFragmentation).
//
// Regular VACUUM frees item slots (LP_UNUSED) but leaves the original tuple
// bytes sitting below pd_upper, creating internal fragmentation that makes
// FreeSpace() report less available space than actually exists. CompactPage
// eliminates that gap: after compaction, the gap between pd_lower (item
// pointer array end) and pd_upper (tuple data start) is exactly the usable
// free space.
//
// LP_REDIRECT and LP_UNUSED slots retain their state but consume no data
// bytes. The item slot positions (0-based indices) are unchanged so that
// index entries referencing slot numbers remain valid.
//
// Returns the number of bytes reclaimed (new FreeSpace - old FreeSpace).
func CompactPage(page *Page) int {
	n := page.ItemCount()
	if n == 0 {
		return 0
	}

	prevFree := page.FreeSpace()

	// Snapshot every item pointer and the live tuple data it points to.
	type liveSlot struct {
		index int
		data  []byte
	}
	live := make([]liveSlot, 0, n)

	allIds := make([]ItemIdData, n)
	raw := page.Bytes()

	for i := 0; i < n; i++ {
		lp, err := page.GetItemId(i)
		if err != nil {
			continue
		}
		allIds[i] = lp

		if !lp.IsNormal() {
			continue // redirect / unused / dead - no tuple bytes
		}
		off := int(lp.Off())
		ln := int(lp.Len())
		if off+ln > PageSize {
			continue
		}
		buf := make([]byte, ln)
		copy(buf, raw[off:off+ln])
		live = append(live, liveSlot{index: i, data: buf})
	}

	// Zero the entire tuple data area (from end of item array to pd_special),
	// then repack live tuples downward from pd_special.
	h := page.Header()
	tupleAreaStart := PageHeaderSize + n*ItemIdSize
	for i := tupleAreaStart; i < int(h.PdSpecial); i++ {
		raw[i] = 0
	}
	h.PdUpper = h.PdSpecial

	for _, slot := range live {
		ln := len(slot.data)
		newUpper := int(h.PdUpper) - ln
		copy(raw[newUpper:newUpper+ln], slot.data)
		allIds[slot.index] = NewItemId(uint16(newUpper), uint16(ln))
		h.PdUpper = LocationIndex(newUpper)
	}

	// Write updated item pointers and header back.
	for i, id := range allIds {
		encodeItemId(raw[itemIdOffset(i):itemIdOffset(i)+ItemIdSize], id)
	}
	encodeHeader(raw[:PageHeaderSize], &h)

	return page.FreeSpace() - prevFree
}

// ── helpers ───────────────────────────────────────────────────────────────────

// tupleDeadToAll reports whether a tuple's deletion is visible to every
// possible snapshot: xmax is set, committed, and < oldestXmin.
func tupleDeadToAll(hdr *HeapTupleHeader, oldestXmin TransactionId, oracle TransactionOracle) bool {
	xmax := hdr.TXmax
	if xmax == InvalidTransactionId {
		return false
	}
	im := hdr.Infomask()
	if im&HeapXmaxInvalid != 0 {
		return false
	}
	// Use hint bits when available to avoid a clog lookup.
	committed := im&HeapXmaxCommitted != 0
	if !committed {
		committed = oracle.Status(xmax) == TxCommitted
	}
	return committed && xmax < oldestXmin
}

// shouldFreeze reports whether a tuple's xmin should be replaced with
// FrozenTransactionId to prevent XID wraparound.
func shouldFreeze(hdr *HeapTupleHeader, freezeLimit TransactionId, oracle TransactionOracle) bool {
	if freezeLimit == 0 {
		return false
	}
	xmin := hdr.TXmin
	if xmin == FrozenTransactionId || xmin == InvalidTransactionId {
		return false // already frozen or invalid
	}
	if xmin >= freezeLimit {
		return false // too recent
	}
	im := hdr.Infomask()
	// Already frozen (both hint bits set simultaneously means frozen in PG;
	// for us, FrozenTransactionId in xmin is the canonical check above).
	if im&HeapXminInvalid != 0 {
		return false // aborted inserter — don't freeze
	}
	if im&HeapXminCommitted != 0 {
		return true // hint bit says committed
	}
	return oracle.Status(xmin) == TxCommitted
}

// hotChainLiveHead follows t_ctid forward through a HOT chain starting at
// startOffset (1-based), skipping dead intermediate versions, and returns the
// OffsetNumber of the first live (not dead-to-all) version.
// Returns InvalidOffsetNumber if the entire chain is dead.
func hotChainLiveHead(page *Page, startOffset OffsetNumber, oldestXmin TransactionId, oracle TransactionOracle) OffsetNumber {
	off := startOffset
	// Guard against infinite loops (chain length bounded by page item count).
	for range page.ItemCount() + 1 {
		if off == InvalidOffsetNumber {
			return InvalidOffsetNumber
		}
		lp, err := page.GetItemId(int(off) - 1)
		if err != nil || !lp.IsNormal() {
			return InvalidOffsetNumber
		}
		raw := page.Bytes()
		hdrOff := int(lp.Off())
		if hdrOff+HeapTupleHeaderSize > len(raw) {
			return InvalidOffsetNumber
		}
		hdr := decodeHeapTupleHeader(raw[hdrOff:])

		if !tupleDeadToAll(&hdr, oldestXmin, oracle) {
			return off // this version is alive - it's the live head
		}
		if !hdr.IsHotUpdated() {
			return InvalidOffsetNumber // dead and no successor
		}
		_, off = hdr.Ctid() // follow t_ctid to the next version
	}
	return InvalidOffsetNumber // chain too long (shouldn't happen)
}
