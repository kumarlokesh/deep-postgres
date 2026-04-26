package storage

// HeapHotSearchBuffer - follow a HOT chain to find the MVCC-visible tuple.
//
// Mirrors PostgreSQL's heap_hot_search_buffer() in heapam.c.
//
// A HOT (Heap Only Tuple) chain is a sequence of tuple versions that all live
// on the same heap page. The chain head may be reached via an LP_REDIRECT line
// pointer left by page pruning. Each non-head version points to its successor
// via t_ctid (forward pointer), and carries HEAP_HOT_UPDATED in infomask2.
// The chain tail carries HEAP_ONLY_TUPLE (no index entry points to it directly).
//
// Algorithm:
//  1. Follow one LP_REDIRECT hop (if present) to reach the LP_NORMAL chain head.
//  2. Decode the tuple at that slot.
//  3. Apply HeapTupleSatisfiesMVCC. If visible, return the tuple and slot.
//  4. If not visible and HEAP_HOT_UPDATED is set, follow t_ctid to the next
//     version. Abort if the chain leaves the page (HOT is always intra-page).
//  5. Repeat from step 2 until a visible version is found or the chain ends.

// HeapHotSearchBuffer searches the HOT chain starting at offset (1-based) on
// page for an MVCC-visible tuple version.
//
// blockNum is the block number of page; it is used to detect when t_ctid leaves
// the page (which terminates a HOT chain).
//
// Returns the visible HeapTuple, its final 1-based OffsetNumber, and true.
// Returns (nil, 0, false) if no visible version exists.
func HeapHotSearchBuffer(
	page *Page,
	blockNum BlockNumber,
	offset OffsetNumber, // 1-based
	snap *Snapshot,
	oracle TransactionOracle,
) (*HeapTuple, OffsetNumber, bool) {
	// Step 1: follow one LP_REDIRECT hop if present.
	if target, ok := page.FollowRedirect(offset); ok {
		offset = target
	}

	raw := page.Bytes()

	for {
		idx := int(offset) - 1
		if idx < 0 || idx >= page.ItemCount() {
			return nil, 0, false
		}
		lp, err := page.GetItemId(idx)
		if err != nil || !lp.IsNormal() {
			return nil, 0, false
		}

		off := int(lp.Off())
		length := int(lp.Len())
		if off+length > len(raw) {
			return nil, 0, false
		}

		tup, err := HeapTupleFromBytes(raw[off : off+length])
		if err != nil {
			return nil, 0, false
		}

		vis := HeapTupleSatisfiesMVCC(&tup.Header, snap, oracle)
		if vis.IsVisible() {
			SetHintBits(&tup.Header, oracle)
			return tup, offset, true
		}

		// Walk to the next HOT version only if this tuple has a successor.
		if !tup.Header.IsHotUpdated() {
			return nil, 0, false
		}

		nextBlock, nextOffset := tup.Header.Ctid()
		// HOT chains are always intra-page; a cross-page t_ctid means the chain ended.
		if nextBlock != blockNum {
			return nil, 0, false
		}
		// Self-referential t_ctid (chain tail before VACUUM updates it) → stop.
		if nextOffset == offset {
			return nil, 0, false
		}
		offset = nextOffset
	}
}
