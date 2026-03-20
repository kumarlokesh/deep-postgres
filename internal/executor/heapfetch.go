package executor

// HeapFetch — fetch the MVCC-visible version of a tuple at a specific heap TID.
//
// The algorithm mirrors PostgreSQL's heap_hot_search_buffer() in heapam.c:
//
//  1. Follow one LP_REDIRECT hop (left by HOT chain pruning) to reach the
//     LP_NORMAL chain head.
//  2. Walk the t_ctid forward pointer chain while the current tuple is not
//     visible to the snapshot but has HEAP_HOT_UPDATED set.  Each hop stays
//     on the same page (HOT chains are always intra-page until VACUUM removes
//     them via LP_REDIRECT).
//  3. Return the first visible version found, or nil if none is visible.
//
// Buffer discipline: one page is pinned for the duration of the call.

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// HeapFetch loads the block at (block, offset), follows any LP_REDIRECT to
// the HOT chain head, walks the t_ctid chain for HOT-updated tuples, and
// returns the first MVCC-visible version.  Returns nil, nil if nothing is
// visible.
func HeapFetch(
	rel *storage.Relation,
	snap *storage.Snapshot,
	oracle storage.TransactionOracle,
	block storage.BlockNumber,
	offset storage.OffsetNumber,
) (*ScanTuple, error) {
	id, err := rel.ReadBlock(storage.ForkMain, block)
	if err != nil {
		return nil, fmt.Errorf("heapfetch: ReadBlock(%d): %w", block, err)
	}
	defer rel.Pool.UnpinBuffer(id) //nolint:errcheck

	page, err := rel.Pool.GetPage(id)
	if err != nil {
		return nil, fmt.Errorf("heapfetch: GetPage: %w", err)
	}

	// offset is 1-based; ItemId array is 0-based.
	lp, err := page.GetItemId(int(offset) - 1)
	if err != nil {
		return nil, fmt.Errorf("heapfetch: GetItemId(%d): %w", offset-1, err)
	}

	// Follow one LP_REDIRECT hop to the HOT chain head (same page).
	// lp.Off() for an LP_REDIRECT is the 1-based OffsetNumber of the target.
	if lp.IsRedirect() {
		chainOffset := lp.Off()
		lp, err = page.GetItemId(int(chainOffset) - 1)
		if err != nil {
			return nil, fmt.Errorf("heapfetch: GetItemId after redirect(%d): %w", chainOffset-1, err)
		}
		offset = chainOffset
	}

	if !lp.IsNormal() {
		// LP_UNUSED or LP_DEAD — tuple is gone.
		return nil, nil
	}

	tup, err := decodeLPTuple(page, lp)
	if err != nil {
		return nil, fmt.Errorf("heapfetch: decode: %w", err)
	}

	// Walk the HOT chain: if the current version is not visible but has
	// HEAP_HOT_UPDATED, follow t_ctid to the next version on this page.
	for {
		vis := storage.HeapTupleSatisfiesMVCC(&tup.Header, snap, oracle)
		if vis.IsVisible() {
			storage.SetHintBits(&tup.Header, oracle)
			return &ScanTuple{Tuple: tup, Block: block, Offset: offset}, nil
		}

		// Stop if there is no HOT successor on this page.
		if !tup.Header.IsHotUpdated() {
			break
		}

		nextBlock, nextOffset := tup.Header.Ctid()
		// HOT chains are intra-page; if t_ctid left the page, stop.
		if nextBlock != block {
			break
		}

		nextLp, lerr := page.GetItemId(int(nextOffset) - 1)
		if lerr != nil || !nextLp.IsNormal() {
			break
		}
		nextTup, terr := decodeLPTuple(page, nextLp)
		if terr != nil {
			break
		}
		tup = nextTup
		offset = nextOffset
	}

	return nil, nil
}

// decodeLPTuple reads the tuple bytes pointed to by an LP_NORMAL line pointer.
func decodeLPTuple(page *storage.Page, lp storage.ItemIdData) (*storage.HeapTuple, error) {
	off := int(lp.Off())
	length := int(lp.Len())
	raw := page.Bytes()
	if off+length > len(raw) {
		return nil, fmt.Errorf("line pointer out of bounds (off=%d len=%d pagesize=%d)",
			off, length, len(raw))
	}
	tup, err := storage.HeapTupleFromBytes(raw[off : off+length])
	if err != nil {
		return nil, err
	}
	return tup, nil
}
