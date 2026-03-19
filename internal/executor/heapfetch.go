package executor

// HeapFetch — fetch the MVCC-visible version of a tuple at a specific heap TID.
//
// The TID supplied by an index entry may point to an LP_REDIRECT slot created
// by HOT chain compaction.  HeapFetch follows that single redirect hop to the
// LP_NORMAL chain head before checking visibility.
//
// HOT chain note: after reaching the chain head we do not walk the t_ctid
// forward pointer here because we have not yet implemented heap UPDATE.  When
// UPDATE is added, heap_hot_search_buffer logic (walk t_ctid until flags say
// HEAP_ONLY_TUPLE) can be layered in at that point.

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// HeapFetch loads the block at (block, offset), follows any LP_REDIRECT to
// the HOT chain head, applies MVCC visibility, and returns the tuple if
// visible.  Returns nil, nil if the tuple is not visible to snap.
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

	// Follow one LP_REDIRECT hop to the HOT chain head (same page, different slot).
	if lp.IsRedirect() {
		// lp.Off() is the 1-based OffsetNumber of the chain head on this page.
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

	vis := storage.HeapTupleSatisfiesMVCC(&tup.Header, snap, oracle)
	if !vis.IsVisible() {
		return nil, nil
	}
	storage.SetHintBits(&tup.Header, oracle)

	return &ScanTuple{Tuple: tup, Block: block, Offset: offset}, nil
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
