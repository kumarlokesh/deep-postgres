package executor

// HeapInsert — write a new tuple into the heap.
//
// PostgreSQL inserts a heap tuple in heap_insert() (heapam.c):
//  1. Find a page with sufficient free space using the Free Space Map.
//  2. If none found, extend the relation to add a new page.
//  3. Write the tuple, update the FSM with the page's new free space,
//     and clear the page's all-visible bit in the Visibility Map.
//
// This executor mirrors that algorithm.  It replaces the ad-hoc insertTuple
// helper used in tests so that FSM bookkeeping happens on every insert.

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// HeapInsert writes tup into rel.  It consults rel.FSM to find an existing
// page with enough room before falling back to extending the relation.
//
// Returns the physical location (block, 1-based offset) where the tuple was
// written.
func HeapInsert(
	rel *storage.Relation,
	tup *storage.HeapTuple,
) (storage.BlockNumber, storage.OffsetNumber, error) {
	needed := uint16(tup.Len()) + storage.ItemIdSize

	// ── Try FSM ───────────────────────────────────────────────────────────────
	if blk, ok := rel.FSM.FindWithSpace(needed); ok {
		id, err := rel.ReadBlock(storage.ForkMain, blk)
		if err != nil {
			return 0, 0, fmt.Errorf("heapinsert: ReadBlock(%d): %w", blk, err)
		}
		pg, err := rel.Pool.GetPageForWrite(id)
		if err != nil {
			rel.Pool.UnpinBuffer(id) //nolint:errcheck
			return 0, 0, err
		}

		// Re-check actual free space; FSM may be stale.
		if pg.FreeSpace() >= int(needed) {
			idx, err := pg.InsertTuple(tup.ToBytes())
			if err != nil {
				rel.Pool.UnpinBuffer(id) //nolint:errcheck
				return 0, 0, fmt.Errorf("heapinsert: InsertTuple: %w", err)
			}
			rel.FSM.Update(blk, uint16(pg.FreeSpace()))
			rel.VM.ClearAllVisible(blk)
			rel.Pool.UnpinBuffer(id) //nolint:errcheck
			return blk, storage.OffsetNumber(idx + 1), nil
		}

		// FSM was stale; update it and fall through to extend.
		rel.FSM.Update(blk, uint16(pg.FreeSpace()))
		rel.Pool.UnpinBuffer(id) //nolint:errcheck
	}

	// ── Extend the relation ───────────────────────────────────────────────────
	blk, id, err := rel.Extend(storage.ForkMain)
	if err != nil {
		return 0, 0, fmt.Errorf("heapinsert: Extend: %w", err)
	}

	pg, err := rel.Pool.GetPageForWrite(id)
	if err != nil {
		rel.Pool.UnpinBuffer(id) //nolint:errcheck
		return 0, 0, err
	}

	idx, err := pg.InsertTuple(tup.ToBytes())
	if err != nil {
		rel.Pool.UnpinBuffer(id) //nolint:errcheck
		return 0, 0, fmt.Errorf("heapinsert: InsertTuple on new page: %w", err)
	}

	rel.FSM.Update(blk, uint16(pg.FreeSpace()))
	// A freshly extended page is not all-visible (xmin of the new tuple is
	// in-progress), so no VM update is needed here.
	rel.Pool.UnpinBuffer(id) //nolint:errcheck
	return blk, storage.OffsetNumber(idx + 1), nil
}
