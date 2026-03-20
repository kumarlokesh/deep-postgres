package executor

// HeapUpdate — replace a heap tuple with a new version.
//
// PostgreSQL performs heap UPDATE in heapam.c:heap_update().  The key steps:
//  1. Lock the old tuple's buffer.
//  2. Check that the old tuple is still visible to the updating transaction
//     (concurrent updates may have beaten us — we skip that check here since
//     we are single-threaded).
//  3. Write the new tuple version.  If the same page has room and the indexed
//     columns are unchanged, perform a HOT (Heap Only Tuple) update: insert
//     the new version on the same page and mark it HEAP_ONLY_TUPLE.
//     Otherwise allocate space on a new page (regular update).
//  4. Update the old tuple header: set xmax = current XID, clear
//     HEAP_XMAX_INVALID, set HEAP_UPDATED, optionally set HEAP_HOT_UPDATED,
//     and point t_ctid at the new version.
//
// This implementation does not handle:
//   - Concurrent update conflicts (assumed single-threaded).
//   - Key-share / exclusive row locking.
//   - WAL logging (tested separately via the wal_apply path).

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// HeapUpdate replaces the heap tuple at (oldBlock, oldOffset) with newTuple.
//
// It attempts a HOT update (same page) if the page has enough free space;
// otherwise it extends the relation to a new block.
//
// newTuple.Header should be pre-populated with xmin, natts, and data; this
// function sets the HEAP_ONLY_TUPLE flag and t_ctid on the new tuple and
// sets xmax, HEAP_UPDATED, HEAP_HOT_UPDATED (if HOT), and t_ctid on the old
// tuple.
//
// Returns (newBlock, newOffset, error).
func HeapUpdate(
	rel *storage.Relation,
	xid storage.TransactionId,
	oldBlock storage.BlockNumber,
	oldOffset storage.OffsetNumber, // 1-based
	newTuple *storage.HeapTuple,
) (storage.BlockNumber, storage.OffsetNumber, error) {
	newTupleLen := newTuple.Len()

	// ── Try HOT (same page) ──────────────────────────────────────────────────
	{
		id, err := rel.ReadBlock(storage.ForkMain, oldBlock)
		if err != nil {
			return 0, 0, fmt.Errorf("heapupdate: ReadBlock(%d): %w", oldBlock, err)
		}

		pg, err := rel.Pool.GetPageForWrite(id)
		if err != nil {
			rel.Pool.UnpinBuffer(id) //nolint:errcheck
			return 0, 0, err
		}

		// FreeSpace must accommodate the new tuple data + one ItemId slot.
		if pg.FreeSpace() >= newTupleLen+storage.ItemIdSize {
			newOffset := storage.OffsetNumber(pg.ItemCount() + 1) // predicted slot

			// Stamp new tuple before writing: HOT flag + self-referential t_ctid.
			newTuple.Header.TInfomask2 = uint16(
				storage.Infomask2Flags(newTuple.Header.TInfomask2) | storage.HeapOnlyTuple,
			)
			newTuple.Header.SetCtid(oldBlock, newOffset)

			got, err := pg.InsertTuple(newTuple.ToBytes())
			if err != nil {
				rel.Pool.UnpinBuffer(id) //nolint:errcheck
				return 0, 0, fmt.Errorf("heapupdate: HOT InsertTuple: %w", err)
			}
			actualOffset := storage.OffsetNumber(got + 1)

			// Update old tuple header in-place.
			if err := updateOldHeader(pg, oldOffset, xid, oldBlock, actualOffset, true); err != nil {
				rel.Pool.UnpinBuffer(id) //nolint:errcheck
				return 0, 0, fmt.Errorf("heapupdate: HOT old header: %w", err)
			}

			rel.Pool.UnpinBuffer(id) //nolint:errcheck
			return oldBlock, actualOffset, nil
		}

		rel.Pool.UnpinBuffer(id) //nolint:errcheck
	}

	// ── Cross-page update: extend relation ───────────────────────────────────
	newBlock, newId, err := rel.Extend(storage.ForkMain)
	if err != nil {
		return 0, 0, fmt.Errorf("heapupdate: Extend: %w", err)
	}

	newPg, err := rel.Pool.GetPageForWrite(newId)
	if err != nil {
		rel.Pool.UnpinBuffer(newId) //nolint:errcheck
		return 0, 0, err
	}

	// Fresh page: new tuple goes at slot 1.
	newOffset := storage.OffsetNumber(1)
	newTuple.Header.SetCtid(newBlock, newOffset) // self-referential (no HOT flag)

	got, err := newPg.InsertTuple(newTuple.ToBytes())
	if err != nil {
		rel.Pool.UnpinBuffer(newId) //nolint:errcheck
		return 0, 0, fmt.Errorf("heapupdate: InsertTuple on new page: %w", err)
	}
	actualOffset := storage.OffsetNumber(got + 1)
	rel.Pool.UnpinBuffer(newId) //nolint:errcheck

	// Update old tuple header on its (original) page.
	oldId, err := rel.ReadBlock(storage.ForkMain, oldBlock)
	if err != nil {
		return 0, 0, fmt.Errorf("heapupdate: ReadBlock old(%d): %w", oldBlock, err)
	}
	oldPg, err := rel.Pool.GetPageForWrite(oldId)
	if err != nil {
		rel.Pool.UnpinBuffer(oldId) //nolint:errcheck
		return 0, 0, err
	}
	if err := updateOldHeader(oldPg, oldOffset, xid, newBlock, actualOffset, false); err != nil {
		rel.Pool.UnpinBuffer(oldId) //nolint:errcheck
		return 0, 0, fmt.Errorf("heapupdate: old header (cross-page): %w", err)
	}
	rel.Pool.UnpinBuffer(oldId) //nolint:errcheck

	return newBlock, actualOffset, nil
}

// updateOldHeader writes xmax, infomask flags, and t_ctid into the old tuple.
// isHot additionally sets HEAP_HOT_UPDATED in infomask2.
func updateOldHeader(
	pg *storage.Page,
	oldOffset storage.OffsetNumber,
	xid storage.TransactionId,
	newBlock storage.BlockNumber,
	newOffset storage.OffsetNumber,
	isHot bool,
) error {
	lp, err := pg.GetItemId(int(oldOffset) - 1)
	if err != nil {
		return err
	}
	if !lp.IsNormal() {
		return fmt.Errorf("old slot %d is not LP_NORMAL", oldOffset)
	}
	raw := pg.Bytes()
	off := int(lp.Off())
	if off+storage.HeapTupleHeaderSize > len(raw) {
		return fmt.Errorf("old tuple header out of bounds at offset %d", off)
	}
	tup, err := storage.HeapTupleFromBytes(raw[off:])
	if err != nil {
		return err
	}

	tup.Header.TXmax = xid
	tup.Header.TInfomask = uint16(
		(storage.InfomaskFlags(tup.Header.TInfomask) &^ storage.HeapXmaxInvalid) | storage.HeapUpdated,
	)
	if isHot {
		tup.Header.TInfomask2 = uint16(
			storage.Infomask2Flags(tup.Header.TInfomask2) | storage.HeapHotUpdated,
		)
	}
	tup.Header.SetCtid(newBlock, newOffset)

	return pg.UpdateTupleHeader(int(oldOffset)-1, &tup.Header)
}
