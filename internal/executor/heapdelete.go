package executor

// HeapDelete - mark a heap tuple as deleted.
//
// Mirrors PostgreSQL's heap_delete() in src/backend/access/heap/heapam.c:
//  1. Pin and lock the page containing the target tuple.
//  2. Stamp xmax = deleting XID on the old tuple header.
//  3. Clear HEAP_XMAX_INVALID so the deletion is visible to MVCC.
//
// This implementation omits:
//   - Concurrent delete conflict detection (assumed single-writer).
//   - WAL logging.
//   - Index dead-item tracking (vacuum handles that separately).

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// HeapDelete marks the tuple at (block, offset) as deleted by xid.
// The tuple's header is updated in-place on the page.
func HeapDelete(
	rel *storage.Relation,
	xid storage.TransactionId,
	block storage.BlockNumber,
	offset storage.OffsetNumber, // 1-based
) error {
	id, err := rel.ReadBlock(storage.ForkMain, block)
	if err != nil {
		return fmt.Errorf("heapdelete: ReadBlock(%d): %w", block, err)
	}
	defer rel.Pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := rel.Pool.GetPageForWrite(id)
	if err != nil {
		return fmt.Errorf("heapdelete: GetPageForWrite(%d): %w", block, err)
	}

	lp, err := pg.GetItemId(int(offset) - 1)
	if err != nil {
		return fmt.Errorf("heapdelete: GetItemId(%d): %w", offset, err)
	}
	if !lp.IsNormal() {
		return fmt.Errorf("heapdelete: slot %d is not LP_NORMAL (flags=%d)", offset, lp.Flags())
	}

	raw := pg.Bytes()
	off := int(lp.Off())
	length := int(lp.Len())
	if off+length > len(raw) {
		return fmt.Errorf("heapdelete: line pointer out of bounds (off=%d len=%d)", off, length)
	}

	tup, err := storage.HeapTupleFromBytes(raw[off : off+length])
	if err != nil {
		return fmt.Errorf("heapdelete: HeapTupleFromBytes: %w", err)
	}

	tup.Header.TXmax = xid
	// Clear HEAP_XMAX_INVALID so MVCC treats xmax as a real deleter.
	tup.Header.SetInfomask(tup.Header.Infomask() &^ storage.HeapXmaxInvalid)

	// Write the modified header back to the page.
	copy(raw[off:off+length], tup.ToBytes())

	// Clear the all-visible flag: the page now has a non-all-visible tuple.
	if rel.VM != nil {
		rel.VM.ClearAllVisible(block)
	}

	return nil
}
