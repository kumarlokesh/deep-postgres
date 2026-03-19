package executor

// Sequential scan integration tests.
//
// Each test builds a real on-disk heap relation (using FileStorageManager in a
// temp directory), inserts tuples with known XID state, then runs SeqScan and
// asserts the correct tuples are returned.
//
// The MVCC scenarios covered:
//   - Single committed insert — tuple is visible.
//   - Aborted insert — tuple is invisible.
//   - Mix of committed and aborted inserts — only committed tuples returned.
//   - Deleted tuple (xmax committed) — invisible.
//   - Multi-block table — scan crosses block boundaries.
//   - Empty relation — scan returns nil immediately.
//   - Snapshot isolation — tuple committed after snapshot is invisible.

import (
	"os"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── test helpers ──────────────────────────────────────────────────────────────

// scanRelation is a shared fixture that pairs a Relation with a
// TransactionManager for easy test setup.
type scanRelation struct {
	rel   *storage.Relation
	pool  *storage.BufferPool
	txmgr *mvcc.TransactionManager
	smgr  storage.StorageManager
	dir   string
}

func newScanRelation(t *testing.T) *scanRelation {
	t.Helper()
	dir, err := os.MkdirTemp("", "seqscan-*")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}

	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(64)
	rfn := storage.RelFileNode{DbId: 1, RelId: 1}
	rel := storage.OpenRelation(rfn, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}

	txmgr := mvcc.NewTransactionManager()

	t.Cleanup(func() {
		smgr.Close()
		os.RemoveAll(dir)
	})
	return &scanRelation{rel: rel, pool: pool, txmgr: txmgr, smgr: smgr, dir: dir}
}

// insertTuple inserts a tuple into the relation under the given XID and returns
// the (block, 1-based offset) it was written to.
func (f *scanRelation) insertTuple(t *testing.T, xid storage.TransactionId, data []byte) (storage.BlockNumber, storage.OffsetNumber) {
	t.Helper()
	// Try to insert on the last block; extend if the relation is empty or full.
	nblocks, err := f.rel.NBlocks(storage.ForkMain)
	if err != nil {
		t.Fatalf("NBlocks: %v", err)
	}

	var blk storage.BlockNumber
	var id storage.BufferId

	if nblocks == 0 {
		blk, id, err = f.rel.Extend(storage.ForkMain)
	} else {
		blk = nblocks - 1
		id, err = f.rel.ReadBlock(storage.ForkMain, blk)
	}
	if err != nil {
		t.Fatalf("get block: %v", err)
	}

	page, err := f.pool.GetPageForWrite(id)
	if err != nil {
		f.pool.UnpinBuffer(id) //nolint:errcheck
		t.Fatalf("GetPageForWrite: %v", err)
	}

	tup := storage.NewHeapTuple(xid, 1, data)
	idx, err := page.InsertTuple(tup.ToBytes())
	if err != nil {
		// Page full — unpin and extend to a new block.
		f.pool.UnpinBuffer(id) //nolint:errcheck

		blk, id, err = f.rel.Extend(storage.ForkMain)
		if err != nil {
			t.Fatalf("Extend: %v", err)
		}
		page, err = f.pool.GetPageForWrite(id)
		if err != nil {
			f.pool.UnpinBuffer(id) //nolint:errcheck
			t.Fatalf("GetPageForWrite on new block: %v", err)
		}
		tup = storage.NewHeapTuple(xid, 1, data)
		idx, err = page.InsertTuple(tup.ToBytes())
		if err != nil {
			f.pool.UnpinBuffer(id) //nolint:errcheck
			t.Fatalf("InsertTuple: %v", err)
		}
	}

	f.pool.UnpinBuffer(id) //nolint:errcheck
	return blk, storage.OffsetNumber(idx + 1)
}

// deleteTuple marks the tuple at (blk, offset) as deleted by xid.
func (f *scanRelation) deleteTuple(t *testing.T, blk storage.BlockNumber, offset storage.OffsetNumber, xid storage.TransactionId) {
	t.Helper()
	id, err := f.rel.ReadBlock(storage.ForkMain, blk)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	defer f.pool.UnpinBuffer(id) //nolint:errcheck
	page, err := f.pool.GetPageForWrite(id)
	if err != nil {
		t.Fatalf("GetPageForWrite: %v", err)
	}
	lp, err := page.GetItemId(int(offset) - 1)
	if err != nil {
		t.Fatalf("GetItemId: %v", err)
	}
	// Decode the tuple, set xmax, write back.
	raw := page.Bytes()
	off := int(lp.Off())
	length := int(lp.Len())
	tup, err := storage.HeapTupleFromBytes(raw[off : off+length])
	if err != nil {
		t.Fatalf("HeapTupleFromBytes: %v", err)
	}
	tup.Header.TXmax = xid
	// Clear HEAP_XMAX_INVALID so the xmax is treated as a real deleter.
	tup.Header.TInfomask = uint16(storage.InfomaskFlags(tup.Header.TInfomask) &^ storage.HeapXmaxInvalid)
	copy(raw[off:off+length], tup.ToBytes())
}

// scan collects all visible tuples from the relation.
func (f *scanRelation) scan(t *testing.T, snap *storage.Snapshot) []*ScanTuple {
	t.Helper()
	ss, err := NewSeqScan(f.rel, snap, f.txmgr)
	if err != nil {
		t.Fatalf("NewSeqScan: %v", err)
	}
	defer ss.Close()

	var out []*ScanTuple
	for {
		st, err := ss.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if st == nil {
			break
		}
		out = append(out, st)
	}
	return out
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestSeqScanEmptyRelation(t *testing.T) {
	f := newScanRelation(t)
	xid := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(xid)

	rows := f.scan(t, snap)
	if len(rows) != 0 {
		t.Errorf("empty relation: got %d rows want 0", len(rows))
	}
}

func TestSeqScanSingleCommittedInsert(t *testing.T) {
	f := newScanRelation(t)

	// Insert and commit.
	xid := f.txmgr.Begin()
	f.insertTuple(t, xid, []byte("hello seqscan"))
	if err := f.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// New snapshot after commit — should see the tuple.
	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap)
	if len(rows) != 1 {
		t.Fatalf("got %d rows want 1", len(rows))
	}
	if string(rows[0].Tuple.Data) != "hello seqscan" {
		t.Errorf("data: got %q want %q", rows[0].Tuple.Data, "hello seqscan")
	}
}

func TestSeqScanAbortedInsertInvisible(t *testing.T) {
	f := newScanRelation(t)

	// Insert and abort.
	xid := f.txmgr.Begin()
	f.insertTuple(t, xid, []byte("aborted row"))
	if err := f.txmgr.Abort(xid); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap)
	if len(rows) != 0 {
		t.Errorf("aborted insert: got %d rows want 0", len(rows))
	}
}

func TestSeqScanMixedCommittedAborted(t *testing.T) {
	f := newScanRelation(t)

	// Three inserts: committed, aborted, committed.
	xid1 := f.txmgr.Begin()
	f.insertTuple(t, xid1, []byte("row-1"))
	if err := f.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit xid1: %v", err)
	}

	xid2 := f.txmgr.Begin()
	f.insertTuple(t, xid2, []byte("row-2-aborted"))
	if err := f.txmgr.Abort(xid2); err != nil {
		t.Fatalf("Abort xid2: %v", err)
	}

	xid3 := f.txmgr.Begin()
	f.insertTuple(t, xid3, []byte("row-3"))
	if err := f.txmgr.Commit(xid3); err != nil {
		t.Fatalf("Commit xid3: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap)
	if len(rows) != 2 {
		t.Fatalf("got %d rows want 2", len(rows))
	}
	if string(rows[0].Tuple.Data) != "row-1" {
		t.Errorf("[0]: got %q want %q", rows[0].Tuple.Data, "row-1")
	}
	if string(rows[1].Tuple.Data) != "row-3" {
		t.Errorf("[1]: got %q want %q", rows[1].Tuple.Data, "row-3")
	}
}

func TestSeqScanDeletedTupleInvisible(t *testing.T) {
	f := newScanRelation(t)

	// Insert.
	xid1 := f.txmgr.Begin()
	blk, off := f.insertTuple(t, xid1, []byte("to be deleted"))
	if err := f.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit insert: %v", err)
	}

	// Delete.
	xid2 := f.txmgr.Begin()
	f.deleteTuple(t, blk, off, xid2)
	if err := f.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	// Scan after delete — tuple should be gone.
	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap)
	if len(rows) != 0 {
		t.Errorf("deleted tuple: got %d rows want 0", len(rows))
	}
}

func TestSeqScanSnapshotIsolation(t *testing.T) {
	// Take snapshot BEFORE insert — the inserted tuple must not be visible.
	f := newScanRelation(t)

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader) // snapshot taken before insert

	writer := f.txmgr.Begin()
	f.insertTuple(t, writer, []byte("committed after snapshot"))
	if err := f.txmgr.Commit(writer); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rows := f.scan(t, snap)
	if len(rows) != 0 {
		t.Errorf("snapshot isolation: got %d rows want 0 (committed after snapshot)", len(rows))
	}
}

func TestSeqScanMultiBlock(t *testing.T) {
	f := newScanRelation(t)
	const nRows = 500 // enough to span multiple 8 KB pages

	// Insert all rows in one transaction.
	xid := f.txmgr.Begin()
	for i := 0; i < nRows; i++ {
		data := []byte{byte(i >> 8), byte(i)} // 2-byte unique payload
		f.insertTuple(t, xid, data)
	}
	if err := f.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify we used more than one block.
	nblocks, err := f.rel.NBlocks(storage.ForkMain)
	if err != nil {
		t.Fatalf("NBlocks: %v", err)
	}
	if nblocks < 2 {
		t.Fatalf("expected multi-block table, got %d blocks", nblocks)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap)
	if len(rows) != nRows {
		t.Errorf("multi-block: got %d rows want %d", len(rows), nRows)
	}
}

func TestSeqScanReturnsTID(t *testing.T) {
	// Verify the returned (Block, Offset) are correct for a known insert.
	f := newScanRelation(t)

	xid := f.txmgr.Begin()
	wantBlk, wantOff := f.insertTuple(t, xid, []byte("tid-test"))
	if err := f.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap)
	if len(rows) != 1 {
		t.Fatalf("got %d rows want 1", len(rows))
	}
	if rows[0].Block != wantBlk || rows[0].Offset != wantOff {
		t.Errorf("TID: got (%d,%d) want (%d,%d)", rows[0].Block, rows[0].Offset, wantBlk, wantOff)
	}
}

func TestSeqScanSurvivorAfterDelete(t *testing.T) {
	// Insert two rows, delete the first, scan should return only the second.
	f := newScanRelation(t)

	xid1 := f.txmgr.Begin()
	blk1, off1 := f.insertTuple(t, xid1, []byte("first"))
	_, _ = f.insertTuple(t, xid1, []byte("second"))
	if err := f.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit inserts: %v", err)
	}

	xid2 := f.txmgr.Begin()
	f.deleteTuple(t, blk1, off1, xid2)
	if err := f.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap)
	if len(rows) != 1 {
		t.Fatalf("got %d rows want 1", len(rows))
	}
	if string(rows[0].Tuple.Data) != "second" {
		t.Errorf("data: got %q want %q", rows[0].Tuple.Data, "second")
	}
}
