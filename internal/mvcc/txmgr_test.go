package mvcc

import (
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── CLOG ─────────────────────────────────────────────────────────────────────

func TestClogSpecialXids(t *testing.T) {
	c := NewClog()
	if s := c.Status(storage.InvalidTransactionId); s != storage.TxAborted {
		t.Errorf("InvalidXID: got %v want TxAborted", s)
	}
	if s := c.Status(storage.FrozenTransactionId); s != storage.TxCommitted {
		t.Errorf("FrozenXID: got %v want TxCommitted", s)
	}
}

func TestClogUnknownIsInProgress(t *testing.T) {
	c := NewClog()
	if s := c.Status(999); s != storage.TxInProgress {
		t.Errorf("unknown xid: got %v want TxInProgress", s)
	}
}

func TestClogSetAndGet(t *testing.T) {
	c := NewClog()
	c.SetStatus(10, storage.TxCommitted)
	c.SetStatus(11, storage.TxAborted)
	if s := c.Status(10); s != storage.TxCommitted {
		t.Errorf("xid 10: got %v", s)
	}
	if s := c.Status(11); s != storage.TxAborted {
		t.Errorf("xid 11: got %v", s)
	}
}

func TestClogGrows(t *testing.T) {
	c := NewClog()
	// Write beyond initial capacity.
	large := storage.TransactionId(clogInitialCap * 4)
	c.SetStatus(large, storage.TxCommitted)
	if s := c.Status(large); s != storage.TxCommitted {
		t.Errorf("large xid: got %v", s)
	}
	// Earlier entries still readable.
	if s := c.Status(large - 1); s != storage.TxInProgress {
		t.Errorf("large-1 xid: got %v want TxInProgress", s)
	}
}

// ── TransactionManager ────────────────────────────────────────────────────────

func TestTxmgrBeginAssignsSequentialXids(t *testing.T) {
	m := NewTransactionManager()
	x1 := m.Begin()
	x2 := m.Begin()
	x3 := m.Begin()
	if x1 != storage.FirstNormalTransactionId {
		t.Errorf("x1: got %d want %d", x1, storage.FirstNormalTransactionId)
	}
	if x2 != x1+1 || x3 != x1+2 {
		t.Errorf("sequential: %d %d %d", x1, x2, x3)
	}
	if m.ActiveCount() != 3 {
		t.Errorf("ActiveCount: got %d want 3", m.ActiveCount())
	}
}

func TestTxmgrCommit(t *testing.T) {
	m := NewTransactionManager()
	xid := m.Begin()
	if s := m.Status(xid); s != storage.TxInProgress {
		t.Errorf("before commit: got %v", s)
	}
	if err := m.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if s := m.Status(xid); s != storage.TxCommitted {
		t.Errorf("after commit: got %v", s)
	}
	if m.ActiveCount() != 0 {
		t.Errorf("ActiveCount after commit: got %d want 0", m.ActiveCount())
	}
}

func TestTxmgrAbort(t *testing.T) {
	m := NewTransactionManager()
	xid := m.Begin()
	if err := m.Abort(xid); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if s := m.Status(xid); s != storage.TxAborted {
		t.Errorf("after abort: got %v", s)
	}
}

func TestTxmgrCommitUnknownErrors(t *testing.T) {
	m := NewTransactionManager()
	if err := m.Commit(999); err == nil {
		t.Error("expected error committing unknown xid")
	}
}

func TestTxmgrAbortUnknownErrors(t *testing.T) {
	m := NewTransactionManager()
	if err := m.Abort(999); err == nil {
		t.Error("expected error aborting unknown xid")
	}
}

// ── Snapshot ──────────────────────────────────────────────────────────────────

func TestSnapshotNoActiveXacts(t *testing.T) {
	m := NewTransactionManager()
	snap := m.Snapshot(0)
	// With no active transactions, xmin == xmax == nextXid.
	if snap.Xmin != snap.Xmax {
		t.Errorf("empty: xmin=%d xmax=%d want equal", snap.Xmin, snap.Xmax)
	}
	if len(snap.Xip) != 0 {
		t.Errorf("Xip should be empty")
	}
}

func TestSnapshotCapturesActiveXids(t *testing.T) {
	m := NewTransactionManager()
	x1 := m.Begin()
	x2 := m.Begin()
	x3 := m.Begin()

	// Commit x2; it should disappear from the snapshot.
	m.Commit(x2) //nolint:errcheck

	snap := m.Snapshot(x1)
	if snap.Xid != x1 {
		t.Errorf("Xid: got %d want %d", snap.Xid, x1)
	}
	if snap.Xmin != x1 {
		t.Errorf("Xmin: got %d want %d (min active)", snap.Xmin, x1)
	}
	// x3 was the last begun; nextXid = x3+1 = xmax.
	if snap.Xmax != x3+1 {
		t.Errorf("Xmax: got %d want %d", snap.Xmax, x3+1)
	}
	// Only x1 and x3 are active.
	if len(snap.Xip) != 2 {
		t.Errorf("Xip len: got %d want 2", len(snap.Xip))
	}
}

func TestSnapshotXidVisibility(t *testing.T) {
	m := NewTransactionManager()
	x1 := m.Begin()
	x2 := m.Begin()
	x3 := m.Begin()

	m.Commit(x1) //nolint:errcheck
	// Take snapshot as transaction x2.
	snap := m.Snapshot(x2)

	// x1 committed before snapshot: visible.
	if !snap.XidVisible(x1) {
		t.Error("x1 (committed before snap) should be visible")
	}
	// x2 is the snapshot's own XID: visible.
	if !snap.XidVisible(x2) {
		t.Error("x2 (own xid) should be visible")
	}
	// x3 is in Xip (in-progress): not visible.
	if snap.XidVisible(x3) {
		t.Error("x3 (in-progress) should not be visible")
	}
	// nextXid (x3+1): >= xmax, not visible.
	if snap.XidVisible(x3 + 1) {
		t.Error("future xid should not be visible")
	}
}

// ── End-to-end: visibility through TransactionManager ─────────────────────────

func TestVisibilityCommittedBeforeSnapshot(t *testing.T) {
	m := NewTransactionManager()
	writer := m.Begin()
	reader := m.Begin()

	// Writer inserts and commits.
	hdr := makeTestHeader(writer, storage.InvalidTransactionId)
	m.Commit(writer) //nolint:errcheck

	// Reader takes a snapshot after writer committed.
	snap := m.Snapshot(reader)
	result := storage.HeapTupleSatisfiesMVCC(hdr, snap, m)
	if !result.IsVisible() {
		t.Errorf("committed tuple should be visible: got %v", result)
	}
}

func TestVisibilityUncommittedInvisible(t *testing.T) {
	m := NewTransactionManager()
	writer := m.Begin()
	reader := m.Begin()

	// Writer has not committed yet.
	hdr := makeTestHeader(writer, storage.InvalidTransactionId)
	snap := m.Snapshot(reader)

	result := storage.HeapTupleSatisfiesMVCC(hdr, snap, m)
	if result.IsVisible() {
		t.Errorf("uncommitted tuple should be invisible: got %v", result)
	}
}

func TestVisibilityAbortedInvisible(t *testing.T) {
	m := NewTransactionManager()
	writer := m.Begin()
	reader := m.Begin()

	hdr := makeTestHeader(writer, storage.InvalidTransactionId)
	m.Abort(writer) //nolint:errcheck

	snap := m.Snapshot(reader)
	result := storage.HeapTupleSatisfiesMVCC(hdr, snap, m)
	if result.IsVisible() {
		t.Errorf("aborted tuple should be invisible: got %v", result)
	}
}

func TestVisibilityDeletedVisible(t *testing.T) {
	// writer inserts + commits, deleter deletes + commits, reader sees deleted.
	m := NewTransactionManager()
	writer := m.Begin()
	deleter := m.Begin()
	reader := m.Begin()

	hdr := makeTestHeader(writer, deleter)
	m.Commit(writer)  //nolint:errcheck
	m.Commit(deleter) //nolint:errcheck

	snap := m.Snapshot(reader)
	result := storage.HeapTupleSatisfiesMVCC(hdr, snap, m)
	if result != storage.VisDeleted {
		t.Errorf("tuple deleted by committed xact: got %v want VisDeleted", result)
	}
}

func TestVisibilitySelfInsert(t *testing.T) {
	// A transaction can only see its own inserts from a PREVIOUS command
	// (CID < CurCid) — the same semantics as PostgreSQL's
	// CommandCounterIncrement / CID visibility rule.
	m := NewTransactionManager()
	txid := m.Begin()

	hdr := makeTestHeader(txid, storage.InvalidTransactionId) // TCid = 0
	m.AdvanceCommand(txid)                                    //nolint:errcheck // now CurCid = 1
	snap := m.Snapshot(txid)                                  // CurCid = 1 > TCid = 0

	result := storage.HeapTupleSatisfiesMVCC(hdr, snap, m)
	if result != storage.VisInsertedBySelf {
		t.Errorf("own insert (after command advance): got %v want VisInsertedBySelf", result)
	}
}

func TestVisibilitySelfDelete(t *testing.T) {
	m := NewTransactionManager()
	txid := m.Begin()

	hdr := makeTestHeader(txid, txid) // TCid = 0, xmax = self
	m.AdvanceCommand(txid)            //nolint:errcheck // CurCid = 1
	snap := m.Snapshot(txid)

	result := storage.HeapTupleSatisfiesMVCC(hdr, snap, m)
	if result != storage.VisDeletedBySelf {
		t.Errorf("own delete (after command advance): got %v want VisDeletedBySelf", result)
	}
}

func TestAdvanceCommand(t *testing.T) {
	m := NewTransactionManager()
	txid := m.Begin()

	// Insert at CID 0.
	hdr := makeTestHeaderCid(txid, storage.InvalidTransactionId, 0)
	snap0 := m.Snapshot(txid) // CurCid = 0
	// Tuple inserted at CID 0 with CurCid = 0: NOT visible (CID >= CurCid).
	if storage.HeapTupleSatisfiesMVCC(hdr, snap0, m).IsVisible() {
		t.Error("insert at CID 0 should be invisible when CurCid=0")
	}

	m.AdvanceCommand(txid)    //nolint:errcheck
	snap1 := m.Snapshot(txid) // CurCid = 1
	// Now CurCid = 1 > 0: tuple from CID 0 is visible.
	if !storage.HeapTupleSatisfiesMVCC(hdr, snap1, m).IsVisible() {
		t.Error("insert at CID 0 should be visible when CurCid=1")
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeTestHeader(xmin, xmax storage.TransactionId) *storage.HeapTupleHeader {
	hdr := &storage.HeapTupleHeader{}
	hdr.TXmin = xmin
	hdr.TXmax = xmax
	hdr.TCid = storage.FirstCommandId
	return hdr
}

func makeTestHeaderCid(xmin, xmax storage.TransactionId, cid storage.CommandId) *storage.HeapTupleHeader {
	hdr := makeTestHeader(xmin, xmax)
	hdr.TCid = cid
	return hdr
}
