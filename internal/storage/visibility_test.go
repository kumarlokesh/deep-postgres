package storage

import "testing"

// makeHeader creates a HeapTupleHeader for tests. If xmax != InvalidTransactionId,
// the HEAP_XMAX_INVALID hint bit is cleared (i.e. the deletion is active).
func makeHeader(xmin, xmax TransactionId) *HeapTupleHeader {
	h := NewHeapTupleHeader(xmin, 0)
	h.TXmax = xmax
	if xmax != InvalidTransactionId {
		// Clear HEAP_XMAX_INVALID so the deletion is considered active.
		h.TInfomask = uint16(h.Infomask() &^ HeapXmaxInvalid)
	}
	return &h
}

func TestSnapshotXidVisible(t *testing.T) {
	snap := NewSnapshot(100, 50, 150)

	cases := []struct {
		xid     TransactionId
		visible bool
	}{
		{InvalidTransactionId, false},
		{FrozenTransactionId, true},
		{100, true},  // own XID
		{40, true},   // < Xmin, committed before snapshot
		{200, false}, // >= Xmax
	}
	for _, c := range cases {
		if got := snap.XidVisible(c.xid); got != c.visible {
			t.Errorf("XidVisible(%d) = %v, want %v", c.xid, got, c.visible)
		}
	}
}

func TestSnapshotInProgress(t *testing.T) {
	snap := NewSnapshot(100, 50, 150)
	snap.AddInProgress(75)
	snap.AddInProgress(80)

	if snap.XidVisible(75) {
		t.Error("in-progress xid 75 should not be visible")
	}
	if snap.XidVisible(80) {
		t.Error("in-progress xid 80 should not be visible")
	}
	if !snap.XidVisible(70) {
		t.Error("xid 70 (not in Xip) should be visible")
	}
}

func TestSimpleOracleDefaults(t *testing.T) {
	oracle := NewSimpleOracle()
	if oracle.Status(100) != TxInProgress {
		t.Error("unknown XID should default to in-progress")
	}
	oracle.Commit(100)
	if oracle.Status(100) != TxCommitted {
		t.Error("expected committed")
	}
	oracle.Abort(200)
	if oracle.Status(200) != TxAborted {
		t.Error("expected aborted")
	}
	if oracle.Status(FrozenTransactionId) != TxCommitted {
		t.Error("FrozenTransactionId should always be committed")
	}
	if oracle.Status(InvalidTransactionId) != TxAborted {
		t.Error("InvalidTransactionId should always be aborted")
	}
}

func TestVisibleCommittedTuple(t *testing.T) {
	oracle := NewSimpleOracle()
	oracle.Commit(50)

	hdr := makeHeader(50, InvalidTransactionId)
	snap := NewSnapshot(100, 40, 150)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisVisible {
		t.Errorf("expected VisVisible, got %v", result)
	}
	if !result.IsVisible() {
		t.Error("IsVisible() should be true")
	}
}

func TestInvisibleUncommittedTuple(t *testing.T) {
	oracle := NewSimpleOracle() // xmin 50 is in-progress

	hdr := makeHeader(50, InvalidTransactionId)
	snap := NewSnapshot(100, 40, 150)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisInvisible {
		t.Errorf("expected VisInvisible, got %v", result)
	}
}

func TestInvisibleAbortedTuple(t *testing.T) {
	oracle := NewSimpleOracle()
	oracle.Abort(50)

	hdr := makeHeader(50, InvalidTransactionId)
	snap := NewSnapshot(100, 40, 150)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisInvisible {
		t.Errorf("expected VisInvisible, got %v", result)
	}
}

func TestDeletedTuple(t *testing.T) {
	oracle := NewSimpleOracle()
	oracle.Commit(50)
	oracle.Commit(60)

	hdr := makeHeader(50, 60)
	snap := NewSnapshot(100, 40, 150)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisDeleted {
		t.Errorf("expected VisDeleted, got %v", result)
	}
	if result.IsVisible() {
		t.Error("deleted tuple should not be visible")
	}
}

func TestTupleWithUncommittedDelete(t *testing.T) {
	oracle := NewSimpleOracle()
	oracle.Commit(50)
	// xmax 60 in-progress

	hdr := makeHeader(50, 60)
	snap := NewSnapshot(100, 40, 150)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisVisible {
		t.Errorf("expected VisVisible (uncommitted delete), got %v", result)
	}
}

func TestTupleWithAbortedDelete(t *testing.T) {
	oracle := NewSimpleOracle()
	oracle.Commit(50)
	oracle.Abort(60)

	hdr := makeHeader(50, 60)
	snap := NewSnapshot(100, 40, 150)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisVisible {
		t.Errorf("expected VisVisible (aborted delete), got %v", result)
	}
}

func TestOwnInsertVisible(t *testing.T) {
	oracle := NewSimpleOracle()

	hdr := makeHeader(100, InvalidTransactionId)
	hdr.TCid = 0

	snap := NewSnapshot(100, 40, 150)
	snap.CurCid = 5

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisInsertedBySelf {
		t.Errorf("expected VisInsertedBySelf, got %v", result)
	}
	if !result.IsVisible() {
		t.Error("own insert should be visible")
	}
}

func TestOwnInsertSameCommandInvisible(t *testing.T) {
	oracle := NewSimpleOracle()

	hdr := makeHeader(100, InvalidTransactionId)
	hdr.TCid = 5

	snap := NewSnapshot(100, 40, 150)
	snap.CurCid = 5

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisInvisible {
		t.Errorf("expected VisInvisible (same CID), got %v", result)
	}
}

func TestOwnDelete(t *testing.T) {
	oracle := NewSimpleOracle()
	oracle.Commit(50)

	hdr := makeHeader(50, 100) // current xact 100 is deleting
	snap := NewSnapshot(100, 40, 150)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisBeingDeleted {
		t.Errorf("expected VisBeingDeleted, got %v", result)
	}
}

func TestFrozenTupleAlwaysVisible(t *testing.T) {
	oracle := NewSimpleOracle()
	hdr := makeHeader(FrozenTransactionId, InvalidTransactionId)
	snap := NewSnapshot(100, 40, 150)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisVisible {
		t.Errorf("expected VisVisible for frozen tuple, got %v", result)
	}
}

func TestDeleteNotVisibleToOldSnapshot(t *testing.T) {
	oracle := NewSimpleOracle()
	oracle.Commit(50)
	oracle.Commit(100)

	hdr := makeHeader(50, 100)
	// Snapshot that cannot see xid 100 (Xmax=90).
	snap := NewSnapshot(80, 40, 90)

	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisVisible {
		t.Errorf("expected VisVisible (delete invisible to old snap), got %v", result)
	}
}

func TestSetHintBits(t *testing.T) {
	oracle := NewSimpleOracle()
	oracle.Commit(50)
	oracle.Abort(60)

	hdr := makeHeader(50, 60)
	if hdr.XminCommitted() || hdr.XmaxInvalid() {
		t.Fatal("hint bits should be unset initially")
	}

	SetHintBits(hdr, oracle)

	if !hdr.XminCommitted() {
		t.Error("expected HEAP_XMIN_COMMITTED after SetHintBits")
	}
	if !hdr.XmaxInvalid() {
		t.Error("expected HEAP_XMAX_INVALID after SetHintBits")
	}
}

func TestHintBitXminCommitted(t *testing.T) {
	oracle := NewSimpleOracle()
	// Deliberately don't add xmin 50 to oracle; rely on hint bit instead.

	hdr := makeHeader(50, InvalidTransactionId)
	hdr.SetXminCommitted() // pre-set hint bit

	snap := NewSnapshot(100, 40, 150)
	// Should see xmin as committed via hint bit, no oracle lookup.
	result := HeapTupleSatisfiesMVCC(hdr, snap, oracle)
	if result != VisVisible {
		t.Errorf("expected VisVisible via hint bit, got %v", result)
	}
}
