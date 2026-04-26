package storage

import (
	"testing"
)

// buildHOTPage creates a page with a two-version HOT chain:
//
//	slot 1 (LP_NORMAL): V1 - xmin=tx0 committed, xmax=tx1 (updater)
//	slot 2 (LP_NORMAL): V2 - xmin=tx1, xmax=0 (live)
//
// The chain is V1 → V2 via t_ctid; V1 carries HEAP_HOT_UPDATED.
// Returns the page and the oracle with tx0 and tx1 committed.
func buildHOTPage(t *testing.T) (*Page, *SimpleTransactionOracle) {
	t.Helper()

	const (
		tx0 TransactionId = 10
		tx1 TransactionId = 11
		blk BlockNumber   = 0
	)

	oracle := NewSimpleOracle()
	oracle.Commit(tx0)
	oracle.Commit(tx1)

	p := NewPage()

	// V1: inserted by tx0, deleted (xmax) by tx1.
	v1Hdr := NewHeapTupleHeader(tx0, 1)
	v1Hdr.TXmax = tx1
	v1Hdr.TInfomask = uint16(HeapXminCommitted) // hint: xmin committed
	v1Hdr.TInfomask2 = uint16(HeapHotUpdated | Infomask2Flags(1))
	// t_ctid points to slot 2 on block 0.
	v1Hdr.SetCtid(blk, 2)
	v1Tup := &HeapTuple{Header: v1Hdr, Data: []byte("v1")}

	if _, err := p.InsertTuple(v1Tup.ToBytes()); err != nil {
		t.Fatalf("insert V1: %v", err)
	}

	// V2: inserted by tx1, no deletion.
	v2Hdr := NewHeapTupleHeader(tx1, 1)
	v2Hdr.TXmax = InvalidTransactionId
	v2Hdr.TInfomask = uint16(HeapXmaxInvalid)
	v2Hdr.TInfomask2 = uint16(HeapOnlyTuple | Infomask2Flags(1))
	v2Hdr.SetCtid(blk, 2) // self-referential (chain tail)
	v2Tup := &HeapTuple{Header: v2Hdr, Data: []byte("v2")}

	if _, err := p.InsertTuple(v2Tup.ToBytes()); err != nil {
		t.Fatalf("insert V2: %v", err)
	}

	return p, oracle
}

// ── HeapHotSearchBuffer ───────────────────────────────────────────────────────

// TestHotSearchOldSnapshotSeesV1 verifies that a snapshot taken before tx1
// started returns V1 from the HOT chain.
func TestHotSearchOldSnapshotSeesV1(t *testing.T) {
	page, oracle := buildHOTPage(t)

	const (
		tx0 TransactionId = 10
		tx1 TransactionId = 11
	)

	// Old snapshot: Xmax=tx1, so tx1 is not yet visible.
	snap := &Snapshot{
		Xid:  20, // observer
		Xmin: tx0,
		Xmax: tx1, // tx1 not visible
		Xip:  nil,
	}

	tup, off, ok := HeapHotSearchBuffer(page, 0, 1, snap, oracle)
	if !ok {
		t.Fatal("HeapHotSearchBuffer: expected visible tuple, got none")
	}
	if string(tup.Data) != "v1" {
		t.Errorf("old snapshot: got %q, want \"v1\"", tup.Data)
	}
	if off != 1 {
		t.Errorf("old snapshot offset: got %d, want 1", off)
	}
}

// TestHotSearchNewSnapshotSeesV2 verifies that a snapshot taken after tx1
// committed returns V2 from the HOT chain.
func TestHotSearchNewSnapshotSeesV2(t *testing.T) {
	page, oracle := buildHOTPage(t)

	const (
		tx0 TransactionId = 10
		tx1 TransactionId = 11
	)

	// New snapshot: both tx0 and tx1 are visible.
	snap := &Snapshot{
		Xid:  20,
		Xmin: tx0,
		Xmax: 20, // everything up to 20 is visible
		Xip:  nil,
	}

	tup, off, ok := HeapHotSearchBuffer(page, 0, 1, snap, oracle)
	if !ok {
		t.Fatal("HeapHotSearchBuffer: expected visible tuple, got none")
	}
	if string(tup.Data) != "v2" {
		t.Errorf("new snapshot: got %q, want \"v2\"", tup.Data)
	}
	if off != 2 {
		t.Errorf("new snapshot offset: got %d, want 2", off)
	}
}

// TestHotSearchViaRedirect verifies that an LP_REDIRECT in slot 0 is followed
// before walking the HOT chain, so callers can pass an index TID that may
// already point at an LP_REDIRECT after pruning.
func TestHotSearchViaRedirect(t *testing.T) {
	// Build a page where slot 0 is LP_REDIRECT → slot 1, and slot 1 is LP_NORMAL V1.
	oracle := NewSimpleOracle()
	const tx0 TransactionId = 10
	oracle.Commit(tx0)

	p := NewPage()

	// Insert a normal tuple at slot 0; we will redirect it shortly.
	v1Hdr := NewHeapTupleHeader(tx0, 1)
	v1Hdr.TInfomask = uint16(HeapXminCommitted | HeapXmaxInvalid)
	v1Hdr.SetCtid(0, 1) // self-referential
	v1Tup := &HeapTuple{Header: v1Hdr, Data: []byte("via-redirect")}

	if _, err := p.InsertTuple(v1Tup.ToBytes()); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Add a dummy second slot so we can redirect slot 0 → slot 1 without
	// pointing at ourselves. This simulates post-VACUUM HOT pruning.
	// First duplicate the tuple into slot 1.
	if _, err := p.InsertTuple(v1Tup.ToBytes()); err != nil {
		t.Fatalf("insert dup: %v", err)
	}

	// Overwrite slot 0 as LP_REDIRECT → offset 2 (slot 1, 1-based).
	if err := p.SetItemIdRedirect(0, 2); err != nil {
		t.Fatalf("SetItemIdRedirect: %v", err)
	}

	snap := &Snapshot{Xid: 99, Xmin: tx0, Xmax: 99}

	// Search starting at offset 1 (the redirect slot).
	tup, off, ok := HeapHotSearchBuffer(p, 0, 1, snap, oracle)
	if !ok {
		t.Fatal("HeapHotSearchBuffer via redirect: expected visible tuple")
	}
	if string(tup.Data) != "via-redirect" {
		t.Errorf("via-redirect: got %q, want \"via-redirect\"", tup.Data)
	}
	if off != 2 {
		t.Errorf("via-redirect: expected offset 2 after redirect, got %d", off)
	}
}

// TestHotSearchNoneVisible returns false when every version is invisible to
// the snapshot (e.g. all versions inserted by in-progress transactions).
func TestHotSearchNoneVisible(t *testing.T) {
	oracle := NewSimpleOracle()
	const tx TransactionId = 10
	// tx is left in-progress (no Commit call).

	p := NewPage()
	hdr := NewHeapTupleHeader(tx, 1)
	hdr.SetCtid(0, 1)
	tup := &HeapTuple{Header: hdr, Data: []byte("invisible")}
	if _, err := p.InsertTuple(tup.ToBytes()); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Snapshot that doesn't include tx.
	snap := &Snapshot{Xid: 99, Xmin: tx + 1, Xmax: 99}

	_, _, ok := HeapHotSearchBuffer(p, 0, 1, snap, oracle)
	if ok {
		t.Error("HeapHotSearchBuffer: expected false for invisible tuple, got true")
	}
}
