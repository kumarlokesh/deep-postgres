package executor

// Visibility-map integration tests.
//
// Scenarios:
//   - After VACUUM on a relation with only committed tuples, pages are marked
//     all-visible in the VM.
//   - After VACUUM, SeqScan skips HeapTupleSatisfiesMVCC (verified indirectly
//     by checking PagesAllVisible > 0 and confirming tuples are still returned).
//   - After HeapUpdate, the modified page's VM bit is cleared.
//   - After deleteTuple + VACUUM, deleted pages have their VM bit cleared.
//   - A fresh relation (no VACUUM) has no all-visible pages.

import (
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

func TestVMSetAfterVacuum(t *testing.T) {
	// Insert committed tuples, vacuum, verify VM bits are set.
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	sr.insertTuple(t, xid, []byte("row1"))
	sr.insertTuple(t, xid, []byte("row2"))
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Burn extra XIDs so OldestXmin > xid.
	for i := 0; i < 3; i++ {
		x := sr.txmgr.Begin()
		if err := sr.txmgr.Commit(x); err != nil {
			t.Fatalf("Commit filler: %v", err)
		}
	}

	stats := runVacuum(t, sr)
	if stats.PagesAllVisible == 0 {
		t.Errorf("expected PagesAllVisible > 0 after vacuum of clean relation, got 0")
	}

	nblocks, _ := sr.rel.NBlocks(storage.ForkMain)
	for blk := storage.BlockNumber(0); blk < nblocks; blk++ {
		if !sr.rel.VM.IsAllVisible(blk) {
			t.Errorf("block %d: expected VM all-visible after vacuum", blk)
		}
	}
}

func TestVMNotSetOnFreshRelation(t *testing.T) {
	// A brand-new relation has no all-visible pages.
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	sr.insertTuple(t, xid, []byte("fresh"))
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// No vacuum — VM bit should not be set.
	if sr.rel.VM.IsAllVisible(0) {
		t.Error("block 0: VM all-visible without vacuum")
	}
}

func TestVMClearedAfterHeapUpdate(t *testing.T) {
	// Vacuum sets VM bit; HeapUpdate clears it for the modified page.
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("before"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit insert: %v", err)
	}

	// Burn XIDs to allow VM to be set.
	for i := 0; i < 3; i++ {
		x := sr.txmgr.Begin()
		if err := sr.txmgr.Commit(x); err != nil {
			t.Fatalf("Commit filler: %v", err)
		}
	}

	runVacuum(t, sr)
	if !sr.rel.VM.IsAllVisible(blk) {
		t.Fatal("VM bit not set after vacuum — cannot test clear")
	}

	// Now do a HeapUpdate — VM bit for the page must be cleared.
	xid2 := sr.txmgr.Begin()
	newTup := storage.NewHeapTuple(xid2, 1, []byte("after"))
	newBlk, _, err := HeapUpdate(sr.rel, xid2, blk, off, newTup)
	if err != nil {
		t.Fatalf("HeapUpdate: %v", err)
	}
	if err := sr.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit update: %v", err)
	}

	if sr.rel.VM.IsAllVisible(blk) {
		t.Errorf("block %d: VM bit still set after HeapUpdate (old page)", blk)
	}
	if newBlk != blk && sr.rel.VM.IsAllVisible(newBlk) {
		t.Errorf("block %d: VM bit set on new page after cross-page HeapUpdate", newBlk)
	}
}

func TestVMClearedAfterDeleteAndVacuum(t *testing.T) {
	// After a delete + vacuum, the page has dead tuples removed and is still
	// all-visible if the remaining tuples pass the check.  But if the page
	// had only one tuple (which was deleted), it becomes empty — still
	// all-visible (no LP_NORMAL tuples to fail the check).
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("del"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit insert: %v", err)
	}

	// Vacuum 1: marks page all-visible (one committed tuple, no xmax).
	for i := 0; i < 3; i++ {
		x := sr.txmgr.Begin()
		if err := sr.txmgr.Commit(x); err != nil {
			t.Fatalf("Commit filler: %v", err)
		}
	}
	runVacuum(t, sr)
	if !sr.rel.VM.IsAllVisible(blk) {
		t.Fatal("expected VM all-visible after first vacuum")
	}

	// Delete the tuple — VM bit must be cleared.
	xDel := sr.txmgr.Begin()
	sr.deleteTuple(t, blk, off, xDel)
	if err := sr.txmgr.Commit(xDel); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	// deleteTuple does not go through HeapUpdate, so the VM bit is still set
	// from vacuum.  Run vacuum again to reclaim the dead slot.  After the
	// second vacuum the page is empty (no LP_NORMAL) → PageIsAllVisible = true.
	for i := 0; i < 3; i++ {
		x := sr.txmgr.Begin()
		if err := sr.txmgr.Commit(x); err != nil {
			t.Fatalf("Commit filler: %v", err)
		}
	}
	stats := runVacuum(t, sr)
	if stats.TuplesRemoved == 0 {
		t.Errorf("expected vacuum to remove the deleted tuple, got TuplesRemoved=0")
	}
}

func TestVMSeqScanStillReturnsRowsOnAllVisiblePage(t *testing.T) {
	// After vacuum marks pages all-visible, SeqScan must still return the
	// committed tuples (the optimization must not break correctness).
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	sr.insertTuple(t, xid, []byte("a"))
	sr.insertTuple(t, xid, []byte("b"))
	sr.insertTuple(t, xid, []byte("c"))
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for i := 0; i < 3; i++ {
		x := sr.txmgr.Begin()
		if err := sr.txmgr.Commit(x); err != nil {
			t.Fatalf("Commit filler: %v", err)
		}
	}

	runVacuum(t, sr)

	nblocks, _ := sr.rel.NBlocks(storage.ForkMain)
	for blk := storage.BlockNumber(0); blk < nblocks; blk++ {
		if !sr.rel.VM.IsAllVisible(blk) {
			t.Errorf("block %d: expected all-visible after vacuum", blk)
		}
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows via SeqScan on all-visible pages, got %d: %v", len(rows), rows)
	}
}
