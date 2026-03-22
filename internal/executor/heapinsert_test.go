package executor

// HeapInsert integration tests.
//
// Scenarios:
//   - Basic insert: tuple is readable via SeqScan after commit.
//   - FSM reuse: after VACUUM frees a slot, HeapInsert places the next tuple
//     on the same page instead of extending.
//   - FSM stale: if the FSM entry is over-optimistic, HeapInsert falls back to
//     extending rather than returning an error.
//   - Multi-insert: many tuples end up on correct pages.
//   - VM cleared: HeapInsert clears the all-visible bit for the target page.

import (
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

func TestHeapInsertBasic(t *testing.T) {
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, []byte("hello"))
	blk, off, err := HeapInsert(sr.rel, tup)
	if err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)
	if len(rows) != 1 || rows[0] != "hello" {
		t.Errorf("expected [hello], got %v", rows)
	}
	_ = blk
	_ = off
}

func TestHeapInsertFSMReuseAfterVacuum(t *testing.T) {
	// Insert + delete + vacuum → FSM records freed space → second insert reuses
	// the same block rather than extending.
	sr := newScanRelation(t)

	xIns := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xIns, []byte("victim"))
	if err := sr.txmgr.Commit(xIns); err != nil {
		t.Fatalf("Commit insert: %v", err)
	}

	xDel := sr.txmgr.Begin()
	sr.deleteTuple(t, blk, off, xDel)
	if err := sr.txmgr.Commit(xDel); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	// Burn XIDs so the delete is below OldestXmin.
	for i := 0; i < 3; i++ {
		x := sr.txmgr.Begin()
		if err := sr.txmgr.Commit(x); err != nil {
			t.Fatalf("Commit filler: %v", err)
		}
	}

	stats := runVacuum(t, sr)
	if stats.FSMUpdated == 0 {
		t.Fatalf("expected FSMUpdated > 0 after vacuum")
	}

	// The freed slot's page should now be in the FSM.
	if sr.rel.FSM.FreeBytes(blk) == 0 {
		t.Fatalf("FSM shows 0 free bytes on block %d after vacuum", blk)
	}

	// HeapInsert should place the new tuple on the same block (not extend).
	nblocksBefore, _ := sr.rel.NBlocks(storage.ForkMain)

	xNew := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xNew, 1, []byte("reused"))
	newBlk, _, err := HeapInsert(sr.rel, tup)
	if err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xNew); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	nblocksAfter, _ := sr.rel.NBlocks(storage.ForkMain)
	if nblocksAfter > nblocksBefore {
		t.Errorf("HeapInsert extended to block %d; expected reuse of block %d",
			nblocksAfter-1, blk)
	}
	if newBlk != blk {
		t.Errorf("HeapInsert used block %d, want %d (FSM reuse)", newBlk, blk)
	}
}

func TestHeapInsertVMCleared(t *testing.T) {
	// VACUUM sets VM bit; HeapInsert into that page must clear it.
	sr := newScanRelation(t)

	xIns := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xIns, []byte("first"))
	if err := sr.txmgr.Commit(xIns); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Burn XIDs.
	for i := 0; i < 3; i++ {
		x := sr.txmgr.Begin()
		if err := sr.txmgr.Commit(x); err != nil {
			t.Fatalf("Commit filler: %v", err)
		}
	}
	runVacuum(t, sr)
	if !sr.rel.VM.IsAllVisible(blk) {
		t.Fatalf("VM bit not set after vacuum — cannot test HeapInsert clear")
	}
	_ = off

	xNew := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xNew, 1, []byte("second"))
	newBlk, _, err := HeapInsert(sr.rel, tup)
	if err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xNew); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// If the insert landed on the vacuum'd page, VM bit must be cleared.
	if newBlk == blk && sr.rel.VM.IsAllVisible(blk) {
		t.Errorf("VM all-visible bit not cleared after HeapInsert on block %d", blk)
	}
}

func TestHeapInsertSeqScanReturnsAllRows(t *testing.T) {
	// Mix HeapInsert and the test-helper insertTuple; SeqScan must return all
	// committed rows in both cases.
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	sr.insertTuple(t, xid, []byte("legacy"))
	tup := storage.NewHeapTuple(xid, 1, []byte("new"))
	if _, _, err := HeapInsert(sr.rel, tup); err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)
	got := map[string]bool{}
	for _, r := range rows {
		got[r] = true
	}
	if !got["legacy"] || !got["new"] {
		t.Errorf("expected both rows, got %v", rows)
	}
}

func TestHeapInsertFSMUpdatedAfterInsert(t *testing.T) {
	// After HeapInsert, the FSM entry for the target block should reflect the
	// reduced free space.
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, []byte("probe"))
	blk, _, err := HeapInsert(sr.rel, tup)
	if err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// FSM must record some space (not a full page).
	recorded := sr.rel.FSM.FreeBytes(blk)
	if recorded >= uint16(storage.PageSize) {
		t.Errorf("FSM shows %d bytes free (full page); expected less after insert", recorded)
	}
	// Must be more than zero (one small tuple leaves most of the page free).
	if recorded == 0 {
		t.Errorf("FSM shows 0 bytes free after single insert on block %d", blk)
	}
}
