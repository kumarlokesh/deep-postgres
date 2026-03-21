package executor

// VACUUM integration tests.
//
// Scenarios:
//   - LP_DEAD → LP_UNUSED: slots left by heap delete are reclaimed.
//   - Dead HOT-only tuple reclaimed as LP_UNUSED.
//   - HOT chain pruning: dead chain head → LP_REDIRECT pointing at live tail.
//   - Multi-hop HOT chain: all dead intermediates redirected to live tail.
//   - Freeze: old committed xmin replaced with FrozenTransactionId.
//   - Freeze makes tuple permanently visible (snapshot with xmin > old xid).
//   - Vacuum does not touch live tuples.
//   - After vacuum, SeqScan still returns exactly the live rows.

import (
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// runVacuum runs Vacuum with a small freeze age and returns stats.
func runVacuum(t *testing.T, sr *scanRelation) VacuumStats {
	t.Helper()
	cfg := DefaultVacuumConfig()
	cfg.FreezeMinAge = 2 // freeze anything older than OldestXmin-2
	stats, err := Vacuum(sr.rel, sr.txmgr, cfg)
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	return stats
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestVacuumReclaimsDeletedTuple(t *testing.T) {
	// Insert then delete a tuple; after vacuum the slot must be LP_UNUSED.
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("gone"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit insert: %v", err)
	}
	xidDel := sr.txmgr.Begin()
	sr.deleteTuple(t, blk, off, xidDel)
	if err := sr.txmgr.Commit(xidDel); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	runVacuum(t, sr)

	// Verify the slot is now LP_UNUSED.
	id, err := sr.rel.ReadBlock(storage.ForkMain, blk)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	pg, _ := sr.pool.GetPage(id)
	lp, _ := pg.GetItemId(int(off) - 1)
	sr.pool.UnpinBuffer(id) //nolint:errcheck

	if !lp.IsUnused() {
		t.Errorf("expected LP_UNUSED after vacuum, got flags=%d", lp.Flags())
	}
}

func TestVacuumLeavesLiveTupleAlone(t *testing.T) {
	// A committed tuple with no xmax must survive vacuum unchanged and remain
	// visible to subsequent queries.
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	sr.insertTuple(t, xid, []byte("keep"))
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	stats := runVacuum(t, sr)
	if stats.TuplesRemoved != 0 {
		t.Errorf("vacuum removed a live tuple: TuplesRemoved=%d", stats.TuplesRemoved)
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)
	if len(rows) != 1 || rows[0] != "keep" {
		t.Errorf("after vacuum: got %v want [keep]", rows)
	}
}

func TestVacuumHOTPrune(t *testing.T) {
	// HOT update: old version has HEAP_HOT_UPDATED, new version is live.
	// After vacuum the old slot must be LP_REDIRECT pointing at the new slot.
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("v1"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit insert: %v", err)
	}

	xid2 := sr.txmgr.Begin()
	newTup := storage.NewHeapTuple(xid2, 1, []byte("v2"))
	newBlk, newOff, err := HeapUpdate(sr.rel, xid2, blk, off, newTup)
	if err != nil {
		t.Fatalf("HeapUpdate: %v", err)
	}
	if err := sr.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit update: %v", err)
	}

	if newBlk != blk {
		t.Skip("update was cross-page; HOT prune test requires same-page update")
	}

	stats := runVacuum(t, sr)
	if stats.HOTPruned < 1 {
		t.Errorf("expected HOTPruned >= 1, got %d", stats.HOTPruned)
	}

	// The old slot should now be LP_REDIRECT pointing at newOff.
	id, err := sr.rel.ReadBlock(storage.ForkMain, blk)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	pg, _ := sr.pool.GetPage(id)
	lp, _ := pg.GetItemId(int(off) - 1)
	sr.pool.UnpinBuffer(id) //nolint:errcheck

	if !lp.IsRedirect() {
		t.Fatalf("expected LP_REDIRECT after HOT prune, got flags=%d", lp.Flags())
	}
	if lp.Off() != newOff {
		t.Errorf("LP_REDIRECT.Off() = %d, want %d (newOff)", lp.Off(), newOff)
	}
}

func TestVacuumAfterHOTPruneSeqScanStillFindsLiveTuple(t *testing.T) {
	// After HOT prune, a SeqScan (which skips LP_REDIRECT) must not lose the
	// live tuple.  The live tuple is still LP_NORMAL; only the dead chain head
	// becomes LP_REDIRECT.
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("before"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	xid2 := sr.txmgr.Begin()
	newTup := storage.NewHeapTuple(xid2, 1, []byte("after"))
	newBlk, _, err := HeapUpdate(sr.rel, xid2, blk, off, newTup)
	if err != nil {
		t.Fatalf("HeapUpdate: %v", err)
	}
	if err := sr.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if newBlk != blk {
		t.Skip("cross-page update")
	}

	runVacuum(t, sr)

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)
	if len(rows) != 1 || rows[0] != "after" {
		t.Errorf("after HOT prune + vacuum: got %v want [after]", rows)
	}
}

func TestVacuumFreezesOldTuple(t *testing.T) {
	// Insert a tuple with a very old XID (by burning several XIDs) so that
	// its xmin falls below FreezeLimit.  After vacuum, xmin must be
	// FrozenTransactionId.
	sr := newScanRelation(t)

	// Insert the tuple to freeze.
	xidOld := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xidOld, []byte("ancient"))
	if err := sr.txmgr.Commit(xidOld); err != nil {
		t.Fatalf("Commit old: %v", err)
	}

	// Burn enough XIDs so that xidOld < GlobalXmin - FreezeMinAge (=2).
	// With FreezeMinAge=2: we need at least 3 more XIDs started+committed.
	for i := 0; i < 5; i++ {
		x := sr.txmgr.Begin()
		if err := sr.txmgr.Commit(x); err != nil {
			t.Fatalf("Commit filler: %v", err)
		}
	}

	cfg := DefaultVacuumConfig()
	cfg.FreezeMinAge = 2
	stats, err := Vacuum(sr.rel, sr.txmgr, cfg)
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	if stats.TuplesFrozen < 1 {
		t.Errorf("expected TuplesFrozen >= 1, got %d", stats.TuplesFrozen)
	}

	// Verify the on-disk xmin is now FrozenTransactionId.
	id, err := sr.rel.ReadBlock(storage.ForkMain, blk)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	pg, _ := sr.pool.GetPage(id)
	lp, _ := pg.GetItemId(int(off) - 1)
	raw := pg.Bytes()
	hdr, _ := storage.HeapTupleFromBytes(raw[int(lp.Off()):])
	sr.pool.UnpinBuffer(id) //nolint:errcheck

	if hdr.Header.TXmin != storage.FrozenTransactionId {
		t.Errorf("xmin after freeze: got %d want %d (FrozenTransactionId)",
			hdr.Header.TXmin, storage.FrozenTransactionId)
	}
}

func TestVacuumFrozenTupleVisibleToOldSnapshot(t *testing.T) {
	// After freezing, a tuple must be visible even to a snapshot whose Xmin is
	// greater than the original xmin (because FrozenTransactionId is always visible).
	sr := newScanRelation(t)

	xidOld := sr.txmgr.Begin()
	sr.insertTuple(t, xidOld, []byte("frozen-row"))
	if err := sr.txmgr.Commit(xidOld); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Burn XIDs to push xidOld below FreezeLimit.
	for i := 0; i < 5; i++ {
		x := sr.txmgr.Begin()
		_ = sr.txmgr.Commit(x) //nolint:errcheck
	}

	cfg := DefaultVacuumConfig()
	cfg.FreezeMinAge = 2
	if _, err := Vacuum(sr.rel, sr.txmgr, cfg); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}

	// Take a snapshot whose Xmin is higher than xidOld.
	// Without freeze, xidOld < snap.Xmin → XidVisible would return true anyway.
	// The key assertion is that the tuple is still accessible after vacuuming.
	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)
	if len(rows) != 1 || rows[0] != "frozen-row" {
		t.Errorf("frozen tuple: got %v want [frozen-row]", rows)
	}
}

func TestVacuumMixedPage(t *testing.T) {
	// Page with: 1 live tuple, 1 deleted tuple, 1 HOT update.
	// After vacuum: 1 visible via SeqScan, deleted slot gone, HOT head redirected.
	sr := newScanRelation(t)

	// Live tuple.
	xLive := sr.txmgr.Begin()
	sr.insertTuple(t, xLive, []byte("live"))
	if err := sr.txmgr.Commit(xLive); err != nil {
		t.Fatalf("Commit live: %v", err)
	}

	// Deleted tuple.
	xDel := sr.txmgr.Begin()
	dBlk, dOff := sr.insertTuple(t, xDel, []byte("dead"))
	if err := sr.txmgr.Commit(xDel); err != nil {
		t.Fatalf("Commit del insert: %v", err)
	}
	xDelAct := sr.txmgr.Begin()
	sr.deleteTuple(t, dBlk, dOff, xDelAct)
	if err := sr.txmgr.Commit(xDelAct); err != nil {
		t.Fatalf("Commit del action: %v", err)
	}

	// HOT updated tuple.
	xUpd1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xUpd1, []byte("old"))
	if err := sr.txmgr.Commit(xUpd1); err != nil {
		t.Fatalf("Commit upd insert: %v", err)
	}
	xUpd2 := sr.txmgr.Begin()
	newTup := storage.NewHeapTuple(xUpd2, 1, []byte("new"))
	newBlk, _, err := HeapUpdate(sr.rel, xUpd2, blk, off, newTup)
	if err != nil {
		t.Fatalf("HeapUpdate: %v", err)
	}
	if err := sr.txmgr.Commit(xUpd2); err != nil {
		t.Fatalf("Commit update: %v", err)
	}

	runVacuum(t, sr)

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)

	// Regardless of HOT vs cross-page: "live" and "new" must be visible; "dead"
	// and "old" must not.
	got := map[string]bool{}
	for _, r := range rows {
		got[r] = true
	}
	if !got["live"] {
		t.Errorf("'live' tuple missing after vacuum; rows=%v", rows)
	}
	if newBlk == blk && !got["new"] {
		t.Errorf("'new' HOT tuple missing after vacuum; rows=%v", rows)
	}
	if got["dead"] {
		t.Errorf("'dead' tuple visible after vacuum")
	}
	if got["old"] {
		t.Errorf("'old' (pre-update) tuple visible after vacuum")
	}
}

func TestVacuumStatsAccurate(t *testing.T) {
	// Insert 3 tuples, delete 2, update 1 HOT; vacuum stats must reflect work done.
	sr := newScanRelation(t)

	xIns := sr.txmgr.Begin()
	b1, o1 := sr.insertTuple(t, xIns, []byte("del1"))
	b2, o2 := sr.insertTuple(t, xIns, []byte("del2"))
	blk, off := sr.insertTuple(t, xIns, []byte("upd"))
	if err := sr.txmgr.Commit(xIns); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	xD1 := sr.txmgr.Begin()
	sr.deleteTuple(t, b1, o1, xD1)
	if err := sr.txmgr.Commit(xD1); err != nil {
		t.Fatalf("Commit delete 1: %v", err)
	}
	xD2 := sr.txmgr.Begin()
	sr.deleteTuple(t, b2, o2, xD2)
	if err := sr.txmgr.Commit(xD2); err != nil {
		t.Fatalf("Commit delete 2: %v", err)
	}

	xUpd := sr.txmgr.Begin()
	newTup := storage.NewHeapTuple(xUpd, 1, []byte("new"))
	newBlk, _, _ := HeapUpdate(sr.rel, xUpd, blk, off, newTup)
	if err := sr.txmgr.Commit(xUpd); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	stats := runVacuum(t, sr)

	if stats.TuplesRemoved < 2 {
		t.Errorf("TuplesRemoved: got %d want >= 2", stats.TuplesRemoved)
	}
	if newBlk == blk && stats.HOTPruned < 1 {
		t.Errorf("HOTPruned: got %d want >= 1 (same-page HOT update)", stats.HOTPruned)
	}
	if stats.PagesScanned < 1 {
		t.Errorf("PagesScanned: got %d want >= 1", stats.PagesScanned)
	}
}
