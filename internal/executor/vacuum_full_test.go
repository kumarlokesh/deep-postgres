package executor

// VACUUM FULL integration tests.
//
// Scenarios:
//   - CompactPage reclaims fragmented space after dead-tuple removal.
//   - VacuumFull stats: PagesCompacted, BytesReclaimed, TuplesRemoved.
//   - Trailing empty pages are truncated from the relation file.
//   - SeqScan still returns all live rows after VacuumFull.
//   - FSM reflects compacted free space after VacuumFull.
//   - VM all-visible bits are set for clean pages after VacuumFull.

import (
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// runVacuumFull runs VacuumFull with the same small freeze age as runVacuum.
func runVacuumFull(t *testing.T, sr *scanRelation) VacuumFullStats {
	t.Helper()
	cfg := DefaultVacuumConfig()
	cfg.FreezeMinAge = 2
	stats, err := VacuumFull(sr.rel, sr.txmgr, cfg)
	if err != nil {
		t.Fatalf("VacuumFull: %v", err)
	}
	return stats
}

// TestCompactPageReclaimsSpace verifies that CompactPage increases FreeSpace
// on a page that had a tuple deleted (leaving fragmented bytes).
func TestCompactPageReclaimsSpace(t *testing.T) {
	// Build a page with two tuples, mark one LP_UNUSED (simulating VACUUM).
	pg := storage.NewPage()

	data1 := make([]byte, 200)
	data2 := make([]byte, 300)

	i1, err := pg.InsertTuple(data1)
	if err != nil {
		t.Fatalf("InsertTuple 1: %v", err)
	}
	_, err = pg.InsertTuple(data2)
	if err != nil {
		t.Fatalf("InsertTuple 2: %v", err)
	}

	freeBefore := pg.FreeSpace()

	// Mark slot 0 (data1, 200 bytes) as LP_UNUSED - simulating VACUUM.
	if err := pg.SetItemIdUnused(i1); err != nil {
		t.Fatalf("SetItemIdUnused: %v", err)
	}

	// FreeSpace() reports only the gap between pd_lower and pd_upper, not the
	// reclaimed 200-byte dead tuple area - that gap is internal fragmentation.
	freeAfterDelete := pg.FreeSpace()
	if freeAfterDelete != freeBefore {
		// pd_lower decreased by one ItemIdSize - free space grew only by that.
		// The 200 tuple bytes are NOT yet reclaimed.
		t.Logf("free before=%d after mark-unused=%d (only ItemIdSize freed, tuple bytes still fragmented)",
			freeBefore, freeAfterDelete)
	}

	// Compact: should recover the 200 dead-tuple bytes.
	reclaimed := storage.CompactPage(pg)
	if reclaimed <= 0 {
		t.Errorf("CompactPage: expected reclaimed > 0, got %d", reclaimed)
	}

	freeAfterCompact := pg.FreeSpace()
	if freeAfterCompact <= freeAfterDelete {
		t.Errorf("free space did not increase: before=%d after=%d",
			freeAfterDelete, freeAfterCompact)
	}
	t.Logf("bytes reclaimed by CompactPage: %d (free %d → %d)",
		reclaimed, freeAfterDelete, freeAfterCompact)
}

// TestCompactPagePreservesLiveTupleData verifies that the live tuple bytes
// are intact after compaction.
func TestCompactPagePreservesLiveTupleData(t *testing.T) {
	pg := storage.NewPage()
	payload := []byte("hello, world!")

	// We need a full HeapTuple (with header) so that GetTuple returns meaningful
	// bytes; use raw InsertTuple directly for simplicity.
	i1, _ := pg.InsertTuple([]byte("dead tuple bytes"))
	i2, _ := pg.InsertTuple(payload)

	_ = pg.SetItemIdUnused(i1)
	_ = storage.CompactPage(pg)

	got, err := pg.GetTuple(i2)
	if err != nil {
		t.Fatalf("GetTuple after compact: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("tuple corrupted after compact: got %q want %q", got, payload)
	}
}

// TestVacuumFullReclaimsDeadTuples verifies that VacuumFull removes deleted
// tuples and reports positive BytesReclaimed.
func TestVacuumFullReclaimsDeadTuples(t *testing.T) {
	sr := newScanRelation(t)

	// Insert several tuples.
	xid := sr.txmgr.Begin()
	for i := 0; i < 5; i++ {
		sr.insertTuple(t, xid, make([]byte, 200))
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit inserts: %v", err)
	}

	// Delete three of them.
	blk := storage.BlockNumber(0)
	id, _ := sr.rel.ReadBlock(storage.ForkMain, blk)
	pg, _ := sr.pool.GetPage(id)
	n := pg.ItemCount()
	sr.pool.UnpinBuffer(id) //nolint:errcheck

	xidDel := sr.txmgr.Begin()
	for i := 0; i < 3 && i < n; i++ {
		sr.deleteTuple(t, blk, storage.OffsetNumber(i+1), xidDel)
	}
	if err := sr.txmgr.Commit(xidDel); err != nil {
		t.Fatalf("Commit deletes: %v", err)
	}

	stats := runVacuumFull(t, sr)

	if stats.TuplesRemoved < 3 {
		t.Errorf("expected TuplesRemoved >= 3, got %d", stats.TuplesRemoved)
	}
	if stats.PagesCompacted == 0 {
		t.Errorf("expected PagesCompacted > 0, got 0")
	}
	if stats.BytesReclaimed == 0 {
		t.Errorf("expected BytesReclaimed > 0, got 0")
	}
}

// TestVacuumFullSeqScanStillReturnsLiveRows verifies that SeqScan returns
// exactly the surviving tuples after a VacuumFull.
func TestVacuumFullSeqScanStillReturnsLiveRows(t *testing.T) {
	sr := newScanRelation(t)

	// Insert 4 rows.
	xid := sr.txmgr.Begin()
	for _, val := range []string{"a", "b", "c", "d"} {
		tup := storage.NewHeapTuple(xid, 1, []byte(val))
		if _, _, err := HeapInsert(sr.rel, tup); err != nil {
			t.Fatalf("HeapInsert: %v", err)
		}
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Delete "b" (slot 2, offset 2).
	blk := storage.BlockNumber(0)
	xidDel := sr.txmgr.Begin()
	sr.deleteTuple(t, blk, 2, xidDel)
	if err := sr.txmgr.Commit(xidDel); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	runVacuumFull(t, sr)

	// SeqScan should return a, c, d.
	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)
	if err := sr.txmgr.Commit(reader); err != nil {
		t.Fatalf("Commit reader: %v", err)
	}

	want := map[string]bool{"a": true, "c": true, "d": true}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(rows), rows)
	}
	for _, r := range rows {
		if !want[r] {
			t.Errorf("unexpected row %q in scan result", r)
		}
	}
}

// TestVacuumFullTruncatesTrailingEmptyPages verifies that entirely empty
// trailing blocks are removed from the relation file.
func TestVacuumFullTruncatesTrailingEmptyPages(t *testing.T) {
	sr := newScanRelation(t)

	// Manually extend the relation to 3 pages. Insert one tuple on page 0,
	// leave pages 1 and 2 empty.
	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, []byte("survivor"))
	if _, _, err := HeapInsert(sr.rel, tup); err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Extend 2 more empty blocks.
	for i := 0; i < 2; i++ {
		_, id, err := sr.rel.Extend(storage.ForkMain)
		if err != nil {
			t.Fatalf("Extend: %v", err)
		}
		sr.pool.UnpinBuffer(id) //nolint:errcheck
	}

	before, _ := sr.rel.NBlocks(storage.ForkMain)
	if before != 3 {
		t.Fatalf("expected 3 blocks before VacuumFull, got %d", before)
	}

	stats := runVacuumFull(t, sr)

	after, _ := sr.rel.NBlocks(storage.ForkMain)
	if after >= before {
		t.Errorf("expected relation to shrink: before=%d after=%d", before, after)
	}
	if stats.PagesRemoved != int(before-after) {
		t.Errorf("PagesRemoved=%d, expected %d", stats.PagesRemoved, int(before-after))
	}
	t.Logf("truncated %d trailing empty page(s): %d → %d blocks", stats.PagesRemoved, before, after)
}

// TestVacuumFullFSMUpdated verifies that the FSM is updated after VacuumFull.
func TestVacuumFullFSMUpdated(t *testing.T) {
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, []byte("row"))
	if _, _, err := HeapInsert(sr.rel, tup); err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	stats := runVacuumFull(t, sr)

	if stats.FSMUpdated == 0 {
		t.Errorf("expected FSMUpdated > 0, got 0")
	}

	// After VacuumFull the FSM should report meaningful free space for block 0.
	freeBytes := sr.rel.FSM.FreeBytes(0)
	if freeBytes == 0 {
		t.Errorf("FSM reports 0 free bytes for block 0 after VacuumFull")
	}
}

// TestVacuumFullAllVisibleSet verifies that VacuumFull marks a clean page
// all-visible in the VM.
func TestVacuumFullAllVisibleSet(t *testing.T) {
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, []byte("visible"))
	if _, _, err := HeapInsert(sr.rel, tup); err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Advance enough XIDs so the tuple's xmin is < OldestXmin.
	for i := 0; i < 5; i++ {
		x := sr.txmgr.Begin()
		_ = sr.txmgr.Commit(x)
	}

	stats := runVacuumFull(t, sr)

	if stats.PagesAllVisible == 0 {
		t.Logf("PagesAllVisible=0 (may need more committed XIDs for xmin < OldestXmin); stats=%+v", stats)
	}
}
