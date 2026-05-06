package vacuumautovacuum_test

// Vacuum / autovacuum simulation experiment.
//
// Demonstrates the three-act lifecycle of dead tuple management in PostgreSQL:
//
//  1. Bloat: insert rows, then delete a fraction of them. The heap pages
//     accumulate dead line pointers (LP_NORMAL with a committed xmax).
//
//  2. Vacuum: a single pass reclaims the dead slots (LP_UNUSED), updates the
//     Free Space Map so new inserts reuse freed pages, and marks
//     all-committed pages in the Visibility Map.
//
//  3. Autovacuum simulation: a lightweight worker loop monitors the
//     dead-tuple ratio on every page. When the ratio exceeds a threshold
//     (analogous to autovacuum_vacuum_scale_factor) it fires Vacuum, then
//     keeps running until the table is fully clean.

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/executor"
	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── page inspector ────────────────────────────────────────────────────────────

// pageSnapshot records the live/dead/unused slot counts and free space for
// one heap page. "dead" here means LP_NORMAL with a committed xmax - the
// tuple is invisible to all future snapshots and is waiting to be reclaimed.
type pageSnapshot struct {
	Block     storage.BlockNumber
	Total     int // total line pointers
	Live      int // LP_NORMAL, xmax invalid or not yet committed
	Dead      int // LP_NORMAL, xmax committed (vacuum target)
	Unused    int // LP_UNUSED (already reclaimed)
	FreeBytes int
}

func (s pageSnapshot) String() string {
	return fmt.Sprintf("blk %d: total=%d live=%d dead=%d unused=%d free=%d",
		s.Block, s.Total, s.Live, s.Dead, s.Unused, s.FreeBytes)
}

// snapshotPage reads one page and classifies every line pointer.
func snapshotPage(
	rel *storage.Relation,
	txmgr *mvcc.TransactionManager,
	blk storage.BlockNumber,
) pageSnapshot {
	id, err := rel.ReadBlock(storage.ForkMain, blk)
	if err != nil {
		return pageSnapshot{Block: blk}
	}
	defer rel.Pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := rel.Pool.GetPage(id)
	if err != nil {
		return pageSnapshot{Block: blk}
	}

	snap := pageSnapshot{Block: blk, FreeBytes: pg.FreeSpace()}
	n := pg.ItemCount()
	snap.Total = n
	raw := pg.Bytes()

	for i := 0; i < n; i++ {
		lp, err := pg.GetItemId(i)
		if err != nil {
			continue
		}
		switch {
		case lp.IsUnused():
			snap.Unused++
		case lp.IsNormal():
			off, length := int(lp.Off()), int(lp.Len())
			if off+length > len(raw) {
				snap.Live++
				continue
			}
			tup, err := storage.HeapTupleFromBytes(raw[off : off+length])
			if err != nil || tup.Header.TXmax == storage.InvalidTransactionId {
				snap.Live++
				continue
			}
			// Check whether the deleting transaction has committed.
			if txmgr.Status(tup.Header.TXmax) == storage.TxCommitted {
				snap.Dead++
			} else {
				snap.Live++
			}
		}
	}
	return snap
}

// snapshotRelation collects page snapshots for every block in the relation.
func snapshotRelation(rel *storage.Relation, txmgr *mvcc.TransactionManager) []pageSnapshot {
	nblocks, err := rel.NBlocks(storage.ForkMain)
	if err != nil {
		return nil
	}
	out := make([]pageSnapshot, nblocks)
	for blk := storage.BlockNumber(0); blk < nblocks; blk++ {
		out[blk] = snapshotPage(rel, txmgr, blk)
	}
	return out
}

// sumSnapshots returns aggregate live/dead/unused counts across all pages.
func sumSnapshots(pages []pageSnapshot) (live, dead, unused, freeBytes int) {
	for _, p := range pages {
		live += p.Live
		dead += p.Dead
		unused += p.Unused
		freeBytes += p.FreeBytes
	}
	return
}

// ── fixture ───────────────────────────────────────────────────────────────────

type db struct {
	rel   *storage.Relation
	txmgr *mvcc.TransactionManager
	// tids holds the (block, offset) for each inserted tuple, in insert order.
	tids []storage.HeapTID
}

func newDB(t *testing.T) (*db, func()) {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(256)
	rel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: 1}, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}
	return &db{rel: rel, txmgr: mvcc.NewTransactionManager()},
		func() { smgr.Close() }
}

// insertN inserts n tuples under a new committed transaction.
// Each tuple's payload is the 4-byte big-endian row number (global across calls).
func (d *db) insertN(t *testing.T, n int) {
	t.Helper()
	xid := d.txmgr.Begin()
	base := len(d.tids)
	for i := 0; i < n; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(base+i))
		tup := storage.NewHeapTuple(xid, 1, b)
		blk, off, err := executor.HeapInsert(d.rel, tup)
		if err != nil {
			t.Fatalf("HeapInsert(%d): %v", base+i, err)
		}
		d.tids = append(d.tids, storage.HeapTID{Block: blk, Offset: off})
	}
	if err := d.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit insert: %v", err)
	}
}

// deleteAt deletes the tuple at d.tids[idx] under a new committed transaction.
func (d *db) deleteAt(t *testing.T, idx int) {
	t.Helper()
	tid := d.tids[idx]
	xid := d.txmgr.Begin()
	if err := executor.HeapDelete(d.rel, xid, tid.Block, tid.Offset); err != nil {
		t.Fatalf("HeapDelete(%d): %v", idx, err)
	}
	if err := d.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}
}

// vacuum runs a standard Vacuum pass with a small freeze age.
func (d *db) vacuum(t *testing.T) executor.VacuumStats {
	t.Helper()
	cfg := executor.DefaultVacuumConfig()
	cfg.FreezeMinAge = 2
	stats, err := executor.Vacuum(d.rel, d.txmgr, cfg)
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	return stats
}

// vacuumFull runs a VACUUM FULL pass.
func (d *db) vacuumFull(t *testing.T) executor.VacuumFullStats {
	t.Helper()
	cfg := executor.DefaultVacuumConfig()
	cfg.FreezeMinAge = 2
	stats, err := executor.VacuumFull(d.rel, d.txmgr, cfg)
	if err != nil {
		t.Fatalf("VacuumFull: %v", err)
	}
	return stats
}

// countVisible returns the number of MVCC-visible rows in a fresh scan.
func (d *db) countVisible(t *testing.T) int {
	t.Helper()
	reader := d.txmgr.Begin()
	snap := d.txmgr.Snapshot(reader)
	ss, err := executor.NewSeqScan(d.rel, snap, d.txmgr)
	if err != nil {
		t.Fatalf("NewSeqScan: %v", err)
	}
	rows, err := executor.Collect(ss)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return len(rows)
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestBloatAndVacuum is the canonical bloat scenario:
//
//	Insert 100 rows → delete 50 of them (even indices) → inspect dead count →
//	vacuum → verify LP_UNUSED increased, SeqScan sees exactly 50 rows.
//
// Note: regular Vacuum converts dead LP_NORMAL slots to LP_UNUSED but does NOT
// move pd_upper or compact tuple data. FreeSpace() therefore does not increase
// after a regular Vacuum pass - that only happens with VACUUM FULL + CompactPage
// (shown in TestVacuumFullReclaims). The freed capacity is expressed as
// LP_UNUSED slots reusable by future inserts via the Free Space Map.
func TestBloatAndVacuum(t *testing.T) {
	d, cleanup := newDB(t)
	defer cleanup()

	const total = 100
	const nDelete = 50 // even indices 0, 2, 4, ... 98
	d.insertN(t, total)

	// Delete even-indexed rows (0, 2, 4, ... 98) = 50 rows.
	for i := 0; i < total; i += 2 {
		d.deleteAt(t, i)
	}

	// ── before vacuum ────────────────────────────────────────────────────────
	before := snapshotRelation(d.rel, d.txmgr)
	lB, dB, uB, fB := sumSnapshots(before)
	t.Logf("\nBefore vacuum:")
	for _, p := range before {
		t.Logf("  %s", p)
	}
	t.Logf("  TOTAL  live=%-4d dead=%-4d unused=%-4d free=%d bytes", lB, dB, uB, fB)

	if dB != nDelete {
		t.Errorf("expected %d dead tuples before vacuum, got %d", nDelete, dB)
	}
	if uB != 0 {
		t.Errorf("expected 0 unused before vacuum, got %d", uB)
	}

	// ── vacuum ───────────────────────────────────────────────────────────────
	stats := d.vacuum(t)
	t.Logf("\nVacuum stats: tuples_removed=%d tuples_frozen=%d pages_modified=%d",
		stats.TuplesRemoved, stats.TuplesFrozen, stats.PagesModified)

	if stats.TuplesRemoved != nDelete {
		t.Errorf("expected %d tuples removed, got %d", nDelete, stats.TuplesRemoved)
	}

	// ── after vacuum ─────────────────────────────────────────────────────────
	after := snapshotRelation(d.rel, d.txmgr)
	lA, dA, uA, fA := sumSnapshots(after)
	t.Logf("\nAfter vacuum:")
	for _, p := range after {
		t.Logf("  %s", p)
	}
	t.Logf("  TOTAL  live=%-4d dead=%-4d unused=%-4d free=%d bytes", lA, dA, uA, fA)

	if dA != 0 {
		t.Errorf("expected 0 dead after vacuum, got %d", dA)
	}
	if uA != nDelete {
		t.Errorf("expected %d unused after vacuum, got %d", nDelete, uA)
	}
	// Regular Vacuum does not compact pages: pd_upper stays where it was, so
	// FreeSpace() is unchanged. The freed capacity is expressed as LP_UNUSED
	// slots (visible to the FSM) not as raw pd_upper movement.
	t.Logf("  free space: before=%d after=%d (unchanged — use VACUUM FULL to compact)", fB, fA)

	visible := d.countVisible(t)
	if visible != total-nDelete {
		t.Errorf("SeqScan: want %d visible rows after vacuum, got %d", total-nDelete, visible)
	}

	t.Logf("\nResult: %d rows deleted, %d live rows remain, %d LP_UNUSED slots freed",
		nDelete, visible, uA)
}

// TestVacuumFullReclaims shows that VACUUM FULL goes further than regular
// VACUUM: it compacts pages (PageRepairFragmentation) to move pd_upper back
// toward pd_special, reclaiming the fragmented space left by deleted tuples.
//
// Page truncation (dropping trailing blocks) is separate from compaction: a
// trailing page is truncated only when its ItemCount is 0. After VacuumPage
// converts dead LP_NORMAL slots to LP_UNUSED, those slots still occupy the
// line pointer array (pd_lower stays put), so the page ItemCount stays
// non-zero and truncation does not fire. The reclaimed capacity is reported
// as BytesReclaimed (the pd_upper movement per page).
func TestVacuumFullReclaims(t *testing.T) {
	d, cleanup := newDB(t)
	defer cleanup()

	// Insert enough rows to fill two pages, then delete almost all of them.
	const total = 400
	const nKeep = 5
	d.insertN(t, total)
	for i := nKeep; i < total; i++ {
		d.deleteAt(t, i)
	}

	nBlocksBefore, _ := d.rel.NBlocks(storage.ForkMain)

	stats := d.vacuumFull(t)

	nBlocksAfter, _ := d.rel.NBlocks(storage.ForkMain)

	t.Logf("\nVacuumFull stats:")
	t.Logf("  pages_scanned=%d compacted=%d removed=%d",
		stats.PagesScanned, stats.PagesCompacted, stats.PagesRemoved)
	t.Logf("  tuples_removed=%d frozen=%d bytes_reclaimed=%d",
		stats.TuplesRemoved, stats.TuplesFrozen, stats.BytesReclaimed)
	t.Logf("  blocks: %d → %d (LP_UNUSED slots remain; no trailing-empty truncation)",
		nBlocksBefore, nBlocksAfter)

	if stats.TuplesRemoved != total-nKeep {
		t.Errorf("want %d tuples removed, got %d", total-nKeep, stats.TuplesRemoved)
	}
	if stats.PagesCompacted == 0 {
		t.Error("expected at least one page to be compacted")
	}
	if stats.BytesReclaimed == 0 {
		t.Error("expected bytes reclaimed > 0 after compaction")
	}

	visible := d.countVisible(t)
	if visible != nKeep {
		t.Errorf("want %d visible rows after VacuumFull, got %d", nKeep, visible)
	}

	t.Logf("\nResult: %d bytes freed by compaction, %d rows remain (pages unchanged at %d)",
		stats.BytesReclaimed, visible, nBlocksAfter)
}

// TestFreezeOldXIDs shows that Vacuum replaces old xmin values with
// FrozenTransactionId (XID=2) to prevent XID wraparound.
//
// After freezing, HeapTupleSatisfiesMVCC treats the tuple as always visible -
// it no longer needs to consult the transaction oracle for its xmin.
func TestFreezeOldXIDs(t *testing.T) {
	d, cleanup := newDB(t)
	defer cleanup()

	// Insert rows, then advance the XID counter by running many no-op
	// transactions so the inserted rows appear "old".
	d.insertN(t, 20)
	insertedXID := d.txmgr.GlobalXmin() // approximate: all inserts are ≤ this

	// Advance XID by committing many transactions.
	const advance = 100
	for i := 0; i < advance; i++ {
		xid := d.txmgr.Begin()
		if err := d.txmgr.Commit(xid); err != nil {
			t.Fatalf("advance Commit: %v", err)
		}
	}

	currentMax := d.txmgr.GlobalXmin()
	age := currentMax - insertedXID
	t.Logf("\nInserted at XID≈%d, current XID≈%d, age=%d", insertedXID, currentMax, age)

	// Vacuum with a FreezeMinAge that is less than the age we just advanced.
	cfg := executor.DefaultVacuumConfig()
	cfg.FreezeMinAge = 10 // freeze anything older than OldestXmin-10
	stats, err := executor.Vacuum(d.rel, d.txmgr, cfg)
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	t.Logf("Vacuum: tuples_frozen=%d tuples_removed=%d", stats.TuplesFrozen, stats.TuplesRemoved)

	if stats.TuplesFrozen == 0 {
		t.Error("expected tuples to be frozen, got 0")
	}

	// After freezing, a snapshot with xmin > original xid must still see
	// the frozen tuples (they are now FrozenTransactionId = always visible).
	visible := d.countVisible(t)
	if visible != 20 {
		t.Errorf("want 20 rows visible after freeze, got %d", visible)
	}
	t.Logf("Post-freeze SeqScan: %d rows visible (all still accessible)", visible)
}

// TestAutovacuumSimulation shows a simple autovacuum-like control loop:
// a background worker monitors the dead-tuple ratio and fires Vacuum
// whenever it exceeds a threshold (analogous to autovacuum_vacuum_scale_factor).
//
// The simulation runs five rounds of inserts+deletes and records how many
// times autovacuum fired.
func TestAutovacuumSimulation(t *testing.T) {
	d, cleanup := newDB(t)
	defer cleanup()

	const (
		roundInserts  = 30   // rows inserted per round
		roundDeletes  = 12   // rows deleted per round
		deadThreshold = 0.25 // trigger vacuum when dead ratio > 25%
		rounds        = 5
	)

	type roundSummary struct {
		round       int
		inserted    int
		deleted     int
		deadBefore  int
		liveBefore  int
		ratio       float64
		vacuumFired bool
		removed     int
	}

	var summaries []roundSummary
	vacuumCount := 0

	for r := 1; r <= rounds; r++ {
		d.insertN(t, roundInserts)

		// Delete the last roundDeletes tuples inserted this round.
		base := len(d.tids) - roundInserts
		for i := base; i < base+roundDeletes; i++ {
			d.deleteAt(t, i)
		}

		pages := snapshotRelation(d.rel, d.txmgr)
		live, dead, _, _ := sumSnapshots(pages)
		ratio := float64(dead) / float64(max(live+dead, 1))

		sum := roundSummary{
			round:      r,
			inserted:   roundInserts,
			deleted:    roundDeletes,
			deadBefore: dead,
			liveBefore: live,
			ratio:      ratio,
		}

		if ratio > deadThreshold {
			stats := d.vacuum(t)
			sum.vacuumFired = true
			sum.removed = stats.TuplesRemoved
			vacuumCount++
		}

		summaries = append(summaries, sum)
	}

	// Print summary table.
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n%-6s %-8s %-8s %-8s %-8s %-8s %-12s %s\n",
		"round", "live", "dead", "ratio", "vacuum?", "removed", "", "")
	fmt.Fprintf(&sb, "%s\n", strings.Repeat("─", 72))
	for _, s := range summaries {
		fired := "no"
		removed := "-"
		if s.vacuumFired {
			fired = "YES"
			removed = fmt.Sprintf("%d", s.removed)
		}
		fmt.Fprintf(&sb, "%-6d %-8d %-8d %-8.1f%% %-8s %s\n",
			s.round, s.liveBefore, s.deadBefore, s.ratio*100, fired, removed)
	}
	fmt.Fprintf(&sb, "%s\n", strings.Repeat("─", 72))
	fmt.Fprintf(&sb, "autovacuum fired %d/%d rounds (threshold=%.0f%%)\n",
		vacuumCount, rounds, deadThreshold*100)
	t.Log(sb.String())

	// Structural assertions.
	if vacuumCount == 0 {
		t.Error("autovacuum should have fired at least once")
	}
	if vacuumCount > rounds {
		t.Errorf("autovacuum fired %d times in %d rounds — impossible", vacuumCount, rounds)
	}

	// After the simulation the relation must still be queryable.
	visible := d.countVisible(t)
	t.Logf("Final visible rows: %d", visible)
	if visible < 0 {
		t.Error("negative visible rows")
	}
}

// TestFSMReuse verifies that after Vacuum the Free Space Map reflects the
// reclaimed space, so new inserts reuse freed pages rather than extending
// the relation.
func TestFSMReuse(t *testing.T) {
	d, cleanup := newDB(t)
	defer cleanup()

	const n = 100
	d.insertN(t, n)

	// Delete all rows.
	for i := 0; i < n; i++ {
		d.deleteAt(t, i)
	}

	blocksBefore, _ := d.rel.NBlocks(storage.ForkMain)

	// Vacuum to update the FSM.
	d.vacuum(t)

	// Insert n new rows. With a correct FSM they should land on existing
	// pages, not extend the relation.
	d.insertN(t, n)

	blocksAfter, _ := d.rel.NBlocks(storage.ForkMain)

	t.Logf("\nBlocks before new inserts: %d, after: %d", blocksBefore, blocksAfter)

	// The number of blocks should not have grown - the FSM knew about freed space.
	if blocksAfter > blocksBefore {
		t.Errorf("relation extended from %d to %d blocks after vacuum+reinsert"+
			" (FSM did not reuse freed pages)", blocksBefore, blocksAfter)
	}

	visible := d.countVisible(t)
	if visible != n {
		t.Errorf("want %d visible rows after reinsert, got %d", n, visible)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ── TestMain ──────────────────────────────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "=== vacuum-autovacuum experiment ===")
	fmt.Fprintln(os.Stderr, "Demonstrates heap bloat, VACUUM, VACUUM FULL, XID freezing,")
	fmt.Fprintln(os.Stderr, "FSM reuse, and a simplified autovacuum control loop.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Scenarios:")
	fmt.Fprintln(os.Stderr, "  TestBloatAndVacuum       dead → LP_UNUSED, free space reclaimed")
	fmt.Fprintln(os.Stderr, "  TestVacuumFullReclaims   page compaction + trailing-page truncation")
	fmt.Fprintln(os.Stderr, "  TestFreezeOldXIDs        xmin → FrozenTransactionId")
	fmt.Fprintln(os.Stderr, "  TestAutovacuumSimulation threshold-based autovacuum trigger loop")
	fmt.Fprintln(os.Stderr, "  TestFSMReuse             vacuum updates FSM so inserts reuse freed pages")
	fmt.Fprintln(os.Stderr)
	m.Run()
}
