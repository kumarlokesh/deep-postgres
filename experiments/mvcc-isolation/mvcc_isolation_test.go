// Package mvcc_isolation demonstrates PostgreSQL's MVCC snapshot isolation
// semantics by interleaving operations from multiple virtual transactions and
// verifying that each transaction observes exactly the rows it should.
//
// Each test is a single isolation scenario.  The transactions are scheduled
// deterministically (no goroutines) — we interleave operations by hand to hit
// specific MVCC edge cases, then assert the exact rows visible to each snapshot.
//
// Run with:
//
//	go test -v ./experiments/mvcc-isolation/
//
// What the scenarios prove:
//   1. Read-your-own-writes       — a tx sees its own uncommitted inserts.
//   2. Dirty read prevention      — uncommitted inserts are invisible to peers.
//   3. Snapshot consistency       — rows committed after snapshot are invisible.
//   4. Delete isolation           — committed deletes are hidden from older snapshots.
//   5. Aborted insert invisible   — aborted tx leaves no visible row.
//   6. Write-write interleaving   — each tx's insert is isolated until commit.
//   7. HOT update chain           — old snapshot sees V1; new snapshot sees V2.
//   8. Command counter visibility — earlier CID rows visible, later CID rows not.
package mvcc_isolation

import (
	"fmt"
	"os"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/executor"
	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── fixture ───────────────────────────────────────────────────────────────────

type fixture struct {
	rel   *storage.Relation
	txmgr *mvcc.TransactionManager
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "mvcc-isolation-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(64)
	rfn := storage.RelFileNode{DbId: 1, RelId: 1}
	rel := storage.OpenRelation(rfn, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}
	t.Cleanup(func() {
		smgr.Close()
		os.RemoveAll(dir)
	})
	return &fixture{rel: rel, txmgr: mvcc.NewTransactionManager()}
}

// insert writes a single-field tuple under xid and returns its (block, offset).
func (f *fixture) insert(t *testing.T, xid storage.TransactionId, payload string) (storage.BlockNumber, storage.OffsetNumber) {
	t.Helper()
	tup := storage.NewHeapTuple(xid, 1, []byte(payload))
	blk, off, err := executor.HeapInsert(f.rel, tup)
	if err != nil {
		t.Fatalf("HeapInsert(%q): %v", payload, err)
	}
	return blk, off
}

// insertCID is like insert but lets the caller specify an explicit command ID.
// Used in command-counter tests.
func (f *fixture) insertCID(t *testing.T, xid storage.TransactionId, cid storage.CommandId, payload string) {
	t.Helper()
	tup := storage.NewHeapTuple(xid, 1, []byte(payload))
	tup.Header.TCid = cid
	if _, _, err := executor.HeapInsert(f.rel, tup); err != nil {
		t.Fatalf("HeapInsert(%q): %v", payload, err)
	}
}

// scan returns all payloads visible to snap, in page order.
func (f *fixture) scan(t *testing.T, snap *storage.Snapshot) []string {
	t.Helper()
	ss, err := executor.NewSeqScan(f.rel, snap, f.txmgr)
	if err != nil {
		t.Fatalf("NewSeqScan: %v", err)
	}
	defer ss.Close()
	var results []string
	for {
		st, err := ss.Next()
		if err != nil {
			t.Fatalf("SeqScan.Next: %v", err)
		}
		if st == nil {
			break
		}
		tup := st.Tuple
		payload := string(tup.Data)
		results = append(results, payload)
	}
	return results
}

// delete marks the tuple at (blk, off) dead under xid.
func (f *fixture) delete(t *testing.T, xid storage.TransactionId, blk storage.BlockNumber, off storage.OffsetNumber) {
	t.Helper()
	id, err := f.rel.ReadBlock(storage.ForkMain, blk)
	if err != nil {
		t.Fatalf("delete ReadBlock: %v", err)
	}
	pg, err := f.rel.Pool.GetPageForWrite(id)
	if err != nil {
		f.rel.Pool.UnpinBuffer(id) //nolint:errcheck
		t.Fatalf("delete GetPageForWrite: %v", err)
	}
	lp, err := pg.GetItemId(int(off) - 1)
	if err != nil {
		f.rel.Pool.UnpinBuffer(id) //nolint:errcheck
		t.Fatalf("delete GetItemId: %v", err)
	}
	raw := pg.Bytes()
	o := int(lp.Off())
	tup, err := storage.HeapTupleFromBytes(raw[o:])
	if err != nil {
		f.rel.Pool.UnpinBuffer(id) //nolint:errcheck
		t.Fatalf("delete HeapTupleFromBytes: %v", err)
	}
	tup.Header.TXmax = xid
	tup.Header.TInfomask = uint16(
		storage.InfomaskFlags(tup.Header.TInfomask) &^ storage.HeapXmaxInvalid,
	)
	if err := pg.UpdateTupleHeader(int(off)-1, &tup.Header); err != nil {
		f.rel.Pool.UnpinBuffer(id) //nolint:errcheck
		t.Fatalf("delete UpdateTupleHeader: %v", err)
	}
	f.rel.Pool.UnpinBuffer(id) //nolint:errcheck
}

// assertRows fails if got != want (order-sensitive).
func assertRows(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d rows %v, want %d rows %v", label, len(got), got, len(want), want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s row[%d]: got %q, want %q", label, i, got[i], want[i])
		}
	}
}

// ── Scenario 1: Read-your-own-writes ─────────────────────────────────────────

// A transaction can see its own uncommitted inserts because xmin == Xid.
// PostgreSQL requires AdvanceCommand between an INSERT and the subsequent scan
// so the command-counter fence (TCid < CurCid) allows self-visibility.
func TestReadYourOwnWrites(t *testing.T) {
	f := newFixture(t)

	tx1 := f.txmgr.Begin()
	f.insert(t, tx1, "row-A") // CID=0

	// Advance command: now CurCid=1. The insert at CID=0 will be visible.
	if err := f.txmgr.AdvanceCommand(tx1); err != nil {
		t.Fatalf("AdvanceCommand: %v", err)
	}

	// Snapshot taken while Tx1 is still open.
	snap1 := f.txmgr.Snapshot(tx1) // CurCid=1

	// Tx1 should see its own insert (CID 0 < CurCid 1).
	assertRows(t, "Tx1 snapshot", f.scan(t, snap1), []string{"row-A"})

	// A concurrent observer (Tx2) takes its own snapshot before Tx1 commits.
	tx2 := f.txmgr.Begin()
	snap2 := f.txmgr.Snapshot(tx2)
	// Tx2 must NOT see Tx1's uncommitted row.
	assertRows(t, "Tx2 pre-commit snapshot", f.scan(t, snap2), []string{})

	f.txmgr.Commit(tx1) //nolint:errcheck
	f.txmgr.Commit(tx2) //nolint:errcheck

	// Tx2's old snapshot still does not see the committed row —
	// snapshot isolation means the row was invisible at snapshot time.
	assertRows(t, "Tx2 old snapshot after Tx1 commit", f.scan(t, snap2), []string{})

	// A fresh snapshot taken after Tx1 commits sees the row.
	tx3 := f.txmgr.Begin()
	snap3 := f.txmgr.Snapshot(tx3)
	assertRows(t, "Tx3 fresh snapshot", f.scan(t, snap3), []string{"row-A"})
	f.txmgr.Commit(tx3) //nolint:errcheck
}

// ── Scenario 2: Dirty read prevention ────────────────────────────────────────

// No transaction can see another transaction's uncommitted inserts.
func TestDirtyReadPrevention(t *testing.T) {
	f := newFixture(t)

	// Tx1 starts and inserts but does not commit.
	tx1 := f.txmgr.Begin()
	f.insert(t, tx1, "dirty-row")

	// Tx2 starts after Tx1's insert but before commit.
	tx2 := f.txmgr.Begin()
	snap2 := f.txmgr.Snapshot(tx2)

	// Dirty read: Tx2 must not see Tx1's uncommitted row.
	assertRows(t, "Tx2 during Tx1 insert", f.scan(t, snap2), []string{})

	f.txmgr.Commit(tx1) //nolint:errcheck
	f.txmgr.Commit(tx2) //nolint:errcheck

	// The old snapshot for Tx2 still does not see it — snapshot was taken
	// when tx1 was in-progress (tx1 was in snap2.Xip).
	assertRows(t, "Tx2 old snap after Tx1 commit", f.scan(t, snap2), []string{})
}

// ── Scenario 3: Snapshot consistency / phantom prevention ────────────────────

// A snapshot taken at time T does not reflect rows committed after T,
// even if those rows are committed before the scan runs.
func TestSnapshotConsistency(t *testing.T) {
	f := newFixture(t)

	// Baseline row visible to everyone.
	tx0 := f.txmgr.Begin()
	f.insert(t, tx0, "baseline")
	f.txmgr.Commit(tx0) //nolint:errcheck

	// Tx1 takes a snapshot now: only sees baseline.
	tx1 := f.txmgr.Begin()
	snap1 := f.txmgr.Snapshot(tx1)

	// Tx2 inserts a new row and commits (after snap1 was taken).
	tx2 := f.txmgr.Begin()
	f.insert(t, tx2, "new-row")
	f.txmgr.Commit(tx2) //nolint:errcheck

	// Tx1's snapshot was taken before Tx2 started → must not see "new-row".
	assertRows(t, "Tx1 old snapshot", f.scan(t, snap1), []string{"baseline"})

	// A fresh snapshot after Tx2 committed sees both.
	tx3 := f.txmgr.Begin()
	snap3 := f.txmgr.Snapshot(tx3)
	assertRows(t, "Tx3 fresh snapshot", f.scan(t, snap3), []string{"baseline", "new-row"})

	f.txmgr.Commit(tx1) //nolint:errcheck
	f.txmgr.Commit(tx3) //nolint:errcheck
}

// ── Scenario 4: Delete isolation ─────────────────────────────────────────────

// A row deleted and committed by Tx2 is still visible to snapshots taken
// before the delete was committed.
func TestDeleteIsolation(t *testing.T) {
	f := newFixture(t)

	// Insert two rows and commit.
	tx0 := f.txmgr.Begin()
	blkA, offA := f.insert(t, tx0, "row-A")
	f.insert(t, tx0, "row-B")
	f.txmgr.Commit(tx0) //nolint:errcheck

	// Tx1 takes a snapshot (sees both rows).
	tx1 := f.txmgr.Begin()
	snap1 := f.txmgr.Snapshot(tx1)
	assertRows(t, "Tx1 before delete", f.scan(t, snap1), []string{"row-A", "row-B"})

	// Tx2 deletes row-A and commits.
	tx2 := f.txmgr.Begin()
	f.delete(t, tx2, blkA, offA)
	f.txmgr.Commit(tx2) //nolint:errcheck

	// Tx1's snapshot was taken before Tx2 started → still sees row-A.
	assertRows(t, "Tx1 after Tx2 delete", f.scan(t, snap1), []string{"row-A", "row-B"})

	// A new snapshot sees only row-B.
	tx3 := f.txmgr.Begin()
	snap3 := f.txmgr.Snapshot(tx3)
	assertRows(t, "Tx3 after delete committed", f.scan(t, snap3), []string{"row-B"})

	f.txmgr.Commit(tx1) //nolint:errcheck
	f.txmgr.Commit(tx3) //nolint:errcheck
}

// ── Scenario 5: Aborted insert is invisible ──────────────────────────────────

// A row inserted by an aborted transaction is invisible to every snapshot.
func TestAbortedInsertInvisible(t *testing.T) {
	f := newFixture(t)

	// Committed baseline.
	tx0 := f.txmgr.Begin()
	f.insert(t, tx0, "committed")
	f.txmgr.Commit(tx0) //nolint:errcheck

	// Tx1 inserts a row but aborts.
	tx1 := f.txmgr.Begin()
	f.insert(t, tx1, "aborted")
	f.txmgr.Abort(tx1) //nolint:errcheck

	// Any snapshot taken after the abort must not see the aborted row.
	tx2 := f.txmgr.Begin()
	snap2 := f.txmgr.Snapshot(tx2)
	assertRows(t, "after abort", f.scan(t, snap2), []string{"committed"})
	f.txmgr.Commit(tx2) //nolint:errcheck
}

// ── Scenario 6: Write-write interleaving ──────────────────────────────────────

// Two transactions insert rows concurrently (from the scheduler's perspective).
// Each transaction's row is invisible to the other until it commits.
func TestWriteWriteInterleaving(t *testing.T) {
	f := newFixture(t)

	// Both transactions start (and thus both XIDs appear in each other's Xip).
	tx1 := f.txmgr.Begin()
	tx2 := f.txmgr.Begin()

	f.insert(t, tx1, "from-tx1") // CID=0
	f.insert(t, tx2, "from-tx2") // CID=0

	// Advance command counter so each transaction can see its own CID=0 row.
	if err := f.txmgr.AdvanceCommand(tx1); err != nil {
		t.Fatalf("AdvanceCommand tx1: %v", err)
	}
	if err := f.txmgr.AdvanceCommand(tx2); err != nil {
		t.Fatalf("AdvanceCommand tx2: %v", err)
	}

	// Each takes a snapshot while the other is still in-progress.
	snap1 := f.txmgr.Snapshot(tx1) // Tx2 is in snap1.Xip; CurCid=1
	snap2 := f.txmgr.Snapshot(tx2) // Tx1 is in snap2.Xip; CurCid=1

	// Tx1 sees only its own row; Tx2's insert is invisible (Tx2 ∈ snap1.Xip).
	assertRows(t, "Tx1 snapshot", f.scan(t, snap1), []string{"from-tx1"})
	// Tx2 sees only its own row; Tx1's insert is invisible (Tx1 ∈ snap2.Xip).
	assertRows(t, "Tx2 snapshot", f.scan(t, snap2), []string{"from-tx2"})

	// Commit both.
	f.txmgr.Commit(tx1) //nolint:errcheck
	f.txmgr.Commit(tx2) //nolint:errcheck

	// Fresh snapshot sees both rows.
	tx3 := f.txmgr.Begin()
	snap3 := f.txmgr.Snapshot(tx3)
	rows3 := f.scan(t, snap3)
	if len(rows3) != 2 {
		t.Errorf("after both commits: got %d rows, want 2: %v", len(rows3), rows3)
	}
	f.txmgr.Commit(tx3) //nolint:errcheck
}

// ── Scenario 7: HOT update chain visibility ───────────────────────────────────

// An old snapshot sees the pre-update version (V1); a new snapshot taken after
// the HOT update is committed sees V2.  HeapFetch follows the LP_REDIRECT and
// t_ctid chain to find the correct version.
func TestHOTUpdateChainVisibility(t *testing.T) {
	f := newFixture(t)

	// Insert V1 and commit.
	tx0 := f.txmgr.Begin()
	blk0, off0 := f.insert(t, tx0, "v1")
	f.txmgr.Commit(tx0) //nolint:errcheck

	// Tx1 takes a snapshot while only V1 exists.
	tx1 := f.txmgr.Begin()
	snap1 := f.txmgr.Snapshot(tx1)
	assertRows(t, "Tx1 before update", f.scan(t, snap1), []string{"v1"})

	// Tx2 performs a HOT update (V1→V2) on the same page.
	tx2 := f.txmgr.Begin()
	newTup := storage.NewHeapTuple(tx2, 1, []byte("v2"))
	newBlk, newOff, err := executor.HeapUpdate(f.rel, tx2, blk0, off0, newTup)
	if err != nil {
		t.Fatalf("HeapUpdate: %v", err)
	}
	f.txmgr.Commit(tx2) //nolint:errcheck

	// Tx1's old snapshot still sees V1.
	// HeapFetch should follow the chain and return V1 (which is visible to snap1).
	st, err := executor.HeapFetch(f.rel, snap1, f.txmgr, blk0, off0)
	if err != nil {
		t.Fatalf("HeapFetch (old snap): %v", err)
	}
	if st == nil {
		t.Fatal("HeapFetch (old snap): expected V1, got nil")
	}
	if string(st.Tuple.Data) != "v1" {
		t.Errorf("HeapFetch (old snap): got %q, want \"v1\"", st.Tuple.Data)
	}

	// A new snapshot after the commit sees V2.
	tx3 := f.txmgr.Begin()
	snap3 := f.txmgr.Snapshot(tx3)
	st3, err := executor.HeapFetch(f.rel, snap3, f.txmgr, newBlk, newOff)
	if err != nil {
		t.Fatalf("HeapFetch (new snap): %v", err)
	}
	if st3 == nil {
		t.Fatal("HeapFetch (new snap): expected V2, got nil")
	}
	if string(st3.Tuple.Data) != "v2" {
		t.Errorf("HeapFetch (new snap): got %q, want \"v2\"", st3.Tuple.Data)
	}

	f.txmgr.Commit(tx1) //nolint:errcheck
	f.txmgr.Commit(tx3) //nolint:errcheck
}

// ── Scenario 8: Command counter visibility ────────────────────────────────────

// Within a single transaction, a row inserted by an earlier command (lower CID)
// is visible to a snapshot taken at a higher CID.  A row inserted at a higher
// CID is NOT visible to a snapshot at a lower CID.
//
// This is the mechanism that prevents a single UPDATE from seeing its own
// inserted rows in a WHERE predicate scan of the same table.
func TestCommandCounterVisibility(t *testing.T) {
	f := newFixture(t)

	tx := f.txmgr.Begin()

	// CID 0: insert row-A.
	f.insertCID(t, tx, 0, "row-A")

	// Advance command counter: CID is now 1.
	if err := f.txmgr.AdvanceCommand(tx); err != nil {
		t.Fatalf("AdvanceCommand: %v", err)
	}

	// CID 1: insert row-B.
	f.insertCID(t, tx, 1, "row-B")

	// Snapshot at CID 1: row-A (CID 0 < 1) is visible; row-B (CID 1 == 1) is not.
	snap := f.txmgr.Snapshot(tx) // CurCid = 1 after AdvanceCommand
	assertRows(t, "snapshot at CID 1", f.scan(t, snap), []string{"row-A"})

	// After advancing to CID 2 both rows become visible.
	if err := f.txmgr.AdvanceCommand(tx); err != nil {
		t.Fatalf("AdvanceCommand 2: %v", err)
	}
	snap2 := f.txmgr.Snapshot(tx) // CurCid = 2
	assertRows(t, "snapshot at CID 2", f.scan(t, snap2), []string{"row-A", "row-B"})

	f.txmgr.Commit(tx) //nolint:errcheck
}

// ── Summary table printed to stderr ──────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "mvcc-isolation: running MVCC snapshot isolation scenarios")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "  Scenario                          Claim")
	fmt.Fprintln(os.Stderr, "  ─────────────────────────────── ──────────────────────────────────────────────")
	fmt.Fprintln(os.Stderr, "  ReadYourOwnWrites                own uncommitted inserts are visible")
	fmt.Fprintln(os.Stderr, "  DirtyReadPrevention              peer uncommitted inserts are invisible")
	fmt.Fprintln(os.Stderr, "  SnapshotConsistency              post-snapshot commits are invisible")
	fmt.Fprintln(os.Stderr, "  DeleteIsolation                  post-snapshot deletes are invisible")
	fmt.Fprintln(os.Stderr, "  AbortedInsertInvisible           aborted inserts leave no trace")
	fmt.Fprintln(os.Stderr, "  WriteWriteInterleaving           each tx isolated until commit")
	fmt.Fprintln(os.Stderr, "  HOTUpdateChainVisibility         old snap→V1, new snap→V2 via chain")
	fmt.Fprintln(os.Stderr, "  CommandCounterVisibility         CID fence prevents self-scan of own inserts")
	fmt.Fprintln(os.Stderr, "")
	os.Exit(m.Run())
}
