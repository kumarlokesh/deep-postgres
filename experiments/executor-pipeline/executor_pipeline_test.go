package execpipeline_test

// Executor pipeline experiment.
//
// Demonstrates the full stack - heap storage, MVCC snapshots, and a
// multi-node executor pipeline - working together end-to-end:
//
//   SeqScan → Filter → Project → Limit → TracedNode
//
// The trace output mirrors a simplified EXPLAIN ANALYZE: each operator
// reports its open event, the tuples it produced, and its close event.

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

// ── Row layout ────────────────────────────────────────────────────────────────
//
// Each row encodes three fixed-width uint32 columns packed as big-endian bytes:
//
//   [0:4]  id       row number (0-based)
//   [4:8]  score    id * 7
//   [8:12] category id % 5

const (
	colIDOff    = 0
	colScoreOff = 4
	colCatOff   = 8
	rowWidth    = 12
)

func encodeRow(id uint32) []byte {
	b := make([]byte, rowWidth)
	binary.BigEndian.PutUint32(b[0:], id)
	binary.BigEndian.PutUint32(b[4:], id*7)
	binary.BigEndian.PutUint32(b[8:], id%5)
	return b
}

func u32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }

// outputSchema is the projected output: id and score only.
var outputSchema = executor.Schema{Cols: []executor.Column{
	{Name: "id", Offset: 0, Len: 4},
	{Name: "score", Offset: 4, Len: 4},
}}

// ── Fixture ───────────────────────────────────────────────────────────────────

type testDB struct {
	rel   *storage.Relation
	txmgr *mvcc.TransactionManager
	snap  *storage.Snapshot
}

func newTestDB(t *testing.T, n int) (*testDB, func()) {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(256)
	rel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: 1}, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}

	txmgr := mvcc.NewTransactionManager()
	xid := txmgr.Begin()
	for i := 0; i < n; i++ {
		tup := storage.NewHeapTuple(xid, 3, encodeRow(uint32(i)))
		if _, _, err := executor.HeapInsert(rel, tup); err != nil {
			t.Fatalf("HeapInsert(%d): %v", i, err)
		}
	}
	if err := txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	reader := txmgr.Begin()
	snap := txmgr.Snapshot(reader)

	return &testDB{rel: rel, txmgr: txmgr, snap: snap},
		func() { smgr.Close() }
}

// ── Trace renderer ────────────────────────────────────────────────────────────

// planLine formats a single trace event for EXPLAIN-ANALYZE-style output.
func planLine(e executor.TraceEvent) string {
	switch e.Phase {
	case executor.TraceOpen:
		return fmt.Sprintf("%-14s  open", "["+e.NodeName+"]")
	case executor.TraceClose:
		return fmt.Sprintf("%-14s  close  (rows=%d)", "["+e.NodeName+"]", e.Seq)
	case executor.TraceTuple:
		d := e.Tuple.Tuple.Data
		if len(d) >= 8 {
			return fmt.Sprintf("%-14s  #%-3d   id=%-4d score=%d",
				"", e.Seq, u32(d[0:4]), u32(d[4:8]))
		}
		return fmt.Sprintf("%-14s  #%-3d   (short data)", "", e.Seq)
	}
	return ""
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestPipelineCorrectness verifies the end-to-end result of:
//
//	SeqScan(100 rows) → Filter(category==2) → Project(id,score) → Limit(5)
//
// category==2 rows are ids 2, 7, 12, 17, 22, ...   (id % 5 == 2)
func TestPipelineCorrectness(t *testing.T) {
	db, cleanup := newTestDB(t, 100)
	defer cleanup()

	ss, err := executor.NewSeqScan(db.rel, db.snap, db.txmgr)
	if err != nil {
		t.Fatal(err)
	}

	pipeline := executor.NewLimit(
		executor.NewProject(
			executor.NewFilter(ss, func(st *executor.ScanTuple) bool {
				return u32(st.Tuple.Data[colCatOff:colCatOff+4]) == 2
			}),
			outputSchema.ProjectFnFrom(),
		),
		5,
	)

	tuples, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 5 {
		t.Fatalf("want 5 tuples, got %d", len(tuples))
	}

	// Verify: first 5 ids with category==2 are 2, 7, 12, 17, 22.
	expected := []uint32{2, 7, 12, 17, 22}
	for i, st := range tuples {
		id := u32(outputSchema.Get("id", st))
		score := u32(outputSchema.Get("score", st))
		if id != expected[i] {
			t.Errorf("tuple %d: want id %d, got %d", i, expected[i], id)
		}
		if score != id*7 {
			t.Errorf("tuple %d: id=%d score want %d, got %d", i, id, id*7, score)
		}
		// Projected data should contain only id + score (8 bytes), not category.
		if len(st.Tuple.Data) != 8 {
			t.Errorf("tuple %d: projected data len want 8, got %d", i, len(st.Tuple.Data))
		}
	}
}

// TestPipelineTrace runs the pipeline wrapped in TracedNode to produce
// EXPLAIN ANALYZE-style output and verifies the event counts.
func TestPipelineTrace(t *testing.T) {
	db, cleanup := newTestDB(t, 50)
	defer cleanup()

	ss, err := executor.NewSeqScan(db.rel, db.snap, db.txmgr)
	if err != nil {
		t.Fatal(err)
	}

	// Build pipeline with a TracedNode at the top.
	var log []string
	pipeline := executor.NewTracedNode(
		executor.NewLimit(
			executor.NewProject(
				executor.NewFilter(ss, func(st *executor.ScanTuple) bool {
					return u32(st.Tuple.Data[colCatOff:colCatOff+4]) == 0
				}),
				outputSchema.ProjectFnFrom(),
			),
			4,
		),
		"Pipeline",
		func(e executor.TraceEvent) {
			log = append(log, planLine(e))
		},
	)

	tuples, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 4 {
		t.Fatalf("want 4 tuples, got %d", len(tuples))
	}

	// 1 Open + 4 Tuple + 1 Close = 6 events.
	if len(log) != 6 {
		t.Fatalf("want 6 trace events, got %d", len(log))
	}
	if !strings.Contains(log[0], "open") {
		t.Errorf("first log line should contain 'open': %q", log[0])
	}
	if !strings.Contains(log[len(log)-1], "close") {
		t.Errorf("last log line should contain 'close': %q", log[len(log)-1])
	}

	t.Logf("\n=== EXPLAIN ANALYZE ===\n%s", strings.Join(log, "\n"))
}

// TestMVCCIsolation verifies that a snapshot taken before a second batch of
// inserts sees only the first batch - the pipeline honours MVCC boundaries.
func TestMVCCIsolation(t *testing.T) {
	db, cleanup := newTestDB(t, 10)
	defer cleanup()

	// Snapshot db.snap was taken after the first 10 rows were committed.
	// Now insert 10 more rows under a new transaction.
	xid2 := db.txmgr.Begin()
	for i := 10; i < 20; i++ {
		tup := storage.NewHeapTuple(xid2, 3, encodeRow(uint32(i)))
		if _, _, err := executor.HeapInsert(db.rel, tup); err != nil {
			t.Fatalf("HeapInsert(%d): %v", i, err)
		}
	}
	if err := db.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// SeqScan with the OLD snapshot - should see only the first 10 rows.
	ss, err := executor.NewSeqScan(db.rel, db.snap, db.txmgr)
	if err != nil {
		t.Fatal(err)
	}
	tuples, err := executor.Collect(ss)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 10 {
		t.Fatalf("old snapshot should see 10 rows, got %d", len(tuples))
	}

	// SeqScan with a FRESH snapshot - should see all 20 rows.
	reader2 := db.txmgr.Begin()
	snap2 := db.txmgr.Snapshot(reader2)
	ss2, err := executor.NewSeqScan(db.rel, snap2, db.txmgr)
	if err != nil {
		t.Fatal(err)
	}
	tuples2, err := executor.Collect(ss2)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples2) != 20 {
		t.Fatalf("fresh snapshot should see 20 rows, got %d", len(tuples2))
	}
}

// TestEmptyResult exercises the pipeline when the filter matches nothing.
func TestEmptyResult(t *testing.T) {
	db, cleanup := newTestDB(t, 20)
	defer cleanup()

	ss, err := executor.NewSeqScan(db.rel, db.snap, db.txmgr)
	if err != nil {
		t.Fatal(err)
	}

	pipeline := executor.NewProject(
		executor.NewFilter(ss, func(*executor.ScanTuple) bool { return false }),
		outputSchema.ProjectFnFrom(),
	)

	tuples, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 0 {
		t.Fatalf("want 0 tuples, got %d", len(tuples))
	}
}

// ── TestMain ─────────────────────────────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "=== executor-pipeline experiment ===")
	fmt.Fprintln(os.Stderr, "Demonstrates the full executor stack:")
	fmt.Fprintln(os.Stderr, "  heap storage + MVCC + SeqScan → Filter → Project → Limit → TracedNode")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Row layout:  id(4B) | score=id*7(4B) | category=id%5(4B)")
	fmt.Fprintln(os.Stderr, "Query:       SELECT id, score WHERE category=2 LIMIT 5")
	fmt.Fprintln(os.Stderr)
	m.Run()
}
