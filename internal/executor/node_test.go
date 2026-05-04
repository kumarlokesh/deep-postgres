package executor_test

// Tests for the executor node model: Node interface, Filter, Limit,
// TracedNode, and Collect. The tests use a real SeqScan backed by
// an in-memory heap relation so that the pipeline exercises actual
// storage code rather than a stub.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/executor"
	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── compile-time interface checks ─────────────────────────────────────────────

var _ executor.Node = (*executor.SeqScan)(nil)
var _ executor.Node = (*executor.IndexScan)(nil)
var _ executor.Node = (*executor.Filter)(nil)
var _ executor.Node = (*executor.Limit)(nil)
var _ executor.Node = (*executor.TracedNode)(nil)

// ── fixture helpers ───────────────────────────────────────────────────────────

type testDB struct {
	rel    *storage.Relation
	txmgr  *mvcc.TransactionManager
	snap   *storage.Snapshot
	smgr   storage.StorageManager
}

// newDB creates a relation and inserts n tuples (payload = 4-byte big-endian
// index), all under a single committed transaction. The returned snap was
// taken after the commit, so all n tuples are visible.
func newDB(t *testing.T, n int) *testDB {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(128)
	node := storage.RelFileNode{DbId: 0, RelId: 1}
	rel := storage.OpenRelation(node, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}

	txmgr := mvcc.NewTransactionManager()
	xid := txmgr.Begin()
	for i := 0; i < n; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(i))
		tup := storage.NewHeapTuple(xid, 1, b)
		if _, _, err := executor.HeapInsert(rel, tup); err != nil {
			t.Fatalf("HeapInsert(%d): %v", i, err)
		}
	}
	if err := txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := txmgr.Begin()
	snap := txmgr.Snapshot(reader)

	t.Cleanup(func() { smgr.Close() })
	return &testDB{rel: rel, txmgr: txmgr, snap: snap, smgr: smgr}
}

func (db *testDB) seqScan(t *testing.T) *executor.SeqScan {
	t.Helper()
	ss, err := executor.NewSeqScan(db.rel, db.snap, db.txmgr)
	if err != nil {
		t.Fatalf("NewSeqScan: %v", err)
	}
	return ss
}

// tupleVal decodes the 4-byte big-endian payload written by newDB.
func tupleVal(st *executor.ScanTuple) uint32 {
	d := st.Tuple.Data
	if len(d) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(d[:4])
}

// ── Filter ────────────────────────────────────────────────────────────────────

func TestFilterEven(t *testing.T) {
	db := newDB(t, 20)
	f := executor.NewFilter(db.seqScan(t), func(st *executor.ScanTuple) bool {
		return tupleVal(st)%2 == 0
	})

	tuples, err := executor.Collect(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 10 {
		t.Fatalf("want 10 even tuples, got %d", len(tuples))
	}
	for _, st := range tuples {
		if v := tupleVal(st); v%2 != 0 {
			t.Errorf("expected even value, got %d", v)
		}
	}
}

func TestFilterNone(t *testing.T) {
	db := newDB(t, 10)
	f := executor.NewFilter(db.seqScan(t), func(*executor.ScanTuple) bool { return false })
	tuples, err := executor.Collect(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 0 {
		t.Fatalf("want 0 tuples, got %d", len(tuples))
	}
}

func TestFilterAll(t *testing.T) {
	db := newDB(t, 8)
	f := executor.NewFilter(db.seqScan(t), func(*executor.ScanTuple) bool { return true })
	tuples, err := executor.Collect(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 8 {
		t.Fatalf("want 8 tuples, got %d", len(tuples))
	}
}

// ── Limit ─────────────────────────────────────────────────────────────────────

func TestLimitBasic(t *testing.T) {
	db := newDB(t, 20)
	l := executor.NewLimit(db.seqScan(t), 5)
	tuples, err := executor.Collect(l)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 5 {
		t.Fatalf("want 5 tuples, got %d", len(tuples))
	}
}

func TestLimitExceedsInput(t *testing.T) {
	db := newDB(t, 3)
	l := executor.NewLimit(db.seqScan(t), 100)
	tuples, err := executor.Collect(l)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 3 {
		t.Fatalf("want 3 tuples, got %d", len(tuples))
	}
}

func TestLimitZero(t *testing.T) {
	db := newDB(t, 10)
	// limit <= 0 means no limit
	l := executor.NewLimit(db.seqScan(t), 0)
	tuples, err := executor.Collect(l)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 10 {
		t.Fatalf("want 10 tuples (no limit), got %d", len(tuples))
	}
}

// ── Chained pipeline: SeqScan → Filter → Limit ───────────────────────────────

func TestPipelineFilterThenLimit(t *testing.T) {
	// Insert 0..19; even values are 0,2,4,...18 (10 values); take first 4.
	db := newDB(t, 20)
	pipeline := executor.NewLimit(
		executor.NewFilter(db.seqScan(t), func(st *executor.ScanTuple) bool {
			return tupleVal(st)%2 == 0
		}),
		4,
	)

	tuples, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 4 {
		t.Fatalf("want 4 tuples, got %d", len(tuples))
	}
	for _, st := range tuples {
		if v := tupleVal(st); v%2 != 0 {
			t.Errorf("expected even value, got %d", v)
		}
	}
}

// ── TracedNode ────────────────────────────────────────────────────────────────

func TestTracedNodeEvents(t *testing.T) {
	db := newDB(t, 5)

	var events []executor.TraceEvent
	traced := executor.NewTracedNode(db.seqScan(t), "SeqScan", func(e executor.TraceEvent) {
		events = append(events, e)
	})

	tuples, err := executor.Collect(traced)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 5 {
		t.Fatalf("want 5 tuples, got %d", len(tuples))
	}

	// Expect: 1 Open + 5 Tuple + 1 Close = 7 events.
	if len(events) != 7 {
		t.Fatalf("want 7 trace events, got %d: %v", len(events), events)
	}
	if events[0].Phase != executor.TraceOpen {
		t.Errorf("first event should be Open, got %v", events[0].Phase)
	}
	if events[len(events)-1].Phase != executor.TraceClose {
		t.Errorf("last event should be Close, got %v", events[len(events)-1].Phase)
	}
	for i, e := range events[1 : len(events)-1] {
		if e.Phase != executor.TraceTuple {
			t.Errorf("event[%d] should be Tuple, got %v", i+1, e.Phase)
		}
		if e.Seq != i+1 {
			t.Errorf("event[%d] Seq want %d, got %d", i+1, i+1, e.Seq)
		}
		if e.Tuple == nil {
			t.Errorf("event[%d] Tuple is nil", i+1)
		}
	}
	if events[len(events)-1].Seq != 5 {
		t.Errorf("Close event Seq want 5, got %d", events[len(events)-1].Seq)
	}
}

func TestTracedNodeNames(t *testing.T) {
	db := newDB(t, 3)

	var log []string
	makeTracer := func(name string) executor.Tracer {
		return func(e executor.TraceEvent) {
			log = append(log, fmt.Sprintf("%s:%s", e.NodeName, e.Phase))
		}
	}

	pipeline := executor.NewTracedNode(
		executor.NewTracedNode(db.seqScan(t), "SeqScan", makeTracer("SeqScan")),
		"Filter",
		makeTracer("Filter"),
	)

	_, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}

	// Both nodes should emit Open + Tuple×3 + Close events.
	openCount, closeCount := 0, 0
	for _, entry := range log {
		if strings.HasSuffix(entry, ":open") {
			openCount++
		}
		if strings.HasSuffix(entry, ":close") {
			closeCount++
		}
	}
	if openCount != 2 {
		t.Errorf("want 2 open events (one per node), got %d; log=%v", openCount, log)
	}
	if closeCount != 2 {
		t.Errorf("want 2 close events (one per node), got %d; log=%v", closeCount, log)
	}
}

func TestTracedNodePhaseOrder(t *testing.T) {
	db := newDB(t, 2)

	var phases []executor.TracePhase
	traced := executor.NewTracedNode(db.seqScan(t), "scan", func(e executor.TraceEvent) {
		phases = append(phases, e.Phase)
	})
	_, err := executor.Collect(traced)
	if err != nil {
		t.Fatal(err)
	}

	// Open must be first, Close must be last.
	if phases[0] != executor.TraceOpen {
		t.Errorf("first phase must be Open, got %v", phases[0])
	}
	if phases[len(phases)-1] != executor.TraceClose {
		t.Errorf("last phase must be Close, got %v", phases[len(phases)-1])
	}
	for i, p := range phases[1 : len(phases)-1] {
		if p != executor.TraceTuple {
			t.Errorf("phases[%d] = %v, want Tuple", i+1, p)
		}
	}
}

// ── Collect ───────────────────────────────────────────────────────────────────

func TestCollectEmpty(t *testing.T) {
	db := newDB(t, 0)
	tuples, err := executor.Collect(db.seqScan(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 0 {
		t.Fatalf("want 0 tuples from empty relation, got %d", len(tuples))
	}
}

// ── Payload integrity through the pipeline ────────────────────────────────────

func TestPayloadIntegrity(t *testing.T) {
	const n = 50
	db := newDB(t, n)

	tuples, err := executor.Collect(db.seqScan(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != n {
		t.Fatalf("want %d tuples, got %d", n, len(tuples))
	}

	// Verify that every encoded value 0..n-1 is present exactly once.
	seen := make(map[uint32]int)
	for _, st := range tuples {
		seen[tupleVal(st)]++
	}
	for i := uint32(0); i < n; i++ {
		if seen[i] != 1 {
			t.Errorf("value %d seen %d times (want 1)", i, seen[i])
		}
	}
}

// ── Filter by payload bytes ───────────────────────────────────────────────────

func TestFilterByPayload(t *testing.T) {
	const n = 30
	db := newDB(t, n)

	target := make([]byte, 4)
	binary.BigEndian.PutUint32(target, 15)

	f := executor.NewFilter(db.seqScan(t), func(st *executor.ScanTuple) bool {
		return bytes.Equal(st.Tuple.Data[:4], target)
	})

	tuples, err := executor.Collect(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 {
		t.Fatalf("want 1 tuple with value 15, got %d", len(tuples))
	}
	if tupleVal(tuples[0]) != 15 {
		t.Errorf("want value 15, got %d", tupleVal(tuples[0]))
	}
}

// ── NodeName in trace events ──────────────────────────────────────────────────

func TestTracedNodeNameInEvents(t *testing.T) {
	db := newDB(t, 1)

	var names []string
	traced := executor.NewTracedNode(db.seqScan(t), "MyScan", func(e executor.TraceEvent) {
		names = append(names, e.NodeName)
	})
	_, err := executor.Collect(traced)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if name != "MyScan" {
			t.Errorf("NodeName want MyScan, got %q", name)
		}
	}
}
