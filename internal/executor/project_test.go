package executor_test

// Tests for the Project node and Schema-based projection.

import (
	"encoding/binary"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/executor"
	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// newWideDB inserts n tuples whose payload is:
//
//	[0:4]  id     uint32 big-endian
//	[4:8]  value  uint32 big-endian   (id * 10)
//	[8:12] flags  uint32 big-endian   (id % 4)
//
// Returns (rel, txmgr, snap, cleanup).
func newWideDB(t *testing.T, n int) (*storage.Relation, *mvcc.TransactionManager, *storage.Snapshot, func()) {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(128)
	node := storage.RelFileNode{DbId: 0, RelId: 2}
	rel := storage.OpenRelation(node, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}

	txmgr := mvcc.NewTransactionManager()
	xid := txmgr.Begin()
	for i := 0; i < n; i++ {
		b := make([]byte, 12)
		binary.BigEndian.PutUint32(b[0:], uint32(i))
		binary.BigEndian.PutUint32(b[4:], uint32(i)*10)
		binary.BigEndian.PutUint32(b[8:], uint32(i)%4)
		tup := storage.NewHeapTuple(xid, 3, b)
		if _, _, err := executor.HeapInsert(rel, tup); err != nil {
			t.Fatalf("HeapInsert(%d): %v", i, err)
		}
	}
	if err := txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	reader := txmgr.Begin()
	snap := txmgr.Snapshot(reader)
	return rel, txmgr, snap, func() { smgr.Close() }
}

func u32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }

// ── Schema tests ──────────────────────────────────────────────────────────────

func TestSchemaWidth(t *testing.T) {
	s := executor.Schema{Cols: []executor.Column{
		{Name: "id", Offset: 0, Len: 4},
		{Name: "value", Offset: 4, Len: 4},
	}}
	if w := s.Width(); w != 8 {
		t.Fatalf("want width 8, got %d", w)
	}
}

func TestSchemaProjectExtracts(t *testing.T) {
	rel, txmgr, snap, cleanup := newWideDB(t, 5)
	defer cleanup()

	ss, err := executor.NewSeqScan(rel, snap, txmgr)
	if err != nil {
		t.Fatal(err)
	}

	// Project: extract only [id, flags] (skip value).
	s := executor.Schema{Cols: []executor.Column{
		{Name: "id", Offset: 0, Len: 4},
		{Name: "flags", Offset: 8, Len: 4},
	}}

	proj := executor.NewProject(ss, s.ProjectFnFrom())
	tuples, err := executor.Collect(proj)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 5 {
		t.Fatalf("want 5 projected tuples, got %d", len(tuples))
	}

	// Each projected tuple's Data should be 8 bytes: id(4) + flags(4).
	for _, st := range tuples {
		if len(st.Tuple.Data) != 8 {
			t.Errorf("projected data len want 8, got %d", len(st.Tuple.Data))
		}
		id := u32(s.Get("id", st))
		flags := u32(s.Get("flags", st))
		if flags != id%4 {
			t.Errorf("id=%d: flags want %d, got %d", id, id%4, flags)
		}
	}
}

func TestSchemaGetUnknownColumn(t *testing.T) {
	s := executor.Schema{Cols: []executor.Column{
		{Name: "id", Offset: 0, Len: 4},
	}}
	rel, txmgr, snap, cleanup := newWideDB(t, 1)
	defer cleanup()

	ss, err := executor.NewSeqScan(rel, snap, txmgr)
	if err != nil {
		t.Fatal(err)
	}
	proj := executor.NewProject(ss, s.ProjectFnFrom())
	tuples, err := executor.Collect(proj)
	if err != nil {
		t.Fatal(err)
	}

	if got := s.Get("nonexistent", tuples[0]); got != nil {
		t.Errorf("Get unknown column should return nil, got %v", got)
	}
}

// ── Project node tests ────────────────────────────────────────────────────────

func TestProjectFnTransform(t *testing.T) {
	rel, txmgr, snap, cleanup := newWideDB(t, 10)
	defer cleanup()

	ss, err := executor.NewSeqScan(rel, snap, txmgr)
	if err != nil {
		t.Fatal(err)
	}

	// Double the value field: read [4:8], multiply by 2, write back.
	proj := executor.NewProject(ss, func(st *executor.ScanTuple) (*executor.ScanTuple, error) {
		d := st.Tuple.Data
		if len(d) < 8 {
			return nil, nil
		}
		out := make([]byte, 8)
		copy(out[0:4], d[0:4]) // id unchanged
		v := u32(d[4:8]) * 2
		binary.BigEndian.PutUint32(out[4:], v)
		result := *st.Tuple
		result.Data = out
		return &executor.ScanTuple{Tuple: &result, Block: st.Block, Offset: st.Offset}, nil
	})

	tuples, err := executor.Collect(proj)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 10 {
		t.Fatalf("want 10 tuples, got %d", len(tuples))
	}
	for _, st := range tuples {
		id := u32(st.Tuple.Data[0:4])
		doubled := u32(st.Tuple.Data[4:8])
		if doubled != id*20 {
			t.Errorf("id=%d: doubled value want %d, got %d", id, id*20, doubled)
		}
	}
}

func TestProjectDropsNilTuples(t *testing.T) {
	rel, txmgr, snap, cleanup := newWideDB(t, 6)
	defer cleanup()

	ss, err := executor.NewSeqScan(rel, snap, txmgr)
	if err != nil {
		t.Fatal(err)
	}

	// Return nil for odd ids - acts as a second filter.
	proj := executor.NewProject(ss, func(st *executor.ScanTuple) (*executor.ScanTuple, error) {
		id := u32(st.Tuple.Data[0:4])
		if id%2 != 0 {
			return nil, nil // drop
		}
		return st, nil
	})

	tuples, err := executor.Collect(proj)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 3 {
		t.Fatalf("want 3 tuples (even ids), got %d", len(tuples))
	}
	for _, st := range tuples {
		if id := u32(st.Tuple.Data[0:4]); id%2 != 0 {
			t.Errorf("unexpected odd id %d", id)
		}
	}
}

func TestProjectPreservesLocation(t *testing.T) {
	rel, txmgr, snap, cleanup := newWideDB(t, 3)
	defer cleanup()

	ss, err := executor.NewSeqScan(rel, snap, txmgr)
	if err != nil {
		t.Fatal(err)
	}

	// Record source locations before projection.
	rawSS, err := executor.NewSeqScan(rel, snap, txmgr)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := executor.Collect(rawSS)
	if err != nil {
		t.Fatal(err)
	}

	// Project: keep only id column.
	s := executor.Schema{Cols: []executor.Column{{Name: "id", Offset: 0, Len: 4}}}
	proj := executor.NewProject(ss, s.ProjectFnFrom())
	projected, err := executor.Collect(proj)
	if err != nil {
		t.Fatal(err)
	}

	if len(raw) != len(projected) {
		t.Fatalf("count mismatch: raw=%d projected=%d", len(raw), len(projected))
	}
	for i := range raw {
		if raw[i].Block != projected[i].Block || raw[i].Offset != projected[i].Offset {
			t.Errorf("tuple %d: location raw=(%d,%d) projected=(%d,%d)",
				i, raw[i].Block, raw[i].Offset, projected[i].Block, projected[i].Offset)
		}
	}
}

// ── Full pipeline: SeqScan → Filter → Project → Limit ────────────────────────

func TestFullPipeline(t *testing.T) {
	// 20 rows; keep rows where flags==0 (ids: 0,4,8,12,16); project id+value; limit 3.
	rel, txmgr, snap, cleanup := newWideDB(t, 20)
	defer cleanup()

	ss, err := executor.NewSeqScan(rel, snap, txmgr)
	if err != nil {
		t.Fatal(err)
	}

	s := executor.Schema{Cols: []executor.Column{
		{Name: "id", Offset: 0, Len: 4},
		{Name: "value", Offset: 4, Len: 4},
	}}

	pipeline := executor.NewLimit(
		executor.NewProject(
			executor.NewFilter(ss, func(st *executor.ScanTuple) bool {
				return u32(st.Tuple.Data[8:12]) == 0 // flags == 0
			}),
			s.ProjectFnFrom(),
		),
		3,
	)

	tuples, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 3 {
		t.Fatalf("want 3 tuples, got %d", len(tuples))
	}
	for _, st := range tuples {
		id := u32(s.Get("id", st))
		value := u32(s.Get("value", st))
		if id%4 != 0 {
			t.Errorf("expected id divisible by 4, got %d", id)
		}
		if value != id*10 {
			t.Errorf("id=%d: value want %d, got %d", id, id*10, value)
		}
		// flags column should not be present after projection.
		if len(st.Tuple.Data) != 8 {
			t.Errorf("projected data should be 8 bytes, got %d", len(st.Tuple.Data))
		}
	}
}
