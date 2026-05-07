package executor_test

// Tests for Sort and HashAgg executor nodes.
//
// Rows have a 12-byte payload:
//   [0:4]  category  uint32 (i % 5)
//   [4:8]  score     uint32 (i * 3)
//   [8:12] id        uint32 (i)

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/executor"
	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// aggDB inserts n wide rows (category/score/id) under one committed tx.
// Returns a *testDB whose seqScan() yields those rows.
func aggDB(t *testing.T, n int) *testDB {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(128)
	rel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: 10}, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}

	txmgr := mvcc.NewTransactionManager()
	xid := txmgr.Begin()
	for i := 0; i < n; i++ {
		b := make([]byte, 12)
		binary.BigEndian.PutUint32(b[0:], uint32(i)%5) // category
		binary.BigEndian.PutUint32(b[4:], uint32(i)*3) // score
		binary.BigEndian.PutUint32(b[8:], uint32(i))   // id
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
	t.Cleanup(func() { smgr.Close() })
	return &testDB{rel: rel, txmgr: txmgr, snap: snap}
}

// ── Sort tests ────────────────────────────────────────────────────────────────

func TestSortByScore(t *testing.T) {
	db := aggDB(t, 20)
	sorted := executor.NewSort(db.seqScan(t), func(a, b *executor.ScanTuple) bool {
		return binary.BigEndian.Uint32(a.Tuple.Data[4:8]) <
			binary.BigEndian.Uint32(b.Tuple.Data[4:8])
	})

	tuples, err := executor.Collect(sorted)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 20 {
		t.Fatalf("want 20 tuples, got %d", len(tuples))
	}
	for i := 1; i < len(tuples); i++ {
		prev := binary.BigEndian.Uint32(tuples[i-1].Tuple.Data[4:8])
		cur := binary.BigEndian.Uint32(tuples[i].Tuple.Data[4:8])
		if prev > cur {
			t.Errorf("out of order at %d: score=%d > score=%d", i, prev, cur)
		}
	}
}

func TestSortEmpty(t *testing.T) {
	db := aggDB(t, 0)
	sorted := executor.NewSort(db.seqScan(t), func(a, b *executor.ScanTuple) bool {
		return bytes.Compare(a.Tuple.Data, b.Tuple.Data) < 0
	})
	tuples, err := executor.Collect(sorted)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 0 {
		t.Fatalf("want 0 tuples, got %d", len(tuples))
	}
}

func TestSortStable(t *testing.T) {
	// 10 rows; sort by category (each category appears twice).
	// Stable sort must preserve insertion order (= id order) within a category.
	db := aggDB(t, 10)
	sorted, err := executor.Collect(executor.NewSort(db.seqScan(t),
		func(a, b *executor.ScanTuple) bool {
			return binary.BigEndian.Uint32(a.Tuple.Data[0:4]) <
				binary.BigEndian.Uint32(b.Tuple.Data[0:4])
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(sorted) != 10 {
		t.Fatalf("want 10 tuples, got %d", len(sorted))
	}

	// Within each category ids must be increasing (insertion order preserved).
	prev := make(map[uint32]uint32)
	for _, st := range sorted {
		cat := binary.BigEndian.Uint32(st.Tuple.Data[0:4])
		id := binary.BigEndian.Uint32(st.Tuple.Data[8:12])
		if last, seen := prev[cat]; seen && id < last {
			t.Errorf("category %d: id went backward %d → %d", cat, last, id)
		}
		prev[cat] = id
	}
}

// ── HashAgg tests ─────────────────────────────────────────────────────────────

func catKey(t *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, t.Tuple.Data[0:4])
	return b
}

func scoreVal(t *executor.ScanTuple) int64 {
	return int64(binary.BigEndian.Uint32(t.Tuple.Data[4:8]))
}

func TestHashAggCount(t *testing.T) {
	// 20 rows, 5 categories → each category has 4 rows.
	db := aggDB(t, 20)
	ha := executor.NewHashAgg(db.seqScan(t), catKey, func() []executor.AggFn {
		return []executor.AggFn{executor.NewCountAgg()}
	})

	groups, err := executor.Collect(ha)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 5 {
		t.Fatalf("want 5 groups, got %d", len(groups))
	}
	for _, g := range groups {
		cnt := executor.DecodeCount(g.Tuple.Data[4:12])
		if cnt != 4 {
			cat := binary.BigEndian.Uint32(g.Tuple.Data[0:4])
			t.Errorf("category %d: want count 4, got %d", cat, cnt)
		}
	}
}

func TestHashAggSum(t *testing.T) {
	// 10 rows: category c gets rows with id=c and id=c+5.
	// score(i) = i*3  →  sum for category c = c*3 + (c+5)*3 = (2c+5)*3
	db := aggDB(t, 10)
	ha := executor.NewHashAgg(db.seqScan(t), catKey, func() []executor.AggFn {
		return []executor.AggFn{executor.NewSumAgg(scoreVal)}
	})

	groups, err := executor.Collect(ha)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 5 {
		t.Fatalf("want 5 groups, got %d", len(groups))
	}
	sums := make(map[uint32]int64)
	for _, g := range groups {
		cat := binary.BigEndian.Uint32(g.Tuple.Data[0:4])
		sums[cat] = executor.DecodeSum(g.Tuple.Data[4:12])
	}
	for c := uint32(0); c < 5; c++ {
		want := int64((2*c+5) * 3)
		if sums[c] != want {
			t.Errorf("category %d: sum want %d, got %d", c, want, sums[c])
		}
	}
}

func TestHashAggMinMax(t *testing.T) {
	// 10 rows. Category 0: ids 0 (score=0) and 5 (score=15).
	db := aggDB(t, 10)
	ha := executor.NewHashAgg(db.seqScan(t), catKey, func() []executor.AggFn {
		return []executor.AggFn{
			executor.NewMinAgg(scoreVal),
			executor.NewMaxAgg(scoreVal),
		}
	})

	groups, err := executor.Collect(ha)
	if err != nil {
		t.Fatal(err)
	}
	type result struct{ min, max int64 }
	seen := make(map[uint32]result)
	for _, g := range groups {
		cat := binary.BigEndian.Uint32(g.Tuple.Data[0:4])
		min := executor.DecodeMin(g.Tuple.Data[4:12])
		max := executor.DecodeMax(g.Tuple.Data[12:20])
		seen[cat] = result{min, max}
	}
	// category 0: rows 0 (score=0) and 5 (score=15)
	if r := seen[0]; r.min != 0 || r.max != 15 {
		t.Errorf("cat 0: min=%d max=%d want 0,15", r.min, r.max)
	}
	// category 1: rows 1 (score=3) and 6 (score=18)
	if r := seen[1]; r.min != 3 || r.max != 18 {
		t.Errorf("cat 1: min=%d max=%d want 3,18", r.min, r.max)
	}
}

func TestHashAggSingleGroup(t *testing.T) {
	db := aggDB(t, 8)
	constKey := func(*executor.ScanTuple) []byte { return []byte{0} }
	ha := executor.NewHashAgg(db.seqScan(t), constKey, func() []executor.AggFn {
		return []executor.AggFn{executor.NewCountAgg()}
	})
	groups, err := executor.Collect(ha)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 {
		t.Fatalf("want 1 group, got %d", len(groups))
	}
	// Data layout: key(1 byte) + count(8 bytes)
	cnt := executor.DecodeCount(groups[0].Tuple.Data[1:9])
	if cnt != 8 {
		t.Errorf("want count 8, got %d", cnt)
	}
}

func TestHashAggEmpty(t *testing.T) {
	db := aggDB(t, 0)
	ha := executor.NewHashAgg(db.seqScan(t), catKey, func() []executor.AggFn {
		return []executor.AggFn{executor.NewCountAgg()}
	})
	groups, err := executor.Collect(ha)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 0 {
		t.Fatalf("want 0 groups, got %d", len(groups))
	}
}

// ── Chained pipeline: SeqScan → HashAgg → Sort ───────────────────────────────

func TestGroupByThenOrderBy(t *testing.T) {
	// 25 rows, 5 categories (each has 5 rows).
	// GROUP BY category, COUNT(*), SUM(score) ORDER BY category ASC.
	db := aggDB(t, 25)

	ha := executor.NewHashAgg(db.seqScan(t), catKey, func() []executor.AggFn {
		return []executor.AggFn{
			executor.NewCountAgg(),
			executor.NewSumAgg(scoreVal),
		}
	})
	sorted := executor.NewSort(ha, func(a, b *executor.ScanTuple) bool {
		return binary.BigEndian.Uint32(a.Tuple.Data[0:4]) <
			binary.BigEndian.Uint32(b.Tuple.Data[0:4])
	})

	groups, err := executor.Collect(sorted)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 5 {
		t.Fatalf("want 5 groups, got %d", len(groups))
	}
	for i := 1; i < len(groups); i++ {
		prev := binary.BigEndian.Uint32(groups[i-1].Tuple.Data[0:4])
		cur := binary.BigEndian.Uint32(groups[i].Tuple.Data[0:4])
		if prev > cur {
			t.Errorf("groups not sorted: cat[%d]=%d > cat[%d]=%d", i-1, prev, i, cur)
		}
	}
	for _, g := range groups {
		cnt := executor.DecodeCount(g.Tuple.Data[4:12])
		if cnt != 5 {
			cat := binary.BigEndian.Uint32(g.Tuple.Data[0:4])
			t.Errorf("cat %d: want count 5, got %d", cat, cnt)
		}
	}
}
