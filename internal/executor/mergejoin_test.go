package executor_test

// Tests for MergeJoin.
//
// Reuses the same row layouts from join_test.go:
//
//	Table A (outer): [0:4] id uint32 | [4:8] key uint32 | [8:12] val uint32
//	Table B (inner): [0:4] key uint32 | [4:8] label uint32
//
// Both sides must be inserted in sorted key order for MergeJoin to work
// correctly (it does not sort inputs itself).

import (
	"encoding/binary"
	"sort"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/executor"
	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// sortedJoinDB inserts n rows using encode, but pre-sorted by sortKey.
func sortedJoinDB(t *testing.T, relId uint32, n int,
	encode func(i int) []byte, sortKey func([]byte) uint32) *testDB {
	t.Helper()
	type row struct {
		key  uint32
		data []byte
	}
	rows := make([]row, n)
	for i := 0; i < n; i++ {
		d := encode(i)
		rows[i] = row{key: sortKey(d), data: d}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].key < rows[j].key })

	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(128)
	rel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: relId}, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}
	txmgr := mvcc.NewTransactionManager()
	xid := txmgr.Begin()
	for _, r := range rows {
		tup := storage.NewHeapTuple(xid, 3, r.data)
		if _, _, err := executor.HeapInsert(rel, tup); err != nil {
			t.Fatalf("HeapInsert: %v", err)
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

// outerKeyForMerge extracts key bytes [4:8] from an outer row.
func outerKeyForMerge(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, st.Tuple.Data[4:8])
	return b
}

// innerKeyForMerge extracts key bytes [0:4] from an inner row.
func innerKeyForMerge(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, st.Tuple.Data[0:4])
	return b
}

// ── MergeJoin tests ───────────────────────────────────────────────────────────

func TestMergeJoinBasic(t *testing.T) {
	// outer: 10 rows, keys 0..4 cycling twice (sorted by key → 0,0,1,1,2,2,3,3,4,4)
	// inner: 5 rows, keys 0..4 (sorted)
	// Each key has 2 outer × 1 inner = 2 matches → 10 total
	outer := sortedJoinDB(t, 50, 10, encodeA,
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[4:8]) })
	inner := sortedJoinDB(t, 51, 5, encodeB,
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[0:4]) })

	mj := executor.NewMergeJoin(outer.seqScan(t), inner.seqScan(t),
		outerKeyForMerge, innerKeyForMerge, combineFn)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Fatalf("want 10 results, got %d", len(rows))
	}
	for _, r := range rows {
		outKey := binary.BigEndian.Uint32(r.Tuple.Data[4:8])
		label := binary.BigEndian.Uint32(r.Tuple.Data[12:16])
		if label != outKey*100 {
			t.Errorf("key=%d: want label=%d, got %d", outKey, outKey*100, label)
		}
	}
}

func TestMergeJoinEmptyOuter(t *testing.T) {
	outer := sortedJoinDB(t, 52, 0, encodeA,
		func(d []byte) uint32 { return 0 })
	inner := sortedJoinDB(t, 53, 5, encodeB,
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[0:4]) })

	mj := executor.NewMergeJoin(outer.seqScan(t), inner.seqScan(t),
		outerKeyForMerge, innerKeyForMerge, combineFn)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results, got %d", len(rows))
	}
}

func TestMergeJoinEmptyInner(t *testing.T) {
	outer := sortedJoinDB(t, 54, 5, encodeA,
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[4:8]) })
	inner := sortedJoinDB(t, 55, 0, encodeB,
		func(d []byte) uint32 { return 0 })

	mj := executor.NewMergeJoin(outer.seqScan(t), inner.seqScan(t),
		outerKeyForMerge, innerKeyForMerge, combineFn)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results, got %d", len(rows))
	}
}

func TestMergeJoinNoOverlap(t *testing.T) {
	// outer keys 0..4, inner keys 10..14 → no match
	outer := sortedJoinDB(t, 56, 5, encodeA,
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[4:8]) })
	inner := sortedJoinDB(t, 57, 5,
		func(i int) []byte { return encodeB(i + 10) },
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[0:4]) })

	mj := executor.NewMergeJoin(outer.seqScan(t), inner.seqScan(t),
		outerKeyForMerge, innerKeyForMerge, combineFn)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results, got %d", len(rows))
	}
}

func TestMergeJoinManyToMany(t *testing.T) {
	// outer: 3 rows all with key=2
	// inner: 4 rows all with key=2
	// cross product = 12 rows
	outer := sortedJoinDB(t, 58, 3,
		func(i int) []byte {
			b := make([]byte, 12)
			binary.BigEndian.PutUint32(b[0:], uint32(i))
			binary.BigEndian.PutUint32(b[4:], 2) // key=2
			binary.BigEndian.PutUint32(b[8:], uint32(i))
			return b
		},
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[4:8]) })
	inner := sortedJoinDB(t, 59, 4,
		func(i int) []byte {
			b := make([]byte, 8)
			binary.BigEndian.PutUint32(b[0:], 2)          // key=2
			binary.BigEndian.PutUint32(b[4:], uint32(i+1)) // label 1..4
			return b
		},
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[0:4]) })

	mj := executor.NewMergeJoin(outer.seqScan(t), inner.seqScan(t),
		outerKeyForMerge, innerKeyForMerge, combineFn)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 12 {
		t.Fatalf("want 12 rows (3×4 cross product), got %d", len(rows))
	}
}

func TestMergeJoinGapInKeys(t *testing.T) {
	// outer keys: 1, 3, 5  (no 2 or 4)
	// inner keys: 1, 2, 3, 4, 5
	// Matches at 1, 3, 5 → 3 rows
	outer := sortedJoinDB(t, 60, 3,
		func(i int) []byte {
			b := make([]byte, 12)
			key := uint32(i*2 + 1) // 1, 3, 5
			binary.BigEndian.PutUint32(b[0:], key)
			binary.BigEndian.PutUint32(b[4:], key)
			binary.BigEndian.PutUint32(b[8:], uint32(i))
			return b
		},
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[4:8]) })
	inner := sortedJoinDB(t, 61, 5,
		func(i int) []byte {
			b := make([]byte, 8)
			binary.BigEndian.PutUint32(b[0:], uint32(i+1)) // 1..5
			binary.BigEndian.PutUint32(b[4:], uint32(i+1)*100)
			return b
		},
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[0:4]) })

	mj := executor.NewMergeJoin(outer.seqScan(t), inner.seqScan(t),
		outerKeyForMerge, innerKeyForMerge, combineFn)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 matches (keys 1,3,5), got %d", len(rows))
	}
}

// TestMergeJoinMatchesHashJoin verifies MergeJoin and HashJoin produce the
// same result set (sorted by id for comparison).
func TestMergeJoinMatchesHashJoin(t *testing.T) {
	const n = 20

	// Pre-sorted outer (by key) for MergeJoin
	mjOuter := sortedJoinDB(t, 62, n, encodeA,
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[4:8]) })
	mjInner := sortedJoinDB(t, 63, 5, encodeB,
		func(d []byte) uint32 { return binary.BigEndian.Uint32(d[0:4]) })

	// Unsorted for HashJoin (uses same data)
	hjOuter := joinDB(t, 64, n, encodeA)
	hjInner := joinDB(t, 65, 5, encodeB)

	mjRows, err := executor.Collect(executor.NewMergeJoin(
		mjOuter.seqScan(t), mjInner.seqScan(t),
		outerKeyForMerge, innerKeyForMerge, combineFn))
	if err != nil {
		t.Fatal(err)
	}
	hjRows, err := executor.Collect(executor.NewHashJoin(
		hjOuter.seqScan(t), hjInner.seqScan(t),
		innerKeyFn, outerKey, combineFn))
	if err != nil {
		t.Fatal(err)
	}

	if len(mjRows) != len(hjRows) {
		t.Fatalf("MJ=%d rows, HJ=%d rows", len(mjRows), len(hjRows))
	}

	byID := func(rows []*executor.ScanTuple) {
		sort.Slice(rows, func(i, j int) bool {
			return binary.BigEndian.Uint32(rows[i].Tuple.Data[0:4]) <
				binary.BigEndian.Uint32(rows[j].Tuple.Data[0:4])
		})
	}
	byID(mjRows)
	byID(hjRows)

	for i := range mjRows {
		mjID := binary.BigEndian.Uint32(mjRows[i].Tuple.Data[0:4])
		hjID := binary.BigEndian.Uint32(hjRows[i].Tuple.Data[0:4])
		mjLabel := binary.BigEndian.Uint32(mjRows[i].Tuple.Data[12:16])
		hjLabel := binary.BigEndian.Uint32(hjRows[i].Tuple.Data[12:16])
		if mjID != hjID || mjLabel != hjLabel {
			t.Errorf("row %d: MJ=(id=%d,label=%d) HJ=(id=%d,label=%d)",
				i, mjID, mjLabel, hjID, hjLabel)
		}
	}
}
