package executor_test

// Tests for NestedLoopJoin and HashJoin.
//
// Row layouts used by these tests:
//
//	Table A (outer): [0:4] id uint32 | [4:8] key uint32 | [8:12] val uint32
//	Table B (inner): [0:4] key uint32 | [4:8] label uint32
//
// Output tuple: [0:4] id | [4:8] key | [8:12] val | [12:16] label  (16 bytes)

import (
	"encoding/binary"
	"sort"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/executor"
	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── fixture ───────────────────────────────────────────────────────────────────

// joinDB inserts n rows into a relation using the provided encoder.
func joinDB(t *testing.T, relId uint32, n int, encode func(i int) []byte) *testDB {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(128)
	rel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: relId}, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}
	txmgr := mvcc.NewTransactionManager()
	xid := txmgr.Begin()
	for i := 0; i < n; i++ {
		tup := storage.NewHeapTuple(xid, 3, encode(i))
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

// encodeA encodes an "outer" row: id=i, key=i%5, val=i*10
func encodeA(i int) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b[0:], uint32(i))
	binary.BigEndian.PutUint32(b[4:], uint32(i)%5)
	binary.BigEndian.PutUint32(b[8:], uint32(i)*10)
	return b
}

// encodeB encodes an "inner" row: key=i, label=i*100
func encodeB(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:], uint32(i))
	binary.BigEndian.PutUint32(b[4:], uint32(i)*100)
	return b
}

// outerKey extracts key from outer row (bytes 4:8).
func outerKey(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, st.Tuple.Data[4:8])
	return b
}

// innerKey extracts key from inner row (bytes 0:4).
func innerKeyFn(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, st.Tuple.Data[0:4])
	return b
}

// joinPred returns true when outer.key == inner.key.
func joinPred(o, i *executor.ScanTuple) bool {
	return binary.BigEndian.Uint32(o.Tuple.Data[4:8]) ==
		binary.BigEndian.Uint32(i.Tuple.Data[0:4])
}

// combineFn appends outer[id, key, val] with inner[label] → 16 bytes.
func combineFn(o, i *executor.ScanTuple) *executor.ScanTuple {
	data := make([]byte, 16)
	copy(data[0:12], o.Tuple.Data[0:12]) // id, key, val
	copy(data[12:16], i.Tuple.Data[4:8]) // label
	tup := storage.NewHeapTuple(storage.FrozenTransactionId, 0, data)
	return &executor.ScanTuple{Tuple: tup}
}

// ── NestedLoopJoin tests ──────────────────────────────────────────────────────

func TestNestedLoopJoinBasic(t *testing.T) {
	// outer: 10 rows (keys 0..4 cycling twice)
	// inner: 5 rows  (keys 0..4)
	// Each outer key matches exactly one inner row → 10 output rows.
	outer := joinDB(t, 20, 10, encodeA)
	inner := joinDB(t, 21, 5, encodeB)

	nl := executor.NewNestedLoopJoin(outer.seqScan(t), inner.seqScan(t), joinPred, combineFn)
	rows, err := executor.Collect(nl)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Fatalf("want 10 join results, got %d", len(rows))
	}
	for _, r := range rows {
		outKey := binary.BigEndian.Uint32(r.Tuple.Data[4:8])
		label := binary.BigEndian.Uint32(r.Tuple.Data[12:16])
		if label != outKey*100 {
			t.Errorf("key=%d: want label=%d, got %d", outKey, outKey*100, label)
		}
	}
}

func TestNestedLoopJoinEmptyInner(t *testing.T) {
	outer := joinDB(t, 22, 5, encodeA)
	inner := joinDB(t, 23, 0, encodeB)

	nl := executor.NewNestedLoopJoin(outer.seqScan(t), inner.seqScan(t), joinPred, combineFn)
	rows, err := executor.Collect(nl)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results with empty inner, got %d", len(rows))
	}
}

func TestNestedLoopJoinEmptyOuter(t *testing.T) {
	outer := joinDB(t, 24, 0, encodeA)
	inner := joinDB(t, 25, 5, encodeB)

	nl := executor.NewNestedLoopJoin(outer.seqScan(t), inner.seqScan(t), joinPred, combineFn)
	rows, err := executor.Collect(nl)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results with empty outer, got %d", len(rows))
	}
}

func TestNestedLoopJoinNoMatch(t *testing.T) {
	// outer keys 0..4, inner keys 10..14 → no matches
	outer := joinDB(t, 26, 5, encodeA)
	inner := joinDB(t, 27, 5, func(i int) []byte { return encodeB(i + 10) })

	nl := executor.NewNestedLoopJoin(outer.seqScan(t), inner.seqScan(t), joinPred, combineFn)
	rows, err := executor.Collect(nl)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results, got %d", len(rows))
	}
}

func TestNestedLoopJoinOneToMany(t *testing.T) {
	// outer: 1 row with key=0
	// inner: 3 rows with key=0 → 3 output rows
	outer := joinDB(t, 28, 1, encodeA) // id=0, key=0
	inner := joinDB(t, 29, 3, func(i int) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint32(b[0:], 0)          // key=0 always
		binary.BigEndian.PutUint32(b[4:], uint32(i+1)) // label 1,2,3
		return b
	})

	nl := executor.NewNestedLoopJoin(outer.seqScan(t), inner.seqScan(t), joinPred, combineFn)
	rows, err := executor.Collect(nl)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 rows (one-to-many), got %d", len(rows))
	}
}

// ── HashJoin tests ────────────────────────────────────────────────────────────

func TestHashJoinBasic(t *testing.T) {
	// Same scenario as TestNestedLoopJoinBasic - should produce identical results.
	outer := joinDB(t, 30, 10, encodeA)
	inner := joinDB(t, 31, 5, encodeB)

	hj := executor.NewHashJoin(outer.seqScan(t), inner.seqScan(t),
		innerKeyFn, outerKey, combineFn)
	rows, err := executor.Collect(hj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Fatalf("want 10 join results, got %d", len(rows))
	}
	for _, r := range rows {
		outKey := binary.BigEndian.Uint32(r.Tuple.Data[4:8])
		label := binary.BigEndian.Uint32(r.Tuple.Data[12:16])
		if label != outKey*100 {
			t.Errorf("key=%d: want label=%d, got %d", outKey, outKey*100, label)
		}
	}
}

func TestHashJoinEmptyInner(t *testing.T) {
	outer := joinDB(t, 32, 5, encodeA)
	inner := joinDB(t, 33, 0, encodeB)

	hj := executor.NewHashJoin(outer.seqScan(t), inner.seqScan(t),
		innerKeyFn, outerKey, combineFn)
	rows, err := executor.Collect(hj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results, got %d", len(rows))
	}
}

func TestHashJoinEmptyOuter(t *testing.T) {
	outer := joinDB(t, 34, 0, encodeA)
	inner := joinDB(t, 35, 5, encodeB)

	hj := executor.NewHashJoin(outer.seqScan(t), inner.seqScan(t),
		innerKeyFn, outerKey, combineFn)
	rows, err := executor.Collect(hj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results, got %d", len(rows))
	}
}

func TestHashJoinNoMatch(t *testing.T) {
	outer := joinDB(t, 36, 5, encodeA)
	inner := joinDB(t, 37, 5, func(i int) []byte { return encodeB(i + 10) })

	hj := executor.NewHashJoin(outer.seqScan(t), inner.seqScan(t),
		innerKeyFn, outerKey, combineFn)
	rows, err := executor.Collect(hj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 results, got %d", len(rows))
	}
}

func TestHashJoinOneToMany(t *testing.T) {
	outer := joinDB(t, 38, 1, encodeA)
	inner := joinDB(t, 39, 3, func(i int) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint32(b[0:], 0)
		binary.BigEndian.PutUint32(b[4:], uint32(i+1))
		return b
	})

	hj := executor.NewHashJoin(outer.seqScan(t), inner.seqScan(t),
		innerKeyFn, outerKey, combineFn)
	rows, err := executor.Collect(hj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// TestHashJoinMatchesNestedLoop verifies that both strategies produce the same
// row count and the same key/label pairs (order may differ).
func TestHashJoinMatchesNestedLoop(t *testing.T) {
	const n = 20
	outerNL := joinDB(t, 40, n, encodeA)
	innerNL := joinDB(t, 41, 5, encodeB)
	outerHJ := joinDB(t, 42, n, encodeA)
	innerHJ := joinDB(t, 43, 5, encodeB)

	nlRows, err := executor.Collect(executor.NewNestedLoopJoin(
		outerNL.seqScan(t), innerNL.seqScan(t), joinPred, combineFn))
	if err != nil {
		t.Fatal(err)
	}
	hjRows, err := executor.Collect(executor.NewHashJoin(
		outerHJ.seqScan(t), innerHJ.seqScan(t), innerKeyFn, outerKey, combineFn))
	if err != nil {
		t.Fatal(err)
	}

	if len(nlRows) != len(hjRows) {
		t.Fatalf("NL produced %d rows, HJ produced %d rows", len(nlRows), len(hjRows))
	}

	// Sort both by id for a stable comparison.
	byID := func(rows []*executor.ScanTuple) {
		sort.Slice(rows, func(i, j int) bool {
			return binary.BigEndian.Uint32(rows[i].Tuple.Data[0:4]) <
				binary.BigEndian.Uint32(rows[j].Tuple.Data[0:4])
		})
	}
	byID(nlRows)
	byID(hjRows)

	for i := range nlRows {
		nlID := binary.BigEndian.Uint32(nlRows[i].Tuple.Data[0:4])
		hjID := binary.BigEndian.Uint32(hjRows[i].Tuple.Data[0:4])
		nlLabel := binary.BigEndian.Uint32(nlRows[i].Tuple.Data[12:16])
		hjLabel := binary.BigEndian.Uint32(hjRows[i].Tuple.Data[12:16])
		if nlID != hjID || nlLabel != hjLabel {
			t.Errorf("row %d: NL=(id=%d,label=%d) HJ=(id=%d,label=%d)",
				i, nlID, nlLabel, hjID, hjLabel)
		}
	}
}
