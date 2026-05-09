package mergejoin_test

// Merge-join experiment.
//
// Models the execution of:
//
//	SELECT e.name_id, d.budget, e.salary
//	FROM   employees e
//	JOIN   departments d ON e.dept_id = d.dept_id
//	ORDER  BY e.dept_id
//
// MergeJoin requires both sides pre-sorted on the join key, so the pipeline is:
//
//	Sort(SeqScan(employees), dept_id ASC)
//	  → MergeJoin(Sort(SeqScan(departments), dept_id ASC))
//	    → Sort(ORDER BY dept_id ASC)   ← already sorted, but verified
//
// The experiment also:
//   - Shows all three strategies (NL, Hash, Merge) producing identical results.
//   - Exercises the many-to-many group property: multiple employees per dept.
//
// PostgreSQL equivalent: nodeMergejoin.c (ExecMergeJoin).
//
// Row layouts:
//
//	employees:   [0:4] emp_id uint32 | [4:8] dept_id uint32 | [8:16] salary int64
//	departments: [0:4] dept_id uint32 | [4:8] budget uint32
//	output:      [0:4] emp_id | [4:8] dept_id | [8:16] salary | [16:20] budget

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/executor"
	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── Row encoding / decoding ───────────────────────────────────────────────────

func encodeEmployee(empID, deptID uint32, salary int64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint32(b[0:], empID)
	binary.BigEndian.PutUint32(b[4:], deptID)
	binary.BigEndian.PutUint64(b[8:], uint64(salary))
	return b
}

func encodeDepartment(deptID, budget uint32) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:], deptID)
	binary.BigEndian.PutUint32(b[4:], budget)
	return b
}

func empID(st *executor.ScanTuple) uint32 {
	return binary.BigEndian.Uint32(st.Tuple.Data[0:4])
}
func deptID(st *executor.ScanTuple) uint32 {
	return binary.BigEndian.Uint32(st.Tuple.Data[4:8])
}
func salary(st *executor.ScanTuple) int64 {
	return int64(binary.BigEndian.Uint64(st.Tuple.Data[8:16]))
}
func budget(st *executor.ScanTuple) uint32 {
	return binary.BigEndian.Uint32(st.Tuple.Data[16:20])
}

func empDeptKey(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, st.Tuple.Data[4:8]) // dept_id from employees
	return b
}

func deptKey(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, st.Tuple.Data[0:4]) // dept_id from departments
	return b
}

// combine produces a 20-byte output row.
func combine(e, d *executor.ScanTuple) *executor.ScanTuple {
	data := make([]byte, 20)
	copy(data[0:16], e.Tuple.Data[0:16])  // emp_id, dept_id, salary
	copy(data[16:20], d.Tuple.Data[4:8])  // budget
	tup := storage.NewHeapTuple(storage.FrozenTransactionId, 0, data)
	return &executor.ScanTuple{Tuple: tup}
}

// nlPred for NestedLoopJoin
func nlPred(e, d *executor.ScanTuple) bool {
	return deptID(e) == binary.BigEndian.Uint32(d.Tuple.Data[0:4])
}

// ── Fixture ───────────────────────────────────────────────────────────────────

type companyDB struct {
	empRel  *storage.Relation
	deptRel *storage.Relation
	txmgr   *mvcc.TransactionManager
	snap    *storage.Snapshot
	smgr    storage.StorageManager
}

const (
	nDepts = 5
	empPerDept = 4 // each dept has exactly 4 employees
)

// newCompanyDB creates:
//   - 5 departments (IDs 0..4, budget = (id+1)*10_000)
//   - nDepts*empPerDept employees (dept_id = i%nDepts, salary = (i+1)*1_000)
func newCompanyDB(t *testing.T) (*companyDB, func()) {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(512)

	empRel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: 1}, pool, smgr)
	if err := empRel.Init(); err != nil {
		t.Fatalf("empRel.Init: %v", err)
	}
	deptRel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: 2}, pool, smgr)
	if err := deptRel.Init(); err != nil {
		t.Fatalf("deptRel.Init: %v", err)
	}

	txmgr := mvcc.NewTransactionManager()
	xid := txmgr.Begin()

	for i := 0; i < nDepts; i++ {
		tup := storage.NewHeapTuple(xid, 3, encodeDepartment(uint32(i), uint32(i+1)*10_000))
		if _, _, err := executor.HeapInsert(deptRel, tup); err != nil {
			t.Fatalf("dept HeapInsert: %v", err)
		}
	}
	nEmployees := nDepts * empPerDept
	for i := 0; i < nEmployees; i++ {
		sal := int64(i+1) * 1_000
		tup := storage.NewHeapTuple(xid, 3, encodeEmployee(uint32(i), uint32(i)%nDepts, sal))
		if _, _, err := executor.HeapInsert(empRel, tup); err != nil {
			t.Fatalf("emp HeapInsert: %v", err)
		}
	}
	if err := txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	reader := txmgr.Begin()
	snap := txmgr.Snapshot(reader)
	return &companyDB{empRel: empRel, deptRel: deptRel, txmgr: txmgr, snap: snap, smgr: smgr},
		func() { smgr.Close() }
}

func (db *companyDB) empScan(t *testing.T) *executor.SeqScan {
	t.Helper()
	ss, err := executor.NewSeqScan(db.empRel, db.snap, db.txmgr)
	if err != nil {
		t.Fatalf("empScan: %v", err)
	}
	return ss
}

func (db *companyDB) deptScan(t *testing.T) *executor.SeqScan {
	t.Helper()
	ss, err := executor.NewSeqScan(db.deptRel, db.snap, db.txmgr)
	if err != nil {
		t.Fatalf("deptScan: %v", err)
	}
	return ss
}

// sortByDeptID wraps a node with a Sort on dept_id ASC.
// This is the pre-sort MergeJoin requires for its inputs.
func sortByDeptID(n executor.Node, keyOffset int) *executor.Sort {
	return executor.NewSort(n, func(a, b *executor.ScanTuple) bool {
		ka := binary.BigEndian.Uint32(a.Tuple.Data[keyOffset : keyOffset+4])
		kb := binary.BigEndian.Uint32(b.Tuple.Data[keyOffset : keyOffset+4])
		return ka < kb
	})
}

// ── Helper ────────────────────────────────────────────────────────────────────

func printResults(t *testing.T, label string, rows []*executor.ScanTuple) {
	t.Helper()
	t.Logf("\n=== %s (%d rows) ===", label, len(rows))
	t.Logf("%-8s %-8s %-10s %-10s", "emp_id", "dept_id", "salary", "budget")
	t.Logf("%s", strings.Repeat("─", 40))
	for _, r := range rows {
		t.Logf("%-8d %-8d %-10d %-10d",
			empID(r), deptID(r), salary(r), budget(r))
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestMergeJoinBasicCorrectness verifies:
//   - Every output row has the correct budget for its dept_id
//   - Total rows = nDepts * empPerDept
//   - All 5 departments appear
func TestMergeJoinBasicCorrectness(t *testing.T) {
	db, cleanup := newCompanyDB(t)
	defer cleanup()

	// MergeJoin requires sorted inputs.
	sortedEmps := sortByDeptID(db.empScan(t), 4)  // sort employees by dept_id
	sortedDepts := sortByDeptID(db.deptScan(t), 0) // sort depts by dept_id (already sorted here)

	mj := executor.NewMergeJoin(sortedEmps, sortedDepts, empDeptKey, deptKey, combine)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}

	printResults(t, "MergeJoin", rows)

	if len(rows) != nDepts*empPerDept {
		t.Fatalf("want %d rows, got %d", nDepts*empPerDept, len(rows))
	}

	deptsSeen := make(map[uint32]int)
	for _, r := range rows {
		d := deptID(r)
		b := budget(r)
		wantBudget := (d + 1) * 10_000
		if b != wantBudget {
			t.Errorf("emp_id=%d dept_id=%d: want budget=%d, got %d",
				empID(r), d, wantBudget, b)
		}
		deptsSeen[d]++
	}

	for d := uint32(0); d < nDepts; d++ {
		if deptsSeen[d] != empPerDept {
			t.Errorf("dept %d: want %d rows, got %d", d, empPerDept, deptsSeen[d])
		}
	}
}

// TestAllThreeStrategiesAgree runs the same logical join through all three
// strategies and verifies they produce identical results.
func TestAllThreeStrategiesAgree(t *testing.T) {
	db, cleanup := newCompanyDB(t)
	defer cleanup()

	// NestedLoopJoin (no sorting required)
	nlRows, err := executor.Collect(executor.NewNestedLoopJoin(
		db.empScan(t), db.deptScan(t), nlPred, combine))
	if err != nil {
		t.Fatal(err)
	}

	// HashJoin (no sorting required)
	hjRows, err := executor.Collect(executor.NewHashJoin(
		db.empScan(t), db.deptScan(t),
		deptKey, empDeptKey, combine))
	if err != nil {
		t.Fatal(err)
	}

	// MergeJoin (inputs must be sorted first)
	mjRows, err := executor.Collect(executor.NewMergeJoin(
		sortByDeptID(db.empScan(t), 4),
		sortByDeptID(db.deptScan(t), 0),
		empDeptKey, deptKey, combine))
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("NL=%d  HJ=%d  MJ=%d rows", len(nlRows), len(hjRows), len(mjRows))

	if len(nlRows) != len(hjRows) || len(hjRows) != len(mjRows) {
		t.Fatalf("row counts differ: NL=%d HJ=%d MJ=%d", len(nlRows), len(hjRows), len(mjRows))
	}

	// Normalize: sort all by emp_id for comparison.
	byEmpID := func(rows []*executor.ScanTuple) {
		sort.Slice(rows, func(i, j int) bool {
			return empID(rows[i]) < empID(rows[j])
		})
	}
	byEmpID(nlRows)
	byEmpID(hjRows)
	byEmpID(mjRows)

	for i := range nlRows {
		nlE, hjE, mjE := empID(nlRows[i]), empID(hjRows[i]), empID(mjRows[i])
		nlB, hjB, mjB := budget(nlRows[i]), budget(hjRows[i]), budget(mjRows[i])
		if nlE != hjE || hjE != mjE {
			t.Errorf("row %d: emp_id NL=%d HJ=%d MJ=%d", i, nlE, hjE, mjE)
		}
		if nlB != hjB || hjB != mjB {
			t.Errorf("row %d: budget NL=%d HJ=%d MJ=%d", i, nlB, hjB, mjB)
		}
	}
	t.Logf("All three strategies agree on %d rows", len(nlRows))
}

// TestMergeJoinPreservesGroupOrder verifies that the output is ordered by
// dept_id when the inputs are sorted (merge produces sorted output for free).
func TestMergeJoinPreservesGroupOrder(t *testing.T) {
	db, cleanup := newCompanyDB(t)
	defer cleanup()

	mj := executor.NewMergeJoin(
		sortByDeptID(db.empScan(t), 4),
		sortByDeptID(db.deptScan(t), 0),
		empDeptKey, deptKey, combine)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}

	// Verify dept_id is non-decreasing (merge produces ordered output).
	for i := 1; i < len(rows); i++ {
		prev := deptID(rows[i-1])
		cur := deptID(rows[i])
		if prev > cur {
			t.Errorf("output not ordered: dept_id[%d]=%d > dept_id[%d]=%d",
				i-1, prev, i, cur)
		}
	}
	t.Logf("Output is ordered by dept_id (MergeJoin produces sorted output for free)")
}

// TestMergeJoinWithFilter chains MergeJoin into a filter and sort pipeline.
//
//	SELECT e.emp_id, d.budget, e.salary
//	FROM employees e JOIN departments d ON e.dept_id = d.dept_id
//	WHERE d.budget >= 30_000
//	ORDER BY e.salary DESC
func TestMergeJoinWithFilter(t *testing.T) {
	db, cleanup := newCompanyDB(t)
	defer cleanup()

	// Depts with budget >= 30_000 are IDs 2, 3, 4 (budgets 30k, 40k, 50k).
	pipeline := executor.NewSort(
		executor.NewFilter(
			executor.NewMergeJoin(
				sortByDeptID(db.empScan(t), 4),
				sortByDeptID(db.deptScan(t), 0),
				empDeptKey, deptKey, combine,
			),
			func(st *executor.ScanTuple) bool {
				return budget(st) >= 30_000
			},
		),
		func(a, b *executor.ScanTuple) bool {
			return salary(a) > salary(b)
		},
	)

	rows, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}

	printResults(t, "MergeJoin → Filter(budget≥30k) → Sort(salary DESC)", rows)

	// 3 depts × 4 employees each = 12 rows
	if len(rows) != 12 {
		t.Fatalf("want 12 rows (depts 2,3,4), got %d", len(rows))
	}

	prevSalary := int64(1 << 62)
	for _, r := range rows {
		d := deptID(r)
		if d < 2 {
			t.Errorf("emp_id=%d: dept_id=%d has budget<%d, should be filtered",
				empID(r), d, 30_000)
		}
		s := salary(r)
		if s > prevSalary {
			t.Errorf("not sorted DESC by salary: %d after %d", s, prevSalary)
		}
		prevSalary = s
	}
}

// TestMergeJoinGappedKeys verifies the merge advance logic when outer keys
// skip values present only on the inner side.
func TestMergeJoinGappedKeys(t *testing.T) {
	db, cleanup := newCompanyDB(t)
	defer cleanup()

	// Filter employees to only depts 0, 2, 4 (skip 1 and 3).
	filteredEmps := executor.NewFilter(db.empScan(t), func(st *executor.ScanTuple) bool {
		d := deptID(st)
		return d == 0 || d == 2 || d == 4
	})

	mj := executor.NewMergeJoin(
		sortByDeptID(filteredEmps, 4),
		sortByDeptID(db.deptScan(t), 0),
		empDeptKey, deptKey, combine)
	rows, err := executor.Collect(mj)
	if err != nil {
		t.Fatal(err)
	}

	// Only depts 0, 2, 4 matched → 3 × 4 = 12 rows
	if len(rows) != 12 {
		t.Fatalf("want 12 rows (depts 0,2,4), got %d", len(rows))
	}
	for _, r := range rows {
		d := deptID(r)
		if d != 0 && d != 2 && d != 4 {
			t.Errorf("unexpected dept_id=%d in result", d)
		}
	}
}

// ── TestMain ──────────────────────────────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "=== merge-join experiment ===")
	fmt.Fprintln(os.Stderr, "Completes the PostgreSQL join strategy trilogy:")
	fmt.Fprintln(os.Stderr, "  NestedLoopJoin → nodeNestloop.c")
	fmt.Fprintln(os.Stderr, "  HashJoin       → nodeHashjoin.c + nodeHash.c")
	fmt.Fprintln(os.Stderr, "  MergeJoin      → nodeMergejoin.c  ← this experiment")
	fmt.Fprintln(os.Stderr, "Tables:  employees (emp_id, dept_id, salary)")
	fmt.Fprintln(os.Stderr, "         departments (dept_id, budget)")
	fmt.Fprintln(os.Stderr, "Key insight: MergeJoin requires sorted inputs;")
	fmt.Fprintln(os.Stderr, "             it produces sorted output for free.")
	fmt.Fprintln(os.Stderr)
	m.Run()
}
