package join_test

// Join experiment.
//
// Models the execution of two queries:
//
//	Query A - NestedLoopJoin
//	  SELECT o.order_id, c.tier, o.amount
//	  FROM   orders  o
//	  JOIN   customers c ON o.customer_id = c.customer_id
//	  WHERE  o.amount > 500
//	  ORDER  BY o.amount DESC
//
//	Query B - HashJoin (same logical query, different strategy)
//
// Pipeline:
//
//	SeqScan(orders) → Filter(amount>500) → NestedLoopJoin(SeqScan(customers))
//	                                     → Sort(ORDER BY amount DESC)
//
//	SeqScan(orders) → HashJoin(SeqScan(customers)) → Filter(amount>500)
//	                                               → Sort(ORDER BY amount DESC)
//
// Each node maps directly to a PostgreSQL executor concept:
//
//	SeqScan         → nodeSeqscan.c
//	Filter          → ExecQual (execExpr.c)
//	NestedLoopJoin  → nodeNestloop.c
//	HashJoin        → nodeHashjoin.c + nodeHash.c
//	Sort            → nodeSort.c

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

// ── Row layouts ───────────────────────────────────────────────────────────────
//
// orders:    [0:4] order_id uint32 | [4:8] customer_id uint32 | [8:16] amount int64
// customers: [0:4] customer_id uint32 | [4:8] tier uint32  (0=gold 1=silver 2=bronze)
// output:    [0:4] order_id | [4:8] customer_id | [8:16] amount | [16:20] tier

const (
	ordersRelId    = uint32(1)
	customersRelId = uint32(2)
)

var tierNames = []string{"gold", "silver", "bronze"}

func encodeOrder(orderID, customerID uint32, amount int64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint32(b[0:], orderID)
	binary.BigEndian.PutUint32(b[4:], customerID)
	binary.BigEndian.PutUint64(b[8:], uint64(amount))
	return b
}

func encodeCustomer(customerID, tier uint32) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:], customerID)
	binary.BigEndian.PutUint32(b[4:], tier)
	return b
}

func getOrderID(st *executor.ScanTuple) uint32 {
	return binary.BigEndian.Uint32(st.Tuple.Data[0:4])
}
func getCustomerID(st *executor.ScanTuple) uint32 {
	return binary.BigEndian.Uint32(st.Tuple.Data[4:8])
}
func getAmount(st *executor.ScanTuple) int64 {
	return int64(binary.BigEndian.Uint64(st.Tuple.Data[8:16]))
}
func getTier(st *executor.ScanTuple) uint32 {
	return binary.BigEndian.Uint32(st.Tuple.Data[16:20])
}

// combineOrderCustomer concatenates relevant fields into a 20-byte output row.
func combineOrderCustomer(o, c *executor.ScanTuple) *executor.ScanTuple {
	data := make([]byte, 20)
	copy(data[0:16], o.Tuple.Data[0:16])   // order_id, customer_id, amount
	copy(data[16:20], c.Tuple.Data[4:8])   // tier
	tup := storage.NewHeapTuple(storage.FrozenTransactionId, 0, data)
	return &executor.ScanTuple{Tuple: tup}
}

// ── Fixture ───────────────────────────────────────────────────────────────────

type salesDB struct {
	ordersRel    *storage.Relation
	customersRel *storage.Relation
	txmgr        *mvcc.TransactionManager
	snap         *storage.Snapshot
}

const nCustomers = 10

// newSalesDB creates:
//   - 10 customers (IDs 0..9, tier = id%3)
//   - nOrders orders (customer_id = i%10, amount = (i+1)*97)
func newSalesDB(t *testing.T, nOrders int) (*salesDB, func()) {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(512)

	ordersRel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: ordersRelId}, pool, smgr)
	if err := ordersRel.Init(); err != nil {
		t.Fatalf("orders.Init: %v", err)
	}
	customersRel := storage.OpenRelation(storage.RelFileNode{DbId: 0, RelId: customersRelId}, pool, smgr)
	if err := customersRel.Init(); err != nil {
		t.Fatalf("customers.Init: %v", err)
	}

	txmgr := mvcc.NewTransactionManager()
	xid := txmgr.Begin()

	for i := 0; i < nCustomers; i++ {
		tup := storage.NewHeapTuple(xid, 3, encodeCustomer(uint32(i), uint32(i)%3))
		if _, _, err := executor.HeapInsert(customersRel, tup); err != nil {
			t.Fatalf("customer HeapInsert(%d): %v", i, err)
		}
	}
	for i := 0; i < nOrders; i++ {
		amount := int64(i+1) * 97
		tup := storage.NewHeapTuple(xid, 3, encodeOrder(uint32(i), uint32(i)%nCustomers, amount))
		if _, _, err := executor.HeapInsert(ordersRel, tup); err != nil {
			t.Fatalf("order HeapInsert(%d): %v", i, err)
		}
	}
	if err := txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := txmgr.Begin()
	snap := txmgr.Snapshot(reader)
	return &salesDB{
		ordersRel:    ordersRel,
		customersRel: customersRel,
		txmgr:        txmgr,
		snap:         snap,
	}, func() { smgr.Close() }
}

func (db *salesDB) orderScan(t *testing.T) *executor.SeqScan {
	t.Helper()
	ss, err := executor.NewSeqScan(db.ordersRel, db.snap, db.txmgr)
	if err != nil {
		t.Fatalf("orders SeqScan: %v", err)
	}
	return ss
}

func (db *salesDB) customerScan(t *testing.T) *executor.SeqScan {
	t.Helper()
	ss, err := executor.NewSeqScan(db.customersRel, db.snap, db.txmgr)
	if err != nil {
		t.Fatalf("customers SeqScan: %v", err)
	}
	return ss
}

// ── Join predicates / key functions ──────────────────────────────────────────

// nlPred is the NestedLoopJoin predicate: orders.customer_id == customers.customer_id
func nlPred(o, c *executor.ScanTuple) bool {
	return getCustomerID(o) == binary.BigEndian.Uint32(c.Tuple.Data[0:4])
}

// orderCustomerKey extracts customer_id from an orders row (bytes 4:8).
func orderCustomerKey(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, st.Tuple.Data[4:8])
	return b
}

// customerIDKey extracts customer_id from a customers row (bytes 0:4).
func customerIDKey(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	copy(b, st.Tuple.Data[0:4])
	return b
}

// amountFilter returns a predicate that passes rows with amount > threshold.
func amountFilter(threshold int64) func(*executor.ScanTuple) bool {
	return func(st *executor.ScanTuple) bool {
		return getAmount(st) > threshold
	}
}

// sortByAmountDesc sorts output rows by amount descending.
var sortByAmountDesc = func(a, b *executor.ScanTuple) bool {
	return getAmount(a) > getAmount(b)
}

// ── printTable logs a formatted result table ──────────────────────────────────

func printTable(t *testing.T, rows []*executor.ScanTuple) {
	t.Helper()
	t.Logf("\n%-10s %-12s %-10s %-8s", "order_id", "customer_id", "amount", "tier")
	t.Logf("%s", strings.Repeat("─", 44))
	for _, r := range rows {
		tier := getTier(r)
		tierName := "unknown"
		if int(tier) < len(tierNames) {
			tierName = tierNames[tier]
		}
		t.Logf("%-10d %-12d %-10d %-8s",
			getOrderID(r), getCustomerID(r), getAmount(r), tierName)
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestNestedLoopJoinQuery executes the full NL pipeline and verifies:
//   - all returned orders have amount > 500
//   - each row has the correct tier for its customer
//   - result is sorted DESC by amount
func TestNestedLoopJoinQuery(t *testing.T) {
	db, cleanup := newSalesDB(t, 60)
	defer cleanup()

	pipeline := executor.NewSort(
		executor.NewNestedLoopJoin(
			executor.NewFilter(db.orderScan(t), amountFilter(500)),
			db.customerScan(t),
			nlPred,
			combineOrderCustomer,
		),
		sortByAmountDesc,
	)

	rows, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("=== NestedLoopJoin: orders JOIN customers WHERE amount>500 ORDER BY amount DESC ===")
	printTable(t, rows)

	if len(rows) == 0 {
		t.Fatal("expected non-empty result")
	}

	prevAmount := int64(1<<62)
	for _, r := range rows {
		amt := getAmount(r)
		cid := getCustomerID(r)
		tier := getTier(r)

		if amt <= 500 {
			t.Errorf("row order_id=%d: amount=%d should be >500", getOrderID(r), amt)
		}
		if amt > prevAmount {
			t.Errorf("rows not sorted DESC: amount=%d after %d", amt, prevAmount)
		}
		prevAmount = amt

		wantTier := cid % 3
		if tier != wantTier {
			t.Errorf("order_id=%d cid=%d: want tier=%d, got %d", getOrderID(r), cid, wantTier, tier)
		}
	}
}

// TestHashJoinQuery executes the same logical query via HashJoin and verifies
// it produces the same number of results as NestedLoopJoin.
func TestHashJoinQuery(t *testing.T) {
	db, cleanup := newSalesDB(t, 60)
	defer cleanup()

	// HashJoin: filter after the join (equivalent result).
	pipeline := executor.NewSort(
		executor.NewFilter(
			executor.NewHashJoin(
				db.orderScan(t),
				db.customerScan(t),
				customerIDKey,
				orderCustomerKey,
				combineOrderCustomer,
			),
			amountFilter(500),
		),
		sortByAmountDesc,
	)

	rows, err := executor.Collect(pipeline)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("=== HashJoin: orders JOIN customers WHERE amount>500 ORDER BY amount DESC ===")
	printTable(t, rows)

	if len(rows) == 0 {
		t.Fatal("expected non-empty result")
	}

	prevAmount := int64(1 << 62)
	for _, r := range rows {
		amt := getAmount(r)
		cid := getCustomerID(r)
		tier := getTier(r)

		if amt <= 500 {
			t.Errorf("row order_id=%d: amount=%d should be >500", getOrderID(r), amt)
		}
		if amt > prevAmount {
			t.Errorf("rows not sorted DESC: amount=%d after %d", amt, prevAmount)
		}
		prevAmount = amt

		wantTier := cid % 3
		if tier != wantTier {
			t.Errorf("order_id=%d cid=%d: want tier=%d, got %d", getOrderID(r), cid, wantTier, tier)
		}
	}
}

// TestHashJoinMatchesNestedLoop verifies that both strategies produce identical
// result sets (same rows in the same order after sorting by order_id).
func TestHashJoinMatchesNestedLoop(t *testing.T) {
	db1, cleanup1 := newSalesDB(t, 40)
	defer cleanup1()
	db2, cleanup2 := newSalesDB(t, 40)
	defer cleanup2()

	nlRows, err := executor.Collect(executor.NewSort(
		executor.NewNestedLoopJoin(
			executor.NewFilter(db1.orderScan(t), amountFilter(200)),
			db1.customerScan(t),
			nlPred,
			combineOrderCustomer,
		),
		func(a, b *executor.ScanTuple) bool {
			return getOrderID(a) < getOrderID(b)
		},
	))
	if err != nil {
		t.Fatal(err)
	}

	hjRows, err := executor.Collect(executor.NewSort(
		executor.NewFilter(
			executor.NewHashJoin(
				db2.orderScan(t),
				db2.customerScan(t),
				customerIDKey,
				orderCustomerKey,
				combineOrderCustomer,
			),
			amountFilter(200),
		),
		func(a, b *executor.ScanTuple) bool {
			return getOrderID(a) < getOrderID(b)
		},
	))
	if err != nil {
		t.Fatal(err)
	}

	if len(nlRows) != len(hjRows) {
		t.Fatalf("NL produced %d rows, HJ produced %d rows", len(nlRows), len(hjRows))
	}
	for i := range nlRows {
		nlID := getOrderID(nlRows[i])
		hjID := getOrderID(hjRows[i])
		nlTier := getTier(nlRows[i])
		hjTier := getTier(hjRows[i])
		if nlID != hjID || nlTier != hjTier {
			t.Errorf("row %d: NL=(id=%d,tier=%d) HJ=(id=%d,tier=%d)",
				i, nlID, nlTier, hjID, hjTier)
		}
	}
	t.Logf("NL and HJ agree: %d rows", len(nlRows))
}

// TestJoinWithGroupBy demonstrates a more complex pipeline:
//
//	SELECT c.tier, COUNT(*), SUM(o.amount)
//	FROM orders o JOIN customers c ON o.customer_id = c.customer_id
//	GROUP BY c.tier
//	ORDER BY total_amount DESC
func TestJoinWithGroupBy(t *testing.T) {
	db, cleanup := newSalesDB(t, 90)
	defer cleanup()

	// HashJoin produces rows with tier at bytes [16:20].
	joined := executor.NewHashJoin(
		db.orderScan(t),
		db.customerScan(t),
		customerIDKey,
		orderCustomerKey,
		combineOrderCustomer,
	)

	// GROUP BY tier (bytes 16:20), COUNT(*), SUM(amount)
	tierKey := func(st *executor.ScanTuple) []byte {
		b := make([]byte, 4)
		copy(b, st.Tuple.Data[16:20])
		return b
	}
	amountVal := func(st *executor.ScanTuple) int64 { return getAmount(st) }

	ha := executor.NewHashAgg(joined, tierKey, func() []executor.AggFn {
		return []executor.AggFn{
			executor.NewCountAgg(),
			executor.NewSumAgg(amountVal),
		}
	})

	// Sort by sum DESC.
	sorted := executor.NewSort(ha, func(a, b *executor.ScanTuple) bool {
		// agg output layout: key(4B) + count(8B) + sum(8B)
		sumA := executor.DecodeSum(a.Tuple.Data[12:20])
		sumB := executor.DecodeSum(b.Tuple.Data[12:20])
		return sumA > sumB
	})

	groups, err := executor.Collect(sorted)
	if err != nil {
		t.Fatal(err)
	}

	if len(groups) != 3 {
		t.Fatalf("want 3 tier groups, got %d", len(groups))
	}

	t.Logf("\n%-10s %8s %14s", "tier", "count", "total_amount")
	t.Logf("%s", strings.Repeat("─", 36))

	tiersSeen := make(map[uint32]bool)
	prevSum := int64(1 << 62)
	totalCount := int64(0)
	for _, g := range groups {
		tier := binary.BigEndian.Uint32(g.Tuple.Data[0:4])
		count := executor.DecodeCount(g.Tuple.Data[4:12])
		sum := executor.DecodeSum(g.Tuple.Data[12:20])
		name := "unknown"
		if int(tier) < len(tierNames) {
			name = tierNames[tier]
		}
		t.Logf("%-10s %8d %14d", name, count, sum)

		if sum > prevSum {
			t.Errorf("groups not sorted DESC by sum: %d after %d", sum, prevSum)
		}
		prevSum = sum
		tiersSeen[tier] = true
		totalCount += count
	}

	t.Logf("%s", strings.Repeat("─", 36))
	t.Logf("%-10s %8d", "total", totalCount)

	if totalCount != 90 {
		t.Errorf("total count want 90, got %d", totalCount)
	}
	for tier := uint32(0); tier < 3; tier++ {
		if !tiersSeen[tier] {
			t.Errorf("tier %d (%s) missing from result", tier, tierNames[tier])
		}
	}
}

// TestNoMatchingJoinRows: filter eliminates all orders → join produces 0 rows.
func TestNoMatchingJoinRows(t *testing.T) {
	db, cleanup := newSalesDB(t, 20)
	defer cleanup()

	hj := executor.NewHashJoin(
		executor.NewFilter(db.orderScan(t), func(*executor.ScanTuple) bool { return false }),
		db.customerScan(t),
		customerIDKey,
		orderCustomerKey,
		combineOrderCustomer,
	)
	rows, err := executor.Collect(hj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %d", len(rows))
	}
}

// TestJoinAllOrders verifies that without a filter, every order gets a customer.
func TestJoinAllOrders(t *testing.T) {
	const nOrders = 50
	db, cleanup := newSalesDB(t, nOrders)
	defer cleanup()

	hj := executor.NewHashJoin(
		db.orderScan(t),
		db.customerScan(t),
		customerIDKey,
		orderCustomerKey,
		combineOrderCustomer,
	)
	rows, err := executor.Collect(hj)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != nOrders {
		t.Fatalf("want %d joined rows, got %d", nOrders, len(rows))
	}

	// Verify every order's tier matches customer_id % 3.
	for _, r := range rows {
		cid := getCustomerID(r)
		tier := getTier(r)
		if tier != cid%3 {
			t.Errorf("order_id=%d cid=%d: want tier=%d, got %d",
				getOrderID(r), cid, cid%3, tier)
		}
	}
}

// TestJoinSorted demonstrates a full pipeline with joins from both strategies
// sorted and logged side by side.
func TestJoinSorted(t *testing.T) {
	db, cleanup := newSalesDB(t, 30)
	defer cleanup()

	sortByOrdID := func(rows []*executor.ScanTuple) {
		sort.Slice(rows, func(i, j int) bool {
			return getOrderID(rows[i]) < getOrderID(rows[j])
		})
	}

	nlRows, _ := executor.Collect(executor.NewNestedLoopJoin(
		db.orderScan(t), db.customerScan(t), nlPred, combineOrderCustomer))
	hjRows, _ := executor.Collect(executor.NewHashJoin(
		db.orderScan(t), db.customerScan(t),
		customerIDKey, orderCustomerKey, combineOrderCustomer))

	sortByOrdID(nlRows)
	sortByOrdID(hjRows)

	if len(nlRows) != len(hjRows) {
		t.Fatalf("NL=%d HJ=%d row count mismatch", len(nlRows), len(hjRows))
	}
	t.Logf("Both strategies produced %d rows (all %d orders matched a customer)", len(nlRows), 30)
}

// ── TestMain ──────────────────────────────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "=== join experiment ===")
	fmt.Fprintln(os.Stderr, "Demonstrates NestedLoopJoin and HashJoin executor nodes.")
	fmt.Fprintln(os.Stderr, "  Tables:  orders (order_id, customer_id, amount)")
	fmt.Fprintln(os.Stderr, "           customers (customer_id, tier)")
	fmt.Fprintln(os.Stderr, "  Queries modelled:")
	fmt.Fprintln(os.Stderr, "    SELECT o.order_id, c.tier, o.amount")
	fmt.Fprintln(os.Stderr, "    FROM orders o JOIN customers c ON o.customer_id = c.customer_id")
	fmt.Fprintln(os.Stderr, "    WHERE o.amount > 500 ORDER BY o.amount DESC")
	fmt.Fprintln(os.Stderr, "  Plus: GROUP BY tier over the joined result")
	fmt.Fprintln(os.Stderr)
	m.Run()
}
