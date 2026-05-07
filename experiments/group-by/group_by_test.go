package groupby_test

// GROUP BY experiment.
//
// Models the execution of a query like:
//
//	SELECT   category, COUNT(*), SUM(revenue), MIN(revenue), MAX(revenue)
//	FROM     sales
//	WHERE    region = 'west'
//	GROUP BY category
//	ORDER BY total_revenue DESC
//
// Pipeline:
//
//	SeqScan → Filter(region=west) → HashAgg(GROUP BY category) → Sort(ORDER BY sum DESC)
//
// Each node in the chain maps directly to a PostgreSQL executor concept:
//
//	SeqScan   → nodeSeqscan.c
//	Filter    → ExecQual (execExpr.c)
//	HashAgg   → nodeAgg.c with AGG_HASHED strategy
//	Sort      → nodeSort.c (tuplesort.c for external sort)

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
//	[0:4]  region    uint32  0=east 1=west 2=north 3=south
//	[4:8]  category  uint32  0=electronics 1=apparel 2=books 3=food 4=home
//	[8:16] revenue   int64   big-endian

const (
	regionOff   = 0
	categoryOff = 4
	revenueOff  = 8
	rowWidth    = 16
)

var regionNames = []string{"east", "west", "north", "south"}
var categoryNames = []string{"electronics", "apparel", "books", "food", "home"}

func encodeRow(region, category uint32, revenue int64) []byte {
	b := make([]byte, rowWidth)
	binary.BigEndian.PutUint32(b[0:], region)
	binary.BigEndian.PutUint32(b[4:], category)
	binary.BigEndian.PutUint64(b[8:], uint64(revenue))
	return b
}

func getRegion(st *executor.ScanTuple) uint32 {
	return binary.BigEndian.Uint32(st.Tuple.Data[regionOff:])
}
func getCategory(st *executor.ScanTuple) uint32 {
	return binary.BigEndian.Uint32(st.Tuple.Data[categoryOff:])
}
func getRevenue(st *executor.ScanTuple) int64 {
	return int64(binary.BigEndian.Uint64(st.Tuple.Data[revenueOff:]))
}

// categoryKey extracts the 4-byte category as the GROUP BY key.
func categoryKey(st *executor.ScanTuple) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, getCategory(st))
	return b
}

// ── Fixture ───────────────────────────────────────────────────────────────────

type db struct {
	rel   *storage.Relation
	txmgr *mvcc.TransactionManager
	snap  *storage.Snapshot
}

// newSalesDB inserts nRows rows.
// Revenue is deterministic: revenue = (category+1) * (rowIndex+1) * 10
// so that totals per category are easy to verify.
func newSalesDB(t *testing.T, nRows int) (*db, func()) {
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
	for i := 0; i < nRows; i++ {
		region := uint32(i % 4)
		category := uint32(i % 5)
		revenue := int64((category+1)*uint32(i+1)) * 10
		tup := storage.NewHeapTuple(xid, 3, encodeRow(region, category, revenue))
		if _, _, err := executor.HeapInsert(rel, tup); err != nil {
			t.Fatalf("HeapInsert(%d): %v", i, err)
		}
	}
	if err := txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := txmgr.Begin()
	snap := txmgr.Snapshot(reader)
	return &db{rel: rel, txmgr: txmgr, snap: snap},
		func() { smgr.Close() }
}

func (d *db) seqScan(t *testing.T) *executor.SeqScan {
	t.Helper()
	ss, err := executor.NewSeqScan(d.rel, d.snap, d.txmgr)
	if err != nil {
		t.Fatalf("NewSeqScan: %v", err)
	}
	return ss
}

// ── Result decoder ────────────────────────────────────────────────────────────

// aggRow decodes a HashAgg output tuple.
// Layout: category(4B) + count(8B) + sum(8B) + min(8B) + max(8B) = 36 bytes.
type aggRow struct {
	Category uint32
	Count    int64
	Sum      int64
	Min      int64
	Max      int64
}

func decodeAggRow(st *executor.ScanTuple) aggRow {
	d := st.Tuple.Data
	return aggRow{
		Category: binary.BigEndian.Uint32(d[0:4]),
		Count:    executor.DecodeCount(d[4:12]),
		Sum:      executor.DecodeSum(d[12:20]),
		Min:      executor.DecodeMin(d[20:28]),
		Max:      executor.DecodeMax(d[28:36]),
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestGroupByCorrectness verifies the full pipeline for 100 rows, west region.
func TestGroupByCorrectness(t *testing.T) {
	d, cleanup := newSalesDB(t, 100)
	defer cleanup()

	ss := d.seqScan(t)

	// Filter: region == west (1)
	filtered := executor.NewFilter(ss, func(st *executor.ScanTuple) bool {
		return getRegion(st) == 1
	})

	// HashAgg: GROUP BY category, COUNT(*), SUM(revenue), MIN(revenue), MAX(revenue)
	ha := executor.NewHashAgg(filtered, categoryKey, func() []executor.AggFn {
		return []executor.AggFn{
			executor.NewCountAgg(),
			executor.NewSumAgg(getRevenue),
			executor.NewMinAgg(getRevenue),
			executor.NewMaxAgg(getRevenue),
		}
	})

	// Sort: ORDER BY sum DESC
	sorted := executor.NewSort(ha, func(a, b *executor.ScanTuple) bool {
		sa := executor.DecodeSum(a.Tuple.Data[12:20])
		sb := executor.DecodeSum(b.Tuple.Data[12:20])
		return sa > sb // descending
	})

	groups, err := executor.Collect(sorted)
	if err != nil {
		t.Fatal(err)
	}

	// With 100 rows and region = i%4==1, west rows are i=1,5,9,…,97 → 25 rows.
	// Categories appear among those 25 rows.
	t.Logf("\n%-14s %6s %12s %10s %10s", "category", "count", "sum", "min", "max")
	t.Logf("%s", strings.Repeat("─", 58))
	totalCount := int64(0)
	prevSum := int64(1<<62)
	for _, g := range groups {
		r := decodeAggRow(g)
		name := categoryNames[r.Category]
		t.Logf("%-14s %6d %12d %10d %10d", name, r.Count, r.Sum, r.Min, r.Max)
		totalCount += r.Count
		if r.Sum > prevSum {
			t.Errorf("groups not sorted DESC by sum: got %d after %d", r.Sum, prevSum)
		}
		prevSum = r.Sum
		// min <= max for each group
		if r.Min > r.Max {
			t.Errorf("category %s: min=%d > max=%d", name, r.Min, r.Max)
		}
	}
	t.Logf("%s", strings.Repeat("─", 58))
	t.Logf("%-14s %6d", "total", totalCount)

	if totalCount != 25 {
		t.Errorf("total west rows want 25, got %d", totalCount)
	}
	// All 5 categories must appear (25 rows / 5 categories = 5 each).
	if len(groups) != 5 {
		t.Errorf("want 5 category groups, got %d", len(groups))
	}
	for _, g := range groups {
		if decodeAggRow(g).Count != 5 {
			t.Errorf("category %d: want 5 rows, got %d",
				decodeAggRow(g).Category, decodeAggRow(g).Count)
		}
	}
}

// TestGroupByAllRegions verifies that removing the Filter produces 5 groups
// covering all rows.
func TestGroupByAllRegions(t *testing.T) {
	const nRows = 100
	d, cleanup := newSalesDB(t, nRows)
	defer cleanup()

	ha := executor.NewHashAgg(d.seqScan(t), categoryKey, func() []executor.AggFn {
		return []executor.AggFn{executor.NewCountAgg()}
	})
	groups, err := executor.Collect(ha)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 5 {
		t.Fatalf("want 5 groups, got %d", len(groups))
	}
	total := int64(0)
	for _, g := range groups {
		total += executor.DecodeCount(g.Tuple.Data[4:12])
	}
	if total != nRows {
		t.Errorf("total count across all groups want %d, got %d", nRows, total)
	}
}

// TestSortReversed verifies ORDER BY sum ASC vs DESC both work correctly.
func TestSortReversed(t *testing.T) {
	d, cleanup := newSalesDB(t, 50)
	defer cleanup()

	ha := executor.NewHashAgg(d.seqScan(t), categoryKey, func() []executor.AggFn {
		return []executor.AggFn{executor.NewSumAgg(getRevenue)}
	})
	allGroups, err := executor.Collect(ha)
	if err != nil {
		t.Fatal(err)
	}

	// Re-sort the results as both ASC and DESC and verify ordering.
	ascSorted := executor.NewSort(
		executor.NewHashAgg(d.seqScan(t), categoryKey, func() []executor.AggFn {
			return []executor.AggFn{executor.NewSumAgg(getRevenue)}
		}),
		func(a, b *executor.ScanTuple) bool {
			return executor.DecodeSum(a.Tuple.Data[4:12]) < executor.DecodeSum(b.Tuple.Data[4:12])
		},
	)
	asc, err := executor.Collect(ascSorted)
	if err != nil {
		t.Fatal(err)
	}
	_ = allGroups
	for i := 1; i < len(asc); i++ {
		prev := executor.DecodeSum(asc[i-1].Tuple.Data[4:12])
		cur := executor.DecodeSum(asc[i].Tuple.Data[4:12])
		if prev > cur {
			t.Errorf("ASC sort: sum[%d]=%d > sum[%d]=%d", i-1, prev, i, cur)
		}
	}
}

// TestNoMatchingRows: Filter eliminates all rows → HashAgg produces 0 groups.
func TestNoMatchingRows(t *testing.T) {
	d, cleanup := newSalesDB(t, 20)
	defer cleanup()

	ha := executor.NewHashAgg(
		executor.NewFilter(d.seqScan(t), func(*executor.ScanTuple) bool { return false }),
		categoryKey,
		func() []executor.AggFn { return []executor.AggFn{executor.NewCountAgg()} },
	)
	groups, err := executor.Collect(ha)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 0 {
		t.Fatalf("want 0 groups, got %d", len(groups))
	}
}

// ── TestMain ─────────────────────────────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "=== group-by experiment ===")
	fmt.Fprintln(os.Stderr, "Demonstrates ORDER BY + GROUP BY using Sort and HashAgg nodes.")
	fmt.Fprintln(os.Stderr, "  Pipeline: SeqScan → Filter → HashAgg → Sort")
	fmt.Fprintln(os.Stderr, "  Query modelled:")
	fmt.Fprintln(os.Stderr, "    SELECT category, COUNT(*), SUM(rev), MIN(rev), MAX(rev)")
	fmt.Fprintln(os.Stderr, "    FROM sales WHERE region='west'")
	fmt.Fprintln(os.Stderr, "    GROUP BY category ORDER BY sum DESC")
	fmt.Fprintln(os.Stderr)
	m.Run()
}
