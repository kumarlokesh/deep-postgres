package executor

// Index scan integration tests.
//
// Each test builds a real on-disk heap relation and a B-tree index in a temp
// directory, inserts tuples with known XID state, then runs IndexScan and
// asserts the correct tuples are returned.
//
// Scenarios covered:
//   - Simple lookup — committed tuple found by key.
//   - Key not in index — empty result.
//   - Aborted insert — index entry exists but heap tuple is invisible.
//   - Duplicate keys — all matching committed tuples returned.
//   - Snapshot isolation — tuple committed after snapshot is invisible.
//   - HOT redirect — LP_REDIRECT on heap page is followed correctly.

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── fixture ───────────────────────────────────────────────────────────────────

// indexFixture holds a heap relation and a matching B-tree index.
type indexFixture struct {
	heap  *scanRelation // reuse seqscan fixture for heap management
	idx   *storage.BTreeIndex
	txmgr *mvcc.TransactionManager
}

// bytesComparator is a simple lexicographic comparator for test keys.
func bytesComparator(a, b []byte) int { return bytes.Compare(a, b) }

func newIndexFixture(t *testing.T) *indexFixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "idxscan-*")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}

	// Heap relation (RelId 1).
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(64)
	heapRFN := storage.RelFileNode{DbId: 1, RelId: 1}
	heapRel := storage.OpenRelation(heapRFN, pool, smgr)
	if err := heapRel.Init(); err != nil {
		t.Fatalf("heapRel.Init: %v", err)
	}

	// Index relation (RelId 2, same pool and smgr, different relid).
	idxRFN := storage.RelFileNode{DbId: 1, RelId: 2}
	idxRel := storage.OpenRelation(idxRFN, pool, smgr)
	if err := idxRel.Init(); err != nil {
		t.Fatalf("idxRel.Init: %v", err)
	}

	idx, err := storage.NewBTreeIndex(idxRel, bytesComparator)
	if err != nil {
		t.Fatalf("NewBTreeIndex: %v", err)
	}

	txmgr := mvcc.NewTransactionManager()

	t.Cleanup(func() {
		smgr.Close()
		os.RemoveAll(dir)
	})

	sr := &scanRelation{rel: heapRel, pool: pool, txmgr: txmgr, smgr: smgr, dir: dir}
	return &indexFixture{heap: sr, idx: idx, txmgr: txmgr}
}

// insertIndexed inserts a heap tuple and adds an index entry pointing to it.
// Returns the heap TID so callers can also test HeapFetch directly.
func (f *indexFixture) insertIndexed(t *testing.T, xid storage.TransactionId, key []byte, data []byte) (storage.BlockNumber, storage.OffsetNumber) {
	t.Helper()
	blk, off := f.heap.insertTuple(t, xid, data)
	if err := f.idx.Insert(key, blk, off); err != nil {
		t.Fatalf("idx.Insert: %v", err)
	}
	return blk, off
}

// scan runs an IndexScan for key and returns all visible ScanTuples.
func (f *indexFixture) scan(t *testing.T, snap *storage.Snapshot, key []byte) []*ScanTuple {
	t.Helper()
	is, err := NewIndexScan(f.idx, f.heap.rel, snap, f.txmgr, key)
	if err != nil {
		t.Fatalf("NewIndexScan: %v", err)
	}
	defer is.Close()

	var out []*ScanTuple
	for {
		st, err := is.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if st == nil {
			break
		}
		out = append(out, st)
	}
	return out
}

// u32key encodes n as a 4-byte big-endian key so ordering matches numeric order.
func u32key(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestIndexScanSimpleLookup(t *testing.T) {
	f := newIndexFixture(t)

	xid := f.txmgr.Begin()
	key := u32key(42)
	f.insertIndexed(t, xid, key, []byte("hello index"))
	if err := f.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap, key)
	if len(rows) != 1 {
		t.Fatalf("got %d rows want 1", len(rows))
	}
	if string(rows[0].Tuple.Data) != "hello index" {
		t.Errorf("data: got %q want %q", rows[0].Tuple.Data, "hello index")
	}
}

func TestIndexScanKeyNotFound(t *testing.T) {
	f := newIndexFixture(t)

	xid := f.txmgr.Begin()
	f.insertIndexed(t, xid, u32key(10), []byte("row-10"))
	if err := f.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap, u32key(99))
	if len(rows) != 0 {
		t.Errorf("key not in index: got %d rows want 0", len(rows))
	}
}

func TestIndexScanAbortedInsertInvisible(t *testing.T) {
	f := newIndexFixture(t)

	// Insert and abort — index entry exists, heap tuple is invisible.
	xid := f.txmgr.Begin()
	key := u32key(7)
	f.insertIndexed(t, xid, key, []byte("aborted"))
	if err := f.txmgr.Abort(xid); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap, key)
	if len(rows) != 0 {
		t.Errorf("aborted insert: got %d rows want 0", len(rows))
	}
}

func TestIndexScanDuplicateKeys(t *testing.T) {
	f := newIndexFixture(t)

	key := u32key(5)

	xid1 := f.txmgr.Begin()
	f.insertIndexed(t, xid1, key, []byte("dup-a"))
	if err := f.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit xid1: %v", err)
	}

	// Aborted duplicate — should not appear.
	xid2 := f.txmgr.Begin()
	f.insertIndexed(t, xid2, key, []byte("dup-aborted"))
	if err := f.txmgr.Abort(xid2); err != nil {
		t.Fatalf("Abort xid2: %v", err)
	}

	xid3 := f.txmgr.Begin()
	f.insertIndexed(t, xid3, key, []byte("dup-b"))
	if err := f.txmgr.Commit(xid3); err != nil {
		t.Fatalf("Commit xid3: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap, key)
	if len(rows) != 2 {
		t.Fatalf("duplicate keys: got %d rows want 2", len(rows))
	}
	// Order is determined by B-tree leaf position (same-key entries are
	// inserted at the lower-bound position, so newer entries precede older
	// ones for equal keys).  We check for set membership, not order.
	got := map[string]bool{}
	for _, r := range rows {
		got[string(r.Tuple.Data)] = true
	}
	for _, want := range []string{"dup-a", "dup-b"} {
		if !got[want] {
			t.Errorf("missing expected row %q", want)
		}
	}
}

func TestIndexScanSnapshotIsolation(t *testing.T) {
	f := newIndexFixture(t)

	// Snapshot taken before insert — must not see the row.
	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	writer := f.txmgr.Begin()
	key := u32key(3)
	f.insertIndexed(t, writer, key, []byte("after snapshot"))
	if err := f.txmgr.Commit(writer); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rows := f.scan(t, snap, key)
	if len(rows) != 0 {
		t.Errorf("snapshot isolation: got %d rows want 0", len(rows))
	}
}

func TestIndexScanReturnsTID(t *testing.T) {
	f := newIndexFixture(t)

	xid := f.txmgr.Begin()
	key := u32key(100)
	wantBlk, wantOff := f.insertIndexed(t, xid, key, []byte("tid-check"))
	if err := f.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	rows := f.scan(t, snap, key)
	if len(rows) != 1 {
		t.Fatalf("got %d rows want 1", len(rows))
	}
	if rows[0].Block != wantBlk || rows[0].Offset != wantOff {
		t.Errorf("TID: got (%d,%d) want (%d,%d)", rows[0].Block, rows[0].Offset, wantBlk, wantOff)
	}
}

func TestIndexScanHotRedirect(t *testing.T) {
	// Manually craft an LP_REDIRECT on the heap page and verify HeapFetch
	// follows it to the LP_NORMAL chain head.
	f := newIndexFixture(t)

	xid := f.txmgr.Begin()
	key := u32key(77)
	// Insert a real tuple at some position.
	blk, off := f.insertIndexed(t, xid, key, []byte("hot-target"))
	if err := f.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Add a second slot to the same page that will become the LP_REDIRECT,
	// pointing at the tuple we just inserted.  We do this by inserting a dummy
	// tuple and then rewriting its line pointer as a redirect.
	xidDummy := f.txmgr.Begin()
	dummyBlk, dummyOff := f.heap.insertTuple(t, xidDummy, []byte("dummy"))
	if err := f.txmgr.Abort(xidDummy); err != nil {
		t.Fatalf("Abort dummy: %v", err)
	}

	if dummyBlk != blk {
		// Tuples ended up on different pages; HOT redirect is intra-page only.
		// Skip the redirect manipulation and just confirm the plain scan works.
		t.Logf("skipping redirect rewrite: tuples on different blocks (%d, %d)", blk, dummyBlk)
	} else {
		// Rewrite the dummy slot as LP_REDIRECT → off (the real tuple).
		id, err := f.heap.rel.ReadBlock(storage.ForkMain, blk)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		page, err := f.heap.pool.GetPageForWrite(id)
		if err != nil {
			f.heap.pool.UnpinBuffer(id) //nolint:errcheck
			t.Fatalf("GetPageForWrite: %v", err)
		}
		if err := page.SetItemIdRedirect(int(dummyOff)-1, off); err != nil {
			f.heap.pool.UnpinBuffer(id) //nolint:errcheck
			t.Fatalf("SetItemIdRedirect: %v", err)
		}
		f.heap.pool.UnpinBuffer(id) //nolint:errcheck

		// Build an index entry pointing at the redirect slot.
		if err := f.idx.Insert(key, blk, dummyOff); err != nil {
			t.Fatalf("idx.Insert redirect TID: %v", err)
		}

		// Scan — both TIDs (real and redirect) should yield the same visible tuple.
		reader := f.txmgr.Begin()
		snap := f.txmgr.Snapshot(reader)

		rows := f.scan(t, snap, key)
		// One from the original insert + one from the redirect — both resolve to the same visible tuple.
		if len(rows) != 2 {
			t.Fatalf("redirect test: got %d rows want 2", len(rows))
		}
		for i, r := range rows {
			if string(r.Tuple.Data) != "hot-target" {
				t.Errorf("row[%d]: got %q want %q", i, r.Tuple.Data, "hot-target")
			}
		}
		return
	}

	// Fallback: plain visibility check when tuples are on different blocks.
	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)
	rows := f.scan(t, snap, key)
	if len(rows) != 1 {
		t.Fatalf("got %d rows want 1", len(rows))
	}
}
