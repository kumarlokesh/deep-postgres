package storage

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────────────

// encKey encodes a uint32 as a 4-byte big-endian key so that lexicographic
// order equals numeric order.
func encKey(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// cmpBytes is a lexicographic comparator suitable for encKey-encoded integers.
func cmpBytes(a, b []byte) int { return bytes.Compare(a, b) }

// newTestBTreeIndex creates a fresh, empty BTreeIndex backed by a temp dir.
func newTestBTreeIndex(t *testing.T) (*BTreeIndex, *Relation, func()) {
	t.Helper()
	dir := t.TempDir()
	smgr := NewFileStorageManager(dir)
	pool := NewBufferPool(256)
	node := RelFileNode{DbId: 0, RelId: 42}
	rel := OpenRelation(node, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}
	idx, err := NewBTreeIndex(rel, cmpBytes)
	if err != nil {
		t.Fatalf("NewBTreeIndex: %v", err)
	}
	return idx, rel, func() { smgr.Close() }
}

// insertRange inserts keys [lo, hi) into idx, mapping each key to
// heapBlock=key, heapOffset=1.
func insertRange(t *testing.T, idx *BTreeIndex, lo, hi uint32) {
	t.Helper()
	for i := lo; i < hi; i++ {
		if err := idx.Insert(encKey(i), i, 1); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
}

// searchAll verifies that every key in [lo, hi) is found with the expected TID.
func searchAll(t *testing.T, idx *BTreeIndex, lo, hi uint32) { //nolint:unparam
	t.Helper()
	for i := lo; i < hi; i++ {
		blk, off, found, err := idx.Search(encKey(i))
		if err != nil {
			t.Fatalf("Search(%d): %v", i, err)
		}
		if !found {
			t.Errorf("key %d not found", i)
			continue
		}
		if blk != i || off != 1 {
			t.Errorf("key %d: got (%d,%d), want (%d,1)", i, blk, off, i)
		}
	}
}

// ── unit tests ────────────────────────────────────────────────────────────────

// TestBTreeIndexSinglePage inserts a small number of keys that fit on one leaf
// page and verifies every lookup is correct.
func TestBTreeIndexSinglePage(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	const n = 50
	insertRange(t, idx, 0, n)
	searchAll(t, idx, 0, n)
}

// TestBTreeIndexSinglePageReverseOrder inserts in reverse order to stress the
// sorted-insert path and verifies all lookups succeed.
func TestBTreeIndexSinglePageReverseOrder(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	const n = 50
	for i := uint32(n - 1); i < n; i-- { // safe unsigned loop: exits when i wraps
		if err := idx.Insert(encKey(i), i, 1); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
		if i == 0 {
			break
		}
	}
	searchAll(t, idx, 0, n)
}

// TestBTreeIndexSearchMiss verifies that searching for absent keys returns
// found=false without error.
func TestBTreeIndexSearchMiss(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	insertRange(t, idx, 100, 200)

	for _, miss := range []uint32{0, 50, 99, 200, 300, 1000} {
		_, _, found, err := idx.Search(encKey(miss))
		if err != nil {
			t.Fatalf("Search(%d): unexpected error %v", miss, err)
		}
		if found {
			t.Errorf("Search(%d): expected not found, got found", miss)
		}
	}
}

// TestBTreeIndexLeafSplit inserts enough entries to force at least one leaf
// split (> 509 entries with 4-byte keys on an 8 KB page) and verifies all
// keys are still reachable after the split.
//
// Page capacity math:
//
//	available = opaqueOffset(8176) - PageHeaderSize(24) = 8152 bytes
//	per entry = IndexTupleHeaderSize(8) + keyLen(4) + ItemIdSize(4) = 16 bytes
//	max       = 8152 / 16 = 509 entries  → 510 triggers first split
func TestBTreeIndexLeafSplit(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	const n = 600 // well past the 509-entry split threshold
	insertRange(t, idx, 0, n)
	searchAll(t, idx, 0, n)
}

// TestBTreeIndexLeafSplitReverseOrder stress-tests the split path with keys
// arriving in descending order, which exercises the rightmost-split fast-path
// (and the sibling pointer update path when the old right-sibling exists).
func TestBTreeIndexLeafSplitReverseOrder(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	const n = 600
	for i := uint32(n - 1); ; i-- {
		if err := idx.Insert(encKey(i), i, 1); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
		if i == 0 {
			break
		}
	}
	searchAll(t, idx, 0, n)
}

// TestBTreeIndexTwoLevelTree inserts 1200 keys, which forces the root to
// become an internal page pointing to multiple leaf pages (2-level tree).
// All keys must still be searchable.
func TestBTreeIndexTwoLevelTree(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	const n = 1200
	insertRange(t, idx, 0, n)
	searchAll(t, idx, 0, n)
}

// TestBTreeIndexMetaRootTracking inserts enough to cause a root promotion and
// confirms that the meta page records a root block > 1 (block 1 is the
// original root/leaf) with a non-zero level.
func TestBTreeIndexMetaRootTracking(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	insertRange(t, idx, 0, 600) // force a split → root promotion

	meta, err := idx.readMeta()
	if err != nil {
		t.Fatalf("readMeta: %v", err)
	}
	if meta.Root <= 1 {
		t.Errorf("root block = %d, want > 1 (original root/leaf was block 1)", meta.Root)
	}
	if meta.Level == 0 {
		t.Errorf("meta.Level = 0, want > 0 after root promotion")
	}
}

// TestBTreeIndexSiblingLinks verifies that after a leaf split the leaf-level
// pages form a complete doubly-linked chain covering all keys in sorted order.
func TestBTreeIndexSiblingLinks(t *testing.T) {
	idx, rel, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	const n = 600
	insertRange(t, idx, 0, n)

	// Walk the leaf level starting from the leftmost leaf.
	// We find it by descending via the smallest possible key.
	_, leafBlk, err := idx.findLeaf(
		func() BlockNumber { m, _ := idx.readMeta(); return m.Root }(),
		func() uint32 { m, _ := idx.readMeta(); return m.Level }(),
		encKey(0),
	)
	if err != nil {
		t.Fatalf("findLeaf: %v", err)
	}

	var (
		visited    []BlockNumber
		prevBlk    = InvalidBlockNumber
		totalItems int
	)
	for blk := leafBlk; blk != InvalidBlockNumber; {
		id, err := rel.ReadBlock(ForkMain, blk)
		if err != nil {
			t.Fatalf("ReadBlock(%d): %v", blk, err)
		}
		page, _ := rel.Pool.GetPage(id)
		bp, err := BTreePageFromPage(page)
		if err != nil {
			rel.Pool.UnpinBuffer(id)
			t.Fatalf("BTreePageFromPage(%d): %v", blk, err)
		}

		if !bp.IsLeaf() {
			rel.Pool.UnpinBuffer(id)
			t.Fatalf("block %d is not a leaf page", blk)
		}

		// Verify back-pointer.
		if bp.Opaque().BtpoPrev != prevBlk {
			t.Errorf("block %d: btpo_prev = %d, want %d", blk, bp.Opaque().BtpoPrev, prevBlk)
		}

		totalItems += bp.NumEntries()
		visited = append(visited, blk)
		prevBlk = blk
		next, hasNext := bp.RightSibling()
		rel.Pool.UnpinBuffer(id)
		if !hasNext {
			break
		}
		blk = next
	}

	if totalItems != n {
		t.Errorf("total items across leaf pages = %d, want %d", totalItems, n)
	}
	if len(visited) < 2 {
		t.Errorf("expected at least 2 leaf pages after split, got %d", len(visited))
	}
}

// TestBTreeIndexPersistence inserts keys, flushes to disk, reopens the
// relation with a fresh buffer pool and storage manager, and verifies all
// keys are still searchable.
func TestBTreeIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	node := RelFileNode{DbId: 0, RelId: 99}
	const n = 600

	// Session 1: write.
	{
		smgr := NewFileStorageManager(dir)
		pool := NewBufferPool(256)
		rel := OpenRelation(node, pool, smgr)
		rel.Init()

		idx, err := NewBTreeIndex(rel, cmpBytes)
		if err != nil {
			t.Fatalf("NewBTreeIndex: %v", err)
		}
		insertRange(t, idx, 0, n)
		if err := rel.Flush(ForkMain); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		smgr.Close()
	}

	// Session 2: read back.
	{
		smgr := NewFileStorageManager(dir)
		defer smgr.Close()
		pool := NewBufferPool(256)
		rel := OpenRelation(node, pool, smgr)

		idx, err := OpenBTreeIndex(rel, cmpBytes)
		if err != nil {
			t.Fatalf("OpenBTreeIndex: %v", err)
		}
		searchAll(t, idx, 0, n)

		// Also verify misses.
		for _, miss := range []uint32{n, n + 1, n + 100} {
			_, _, found, err := idx.Search(encKey(miss))
			if err != nil {
				t.Fatalf("Search(%d): %v", miss, err)
			}
			if found {
				t.Errorf("Search(%d): unexpected hit after reopen", miss)
			}
		}
	}
}

// TestBTreeIndexDuplicateKeys inserts the same key multiple times (allowed by
// spec) and verifies at least the first occurrence is findable.
func TestBTreeIndexDuplicateKeys(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	key := encKey(42)
	for i := uint32(0); i < 5; i++ {
		if err := idx.Insert(key, i, OffsetNumber(i+1)); err != nil {
			t.Fatalf("Insert dup %d: %v", i, err)
		}
	}

	_, _, found, err := idx.Search(key)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !found {
		t.Error("duplicate key not found")
	}
}

// TestBTreeIndexInterleaved inserts keys in interleaved (even then odd) order
// to exercise mid-page insertions that shift existing entries right.
func TestBTreeIndexInterleaved(t *testing.T) {
	idx, _, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	const n = 400
	// Insert evens first, then odds.
	for i := uint32(0); i < n; i += 2 {
		if err := idx.Insert(encKey(i), i, 1); err != nil {
			t.Fatalf("Insert(even %d): %v", i, err)
		}
	}
	for i := uint32(1); i < n; i += 2 {
		if err := idx.Insert(encKey(i), i, 1); err != nil {
			t.Fatalf("Insert(odd %d): %v", i, err)
		}
	}
	searchAll(t, idx, 0, n)
}
