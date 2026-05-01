package storage

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBTPageOpaqueEncoding(t *testing.T) {
	o := BTPageOpaqueData{
		BtpoPrev:    10,
		BtpoNext:    20,
		BtpoLevel:   3,
		BtpoFlags:   uint16(BTPLeaf | BTPRoot),
		BtpoCycleid: 7,
	}
	buf := make([]byte, BTPageOpaqueSize)
	encodeBTOpaque(buf, &o)
	got := decodeBTOpaque(buf)
	if got != o {
		t.Fatalf("opaque round-trip failed:\n  got  %+v\n  want %+v", got, o)
	}
}

func TestIndexTupleEncoding(t *testing.T) {
	it := NewIndexTuple(100, 5, 24)
	buf := make([]byte, IndexTupleHeaderSize)
	encodeIndexTuple(buf, &it)
	got := decodeIndexTuple(buf)
	if got != it {
		t.Fatalf("IndexTuple round-trip failed: got %+v, want %+v", got, it)
	}
	blk, off := got.HeapTid()
	if blk != 100 || off != 5 {
		t.Errorf("HeapTid = (%d, %d), want (100, 5)", blk, off)
	}
}

func TestNewLeafPage(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)
	if !bp.IsLeaf() {
		t.Error("expected leaf")
	}
	if bp.IsRoot() {
		t.Error("leaf should not be root")
	}
	if bp.IsMeta() {
		t.Error("leaf should not be meta")
	}
	if bp.Level() != 0 {
		t.Errorf("leaf level = %d, want 0", bp.Level())
	}
}

func TestNewRootLeafPage(t *testing.T) {
	bp := NewBTreePage(BTreeRootLeaf)
	if !bp.IsLeaf() || !bp.IsRoot() {
		t.Error("expected root leaf")
	}
	if bp.Level() != 0 {
		t.Errorf("root leaf level = %d, want 0", bp.Level())
	}
}

func TestNewInternalPage(t *testing.T) {
	bp := NewBTreePage(BTreeInternal)
	if bp.IsLeaf() {
		t.Error("internal should not be leaf")
	}
	if bp.Level() != 1 {
		t.Errorf("internal level = %d, want 1", bp.Level())
	}
}

func TestSiblingPointers(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)

	if _, ok := bp.LeftSibling(); ok {
		t.Error("should have no left sibling initially")
	}
	if _, ok := bp.RightSibling(); ok {
		t.Error("should have no right sibling initially")
	}

	bp.SetSiblings(10, 20)
	left, ok := bp.LeftSibling()
	if !ok || left != 10 {
		t.Errorf("LeftSibling = (%d, %v), want (10, true)", left, ok)
	}
	right, ok := bp.RightSibling()
	if !ok || right != 20 {
		t.Errorf("RightSibling = (%d, %v), want (20, true)", right, ok)
	}
}

func TestInsertAndGetEntry(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)

	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, 42)

	idx, err := bp.InsertEntry(key, 100, 5)
	if err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	if idx != 0 {
		t.Errorf("first entry index = %d, want 0", idx)
	}
	if bp.NumEntries() != 1 {
		t.Errorf("NumEntries = %d, want 1", bp.NumEntries())
	}

	gotKey, blk, off, err := bp.GetEntry(0)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Errorf("key mismatch: got %v, want %v", gotKey, key)
	}
	if blk != 100 || off != 5 {
		t.Errorf("TID = (%d, %d), want (100, 5)", blk, off)
	}
}

func TestInsertMultipleEntries(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)
	for i := 0; i < 10; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		bp.InsertEntry(key, uint32(i), 1)
	}
	if bp.NumEntries() != 10 {
		t.Errorf("NumEntries = %d, want 10", bp.NumEntries())
	}
	for i := 0; i < 10; i++ {
		key, blk, _, err := bp.GetEntry(i)
		if err != nil {
			t.Fatalf("GetEntry %d: %v", i, err)
		}
		wantKey := make([]byte, 4)
		binary.BigEndian.PutUint32(wantKey, uint32(i))
		if !bytes.Equal(key, wantKey) {
			t.Errorf("entry %d key mismatch", i)
		}
		if blk != uint32(i) {
			t.Errorf("entry %d block = %d, want %d", i, blk, i)
		}
	}
}

func TestSearchLeafPage(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)
	// Insert sorted keys: 10, 20, 30, 40, 50
	for _, v := range []int32{10, 20, 30, 40, 50} {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(v))
		bp.InsertEntry(key, uint32(v), 1)
	}

	search := func(target int32) int {
		return bp.SearchLeaf(func(ek []byte) int {
			ev := int32(binary.BigEndian.Uint32(ek))
			if ev < target {
				return -1
			} else if ev > target {
				return 1
			}
			return 0
		})
	}

	// Exact match for 30 (index 2).
	if pos := search(30); pos != 2 {
		t.Errorf("search(30) = %d, want 2", pos)
	}
	// 25 lands at position of 30.
	if pos := search(25); pos != 2 {
		t.Errorf("search(25) = %d, want 2", pos)
	}
	// Below all keys.
	if pos := search(5); pos != 0 {
		t.Errorf("search(5) = %d, want 0", pos)
	}
	// Above all keys.
	if pos := search(100); pos != 5 {
		t.Errorf("search(100) = %d, want 5", pos)
	}
}

func TestInternalPageDownlinks(t *testing.T) {
	bp := NewBTreePage(BTreeInternal)
	for _, pair := range [][2]int32{{10, 100}, {20, 200}, {30, 300}} {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(pair[0]))
		bp.InsertDownlink(key, uint32(pair[1]))
	}
	if bp.NumEntries() != 3 {
		t.Errorf("NumEntries = %d, want 3", bp.NumEntries())
	}
	// Key 15 should route to block 100 (first downlink).
	child, ok := bp.SearchInternal(func(ek []byte) int {
		ev := int32(binary.BigEndian.Uint32(ek))
		if ev < 15 {
			return -1
		} else if ev > 15 {
			return 1
		}
		return 0
	})
	if !ok || child != 100 {
		t.Errorf("SearchInternal(15) = (%d, %v), want (100, true)", child, ok)
	}
}

func TestBTreePageFromPage(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)
	bp2, err := BTreePageFromPage(bp.Page())
	if err != nil {
		t.Fatalf("BTreePageFromPage: %v", err)
	}
	if !bp2.IsLeaf() {
		t.Error("reconstructed page should be leaf")
	}
}

func TestBTreeFreeSpace(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)
	free := bp.FreeSpace()
	// Must have room minus special space.
	maxExpected := PageSize - PageHeaderSize - BTPageOpaqueSize
	if free > maxExpected {
		t.Errorf("free space %d exceeds max %d", free, maxExpected)
	}
	if free < 100 {
		t.Errorf("free space %d too low", free)
	}
}

// TestBTreeIsRightmost verifies that IsRightmost reflects BtpoNext state.
func TestBTreeIsRightmost(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)
	if !bp.IsRightmost() {
		t.Error("fresh page should be rightmost (BtpoNext = Invalid)")
	}
	bp.SetSiblings(InvalidBlockNumber, 5)
	if bp.IsRightmost() {
		t.Error("page with BtpoNext=5 should not be rightmost")
	}
	bp.SetSiblings(InvalidBlockNumber, InvalidBlockNumber)
	if !bp.IsRightmost() {
		t.Error("page with BtpoNext=Invalid should be rightmost again")
	}
}

// TestBTreeHighKey verifies SetHighKey / HighKey round-trip.
func TestBTreeHighKey(t *testing.T) {
	bp := NewBTreePage(BTreeLeaf)

	if _, ok := bp.HighKey(); ok {
		t.Error("rightmost page should have no high key")
	}

	// Make the page non-rightmost, then set a high key.
	bp.SetSiblings(InvalidBlockNumber, 7)
	if bp.IsRightmost() {
		t.Fatal("expected non-rightmost after SetSiblings with right=7")
	}

	hk := make([]byte, 4)
	binary.BigEndian.PutUint32(hk, 42)
	if err := bp.SetHighKey(hk); err != nil {
		t.Fatalf("SetHighKey: %v", err)
	}

	got, ok := bp.HighKey()
	if !ok {
		t.Fatal("HighKey returned false after SetHighKey")
	}
	if !bytes.Equal(got, hk) {
		t.Errorf("HighKey = %v, want %v", got, hk)
	}

	// Data entries added after SetHighKey should not be confused with high key.
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, 10)
	if _, err := bp.InsertEntry(key, 1, 1); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	// NumEntries must be 1 (data) despite 2 physical slots.
	if n := bp.NumEntries(); n != 1 {
		t.Errorf("NumEntries = %d, want 1", n)
	}
	gotKey, blk, off, err := bp.GetEntry(0)
	if err != nil {
		t.Fatalf("GetEntry(0): %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Errorf("GetEntry key = %v, want %v", gotKey, key)
	}
	if blk != 1 || off != 1 {
		t.Errorf("GetEntry TID = (%d, %d), want (1, 1)", blk, off)
	}
}

// TestBTreeHighKeyOnSplit verifies that after a leaf split:
//   - the left page carries a high key equal to the first key of the right page
//   - the right page (rightmost) has no high key
func TestBTreeHighKeyOnSplit(t *testing.T) {
	idx, rel, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	// Insert enough keys to force exactly one leaf split.
	const n = 600
	insertRange(t, idx, 0, n)

	// Walk the leaf level and collect high keys.
	meta, _ := idx.readMeta()
	_, firstLeafBlk, err := idx.findLeaf(meta.Root, meta.Level, encKey(0))
	if err != nil {
		t.Fatalf("findLeaf: %v", err)
	}

	var leafBlocks []BlockNumber
	for blk := firstLeafBlk; blk != InvalidBlockNumber; {
		id, err := rel.ReadBlock(ForkMain, blk)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		page, _ := rel.Pool.GetPage(id)
		bp, _ := BTreePageFromPage(page)
		opaque := bp.Opaque()
		leafBlocks = append(leafBlocks, blk)

		// Every non-rightmost leaf must have a high key.
		if !bp.IsRightmost() {
			hk, ok := bp.HighKey()
			if !ok {
				t.Errorf("leaf block %d is non-rightmost but has no high key", blk)
			} else {
				// High key must equal the first data entry of the right sibling.
				nextId, nerr := rel.ReadBlock(ForkMain, opaque.BtpoNext)
				if nerr == nil {
					nextPage, _ := rel.Pool.GetPage(nextId)
					nextBP, _ := BTreePageFromPage(nextPage)
					if nextBP.NumEntries() > 0 {
						firstRight, _, _, _ := nextBP.GetEntry(0)
						if !bytes.Equal(hk, firstRight) {
							t.Errorf("block %d high key %v != right sibling first key %v",
								blk, hk, firstRight)
						}
					}
					rel.Pool.UnpinBuffer(nextId)
				}
			}
		} else {
			if _, ok := bp.HighKey(); ok {
				t.Errorf("rightmost leaf block %d should not have a high key", blk)
			}
		}

		next := opaque.BtpoNext
		rel.Pool.UnpinBuffer(id)
		blk = next
	}

	if len(leafBlocks) < 2 {
		t.Errorf("expected multiple leaf pages, got %d", len(leafBlocks))
	}
}

// TestBTreeIncompleteSplitCleared verifies that no leaf page retains
// BTPIncomplete after a successful multi-split insert sequence.
func TestBTreeIncompleteSplitCleared(t *testing.T) {
	idx, rel, cleanup := newTestBTreeIndex(t)
	defer cleanup()

	const n = 1200 // two-level tree with multiple internal splits
	insertRange(t, idx, 0, n)

	meta, _ := idx.readMeta()
	_, firstLeafBlk, err := idx.findLeaf(meta.Root, meta.Level, encKey(0))
	if err != nil {
		t.Fatalf("findLeaf: %v", err)
	}

	for blk := firstLeafBlk; blk != InvalidBlockNumber; {
		id, err := rel.ReadBlock(ForkMain, blk)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		page, _ := rel.Pool.GetPage(id)
		bp, _ := BTreePageFromPage(page)
		opaque := bp.Opaque()

		if BTreePageFlags(opaque.BtpoFlags)&BTPIncomplete != 0 {
			t.Errorf("leaf block %d still has BTPIncomplete set", blk)
		}
		next := opaque.BtpoNext
		rel.Pool.UnpinBuffer(id)
		blk = next
	}
}
