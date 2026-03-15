package storage

import (
	"bytes"
	"testing"
)

func newTestRelation(t *testing.T) (*Relation, func()) {
	t.Helper()
	dir := t.TempDir()
	smgr := NewFileStorageManager(dir)
	pool := NewBufferPool(64)
	node := RelFileNode{DbId: 0, RelId: 10}
	rel := OpenRelation(node, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return rel, func() { smgr.Close() }
}

func TestRelationExtend(t *testing.T) {
	rel, cleanup := newTestRelation(t)
	defer cleanup()

	blk, id, err := rel.Extend(ForkMain)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	defer rel.Pool.UnpinBuffer(id)

	if blk != 0 {
		t.Errorf("first block = %d, want 0", blk)
	}

	n, _ := rel.NBlocks(ForkMain)
	if n != 1 {
		t.Errorf("NBlocks = %d, want 1", n)
	}

	page, err := rel.Pool.GetPage(id)
	if err != nil {
		t.Fatal(err)
	}
	if !page.IsEmpty() {
		t.Error("extended page should be empty")
	}
}

func TestRelationWriteAndReadBack(t *testing.T) {
	rel, cleanup := newTestRelation(t)
	defer cleanup()

	payload := []byte("durable tuple data")

	// Write.
	blk, id, err := rel.Extend(ForkMain)
	if err != nil {
		t.Fatal(err)
	}
	page, _ := rel.Pool.GetPageForWrite(id)
	if _, err := page.InsertTuple(payload); err != nil {
		t.Fatal(err)
	}
	rel.Pool.UnpinBuffer(id)

	// Flush to disk.
	if err := rel.Flush(ForkMain); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Evict from cache by filling the pool with other pages.
	for i := 1; i <= 65; i++ {
		tag := BufferTag{RelationId: 99, Fork: ForkMain, BlockNum: BlockNumber(i)}
		if bid, err := rel.Pool.ReadBuffer(tag); err == nil {
			rel.Pool.UnpinBuffer(bid)
		}
	}

	// Re-read from disk.
	id2, err := rel.ReadBlock(ForkMain, blk)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	defer rel.Pool.UnpinBuffer(id2)

	page2, _ := rel.Pool.GetPage(id2)
	got, err := page2.GetTuple(0)
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip data mismatch: got %q, want %q", got, payload)
	}
}

func TestRelationMultipleBlocks(t *testing.T) {
	rel, cleanup := newTestRelation(t)
	defer cleanup()

	const nBlocks = 5
	for i := 0; i < nBlocks; i++ {
		blk, id, err := rel.Extend(ForkMain)
		if err != nil {
			t.Fatalf("Extend %d: %v", i, err)
		}
		if blk != BlockNumber(i) {
			t.Errorf("block %d: got %d", i, blk)
		}
		rel.Pool.UnpinBuffer(id)
	}

	n, _ := rel.NBlocks(ForkMain)
	if n != nBlocks {
		t.Errorf("NBlocks = %d, want %d", n, nBlocks)
	}
}

func TestRelationPersistenceAcrossSmgrReopen(t *testing.T) {
	dir := t.TempDir()
	node := RelFileNode{DbId: 0, RelId: 7}
	payload := []byte("persisted across reopen")

	// First session: write a tuple.
	{
		smgr1 := NewFileStorageManager(dir)
		pool1 := NewBufferPool(32)
		rel1 := OpenRelation(node, pool1, smgr1)
		rel1.Init()
		_, id, _ := rel1.Extend(ForkMain)
		page, _ := pool1.GetPageForWrite(id)
		page.InsertTuple(payload)
		pool1.UnpinBuffer(id)
		rel1.Flush(ForkMain)
		smgr1.Close()
	}

	// Second session: read back without the buffer pool cache.
	{
		smgr2 := NewFileStorageManager(dir)
		defer smgr2.Close()
		pool2 := NewBufferPool(32)
		rel2 := OpenRelation(node, pool2, smgr2)

		id, err := rel2.ReadBlock(ForkMain, 0)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		defer pool2.UnpinBuffer(id)

		page, _ := pool2.GetPage(id)
		got, err := page.GetTuple(0)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("got %q, want %q", got, payload)
		}
	}
}

func TestRelationDrop(t *testing.T) {
	rel, cleanup := newTestRelation(t)
	defer cleanup()

	rel.Extend(ForkMain)
	if err := rel.Drop(); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if rel.smgr.Exists(rel.Node, ForkMain) {
		t.Error("relation fork should not exist after Drop")
	}
}
