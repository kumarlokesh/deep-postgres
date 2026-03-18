package storage

// B-tree WAL redo integration tests.
//
// Each test encodes a WAL record for a B-tree operation, replays it through
// a RedoEngine with a WalPageStore, and verifies the resulting page state
// using the BTreePage API.

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/wal"
)

// btreeReln is the synthetic relation used for B-tree index pages.
var btreeReln = wal.RelFileLocator{SpcOid: 0, DbOid: 1, RelOid: 200}

// encIndexTuple builds a raw IndexTuple (header + key) in the same format
// that BTreePage.InsertEntry produces.
func encIndexTuple(t *testing.T, heapBlock BlockNumber, heapOff OffsetNumber, key []byte) []byte {
	t.Helper()
	sz := IndexTupleHeaderSize + len(key)
	hdr := NewIndexTuple(heapBlock, heapOff, uint16(sz))
	buf := make([]byte, sz)
	encodeIndexTuple(buf[:IndexTupleHeaderSize], &hdr)
	copy(buf[IndexTupleHeaderSize:], key)
	return buf
}

// seedBtreePage allocates an empty B-tree page in pool for the given block
// and initialises it to the given page type.
func seedBtreePage(t *testing.T, pool *BufferPool, store *WalPageStore, block uint32, pt BTreePageType) {
	t.Helper()
	relOid := store.relOid(btreeReln)
	tag := BufferTag{RelationId: relOid, Fork: ForkMain, BlockNum: BlockNumber(block)}
	bufID, err := pool.ReadBuffer(tag)
	if err != nil {
		t.Fatalf("seedBtreePage ReadBuffer block=%d: %v", block, err)
	}
	pg, err := pool.GetPageForWrite(bufID)
	if err != nil {
		pool.UnpinBuffer(bufID)
		t.Fatalf("seedBtreePage GetPageForWrite: %v", err)
	}
	fresh := NewBTreePage(pt)
	copy(pg.Bytes(), fresh.Page().Bytes())
	pool.UnpinBuffer(bufID) //nolint:errcheck
}

// replayBtree builds a WAL segment from recs, runs the redo engine with
// a fresh WalPageStore, and returns (pool, store) for subsequent assertions.
func replayBtree(t *testing.T, recs []*wal.Record) (*BufferPool, *WalPageStore) {
	t.Helper()
	pool := NewBufferPool(32)
	store := NewWalPageStore(pool, nil)

	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)
	b := wal.NewSegmentBuilder(tli, segStart, 1)
	for i, rec := range recs {
		data, err := wal.Encode(rec)
		if err != nil {
			t.Fatalf("Encode record %d: %v", i, err)
		}
		if _, err := b.AppendRecord(data); err != nil {
			t.Fatalf("AppendRecord %d: %v", i, err)
		}
	}

	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())
	engine := wal.NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	engine.SetStore(store)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return pool, store
}

// getBtreePage returns a copy of the BTreePage at block after replay.
func getBtreePage(t *testing.T, pool *BufferPool, store *WalPageStore, block uint32) *BTreePage {
	t.Helper()
	relOid := store.relOid(btreeReln)
	tag := BufferTag{RelationId: relOid, Fork: ForkMain, BlockNum: BlockNumber(block)}
	bufID, err := pool.ReadBuffer(tag)
	if err != nil {
		t.Fatalf("getBtreePage ReadBuffer: %v", err)
	}
	defer pool.UnpinBuffer(bufID) //nolint:errcheck
	pg, err := pool.GetPage(bufID)
	if err != nil {
		t.Fatalf("getBtreePage GetPage: %v", err)
	}
	pgCopy := *pg
	bp, err := BTreePageFromPage(&pgCopy)
	if err != nil {
		t.Fatalf("BTreePageFromPage: %v", err)
	}
	return bp
}

// makeBtreeInsertRecord builds a XLOG_BTREE_INSERT_LEAF record for a single
// index tuple being inserted at offnum on the given block.
func makeBtreeInsertRecord(block uint32, offnum uint16, tupleData []byte, initPage bool) *wal.Record {
	mainData := make([]byte, 2)
	binary.LittleEndian.PutUint16(mainData, offnum)

	info := uint8(0x00) // XLOG_BTREE_INSERT_LEAF
	if initPage {
		info |= 0x08 // XLOG_BTREE_INIT_PAGE (bit 3, low nibble)
	}
	return &wal.Record{
		Header: wal.XLogRecord{XlRmid: wal.RmgrBtree, XlInfo: info},
		BlockRefs: []wal.BlockRef{
			{ID: 0, Reln: btreeReln, ForkNum: wal.ForkMain, BlockNum: block, Data: tupleData},
		},
		MainData: mainData,
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestBtreeWalInsertLeaf(t *testing.T) {
	key := []byte{0x00, 0x00, 0x00, 0x05} // big-endian 5
	tup := encIndexTuple(t, 10, 1, key)

	pool := NewBufferPool(16)
	store := NewWalPageStore(pool, nil)
	seedBtreePage(t, pool, store, 1, BTreeLeaf)

	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)
	b := wal.NewSegmentBuilder(tli, segStart, 1)
	rec := makeBtreeInsertRecord(1, 1, tup, false)
	data, _ := wal.Encode(rec)
	b.AppendRecord(data)

	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())
	engine := wal.NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	engine.SetStore(store)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	bp := getBtreePage(t, pool, store, 1)
	if bp.NumEntries() != 1 {
		t.Fatalf("NumEntries: got %d want 1", bp.NumEntries())
	}
	gotKey, gotBlock, _, err := bp.GetEntry(0)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Errorf("key: got %v want %v", gotKey, key)
	}
	if gotBlock != 10 {
		t.Errorf("heap block: got %d want 10", gotBlock)
	}
	if bp.Page().LSN() == 0 {
		t.Error("page LSN not updated")
	}
}

func TestBtreeWalInsertMultiple(t *testing.T) {
	// Insert 3 keys in ascending order; sorted B-tree layout must hold.
	keys := [][]byte{
		{0, 0, 0, 1},
		{0, 0, 0, 2},
		{0, 0, 0, 3},
	}

	pool := NewBufferPool(16)
	store := NewWalPageStore(pool, nil)
	seedBtreePage(t, pool, store, 1, BTreeLeaf)

	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)
	b := wal.NewSegmentBuilder(tli, segStart, 1)
	for i, k := range keys {
		tup := encIndexTuple(t, BlockNumber(i+10), OffsetNumber(i+1), k)
		rec := makeBtreeInsertRecord(1, uint16(i+1), tup, false)
		data, _ := wal.Encode(rec)
		b.AppendRecord(data)
	}

	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())
	engine := wal.NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	engine.SetStore(store)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	bp := getBtreePage(t, pool, store, 1)
	if bp.NumEntries() != 3 {
		t.Fatalf("NumEntries: got %d want 3", bp.NumEntries())
	}
	for i, wantKey := range keys {
		gotKey, _, _, err := bp.GetEntry(i)
		if err != nil {
			t.Fatalf("GetEntry(%d): %v", i, err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("entry[%d] key: got %v want %v", i, gotKey, wantKey)
		}
	}
}

func TestBtreeWalInsertWithInitPage(t *testing.T) {
	// XLOG_BTREE_INIT_PAGE: page must be initialised before insert.
	// No pre-seeding: the initPage flag covers initialisation.
	key := []byte{0x00, 0x00, 0x00, 0x07}
	tup := encIndexTuple(t, 20, 1, key)

	pool := NewBufferPool(16)
	store := NewWalPageStore(pool, nil)

	// Pre-allocate the block (uninitialised raw buffer).
	relOid := store.relOid(btreeReln)
	tag := BufferTag{RelationId: relOid, Fork: ForkMain, BlockNum: 5}
	bufID, _ := pool.ReadBuffer(tag)
	pool.UnpinBuffer(bufID) //nolint:errcheck

	rec := makeBtreeInsertRecord(5, 1, tup, true /*initPage*/)
	_, store2 := replayBtree(t, []*wal.Record{rec})
	// replayBtree creates its own pool; use our manually-seeded one instead.
	_ = store2

	// Use the original store that owns the pre-allocated block.
	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)
	b := wal.NewSegmentBuilder(tli, segStart, 1)
	data, _ := wal.Encode(rec)
	b.AppendRecord(data)
	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())
	engine := wal.NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	engine.SetStore(store)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	bp := getBtreePage(t, pool, store, 5)
	if bp.NumEntries() != 1 {
		t.Fatalf("NumEntries: got %d want 1", bp.NumEntries())
	}
	if !bp.IsLeaf() {
		t.Error("expected leaf page")
	}
}

func TestBtreeWalSplitRight(t *testing.T) {
	// Simulate a SPLIT_R: left page already has entries; right page is new.
	// We construct three items for the right page and verify they land there.

	type entry struct {
		key   []byte
		block BlockNumber
		off   OffsetNumber
	}
	rightEntries := []entry{
		{[]byte{0, 0, 0, 4}, 14, 1},
		{[]byte{0, 0, 0, 5}, 15, 1},
		{[]byte{0, 0, 0, 6}, 16, 1},
	}

	// Build packed rightItems bytes.
	var rightItems []byte
	for _, e := range rightEntries {
		rightItems = append(rightItems, encIndexTuple(t, e.block, e.off, e.key)...)
	}

	// xl_btree_split main data (10 bytes).
	mainData := make([]byte, 10)
	binary.LittleEndian.PutUint32(mainData[0:], 0) // level = 0 (leaf)
	binary.LittleEndian.PutUint16(mainData[4:], 4) // firstrightoff
	binary.LittleEndian.PutUint16(mainData[6:], 0) // newitemoff
	binary.LittleEndian.PutUint16(mainData[8:], 0) // postingoff

	const leftBlock = uint32(1)
	const rightBlock = uint32(2)

	// XLOG_BTREE_SPLIT_R = 0x40
	rec := &wal.Record{
		Header: wal.XLogRecord{XlRmid: wal.RmgrBtree, XlInfo: 0x40},
		BlockRefs: []wal.BlockRef{
			{ID: 0, Reln: btreeReln, ForkNum: wal.ForkMain, BlockNum: leftBlock},
			{ID: 1, Reln: btreeReln, ForkNum: wal.ForkMain, BlockNum: rightBlock, Data: rightItems},
		},
		MainData: mainData,
	}

	pool := NewBufferPool(32)
	store := NewWalPageStore(pool, nil)

	// Pre-seed left page (already has entries from prior inserts).
	seedBtreePage(t, pool, store, leftBlock, BTreeLeaf)
	// Pre-allocate right block.
	relOid := store.relOid(btreeReln)
	rtag := BufferTag{RelationId: relOid, Fork: ForkMain, BlockNum: rightBlock}
	rbuf, _ := pool.ReadBuffer(rtag)
	pool.UnpinBuffer(rbuf) //nolint:errcheck

	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)
	b := wal.NewSegmentBuilder(tli, segStart, 1)
	data, _ := wal.Encode(rec)
	b.AppendRecord(data)
	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())
	engine := wal.NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	engine.SetStore(store)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify right page contents.
	rightBP := getBtreePage(t, pool, store, rightBlock)
	if !rightBP.IsLeaf() {
		t.Error("right page should be a leaf")
	}
	if rightBP.NumEntries() != len(rightEntries) {
		t.Fatalf("right page NumEntries: got %d want %d", rightBP.NumEntries(), len(rightEntries))
	}
	for i, e := range rightEntries {
		gotKey, gotBlock, _, err := rightBP.GetEntry(i)
		if err != nil {
			t.Fatalf("GetEntry(%d): %v", i, err)
		}
		if !bytes.Equal(gotKey, e.key) {
			t.Errorf("right entry[%d] key: got %v want %v", i, gotKey, e.key)
		}
		if gotBlock != e.block {
			t.Errorf("right entry[%d] block: got %d want %d", i, gotBlock, e.block)
		}
	}

	// Verify sibling chain: right.prev == leftBlock.
	o := rightBP.Opaque()
	if o.BtpoPrev != BlockNumber(leftBlock) {
		t.Errorf("right.BtpoPrev: got %d want %d", o.BtpoPrev, leftBlock)
	}
	if o.BtpoNext != InvalidBlockNumber {
		t.Errorf("right.BtpoNext: got %d want InvalidBlockNumber", o.BtpoNext)
	}
}

func TestBtreeWalIdempotent(t *testing.T) {
	// Replaying a btree insert twice must not double-insert the key.
	key := []byte{0, 0, 0, 9}
	tup := encIndexTuple(t, 99, 1, key)

	pool := NewBufferPool(16)
	store := NewWalPageStore(pool, nil)
	seedBtreePage(t, pool, store, 1, BTreeLeaf)

	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)

	buildAndRun := func() {
		b := wal.NewSegmentBuilder(tli, segStart, 1)
		rec := makeBtreeInsertRecord(1, 1, tup, false)
		data, _ := wal.Encode(rec)
		b.AppendRecord(data)
		provider := wal.NewInMemoryProvider()
		provider.Add(segStart, b.Bytes())
		engine := wal.NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
		engine.SetStore(store)
		if err := engine.Run(); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}

	buildAndRun()
	buildAndRun() // second replay — must be idempotent

	bp := getBtreePage(t, pool, store, 1)
	if bp.NumEntries() != 1 {
		t.Errorf("idempotency failed: NumEntries = %d want 1", bp.NumEntries())
	}
}
