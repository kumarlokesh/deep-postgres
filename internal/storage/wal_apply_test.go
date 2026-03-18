package storage

// End-to-end WAL → storage integration tests.
//
// Each test:
//   1. Encodes a WAL record (using the wal package encoder).
//   2. Builds an in-memory WAL segment via wal.SegmentBuilder.
//   3. Runs wal.RedoEngine with a WalPageStore backed by a fresh BufferPool.
//   4. Verifies the expected page state through the normal storage API.

import (
	"bytes"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/wal"
)

// testReln is a stable RelFileLocator used across tests.
var testReln = wal.RelFileLocator{SpcOid: 0, DbOid: 1, RelOid: 100}

// buildAndReplay is a helper that:
//   - pre-seeds the buffer pool with an empty page at (testReln, ForkMain, blockNum),
//   - encodes each record and appends it to a SegmentBuilder,
//   - runs the redo engine with a WalPageStore,
//   - returns the Page for further assertions.
func buildAndReplay(t *testing.T, blockNum uint32, recs []*wal.Record) *Page {
	t.Helper()

	pool := NewBufferPool(16)
	store := NewWalPageStore(pool, nil)

	// Pre-seed: allocate the target block in the pool so ReadBuffer succeeds
	// without a storage manager.  We register a synthetic relOid and pre-load
	// a clean page into the pool.
	relOid := store.relOid(testReln)
	tag := BufferTag{RelationId: relOid, Fork: ForkMain, BlockNum: blockNum}
	bufID, err := pool.ReadBuffer(tag)
	if err != nil {
		t.Fatalf("pre-seed ReadBuffer: %v", err)
	}
	// Init the page so it has a valid header.
	pg, err := pool.GetPageForWrite(bufID)
	if err != nil {
		t.Fatalf("pre-seed GetPageForWrite: %v", err)
	}
	pg.init()
	pool.UnpinBuffer(bufID) //nolint:errcheck

	// Build WAL segment.
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

	// Read back the page.
	bufID, err = pool.ReadBuffer(tag)
	if err != nil {
		t.Fatalf("ReadBuffer after redo: %v", err)
	}
	defer pool.UnpinBuffer(bufID) //nolint:errcheck
	finalPage, err := pool.GetPage(bufID)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	// Return a copy so the caller is not affected by pool eviction.
	snapshot := *finalPage
	return &snapshot
}

// makeHeapInsertRecord builds a XLOG_HEAP_INSERT wal.Record for the given
// tuple bytes at offnum on (testReln, ForkMain, blockNum).
func makeHeapInsertRecord(blockNum uint32, offnum uint16, tupleData []byte, recLSN wal.LSN) *wal.Record { //nolint:unparam
	// xl_heap_insert main data: offnum (uint16) + flags (uint8).
	mainData := []byte{
		byte(offnum), byte(offnum >> 8), // little-endian uint16
		0x00, // flags
	}
	return &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrHeap,
			XlInfo: 0x00, // XLOG_HEAP_INSERT, no INIT_PAGE
		},
		LSN: recLSN,
		BlockRefs: []wal.BlockRef{
			{
				ID:       0,
				Reln:     testReln,
				ForkNum:  wal.ForkMain,
				BlockNum: blockNum,
				Data:     tupleData,
			},
		},
		MainData: mainData,
	}
}

// makeHeapDeleteRecord builds a XLOG_HEAP_DELETE wal.Record for offnum on
// (testReln, ForkMain, blockNum).
func makeHeapDeleteRecord(blockNum uint32, offnum uint16, xmax uint32, recLSN wal.LSN) *wal.Record {
	// xl_heap_delete: xmax(4) + offnum(2) + infobits(1) + flags(1) = 8 bytes.
	mainData := make([]byte, 8)
	mainData[0] = byte(xmax)
	mainData[1] = byte(xmax >> 8)
	mainData[2] = byte(xmax >> 16)
	mainData[3] = byte(xmax >> 24)
	mainData[4] = byte(offnum)
	mainData[5] = byte(offnum >> 8)
	// infobits, flags = 0
	return &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrHeap,
			XlInfo: 0x10, // XLOG_HEAP_DELETE
		},
		LSN: recLSN,
		BlockRefs: []wal.BlockRef{
			{
				ID:       0,
				Reln:     testReln,
				ForkNum:  wal.ForkMain,
				BlockNum: blockNum,
			},
		},
		MainData: mainData,
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestWalApplyInsert(t *testing.T) {
	tuple := []byte("hello world tuple")
	lsn := wal.MakeLSN(0, 0x1000)

	rec := makeHeapInsertRecord(0, 1, tuple, lsn)
	page := buildAndReplay(t, 0, []*wal.Record{rec})

	if page.ItemCount() != 1 {
		t.Fatalf("ItemCount: got %d want 1", page.ItemCount())
	}
	got, err := page.GetTuple(0)
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if !bytes.Equal(got, tuple) {
		t.Errorf("tuple mismatch:\n  got  %q\n  want %q", got, tuple)
	}
	if page.LSN() == 0 {
		t.Error("page LSN not updated after redo")
	}
}

func TestWalApplyMultipleInserts(t *testing.T) {
	tuples := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}
	recs := make([]*wal.Record, len(tuples))
	for i, tup := range tuples {
		lsn := wal.MakeLSN(0, uint32(0x1000+i*0x100))
		recs[i] = makeHeapInsertRecord(0, uint16(i+1), tup, lsn)
	}

	page := buildAndReplay(t, 0, recs)

	if page.ItemCount() != len(tuples) {
		t.Fatalf("ItemCount: got %d want %d", page.ItemCount(), len(tuples))
	}
	for i, want := range tuples {
		got, err := page.GetTuple(i)
		if err != nil {
			t.Fatalf("GetTuple(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("tuple[%d]: got %q want %q", i, got, want)
		}
	}
}

func TestWalApplyInsertThenDelete(t *testing.T) {
	tuple := []byte("to be deleted")

	insertLSN := wal.MakeLSN(0, 0x1000)
	deleteLSN := wal.MakeLSN(0, 0x2000)

	insertRec := makeHeapInsertRecord(0, 1, tuple, insertLSN)
	deleteRec := makeHeapDeleteRecord(0, 1, 42 /*xmax*/, deleteLSN)

	page := buildAndReplay(t, 0, []*wal.Record{insertRec, deleteRec})

	if page.ItemCount() != 1 {
		t.Fatalf("ItemCount: got %d want 1", page.ItemCount())
	}
	// After delete, the line pointer should be LpDead.
	id, err := page.GetItemId(0)
	if err != nil {
		t.Fatalf("GetItemId: %v", err)
	}
	if !id.IsDead() {
		t.Errorf("line pointer should be LpDead after delete, got flags=%d", id.Flags())
	}
}

func TestWalApplyIdempotent(t *testing.T) {
	// Replaying the same record twice must not double-insert.
	tuple := []byte("idempotent test")
	lsn := wal.MakeLSN(0, 0x1000)

	// Build a segment with the same record encoded twice.
	rec := makeHeapInsertRecord(0, 1, tuple, lsn)

	pool := NewBufferPool(16)
	store := NewWalPageStore(pool, nil)
	relOid := store.relOid(testReln)
	tag := BufferTag{RelationId: relOid, Fork: ForkMain, BlockNum: 0}

	// Pre-seed.
	bufID, _ := pool.ReadBuffer(tag)
	pg, _ := pool.GetPageForWrite(bufID)
	pg.init()
	pool.UnpinBuffer(bufID) //nolint:errcheck

	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)

	// First replay.
	b1 := wal.NewSegmentBuilder(tli, segStart, 1)
	data, _ := wal.Encode(rec)
	b1.AppendRecord(data)
	p1 := wal.NewInMemoryProvider()
	p1.Add(segStart, b1.Bytes())
	e1 := wal.NewRedoEngine(tli, segStart, 0, p1.Provide, nil)
	e1.SetStore(store)
	if err := e1.Run(); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Second replay of the same segment — page LSN >= record LSN → skip.
	b2 := wal.NewSegmentBuilder(tli, segStart, 1)
	b2.AppendRecord(data)
	p2 := wal.NewInMemoryProvider()
	p2.Add(segStart, b2.Bytes())
	e2 := wal.NewRedoEngine(tli, segStart, 0, p2.Provide, nil)
	e2.SetStore(store)
	if err := e2.Run(); err != nil {
		t.Fatalf("second Run: %v", err)
	}

	bufID, _ = pool.ReadBuffer(tag)
	defer pool.UnpinBuffer(bufID) //nolint:errcheck
	finalPage, _ := pool.GetPage(bufID)

	if finalPage.ItemCount() != 1 {
		t.Errorf("idempotency failed: ItemCount = %d, want 1", finalPage.ItemCount())
	}
}

func TestWalApplyPageLSNUpdated(t *testing.T) {
	// The page LSN must reflect the last applied record's positional LSN.
	// Record.LSN is set during decode from the byte position in the segment —
	// not from the LSN field we populate before encoding.

	pool := NewBufferPool(16)
	store := NewWalPageStore(pool, nil)
	relOid := store.relOid(testReln)
	tag := BufferTag{RelationId: relOid, Fork: ForkMain, BlockNum: 0}

	bufID, _ := pool.ReadBuffer(tag)
	pg, _ := pool.GetPageForWrite(bufID)
	pg.init()
	pool.UnpinBuffer(bufID) //nolint:errcheck

	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)
	b := wal.NewSegmentBuilder(tli, segStart, 1)

	recs := []*wal.Record{
		makeHeapInsertRecord(0, 1, []byte("alpha"), 0),
		makeHeapInsertRecord(0, 2, []byte("beta"), 0),
	}
	var lastLSN wal.LSN
	for i, rec := range recs {
		data, _ := wal.Encode(rec)
		lsn, err := b.AppendRecord(data)
		if err != nil {
			t.Fatalf("AppendRecord %d: %v", i, err)
		}
		lastLSN = lsn
	}

	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())
	engine := wal.NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	engine.SetStore(store)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	bufID, _ = pool.ReadBuffer(tag)
	defer pool.UnpinBuffer(bufID) //nolint:errcheck
	finalPage, _ := pool.GetPage(bufID)

	if finalPage.LSN() != uint64(lastLSN) {
		t.Errorf("page LSN: got %d want %d", finalPage.LSN(), uint64(lastLSN))
	}
}
