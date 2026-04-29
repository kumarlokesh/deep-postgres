package storage

// Integration tests for HEAP2 WAL redo → WalPageStore.
//
// Each test builds a synthetic WAL record, replays it through RedoEngine +
// WalPageStore, and verifies the resulting page state via direct page APIs.

import (
	"encoding/binary"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/wal"
)

// buildMultiInsertBlockData encodes N tuples into the packed xl_multi_insert_tuple
// wire format that HEAP2/MULTI_INSERT stores in BlockRef[0].Data.
func buildMultiInsertBlockData(tuples [][]byte) []byte {
	var out []byte
	for _, data := range tuples {
		// SHORTALIGN
		if len(out)%2 != 0 {
			out = append(out, 0x00)
		}
		entry := make([]byte, 7+len(data))
		binary.LittleEndian.PutUint16(entry[0:], uint16(len(data)))
		binary.LittleEndian.PutUint16(entry[2:], 0x0001) // t_infomask2 = 1 attr
		binary.LittleEndian.PutUint16(entry[4:], 0x0800) // t_infomask = HEAP_XMAX_INVALID
		entry[6] = 24                                     // t_hoff
		copy(entry[7:], data)
		out = append(out, entry...)
	}
	return out
}

// makeHeap2MultiInsertRecord builds a XLOG_HEAP2_MULTI_INSERT wal.Record.
func makeHeap2MultiInsertRecord(xid uint32, blockNum uint32, tuplePayloads [][]byte, initPage bool, recLSN wal.LSN) *wal.Record {
	// xl_heap_multi_insert: flags(1) + pad(1) + ntuples(2) = 4 bytes
	mainData := []byte{0x00, 0x00, byte(len(tuplePayloads)), 0x00}
	info := uint8(0x50) // XLOG_HEAP2_MULTI_INSERT
	if initPage {
		info |= 0x80 // XLOG_HEAP_INIT_PAGE
	}
	return &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrHeap2,
			XlInfo: info,
			XlXid:  xid,
		},
		LSN: recLSN,
		BlockRefs: []wal.BlockRef{{
			ID:       0,
			Reln:     testReln,
			ForkNum:  wal.ForkMain,
			BlockNum: blockNum,
			Data:     buildMultiInsertBlockData(tuplePayloads),
		}},
		MainData: mainData,
	}
}

// TestWalApplyMultiInsert verifies that MULTI_INSERT places N tuples on the page.
func TestWalApplyMultiInsert(t *testing.T) {
	payloads := [][]byte{[]byte("tuple-a"), []byte("tuple-b"), []byte("tuple-c")}
	rec := makeHeap2MultiInsertRecord(100, 0, payloads, false, wal.MakeLSN(0, 0x1000))

	page := buildAndReplay(t, 0, []*wal.Record{rec})

	if got := page.ItemCount(); got != 3 {
		t.Fatalf("ItemCount: got %d want 3", got)
	}
	for i, want := range payloads {
		raw, err := page.GetTuple(i)
		if err != nil {
			t.Fatalf("GetTuple(%d): %v", i, err)
		}
		if len(raw) < HeapTupleHeaderSize {
			t.Fatalf("slot %d: raw too short (%d bytes)", i, len(raw))
		}
		gotData := raw[HeapTupleHeaderSize:]
		if string(gotData) != string(want) {
			t.Errorf("slot %d: data=%q want %q", i, gotData, want)
		}
	}
}

// TestWalApplyMultiInsertInitPage verifies MULTI_INSERT with XLOG_HEAP_INIT_PAGE
// initialises the page before inserting.
func TestWalApplyMultiInsertInitPage(t *testing.T) {
	payloads := [][]byte{[]byte("init-a"), []byte("init-b")}
	rec := makeHeap2MultiInsertRecord(101, 0, payloads, true, wal.MakeLSN(0, 0x2000))

	page := buildAndReplay(t, 0, []*wal.Record{rec})

	if got := page.ItemCount(); got != 2 {
		t.Fatalf("ItemCount after init-page multi_insert: got %d want 2", got)
	}
}

// TestWalApplyPrune verifies that PRUNE sets LP_REDIRECT and LP_DEAD.
func TestWalApplyPrune(t *testing.T) {
	// Pre-populate the page with 3 tuples, then prune:
	//   slot 1 → redirect to slot 2
	//   slot 3 → mark dead
	insertRec := makeHeapInsertRecord(0, 1, makeTupleBytes(50, "v1"), wal.MakeLSN(0, 0x100))
	insertRec2 := makeHeapInsertRecord(0, 2, makeTupleBytes(51, "v2"), wal.MakeLSN(0, 0x200))
	insertRec3 := makeHeapInsertRecord(0, 3, makeTupleBytes(52, "v3"), wal.MakeLSN(0, 0x300))

	// xl_heap_prune: latestRemovedXid(4) + nredirected(2) + ndead(2) = 8 bytes
	pruneMain := make([]byte, 8)
	binary.LittleEndian.PutUint32(pruneMain[0:], 50) // latestRemovedXid
	binary.LittleEndian.PutUint16(pruneMain[4:], 1)  // nredirected=1
	binary.LittleEndian.PutUint16(pruneMain[6:], 1)  // ndead=1

	// Block data: 1 redirect pair (from=1, to=2), 1 dead (offset=3)
	blockData := make([]byte, 6)
	binary.LittleEndian.PutUint16(blockData[0:], 1) // from=1
	binary.LittleEndian.PutUint16(blockData[2:], 2) // to=2
	binary.LittleEndian.PutUint16(blockData[4:], 3) // dead=3

	pruneRec := &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrHeap2,
			XlInfo: 0x10, // XLOG_HEAP2_PRUNE
		},
		LSN: wal.MakeLSN(0, 0x400),
		BlockRefs: []wal.BlockRef{{
			ID:       0,
			Reln:     testReln,
			ForkNum:  wal.ForkMain,
			BlockNum: 0,
			Data:     blockData,
		}},
		MainData: pruneMain,
	}

	page := buildAndReplay(t, 0, []*wal.Record{insertRec, insertRec2, insertRec3, pruneRec})

	// Slot 0 (offset 1) should be LP_REDIRECT → 2.
	lp0, err := page.GetItemId(0)
	if err != nil {
		t.Fatalf("GetItemId(0): %v", err)
	}
	if !lp0.IsRedirect() {
		t.Errorf("slot 0: expected LP_REDIRECT, got flags=%d", lp0.Flags())
	}
	if lp0.Off() != 2 {
		t.Errorf("slot 0: redirect target=%d want 2", lp0.Off())
	}

	// Slot 2 (offset 3) should be LP_DEAD.
	lp2, err := page.GetItemId(2)
	if err != nil {
		t.Fatalf("GetItemId(2): %v", err)
	}
	if !lp2.IsDead() {
		t.Errorf("slot 2: expected LP_DEAD, got flags=%d", lp2.Flags())
	}
}

// TestWalApplyVacuumDeadItems verifies that VACUUM removes the given offsets.
func TestWalApplyVacuumDeadItems(t *testing.T) {
	insertRec := makeHeapInsertRecord(0, 1, makeTupleBytes(60, "alive"), wal.MakeLSN(0, 0x100))
	insertRec2 := makeHeapInsertRecord(0, 2, makeTupleBytes(61, "dead"), wal.MakeLSN(0, 0x200))

	// xl_heap_vacuum: noffsets(2) + offset[0](2) = 4 bytes
	vacMain := make([]byte, 4)
	binary.LittleEndian.PutUint16(vacMain[0:], 1) // noffsets=1
	binary.LittleEndian.PutUint16(vacMain[2:], 2) // offset=2 (1-based)

	vacRec := &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrHeap2,
			XlInfo: 0x20, // XLOG_HEAP2_VACUUM
		},
		LSN: wal.MakeLSN(0, 0x300),
		BlockRefs: []wal.BlockRef{{
			ID:       0,
			Reln:     testReln,
			ForkNum:  wal.ForkMain,
			BlockNum: 0,
		}},
		MainData: vacMain,
	}

	page := buildAndReplay(t, 0, []*wal.Record{insertRec, insertRec2, vacRec})

	// Slot 0 (offset 1): LP_NORMAL — untouched.
	lp0, err := page.GetItemId(0)
	if err != nil {
		t.Fatalf("GetItemId(0): %v", err)
	}
	if !lp0.IsNormal() {
		t.Errorf("slot 0: expected LP_NORMAL, got flags=%d", lp0.Flags())
	}

	// Slot 1 (offset 2): LP_DEAD after vacuum.
	lp1, err := page.GetItemId(1)
	if err != nil {
		t.Fatalf("GetItemId(1): %v", err)
	}
	if !lp1.IsDead() {
		t.Errorf("slot 1: expected LP_DEAD after vacuum, got flags=%d", lp1.Flags())
	}
}

// TestWalApplySetVisible verifies that VISIBLE sets PD_ALL_VISIBLE on the page.
func TestWalApplySetVisible(t *testing.T) {
	insertRec := makeHeapInsertRecord(0, 1, makeTupleBytes(70, "vis"), wal.MakeLSN(0, 0x100))

	// xl_heap_visible: snapshotConflictHorizon(4) + flags(1) = 5 bytes
	visMain := make([]byte, 5)
	binary.LittleEndian.PutUint32(visMain[0:], 70) // cutoff xid

	visRec := &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrHeap2,
			XlInfo: 0x40, // XLOG_HEAP2_VISIBLE
		},
		LSN: wal.MakeLSN(0, 0x200),
		BlockRefs: []wal.BlockRef{{
			ID:       0,
			Reln:     testReln,
			ForkNum:  wal.ForkMain,
			BlockNum: 0,
		}},
		MainData: visMain,
	}

	page := buildAndReplay(t, 0, []*wal.Record{insertRec, visRec})

	if !page.HasFlag(PDAllVisible) {
		t.Error("expected PD_ALL_VISIBLE flag set after VISIBLE record")
	}
}

// TestWalApplyMultiInsertIdempotent verifies LSN-based idempotency by replaying
// the same WAL segment twice against the same store.
func TestWalApplyMultiInsertIdempotent(t *testing.T) {
	payloads := [][]byte{[]byte("once")}
	rec := makeHeap2MultiInsertRecord(100, 0, payloads, false, wal.MakeLSN(0, 0))

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

	data, err := wal.Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b := wal.NewSegmentBuilder(tli, segStart, 1)
	if _, err := b.AppendRecord(data); err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	segBytes := b.Bytes()

	for pass := 1; pass <= 2; pass++ {
		p := wal.NewInMemoryProvider()
		p.Add(segStart, segBytes)
		e := wal.NewRedoEngine(tli, segStart, 0, p.Provide, nil)
		e.SetStore(store)
		if err := e.Run(); err != nil {
			t.Fatalf("pass %d Run: %v", pass, err)
		}
	}

	bufID, _ = pool.ReadBuffer(tag)
	defer pool.UnpinBuffer(bufID) //nolint:errcheck
	finalPage, _ := pool.GetPage(bufID)
	if got := finalPage.ItemCount(); got != 1 {
		t.Fatalf("idempotency: ItemCount=%d want 1", got)
	}
}

// makeTupleBytes returns full HeapTuple bytes (header + payload) for xmin and data.
func makeTupleBytes(xmin uint32, payload string) []byte {
	data := []byte(payload)
	buf := make([]byte, HeapTupleHeaderSize+len(data))
	binary.LittleEndian.PutUint32(buf[0:], xmin) // TXmin
	binary.LittleEndian.PutUint32(buf[4:], 0)    // TXmax
	binary.LittleEndian.PutUint32(buf[8:], 0)    // TCid
	binary.LittleEndian.PutUint32(buf[12:], 0)   // TCtidBlock
	binary.LittleEndian.PutUint16(buf[16:], 0)   // TCtidOffset
	binary.LittleEndian.PutUint16(buf[18:], 0x0001)
	binary.LittleEndian.PutUint16(buf[20:], 0x0800) // HEAP_XMAX_INVALID
	buf[22] = HeapTupleHeaderSize
	copy(buf[HeapTupleHeaderSize:], data)
	return buf
}
