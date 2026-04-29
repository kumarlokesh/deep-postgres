package wal

import (
	"encoding/binary"
	"testing"
)

// ── decodeXLHeapMultiInsert ────────────────────────────────────────────────────

func TestDecodeXLHeapMultiInsert(t *testing.T) {
	// flags=0x02, pad=0x00, ntuples=3 (little-endian)
	data := []byte{0x02, 0x00, 0x03, 0x00}
	got, err := decodeXLHeapMultiInsert(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Flags != 0x02 {
		t.Errorf("Flags: got %d want 2", got.Flags)
	}
	if got.NTuples != 3 {
		t.Errorf("NTuples: got %d want 3", got.NTuples)
	}
}

func TestDecodeXLHeapMultiInsertTooShort(t *testing.T) {
	_, err := decodeXLHeapMultiInsert([]byte{0x00, 0x00})
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

// ── parseMultiInsertTuples ────────────────────────────────────────────────────

// buildMultiInsertBlockData encodes a slice of (infomask2, infomask, data) triples
// into the SHORTALIGN-padded wire format that PostgreSQL writes to BlockRef[0].Data.
func buildMultiInsertBlockData(tuples []struct {
	im2, im uint16
	data    []byte
}) []byte {
	var out []byte
	for _, tup := range tuples {
		// SHORTALIGN before each entry.
		if len(out)%2 != 0 {
			out = append(out, 0x00)
		}
		entry := make([]byte, xlMultiInsertTupleHeaderSize+len(tup.data))
		binary.LittleEndian.PutUint16(entry[0:], uint16(len(tup.data)))
		binary.LittleEndian.PutUint16(entry[2:], tup.im2)
		binary.LittleEndian.PutUint16(entry[4:], tup.im)
		entry[6] = heap2TupleHeaderSize // t_hoff
		copy(entry[xlMultiInsertTupleHeaderSize:], tup.data)
		out = append(out, entry...)
	}
	return out
}

func TestParseMultiInsertTuples(t *testing.T) {
	input := []struct {
		im2, im uint16
		data    []byte
	}{
		{0x0001, 0x0800, []byte("hello")},
		{0x0002, 0x0800, []byte("world!")},
	}
	blockData := buildMultiInsertBlockData(input)
	xid := uint32(42)
	block := uint32(7)

	tuples, err := parseMultiInsertTuples(xid, block, uint16(len(input)), blockData)
	if err != nil {
		t.Fatalf("parseMultiInsertTuples: %v", err)
	}
	if len(tuples) != len(input) {
		t.Fatalf("got %d tuples want %d", len(tuples), len(input))
	}

	for i, tup := range tuples {
		if len(tup) < heap2TupleHeaderSize {
			t.Fatalf("tuple %d too short: %d bytes", i, len(tup))
		}
		gotXmin := binary.LittleEndian.Uint32(tup[0:])
		if gotXmin != xid {
			t.Errorf("tuple %d: TXmin=%d want %d", i, gotXmin, xid)
		}
		gotBlock := binary.LittleEndian.Uint32(tup[12:])
		if gotBlock != block {
			t.Errorf("tuple %d: TCtidBlock=%d want %d", i, gotBlock, block)
		}
		gotIM2 := binary.LittleEndian.Uint16(tup[18:])
		if gotIM2 != input[i].im2 {
			t.Errorf("tuple %d: TInfomask2=0x%04x want 0x%04x", i, gotIM2, input[i].im2)
		}
		data := tup[heap2TupleHeaderSize:]
		if string(data) != string(input[i].data) {
			t.Errorf("tuple %d: data=%q want %q", i, data, input[i].data)
		}
	}
}

func TestParseMultiInsertTuplesTruncated(t *testing.T) {
	// Header present but data missing.
	entry := make([]byte, xlMultiInsertTupleHeaderSize)
	binary.LittleEndian.PutUint16(entry[0:], 100) // datalen=100 but no data follows
	_, err := parseMultiInsertTuples(1, 0, 1, entry)
	if err == nil {
		t.Fatal("expected error for truncated tuple data")
	}
}

// ── decodeXLHeapPrune ─────────────────────────────────────────────────────────

func TestDecodeXLHeapPrune(t *testing.T) {
	// latestRemovedXid=5, nredirected=2, ndead=3
	data := make([]byte, 12)
	binary.LittleEndian.PutUint32(data[0:], 5)
	binary.LittleEndian.PutUint16(data[4:], 2)
	binary.LittleEndian.PutUint16(data[6:], 3)

	got, err := decodeXLHeapPrune(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.NRedirected != 2 {
		t.Errorf("NRedirected: got %d want 2", got.NRedirected)
	}
	if got.NDead != 3 {
		t.Errorf("NDead: got %d want 3", got.NDead)
	}
}

func TestParsePruneOffsets(t *testing.T) {
	// 1 redirect (from=2, to=5) and 2 dead (offsets 3, 4).
	data := make([]byte, 4+4) // 4B redirect + 4B dead
	binary.LittleEndian.PutUint16(data[0:], 2) // from
	binary.LittleEndian.PutUint16(data[2:], 5) // to
	binary.LittleEndian.PutUint16(data[4:], 3) // dead[0]
	binary.LittleEndian.PutUint16(data[6:], 4) // dead[1]

	redirects, dead, err := parsePruneOffsets(data, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(redirects) != 1 || redirects[0] != [2]uint16{2, 5} {
		t.Errorf("redirects: got %v want [{2 5}]", redirects)
	}
	if len(dead) != 2 || dead[0] != 3 || dead[1] != 4 {
		t.Errorf("dead: got %v want [3 4]", dead)
	}
}

// ── decodeXLHeapVacuumOffsets ─────────────────────────────────────────────────

func TestDecodeXLHeapVacuumOffsets(t *testing.T) {
	// noffsets=3, offsets=1,2,3
	data := make([]byte, 2+6)
	binary.LittleEndian.PutUint16(data[0:], 3)
	binary.LittleEndian.PutUint16(data[2:], 1)
	binary.LittleEndian.PutUint16(data[4:], 2)
	binary.LittleEndian.PutUint16(data[6:], 3)

	offsets, err := decodeXLHeapVacuumOffsets(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 3 || offsets[0] != 1 || offsets[1] != 2 || offsets[2] != 3 {
		t.Errorf("offsets: got %v want [1 2 3]", offsets)
	}
}

func TestDecodeXLHeapVacuumTooShort(t *testing.T) {
	_, err := decodeXLHeapVacuumOffsets([]byte{0x01})
	if err == nil {
		t.Fatal("expected error for truncated vacuum data")
	}
}

// ── heap2Redo with nil store ───────────────────────────────────────────────────

func TestHeap2RedoMultiInsertNilStore(t *testing.T) {
	blockData := buildMultiInsertBlockData([]struct {
		im2, im uint16
		data    []byte
	}{{0x0001, 0x0800, []byte("x")}})

	mainData := []byte{0x00, 0x00, 0x01, 0x00} // flags=0, pad=0, ntuples=1

	rec := &Record{
		Header: XLogRecord{
			XlRmid: RmgrHeap2,
			XlInfo: xlHeap2MultiInsert,
			XlXid:  10,
		},
		BlockRefs: []BlockRef{{
			ID:       0,
			Reln:     RelFileLocator{DbOid: 1, RelOid: 100},
			ForkNum:  ForkMain,
			BlockNum: 0,
			Data:     blockData,
		}},
		MainData: mainData,
	}

	ctx := RedoContext{Rec: rec, LSN: MakeLSN(0, 0x1000), Store: nil}
	if err := heap2Redo(ctx); err != nil {
		t.Fatalf("heap2Redo with nil store: %v", err)
	}
}

func TestHeap2RedoVisibleNilStore(t *testing.T) {
	// xl_heap_visible: snapshotConflictHorizon(4) + flags(1)
	mainData := []byte{0, 0, 0, 0, 0}
	rec := &Record{
		Header: XLogRecord{XlRmid: RmgrHeap2, XlInfo: xlHeap2Visible},
		BlockRefs: []BlockRef{{
			ID:       0,
			Reln:     RelFileLocator{DbOid: 1, RelOid: 100},
			ForkNum:  ForkMain,
			BlockNum: 0,
		}},
		MainData: mainData,
	}
	ctx := RedoContext{Rec: rec, LSN: MakeLSN(0, 0x2000), Store: nil}
	if err := heap2Redo(ctx); err != nil {
		t.Fatalf("heap2Redo visible nil store: %v", err)
	}
}

func TestHeap2RedoStubbedOps(t *testing.T) {
	for _, info := range []uint8{xlHeap2FreezePage, xlHeap2LockUpdated, xlHeap2NewCid, xlHeap2Rewrite} {
		rec := &Record{
			Header: XLogRecord{XlRmid: RmgrHeap2, XlInfo: info},
		}
		ctx := RedoContext{Rec: rec, LSN: MakeLSN(0, 0x3000)}
		if err := heap2Redo(ctx); err != nil {
			t.Errorf("heap2Redo info=0x%02x: unexpected error: %v", info, err)
		}
	}
}

func TestHeap2Identify(t *testing.T) {
	ops := Lookup(RmgrHeap2)
	if ops == nil {
		t.Fatal("RmgrHeap2 not registered")
	}
	for info, want := range map[uint8]string{
		xlHeap2MultiInsert: "Heap2/MULTI_INSERT",
		xlHeap2Visible:     "Heap2/VISIBLE",
		xlHeap2Prune:       "Heap2/PRUNE",
		xlHeap2Vacuum:      "Heap2/VACUUM",
		xlHeap2FreezePage:  "Heap2/FREEZE_PAGE",
	} {
		rec := &Record{Header: XLogRecord{XlRmid: RmgrHeap2, XlInfo: info}}
		got := ops.Identify(rec)
		if got != want {
			t.Errorf("Identify(0x%02x): got %q want %q", info, got, want)
		}
	}

	// MULTI_INSERT + INIT_PAGE
	rec := &Record{Header: XLogRecord{XlRmid: RmgrHeap2, XlInfo: xlHeap2MultiInsert | xlHeapInitPage}}
	got := ops.Identify(rec)
	if got != "Heap2/MULTI_INSERT+INIT" {
		t.Errorf("Identify with INIT_PAGE: got %q want %q", got, "Heap2/MULTI_INSERT+INIT")
	}
}
