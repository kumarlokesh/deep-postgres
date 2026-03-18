package wal

import (
	"errors"
	"testing"
)

// ── LSN arithmetic ────────────────────────────────────────────────────────────

func TestLSNString(t *testing.T) {
	cases := []struct {
		lsn  LSN
		want string
	}{
		{0, "00000000/00000000"},
		{MakeLSN(0, 0x1000000), "00000000/01000000"},
		{MakeLSN(1, 0), "00000001/00000000"},
		{MakeLSN(0xABCDEF01, 0x12345678), "ABCDEF01/12345678"},
	}
	for _, c := range cases {
		if got := c.lsn.String(); got != c.want {
			t.Errorf("LSN(%d).String() = %q, want %q", uint64(c.lsn), got, c.want)
		}
	}
}

func TestLSNAddSub(t *testing.T) {
	base := MakeLSN(0, 0x1000)
	advanced := base.Add(0x500)
	if advanced != base+0x500 {
		t.Fatalf("Add: got %s, want %s", advanced, (base + 0x500))
	}
	if diff := advanced.Sub(base); diff != 0x500 {
		t.Fatalf("Sub: got %d, want %d", diff, 0x500)
	}
}

func TestLSNSubUnderflowPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on Sub underflow")
		}
	}()
	MakeLSN(0, 100).Sub(MakeLSN(0, 200))
}

func TestXLByteToSeg(t *testing.T) {
	// LSN at start of segment 0
	seg, off := XLByteToSeg(0, WALSegSize)
	if seg != 0 || off != 0 {
		t.Fatalf("seg=%d off=%d", seg, off)
	}
	// LSN in the middle of segment 1
	lsn := LSN(WALSegSize + 42)
	seg, off = XLByteToSeg(lsn, WALSegSize)
	if seg != 1 || off != 42 {
		t.Fatalf("seg=%d off=%d", seg, off)
	}
}

// ── MaxAlignedSize ────────────────────────────────────────────────────────────

func TestMaxAlignedSize(t *testing.T) {
	cases := [][2]uint32{
		{24, 24},
		{25, 32},
		{32, 32},
		{33, 40},
		{0, 0},
		{1, 8},
	}
	for _, c := range cases {
		if got := MaxAlignedSize(c[0]); got != c[1] {
			t.Errorf("MaxAlignedSize(%d) = %d, want %d", c[0], got, c[1])
		}
	}
}

// ── Record encode/decode round-trip ──────────────────────────────────────────

func minimalRecord() *Record {
	return &Record{
		Header: XLogRecord{
			XlXid:  42,
			XlPrev: MakeLSN(0, 0),
			XlInfo: 0x10,
			XlRmid: RmgrXact,
		},
		LSN:      MakeLSN(0, 0x1000),
		MainData: []byte("hello"),
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	rec := minimalRecord()
	data, err := Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data, rec.LSN)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Header.XlXid != rec.Header.XlXid {
		t.Errorf("XlXid: got %d want %d", got.Header.XlXid, rec.Header.XlXid)
	}
	if got.Header.XlRmid != rec.Header.XlRmid {
		t.Errorf("XlRmid: got %d want %d", got.Header.XlRmid, rec.Header.XlRmid)
	}
	if string(got.MainData) != string(rec.MainData) {
		t.Errorf("MainData: got %q want %q", got.MainData, rec.MainData)
	}
	if len(got.BlockRefs) != 0 {
		t.Errorf("expected no block refs, got %d", len(got.BlockRefs))
	}
}

func TestEncodeDecodeNoMainData(t *testing.T) {
	rec := &Record{
		Header: XLogRecord{XlRmid: RmgrXlog},
		LSN:    MakeLSN(0, 0x100),
	}
	data, err := Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data, rec.LSN)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.MainData) != 0 {
		t.Errorf("expected empty MainData, got %d bytes", len(got.MainData))
	}
}

func TestEncodeDecodeLargeMainData(t *testing.T) {
	// > 255 bytes triggers the long main-data header (0xFF prefix)
	payload := make([]byte, 300)
	for i := range payload {
		payload[i] = byte(i)
	}
	rec := &Record{
		Header:   XLogRecord{XlRmid: RmgrHeap},
		LSN:      MakeLSN(1, 0),
		MainData: payload,
	}
	data, err := Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data, rec.LSN)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.MainData) != len(payload) {
		t.Fatalf("MainData length: got %d want %d", len(got.MainData), len(payload))
	}
	for i, b := range got.MainData {
		if b != payload[i] {
			t.Fatalf("MainData[%d]: got %d want %d", i, b, payload[i])
		}
	}
}

func TestEncodeDecodeBlockRef(t *testing.T) {
	reln := RelFileLocator{SpcOid: 1663, DbOid: 16384, RelOid: 25000}
	rec := &Record{
		Header: XLogRecord{XlRmid: RmgrHeap, XlInfo: 0x00},
		LSN:    MakeLSN(0, 0x2000),
		BlockRefs: []BlockRef{
			{
				ID:       0,
				Reln:     reln,
				ForkNum:  ForkMain,
				BlockNum: 7,
				Data:     []byte{0xDE, 0xAD, 0xBE, 0xEF},
			},
		},
		MainData: []byte("main"),
	}
	data, err := Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data, rec.LSN)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.BlockRefs) != 1 {
		t.Fatalf("BlockRefs count: got %d want 1", len(got.BlockRefs))
	}
	br := got.BlockRefs[0]
	if br.Reln != reln {
		t.Errorf("Reln: got %+v want %+v", br.Reln, reln)
	}
	if br.BlockNum != 7 {
		t.Errorf("BlockNum: got %d want 7", br.BlockNum)
	}
	if string(br.Data) != string(rec.BlockRefs[0].Data) {
		t.Errorf("BlockRef.Data: got %v want %v", br.Data, rec.BlockRefs[0].Data)
	}
	if string(got.MainData) != "main" {
		t.Errorf("MainData: got %q", got.MainData)
	}
}

func TestEncodeDecodeSameRelOptimisation(t *testing.T) {
	reln := RelFileLocator{SpcOid: 1663, DbOid: 16384, RelOid: 99999}
	rec := &Record{
		Header: XLogRecord{XlRmid: RmgrHeap},
		LSN:    MakeLSN(0, 0x3000),
		BlockRefs: []BlockRef{
			{ID: 0, Reln: reln, ForkNum: ForkMain, BlockNum: 1},
			{ID: 1, Reln: reln, ForkNum: ForkMain, BlockNum: 2},
		},
	}
	data, err := Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data, rec.LSN)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.BlockRefs) != 2 {
		t.Fatalf("BlockRefs count: got %d want 2", len(got.BlockRefs))
	}
	if got.BlockRefs[1].Reln != reln {
		t.Errorf("second block ref reln mismatch: %+v", got.BlockRefs[1].Reln)
	}
}

func TestCRCMismatch(t *testing.T) {
	rec := minimalRecord()
	data, err := Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Corrupt a body byte (past the header).
	data[XLogRecordHeaderSize] ^= 0xFF
	_, err = Decode(data, rec.LSN)
	if !errors.Is(err, ErrCRCMismatch) {
		t.Fatalf("expected ErrCRCMismatch, got %v", err)
	}
}

func TestDecodeRecordTooShort(t *testing.T) {
	_, err := Decode([]byte{0x01, 0x02}, MakeLSN(0, 0))
	if !errors.Is(err, ErrRecordTooShort) {
		t.Fatalf("expected ErrRecordTooShort, got %v", err)
	}
}

// ── Block image (full-page write) round-trip ──────────────────────────────────

func TestEncodeDecodeBlockImage(t *testing.T) {
	imgData := make([]byte, 4096)
	for i := range imgData {
		imgData[i] = byte(i & 0xFF)
	}
	reln := RelFileLocator{SpcOid: 1663, DbOid: 1, RelOid: 1234}
	rec := &Record{
		Header: XLogRecord{XlRmid: RmgrHeap},
		LSN:    MakeLSN(0, 0x4000),
		BlockRefs: []BlockRef{
			{
				ID:       0,
				Reln:     reln,
				ForkNum:  ForkMain,
				BlockNum: 3,
				Image: &BlockImage{
					Length:     4096,
					HoleOffset: 100,
					BimgInfo:   BimgHasHole | BimgApply,
					HoleLength: 200,
					Data:       imgData,
				},
			},
		},
	}
	data, err := Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data, rec.LSN)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.BlockRefs) != 1 {
		t.Fatalf("BlockRefs count: got %d", len(got.BlockRefs))
	}
	im := got.BlockRefs[0].Image
	if im == nil {
		t.Fatal("expected non-nil Image")
	}
	if im.Length != 4096 {
		t.Errorf("Length: got %d want 4096", im.Length)
	}
	if im.HoleOffset != 100 {
		t.Errorf("HoleOffset: got %d want 100", im.HoleOffset)
	}
	if im.HoleLength != 200 {
		t.Errorf("HoleLength: got %d want 200", im.HoleLength)
	}
	if len(im.Data) != len(imgData) {
		t.Fatalf("image data length: got %d want %d", len(im.Data), len(imgData))
	}
	for i, b := range im.Data {
		if b != imgData[i] {
			t.Fatalf("image data[%d]: got %d want %d", i, b, imgData[i])
		}
	}
}

// ── Page header encode/decode ─────────────────────────────────────────────────

func TestPageHeaderRoundTrip(t *testing.T) {
	h := XLogPageHeaderData{
		XlpMagic:    XLogPageMagic,
		XlpInfo:     XLPFirstIsContRecord,
		XlpTli:      3,
		XlpPageaddr: MakeLSN(0, WALPageSize),
		XlpRemLen:   128,
	}
	buf := make([]byte, XLogPageHeaderSize)
	EncodePageHeader(buf, &h)
	got, err := DecodePageHeader(buf)
	if err != nil {
		t.Fatalf("DecodePageHeader: %v", err)
	}
	if got != h {
		t.Errorf("mismatch:\n  got  %+v\n  want %+v", got, h)
	}
}

func TestLongPageHeaderRoundTrip(t *testing.T) {
	h := XLogLongPageHeaderData{
		XLogPageHeaderData: XLogPageHeaderData{
			XlpMagic:    XLogPageMagic,
			XlpInfo:     XLPLongHeader,
			XlpTli:      1,
			XlpPageaddr: 0,
			XlpRemLen:   0,
		},
		XlpSysid:      0xDEADBEEFCAFEBABE,
		XlpSegSize:    WALSegSize,
		XlpXlogBlcksz: WALPageSize,
	}
	buf := make([]byte, XLogLongPageHeaderSize)
	EncodeLongPageHeader(buf, &h)
	got, err := DecodeLongPageHeader(buf)
	if err != nil {
		t.Fatalf("DecodeLongPageHeader: %v", err)
	}
	if got != h {
		t.Errorf("mismatch:\n  got  %+v\n  want %+v", got, h)
	}
}

func TestDecodePageHeaderBadMagic(t *testing.T) {
	h := XLogPageHeaderData{
		XlpMagic:    0xDEAD, // wrong
		XlpInfo:     0,
		XlpTli:      1,
		XlpPageaddr: 0,
		XlpRemLen:   0,
	}
	buf := make([]byte, XLogPageHeaderSize)
	EncodePageHeader(buf, &h)
	_, err := DecodePageHeader(buf)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

// ── SegmentReader ─────────────────────────────────────────────────────────────

func buildOnePageSegment(t *testing.T, tli TimeLineID, sysid uint64, recs []*Record) ([]byte, []LSN) {
	t.Helper()
	segStart := MakeLSN(0, 0)
	b := NewSegmentBuilder(tli, segStart, sysid)
	lsns := make([]LSN, len(recs))
	for i, rec := range recs {
		data, err := Encode(rec)
		if err != nil {
			t.Fatalf("Encode record %d: %v", i, err)
		}
		lsn, err := b.AppendRecord(data)
		if err != nil {
			t.Fatalf("AppendRecord %d: %v", i, err)
		}
		lsns[i] = lsn
	}
	return b.Bytes(), lsns
}

func makeSimpleRecord(rmgr RmgrID, mainData []byte) *Record {
	return &Record{
		Header:   XLogRecord{XlRmid: rmgr},
		MainData: mainData,
	}
}

func TestSegmentReaderSinglePage(t *testing.T) {
	tli := TimeLineID(1)
	recs := []*Record{
		makeSimpleRecord(RmgrXact, []byte("commit")),
		makeSimpleRecord(RmgrHeap, []byte("insert")),
		makeSimpleRecord(RmgrXlog, []byte("checkpoint")),
	}
	segData, wantLSNs := buildOnePageSegment(t, tli, 12345, recs)
	segStart := MakeLSN(0, 0)

	r := NewSegmentReader(segData, segStart, tli)
	var gotLSNs []LSN
	var gotMains []string
	for {
		rec, err := r.Next()
		if errors.Is(err, ErrEndOfSegment) || errors.Is(err, nil) && rec == nil {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		gotLSNs = append(gotLSNs, rec.LSN)
		gotMains = append(gotMains, string(rec.MainData))
	}

	if len(gotLSNs) < len(wantLSNs) {
		t.Fatalf("read %d records, want at least %d", len(gotLSNs), len(wantLSNs))
	}
	for i := range wantLSNs {
		if gotLSNs[i] != wantLSNs[i] {
			t.Errorf("record %d LSN: got %s want %s", i, gotLSNs[i], wantLSNs[i])
		}
		if gotMains[i] != string(recs[i].MainData) {
			t.Errorf("record %d MainData: got %q want %q", i, gotMains[i], string(recs[i].MainData))
		}
	}
}

func TestSegmentReaderCrossPageRecord(t *testing.T) {
	// Build a record large enough to span a page boundary.
	// WALPageSize=8192, long header=40, so first page has 8192-40=8152 bytes.
	// A record with ~8500 bytes of main data will cross the page boundary.
	tli := TimeLineID(1)
	bigPayload := make([]byte, 8500)
	for i := range bigPayload {
		bigPayload[i] = byte(i & 0xFF)
	}
	recs := []*Record{
		makeSimpleRecord(RmgrHeap, bigPayload),
	}
	segData, wantLSNs := buildOnePageSegment(t, tli, 9999, recs)
	segStart := MakeLSN(0, 0)

	r := NewSegmentReader(segData, segStart, tli)
	rec, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec == nil {
		t.Fatal("got nil record")
	}
	if rec.LSN != wantLSNs[0] {
		t.Errorf("LSN: got %s want %s", rec.LSN, wantLSNs[0])
	}
	if len(rec.MainData) != len(bigPayload) {
		t.Fatalf("MainData length: got %d want %d", len(rec.MainData), len(bigPayload))
	}
	for i, b := range rec.MainData {
		if b != bigPayload[i] {
			t.Fatalf("MainData[%d]: got %d want %d", i, b, bigPayload[i])
		}
	}
}

// ── Rmgr registry ─────────────────────────────────────────────────────────────

func TestRmgrDispatchUnknown(t *testing.T) {
	rec := &Record{Header: XLogRecord{XlRmid: RmgrMax - 1}, LSN: MakeLSN(0, 0)}
	// RmgrMax-1 is not registered in tests; verify ErrUnknownRmgr.
	err := Dispatch(RedoContext{Rec: rec, LSN: rec.LSN})
	var unkErr ErrUnknownRmgr
	if !errors.As(err, &unkErr) {
		t.Fatalf("expected ErrUnknownRmgr, got %v", err)
	}
}

func TestRmgrDispatchUnimplemented(t *testing.T) {
	// Register a test rmgr with nil Redo to trigger ErrUnimplementedRedo.
	// Use a scratch ID that isn't otherwise registered.
	const testID RmgrID = RmgrMax - 1

	// Guard: only register once across test runs (registry is a package-level var).
	if Lookup(testID) == nil {
		Register(testID, RmgrOps{Name: "TestRmgr", Redo: nil})
	}

	rec := &Record{Header: XLogRecord{XlRmid: testID}, LSN: MakeLSN(0, 0)}
	err := Dispatch(RedoContext{Rec: rec, LSN: rec.LSN})
	var unimplErr ErrUnimplementedRedo
	if !errors.As(err, &unimplErr) {
		t.Fatalf("expected ErrUnimplementedRedo, got %v", err)
	}
}

// ── RedoEngine ────────────────────────────────────────────────────────────────

func TestRedoEngineAppliesRecords(t *testing.T) {
	tli := TimeLineID(1)
	segStart := MakeLSN(0, 0)

	// Register a simple test rmgr that records what it sees.
	const testApplyID RmgrID = 5 // RmgrClog — must not already be registered

	var applied []string
	if Lookup(testApplyID) == nil {
		Register(testApplyID, RmgrOps{
			Name: "TestApply",
			Redo: func(ctx RedoContext) error {
				applied = append(applied, string(ctx.Rec.MainData))
				return nil
			},
		})
	}

	recs := []*Record{
		{Header: XLogRecord{XlRmid: testApplyID}, MainData: []byte("A")},
		{Header: XLogRecord{XlRmid: testApplyID}, MainData: []byte("B")},
		{Header: XLogRecord{XlRmid: testApplyID}, MainData: []byte("C")},
	}

	b := NewSegmentBuilder(tli, segStart, 1)
	for i, rec := range recs {
		data, err := Encode(rec)
		if err != nil {
			t.Fatalf("Encode %d: %v", i, err)
		}
		if _, err := b.AppendRecord(data); err != nil {
			t.Fatalf("AppendRecord %d: %v", i, err)
		}
	}

	provider := NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())

	engine := NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	stats := engine.Stats()
	if stats.RecordsApplied < 3 {
		t.Errorf("RecordsApplied: got %d want >= 3", stats.RecordsApplied)
	}
	if stats.SegmentsRead != 1 {
		t.Errorf("SegmentsRead: got %d want 1", stats.SegmentsRead)
	}

	if len(applied) < 3 {
		t.Fatalf("applied %d records, want 3", len(applied))
	}
	want := []string{"A", "B", "C"}
	for i, w := range want {
		if applied[i] != w {
			t.Errorf("applied[%d] = %q, want %q", i, applied[i], w)
		}
	}
}

func TestRedoEngineSkipsUnknownRmgr(t *testing.T) {
	tli := TimeLineID(1)
	segStart := MakeLSN(0, 0)

	// Use an unregistered rmgr ID; 18 (RmgrCommitTs) is not registered by default.
	unknownID := RmgrID(18)
	if Lookup(unknownID) != nil {
		t.Skip("RmgrCommitTs is registered; skipping")
	}

	rec := &Record{Header: XLogRecord{XlRmid: unknownID}, MainData: []byte("x")}
	data, err := Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	b := NewSegmentBuilder(tli, segStart, 1)
	if _, err := b.AppendRecord(data); err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}

	provider := NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())

	engine := NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run should not fail on unknown rmgr, got: %v", err)
	}
	if engine.Stats().RecordsSkipped < 1 {
		t.Errorf("expected at least 1 skipped record")
	}
}

func TestRedoEngineEndLSN(t *testing.T) {
	tli := TimeLineID(1)
	segStart := MakeLSN(0, 0)

	const applyID RmgrID = 6 // RmgrMultixact
	var count int
	if Lookup(applyID) == nil {
		Register(applyID, RmgrOps{
			Name: "TestEndLSN",
			Redo: func(ctx RedoContext) error {
				count++
				return nil
			},
		})
	} else {
		t.Skip("RmgrMultixact already registered")
	}

	b := NewSegmentBuilder(tli, segStart, 1)
	var lsns []LSN
	for i := 0; i < 5; i++ {
		rec := &Record{Header: XLogRecord{XlRmid: applyID}, MainData: []byte{byte(i)}}
		data, err := Encode(rec)
		if err != nil {
			t.Fatalf("Encode %d: %v", i, err)
		}
		lsn, err := b.AppendRecord(data)
		if err != nil {
			t.Fatalf("AppendRecord %d: %v", i, err)
		}
		lsns = append(lsns, lsn)
	}

	provider := NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())

	// Replay only up to (but not including) lsns[2].
	endLSN := lsns[2]
	engine := NewRedoEngine(tli, segStart, endLSN, provider.Provide, nil)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if count != 2 {
		t.Errorf("applied %d records, want exactly 2 (endLSN=%s)", count, endLSN)
	}
}

func TestRedoEngineProgressCallback(t *testing.T) {
	tli := TimeLineID(1)
	segStart := MakeLSN(0, 0)

	const progressID RmgrID = 7 // RmgrRelMap
	if Lookup(progressID) == nil {
		Register(progressID, RmgrOps{
			Name: "TestProgress",
			Redo: func(ctx RedoContext) error { return nil },
		})
	}

	b := NewSegmentBuilder(tli, segStart, 1)
	for i := 0; i < 3; i++ {
		rec := &Record{Header: XLogRecord{XlRmid: progressID}, MainData: []byte{byte(i)}}
		data, _ := Encode(rec)
		b.AppendRecord(data)
	}

	provider := NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())

	var progressLSNs []LSN
	onProgress := func(rec *Record) {
		progressLSNs = append(progressLSNs, rec.LSN)
	}

	engine := NewRedoEngine(tli, segStart, 0, provider.Provide, onProgress)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(progressLSNs) < 3 {
		t.Errorf("progress called %d times, want >= 3", len(progressLSNs))
	}
}

func TestRedoEngineMultipleSegments(t *testing.T) {
	tli := TimeLineID(1)

	const multiID RmgrID = 8 // RmgrStandby
	var multiApplied int
	if Lookup(multiID) == nil {
		Register(multiID, RmgrOps{
			Name: "TestMultiSeg",
			Redo: func(ctx RedoContext) error {
				multiApplied++
				return nil
			},
		})
	}

	provider := NewInMemoryProvider()

	// Two segments; each with 2 records.
	seg0Start := MakeLSN(0, 0)
	seg1Start := LSN(WALSegSize)

	for si, segStart := range []LSN{seg0Start, seg1Start} {
		b := NewSegmentBuilder(tli, segStart, 1)
		for i := 0; i < 2; i++ {
			rec := &Record{Header: XLogRecord{XlRmid: multiID}, MainData: []byte{byte(si*2 + i)}}
			data, _ := Encode(rec)
			b.AppendRecord(data)
		}
		provider.Add(segStart, b.Bytes())
	}

	engine := NewRedoEngine(tli, seg0Start, 0, provider.Provide, nil)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if multiApplied < 4 {
		t.Errorf("applied %d records across 2 segments, want >= 4", multiApplied)
	}
	if engine.Stats().SegmentsRead < 2 {
		t.Errorf("SegmentsRead: got %d want >= 2", engine.Stats().SegmentsRead)
	}
}
