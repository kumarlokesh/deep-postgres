package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// ── ParseLSN ─────────────────────────────────────────────────────────────────

func TestParseLSNValid(t *testing.T) {
	cases := []struct {
		input string
		want  LSN
	}{
		{"0/0", MakeLSN(0, 0)},
		{"0/1000000", MakeLSN(0, 0x1000000)},
		{"1/0", MakeLSN(1, 0)},
		{"ABCDEF01/12345678", MakeLSN(0xABCDEF01, 0x12345678)},
		{"00000001/00400028", MakeLSN(1, 0x400028)},
	}
	for _, c := range cases {
		got, err := ParseLSN(c.input)
		if err != nil {
			t.Errorf("ParseLSN(%q): unexpected error: %v", c.input, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseLSN(%q) = %s, want %s", c.input, got, c.want)
		}
	}
}

func TestParseLSNInvalid(t *testing.T) {
	cases := []string{
		"",
		"notlsn",
		"1/",
		"/1",
		"0x0/0x0",
		"1-2",
	}
	for _, s := range cases {
		if _, err := ParseLSN(s); err == nil {
			t.Errorf("ParseLSN(%q): expected error, got nil", s)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// writeSegment writes a synthetic WAL segment for (tli, lsn) into dir,
// using SegmentBuilder so it has a valid long page header.
// Returns the file path and the segment start LSN.
func writeSegment(t *testing.T, dir string, tli TimeLineID, lsn LSN, sysid uint64) (string, LSN) {
	t.Helper()
	seg, _ := XLByteToSeg(lsn, WALSegSize)
	name := SegmentFileName(tli, seg, WALSegSize)
	segStart := LSN(seg * WALSegSize)
	b := NewSegmentBuilder(tli, segStart, sysid)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, b.Bytes(), 0644); err != nil {
		t.Fatalf("writeSegment: %v", err)
	}
	return path, segStart
}

// ── ListSegments ─────────────────────────────────────────────────────────────

func TestListSegmentsFiltersNonWAL(t *testing.T) {
	dir := t.TempDir()
	tli := TimeLineID(1)

	// Write two valid WAL segment files and some noise.
	writeSegment(t, dir, tli, 0, 1)
	writeSegment(t, dir, tli, LSN(WALSegSize), 1)
	os.WriteFile(filepath.Join(dir, "README"), []byte("ignore"), 0644)
	os.WriteFile(filepath.Join(dir, "00000001000000000000000Z"), []byte("bad hex"), 0644) // Z is not hex
	os.MkdirAll(filepath.Join(dir, "archive_status"), 0755)

	p := NewFileSegmentProvider(dir)
	names, err := p.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(names) != 2 {
		t.Errorf("got %d names, want 2: %v", len(names), names)
	}
}

func TestListSegmentsEmptyDir(t *testing.T) {
	dir := t.TempDir()
	p := NewFileSegmentProvider(dir)
	names, err := p.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("expected empty list, got %v", names)
	}
}

// ── Provide ───────────────────────────────────────────────────────────────────

func TestProvideReturnsSegment(t *testing.T) {
	dir := t.TempDir()
	tli := TimeLineID(1)

	// Write a segment containing one record.
	seg0 := LSN(0)
	_, segStart := writeSegment(t, dir, tli, seg0, 99)
	if segStart != 0 {
		t.Fatalf("unexpected segStart: %s", segStart)
	}

	p := NewFileSegmentProvider(dir)
	data, err := p.Provide(seg0, tli)
	if err != nil {
		t.Fatalf("Provide: %v", err)
	}
	if uint64(len(data)) != WALSegSize {
		t.Errorf("data length: got %d want %d", len(data), WALSegSize)
	}
	// Verify the long page header magic at offset 0.
	hdr, err := DecodeLongPageHeader(data[:XLogLongPageHeaderSize])
	if err != nil {
		t.Fatalf("DecodeLongPageHeader: %v", err)
	}
	if hdr.XlpMagic != XLogPageMagic {
		t.Errorf("magic: got 0x%04X want 0x%04X", hdr.XlpMagic, XLogPageMagic)
	}
}

func TestProvideEOFWhenMissing(t *testing.T) {
	dir := t.TempDir()
	p := NewFileSegmentProvider(dir)

	// No files in dir - should return io.EOF.
	_, err := p.Provide(0, TimeLineID(1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestProvideWrongSize(t *testing.T) {
	dir := t.TempDir()
	tli := TimeLineID(1)
	seg, _ := XLByteToSeg(0, WALSegSize)
	name := SegmentFileName(tli, seg, WALSegSize)
	// Write a file with wrong size.
	os.WriteFile(filepath.Join(dir, name), []byte("short"), 0644)

	p := NewFileSegmentProvider(dir)
	_, err := p.Provide(0, tli)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("expected size error, got %v", err)
	}
}

func TestProvideUsesSegmentSize(t *testing.T) {
	// Use a non-default segment size (1 MB) to verify SetSegmentSize is respected.
	const smallSeg = 1 * 1024 * 1024
	dir := t.TempDir()
	tli := TimeLineID(1)
	lsn := LSN(0)
	seg, _ := XLByteToSeg(lsn, smallSeg)
	name := SegmentFileName(tli, seg, smallSeg)
	data := make([]byte, smallSeg)
	os.WriteFile(filepath.Join(dir, name), data, 0644)

	p := NewFileSegmentProvider(dir)
	p.SetSegmentSize(smallSeg)
	if p.SegmentSize() != smallSeg {
		t.Fatalf("SegmentSize: got %d want %d", p.SegmentSize(), smallSeg)
	}

	got, err := p.Provide(lsn, tli)
	if err != nil {
		t.Fatalf("Provide: %v", err)
	}
	if uint64(len(got)) != smallSeg {
		t.Errorf("data length: got %d want %d", len(got), smallSeg)
	}
}

// ── DetectSegmentSize ─────────────────────────────────────────────────────────

func TestDetectSegmentSizeDefault(t *testing.T) {
	dir := t.TempDir()
	tli := TimeLineID(1)
	writeSegment(t, dir, tli, 0, 12345)

	p := NewFileSegmentProvider(dir)
	sz, err := p.DetectSegmentSize()
	if err != nil {
		t.Fatalf("DetectSegmentSize: %v", err)
	}
	if sz != WALSegSize {
		t.Errorf("size: got %d want %d", sz, WALSegSize)
	}
}

func TestDetectSegmentSizeEmptyDir(t *testing.T) {
	dir := t.TempDir()
	p := NewFileSegmentProvider(dir)
	sz, err := p.DetectSegmentSize()
	if err != nil {
		t.Fatalf("DetectSegmentSize empty dir: %v", err)
	}
	if sz != 0 {
		t.Errorf("expected 0 for empty dir, got %d", sz)
	}
}

// ── End-to-end: replay from a file-backed directory ──────────────────────────

// TestFileProviderRedoEngine writes two WAL segment files to a temp directory
// and drives a RedoEngine with FileSegmentProvider to replay all records.
func TestFileProviderRedoEngine(t *testing.T) {
	const e2eID RmgrID = 12 // RmgrHash - not registered by init() or other tests

	var applied [][]byte
	if Lookup(e2eID) == nil {
		Register(e2eID, RmgrOps{
			Name: "FileProviderE2E",
			Redo: func(ctx RedoContext) error {
				dst := make([]byte, len(ctx.Rec.MainData))
				copy(dst, ctx.Rec.MainData)
				applied = append(applied, dst)
				return nil
			},
		})
	}

	tli := TimeLineID(1)
	dir := t.TempDir()

	// Segment 0: three records.
	seg0Start := LSN(0)
	b0 := NewSegmentBuilder(tli, seg0Start, 1)
	want := []string{"alpha", "beta", "gamma"}
	for _, s := range want {
		rec := &Record{Header: XLogRecord{XlRmid: e2eID}, MainData: []byte(s)}
		data, err := Encode(rec)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if _, err := b0.AppendRecord(data); err != nil {
			t.Fatalf("AppendRecord: %v", err)
		}
	}
	seg0Name := SegmentFileName(tli, 0, WALSegSize)
	if err := os.WriteFile(filepath.Join(dir, seg0Name), b0.Bytes(), 0644); err != nil {
		t.Fatalf("WriteFile seg0: %v", err)
	}

	p := NewFileSegmentProvider(dir)
	engine := NewRedoEngine(tli, seg0Start, 0, p.Provide, nil)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	stats := engine.Stats()
	if stats.SegmentsRead != 1 {
		t.Errorf("SegmentsRead: got %d want 1", stats.SegmentsRead)
	}
	if int(stats.RecordsApplied) < len(want) {
		t.Errorf("RecordsApplied: got %d want >= %d", stats.RecordsApplied, len(want))
	}

	if len(applied) < len(want) {
		t.Fatalf("applied %d records, want %d", len(applied), len(want))
	}
	for i, s := range want {
		if string(applied[i]) != s {
			t.Errorf("applied[%d] = %q, want %q", i, applied[i], s)
		}
	}
}

// TestFileProviderMultiSegmentReplay writes two segment files and verifies that
// the engine crosses the segment boundary correctly.
func TestFileProviderMultiSegmentReplay(t *testing.T) {
	const multiFileID RmgrID = 13 // RmgrGin - not registered by init() or other tests

	var multiApplied int
	if Lookup(multiFileID) == nil {
		Register(multiFileID, RmgrOps{
			Name: "FileProviderMulti",
			Redo: func(ctx RedoContext) error {
				multiApplied++
				return nil
			},
		})
	}

	tli := TimeLineID(1)
	dir := t.TempDir()

	for segIdx := uint64(0); segIdx < 2; segIdx++ {
		segStart := LSN(segIdx * WALSegSize)
		b := NewSegmentBuilder(tli, segStart, 1)
		for i := 0; i < 3; i++ {
			rec := &Record{
				Header:   XLogRecord{XlRmid: multiFileID},
				MainData: []byte{byte(segIdx*3) + byte(i)},
			}
			data, err := Encode(rec)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			b.AppendRecord(data)
		}
		name := SegmentFileName(tli, segIdx, WALSegSize)
		if err := os.WriteFile(filepath.Join(dir, name), b.Bytes(), 0644); err != nil {
			t.Fatalf("WriteFile seg%d: %v", segIdx, err)
		}
	}

	p := NewFileSegmentProvider(dir)
	engine := NewRedoEngine(tli, 0, 0, p.Provide, nil)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if engine.Stats().SegmentsRead < 2 {
		t.Errorf("SegmentsRead: got %d want >= 2", engine.Stats().SegmentsRead)
	}
	if multiApplied < 6 {
		t.Errorf("applied %d records, want >= 6", multiApplied)
	}
}
