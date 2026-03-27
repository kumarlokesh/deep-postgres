package wal_test

import (
	"errors"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/wal"
)

// recordingRedoTracer captures all events.
type recordingRedoTracer struct {
	applied  []wal.LSN
	skipped  []wal.LSN
	segments []wal.LSN
	skipErrs []error
}

func (r *recordingRedoTracer) OnApply(lsn wal.LSN, _ *wal.Record) { r.applied = append(r.applied, lsn) }
func (r *recordingRedoTracer) OnSkip(lsn wal.LSN, _ *wal.Record, err error) {
	r.skipped = append(r.skipped, lsn)
	r.skipErrs = append(r.skipErrs, err)
}
func (r *recordingRedoTracer) OnSegment(seg wal.LSN) { r.segments = append(r.segments, seg) }

// buildSingleRecordEngine builds a redo engine primed with one heap-insert
// record so that Run() exercises the apply path.
func buildSingleRecordEngine(t *testing.T) *wal.RedoEngine {
	t.Helper()

	const tli wal.TimeLineID = 1
	sysid := uint64(12345)
	segStart := wal.LSN(0)

	builder := wal.NewSegmentBuilder(tli, segStart, sysid)

	rec := &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrHeap,
			XlInfo: 0x00, // XLOG_HEAP_INSERT
			XlXid:  3,
		},
		BlockRefs: []wal.BlockRef{
			{
				Reln:     wal.RelFileLocator{SpcOid: 1, DbOid: 1, RelOid: 1},
				ForkNum:  0,
				BlockNum: 0,
			},
		},
	}

	encoded, err := wal.Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	lsn, err := builder.AppendRecord(encoded)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	_ = lsn

	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, builder.Bytes())

	return wal.NewRedoEngine(tli, segStart+wal.LSN(wal.XLogLongPageHeaderSize), 0, provider.Provide, nil)
}

// TestRedoTracerNoopCompiles verifies the NoopRedoTracer satisfies the interface
// and that SetTracer/Run compile without wiring a real store.
func TestRedoTracerNoopCompiles(t *testing.T) {
	eng := buildSingleRecordEngine(t)
	eng.SetTracer(wal.NoopRedoTracer{})
	// Just check it doesn't panic; heap rmgr will error without a store but
	// that surfaces as a hard error, not a skip — so wrap it and check.
	_ = eng.Run() // error is expected (no store wired), but no panic
}

// TestRedoTracerOnSegment verifies OnSegment fires once per segment loaded.
func TestRedoTracerOnSegment(t *testing.T) {
	eng := buildSingleRecordEngine(t)
	tr := &recordingRedoTracer{}
	eng.SetTracer(tr)
	_ = eng.Run()

	if len(tr.segments) != 1 {
		t.Errorf("expected 1 OnSegment call, got %d", len(tr.segments))
	}
	if tr.segments[0] != 0 {
		t.Errorf("expected segment start LSN 0, got %d", tr.segments[0])
	}
}

// TestRedoTracerOnSkip verifies OnSkip fires for unknown rmgr records.
func TestRedoTracerOnSkip(t *testing.T) {
	const tli wal.TimeLineID = 1
	const unknownRmgr wal.RmgrID = 99
	segStart := wal.LSN(0)
	sysid := uint64(42)

	builder := wal.NewSegmentBuilder(tli, segStart, sysid)
	rec := &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: unknownRmgr,
			XlInfo: 0,
			XlXid:  3,
		},
	}
	encoded, err := wal.Encode(rec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, err = builder.AppendRecord(encoded); err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}

	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, builder.Bytes())

	eng := wal.NewRedoEngine(tli, segStart+wal.LSN(wal.XLogLongPageHeaderSize), 0, provider.Provide, nil)
	tr := &recordingRedoTracer{}
	eng.SetTracer(tr)
	if err := eng.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(tr.skipped) != 1 {
		t.Errorf("expected 1 OnSkip call, got %d", len(tr.skipped))
	}
	if len(tr.applied) != 0 {
		t.Errorf("expected 0 OnApply calls, got %d", len(tr.applied))
	}
	// Confirm the error is ErrUnknownRmgr.
	var unknownErr wal.ErrUnknownRmgr
	if !errors.As(tr.skipErrs[0], &unknownErr) {
		t.Errorf("expected ErrUnknownRmgr in skip error, got %T: %v", tr.skipErrs[0], tr.skipErrs[0])
	}
}

// TestRedoTracerMultiSegment verifies OnSegment fires once per segment when
// two segments are provided.
func TestRedoTracerMultiSegment(t *testing.T) {
	const tli wal.TimeLineID = 1
	sysid := uint64(1)

	build := func(segStart wal.LSN, xid uint32) []byte {
		b := wal.NewSegmentBuilder(tli, segStart, sysid)
		rec := &wal.Record{
			Header: wal.XLogRecord{XlRmid: 99, XlInfo: 0, XlXid: xid},
		}
		encoded, _ := wal.Encode(rec)
		_, _ = b.AppendRecord(encoded)
		return b.Bytes()
	}

	seg0 := wal.LSN(0)
	seg1 := wal.LSN(wal.WALSegSize)

	provider := wal.NewInMemoryProvider()
	provider.Add(seg0, build(seg0, 3))
	provider.Add(seg1, build(seg1, 4))

	eng := wal.NewRedoEngine(tli, seg0+wal.LSN(wal.XLogLongPageHeaderSize), 0, provider.Provide, nil)
	tr := &recordingRedoTracer{}
	eng.SetTracer(tr)
	_ = eng.Run()

	if len(tr.segments) != 2 {
		t.Errorf("expected 2 OnSegment calls, got %d", len(tr.segments))
	}
}

// TestRedoTracerStatsConsistency verifies that tracer event counts match
// engine stats (applied + skipped).
func TestRedoTracerStatsConsistency(t *testing.T) {
	const tli wal.TimeLineID = 1
	sysid := uint64(1)
	segStart := wal.LSN(0)

	builder := wal.NewSegmentBuilder(tli, segStart, sysid)
	// One unknown record → skip.
	for i := range 3 {
		rec := &wal.Record{
			Header: wal.XLogRecord{XlRmid: 99, XlInfo: 0, XlXid: uint32(3 + i)},
		}
		encoded, _ := wal.Encode(rec)
		_, _ = builder.AppendRecord(encoded)
	}

	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, builder.Bytes())

	eng := wal.NewRedoEngine(tli, segStart+wal.LSN(wal.XLogLongPageHeaderSize), 0, provider.Provide, nil)
	tr := &recordingRedoTracer{}
	eng.SetTracer(tr)
	if err := eng.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	stats := eng.Stats()
	if int(stats.RecordsSkipped) != len(tr.skipped) {
		t.Errorf("stats.RecordsSkipped=%d != len(tr.skipped)=%d",
			stats.RecordsSkipped, len(tr.skipped))
	}
	if int(stats.RecordsApplied) != len(tr.applied) {
		t.Errorf("stats.RecordsApplied=%d != len(tr.applied)=%d",
			stats.RecordsApplied, len(tr.applied))
	}
}
