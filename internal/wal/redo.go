package wal

// WAL redo engine.
//
// RedoEngine drives crash recovery by reading WAL records sequentially from
// a stream of segment data providers and applying them in LSN order via the
// registered resource manager callbacks.
//
// Design mirrors PostgreSQL's StartupXLOG / xlog_redo pathway:
//   src/backend/access/transam/xlog.c    — top-level redo loop
//   src/backend/access/transam/xlogutils.c — block ref application helpers

import (
	"errors"
	"fmt"
	"io"
)

// ── Progress callback ─────────────────────────────────────────────────────────

// RedoProgress is called after each record is successfully applied.
// It can be used for checkpointing, progress reporting, or testing.
type RedoProgress func(rec *Record)

// ── Segment provider ──────────────────────────────────────────────────────────

// SegmentProvider returns the raw bytes for the WAL segment that contains lsn.
// Returns (nil, io.EOF) when no further segments are available.
type SegmentProvider func(lsn LSN, tli TimeLineID) ([]byte, error)

// ── RedoEngine ────────────────────────────────────────────────────────────────

// RedoEngine applies WAL records from startLSN onward on the given timeline.
type RedoEngine struct {
	tli      TimeLineID
	startLSN LSN
	endLSN   LSN // replay stops when currentLSN >= endLSN (0 = replay all)
	provider SegmentProvider
	progress RedoProgress
	store    PageWriter // optional; nil = no page application

	// stats
	stats RedoStats
}

// RedoStats accumulates replay statistics.
type RedoStats struct {
	RecordsApplied  uint64
	RecordsSkipped  uint64
	SegmentsRead    uint64
	BytesProcessed  uint64
}

// NewRedoEngine creates a RedoEngine that replays WAL from startLSN on tli.
// provider is called whenever a new segment is needed.
// onProgress is called after each successfully applied record (may be nil).
// endLSN = 0 means replay until no more segments are available.
func NewRedoEngine(
	tli TimeLineID,
	startLSN, endLSN LSN,
	provider SegmentProvider,
	onProgress RedoProgress,
) *RedoEngine {
	return &RedoEngine{
		tli:      tli,
		startLSN: startLSN,
		endLSN:   endLSN,
		provider: provider,
		progress: onProgress,
	}
}

// Run replays WAL records, calling the registered rmgr Redo callbacks.
//
// Replay stops when:
//   - endLSN > 0 and currentLSN >= endLSN
//   - The segment provider returns io.EOF
//   - A hard error is encountered (returned to caller)
//
// Soft errors (ErrUnknownRmgr, ErrUnimplementedRedo) are logged but do not
// halt replay — this matches PostgreSQL's behaviour of tolerating unknown
// extension rmgrs during recovery.
func (e *RedoEngine) Run() error {
	currentLSN := e.startLSN

	for {
		// Stop condition: endLSN reached.
		if e.endLSN != 0 && currentLSN >= e.endLSN {
			return nil
		}

		// Load the segment containing currentLSN.
		segData, err := e.provider(currentLSN, e.tli)
		if errors.Is(err, io.EOF) {
			return nil // no more segments — clean end of WAL
		}
		if err != nil {
			return fmt.Errorf("wal: loading segment for lsn %s: %w", currentLSN, err)
		}
		e.stats.SegmentsRead++

		// Compute the LSN of the first byte of this segment.
		_, segOffset := XLByteToSeg(currentLSN, WALSegSize)
		segStartLSN := currentLSN - LSN(segOffset)

		reader := NewSegmentReader(segData, segStartLSN, e.tli)

		// Seek reader past the start of the segment to currentLSN.
		// NewSegmentReader positions at byte 0 of the segment; we need to
		// advance pos to the intra-segment offset of currentLSN.
		reader.pos = int(segOffset)

		for {
			// Stop condition checked per-record.
			if e.endLSN != 0 && reader.CurrentLSN() >= e.endLSN {
				return nil
			}

			rec, err := reader.Next()
			if errors.Is(err, ErrEndOfSegment) || errors.Is(err, io.EOF) {
				break // move to the next segment
			}
			if err != nil {
				return fmt.Errorf("wal: reading record at lsn %s: %w",
					reader.CurrentLSN(), err)
			}
			if rec == nil {
				break
			}

			e.stats.BytesProcessed += uint64(rec.Header.XlTotLen)

			redoErr := Dispatch(RedoContext{Rec: rec, LSN: rec.LSN, Store: e.store})
			switch {
			case redoErr == nil:
				e.stats.RecordsApplied++
				if e.progress != nil {
					e.progress(rec)
				}
			case errors.As(redoErr, new(ErrUnknownRmgr)),
				errors.As(redoErr, new(ErrUnimplementedRedo)):
				// Non-fatal: skip unknown / unimplemented rmgrs.
				e.stats.RecordsSkipped++
			default:
				return fmt.Errorf("wal: redo at lsn %s: %w", rec.LSN, redoErr)
			}

			currentLSN = rec.LSN.Add(uint64(MaxAlignedSize(rec.Header.XlTotLen)))
		}

		// All records in this segment have been consumed.  Advance currentLSN
		// to the start of the next segment so the outer loop does not reload
		// the same segment forever.
		nextSeg := NextSegmentLSN(segStartLSN, WALSegSize)
		if currentLSN < nextSeg {
			currentLSN = nextSeg
		}
	}
}

// SetStore wires a storage backend into the engine.  Call before Run.
// When set, each rmgr Redo callback receives a non-nil Store in its RedoContext.
func (e *RedoEngine) SetStore(s PageWriter) { e.store = s }

// Stats returns a copy of the current replay statistics.
func (e *RedoEngine) Stats() RedoStats { return e.stats }

// ── InMemoryProvider ─────────────────────────────────────────────────────────

// InMemoryProvider is a SegmentProvider backed by a static map of LSN → segment
// bytes.  Useful for testing; not thread-safe.
type InMemoryProvider struct {
	// segments maps segStart LSN to raw segment bytes.
	segments map[LSN][]byte
	// order keeps insertion order for deterministic iteration.
	order []LSN
}

// NewInMemoryProvider creates an empty provider.
func NewInMemoryProvider() *InMemoryProvider {
	return &InMemoryProvider{segments: make(map[LSN][]byte)}
}

// Add registers a segment starting at segStart.
func (p *InMemoryProvider) Add(segStart LSN, data []byte) {
	if _, exists := p.segments[segStart]; !exists {
		p.order = append(p.order, segStart)
	}
	p.segments[segStart] = data
}

// Provide implements SegmentProvider.
func (p *InMemoryProvider) Provide(lsn LSN, _ TimeLineID) ([]byte, error) {
	_, offset := XLByteToSeg(lsn, WALSegSize)
	segStart := lsn - LSN(offset)
	data, ok := p.segments[segStart]
	if !ok {
		return nil, io.EOF
	}
	return data, nil
}

// ── SegmentBuilder ─────────────────────────────────────────────────────────

// SegmentBuilder constructs a synthetic WAL segment in memory.
// It handles page header insertion and cross-page record splitting.
// Primarily used for testing.
type SegmentBuilder struct {
	tli      TimeLineID
	segStart LSN
	sysid    uint64
	buf      []byte   // segment bytes being built
	pos      int      // current write position
}

// NewSegmentBuilder creates a builder for a segment starting at segStart.
func NewSegmentBuilder(tli TimeLineID, segStart LSN, sysid uint64) *SegmentBuilder {
	b := &SegmentBuilder{
		tli:      tli,
		segStart: segStart,
		sysid:    sysid,
		buf:      make([]byte, WALSegSize),
	}
	// Write the long page header on page 0.
	lhdr := XLogLongPageHeaderData{
		XLogPageHeaderData: XLogPageHeaderData{
			XlpMagic:    XLogPageMagic,
			XlpInfo:     XLPLongHeader,
			XlpTli:      tli,
			XlpPageaddr: segStart,
			XlpRemLen:   0,
		},
		XlpSysid:      sysid,
		XlpSegSize:    WALSegSize,
		XlpXlogBlcksz: WALPageSize,
	}
	EncodeLongPageHeader(b.buf[0:], &lhdr)
	b.pos = XLogLongPageHeaderSize
	return b
}

// AppendRecord writes rec (already encoded via Encode) into the segment,
// inserting page headers at every WALPageSize boundary.
// Returns the start LSN of the appended record.
func (b *SegmentBuilder) AppendRecord(recBytes []byte) (LSN, error) {
	startLSN := b.segStart.Add(uint64(b.pos))
	remaining := recBytes
	for len(remaining) > 0 {
		// If we are exactly at a page boundary (and not page 0), write a short
		// page header.
		if b.pos%WALPageSize == 0 && b.pos > 0 {
			if b.pos+WALPageSize > len(b.buf) {
				return 0, fmt.Errorf("wal: segment full at offset %d", b.pos)
			}
			phdr := XLogPageHeaderData{
				XlpMagic:    XLogPageMagic,
				XlpInfo:     XLPFirstIsContRecord,
				XlpTli:      b.tli,
				XlpPageaddr: b.segStart.Add(uint64(b.pos)),
				XlpRemLen:   uint32(len(remaining)),
			}
			EncodePageHeader(b.buf[b.pos:], &phdr)
			b.pos += XLogPageHeaderSize
		}

		pageEnd := (b.pos/WALPageSize + 1) * WALPageSize
		avail := pageEnd - b.pos
		take := len(remaining)
		if take > avail {
			take = avail
		}
		copy(b.buf[b.pos:], remaining[:take])
		b.pos += take
		remaining = remaining[take:]
	}
	return startLSN, nil
}

// Bytes returns the completed segment buffer.
func (b *SegmentBuilder) Bytes() []byte { return b.buf }
