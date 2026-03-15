package wal

// WAL page layout.
//
// Mirrors src/include/access/xlog_internal.h.
//
// Every WAL segment is divided into 8 KB pages.  The first page in a segment
// carries an XLogLongPageHeaderData (40 bytes); all subsequent pages carry an
// XLogPageHeaderData (24 bytes).  Record data is packed after the header and
// continues across page boundaries — the continuation on the next page is
// preceded by the page header only, not a new record header.
//
// Page layout:
//
//	+----------------------------+
//	| XLogPageHeaderData (24 B)  |  or XLogLongPageHeaderData (40 B) on page 0
//	+----------------------------+
//	| record / continuation data |
//	+----------------------------+

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ── Page header sizes ─────────────────────────────────────────────────────────

// XLogPageHeaderSize is the size of the short page header (all pages except
// the first page of a segment).
const XLogPageHeaderSize = 24

// XLogLongPageHeaderSize is the size of the long page header (first page of
// each segment).
const XLogLongPageHeaderSize = 40

// ── WAL page magic ────────────────────────────────────────────────────────────

// XLogPageMagic is the magic number for WAL page headers.
// PostgreSQL increments this on incompatible WAL format changes.
// Current value as of PostgreSQL 16.
const XLogPageMagic uint16 = 0xD11D

// ── XLogPageHeaderData flags ──────────────────────────────────────────────────

// XLogPageHeaderFlags are bits in xlp_info.
type XLogPageHeaderFlags uint16

const (
	// XLPFirstIsContRecord indicates the first record on this page continues
	// from the previous page (is a record continuation, not a new record).
	XLPFirstIsContRecord XLogPageHeaderFlags = 0x0001

	// XLPLastIsDummy indicates the page was padded (no real records follow).
	XLPLastIsDummy XLogPageHeaderFlags = 0x0002

	// XLPLongHeader indicates the page carries an XLogLongPageHeaderData
	// (segment-first-page only).
	XLPLongHeader XLogPageHeaderFlags = 0x0002
)

// ── XLogPageHeaderData ────────────────────────────────────────────────────────

// XLogPageHeaderData is the 24-byte header present on every WAL page.
//
// Matches PostgreSQL's XLogPageHeaderData (xlog_internal.h):
//
//	uint16  xlp_magic      — XLogPageMagic
//	uint16  xlp_info       — flag bits
//	TimeLineID xlp_tli     — timeline this page belongs to
//	XLogRecPtr xlp_pageaddr — LSN of the start of this page
//	uint32  xlp_rem_len    — bytes remaining from a previous record
type XLogPageHeaderData struct {
	XlpMagic    uint16
	XlpInfo     XLogPageHeaderFlags
	XlpTli      TimeLineID
	XlpPageaddr LSN
	XlpRemLen   uint32
}

// XLogLongPageHeaderData extends XLogPageHeaderData for the first segment page.
//
// Matches PostgreSQL's XLogLongPageHeaderData (xlog_internal.h):
//
//	XLogPageHeaderData std
//	uint64  xlp_sysid      — system identifier (from pg_control)
//	uint32  xlp_seg_size   — WAL segment size
//	uint32  xlp_xlog_blcksz — WAL page size
type XLogLongPageHeaderData struct {
	XLogPageHeaderData
	XlpSysid      uint64
	XlpSegSize    uint32
	XlpXlogBlcksz uint32
}

// ── Encoding / decoding ───────────────────────────────────────────────────────

// EncodePageHeader serialises h into dst (must be >= XLogPageHeaderSize bytes).
func EncodePageHeader(dst []byte, h *XLogPageHeaderData) {
	binary.LittleEndian.PutUint16(dst[0:], h.XlpMagic)
	binary.LittleEndian.PutUint16(dst[2:], uint16(h.XlpInfo))
	binary.LittleEndian.PutUint32(dst[4:], uint32(h.XlpTli))
	binary.LittleEndian.PutUint64(dst[8:], uint64(h.XlpPageaddr))
	binary.LittleEndian.PutUint32(dst[16:], h.XlpRemLen)
	// bytes 20-23: padding (zero)
	binary.LittleEndian.PutUint32(dst[20:], 0)
}

// DecodePageHeader deserialises a short page header from src.
func DecodePageHeader(src []byte) (XLogPageHeaderData, error) {
	if len(src) < XLogPageHeaderSize {
		return XLogPageHeaderData{}, ErrRecordTooShort
	}
	h := XLogPageHeaderData{
		XlpMagic:    binary.LittleEndian.Uint16(src[0:]),
		XlpInfo:     XLogPageHeaderFlags(binary.LittleEndian.Uint16(src[2:])),
		XlpTli:      TimeLineID(binary.LittleEndian.Uint32(src[4:])),
		XlpPageaddr: LSN(binary.LittleEndian.Uint64(src[8:])),
		XlpRemLen:   binary.LittleEndian.Uint32(src[16:]),
	}
	if h.XlpMagic != XLogPageMagic {
		return XLogPageHeaderData{}, fmt.Errorf("wal: invalid page magic %04X (want %04X)",
			h.XlpMagic, XLogPageMagic)
	}
	return h, nil
}

// EncodeLongPageHeader serialises a long (first-segment-page) header.
func EncodeLongPageHeader(dst []byte, h *XLogLongPageHeaderData) {
	EncodePageHeader(dst[:XLogPageHeaderSize], &h.XLogPageHeaderData)
	binary.LittleEndian.PutUint64(dst[XLogPageHeaderSize:], h.XlpSysid)
	binary.LittleEndian.PutUint32(dst[XLogPageHeaderSize+8:], h.XlpSegSize)
	binary.LittleEndian.PutUint32(dst[XLogPageHeaderSize+12:], h.XlpXlogBlcksz)
}

// DecodeLongPageHeader deserialises a long page header from src.
func DecodeLongPageHeader(src []byte) (XLogLongPageHeaderData, error) {
	if len(src) < XLogLongPageHeaderSize {
		return XLogLongPageHeaderData{}, ErrRecordTooShort
	}
	short, err := DecodePageHeader(src[:XLogPageHeaderSize])
	if err != nil {
		return XLogLongPageHeaderData{}, err
	}
	return XLogLongPageHeaderData{
		XLogPageHeaderData: short,
		XlpSysid:           binary.LittleEndian.Uint64(src[XLogPageHeaderSize:]),
		XlpSegSize:         binary.LittleEndian.Uint32(src[XLogPageHeaderSize+8:]),
		XlpXlogBlcksz:      binary.LittleEndian.Uint32(src[XLogPageHeaderSize+12:]),
	}, nil
}

// ── Segment reader ────────────────────────────────────────────────────────────

// ErrEndOfSegment is returned when the reader has consumed all records in a
// segment and the caller should open the next segment file.
var ErrEndOfSegment = errors.New("wal: end of segment")

// ErrInvalidWAL is returned when the page or record data is structurally
// corrupt in a way that prevents further parsing.
var ErrInvalidWAL = errors.New("wal: invalid WAL data")

// SegmentReader reads Records from a single WAL segment, handling page
// boundaries transparently.  It reassembles records that span multiple pages.
//
// Usage:
//
//	r := NewSegmentReader(data, startLSN, tli)
//	for {
//	    rec, err := r.Next()
//	    if errors.Is(err, wal.ErrEndOfSegment) { break }
//	    if err != nil { … }
//	    process(rec)
//	}
type SegmentReader struct {
	data     []byte     // full segment bytes
	segStart LSN        // LSN of the first byte of this segment
	tli      TimeLineID // expected timeline
	pos      int        // current byte offset within data
	segSize  int        // segment size (default WALSegSize)
}

// NewSegmentReader creates a reader over a full in-memory WAL segment.
// segStart is the LSN corresponding to data[0].
func NewSegmentReader(data []byte, segStart LSN, tli TimeLineID) *SegmentReader {
	return &SegmentReader{
		data:     data,
		segStart: segStart,
		tli:      tli,
		pos:      0,
		segSize:  len(data),
	}
}

// CurrentLSN returns the LSN corresponding to r.pos.
func (r *SegmentReader) CurrentLSN() LSN {
	return r.segStart.Add(uint64(r.pos))
}

// Next reads and returns the next Record from the segment.
// Returns ErrEndOfSegment when all records have been consumed.
func (r *SegmentReader) Next() (*Record, error) {
	// Skip the page header if we are at a page boundary.
	if err := r.skipPageHeaderIfNeeded(); err != nil {
		return nil, err
	}

	if r.pos >= len(r.data) {
		return nil, ErrEndOfSegment
	}

	// At this point r.pos is the start of a new record header.
	recordLSN := r.CurrentLSN()

	// We need to gather the complete record bytes, which may span pages.
	// Read the fixed header first.
	hdrBuf, err := r.readBytes(XLogRecordHeaderSize)
	if err != nil {
		return nil, err
	}

	hdr := decodeXLogRecord(hdrBuf)
	if hdr.XlTotLen == 0 {
		// Zero-filled space means end of written WAL in this segment.
		return nil, ErrEndOfSegment
	}
	if hdr.XlTotLen < XLogRecordHeaderSize {
		return nil, ErrInvalidWAL
	}
	if uint64(hdr.XlTotLen) > XLogRecordMaxSize {
		return nil, ErrRecordTooLarge
	}

	// Allocate a contiguous buffer and copy the full record (spanning pages).
	recBuf := make([]byte, hdr.XlTotLen)
	copy(recBuf[:XLogRecordHeaderSize], hdrBuf)
	remaining := int(hdr.XlTotLen) - XLogRecordHeaderSize
	if remaining > 0 {
		tail, err := r.readBytes(remaining)
		if err != nil {
			return nil, err
		}
		copy(recBuf[XLogRecordHeaderSize:], tail)
	}

	// Advance past alignment padding.
	aligned := int(MaxAlignedSize(hdr.XlTotLen))
	padding := aligned - int(hdr.XlTotLen)
	if padding > 0 {
		if _, err := r.readBytes(padding); err != nil && !errors.Is(err, io.EOF) {
			// Padding at end of segment is acceptable.
			_ = err
		}
	}

	return Decode(recBuf, recordLSN)
}

// skipPageHeaderIfNeeded advances past the page header if r.pos is exactly
// at a page boundary.
func (r *SegmentReader) skipPageHeaderIfNeeded() error {
	pageOffset := r.pos % WALPageSize
	if pageOffset != 0 {
		return nil // mid-page; no header to skip
	}
	if r.pos >= len(r.data) {
		return ErrEndOfSegment
	}

	// Determine header size: long header on the first page of the segment.
	isFirstPage := r.pos == 0
	headerSize := XLogPageHeaderSize
	if isFirstPage {
		headerSize = XLogLongPageHeaderSize
		if len(r.data)-r.pos < headerSize {
			return fmt.Errorf("%w: insufficient data for long page header", ErrInvalidWAL)
		}
		lhdr, err := DecodeLongPageHeader(r.data[r.pos:])
		if err != nil {
			return err
		}
		if lhdr.XlpTli != r.tli {
			return fmt.Errorf("%w: timeline mismatch: got %d want %d",
				ErrInvalidWAL, lhdr.XlpTli, r.tli)
		}
	} else {
		if len(r.data)-r.pos < headerSize {
			return ErrEndOfSegment
		}
		phdr, err := DecodePageHeader(r.data[r.pos:])
		if err != nil {
			return err
		}
		if phdr.XlpTli != r.tli {
			return fmt.Errorf("%w: timeline mismatch: got %d want %d",
				ErrInvalidWAL, phdr.XlpTli, r.tli)
		}
		// If a record continues from the previous page, the caller already
		// collected the continuation bytes; xlp_rem_len tells us how many bytes
		// of the current record appear at the start of this page before the next
		// record begins.  Here we are at the start of a new record (the caller
		// does all continuation gathering in readBytes), so we can simply
		// skip the header.
	}

	r.pos += headerSize
	return nil
}

// readBytes reads exactly n bytes from the segment, transparently skipping
// page headers when crossing page boundaries.  Returns io.EOF if the segment
// ends before n bytes are available.
func (r *SegmentReader) readBytes(n int) ([]byte, error) {
	buf := make([]byte, 0, n)
	for len(buf) < n {
		if r.pos >= len(r.data) {
			return nil, io.EOF
		}

		// Skip page header if at a boundary (but not the very first byte,
		// which skipPageHeaderIfNeeded already handled for the record start).
		if r.pos%WALPageSize == 0 {
			if err := r.skipPageHeaderIfNeeded(); err != nil {
				return nil, err
			}
		}

		// Copy up to the end of the current page.
		pageEnd := (r.pos/WALPageSize + 1) * WALPageSize
		if pageEnd > len(r.data) {
			pageEnd = len(r.data)
		}
		avail := pageEnd - r.pos
		want := n - len(buf)
		take := avail
		if take > want {
			take = want
		}
		buf = append(buf, r.data[r.pos:r.pos+take]...)
		r.pos += take
	}
	return buf, nil
}
