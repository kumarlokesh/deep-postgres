package wal

// XLogRecord on-disk layout and in-memory representation.
//
// Mirrors src/include/access/xlogrecord.h.
//
// On-disk format (little-endian, after the fixed XLogRecord header):
//
//	for each block reference:
//	  [1 byte: block ID (0..MaxBlockID-1)]
//	  [1 byte: fork_flags]
//	  [2 bytes: block redo data length]
//	  if !XLBKTSameRel: [12 bytes: RelFileLocator]
//	  [4 bytes: block number]
//	  [1 byte: fork number]
//	  if XLBKTHasImage:
//	    [2 bytes: image length] [2 bytes: hole offset]
//	    [1 byte: bimg_info] [1 byte: packed hole length]
//	[0xFF end-of-block-refs marker]
//	[main-data header: 1 byte (short) or 1+2 bytes (long/0xFF prefix)]
//	[block image data, in block-ref order]
//	[block redo data,  in block-ref order]
//	[main data]

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// ── Errors ────────────────────────────────────────────────────────────────────

var (
	ErrRecordTooShort    = errors.New("wal: record too short")
	ErrRecordTooLarge    = errors.New("wal: record exceeds maximum size")
	ErrCRCMismatch       = errors.New("wal: CRC mismatch")
	ErrBadBlockID        = errors.New("wal: block ID out of range")
	ErrDuplicateBlockRef = errors.New("wal: duplicate block reference ID")
)

// ── XLogRecord fixed header ───────────────────────────────────────────────────

// XLogRecord is the 24-byte fixed header at the start of every WAL record.
//
// PostgreSQL layout (xlogrecord.h):
//
//	uint32  xl_tot_len   — total length of entire record
//	uint32  xl_xid       — transaction ID
//	uint64  xl_prev      — LSN of previous record
//	uint8   xl_info      — rmgr flag bits
//	uint8   xl_rmid      — resource manager
//	uint16  padding
//	uint32  xl_crc       — CRC-32C over body then header[0:20]
type XLogRecord struct {
	XlTotLen uint32
	XlXid    uint32
	XlPrev   LSN
	XlInfo   uint8
	XlRmid   RmgrID
	XlCrc    uint32 // bytes 20-23
}

func encodeXLogRecord(dst []byte, r *XLogRecord) {
	binary.LittleEndian.PutUint32(dst[0:], r.XlTotLen)
	binary.LittleEndian.PutUint32(dst[4:], r.XlXid)
	binary.LittleEndian.PutUint64(dst[8:], uint64(r.XlPrev))
	dst[16] = r.XlInfo
	dst[17] = uint8(r.XlRmid)
	dst[18] = 0 // padding
	dst[19] = 0
	binary.LittleEndian.PutUint32(dst[20:], r.XlCrc)
}

func decodeXLogRecord(src []byte) XLogRecord {
	return XLogRecord{
		XlTotLen: binary.LittleEndian.Uint32(src[0:]),
		XlXid:    binary.LittleEndian.Uint32(src[4:]),
		XlPrev:   LSN(binary.LittleEndian.Uint64(src[8:])),
		XlInfo:   src[16],
		XlRmid:   RmgrID(src[17]),
		XlCrc:    binary.LittleEndian.Uint32(src[20:]),
	}
}

// ── Block reference ───────────────────────────────────────────────────────────

// XLogBlockFlags are bits in the fork_flags byte of each block reference.
type XLogBlockFlags uint8

const (
	XLBKTHasImage XLogBlockFlags = 0x01 // full-page image follows
	XLBKTHasData  XLogBlockFlags = 0x02 // per-block redo data follows
	XLBKTWillInit XLogBlockFlags = 0x04 // block will be re-initialised
	XLBKTSameRel  XLogBlockFlags = 0x08 // same RelFileLocator as previous ref
)

// BimgInfo flags on the block image header.
type BimgInfo uint8

const (
	BimgHasHole      BimgInfo = 0x01 // image has a hole (unchanged region)
	BimgIsCompressed BimgInfo = 0x02 // image data is compressed
	BimgApply        BimgInfo = 0x04 // apply during recovery
)

// BlockImage is an optional full-page write stored within a block reference.
type BlockImage struct {
	Length     uint16   // bytes of image data stored
	HoleOffset uint16   // start of the hole within the 8 KB page
	BimgInfo   BimgInfo // flags
	HoleLength uint16   // length of the hole in bytes
	Data       []byte   // raw image bytes (len == Length)
}

// RelFileLocator identifies the physical file for a relation block reference.
// Matches PostgreSQL's RelFileLocator (relpath.h).
type RelFileLocator struct {
	SpcOid uint32 // tablespace OID
	DbOid  uint32 // database OID
	RelOid uint32 // relation OID
}

// ForkNum identifies which relation fork a block reference addresses.
type ForkNum uint8

const (
	ForkMain ForkNum = 0
	ForkFsm  ForkNum = 1
	ForkVm   ForkNum = 2
	ForkInit ForkNum = 3
)

// BlockRef is one fully-decoded block reference from a WAL record.
type BlockRef struct {
	ID       uint8
	Reln     RelFileLocator
	ForkNum  ForkNum
	BlockNum uint32
	Flags    XLogBlockFlags
	Image    *BlockImage // non-nil when XLBKTHasImage
	Data     []byte      // per-block redo data; nil when !XLBKTHasData
}

// ── Decoded record ────────────────────────────────────────────────────────────

// Record is a fully-decoded WAL record.
type Record struct {
	Header    XLogRecord
	LSN       LSN
	BlockRefs []BlockRef
	MainData  []byte
}

// RmgrInfo returns the rmgr-specific sub-type (low 4 bits of xl_info).
func (r *Record) RmgrInfo() uint8 { return r.Header.XlInfo & XLRInfoMask }

// String returns a one-line diagnostic summary.
func (r *Record) String() string {
	return fmt.Sprintf("lsn=%s rmgr=%s xid=%d totlen=%d blocks=%d maindata=%d",
		r.LSN, r.Header.XlRmid, r.Header.XlXid,
		r.Header.XlTotLen, len(r.BlockRefs), len(r.MainData))
}

// ── Encoder ───────────────────────────────────────────────────────────────────

// Encode serialises rec into a byte slice padded to XLogRecordAlignment.
// It computes and fills XlTotLen and XlCrc.
func Encode(rec *Record) ([]byte, error) {
	var body []byte

	prevReln := RelFileLocator{}
	hasPrev := false

	// Block reference headers.
	for i := range rec.BlockRefs {
		br := &rec.BlockRefs[i]
		if br.ID >= MaxBlockID {
			return nil, ErrBadBlockID
		}

		flags := br.Flags
		if hasPrev && br.Reln == prevReln {
			flags |= XLBKTSameRel
		} else {
			flags &^= XLBKTSameRel
		}
		if br.Image != nil {
			flags |= XLBKTHasImage
		}
		dataLen := uint16(len(br.Data))
		if dataLen > 0 {
			flags |= XLBKTHasData
		}

		body = append(body, br.ID, uint8(flags))
		body = appendU16LE(body, dataLen)

		if flags&XLBKTSameRel == 0 {
			body = appendU32LE(body, br.Reln.SpcOid)
			body = appendU32LE(body, br.Reln.DbOid)
			body = appendU32LE(body, br.Reln.RelOid)
			prevReln = br.Reln
			hasPrev = true
		}
		body = appendU32LE(body, br.BlockNum)
		body = append(body, uint8(br.ForkNum))

		if br.Image != nil {
			im := br.Image
			body = appendU16LE(body, im.Length)
			body = appendU16LE(body, im.HoleOffset)
			body = append(body, uint8(im.BimgInfo))
			// Pack hole_length: PG stores (hole_length >> 1) in one byte.
			packedHole := uint8(0)
			if im.BimgInfo&BimgHasHole != 0 {
				packedHole = uint8(im.HoleLength >> 1)
			}
			body = append(body, packedHole)
		}
	}

	// End-of-block-refs marker.
	body = append(body, 0xFF)

	// Main data header.
	mainLen := len(rec.MainData)
	if mainLen < 256 {
		body = append(body, uint8(mainLen))
	} else {
		body = append(body, 0xFF)
		body = appendU16LE(body, uint16(mainLen))
	}

	// Block images.
	for i := range rec.BlockRefs {
		if rec.BlockRefs[i].Image != nil {
			body = append(body, rec.BlockRefs[i].Image.Data...)
		}
	}

	// Block redo data.
	for i := range rec.BlockRefs {
		body = append(body, rec.BlockRefs[i].Data...)
	}

	// Main data.
	body = append(body, rec.MainData...)

	// Assemble the full record.
	totalLen := uint32(XLogRecordHeaderSize + len(body))
	aligned := MaxAlignedSize(totalLen)

	buf := make([]byte, aligned)
	copy(buf[XLogRecordHeaderSize:], body)

	// Write header with CRC = 0, compute CRC, write final header.
	hdr := rec.Header
	hdr.XlTotLen = totalLen
	hdr.XlCrc = 0
	encodeXLogRecord(buf[:XLogRecordHeaderSize], &hdr)

	crc := crc32.New(crc32cTable)
	_, _ = crc.Write(buf[XLogRecordHeaderSize:totalLen])
	_, _ = crc.Write(buf[:20]) // header bytes 0-19 (excludes CRC field)
	binary.LittleEndian.PutUint32(buf[20:], crc.Sum32())

	return buf, nil
}

// ── Decoder ───────────────────────────────────────────────────────────────────

// Decode parses a WAL record from data, validates CRC, and returns the decoded
// Record.  lsn is set on Record.LSN.  data must start at the record's first
// byte and be at least hdr.XlTotLen bytes long.
func Decode(data []byte, lsn LSN) (*Record, error) {
	if len(data) < XLogRecordHeaderSize {
		return nil, ErrRecordTooShort
	}

	hdr := decodeXLogRecord(data[:XLogRecordHeaderSize])
	if hdr.XlTotLen < XLogRecordHeaderSize {
		return nil, ErrRecordTooShort
	}
	if uint64(hdr.XlTotLen) > XLogRecordMaxSize {
		return nil, ErrRecordTooLarge
	}
	if int(hdr.XlTotLen) > len(data) {
		return nil, ErrRecordTooShort
	}

	// Validate CRC-32C.
	crc := crc32.New(crc32cTable)
	_, _ = crc.Write(data[XLogRecordHeaderSize:hdr.XlTotLen])
	_, _ = crc.Write(data[:20])
	if got := crc.Sum32(); got != hdr.XlCrc {
		return nil, fmt.Errorf("%w: got %08X want %08X", ErrCRCMismatch, got, hdr.XlCrc)
	}

	rec := &Record{Header: hdr, LSN: lsn}
	body := data[:hdr.XlTotLen]
	pos := XLogRecordHeaderSize

	// ── Parse block reference headers ────────────────────────────────────
	type pending struct {
		idx     int
		dataLen uint16
	}
	var refs []BlockRef
	var pendingData []pending
	seenIDs := make(map[uint8]bool)
	var lastReln RelFileLocator

	for pos < len(body) {
		bid := body[pos]
		pos++
		if bid == 0xFF {
			break
		}
		if bid >= MaxBlockID {
			return nil, ErrBadBlockID
		}
		if seenIDs[bid] {
			return nil, ErrDuplicateBlockRef
		}
		seenIDs[bid] = true

		if pos+3 > len(body) {
			return nil, ErrRecordTooShort
		}
		flags := XLogBlockFlags(body[pos])
		dataLen := binary.LittleEndian.Uint16(body[pos+1 : pos+3])
		pos += 3

		var reln RelFileLocator
		if flags&XLBKTSameRel != 0 {
			reln = lastReln
		} else {
			if pos+12 > len(body) {
				return nil, ErrRecordTooShort
			}
			reln.SpcOid = binary.LittleEndian.Uint32(body[pos:])
			reln.DbOid = binary.LittleEndian.Uint32(body[pos+4:])
			reln.RelOid = binary.LittleEndian.Uint32(body[pos+8:])
			pos += 12
			lastReln = reln
		}

		if pos+5 > len(body) {
			return nil, ErrRecordTooShort
		}
		blockNum := binary.LittleEndian.Uint32(body[pos:])
		forkNum := ForkNum(body[pos+4])
		pos += 5

		var img *BlockImage
		if flags&XLBKTHasImage != 0 {
			if pos+6 > len(body) {
				return nil, ErrRecordTooShort
			}
			imgLen := binary.LittleEndian.Uint16(body[pos:])
			holeOff := binary.LittleEndian.Uint16(body[pos+2:])
			bimgInfo := BimgInfo(body[pos+4])
			packedHole := body[pos+5]
			pos += 6
			holeLen := uint16(0)
			if bimgInfo&BimgHasHole != 0 {
				holeLen = uint16(packedHole) << 1
			}
			img = &BlockImage{
				Length:     imgLen,
				HoleOffset: holeOff,
				BimgInfo:   bimgInfo,
				HoleLength: holeLen,
			}
		}

		idx := len(refs)
		refs = append(refs, BlockRef{
			ID:       bid,
			Reln:     reln,
			ForkNum:  forkNum,
			BlockNum: blockNum,
			Flags:    flags,
			Image:    img,
		})
		pendingData = append(pendingData, pending{idx: idx, dataLen: dataLen})
	}

	// ── Main data header ─────────────────────────────────────────────────
	if pos >= len(body) {
		return nil, ErrRecordTooShort
	}
	var mainDataLen int
	if body[pos] == 0xFF {
		pos++
		if pos+2 > len(body) {
			return nil, ErrRecordTooShort
		}
		mainDataLen = int(binary.LittleEndian.Uint16(body[pos:]))
		pos += 2
	} else {
		mainDataLen = int(body[pos])
		pos++
	}

	// ── Block image data ─────────────────────────────────────────────────
	for i := range refs {
		if refs[i].Image == nil {
			continue
		}
		imgLen := int(refs[i].Image.Length)
		if pos+imgLen > len(body) {
			return nil, ErrRecordTooShort
		}
		refs[i].Image.Data = make([]byte, imgLen)
		copy(refs[i].Image.Data, body[pos:pos+imgLen])
		pos += imgLen
	}

	// ── Per-block redo data ───────────────────────────────────────────────
	for _, p := range pendingData {
		dlen := int(p.dataLen)
		if dlen == 0 {
			continue
		}
		if pos+dlen > len(body) {
			return nil, ErrRecordTooShort
		}
		refs[p.idx].Data = make([]byte, dlen)
		copy(refs[p.idx].Data, body[pos:pos+dlen])
		pos += dlen
	}

	// ── Main data ─────────────────────────────────────────────────────────
	if mainDataLen > 0 {
		if pos+mainDataLen > len(body) {
			return nil, ErrRecordTooShort
		}
		rec.MainData = make([]byte, mainDataLen)
		copy(rec.MainData, body[pos:pos+mainDataLen])
	}

	rec.BlockRefs = refs
	return rec, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// crc32cTable is the CRC-32C (Castagnoli) polynomial table used by PostgreSQL.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func appendU16LE(b []byte, v uint16) []byte {
	return append(b, byte(v), byte(v>>8))
}

func appendU32LE(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}
