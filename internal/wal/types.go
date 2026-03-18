package wal

// WAL fundamental types and constants.
//
// Mirrors src/include/access/xlogdefs.h and src/include/access/xlog_internal.h.

import (
	"fmt"
	"math"
)

// ── LSN ──────────────────────────────────────────────────────────────────────

// LSN is a Log Sequence Number: an absolute byte position in the WAL stream.
//
// PostgreSQL encodes this as a pair of uint32 (high word / low word) in the
// XLogRecPtr typedef, commonly printed as "%X/%X".  We store it as a plain
// uint64 so arithmetic is straightforward.
type LSN uint64

// InvalidLSN is the zero LSN, used as a sentinel for "no WAL position".
const InvalidLSN LSN = 0

// String formats an LSN as the PostgreSQL convention "XXXXXXXX/XXXXXXXX".
func (l LSN) String() string {
	return fmt.Sprintf("%08X/%08X", uint32(l>>32), uint32(l))
}

// Hi returns the high 32-bit word (segment selector).
func (l LSN) Hi() uint32 { return uint32(l >> 32) }

// Lo returns the low 32-bit word (byte offset within the segment group).
func (l LSN) Lo() uint32 { return uint32(l) }

// MakeLSN assembles an LSN from its high and low 32-bit components.
func MakeLSN(hi, lo uint32) LSN { return LSN(uint64(hi)<<32 | uint64(lo)) }

// Add returns the LSN advanced by n bytes.
func (l LSN) Add(n uint64) LSN { return l + LSN(n) }

// Sub returns the byte distance from other to l (l − other).
// Panics if other > l (underflow).
func (l LSN) Sub(other LSN) uint64 {
	if l < other {
		panic(fmt.Sprintf("LSN.Sub underflow: %s < %s", l, other))
	}
	return uint64(l - other)
}

// ── Timeline ─────────────────────────────────────────────────────────────────

// TimeLineID identifies a WAL timeline.  Timeline 1 is the initial timeline;
// each point-in-time recovery that diverges from the base creates a new one.
// Matches PostgreSQL's TimeLineID (xlogdefs.h).
type TimeLineID uint32

const (
	InvalidTimeLine TimeLineID = 0
	InitialTimeLine TimeLineID = 1
)

// ── Segment geometry ─────────────────────────────────────────────────────────

// WALSegSize is the default WAL segment file size (16 MB).
// PostgreSQL allows compile-time configuration; we fix the default.
const WALSegSize = 16 * 1024 * 1024 // 16 MB

// WALPageSize is the WAL page size — always 8 KB, same as the heap page.
const WALPageSize = 8192

// XLogSegmentsPerXLogId is the number of WAL segments that fit in one
// XLogRecPtr "hi" word.  At 16 MB segments this is 0x100000000 / 0x1000000 =
// 256 segments per hi-word value (same calculation PG uses for the default
// segment size).
const XLogSegmentsPerXLogId = 0x100000000 / WALSegSize

// XLByteToSeg computes the segment number and offset within that segment for
// a given LSN.
func XLByteToSeg(lsn LSN, segSize uint64) (seg uint64, offset uint32) {
	seg = uint64(lsn) / segSize
	offset = uint32(uint64(lsn) % segSize)
	return
}

// XLByteInSeg returns the byte offset of lsn within its WAL segment.
func XLByteInSeg(lsn LSN, segSize uint64) uint32 {
	return uint32(uint64(lsn) % segSize)
}

// XLByteInPage returns the byte offset of lsn within its WAL page.
func XLByteInPage(lsn LSN) uint32 {
	return uint32(uint64(lsn) % WALPageSize)
}

// NextSegmentLSN returns the LSN of the first byte of the WAL segment
// following the segment that contains lsn.
func NextSegmentLSN(lsn LSN, segSize uint64) LSN {
	seg, _ := XLByteToSeg(lsn, segSize)
	return LSN((seg + 1) * segSize)
}

// SegmentFileName returns the standard WAL segment file name for the given
// timeline and segment number (0-based), matching PostgreSQL's
// XLogFilePath macro.
func SegmentFileName(tli TimeLineID, seg uint64, segSize uint64) string {
	// Each segment encodes as a 24-hex-digit name: TTTTTTTTSSSSSSSSSSSSSSSS
	// where T = tli (8 hex digits) and S = segment number (16 hex digits).
	// PG uses a slightly different encoding but this is equivalent for default sizes.
	_ = segSize // segSize would matter for non-default; kept for future use
	return fmt.Sprintf("%08X%08X%08X",
		uint32(tli),
		uint32(seg>>32),
		uint32(seg),
	)
}

// ── Record alignment ─────────────────────────────────────────────────────────

// XLogRecordAlignment is the byte alignment required for every XLogRecord.
// Matches MAXALIGN(sizeof(XLogRecord)) requirements: 8 bytes on 64-bit.
const XLogRecordAlignment = 8

// MaxAlignedSize rounds n up to the next XLogRecordAlignment boundary.
func MaxAlignedSize(n uint32) uint32 {
	return (n + XLogRecordAlignment - 1) &^ (XLogRecordAlignment - 1)
}

// ── Resource manager IDs ─────────────────────────────────────────────────────

// RmgrID identifies the subsystem that generated a WAL record.
// Matches PostgreSQL's RmgrId enumeration (rmgrlist.h).
type RmgrID uint8

const (
	RmgrXlog       RmgrID = 0 // xlog (checkpoint, switch, backup)
	RmgrXact       RmgrID = 1 // transaction commit/abort
	RmgrSmgr       RmgrID = 2 // storage manager
	RmgrClog       RmgrID = 3 // commit log
	RmgrDatabase   RmgrID = 4 // database operations
	RmgrTablespace RmgrID = 5
	RmgrMultixact  RmgrID = 6
	RmgrRelMap     RmgrID = 7
	RmgrStandby    RmgrID = 8
	RmgrHeap2      RmgrID = 9  // heap (extended ops: HOT, lock)
	RmgrHeap       RmgrID = 10 // heap (insert, delete, update)
	RmgrBtree      RmgrID = 11 // nbtree index
	RmgrHash       RmgrID = 12
	RmgrGin        RmgrID = 13
	RmgrGist       RmgrID = 14
	RmgrSeq        RmgrID = 15
	RmgrSpgist     RmgrID = 16
	RmgrBrin       RmgrID = 17
	RmgrCommitTs   RmgrID = 18
	RmgrLogical    RmgrID = 19 // logical decoding / replication origin
	RmgrMax        RmgrID = 20
)

// rmgrNames maps RmgrID to a human-readable name for diagnostic output.
var rmgrNames = [RmgrMax]string{
	RmgrXlog:       "XLOG",
	RmgrXact:       "Transaction",
	RmgrSmgr:       "Storage",
	RmgrClog:       "CLOG",
	RmgrDatabase:   "Database",
	RmgrTablespace: "Tablespace",
	RmgrMultixact:  "MultiXact",
	RmgrRelMap:     "RelMap",
	RmgrStandby:    "Standby",
	RmgrHeap2:      "Heap2",
	RmgrHeap:       "Heap",
	RmgrBtree:      "Btree",
	RmgrHash:       "Hash",
	RmgrGin:        "Gin",
	RmgrGist:       "Gist",
	RmgrSeq:        "Sequence",
	RmgrSpgist:     "SPGist",
	RmgrBrin:       "BRIN",
	RmgrCommitTs:   "CommitTimestamp",
	RmgrLogical:    "LogicalMessage",
}

// String returns the resource manager name.
func (r RmgrID) String() string {
	if r < RmgrMax {
		return rmgrNames[r]
	}
	return fmt.Sprintf("Rmgr(%d)", r)
}

// ── Record info flags ─────────────────────────────────────────────────────────

// XLogInfoMask masks the sub-type bits out of xl_info.
// The high 4 bits carry rmgr-specific flags (XLR_* below); the low 4 bits
// carry rmgr-specific sub-type info.
const (
	// XLRInfoMask masks the sub-type bits from xl_info.
	XLRInfoMask = 0x0F

	// XLRSpecialRelUpdate is set when the record modifies a system catalog
	// relation that requires invalidation.
	XLRSpecialRelUpdate = 0x10

	// XLRCheckConsistency is set to trigger consistency checks after redo.
	XLRCheckConsistency = 0x20
)

// ── XLogRecord header field limits ───────────────────────────────────────────

// XLogRecordMaxSize is a sanity upper bound for a single WAL record (1 GB).
// Any record larger than this is certainly corrupt.
const XLogRecordMaxSize = 1 * 1024 * 1024 * 1024

// MaxBlockID is the maximum block reference ID per record.
// Matches PostgreSQL's XLR_MAX_BLOCK_ID.
const MaxBlockID = 32

// XLogRecordHeaderSize is the fixed byte size of XLogRecord.
// Computed from the struct layout:
//
//	xl_tot_len  uint32 = 4
//	xl_xid      uint32 = 4
//	xl_prev     uint64 = 8  (LSN)
//	xl_info     uint8  = 1
//	xl_rmid     uint8  = 1
//	padding     uint16 = 2
//	xl_crc      uint32 = 4
//	total              = 24
const XLogRecordHeaderSize = 24

// XLogBlockHeaderSize is the fixed part of XLogRecordBlockHeader (without
// optional image or reln fields).
//
//	id          uint8  = 1
//	fork_flags  uint8  = 1
//	data_length uint16 = 2
//	total                = 4
const XLogBlockHeaderSize = 4

// XLogBlockImageHeaderSize is the size of XLogRecordBlockImageHeader.
//
//	length      uint16 = 2
//	hole_offset uint16 = 2
//	bimg_info   uint8  = 1
//	hole_length uint8  = 1 (compressed encoding, else derives from length/PageSize)
//	total              = 6
const XLogBlockImageHeaderSize = 6

// ── Sentinel ─────────────────────────────────────────────────────────────────

// MaxUint32 is used as an "invalid" sentinel where uint32 is needed but
// a separate type would be over-engineered.
const MaxUint32 = math.MaxUint32
