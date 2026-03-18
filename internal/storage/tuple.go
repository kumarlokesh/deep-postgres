package storage

// Heap tuple layout (from src/include/access/htup_details.h):
//
//	+------------------+------------+-------------+
//	| HeapTupleHeader  | null bitmap| user data   |
//	| (23 bytes + pad) | (optional) | (attributes)|
//	+------------------+------------+-------------+
//
// The header contains MVCC fields (xmin, xmax, cid), a self-TID, infomask
// flags, and a header-offset field (t_hoff) pointing past any null bitmap.
//
// We model the "heap" variant of t_choice (i.e. t_heap, not t_datum).

import "encoding/binary"

// HeapTupleHeaderSize is the fixed size of HeapTupleHeader in bytes.
// PostgreSQL's HeapTupleHeaderData is 23 bytes but padded to 24.
const HeapTupleHeaderSize = 24

// MaxHeapAttributeNumber is the maximum number of attributes in a tuple.
const MaxHeapAttributeNumber uint16 = 1600

// heapNattsMask extracts the attribute count from t_infomask2 (low 11 bits).
const heapNattsMask uint16 = 0x07FF

// InfomaskFlags are bits in HeapTupleHeader.TInfomask.
// Matches PostgreSQL's HEAP_* flags in htup_details.h.
type InfomaskFlags uint16

const (
	HeapHasNull        InfomaskFlags = 0x0001 // tuple has at least one null attribute
	HeapHasVarWidth    InfomaskFlags = 0x0002 // has variable-length attribute(s)
	HeapHasExternal    InfomaskFlags = 0x0004 // has external TOAST attribute(s)
	HeapHasOidOld      InfomaskFlags = 0x0008 // has OID field (pre-12 compat)
	HeapXmaxKeyshrLock InfomaskFlags = 0x0010 // xmax is a key-share locker
	HeapComboCid       InfomaskFlags = 0x0020 // t_cid is a combo CID
	HeapXmaxExclLock   InfomaskFlags = 0x0040 // xmax is exclusive locker
	HeapXmaxLockOnly   InfomaskFlags = 0x0080 // xmax is a locker, not deleter
	HeapXminCommitted  InfomaskFlags = 0x0100 // xmin committed (hint bit)
	HeapXminInvalid    InfomaskFlags = 0x0200 // xmin invalid / aborted (hint bit)
	HeapXmaxCommitted  InfomaskFlags = 0x0400 // xmax committed (hint bit)
	HeapXmaxInvalid    InfomaskFlags = 0x0800 // xmax invalid / aborted (hint bit)
	HeapXmaxIsMulti    InfomaskFlags = 0x1000 // xmax is a MultiXactId
	HeapUpdated        InfomaskFlags = 0x2000 // tuple was updated
	HeapMovedOff       InfomaskFlags = 0x4000 // moved by pre-9.0 VACUUM FULL
	HeapMovedIn        InfomaskFlags = 0x8000 // moved in by pre-9.0 VACUUM FULL
)

// Infomask2Flags are the high bits of HeapTupleHeader.TInfomask2.
// The low 11 bits of t_infomask2 encode the attribute count.
type Infomask2Flags uint16

const (
	HeapHotUpdated Infomask2Flags = 0x4000 // tuple has HOT successor
	HeapOnlyTuple  Infomask2Flags = 0x8000 // tuple is a heap-only (HOT) version
)

// HeapTupleHeader is the 24-byte header prepended to every heap tuple.
//
// Matches PostgreSQL's HeapTupleHeaderData (htup_details.h):
//
//	typedef struct HeapTupleHeaderData {
//	    union { HeapTupleFields t_heap; DatumTupleFields t_datum; } t_choice;
//	    ItemPointerData t_ctid;     // 6 bytes
//	    uint16          t_infomask2;
//	    uint16          t_infomask;
//	    uint8           t_hoff;
//	    /* ^ 23 bytes ^ */
//	    bits8 t_bits[FLEXIBLE_ARRAY_MEMBER]; // null bitmap
//	} HeapTupleHeaderData;
//
// We use the t_heap variant:
//   - TXmin (4): inserting XID
//   - TXmax (4): deleting XID (0 if alive)
//   - TCid  (4): command ID within transaction
//   - TCtidBlock (4) + TCtidOffset (2): self/forward TID
//   - TInfomask2 (2), TInfomask (2), THoff (1), _pad (1) → total 24 bytes
type HeapTupleHeader struct {
	TXmin       TransactionId
	TXmax       TransactionId
	TCid        CommandId
	TCtidBlock  BlockNumber
	TCtidOffset OffsetNumber
	TInfomask2  uint16
	TInfomask   uint16
	THoff       uint8
	_pad        uint8 //nolint:unused // alignment padding to reach 24 bytes
}

// NewHeapTupleHeader creates a header for a freshly-inserted tuple.
// xmin is the inserting transaction; natts is the number of attributes.
// xmax is initialised to 0 with HEAP_XMAX_INVALID set.
func NewHeapTupleHeader(xmin TransactionId, natts uint16) HeapTupleHeader {
	return HeapTupleHeader{
		TXmin:      xmin,
		TXmax:      0,
		TCid:       0,
		TInfomask2: natts & heapNattsMask,
		TInfomask:  uint16(HeapXmaxInvalid),
		THoff:      HeapTupleHeaderSize,
	}
}

// Natts returns the number of attributes encoded in TInfomask2.
func (h *HeapTupleHeader) Natts() uint16 { return h.TInfomask2 & heapNattsMask }

// SetNatts sets the attribute count in TInfomask2, preserving flag bits.
func (h *HeapTupleHeader) SetNatts(natts uint16) {
	h.TInfomask2 = (h.TInfomask2 & ^heapNattsMask) | (natts & heapNattsMask)
}

// Infomask returns the InfomaskFlags.
func (h *HeapTupleHeader) Infomask() InfomaskFlags {
	return InfomaskFlags(h.TInfomask)
}

// SetInfomask replaces the infomask.
func (h *HeapTupleHeader) SetInfomask(f InfomaskFlags) { h.TInfomask = uint16(f) }

// Infomask2Flags returns the high-bit flags from TInfomask2.
func (h *HeapTupleHeader) Infomask2Flags() Infomask2Flags {
	return Infomask2Flags(h.TInfomask2 & ^heapNattsMask)
}

// --- Hint bit helpers ---

func (h *HeapTupleHeader) XminCommitted() bool {
	return h.Infomask()&HeapXminCommitted != 0
}
func (h *HeapTupleHeader) XminInvalid() bool {
	return h.Infomask()&HeapXminInvalid != 0
}
func (h *HeapTupleHeader) XmaxCommitted() bool {
	return h.Infomask()&HeapXmaxCommitted != 0
}
func (h *HeapTupleHeader) XmaxInvalid() bool {
	return h.Infomask()&HeapXmaxInvalid != 0
}
func (h *HeapTupleHeader) IsHotUpdated() bool {
	return h.Infomask2Flags()&HeapHotUpdated != 0
}
func (h *HeapTupleHeader) IsHeapOnly() bool {
	return h.Infomask2Flags()&HeapOnlyTuple != 0
}

func (h *HeapTupleHeader) SetXminCommitted() {
	h.TInfomask = uint16((h.Infomask() | HeapXminCommitted) &^ HeapXminInvalid)
}
func (h *HeapTupleHeader) SetXminInvalid() {
	h.TInfomask = uint16((h.Infomask() | HeapXminInvalid) &^ HeapXminCommitted)
}
func (h *HeapTupleHeader) SetXmaxCommitted() {
	h.TInfomask = uint16((h.Infomask() | HeapXmaxCommitted) &^ HeapXmaxInvalid)
}
func (h *HeapTupleHeader) SetXmaxInvalid() {
	h.TInfomask = uint16((h.Infomask() | HeapXmaxInvalid) &^ HeapXmaxCommitted)
}

// Ctid returns the self-referential TID as (BlockNumber, OffsetNumber).
func (h *HeapTupleHeader) Ctid() (BlockNumber, OffsetNumber) {
	return h.TCtidBlock, h.TCtidOffset
}

// SetCtid sets the self TID.
func (h *HeapTupleHeader) SetCtid(block BlockNumber, offset OffsetNumber) {
	h.TCtidBlock = block
	h.TCtidOffset = offset
}

// encodeHeapTupleHeader serializes h into dst[0:HeapTupleHeaderSize].
func encodeHeapTupleHeader(dst []byte, h *HeapTupleHeader) {
	binary.LittleEndian.PutUint32(dst[0:], h.TXmin)
	binary.LittleEndian.PutUint32(dst[4:], h.TXmax)
	binary.LittleEndian.PutUint32(dst[8:], h.TCid)
	binary.LittleEndian.PutUint32(dst[12:], h.TCtidBlock)
	binary.LittleEndian.PutUint16(dst[16:], h.TCtidOffset)
	binary.LittleEndian.PutUint16(dst[18:], h.TInfomask2)
	binary.LittleEndian.PutUint16(dst[20:], h.TInfomask)
	dst[22] = h.THoff
	dst[23] = 0 // padding
}

// decodeHeapTupleHeader deserializes from src[0:HeapTupleHeaderSize].
func decodeHeapTupleHeader(src []byte) HeapTupleHeader {
	return HeapTupleHeader{
		TXmin:       binary.LittleEndian.Uint32(src[0:]),
		TXmax:       binary.LittleEndian.Uint32(src[4:]),
		TCid:        binary.LittleEndian.Uint32(src[8:]),
		TCtidBlock:  binary.LittleEndian.Uint32(src[12:]),
		TCtidOffset: binary.LittleEndian.Uint16(src[16:]),
		TInfomask2:  binary.LittleEndian.Uint16(src[18:]),
		TInfomask:   binary.LittleEndian.Uint16(src[20:]),
		THoff:       src[22],
	}
}

// HeapTuple pairs a HeapTupleHeader with its variable-length data payload.
type HeapTuple struct {
	Header HeapTupleHeader
	Data   []byte // null bitmap + attribute values
}

// NewHeapTuple creates a tuple with the given xmin, attribute count, and data.
func NewHeapTuple(xmin TransactionId, natts uint16, data []byte) *HeapTuple {
	return &HeapTuple{
		Header: NewHeapTupleHeader(xmin, natts),
		Data:   data,
	}
}

// Len returns the total byte size: header + data.
func (t *HeapTuple) Len() int { return HeapTupleHeaderSize + len(t.Data) }

// ToBytes serializes the tuple to a byte slice.
func (t *HeapTuple) ToBytes() []byte {
	buf := make([]byte, t.Len())
	encodeHeapTupleHeader(buf[:HeapTupleHeaderSize], &t.Header)
	copy(buf[HeapTupleHeaderSize:], t.Data)
	return buf
}

// HeapTupleFromBytes deserializes a tuple from bytes.
// Returns an error if len(src) < HeapTupleHeaderSize.
func HeapTupleFromBytes(src []byte) (*HeapTuple, error) {
	if len(src) < HeapTupleHeaderSize {
		return nil, &StorageError{
			Code:    ErrInvalidPageLayout,
			Message: "bytes too short for HeapTupleHeader",
		}
	}
	h := decodeHeapTupleHeader(src[:HeapTupleHeaderSize])
	data := make([]byte, len(src)-HeapTupleHeaderSize)
	copy(data, src[HeapTupleHeaderSize:])
	return &HeapTuple{Header: h, Data: data}, nil
}
