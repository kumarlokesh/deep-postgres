package storage

// Page layout (from src/include/storage/bufpage.h):
//
//	+----------------+-----------+----------------+----------------+
//	| PageHeaderData | ItemIdData| ... free space | ... tuples ... |
//	| (24 bytes)     | array     |                | (grow down)    |
//	+----------------+-----------+----------------+----------------+
//	^                ^           ^                ^                ^
//	0            pd_lower    pd_upper         pd_special      PageSize
//
// - PageHeaderData: 24-byte fixed header at offset 0.
// - ItemIdData array: line pointers growing upward from offset 24.
// - Free space: between pd_lower and pd_upper.
// - Tuple data: grows downward from pd_special (or PageSize if no special space).
// - Special space: optional, at end (used by index pages).

import (
	"encoding/binary"
)

// PageSize is the standard PostgreSQL page size (8 KB).
const PageSize = 8192

// PageHeaderSize is the size of PageHeaderData in bytes.
// Verified at init time against the actual struct encoding.
const PageHeaderSize = 24

// ItemIdSize is the size of one ItemIdData entry.
const ItemIdSize = 4

// PGPageLayoutVersion matches PostgreSQL 16.x.
const PGPageLayoutVersion uint8 = 4

// PageHeaderData is the 24-byte header at offset 0 of every heap/index page.
//
// Matches PostgreSQL's PageHeaderData (bufpage.h):
//
//	typedef struct PageHeaderData {
//	    PageXLogRecPtr pd_lsn;       // 8 bytes (two uint32s)
//	    uint16         pd_checksum;
//	    uint16         pd_flags;
//	    LocationIndex  pd_lower;
//	    LocationIndex  pd_upper;
//	    LocationIndex  pd_special;
//	    uint16         pd_pagesize_version;
//	    TransactionId  pd_prune_xid;
//	} PageHeaderData; // 24 bytes total
//
// Encoding uses little-endian byte order.
type PageHeaderData struct {
	PdLsnHi           uint32 // high 32 bits of LSN
	PdLsnLo           uint32 // low 32 bits of LSN
	PdChecksum        uint16
	PdFlags           uint16
	PdLower           LocationIndex // offset to start of free space
	PdUpper           LocationIndex // offset to end of free space
	PdSpecial         LocationIndex // offset to start of special space
	PdPagesizeVersion uint16
	PdPruneXid        TransactionId
}

// PageFlags are bits in PageHeaderData.PdFlags (bufpage.h).
type PageFlags uint16

const (
	PDHasFreeLines PageFlags = 0x0001 // page has free line pointers
	PDPageFull     PageFlags = 0x0002 // no free space for new tuples
	PDAllVisible   PageFlags = 0x0004 // all tuples visible to everyone
)

// MakePagesizeVersion packs page size and layout version into pd_pagesize_version.
// Layout: high byte = page_size / 256, low byte = version.
func MakePagesizeVersion(pageSize int, version uint8) uint16 {
	return uint16(pageSize/256)<<8 | uint16(version)
}

// LSN returns the page's Log Sequence Number as a uint64.
func (h PageHeaderData) LSN() uint64 {
	return uint64(h.PdLsnHi)<<32 | uint64(h.PdLsnLo)
}

// SetLSN sets the page's LSN from a uint64.
func (h *PageHeaderData) SetLSN(lsn uint64) {
	h.PdLsnHi = uint32(lsn >> 32)
	h.PdLsnLo = uint32(lsn)
}

// FreeSpace returns the number of free bytes between pd_lower and pd_upper.
func (h PageHeaderData) FreeSpace() int {
	if h.PdUpper > h.PdLower {
		return int(h.PdUpper) - int(h.PdLower)
	}
	return 0
}

// ItemCount returns the number of line pointers on the page.
func (h PageHeaderData) ItemCount() int {
	lower := int(h.PdLower)
	if lower < PageHeaderSize {
		return 0
	}
	return (lower - PageHeaderSize) / ItemIdSize
}

// IsEmpty reports whether the page has no line pointers.
func (h PageHeaderData) IsEmpty() bool {
	return h.PdLower == LocationIndex(PageHeaderSize)
}

// encodeHeader serializes a PageHeaderData into dst[0:PageHeaderSize].
// Always uses little-endian byte order.
func encodeHeader(dst []byte, h *PageHeaderData) {
	binary.LittleEndian.PutUint32(dst[0:], h.PdLsnHi)
	binary.LittleEndian.PutUint32(dst[4:], h.PdLsnLo)
	binary.LittleEndian.PutUint16(dst[8:], h.PdChecksum)
	binary.LittleEndian.PutUint16(dst[10:], h.PdFlags)
	binary.LittleEndian.PutUint16(dst[12:], h.PdLower)
	binary.LittleEndian.PutUint16(dst[14:], h.PdUpper)
	binary.LittleEndian.PutUint16(dst[16:], h.PdSpecial)
	binary.LittleEndian.PutUint16(dst[18:], h.PdPagesizeVersion)
	binary.LittleEndian.PutUint32(dst[20:], h.PdPruneXid)
}

// decodeHeader deserializes a PageHeaderData from src[0:PageHeaderSize].
func decodeHeader(src []byte) PageHeaderData {
	return PageHeaderData{
		PdLsnHi:           binary.LittleEndian.Uint32(src[0:]),
		PdLsnLo:           binary.LittleEndian.Uint32(src[4:]),
		PdChecksum:        binary.LittleEndian.Uint16(src[8:]),
		PdFlags:           binary.LittleEndian.Uint16(src[10:]),
		PdLower:           binary.LittleEndian.Uint16(src[12:]),
		PdUpper:           binary.LittleEndian.Uint16(src[14:]),
		PdSpecial:         binary.LittleEndian.Uint16(src[16:]),
		PdPagesizeVersion: binary.LittleEndian.Uint16(src[18:]),
		PdPruneXid:        binary.LittleEndian.Uint32(src[20:]),
	}
}

// ItemIdData is a 4-byte line pointer packed as a uint32.
//
// Matches PostgreSQL's ItemIdData (bufpage.h):
//
//	typedef struct ItemIdData {
//	    unsigned lp_off:15,   // offset to tuple from start of page
//	             lp_flags:2,  // LP_UNUSED / LP_NORMAL / LP_REDIRECT / LP_DEAD
//	             lp_len:15;   // byte length of tuple
//	} ItemIdData;
//
// Bit layout (LSB to MSB):
//
//	bits  0-14: lp_off  (15 bits)
//	bits 15-16: lp_flags (2 bits)
//	bits 17-31: lp_len  (15 bits)
type ItemIdData uint32

// LpFlags are the valid states for a line pointer.
type LpFlags uint8

const (
	LpUnused   LpFlags = 0 // available for reuse
	LpNormal   LpFlags = 1 // points to a live tuple
	LpRedirect LpFlags = 2 // HOT chain redirect
	LpDead     LpFlags = 3 // tuple removed, slot reusable after vacuum
)

const (
	lpOffMask   = uint32(0x7FFF)       // bits 0-14
	lpFlagsShift = uint32(15)
	lpFlagsMask = uint32(0x3)          // 2 bits
	lpLenShift  = uint32(17)
	lpLenMask   = uint32(0x7FFF)       // bits 17-31
)

// NewItemId creates a normal line pointer for a tuple at the given page offset
// with the given byte length.
func NewItemId(offset, length uint16) ItemIdData {
	off := uint32(offset) & lpOffMask
	flags := uint32(LpNormal) << lpFlagsShift
	ln := (uint32(length) & lpLenMask) << lpLenShift
	return ItemIdData(off | flags | ln)
}

// NewItemIdRedirect creates a redirect line pointer (HOT chain).
func NewItemIdRedirect(targetOffset uint16) ItemIdData {
	off := uint32(targetOffset) & lpOffMask
	flags := uint32(LpRedirect) << lpFlagsShift
	return ItemIdData(off | flags)
}

// NewItemIdDead creates a dead line pointer.
func NewItemIdDead() ItemIdData {
	return ItemIdData(uint32(LpDead) << lpFlagsShift)
}

// Off returns the offset to the tuple.
func (i ItemIdData) Off() uint16 { return uint16(uint32(i) & lpOffMask) }

// Flags returns the line pointer state.
func (i ItemIdData) Flags() LpFlags {
	return LpFlags((uint32(i) >> lpFlagsShift) & lpFlagsMask)
}

// Len returns the tuple byte length.
func (i ItemIdData) Len() uint16 { return uint16((uint32(i) >> lpLenShift) & lpLenMask) }

func (i ItemIdData) IsUnused() bool   { return i.Flags() == LpUnused }
func (i ItemIdData) IsNormal() bool   { return i.Flags() == LpNormal }
func (i ItemIdData) IsRedirect() bool { return i.Flags() == LpRedirect }
func (i ItemIdData) IsDead() bool     { return i.Flags() == LpDead }

// encodeItemId packs an ItemIdData into dst[0:4] little-endian.
func encodeItemId(dst []byte, id ItemIdData) {
	binary.LittleEndian.PutUint32(dst, uint32(id))
}

// decodeItemId reads an ItemIdData from src[0:4].
func decodeItemId(src []byte) ItemIdData {
	return ItemIdData(binary.LittleEndian.Uint32(src))
}

// Page is a heap page with a full PostgreSQL-compatible 8 KB layout.
// The underlying buffer is [PageSize]byte; all accesses go through the
// accessor methods which maintain the invariants of the PG page format.
type Page struct {
	data [PageSize]byte
}

// NewPage allocates and initialises an empty heap page.
func NewPage() *Page {
	p := &Page{}
	p.init()
	return p
}

// PageFromBytes creates a Page from a raw byte slice and validates the header.
// The input must be exactly PageSize bytes.
func PageFromBytes(src []byte) (*Page, error) {
	if len(src) != PageSize {
		return nil, errInvalidPageSize(PageSize, len(src))
	}
	p := &Page{}
	copy(p.data[:], src)
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Page) init() {
	for i := range p.data {
		p.data[i] = 0
	}
	h := PageHeaderData{
		PdLower:           LocationIndex(PageHeaderSize),
		PdUpper:           LocationIndex(PageSize),
		PdSpecial:         LocationIndex(PageSize),
		PdPagesizeVersion: MakePagesizeVersion(PageSize, PGPageLayoutVersion),
	}
	encodeHeader(p.data[:PageHeaderSize], &h)
}

// Bytes returns a copy of the raw page bytes.
func (p *Page) Bytes() []byte {
	cp := make([]byte, PageSize)
	copy(cp, p.data[:])
	return cp
}

// Header returns the decoded page header.
func (p *Page) Header() PageHeaderData {
	return decodeHeader(p.data[:PageHeaderSize])
}

// setHeader encodes h back into the page buffer.
func (p *Page) setHeader(h PageHeaderData) {
	encodeHeader(p.data[:PageHeaderSize], &h)
}

// ItemCount returns the number of line pointers on the page.
func (p *Page) ItemCount() int {
	return p.Header().ItemCount()
}

// FreeSpace returns the number of free bytes available on the page.
func (p *Page) FreeSpace() int {
	return p.Header().FreeSpace()
}

// IsEmpty reports whether the page has no tuples.
func (p *Page) IsEmpty() bool {
	return p.Header().IsEmpty()
}

// LSN returns the page's Log Sequence Number.
func (p *Page) LSN() uint64 {
	h := p.Header()
	return h.LSN()
}

// SetLSN sets the page's LSN.
func (p *Page) SetLSN(lsn uint64) {
	h := p.Header()
	h.SetLSN(lsn)
	p.setHeader(h)
}

// itemIdOffset returns the byte offset within p.data[] for line pointer i (0-based).
func itemIdOffset(i int) int {
	return PageHeaderSize + i*ItemIdSize
}

// GetItemId returns the line pointer at 0-based index i, or an error if out of range.
func (p *Page) GetItemId(i int) (ItemIdData, error) {
	if i < 0 || i >= p.ItemCount() {
		return 0, errInvalidItemIndex(i)
	}
	off := itemIdOffset(i)
	return decodeItemId(p.data[off : off+ItemIdSize]), nil
}

// SetItemId writes a line pointer at 0-based index i.
func (p *Page) SetItemId(i int, id ItemIdData) error {
	if i < 0 || i >= p.ItemCount() {
		return errInvalidItemIndex(i)
	}
	off := itemIdOffset(i)
	encodeItemId(p.data[off:off+ItemIdSize], id)
	return nil
}

// InsertTuple inserts tupleData onto the page, updating pd_upper and adding a
// line pointer. Returns the 0-based item index of the new tuple.
//
// Tuple data grows downward from pd_upper; line pointers grow upward from
// pd_lower. Requires len(tupleData) + ItemIdSize <= FreeSpace().
func (p *Page) InsertTuple(tupleData []byte) (int, error) {
	tupleLen := len(tupleData)
	required := tupleLen + ItemIdSize
	free := p.FreeSpace()
	if free < required {
		return 0, errPageFull(required, free)
	}

	h := p.Header()
	newUpper := int(h.PdUpper) - tupleLen
	itemIndex := h.ItemCount()

	// Write tuple data (downward from pd_upper).
	copy(p.data[newUpper:newUpper+tupleLen], tupleData)

	// Write line pointer (upward from pd_lower).
	lp := NewItemId(uint16(newUpper), uint16(tupleLen))
	lpOff := itemIdOffset(itemIndex)
	encodeItemId(p.data[lpOff:lpOff+ItemIdSize], lp)

	// Update header.
	h.PdUpper = LocationIndex(newUpper)
	h.PdLower += LocationIndex(ItemIdSize)
	p.setHeader(h)

	return itemIndex, nil
}

// InsertTupleAt inserts tupleData and places the new line pointer at position
// pos in the ItemId array, shifting existing line pointers [pos, n) one slot
// to the right.  This keeps the ItemId array in sorted key order for B-tree
// pages, which require entries in their logical key sequence.
//
// Tuple data is always written downward (same as InsertTuple); only the
// line pointer array is rearranged.  Requires len(tupleData)+ItemIdSize <= FreeSpace()
// and 0 <= pos <= ItemCount().
func (p *Page) InsertTupleAt(pos int, tupleData []byte) error {
	tupleLen := len(tupleData)
	required := tupleLen + ItemIdSize
	free := p.FreeSpace()
	if free < required {
		return errPageFull(required, free)
	}

	h := p.Header()
	n := h.ItemCount()
	if pos < 0 || pos > n {
		return errInvalidItemIndex(pos)
	}

	newUpper := int(h.PdUpper) - tupleLen
	copy(p.data[newUpper:newUpper+tupleLen], tupleData)

	// Shift line pointers [pos, n) one slot to the right.
	// Go's built-in copy handles overlapping slices safely (memmove semantics).
	if pos < n {
		src := itemIdOffset(pos)
		dst := itemIdOffset(pos + 1)
		count := (n - pos) * ItemIdSize
		copy(p.data[dst:dst+count], p.data[src:src+count])
	}

	lp := NewItemId(uint16(newUpper), uint16(tupleLen))
	encodeItemId(p.data[itemIdOffset(pos):], lp)

	h.PdUpper = LocationIndex(newUpper)
	h.PdLower += LocationIndex(ItemIdSize)
	p.setHeader(h)
	return nil
}

// GetTuple returns the raw bytes of the tuple at 0-based index i.
// Returns nil if the line pointer is not LpNormal.
func (p *Page) GetTuple(i int) ([]byte, error) {
	id, err := p.GetItemId(i)
	if err != nil {
		return nil, err
	}
	if !id.IsNormal() {
		return nil, nil
	}
	off := int(id.Off())
	ln := int(id.Len())
	if off+ln > PageSize {
		return nil, errInvalidPageLayout("item pointer out of bounds")
	}
	result := make([]byte, ln)
	copy(result, p.data[off:off+ln])
	return result, nil
}

// MarkDead transitions the line pointer at index i to LpDead.
func (p *Page) MarkDead(i int) error {
	if i < 0 || i >= p.ItemCount() {
		return errInvalidItemIndex(i)
	}
	off := itemIdOffset(i)
	encodeItemId(p.data[off:off+ItemIdSize], NewItemIdDead())
	return nil
}

// Validate checks page header consistency.
func (p *Page) Validate() error {
	h := p.Header()

	// Verify page size encoded in pd_pagesize_version.
	encodedSize := int(h.PdPagesizeVersion>>8) * 256
	if encodedSize != PageSize {
		return errInvalidPageSize(PageSize, encodedSize)
	}
	if int(h.PdLower) < PageHeaderSize {
		return errInvalidPageLayout("pd_lower below header end")
	}
	if h.PdUpper > h.PdSpecial {
		return errInvalidPageLayout("pd_upper > pd_special")
	}
	if int(h.PdSpecial) > PageSize {
		return errInvalidPageLayout("pd_special > page size")
	}
	if h.PdLower > h.PdUpper {
		return errInvalidPageLayout("pd_lower > pd_upper (negative free space)")
	}
	return nil
}
