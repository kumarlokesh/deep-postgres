package storage

// B-tree index page layout (src/backend/access/nbtree/, src/include/access/nbtree.h).
//
// B-tree pages use the standard heap page layout but reserve BTPageOpaqueSize
// bytes of "special space" at the end for BTPageOpaqueData (sibling pointers,
// level, and flags).
//
//	+----------------+----------+----------------+--------------+---------------+
//	| PageHeaderData |ItemIdData| ... free space | index tuples | BTPageOpaque  |
//	| (24 bytes)     | array    |                | (grow down)  | (16 bytes)    |
//	+----------------+----------+----------------+--------------+---------------+
//	^                ^          ^                ^               ^               ^
//	0            pd_lower   pd_upper          (tuples)      pd_special       PageSize
//
// Index tuple format (IndexTupleData, itup.h):
//
//	+------------------+----------+
//	| IndexTupleData   | key data |
//	| (8 bytes)        |          |
//	+------------------+----------+

import "encoding/binary"

// BTPageOpaqueSize is the size of BTPageOpaqueData in bytes.
const BTPageOpaqueSize = 16

// IndexTupleHeaderSize is the size of IndexTupleData in bytes.
const IndexTupleHeaderSize = 8

// BTCurrentVersion is the current B-tree index format version.
const BTCurrentVersion uint32 = 4

// BTPageOpaqueData is the special space at the end of every B-tree page.
//
// Matches PostgreSQL's BTPageOpaqueData (nbtree.h):
//
//	typedef struct BTPageOpaqueData {
//	    BlockNumber btpo_prev;     // left sibling (P_NONE if leftmost)
//	    BlockNumber btpo_next;     // right sibling (P_NONE if rightmost)
//	    uint32      btpo_level;    // 0 for leaf pages
//	    uint16      btpo_flags;
//	    uint16      btpo_cycleid;  // vacuum cycle ID of latest split
//	} BTPageOpaqueData; // 16 bytes
type BTPageOpaqueData struct {
	BtpoPrev    BlockNumber
	BtpoNext    BlockNumber
	BtpoLevel   uint32
	BtpoFlags   uint16
	BtpoCycleid uint16
}

// BTreePageFlags are bits in BTPageOpaqueData.BtpoFlags.
type BTreePageFlags uint16

const (
	BTPLeaf       BTreePageFlags = 1 << 0 // leaf page
	BTPRoot       BTreePageFlags = 1 << 1 // root page
	BTPDeleted    BTreePageFlags = 1 << 2 // deleted (on freelist)
	BTPMeta       BTreePageFlags = 1 << 3 // meta page
	BTPHalfDead   BTreePageFlags = 1 << 4 // half-dead (being deleted)
	BTPSplitEnd   BTreePageFlags = 1 << 5 // split, follow right link
	BTPHasGarbage BTreePageFlags = 1 << 6 // has LP_DEAD items
	BTPIncomplete BTreePageFlags = 1 << 7 // incomplete split
)

// BTreePageType describes the role of a B-tree page.
type BTreePageType int

const (
	BTreeLeaf         BTreePageType = iota // leaf page (bottom level, holds actual index entries)
	BTreeInternal                          // internal page (holds downlinks to child pages)
	BTreeRootLeaf                          // root page that is also a leaf (single-page tree)
	BTreeRootInternal                      // root page that is internal
	BTreeMeta                              // meta page (page 0, holds btm_root etc.)
)

func (t BTreePageType) flags() BTreePageFlags {
	switch t {
	case BTreeLeaf:
		return BTPLeaf
	case BTreeInternal:
		return 0
	case BTreeRootLeaf:
		return BTPRoot | BTPLeaf
	case BTreeRootInternal:
		return BTPRoot
	case BTreeMeta:
		return BTPMeta
	default:
		return 0
	}
}

func (t BTreePageType) level() uint32 {
	switch t {
	case BTreeLeaf, BTreeRootLeaf, BTreeMeta:
		return 0
	default:
		return 1 // simplified; real PG tracks actual depth
	}
}

// encodeBTOpaque serializes BTPageOpaqueData into dst[0:BTPageOpaqueSize].
func encodeBTOpaque(dst []byte, o *BTPageOpaqueData) {
	binary.LittleEndian.PutUint32(dst[0:], o.BtpoPrev)
	binary.LittleEndian.PutUint32(dst[4:], o.BtpoNext)
	binary.LittleEndian.PutUint32(dst[8:], o.BtpoLevel)
	binary.LittleEndian.PutUint16(dst[12:], o.BtpoFlags)
	binary.LittleEndian.PutUint16(dst[14:], o.BtpoCycleid)
}

// decodeBTOpaque reads BTPageOpaqueData from src[0:BTPageOpaqueSize].
func decodeBTOpaque(src []byte) BTPageOpaqueData {
	return BTPageOpaqueData{
		BtpoPrev:    binary.LittleEndian.Uint32(src[0:]),
		BtpoNext:    binary.LittleEndian.Uint32(src[4:]),
		BtpoLevel:   binary.LittleEndian.Uint32(src[8:]),
		BtpoFlags:   binary.LittleEndian.Uint16(src[12:]),
		BtpoCycleid: binary.LittleEndian.Uint16(src[14:]),
	}
}

// IndexTupleData is the 8-byte header prepended to every index tuple.
//
// Matches PostgreSQL's IndexTupleData (itup.h):
//
//	typedef struct IndexTupleData {
//	    ItemPointerData t_tid;  // heap TID (block + offset, 6 bytes)
//	    unsigned short  t_info; // size | flags
//	} IndexTupleData; // 8 bytes
//
// t_info layout:
//
//	bits 0-12: tuple size (including this header)
//	bit   13 : has nulls
//	bit   14 : has variable-width attrs
//	bit   15 : unused
type IndexTupleData struct {
	TTidBlock  BlockNumber
	TTidOffset OffsetNumber
	TInfo      uint16
}

const (
	indexSizeMask uint16 = 0x1FFF
	indexNullMask uint16 = 0x2000
	indexVarMask  uint16 = 0x4000
)

// NewIndexTuple creates an IndexTupleData pointing to the given heap TID.
// size is the total byte size of the index tuple (header + key data).
func NewIndexTuple(heapBlock BlockNumber, heapOffset OffsetNumber, size uint16) IndexTupleData {
	return IndexTupleData{
		TTidBlock:  heapBlock,
		TTidOffset: heapOffset,
		TInfo:      size & indexSizeMask,
	}
}

func (t *IndexTupleData) Size() uint16      { return t.TInfo & indexSizeMask }
func (t *IndexTupleData) HasNulls() bool    { return t.TInfo&indexNullMask != 0 }
func (t *IndexTupleData) HasVarWidth() bool { return t.TInfo&indexVarMask != 0 }
func (t *IndexTupleData) HeapTid() (BlockNumber, OffsetNumber) {
	return t.TTidBlock, t.TTidOffset
}

func encodeIndexTuple(dst []byte, it *IndexTupleData) {
	binary.LittleEndian.PutUint32(dst[0:], it.TTidBlock)
	binary.LittleEndian.PutUint16(dst[4:], it.TTidOffset)
	binary.LittleEndian.PutUint16(dst[6:], it.TInfo)
}

func decodeIndexTuple(src []byte) IndexTupleData {
	return IndexTupleData{
		TTidBlock:  binary.LittleEndian.Uint32(src[0:]),
		TTidOffset: binary.LittleEndian.Uint16(src[4:]),
		TInfo:      binary.LittleEndian.Uint16(src[6:]),
	}
}

// opaqueOffset is where the opaque data starts within the page buffer.
const opaqueOffset = PageSize - BTPageOpaqueSize

// BTreePage wraps a Page and exposes B-tree-specific operations.
// The underlying Page has pd_special = opaqueOffset and carries
// BTPageOpaqueData in the last BTPageOpaqueSize bytes.
type BTreePage struct {
	page *Page
}

// NewBTreePage creates a fresh B-tree page of the given type.
func NewBTreePage(pageType BTreePageType) *BTreePage {
	p := NewPage()

	// Carve out special space: reduce pd_upper and set pd_special.
	h := p.Header()
	h.PdSpecial = LocationIndex(opaqueOffset)
	h.PdUpper = LocationIndex(opaqueOffset)
	encodeHeader(p.data[:PageHeaderSize], &h)

	opaque := BTPageOpaqueData{
		BtpoPrev:  InvalidBlockNumber,
		BtpoNext:  InvalidBlockNumber,
		BtpoLevel: pageType.level(),
		BtpoFlags: uint16(pageType.flags()),
	}
	encodeBTOpaque(p.data[opaqueOffset:], &opaque)

	return &BTreePage{page: p}
}

// BTreePageFromPage wraps an existing Page as a BTreePage.
// Returns an error if pd_special does not match the expected B-tree layout.
func BTreePageFromPage(p *Page) (*BTreePage, error) {
	h := p.Header()
	if int(h.PdSpecial) != opaqueOffset {
		return nil, errInvalidPageLayout("not a B-tree page: wrong pd_special")
	}
	return &BTreePage{page: p}, nil
}

// Page returns the underlying Page.
func (b *BTreePage) Page() *Page { return b.page }

// Opaque returns the decoded BTPageOpaqueData.
func (b *BTreePage) Opaque() BTPageOpaqueData {
	return decodeBTOpaque(b.page.data[opaqueOffset:])
}

// setOpaque writes updated opaque data back.
func (b *BTreePage) setOpaque(o BTPageOpaqueData) {
	encodeBTOpaque(b.page.data[opaqueOffset:], &o)
}

// IsLeaf reports whether this is a leaf page.
func (b *BTreePage) IsLeaf() bool {
	return BTreePageFlags(b.Opaque().BtpoFlags)&BTPLeaf != 0
}

// IsRoot reports whether this is the root page.
func (b *BTreePage) IsRoot() bool {
	return BTreePageFlags(b.Opaque().BtpoFlags)&BTPRoot != 0
}

// IsMeta reports whether this is the meta page.
func (b *BTreePage) IsMeta() bool {
	return BTreePageFlags(b.Opaque().BtpoFlags)&BTPMeta != 0
}

// Level returns the tree level (0 = leaf).
func (b *BTreePage) Level() uint32 { return b.Opaque().BtpoLevel }

// LeftSibling returns the left sibling block number, or (_, false) if none.
func (b *BTreePage) LeftSibling() (BlockNumber, bool) {
	prev := b.Opaque().BtpoPrev
	if prev == InvalidBlockNumber {
		return 0, false
	}
	return prev, true
}

// RightSibling returns the right sibling block number, or (_, false) if none.
func (b *BTreePage) RightSibling() (BlockNumber, bool) {
	next := b.Opaque().BtpoNext
	if next == InvalidBlockNumber {
		return 0, false
	}
	return next, true
}

// SetSiblings sets the left and right sibling pointers.
// Pass InvalidBlockNumber for a missing sibling.
func (b *BTreePage) SetSiblings(left, right BlockNumber) {
	o := b.Opaque()
	o.BtpoPrev = left
	o.BtpoNext = right
	b.setOpaque(o)
}

// NumEntries returns the number of index entries on this page.
func (b *BTreePage) NumEntries() int { return b.page.ItemCount() }

// FreeSpace returns available bytes for new entries.
func (b *BTreePage) FreeSpace() int { return b.page.FreeSpace() }

// InsertEntry appends an index entry with the given key data pointing to
// (heapBlock, heapOffset) in the heap. Returns the 0-based slot index.
func (b *BTreePage) InsertEntry(key []byte, heapBlock BlockNumber, heapOffset OffsetNumber) (int, error) {
	tupleSize := IndexTupleHeaderSize + len(key)
	header := NewIndexTuple(heapBlock, heapOffset, uint16(tupleSize))

	buf := make([]byte, tupleSize)
	encodeIndexTuple(buf[:IndexTupleHeaderSize], &header)
	copy(buf[IndexTupleHeaderSize:], key)

	return b.page.InsertTuple(buf)
}

// InsertDownlink inserts a downlink for an internal page.
// childBlock is the block number of the child; key is the high key.
func (b *BTreePage) InsertDownlink(key []byte, childBlock BlockNumber) (int, error) {
	return b.InsertEntry(key, childBlock, 0)
}

// GetEntry returns the key bytes and heap TID for the 0-based slot index.
func (b *BTreePage) GetEntry(i int) (key []byte, block BlockNumber, offset OffsetNumber, err error) {
	raw, err := b.page.GetTuple(i)
	if err != nil {
		return nil, 0, 0, err
	}
	if raw == nil {
		return nil, 0, 0, errInvalidItemIndex(i)
	}
	if len(raw) < IndexTupleHeaderSize {
		return nil, 0, 0, errInvalidPageLayout("index tuple too short")
	}
	hdr := decodeIndexTuple(raw[:IndexTupleHeaderSize])
	blk, off := hdr.HeapTid()
	k := make([]byte, len(raw)-IndexTupleHeaderSize)
	copy(k, raw[IndexTupleHeaderSize:])
	return k, blk, off, nil
}

// SearchLeaf returns the 0-based position of the first entry where
// cmp(entryKey) >= 0 (i.e. entryKey >= searchKey). This is a standard
// lower-bound binary search. cmp should return a negative value when
// entryKey < searchKey, zero when equal, and positive when greater.
// Returns NumEntries() if all entries are less than the search key.
func (b *BTreePage) SearchLeaf(cmp func(entryKey []byte) int) int {
	n := b.NumEntries()
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		key, _, _, err := b.GetEntry(mid)
		if err != nil {
			break
		}
		if cmp(key) < 0 {
			// entry key < search key → first match is to the right
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// SearchInternal returns the child BlockNumber to follow for the given key.
// Returns (InvalidBlockNumber, false) if the page is empty.
func (b *BTreePage) SearchInternal(cmp func(entryKey []byte) int) (BlockNumber, bool) {
	n := b.NumEntries()
	if n == 0 {
		return InvalidBlockNumber, false
	}
	pos := b.SearchLeaf(cmp)
	// Internal page: pos 0 → leftmost child; pos i → child at i-1.
	entryIdx := pos
	if entryIdx >= n {
		entryIdx = n - 1
	}
	if pos > 0 {
		entryIdx = pos - 1
	}
	_, blk, _, err := b.GetEntry(entryIdx)
	if err != nil {
		return InvalidBlockNumber, false
	}
	return blk, true
}
