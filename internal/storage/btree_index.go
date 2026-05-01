package storage

import "encoding/binary"

// BTreeIndex implements a persistent, multi-page B-tree index backed by a
// Relation.  The design maps closely to PostgreSQL's nbtree AM:
//
//   - Block 0: meta page (BTMetaPageData embedded in the special space).
//   - Block 1: initial root/leaf page (a combined BTP_ROOT | BTP_LEAF page).
//   - Subsequent blocks: split halves and new internal pages.
//
// Key comparison is caller-supplied via a Comparator, which must implement a
// total order consistent with the stored keys.
//
// Thread safety: single-threaded; no locking.
//
// Limitations (intentional for this research stage):
//   - Keys must be fixed-width (variable-width support is future work).
//   - Duplicate keys are allowed; they are stored in insertion order within
//     the same leaf key group.
//   - Only the main fork is used.

// Comparator compares two key byte slices.
// Returns negative if a < b, zero if a == b, positive if a > b.
type Comparator func(a, b []byte) int

// btreeMeta is stored starting at PageHeaderSize in the meta page's data area.
// We embed it after the page header in block 0.
type btreeMeta struct {
	Version   uint32
	Root      BlockNumber
	Level     uint32
	FastRoot  BlockNumber
	FastLevel uint32
}

const btreeMetaVersion = 4

// metaBlockNum is always block 0.
const metaBlockNum BlockNumber = 0

// BTreeIndex is the top-level handle for a B-tree index stored in rel.
type BTreeIndex struct {
	rel *Relation
	cmp Comparator
}

// NewBTreeIndex creates and initialises a new B-tree index in rel.
// rel must already have been Init()d (fork files created).
// On return the meta page and an initial root/leaf page have been written.
func NewBTreeIndex(rel *Relation, cmp Comparator) (*BTreeIndex, error) {
	idx := &BTreeIndex{rel: rel, cmp: cmp}

	// Allocate meta page (block 0).
	_, metaId, err := rel.Extend(ForkMain)
	if err != nil {
		return nil, err
	}
	defer rel.Pool.UnpinBuffer(metaId)

	// Allocate initial root/leaf page (block 1).
	rootBlk, rootId, err := rel.Extend(ForkMain)
	if err != nil {
		return nil, err
	}
	defer rel.Pool.UnpinBuffer(rootId)

	// Initialise root as a combined root+leaf B-tree page.
	rootPage, err := rel.Pool.GetPageForWrite(rootId)
	if err != nil {
		return nil, err
	}
	bt := NewBTreePage(BTreeRootLeaf)
	copy(rootPage.data[:], bt.page.data[:])

	// Write meta.
	metaPage, err := rel.Pool.GetPageForWrite(metaId)
	if err != nil {
		return nil, err
	}
	idx.writeMeta(metaPage, btreeMeta{
		Version:   btreeMetaVersion,
		Root:      rootBlk,
		Level:     0,
		FastRoot:  rootBlk,
		FastLevel: 0,
	})

	if err := rel.Flush(ForkMain); err != nil {
		return nil, err
	}
	return idx, nil
}

// OpenBTreeIndex opens an existing B-tree index in rel (reads meta from block 0).
func OpenBTreeIndex(rel *Relation, cmp Comparator) (*BTreeIndex, error) {
	idx := &BTreeIndex{rel: rel, cmp: cmp}
	if _, err := idx.readMeta(); err != nil {
		return nil, err
	}
	return idx, nil
}

// ── Meta page helpers ────────────────────────────────────────────────────────

// metaOffset is the byte offset within the page where btreeMeta is stored
// (right after the page header).
const metaDataOffset = PageHeaderSize

func (idx *BTreeIndex) writeMeta(page *Page, m btreeMeta) {
	b := page.data[metaDataOffset:]
	putU32(b[0:], m.Version)
	putU32(b[4:], m.Root)
	putU32(b[8:], m.Level)
	putU32(b[12:], m.FastRoot)
	putU32(b[16:], m.FastLevel)
}

func (idx *BTreeIndex) readMeta() (btreeMeta, error) {
	id, err := idx.rel.ReadBlock(ForkMain, metaBlockNum)
	if err != nil {
		return btreeMeta{}, err
	}
	defer idx.rel.Pool.UnpinBuffer(id)
	page, err := idx.rel.Pool.GetPage(id)
	if err != nil {
		return btreeMeta{}, err
	}
	b := page.data[metaDataOffset:]
	return btreeMeta{
		Version:   getU32(b[0:]),
		Root:      getU32(b[4:]),
		Level:     getU32(b[8:]),
		FastRoot:  getU32(b[12:]),
		FastLevel: getU32(b[16:]),
	}, nil
}

func (idx *BTreeIndex) updateMetaRoot(root BlockNumber, level uint32) error {
	id, err := idx.rel.ReadBlock(ForkMain, metaBlockNum)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(id)
	page, err := idx.rel.Pool.GetPageForWrite(id)
	if err != nil {
		return err
	}
	m, _ := idx.readMeta()
	m.Root = root
	m.Level = level
	m.FastRoot = root
	m.FastLevel = level
	idx.writeMeta(page, m)
	return nil
}

// ── Insert ───────────────────────────────────────────────────────────────────

// Insert adds (key, heapBlock, heapOffset) to the index.
func (idx *BTreeIndex) Insert(key []byte, heapBlock BlockNumber, heapOffset OffsetNumber) error {
	meta, err := idx.readMeta()
	if err != nil {
		return err
	}
	// Descend to leaf, collecting parent path.
	path, leafBlk, err := idx.findLeaf(meta.Root, meta.Level, key)
	if err != nil {
		return err
	}
	return idx.insertAtLeaf(path, leafBlk, key, heapBlock, heapOffset)
}

// findLeaf descends from rootBlk at tree level rootLevel, returning the
// descent path (parent block+position pairs) and the leaf block number.
func (idx *BTreeIndex) findLeaf(rootBlk BlockNumber, _ uint32, key []byte) ([]btreePath, BlockNumber, error) {
	var path []btreePath
	blk := rootBlk

	for {
		id, err := idx.rel.ReadBlock(ForkMain, blk)
		if err != nil {
			return nil, InvalidBlockNumber, err
		}
		page, err := idx.rel.Pool.GetPage(id)
		if err != nil {
			idx.rel.Pool.UnpinBuffer(id)
			return nil, InvalidBlockNumber, err
		}
		bp, err := BTreePageFromPage(page)
		if err != nil {
			idx.rel.Pool.UnpinBuffer(id)
			return nil, InvalidBlockNumber, err
		}

		// BTPIncomplete means this page was split but the parent downlink was
		// not yet inserted.  The key may already live on the right sibling.
		if BTreePageFlags(bp.Opaque().BtpoFlags)&BTPIncomplete != 0 {
			next := bp.Opaque().BtpoNext
			idx.rel.Pool.UnpinBuffer(id)
			blk = next
			continue
		}

		// Right-link traversal: if the search key exceeds this page's high key
		// the page was split since we latched it and the key is now on the right.
		if hk, ok := bp.HighKey(); ok {
			if idx.cmp(key, hk) > 0 {
				next := bp.Opaque().BtpoNext
				idx.rel.Pool.UnpinBuffer(id)
				blk = next
				continue
			}
		}

		if bp.IsLeaf() {
			idx.rel.Pool.UnpinBuffer(id)
			return path, blk, nil
		}

		// Internal page: choose child to follow.
		//
		// SearchLeaf returns the first position where entryKey >= key.
		// Internal entries are: [0]=leftmost child (zero/placeholder key),
		// [i]=child whose subtree begins at separator key[i].
		//
		// Because the zero-key placeholder at pos 0 is lexicographically
		// less than any real key, SearchLeaf always returns pos >= 1 for
		// non-zero keys.  We need to check: if the found entry's key is
		// strictly greater than searchKey, the correct child is pos-1.
		// Otherwise (key == entryKey[pos] or pos == 0) follow pos.
		pos := bp.SearchLeaf(func(entryKey []byte) int { return idx.cmp(entryKey, key) })
		n := bp.NumEntries()
		if pos >= n {
			pos = n - 1
		} else if pos > 0 {
			ek, _, _, ekErr := bp.GetEntry(pos)
			if ekErr != nil {
				idx.rel.Pool.UnpinBuffer(id)
				return nil, InvalidBlockNumber, ekErr
			}
			if idx.cmp(ek, key) > 0 {
				pos-- // separator > key → key lives in the subtree to the left
			}
		}
		_, childBlk, _, err := bp.GetEntry(pos)
		if err != nil {
			idx.rel.Pool.UnpinBuffer(id)
			return nil, InvalidBlockNumber, err
		}
		path = append(path, btreePath{blockNum: blk, pos: pos})
		idx.rel.Pool.UnpinBuffer(id)
		blk = childBlk
	}
}

// btreePath records a step during descent so we can propagate splits upward.
type btreePath struct {
	blockNum BlockNumber
	pos      int
}

// insertAtLeaf inserts the entry into the leaf page.  If the page is full it
// splits the page and propagates the split upward.
func (idx *BTreeIndex) insertAtLeaf(path []btreePath, leafBlk BlockNumber, key []byte, heapBlock BlockNumber, heapOffset OffsetNumber) error {
	id, err := idx.rel.ReadBlock(ForkMain, leafBlk)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(id)

	page, err := idx.rel.Pool.GetPageForWrite(id)
	if err != nil {
		return err
	}
	bp, err := BTreePageFromPage(page)
	if err != nil {
		return err
	}

	// Find insertion position to maintain sorted order.
	pos := bp.SearchLeaf(func(ek []byte) int { return idx.cmp(ek, key) })

	tupleSize := IndexTupleHeaderSize + len(key)
	required := tupleSize + ItemIdSize
	if bp.FreeSpace() >= required {
		return bp.InsertEntrySortedAt(pos, key, heapBlock, heapOffset)
	}

	// Page is full: split.
	return idx.splitLeaf(path, leafBlk, bp, pos, key, heapBlock, heapOffset)
}

// splitLeaf splits the full leaf page, inserts the new key in the correct
// half, updates sibling pointers, and propagates the split upward into the
// parent (or creates a new root).
func (idx *BTreeIndex) splitLeaf(path []btreePath, leftBlk BlockNumber, leftBP *BTreePage, insertPos int, key []byte, heapBlock BlockNumber, heapOffset OffsetNumber) error {
	n := leftBP.NumEntries()

	// Save the old high key before rebuilding — the right page inherits it when
	// the original left was itself non-rightmost.
	oldHighKey, hasOldHighKey := leftBP.HighKey()

	// Collect all data entries plus the new one.
	type entry struct {
		key   []byte
		block BlockNumber
		off   OffsetNumber
	}
	all := make([]entry, 0, n+1)
	for i := 0; i < n; i++ {
		k, b, o, err := leftBP.GetEntry(i)
		if err != nil {
			return err
		}
		all = append(all, entry{k, b, o})
	}
	newE := entry{key, heapBlock, heapOffset}
	all = append(all, entry{})
	copy(all[insertPos+1:], all[insertPos:])
	all[insertPos] = newE

	total := len(all)
	splitPos := total / 2

	// The promoted separator key is the first key of the right page.
	promotedKey := make([]byte, len(all[splitPos].key))
	copy(promotedKey, all[splitPos].key)

	// Allocate the right page.
	rightBlk, rightId, err := idx.rel.Extend(ForkMain)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(rightId)

	rightPage, err := idx.rel.Pool.GetPageForWrite(rightId)
	if err != nil {
		return err
	}

	// Determine page types: preserve root flag on left if it was root.
	leftIsRoot := leftBP.IsRoot()
	var leftType, rightType BTreePageType
	if leftIsRoot {
		leftType = BTreeRootLeaf
	} else {
		leftType = BTreeLeaf
	}
	rightType = BTreeLeaf

	// Rebuild left page with data entries [0, splitPos) while still rightmost.
	newLeft := NewBTreePage(leftType)
	for i := 0; i < splitPos; i++ {
		if err := newLeft.InsertEntrySortedAt(i, all[i].key, all[i].block, all[i].off); err != nil {
			return err
		}
	}
	// Rebuild right page with data entries [splitPos, total) while still rightmost.
	newRight := NewBTreePage(rightType)
	for i := splitPos; i < total; i++ {
		if err := newRight.InsertEntrySortedAt(i-splitPos, all[i].key, all[i].block, all[i].off); err != nil {
			return err
		}
	}

	// Update sibling chains.
	// Use Opaque() directly for raw block numbers (InvalidBlockNumber must be
	// preserved; the bool-returning helpers return 0 on "no sibling", which
	// would collide with the meta-page block number).
	leftOldOpaque := leftBP.Opaque()
	newRight.SetSiblings(leftBlk, leftOldOpaque.BtpoNext)
	newLeft.SetSiblings(leftOldOpaque.BtpoPrev, rightBlk)
	if leftOldOpaque.BtpoNext != InvalidBlockNumber {
		if err := idx.updateLeftSibling(leftOldOpaque.BtpoNext, rightBlk); err != nil {
			return err
		}
	}

	// Set high keys now that sibling pointers are wired (IsRightmost() is accurate).
	// Left page is non-rightmost after SetSiblings → high key = first key of right page.
	if err := newLeft.SetHighKey(promotedKey); err != nil {
		return err
	}
	// Right page inherits the original left page's high key when left was non-rightmost.
	if hasOldHighKey && !newRight.IsRightmost() {
		if err := newRight.SetHighKey(oldHighKey); err != nil {
			return err
		}
	}

	// Copy rebuilt pages back to the buffer pool.
	leftId, err := idx.rel.ReadBlock(ForkMain, leftBlk)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(leftId)
	leftPage, err := idx.rel.Pool.GetPageForWrite(leftId)
	if err != nil {
		return err
	}
	copy(leftPage.data[:], newLeft.page.data[:])
	copy(rightPage.data[:], newRight.page.data[:])

	// Mark left page incomplete until the parent downlink is inserted.
	// This lets findLeaf detect a crash-interrupted split and follow the right-link.
	{
		bp, _ := BTreePageFromPage(leftPage)
		o := bp.Opaque()
		o.BtpoFlags |= uint16(BTPIncomplete)
		bp.setOpaque(o)
	}

	var parentErr error
	if leftIsRoot {
		parentErr = idx.createNewRoot(leftBlk, rightBlk, promotedKey, newLeft.Level()+1)
	} else {
		parentErr = idx.insertInParent(path, leftBlk, rightBlk, promotedKey)
	}
	if parentErr != nil {
		return parentErr
	}

	// Clear BTPIncomplete now that the parent downlink is in place.
	{
		bp, _ := BTreePageFromPage(leftPage)
		o := bp.Opaque()
		o.BtpoFlags &^= uint16(BTPIncomplete)
		bp.setOpaque(o)
	}
	return nil
}

// createNewRoot allocates a new internal root page with two downlinks.
func (idx *BTreeIndex) createNewRoot(leftBlk, rightBlk BlockNumber, separatorKey []byte, level uint32) error {
	newRootBlk, newRootId, err := idx.rel.Extend(ForkMain)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(newRootId)

	newRootPage, err := idx.rel.Pool.GetPageForWrite(newRootId)
	if err != nil {
		return err
	}

	bp := NewBTreePage(BTreeRootInternal)
	// Internal page opaque level.
	o := bp.Opaque()
	o.BtpoLevel = level
	bp.setOpaque(o)
	// Insert left downlink (no key — leftmost child uses empty/smallest key).
	leftKey := make([]byte, len(separatorKey)) // placeholder: same width, zero-filled
	if err := bp.InsertEntrySortedAt(0, leftKey, leftBlk, 0); err != nil {
		return err
	}
	// Insert right downlink with the separator key.
	if err := bp.InsertEntrySortedAt(1, separatorKey, rightBlk, 0); err != nil {
		return err
	}

	copy(newRootPage.data[:], bp.page.data[:])

	// Demote old root (left child): clear BTP_ROOT flag.
	leftId, err := idx.rel.ReadBlock(ForkMain, leftBlk)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(leftId)
	leftPage, err := idx.rel.Pool.GetPageForWrite(leftId)
	if err != nil {
		return err
	}
	leftBP, _ := BTreePageFromPage(leftPage)
	o = leftBP.Opaque()
	o.BtpoFlags &^= uint16(BTPRoot)
	leftBP.setOpaque(o)

	return idx.updateMetaRoot(newRootBlk, level)
}

// insertInParent inserts a downlink for rightBlk into the parent page.
// If the parent is full, it splits recursively.
func (idx *BTreeIndex) insertInParent(path []btreePath, leftBlk, rightBlk BlockNumber, separatorKey []byte) error {
	if len(path) == 0 {
		// No parent in path — the left page was the root.
		return idx.createNewRoot(leftBlk, rightBlk, separatorKey, 1)
	}

	parent := path[len(path)-1]
	path = path[:len(path)-1]

	parentId, err := idx.rel.ReadBlock(ForkMain, parent.blockNum)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(parentId)

	parentPage, err := idx.rel.Pool.GetPageForWrite(parentId)
	if err != nil {
		return err
	}
	parentBP, err := BTreePageFromPage(parentPage)
	if err != nil {
		return err
	}

	// Find where the new downlink belongs in the parent.
	pos := parentBP.SearchLeaf(func(ek []byte) int { return idx.cmp(ek, separatorKey) })

	tupleSize := IndexTupleHeaderSize + len(separatorKey)
	required := tupleSize + ItemIdSize
	if parentBP.FreeSpace() >= required {
		return parentBP.InsertEntrySortedAt(pos, separatorKey, rightBlk, 0)
	}

	// Parent is also full — split it.
	return idx.splitInternal(path, parent.blockNum, parentBP, pos, separatorKey, rightBlk)
}

// splitInternal splits a full internal page and propagates upward.
func (idx *BTreeIndex) splitInternal(path []btreePath, leftBlk BlockNumber, leftBP *BTreePage, insertPos int, key []byte, childBlk BlockNumber) error {
	n := leftBP.NumEntries()

	// Save old high key before rebuilding.
	oldHighKey, hasOldHighKey := leftBP.HighKey()

	type entry struct {
		key   []byte
		block BlockNumber
		off   OffsetNumber
	}
	all := make([]entry, 0, n+1)
	for i := 0; i < n; i++ {
		k, b, o, err := leftBP.GetEntry(i)
		if err != nil {
			return err
		}
		all = append(all, entry{k, b, o})
	}
	newE := entry{key, childBlk, 0}
	all = append(all, entry{})
	copy(all[insertPos+1:], all[insertPos:])
	all[insertPos] = newE

	total := len(all)
	splitPos := total / 2

	// Middle entry is promoted to the parent; left keeps [0,splitPos),
	// right gets [splitPos+1, total).
	promotedKey := make([]byte, len(all[splitPos].key))
	copy(promotedKey, all[splitPos].key)

	rightBlk, rightId, err := idx.rel.Extend(ForkMain)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(rightId)
	rightPage, err := idx.rel.Pool.GetPageForWrite(rightId)
	if err != nil {
		return err
	}

	leftIsRoot := leftBP.IsRoot()
	leftLevel := leftBP.Level()

	// Rebuild both halves while rightmost (no high key yet).
	newLeft := NewBTreePage(BTreeInternal)
	newRight := NewBTreePage(BTreeInternal)
	if leftIsRoot {
		o := newLeft.Opaque()
		o.BtpoFlags |= uint16(BTPRoot)
		newLeft.setOpaque(o)
	}
	for _, bp := range []*BTreePage{newLeft, newRight} {
		o := bp.Opaque()
		o.BtpoLevel = leftLevel
		bp.setOpaque(o)
	}

	for i := 0; i < splitPos; i++ {
		if err := newLeft.InsertEntrySortedAt(i, all[i].key, all[i].block, all[i].off); err != nil {
			return err
		}
	}
	// Skip splitPos — it moves up as the separator.
	for i := splitPos + 1; i < total; i++ {
		if err := newRight.InsertEntrySortedAt(i-splitPos-1, all[i].key, all[i].block, all[i].off); err != nil {
			return err
		}
	}

	leftOldOpaque := leftBP.Opaque()
	newRight.SetSiblings(leftBlk, leftOldOpaque.BtpoNext)
	newLeft.SetSiblings(leftOldOpaque.BtpoPrev, rightBlk)
	if leftOldOpaque.BtpoNext != InvalidBlockNumber {
		if err := idx.updateLeftSibling(leftOldOpaque.BtpoNext, rightBlk); err != nil {
			return err
		}
	}

	// Set high keys now that sibling pointers are correct.
	// Left page high key = the promoted separator (upper bound of the left half).
	if err := newLeft.SetHighKey(promotedKey); err != nil {
		return err
	}
	if hasOldHighKey && !newRight.IsRightmost() {
		if err := newRight.SetHighKey(oldHighKey); err != nil {
			return err
		}
	}

	leftId, err := idx.rel.ReadBlock(ForkMain, leftBlk)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(leftId)
	leftPage, err := idx.rel.Pool.GetPageForWrite(leftId)
	if err != nil {
		return err
	}
	copy(leftPage.data[:], newLeft.page.data[:])
	copy(rightPage.data[:], newRight.page.data[:])

	// BTPIncomplete signals an in-progress split until the parent downlink lands.
	{
		bp, _ := BTreePageFromPage(leftPage)
		o := bp.Opaque()
		o.BtpoFlags |= uint16(BTPIncomplete)
		bp.setOpaque(o)
	}

	var parentErr error
	if leftIsRoot {
		parentErr = idx.createNewRoot(leftBlk, rightBlk, promotedKey, leftLevel+1)
	} else {
		parentErr = idx.insertInParent(path, leftBlk, rightBlk, promotedKey)
	}
	if parentErr != nil {
		return parentErr
	}

	{
		bp, _ := BTreePageFromPage(leftPage)
		o := bp.Opaque()
		o.BtpoFlags &^= uint16(BTPIncomplete)
		bp.setOpaque(o)
	}
	return nil
}

// updateLeftSibling sets the btpo_prev of the page at blk to newPrev.
func (idx *BTreeIndex) updateLeftSibling(blk, newPrev BlockNumber) error {
	id, err := idx.rel.ReadBlock(ForkMain, blk)
	if err != nil {
		return err
	}
	defer idx.rel.Pool.UnpinBuffer(id)
	page, err := idx.rel.Pool.GetPageForWrite(id)
	if err != nil {
		return err
	}
	bp, err := BTreePageFromPage(page)
	if err != nil {
		return err
	}
	o := bp.Opaque()
	o.BtpoPrev = newPrev
	bp.setOpaque(o)
	return nil
}

// ── Search ───────────────────────────────────────────────────────────────────

// Search finds the first index entry whose key equals key.
// Returns (heapBlock, heapOffset, true) on success, (_, _, false) if not found.
func (idx *BTreeIndex) Search(key []byte) (BlockNumber, OffsetNumber, bool, error) {
	meta, err := idx.readMeta()
	if err != nil {
		return 0, 0, false, err
	}
	_, leafBlk, err := idx.findLeaf(meta.Root, meta.Level, key)
	if err != nil {
		return 0, 0, false, err
	}

	id, err := idx.rel.ReadBlock(ForkMain, leafBlk)
	if err != nil {
		return 0, 0, false, err
	}
	defer idx.rel.Pool.UnpinBuffer(id)
	page, err := idx.rel.Pool.GetPage(id)
	if err != nil {
		return 0, 0, false, err
	}
	bp, err := BTreePageFromPage(page)
	if err != nil {
		return 0, 0, false, err
	}

	pos := bp.SearchLeaf(func(ek []byte) int { return idx.cmp(ek, key) })
	if pos >= bp.NumEntries() {
		return 0, 0, false, nil
	}
	gotKey, blk, off, err := bp.GetEntry(pos)
	if err != nil {
		return 0, 0, false, err
	}
	if idx.cmp(gotKey, key) != 0 {
		return 0, 0, false, nil
	}
	return blk, off, true, nil
}

// HeapTID is a heap tuple identifier: the physical location of a heap tuple.
type HeapTID struct {
	Block  BlockNumber
	Offset OffsetNumber // 1-based, PostgreSQL convention
}

// SearchAll returns all index entries whose key equals key, across all leaf
// pages (following BtpoNext sibling links until the key no longer matches).
// Duplicate keys are returned in the order they were inserted.
func (idx *BTreeIndex) SearchAll(key []byte) ([]HeapTID, error) {
	meta, err := idx.readMeta()
	if err != nil {
		return nil, err
	}
	_, leafBlk, err := idx.findLeaf(meta.Root, meta.Level, key)
	if err != nil {
		return nil, err
	}

	var results []HeapTID

	for leafBlk != InvalidBlockNumber {
		id, err := idx.rel.ReadBlock(ForkMain, leafBlk)
		if err != nil {
			return nil, err
		}
		page, err := idx.rel.Pool.GetPage(id)
		if err != nil {
			idx.rel.Pool.UnpinBuffer(id) //nolint:errcheck
			return nil, err
		}
		bp, err := BTreePageFromPage(page)
		if err != nil {
			idx.rel.Pool.UnpinBuffer(id) //nolint:errcheck
			return nil, err
		}

		opaque := bp.Opaque()
		nextBlk := opaque.BtpoNext

		// Find the first matching position on this page.
		pos := bp.SearchLeaf(func(ek []byte) int { return idx.cmp(ek, key) })

		// Collect consecutive matching entries.
		n := bp.NumEntries()
		foundAny := false
		for i := pos; i < n; i++ {
			k, blk, off, err := bp.GetEntry(i)
			if err != nil {
				idx.rel.Pool.UnpinBuffer(id) //nolint:errcheck
				return nil, err
			}
			if idx.cmp(k, key) != 0 {
				// Past all matches on this page; no need to follow sibling.
				idx.rel.Pool.UnpinBuffer(id) //nolint:errcheck
				return results, nil
			}
			results = append(results, HeapTID{Block: blk, Offset: off})
			foundAny = true
		}

		idx.rel.Pool.UnpinBuffer(id) //nolint:errcheck

		if !foundAny {
			// No matches on this page; key is not in the index.
			break
		}

		// The page was exhausted; matches may continue on the right sibling.
		leafBlk = nextBlk
	}

	return results, nil
}

// ── BTreePage sorted-insert helper ──────────────────────────────────────────

// InsertEntrySortedAt inserts an index entry at the given data-entry position
// (0-based among data entries), shifting existing entries right.
// The physical slot is pos + dataOffset() to skip the high key on non-rightmost pages.
func (b *BTreePage) InsertEntrySortedAt(pos int, key []byte, heapBlock BlockNumber, heapOffset OffsetNumber) error {
	tupleSize := IndexTupleHeaderSize + len(key)
	header := NewIndexTuple(heapBlock, heapOffset, uint16(tupleSize))

	buf := make([]byte, tupleSize)
	encodeIndexTuple(buf[:IndexTupleHeaderSize], &header)
	copy(buf[IndexTupleHeaderSize:], key)

	return b.page.InsertTupleAt(pos+b.dataOffset(), buf)
}

// ── Byte helpers ─────────────────────────────────────────────────────────────

func putU32(b []byte, v uint32) { binary.LittleEndian.PutUint32(b, v) }
func getU32(b []byte) uint32    { return binary.LittleEndian.Uint32(b) }
