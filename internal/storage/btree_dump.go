package storage

// B-tree structural snapshot - read-only helpers for visualization and
// invariant checking.
//
// SnapshotTree reads every reachable page in the index and returns them as a
// slice of levels, root-first (index 0 = root level, last = leaf level).
// PageInfo / EntryInfo are plain value types: safe to inspect, log, or pass to
// an external checker without holding any buffer-pool pin.

// PageInfo is a complete, pin-free snapshot of one B-tree page.
type PageInfo struct {
	Block   BlockNumber
	Level   uint32 // 0 = leaf
	Flags   uint16 // BTreePageFlags bitmask
	Prev    BlockNumber
	Next    BlockNumber
	HighKey []byte      // nil on rightmost pages (no high key)
	Entries []EntryInfo // data entries only (high key excluded)
}

// EntryInfo holds the decoded content of one index entry.
// For internal pages HeapBlk is the child block number and HeapOff is 0.
// For leaf pages HeapBlk/HeapOff point to the actual heap tuple.
type EntryInfo struct {
	Key     []byte
	HeapBlk BlockNumber
	HeapOff OffsetNumber
}

// SnapshotTree reads every reachable page in the B-tree and returns them
// grouped by depth level, root-first.
//
//	levels[0]             = root level (one page if not yet split)
//	levels[len(levels)-1] = leaf level (all leaf pages in left-to-right order)
func (idx *BTreeIndex) SnapshotTree() ([][]PageInfo, error) {
	meta, err := idx.readMeta()
	if err != nil {
		return nil, err
	}

	nLevels := int(meta.Level) + 1
	levels := make([][]PageInfo, nLevels)

	// Find the leftmost block at every tree depth by descending through the
	// first child of each internal page.
	leftmost := make([]BlockNumber, nLevels)
	leftmost[0] = meta.Root

	blk := meta.Root
	for depth := 0; depth < nLevels-1; depth++ {
		id, err := idx.rel.ReadBlock(ForkMain, blk)
		if err != nil {
			return nil, err
		}
		page, err := idx.rel.Pool.GetPage(id)
		if err != nil {
			idx.rel.Pool.UnpinBuffer(id)
			return nil, err
		}
		bp, err := BTreePageFromPage(page)
		if err != nil {
			idx.rel.Pool.UnpinBuffer(id)
			return nil, err
		}
		if bp.NumEntries() == 0 {
			idx.rel.Pool.UnpinBuffer(id)
			break
		}
		_, childBlk, _, getErr := bp.GetEntry(0)
		idx.rel.Pool.UnpinBuffer(id)
		if getErr != nil {
			return nil, getErr
		}
		leftmost[depth+1] = childBlk
		blk = childBlk
	}

	// Walk each level's sibling chain starting from the leftmost block.
	for depth := 0; depth < nLevels; depth++ {
		blk := leftmost[depth]
		for blk != InvalidBlockNumber {
			info, next, err := idx.snapshotPage(blk)
			if err != nil {
				return nil, err
			}
			levels[depth] = append(levels[depth], info)
			blk = next
		}
	}

	return levels, nil
}

// snapshotPage reads one page into a PageInfo and returns the next sibling.
func (idx *BTreeIndex) snapshotPage(blk BlockNumber) (PageInfo, BlockNumber, error) {
	id, err := idx.rel.ReadBlock(ForkMain, blk)
	if err != nil {
		return PageInfo{}, InvalidBlockNumber, err
	}
	defer idx.rel.Pool.UnpinBuffer(id)

	page, err := idx.rel.Pool.GetPage(id)
	if err != nil {
		return PageInfo{}, InvalidBlockNumber, err
	}
	bp, err := BTreePageFromPage(page)
	if err != nil {
		return PageInfo{}, InvalidBlockNumber, err
	}

	opaque := bp.Opaque()
	hk, _ := bp.HighKey()

	info := PageInfo{
		Block: blk,
		Level: opaque.BtpoLevel,
		Flags: opaque.BtpoFlags,
		Prev:  opaque.BtpoPrev,
		Next:  opaque.BtpoNext,
	}
	if hk != nil {
		info.HighKey = append([]byte(nil), hk...)
	}

	n := bp.NumEntries()
	info.Entries = make([]EntryInfo, n)
	for i := 0; i < n; i++ {
		key, heapBlk, heapOff, err := bp.GetEntry(i)
		if err != nil {
			return PageInfo{}, InvalidBlockNumber, err
		}
		info.Entries[i] = EntryInfo{
			Key:     append([]byte(nil), key...),
			HeapBlk: heapBlk,
			HeapOff: heapOff,
		}
	}

	return info, opaque.BtpoNext, nil
}
