package storage

// WalPageStore bridges the WAL redo engine to the storage buffer pool.
//
// It implements wal.PageWriter so that rmgr Redo callbacks (defined in the WAL
// package) can apply page-level changes without importing the storage package
// directly — avoiding a circular dependency.
//
// Thread-safety: same as BufferPool — single-threaded.

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/wal"
)

// WalPageStore maps RelFileLocator → Oid and applies WAL changes via a BufferPool.
type WalPageStore struct {
	pool   *BufferPool
	smgr   StorageManager
	relMap map[walRelKey]Oid
	nextID Oid // next synthetic Oid to assign
}

type walRelKey struct {
	SpcOid, DbOid, RelOid uint32
}

// NewWalPageStore creates a WalPageStore backed by pool.
// smgr is used for disk I/O when a block must be loaded; may be nil for
// pure in-memory tests.
func NewWalPageStore(pool *BufferPool, smgr StorageManager) *WalPageStore {
	return &WalPageStore{
		pool:   pool,
		smgr:   smgr,
		relMap: make(map[walRelKey]Oid),
		nextID: 1,
	}
}

// relOid returns (or assigns) the internal Oid for a RelFileLocator.
func (s *WalPageStore) relOid(loc wal.RelFileLocator) Oid {
	k := walRelKey{loc.SpcOid, loc.DbOid, loc.RelOid}
	if id, ok := s.relMap[k]; ok {
		return id
	}
	id := s.nextID
	s.nextID++
	s.relMap[k] = id
	// Register with the buffer pool so evictions can be written back.
	if s.smgr != nil {
		// storage.RelFileNode has only DbId and RelId (no tablespace).
		node := RelFileNode{DbId: loc.DbOid, RelId: loc.RelOid}
		s.pool.RegisterRelation(id, node, s.smgr)
	}
	return id
}

// forkNum converts a wal.ForkNum to storage.ForkNumber.
// The constants are identical in value.
func forkNum(f wal.ForkNum) ForkNumber { return ForkNumber(f) }

// getBuffer returns a pinned buffer for the given block, loading from disk if needed.
func (s *WalPageStore) getBuffer(loc wal.RelFileLocator, fork wal.ForkNum, block uint32) (BufferId, error) {
	tag := BufferTag{
		RelationId: s.relOid(loc),
		Fork:       forkNum(fork),
		BlockNum:   block,
	}
	return s.pool.ReadBuffer(tag)
}

// ── PageWriter implementation ─────────────────────────────────────────────────

// ApplyFPW restores a full-page write image to the given block.
func (s *WalPageStore) ApplyFPW(loc wal.RelFileLocator, fork wal.ForkNum, block uint32, img *wal.BlockImage, recLSN wal.LSN) error {
	id, err := s.getBuffer(loc, fork, block)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyFPW ReadBuffer: %w", err)
	}
	defer s.pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := s.pool.GetPageForWrite(id)
	if err != nil {
		return err
	}

	// Idempotency: skip if page is already at or past this LSN.
	if pg.LSN() >= uint64(recLSN) {
		return nil
	}

	// Restore page from image into the page's underlying byte slice.
	restoreFPWBytes(pg.Bytes(), img)
	pg.SetLSN(uint64(recLSN))
	return nil
}

// ApplyInsert places tupleData at offnum (1-based) on the given block.
func (s *WalPageStore) ApplyInsert(loc wal.RelFileLocator, fork wal.ForkNum, block uint32, offnum uint16, tupleData []byte, initPage bool, recLSN wal.LSN) error {
	id, err := s.getBuffer(loc, fork, block)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyInsert ReadBuffer: %w", err)
	}
	defer s.pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := s.pool.GetPageForWrite(id)
	if err != nil {
		return err
	}

	// Idempotency.
	if pg.LSN() >= uint64(recLSN) {
		return nil
	}

	if initPage {
		pg.init()
	}

	// Insert the tuple.  During sequential replay the page's item count
	// equals offnum-1 at this point, so InsertTuple (which appends) lands
	// at the correct slot.
	got, err := pg.InsertTuple(tupleData)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyInsert InsertTuple: %w", err)
	}
	// Verify the slot matches what the WAL record says.
	if want := int(offnum) - 1; got != want {
		return fmt.Errorf("wal_apply: ApplyInsert offnum mismatch: got slot %d want %d", got, want)
	}

	pg.SetLSN(uint64(recLSN))
	return nil
}

// ApplyDelete marks the item at offnum (1-based) as dead on the given block.
func (s *WalPageStore) ApplyDelete(loc wal.RelFileLocator, fork wal.ForkNum, block uint32, offnum uint16, recLSN wal.LSN) error {
	id, err := s.getBuffer(loc, fork, block)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyDelete ReadBuffer: %w", err)
	}
	defer s.pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := s.pool.GetPageForWrite(id)
	if err != nil {
		return err
	}

	// Idempotency.
	if pg.LSN() >= uint64(recLSN) {
		return nil
	}

	if err := pg.MarkDead(int(offnum) - 1); err != nil {
		return fmt.Errorf("wal_apply: ApplyDelete MarkDead: %w", err)
	}
	pg.SetLSN(uint64(recLSN))
	return nil
}

// ApplyUpdate applies a heap UPDATE: overwrites the old tuple header (xmax,
// infomask flags, t_ctid) and inserts the new tuple on the target block.
// For HOT updates (isHot=true) both are on the same page and handled with a
// single buffer pin.  For cross-page updates two buffer operations are used.
func (s *WalPageStore) ApplyUpdate(
	loc wal.RelFileLocator, fork wal.ForkNum,
	xid uint32,
	oldBlock, newBlock uint32,
	oldOffnum, newOffnum uint16,
	newTupleData []byte,
	isHot, initNewPage bool,
	recLSN wal.LSN,
) error {
	if isHot {
		return s.applyUpdateSamePage(loc, fork, xid, oldBlock, oldOffnum, newOffnum, newTupleData, recLSN)
	}
	// Cross-page: update old tuple first, then insert new tuple.
	if err := s.applyUpdateOldPage(loc, fork, xid, oldBlock, newBlock, oldOffnum, newOffnum, recLSN); err != nil {
		return err
	}
	return s.applyUpdateNewPage(loc, fork, newBlock, newOffnum, newTupleData, initNewPage, recLSN)
}

// applyUpdateSamePage handles a HOT update where old and new tuples share a page.
func (s *WalPageStore) applyUpdateSamePage(
	loc wal.RelFileLocator, fork wal.ForkNum,
	xid uint32,
	block uint32, oldOffnum, newOffnum uint16,
	newTupleData []byte, recLSN wal.LSN,
) error {
	id, err := s.getBuffer(loc, fork, block)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyUpdate(HOT) ReadBuffer: %w", err)
	}
	defer s.pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := s.pool.GetPageForWrite(id)
	if err != nil {
		return err
	}
	if pg.LSN() >= uint64(recLSN) {
		return nil // idempotent
	}

	// Update old tuple header in-place.
	if err := s.updateOldTupleHeader(pg, xid, oldOffnum, block, newOffnum, true); err != nil {
		return fmt.Errorf("wal_apply: ApplyUpdate(HOT) old header: %w", err)
	}

	// Insert new tuple.
	got, err := pg.InsertTuple(newTupleData)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyUpdate(HOT) InsertTuple: %w", err)
	}
	if want := int(newOffnum) - 1; got != want {
		return fmt.Errorf("wal_apply: ApplyUpdate(HOT) newOffnum mismatch: got slot %d want %d", got, want)
	}
	pg.SetLSN(uint64(recLSN))
	return nil
}

// applyUpdateOldPage sets xmax and HOT flags on the old tuple during cross-page update.
func (s *WalPageStore) applyUpdateOldPage(
	loc wal.RelFileLocator, fork wal.ForkNum,
	xid uint32,
	oldBlock, newBlock uint32, oldOffnum, newOffnum uint16,
	recLSN wal.LSN,
) error {
	id, err := s.getBuffer(loc, fork, oldBlock)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyUpdate old ReadBuffer: %w", err)
	}
	defer s.pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := s.pool.GetPageForWrite(id)
	if err != nil {
		return err
	}
	if pg.LSN() >= uint64(recLSN) {
		return nil
	}
	if err := s.updateOldTupleHeader(pg, xid, oldOffnum, newBlock, newOffnum, false); err != nil {
		return fmt.Errorf("wal_apply: ApplyUpdate old header: %w", err)
	}
	pg.SetLSN(uint64(recLSN))
	return nil
}

// applyUpdateNewPage inserts the new tuple on the new block.
func (s *WalPageStore) applyUpdateNewPage(
	loc wal.RelFileLocator, fork wal.ForkNum,
	newBlock uint32, newOffnum uint16,
	newTupleData []byte, initNewPage bool,
	recLSN wal.LSN,
) error {
	id, err := s.getBuffer(loc, fork, newBlock)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyUpdate new ReadBuffer: %w", err)
	}
	defer s.pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := s.pool.GetPageForWrite(id)
	if err != nil {
		return err
	}
	if pg.LSN() >= uint64(recLSN) {
		return nil
	}
	if initNewPage {
		pg.init()
	}
	got, err := pg.InsertTuple(newTupleData)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyUpdate new InsertTuple: %w", err)
	}
	if want := int(newOffnum) - 1; got != want {
		return fmt.Errorf("wal_apply: ApplyUpdate new offnum mismatch: got slot %d want %d", got, want)
	}
	pg.SetLSN(uint64(recLSN))
	return nil
}

// updateOldTupleHeader sets xmax, clears HeapXmaxInvalid, sets HeapUpdated,
// optionally sets HeapHotUpdated, and sets t_ctid on the old tuple in-place.
func (s *WalPageStore) updateOldTupleHeader(
	pg *Page, xid uint32, oldOffnum uint16,
	newBlock uint32, newOffnum uint16, isHot bool,
) error {
	lp, err := pg.GetItemId(int(oldOffnum) - 1)
	if err != nil {
		return err
	}
	if !lp.IsNormal() {
		return fmt.Errorf("old slot %d is not LP_NORMAL (flags=%d)", oldOffnum, lp.Flags())
	}
	off := int(lp.Off())
	if off+HeapTupleHeaderSize > PageSize {
		return fmt.Errorf("old tuple header out of bounds at offset %d", off)
	}

	hdr := decodeHeapTupleHeader(pg.data[off:])
	hdr.TXmax = xid
	hdr.TInfomask = uint16(
		(InfomaskFlags(hdr.TInfomask) &^ HeapXmaxInvalid) | HeapUpdated,
	)
	if isHot {
		hdr.TInfomask2 = uint16(Infomask2Flags(hdr.TInfomask2) | HeapHotUpdated)
	}
	hdr.TCtidBlock = newBlock
	hdr.TCtidOffset = newOffnum
	encodeHeapTupleHeader(pg.data[off:], &hdr)
	return nil
}

// ApplyBtreeInsert inserts an index tuple at offnum (1-based) into a B-tree page,
// maintaining sorted order by shifting existing line pointers to the right.
func (s *WalPageStore) ApplyBtreeInsert(loc wal.RelFileLocator, fork wal.ForkNum, block uint32, offnum uint16, tupleData []byte, initPage, isLeaf bool, recLSN wal.LSN) error {
	id, err := s.getBuffer(loc, fork, block)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyBtreeInsert ReadBuffer: %w", err)
	}
	defer s.pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := s.pool.GetPageForWrite(id)
	if err != nil {
		return err
	}

	// Idempotency.
	if pg.LSN() >= uint64(recLSN) {
		return nil
	}

	if initPage {
		// Initialise as a fresh B-tree page with the correct special space.
		pageType := BTreeInternal
		if isLeaf {
			pageType = BTreeLeaf
		}
		fresh := NewBTreePage(pageType)
		copy(pg.Bytes(), fresh.Page().Bytes())
	}

	// InsertTupleAt maintains sorted order — correct for B-tree pages where
	// the item-pointer array must stay in key sequence.
	if err := pg.InsertTupleAt(int(offnum)-1, tupleData); err != nil {
		return fmt.Errorf("wal_apply: ApplyBtreeInsert InsertTupleAt: %w", err)
	}
	pg.SetLSN(uint64(recLSN))
	return nil
}

// ApplyBtreeSplit initialises the new right page created by a B-tree split,
// populates it with rightItems, and wires sibling chain pointers.
func (s *WalPageStore) ApplyBtreeSplit(
	loc wal.RelFileLocator, fork wal.ForkNum,
	rightBlock, leftBlock, oldRight uint32,
	level uint32, isLeaf bool,
	rightItems []byte, recLSN wal.LSN,
) error {
	id, err := s.getBuffer(loc, fork, rightBlock)
	if err != nil {
		return fmt.Errorf("wal_apply: ApplyBtreeSplit ReadBuffer: %w", err)
	}
	defer s.pool.UnpinBuffer(id) //nolint:errcheck

	pg, err := s.pool.GetPageForWrite(id)
	if err != nil {
		return err
	}

	// Idempotency.
	if pg.LSN() >= uint64(recLSN) {
		return nil
	}

	// Initialise the right page.
	pageType := BTreeInternal
	if isLeaf {
		pageType = BTreeLeaf
	}
	fresh := NewBTreePage(pageType)

	// Set the level explicitly (NewBTreePage uses the type's default level).
	o := fresh.Opaque()
	o.BtpoLevel = level
	o.BtpoPrev = leftBlock
	if oldRight == InvalidBlockNumber {
		o.BtpoNext = InvalidBlockNumber
	} else {
		o.BtpoNext = oldRight
	}
	fresh.setOpaque(o)

	// Copy the initialised page into the buffer.
	copy(pg.Bytes(), fresh.Page().Bytes())
	bp := &BTreePage{page: pg}

	// Append each item from rightItems.  Items are packed consecutively;
	// each begins with an IndexTupleData header whose TInfo & indexSizeMask
	// gives the total tuple byte count.
	rem := rightItems
	for len(rem) >= IndexTupleHeaderSize {
		hdr := decodeIndexTuple(rem[:IndexTupleHeaderSize])
		sz := int(hdr.TInfo & indexSizeMask)
		if sz < IndexTupleHeaderSize || sz > len(rem) {
			break
		}
		if _, err := bp.page.InsertTuple(rem[:sz]); err != nil {
			return fmt.Errorf("wal_apply: ApplyBtreeSplit InsertTuple: %w", err)
		}
		rem = rem[sz:]
	}

	pg.SetLSN(uint64(recLSN))
	return nil
}

// ── FPW restoration helper ────────────────────────────────────────────────────

// restoreFPWBytes writes a BlockImage back into the raw 8 KB page slice.
func restoreFPWBytes(page []byte, img *wal.BlockImage) {
	if img.BimgInfo&wal.BimgHasHole == 0 {
		// No hole: the image is the complete page.
		copy(page, img.Data)
		return
	}
	// Image with hole: Data = page[0:HoleOffset] ++ page[HoleOffset+HoleLength:].
	hOff := int(img.HoleOffset)
	hLen := int(img.HoleLength)
	copy(page[:hOff], img.Data[:hOff])
	for i := hOff; i < hOff+hLen; i++ {
		page[i] = 0
	}
	copy(page[hOff+hLen:], img.Data[hOff:])
}
