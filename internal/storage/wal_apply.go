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
		node := RelFileNode{DbId: Oid(loc.DbOid), RelId: Oid(loc.RelOid)}
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
		BlockNum:   BlockNumber(block),
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
