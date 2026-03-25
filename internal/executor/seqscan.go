package executor

// SeqScan — sequential heap scan with MVCC visibility filtering.
//
// Mirrors PostgreSQL's SeqScan executor node (src/backend/executor/nodeSeqscan.c)
// and the underlying heap scan in src/backend/access/heap/heapam.c (heapgettup).
//
// Algorithm
//
//  1. Iterate blocks 0 … nblocks-1 of the relation's main fork.
//  2. For each block load the page from the buffer pool (one block pinned at a
//     time; unpinned before moving to the next).
//  3. If rel.VM marks the block all-visible, skip HeapTupleSatisfiesMVCC and
//     return all LP_NORMAL tuples directly (they are guaranteed visible to
//     every snapshot).
//  4. Walk the ItemId array (line pointers):
//       LP_UNUSED / LP_DEAD  → skip
//       LP_REDIRECT          → skip (the chain head is a normal LP on the same
//                              page; we will visit it in the same pass)
//       LP_NORMAL            → decode tuple, apply MVCC visibility.
//  5. Return each visible tuple together with its physical location (block,
//     1-based offset number) so callers can form heap TIDs.
//
// HOT chain note: LP_REDIRECT slots are left by page-level HOT chain
// compaction.  During a seqscan we visit every LP_NORMAL entry on the page,
// which already includes all live chain heads.  We therefore do NOT need to
// follow redirect slots — skipping them gives the same result as following
// them (since the target LP is also LP_NORMAL and will be visited directly).

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ScanTuple is the result of one SeqScan.Next call.
// It carries the decoded tuple and its physical location within the heap so
// that callers can form a heap TID for later updates or index entries.
type ScanTuple struct {
	Tuple  *storage.HeapTuple
	Block  storage.BlockNumber
	Offset storage.OffsetNumber // 1-based, matching PostgreSQL's convention
}

// SeqScan performs a forward sequential scan of a heap relation, returning
// only tuples that are visible according to the MVCC snapshot.
type SeqScan struct {
	rel    *storage.Relation
	snap   *storage.Snapshot
	txmgr  *mvcc.TransactionManager
	oracle storage.TransactionOracle

	// scan state
	nblocks   storage.BlockNumber
	curBlock  storage.BlockNumber
	curPage   *storage.Page
	curBufID  storage.BufferId
	curOffset int // next ItemId index to examine (0-based)
}

// NewSeqScan opens a sequential scan on rel using snap for visibility.
// txmgr is used as the TransactionOracle; pass nil to use a no-op oracle
// that treats every transaction as committed (useful in recovery contexts).
func NewSeqScan(rel *storage.Relation, snap *storage.Snapshot, txmgr *mvcc.TransactionManager) (*SeqScan, error) {
	nblocks, err := rel.NBlocks(storage.ForkMain)
	if err != nil {
		return nil, fmt.Errorf("seqscan: NBlocks: %w", err)
	}
	var oracle storage.TransactionOracle = &allCommittedOracle{}
	if txmgr != nil {
		oracle = txmgr
	}
	return &SeqScan{
		rel:      rel,
		snap:     snap,
		txmgr:    txmgr,
		oracle:   oracle,
		nblocks:  nblocks,
		curBlock: 0,
		curBufID: storage.InvalidBufferId,
	}, nil
}

// Next returns the next MVCC-visible tuple.
// It returns nil, nil when the scan is exhausted.
func (s *SeqScan) Next() (*ScanTuple, error) {
	for {
		// If we have no current page, advance to the next block.
		if s.curPage == nil {
			if s.curBlock >= s.nblocks {
				return nil, nil // scan complete
			}
			if err := s.loadBlock(s.curBlock); err != nil {
				return nil, err
			}
		}

		// If the VM marks this block all-visible we can skip per-tuple MVCC
		// checks: every LP_NORMAL tuple is guaranteed visible to all snapshots.
		allVisible := s.rel.VM.IsAllVisible(s.curBlock)

		// Scan line pointers on the current page.
		n := s.curPage.ItemCount()
		for s.curOffset < n {
			idx := s.curOffset
			s.curOffset++

			lp, err := s.curPage.GetItemId(idx)
			if err != nil {
				return nil, fmt.Errorf("seqscan: GetItemId(%d): %w", idx, err)
			}

			switch lp.Flags() {
			case storage.LpUnused, storage.LpDead, storage.LpRedirect:
				// Skip dead/redirect slots; see note at top of file.
				continue
			case storage.LpNormal:
				// fall through to visibility check below
			default:
				continue
			}

			tup, err := s.decodeTuple(lp)
			if err != nil {
				return nil, err
			}

			if !allVisible {
				result := storage.HeapTupleSatisfiesMVCC(&tup.Header, s.snap, s.oracle)
				if !result.IsVisible() {
					continue
				}
				// Cache hint bits back into the page so future scans skip
				// clog lookups.  This write is safe even with read-only
				// callers: hint bits are idempotent and never change data
				// semantics.
				storage.SetHintBits(&tup.Header, s.oracle)
			}

			block := s.curBlock
			offset := storage.OffsetNumber(idx + 1) // 1-based
			return &ScanTuple{Tuple: tup, Block: block, Offset: offset}, nil
		}

		// Exhausted this page; release the buffer and move to the next block.
		s.releaseBlock()
		s.curBlock++
	}
}

// Close releases any pinned buffer held by the scan.
func (s *SeqScan) Close() {
	s.releaseBlock()
}

// ── helpers ───────────────────────────────────────────────────────────────────

func (s *SeqScan) loadBlock(blk storage.BlockNumber) error {
	id, err := s.rel.ReadBlock(storage.ForkMain, blk)
	if err != nil {
		return fmt.Errorf("seqscan: ReadBlock(%d): %w", blk, err)
	}
	pg, err := s.rel.Pool.GetPage(id)
	if err != nil {
		s.rel.Pool.UnpinBuffer(id) //nolint:errcheck
		return fmt.Errorf("seqscan: GetPage(%d): %w", blk, err)
	}
	s.curBufID = id
	s.curPage = pg
	s.curOffset = 0
	return nil
}

func (s *SeqScan) releaseBlock() {
	if s.curPage != nil {
		s.rel.Pool.UnpinBuffer(s.curBufID) //nolint:errcheck
		s.curPage = nil
		s.curBufID = storage.InvalidBufferId
	}
}

// decodeTuple reads the tuple bytes from the page at the given line pointer.
func (s *SeqScan) decodeTuple(lp storage.ItemIdData) (*storage.HeapTuple, error) {
	off := int(lp.Off())
	length := int(lp.Len())
	raw := s.curPage.Bytes()

	if off+length > len(raw) {
		return nil, fmt.Errorf("seqscan: line pointer out of bounds (off=%d len=%d pagesize=%d)",
			off, length, len(raw))
	}

	tup, err := storage.HeapTupleFromBytes(raw[off : off+length])
	if err != nil {
		return nil, fmt.Errorf("seqscan: HeapTupleFromBytes: %w", err)
	}
	return tup, nil
}

// allCommittedOracle treats every transaction as committed.
// Used when no transaction manager is available (e.g. crash recovery reads).
type allCommittedOracle struct{}

func (allCommittedOracle) Status(xid storage.TransactionId) storage.TransactionStatus {
	if xid == storage.InvalidTransactionId {
		return storage.TxAborted
	}
	return storage.TxCommitted
}
