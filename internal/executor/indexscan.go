package executor

// IndexScan — B-tree index scan with MVCC heap fetch.
//
// Mirrors PostgreSQL's IndexScan executor node (nodeIndexscan.c) and the
// underlying heap_fetch call in heapam.c:
//
//  1. Descend the B-tree to the leaf page that could contain key.
//  2. Collect all matching index TIDs (following right-sibling links for
//     duplicate keys that span multiple leaf pages).
//  3. For each TID call HeapFetch, which pins the heap block, follows any
//     LP_REDIRECT to the HOT chain head, and checks MVCC visibility.
//  4. Return visible tuples to the caller one at a time.

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// IndexScan performs an equality index lookup on a B-tree, then fetches the
// corresponding MVCC-visible heap tuples.
type IndexScan struct {
	idx    *storage.BTreeIndex
	rel    *storage.Relation
	snap   *storage.Snapshot
	oracle storage.TransactionOracle

	// tids is the full list of heap TIDs returned by SearchAll.
	tids []storage.HeapTID
	pos  int // next TID to fetch
}

// NewIndexScan prepares an equality scan on idx for the given key.
// It calls SearchAll immediately so that any index I/O errors surface here.
// txmgr is used as the TransactionOracle; pass nil for a recovery context
// that treats every transaction as committed.
func NewIndexScan(
	idx *storage.BTreeIndex,
	rel *storage.Relation,
	snap *storage.Snapshot,
	txmgr *mvcc.TransactionManager,
	key []byte,
) (*IndexScan, error) {
	tids, err := idx.SearchAll(key)
	if err != nil {
		return nil, fmt.Errorf("indexscan: SearchAll: %w", err)
	}

	var oracle storage.TransactionOracle = &allCommittedOracle{}
	if txmgr != nil {
		oracle = txmgr
	}

	return &IndexScan{
		idx:    idx,
		rel:    rel,
		snap:   snap,
		oracle: oracle,
		tids:   tids,
	}, nil
}

// Next returns the next MVCC-visible heap tuple whose key matches the index
// lookup.  Returns nil, nil when all matching TIDs have been exhausted.
func (s *IndexScan) Next() (*ScanTuple, error) {
	for s.pos < len(s.tids) {
		tid := s.tids[s.pos]
		s.pos++

		st, err := HeapFetch(s.rel, s.snap, s.oracle, tid.Block, tid.Offset)
		if err != nil {
			return nil, err
		}
		if st != nil {
			return st, nil
		}
		// Tuple not visible (aborted inserter, deleted, invisible snapshot) — skip.
	}
	return nil, nil
}

// Close is a no-op for IndexScan: all buffer pins are released inside
// HeapFetch at the end of each call.  Kept for symmetry with SeqScan.
func (s *IndexScan) Close() {}
