package executor

// Vacuum — reclaim dead heap tuples and freeze old transaction IDs.
//
// PostgreSQL's VACUUM (src/backend/commands/vacuum.c) is a multi-phase
// operation.  This implementation covers the core heap-pass logic:
//
//  1. Compute OldestXmin via TransactionManager.GlobalXmin — the XID horizon
//     below which all committed deletions are dead to every snapshot.
//  2. Compute FreezeLimit = OldestXmin - FreezeMinAge — tuples older than
//     this have their xmin replaced with FrozenTransactionId.
//  3. Scan every heap page.  For each page:
//       • Convert old LP_DEAD slots to LP_UNUSED.
//       • Reclaim dead HOT-only tuples (LP_UNUSED).
//       • Convert dead HOT chain heads to LP_REDIRECT pointing at the live
//         chain tail, so existing index entries remain valid.
//       • Freeze eligible tuples.
//
// Omissions (intentional for research scope):
//  • Index vacuum (removing index entries for dead heap TIDs) is skipped;
//    dead tuples go directly to LP_UNUSED instead of LP_DEAD.
//  • Page compaction (moving pd_lower/pd_upper to reclaim tuple data space).
//  • Visibility map updates.
//  • FSM (free space map) updates.

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// DefaultFreezeMinAge is the minimum XID age before a tuple is frozen.
// PostgreSQL defaults to vacuum_freeze_min_age = 50,000,000; we use a small
// value suitable for tests.
const DefaultFreezeMinAge storage.TransactionId = 50

// VacuumStats accumulates statistics across the full relation vacuum.
type VacuumStats struct {
	PagesScanned  int
	PagesModified int
	TuplesRemoved int
	TuplesFrozen  int
	HOTPruned     int
}

// VacuumConfig holds tunable parameters.
type VacuumConfig struct {
	// FreezeMinAge: freeze tuples whose xmin is more than this many transactions
	// older than OldestXmin.  0 disables freezing.
	FreezeMinAge storage.TransactionId
}

// DefaultVacuumConfig returns a config with sensible defaults for tests.
func DefaultVacuumConfig() VacuumConfig {
	return VacuumConfig{FreezeMinAge: DefaultFreezeMinAge}
}

// Vacuum runs a heap pass over rel, reclaiming dead tuples and freezing old
// xmin values.  txmgr is used to compute OldestXmin and FreezeLimit.
func Vacuum(rel *storage.Relation, txmgr *mvcc.TransactionManager, cfg VacuumConfig) (VacuumStats, error) {
	oldestXmin := txmgr.GlobalXmin()

	// FreezeLimit: freeze tuples whose xmin < OldestXmin - FreezeMinAge.
	// Guard against underflow below FrozenTransactionId.
	var freezeLimit storage.TransactionId
	if cfg.FreezeMinAge > 0 && oldestXmin > cfg.FreezeMinAge+storage.FrozenTransactionId {
		freezeLimit = oldestXmin - cfg.FreezeMinAge
	}

	opts := storage.VacuumOptions{
		OldestXmin:  oldestXmin,
		FreezeLimit: freezeLimit,
	}

	nblocks, err := rel.NBlocks(storage.ForkMain)
	if err != nil {
		return VacuumStats{}, fmt.Errorf("vacuum: NBlocks: %w", err)
	}

	var stats VacuumStats

	for blk := storage.BlockNumber(0); blk < nblocks; blk++ {
		id, err := rel.ReadBlock(storage.ForkMain, blk)
		if err != nil {
			return stats, fmt.Errorf("vacuum: ReadBlock(%d): %w", blk, err)
		}

		// Use GetPageForWrite so changes are visible through the buffer pool.
		pg, err := rel.Pool.GetPageForWrite(id)
		if err != nil {
			rel.Pool.UnpinBuffer(id) //nolint:errcheck
			return stats, fmt.Errorf("vacuum: GetPageForWrite(%d): %w", blk, err)
		}

		pageStats, modified := storage.VacuumPage(pg, opts, txmgr)

		rel.Pool.UnpinBuffer(id) //nolint:errcheck

		stats.PagesScanned++
		if modified {
			stats.PagesModified++
		}
		stats.TuplesRemoved += pageStats.TuplesRemoved
		stats.TuplesFrozen += pageStats.TuplesFrozen
		stats.HOTPruned += pageStats.HOTPruned
	}

	return stats, nil
}
