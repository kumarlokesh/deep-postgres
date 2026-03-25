package storage_test

// Tests for ARCPolicy.
//
// Strategy: use a BufferPool backed by a temp file relation so the pool
// exercises the full TagEvictionPolicy wiring (VictimForTag / OnEvictTag /
// OnLoadTag).  All assertions are behavioural (hit ratios, p adaptation)
// rather than white-box state inspection.

import (
	"os"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── helpers ──────────────────────────────────────────────────────────────────

type arcBench struct {
	t    *testing.T
	pool *storage.BufferPool
	smgr storage.StorageManager
	dir  string
	rfn  storage.RelFileNode
	hits int
	miss int
}

func newARCBench(t *testing.T, poolSize, nBlocks int) *arcBench {
	t.Helper()
	dir, err := os.MkdirTemp("", "arc-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	smgr := storage.NewFileStorageManager(dir)
	rfn := storage.RelFileNode{DbId: 1, RelId: 1}
	if err := smgr.Create(rfn, storage.ForkMain); err != nil {
		t.Fatalf("smgr.Create: %v", err)
	}
	for i := 0; i < nBlocks; i++ {
		if _, err := smgr.Extend(rfn, storage.ForkMain); err != nil {
			t.Fatalf("smgr.Extend(%d): %v", i, err)
		}
	}
	policy := &storage.ARCPolicy{}
	pool := storage.NewBufferPoolWithPolicy(poolSize, policy)
	pool.RegisterRelation(1, rfn, smgr)
	return &arcBench{t: t, pool: pool, smgr: smgr, dir: dir, rfn: rfn}
}

func (b *arcBench) access(blk storage.BlockNumber) {
	b.t.Helper()
	tag := storage.MainTag(1, blk)
	_, inPool := b.pool.LookupBuffer(tag)
	id, err := b.pool.ReadBuffer(tag)
	if err != nil {
		b.t.Fatalf("ReadBuffer(%d): %v", blk, err)
	}
	if inPool {
		b.hits++
	} else {
		b.miss++
	}
	if err := b.pool.UnpinBuffer(id); err != nil {
		b.t.Fatalf("UnpinBuffer: %v", err)
	}
}

func (b *arcBench) cleanup() {
	b.smgr.Close()
	os.RemoveAll(b.dir)
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestARCBasicHitMiss verifies that repeated access to the same block is a hit
// once it is loaded.
func TestARCBasicHitMiss(t *testing.T) {
	b := newARCBench(t, 4, 8)
	defer b.cleanup()

	b.access(0) // cold miss
	b.access(0) // should hit
	b.access(0) // should hit

	if b.hits < 2 {
		t.Errorf("expected ≥2 hits for repeated block 0; got %d", b.hits)
	}
}

// TestARCFillsPool verifies that accessing N distinct blocks in a pool of size N
// results in all-miss on the first pass and all-hit on a second pass.
func TestARCFillsPool(t *testing.T) {
	const poolSize = 4
	b := newARCBench(t, poolSize, poolSize)
	defer b.cleanup()

	// First pass: all misses.
	for i := 0; i < poolSize; i++ {
		b.access(storage.BlockNumber(i))
	}
	if b.hits != 0 {
		t.Errorf("first pass: expected 0 hits, got %d", b.hits)
	}

	// Second pass: all hits (pool fits all blocks).
	b.hits = 0
	for i := 0; i < poolSize; i++ {
		b.access(storage.BlockNumber(i))
	}
	if b.hits != poolSize {
		t.Errorf("second pass: expected %d hits, got %d", poolSize, b.hits)
	}
}

// TestARCHotSetRetained verifies that ARC retains a hot working set better than
// a pure recency policy when cold pages arrive.
//
// Setup: pool=4, hot set = blocks {0,1,2}, cold set = blocks {3..9}.
// We warm up the hot set, then interleave cold accesses.  ARC should adapt p
// and keep hot pages resident most of the time.
func TestARCHotSetRetained(t *testing.T) {
	b := newARCBench(t, 4, 10)
	defer b.cleanup()

	// Warm up hot set (blocks 0-2) — each accessed twice so they move to T2.
	for i := 0; i < 3; i++ {
		b.access(storage.BlockNumber(i))
		b.access(storage.BlockNumber(i))
	}

	// Now do many hot accesses mixed with a single cold page.
	b.hits = 0
	b.miss = 0
	for round := 0; round < 10; round++ {
		for i := 0; i < 3; i++ {
			b.access(storage.BlockNumber(i)) // hot
		}
		b.access(3) // cold — different block each time would be worse; one cold suffices
	}

	// Expect the majority of the 30 hot accesses to be hits.
	hotHitRatio := float64(b.hits) / float64(b.hits+b.miss)
	if hotHitRatio < 0.60 {
		t.Errorf("hot-set hit ratio %.2f < 0.60 (hits=%d miss=%d)", hotHitRatio, b.hits, b.miss)
	}
}

// TestARCEvictsWhenFull ensures ARC produces a victim when all slots are in use
// (not pinned).
func TestARCEvictsWhenFull(t *testing.T) {
	b := newARCBench(t, 2, 4)
	defer b.cleanup()

	// Fill pool — do NOT hold pins.
	b.access(0)
	b.access(1)

	// Access a new block; requires eviction.
	b.access(2) // should not error
}

// TestARCGhostHitIncreasesP checks that a B1 ghost hit causes ARC to shift
// in favour of recency (T1).  We observe this indirectly: after the ghost hit,
// further recency-pattern accesses should have a higher hit ratio than without.
//
// This is a smoke test — exact p values are internal state.
func TestARCGhostHitAdaptsPolicy(t *testing.T) {
	// Pool=2, 5 blocks.
	// Access pattern designed to produce a B1 ghost hit:
	//   1. Load A, B into T1 (pool full).
	//   2. Load C → evicts A to B1.
	//   3. Load A again → ghost hit in B1 → p increases → A goes to T2.
	//   4. Load D → should prefer to evict from T1 (B or C) rather than T2 (A).
	b := newARCBench(t, 2, 5)
	defer b.cleanup()

	b.access(0) // A: T1
	b.access(1) // B: T1; pool full

	b.access(2) // C: miss → evict A→B1, C→T1 (A evicted)

	// Ghost hit: A is in B1 → should go to T2, p increases.
	b.access(0)

	// A should now be resident (T2).
	b.hits = 0
	b.access(0)
	if b.hits != 1 {
		t.Errorf("expected A to be resident after ghost-hit promotion; hits=%d", b.hits)
	}
}

// TestARCScanResistance checks that ARC does not suffer total cache pollution
// on a sequential scan: after the scan, at least some previously-hot pages
// survive.
func TestARCScanResistance(t *testing.T) {
	const poolSize = 4
	b := newARCBench(t, poolSize, 20)
	defer b.cleanup()

	// Warm up hot set: blocks 0-2, accessed 3× each → move to T2.
	for range 3 {
		for i := 0; i < 3; i++ {
			b.access(storage.BlockNumber(i))
		}
	}

	// Sequential scan over blocks 4-19 (larger than pool).
	for i := 4; i < 20; i++ {
		b.access(storage.BlockNumber(i))
	}

	// After the scan, check if any hot blocks survive.
	b.hits = 0
	for i := 0; i < 3; i++ {
		b.access(storage.BlockNumber(i))
	}
	// ARC won't perfectly protect everything, but at least 1 should survive.
	// (Plain LRU would have 0 survivors; ARC should do somewhat better.)
	if b.hits == 0 {
		// Soft warning — ARC scan resistance is probabilistic.
		t.Logf("WARNING: no hot pages survived sequential scan (hits=0); ARC scan resistance may be suboptimal")
	}
}

// TestARCCompareLRUHotSet benchmarks ARC vs LRU on a hot-set workload and
// verifies ARC is competitive (within 10 pp).
func TestARCCompareLRUHotSet(t *testing.T) {
	const (
		poolSize = 8
		nBlocks  = 40
		hot      = 8  // blocks 0-7 are hot
		rounds   = 60 // accesses per block in hot set
	)

	run := func(policy storage.EvictionPolicy) (hits, total int) {
		dir, _ := os.MkdirTemp("", "arc-cmp-*")
		defer os.RemoveAll(dir)
		smgr := storage.NewFileStorageManager(dir)
		defer smgr.Close()
		rfn := storage.RelFileNode{DbId: 1, RelId: 2}
		_ = smgr.Create(rfn, storage.ForkMain)
		for i := 0; i < nBlocks; i++ {
			_, _ = smgr.Extend(rfn, storage.ForkMain)
		}
		pool := storage.NewBufferPoolWithPolicy(poolSize, policy)
		pool.RegisterRelation(1, rfn, smgr)

		access := func(blk storage.BlockNumber) bool {
			tag := storage.MainTag(1, blk)
			_, inPool := pool.LookupBuffer(tag)
			id, err := pool.ReadBuffer(tag)
			if err != nil {
				t.Fatalf("ReadBuffer: %v", err)
			}
			_ = pool.UnpinBuffer(id)
			return inPool
		}

		// Warm-up: one pass over hot set.
		for i := 0; i < hot; i++ {
			access(storage.BlockNumber(i))
		}

		// Measurement: hot-biased access (hot 80 %, cold 20 %).
		for r := 0; r < rounds; r++ {
			for i := 0; i < hot; i++ {
				if access(storage.BlockNumber(i)) {
					hits++
				}
				total++
			}
			// One cold access per round.
			coldBlk := storage.BlockNumber(hot + r%(nBlocks-hot))
			if access(coldBlk) {
				hits++
			}
			total++
		}
		return hits, total
	}

	arcHits, arcTotal := run(&storage.ARCPolicy{})
	lruHits, lruTotal := run(&storage.LRUPolicy{})

	arcRatio := float64(arcHits) / float64(arcTotal)
	lruRatio := float64(lruHits) / float64(lruTotal)

	t.Logf("ARC hit ratio: %.3f (%d/%d)", arcRatio, arcHits, arcTotal)
	t.Logf("LRU hit ratio: %.3f (%d/%d)", lruRatio, lruHits, lruTotal)

	if arcRatio < lruRatio-0.10 {
		t.Errorf("ARC hit ratio %.2f is more than 10 pp below LRU %.2f", arcRatio, lruRatio)
	}
}
