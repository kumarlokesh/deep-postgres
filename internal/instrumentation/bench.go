package instrumentation

// Buffer replacement benchmark harness.
//
// Provides:
//   - AccessPattern interface: generates a sequence of block numbers.
//   - SequentialPattern: blocks 0, 1, 2, …, n-1, 0, 1, … (scan simulation).
//   - RandomPattern: uniformly random blocks in [0, n).
//   - HotSetPattern: hot fraction accessed with high probability; rest cold.
//   - BenchmarkPolicy: runs nAccesses against an in-memory relation, records
//     hit ratio and eviction counts via BufferStats.
//
// Usage:
//
//	result := BenchmarkPolicy(
//	    &HotSetPattern{N: 1000, HotFrac: 0.2, HotProb: 0.8, Rng: rand.New(...)},
//	    &storage.LRUPolicy{},
//	    64,    // pool size
//	    10000, // accesses
//	)
//	fmt.Println(result)

import (
	"fmt"
	"math/rand/v2"
	"os"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── Access patterns ───────────────────────────────────────────────────────────

// AccessPattern generates the next block number to access in a workload.
type AccessPattern interface {
	Next() storage.BlockNumber
}

// SequentialPattern scans blocks 0 … N-1 in order, then wraps around.
// This simulates a sequential table scan — the worst case for LRU because
// every page is evicted before it can be reused.
type SequentialPattern struct {
	N   int // number of distinct blocks
	cur int
}

func (s *SequentialPattern) Next() storage.BlockNumber {
	blk := s.cur
	s.cur = (s.cur + 1) % s.N
	return storage.BlockNumber(blk)
}

// RandomPattern accesses a uniformly random block in [0, N).
type RandomPattern struct {
	N   int
	Rng *rand.Rand
}

func (r *RandomPattern) Next() storage.BlockNumber {
	return storage.BlockNumber(r.Rng.IntN(r.N))
}

// HotSetPattern models a skewed workload: a "hot" fraction of blocks is
// accessed with probability HotProb; the remaining "cold" blocks share the
// remaining probability mass.
//
// With N=1000, HotFrac=0.2, HotProb=0.8:
//   - 200 hot blocks  → accessed 80 % of the time
//   - 800 cold blocks → accessed 20 % of the time
//
// This is a simplified Zipf-like distribution and represents the common
// database pattern where a small working set fits in the buffer pool.
type HotSetPattern struct {
	N       int     // total blocks
	HotFrac float64 // fraction of N that is "hot"
	HotProb float64 // probability of accessing the hot set
	Rng     *rand.Rand
}

func (h *HotSetPattern) Next() storage.BlockNumber {
	hotN := max(1, int(float64(h.N)*h.HotFrac))
	coldN := h.N - hotN
	if h.Rng.Float64() < h.HotProb {
		return storage.BlockNumber(h.Rng.IntN(hotN))
	}
	if coldN == 0 {
		return storage.BlockNumber(h.Rng.IntN(hotN))
	}
	return storage.BlockNumber(hotN + h.Rng.IntN(coldN))
}

// ── Result ────────────────────────────────────────────────────────────────────

// PolicyResult records the outcome of one benchmark run.
type PolicyResult struct {
	PolicyName     string
	PoolSize       int
	NBlocks        int
	NAccesses      int
	Hits           int64
	Misses         int64
	Evictions      int64
	DirtyEvictions int64
	HitRatio       float64
}

func (r PolicyResult) String() string {
	return fmt.Sprintf("%-12s pool=%-4d blocks=%-6d accesses=%-7d  hits=%d misses=%d evictions=%d dirty=%d  hit%%=%.1f%%",
		r.PolicyName, r.PoolSize, r.NBlocks, r.NAccesses,
		r.Hits, r.Misses, r.Evictions, r.DirtyEvictions,
		r.HitRatio*100)
}

// ── Benchmark runner ──────────────────────────────────────────────────────────

// BenchmarkPolicy runs nAccesses against a buffer pool of size poolSize backed
// by an in-memory relation with nBlocks pages.  It returns a PolicyResult with
// the counters collected by a BufferStats tracer.
//
// The pool uses the supplied policy.  Access patterns that write to a page
// (writePct % of accesses) mark it dirty, exercising dirty eviction paths.
//
// nBlocks must be >= 1.  writePct is a percentage in [0, 100].
func BenchmarkPolicy(
	pattern AccessPattern,
	policyName string,
	policy storage.EvictionPolicy,
	poolSize int,
	nBlocks int,
	nAccesses int,
	writePct int,
) (PolicyResult, error) {
	// Set up a temporary file-backed relation.
	dir, err := os.MkdirTemp("", "benchpolicy-*")
	if err != nil {
		return PolicyResult{}, fmt.Errorf("bench: MkdirTemp: %w", err)
	}
	defer os.RemoveAll(dir)

	smgr := storage.NewFileStorageManager(dir)
	defer smgr.Close()

	rfn := storage.RelFileNode{DbId: 1, RelId: 1}
	if err := smgr.Create(rfn, storage.ForkMain); err != nil {
		return PolicyResult{}, fmt.Errorf("bench: Create: %w", err)
	}
	for i := 0; i < nBlocks; i++ {
		if _, err := smgr.Extend(rfn, storage.ForkMain); err != nil {
			return PolicyResult{}, fmt.Errorf("bench: Extend(%d): %w", i, err)
		}
	}

	pool := storage.NewBufferPoolWithPolicy(poolSize, policy)
	pool.RegisterRelation(1, rfn, smgr)

	stats := &BufferStats{}
	pool.SetTracer(stats)

	rng := rand.New(rand.NewPCG(42, 0))

	for i := 0; i < nAccesses; i++ {
		blk := pattern.Next()
		tag := storage.MainTag(1, blk)
		id, err := pool.ReadBuffer(tag)
		if err != nil {
			return PolicyResult{}, fmt.Errorf("bench: ReadBuffer(%d): %w", blk, err)
		}
		if writePct > 0 && rng.IntN(100) < writePct {
			pool.GetPageForWrite(id) //nolint:errcheck
		}
		pool.UnpinBuffer(id) //nolint:errcheck
	}

	snap := stats.Snapshot()
	return PolicyResult{
		PolicyName:     policyName,
		PoolSize:       poolSize,
		NBlocks:        nBlocks,
		NAccesses:      nAccesses,
		Hits:           snap.Hits,
		Misses:         snap.Misses,
		Evictions:      snap.Evictions,
		DirtyEvictions: snap.DirtyEvictions,
		HitRatio:       snap.HitRatio,
	}, nil
}
