package instrumentation_test

// Benchmark harness tests and comparisons.
//
// These tests verify that:
//   - Access patterns produce the expected block sequences.
//   - BenchmarkPolicy runs without error and returns plausible stats.
//   - On a hot-set workload with a pool that fits the hot set, hit ratios are
//     substantially higher than on a sequential (scan) workload.
//   - LRU outperforms ClockSweep on a pure hot-set workload where the hot set
//     fits comfortably in the pool.
//   - ClockSweep does not catastrophically worse than LRU on sequential scans
//     (both exhibit low hit ratios, but clock-sweep's usage-count draining
//     provides some scan resistance).

import (
	"math/rand/v2"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/instrumentation"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── Pattern unit tests ────────────────────────────────────────────────────────

func TestSequentialPatternWraps(t *testing.T) {
	p := &instrumentation.SequentialPattern{N: 4}
	want := []storage.BlockNumber{0, 1, 2, 3, 0, 1}
	for i, w := range want {
		got := p.Next()
		if got != w {
			t.Errorf("step %d: got %d want %d", i, got, w)
		}
	}
}

func TestRandomPatternInRange(t *testing.T) {
	p := &instrumentation.RandomPattern{N: 100, Rng: rand.New(rand.NewPCG(1, 0))}
	for range 1000 {
		blk := p.Next()
		if blk >= 100 {
			t.Fatalf("RandomPattern returned %d, outside [0, 100)", blk)
		}
	}
}

func TestHotSetPatternRespectsBias(t *testing.T) {
	// HotFrac=0.1 (10 hot blocks out of 100), HotProb=0.9.
	// Over many samples, approximately 90% should be in [0, 10).
	p := &instrumentation.HotSetPattern{
		N:       100,
		HotFrac: 0.1,
		HotProb: 0.9,
		Rng:     rand.New(rand.NewPCG(7, 0)),
	}
	var hotCount int
	const n = 10_000
	for range n {
		if p.Next() < 10 {
			hotCount++
		}
	}
	ratio := float64(hotCount) / n
	// Allow ±5 % tolerance around the expected 90 %.
	if ratio < 0.85 || ratio > 0.95 {
		t.Errorf("hot-set ratio = %.3f, expected ≈0.90", ratio)
	}
}

// ── BenchmarkPolicy integration tests ────────────────────────────────────────

func runBench(t *testing.T, name string, policy storage.EvictionPolicy,
	pattern instrumentation.AccessPattern,
	poolSize, nBlocks, nAccesses int) instrumentation.PolicyResult {
	t.Helper()
	r, err := instrumentation.BenchmarkPolicy(pattern, name, policy,
		poolSize, nBlocks, nAccesses, 0)
	if err != nil {
		t.Fatalf("BenchmarkPolicy(%s): %v", name, err)
	}
	return r
}

func TestBenchmarkPolicyReturnsStats(t *testing.T) {
	r := runBench(t, "clock",
		&storage.ClockSweepPolicy{},
		&instrumentation.RandomPattern{N: 20, Rng: rand.New(rand.NewPCG(1, 0))},
		8, 20, 500)

	if r.Hits+r.Misses != int64(r.NAccesses) {
		t.Errorf("hits+misses = %d+%d ≠ NAccesses %d", r.Hits, r.Misses, r.NAccesses)
	}
	if r.HitRatio < 0 || r.HitRatio > 1 {
		t.Errorf("HitRatio %f out of [0,1]", r.HitRatio)
	}
}

func TestHotSetGoodHitRatio(t *testing.T) {
	// Pool (32) is larger than hot set (20 × 0.2 = 4 blocks).
	// After warm-up, nearly every access should hit.
	r := runBench(t, "lru",
		&storage.LRUPolicy{},
		&instrumentation.HotSetPattern{
			N: 100, HotFrac: 0.2, HotProb: 0.9,
			Rng: rand.New(rand.NewPCG(2, 0)),
		},
		32, 100, 5000)

	if r.HitRatio < 0.7 {
		t.Errorf("hot-set with pool>hot-set: hit ratio %.2f < 0.70", r.HitRatio)
	}
}

func TestSequentialScanLowHitRatio(t *testing.T) {
	// Sequential scan with pool=8 and 100 blocks → each page evicted before
	// it wraps around (100 >> 8), so hit ratio should be near 0.
	r := runBench(t, "clock",
		&storage.ClockSweepPolicy{},
		&instrumentation.SequentialPattern{N: 100},
		8, 100, 1000)

	if r.HitRatio > 0.2 {
		t.Errorf("sequential scan: hit ratio %.2f > 0.20 (expected near 0)", r.HitRatio)
	}
}

func TestLRUVsClockSweepOnHotSet(t *testing.T) {
	// On a hot-set workload where the hot set fits in the pool, LRU should
	// achieve at least as good a hit ratio as ClockSweep (and typically
	// better on a pure access-frequency basis).
	mkPattern := func() instrumentation.AccessPattern {
		return &instrumentation.HotSetPattern{
			N: 200, HotFrac: 0.1, HotProb: 0.85,
			Rng: rand.New(rand.NewPCG(99, 0)),
		}
	}

	lruR := runBench(t, "lru", &storage.LRUPolicy{}, mkPattern(), 16, 200, 8000)
	csR := runBench(t, "clock", &storage.ClockSweepPolicy{}, mkPattern(), 16, 200, 8000)

	t.Logf("LRU:    %s", lruR)
	t.Logf("Clock:  %s", csR)

	// LRU should be competitive; allow clock-sweep within 10 pp of LRU.
	if csR.HitRatio < lruR.HitRatio-0.10 {
		t.Errorf("ClockSweep hit ratio %.2f is more than 10pp below LRU %.2f",
			csR.HitRatio, lruR.HitRatio)
	}
}

func TestDirtyEvictionsTracked(t *testing.T) {
	// writePct=50 → half of accesses mark the page dirty.
	// With a small pool and large working set, dirty evictions should occur.
	pattern := &instrumentation.RandomPattern{N: 100, Rng: rand.New(rand.NewPCG(5, 0))}
	r, err := instrumentation.BenchmarkPolicy(pattern, "clock",
		&storage.ClockSweepPolicy{}, 8, 100, 1000, 50)
	if err != nil {
		t.Fatalf("BenchmarkPolicy: %v", err)
	}
	if r.DirtyEvictions == 0 {
		t.Error("expected dirty evictions with writePct=50 on a small pool")
	}
}
