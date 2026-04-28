// Package buffer_eviction benchmarks three buffer replacement policies -
// ClockSweep, LRU, and ARC - under three canonical workload shapes.
//
// The three workloads reveal different policy strengths:
//
//	Sequential  - full-table scan, cycling through all blocks once per pass.
//	             Each page is used once then not needed until the next lap.
//	             Pool size << dataset size, so every access is a compulsory miss
//	             on the first pass and the policy must decide what to keep across
//	             passes. LRU thrashes badly (scan consumes the whole pool).
//	             ClockSweep is marginally better (usage_count drains slowly).
//	             ARC adapts its ghost lists and learns the stride.
//
//	Random      - uniformly random block accesses over the full dataset.
//	             No temporal locality - every policy converges to the same
//	             hit ratio (≈ poolSize/nBlocks). A baseline for fairness.
//
//	HotSet      - Zipf-like skew: 20 % of pages account for 80 % of accesses.
//	             The hot set fits comfortably in the pool. LRU and ARC both
//	             keep it resident after warm-up. ClockSweep may occasionally
//	             evict hot pages (usage_count drains between visits).
//
// Run benchmarks:
//
//	go test -bench=. -benchtime=3s ./experiments/buffer-eviction/
//
// Run correctness assertions:
//
//	go test -v ./experiments/buffer-eviction/
package buffer_eviction

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"text/tabwriter"

	"github.com/kumarlokesh/deep-postgres/internal/instrumentation"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── workload parameters ───────────────────────────────────────────────────────

const (
	poolSize  = 32      // buffer slots
	nBlocks   = 256     // pages in the relation (8× pool)
	nAccesses = 100_000 // total accesses per run
	writePct  = 20      // % of accesses that mark the page dirty
)

// seed is fixed so runs are reproducible.
var seed = rand.NewPCG(0xdeadbeef, 0xcafebabe)

func newRng() *rand.Rand { return rand.New(rand.NewPCG(0xdeadbeef, 0xcafebabe)) }

// ── policy constructors ───────────────────────────────────────────────────────

func clockSweep() (string, storage.EvictionPolicy) {
	return "ClockSweep", &storage.ClockSweepPolicy{}
}
func lru() (string, storage.EvictionPolicy) {
	return "LRU", &storage.LRUPolicy{}
}
func arc() (string, storage.EvictionPolicy) {
	return "ARC", &storage.ARCPolicy{}
}

// policies returns all three policies for table-driven tests/benchmarks.
func policies() []func() (string, storage.EvictionPolicy) {
	return []func() (string, storage.EvictionPolicy){clockSweep, lru, arc}
}

// ── correctness assertions ────────────────────────────────────────────────────

// TestHotSetARCBetterThanLRU verifies that on a skewed workload ARC achieves
// a higher hit ratio than LRU.  ARC's ghost lists let it learn frequency; LRU
// cannot distinguish hot from cold pages after the initial warm-up is disrupted.
//
// We set the pool small enough that the hot set (20 % of nBlocks = ~51 pages)
// does NOT fit entirely, so the two policies diverge.
func TestHotSetARCBetterThanLRU(t *testing.T) {
	const smallPool = 24 // hot set ≈ 51 pages, pool holds 24 - they compete
	pattern := func() instrumentation.AccessPattern {
		return &instrumentation.HotSetPattern{
			N: nBlocks, HotFrac: 0.20, HotProb: 0.80,
			Rng: newRng(),
		}
	}

	runOne := func(name string, policy storage.EvictionPolicy) float64 {
		r, err := instrumentation.BenchmarkPolicy(
			pattern(), name, policy, smallPool, nBlocks, nAccesses, writePct,
		)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return r.HitRatio
	}

	lruHit := runOne("LRU", &storage.LRUPolicy{})
	arcHit := runOne("ARC", &storage.ARCPolicy{})

	t.Logf("LRU hit%%=%.1f%%  ARC hit%%=%.1f%%", lruHit*100, arcHit*100)
	if arcHit <= lruHit {
		t.Errorf("expected ARC (%.1f%%) > LRU (%.1f%%) on hot-set workload",
			arcHit*100, lruHit*100)
	}
}

// TestSequentialAllPoliciesSimilar verifies that no single policy dominates
// on a sequential scan (all are similarly bad - scan thrashing is fundamental).
// The assertion is loose: no policy may be more than 5x better than another.
func TestSequentialAllPoliciesSimilar(t *testing.T) {
	pattern := func() instrumentation.AccessPattern {
		return &instrumentation.SequentialPattern{N: nBlocks}
	}

	var ratios []float64
	for _, pf := range policies() {
		name, policy := pf()
		r, err := instrumentation.BenchmarkPolicy(
			pattern(), name, policy, poolSize, nBlocks, nAccesses, 0,
		)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		ratios = append(ratios, r.HitRatio)
		t.Logf("%-12s hit%%=%.1f%%", name, r.HitRatio*100)
	}

	best, worst := ratios[0], ratios[0]
	for _, r := range ratios[1:] {
		if r > best {
			best = r
		}
		if r < worst {
			worst = r
		}
	}
	if worst > 0 && best/worst > 5.0 {
		t.Errorf("policies diverge too much on sequential workload (best/worst = %.1f×)", best/worst)
	}
}

// TestRandomAllPoliciesConverge verifies that on a uniform random workload all
// three policies converge to approximately the same hit ratio (≈ pool/nBlocks).
func TestRandomAllPoliciesConverge(t *testing.T) {
	pattern := func() instrumentation.AccessPattern {
		return &instrumentation.RandomPattern{N: nBlocks, Rng: newRng()}
	}

	var ratios []float64
	for _, pf := range policies() {
		name, policy := pf()
		r, err := instrumentation.BenchmarkPolicy(
			pattern(), name, policy, poolSize, nBlocks, nAccesses, 0,
		)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		ratios = append(ratios, r.HitRatio)
		t.Logf("%-12s hit%%=%.1f%%", name, r.HitRatio*100)
	}

	// All ratios should be within 5 pp of the mean.
	var sum float64
	for _, r := range ratios {
		sum += r
	}
	mean := sum / float64(len(ratios))
	for i, r := range ratios {
		if abs(r-mean) > 0.05 {
			t.Errorf("policy %d hit ratio %.1f%% diverges from mean %.1f%% by >5pp",
				i, r*100, mean*100)
		}
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// ── summary table printed to stderr ──────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "buffer-eviction: comparing ClockSweep / LRU / ARC")
	fmt.Fprintln(os.Stderr, fmt.Sprintf("  pool=%d  blocks=%d  accesses=%d  write%%=%d%%",
		poolSize, nBlocks, nAccesses, writePct))
	fmt.Fprintln(os.Stderr, "")

	workloads := []struct {
		name    string
		pattern func() instrumentation.AccessPattern
	}{
		{"Sequential", func() instrumentation.AccessPattern {
			return &instrumentation.SequentialPattern{N: nBlocks}
		}},
		{"Random", func() instrumentation.AccessPattern {
			return &instrumentation.RandomPattern{N: nBlocks, Rng: newRng()}
		}},
		{"HotSet(20/80)", func() instrumentation.AccessPattern {
			return &instrumentation.HotSetPattern{
				N: nBlocks, HotFrac: 0.20, HotProb: 0.80,
				Rng: newRng(),
			}
		}},
	}

	tw := tabwriter.NewWriter(os.Stderr, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "Workload\tPolicy\tHits\tMisses\tEvictions\tHit%")
	fmt.Fprintln(tw, "────────\t──────\t────\t──────\t─────────\t────")

	for _, w := range workloads {
		for _, pf := range policies() {
			name, policy := pf()
			r, err := instrumentation.BenchmarkPolicy(
				w.pattern(), name, policy, poolSize, nBlocks, nAccesses, writePct,
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR %s/%s: %v\n", w.name, name, err)
				continue
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%.1f%%\n",
				w.name, r.PolicyName, r.Hits, r.Misses, r.Evictions, r.HitRatio*100)
		}
	}
	tw.Flush()
	fmt.Fprintln(os.Stderr, "")

	os.Exit(m.Run())
}

// ── Go benchmarks ─────────────────────────────────────────────────────────────

func BenchmarkSequential(b *testing.B) {
	benchWorkload(b, func() instrumentation.AccessPattern {
		return &instrumentation.SequentialPattern{N: nBlocks}
	})
}

func BenchmarkRandom(b *testing.B) {
	benchWorkload(b, func() instrumentation.AccessPattern {
		return &instrumentation.RandomPattern{N: nBlocks, Rng: newRng()}
	})
}

func BenchmarkHotSet(b *testing.B) {
	benchWorkload(b, func() instrumentation.AccessPattern {
		return &instrumentation.HotSetPattern{
			N: nBlocks, HotFrac: 0.20, HotProb: 0.80,
			Rng: newRng(),
		}
	})
}

func benchWorkload(b *testing.B, mkPattern func() instrumentation.AccessPattern) {
	b.Helper()
	for _, pf := range policies() {
		name, policy := pf()
		b.Run(name, func(b *testing.B) {
			for range b.N {
				_, err := instrumentation.BenchmarkPolicy(
					mkPattern(), name, policy,
					poolSize, nBlocks, nAccesses, writePct,
				)
				if err != nil {
					b.Fatalf("%s: %v", name, err)
				}
			}
		})
	}
}
