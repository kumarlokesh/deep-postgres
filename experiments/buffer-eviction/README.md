# buffer-eviction

Benchmarks three buffer replacement policies - ClockSweep, LRU, and ARC - under
three canonical workload shapes to show why PostgreSQL chose clock-sweep and how
ARC's adaptive ghost lists improve on both.

## Policies

| Policy | Mechanism | PostgreSQL analogue |
| --- | --- | --- |
| `ClockSweep` | Clock hand, usage_count 0–5; evict first frame with count=0 | `StrategyGetBuffer` (bufmgr.c) |
| `LRU` | Doubly-linked recency list; evict tail (least recently used) | Conceptual baseline |
| `ARC` | Two resident lists (T1 recency, T2 frequency) + two ghost lists (B1, B2); adaptive split `p` | ARC paper (Megiddo & Modha 2003) |

Pool size: 32 frames. Dataset: 256 blocks (8× pool). Accesses per run: 100 000.
Write %: 20 % (dirty-eviction path exercised).

## Workloads

**Sequential** - full-table scan cycling through all 256 blocks. The pool is
8× smaller than the dataset, so every page is evicted before its next visit.
All three policies thrash equally; scan resistance is a fundamental limit, not a
policy difference.

**Random** - uniform random block accesses. No temporal or frequency locality.
Every policy converges to the theoretical optimum of `poolSize / nBlocks` =
32/256 = 12.5 % hit ratio. A fairness baseline.

**HotSet (20/80)** - Zipf-like skew: 20 % of pages account for 80 % of accesses.
The hot set (≈ 51 pages) is larger than the pool, so policies compete. LRU keeps
the hot set resident after warm-up but the cold 80 % occasionally displaces hot
pages. ARC's ghost lists detect frequency and shift more pool space toward hot
pages, widening the gap.

## Running

```bash
# Correctness tests
go test -v ./experiments/buffer-eviction/

# Benchmarks (single-core; policy comparison is workload-driven, not parallel)
go test -bench=. -benchtime=5s ./experiments/buffer-eviction/
```

## Results

### Hit ratios (pool=32, blocks=256, accesses=100 000)

| Workload | ClockSweep | LRU | ARC | Winner |
| --- | --- | --- | --- | --- |
| Sequential | 0.0 % | 0.0 % | 0.0 % | - (all thrash) |
| Random | 12.5 % | 12.5 % | 12.5 % | tied (≈ pool/N) |
| HotSet (20/80) | 38.6 % | 38.4 % | **46.2 %** | ARC +7.6 pp |

Correctness test (`smallPool=24`, hot set ≈ 51 pages - hot set exceeds pool more
aggressively): LRU 29.5 % vs ARC 35.7 % (+6.2 pp).

### Throughput (ns/op = time for 100 000 accesses, GOMAXPROCS=1)

Measured on Intel Core i9-9880H @ 2.30 GHz, Go 1.24.3, macOS.

```text
BenchmarkSequential/ClockSweep   25   230 499 723 ns/op
BenchmarkSequential/LRU          22   235 269 518 ns/op
BenchmarkSequential/ARC          18   318 196 891 ns/op

BenchmarkRandom/ClockSweep       26   222 603 865 ns/op
BenchmarkRandom/LRU              26   220 295 973 ns/op
BenchmarkRandom/ARC              21   282 829 374 ns/op

BenchmarkHotSet/ClockSweep       36   176 602 077 ns/op
BenchmarkHotSet/LRU              31   178 566 488 ns/op
BenchmarkHotSet/ARC              30   200 086 515 ns/op
```

ARC overhead vs ClockSweep (same workload):

| Workload | ARC overhead |
| --- | --- |
| Sequential | +38 % |
| Random | +27 % |
| HotSet | +13 % |

ARC's ghost-list bookkeeping costs CPU. When the workload has no frequency
signal (Sequential, Random), that overhead is pure waste. When frequency signal
exists (HotSet), the overhead buys a meaningful hit-ratio gain.

## What the numbers say

**Sequential scan is policy-agnostic.** All three policies hit 0 % because the
pool (32 frames) is 8× smaller than the dataset (256 blocks). No policy can keep
a page resident long enough for its next visit. This is scan thrashing - the
fundamental reason PostgreSQL has a ring buffer strategy for large sequential
scans rather than relying on clock-sweep.

**Random workload is a fairness baseline.** With no locality, every policy
converges to the theoretical optimum of 12.5 % (32/256). Any divergence here
would indicate a policy bug, not a policy strength.

**ARC wins on skewed workloads, at a CPU cost.** On HotSet (20/80), ARC's
hit ratio is 46.2 % vs 38.6 % for ClockSweep and 38.4 % for LRU - a 7.6
percentage-point gain. The mechanism: ARC's B1/B2 ghost lists record recently
evicted pages. A ghost hit in B1 (recency ghost) signals the page should be in
T1; a ghost hit in B2 (frequency ghost) signals T2. The adaptive split `p`
shifts capacity toward whichever list is being hit more. LRU has no such
memory of evicted pages - once a page leaves the pool it is forgotten.

**ClockSweep and LRU are equivalent on all tested workloads** (within 0.2 pp
on hit ratio, within 2 % on throughput). ClockSweep's advantage over LRU
shows up in pathological cases not tested here: workloads where a single large
scan temporarily displaces the entire working set, and the slow usage_count
drain gives hot pages a chance to survive. LRU evicts them immediately.

**PostgreSQL chose ClockSweep** (`StrategyGetBuffer`) because it is O(1) with
no per-page list manipulation, its usage_count acts as a lightweight frequency
approximation, and it handles sequential-scan-within-pool gracefully. Full ARC
requires tracking every evicted page tag, which PostgreSQL delegates to the
operating system's page cache for double-buffering avoidance instead.
