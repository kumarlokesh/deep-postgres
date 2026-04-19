# pin-contention

Benchmarks three implementations of a buffer descriptor pin count under heavy
goroutine contention to validate the atomic-CAS design used in
`internal/storage/buffer.go`.

## Implementations

| Name | Lock | State |
|---|---|---|
| `MutexSeparate` | `sync.Mutex` per descriptor | `refcount uint32`, `usageCount uint8`, `flags uint32` as separate fields |
| `MutexPacked` | `sync.Mutex` per descriptor | single `packed uint32` (bits 0-17 = refcount, etc.) |
| `AtomicCAS` | lock-free | single `atomic.Uint32`, CAS loops for multi-field updates |

`AtomicCAS` mirrors PostgreSQL's `BufferDesc.state` (buf_internals.h).

## Running

```bash
# Single core - all three should be similar
go test -bench=. -benchtime=5s -cpu=1 ./experiments/pin-contention/

# Multi-core - AtomicCAS should pull ahead
go test -bench=. -benchtime=5s -cpu=1,4,8 ./experiments/pin-contention/

# With race detector (correctness check)
go test -race ./experiments/pin-contention/
```

## Results

Measured on Intel Core i9-9880H @ 2.30 GHz, Go 1.24.3, macOS.
Each iteration is 1 000 goroutines × 1 000 pin/unpin cycles (1 M total ops).
Lower ns/op is better.

```text
goos: darwin / goarch: amd64
BenchmarkMutexSeparate      146    25 229 759 ns/op
BenchmarkMutexSeparate-4     16   194 409 430 ns/op
BenchmarkMutexSeparate-8     20   164 755 274 ns/op
BenchmarkMutexPacked        133    26 435 975 ns/op
BenchmarkMutexPacked-4       16   203 459 643 ns/op
BenchmarkMutexPacked-8       20   169 491 301 ns/op
BenchmarkAtomicCAS          264    14 174 554 ns/op
BenchmarkAtomicCAS-4         76    45 388 600 ns/op
BenchmarkAtomicCAS-8         55    63 036 219 ns/op
```

Derived ratios (ns/op relative to `AtomicCAS` at the same core count):

| Implementation | GOMAXPROCS=1 | GOMAXPROCS=4 | GOMAXPROCS=8 |
| --- | --- | --- | --- |
| `MutexSeparate` | 1.8× slower | **4.3× slower** | **2.6× slower** |
| `MutexPacked` | 1.9× slower | **4.5× slower** | **2.7× slower** |
| `AtomicCAS` | 1× (baseline) | 1× (baseline) | 1× (baseline) |

### What the numbers say

**Packing buys nothing under a mutex.** `MutexSeparate` and `MutexPacked` are
statistically identical at every core count. Memory layout only matters once
the lock is removed - the bottleneck is lock acquisition, not cache-line size.

**AtomicCAS is already faster at GOMAXPROCS=1** (1.8×). Go's `sync.Mutex`
has non-trivial overhead even without contention: it writes to the lock word
and emits memory barriers. The CAS path avoids both.

**Mutex implementations blow up under contention.** Going from 1→4 cores,
both mutex variants degrade 7-8×. With 1 000 goroutines all hammering one
lock, most goroutines spend their time in the kernel scheduler queue, not
doing work. `AtomicCAS` degrades only 3.2× over the same range.

**The 8-core numbers are slightly better than 4-core for mutexes** because
the Go scheduler can overlap OS-thread blocking across more physical cores,
slightly masking the queue depth - this is not a win, it is the scheduler
partially absorbing lock traffic that should not exist.

This is the same bottleneck described in *30 Years of Buffer Manager Evolution*
(Li 2022): pre-8.1 PostgreSQL used a single `BufMgrLock`; every `ReadBuffer`
call acquired it. Splitting into 128 partitions + per-buffer atomic state
eliminated most of that contention.

## Expected results is not deterministic, but the general trends should hold

At `GOMAXPROCS=1` the three are roughly equivalent - no real contention.

At `GOMAXPROCS≥4` the mutex implementations serialise: every goroutine
queues behind the lock, so throughput flattens as core count increases.
`AtomicCAS` scales because there is no global serialisation point - the CAS
retry loop succeeds on the first try under moderate contention and degrades
gracefully under heavy contention (versus a hard queue on a mutex).

The gap widens with more goroutines and tighter loops. This is exactly the
bottleneck that drove PostgreSQL to adopt the packed atomic state word in its
buffer manager.
