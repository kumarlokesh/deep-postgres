// Package pin_contention benchmarks three implementations of the buffer
// descriptor pin count under heavy goroutine contention.
//
// The three implementations mirror the evolution from a naive struct with
// separate fields to PostgreSQL's single-word atomic layout:
//
//	MutexSeparate - per-descriptor sync.Mutex protecting individual fields.
//	MutexPacked   - per-descriptor sync.Mutex protecting a single packed uint32.
//	AtomicCAS     - lock-free packed uint32 using sync/atomic CAS (current code).
//
// Run with:
//
//	go test -bench=. -benchtime=5s -cpu=1,4,8 ./experiments/pin-contention/
//
// Expected observation:
//   - At GOMAXPROCS=1  all three are similar (no true contention).
//   - At GOMAXPROCS≥4  AtomicCAS pulls ahead; MutexSeparate/MutexPacked
//     serialise on the lock and throughput flattens or falls.
package pin_contention

import (
	"sync"
	"sync/atomic"
	"testing"
)

// ── Implementation 1: mutex-protected separate fields ─────────────────────────

type descMutexSeparate struct {
	mu         sync.Mutex
	refcount   uint32
	usageCount uint8
	flags      uint32
}

func (d *descMutexSeparate) pin() {
	d.mu.Lock()
	d.refcount++
	d.mu.Unlock()
}

func (d *descMutexSeparate) unpin() {
	d.mu.Lock()
	if d.refcount == 0 {
		d.mu.Unlock()
		panic("unpin at zero")
	}
	d.refcount--
	d.mu.Unlock()
}

func (d *descMutexSeparate) pinCount() uint32 {
	d.mu.Lock()
	rc := d.refcount
	d.mu.Unlock()
	return rc
}

// ── Implementation 2: mutex-protected packed uint32 ──────────────────────────

// Packing layout (same as the current codebase):
//
//	bits  0-17: refcount
//	bits 18-22: usage_count
//	bits 23+:   flags
const (
	rcMask  = uint32(0x0003_FFFF)
	rcOne   = uint32(1)
	flagBit = uint32(1) << 23
)

type descMutexPacked struct {
	mu     sync.Mutex
	packed uint32
}

func (d *descMutexPacked) pin() {
	d.mu.Lock()
	d.packed += rcOne
	d.mu.Unlock()
}

func (d *descMutexPacked) unpin() {
	d.mu.Lock()
	if d.packed&rcMask == 0 {
		d.mu.Unlock()
		panic("unpin at zero")
	}
	d.packed--
	d.mu.Unlock()
}

func (d *descMutexPacked) pinCount() uint32 {
	d.mu.Lock()
	rc := d.packed & rcMask
	d.mu.Unlock()
	return rc
}

// ── Implementation 3: lock-free atomic CAS on packed uint32 ──────────────────

type descAtomicCAS struct {
	state atomic.Uint32
}

func (d *descAtomicCAS) pin() { d.state.Add(rcOne) }

func (d *descAtomicCAS) unpin() {
	for {
		s := d.state.Load()
		if s&rcMask == 0 {
			panic("unpin at zero")
		}
		if d.state.CompareAndSwap(s, s-rcOne) {
			return
		}
	}
}

func (d *descAtomicCAS) pinCount() uint32 { return d.state.Load() & rcMask }

// ── Benchmark harness ─────────────────────────────────────────────────────────

const goroutines = 1000
const itersPerGoroutine = 1000

func runConcurrent(b *testing.B, pin, unpin func()) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				for range itersPerGoroutine {
					pin()
					unpin()
				}
			}()
		}
		wg.Wait()
	}
}

func BenchmarkMutexSeparate(b *testing.B) {
	d := &descMutexSeparate{}
	runConcurrent(b, d.pin, d.unpin)
}

func BenchmarkMutexPacked(b *testing.B) {
	d := &descMutexPacked{}
	runConcurrent(b, d.pin, d.unpin)
}

func BenchmarkAtomicCAS(b *testing.B) {
	d := &descAtomicCAS{}
	runConcurrent(b, d.pin, d.unpin)
}

// ── Correctness smoke test ─────────────────────────────────────────────────────

// TestPinCountIsZeroAfterRoundTrip verifies that all three implementations
// return to pin count 0 after goroutines finish (no leaked pins, no lost
// decrements).
func TestPinCountIsZeroAfterRoundTrip(t *testing.T) {
	t.Run("MutexSeparate", func(t *testing.T) {
		d := &descMutexSeparate{}
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				for range itersPerGoroutine {
					d.pin()
					d.unpin()
				}
			}()
		}
		wg.Wait()
		if got := d.pinCount(); got != 0 {
			t.Errorf("pin count = %d, want 0", got)
		}
	})

	t.Run("MutexPacked", func(t *testing.T) {
		d := &descMutexPacked{}
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				for range itersPerGoroutine {
					d.pin()
					d.unpin()
				}
			}()
		}
		wg.Wait()
		if got := d.pinCount(); got != 0 {
			t.Errorf("pin count = %d, want 0", got)
		}
	})

	t.Run("AtomicCAS", func(t *testing.T) {
		d := &descAtomicCAS{}
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				for range itersPerGoroutine {
					d.pin()
					d.unpin()
				}
			}()
		}
		wg.Wait()
		if got := d.pinCount(); got != 0 {
			t.Errorf("pin count = %d, want 0", got)
		}
	})
}
