package instrumentation

// BufferStats implements storage.BufferTracer and accumulates buffer pool
// hit/miss/eviction counters.
//
// Wire it into a buffer pool with:
//
//	stats := &instrumentation.BufferStats{}
//	pool.SetTracer(stats)
//
// Then read counters after a workload:
//
//	fmt.Printf("hit ratio: %.2f\n", stats.HitRatio())
//
// Thread-safety: all counters use sync/atomic so the stats can be read from
// a different goroutine than the one running the buffer pool.

import (
	"sync/atomic"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// BufferStats counts buffer pool events.
type BufferStats struct {
	hits           atomic.Int64
	misses         atomic.Int64
	evictions      atomic.Int64
	dirtyEvictions atomic.Int64
}

// OnHit is called on every buffer cache hit.
func (s *BufferStats) OnHit(_ storage.BufferTag) { s.hits.Add(1) }

// OnMiss is called on every cache miss (page loaded from disk or initialised).
func (s *BufferStats) OnMiss(_ storage.BufferTag) { s.misses.Add(1) }

// OnEvict is called whenever a buffer is evicted to make room for a new page.
// dirty is true if the evicted buffer had to be flushed to disk.
func (s *BufferStats) OnEvict(_ storage.BufferTag, dirty bool) {
	s.evictions.Add(1)
	if dirty {
		s.dirtyEvictions.Add(1)
	}
}

// Hits returns the total number of cache hits.
func (s *BufferStats) Hits() int64 { return s.hits.Load() }

// Misses returns the total number of cache misses.
func (s *BufferStats) Misses() int64 { return s.misses.Load() }

// Evictions returns the total number of buffer evictions.
func (s *BufferStats) Evictions() int64 { return s.evictions.Load() }

// DirtyEvictions returns the number of evictions that required a disk write.
func (s *BufferStats) DirtyEvictions() int64 { return s.dirtyEvictions.Load() }

// HitRatio returns hits / (hits + misses).  Returns 0 if no requests have
// been made.
func (s *BufferStats) HitRatio() float64 {
	h := s.hits.Load()
	m := s.misses.Load()
	total := h + m
	if total == 0 {
		return 0
	}
	return float64(h) / float64(total)
}

// Reset zeroes all counters.
func (s *BufferStats) Reset() {
	s.hits.Store(0)
	s.misses.Store(0)
	s.evictions.Store(0)
	s.dirtyEvictions.Store(0)
}

// Snapshot returns a point-in-time copy of the counters.
type BufferSnapshot struct {
	Hits           int64
	Misses         int64
	Evictions      int64
	DirtyEvictions int64
	HitRatio       float64
}

// Snapshot captures the current counter values atomically.
func (s *BufferStats) Snapshot() BufferSnapshot {
	h := s.hits.Load()
	m := s.misses.Load()
	e := s.evictions.Load()
	d := s.dirtyEvictions.Load()
	var ratio float64
	if h+m > 0 {
		ratio = float64(h) / float64(h+m)
	}
	return BufferSnapshot{
		Hits:           h,
		Misses:         m,
		Evictions:      e,
		DirtyEvictions: d,
		HitRatio:       ratio,
	}
}
