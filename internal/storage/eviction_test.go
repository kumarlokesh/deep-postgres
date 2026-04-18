package storage

// Eviction policy tests.
//
// Scenarios for ClockSweep:
//   - Victim skips pinned buffers.
//   - Access increments UsageCount by 1 up to maxUsageCount; Victim drains it.
//   - Victim rotates through the ring.
//
// Scenarios for LRU:
//   - Victim returns the least recently accessed unpinned buffer.
//   - Access moves a buffer to the MRU position.
//   - Victim skips pinned buffers.

import "testing"

// makeDescs builds a descriptor slice of length n, all unpinned, UsageCount 0.
func makeDescs(n int) []BufferDesc {
	return make([]BufferDesc, n)
}

// ── ClockSweep ────────────────────────────────────────────────────────────────

func TestClockSweepVictimBasic(t *testing.T) {
	cs := &ClockSweepPolicy{}
	cs.Init(4)
	descs := makeDescs(4)

	id, ok := cs.Victim(descs)
	if !ok {
		t.Fatal("expected a victim")
	}
	if id >= 4 {
		t.Errorf("victim id %d out of range", id)
	}
}

func TestClockSweepSkipsPinned(t *testing.T) {
	cs := &ClockSweepPolicy{}
	cs.Init(3)
	descs := makeDescs(3)

	// Pin 0 and 1; only 2 should be returned.
	descs[0].Pin()
	descs[1].Pin()

	id, ok := cs.Victim(descs)
	if !ok {
		t.Fatal("expected victim 2")
	}
	if id != 2 {
		t.Errorf("expected victim 2, got %d", id)
	}
}

func TestClockSweepDrainsUsageCount(t *testing.T) {
	cs := &ClockSweepPolicy{}
	cs.Init(2)
	descs := makeDescs(2)

	// Set UsageCount on both to 1.  First Victim call should drain them;
	// second call should return 0.
	descs[0].UsageCount = 1
	descs[1].UsageCount = 1

	// Multiple calls drain usage counts; eventually returns a victim.
	var found bool
	for range 10 {
		if _, ok := cs.Victim(descs); ok {
			found = true
			break
		}
	}
	if !found {
		t.Error("clock sweep never found a victim after draining usage counts")
	}
}

func TestClockSweepAllPinned(t *testing.T) {
	cs := &ClockSweepPolicy{}
	cs.Init(3)
	descs := makeDescs(3)
	descs[0].Pin()
	descs[1].Pin()
	descs[2].Pin()

	_, ok := cs.Victim(descs)
	if ok {
		t.Error("expected no victim when all buffers are pinned")
	}
}

func TestClockSweepAccessIncrementsUsageCount(t *testing.T) {
	cs := &ClockSweepPolicy{}
	cs.Init(2)
	descs := makeDescs(2)

	// Each access increments by 1.
	for want := uint8(1); want <= maxUsageCount; want++ {
		cs.Access(0, descs)
		if descs[0].UsageCount != want {
			t.Errorf("after %d Access calls: UsageCount=%d want %d",
				want, descs[0].UsageCount, want)
		}
	}

	// Further accesses must not exceed the cap.
	cs.Access(0, descs)
	if descs[0].UsageCount != maxUsageCount {
		t.Errorf("UsageCount exceeded cap: got %d want %d",
			descs[0].UsageCount, maxUsageCount)
	}
}

// ── LRU ───────────────────────────────────────────────────────────────────────

func TestLRUVictimIsLeastRecent(t *testing.T) {
	lru := &LRUPolicy{}
	lru.Init(4)
	descs := makeDescs(4)

	// Access 0, 1, 2, 3 in order → 3 is MRU, 0 is LRU.
	lru.Access(0, descs)
	lru.Access(1, descs)
	lru.Access(2, descs)
	lru.Access(3, descs)

	id, ok := lru.Victim(descs)
	if !ok {
		t.Fatal("expected a victim")
	}
	if id != 0 {
		t.Errorf("expected LRU victim 0 (accessed first), got %d", id)
	}
}

func TestLRUAccessPromotesToMRU(t *testing.T) {
	lru := &LRUPolicy{}
	lru.Init(3)
	descs := makeDescs(3)

	// Initial order after sequential access: 0→1→2 means 0 is LRU.
	lru.Access(0, descs)
	lru.Access(1, descs)
	lru.Access(2, descs)

	// Re-access 0 → it becomes MRU; 1 is now LRU.
	lru.Access(0, descs)

	id, ok := lru.Victim(descs)
	if !ok {
		t.Fatal("expected a victim")
	}
	if id != 1 {
		t.Errorf("expected LRU victim 1 after re-accessing 0, got %d", id)
	}
}

func TestLRUSkipsPinned(t *testing.T) {
	lru := &LRUPolicy{}
	lru.Init(3)
	descs := makeDescs(3)

	lru.Access(0, descs)
	lru.Access(1, descs)
	lru.Access(2, descs)

	// Pin 0 (LRU); victim should be 1.
	descs[0].Pin()

	id, ok := lru.Victim(descs)
	if !ok {
		t.Fatal("expected a victim")
	}
	if id != 1 {
		t.Errorf("expected victim 1 (0 is pinned), got %d", id)
	}
}

func TestLRUAllPinned(t *testing.T) {
	lru := &LRUPolicy{}
	lru.Init(2)
	descs := makeDescs(2)
	descs[0].Pin()
	descs[1].Pin()

	_, ok := lru.Victim(descs)
	if ok {
		t.Error("expected no victim when all buffers are pinned")
	}
}
