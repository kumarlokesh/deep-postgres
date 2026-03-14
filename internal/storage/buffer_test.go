package storage

import (
	"testing"
)

func TestNewBufferPool(t *testing.T) {
	pool := NewBufferPool(16)
	if pool.NumBuffers() != 16 {
		t.Errorf("NumBuffers = %d, want 16", pool.NumBuffers())
	}
	s := pool.Stats()
	if s.Total != 16 || s.Valid != 0 || s.Dirty != 0 || s.Pinned != 0 {
		t.Errorf("unexpected initial stats: %+v", s)
	}
}

func TestReadBufferAllocatesNew(t *testing.T) {
	pool := NewBufferPool(16)
	tag := MainTag(1, 0)

	id, err := pool.ReadBuffer(tag)
	if err != nil {
		t.Fatalf("ReadBuffer: %v", err)
	}
	desc, _ := pool.GetDescriptor(id)
	if !desc.IsValid() {
		t.Error("buffer should be valid")
	}
	if !desc.IsPinned() {
		t.Error("buffer should be pinned after ReadBuffer")
	}
	if desc.IsDirty() {
		t.Error("fresh buffer should not be dirty")
	}
}

func TestReadBufferReturnsExisting(t *testing.T) {
	pool := NewBufferPool(16)
	tag := MainTag(1, 0)

	id1, _ := pool.ReadBuffer(tag)
	pool.UnpinBuffer(id1)

	id2, err := pool.ReadBuffer(tag)
	if err != nil {
		t.Fatalf("second ReadBuffer: %v", err)
	}
	if id1 != id2 {
		t.Errorf("second read should return same buffer: got %d, want %d", id2, id1)
	}
}

func TestPinUnpin(t *testing.T) {
	pool := NewBufferPool(16)
	id, _ := pool.ReadBuffer(MainTag(1, 0))

	desc, _ := pool.GetDescriptor(id)
	if desc.PinCount() != 1 {
		t.Errorf("pin count = %d, want 1", desc.PinCount())
	}

	desc.Pin()
	if desc.PinCount() != 2 {
		t.Errorf("pin count after extra pin = %d, want 2", desc.PinCount())
	}

	pool.UnpinBuffer(id)
	if desc.PinCount() != 1 {
		t.Errorf("pin count after unpin = %d, want 1", desc.PinCount())
	}

	pool.UnpinBuffer(id)
	if desc.PinCount() != 0 {
		t.Errorf("pin count after second unpin = %d, want 0", desc.PinCount())
	}
}

func TestGetPageRequiresPin(t *testing.T) {
	pool := NewBufferPool(16)
	id, _ := pool.ReadBuffer(MainTag(1, 0))
	pool.UnpinBuffer(id)

	_, err := pool.GetPage(id)
	if err == nil {
		t.Fatal("expected error when getting unpinned buffer")
	}
	se, ok := err.(*StorageError)
	if !ok || se.Code != ErrBufferNotPinned {
		t.Errorf("expected ErrBufferNotPinned, got %v", err)
	}
}

func TestGetPageForWriteMarksDirty(t *testing.T) {
	pool := NewBufferPool(16)
	id, _ := pool.ReadBuffer(MainTag(1, 0))

	desc, _ := pool.GetDescriptor(id)
	if desc.IsDirty() {
		t.Error("should not be dirty before write")
	}

	_, err := pool.GetPageForWrite(id)
	if err != nil {
		t.Fatalf("GetPageForWrite: %v", err)
	}
	if !desc.IsDirty() {
		t.Error("should be dirty after GetPageForWrite")
	}
}

func TestClockSweepEviction(t *testing.T) {
	pool := NewBufferPool(4)

	for i := 0; i < 4; i++ {
		id, _ := pool.ReadBuffer(MainTag(1, BlockNumber(i)))
		pool.UnpinBuffer(id)
	}
	if pool.Stats().Valid != 4 {
		t.Errorf("expected 4 valid buffers, got %d", pool.Stats().Valid)
	}

	// Read a 5th block; must evict one existing.
	id, err := pool.ReadBuffer(MainTag(1, 100))
	if err != nil {
		t.Fatalf("eviction ReadBuffer: %v", err)
	}
	if pool.Stats().Valid != 4 {
		t.Errorf("expected 4 valid buffers after eviction, got %d", pool.Stats().Valid)
	}
	desc, _ := pool.GetDescriptor(id)
	if !desc.IsPinned() {
		t.Error("new buffer should be pinned")
	}
}

func TestBufferPoolExhausted(t *testing.T) {
	pool := NewBufferPool(2)

	_, err1 := pool.ReadBuffer(MainTag(1, 0))
	_, err2 := pool.ReadBuffer(MainTag(1, 1))
	if err1 != nil || err2 != nil {
		t.Fatalf("initial reads failed: %v %v", err1, err2)
	}

	// Both buffers are pinned; a third read must fail.
	_, err := pool.ReadBuffer(MainTag(1, 2))
	if err == nil {
		t.Fatal("expected ErrBufferExhausted")
	}
	se, ok := err.(*StorageError)
	if !ok || se.Code != ErrBufferExhausted {
		t.Errorf("expected ErrBufferExhausted, got %v", err)
	}
}

func TestLookupBuffer(t *testing.T) {
	pool := NewBufferPool(16)
	tag := MainTag(1, 0)

	if _, ok := pool.LookupBuffer(tag); ok {
		t.Error("should not find buffer before read")
	}

	id, _ := pool.ReadBuffer(tag)
	found, ok := pool.LookupBuffer(tag)
	if !ok || found != id {
		t.Errorf("LookupBuffer = (%d, %v), want (%d, true)", found, ok, id)
	}
}

func TestFlushAllClearsDirty(t *testing.T) {
	pool := NewBufferPool(16)
	id, _ := pool.ReadBuffer(MainTag(1, 0))
	pool.MarkDirty(id)

	desc, _ := pool.GetDescriptor(id)
	if !desc.IsDirty() {
		t.Fatal("should be dirty")
	}
	pool.FlushAll()
	if desc.IsDirty() {
		t.Error("should not be dirty after FlushAll")
	}
}

func TestInsertTupleViaBuffer(t *testing.T) {
	pool := NewBufferPool(16)
	id, _ := pool.ReadBuffer(MainTag(1, 0))

	page, err := pool.GetPageForWrite(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := page.InsertTuple([]byte("buffered tuple")); err != nil {
		t.Fatal(err)
	}

	page2, _ := pool.GetPage(id)
	got, _ := page2.GetTuple(0)
	if string(got) != "buffered tuple" {
		t.Errorf("got %q", got)
	}

	desc, _ := pool.GetDescriptor(id)
	if !desc.IsDirty() {
		t.Error("buffer should be dirty after tuple insert")
	}
}
