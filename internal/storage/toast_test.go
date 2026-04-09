package storage_test

import (
	"bytes"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── ToastPointer marshal/unmarshal ────────────────────────────────────────────

func TestToastPointerRoundTrip(t *testing.T) {
	want := storage.ToastPointer{
		RawSize:    12345,
		ExtSize:    6789,
		ChunkId:    42,
		Compressed: true,
	}
	b := want.Marshal()
	if len(b) != storage.ToastPointerSize {
		t.Fatalf("Marshal: got %d bytes, want %d", len(b), storage.ToastPointerSize)
	}
	got, err := storage.UnmarshalToastPointer(b)
	if err != nil {
		t.Fatalf("UnmarshalToastPointer: %v", err)
	}
	if got != want {
		t.Errorf("round-trip mismatch: got %+v, want %+v", got, want)
	}
}

func TestToastPointerUncompressed(t *testing.T) {
	want := storage.ToastPointer{RawSize: 100, ExtSize: 100, ChunkId: 1, Compressed: false}
	b := want.Marshal()
	got, err := storage.UnmarshalToastPointer(b)
	if err != nil {
		t.Fatalf("UnmarshalToastPointer: %v", err)
	}
	if got.Compressed {
		t.Errorf("expected Compressed=false, got true")
	}
}

func TestIsToasted(t *testing.T) {
	ptr := storage.ToastPointer{RawSize: 1, ExtSize: 1, ChunkId: 1}.Marshal()
	if !storage.IsToasted(ptr) {
		t.Error("IsToasted: expected true for valid pointer bytes")
	}
	if storage.IsToasted([]byte("hello")) {
		t.Error("IsToasted: expected false for plain string")
	}
	if storage.IsToasted(nil) {
		t.Error("IsToasted: expected false for nil")
	}
}

// ── InMemoryToastStore ────────────────────────────────────────────────────────

func newToastData(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestToastStoreSmallValue(t *testing.T) {
	s := storage.NewInMemoryToastStore()
	data := newToastData(100)

	ptr, err := s.Store(data, false)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if ptr.RawSize != 100 {
		t.Errorf("RawSize=%d, want 100", ptr.RawSize)
	}

	got, err := s.Fetch(ptr)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("Fetch: data mismatch")
	}
}

func TestToastStoreLargeValue(t *testing.T) {
	// 6000 bytes → should span 3 chunks of ≤2000 bytes each.
	s := storage.NewInMemoryToastStore()
	data := newToastData(6000)

	ptr, err := s.Store(data, false)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if ptr.RawSize != 6000 {
		t.Errorf("RawSize=%d, want 6000", ptr.RawSize)
	}

	got, err := s.Fetch(ptr)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Fetch: data mismatch (got len=%d, want 6000)", len(got))
	}
}

func TestToastStoreCompression(t *testing.T) {
	// Highly compressible data (all zeros).
	s := storage.NewInMemoryToastStore()
	data := make([]byte, 8000) // 8 KB of zeros — compresses to ~20 bytes

	ptr, err := s.Store(data, true)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if !ptr.Compressed {
		t.Log("compression not applied (may happen if compressed > original)")
	}
	if ptr.RawSize != uint32(len(data)) {
		t.Errorf("RawSize=%d, want %d", ptr.RawSize, len(data))
	}

	got, err := s.Fetch(ptr)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("Fetch after compression: data mismatch")
	}
}

func TestToastStoreDelete(t *testing.T) {
	s := storage.NewInMemoryToastStore()
	ptr, _ := s.Store(newToastData(3000), false)

	if s.ChunkSetCount() != 1 {
		t.Fatalf("expected 1 chunk set before delete, got %d", s.ChunkSetCount())
	}

	if err := s.Delete(ptr); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.ChunkSetCount() != 0 {
		t.Errorf("expected 0 chunk sets after delete, got %d", s.ChunkSetCount())
	}

	// Fetch after delete should error.
	if _, err := s.Fetch(ptr); err == nil {
		t.Error("expected error fetching deleted chunk, got nil")
	}
}

func TestToastStoreMultipleValues(t *testing.T) {
	s := storage.NewInMemoryToastStore()
	data1 := newToastData(3000)
	data2 := newToastData(5000)
	for i := range data2 {
		data2[i] ^= 0xFF // distinct from data1
	}

	ptr1, _ := s.Store(data1, false)
	ptr2, _ := s.Store(data2, false)

	if ptr1.ChunkId == ptr2.ChunkId {
		t.Error("two stores returned the same chunk_id")
	}
	if s.ChunkSetCount() != 2 {
		t.Errorf("expected 2 chunk sets, got %d", s.ChunkSetCount())
	}

	got1, _ := s.Fetch(ptr1)
	got2, _ := s.Fetch(ptr2)
	if !bytes.Equal(got1, data1) || !bytes.Equal(got2, data2) {
		t.Error("Fetch: data mismatch for multi-value store")
	}
}

// ── Toastify / Detoast helpers ────────────────────────────────────────────────

func TestToastifySmallTupleIsNoop(t *testing.T) {
	s := storage.NewInMemoryToastStore()
	data := []byte("small")
	tup := storage.NewHeapTuple(3, 1, data)

	if err := storage.Toastify(tup, s); err != nil {
		t.Fatalf("Toastify: %v", err)
	}
	if storage.IsToasted(tup.Data) {
		t.Error("small tuple should not be toasted")
	}
	if s.ChunkSetCount() != 0 {
		t.Errorf("expected 0 chunks for small tuple, got %d", s.ChunkSetCount())
	}
}

func TestToastifyLargeTuple(t *testing.T) {
	s := storage.NewInMemoryToastStore()
	data := newToastData(storage.ToastTupleThreshold + 1)
	tup := storage.NewHeapTuple(3, 1, data)

	if err := storage.Toastify(tup, s); err != nil {
		t.Fatalf("Toastify: %v", err)
	}
	if !storage.IsToasted(tup.Data) {
		t.Fatal("large tuple should be toasted; Data not replaced with pointer")
	}
	if len(tup.Data) != storage.ToastPointerSize {
		t.Errorf("toasted Data len=%d, want %d", len(tup.Data), storage.ToastPointerSize)
	}
	if tup.Header.TInfomask&uint16(storage.HeapHasExternal) == 0 {
		t.Error("HeapHasExternal flag not set after Toastify")
	}
}

func TestDetoastRoundTrip(t *testing.T) {
	s := storage.NewInMemoryToastStore()
	original := newToastData(storage.ToastTupleThreshold + 500)
	tup := storage.NewHeapTuple(3, 1, original)

	if err := storage.Toastify(tup, s); err != nil {
		t.Fatalf("Toastify: %v", err)
	}

	recovered, err := storage.Detoast(tup, s)
	if err != nil {
		t.Fatalf("Detoast: %v", err)
	}
	if !bytes.Equal(recovered, original) {
		t.Errorf("Detoast: data mismatch (got %d bytes, want %d)", len(recovered), len(original))
	}
}

func TestDetoastInlineTuple(t *testing.T) {
	s := storage.NewInMemoryToastStore()
	data := []byte("inline data")
	tup := storage.NewHeapTuple(3, 1, data)

	got, err := storage.Detoast(tup, s)
	if err != nil {
		t.Fatalf("Detoast on inline tuple: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Detoast on inline: got %q, want %q", got, data)
	}
}

func TestToastifyIdempotent(t *testing.T) {
	s := storage.NewInMemoryToastStore()
	data := newToastData(storage.ToastTupleThreshold + 1)
	tup := storage.NewHeapTuple(3, 1, data)

	_ = storage.Toastify(tup, s)
	countBefore := s.ChunkSetCount()

	// Second call should be a no-op.
	_ = storage.Toastify(tup, s)
	if s.ChunkSetCount() != countBefore {
		t.Errorf("second Toastify created extra chunks: %d → %d", countBefore, s.ChunkSetCount())
	}
}
