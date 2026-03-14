package storage

import (
	"bytes"
	"testing"
)

func TestHeapTupleHeaderSize(t *testing.T) {
	// Encode to a 24-byte buffer and decode; verify field values survive.
	h := NewHeapTupleHeader(100, 5)
	buf := make([]byte, HeapTupleHeaderSize)
	encodeHeapTupleHeader(buf, &h)
	got := decodeHeapTupleHeader(buf)
	if got.TXmin != 100 {
		t.Errorf("TXmin = %d, want 100", got.TXmin)
	}
	if got.Natts() != 5 {
		t.Errorf("Natts = %d, want 5", got.Natts())
	}
}

func TestNewHeaderDefaults(t *testing.T) {
	h := NewHeapTupleHeader(42, 3)
	if h.TXmin != 42 {
		t.Errorf("TXmin = %d", h.TXmin)
	}
	if h.TXmax != 0 {
		t.Errorf("TXmax should be 0, got %d", h.TXmax)
	}
	if !h.XmaxInvalid() {
		t.Error("new header should have HEAP_XMAX_INVALID set")
	}
	if h.XminCommitted() {
		t.Error("new header should not have HEAP_XMIN_COMMITTED")
	}
}

func TestNattsEncoding(t *testing.T) {
	h := NewHeapTupleHeader(0, 0)
	h.SetNatts(1000)
	if h.Natts() != 1000 {
		t.Errorf("Natts = %d, want 1000", h.Natts())
	}
	// Setting flags in high bits should not corrupt natts.
	h.TInfomask2 |= uint16(HeapHotUpdated)
	if h.Natts() != 1000 {
		t.Error("natts corrupted by setting infomask2 flag")
	}
}

func TestInfomaskHintBits(t *testing.T) {
	h := NewHeapTupleHeader(100, 2)

	h.SetXminCommitted()
	if !h.XminCommitted() {
		t.Error("expected XminCommitted after set")
	}
	if h.XminInvalid() {
		t.Error("XminInvalid should be clear after SetXminCommitted")
	}

	h.SetXminInvalid()
	if !h.XminInvalid() {
		t.Error("expected XminInvalid after set")
	}
	if h.XminCommitted() {
		t.Error("XminCommitted should be cleared by SetXminInvalid")
	}

	h.SetXmaxCommitted()
	if !h.XmaxCommitted() {
		t.Error("expected XmaxCommitted")
	}
	if h.XmaxInvalid() {
		t.Error("XmaxInvalid should be cleared")
	}
}

func TestCtid(t *testing.T) {
	h := NewHeapTupleHeader(0, 0)
	h.SetCtid(99, 7)
	blk, off := h.Ctid()
	if blk != 99 || off != 7 {
		t.Errorf("Ctid = (%d, %d), want (99, 7)", blk, off)
	}
}

func TestHeapTupleSerialization(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	tup := NewHeapTuple(100, 3, data)
	if tup.Len() != HeapTupleHeaderSize+5 {
		t.Errorf("Len = %d, want %d", tup.Len(), HeapTupleHeaderSize+5)
	}

	raw := tup.ToBytes()
	if len(raw) != tup.Len() {
		t.Fatalf("ToBytes length mismatch")
	}

	restored, err := HeapTupleFromBytes(raw)
	if err != nil {
		t.Fatalf("HeapTupleFromBytes: %v", err)
	}
	if restored.Header.TXmin != 100 {
		t.Errorf("TXmin = %d, want 100", restored.Header.TXmin)
	}
	if restored.Header.Natts() != 3 {
		t.Errorf("Natts = %d, want 3", restored.Header.Natts())
	}
	if !bytes.Equal(restored.Data, data) {
		t.Errorf("data mismatch: got %v, want %v", restored.Data, data)
	}
}

func TestHeapTupleFromBytesTooShort(t *testing.T) {
	_, err := HeapTupleFromBytes([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for too-short byte slice")
	}
}
