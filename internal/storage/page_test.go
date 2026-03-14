package storage

import (
	"bytes"
	"testing"
)

func TestPageHeaderSize(t *testing.T) {
	// Verify our manual serialization produces exactly PageHeaderSize bytes.
	h := PageHeaderData{
		PdLsnHi:           0xDEAD,
		PdLsnLo:           0xBEEF,
		PdChecksum:        0x1234,
		PdFlags:           0x0002,
		PdLower:           24,
		PdUpper:           8192,
		PdSpecial:         8192,
		PdPagesizeVersion: MakePagesizeVersion(PageSize, PGPageLayoutVersion),
		PdPruneXid:        0,
	}
	buf := make([]byte, PageHeaderSize)
	encodeHeader(buf, &h)
	decoded := decodeHeader(buf)
	if decoded != h {
		t.Fatalf("encode/decode round-trip failed:\n  got  %+v\n  want %+v", decoded, h)
	}
}

func TestItemIdBitPacking(t *testing.T) {
	id := NewItemId(1000, 500)
	if id.Off() != 1000 {
		t.Errorf("Off() = %d, want 1000", id.Off())
	}
	if id.Len() != 500 {
		t.Errorf("Len() = %d, want 500", id.Len())
	}
	if !id.IsNormal() {
		t.Error("expected IsNormal()")
	}
	if id.IsUnused() || id.IsDead() || id.IsRedirect() {
		t.Error("unexpected flag")
	}

	dead := NewItemIdDead()
	if !dead.IsDead() {
		t.Error("dead item not flagged dead")
	}

	redir := NewItemIdRedirect(42)
	if !redir.IsRedirect() {
		t.Error("expected redirect")
	}
	if redir.Off() != 42 {
		t.Errorf("redirect Off() = %d, want 42", redir.Off())
	}
}

func TestItemIdEncoding(t *testing.T) {
	id := NewItemId(4096, 128)
	buf := make([]byte, ItemIdSize)
	encodeItemId(buf, id)
	decoded := decodeItemId(buf)
	if decoded != id {
		t.Fatalf("ItemId round-trip failed: got %d, want %d", decoded, id)
	}
}

func TestNewPageIsValid(t *testing.T) {
	p := NewPage()
	if err := p.Validate(); err != nil {
		t.Fatalf("new page invalid: %v", err)
	}
	if !p.IsEmpty() {
		t.Error("new page should be empty")
	}
	if p.ItemCount() != 0 {
		t.Errorf("ItemCount = %d, want 0", p.ItemCount())
	}
	h := p.Header()
	if int(h.PdLower) != PageHeaderSize {
		t.Errorf("pd_lower = %d, want %d", h.PdLower, PageHeaderSize)
	}
	if int(h.PdUpper) != PageSize {
		t.Errorf("pd_upper = %d, want %d", h.PdUpper, PageSize)
	}
}

func TestInsertAndGetTuple(t *testing.T) {
	p := NewPage()
	data := []byte("hello, postgres!")

	idx, err := p.InsertTuple(data)
	if err != nil {
		t.Fatalf("InsertTuple: %v", err)
	}
	if idx != 0 {
		t.Errorf("first tuple index = %d, want 0", idx)
	}
	if p.ItemCount() != 1 {
		t.Errorf("ItemCount = %d, want 1", p.ItemCount())
	}

	got, err := p.GetTuple(0)
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("GetTuple = %q, want %q", got, data)
	}
}

func TestInsertMultipleTuples(t *testing.T) {
	p := NewPage()
	var payloads [][]byte
	for i := 0; i < 10; i++ {
		payloads = append(payloads, []byte("tuple payload number "+string(rune('0'+i))))
	}
	for i, pl := range payloads {
		idx, err := p.InsertTuple(pl)
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		if idx != i {
			t.Errorf("insert %d returned index %d", i, idx)
		}
	}
	for i, want := range payloads {
		got, err := p.GetTuple(i)
		if err != nil {
			t.Fatalf("GetTuple %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("tuple %d mismatch", i)
		}
	}
}

func TestPageFull(t *testing.T) {
	p := NewPage()
	large := make([]byte, 1000)
	for {
		_, err := p.InsertTuple(large)
		if err != nil {
			se, ok := err.(*StorageError)
			if !ok || se.Code != ErrPageFull {
				t.Fatalf("expected ErrPageFull, got %v", err)
			}
			return
		}
	}
}

func TestMarkDead(t *testing.T) {
	p := NewPage()
	_, err := p.InsertTuple([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.MarkDead(0); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}
	got, err := p.GetTuple(0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil for dead tuple")
	}
	id, _ := p.GetItemId(0)
	if !id.IsDead() {
		t.Error("expected line pointer to be LpDead")
	}
}

func TestFreeSpaceTracking(t *testing.T) {
	p := NewPage()
	initial := p.FreeSpace()
	data := []byte("test data")
	p.InsertTuple(data)
	after := p.FreeSpace()
	expected := initial - len(data) - ItemIdSize
	if after != expected {
		t.Errorf("free space after insert = %d, want %d", after, expected)
	}
}

func TestLSNRoundTrip(t *testing.T) {
	p := NewPage()
	const lsn uint64 = 0x0001_0002_0003_0004
	p.SetLSN(lsn)
	if got := p.LSN(); got != lsn {
		t.Errorf("LSN = %x, want %x", got, lsn)
	}
	h := p.Header()
	if h.PdLsnHi != 0x0001_0002 || h.PdLsnLo != 0x0003_0004 {
		t.Errorf("header fields wrong: hi=%x lo=%x", h.PdLsnHi, h.PdLsnLo)
	}
}

func TestMakePagesizeVersion(t *testing.T) {
	encoded := MakePagesizeVersion(8192, 4)
	size := int(encoded>>8) * 256
	ver := uint8(encoded & 0xFF)
	if size != 8192 {
		t.Errorf("size = %d, want 8192", size)
	}
	if ver != 4 {
		t.Errorf("version = %d, want 4", ver)
	}
}

func TestPageValidationRejectsCorruption(t *testing.T) {
	p := NewPage()
	h := p.Header()
	h.PdLower = 0 // below header end
	encodeHeader(p.data[:PageHeaderSize], &h)
	if err := p.Validate(); err == nil {
		t.Error("expected validation error for pd_lower = 0")
	}
}

func TestPageFromBytes(t *testing.T) {
	p := NewPage()
	_, err := p.InsertTuple([]byte("round-trip"))
	if err != nil {
		t.Fatal(err)
	}
	raw := p.Bytes()
	p2, err := PageFromBytes(raw)
	if err != nil {
		t.Fatalf("PageFromBytes: %v", err)
	}
	got, err := p2.GetTuple(0)
	if err != nil || !bytes.Equal(got, []byte("round-trip")) {
		t.Errorf("round-trip tuple mismatch: %v %q", err, got)
	}
}
