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

// TestBidirectionalLayout pins the exact physical byte offsets that the
// bidirectional page layout must maintain:
//
//	0       24       28       32  ...  8180     8188     8192
//	|hdr(24)| LP[0](4)| LP[1](4)|free | B(8)    | A(4)    |
//
// Line pointers grow upward from pd_lower (offset 24); tuple data grows
// downward from pd_special (offset 8192 for heap pages). They advance toward
// each other and the gap between them is the usable free space.
func TestBidirectionalLayout(t *testing.T) {
	p := NewPage()
	const (
		dataA = "AAAA"     // 4 bytes
		dataB = "BBBBBBBB" // 8 bytes
	)

	// ── After first insert ────────────────────────────────────────────────────
	idxA, err := p.InsertTuple([]byte(dataA))
	if err != nil {
		t.Fatalf("InsertTuple A: %v", err)
	}
	hA := p.Header()

	// pd_lower must have advanced by exactly one ItemIdSize.
	wantLowerA := PageHeaderSize + ItemIdSize // 24 + 4 = 28
	if int(hA.PdLower) != wantLowerA {
		t.Errorf("after A: pd_lower=%d, want %d", hA.PdLower, wantLowerA)
	}
	// pd_upper must have retreated by exactly len(dataA).
	wantUpperA := PageSize - len(dataA) // 8192 - 4 = 8188
	if int(hA.PdUpper) != wantUpperA {
		t.Errorf("after A: pd_upper=%d, want %d", hA.PdUpper, wantUpperA)
	}

	// The line pointer for A must point to the byte just after the new pd_upper.
	lpA, _ := p.GetItemId(idxA)
	if int(lpA.Off()) != wantUpperA {
		t.Errorf("LP[A].Off=%d, want %d (= pd_upper after A)", lpA.Off(), wantUpperA)
	}
	if int(lpA.Len()) != len(dataA) {
		t.Errorf("LP[A].Len=%d, want %d", lpA.Len(), len(dataA))
	}

	// Tuple A bytes must live at [pd_upper, pd_upper+len(A)) - the top of the page.
	raw := p.Bytes()
	if string(raw[wantUpperA:wantUpperA+len(dataA)]) != dataA {
		t.Errorf("raw bytes at pd_upper: got %q, want %q",
			raw[wantUpperA:wantUpperA+len(dataA)], dataA)
	}

	// ── After second insert ───────────────────────────────────────────────────
	idxB, err := p.InsertTuple([]byte(dataB))
	if err != nil {
		t.Fatalf("InsertTuple B: %v", err)
	}
	hB := p.Header()

	wantLowerB := wantLowerA + ItemIdSize // 28 + 4 = 32
	if int(hB.PdLower) != wantLowerB {
		t.Errorf("after B: pd_lower=%d, want %d", hB.PdLower, wantLowerB)
	}
	wantUpperB := wantUpperA - len(dataB) // 8188 - 8 = 8180
	if int(hB.PdUpper) != wantUpperB {
		t.Errorf("after B: pd_upper=%d, want %d", hB.PdUpper, wantUpperB)
	}

	lpB, _ := p.GetItemId(idxB)
	if int(lpB.Off()) != wantUpperB {
		t.Errorf("LP[B].Off=%d, want %d", lpB.Off(), wantUpperB)
	}
	if int(lpB.Len()) != len(dataB) {
		t.Errorf("LP[B].Len=%d, want %d", lpB.Len(), len(dataB))
	}

	if string(raw[wantUpperB:wantUpperB+len(dataB)]) != dataB {
		t.Errorf("raw bytes at B's offset: got %q, want %q",
			raw[wantUpperB:wantUpperB+len(dataB)], dataB)
	}

	// ── Free space is the gap between pd_lower and pd_upper ──────────────────
	wantFree := wantUpperB - wantLowerB // 8180 - 32 = 8148
	if p.FreeSpace() != wantFree {
		t.Errorf("FreeSpace=%d, want %d", p.FreeSpace(), wantFree)
	}

	// ── Line pointer array occupies [PageHeaderSize, pd_lower) ───────────────
	// Bytes between pd_lower and pd_upper must be the free gap (all zero on
	// a fresh page, untouched by either insert).
	for i := wantLowerB; i < wantUpperB; i++ {
		if raw[i] != 0 {
			t.Errorf("free gap byte %d = 0x%02x, want 0x00 (gap is not clean)", i, raw[i])
			break
		}
	}

	// ── Tuple data is inaccessible without going through GetTuple ─────────────
	// Both tuples must still round-trip through the index → pointer → bytes path.
	gotA, _ := p.GetTuple(idxA)
	gotB, _ := p.GetTuple(idxB)
	if string(gotA) != dataA {
		t.Errorf("GetTuple(A)=%q, want %q", gotA, dataA)
	}
	if string(gotB) != dataB {
		t.Errorf("GetTuple(B)=%q, want %q", gotB, dataB)
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
