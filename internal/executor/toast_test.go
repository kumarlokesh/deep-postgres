package executor

// TOAST integration tests.
//
// Scenarios:
//   - HeapInsert toastifies a large tuple; the in-heap tuple is compact.
//   - Detoast after HeapInsert returns the original payload.
//   - SeqScan returns a toasted tuple; Detoast restores the data.
//   - Small tuples bypass TOAST (stored inline).
//   - Multiple large rows each get distinct chunk_ids.
//   - VacuumFull on a relation with toasted tuples succeeds.

import (
	"bytes"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// largePaylod returns n bytes of patterned data (large enough to trigger TOAST).
func largePayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 253)
	}
	return b
}

// TestHeapInsertToastifiesLargeTuple verifies that inserting a tuple whose
// payload exceeds ToastTupleThreshold stores a compact TOAST pointer in the
// heap page, not the full payload bytes.
func TestHeapInsertToastifiesLargeTuple(t *testing.T) {
	sr := newScanRelation(t)
	payload := largePayload(storage.ToastTupleThreshold + 500)

	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, payload)
	blk, off, err := HeapInsert(sr.rel, tup)
	if err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Read back the raw in-heap tuple.
	id, err := sr.rel.ReadBlock(storage.ForkMain, blk)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	pg, _ := sr.pool.GetPage(id)
	raw, _ := pg.GetTuple(int(off) - 1)
	sr.pool.UnpinBuffer(id) //nolint:errcheck

	if len(raw) < storage.HeapTupleHeaderSize {
		t.Fatalf("raw tuple too short: %d bytes", len(raw))
	}
	inlineData := raw[storage.HeapTupleHeaderSize:]

	// The inline Data must be exactly a TOAST pointer, not the full payload.
	if !storage.IsToasted(inlineData) {
		t.Fatalf("expected TOAST pointer in heap; got %d inline bytes (payload was %d)",
			len(inlineData), len(payload))
	}
	if len(inlineData) != storage.ToastPointerSize {
		t.Errorf("inline data length=%d, want %d (ToastPointerSize)", len(inlineData), storage.ToastPointerSize)
	}
}

// TestHeapInsertToastDetoastRoundTrip verifies that Detoast recovers the
// original payload after a round-trip through HeapInsert.
func TestHeapInsertToastDetoastRoundTrip(t *testing.T) {
	sr := newScanRelation(t)
	original := largePayload(storage.ToastTupleThreshold + 1000)

	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, original)
	blk, off, err := HeapInsert(sr.rel, tup)
	if err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Fetch the tuple back from the page.
	id, _ := sr.rel.ReadBlock(storage.ForkMain, blk)
	pg, _ := sr.pool.GetPage(id)
	raw, _ := pg.GetTuple(int(off) - 1)
	sr.pool.UnpinBuffer(id) //nolint:errcheck

	fetched, err := storage.HeapTupleFromBytes(raw)
	if err != nil {
		t.Fatalf("HeapTupleFromBytes: %v", err)
	}

	recovered, err := storage.Detoast(fetched, sr.rel.Toast)
	if err != nil {
		t.Fatalf("Detoast: %v", err)
	}
	if !bytes.Equal(recovered, original) {
		t.Errorf("round-trip mismatch: recovered %d bytes, want %d", len(recovered), len(original))
	}
}

// TestHeapInsertSmallTupleNotToasted verifies that a small tuple is stored
// inline without touching the TOAST store.
func TestHeapInsertSmallTupleNotToasted(t *testing.T) {
	sr := newScanRelation(t)
	payload := []byte("tiny row")

	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, payload)
	blk, off, err := HeapInsert(sr.rel, tup)
	if err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	_ = sr.txmgr.Commit(xid)

	id, _ := sr.rel.ReadBlock(storage.ForkMain, blk)
	pg, _ := sr.pool.GetPage(id)
	raw, _ := pg.GetTuple(int(off) - 1)
	sr.pool.UnpinBuffer(id) //nolint:errcheck

	inlineData := raw[storage.HeapTupleHeaderSize:]
	if storage.IsToasted(inlineData) {
		t.Error("small tuple should not be toasted")
	}
	if !bytes.Equal(inlineData, payload) {
		t.Errorf("inline data %q, want %q", inlineData, payload)
	}
	if sr.rel.Toast.ChunkSetCount() != 0 {
		t.Errorf("expected 0 chunk sets for small tuple, got %d", sr.rel.Toast.ChunkSetCount())
	}
}

// TestSeqScanReturnsToastedTuple verifies that SeqScan returns a tuple with a
// TOAST pointer in its Data field when the original payload was large, and that
// Detoast recovers the correct payload.
func TestSeqScanReturnsToastedTuple(t *testing.T) {
	sr := newScanRelation(t)
	original := largePayload(storage.ToastTupleThreshold + 200)

	xid := sr.txmgr.Begin()
	tup := storage.NewHeapTuple(xid, 1, original)
	if _, _, err := HeapInsert(sr.rel, tup); err != nil {
		t.Fatalf("HeapInsert: %v", err)
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	scan, err := NewSeqScan(sr.rel, snap, sr.txmgr)
	if err != nil {
		t.Fatalf("NewSeqScan: %v", err)
	}

	st, err := scan.Next()
	if err != nil {
		t.Fatalf("SeqScan.Next: %v", err)
	}
	_ = sr.txmgr.Commit(reader)
	if st == nil {
		t.Fatal("SeqScan.Next: got nil (no row returned)")
		return
	}

	if !storage.IsToasted(st.Tuple.Data) {
		t.Fatalf("expected toasted tuple from scan; got %d inline bytes", len(st.Tuple.Data))
	}

	recovered, err := storage.Detoast(st.Tuple, sr.rel.Toast)
	if err != nil {
		t.Fatalf("Detoast: %v", err)
	}
	if !bytes.Equal(recovered, original) {
		t.Errorf("recovered %d bytes, want %d", len(recovered), len(original))
	}
}

// TestMultipleLargeRowsDistinctChunkIds verifies that each large tuple gets
// its own chunk_id in the TOAST store.
func TestMultipleLargeRowsDistinctChunkIds(t *testing.T) {
	sr := newScanRelation(t)
	const n = 3

	xid := sr.txmgr.Begin()
	for i := 0; i < n; i++ {
		payload := largePayload(storage.ToastTupleThreshold + i*100 + 1)
		tup := storage.NewHeapTuple(xid, 1, payload)
		if _, _, err := HeapInsert(sr.rel, tup); err != nil {
			t.Fatalf("HeapInsert[%d]: %v", i, err)
		}
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if sr.rel.Toast.ChunkSetCount() != n {
		t.Errorf("expected %d chunk sets, got %d", n, sr.rel.Toast.ChunkSetCount())
	}
}

// TestVacuumFullWithToastedTuples verifies that VacuumFull runs without error
// on a relation that contains toasted tuples.
func TestVacuumFullWithToastedTuples(t *testing.T) {
	sr := newScanRelation(t)

	xid := sr.txmgr.Begin()
	for i := 0; i < 2; i++ {
		payload := largePayload(storage.ToastTupleThreshold + 1)
		tup := storage.NewHeapTuple(xid, 1, payload)
		if _, _, err := HeapInsert(sr.rel, tup); err != nil {
			t.Fatalf("HeapInsert: %v", err)
		}
	}
	if err := sr.txmgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	stats := runVacuumFull(t, sr)
	if stats.PagesScanned == 0 {
		t.Error("VacuumFull scanned 0 pages")
	}
	// Toasted tuples are compact in the heap; no dead tuples to remove.
	if stats.TuplesRemoved != 0 {
		t.Errorf("unexpected TuplesRemoved=%d", stats.TuplesRemoved)
	}
}
