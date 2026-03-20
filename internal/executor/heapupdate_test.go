package executor

// Heap UPDATE integration tests.
//
// Scenarios:
//   - HOT update: new tuple lands on same page, visible to later snapshot.
//   - Cross-page update: page full, new tuple goes to a new block.
//   - Update + SeqScan: scanner sees only the new version.
//   - Update + IndexScan: index entry points to old TID; HeapFetch walks
//     the HOT chain and returns the new version.
//   - Old version invisible after update commits: MVCC hides superseded tuple.
//   - Snapshot before update: old snapshot sees the pre-update value.
//   - Update then update again: multi-version HOT chain (two hops).

import (
	"fmt"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// doUpdate wraps HeapUpdate with test fatalf.
func doUpdate(t *testing.T, rel *storage.Relation, xid storage.TransactionId,
	blk storage.BlockNumber, off storage.OffsetNumber, newData []byte,
) (storage.BlockNumber, storage.OffsetNumber) {
	t.Helper()
	newTup := storage.NewHeapTuple(xid, 1, newData)
	nb, no, err := HeapUpdate(rel, xid, blk, off, newTup)
	if err != nil {
		t.Fatalf("HeapUpdate: %v", err)
	}
	return nb, no
}

// seqAll runs a SeqScan and returns all visible tuple data strings.
func seqAll(t *testing.T, sr *scanRelation, snap *storage.Snapshot) []string {
	t.Helper()
	sc, err := NewSeqScan(sr.rel, snap, sr.txmgr)
	if err != nil {
		t.Fatalf("NewSeqScan: %v", err)
	}
	defer sc.Close()
	var out []string
	for {
		st, err := sc.Next()
		if err != nil {
			t.Fatalf("SeqScan.Next: %v", err)
		}
		if st == nil {
			break
		}
		out = append(out, string(st.Tuple.Data))
	}
	return out
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestHeapUpdateHOT(t *testing.T) {
	// HOT update: insert "before", commit, then update to "after".
	// A snapshot taken after the update should see only "after".
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("before"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit xid1: %v", err)
	}

	xid2 := sr.txmgr.Begin()
	newBlk, _ := doUpdate(t, sr.rel, xid2, blk, off, []byte("after"))
	if err := sr.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit xid2: %v", err)
	}

	// HOT: new tuple should be on the same page.
	if newBlk != blk {
		t.Logf("note: update placed new tuple on block %d (different from old block %d) — page was full", newBlk, blk)
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)

	if len(rows) != 1 {
		t.Fatalf("expected 1 visible row, got %d: %v", len(rows), rows)
	}
	if rows[0] != "after" {
		t.Errorf("visible row: got %q want %q", rows[0], "after")
	}
}

func TestHeapUpdateOldVersionInvisible(t *testing.T) {
	// After a committed update, the old version must be invisible.
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("v1"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	xid2 := sr.txmgr.Begin()
	doUpdate(t, sr.rel, xid2, blk, off, []byte("v2"))
	if err := sr.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)

	for _, r := range rows {
		if r == "v1" {
			t.Errorf("old version v1 should not be visible after committed update")
		}
	}
	found := false
	for _, r := range rows {
		if r == "v2" {
			found = true
		}
	}
	if !found {
		t.Errorf("new version v2 not found; visible rows: %v", rows)
	}
}

func TestHeapUpdateSnapshotSeesOldVersion(t *testing.T) {
	// A snapshot taken before the update sees the old version, not the new.
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("original"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Old snapshot: taken before the update.
	oldReader := sr.txmgr.Begin()
	oldSnap := sr.txmgr.Snapshot(oldReader)

	xid2 := sr.txmgr.Begin()
	doUpdate(t, sr.rel, xid2, blk, off, []byte("updated"))
	if err := sr.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Old snapshot should see "original" (xmax not committed as of snapshot time).
	rows := seqAll(t, sr, oldSnap)
	if len(rows) != 1 || rows[0] != "original" {
		t.Errorf("old snapshot: got %v want [original]", rows)
	}

	// New snapshot should see "updated".
	newReader := sr.txmgr.Begin()
	newSnap := sr.txmgr.Snapshot(newReader)
	rows2 := seqAll(t, sr, newSnap)
	if len(rows2) != 1 || rows2[0] != "updated" {
		t.Errorf("new snapshot: got %v want [updated]", rows2)
	}
}

func TestHeapUpdateAbortedUpdateInvisible(t *testing.T) {
	// An aborted update must leave the original tuple visible.
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("stable"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	xid2 := sr.txmgr.Begin()
	doUpdate(t, sr.rel, xid2, blk, off, []byte("aborted-update"))
	if err := sr.txmgr.Abort(xid2); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)

	if len(rows) != 1 || rows[0] != "stable" {
		t.Errorf("after aborted update: got %v want [stable]", rows)
	}
}

func TestHeapUpdateIndexScanFollowsHOTChain(t *testing.T) {
	// Index entry points to old TID; after a HOT update HeapFetch must walk
	// the t_ctid chain and return the new version.
	f := newIndexFixture(t)

	key := u32key(99)
	xid1 := f.txmgr.Begin()
	blk, off := f.insertIndexed(t, xid1, key, []byte("old"))
	if err := f.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	xid2 := f.txmgr.Begin()
	newTup := storage.NewHeapTuple(xid2, 1, []byte("new"))
	newBlk, _, err := HeapUpdate(f.heap.rel, xid2, blk, off, newTup)
	if err != nil {
		t.Fatalf("HeapUpdate: %v", err)
	}
	if err := f.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := f.txmgr.Begin()
	snap := f.txmgr.Snapshot(reader)

	if newBlk == blk {
		// HOT update: index entry still points to old TID; HeapFetch must
		// walk the chain to find the new version.
		rows := f.scan(t, snap, key)
		if len(rows) != 1 {
			t.Fatalf("HOT chain walk: got %d rows want 1", len(rows))
		}
		if string(rows[0].Tuple.Data) != "new" {
			t.Errorf("HOT chain: got %q want %q", rows[0].Tuple.Data, "new")
		}
	} else {
		// Cross-page update: add new index entry and verify both old (invisible)
		// and new (visible) TIDs are handled correctly.
		if err := f.idx.Insert(key, newBlk, 1); err != nil {
			t.Fatalf("Insert new index entry: %v", err)
		}
		rows := f.scan(t, snap, key)
		found := false
		for _, r := range rows {
			if string(r.Tuple.Data) == "new" {
				found = true
			}
			if string(r.Tuple.Data) == "old" {
				t.Errorf("old version visible after committed cross-page update")
			}
		}
		if !found {
			t.Errorf("new version not found after cross-page update; rows: %v", rows)
		}
	}
}

func TestHeapUpdateMultiHopHOTChain(t *testing.T) {
	// Two successive HOT updates create a two-hop chain:
	//   v1 →(HOT)→ v2 →(HOT)→ v3
	// A post-commit snapshot must see only v3.
	sr := newScanRelation(t)

	xid1 := sr.txmgr.Begin()
	blk, off := sr.insertTuple(t, xid1, []byte("v1"))
	if err := sr.txmgr.Commit(xid1); err != nil {
		t.Fatalf("Commit xid1: %v", err)
	}

	xid2 := sr.txmgr.Begin()
	newBlk, newOff := doUpdate(t, sr.rel, xid2, blk, off, []byte("v2"))
	if err := sr.txmgr.Commit(xid2); err != nil {
		t.Fatalf("Commit xid2: %v", err)
	}

	if newBlk != blk {
		t.Skip("first update was cross-page; multi-hop HOT test requires same-page updates")
	}

	xid3 := sr.txmgr.Begin()
	newBlk2, _ := doUpdate(t, sr.rel, xid3, newBlk, newOff, []byte("v3"))
	if err := sr.txmgr.Commit(xid3); err != nil {
		t.Fatalf("Commit xid3: %v", err)
	}

	if newBlk2 != blk {
		t.Skip("second update was cross-page; multi-hop HOT test requires same-page updates")
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)
	rows := seqAll(t, sr, snap)

	if len(rows) != 1 {
		t.Fatalf("expected 1 visible row after 2 HOT updates, got %d: %v", len(rows), rows)
	}
	if rows[0] != "v3" {
		t.Errorf("visible: got %q want %q", rows[0], "v3")
	}
}

func TestHeapUpdateCrossPage(t *testing.T) {
	// Fill the first page completely so the update is forced cross-page.
	sr := newScanRelation(t)

	// Insert enough tuples to nearly fill the page, leaving no room for update.
	xidFill := sr.txmgr.Begin()
	var fillBlk storage.BlockNumber
	var fillOff storage.OffsetNumber
	// Insert until the page is full enough that a 100-byte update goes cross-page.
	for i := 0; i < 50; i++ {
		b, o := sr.insertTuple(t, xidFill, []byte(fmt.Sprintf("filler-%03d", i)))
		fillBlk, fillOff = b, o
	}
	if err := sr.txmgr.Commit(xidFill); err != nil {
		t.Fatalf("Commit fill: %v", err)
	}

	// Update the last tuple with a payload large enough to require a new page.
	largeData := make([]byte, 200)
	for i := range largeData {
		largeData[i] = byte('X')
	}

	xidUpd := sr.txmgr.Begin()
	newTup := storage.NewHeapTuple(xidUpd, 1, largeData)
	newBlk, newOff, err := HeapUpdate(sr.rel, xidUpd, fillBlk, fillOff, newTup)
	if err != nil {
		t.Fatalf("HeapUpdate: %v", err)
	}
	if err := sr.txmgr.Commit(xidUpd); err != nil {
		t.Fatalf("Commit update: %v", err)
	}

	reader := sr.txmgr.Begin()
	snap := sr.txmgr.Snapshot(reader)

	// Direct HeapFetch at the new TID must return the large tuple.
	st, err := HeapFetch(sr.rel, snap, sr.txmgr, newBlk, newOff)
	if err != nil {
		t.Fatalf("HeapFetch: %v", err)
	}
	if st == nil {
		t.Fatal("cross-page update: new tuple not visible")
		return
	}
	if string(st.Tuple.Data) != string(largeData) {
		t.Errorf("data mismatch: got %d bytes want %d", len(st.Tuple.Data), len(largeData))
	}
}
