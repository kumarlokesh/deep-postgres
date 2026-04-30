// Package crash_recovery demonstrates end-to-end crash recovery using the
// deep-postgres WAL and storage subsystems.
//
// # Scenario
//
// Three transactions write to the same heap page before a simulated crash:
//
//	tx10: INSERT "committed-a"  → COMMIT
//	tx11: INSERT "in-flight"    → [crash — no COMMIT or ABORT record]
//	tx12: INSERT "committed-b"  → COMMIT
//
// The WAL segment encoding these records is replayed into a fresh buffer pool
// (simulating crash recovery from a cold start). A commitTracker watches
// XACT_COMMIT records to populate a SimpleTransactionOracle, which is then
// used for MVCC visibility checks.
//
// Expected post-recovery scan: "committed-a" and "committed-b" visible;
// "in-flight" invisible (tx11's status is TxInProgress in the oracle).
//
// Run:
//
//	go test -v ./experiments/crash-recovery/
package crash_recovery

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"text/tabwriter"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
	"github.com/kumarlokesh/deep-postgres/internal/wal"
)

// ── constants ─────────────────────────────────────────────────────────────────

const (
	txCommittedA uint32 = 10 // commits before crash
	txInFlight   uint32 = 11 // in-progress at crash
	txCommittedB uint32 = 12 // commits before crash
)

var crashReln = wal.RelFileLocator{SpcOid: 0, DbOid: 1, RelOid: 300}

// ── commitTracker ─────────────────────────────────────────────────────────────

// commitTracker implements wal.LogicalDecoder to track which XIDs committed
// during WAL replay, populating a SimpleTransactionOracle.
type commitTracker struct {
	oracle *storage.SimpleTransactionOracle
}

func (ct *commitTracker) OnInsert(_ uint32, _ wal.LSN, _ wal.RelFileLocator, _ uint32, _ uint16, _ []byte) {
}
func (ct *commitTracker) OnDelete(_ uint32, _ wal.LSN, _ wal.RelFileLocator, _ uint32, _ uint16) {}
func (ct *commitTracker) OnUpdate(_ uint32, _ wal.LSN, _ wal.RelFileLocator, _ uint32, _ uint16, _ []byte) {
}
func (ct *commitTracker) OnCommit(xid uint32, _ wal.LSN) {
	ct.oracle.Commit(storage.TransactionId(xid))
}
func (ct *commitTracker) OnAbort(xid uint32, _ wal.LSN) {
	ct.oracle.Abort(storage.TransactionId(xid))
}

// ── WAL record builders ───────────────────────────────────────────────────────

// heapTupleBytes builds the raw bytes (24-byte header + payload) for a freshly
// inserted tuple. TInfomask carries HEAP_XMAX_INVALID so xmax is treated as
// absent by the visibility rules.
func heapTupleBytes(xmin uint32, payload string) []byte {
	data := []byte(payload)
	buf := make([]byte, storage.HeapTupleHeaderSize+len(data))
	binary.LittleEndian.PutUint32(buf[0:], xmin)    // TXmin
	binary.LittleEndian.PutUint32(buf[4:], 0)        // TXmax = 0 (InvalidTransactionId)
	binary.LittleEndian.PutUint32(buf[8:], 0)        // TCid = 0 (FirstCommandId)
	binary.LittleEndian.PutUint32(buf[12:], 0)       // TCtidBlock
	binary.LittleEndian.PutUint16(buf[16:], 0)       // TCtidOffset
	binary.LittleEndian.PutUint16(buf[18:], 0x0001)  // TInfomask2 = 1 attr
	binary.LittleEndian.PutUint16(buf[20:], 0x0800)  // TInfomask = HEAP_XMAX_INVALID
	buf[22] = storage.HeapTupleHeaderSize             // THoff
	copy(buf[storage.HeapTupleHeaderSize:], data)
	return buf
}

// insertRecord returns an XLOG_HEAP_INSERT record for tupleData at offnum.
// The first insert for a block sets initPage=true so the page is initialised
// during replay from a cold (crashed) buffer pool.
func insertRecord(xid uint32, offnum uint16, tupleData []byte, initPage bool) *wal.Record {
	// xl_heap_insert: offnum(2) + flags(1) = 3 bytes
	mainData := []byte{byte(offnum), byte(offnum >> 8), 0x00}
	info := uint8(0x00) // XLOG_HEAP_INSERT
	if initPage {
		info |= 0x80 // XLOG_HEAP_INIT_PAGE
	}
	return &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrHeap,
			XlInfo: info,
			XlXid:  xid,
		},
		BlockRefs: []wal.BlockRef{{
			ID:       0,
			Reln:     crashReln,
			ForkNum:  wal.ForkMain,
			BlockNum: 0,
			Data:     tupleData,
		}},
		MainData: mainData,
	}
}

// commitRecord returns an XLOG_XACT_COMMIT record for xid.
func commitRecord(xid uint32) *wal.Record {
	return &wal.Record{
		Header: wal.XLogRecord{
			XlRmid: wal.RmgrXact,
			XlInfo: 0x00, // XLOG_XACT_COMMIT
			XlXid:  xid,
		},
		MainData: []byte{}, // xl_xact_commit body not needed for our redo
	}
}

// ── replay helper ─────────────────────────────────────────────────────────────

// recoveryResult holds everything needed to inspect post-recovery state.
type recoveryResult struct {
	pool   *storage.BufferPool
	store  *storage.WalPageStore
	oracle *storage.SimpleTransactionOracle
}

// buildSegmentAndReplay encodes recs into a WAL segment and replays it into a
// fresh pool, returning the components needed for post-recovery scanning.
// The commitTracker is wired as the logical decoder so XACT_COMMIT records
// update the oracle.
func buildSegmentAndReplay(t *testing.T, recs []*wal.Record) recoveryResult {
	t.Helper()

	tli := wal.TimeLineID(1)
	segStart := wal.MakeLSN(0, 0)
	b := wal.NewSegmentBuilder(tli, segStart, 1)
	for i, rec := range recs {
		data, err := wal.Encode(rec)
		if err != nil {
			t.Fatalf("Encode record %d: %v", i, err)
		}
		if _, err := b.AppendRecord(data); err != nil {
			t.Fatalf("AppendRecord %d: %v", i, err)
		}
	}

	pool := storage.NewBufferPool(32)
	store := storage.NewWalPageStore(pool, nil)
	oracle := storage.NewSimpleOracle()
	tracker := &commitTracker{oracle: oracle}

	provider := wal.NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())

	engine := wal.NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	engine.SetStore(store)
	engine.SetLogical(tracker)
	if err := engine.Run(); err != nil {
		t.Fatalf("RedoEngine.Run: %v", err)
	}

	return recoveryResult{pool: pool, store: store, oracle: oracle}
}

// scanVisible returns the data strings of all MVCC-visible tuples on block 0
// of crashReln after recovery.
func scanVisible(t *testing.T, r recoveryResult, snap *storage.Snapshot) []string {
	t.Helper()

	relOid := r.store.RelOidFor(crashReln)
	tag := storage.BufferTag{
		RelationId: relOid,
		Fork:       storage.ForkMain,
		BlockNum:   0,
	}
	bufID, err := r.pool.ReadBuffer(tag)
	if err != nil {
		t.Fatalf("ReadBuffer: %v", err)
	}
	defer r.pool.UnpinBuffer(bufID) //nolint:errcheck

	page, err := r.pool.GetPage(bufID)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}

	var results []string
	for i := 0; i < page.ItemCount(); i++ {
		lp, err := page.GetItemId(i)
		if err != nil || !lp.IsNormal() {
			continue
		}
		raw, err := page.GetTuple(i)
		if err != nil || len(raw) < storage.HeapTupleHeaderSize {
			continue
		}
		tup, err := storage.HeapTupleFromBytes(raw)
		if err != nil {
			continue
		}
		vis := storage.HeapTupleSatisfiesMVCC(&tup.Header, snap, r.oracle)
		if vis.IsVisible() {
			results = append(results, string(tup.Data))
		}
	}
	return results
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestCrashRecovery is the core scenario: two committed + one in-flight tx.
func TestCrashRecovery(t *testing.T) {
	recs := []*wal.Record{
		// tx10: insert + commit (before crash)
		insertRecord(txCommittedA, 1, heapTupleBytes(txCommittedA, "committed-a"), true),
		commitRecord(txCommittedA),
		// tx11: insert, NO commit (in-flight at crash)
		insertRecord(txInFlight, 2, heapTupleBytes(txInFlight, "in-flight"), false),
		// tx12: insert + commit (before crash)
		insertRecord(txCommittedB, 3, heapTupleBytes(txCommittedB, "committed-b"), false),
		commitRecord(txCommittedB),
		// <crash: WAL ends here, tx11 never committed>
	}

	r := buildSegmentAndReplay(t, recs)

	// Post-recovery snapshot: observer sees the world after WAL replay.
	snap := storage.NewSnapshot(99, 1, 99)

	visible := scanVisible(t, r, snap)

	t.Logf("visible tuples after recovery: %v", visible)
	t.Logf("oracle: committed=%v in-flight=%v",
		r.oracle.Status(txCommittedA) == storage.TxCommitted,
		r.oracle.Status(txInFlight) == storage.TxInProgress,
	)

	// All three tuples must be physically present on the page.
	relOid := r.store.RelOidFor(crashReln)
	tag := storage.BufferTag{RelationId: relOid, Fork: storage.ForkMain, BlockNum: 0}
	bufID, _ := r.pool.ReadBuffer(tag)
	page, _ := r.pool.GetPage(bufID)
	r.pool.UnpinBuffer(bufID) //nolint:errcheck
	if page.ItemCount() != 3 {
		t.Errorf("page slot count: got %d want 3", page.ItemCount())
	}

	// Only committed transactions must be visible.
	if len(visible) != 2 {
		t.Errorf("visible count: got %d want 2", len(visible))
	}
	seen := make(map[string]bool, len(visible))
	for _, v := range visible {
		seen[v] = true
	}
	if !seen["committed-a"] {
		t.Error("committed-a: expected visible, got invisible")
	}
	if !seen["committed-b"] {
		t.Error("committed-b: expected visible, got invisible")
	}
	if seen["in-flight"] {
		t.Error("in-flight: expected invisible (tx11 never committed), got visible")
	}
}

// TestCrashRecoveryExplicitAbort verifies that an explicitly aborted tx is
// invisible, not just in-flight ones.
func TestCrashRecoveryExplicitAbort(t *testing.T) {
	abortXid := uint32(20)
	commitXid := uint32(21)

	recs := []*wal.Record{
		insertRecord(abortXid, 1, heapTupleBytes(abortXid, "aborted"), true),
		{
			Header: wal.XLogRecord{
				XlRmid: wal.RmgrXact,
				XlInfo: 0x20, // XLOG_XACT_ABORT
				XlXid:  abortXid,
			},
			MainData: []byte{},
		},
		insertRecord(commitXid, 2, heapTupleBytes(commitXid, "present"), false),
		commitRecord(commitXid),
	}

	r := buildSegmentAndReplay(t, recs)
	snap := storage.NewSnapshot(99, 1, 99)
	visible := scanVisible(t, r, snap)

	t.Logf("explicit-abort: visible=%v oracle(aborted)=%v oracle(committed)=%v",
		visible,
		r.oracle.Status(storage.TransactionId(abortXid)),
		r.oracle.Status(storage.TransactionId(commitXid)),
	)

	if len(visible) != 1 || visible[0] != "present" {
		t.Errorf("visible: got %v want [present]", visible)
	}
}

// TestCrashRecoveryAllCommitted verifies that when all transactions commit,
// all tuples are visible.
func TestCrashRecoveryAllCommitted(t *testing.T) {
	recs := []*wal.Record{
		insertRecord(10, 1, heapTupleBytes(10, "a"), true),
		commitRecord(10),
		insertRecord(11, 2, heapTupleBytes(11, "b"), false),
		commitRecord(11),
		insertRecord(12, 3, heapTupleBytes(12, "c"), false),
		commitRecord(12),
	}

	r := buildSegmentAndReplay(t, recs)
	snap := storage.NewSnapshot(99, 1, 99)
	visible := scanVisible(t, r, snap)

	if len(visible) != 3 {
		t.Errorf("all-committed: visible=%v want [a b c]", visible)
	}
}

// ── TestMain: summary table ───────────────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "crash-recovery: WAL replay + MVCC visibility after simulated crash")
	fmt.Fprintln(os.Stderr, "")

	scenarios := []struct {
		name   string
		recs   []*wal.Record
		expect []string // visible payloads
	}{
		{
			name: "2-committed + 1-in-flight",
			recs: []*wal.Record{
				insertRecord(txCommittedA, 1, heapTupleBytes(txCommittedA, "committed-a"), true),
				commitRecord(txCommittedA),
				insertRecord(txInFlight, 2, heapTupleBytes(txInFlight, "in-flight"), false),
				insertRecord(txCommittedB, 3, heapTupleBytes(txCommittedB, "committed-b"), false),
				commitRecord(txCommittedB),
			},
			expect: []string{"committed-a", "committed-b"},
		},
		{
			name: "1-committed + 1-aborted",
			recs: []*wal.Record{
				insertRecord(20, 1, heapTupleBytes(20, "aborted"), true),
				{Header: wal.XLogRecord{XlRmid: wal.RmgrXact, XlInfo: 0x20, XlXid: 20}, MainData: []byte{}},
				insertRecord(21, 2, heapTupleBytes(21, "committed"), false),
				commitRecord(21),
			},
			expect: []string{"committed"},
		},
		{
			name: "all-committed",
			recs: []*wal.Record{
				insertRecord(10, 1, heapTupleBytes(10, "a"), true),
				commitRecord(10),
				insertRecord(11, 2, heapTupleBytes(11, "b"), false),
				commitRecord(11),
			},
			expect: []string{"a", "b"},
		},
	}

	tw := tabwriter.NewWriter(os.Stderr, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "Scenario\tPhysical tuples\tVisible\tInvisible")
	fmt.Fprintln(tw, "--------\t---------------\t-------\t---------")

	snap := storage.NewSnapshot(99, 1, 99)
	for _, sc := range scenarios {
		pool := storage.NewBufferPool(32)
		store := storage.NewWalPageStore(pool, nil)
		oracle := storage.NewSimpleOracle()

		tli := wal.TimeLineID(1)
		seg := wal.MakeLSN(0, 0)
		b := wal.NewSegmentBuilder(tli, seg, 1)
		for _, rec := range sc.recs {
			data, _ := wal.Encode(rec)
			b.AppendRecord(data) //nolint:errcheck
		}
		p := wal.NewInMemoryProvider()
		p.Add(seg, b.Bytes())
		e := wal.NewRedoEngine(tli, seg, 0, p.Provide, nil)
		e.SetStore(store)
		e.SetLogical(&commitTracker{oracle: oracle})
		e.Run() //nolint:errcheck

		relOid := store.RelOidFor(crashReln)
		tag := storage.BufferTag{RelationId: relOid, Fork: storage.ForkMain, BlockNum: 0}
		bufID, _ := pool.ReadBuffer(tag)
		page, _ := pool.GetPage(bufID)
		pool.UnpinBuffer(bufID) //nolint:errcheck

		physical := page.ItemCount()
		var vis, invis int
		for i := 0; i < physical; i++ {
			lp, err := page.GetItemId(i)
			if err != nil || !lp.IsNormal() {
				continue
			}
			raw, err := page.GetTuple(i)
			if err != nil || len(raw) < storage.HeapTupleHeaderSize {
				continue
			}
			tup, err := storage.HeapTupleFromBytes(raw)
			if err != nil {
				continue
			}
			if storage.HeapTupleSatisfiesMVCC(&tup.Header, snap, oracle).IsVisible() {
				vis++
			} else {
				invis++
			}
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\n", sc.name, physical, vis, invis)
	}
	tw.Flush()
	fmt.Fprintln(os.Stderr, "")

	os.Exit(m.Run())
}
