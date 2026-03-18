package wal

// Logical decoding integration tests.
//
// Each test builds a synthetic WAL segment containing heap DML records and an
// XLOG_XACT_COMMIT record, then runs the RedoEngine with a ReorderBuffer and
// asserts the emitted changes.
//
// Helper makeXactCommitRecord encodes an XLOG_XACT_COMMIT record whose XlXid
// matches the inserting transaction so that OnCommit flushes the right buffer.

import (
	"encoding/binary"
	"testing"
)

// testRelnLogical is the synthetic relation used in logical decoding tests.
var testRelnLogical = RelFileLocator{SpcOid: 0, DbOid: 1, RelOid: 50}

// makeXactCommitRecord builds a minimal XLOG_XACT_COMMIT record for xid.
// PostgreSQL's xl_xact_commit contains a timestamp and optional arrays; our
// decoder only needs the rmgr header to call OnCommit.
func makeXactCommitRecord(xid uint32) *Record {
	return &Record{
		Header: XLogRecord{
			XlRmid: RmgrXact,
			XlXid:  xid,
			XlInfo: xlXactCommit,
		},
		MainData: []byte{}, // no main-data payload needed for our redo
	}
}

// makeHeapInsertRecordWithXid builds an XLOG_HEAP_INSERT record with an
// explicit xl_xid so the logical decoder can attribute the change.
func makeHeapInsertRecordWithXid(xid uint32, block uint32, offnum uint16, tuple []byte) *Record {
	mainData := make([]byte, xlHeapInsertSize)
	binary.LittleEndian.PutUint16(mainData[0:], offnum)
	mainData[2] = 0 // flags

	return &Record{
		Header: XLogRecord{
			XlRmid: RmgrHeap,
			XlXid:  xid,
			XlInfo: xlHeapInsert,
		},
		BlockRefs: []BlockRef{
			{ID: 0, Reln: testRelnLogical, ForkNum: ForkMain, BlockNum: block, Data: tuple},
		},
		MainData: mainData,
	}
}

// makeHeapDeleteRecordWithXid builds an XLOG_HEAP_DELETE record.
func makeHeapDeleteRecordWithXid(xid uint32, block uint32, offnum uint16) *Record {
	mainData := make([]byte, xlHeapDeleteSize)
	binary.LittleEndian.PutUint32(mainData[0:], 0) // xmax (unused in our redo)
	binary.LittleEndian.PutUint16(mainData[4:], offnum)
	mainData[6] = 0 // infobits_set
	mainData[7] = 0 // flags

	return &Record{
		Header: XLogRecord{
			XlRmid: RmgrHeap,
			XlXid:  xid,
			XlInfo: xlHeapDelete,
		},
		BlockRefs: []BlockRef{
			{ID: 0, Reln: testRelnLogical, ForkNum: ForkMain, BlockNum: block},
		},
		MainData: mainData,
	}
}

// runLogical builds a one-segment WAL stream from recs and replays it with a
// ReorderBuffer, returning the buffer and emitted changes in order.
func runLogical(t *testing.T, recs []*Record) [][]Change {
	t.Helper()
	var committed [][]Change

	rb := NewReorderBuffer(func(_ uint32, _ LSN, changes []Change) {
		cp := make([]Change, len(changes))
		copy(cp, changes)
		committed = append(committed, cp)
	})

	tli := TimeLineID(1)
	segStart := MakeLSN(0, 0)
	b := NewSegmentBuilder(tli, segStart, 1)
	for i, rec := range recs {
		data, err := Encode(rec)
		if err != nil {
			t.Fatalf("Encode[%d]: %v", i, err)
		}
		if _, err := b.AppendRecord(data); err != nil {
			t.Fatalf("AppendRecord[%d]: %v", i, err)
		}
	}

	provider := NewInMemoryProvider()
	provider.Add(segStart, b.Bytes())
	engine := NewRedoEngine(tli, segStart, 0, provider.Provide, nil)
	engine.SetLogical(rb)
	if err := engine.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return committed
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestLogicalSingleInsertCommit(t *testing.T) {
	const xid = uint32(10)
	tuple := []byte("hello logical world")

	committed := runLogical(t, []*Record{
		makeHeapInsertRecordWithXid(xid, 0, 1, tuple),
		makeXactCommitRecord(xid),
	})

	if len(committed) != 1 {
		t.Fatalf("committed txns: got %d want 1", len(committed))
	}
	changes := committed[0]
	if len(changes) != 1 {
		t.Fatalf("changes: got %d want 1", len(changes))
	}
	c := changes[0]
	if c.Type != ChangeInsert {
		t.Errorf("Type: got %v want INSERT", c.Type)
	}
	if c.XID != xid {
		t.Errorf("XID: got %d want %d", c.XID, xid)
	}
	if c.Reln != testRelnLogical {
		t.Errorf("Reln: got %v want %v", c.Reln, testRelnLogical)
	}
	if string(c.TupleData) != string(tuple) {
		t.Errorf("TupleData: got %q want %q", c.TupleData, tuple)
	}
}

func TestLogicalAbortedTransactionNotEmitted(t *testing.T) {
	const xid = uint32(20)
	tuple := []byte("should not appear")

	committed := runLogical(t, []*Record{
		makeHeapInsertRecordWithXid(xid, 0, 1, tuple),
		{
			Header: XLogRecord{XlRmid: RmgrXact, XlXid: xid, XlInfo: xlXactAbort},
		},
	})

	if len(committed) != 0 {
		t.Errorf("aborted txn: expected 0 committed batches, got %d", len(committed))
	}
}

func TestLogicalMultipleInsertsInTransaction(t *testing.T) {
	const xid = uint32(30)
	tuples := [][]byte{
		[]byte("row one"),
		[]byte("row two"),
		[]byte("row three"),
	}

	recs := make([]*Record, 0, len(tuples)+1)
	for i, tup := range tuples {
		recs = append(recs, makeHeapInsertRecordWithXid(xid, 0, uint16(i+1), tup))
	}
	recs = append(recs, makeXactCommitRecord(xid))

	committed := runLogical(t, recs)

	if len(committed) != 1 {
		t.Fatalf("committed txns: got %d want 1", len(committed))
	}
	changes := committed[0]
	if len(changes) != 3 {
		t.Fatalf("changes: got %d want 3", len(changes))
	}
	for i, tup := range tuples {
		if changes[i].Type != ChangeInsert {
			t.Errorf("[%d] Type: got %v", i, changes[i].Type)
		}
		if string(changes[i].TupleData) != string(tup) {
			t.Errorf("[%d] TupleData: got %q want %q", i, changes[i].TupleData, tup)
		}
	}
}

func TestLogicalInsertThenDelete(t *testing.T) {
	const xid = uint32(40)
	tuple := []byte("to be deleted")

	committed := runLogical(t, []*Record{
		makeHeapInsertRecordWithXid(xid, 0, 1, tuple),
		makeHeapDeleteRecordWithXid(xid, 0, 1),
		makeXactCommitRecord(xid),
	})

	if len(committed) != 1 {
		t.Fatalf("committed txns: got %d want 1", len(committed))
	}
	changes := committed[0]
	if len(changes) != 2 {
		t.Fatalf("changes: got %d want 2 (insert+delete)", len(changes))
	}
	if changes[0].Type != ChangeInsert {
		t.Errorf("[0] Type: got %v want INSERT", changes[0].Type)
	}
	if changes[1].Type != ChangeDelete {
		t.Errorf("[1] Type: got %v want DELETE", changes[1].Type)
	}
	if changes[1].TupleData != nil {
		t.Errorf("[1] DELETE should have nil TupleData")
	}
}

func TestLogicalInterleavedTransactions(t *testing.T) {
	// Two transactions interleaved in WAL; each must emit independently.
	const xidA = uint32(50)
	const xidB = uint32(51)

	committed := runLogical(t, []*Record{
		makeHeapInsertRecordWithXid(xidA, 0, 1, []byte("A-first")),
		makeHeapInsertRecordWithXid(xidB, 1, 1, []byte("B-first")),
		makeHeapInsertRecordWithXid(xidA, 0, 2, []byte("A-second")),
		makeXactCommitRecord(xidA), // A commits first
		makeXactCommitRecord(xidB), // B commits second
	})

	if len(committed) != 2 {
		t.Fatalf("committed txns: got %d want 2", len(committed))
	}
	// First committed batch should be A's changes (2 inserts).
	if len(committed[0]) != 2 {
		t.Errorf("txnA changes: got %d want 2", len(committed[0]))
	}
	// Second committed batch should be B's changes (1 insert).
	if len(committed[1]) != 1 {
		t.Errorf("txnB changes: got %d want 1", len(committed[1]))
	}
}

func TestLogicalReorderBufferPendingAfterInsert(t *testing.T) {
	// Changes are buffered until commit; pending count reflects this.
	rb := NewReorderBuffer(nil)
	rb.OnInsert(99, 0, testRelnLogical, 0, 1, []byte("pending"))
	if rb.PendingCount() != 1 {
		t.Errorf("PendingCount: got %d want 1", rb.PendingCount())
	}
	rb.OnCommit(99, 0)
	if rb.PendingCount() != 0 {
		t.Errorf("PendingCount after commit: got %d want 0", rb.PendingCount())
	}
}

func TestLogicalChangesOrderedByWalPosition(t *testing.T) {
	// Verify that changes within a transaction are emitted in WAL order
	// (the order they were appended).
	const xid = uint32(60)
	var got []ChangeType
	rb := NewReorderBuffer(func(_ uint32, _ LSN, changes []Change) {
		for _, c := range changes {
			got = append(got, c.Type)
		}
	})

	rb.OnInsert(xid, 1, testRelnLogical, 0, 1, nil)
	rb.OnDelete(xid, 2, testRelnLogical, 0, 1)
	rb.OnInsert(xid, 3, testRelnLogical, 0, 2, nil)
	rb.OnCommit(xid, 10)

	want := []ChangeType{ChangeInsert, ChangeDelete, ChangeInsert}
	if len(got) != len(want) {
		t.Fatalf("order: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d]: got %v want %v", i, got[i], want[i])
		}
	}
}
