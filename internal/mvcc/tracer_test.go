package mvcc_test

import (
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/mvcc"
	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// recordingTxTracer captures all lifecycle events.
type recordingTxTracer struct {
	begins    []storage.TransactionId
	commits   []storage.TransactionId
	aborts    []storage.TransactionId
	snapshots []storage.TransactionId
}

func (r *recordingTxTracer) OnBegin(xid storage.TransactionId)  { r.begins = append(r.begins, xid) }
func (r *recordingTxTracer) OnCommit(xid storage.TransactionId) { r.commits = append(r.commits, xid) }
func (r *recordingTxTracer) OnAbort(xid storage.TransactionId)  { r.aborts = append(r.aborts, xid) }
func (r *recordingTxTracer) OnSnapshot(xid storage.TransactionId, _ *storage.Snapshot) {
	r.snapshots = append(r.snapshots, xid)
}

func newMgrWithTracer(t *testing.T) (*mvcc.TransactionManager, *recordingTxTracer) {
	t.Helper()
	mgr := mvcc.NewTransactionManager()
	tr := &recordingTxTracer{}
	mgr.SetTracer(tr)
	return mgr, tr
}

// TestTxTracerOnBegin verifies OnBegin fires with the correct XID.
func TestTxTracerOnBegin(t *testing.T) {
	mgr, tr := newMgrWithTracer(t)

	xid := mgr.Begin()
	if len(tr.begins) != 1 {
		t.Fatalf("expected 1 OnBegin, got %d", len(tr.begins))
	}
	if tr.begins[0] != xid {
		t.Errorf("OnBegin xid=%d, Begin()=%d", tr.begins[0], xid)
	}
}

// TestTxTracerOnCommit verifies OnCommit fires after Commit.
func TestTxTracerOnCommit(t *testing.T) {
	mgr, tr := newMgrWithTracer(t)

	xid := mgr.Begin()
	if err := mgr.Commit(xid); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(tr.commits) != 1 || tr.commits[0] != xid {
		t.Errorf("expected OnCommit(%d), got %v", xid, tr.commits)
	}
	if len(tr.aborts) != 0 {
		t.Errorf("unexpected OnAbort events: %v", tr.aborts)
	}
}

// TestTxTracerOnAbort verifies OnAbort fires after Abort.
func TestTxTracerOnAbort(t *testing.T) {
	mgr, tr := newMgrWithTracer(t)

	xid := mgr.Begin()
	if err := mgr.Abort(xid); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if len(tr.aborts) != 1 || tr.aborts[0] != xid {
		t.Errorf("expected OnAbort(%d), got %v", xid, tr.aborts)
	}
	if len(tr.commits) != 0 {
		t.Errorf("unexpected OnCommit events: %v", tr.commits)
	}
}

// TestTxTracerOnSnapshot verifies OnSnapshot fires with the requesting XID.
func TestTxTracerOnSnapshot(t *testing.T) {
	mgr, tr := newMgrWithTracer(t)

	xid := mgr.Begin()
	snap := mgr.Snapshot(xid)
	if snap == nil {
		t.Fatal("Snapshot returned nil")
	}
	if len(tr.snapshots) != 1 || tr.snapshots[0] != xid {
		t.Errorf("expected OnSnapshot(%d), got %v", xid, tr.snapshots)
	}
}

// TestTxTracerMultipleTransactions verifies event counts across several
// concurrent transactions with mixed commit/abort outcomes.
func TestTxTracerMultipleTransactions(t *testing.T) {
	mgr, tr := newMgrWithTracer(t)

	// Start 5 transactions.
	xids := make([]storage.TransactionId, 5)
	for i := range xids {
		xids[i] = mgr.Begin()
	}

	// Commit 3, abort 2.
	for i, xid := range xids {
		if i < 3 {
			_ = mgr.Commit(xid)
		} else {
			_ = mgr.Abort(xid)
		}
	}

	if len(tr.begins) != 5 {
		t.Errorf("expected 5 OnBegin, got %d", len(tr.begins))
	}
	if len(tr.commits) != 3 {
		t.Errorf("expected 3 OnCommit, got %d", len(tr.commits))
	}
	if len(tr.aborts) != 2 {
		t.Errorf("expected 2 OnAbort, got %d", len(tr.aborts))
	}
}

// TestTxTracerSnapshotContents verifies the snapshot passed to OnSnapshot
// is the live snapshot (xmin/xmax set correctly).
func TestTxTracerSnapshotContents(t *testing.T) {
	mgr := mvcc.NewTransactionManager()
	var capturedSnap *storage.Snapshot
	mgr.SetTracer(&captureSnapshotTracer{onSnap: func(_ storage.TransactionId, s *storage.Snapshot) {
		capturedSnap = s
	}})

	// Start two concurrent transactions; take a snapshot as the second.
	x1 := mgr.Begin()
	x2 := mgr.Begin()
	snap := mgr.Snapshot(x2)

	if capturedSnap == nil {
		t.Fatal("OnSnapshot was not called")
	}
	if capturedSnap != snap {
		t.Error("OnSnapshot received a different snapshot pointer than Snapshot() returned")
	}
	// x1 is still active → xmin should be ≤ x1.
	if capturedSnap.Xmin > x1 {
		t.Errorf("xmin %d > x1 %d; x1 should be in snapshot", capturedSnap.Xmin, x1)
	}
	_ = mgr.Commit(x1)
	_ = mgr.Commit(x2)
}

// TestTxTracerNoopSatisfiesInterface ensures NoopTxTracer is usable.
func TestTxTracerNoopSatisfiesInterface(t *testing.T) {
	mgr := mvcc.NewTransactionManager()
	mgr.SetTracer(mvcc.NoopTxTracer{})
	xid := mgr.Begin()
	_ = mgr.Commit(xid)
}

// ── helper ────────────────────────────────────────────────────────────────────

type captureSnapshotTracer struct {
	onSnap func(storage.TransactionId, *storage.Snapshot)
}

func (c *captureSnapshotTracer) OnBegin(storage.TransactionId)  {}
func (c *captureSnapshotTracer) OnCommit(storage.TransactionId) {}
func (c *captureSnapshotTracer) OnAbort(storage.TransactionId)  {}
func (c *captureSnapshotTracer) OnSnapshot(xid storage.TransactionId, s *storage.Snapshot) {
	c.onSnap(xid, s)
}
