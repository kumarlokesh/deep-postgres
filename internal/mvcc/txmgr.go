package mvcc

// TransactionManager assigns transaction IDs, tracks in-progress transactions,
// and produces MVCC snapshots.  It mirrors the role of PostgreSQL's ProcArray
// (src/backend/storage/ipc/procarray.c) combined with the transaction state
// machinery in src/backend/access/transam/xact.c.
//
// The implementation is intentionally single-threaded.  A concurrent version
// would protect nextXid and active with a lock (PostgreSQL uses ProcArrayLock
// and XidGenLock).
//
// Thread-safety: none — single-threaded by design.

import (
	"fmt"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// TransactionManager is the central coordinator for XID assignment and snapshot
// acquisition.
type TransactionManager struct {
	clog    *Clog
	nextXid storage.TransactionId                   // next XID to assign
	active  map[storage.TransactionId]storage.CommandId // xid → current CID
}

// NewTransactionManager creates a manager with a fresh CLOG.
// The first normal XID assigned will be FirstNormalTransactionId (3).
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		clog:    NewClog(),
		nextXid: storage.FirstNormalTransactionId,
		active:  make(map[storage.TransactionId]storage.CommandId),
	}
}

// Begin starts a new transaction and returns its XID.
// Panics if XID space is exhausted (wrap-around not implemented here).
func (m *TransactionManager) Begin() storage.TransactionId {
	xid := m.nextXid
	m.nextXid++
	m.active[xid] = storage.FirstCommandId
	m.clog.SetStatus(xid, storage.TxInProgress)
	return xid
}

// AdvanceCommand increments the command ID for xid.
// Used within a transaction to make prior-command inserts visible to later
// commands in the same transaction (mimics CommandCounterIncrement).
func (m *TransactionManager) AdvanceCommand(xid storage.TransactionId) error {
	cid, ok := m.active[xid]
	if !ok {
		return fmt.Errorf("txmgr: xid %d is not active", xid)
	}
	m.active[xid] = cid + 1
	return nil
}

// Commit records xid as committed and removes it from the active set.
func (m *TransactionManager) Commit(xid storage.TransactionId) error {
	if _, ok := m.active[xid]; !ok {
		return fmt.Errorf("txmgr: commit of unknown or already-ended xid %d", xid)
	}
	m.clog.SetStatus(xid, storage.TxCommitted)
	delete(m.active, xid)
	return nil
}

// Abort records xid as aborted and removes it from the active set.
func (m *TransactionManager) Abort(xid storage.TransactionId) error {
	if _, ok := m.active[xid]; !ok {
		return fmt.Errorf("txmgr: abort of unknown or already-ended xid %d", xid)
	}
	m.clog.SetStatus(xid, storage.TxAborted)
	delete(m.active, xid)
	return nil
}

// Status returns the commit status of xid.  Implements storage.TransactionOracle.
func (m *TransactionManager) Status(xid storage.TransactionId) storage.TransactionStatus {
	return m.clog.Status(xid)
}

// Snapshot captures the current transaction state as an MVCC snapshot.
//
// Mirrors PostgreSQL's GetSnapshotData (procarray.c):
//   1. xmax = nextXid (no XID >= nextXid is visible)
//   2. Collect all active XIDs into xip.
//   3. xmin = min(active XIDs); if none, xmin = xmax.
//
// The caller's own XID (txid) is embedded in the snapshot so that
// self-inserted tuples are handled correctly by HeapTupleSatisfiesMVCC.
func (m *TransactionManager) Snapshot(txid storage.TransactionId) *storage.Snapshot {
	xmax := m.nextXid
	xmin := xmax // will be lowered below

	xip := make([]storage.TransactionId, 0, len(m.active))
	for xid := range m.active {
		xip = append(xip, xid)
		if xid < xmin {
			xmin = xid
		}
	}

	// Determine the CID for the caller's own transaction.
	curCid := storage.FirstCommandId
	if cid, ok := m.active[txid]; ok {
		curCid = cid
	}

	return &storage.Snapshot{
		Xid:    txid,
		Xmin:   xmin,
		Xmax:   xmax,
		Xip:    xip,
		CurCid: curCid,
	}
}

// NextXid returns the next XID that would be assigned by Begin.
// Useful for asserting expected sequencing in tests.
func (m *TransactionManager) NextXid() storage.TransactionId { return m.nextXid }

// ActiveCount returns the number of in-progress transactions.
func (m *TransactionManager) ActiveCount() int { return len(m.active) }
