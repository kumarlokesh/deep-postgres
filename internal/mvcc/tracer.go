package mvcc

import "github.com/kumarlokesh/deep-postgres/internal/storage"

// TxTracer receives transaction lifecycle events from TransactionManager.
//
// The interface mirrors the pluggable-observer pattern used by
// storage.BufferTracer and wal.RedoTracer.  Use NoopTxTracer as a zero-cost
// default; replace with a recording implementation for testing or metrics.
//
// All callbacks are invoked synchronously inside TransactionManager methods,
// so implementations must not re-enter the manager.

// TxTracer observes transaction lifecycle events.
type TxTracer interface {
	// OnBegin is called when a new transaction is started, after the XID is
	// assigned and marked TxInProgress in the CLOG.
	OnBegin(xid storage.TransactionId)

	// OnCommit is called when a transaction commits, after the CLOG is updated
	// to TxCommitted.
	OnCommit(xid storage.TransactionId)

	// OnAbort is called when a transaction aborts, after the CLOG is updated
	// to TxAborted.
	OnAbort(xid storage.TransactionId)

	// OnSnapshot is called when a snapshot is acquired.  snap is the freshly
	// built Snapshot; xid is the requesting transaction's XID.
	OnSnapshot(xid storage.TransactionId, snap *storage.Snapshot)
}

// NoopTxTracer discards all events.
type NoopTxTracer struct{}

func (NoopTxTracer) OnBegin(storage.TransactionId)                       {}
func (NoopTxTracer) OnCommit(storage.TransactionId)                      {}
func (NoopTxTracer) OnAbort(storage.TransactionId)                       {}
func (NoopTxTracer) OnSnapshot(storage.TransactionId, *storage.Snapshot) {}
