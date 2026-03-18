package wal

// Logical decoding — row-level change events extracted from WAL.
//
// PostgreSQL's logical decoding (src/backend/replication/logical/) reads the
// same WAL stream used for crash recovery but extracts row-level INSERT /
// UPDATE / DELETE events rather than replaying page-level changes.
//
// This package implements the minimal interface needed to:
//  1. Receive decoded row changes from rmgr Redo callbacks.
//  2. Buffer those changes per transaction in a ReorderBuffer.
//  3. Emit them in commit order when a COMMIT record arrives.
//
// The architecture mirrors PostgreSQL's:
//   WAL record → rmgr logical callback → ReorderBuffer.Append
//                                                    ↓ COMMIT
//                                              ChangeHandler.OnChanges
//
// Tuple data is delivered as raw bytes (HeapTupleHeader + body); callers that
// need column-level decoding must parse the tuple against their own catalog.

// ChangeType classifies a row-level change.
type ChangeType uint8

const (
	ChangeInsert ChangeType = iota
	ChangeUpdate
	ChangeDelete
)

func (c ChangeType) String() string {
	switch c {
	case ChangeInsert:
		return "INSERT"
	case ChangeUpdate:
		return "UPDATE"
	case ChangeDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Change is a single decoded row-level change event.
type Change struct {
	// Type classifies the operation.
	Type ChangeType

	// XID is the transaction ID that made this change.
	XID uint32

	// LSN is the WAL position of the record that produced this change.
	LSN LSN

	// Reln identifies the heap relation that was modified.
	Reln RelFileLocator

	// Block and Offnum locate the tuple on its page.
	Block  uint32
	Offnum uint16

	// TupleData holds the raw HeapTuple bytes for INSERT and the new-image
	// bytes for UPDATE.  Nil for DELETE (the old tuple is not stored in WAL
	// by default unless REPLICA IDENTITY FULL is set).
	TupleData []byte
}

// ChangeHandler receives committed transaction change lists.
// Implementations must not retain the slice after OnChanges returns.
type ChangeHandler func(xid uint32, commitLSN LSN, changes []Change)

// LogicalDecoder is the interface that rmgr Redo callbacks use to record
// row-level changes.  It is separate from PageWriter so that logical and
// physical redo can be enabled independently.
type LogicalDecoder interface {
	// OnInsert records a heap insert.
	OnInsert(xid uint32, lsn LSN, reln RelFileLocator, block uint32, offnum uint16, tupleData []byte)

	// OnDelete records a heap delete.
	OnDelete(xid uint32, lsn LSN, reln RelFileLocator, block uint32, offnum uint16)

	// OnUpdate records a heap update (new-image only).
	OnUpdate(xid uint32, lsn LSN, reln RelFileLocator, newBlock uint32, newOffnum uint16, newTupleData []byte)

	// OnCommit signals that xid has committed.  The decoder must emit any
	// buffered changes for xid in WAL order and then discard them.
	OnCommit(xid uint32, lsn LSN)

	// OnAbort signals that xid has aborted.  Buffered changes must be
	// discarded without emitting.
	OnAbort(xid uint32, lsn LSN)
}
