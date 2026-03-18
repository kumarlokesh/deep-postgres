package wal

// Transaction resource manager (RmgrXact).
//
// Handles XLOG_XACT_COMMIT and XLOG_XACT_ABORT WAL records.
//
// For physical redo these are no-ops (crash recovery uses the commit log, not
// WAL commit records, to determine visibility).  For logical decoding they are
// the trigger to flush (or discard) a transaction's buffered changes.
//
// Mirrors src/backend/access/transam/xact.c — xact_redo().
//
// xl_info constants (xact.h):
//   XLOG_XACT_COMMIT           0x00
//   XLOG_XACT_PREPARE          0x10
//   XLOG_XACT_ABORT            0x20
//   XLOG_XACT_COMMIT_PREPARED  0x30
//   XLOG_XACT_ABORT_PREPARED   0x40
//   XLOG_XACT_ASSIGNMENT       0x50 (subtransaction)
//   XLOG_XACT_INVALIDATIONS    0x60

import "fmt"

const (
	xlXactCommit         uint8 = 0x00
	xlXactPrepare        uint8 = 0x10
	xlXactAbort          uint8 = 0x20
	xlXactCommitPrepared uint8 = 0x30
	xlXactAbortPrepared  uint8 = 0x40
	xlXactAssignment     uint8 = 0x50
	xlXactInvalidations  uint8 = 0x60

	xlXactOpMask uint8 = 0xF0
)

func xactRedo(ctx RedoContext) error {
	op := ctx.Rec.Header.XlInfo & xlXactOpMask
	xid := ctx.Rec.Header.XlXid

	switch op {
	case xlXactCommit, xlXactCommitPrepared:
		// Physical redo: no-op (commit log is updated separately).
		// Logical decoding: emit buffered changes for this XID.
		if ctx.Logical != nil {
			ctx.Logical.OnCommit(xid, ctx.LSN)
		}
	case xlXactAbort, xlXactAbortPrepared:
		// Physical redo: no-op.
		// Logical decoding: discard buffered changes.
		if ctx.Logical != nil {
			ctx.Logical.OnAbort(xid, ctx.LSN)
		}
	case xlXactPrepare, xlXactAssignment, xlXactInvalidations:
		// Not yet implemented.
	default:
		// Unknown sub-type; ignore.
	}
	return nil
}

func init() {
	Register(RmgrXact, RmgrOps{
		Name: "Transaction",
		Redo: xactRedo,
		Identify: func(rec *Record) string {
			op := rec.Header.XlInfo & xlXactOpMask
			opName := map[uint8]string{
				xlXactCommit:         "COMMIT",
				xlXactPrepare:        "PREPARE",
				xlXactAbort:          "ABORT",
				xlXactCommitPrepared: "COMMIT_PREPARED",
				xlXactAbortPrepared:  "ABORT_PREPARED",
				xlXactAssignment:     "ASSIGNMENT",
				xlXactInvalidations:  "INVALIDATIONS",
			}[op]
			if opName == "" {
				opName = fmt.Sprintf("op=0x%02X", op)
			}
			return fmt.Sprintf("Transaction/%s lsn=%s xid=%d", opName, rec.LSN, rec.Header.XlXid)
		},
	})
}
