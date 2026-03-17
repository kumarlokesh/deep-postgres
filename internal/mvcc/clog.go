package mvcc

// Clog is an in-memory commit log (mirrors PostgreSQL's pg_xact / CLOG).
//
// PostgreSQL stores 2 bits per transaction (IN_PROGRESS=00, COMMITTED=01,
// ABORTED=10, SUBCOMMITTED=11) in 8 KB pages under $PGDATA/pg_xact.  This
// implementation uses a plain slice of TransactionStatus values for clarity;
// the indexing and growth semantics are identical to the real thing.
//
// Special XIDs handled outside the log:
//   - InvalidTransactionId (0): always TxAborted (never committed)
//   - FrozenTransactionId  (2): always TxCommitted (visible to all)

import "github.com/kumarlokesh/deep-postgres/internal/storage"

const clogInitialCap = 64 // slots pre-allocated on creation

// Clog records the commit/abort status of every transaction by XID.
type Clog struct {
	statuses []storage.TransactionStatus // index = XID
}

// NewClog creates an empty commit log.
func NewClog() *Clog {
	return &Clog{statuses: make([]storage.TransactionStatus, clogInitialCap)}
}

// Status returns the recorded status for xid.
// Unknown XIDs (never written) are treated as TxInProgress.
func (c *Clog) Status(xid storage.TransactionId) storage.TransactionStatus {
	switch xid {
	case storage.InvalidTransactionId:
		return storage.TxAborted
	case storage.FrozenTransactionId:
		return storage.TxCommitted
	}
	if int(xid) >= len(c.statuses) {
		return storage.TxInProgress
	}
	return c.statuses[xid]
}

// SetStatus records status for xid, growing the backing slice as needed.
func (c *Clog) SetStatus(xid storage.TransactionId, s storage.TransactionStatus) {
	c.grow(xid)
	c.statuses[xid] = s
}

// grow ensures the slice can hold index xid.
func (c *Clog) grow(xid storage.TransactionId) {
	need := int(xid) + 1
	if need <= len(c.statuses) {
		return
	}
	// Double until large enough.
	newCap := len(c.statuses) * 2
	if newCap < need {
		newCap = need
	}
	next := make([]storage.TransactionStatus, newCap)
	copy(next, c.statuses)
	c.statuses = next
}
