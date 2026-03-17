package storage

// MVCC tuple visibility (src/backend/access/heap/heapam_visibility.c).
//
// PostgreSQL determines whether a heap tuple is visible to a transaction by
// inspecting the tuple's xmin/xmax and comparing them against a Snapshot.
//
// Rules (simplified to the MVCC snapshot case):
//  1. xmin must be committed and visible to the snapshot.
//  2. xmax must be invalid, aborted, or not visible to the snapshot.
//
// Hint bits (HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID, etc.) cache the result
// of transaction status lookups so they aren't repeated on every tuple scan.

// TransactionStatus is the commit state of a transaction.
type TransactionStatus int

const (
	TxInProgress TransactionStatus = iota
	TxCommitted
	TxAborted
)

// TransactionOracle provides transaction status for visibility checks.
// In a real implementation this consults pg_xact (clog).
type TransactionOracle interface {
	Status(xid TransactionId) TransactionStatus
}

// Snapshot represents a consistent point-in-time view of the database.
//
// Matches PostgreSQL's SnapshotData (snapshot.h):
//   - Xmin: all XIDs < Xmin are visible (if committed)
//   - Xmax: all XIDs >= Xmax are invisible
//   - Xip:  in-progress XIDs between Xmin and Xmax (not visible)
//   - Xid:  the current transaction's own XID
type Snapshot struct {
	Xid    TransactionId   // current transaction ID
	Xmin   TransactionId   // oldest active XID at snapshot time
	Xmax   TransactionId   // first as-yet-unassigned XID
	Xip    []TransactionId // in-progress XIDs [Xmin, Xmax)
	CurCid CommandId       // current command ID within transaction
}

// NewSnapshot constructs a snapshot. xip may be nil.
func NewSnapshot(xid, xmin, xmax TransactionId) *Snapshot {
	return &Snapshot{Xid: xid, Xmin: xmin, Xmax: xmax}
}

// AddInProgress marks xid as in-progress in this snapshot.
func (s *Snapshot) AddInProgress(xid TransactionId) {
	for _, x := range s.Xip {
		if x == xid {
			return
		}
	}
	s.Xip = append(s.Xip, xid)
}

// XidVisible reports whether xid is visible in this snapshot.
//
// Logic mirrors XidInMVCCSnapshot() in PostgreSQL:
//   - FrozenTransactionId is always visible.
//   - InvalidTransactionId is never visible.
//   - The current transaction's own XID is visible.
//   - XIDs >= Xmax are invisible (committed after snapshot).
//   - XIDs < Xmin are visible (committed before snapshot).
//   - XIDs in [Xmin, Xmax) are visible unless they appear in Xip.
func (s *Snapshot) XidVisible(xid TransactionId) bool {
	if xid == InvalidTransactionId {
		return false
	}
	if xid == FrozenTransactionId {
		return true
	}
	if xid == s.Xid {
		return true
	}
	if xid >= s.Xmax {
		return false
	}
	if xid < s.Xmin {
		return true
	}
	// In [Xmin, Xmax) — visible only if not in Xip.
	for _, x := range s.Xip {
		if x == xid {
			return false
		}
	}
	return true
}

// VisibilityResult is the outcome of a tuple visibility check.
type VisibilityResult int

const (
	VisVisible       VisibilityResult = iota // tuple visible to snapshot
	VisInvisible                             // inserted by uncommitted/aborted xact
	VisDeleted                               // deleted and deletion is visible
	VisBeingDeleted                          // current xact is deleting it
	VisInsertedBySelf                        // inserted by current xact, visible
	VisDeletedBySelf                         // deleted by current xact
)

// IsVisible reports whether the result means the tuple should be returned.
func (r VisibilityResult) IsVisible() bool {
	return r == VisVisible || r == VisInsertedBySelf
}

// HeapTupleSatisfiesMVCC implements PostgreSQL's HeapTupleSatisfiesMVCC.
//
// It checks whether the tuple described by hdr is visible to snap using
// oracle for transaction status lookups.
func HeapTupleSatisfiesMVCC(hdr *HeapTupleHeader, snap *Snapshot, oracle TransactionOracle) VisibilityResult {
	infomask := hdr.Infomask()

	// ── xmin check ──────────────────────────────────────────────────────────
	if !xminVisible(hdr, snap, oracle, infomask) {
		return VisInvisible
	}

	// ── self-insert check ────────────────────────────────────────────────────
	if hdr.TXmin == snap.Xid {
		if hdr.TCid >= snap.CurCid {
			// Inserted by a later command within the same transaction.
			return VisInvisible
		}
		if hdr.TXmax == InvalidTransactionId || infomask&HeapXmaxInvalid != 0 {
			return VisInsertedBySelf
		}
		if hdr.TXmax == snap.Xid {
			return VisDeletedBySelf
		}
	}

	// ── xmax check ──────────────────────────────────────────────────────────
	return xmaxVisibility(hdr, snap, oracle, infomask)
}

// xminVisible reports whether xmin makes the tuple eligible for visibility.
func xminVisible(hdr *HeapTupleHeader, snap *Snapshot, oracle TransactionOracle, infomask InfomaskFlags) bool {
	xmin := hdr.TXmin
	if xmin == InvalidTransactionId {
		return false
	}
	if xmin == FrozenTransactionId {
		return true
	}

	// Consult hint bits first to avoid a clog lookup.
	if infomask&HeapXminCommitted != 0 {
		return snap.XidVisible(xmin)
	}
	if infomask&HeapXminInvalid != 0 {
		return false
	}

	// No hint bits; ask the oracle.
	switch oracle.Status(xmin) {
	case TxCommitted:
		return snap.XidVisible(xmin)
	case TxAborted:
		return false
	default: // TxInProgress
		return xmin == snap.Xid
	}
}

// xmaxVisibility determines the final visibility result once xmin is known visible.
func xmaxVisibility(hdr *HeapTupleHeader, snap *Snapshot, oracle TransactionOracle, infomask InfomaskFlags) VisibilityResult {
	xmax := hdr.TXmax
	if xmax == InvalidTransactionId || infomask&HeapXmaxInvalid != 0 {
		return VisVisible
	}

	// Hint bit: xmax already committed.
	if infomask&HeapXmaxCommitted != 0 {
		if snap.XidVisible(xmax) {
			return VisDeleted
		}
		return VisVisible
	}

	// Current transaction is the deleter.
	if xmax == snap.Xid {
		return VisBeingDeleted
	}

	switch oracle.Status(xmax) {
	case TxCommitted:
		if snap.XidVisible(xmax) {
			return VisDeleted
		}
		return VisVisible
	case TxAborted:
		return VisVisible
	default: // TxInProgress (another transaction is deleting)
		return VisVisible
	}
}

// ── Additional snapshot types ─────────────────────────────────────────────────

// HeapTupleSatisfiesDirty reports whether a tuple is the latest version of a
// row, regardless of commit status.  Used internally when examining the live
// tuple during an update (PostgreSQL's HeapTupleSatisfiesDirty).
//
// Rules:
//   - If xmin is aborted, the tuple never existed: invisible.
//   - If xmax is absent or aborted, this is the latest version: visible.
//   - Otherwise the tuple has been or is being deleted: invisible.
func HeapTupleSatisfiesDirty(hdr *HeapTupleHeader, oracle TransactionOracle) VisibilityResult {
	xmin := hdr.TXmin
	if xmin == InvalidTransactionId {
		return VisInvisible
	}
	if xmin != FrozenTransactionId && oracle.Status(xmin) == TxAborted {
		return VisInvisible
	}

	xmax := hdr.TXmax
	infomask := hdr.Infomask()
	if xmax == InvalidTransactionId || infomask&HeapXmaxInvalid != 0 {
		return VisVisible
	}
	if oracle.Status(xmax) == TxAborted {
		return VisVisible
	}
	return VisDeleted
}

// HeapTupleSatisfiesNow reports whether a tuple is visible to any reader that
// uses a snapshot taken right now (i.e., it is the most recent committed
// version).  Mirrors PostgreSQL's SnapshotNow semantics: xmin committed, xmax
// either absent or not yet committed.
func HeapTupleSatisfiesNow(hdr *HeapTupleHeader, oracle TransactionOracle) VisibilityResult {
	xmin := hdr.TXmin
	if xmin == InvalidTransactionId {
		return VisInvisible
	}

	infomask := hdr.Infomask()

	// xmin must be committed.
	xminCommitted := false
	if xmin == FrozenTransactionId {
		xminCommitted = true
	} else if infomask&HeapXminCommitted != 0 {
		xminCommitted = true
	} else if infomask&HeapXminInvalid != 0 {
		return VisInvisible
	} else {
		switch oracle.Status(xmin) {
		case TxCommitted:
			xminCommitted = true
		case TxAborted:
			return VisInvisible
		default:
			return VisInvisible // still in progress
		}
	}
	if !xminCommitted {
		return VisInvisible
	}

	// xmax must be absent or not yet committed.
	xmax := hdr.TXmax
	if xmax == InvalidTransactionId || infomask&HeapXmaxInvalid != 0 {
		return VisVisible
	}
	if infomask&HeapXmaxCommitted != 0 {
		return VisDeleted
	}
	switch oracle.Status(xmax) {
	case TxCommitted:
		return VisDeleted
	case TxAborted:
		return VisVisible
	default: // xmax in progress: tuple is being deleted but not yet gone
		return VisVisible
	}
}

// HeapTupleSatisfiesAny always returns VisVisible.  Used by VACUUM and index
// builds that need to see every tuple regardless of transaction state.
// Mirrors PostgreSQL's SnapshotAny.
func HeapTupleSatisfiesAny(_ *HeapTupleHeader) VisibilityResult {
	return VisVisible
}

// SetHintBits updates HEAP_XMIN_COMMITTED / HEAP_XMIN_INVALID and
// HEAP_XMAX_COMMITTED / HEAP_XMAX_INVALID hint bits based on oracle status.
// This should be called after a successful visibility check to avoid repeated
// clog lookups (write-once, then cheap to re-read).
func SetHintBits(hdr *HeapTupleHeader, oracle TransactionOracle) {
	infomask := hdr.Infomask()

	if infomask&HeapXminCommitted == 0 && infomask&HeapXminInvalid == 0 {
		switch oracle.Status(hdr.TXmin) {
		case TxCommitted:
			hdr.TInfomask = uint16(infomask | HeapXminCommitted)
			infomask = hdr.Infomask() // re-read
		case TxAborted:
			hdr.TInfomask = uint16(infomask | HeapXminInvalid)
			infomask = hdr.Infomask()
		}
	}

	if hdr.TXmax != InvalidTransactionId &&
		infomask&HeapXmaxCommitted == 0 && infomask&HeapXmaxInvalid == 0 {
		switch oracle.Status(hdr.TXmax) {
		case TxCommitted:
			hdr.TInfomask = uint16(infomask | HeapXmaxCommitted)
		case TxAborted:
			hdr.TInfomask = uint16(infomask | HeapXmaxInvalid)
		}
	}
}

// ── SimpleTransactionOracle ──────────────────────────────────────────────────

// SimpleTransactionOracle is an in-memory oracle backed by a map.
// Useful for unit tests and single-process experiments.
type SimpleTransactionOracle struct {
	statuses map[TransactionId]TransactionStatus
}

// NewSimpleOracle creates an empty oracle (unknown XIDs default to TxInProgress).
func NewSimpleOracle() *SimpleTransactionOracle {
	return &SimpleTransactionOracle{statuses: make(map[TransactionId]TransactionStatus)}
}

// SetStatus records the status for xid.
func (o *SimpleTransactionOracle) SetStatus(xid TransactionId, s TransactionStatus) {
	o.statuses[xid] = s
}

// Commit records xid as committed.
func (o *SimpleTransactionOracle) Commit(xid TransactionId) { o.SetStatus(xid, TxCommitted) }

// Abort records xid as aborted.
func (o *SimpleTransactionOracle) Abort(xid TransactionId) { o.SetStatus(xid, TxAborted) }

// Status implements TransactionOracle.
func (o *SimpleTransactionOracle) Status(xid TransactionId) TransactionStatus {
	if xid == InvalidTransactionId {
		return TxAborted
	}
	if xid == FrozenTransactionId {
		return TxCommitted
	}
	if s, ok := o.statuses[xid]; ok {
		return s
	}
	return TxInProgress
}
