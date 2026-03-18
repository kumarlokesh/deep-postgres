package wal

// ReorderBuffer implements LogicalDecoder by buffering row changes per
// transaction and emitting them in WAL order only after a COMMIT is received.
//
// PostgreSQL's ReorderBuffer (src/backend/replication/reorderbuffer.c) serves
// the same role.  Ours is a stripped-down in-memory version; the production
// one spills to disk when a transaction's footprint exceeds
// logical_decoding_work_mem.
//
// Ordering guarantee:
//   Within a transaction, changes are emitted in the order they were appended
//   (= WAL LSN order).  Across transactions, the caller's ChangeHandler is
//   invoked in commit-LSN order because we process WAL strictly sequentially.
//
// Thread-safety: single-threaded — same as the rest of this package.

// txBuf holds the uncommitted changes for one transaction.
type txBuf struct {
	changes []Change
}

// ReorderBuffer implements LogicalDecoder and accumulates changes until commit.
type ReorderBuffer struct {
	txns    map[uint32]*txBuf // xid → buffered changes
	handler ChangeHandler     // called on COMMIT with the ordered change list
}

// NewReorderBuffer creates a buffer that invokes handler on each committed
// transaction.  handler may be nil (changes are decoded but silently dropped).
func NewReorderBuffer(handler ChangeHandler) *ReorderBuffer {
	return &ReorderBuffer{
		txns:    make(map[uint32]*txBuf),
		handler: handler,
	}
}

// ── LogicalDecoder implementation ─────────────────────────────────────────────

func (rb *ReorderBuffer) OnInsert(xid uint32, lsn LSN, reln RelFileLocator, block uint32, offnum uint16, tupleData []byte) {
	rb.append(xid, Change{
		Type:      ChangeInsert,
		XID:       xid,
		LSN:       lsn,
		Reln:      reln,
		Block:     block,
		Offnum:    offnum,
		TupleData: tupleData,
	})
}

func (rb *ReorderBuffer) OnDelete(xid uint32, lsn LSN, reln RelFileLocator, block uint32, offnum uint16) {
	rb.append(xid, Change{
		Type:   ChangeDelete,
		XID:    xid,
		LSN:    lsn,
		Reln:   reln,
		Block:  block,
		Offnum: offnum,
	})
}

func (rb *ReorderBuffer) OnUpdate(xid uint32, lsn LSN, reln RelFileLocator, newBlock uint32, newOffnum uint16, newTupleData []byte) {
	rb.append(xid, Change{
		Type:      ChangeUpdate,
		XID:       xid,
		LSN:       lsn,
		Reln:      reln,
		Block:     newBlock,
		Offnum:    newOffnum,
		TupleData: newTupleData,
	})
}

func (rb *ReorderBuffer) OnCommit(xid uint32, commitLSN LSN) {
	buf, ok := rb.txns[xid]
	if !ok {
		// Empty transaction (no data changes) or already seen.  No-op.
		return
	}
	if rb.handler != nil {
		rb.handler(xid, commitLSN, buf.changes)
	}
	delete(rb.txns, xid)
}

func (rb *ReorderBuffer) OnAbort(xid uint32, _ LSN) {
	delete(rb.txns, xid)
}

// ── Diagnostics ───────────────────────────────────────────────────────────────

// PendingCount returns the number of transactions with buffered but uncommitted
// changes.  Useful for tests and monitoring.
func (rb *ReorderBuffer) PendingCount() int { return len(rb.txns) }

// PendingChanges returns the buffered change list for xid, or nil if xid is
// not known.  Intended for unit tests only; do not modify the returned slice.
func (rb *ReorderBuffer) PendingChanges(xid uint32) []Change {
	if buf, ok := rb.txns[xid]; ok {
		return buf.changes
	}
	return nil
}

// ── internal ──────────────────────────────────────────────────────────────────

func (rb *ReorderBuffer) append(xid uint32, c Change) {
	buf, ok := rb.txns[xid]
	if !ok {
		buf = &txBuf{}
		rb.txns[xid] = buf
	}
	buf.changes = append(buf.changes, c)
}
