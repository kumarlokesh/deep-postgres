package wal

// Resource manager (rmgr) dispatch table.
//
// Every WAL record belongs to a resource manager identified by xl_rmid.  On
// crash recovery, the redo engine calls the correct rmgr's Redo function with
// the decoded record.  On logical decoding, the Decode function is called
// instead (optional — set to nil for rmgrs that don't support it).
//
// This mirrors PostgreSQL's RmgrData table (src/include/access/rmgr.h and
// src/backend/access/transam/rmgr.c).

import "fmt"

// PageWriter is the storage interface used by rmgr Redo callbacks.
//
// It abstracts the buffer pool so the WAL package does not import the storage
// package directly.  The storage package provides a concrete implementation
// (WalPageStore) that the caller wires in via RedoEngine.SetStore.
//
// Each method is idempotent: if the target page's LSN is already >= recLSN
// the change has been applied and the method must be a no-op.
type PageWriter interface {
	// ApplyFPW restores a full-page write image to the given block.
	ApplyFPW(loc RelFileLocator, fork ForkNum, block uint32, img *BlockImage, recLSN LSN) error

	// ApplyInsert places tupleData at offnum (1-based) on the given block.
	// If initPage is true the page is initialised first.
	ApplyInsert(loc RelFileLocator, fork ForkNum, block uint32, offnum uint16, tupleData []byte, initPage bool, recLSN LSN) error

	// ApplyDelete marks the item at offnum (1-based) on the given block as dead.
	ApplyDelete(loc RelFileLocator, fork ForkNum, block uint32, offnum uint16, recLSN LSN) error

	// ApplyBtreeInsert inserts an index tuple at offnum (1-based) on the given
	// B-tree page, maintaining the sorted item-pointer array.
	// If initPage is true the page is initialised as a new B-tree page first
	// (leaf when isLeaf is true, internal otherwise).
	ApplyBtreeInsert(loc RelFileLocator, fork ForkNum, block uint32, offnum uint16, tupleData []byte, initPage, isLeaf bool, recLSN LSN) error

	// ApplyBtreeSplit initialises a new right B-tree page that results from a
	// page split, populating it with the entries in rightItems and updating the
	// sibling chain pointers (left↔right, right↔oldRight).
	//
	// leftBlock  — block number of the left page (already updated by caller)
	// rightBlock — block number of the new right page (this block)
	// oldRight   — former right sibling of left, InvalidBlockNumber if none
	// level      — B-tree level (0 = leaf)
	// isLeaf     — true if the right page is a leaf
	// rightItems — raw index-tuple bytes to place on the right page (in order)
	ApplyBtreeSplit(loc RelFileLocator, fork ForkNum, rightBlock, leftBlock, oldRight uint32, level uint32, isLeaf bool, rightItems []byte, recLSN LSN) error
}

// RedoContext carries the per-record context passed to RmgrOps.Redo.
type RedoContext struct {
	// Rec is the record being replayed.
	Rec *Record
	// LSN is the start LSN of Rec.
	LSN LSN
	// Store is the optional storage layer for page application.
	// Nil when the engine is run without a storage backend (e.g. unit tests).
	Store PageWriter
	// Logical is the optional logical decoding layer.
	// Nil when logical decoding is not enabled.
	Logical LogicalDecoder
}

// RmgrOps is the set of callbacks registered by each resource manager.
type RmgrOps struct {
	// Name is a human-readable identifier (for diagnostics).
	Name string

	// Redo applies the record during crash recovery / WAL replay.
	// A nil Redo means the rmgr is recognised but not yet implemented.
	Redo func(ctx RedoContext) error

	// Identify returns a one-line description of the record for WAL introspection
	// tools (equivalent to pg_walinspect / pg_waldump output).
	// A nil Identify means the rmgr is recognised but not yet described.
	Identify func(rec *Record) string
}

// registry maps RmgrID to its registered ops.
// Populated by Register; read by Dispatch.
var registry [RmgrMax]*RmgrOps

// Register installs ops for id.  Panics on duplicate registration (catches
// wiring errors at init time, identical to PostgreSQL's behaviour when a
// module registers the same rmgr twice).
func Register(id RmgrID, ops RmgrOps) {
	if id >= RmgrMax {
		panic(fmt.Sprintf("wal: RmgrID %d out of range", id))
	}
	if registry[id] != nil {
		panic(fmt.Sprintf("wal: duplicate registration for rmgr %s (%d)", ops.Name, id))
	}
	registry[id] = &ops
}

// Lookup returns the ops for id, or nil if not registered.
func Lookup(id RmgrID) *RmgrOps {
	if id >= RmgrMax {
		return nil
	}
	return registry[id]
}

// ── Redo engine ───────────────────────────────────────────────────────────────

// ErrUnknownRmgr is returned when a WAL record references an rmgr with no
// registered ops.
type ErrUnknownRmgr struct{ ID RmgrID }

func (e ErrUnknownRmgr) Error() string {
	return fmt.Sprintf("wal: unknown resource manager %d", e.ID)
}

// ErrUnimplementedRedo is returned when a record's rmgr has registered ops
// but has not yet implemented Redo.
type ErrUnimplementedRedo struct{ RmgrName string }

func (e ErrUnimplementedRedo) Error() string {
	return fmt.Sprintf("wal: redo not implemented for rmgr %s", e.RmgrName)
}

// Dispatch calls the Redo callback for the resource manager referenced by ctx.Rec.
func Dispatch(ctx RedoContext) error {
	ops := Lookup(ctx.Rec.Header.XlRmid)
	if ops == nil {
		return ErrUnknownRmgr{ID: ctx.Rec.Header.XlRmid}
	}
	if ops.Redo == nil {
		return ErrUnimplementedRedo{RmgrName: ops.Name}
	}
	return ops.Redo(ctx)
}

// Describe returns a human-readable description of a WAL record.
// Falls back to a generic format if the rmgr has no Identify callback.
func Describe(rec *Record) string {
	ops := Lookup(rec.Header.XlRmid)
	if ops != nil && ops.Identify != nil {
		return ops.Identify(rec)
	}
	return rec.String()
}
