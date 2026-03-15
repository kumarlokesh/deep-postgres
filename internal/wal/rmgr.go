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

// RedoContext carries the per-record context passed to RmgrOps.Redo.
// It will be extended in later milestones with a buffer manager reference,
// transaction map, etc.
type RedoContext struct {
	// Rec is the record being replayed.
	Rec *Record
	// LSN is the start LSN of Rec.
	LSN LSN
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

// Dispatch calls the Redo callback for the resource manager referenced by rec.
func Dispatch(rec *Record) error {
	ops := Lookup(rec.Header.XlRmid)
	if ops == nil {
		return ErrUnknownRmgr{ID: rec.Header.XlRmid}
	}
	if ops.Redo == nil {
		return ErrUnimplementedRedo{RmgrName: ops.Name}
	}
	return ops.Redo(RedoContext{Rec: rec, LSN: rec.LSN})
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
