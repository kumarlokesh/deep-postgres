package wal

// RedoTracer receives events from the WAL redo engine.
//
// The interface follows the same pluggable-observer pattern as
// storage.BufferTracer: implement the interface to collect metrics or build
// replay audit logs; use NoopRedoTracer as a zero-cost default.
//
// All callbacks are invoked synchronously inside RedoEngine.Run, so
// implementations must not call back into the engine.

// RedoTracer observes WAL replay events.
type RedoTracer interface {
	// OnApply is called after a WAL record is successfully dispatched to the
	// resource manager's Redo callback.
	OnApply(lsn LSN, rec *Record)

	// OnSkip is called when a record is skipped — either because the rmgr is
	// unknown or because Redo returned ErrUnimplementedRedo.
	OnSkip(lsn LSN, rec *Record, reason error)

	// OnSegment is called each time the engine loads a new WAL segment.
	// segStart is the LSN of the segment's first byte.
	OnSegment(segStart LSN)
}

// NoopRedoTracer discards all events.
type NoopRedoTracer struct{}

func (NoopRedoTracer) OnApply(LSN, *Record)       {}
func (NoopRedoTracer) OnSkip(LSN, *Record, error) {}
func (NoopRedoTracer) OnSegment(LSN)              {}
