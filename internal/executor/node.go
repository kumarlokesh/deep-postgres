package executor

// Node is the Volcano-model iterator interface implemented by every executor
// node. Each call to Next produces one tuple; nil signals EOF.
//
// Mirrors PostgreSQL's ExecProcNode dispatch in
// src/backend/executor/execProcnode.c.
type Node interface {
	// Next returns the next tuple from this node, or nil if the stream is
	// exhausted. An error is returned only for I/O or internal failures;
	// EOF is signalled by (nil, nil).
	Next() (*ScanTuple, error)

	// Close releases any resources held by the node. It is safe to call
	// Close on a node that has already been closed or has not produced any
	// tuples.
	Close()
}

// TracePhase identifies the point in a node's lifecycle at which a trace
// event was emitted.
type TracePhase int

const (
	// TraceOpen is emitted the first time Next is called on a TracedNode.
	TraceOpen TracePhase = iota
	// TraceTuple is emitted for each non-nil tuple returned.
	TraceTuple
	// TraceClose is emitted when Close is called.
	TraceClose
)

func (p TracePhase) String() string {
	switch p {
	case TraceOpen:
		return "open"
	case TraceTuple:
		return "tuple"
	case TraceClose:
		return "close"
	default:
		return "unknown"
	}
}

// TraceEvent carries the context emitted by a TracedNode at each lifecycle
// point. Mirrors the instrumentation fields in PostgreSQL's
// InstrumentationState (src/include/executor/instrument.h).
type TraceEvent struct {
	// NodeName is the label passed to NewTracedNode.
	NodeName string
	// Phase is the lifecycle point at which the event was emitted.
	Phase TracePhase
	// Tuple is the tuple returned on this call (nil for Open/Close events).
	Tuple *ScanTuple
	// Seq is the 1-based count of tuples returned so far by this node.
	// It is 0 for Open and Close events.
	Seq int
}

// Tracer is called by TracedNode on every lifecycle event.
// Implementations must not retain the *TraceEvent after the call returns.
type Tracer func(TraceEvent)

// TracedNode wraps any Node and fires a Tracer on every lifecycle event.
// It implements Node itself, so wrappers can be stacked.
//
// Corresponds to PostgreSQL's per-node InstrStartNode / InstrStopNode
// instrumentation calls in execProcnode.c.
type TracedNode struct {
	inner    Node
	name     string
	tracer   Tracer
	seq      int
	opened   bool
}

// NewTracedNode wraps inner and emits trace events via tracer.
// name labels the node in trace output (e.g. "SeqScan", "Filter").
func NewTracedNode(inner Node, name string, tracer Tracer) *TracedNode {
	return &TracedNode{inner: inner, name: name, tracer: tracer}
}

// Next emits a TraceOpen event on the first call, delegates to the inner
// node, and emits a TraceTuple event for each non-nil tuple.
func (t *TracedNode) Next() (*ScanTuple, error) {
	if !t.opened {
		t.opened = true
		t.tracer(TraceEvent{NodeName: t.name, Phase: TraceOpen})
	}
	tup, err := t.inner.Next()
	if err != nil {
		return nil, err
	}
	if tup != nil {
		t.seq++
		t.tracer(TraceEvent{NodeName: t.name, Phase: TraceTuple, Tuple: tup, Seq: t.seq})
	}
	return tup, nil
}

// Close emits a TraceClose event and delegates to the inner node.
func (t *TracedNode) Close() {
	t.tracer(TraceEvent{NodeName: t.name, Phase: TraceClose, Seq: t.seq})
	t.inner.Close()
}

// Collect drains node until EOF and returns all tuples.
// It closes the node before returning.
func Collect(node Node) ([]*ScanTuple, error) {
	defer node.Close()
	var out []*ScanTuple
	for {
		tup, err := node.Next()
		if err != nil {
			return nil, err
		}
		if tup == nil {
			return out, nil
		}
		out = append(out, tup)
	}
}
