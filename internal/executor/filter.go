package executor

// Filter is an executor node that passes tuples through a predicate.
//
// Mirrors PostgreSQL's qual evaluation in ExecQual
// (src/backend/executor/execExpr.c) - the distinction between the scan
// returning a tuple and the qual deciding whether to project it upward.
type Filter struct {
	input Node
	pred  func(*ScanTuple) bool
}

// NewFilter wraps input and discards any tuple for which pred returns false.
// pred must not be nil.
func NewFilter(input Node, pred func(*ScanTuple) bool) *Filter {
	return &Filter{input: input, pred: pred}
}

// Next returns the next tuple from input that satisfies pred.
func (f *Filter) Next() (*ScanTuple, error) {
	for {
		tup, err := f.input.Next()
		if err != nil || tup == nil {
			return tup, err
		}
		if f.pred(tup) {
			return tup, nil
		}
	}
}

// Close delegates to the inner node.
func (f *Filter) Close() { f.input.Close() }
