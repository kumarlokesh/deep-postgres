package executor

// Limit stops the tuple stream after at most n tuples.
//
// Mirrors PostgreSQL's Limit executor node
// (src/backend/executor/nodeLimit.c).
type Limit struct {
	input Node
	max   int
	count int
}

// NewLimit wraps input and stops after n tuples.
// n <= 0 means no limit (all tuples are passed through).
func NewLimit(input Node, n int) *Limit {
	return &Limit{input: input, max: n}
}

// Next returns the next tuple from input until the limit is reached.
func (l *Limit) Next() (*ScanTuple, error) {
	if l.max > 0 && l.count >= l.max {
		return nil, nil
	}
	tup, err := l.input.Next()
	if err != nil || tup == nil {
		return tup, err
	}
	l.count++
	return tup, nil
}

// Close delegates to the inner node.
func (l *Limit) Close() { l.input.Close() }
