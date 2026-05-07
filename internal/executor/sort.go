package executor

import "sort"

// Sort materializes every tuple from its child node, sorts them in memory,
// and yields them in order one at a time.
//
// Mirrors PostgreSQL's Sort executor node (src/backend/executor/nodeSort.c).
// PostgreSQL uses a two-phase merge sort when the result set exceeds
// work_mem; this implementation is in-memory only (suitable for moderate
// result sets).
//
// Usage:
//
//	s := NewSort(scan, func(a, b *ScanTuple) bool {
//	    return bytes.Compare(a.Tuple.Data[:4], b.Tuple.Data[:4]) < 0
//	})
//	for tup, err := s.Next(); tup != nil; tup, err = s.Next() { ... }
type Sort struct {
	input  Node
	less   func(a, b *ScanTuple) bool
	buf    []*ScanTuple
	pos    int
	loaded bool
}

// NewSort wraps input and sorts its output using the less comparator.
// Sorting is deferred until the first Next call (lazy materialization).
func NewSort(input Node, less func(a, b *ScanTuple) bool) *Sort {
	return &Sort{input: input, less: less}
}

// Next returns the next tuple in sorted order.
// On the first call it drains and sorts the entire input; subsequent calls
// stream from the in-memory buffer.
func (s *Sort) Next() (*ScanTuple, error) {
	if !s.loaded {
		if err := s.load(); err != nil {
			return nil, err
		}
	}
	if s.pos >= len(s.buf) {
		return nil, nil
	}
	tup := s.buf[s.pos]
	s.pos++
	return tup, nil
}

// Close delegates to the inner node.
func (s *Sort) Close() { s.input.Close() }

func (s *Sort) load() error {
	s.loaded = true
	for {
		tup, err := s.input.Next()
		if err != nil {
			return err
		}
		if tup == nil {
			break
		}
		s.buf = append(s.buf, tup)
	}
	sort.SliceStable(s.buf, func(i, j int) bool {
		return s.less(s.buf[i], s.buf[j])
	})
	return nil
}
