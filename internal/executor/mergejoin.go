package executor

import "bytes"

// MergeJoin implements a sort-merge join.
//
// Both inputs must arrive pre-sorted on their respective join keys.
// The algorithm does a single coordinated scan of both sides:
//   - if outerKey < innerKey: advance outer
//   - if outerKey > innerKey: advance inner
//   - if outerKey == innerKey: emit the cross product of all matching rows
//
// For many-to-many groups the inner group is buffered once and replayed for
// each successive outer tuple that shares the same key.
//
// Time complexity: O(outer + inner) scan + O(inner-group) buffer per key group.
// Preferred by PostgreSQL when both inputs are already sorted
// (e.g. from an IndexScan or an upstream Sort node).
//
// PostgreSQL equivalent: nodeMergejoin.c (ExecMergeJoin).
type MergeJoin struct {
	outer      Node
	inner      Node
	outerKeyFn func(*ScanTuple) []byte
	innerKeyFn func(*ScanTuple) []byte
	combine    func(outer, inner *ScanTuple) *ScanTuple

	outerTup    *ScanTuple
	innerGroup  []*ScanTuple // all inner tuples sharing the current match key
	innerPos    int
	innerPeeked *ScanTuple // first tuple of the next inner group (already fetched)
	started     bool
	done        bool
}

// NewMergeJoin constructs a MergeJoin node.
//
//   - Both outer and inner must be pre-sorted by their join key.
//   - outerKeyFn / innerKeyFn extract the join key bytes from each side.
//   - combine assembles the output tuple; returning nil drops the row.
func NewMergeJoin(
	outer, inner Node,
	outerKeyFn func(*ScanTuple) []byte,
	innerKeyFn func(*ScanTuple) []byte,
	combine func(outer, inner *ScanTuple) *ScanTuple,
) *MergeJoin {
	return &MergeJoin{
		outer:      outer,
		inner:      inner,
		outerKeyFn: outerKeyFn,
		innerKeyFn: innerKeyFn,
		combine:    combine,
	}
}

// Next returns the next joined tuple.
//
// On the first call it fetches the first tuple from both sides and positions
// the merge. Subsequent calls advance through matching groups.
func (m *MergeJoin) Next() (*ScanTuple, error) {
	if m.done {
		return nil, nil
	}

	if !m.started {
		m.started = true
		var err error
		m.outerTup, err = m.outer.Next()
		if err != nil {
			return nil, err
		}
		m.innerPeeked, err = m.inner.Next()
		if err != nil {
			return nil, err
		}
		if m.outerTup == nil {
			m.done = true
			return nil, nil
		}
		if err := m.rebuildInnerGroup(); err != nil {
			return nil, err
		}
	}

	for {
		// Drain inner group for the current outer tuple.
		if m.outerTup != nil && m.innerPos < len(m.innerGroup) {
			innerTup := m.innerGroup[m.innerPos]
			m.innerPos++
			if out := m.combine(m.outerTup, innerTup); out != nil {
				return out, nil
			}
			continue
		}

		if m.outerTup == nil {
			m.done = true
			return nil, nil
		}

		// Advance outer.
		nextOuter, err := m.outer.Next()
		if err != nil {
			return nil, err
		}

		// If the new outer row has the same key, replay the buffered inner group.
		if nextOuter != nil && len(m.innerGroup) > 0 {
			if bytes.Equal(m.outerKeyFn(nextOuter), m.innerKeyFn(m.innerGroup[0])) {
				m.outerTup = nextOuter
				m.innerPos = 0
				continue
			}
		}

		m.outerTup = nextOuter
		if m.outerTup == nil {
			m.done = true
			return nil, nil
		}

		if err := m.rebuildInnerGroup(); err != nil {
			return nil, err
		}
	}
}

// rebuildInnerGroup advances the inner side until it aligns with outerTup,
// then buffers all inner tuples that share that key.
func (m *MergeJoin) rebuildInnerGroup() error {
	m.innerGroup = nil
	m.innerPos = 0
	oKey := m.outerKeyFn(m.outerTup)

	// Skip inner tuples that are less than the current outer key.
	for m.innerPeeked != nil {
		cmp := bytes.Compare(oKey, m.innerKeyFn(m.innerPeeked))
		if cmp <= 0 {
			break
		}
		var err error
		m.innerPeeked, err = m.inner.Next()
		if err != nil {
			return err
		}
	}

	if m.innerPeeked == nil {
		return nil // inner exhausted
	}

	// No match for this outer key (outer < inner).
	iKey := m.innerKeyFn(m.innerPeeked)
	if !bytes.Equal(oKey, iKey) {
		return nil
	}

	// Collect all inner tuples that share iKey.
	m.innerGroup = append(m.innerGroup, m.innerPeeked)
	for {
		next, err := m.inner.Next()
		if err != nil {
			return err
		}
		if next == nil {
			m.innerPeeked = nil
			break
		}
		if !bytes.Equal(iKey, m.innerKeyFn(next)) {
			m.innerPeeked = next
			break
		}
		m.innerGroup = append(m.innerGroup, next)
	}
	return nil
}

// Close releases resources from both child nodes.
func (m *MergeJoin) Close() {
	m.outer.Close()
	m.inner.Close()
}
