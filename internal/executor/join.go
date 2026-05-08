package executor

// Join executor nodes: NestedLoopJoin and HashJoin.
//
// Both accept an outer Node (probe side) and an inner Node (build side).
// A user-supplied combine function assembles each output tuple from a matching
// (outer, inner) pair. A nil return from combine drops the row, allowing
// caller-side filtering without an extra Filter wrapper.
//
// PostgreSQL equivalents:
//
//	NestedLoopJoin → nodeNestloop.c  (ExecNestLoop)
//	HashJoin       → nodeHashjoin.c  (ExecHashJoin) + nodeHash.c

// ── NestedLoopJoin ────────────────────────────────────────────────────────────

// NestedLoopJoin materializes the inner side once, then for each outer tuple
// scans the buffered inner side and emits combined rows for every pair where
// pred returns true.
//
// Time complexity: O(outer × inner).
// Preferred by PostgreSQL when the inner side is small or already sorted.
type NestedLoopJoin struct {
	outer   Node
	inner   Node
	pred    func(outer, inner *ScanTuple) bool
	combine func(outer, inner *ScanTuple) *ScanTuple

	innerBuf []*ScanTuple
	outerTup *ScanTuple
	innerPos int
	built    bool
	done     bool
}

// NewNestedLoopJoin wraps outer and inner with pred as the join condition.
// combine assembles the output tuple; returning nil drops the row.
func NewNestedLoopJoin(
	outer, inner Node,
	pred func(outer, inner *ScanTuple) bool,
	combine func(outer, inner *ScanTuple) *ScanTuple,
) *NestedLoopJoin {
	return &NestedLoopJoin{outer: outer, inner: inner, pred: pred, combine: combine}
}

// Next returns the next joined tuple.
// On the first call it materializes the entire inner side; subsequent calls
// advance through (outer × inner) pairs.
func (n *NestedLoopJoin) Next() (*ScanTuple, error) {
	if n.done {
		return nil, nil
	}

	if !n.built {
		for {
			tup, err := n.inner.Next()
			if err != nil {
				return nil, err
			}
			if tup == nil {
				break
			}
			n.innerBuf = append(n.innerBuf, tup)
		}
		n.built = true
		tup, err := n.outer.Next()
		if err != nil {
			return nil, err
		}
		n.outerTup = tup
		n.innerPos = 0
	}

	for n.outerTup != nil {
		for n.innerPos < len(n.innerBuf) {
			innerTup := n.innerBuf[n.innerPos]
			n.innerPos++
			if !n.pred(n.outerTup, innerTup) {
				continue
			}
			if out := n.combine(n.outerTup, innerTup); out != nil {
				return out, nil
			}
		}
		tup, err := n.outer.Next()
		if err != nil {
			return nil, err
		}
		n.outerTup = tup
		n.innerPos = 0
	}

	n.done = true
	return nil, nil
}

// Close releases resources from both child nodes.
func (n *NestedLoopJoin) Close() {
	n.outer.Close()
	n.inner.Close()
}

// ── HashJoin ──────────────────────────────────────────────────────────────────

// HashJoin performs a classic two-phase hash join.
//
//   - Build phase: reads every inner tuple, hashes it with buildKeyFn, and
//     stores it in an in-memory hash table.
//   - Probe phase: for each outer tuple, looks up matching inner rows via
//     probeKeyFn and emits combined output.
//
// Time complexity: O(inner) build + O(outer) probe (hash lookups O(1) avg).
// Preferred by PostgreSQL for larger unsorted inputs (nodeHashjoin.c).
type HashJoin struct {
	outer      Node
	inner      Node
	buildKeyFn func(*ScanTuple) []byte
	probeKeyFn func(*ScanTuple) []byte
	combine    func(outer, inner *ScanTuple) *ScanTuple

	table    map[string][]*ScanTuple
	built    bool
	outerTup *ScanTuple
	matches  []*ScanTuple
	matchPos int
}

// NewHashJoin constructs a HashJoin.
//   - inner is the build side (hashed).
//   - outer is the probe side.
//   - buildKeyFn extracts the join key bytes from an inner tuple.
//   - probeKeyFn extracts the join key bytes from an outer tuple.
//   - combine assembles the output tuple; returning nil drops the row.
func NewHashJoin(
	outer, inner Node,
	buildKeyFn func(*ScanTuple) []byte,
	probeKeyFn func(*ScanTuple) []byte,
	combine func(outer, inner *ScanTuple) *ScanTuple,
) *HashJoin {
	return &HashJoin{
		outer:      outer,
		inner:      inner,
		buildKeyFn: buildKeyFn,
		probeKeyFn: probeKeyFn,
		combine:    combine,
		table:      make(map[string][]*ScanTuple),
	}
}

// Next returns the next joined tuple.
// The first call builds the hash table; subsequent calls probe it.
func (h *HashJoin) Next() (*ScanTuple, error) {
	if !h.built {
		for {
			tup, err := h.inner.Next()
			if err != nil {
				return nil, err
			}
			if tup == nil {
				break
			}
			key := string(h.buildKeyFn(tup))
			h.table[key] = append(h.table[key], tup)
		}
		h.built = true
	}

	for {
		for h.matchPos < len(h.matches) {
			innerTup := h.matches[h.matchPos]
			h.matchPos++
			if out := h.combine(h.outerTup, innerTup); out != nil {
				return out, nil
			}
		}

		tup, err := h.outer.Next()
		if err != nil {
			return nil, err
		}
		if tup == nil {
			return nil, nil
		}
		h.outerTup = tup
		key := string(h.probeKeyFn(tup))
		h.matches = h.table[key]
		h.matchPos = 0
	}
}

// Close releases resources from both child nodes.
func (h *HashJoin) Close() {
	h.outer.Close()
	h.inner.Close()
}
