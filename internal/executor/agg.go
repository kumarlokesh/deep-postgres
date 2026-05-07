package executor

import (
	"encoding/binary"
	"math"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// HashAgg implements a hash-based GROUP BY aggregation.
//
// Mirrors PostgreSQL's Agg executor node with AGG_HASHED strategy
// (src/backend/executor/nodeAgg.c).  Each distinct key produced by keyFn
// gets its own set of AggFn instances.  All input is consumed on the first
// Next call; subsequent calls stream groups one at a time.
//
// Output tuple layout:
//
//	Data = keyFn(row) bytes
//	       ++ AggFn[0].Result() bytes
//	       ++ AggFn[1].Result() bytes …
//
// Block and Offset are both 0 (no heap location — this is a computed row).
//
// Usage:
//
//	ha := NewHashAgg(
//	    scan,
//	    func(t *ScanTuple) []byte { return t.Tuple.Data[:4] }, // group key
//	    func() []AggFn { return []AggFn{NewCountAgg(), NewSumAgg(extractScore)} },
//	)
type HashAgg struct {
	input  Node
	keyFn  func(*ScanTuple) []byte
	newAgg func() []AggFn // factory for per-group aggregator set

	groups map[string][]AggFn // encoded key → aggregator set
	order  []string           // insertion order (deterministic output)
	pos    int
	done   bool
}

// NewHashAgg returns a HashAgg node.
//
//   - keyFn extracts the group key bytes from each input tuple.
//   - newAgg is called once per distinct key to create a fresh aggregator set.
func NewHashAgg(input Node, keyFn func(*ScanTuple) []byte, newAgg func() []AggFn) *HashAgg {
	return &HashAgg{
		input:  input,
		keyFn:  keyFn,
		newAgg: newAgg,
		groups: make(map[string][]AggFn),
	}
}

// Next returns the next aggregated group tuple.
// The first call materializes the entire input and computes all aggregates.
func (h *HashAgg) Next() (*ScanTuple, error) {
	if !h.done {
		if err := h.build(); err != nil {
			return nil, err
		}
	}
	if h.pos >= len(h.order) {
		return nil, nil
	}
	key := h.order[h.pos]
	h.pos++
	return h.makeTuple([]byte(key), h.groups[key]), nil
}

// Close delegates to the inner node.
func (h *HashAgg) Close() { h.input.Close() }

func (h *HashAgg) build() error {
	h.done = true
	for {
		tup, err := h.input.Next()
		if err != nil {
			return err
		}
		if tup == nil {
			break
		}
		key := string(h.keyFn(tup))
		aggs, ok := h.groups[key]
		if !ok {
			aggs = h.newAgg()
			h.groups[key] = aggs
			h.order = append(h.order, key)
		}
		for _, a := range aggs {
			a.Update(tup)
		}
	}
	return nil
}

func (h *HashAgg) makeTuple(key []byte, aggs []AggFn) *ScanTuple {
	data := make([]byte, 0, len(key)+len(aggs)*8)
	data = append(data, key...)
	for _, a := range aggs {
		data = append(data, a.Result()...)
	}
	tup := storage.NewHeapTuple(storage.FrozenTransactionId, 0, data)
	return &ScanTuple{Tuple: tup}
}

// ── AggFn interface and built-in implementations ──────────────────────────────

// AggFn is an incremental aggregation function.
// Each distinct group gets its own AggFn instance via the newAgg factory.
//
// Mirrors a simplified PostgreSQL AggStatePerTrans with transfn + finalfn.
type AggFn interface {
	// Update folds one more input tuple into the running state.
	Update(tup *ScanTuple)
	// Result encodes the final value as a fixed-width byte slice.
	Result() []byte
}

// ── COUNT(*) ─────────────────────────────────────────────────────────────────

// CountAgg counts the number of input tuples per group.
// Result is an 8-byte big-endian int64.
type CountAgg struct{ n int64 }

func NewCountAgg() *CountAgg { return &CountAgg{} }

func (a *CountAgg) Update(_ *ScanTuple) { a.n++ }

func (a *CountAgg) Result() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(a.n))
	return b
}

// DecodeCount reads a CountAgg Result slice.
func DecodeCount(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

// ── SUM(int64) ────────────────────────────────────────────────────────────────

// SumAgg computes the sum of a user-supplied int64 extraction function.
// Result is an 8-byte big-endian int64.
type SumAgg struct {
	extractFn func(*ScanTuple) int64
	sum       int64
}

func NewSumAgg(extractFn func(*ScanTuple) int64) *SumAgg {
	return &SumAgg{extractFn: extractFn}
}

func (a *SumAgg) Update(tup *ScanTuple) { a.sum += a.extractFn(tup) }

func (a *SumAgg) Result() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(a.sum))
	return b
}

// DecodeSum reads a SumAgg Result slice.
func DecodeSum(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

// ── MIN(int64) ────────────────────────────────────────────────────────────────

// MinAgg tracks the minimum of a user-supplied int64 extraction function.
// Result is an 8-byte big-endian int64.
type MinAgg struct {
	extractFn func(*ScanTuple) int64
	val       int64
	set       bool
}

func NewMinAgg(extractFn func(*ScanTuple) int64) *MinAgg {
	return &MinAgg{extractFn: extractFn, val: math.MaxInt64}
}

func (a *MinAgg) Update(tup *ScanTuple) {
	v := a.extractFn(tup)
	if !a.set || v < a.val {
		a.val = v
		a.set = true
	}
}

func (a *MinAgg) Result() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(a.val))
	return b
}

// DecodeMin reads a MinAgg Result slice.
func DecodeMin(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

// ── MAX(int64) ────────────────────────────────────────────────────────────────

// MaxAgg tracks the maximum of a user-supplied int64 extraction function.
// Result is an 8-byte big-endian int64.
type MaxAgg struct {
	extractFn func(*ScanTuple) int64
	val       int64
	set       bool
}

func NewMaxAgg(extractFn func(*ScanTuple) int64) *MaxAgg {
	return &MaxAgg{extractFn: extractFn, val: math.MinInt64}
}

func (a *MaxAgg) Update(tup *ScanTuple) {
	v := a.extractFn(tup)
	if !a.set || v > a.val {
		a.val = v
		a.set = true
	}
}

func (a *MaxAgg) Result() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(a.val))
	return b
}

// DecodeMax reads a MaxAgg Result slice.
func DecodeMax(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }
