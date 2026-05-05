package executor

// Project applies a tuple transformation at each row in the pipeline.
//
// Mirrors PostgreSQL's ExecProject / TupleTableSlot mechanism
// (src/backend/executor/execTuples.c). The projection function receives the
// full ScanTuple from the child node and returns a new ScanTuple carrying only
// the fields the parent cares about - analogous to the tlist (target list) in
// a PostgreSQL Plan node.
//
// The Block and Offset fields are preserved from the input so that the
// projected tuple can still be used to form a heap TID for UPDATE/DELETE.

// ProjectFn transforms one input tuple into one output tuple.
// Return nil, nil to drop the tuple (acts as an additional filter).
// Return an error only for unrecoverable failures.
type ProjectFn func(*ScanTuple) (*ScanTuple, error)

// Project wraps a Node and applies fn to every tuple it yields.
type Project struct {
	input Node
	fn    ProjectFn
}

// NewProject wraps input and transforms each tuple with fn.
func NewProject(input Node, fn ProjectFn) *Project {
	return &Project{input: input, fn: fn}
}

// Next returns the next projected tuple. Tuples for which fn returns nil are
// silently skipped (useful for computed-column expressions that can fail to
// produce a value).
func (p *Project) Next() (*ScanTuple, error) {
	for {
		tup, err := p.input.Next()
		if err != nil || tup == nil {
			return tup, err
		}
		out, err := p.fn(tup)
		if err != nil {
			return nil, err
		}
		if out != nil {
			return out, nil
		}
	}
}

// Close delegates to the inner node.
func (p *Project) Close() { p.input.Close() }

// ── Schema-based projection ───────────────────────────────────────────────────

// Column describes one fixed-width attribute within a tuple's Data bytes.
// This mirrors a simplified version of PostgreSQL's Form_pg_attribute
// (pg_attribute.h): attname, attlen, attbyval.
type Column struct {
	Name   string
	Offset int // byte offset within ScanTuple.Tuple.Data
	Len    int // byte length
}

// Schema is an ordered list of columns that together describe the layout of a
// tuple's Data bytes. It can project any ScanTuple down to the subset of
// columns it describes.
//
// This mirrors PostgreSQL's TupleDesc (src/include/access/tupdesc.h), which
// stores the column metadata for a TupleTableSlot.
type Schema struct {
	Cols []Column
}

// Width returns the total byte width of all columns in the schema.
func (s *Schema) Width() int {
	total := 0
	for _, c := range s.Cols {
		total += c.Len
	}
	return total
}

// Project extracts the schema's columns from src and returns a new ScanTuple
// whose Data is the tightly-packed concatenation of those columns.
// Block and Offset are inherited from src so TIDs remain valid.
// Returns nil if src.Tuple.Data is too short to satisfy the schema.
func (s *Schema) Project(src *ScanTuple) *ScanTuple {
	data := src.Tuple.Data
	out := make([]byte, 0, s.Width())
	for _, c := range s.Cols {
		end := c.Offset + c.Len
		if end > len(data) {
			return nil // data too short — drop row
		}
		out = append(out, data[c.Offset:end]...)
	}
	projected := *src.Tuple // shallow copy; Header unchanged
	projected.Data = out
	return &ScanTuple{Tuple: &projected, Block: src.Block, Offset: src.Offset}
}

// ProjectFnFrom returns a ProjectFn that applies this schema.
func (s *Schema) ProjectFnFrom() ProjectFn {
	return func(src *ScanTuple) (*ScanTuple, error) {
		return s.Project(src), nil
	}
}

// Get extracts the bytes of the named column from a projected tuple's Data.
// The tuple must have been produced by this schema.
// Returns nil if the column is not found.
func (s *Schema) Get(name string, tup *ScanTuple) []byte {
	off := 0
	for _, c := range s.Cols {
		if c.Name == name {
			end := off + c.Len
			if end > len(tup.Tuple.Data) {
				return nil
			}
			return tup.Tuple.Data[off:end]
		}
		off += c.Len
	}
	return nil
}
