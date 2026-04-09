package storage

// TOAST — The Oversized-Attribute Storage Technique.
//
// PostgreSQL stores large tuple attributes out-of-line in a dedicated TOAST
// relation (pg_toast_NNNN), leaving a compact "varatt_external" pointer in the
// main heap.  This research implementation reproduces the same mechanism using
// an in-memory chunk store.
//
// Key constants (tuptoaster.h / pg_config_manual.h):
//
//   TOAST_TUPLE_THRESHOLD  = 2048  — compress/detoast when tuple exceeds this
//   TOAST_MAX_CHUNK_SIZE   = 2000  — max bytes written per TOAST chunk
//   TOAST_COMPRESSION_MIN  =  512  — skip compression if result > this
//
// Chunk layout mirrors pg_toast_NNNN (chunk_id, chunk_seq, chunk_data).
// An InMemoryToastStore maps chunk_id → []ToastChunk and can store/fetch/delete
// multi-chunk values.
//
// MVCC note: in PostgreSQL each TOAST chunk is an ordinary heap tuple with its
// own xmin/xmax.  VACUUM reclaims orphaned chunks (dead-tuple TOAST cleanup).
// This implementation is non-transactional: chunks are stored on first write
// and deleted explicitly.  The VACUUM-TOAST integration and per-chunk
// visibility is documented but not implemented; see the blog post for details.

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
)

// TOAST thresholds.
const (
	// ToastTupleThreshold: attempt TOAST when tuple data exceeds this size.
	// Matches PostgreSQL's TOAST_TUPLE_THRESHOLD = 2 * TOAST_MAX_CHUNK_SIZE (2048).
	ToastTupleThreshold = 2048

	// ToastMaxChunkSize: bytes stored per chunk in the TOAST table.
	// Matches PostgreSQL's TOAST_MAX_CHUNK_SIZE = 1996 (we use 2000 for simplicity).
	ToastMaxChunkSize = 2000

	// ToastCompressionMin: only keep compressed result if it is smaller than
	// the original.  PostgreSQL tries compression whenever
	// len(data) > TOAST_COMPRESSION_THRESHOLD.
	ToastCompressionMin = 512
)

// ToastPointerSize is the fixed serialised byte size of a ToastPointer.
// Matches the payload of varatt_external (18 bytes in PostgreSQL; we use 13 to
// keep the tag + 4+4+4 byte fields).
const ToastPointerSize = 13

// toastTag is the leading byte that identifies a serialised ToastPointer.
// PostgreSQL uses the VARTAG_EXTERNAL varattrib tag; we use a sentinel value
// that does not conflict with normal data.
const toastTag = byte(0x12)

// toastCompressedBit is ORed into the tag byte when stored data is compressed.
const toastCompressedBit = byte(0x01)

// ToastPointer is stored inside HeapTuple.Data when the original payload was
// too large to hold in-line.
//
// Corresponds to PostgreSQL's varatt_external (postgres_ext.h):
//
//	va_rawsize    — original uncompressed data length
//	va_extsize    — bytes stored in TOAST (may be compressed)
//	va_valueid    — chunk_id identifying the chunk set
type ToastPointer struct {
	RawSize    uint32 // uncompressed length
	ExtSize    uint32 // stored length (compressed if Compressed is true)
	ChunkId    Oid    // identifies the chunk set in the TOAST store
	Compressed bool   // true when stored bytes are deflate-compressed
}

// Marshal serialises p into exactly ToastPointerSize bytes.
// The tag byte encodes whether the value was compressed.
func (p ToastPointer) Marshal() []byte {
	tag := toastTag
	if p.Compressed {
		tag |= toastCompressedBit
	}
	b := make([]byte, ToastPointerSize)
	b[0] = tag
	binary.LittleEndian.PutUint32(b[1:5], p.RawSize)
	binary.LittleEndian.PutUint32(b[5:9], p.ExtSize)
	binary.LittleEndian.PutUint32(b[9:13], p.ChunkId)
	return b
}

// UnmarshalToastPointer decodes a ToastPointer from exactly ToastPointerSize bytes.
func UnmarshalToastPointer(b []byte) (ToastPointer, error) {
	if len(b) < ToastPointerSize {
		return ToastPointer{}, fmt.Errorf("toast: pointer too short (%d bytes)", len(b))
	}
	tag := b[0]
	if tag&^toastCompressedBit != toastTag {
		return ToastPointer{}, fmt.Errorf("toast: not a toast pointer (tag=0x%02x)", tag)
	}
	return ToastPointer{
		RawSize:    binary.LittleEndian.Uint32(b[1:5]),
		ExtSize:    binary.LittleEndian.Uint32(b[5:9]),
		ChunkId:    binary.LittleEndian.Uint32(b[9:13]),
		Compressed: tag&toastCompressedBit != 0,
	}, nil
}

// IsToasted reports whether data begins with a ToastPointer tag byte.
// A heap tuple's Data field is toasted when this returns true.
func IsToasted(data []byte) bool {
	return len(data) >= ToastPointerSize && data[0]&^toastCompressedBit == toastTag
}

// ── ToastChunk ───────────────────────────────────────────────────────────────

// ToastChunk is one piece of a TOASTed value, corresponding to one row in a
// pg_toast_NNNN relation.
//
// PostgreSQL stores (chunk_id oid, chunk_seq int4, chunk_data bytea).
// We omit chunk_id from the struct because it is already the map key in
// InMemoryToastStore.
type ToastChunk struct {
	Seq  int32  // 0-based chunk sequence number
	Data []byte // raw chunk bytes (uninterpreted)
}

// ── ToastStore interface ──────────────────────────────────────────────────────

// ToastStore stores and retrieves chunked TOAST values.
//
// In PostgreSQL the TOAST table is an ordinary heap relation; every chunk has
// its own MVCC visibility.  Here we model the observable behaviour (store /
// fetch / delete) without per-chunk transaction IDs.
type ToastStore interface {
	// Store splits data into ToastMaxChunkSize-byte chunks.  If compress is
	// true and flate compression yields a smaller result, the compressed bytes
	// are stored instead.  Returns the pointer to embed in the heap tuple.
	Store(data []byte, compress bool) (ToastPointer, error)

	// Fetch reassembles and (if necessary) decompresses the value referenced
	// by ptr.
	Fetch(ptr ToastPointer) ([]byte, error)

	// Delete removes all chunks for ptr.ChunkId.  Called when a toasted tuple
	// is reclaimed (e.g. by VACUUM).
	Delete(ptr ToastPointer) error
}

// ── InMemoryToastStore ────────────────────────────────────────────────────────

// InMemoryToastStore implements ToastStore using an in-memory map.
// Not thread-safe.
type InMemoryToastStore struct {
	nextId atomic.Uint32        // monotonically increasing chunk-id counter
	chunks map[Oid][]ToastChunk // chunkId → sorted chunk slice
}

// NewInMemoryToastStore allocates an empty toast store.
func NewInMemoryToastStore() *InMemoryToastStore {
	return &InMemoryToastStore{
		chunks: make(map[Oid][]ToastChunk),
	}
}

// Store implements ToastStore.
func (s *InMemoryToastStore) Store(data []byte, compress bool) (ToastPointer, error) {
	rawSize := uint32(len(data))
	stored := data
	compressed := false

	if compress && len(data) > ToastCompressionMin {
		if c, err := toastCompress(data); err == nil && len(c) < len(data) {
			stored = c
			compressed = true
		}
	}

	chunkId := s.nextId.Add(1)
	var chunks []ToastChunk
	for seq := 0; len(stored) > 0; seq++ {
		n := len(stored)
		if n > ToastMaxChunkSize {
			n = ToastMaxChunkSize
		}
		chunk := make([]byte, n)
		copy(chunk, stored[:n])
		chunks = append(chunks, ToastChunk{Seq: int32(seq), Data: chunk})
		stored = stored[n:]
	}
	s.chunks[chunkId] = chunks

	extSize := uint32(0)
	for _, c := range chunks {
		extSize += uint32(len(c.Data))
	}

	return ToastPointer{
		RawSize:    rawSize,
		ExtSize:    extSize,
		ChunkId:    chunkId,
		Compressed: compressed,
	}, nil
}

// Fetch implements ToastStore.
func (s *InMemoryToastStore) Fetch(ptr ToastPointer) ([]byte, error) {
	chunks, ok := s.chunks[ptr.ChunkId]
	if !ok {
		return nil, fmt.Errorf("toast: chunk_id %d not found", ptr.ChunkId)
	}

	buf := make([]byte, 0, ptr.ExtSize)
	for _, c := range chunks {
		buf = append(buf, c.Data...)
	}

	if ptr.Compressed {
		out, err := toastDecompress(buf, int(ptr.RawSize))
		if err != nil {
			return nil, fmt.Errorf("toast: decompress chunk_id %d: %w", ptr.ChunkId, err)
		}
		return out, nil
	}
	return buf, nil
}

// Delete implements ToastStore.
func (s *InMemoryToastStore) Delete(ptr ToastPointer) error {
	if _, ok := s.chunks[ptr.ChunkId]; !ok {
		return fmt.Errorf("toast: chunk_id %d not found", ptr.ChunkId)
	}
	delete(s.chunks, ptr.ChunkId)
	return nil
}

// ChunkSetCount returns the number of distinct chunk sets stored.  Useful in tests.
func (s *InMemoryToastStore) ChunkSetCount() int { return len(s.chunks) }

// ── Tuple helpers ─────────────────────────────────────────────────────────────

// Toastify stores tup.Data in store if it exceeds ToastTupleThreshold.
// On success, tup.Data is replaced with the serialised ToastPointer and the
// HeapHasExternal infomask flag is set.  The original data is returned so the
// caller can confirm the round-trip if needed.
//
// Toastify is idempotent: calling it on an already-toasted tuple is a no-op.
func Toastify(tup *HeapTuple, store ToastStore) error {
	if IsToasted(tup.Data) {
		return nil // already toasted
	}
	if len(tup.Data) <= ToastTupleThreshold {
		return nil // small enough to stay in-line
	}

	ptr, err := store.Store(tup.Data, true)
	if err != nil {
		return fmt.Errorf("toast: store: %w", err)
	}

	tup.Data = ptr.Marshal()
	tup.Header.TInfomask |= uint16(HeapHasExternal)
	return nil
}

// Detoast retrieves the original data for a toasted tuple.  If tup.Data is not
// a TOAST pointer (the tuple is in-line), the data is returned unchanged.
func Detoast(tup *HeapTuple, store ToastStore) ([]byte, error) {
	if !IsToasted(tup.Data) {
		return tup.Data, nil
	}
	ptr, err := UnmarshalToastPointer(tup.Data)
	if err != nil {
		return nil, err
	}
	return store.Fetch(ptr)
}

// ── compression helpers ───────────────────────────────────────────────────────

func toastCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func toastDecompress(data []byte, rawSize int) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	out := make([]byte, 0, rawSize)
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
