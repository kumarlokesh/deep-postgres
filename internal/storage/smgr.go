package storage

// Storage manager (smgr) — the I/O abstraction between the buffer manager and
// the file system.  Matches the role of src/backend/storage/smgr/smgr.c.
//
// Each relation fork is stored as a flat file of PageSize-byte blocks:
//
//   {dataDir}/{dbId}/{relId}          — main fork
//   {dataDir}/{dbId}/{relId}.fsm      — free-space map fork
//   {dataDir}/{dbId}/{relId}.vm       — visibility map fork
//   {dataDir}/{dbId}/{relId}.init     — init fork
//
// Reads and writes operate at block granularity. The caller is responsible for
// presenting / consuming PageSize-byte slices.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// RelFileNode identifies the on-disk storage for a relation.
// Matches PostgreSQL's RelFileLocator (formerly RelFileNode).
type RelFileNode struct {
	DbId  Oid // 0 = global / shared catalog
	RelId Oid
}

// ErrBlockNotFound is returned when reading a block number beyond the end of
// the relation file.
var ErrBlockNotFound = errors.New("smgr: block not found")

// StorageManager is the interface presented to the buffer manager for all
// physical I/O.  Implementations may be file-based, in-memory, or remote.
type StorageManager interface {
	// Create creates the storage file(s) for rfn/fork.  If the file already
	// exists, Create is a no-op.
	Create(rfn RelFileNode, fork ForkNumber) error

	// Exists reports whether the fork file exists on disk.
	Exists(rfn RelFileNode, fork ForkNumber) bool

	// Read reads block blockNum of rfn/fork into dst, which must be exactly
	// PageSize bytes.  Returns ErrBlockNotFound if blockNum >= NBlocks.
	Read(rfn RelFileNode, fork ForkNumber, blockNum BlockNumber, dst []byte) error

	// Write writes src (exactly PageSize bytes) to block blockNum of rfn/fork.
	// The block must already exist (i.e. blockNum < NBlocks); use Extend to
	// append new blocks.
	Write(rfn RelFileNode, fork ForkNumber, blockNum BlockNumber, src []byte) error

	// Extend appends one zero-filled block to rfn/fork and returns the new
	// block's number.
	Extend(rfn RelFileNode, fork ForkNumber) (BlockNumber, error)

	// NBlocks returns the current size of rfn/fork in blocks.
	NBlocks(rfn RelFileNode, fork ForkNumber) (BlockNumber, error)

	// Truncate reduces rfn/fork to nBlocks blocks.
	Truncate(rfn RelFileNode, fork ForkNumber, nBlocks BlockNumber) error

	// Sync flushes all pending writes for rfn/fork to durable storage.
	Sync(rfn RelFileNode, fork ForkNumber) error

	// Unlink removes all fork files for rfn.
	Unlink(rfn RelFileNode) error

	// Close releases all open file handles held by the manager.
	Close() error
}

// ── FileStorageManager ───────────────────────────────────────────────────────

// FileStorageManager implements StorageManager using the local file system.
// It is safe for concurrent use.
type FileStorageManager struct {
	DataDir string // root data directory

	mu    sync.Mutex
	files map[fileKey]*os.File // open file handles
}

type fileKey struct {
	rfn  RelFileNode
	fork ForkNumber
}

// NewFileStorageManager creates a manager rooted at dataDir, which must exist.
func NewFileStorageManager(dataDir string) *FileStorageManager {
	return &FileStorageManager{
		DataDir: dataDir,
		files:   make(map[fileKey]*os.File),
	}
}

// forkSuffix returns the file-name suffix for a relation fork.
func forkSuffix(fork ForkNumber) string {
	switch fork {
	case ForkFsm:
		return ".fsm"
	case ForkVm:
		return ".vm"
	case ForkInit:
		return ".init"
	default:
		return ""
	}
}

// relPath returns the absolute path for rfn/fork.
func (m *FileStorageManager) relPath(rfn RelFileNode, fork ForkNumber) string {
	dbDir := filepath.Join(m.DataDir, fmt.Sprintf("%d", rfn.DbId))
	return filepath.Join(dbDir, fmt.Sprintf("%d%s", rfn.RelId, forkSuffix(fork)))
}

// openFile returns a cached *os.File for rfn/fork, creating the database
// directory and file if they do not yet exist.
func (m *FileStorageManager) openFile(rfn RelFileNode, fork ForkNumber, create bool) (*os.File, error) {
	k := fileKey{rfn, fork}

	m.mu.Lock()
	defer m.mu.Unlock()

	if f, ok := m.files[k]; ok {
		return f, nil
	}

	path := m.relPath(rfn, fork)

	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}

	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	m.files[k] = f
	return f, nil
}

// Create creates the storage file for rfn/fork if it does not already exist.
func (m *FileStorageManager) Create(rfn RelFileNode, fork ForkNumber) error {
	_, err := m.openFile(rfn, fork, true)
	return err
}

// Exists reports whether the fork file exists.
func (m *FileStorageManager) Exists(rfn RelFileNode, fork ForkNumber) bool {
	_, err := os.Stat(m.relPath(rfn, fork))
	return err == nil
}

// Read reads block blockNum into dst (len must be PageSize).
func (m *FileStorageManager) Read(rfn RelFileNode, fork ForkNumber, blockNum BlockNumber, dst []byte) error {
	if len(dst) != PageSize {
		return fmt.Errorf("smgr.Read: dst must be %d bytes", PageSize)
	}
	f, err := m.openFile(rfn, fork, false)
	if err != nil {
		return ErrBlockNotFound
	}
	offset := int64(blockNum) * int64(PageSize)
	n, err := f.ReadAt(dst, offset)
	if err != nil {
		if errors.Is(err, io.EOF) || (n == 0 && errors.Is(err, io.ErrUnexpectedEOF)) {
			return ErrBlockNotFound
		}
		return err
	}
	if n != PageSize {
		return ErrBlockNotFound
	}
	return nil
}

// Write writes src (PageSize bytes) to block blockNum.
// The block must already exist (blockNum < NBlocks).
func (m *FileStorageManager) Write(rfn RelFileNode, fork ForkNumber, blockNum BlockNumber, src []byte) error {
	if len(src) != PageSize {
		return fmt.Errorf("smgr.Write: src must be %d bytes", PageSize)
	}
	f, err := m.openFile(rfn, fork, false)
	if err != nil {
		return err
	}
	n, err := m.NBlocks(rfn, fork)
	if err != nil {
		return err
	}
	if blockNum >= n {
		return fmt.Errorf("smgr.Write: block %d out of range (nblocks=%d)", blockNum, n)
	}
	offset := int64(blockNum) * int64(PageSize)
	_, err = f.WriteAt(src, offset)
	return err
}

// Extend appends one zero-filled block to rfn/fork and returns the new block number.
func (m *FileStorageManager) Extend(rfn RelFileNode, fork ForkNumber) (BlockNumber, error) {
	f, err := m.openFile(rfn, fork, true)
	if err != nil {
		return InvalidBlockNumber, err
	}

	n, err := m.NBlocks(rfn, fork)
	if err != nil {
		return InvalidBlockNumber, err
	}

	zeros := make([]byte, PageSize)
	offset := int64(n) * int64(PageSize)
	if _, err := f.WriteAt(zeros, offset); err != nil {
		return InvalidBlockNumber, err
	}
	return n, nil
}

// NBlocks returns the current size of rfn/fork in blocks.
func (m *FileStorageManager) NBlocks(rfn RelFileNode, fork ForkNumber) (BlockNumber, error) {
	f, err := m.openFile(rfn, fork, false)
	if err != nil {
		// File doesn't exist yet.
		return 0, nil
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return BlockNumber(info.Size() / int64(PageSize)), nil
}

// Truncate reduces rfn/fork to nBlocks blocks.
func (m *FileStorageManager) Truncate(rfn RelFileNode, fork ForkNumber, nBlocks BlockNumber) error {
	f, err := m.openFile(rfn, fork, false)
	if err != nil {
		return err
	}
	return f.Truncate(int64(nBlocks) * int64(PageSize))
}

// Sync flushes pending writes for rfn/fork.
func (m *FileStorageManager) Sync(rfn RelFileNode, fork ForkNumber) error {
	m.mu.Lock()
	f, ok := m.files[fileKey{rfn, fork}]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return f.Sync()
}

// Unlink removes all fork files for rfn.
func (m *FileStorageManager) Unlink(rfn RelFileNode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, fork := range []ForkNumber{ForkMain, ForkFsm, ForkVm, ForkInit} {
		k := fileKey{rfn, fork}
		if f, ok := m.files[k]; ok {
			f.Close()
			delete(m.files, k)
		}
		os.Remove(m.relPath(rfn, fork)) // ignore not-found errors
	}
	return nil
}

// Close closes all open file handles.
func (m *FileStorageManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for k, f := range m.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.files, k)
	}
	return firstErr
}
