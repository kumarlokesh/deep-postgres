package storage

import (
	"bytes"
	"errors"
	"testing"
)

func newTestSmgr(t *testing.T) (*FileStorageManager, RelFileNode) {
	t.Helper()
	dir := t.TempDir()
	smgr := NewFileStorageManager(dir)
	rfn := RelFileNode{DbId: 0, RelId: 1}
	if err := smgr.Create(rfn, ForkMain); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return smgr, rfn
}

func TestSmgrCreateAndExists(t *testing.T) {
	smgr, rfn := newTestSmgr(t)
	defer smgr.Close()

	if !smgr.Exists(rfn, ForkMain) {
		t.Error("expected fork to exist after Create")
	}
	rfn2 := RelFileNode{DbId: 0, RelId: 99}
	if smgr.Exists(rfn2, ForkMain) {
		t.Error("non-existent fork should not exist")
	}
}

func TestSmgrNBlocksEmpty(t *testing.T) {
	smgr, rfn := newTestSmgr(t)
	defer smgr.Close()

	n, err := smgr.NBlocks(rfn, ForkMain)
	if err != nil {
		t.Fatalf("NBlocks: %v", err)
	}
	if n != 0 {
		t.Errorf("NBlocks of empty file = %d, want 0", n)
	}
}

func TestSmgrExtendAndRead(t *testing.T) {
	smgr, rfn := newTestSmgr(t)
	defer smgr.Close()

	blk, err := smgr.Extend(rfn, ForkMain)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if blk != 0 {
		t.Errorf("first block = %d, want 0", blk)
	}

	n, _ := smgr.NBlocks(rfn, ForkMain)
	if n != 1 {
		t.Errorf("NBlocks after first extend = %d, want 1", n)
	}

	blk1, _ := smgr.Extend(rfn, ForkMain)
	if blk1 != 1 {
		t.Errorf("second block = %d, want 1", blk1)
	}

	n, _ = smgr.NBlocks(rfn, ForkMain)
	if n != 2 {
		t.Errorf("NBlocks after second extend = %d, want 2", n)
	}
}

func TestSmgrWriteAndRead(t *testing.T) {
	smgr, rfn := newTestSmgr(t)
	defer smgr.Close()

	smgr.Extend(rfn, ForkMain)

	page := NewPage()
	page.InsertTuple([]byte("hello from disk"))

	src := page.Bytes()
	if err := smgr.Write(rfn, ForkMain, 0, src); err != nil {
		t.Fatalf("Write: %v", err)
	}

	dst := make([]byte, PageSize)
	if err := smgr.Read(rfn, ForkMain, 0, dst); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(dst, src) {
		t.Error("read data does not match written data")
	}
}

func TestSmgrReadBeyondEOF(t *testing.T) {
	smgr, rfn := newTestSmgr(t)
	defer smgr.Close()

	dst := make([]byte, PageSize)
	err := smgr.Read(rfn, ForkMain, 0, dst)
	if !errors.Is(err, ErrBlockNotFound) {
		t.Errorf("expected ErrBlockNotFound, got %v", err)
	}
}

func TestSmgrWriteBeyondRange(t *testing.T) {
	smgr, rfn := newTestSmgr(t)
	defer smgr.Close()

	src := make([]byte, PageSize)
	// Block 0 does not exist yet.
	err := smgr.Write(rfn, ForkMain, 0, src)
	if err == nil {
		t.Error("expected error writing to non-existent block")
	}
}

func TestSmgrTruncate(t *testing.T) {
	smgr, rfn := newTestSmgr(t)
	defer smgr.Close()

	for i := 0; i < 4; i++ {
		smgr.Extend(rfn, ForkMain)
	}
	n, _ := smgr.NBlocks(rfn, ForkMain)
	if n != 4 {
		t.Fatalf("NBlocks = %d, want 4", n)
	}

	if err := smgr.Truncate(rfn, ForkMain, 2); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	n, _ = smgr.NBlocks(rfn, ForkMain)
	if n != 2 {
		t.Errorf("NBlocks after truncate = %d, want 2", n)
	}
}

func TestSmgrMultipleForks(t *testing.T) {
	dir := t.TempDir()
	smgr := NewFileStorageManager(dir)
	defer smgr.Close()
	rfn := RelFileNode{DbId: 0, RelId: 1}

	for _, fork := range []ForkNumber{ForkMain, ForkFsm, ForkVm} {
		if err := smgr.Create(rfn, fork); err != nil {
			t.Fatalf("Create fork %d: %v", fork, err)
		}
		blk, err := smgr.Extend(rfn, fork)
		if err != nil {
			t.Fatalf("Extend fork %d: %v", fork, err)
		}
		if blk != 0 {
			t.Errorf("fork %d first block = %d, want 0", fork, blk)
		}
	}

	// Verify each fork has exactly 1 block.
	for _, fork := range []ForkNumber{ForkMain, ForkFsm, ForkVm} {
		n, _ := smgr.NBlocks(rfn, fork)
		if n != 1 {
			t.Errorf("fork %d NBlocks = %d, want 1", fork, n)
		}
	}
}

func TestSmgrUnlink(t *testing.T) {
	smgr, rfn := newTestSmgr(t)
	defer smgr.Close()

	smgr.Extend(rfn, ForkMain)
	smgr.Unlink(rfn)

	if smgr.Exists(rfn, ForkMain) {
		t.Error("fork should not exist after Unlink")
	}
}

func TestSmgrReopenPreservesData(t *testing.T) {
	dir := t.TempDir()
	rfn := RelFileNode{DbId: 0, RelId: 42}
	payload := NewPage()
	payload.InsertTuple([]byte("persist across reopen"))
	data := payload.Bytes()

	// Write with first manager instance.
	m1 := NewFileStorageManager(dir)
	m1.Create(rfn, ForkMain)
	m1.Extend(rfn, ForkMain)
	if err := m1.Write(rfn, ForkMain, 0, data); err != nil {
		t.Fatalf("m1.Write: %v", err)
	}
	m1.Sync(rfn, ForkMain)
	m1.Close()

	// Read with a fresh manager instance (no cached file handles).
	m2 := NewFileStorageManager(dir)
	defer m2.Close()
	dst := make([]byte, PageSize)
	if err := m2.Read(rfn, ForkMain, 0, dst); err != nil {
		t.Fatalf("m2.Read: %v", err)
	}
	if !bytes.Equal(dst, data) {
		t.Error("data not preserved across reopen")
	}
}
