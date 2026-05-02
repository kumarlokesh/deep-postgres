package concurrentsplit_test

// Concurrent split experiment.
//
// Two scenarios:
//
//  1. Concurrent inserts: N goroutines each write a disjoint key range into a
//     shared index protected by a sync.RWMutex. After all goroutines finish,
//     every inserted key must be searchable - verifying that concurrent splits
//     never lose entries or corrupt sibling pointers.
//
//  2. Right-link traversal: fill a leaf page to capacity, record its block
//     number, trigger a split so that the upper half of keys moves to a new
//     right-sibling page, then call SearchFromBlock with the *old* block and a
//     key from the right half. The search must follow the BtpoNext right-link
//     and return the correct result - the same recovery path a goroutine would
//     take after discovering its cached leaf reference is stale.
//
// Run with -race to detect data races:
//
//	go test -race -v ./experiments/concurrent-split/

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func encKey(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func cmpBytes(a, b []byte) int { return bytes.Compare(a, b) }

// safeIndex wraps BTreeIndex with a RWMutex for goroutine-safe access.
// Writes (Insert) take an exclusive lock; reads (Search, SearchAll) share.
type safeIndex struct {
	mu  sync.RWMutex
	idx *storage.BTreeIndex
}

func (s *safeIndex) Insert(key []byte, blk storage.BlockNumber, off storage.OffsetNumber) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idx.Insert(key, blk, off)
}

func (s *safeIndex) SearchAll(key []byte) ([]storage.HeapTID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idx.SearchAll(key)
}

func newSafeIndex(t *testing.T) *safeIndex {
	t.Helper()
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(512)
	node := storage.RelFileNode{DbId: 0, RelId: 1}
	rel := storage.OpenRelation(node, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}
	idx, err := storage.NewBTreeIndex(rel, cmpBytes)
	if err != nil {
		t.Fatalf("NewBTreeIndex: %v", err)
	}
	t.Cleanup(func() { smgr.Close() })
	return &safeIndex{idx: idx}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestConcurrentInsertDisjoint spawns numWorkers goroutines each inserting a
// disjoint key range. After all goroutines finish, every key must be found.
func TestConcurrentInsertDisjoint(t *testing.T) {
	si := newSafeIndex(t)

	const (
		numWorkers    = 4
		keysPerWorker = 250 // 1 000 total → several leaf splits
	)

	var (
		wg     sync.WaitGroup
		errors atomic.Int32
	)
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lo := uint32(id * keysPerWorker)
			for k := lo; k < lo+keysPerWorker; k++ {
				if err := si.Insert(encKey(k), k, 1); err != nil {
					t.Errorf("worker %d Insert(%d): %v", id, k, err)
					errors.Add(1)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if errors.Load() != 0 {
		t.Fatalf("%d insert error(s) during concurrent phase", errors.Load())
	}

	total := numWorkers * keysPerWorker
	missing := 0
	for k := uint32(0); k < uint32(total); k++ {
		tids, err := si.SearchAll(encKey(k))
		if err != nil {
			t.Fatalf("SearchAll(%d): %v", k, err)
		}
		if len(tids) == 0 {
			missing++
			if missing <= 5 {
				t.Errorf("key %d not found after concurrent inserts", k)
			}
		}
	}
	if missing > 0 {
		t.Errorf("%d/%d keys missing after concurrent inserts", missing, total)
	}
}

// TestConcurrentReadsDuringWrites has one writer advancing a key frontier and
// three readers continuously searching for the most recently committed key.
// The frontier is updated only after the write lock is released, so readers
// never ask for a key that isn't yet in the tree.
func TestConcurrentReadsDuringWrites(t *testing.T) {
	si := newSafeIndex(t)
	const total = uint32(800)

	var frontier atomic.Uint32 // one past the highest committed key

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for k := uint32(0); k < total; k++ {
			if err := si.Insert(encKey(k), k, 1); err != nil {
				t.Errorf("writer Insert(%d): %v", k, err)
				return
			}
			frontier.Store(k + 1)
		}
	}()

	var rwg sync.WaitGroup
	for r := 0; r < 3; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				f := frontier.Load()
				if f == 0 {
					continue
				}
				key := encKey(f - 1)
				tids, err := si.SearchAll(key)
				if err != nil {
					t.Errorf("reader SearchAll: %v", err)
					return
				}
				if len(tids) == 0 {
					t.Errorf("committed key %d not found during concurrent writes", f-1)
				}
				if f >= total {
					return
				}
			}
		}()
	}

	<-writerDone
	rwg.Wait()
}

// TestRightLinkTraversal is the core right-link test.
//
// Setup:
//  1. Insert 509 keys (fills the initial rightmost leaf to capacity).
//  2. Record the leaf block that holds the top of the range.
//  3. Insert key 509, forcing a split: the left page keeps keys [0, ~254]
//     and gets BtpoNext pointing to a new right page holding keys [~255, 509].
//  4. Call SearchFromBlock(oldLeafBlk, key=400).
//
// Expected: key=400 now lives on the right sibling. findLeaf sees
// key(400) > highKey(left page) and follows BtpoNext, landing on the page
// that holds the key - demonstrating right-link traversal.
func TestRightLinkTraversal(t *testing.T) {
	dir := t.TempDir()
	smgr := storage.NewFileStorageManager(dir)
	pool := storage.NewBufferPool(512)
	node := storage.RelFileNode{DbId: 0, RelId: 2}
	rel := storage.OpenRelation(node, pool, smgr)
	if err := rel.Init(); err != nil {
		t.Fatalf("rel.Init: %v", err)
	}
	defer smgr.Close()

	idx, err := storage.NewBTreeIndex(rel, cmpBytes)
	if err != nil {
		t.Fatalf("NewBTreeIndex: %v", err)
	}

	// Fill the initial leaf to capacity (509 entries for a rightmost page).
	// 509 × 16 B/entry = 8 144 B; available space = 8 176 − 24 = 8 152 B → fits.
	const fillCount = uint32(509)
	for k := uint32(0); k < fillCount; k++ {
		if err := idx.Insert(encKey(k), k, 1); err != nil {
			t.Fatalf("pre-split Insert(%d): %v", k, err)
		}
	}

	// Record which block currently holds a key near the top of the range.
	// After the split this key will migrate to the right sibling.
	staleTarget := uint32(400)
	staleBlk, err := idx.FindLeaf(encKey(staleTarget))
	if err != nil {
		t.Fatalf("FindLeaf before split: %v", err)
	}

	// Trigger the split by inserting one more key.
	if err := idx.Insert(encKey(fillCount), fillCount, 1); err != nil {
		t.Fatalf("split-triggering Insert(%d): %v", fillCount, err)
	}

	// Verify via normal root-descent search that the target key is still found.
	_, _, found, err := idx.Search(encKey(staleTarget))
	if err != nil {
		t.Fatalf("Search after split: %v", err)
	}
	if !found {
		t.Fatalf("key %d not found via normal Search after split", staleTarget)
	}

	// Now simulate a goroutine that cached staleBlk before the split.
	// SearchFromBlock starts at staleBlk; because key(400) > high key of the
	// left page, it follows BtpoNext to the right sibling and finds the key.
	heapBlk, _, foundViaRL, err := idx.SearchFromBlock(staleBlk, encKey(staleTarget))
	if err != nil {
		t.Fatalf("SearchFromBlock: %v", err)
	}
	if !foundViaRL {
		t.Fatalf("right-link traversal: key %d not found starting from stale block %d",
			staleTarget, staleBlk)
	}
	if heapBlk != staleTarget {
		t.Errorf("right-link traversal: got heapBlk=%d, want %d", heapBlk, staleTarget)
	}
	t.Logf("right-link traversal: key %d found correctly starting from stale block %d",
		staleTarget, staleBlk)
}

// ── summary ──────────────────────────────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Println("=== concurrent-split experiment ===")
	fmt.Println("  TestConcurrentInsertDisjoint : 4 goroutines × 250 keys - verify no losses after splits")
	fmt.Println("  TestConcurrentReadsDuringWrites : 1 writer + 3 readers - committed key always visible")
	fmt.Println("  TestRightLinkTraversal       : stale leaf ref → right-link traversal finds migrated key")
	fmt.Println()
	m.Run()
}
