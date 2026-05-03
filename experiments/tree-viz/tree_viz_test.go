package treeviz_test

// Tree visualization experiment.
//
// Renders the full B-tree structure (root → internal → leaf pages, sibling
// links, high keys, entry counts) and verifies structural invariants after
// each insert sequence.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

// ── key helpers ───────────────────────────────────────────────────────────────

func encKey(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func cmpBytes(a, b []byte) int { return bytes.Compare(a, b) }

func keyStr(b []byte) string {
	if len(b) < 4 {
		return fmt.Sprintf("%x", b)
	}
	return fmt.Sprintf("%d", binary.BigEndian.Uint32(b))
}

// ── index factory ─────────────────────────────────────────────────────────────

func newIndex(t *testing.T) (*storage.BTreeIndex, func()) {
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
	return idx, func() { smgr.Close() }
}

func insertRange(t *testing.T, idx *storage.BTreeIndex, lo, hi uint32) {
	t.Helper()
	for k := lo; k < hi; k++ {
		if err := idx.Insert(encKey(k), k, 1); err != nil {
			t.Fatalf("Insert(%d): %v", k, err)
		}
	}
}

// ── renderer ──────────────────────────────────────────────────────────────────

const invalid = storage.InvalidBlockNumber

func blkStr(b storage.BlockNumber) string {
	if b == invalid {
		return "·"
	}
	return fmt.Sprintf("%d", b)
}

func flagStr(flags uint16) string {
	f := storage.BTreePageFlags(flags)
	var parts []string
	if f&storage.BTPRoot != 0 {
		parts = append(parts, "ROOT")
	}
	if f&storage.BTPLeaf != 0 {
		parts = append(parts, "LEAF")
	} else {
		parts = append(parts, "INTERNAL")
	}
	if f&storage.BTPIncomplete != 0 {
		parts = append(parts, "INCOMPLETE!")
	}
	if f&storage.BTPDeleted != 0 {
		parts = append(parts, "DELETED")
	}
	if f&storage.BTPHalfDead != 0 {
		parts = append(parts, "HALF-DEAD")
	}
	return strings.Join(parts, "|")
}

// renderTree formats the full tree snapshot as a multi-line string.
// levels[0] = root (top); levels[last] = leaves (bottom).
// Leaf pages with ≤ showAllThreshold entries print every entry individually;
// larger pages print a summary line.
func renderTree(levels [][]storage.PageInfo, title string) string {
	var sb strings.Builder

	// Compute totals.
	totalLeafEntries := 0
	var leafLevel []storage.PageInfo
	if len(levels) > 0 {
		leafLevel = levels[len(levels)-1]
		for _, p := range leafLevel {
			totalLeafEntries += len(p.Entries)
		}
	}

	treeHeight := len(levels)
	nLeaves := len(leafLevel)

	// Header.
	var rootBlk storage.BlockNumber = invalid
	if len(levels) > 0 && len(levels[0]) > 0 {
		rootBlk = levels[0][0].Block
	}
	fmt.Fprintf(&sb, "B-tree: %s\n", title)
	fmt.Fprintf(&sb, "  root=blk %s  height=%d  leaves=%d  total_keys=%d\n\n",
		blkStr(rootBlk), treeHeight, nLeaves, totalLeafEntries)

	// One section per depth level, top-down.
	for depth, pages := range levels {
		isLeaf := depth == len(levels)-1
		levelName := "internal"
		if isLeaf {
			levelName = "leaf"
		}
		if len(levels) == 1 {
			levelName = "root+leaf"
		}
		fmt.Fprintf(&sb, "── level %d (%s, %d page(s)) ", depth, levelName, len(pages))
		fmt.Fprintf(&sb, "%s\n", strings.Repeat("─", max(0, 56-len(levelName)-10)))

		for _, p := range pages {
			hk := "·"
			if p.HighKey != nil {
				hk = keyStr(p.HighKey)
			}
			fmt.Fprintf(&sb, "  blk %3d  [%s]  ←%s  →%s",
				p.Block, flagStr(p.Flags), blkStr(p.Prev), blkStr(p.Next))
			if p.HighKey != nil {
				fmt.Fprintf(&sb, "  hk=%s", hk)
			}
			fmt.Fprintln(&sb)

			n := len(p.Entries)
			const showAllThreshold = 16
			if n <= showAllThreshold {
				for i, e := range p.Entries {
					if storage.BTreePageFlags(p.Flags)&storage.BTPLeaf != 0 {
						fmt.Fprintf(&sb, "    [%2d] key=%-6s  heap(%d,%d)\n",
							i, keyStr(e.Key), e.HeapBlk, e.HeapOff)
					} else {
						fmt.Fprintf(&sb, "    [%2d] key=%-6s  →blk %d\n",
							i, keyStr(e.Key), e.HeapBlk)
					}
				}
			} else {
				fmt.Fprintf(&sb, "    %d entries: %s … %s\n",
					n, keyStr(p.Entries[0].Key), keyStr(p.Entries[n-1].Key))
			}
		}
		fmt.Fprintln(&sb)
	}

	return sb.String()
}

// ── invariant checker ─────────────────────────────────────────────────────────

// Violation describes one broken structural invariant.
type Violation struct {
	Invariant string
	Block     storage.BlockNumber
	Detail    string
}

func (v Violation) String() string {
	return fmt.Sprintf("  ✗ %s  blk %d: %s", v.Invariant, v.Block, v.Detail)
}

// checkInvariants runs five structural checks over the tree snapshot and
// returns a list of violations. An empty list means the tree is healthy.
//
//  1. sorted-order    keys within each page are non-decreasing
//  2. high-key-bound  every data key on a non-rightmost page is ≤ its high key
//  3. sibling-links   A→B implies B←A (prev/next pointers are symmetric)
//  4. high-key-match  high key of page A equals first data key of its right sibling
//  5. no-incomplete   BTP_INCOMPLETE_SPLIT is not set on any page
func checkInvariants(levels [][]storage.PageInfo) []Violation {
	var vs []Violation

	// Build a block→PageInfo index for sibling look-ups.
	byBlock := make(map[storage.BlockNumber]storage.PageInfo)
	for _, pages := range levels {
		for _, p := range pages {
			byBlock[p.Block] = p
		}
	}

	for _, pages := range levels {
		for _, p := range pages {

			// 1. sorted-order
			for i := 1; i < len(p.Entries); i++ {
				if bytes.Compare(p.Entries[i-1].Key, p.Entries[i].Key) > 0 {
					vs = append(vs, Violation{
						"sorted-order", p.Block,
						fmt.Sprintf("entry[%d]=%s > entry[%d]=%s",
							i-1, keyStr(p.Entries[i-1].Key),
							i, keyStr(p.Entries[i].Key)),
					})
				}
			}

			// 2. high-key-bound
			if p.HighKey != nil {
				for i, e := range p.Entries {
					if bytes.Compare(e.Key, p.HighKey) > 0 {
						vs = append(vs, Violation{
							"high-key-bound", p.Block,
							fmt.Sprintf("entry[%d]=%s > highKey=%s",
								i, keyStr(e.Key), keyStr(p.HighKey)),
						})
					}
				}
			}

			// 3. sibling-links
			if p.Next != invalid {
				right, ok := byBlock[p.Next]
				if !ok {
					vs = append(vs, Violation{
						"sibling-links", p.Block,
						fmt.Sprintf("next=blk %d not found in snapshot", p.Next),
					})
				} else if right.Prev != p.Block {
					vs = append(vs, Violation{
						"sibling-links", p.Block,
						fmt.Sprintf("blk %d →next→ blk %d but blk %d ←prev= blk %d (want %d)",
							p.Block, p.Next, p.Next, right.Prev, p.Block),
					})
				}
			}

			// 4. high-key-match
			if p.HighKey != nil && p.Next != invalid {
				right, ok := byBlock[p.Next]
				if ok && len(right.Entries) > 0 {
					if !bytes.Equal(p.HighKey, right.Entries[0].Key) {
						vs = append(vs, Violation{
							"high-key-match", p.Block,
							fmt.Sprintf("highKey=%s ≠ right-sibling first key=%s",
								keyStr(p.HighKey), keyStr(right.Entries[0].Key)),
						})
					}
				}
			}

			// 5. no-incomplete
			if storage.BTreePageFlags(p.Flags)&storage.BTPIncomplete != 0 {
				vs = append(vs, Violation{
					"no-incomplete", p.Block,
					"BTP_INCOMPLETE_SPLIT is still set",
				})
			}
		}
	}

	return vs
}

func summariseInvariants(vs []Violation) string {
	checks := []string{
		"sorted-order", "high-key-bound", "sibling-links", "high-key-match", "no-incomplete",
	}
	failed := make(map[string]bool)
	for _, v := range vs {
		failed[v.Invariant] = true
	}
	var parts []string
	for _, c := range checks {
		if failed[c] {
			parts = append(parts, "✗ "+c)
		} else {
			parts = append(parts, "✓ "+c)
		}
	}
	return strings.Join(parts, "  ")
}

// ── tests ─────────────────────────────────────────────────────────────────────

func runCase(t *testing.T, nKeys uint32) (string, []Violation) {
	t.Helper()
	idx, cleanup := newIndex(t)
	defer cleanup()

	insertRange(t, idx, 0, nKeys)

	levels, err := idx.SnapshotTree()
	if err != nil {
		t.Fatalf("SnapshotTree: %v", err)
	}

	title := fmt.Sprintf("after %d inserts", nKeys)
	rendered := renderTree(levels, title)
	violations := checkInvariants(levels)
	return rendered, violations
}

// TestSinglePage: tree fits on one root+leaf page.
func TestSinglePage(t *testing.T) {
	rendered, vs := runCase(t, 10)
	t.Log("\n" + rendered)
	for _, v := range vs {
		t.Error(v)
	}
	if len(vs) == 0 {
		t.Logf("invariants: %s", summariseInvariants(vs))
	}
}

// TestFirstSplit: exactly enough keys to trigger the first leaf split.
func TestFirstSplit(t *testing.T) {
	rendered, vs := runCase(t, 510)
	t.Log("\n" + rendered)
	for _, v := range vs {
		t.Error(v)
	}
	if len(vs) == 0 {
		t.Logf("invariants: %s", summariseInvariants(vs))
	}
}

// TestMultiSplit: several leaf splits without a root split.
func TestMultiSplit(t *testing.T) {
	rendered, vs := runCase(t, 1000)
	t.Log("\n" + rendered)
	for _, v := range vs {
		t.Error(v)
	}
	if len(vs) == 0 {
		t.Logf("invariants: %s", summariseInvariants(vs))
	}
}

// TestTwoLevel: enough keys to promote the root to an internal page (two
// levels: one internal root + multiple leaf pages).
func TestTwoLevel(t *testing.T) {
	rendered, vs := runCase(t, 2000)
	t.Log("\n" + rendered)
	for _, v := range vs {
		t.Error(v)
	}
	if len(vs) == 0 {
		t.Logf("invariants: %s", summariseInvariants(vs))
	}
}

// TestReverseInsert: keys inserted in descending order stress the left-split
// path, which creates non-rightmost pages immediately.
func TestReverseInsert(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	const n = uint32(600)
	for k := n - 1; ; k-- {
		if err := idx.Insert(encKey(k), k, 1); err != nil {
			t.Fatalf("Insert(%d): %v", k, err)
		}
		if k == 0 {
			break
		}
	}

	levels, err := idx.SnapshotTree()
	if err != nil {
		t.Fatalf("SnapshotTree: %v", err)
	}

	rendered := renderTree(levels, fmt.Sprintf("after %d reverse-order inserts", n))
	t.Log("\n" + rendered)

	vs := checkInvariants(levels)
	for _, v := range vs {
		t.Error(v)
	}
	if len(vs) == 0 {
		t.Logf("invariants: %s", summariseInvariants(vs))
	}
}

// ── TestMain: print summary to stderr ────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Fprintln(os.Stderr, "=== tree-viz experiment ===")
	fmt.Fprintln(os.Stderr, "Renders B-tree pages (root → internal → leaf), sibling links,")
	fmt.Fprintln(os.Stderr, "high keys, and verifies 5 structural invariants.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Invariants checked:")
	fmt.Fprintln(os.Stderr, "  sorted-order   keys within each page are non-decreasing")
	fmt.Fprintln(os.Stderr, "  high-key-bound all data keys ≤ high key on non-rightmost pages")
	fmt.Fprintln(os.Stderr, "  sibling-links  A→B implies B←A")
	fmt.Fprintln(os.Stderr, "  high-key-match highKey(A) == first data key of A's right sibling")
	fmt.Fprintln(os.Stderr, "  no-incomplete  BTP_INCOMPLETE_SPLIT not set after full insert")
	fmt.Fprintln(os.Stderr)
	m.Run()
}
