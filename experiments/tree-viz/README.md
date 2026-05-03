# tree-viz

Renders the full B-tree structure - root, internal, and leaf pages; sibling
links; high keys; entry ranges - and verifies five structural invariants after
each insert sequence.

## What it shows

After every insert workload the experiment calls `idx.SnapshotTree()`, which
reads every reachable page and returns them grouped by level (root first, leaves
last). The renderer formats each page as a compact block showing flags, sibling
pointers, the high key (if non-rightmost), and either every entry (≤ 16) or a
first/last/count summary.

```
B-tree: after 510 inserts
  root=blk 3  height=2  leaves=2  total_keys=510

── level 0 (internal, 1 page(s)) ───────────────────────────
  blk   3  [ROOT|INTERNAL]  ←·  →·
    [ 0] key=0       →blk 1
    [ 1] key=255     →blk 2

── level 1 (leaf, 2 page(s)) ───────────────────────────────
  blk   1  [LEAF]  ←·  →2  hk=255
    255 entries: 0 … 254
  blk   2  [LEAF]  ←1  →·
    255 entries: 255 … 509

invariants: ✓ sorted-order  ✓ high-key-bound  ✓ sibling-links  ✓ high-key-match  ✓ no-incomplete
```

## Invariants checked

| Invariant | Rule |
|---|---|
| `sorted-order` | Keys within each page are non-decreasing |
| `high-key-bound` | Every data key on a non-rightmost page is ≤ its high key |
| `sibling-links` | If A→next=B then B→prev=A (doubly-linked chain is symmetric) |
| `high-key-match` | `highKey(A)` equals the first data key of A's right sibling |
| `no-incomplete` | `BTP_INCOMPLETE_SPLIT` is not set on any page after a complete insert |

A violation is reported as a `t.Error` with the failing invariant name, the
offending block number, and a description of what was found vs. expected.

## Test cases

| Test | Keys | What it exercises |
|---|---|---|
| `TestSinglePage` | 10 | One root+leaf page; no high key (rightmost); all entries printed |
| `TestFirstSplit` | 510 | First leaf split; left page gets a high key; root promoted to internal |
| `TestMultiSplit` | 1 000 | Multiple leaf splits off one internal root; sibling chain grows |
| `TestTwoLevel` | 2 000 | Seven leaves; internal page shows all separator keys and child blocks |
| `TestReverseInsert` | 600 (desc) | Keys inserted right-to-left; tests non-rightmost split path immediately |

## Output walk-through (TestTwoLevel)

```
── level 0 (internal, 1 page(s)) ────────────────────────────
  blk   3  [ROOT|INTERNAL]  ←·  →·
    [ 0] key=0       →blk 1       ← leftmost child (placeholder key)
    [ 1] key=255     →blk 2       ← separator: keys ≥ 255 go right
    ...
    [ 6] key=1530    →blk 8

── level 1 (leaf, 7 page(s)) ────────────────────────────────
  blk   1  [LEAF]  ←·  →2  hk=255      ← non-rightmost: has high key
    255 entries: 0 … 254
  blk   2  [LEAF]  ←1  →4  hk=510
    255 entries: 255 … 509
  ...
  blk   8  [LEAF]  ←7  →·             ← rightmost: no high key
    470 entries: 1530 … 1999
```

- **`hk=255`** is the high key stored at physical slot 0 (`P_HIKEY`) on block 1.
  It equals the first data key of block 2, satisfying `high-key-match`.
- The `←·` / `→·` notation shows `btpo_prev` / `btpo_next` = `InvalidBlockNumber`.
  Symmetric pointers satisfy `sibling-links`.
- The root (block 3) is rightmost and has no high key - the root never has an
  upper bound because there is no right sibling at the root level.

## How `SnapshotTree` works

```
1. Read meta → get root block and tree height (meta.Level)
2. Descend via GetEntry(0).ChildBlk at each internal level
   to find the leftmost block at every depth
3. At each depth, walk BtpoNext from leftmost → collect all pages
4. Return levels[0..height], root-first
```

All buffer pins are released before returning; `PageInfo` / `EntryInfo` are
plain value types safe to read after the snapshot call.

## Running

```bash
go test -v ./experiments/tree-viz/
```

## Relationship to PostgreSQL source

| This experiment | PostgreSQL |
|---|---|
| `PageInfo.HighKey` | `P_HIKEY` slot in `nbtree.h` |
| `PageInfo.Flags` | `BTPageOpaqueData.btpo_flags` in `nbtree.h` |
| `sibling-links` invariant | `_bt_check_unique` / `bt_page_stats` in `pageinspect` extension |
| `high-key-match` invariant | `_bt_check_page_contents` in `src/backend/access/nbtree/nbtutils.c` |
| `no-incomplete` invariant | `BTPageIsIncompleteSplit` check in `nbtinsert.c` |
