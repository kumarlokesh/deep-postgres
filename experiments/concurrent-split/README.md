# concurrent-split

Tests B-tree correctness under concurrent insertion and demonstrates right-link
traversal: after a page split, a goroutine holding a stale leaf-page reference
follows `BtpoNext` to find a key that migrated to the right sibling.

## The problem

PostgreSQL's nbtree uses a technique called **right-link traversal** (the
Lehman-Yao B-link tree) to handle the window between a page split and the
insertion of the parent downlink:

1. **Split begins**: left page is rewritten with entries `[0, splitPos)` and
   `BtpoNext` is set to the new right page. A high key (the first key of the
   right page) is written at slot 0 of the left page. `BTP_INCOMPLETE_SPLIT`
   is set on the left page.
2. **Parent downlink inserted**: the separator key is pushed up to the parent
   internal page. `BTP_INCOMPLETE_SPLIT` is cleared.

If a goroutine descended to the left page *before* step 1 and the split then
occurs, its cached pointer now points to a page whose high key is less than the
search key. The rule is:

```
if searchKey > page.HighKey() → follow page.BtpoNext (right-link)
```

The same rule fires when `BTP_INCOMPLETE_SPLIT` is set: the parent may not yet
know about the right page, so descent must follow the right-link rather than
trust the parent's downlinks.

## Architecture

```
safeIndex (sync.RWMutex wrapper)
    │  Lock() for Insert
    │  RLock() for SearchAll
    ▼
BTreeIndex
    ├── Insert → findLeaf → insertAtLeaf → splitLeaf
    │                ↑
    │         right-link check: key > highKey → follow BtpoNext
    │         BTPIncomplete check → follow BtpoNext
    │
    └── SearchFromBlock(staleBlk, key)
              starts findLeaf at staleBlk instead of root
              exercises right-link traversal on the stale ref
```

The `safeIndex` wrapper serialises concurrent access with a `sync.RWMutex`
(multiple readers, exclusive writers), making `BTreeIndex` goroutine-safe
without modifying its internals.

## High key layout

On every non-rightmost page, slot 0 (`P_HIKEY`) holds the high key - the
exclusive upper bound for keys on this page. Data entries start at slot 1
(`P_FIRSTDATAKEY`). Rightmost pages have no high key (slot 0 is a data entry).

```
Non-rightmost leaf:
  slot 0: high key  (upper bound, zero TID)
  slot 1: data entry (key₀, heapTID₀)
  slot 2: data entry (key₁, heapTID₁)
  ...

Rightmost leaf:
  slot 0: data entry (key₀, heapTID₀)
  slot 1: data entry (key₁, heapTID₁)
  ...
```

`NumEntries()`, `GetEntry(i)`, and `InsertEntrySortedAt(pos, ...)` all apply a
`dataOffset()` (0 for rightmost, 1 otherwise) so callers always work in
data-entry coordinates.

## Tests

### TestConcurrentInsertDisjoint

4 goroutines each insert 250 keys in a disjoint range (keys 0–999), forcing
several leaf splits. After all goroutines finish, every key is searched and
must be found.

This verifies that concurrent splits never corrupt sibling pointers or lose
entries. Run with `-race` to confirm no data races.

### TestConcurrentReadsDuringWrites

1 writer inserts keys 0–799 in order, advancing an atomic frontier after each
committed insert. 3 reader goroutines continuously search for the most recently
committed key. Because the frontier is updated *after* the write lock is
released, readers never ask for a key that is not yet in the tree.

This verifies that concurrent reads and writes coexist correctly.

### TestRightLinkTraversal

The core right-link test:

1. Insert 509 keys (keys 0–508) - fills the initial rightmost leaf exactly.
2. Record `staleBlk = FindLeaf(key=400)` - the block that currently holds key 400.
3. Insert key 509, triggering the first split:
   - Left page (block `staleBlk`): keys 0–254, high key = 255.
   - Right page (new block): keys 255–509, rightmost (no high key).
4. Call `SearchFromBlock(staleBlk, key=400)`.

`SearchFromBlock` calls `findLeaf` starting at `staleBlk`. The high key of the
left page is 255 and `400 > 255`, so `findLeaf` follows `BtpoNext` to the right
page and finds key 400.

```
staleBlk (left page after split):
  high key = 255
  data: 0 ... 254

  key=400 > 255 → follow BtpoNext →

right page:
  no high key (rightmost)
  data: 255 ... 509  ← key 400 found here ✓
```

## Results

```
=== RUN   TestConcurrentInsertDisjoint
--- PASS  (0.14s)   1 000 keys, 0 missing

=== RUN   TestConcurrentReadsDuringWrites
--- PASS  (0.13s)   800 committed keys, 0 missed by readers

=== RUN   TestRightLinkTraversal
    right-link traversal: key 400 found correctly starting from stale block 1
--- PASS  (0.07s)
```

All three tests pass with `-race`.

## Running

```bash
go test -race -v ./experiments/concurrent-split/
```

## Relationship to PostgreSQL source

| This experiment | PostgreSQL |
|---|---|
| High key at `P_HIKEY` (slot 0) | `nbtree.h`: `P_HIKEY`, `P_FIRSTDATAKEY` |
| `BTP_INCOMPLETE_SPLIT` lifecycle | `_bt_split` / `_bt_insert_parent` in `nbtinsert.c` |
| Right-link traversal (`key > highKey → BtpoNext`) | `_bt_moveright` in `nbtinsert.c` |
| `SearchFromBlock` stale-pointer scenario | Latch coupling / page latch hand-off in `nbtree.c` |
