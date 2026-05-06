# vacuum-autovacuum

Demonstrates the three-act lifecycle of dead tuple management in PostgreSQL:
bloat accumulation, vacuum reclamation, and a simplified autovacuum control
loop.

## What it shows

```
INSERT rows  →  DELETE rows  →  heap bloat (LP_NORMAL with committed xmax)
                                      │
                               VACUUM pass
                                      │
                          LP_UNUSED slots (reusable via FSM)
                          FreezeLimit → FrozenTransactionId
                                      │
                            VACUUM FULL pass
                                      │
                        CompactPage: pd_upper reclaimed
                        Bytes freed = new FreeSpace - old FreeSpace
```

## The bloat problem

When PostgreSQL deletes a row it stamps `xmax` on the tuple header but leaves
the line pointer (`LP_NORMAL`) in place. The heap page grows "bloated": it
contains dead tuples that no future snapshot can see but still consume space.

```
Before vacuum:
  blk 0: total=100 live=50 dead=50 unused=0 free=4968

After VACUUM:
  blk 0: total=100 live=50 dead=0 unused=50 free=4968  ← LP_UNUSED, FSM updated

After VACUUM FULL (+ CompactPage):
  blk 0: total=100 live=50 dead=0 unused=50 free=8232  ← pd_upper moved up
```

## Regular VACUUM vs VACUUM FULL

| Step | Regular VACUUM | VACUUM FULL |
|---|---|---|
| Dead LP_NORMAL → LP_UNUSED | ✓ | ✓ |
| Update Free Space Map | ✓ | ✓ |
| Update Visibility Map | ✓ | ✓ |
| Freeze old xmin values | ✓ | ✓ |
| Move pd_upper (CompactPage) | ✗ | ✓ |
| `FreeSpace()` increases | ✗ | ✓ |
| Trailing empty-page truncation | ✗ | ✓ (if ItemCount=0) |

Regular VACUUM's freed capacity appears as LP_UNUSED slots (not raw
`FreeSpace()`). New inserts pick them up through the FSM without extending
the relation - demonstrated in `TestFSMReuse`.

## XID freezing

Tuples whose `xmin` is more than `FreezeMinAge` transactions older than
`OldestXmin` have their xmin replaced with `FrozenTransactionId` (XID=2).
Frozen tuples are permanently visible - `HeapTupleSatisfiesMVCC` skips the
oracle lookup for them. This prevents XID wraparound after ~2 billion
transactions.

## Autovacuum simulation

`TestAutovacuumSimulation` implements a threshold-based control loop analogous
to PostgreSQL's `autovacuum_vacuum_scale_factor` (default 20%):

```
for each round:
    insert 30 rows, delete 12
    compute dead_ratio = dead / (live + dead)
    if dead_ratio > 25%: fire Vacuum
```

Sample output:

```
round  live     dead     ratio    vacuum?  removed
────────────────────────────────────────────────────────────────────────
1      18       12       40.0    % YES      12
2      36       12       25.0    % no       -
3      54       24       30.8    % YES      24
4      72       12       14.3    % no       -
5      90       24       21.1    % no       -
────────────────────────────────────────────────────────────────────────
autovacuum fired 2/5 rounds (threshold=25%)
```

## Tests

| Test | What it verifies |
|---|---|
| `TestBloatAndVacuum` | 100 inserts, 50 deletes: dead→LP_UNUSED count matches; FreeSpace unchanged (no compaction in regular Vacuum); SeqScan sees 50 rows |
| `TestVacuumFullReclaims` | 400 inserts, 395 deletes: VacuumFull reports BytesReclaimed>0, PagesCompacted>0; 5 rows remain visible |
| `TestFreezeOldXIDs` | advance XID counter 100 steps; Vacuum freezes all 20 old tuples; post-freeze SeqScan still returns 20 rows |
| `TestAutovacuumSimulation` | 5 rounds; autovacuum fires when dead ratio exceeds 25%; fires at least once; relation remains queryable |
| `TestFSMReuse` | insert 100, delete all, vacuum; re-insert 100: block count does not grow (FSM reuses freed slots) |

## Running

```bash
go test -v ./experiments/vacuum-autovacuum/
```

## Relationship to PostgreSQL source

| This experiment | PostgreSQL |
|---|---|
| `executor.Vacuum` | `lazy_scan_heap` in `src/backend/commands/vacuumlazy.c` |
| `executor.VacuumFull` | `full_vacuum_rel` in `vacuumfull.c` |
| `storage.VacuumPage` | `heap_page_prune` in `heapam.c` |
| `storage.CompactPage` | `PageRepairFragmentation` in `bufpage.c` |
| `FreezeLimit` / frozen tuples | `vacuum_freeze_min_age`, `FreezeLimit` in `vacuum.c` |
| Dead-ratio threshold | `autovacuum_vacuum_scale_factor` in `autovacuum.c` |
| `FSM.Update` after vacuum | `RecordPageWithFreeSpace` in `freespace.c` |
| `VM.SetAllVisible` | `visibilitymap_set` in `visibilitymap.c` |
