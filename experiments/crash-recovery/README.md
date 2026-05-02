# crash-recovery

End-to-end crash recovery: a WAL segment is replayed into a cold buffer pool,
a commit tracker populates a transaction oracle, and MVCC visibility rules
separate committed rows from in-flight ones - the same pipeline PostgreSQL runs
at startup after an unclean shutdown.

## The problem crash recovery solves

When PostgreSQL crashes, the on-disk heap pages may contain tuples from
transactions that never committed. The WAL records enough information to:

1. **Redo** every physical page change up to the crash point.
2. **Identify** which transactions committed (via `XLOG_XACT_COMMIT` records).
3. **Hide** tuples whose xmin was never committed, using MVCC visibility rules.

This experiment reconstructs all three steps from scratch.

## Architecture

```
WAL segment (in-memory)
        │
        ▼
  RedoEngine.Run()
        │
        ├─── WalPageStore (PageWriter)
        │         applies HEAP_INSERT / HEAP_MULTI_INSERT / etc.
        │         to a fresh BufferPool (cold start)
        │
        └─── commitTracker (LogicalDecoder)
                  intercepts XACT_COMMIT → oracle.Commit(xid)
                  intercepts XACT_ABORT  → oracle.Abort(xid)

After replay:
  HeapTupleSatisfiesMVCC(header, snap, oracle)
        ├─ xmin committed → visible
        └─ xmin in-progress or aborted → invisible
```

The `commitTracker` is a thin `wal.LogicalDecoder` that does nothing on
INSERT/UPDATE/DELETE events; it only watches commit and abort records. The
resulting `SimpleTransactionOracle` is passed to `HeapTupleSatisfiesMVCC` in
place of the live `CLOG`, reproducing what PostgreSQL does via `pg_xact` after
redo.

## Scenario

Three transactions write to the same heap page before a simulated crash:

```
tx10: INSERT "committed-a"  →  COMMIT         (written to WAL)
tx11: INSERT "in-flight"    →  [crash]         (no COMMIT or ABORT record)
tx12: INSERT "committed-b"  →  COMMIT         (written to WAL)
```

The WAL segment ends here. During replay:

- All three INSERT records are applied - every tuple lands on the page.
- The two COMMIT records cause `oracle.Commit(10)` and `oracle.Commit(12)`.
- tx11 is never committed, so `oracle.Status(11)` returns `TxInProgress`.

Post-recovery MVCC scan with a snapshot taken after replay:

| Tuple | xmin | Oracle status | Visible? |
|---|---|---|---|
| `committed-a` | 10 | committed | yes |
| `in-flight` | 11 | in-progress | **no** |
| `committed-b` | 12 | committed | yes |

## Tests

| Test | Scenario | Expected visible |
|---|---|---|
| `TestCrashRecovery` | 2 committed + 1 in-flight (no commit record) | `committed-a`, `committed-b` |
| `TestCrashRecoveryExplicitAbort` | 1 aborted (XLOG_XACT_ABORT present) + 1 committed | `present` only |
| `TestCrashRecoveryAllCommitted` | 3 committed | all 3 |

## Results

```
Scenario                   Physical tuples  Visible  Invisible
--------                   ---------------  -------  ---------
2-committed + 1-in-flight  3                2        1
1-committed + 1-aborted    2                1        1
all-committed              2                2        0
```

Physical tuples = total line pointers on the page after WAL redo. All tuples
are replayed regardless of commit status - the heap is restored exactly as it
was at crash time. MVCC visibility is a read-time filter, not a redo filter.

## Running

```bash
go test -v ./experiments/crash-recovery/
```

## Relationship to PostgreSQL source

| This experiment | PostgreSQL |
|---|---|
| `RedoEngine.Run` | `StartupXLOG` in `src/backend/access/transam/xlog.c` |
| `WalPageStore` rmgr dispatch | `RmgrTable[rmid].rm_redo` in `xlog.c` |
| `commitTracker.OnCommit` | `xact_redo` → `TransactionIdCommitTree` in `xact.c` |
| `SimpleTransactionOracle` | `TransactionIdGetStatus` via `pg_xact` (CLOG) |
| `HeapTupleSatisfiesMVCC` | `HeapTupleSatisfiesMVCC` in `heapam_visibility.c` |
| `XLOG_HEAP_INIT_PAGE` flag | Initialises page before first tuple during redo |
