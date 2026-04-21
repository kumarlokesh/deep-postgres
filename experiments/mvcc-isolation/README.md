# mvcc-isolation

Demonstrates PostgreSQL's MVCC snapshot isolation semantics by interleaving
operations from multiple virtual transactions and asserting that each
transaction sees exactly the rows it should.

## What is being tested

PostgreSQL implements **Snapshot Isolation (SI)**: each transaction works
against a consistent point-in-time snapshot taken at the start of the first
command.  Reads never block writes and writes never block reads — isolation is
achieved by keeping multiple row versions on the heap and filtering by XID.

The engine under test is `internal/storage` + `internal/mvcc` +
`internal/executor`, wired together:

```
TransactionManager.Begin/Commit/Abort  →  Snapshot   →  SeqScan / HeapFetch
                  ↓                                          ↑
           CLOG (pg_xact)            HeapTupleSatisfiesMVCC (xmin/xmax checks)
```

## Scenarios

| Test | What it proves |
|---|---|
| `TestReadYourOwnWrites` | A transaction sees its own uncommitted inserts after `AdvanceCommand` advances the CID fence |
| `TestDirtyReadPrevention` | Uncommitted inserts by a peer transaction are invisible (XID is in snapshot's `Xip`) |
| `TestSnapshotConsistency` | Rows committed after a snapshot was taken do not appear in that snapshot (`Xmax` fence) |
| `TestDeleteIsolation` | A row deleted and committed after the snapshot was taken is still visible to the old snapshot |
| `TestAbortedInsertInvisible` | An aborted transaction leaves no visible rows; the CLOG marks xmin as aborted |
| `TestWriteWriteInterleaving` | Two concurrent transactions each see only their own rows; neither sees the other's until commit |
| `TestHOTUpdateChainVisibility` | An old snapshot reaches V1 via `HeapFetch`'s HOT chain walk; a new snapshot reaches V2 |
| `TestCommandCounterVisibility` | The CID fence (`TCid < CurCid`) prevents a statement from seeing rows it inserted in the same command |

## The command-counter fence

The `TestReadYourOwnWrites` and `TestWriteWriteInterleaving` scenarios both
call `AdvanceCommand` before scanning.  This is not an accident — it mirrors
exactly what PostgreSQL does after every SQL statement (`CommandCounterIncrement`
in `xact.c`).

The fence exists so that a single `UPDATE ... WHERE` statement cannot scan its
own newly-inserted rows in the same pass.  The rule is:

```
visible if: tuple.TCid < snapshot.CurCid
```

A tuple inserted at CID=0 is invisible to a scan taken at CID=0.  After
`AdvanceCommand`, CurCid becomes 1, and the CID=0 tuple becomes visible.

## The snapshot structure

```
Snapshot {
    Xid     — the current transaction's own XID (self-insert path)
    Xmin    — all XIDs < Xmin are visible (if committed)
    Xmax    — all XIDs >= Xmax are invisible (committed after snapshot)
    Xip     — in-progress XIDs in [Xmin, Xmax) (invisible)
    CurCid  — command ID fence for self-visibility
}
```

`XidVisible(xid)` implements the four-way check:

```
FrozenTransactionId         → always visible
InvalidTransactionId        → never visible
xid >= Xmax                 → invisible (future commit)
xid < Xmin                  → visible (old committed tx)
xid in [Xmin, Xmax)         → visible only if NOT in Xip
```

## Running

```bash
go test -v ./experiments/mvcc-isolation/
```

## Relationship to PostgreSQL source

| This experiment | PostgreSQL |
|---|---|
| `HeapTupleSatisfiesMVCC` | `src/backend/access/heap/heapam_visibility.c` |
| `TransactionManager.Snapshot` | `GetSnapshotData` in `src/backend/storage/ipc/procarray.c` |
| `Clog.Status` | `TransactionIdGetStatus` in `src/backend/access/transam/clog.c` |
| `AdvanceCommand` | `CommandCounterIncrement` in `src/backend/access/transam/xact.c` |
| `HeapFetch` HOT chain walk | `heap_hot_search_buffer` in `src/backend/access/heap/heapam.c` |
