# executor-pipeline

Integrated sandbox demonstrating the full deep-postgres execution stack: heap
storage, MVCC snapshots, and a multi-node executor pipeline working together
end-to-end.

## What it shows

```
SeqScan → Filter → Project → Limit → TracedNode
```

Each node in the chain maps directly to a PostgreSQL executor concept:

| Node | PostgreSQL analogue |
|---|---|
| `SeqScan` | `nodeSeqscan.c` + `heapam.c:heapgettup` |
| `Filter` | `ExecQual` in `execExpr.c` |
| `Project` | `ExecProject` + `TupleTableSlot` in `execTuples.c` |
| `Limit` | `nodeLimit.c` |
| `TracedNode` | `InstrStartNode / InstrStopNode` in `instrument.h` |

## Row layout

Each row packs three fixed-width `uint32` columns as big-endian bytes:

```
[0:4]  id        row number (0-based)
[4:8]  score     id × 7
[8:12] category  id % 5
```

`Schema` + `Column` describe these offsets so `Project` can extract just the
columns the query needs.

## Query modelled

```sql
SELECT id, score
FROM   t
WHERE  category = 2
LIMIT  5
```

The pipeline is:

1. `SeqScan` - reads all heap pages, applies MVCC visibility per tuple.
2. `Filter(category == 2)` - discards rows where `category` ≠ 2.
3. `Project(id, score)` - strips the `category` column; output tuples are 8 B.
4. `Limit(5)` - stops after the first five matching rows.
5. `TracedNode("Pipeline")` - wraps the whole tree; fires one event per
   lifecycle point.

## EXPLAIN ANALYZE-style output

`TestPipelineTrace` prints each trace event as a plan line:

```
=== EXPLAIN ANALYZE ===
[Pipeline]      open
                #1     id=0    score=0
                #2     id=5    score=35
                #3     id=10   score=70
                #4     id=15   score=105
[Pipeline]      close  (rows=4)
```

- `open` fires on the first `Next` call (node initialised).
- Each `#N` line is one tuple emitted upward through the pipeline.
- `close (rows=N)` fires when `Close` is called; `rows` is the total count.

## Tests

| Test | What it verifies |
|---|---|
| `TestPipelineCorrectness` | Filter + Project + Limit produces the exact ids 2, 7, 12, 17, 22; projected tuples are 8 B (no `category` column) |
| `TestPipelineTrace` | TracedNode emits 1 open + N tuple + 1 close events; close carries the final row count |
| `TestMVCCIsolation` | Snapshot taken before a second insert batch sees only the first batch; fresh snapshot sees both - the pipeline honours MVCC boundaries |
| `TestEmptyResult` | Pipeline with a never-matching Filter returns zero tuples without error |

## Running

```bash
go test -v ./experiments/executor-pipeline/
```

## Relationship to PostgreSQL source

| This experiment | PostgreSQL |
|---|---|
| `executor.Node` interface | `ExecProcNode` dispatch in `execProcnode.c` |
| `executor.Schema` / `Column` | `TupleDesc` + `Form_pg_attribute` in `tupdesc.h` |
| `executor.Project` | `ExecProject` in `execTuples.c` |
| `executor.TracedNode` | `InstrStartNode` / `InstrStopNode` in `instrument.h` |
| MVCC snapshot boundary | `GetSnapshotData` in `procarray.c` |
