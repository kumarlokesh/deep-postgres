# deep-postgres

PostgreSQL engine internals reconstructed in Go - storage, WAL, MVCC, and execution,
built subsystem by subsystem from the source.

## Subsystems

### Storage (`internal/storage/`)

Heap page layout (faithful to PostgreSQL's 8 KB `PageHeaderData` format),
B-tree index pages, buffer manager, and access methods.

- **Page layout**: `PageHeaderData` (24 bytes), `ItemIdData` (4-byte bit-packed line
  pointers with `LP_UNUSED`, `LP_NORMAL`, `LP_REDIRECT`, `LP_DEAD` states),
  `pd_flags` (`PD_HAS_FREE_LINES`, `PD_PAGE_FULL`, `PD_ALL_VISIBLE`),
  `pd_prune_xid`, `pd_lsn`
- **HOT chains**: `FollowRedirect()` resolves one `LP_REDIRECT` hop;
  `HeapHotSearchBuffer()` walks the full `t_ctid` chain applying MVCC visibility
- **Heap tuples**: `HeapTupleHeader` (24 bytes), `HEAP_HOT_UPDATED`, `HEAP_ONLY_TUPLE`,
  `HEAP_XMIN_COMMITTED` / `HEAP_XMAX_INVALID` hint bits; `HeapTupleSatisfiesMVCC`,
  `HeapTupleSatisfiesDirty`, `HeapTupleSatisfiesNow`, `HeapTupleSatisfiesAny`
- **Buffer manager**: clock-sweep eviction, packed atomic state word (bits 0-17 pin
  count, 18-22 usage count, 23-27 flags), content `RWMutex`, `BM_IO_IN_PROGRESS`
  coordination, WAL write-back hook
- **B-tree**: leaf/internal page ops, binary search, multi-level inserts with page
  splits, persistent `BTreeIndex` backed by `smgr`
- **TOAST**: out-of-line tuple storage - `Toastify` / `Detoast`, chunked storage
  relation, inline/external threshold
- **Vacuum**: `VacuumPage` (dead tuple pruning, HOT chain compaction, freezing),
  `CompactPage` (fragmentation reclaim), `VacuumFull` with trailing-page truncation
- **Free Space Map** (`FreeSpaceMap`), **Visibility Map** (`VisibilityMap`)
- **smgr**: `FileStorageManager` for block-granular fork-aware file I/O

### WAL (`internal/wal/`)

WAL record layout, redo engine, and resource manager dispatch - faithful to
PostgreSQL's `XLogRecord` wire format and crash recovery pipeline.

- **Record encoding**: `XLogRecord` 24-byte header, `BlockRef` / `BlockImage`,
  `RelFileLocator`, CRC-32C two-phase checksum; `Encode` / `Decode`
- **Page framing**: `XLogPageHeaderData` (24 B) / `XLogLongPageHeaderData` (40 B);
  `SegmentReader` with transparent page-boundary reassembly
- **Redo engine**: `RedoEngine` (start/end LSN, `SegmentProvider`, progress callback,
  `SetStore`, `SetLogical`, `SetTracer`); `InMemoryProvider`; `SegmentBuilder`
- **Resource managers**: `RmgrHeap` (INSERT, DELETE, UPDATE + HOT, FPW),
  `RmgrHeap2` (MULTI_INSERT, PRUNE, VACUUM, VISIBLE; FREEZE_PAGE/LOCK_UPDATED/NEW_CID/REWRITE acknowledged),
  `RmgrBtree` (leaf/upper/meta insert, split), `RmgrXact` (commit/abort),
  `RmgrXlog` (checkpoint, WAL-switch)
- **Logical decoding**: `ReorderBuffer` buffers per-XID changes and emits on commit;
  `ChangeHandler` interface; `LogicalDecoder` wired into `RedoEngine`
- **WAL–storage bridge**: `WalPageStore` implements `wal.PageWriter`, applying FPW,
  heap insert/delete/update, multi-insert, prune, vacuum dead-item removal,
  all-visible marking, and B-tree insert/split to the buffer pool
- **File-backed replay**: `FileSegmentProvider` reads real `pg_wal` directories;
  `ParseLSN`, `ListSegments`, `DetectSegmentSize`
- **CLI tool** (`cmd/walreplay`): `--wal-dir / --timeline / --start-lsn / --end-lsn`,
  auto-detects segment size, prints per-rmgr applied/skipped statistics

### MVCC (`internal/mvcc/`)

Snapshot manager and transaction lifecycle.

- **CLOG** (`Clog`): in-memory `pg_xact` equivalent; `Status` / `SetStatus`; special
  XID handling (`InvalidTransactionId` → aborted, `FrozenTransactionId` → committed)
- **Transaction manager** (`TransactionManager`): `Begin` / `Commit` / `Abort` /
  `AdvanceCommand`; `Snapshot` mirrors `GetSnapshotData` (xmin, xmax, Xip, CurCid);
  `GlobalXmin` for VACUUM horizon; implements `TransactionOracle`

### Executor (`internal/executor/`)

Query execution operators wired to the buffer pool and MVCC layer.

- **SeqScan**: forward heap scan with per-tuple `HeapTupleSatisfiesMVCC` filtering;
  VM all-visible fast path; hint-bit writeback
- **IndexScan**: B-tree equality lookup via `SearchAll`, then `HeapFetch` per TID
- **HeapFetch**: `LP_REDIRECT` resolution + `t_ctid` HOT chain walk with MVCC check
- **HeapInsert**: FSM-guided page selection, relation extension fallback, TOAST
  threshold check, VM / FSM update
- **HeapUpdate**: HOT update (same-page new version) with fallback to cross-page;
  old-tuple xmax / `HEAP_HOT_UPDATED` / `t_ctid` stamping
- **Vacuum** / **VacuumFull**: dead-tuple reclaim, HOT chain pruning, tuple freezing,
  FSM/VM refresh, trailing-page truncation

### Instrumentation (`internal/instrumentation/`)

- **Buffer benchmark harness**: `AccessPattern` interface; `SequentialPattern`,
  `RandomPattern`, `HotSetPattern`; `BenchmarkPolicy` measures hit ratio and
  eviction counts for any `EvictionPolicy`
- **BufferStats**: hit/miss/eviction/dirty-eviction counters via `BufferTracer`

## Experiments (`experiments/`)

Isolated benchmarks and correctness proofs.

| Experiment | What it shows |
| --- | --- |
| `pin-contention/` | Atomic CAS vs mutex for pin/unpin under 1000-goroutine contention; CAS is 4.3× faster at 4 cores |
| `mvcc-isolation/` | Eight MVCC snapshot isolation scenarios: dirty-read prevention, snapshot consistency, HOT chain visibility, command-counter fence |
| `buffer-eviction/` | ClockSweep vs LRU vs ARC under sequential, random, and hot-set workloads; ARC gains +7.6 pp on skewed workloads at a 13–38 % CPU cost |
| `crash-recovery/` | End-to-end crash recovery: WAL replay into a fresh pool, commit tracking via logical decoder, MVCC scan - uncommitted tx invisible, committed visible |

## Build and test

Prerequisites: Go (see `go.mod`), `golangci-lint` (optional).

```sh
make test          # run all tests
make test-race     # with -race detector
make bench         # run benchmarks
make lint          # golangci-lint
make build         # compile binaries
```

Run experiments directly:

```sh
go test -v ./experiments/mvcc-isolation/
go test -bench=. -benchtime=5s -cpu=1,4,8 ./experiments/pin-contention/
```

Replay a real `pg_wal` directory:

```sh
go run ./cmd/walreplay \
  --wal-dir /var/lib/postgresql/16/main/pg_wal \
  --timeline 1 \
  --start-lsn 0/1000000
```

## Docker

```sh
docker compose build
docker compose run --rm dev
```

Or via Makefile:

```sh
make docker-test
make docker-bench
```

## License

Apache-2.0 (see `LICENSE`).
