# deep-postgres

PostgreSQL internals, engineered end-to-end.

deep-postgres reconstructs core PostgreSQL engine subsystems in Go for learning,
correctness-first implementation, and experimentation:

- **Storage**: heap page layout, heap tuples, buffer manager, B-tree indexes
- **WAL**: record/page parsing, redo, (prototype) logical decoding
- **MVCC**: snapshots, visibility rules, transaction semantics
- **Executor**: small execution engine for scanning and operator behavior (planned)

## Status

This is a research/education codebase. It is **not** a production database.

- **Storage**: complete
- **WAL**: in progress
- **MVCC**: in progress
- **Executor**: pending

## Repository layout

- **`cmd/deep-postgres/`**: main entrypoint
- **`internal/storage/`**: page/tuple layout, buffer pool, smgr, B-tree index
- **`internal/wal/`**: WAL record/page parsing, resource managers, redo engine
- **`internal/mvcc/`**: CLOG, transaction manager, snapshot/visibility logic
- **`internal/executor/`**: execution operators (SeqScan/IndexScan planned)
- **`internal/instrumentation/`**: tracing/logging hooks

## Build and test

Prerequisites:

- Go toolchain (see `go.mod`)
- `golangci-lint` (optional, for `make lint`)
- Docker (optional, for containerized dev)

Common targets:

```sh
make build
make test
make test-race
make lint
make bench
```

Run the binary:

```sh
./bin/deep-postgres
```

## Docker

```sh
docker compose build
docker compose run --rm dev
```

Or via Makefile helpers:

```sh
make docker-test
make docker-bench
```

## License

Apache-2.0 (see `LICENSE`).
