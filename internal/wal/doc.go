// Package wal implements PostgreSQL Write-Ahead Log internals.
//
// Planned subsystems:
//   - XLogRecord layout and parsing (src/include/access/xlogrecord.h)
//   - WAL redo engine
//   - Logical decoding
//   - Replication client (streaming WAL)
package wal
