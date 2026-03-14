// Package mvcc implements PostgreSQL Multi-Version Concurrency Control.
//
// Planned subsystems:
//   - Full snapshot manager
//   - HeapTupleSatisfies* family (all snapshot types)
//   - HOT chain traversal and cleanup
//   - Transaction ID wrap-around handling
//   - CLOG (pg_xact) simulation
package mvcc
