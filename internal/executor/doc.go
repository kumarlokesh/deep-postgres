// Package executor implements PostgreSQL query execution internals.
//
// Planned subsystems:
//   - Executor node model (Plan → Tuple stream)
//   - SeqScan operator
//   - IndexScan operator (B-tree)
//   - Tuple routing and projection
//   - Execution tracing hooks
package executor
