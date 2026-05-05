// Package executor implements PostgreSQL query execution internals.
//
// Subsystems:
//   - Executor node model (Plan → Tuple stream)
//   - SeqScan operator
//   - IndexScan operator (B-tree)
//   - Filter / Limit pipeline nodes
//   - Execution tracing hooks (TracedNode)
//   - Tuple projection (Project node, Schema, Column)
package executor
