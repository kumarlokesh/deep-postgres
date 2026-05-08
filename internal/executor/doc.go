// Package executor implements PostgreSQL query execution internals.
//
// Subsystems:
//   - Executor node model (Plan → Tuple stream)
//   - SeqScan operator
//   - IndexScan operator (B-tree)
//   - Filter / Limit pipeline nodes
//   - Execution tracing hooks (TracedNode)
//   - Tuple projection (Project node, Schema, Column)
//   - Sort node (in-memory stable sort; mirrors nodeSort.c)
//   - HashAgg node (hash-based GROUP BY; mirrors nodeAgg.c AGG_HASHED)
//   - AggFn interface with COUNT, SUM, MIN, MAX built-ins
//   - NestedLoopJoin node (inner side materialized; mirrors nodeNestloop.c)
//   - HashJoin node (build/probe two-phase hash join; mirrors nodeHashjoin.c)
package executor
