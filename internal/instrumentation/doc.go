// Package instrumentation provides tracing, metrics, and experimental hooks
// for inspecting deep-postgres subsystem behaviour.
//
// Planned subsystems:
//   - Structured event logging per subsystem (WAL, buffer, MVCC)
//   - pprof integration helpers
//   - Subsystem-level benchmark harness
//   - Crash simulation hooks
package instrumentation
