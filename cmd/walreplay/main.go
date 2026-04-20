// walreplay replays a real PostgreSQL pg_wal directory through the redo engine
// and prints per-resource-manager statistics.
//
// Usage:
//
//	walreplay --wal-dir /var/lib/postgresql/16/main/pg_wal \
//	          --timeline 1 \
//	          --start-lsn 0/400000 \
//	          [--end-lsn 0/800000]
//
// The tool reads WAL segments from --wal-dir, drives the RedoEngine, and on
// completion prints a summary of how many records each resource manager
// contributed.
//
// If the PostgreSQL instance was compiled with a non-default --wal-segment-size
// the tool detects it automatically from the long page header of the first
// segment.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/kumarlokesh/deep-postgres/internal/wal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "walreplay:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		walDir       = flag.String("wal-dir", "", "path to pg_wal directory (required)")
		timelineFlag = flag.Uint("timeline", 1, "WAL timeline ID")
		startLSNStr  = flag.String("start-lsn", "0/0", "start LSN (e.g. 0/400000)")
		endLSNStr    = flag.String("end-lsn", "", "end LSN, exclusive (empty = replay all)")
		verbose      = flag.Bool("v", false, "print each record as it is applied")
	)
	flag.Parse()

	if *walDir == "" {
		flag.Usage()
		return fmt.Errorf("--wal-dir is required")
	}

	startLSN, err := wal.ParseLSN(*startLSNStr)
	if err != nil {
		return fmt.Errorf("invalid --start-lsn: %w", err)
	}

	var endLSN wal.LSN
	if *endLSNStr != "" {
		endLSN, err = wal.ParseLSN(*endLSNStr)
		if err != nil {
			return fmt.Errorf("invalid --end-lsn: %w", err)
		}
	}

	tli := wal.TimeLineID(*timelineFlag)

	provider := wal.NewFileSegmentProvider(*walDir)

	// Auto-detect segment size from the first segment's long page header.
	detected, err := provider.DetectSegmentSize()
	if err != nil {
		return fmt.Errorf("detecting segment size: %w", err)
	}
	if detected == 0 {
		fmt.Fprintln(os.Stderr, "walreplay: warning: no segments found in", *walDir)
		return nil
	}
	if detected != wal.WALSegSize {
		fmt.Fprintf(os.Stderr, "walreplay: non-default segment size: %d bytes\n", detected)
		provider.SetSegmentSize(detected)
	}

	// Counters per resource manager.
	rmgrApplied := make(map[wal.RmgrID]uint64)
	rmgrSkipped := make(map[wal.RmgrID]uint64)

	var onProgress wal.RedoProgress
	if *verbose {
		onProgress = func(rec *wal.Record) {
			fmt.Printf("  %s  rmgr=%-16s xid=%-6d len=%d\n",
				rec.LSN,
				rec.Header.XlRmid,
				rec.Header.XlXid,
				rec.Header.XlTotLen,
			)
		}
	}

	engine := wal.NewRedoEngine(tli, startLSN, endLSN, provider.Provide, onProgress)

	// Tracer to collect per-rmgr counts without needing to re-register all rmgrs.
	engine.SetTracer(&countingTracer{applied: rmgrApplied, skipped: rmgrSkipped})

	fmt.Printf("walreplay: dir=%s timeline=%d start=%s", *walDir, tli, startLSN)
	if endLSN != 0 {
		fmt.Printf(" end=%s", endLSN)
	}
	fmt.Println()

	if err := engine.Run(); err != nil {
		return fmt.Errorf("replay failed: %w", err)
	}

	stats := engine.Stats()
	printSummary(stats, rmgrApplied, rmgrSkipped)
	return nil
}

func printSummary(stats wal.RedoStats, applied, skipped map[wal.RmgrID]uint64) {
	fmt.Println()
	fmt.Printf("Segments read:    %d\n", stats.SegmentsRead)
	fmt.Printf("Records applied:  %d\n", stats.RecordsApplied)
	fmt.Printf("Records skipped:  %d\n", stats.RecordsSkipped)
	fmt.Printf("Bytes processed:  %d\n", stats.BytesProcessed)
	fmt.Println()

	// Collect all rmgr IDs that appear in either map.
	seen := make(map[wal.RmgrID]struct{})
	for id := range applied {
		seen[id] = struct{}{}
	}
	for id := range skipped {
		seen[id] = struct{}{}
	}
	if len(seen) == 0 {
		return
	}

	ids := make([]int, 0, len(seen))
	for id := range seen {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RmgrID\tName\tApplied\tSkipped")
	fmt.Fprintln(tw, "------\t----\t-------\t-------")
	for _, id := range ids {
		rmid := wal.RmgrID(id)
		fmt.Fprintf(tw, "%d\t%s\t%d\t%d\n",
			rmid,
			rmid.String(),
			applied[rmid],
			skipped[rmid],
		)
	}
	tw.Flush()
}

// ── countingTracer ────────────────────────────────────────────────────────────

// countingTracer implements wal.RedoTracer and accumulates per-rmgr counts.
type countingTracer struct {
	applied map[wal.RmgrID]uint64
	skipped map[wal.RmgrID]uint64
}

func (t *countingTracer) OnSegment(lsn wal.LSN) {
	fmt.Fprintf(os.Stderr, "walreplay: reading segment at %s\n", lsn)
}

func (t *countingTracer) OnApply(lsn wal.LSN, rec *wal.Record) {
	t.applied[rec.Header.XlRmid]++
}

func (t *countingTracer) OnSkip(lsn wal.LSN, rec *wal.Record, err error) {
	t.skipped[rec.Header.XlRmid]++
}
