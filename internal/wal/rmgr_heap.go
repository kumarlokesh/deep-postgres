package wal

// Heap resource manager (RmgrHeap / RmgrHeap2).
//
// Implements Redo for heap insert and heap delete.
// Mirrors src/backend/access/heap/heapam_xlog.c.
//
// xl_info layout for RmgrHeap (xlog_internal.h):
//   bits 7   (0x80): XLOG_HEAP_INIT_PAGE — page must be initialised before redo
//   bits 4-6 (0x70): operation sub-type
//   bits 0-3 (0x0F): generic XLR flags (XLRInfoMask)

import (
	"encoding/binary"
	"fmt"
)

// ── Operation constants ────────────────────────────────────────────────────────

const (
	xlHeapInsert    uint8 = 0x00
	xlHeapDelete    uint8 = 0x10
	xlHeapUpdate    uint8 = 0x20
	xlHeapHotUpdate uint8 = 0x40
	xlHeapOpMask    uint8 = 0x70 // mask for sub-type bits
	xlHeapInitPage  uint8 = 0x80 // page was freshly initialised
)

// ── xl_heap_insert ─────────────────────────────────────────────────────────────

// xlHeapInsertData mirrors PostgreSQL's xl_heap_insert (heapam_xlog.h):
//
//	uint16  offnum  — target offset number (1-based)
//	uint8   flags
const xlHeapInsertSize = 3

type xlHeapInsertData struct {
	Offnum uint16
	Flags  uint8
}

func decodeXLHeapInsert(data []byte) (xlHeapInsertData, error) {
	if len(data) < xlHeapInsertSize {
		return xlHeapInsertData{}, fmt.Errorf("heap: xl_heap_insert too short (%d bytes)", len(data))
	}
	return xlHeapInsertData{
		Offnum: binary.LittleEndian.Uint16(data[0:]),
		Flags:  data[2],
	}, nil
}

// ── xl_heap_delete ─────────────────────────────────────────────────────────────

// xlHeapDeleteData mirrors PostgreSQL's xl_heap_delete (heapam_xlog.h):
//
//	uint32  xmax            — xmax of the deleted tuple
//	uint16  target_offnum   — target offset number (1-based)
//	uint8   infobits_set
//	uint8   flags
const xlHeapDeleteSize = 8

type xlHeapDeleteData struct {
	Xmax        uint32
	TargetOffnum uint16
	InfobitsSet  uint8
	Flags        uint8
}

func decodeXLHeapDelete(data []byte) (xlHeapDeleteData, error) {
	if len(data) < xlHeapDeleteSize {
		return xlHeapDeleteData{}, fmt.Errorf("heap: xl_heap_delete too short (%d bytes)", len(data))
	}
	return xlHeapDeleteData{
		Xmax:         binary.LittleEndian.Uint32(data[0:]),
		TargetOffnum: binary.LittleEndian.Uint16(data[4:]),
		InfobitsSet:  data[6],
		Flags:        data[7],
	}, nil
}

// ── Redo dispatcher ───────────────────────────────────────────────────────────

func heapRedo(ctx RedoContext) error {
	op := ctx.Rec.Header.XlInfo & xlHeapOpMask
	switch op {
	case xlHeapInsert:
		return heapRedoInsert(ctx)
	case xlHeapDelete:
		return heapRedoDelete(ctx)
	case xlHeapUpdate, xlHeapHotUpdate:
		// Update redo is not yet implemented; silently skip.
		return nil
	default:
		return nil
	}
}

// ── heapRedoInsert ────────────────────────────────────────────────────────────

func heapRedoInsert(ctx RedoContext) error {
	rec := ctx.Rec
	if len(rec.BlockRefs) == 0 {
		return fmt.Errorf("heap insert: no block references")
	}
	br := rec.BlockRefs[0]

	// Full-page write: restore the page image.
	if br.Image != nil && br.Image.BimgInfo&BimgApply != 0 {
		if ctx.Store == nil {
			return nil
		}
		return ctx.Store.ApplyFPW(br.Reln, br.ForkNum, br.BlockNum, br.Image, ctx.LSN)
	}

	// Normal redo: parse xl_heap_insert, insert tuple.
	xlrec, err := decodeXLHeapInsert(rec.MainData)
	if err != nil {
		return err
	}

	initPage := rec.Header.XlInfo&xlHeapInitPage != 0

	if ctx.Store == nil {
		return nil
	}
	return ctx.Store.ApplyInsert(
		br.Reln, br.ForkNum, br.BlockNum,
		xlrec.Offnum, br.Data, initPage, ctx.LSN,
	)
}

// ── heapRedoDelete ────────────────────────────────────────────────────────────

func heapRedoDelete(ctx RedoContext) error {
	rec := ctx.Rec
	if len(rec.BlockRefs) == 0 {
		return fmt.Errorf("heap delete: no block references")
	}
	br := rec.BlockRefs[0]

	// Full-page write: restore the page image.
	if br.Image != nil && br.Image.BimgInfo&BimgApply != 0 {
		if ctx.Store == nil {
			return nil
		}
		return ctx.Store.ApplyFPW(br.Reln, br.ForkNum, br.BlockNum, br.Image, ctx.LSN)
	}

	// Normal redo: parse xl_heap_delete, mark item dead.
	xlrec, err := decodeXLHeapDelete(rec.MainData)
	if err != nil {
		return err
	}

	if ctx.Store == nil {
		return nil
	}
	return ctx.Store.ApplyDelete(
		br.Reln, br.ForkNum, br.BlockNum,
		xlrec.TargetOffnum, ctx.LSN,
	)
}

// ── Registration ──────────────────────────────────────────────────────────────

func init() {
	Register(RmgrHeap, RmgrOps{
		Name: "Heap",
		Redo: heapRedo,
		Identify: func(rec *Record) string {
			op := rec.Header.XlInfo & xlHeapOpMask
			opName := map[uint8]string{
				xlHeapInsert:    "INSERT",
				xlHeapDelete:    "DELETE",
				xlHeapUpdate:    "UPDATE",
				xlHeapHotUpdate: "HOT_UPDATE",
			}[op]
			if opName == "" {
				opName = fmt.Sprintf("op=0x%02X", op)
			}
			return fmt.Sprintf("Heap/%s %s xid=%d", opName, rec.LSN, rec.Header.XlXid)
		},
	})
}
