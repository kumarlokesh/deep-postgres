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

// ── xl_heap_update ─────────────────────────────────────────────────────────────

// xlHeapUpdateIsHot indicates a HOT update: old and new tuples share one page.
const xlHeapUpdateIsHot uint8 = 0x01

// xlHeapUpdateInitPage indicates the new page must be initialised before insert.
const xlHeapUpdateInitPage uint8 = 0x02

// xlHeapUpdateData mirrors the fixed portion of PostgreSQL's xl_heap_update:
//
//	uint16  old_offnum
//	uint8   old_infobits_set
//	uint8   flags           (0x01=HOT, 0x02=init new page)
//	uint16  new_offnum
//	uint16  _pad
const xlHeapUpdateSize = 8

type xlHeapUpdateData struct {
	OldOffnum   uint16
	OldInfobits uint8
	Flags       uint8
	NewOffnum   uint16
}

func decodeXLHeapUpdate(data []byte) (xlHeapUpdateData, error) {
	if len(data) < xlHeapUpdateSize {
		return xlHeapUpdateData{}, fmt.Errorf("heap: xl_heap_update too short (%d bytes)", len(data))
	}
	return xlHeapUpdateData{
		OldOffnum:   binary.LittleEndian.Uint16(data[0:]),
		OldInfobits: data[2],
		Flags:       data[3],
		NewOffnum:   binary.LittleEndian.Uint16(data[4:]),
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
	Xmax         uint32
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
		return heapRedoUpdate(ctx)
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

	// Logical decoding: record the insert before applying it physically.
	if ctx.Logical != nil {
		ctx.Logical.OnInsert(
			rec.Header.XlXid, ctx.LSN,
			br.Reln, br.BlockNum, xlrec.Offnum, br.Data,
		)
	}

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

	// Logical decoding.
	if ctx.Logical != nil {
		ctx.Logical.OnDelete(
			rec.Header.XlXid, ctx.LSN,
			br.Reln, br.BlockNum, xlrec.TargetOffnum,
		)
	}

	if ctx.Store == nil {
		return nil
	}
	return ctx.Store.ApplyDelete(
		br.Reln, br.ForkNum, br.BlockNum,
		xlrec.TargetOffnum, ctx.LSN,
	)
}

// ── heapRedoUpdate ────────────────────────────────────────────────────────────

func heapRedoUpdate(ctx RedoContext) error {
	rec := ctx.Rec
	if len(rec.BlockRefs) == 0 {
		return fmt.Errorf("heap update: no block references")
	}
	// BlockRef[0] = new block (carries new tuple data).
	newBR := rec.BlockRefs[0]

	// Full-page write on the new block.
	if newBR.Image != nil && newBR.Image.BimgInfo&BimgApply != 0 {
		if ctx.Store == nil {
			return nil
		}
		return ctx.Store.ApplyFPW(newBR.Reln, newBR.ForkNum, newBR.BlockNum, newBR.Image, ctx.LSN)
	}

	xlrec, err := decodeXLHeapUpdate(rec.MainData)
	if err != nil {
		return err
	}

	isHot := xlrec.Flags&xlHeapUpdateIsHot != 0
	initNewPage := xlrec.Flags&xlHeapUpdateInitPage != 0

	// For HOT: old block == new block.  For cross-page: old block is BlockRef[1].
	oldBlock := newBR.BlockNum
	if !isHot && len(rec.BlockRefs) > 1 {
		oldBlock = rec.BlockRefs[1].BlockNum
	}

	// Logical decoding: emit the new-image tuple.
	if ctx.Logical != nil {
		ctx.Logical.OnUpdate(
			rec.Header.XlXid, ctx.LSN,
			newBR.Reln, newBR.BlockNum, xlrec.NewOffnum, newBR.Data,
		)
	}

	if ctx.Store == nil {
		return nil
	}
	return ctx.Store.ApplyUpdate(
		newBR.Reln, newBR.ForkNum,
		rec.Header.XlXid,
		oldBlock, newBR.BlockNum,
		xlrec.OldOffnum, xlrec.NewOffnum,
		newBR.Data,
		isHot, initNewPage,
		ctx.LSN,
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
