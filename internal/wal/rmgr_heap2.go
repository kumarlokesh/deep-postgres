package wal

// Heap2 resource manager (RmgrHeap2, ID=9).
//
// Covers the extended heap operations that PostgreSQL groups under RM_HEAP2_ID:
// MULTI_INSERT, PRUNE (HOT chain compaction), VACUUM (dead-tuple removal),
// FREEZE_PAGE (XID freeze), VISIBLE (all-visible marking), and three minor
// ops (LOCK_UPDATED, NEW_CID, REWRITE).
//
// Mirrors src/backend/access/heap/heapam_xlog.c — heap2_redo().

import (
	"encoding/binary"
	"fmt"
)

// ── HEAP2 sub-operation constants ─────────────────────────────────────────────

// HEAP2 info-byte operation codes (xl_info & xlHeap2OpMask).
// Bit 7 (xlHeapInitPage = 0x80) may be OR-ed into any insert op.
const (
	xlHeap2Rewrite     uint8 = 0x00
	xlHeap2Prune       uint8 = 0x10
	xlHeap2Vacuum      uint8 = 0x20
	xlHeap2FreezePage  uint8 = 0x30
	xlHeap2Visible     uint8 = 0x40
	xlHeap2MultiInsert uint8 = 0x50
	xlHeap2LockUpdated uint8 = 0x60
	xlHeap2NewCid      uint8 = 0x70
	xlHeap2OpMask      uint8 = 0x70
)

// heap2TupleHeaderSize is the fixed size of our on-disk HeapTupleHeader.
// Matches storage.HeapTupleHeaderSize; redeclared here to avoid an import cycle.
const heap2TupleHeaderSize = 24

// ── xl_heap_multi_insert ───────────────────────────────────────────────────────

// xl_heap_multi_insert layout (heapam_xlog.h, SizeOfHeapMultiInsert = 4):
//
//	uint8  flags   (offset 0)
//	uint8  pad     (offset 1)
//	uint16 ntuples (offset 2, little-endian)
const xlHeapMultiInsertSize = 4

type xlHeapMultiInsertData struct {
	Flags   uint8
	NTuples uint16
}

func decodeXLHeapMultiInsert(data []byte) (xlHeapMultiInsertData, error) {
	if len(data) < xlHeapMultiInsertSize {
		return xlHeapMultiInsertData{}, fmt.Errorf("heap2: xl_heap_multi_insert too short (%d bytes)", len(data))
	}
	return xlHeapMultiInsertData{
		Flags:   data[0],
		NTuples: binary.LittleEndian.Uint16(data[2:]), // skip pad byte at data[1]
	}, nil
}

// xl_multi_insert_tuple layout (heapam_xlog.h, SizeOfMultiInsertTuple = 7):
//
//	uint16 datalen    (offset 0)
//	uint16 t_infomask2 (offset 2)
//	uint16 t_infomask  (offset 4)
//	uint8  t_hoff      (offset 6)
//	<datalen bytes of tuple data follow>
const xlMultiInsertTupleHeaderSize = 7

type xlMultiInsertTuple struct {
	DataLen    uint16
	TInfomask2 uint16
	TInfomask  uint16
	THoff      uint8
}

// parseMultiInsertTuples reconstructs full HeapTuple byte slices from the
// MULTI_INSERT block-data section (BlockRef[0].Data).
//
// xid becomes TXmin; block is the target page (written into TCtidBlock).
// Entries are SHORTALIGN-ed (2-byte aligned) between tuples, matching PG.
func parseMultiInsertTuples(xid, block uint32, ntuples uint16, blockData []byte) ([][]byte, error) {
	tuples := make([][]byte, 0, ntuples)
	pos := 0
	for i := uint16(0); i < ntuples; i++ {
		// SHORTALIGN: advance to next 2-byte boundary between entries.
		if pos%2 != 0 {
			pos++
		}
		if pos+xlMultiInsertTupleHeaderSize > len(blockData) {
			return nil, fmt.Errorf("heap2: multi_insert tuple %d: header truncated at pos %d", i, pos)
		}
		hdr := xlMultiInsertTuple{
			DataLen:    binary.LittleEndian.Uint16(blockData[pos:]),
			TInfomask2: binary.LittleEndian.Uint16(blockData[pos+2:]),
			TInfomask:  binary.LittleEndian.Uint16(blockData[pos+4:]),
			THoff:      blockData[pos+6],
		}
		pos += xlMultiInsertTupleHeaderSize

		if pos+int(hdr.DataLen) > len(blockData) {
			return nil, fmt.Errorf("heap2: multi_insert tuple %d: data truncated (need %d, have %d)", i, hdr.DataLen, len(blockData)-pos)
		}
		data := blockData[pos : pos+int(hdr.DataLen)]
		pos += int(hdr.DataLen)

		// Reconstruct the full 24-byte HeapTupleHeader + data.
		tup := make([]byte, heap2TupleHeaderSize+int(hdr.DataLen))
		binary.LittleEndian.PutUint32(tup[0:], xid)                // TXmin
		binary.LittleEndian.PutUint32(tup[4:], 0)                  // TXmax = 0 (InvalidTransactionId)
		binary.LittleEndian.PutUint32(tup[8:], 0)                  // TCid = 0 (FirstCommandId)
		binary.LittleEndian.PutUint32(tup[12:], block)              // TCtidBlock (self-ref block)
		binary.LittleEndian.PutUint16(tup[16:], 0)                  // TCtidOffset (updated after insert)
		binary.LittleEndian.PutUint16(tup[18:], hdr.TInfomask2)
		binary.LittleEndian.PutUint16(tup[20:], hdr.TInfomask)
		tup[22] = heap2TupleHeaderSize // THoff
		tup[23] = 0                    // padding
		copy(tup[heap2TupleHeaderSize:], data)
		tuples = append(tuples, tup)
	}
	return tuples, nil
}

// ── xl_heap_prune ──────────────────────────────────────────────────────────────

// xl_heap_prune layout (heapam_xlog.h):
//
//	TransactionId latestRemovedXid  (4 bytes)
//	uint16        nredirected       (2 bytes)
//	uint16        ndead             (2 bytes)
//	bool          isCatalogRel      (1 byte, added in PG15)
//
// Offset numbers follow in BlockRef[0].Data:
//   nredirected * 2 uint16 values  (from, to pairs)
//   ndead * 1    uint16 values     (dead offsets)
//   remainder                      (unused offsets — ignored for redo)
const xlHeapPruneMinSize = 8 // pre-PG15 size without isCatalogRel

type xlHeapPruneData struct {
	NRedirected uint16
	NDead       uint16
}

func decodeXLHeapPrune(data []byte) (xlHeapPruneData, error) {
	if len(data) < xlHeapPruneMinSize {
		return xlHeapPruneData{}, fmt.Errorf("heap2: xl_heap_prune too short (%d bytes)", len(data))
	}
	return xlHeapPruneData{
		NRedirected: binary.LittleEndian.Uint16(data[4:]),
		NDead:       binary.LittleEndian.Uint16(data[6:]),
	}, nil
}

// parsePruneOffsets extracts redirect pairs and dead offsets from block-ref data.
func parsePruneOffsets(data []byte, nredirected, ndead uint16) (redirects [][2]uint16, dead []uint16, err error) {
	redirectBytes := int(nredirected) * 4 // 2 uint16 per redirect
	deadBytes := int(ndead) * 2
	if len(data) < redirectBytes+deadBytes {
		return nil, nil, fmt.Errorf("heap2: prune offset data too short: need %d have %d", redirectBytes+deadBytes, len(data))
	}
	redirects = make([][2]uint16, nredirected)
	for i := range redirects {
		from := binary.LittleEndian.Uint16(data[i*4:])
		to := binary.LittleEndian.Uint16(data[i*4+2:])
		redirects[i] = [2]uint16{from, to}
	}
	dead = make([]uint16, ndead)
	for i := range dead {
		dead[i] = binary.LittleEndian.Uint16(data[redirectBytes+i*2:])
	}
	return redirects, dead, nil
}

// ── xl_heap_vacuum ─────────────────────────────────────────────────────────────

// xl_heap_vacuum layout (heapam_xlog.h):
//
//	uint16 noffsets  (2 bytes)
//	<noffsets * uint16 offset numbers follow in MainData>
const xlHeapVacuumSize = 2

func decodeXLHeapVacuumOffsets(data []byte) ([]uint16, error) {
	if len(data) < xlHeapVacuumSize {
		return nil, fmt.Errorf("heap2: xl_heap_vacuum too short (%d bytes)", len(data))
	}
	noffsets := binary.LittleEndian.Uint16(data[0:])
	offData := data[xlHeapVacuumSize:]
	if len(offData) < int(noffsets)*2 {
		return nil, fmt.Errorf("heap2: vacuum offset data truncated: need %d have %d", int(noffsets)*2, len(offData))
	}
	offsets := make([]uint16, noffsets)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint16(offData[i*2:])
	}
	return offsets, nil
}

// ── Redo dispatch ──────────────────────────────────────────────────────────────

func heap2Redo(ctx RedoContext) error {
	info := ctx.Rec.Header.XlInfo & xlHeap2OpMask
	switch info {
	case xlHeap2MultiInsert:
		return heap2RedoMultiInsert(ctx)
	case xlHeap2Prune:
		return heap2RedoPrune(ctx)
	case xlHeap2Vacuum:
		return heap2RedoVacuum(ctx)
	case xlHeap2Visible:
		return heap2RedoVisible(ctx)
	case xlHeap2FreezePage, xlHeap2LockUpdated, xlHeap2NewCid, xlHeap2Rewrite:
		// Acknowledged but not applied:
		//   FreezePage  — affects XID ageing, not page layout used for scans
		//   LockUpdated — tuple lock, irrelevant for redo page state
		//   NewCid      — logical-decoding metadata only
		//   Rewrite     — CLUSTER/VACUUM FULL; out-of-scope for basic replay
		return nil
	default:
		return fmt.Errorf("heap2: unknown info byte 0x%02x", info)
	}
}

func heap2RedoMultiInsert(ctx RedoContext) error {
	rec := ctx.Rec
	if len(rec.BlockRefs) == 0 {
		return fmt.Errorf("heap2 multi_insert: no block references")
	}
	br := rec.BlockRefs[0]

	if br.Image != nil && br.Image.BimgInfo&BimgApply != 0 {
		if ctx.Store == nil {
			return nil
		}
		return ctx.Store.ApplyFPW(br.Reln, br.ForkNum, br.BlockNum, br.Image, ctx.LSN)
	}

	xlrec, err := decodeXLHeapMultiInsert(rec.MainData)
	if err != nil {
		return err
	}
	initPage := rec.Header.XlInfo&xlHeapInitPage != 0

	tuples, err := parseMultiInsertTuples(rec.Header.XlXid, br.BlockNum, xlrec.NTuples, br.Data)
	if err != nil {
		return err
	}

	if ctx.Store == nil {
		return nil
	}
	return ctx.Store.ApplyMultiInsert(br.Reln, br.ForkNum, br.BlockNum, tuples, initPage, ctx.LSN)
}

func heap2RedoPrune(ctx RedoContext) error {
	rec := ctx.Rec
	if len(rec.BlockRefs) == 0 {
		return fmt.Errorf("heap2 prune: no block references")
	}
	br := rec.BlockRefs[0]

	if br.Image != nil && br.Image.BimgInfo&BimgApply != 0 {
		if ctx.Store == nil {
			return nil
		}
		return ctx.Store.ApplyFPW(br.Reln, br.ForkNum, br.BlockNum, br.Image, ctx.LSN)
	}

	xlrec, err := decodeXLHeapPrune(rec.MainData)
	if err != nil {
		return err
	}
	redirects, dead, err := parsePruneOffsets(br.Data, xlrec.NRedirected, xlrec.NDead)
	if err != nil {
		return err
	}

	if ctx.Store == nil {
		return nil
	}
	return ctx.Store.ApplyPrune(br.Reln, br.ForkNum, br.BlockNum, redirects, dead, ctx.LSN)
}

func heap2RedoVacuum(ctx RedoContext) error {
	rec := ctx.Rec
	if len(rec.BlockRefs) == 0 {
		return fmt.Errorf("heap2 vacuum: no block references")
	}
	br := rec.BlockRefs[0]

	if br.Image != nil && br.Image.BimgInfo&BimgApply != 0 {
		if ctx.Store == nil {
			return nil
		}
		return ctx.Store.ApplyFPW(br.Reln, br.ForkNum, br.BlockNum, br.Image, ctx.LSN)
	}

	offsets, err := decodeXLHeapVacuumOffsets(rec.MainData)
	if err != nil {
		return err
	}
	if ctx.Store == nil || len(offsets) == 0 {
		return nil
	}
	return ctx.Store.ApplyVacuumDeadItems(br.Reln, br.ForkNum, br.BlockNum, offsets, ctx.LSN)
}

func heap2RedoVisible(ctx RedoContext) error {
	rec := ctx.Rec
	if len(rec.BlockRefs) == 0 {
		return nil
	}
	br := rec.BlockRefs[0]

	if br.Image != nil && br.Image.BimgInfo&BimgApply != 0 {
		if ctx.Store == nil {
			return nil
		}
		return ctx.Store.ApplyFPW(br.Reln, br.ForkNum, br.BlockNum, br.Image, ctx.LSN)
	}
	if ctx.Store == nil {
		return nil
	}
	return ctx.Store.ApplySetVisible(br.Reln, br.ForkNum, br.BlockNum, ctx.LSN)
}

// ── Registration ───────────────────────────────────────────────────────────────

func init() {
	Register(RmgrHeap2, RmgrOps{
		Name: "Heap2",
		Redo: heap2Redo,
		Identify: func(rec *Record) string {
			info := rec.Header.XlInfo & xlHeap2OpMask
			names := map[uint8]string{
				xlHeap2Rewrite:     "REWRITE",
				xlHeap2Prune:       "PRUNE",
				xlHeap2Vacuum:      "VACUUM",
				xlHeap2FreezePage:  "FREEZE_PAGE",
				xlHeap2Visible:     "VISIBLE",
				xlHeap2MultiInsert: "MULTI_INSERT",
				xlHeap2LockUpdated: "LOCK_UPDATED",
				xlHeap2NewCid:      "NEW_CID",
			}
			name := names[info]
			if name == "" {
				name = "UNKNOWN"
			}
			if rec.Header.XlInfo&xlHeapInitPage != 0 {
				name += "+INIT"
			}
			return fmt.Sprintf("Heap2/%s", name)
		},
	})
}
