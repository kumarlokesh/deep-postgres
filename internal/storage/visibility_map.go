package storage

// VisibilityMap tracks which heap pages are "all-visible": every tuple on the
// page is visible to all current and future snapshots (committed xmin older
// than OldestXmin, no live xmax).
//
// PostgreSQL stores the VM as a separate relation fork (ForkVm = 2) with two
// bits per heap block: all-visible and all-frozen.  Our research build uses a
// simple in-memory bit array (one bit per block: all-visible only).  The bit
// array is populated by VACUUM and consulted by SeqScan to skip per-tuple
// HeapTupleSatisfiesMVCC calls.
//
// Semantics:
//
//   - Bit is SET by VACUUM after it confirms every LP_NORMAL tuple on the page
//     has a committed xmin < OldestXmin and no live xmax.
//   - Bit is CLEARED whenever a write modifies the page (heap insert/update/
//     delete) so that subsequent scans fall back to full MVCC checks until the
//     next VACUUM pass.
//
// Thread-safety: none — single-threaded by design.

// VisibilityMap holds one bit per heap block.
type VisibilityMap struct {
	bits []uint8 // bits[blk/8] >> (blk%8) & 1
}

// NewVisibilityMap creates an empty map that initially covers nblocks blocks.
// Grow is called automatically by Set/Clear if a larger block number is used.
func NewVisibilityMap(nblocks BlockNumber) *VisibilityMap {
	vm := &VisibilityMap{}
	if nblocks > 0 {
		vm.grow(nblocks)
	}
	return vm
}

// IsAllVisible reports whether blk is marked all-visible.
func (vm *VisibilityMap) IsAllVisible(blk BlockNumber) bool {
	if vm == nil {
		return false
	}
	byteIdx := blk / 8
	if int(byteIdx) >= len(vm.bits) {
		return false
	}
	return vm.bits[byteIdx]>>(blk%8)&1 == 1
}

// SetAllVisible marks blk as all-visible.
func (vm *VisibilityMap) SetAllVisible(blk BlockNumber) {
	vm.grow(blk + 1)
	vm.bits[blk/8] |= 1 << (blk % 8)
}

// ClearAllVisible clears the all-visible bit for blk.
// Called when a page is modified (insert/update/delete).
func (vm *VisibilityMap) ClearAllVisible(blk BlockNumber) {
	if vm == nil {
		return
	}
	byteIdx := blk / 8
	if int(byteIdx) >= len(vm.bits) {
		return
	}
	vm.bits[byteIdx] &^= 1 << (blk % 8)
}

// grow ensures the bit array covers at least nblocks blocks.
func (vm *VisibilityMap) grow(nblocks BlockNumber) {
	need := int((nblocks + 7) / 8)
	if need > len(vm.bits) {
		vm.bits = append(vm.bits, make([]uint8, need-len(vm.bits))...)
	}
}

// ── page-level all-visible check ─────────────────────────────────────────────

// PageIsAllVisible returns true if every LP_NORMAL tuple on page satisfies the
// all-visible condition:
//
//   - xmin is FrozenTransactionId, or committed with xmin < oldestXmin
//   - xmax is InvalidTransactionId or has HeapXmaxInvalid set (not deleted)
//
// This is called by VacuumPage after its cleanup pass to decide whether to
// mark the page in the VisibilityMap.
func PageIsAllVisible(page *Page, oldestXmin TransactionId, oracle TransactionOracle) bool {
	n := page.ItemCount()
	for i := 0; i < n; i++ {
		lp, err := page.GetItemId(i)
		if err != nil || !lp.IsNormal() {
			continue
		}
		raw := page.Bytes()
		off := int(lp.Off())
		if off+HeapTupleHeaderSize > len(raw) {
			return false
		}
		hdr := decodeHeapTupleHeader(raw[off:])

		// xmin: must be frozen, or committed and older than OldestXmin.
		xmin := hdr.TXmin
		if xmin != FrozenTransactionId {
			im := hdr.Infomask()
			xminOK := im&HeapXminCommitted != 0
			if !xminOK {
				xminOK = oracle.Status(xmin) == TxCommitted
			}
			if !xminOK || xmin >= oldestXmin {
				return false
			}
		}

		// xmax: must be absent or explicitly invalidated (no active deletion).
		xmax := hdr.TXmax
		if xmax != InvalidTransactionId && hdr.Infomask()&HeapXmaxInvalid == 0 {
			return false
		}
	}
	return true
}
