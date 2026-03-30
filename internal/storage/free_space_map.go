package storage

// FreeSpaceMap (FSM) records the available free space on each heap page so
// that heap inserts can find an existing page with room rather than always
// extending the relation.
//
// PostgreSQL's FSM (src/backend/storage/freespace/) uses a balanced tree
// stored in a dedicated relation fork (ForkFsm = 1) with one byte per heap
// page at the leaf level.  Space is quantised: each byte encodes
// (freeBytes / FSMBytesPerUnit) where FSMBytesPerUnit = PageSize/256 = 32
// bytes for an 8 KB page.
//
// This research implementation stores the same per-block byte array
// in memory — no persistence.  The key invariants are the same:
//
//   • Update(blk, freeBytes) is called after VACUUM cleans a page or after
//     an insert consumes space.
//   • FindWithSpace(needed) returns the first block whose recorded free space
//     is >= needed, or (0, false) if none exists.
//
// Thread-safety: none — single-threaded by design.

// FSMBytesPerUnit is the granularity of free-space quantisation.
// Matches PostgreSQL: PageSize/256 = 8192/256 = 32 bytes.
const FSMBytesPerUnit = PageSize / 256

// FreeSpaceMap tracks quantised free space per heap block.
type FreeSpaceMap struct {
	avail []uint8 // avail[blk] = floor(freeBytes / FSMBytesPerUnit)
}

// NewFreeSpaceMap creates an FSM that initially covers nblocks blocks.
func NewFreeSpaceMap(nblocks BlockNumber) *FreeSpaceMap {
	fsm := &FreeSpaceMap{}
	if nblocks > 0 {
		fsm.grow(nblocks)
	}
	return fsm
}

// FreeBytes returns the quantised free-byte estimate for blk.
// Returns 0 for blocks beyond the tracked range.
func (fsm *FreeSpaceMap) FreeBytes(blk BlockNumber) uint16 {
	if fsm == nil || int(blk) >= len(fsm.avail) {
		return 0
	}
	return uint16(fsm.avail[blk]) * FSMBytesPerUnit
}

// Update records freeBytes available on blk.
// freeBytes is quantised down to the nearest FSMBytesPerUnit.
func (fsm *FreeSpaceMap) Update(blk BlockNumber, freeBytes uint16) {
	fsm.grow(blk + 1)
	q := freeBytes / FSMBytesPerUnit
	if q > 255 {
		q = 255
	}
	fsm.avail[blk] = uint8(q)
}

// FindWithSpace returns the lowest block number whose recorded free space is
// >= needed bytes.  Returns (0, false) if no such block exists.
func (fsm *FreeSpaceMap) FindWithSpace(needed uint16) (BlockNumber, bool) {
	if fsm == nil {
		return 0, false
	}
	// Quantise needed upward so we only pick pages with at least that much
	// space after quantisation.
	threshold := uint8((needed + FSMBytesPerUnit - 1) / FSMBytesPerUnit)
	for blk, q := range fsm.avail {
		if q >= threshold {
			return BlockNumber(blk), true
		}
	}
	return 0, false
}

// Grow extends the FSM to cover at least nblocks entries.
// Called when the relation is extended.
func (fsm *FreeSpaceMap) Grow(nblocks BlockNumber) {
	fsm.grow(nblocks)
}

// Reset truncates the FSM to cover only nblocks entries.
// Called by VACUUM FULL after trailing empty pages are removed from the file.
func (fsm *FreeSpaceMap) Reset(nblocks BlockNumber) {
	if int(nblocks) < len(fsm.avail) {
		fsm.avail = fsm.avail[:nblocks]
	}
}

func (fsm *FreeSpaceMap) grow(nblocks BlockNumber) {
	need := int(nblocks)
	if need > len(fsm.avail) {
		fsm.avail = append(fsm.avail, make([]uint8, need-len(fsm.avail))...)
	}
}
