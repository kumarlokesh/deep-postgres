//! Heap tuple implementation.
//!
//! This module implements PostgreSQL's heap tuple format as defined in
//! `src/include/access/htup_details.h`. A heap tuple consists of:
//!
//! ```text
//! +------------------+------------+-------------+
//! | HeapTupleHeader  | null bitmap| user data   |
//! | (23 bytes + pad) | (optional) | (attributes)|
//! +------------------+------------+-------------+
//! ```
//!
//! The header contains MVCC information (xmin, xmax, cid) and metadata
//! about the tuple's structure.

use bytemuck::{Pod, Zeroable};
use static_assertions::const_assert_eq;

use crate::types::{CommandId, OffsetNumber, TransactionId};

/// Size of the heap tuple header in bytes (23 bytes, but aligned to 24).
///
/// PostgreSQL's `HeapTupleHeaderData` is 23 bytes, but due to alignment
/// requirements, the actual header size is typically 24 bytes.
pub const HEAP_TUPLE_HEADER_SIZE: usize = 24;

/// Minimum heap tuple header size (without alignment padding).
pub const MIN_HEAP_TUPLE_HEADER_SIZE: usize = 23;

/// Heap tuple header data structure.
///
/// Matches PostgreSQL's `HeapTupleHeaderData` from `htup_details.h`:
///
/// ```c
/// typedef struct HeapTupleHeaderData {
///     union {
///         HeapTupleFields t_heap;
///         DatumTupleFields t_datum;
///     } t_choice;
///     ItemPointerData t_ctid;      /* current TID of this or newer tuple */
///     uint16 t_infomask2;          /* number of attributes + flags */
///     uint16 t_infomask;           /* various flag bits */
///     uint8 t_hoff;                /* sizeof header incl. bitmap, padding */
///     /* ^ - 23 bytes - ^ */
///     bits8 t_bits[FLEXIBLE_ARRAY_MEMBER]; /* bitmap of NULLs */
/// } HeapTupleHeaderData;
/// ```
///
/// We use the heap tuple variant (`t_heap`) which contains:
/// - `t_xmin`: inserting transaction ID
/// - `t_xmax`: deleting transaction ID (or 0)
/// - `t_cid`: command ID (or combo command ID)
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct HeapTupleHeader {
    /// Inserting transaction ID.
    pub t_xmin: TransactionId,
    /// Deleting transaction ID, or 0 if not deleted.
    pub t_xmax: TransactionId,
    /// Command ID within the transaction, or combo command ID.
    ///
    /// In PostgreSQL, this field is overloaded:
    /// - For regular tuples: command ID of inserting command
    /// - For updated tuples: can be a "combo" CID
    pub t_cid: CommandId,
    /// Current TID of this or newer tuple version (for HOT chains).
    ///
    /// CTID block number.
    pub t_ctid_block: u32,
    /// CTID offset number.
    pub t_ctid_offset: OffsetNumber,
    /// Number of attributes + various flag bits.
    ///
    /// Low 11 bits: number of attributes
    /// High 5 bits: flags (see `Infomask2Flags`)
    pub t_infomask2: u16,
    /// Various flag bits (see `InfomaskFlags`).
    pub t_infomask: u16,
    /// Size of header including bitmap and padding.
    pub t_hoff: u8,
    /// Padding to align to 24 bytes.
    padding: u8,
}

const_assert_eq!(std::mem::size_of::<HeapTupleHeader>(), HEAP_TUPLE_HEADER_SIZE);

/// Maximum number of attributes in a tuple.
pub const MAX_HEAP_ATTRIBUTE_NUMBER: u16 = 1600;

/// Mask for extracting attribute count from `t_infomask2`.
const HEAP_NATTS_MASK: u16 = 0x07FF;

bitflags::bitflags! {
    /// Infomask flags (t_infomask field).
    ///
    /// These flags indicate the state of the tuple for MVCC and other purposes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct InfomaskFlags: u16 {
        /// Tuple has null attribute(s).
        const HEAP_HASNULL = 0x0001;
        /// Tuple has variable-width attribute(s).
        const HEAP_HASVARWIDTH = 0x0002;
        /// Tuple has external stored attribute(s).
        const HEAP_HASEXTERNAL = 0x0004;
        /// Tuple has an object ID field.
        const HEAP_HASOID_OLD = 0x0008;
        /// xmax is a key-share locker.
        const HEAP_XMAX_KEYSHR_LOCK = 0x0010;
        /// t_cid is a combo CID.
        const HEAP_COMBOCID = 0x0020;
        /// xmax is exclusive locker.
        const HEAP_XMAX_EXCL_LOCK = 0x0040;
        /// xmax, if valid, is only a locker.
        const HEAP_XMAX_LOCK_ONLY = 0x0080;
        /// xmin committed.
        const HEAP_XMIN_COMMITTED = 0x0100;
        /// xmin invalid/aborted.
        const HEAP_XMIN_INVALID = 0x0200;
        /// xmax committed.
        const HEAP_XMAX_COMMITTED = 0x0400;
        /// xmax invalid/aborted.
        const HEAP_XMAX_INVALID = 0x0800;
        /// xmax is a MultiXactId.
        const HEAP_XMAX_IS_MULTI = 0x1000;
        /// tuple was updated.
        const HEAP_UPDATED = 0x2000;
        /// tuple moved to another place by pre-9.0 VACUUM FULL.
        const HEAP_MOVED_OFF = 0x4000;
        /// tuple moved in from another place by pre-9.0 VACUUM FULL.
        const HEAP_MOVED_IN = 0x8000;
    }
}

bitflags::bitflags! {
    /// Infomask2 flags (high bits of t_infomask2 field).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Infomask2Flags: u16 {
        /// Tuple has HOT successor.
        const HEAP_HOT_UPDATED = 0x4000;
        /// Tuple is HOT-updated version.
        const HEAP_ONLY_TUPLE = 0x8000;
    }
}

impl HeapTupleHeader {
    /// Create a new heap tuple header.
    #[must_use]
    pub fn new(xmin: TransactionId, natts: u16) -> Self {
        Self {
            t_xmin: xmin,
            t_xmax: 0,
            t_cid: 0,
            t_ctid_block: 0,
            t_ctid_offset: 0,
            t_infomask2: natts & HEAP_NATTS_MASK,
            t_infomask: InfomaskFlags::HEAP_XMAX_INVALID.bits(),
            t_hoff: HEAP_TUPLE_HEADER_SIZE as u8,
            padding: 0,
        }
    }

    /// Get the number of attributes.
    #[must_use]
    pub const fn natts(&self) -> u16 {
        self.t_infomask2 & HEAP_NATTS_MASK
    }

    /// Set the number of attributes.
    pub fn set_natts(&mut self, natts: u16) {
        self.t_infomask2 = (self.t_infomask2 & !HEAP_NATTS_MASK) | (natts & HEAP_NATTS_MASK);
    }

    /// Get the infomask flags.
    #[must_use]
    pub fn infomask(&self) -> InfomaskFlags {
        InfomaskFlags::from_bits_truncate(self.t_infomask)
    }

    /// Set the infomask flags.
    pub fn set_infomask(&mut self, flags: InfomaskFlags) {
        self.t_infomask = flags.bits();
    }

    /// Get the infomask2 flags (excluding attribute count).
    #[must_use]
    pub fn infomask2_flags(&self) -> Infomask2Flags {
        Infomask2Flags::from_bits_truncate(self.t_infomask2)
    }

    /// Check if the tuple has null attributes.
    #[must_use]
    pub fn has_nulls(&self) -> bool {
        self.infomask().contains(InfomaskFlags::HEAP_HASNULL)
    }

    /// Check if xmin is committed.
    #[must_use]
    pub fn xmin_committed(&self) -> bool {
        self.infomask().contains(InfomaskFlags::HEAP_XMIN_COMMITTED)
    }

    /// Check if xmin is invalid/aborted.
    #[must_use]
    pub fn xmin_invalid(&self) -> bool {
        self.infomask().contains(InfomaskFlags::HEAP_XMIN_INVALID)
    }

    /// Check if xmax is committed.
    #[must_use]
    pub fn xmax_committed(&self) -> bool {
        self.infomask().contains(InfomaskFlags::HEAP_XMAX_COMMITTED)
    }

    /// Check if xmax is invalid/aborted.
    #[must_use]
    pub fn xmax_invalid(&self) -> bool {
        self.infomask().contains(InfomaskFlags::HEAP_XMAX_INVALID)
    }

    /// Check if this is a HOT-updated tuple.
    #[must_use]
    pub fn is_hot_updated(&self) -> bool {
        self.infomask2_flags().contains(Infomask2Flags::HEAP_HOT_UPDATED)
    }

    /// Check if this is a heap-only tuple (HOT).
    #[must_use]
    pub fn is_heap_only(&self) -> bool {
        self.infomask2_flags().contains(Infomask2Flags::HEAP_ONLY_TUPLE)
    }

    /// Mark xmin as committed.
    pub fn set_xmin_committed(&mut self) {
        let mut flags = self.infomask();
        flags.insert(InfomaskFlags::HEAP_XMIN_COMMITTED);
        flags.remove(InfomaskFlags::HEAP_XMIN_INVALID);
        self.set_infomask(flags);
    }

    /// Mark xmin as invalid/aborted.
    pub fn set_xmin_invalid(&mut self) {
        let mut flags = self.infomask();
        flags.insert(InfomaskFlags::HEAP_XMIN_INVALID);
        flags.remove(InfomaskFlags::HEAP_XMIN_COMMITTED);
        self.set_infomask(flags);
    }

    /// Mark xmax as committed.
    pub fn set_xmax_committed(&mut self) {
        let mut flags = self.infomask();
        flags.insert(InfomaskFlags::HEAP_XMAX_COMMITTED);
        flags.remove(InfomaskFlags::HEAP_XMAX_INVALID);
        self.set_infomask(flags);
    }

    /// Mark xmax as invalid/aborted.
    pub fn set_xmax_invalid(&mut self) {
        let mut flags = self.infomask();
        flags.insert(InfomaskFlags::HEAP_XMAX_INVALID);
        flags.remove(InfomaskFlags::HEAP_XMAX_COMMITTED);
        self.set_infomask(flags);
    }

    /// Set the CTID (current tuple ID).
    pub fn set_ctid(&mut self, block: u32, offset: OffsetNumber) {
        self.t_ctid_block = block;
        self.t_ctid_offset = offset;
    }

    /// Get the CTID as (block, offset).
    #[must_use]
    pub const fn ctid(&self) -> (u32, OffsetNumber) {
        (self.t_ctid_block, self.t_ctid_offset)
    }
}

impl Default for HeapTupleHeader {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

/// A complete heap tuple with header and data.
#[derive(Debug, Clone)]
pub struct HeapTuple {
    /// The tuple header.
    pub header: HeapTupleHeader,
    /// The tuple data (null bitmap + attribute values).
    pub data: Vec<u8>,
}

impl HeapTuple {
    /// Create a new heap tuple.
    #[must_use]
    pub fn new(xmin: TransactionId, natts: u16, data: Vec<u8>) -> Self {
        Self {
            header: HeapTupleHeader::new(xmin, natts),
            data,
        }
    }

    /// Get the total size of the tuple (header + data).
    #[must_use]
    pub fn len(&self) -> usize {
        HEAP_TUPLE_HEADER_SIZE + self.data.len()
    }

    /// Check if the tuple has no data.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Serialize the tuple to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.len());
        bytes.extend_from_slice(bytemuck::bytes_of(&self.header));
        bytes.extend_from_slice(&self.data);
        bytes
    }

    /// Deserialize a tuple from bytes.
    ///
    /// # Panics
    ///
    /// Panics if the bytes are too short to contain a header.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= HEAP_TUPLE_HEADER_SIZE,
            "bytes too short for tuple header"
        );
        let header: HeapTupleHeader = *bytemuck::from_bytes(&bytes[..HEAP_TUPLE_HEADER_SIZE]);
        let data = bytes[HEAP_TUPLE_HEADER_SIZE..].to_vec();
        Self { header, data }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_size_is_correct() {
        assert_eq!(std::mem::size_of::<HeapTupleHeader>(), HEAP_TUPLE_HEADER_SIZE);
    }

    #[test]
    fn new_header_defaults() {
        let header = HeapTupleHeader::new(100, 5);
        assert_eq!(header.t_xmin, 100);
        assert_eq!(header.t_xmax, 0);
        assert_eq!(header.natts(), 5);
        assert!(header.xmax_invalid());
        assert!(!header.xmin_committed());
    }

    #[test]
    fn natts_encoding() {
        let mut header = HeapTupleHeader::new(0, 0);

        header.set_natts(100);
        assert_eq!(header.natts(), 100);

        header.set_natts(MAX_HEAP_ATTRIBUTE_NUMBER);
        assert_eq!(header.natts(), MAX_HEAP_ATTRIBUTE_NUMBER & HEAP_NATTS_MASK);
    }

    #[test]
    fn infomask_operations() {
        let mut header = HeapTupleHeader::new(100, 5);

        assert!(!header.xmin_committed());
        header.set_xmin_committed();
        assert!(header.xmin_committed());
        assert!(!header.xmin_invalid());

        header.set_xmin_invalid();
        assert!(header.xmin_invalid());
        assert!(!header.xmin_committed());
    }

    #[test]
    fn ctid_operations() {
        let mut header = HeapTupleHeader::new(100, 5);

        header.set_ctid(42, 7);
        assert_eq!(header.ctid(), (42, 7));
    }

    #[test]
    fn tuple_serialization() {
        let tuple = HeapTuple::new(100, 3, vec![1, 2, 3, 4, 5]);

        let bytes = tuple.to_bytes();
        assert_eq!(bytes.len(), HEAP_TUPLE_HEADER_SIZE + 5);

        let restored = HeapTuple::from_bytes(&bytes);
        assert_eq!(restored.header.t_xmin, 100);
        assert_eq!(restored.header.natts(), 3);
        assert_eq!(restored.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn tuple_length() {
        let tuple = HeapTuple::new(0, 0, vec![0; 100]);
        assert_eq!(tuple.len(), HEAP_TUPLE_HEADER_SIZE + 100);
    }
}
