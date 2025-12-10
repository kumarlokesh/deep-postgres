//! PostgreSQL page layout implementation.
//!
//! This module implements the standard PostgreSQL 8KB page format as defined in
//! `src/include/storage/bufpage.h`. The page layout is:
//!
//! ```text
//! +----------------+-----------+----------------+----------------+
//! | PageHeaderData | ItemIdData| ... free space | ... tuples ... |
//! | (24 bytes)     | array     |                | (grow down)    |
//! +----------------+-----------+----------------+----------------+
//! ^                ^           ^                ^                ^
//! 0            pd_lower    pd_upper         pd_special      PAGE_SIZE
//! ```
//!
//! - `PageHeaderData`: Fixed 24-byte header at offset 0
//! - `ItemIdData` array: Line pointers grow upward from offset 24
//! - Free space: Between line pointers and tuple data
//! - Tuple data: Grows downward from `pd_special` (or `PAGE_SIZE` if no special)
//! - Special space: Optional, at end of page (used by indexes)

use bytemuck::{Pod, Zeroable};
use static_assertions::const_assert_eq;

use crate::error::{StorageError, StorageResult};
use crate::types::{LocationIndex, TransactionId};

/// Standard PostgreSQL page size (8KB).
pub const PAGE_SIZE: usize = 8192;

/// Size of the page header in bytes.
pub const PAGE_HEADER_SIZE: usize = 24;

/// Maximum size of special space.
pub const MAX_SPECIAL_SPACE: usize = PAGE_SIZE / 2;

/// Page header data structure.
///
/// Matches PostgreSQL's `PageHeaderData` from `bufpage.h`:
/// ```c
/// typedef struct PageHeaderData {
///     PageXLogRecPtr pd_lsn;      /* LSN: next byte after last byte of xlog record */
///     uint16 pd_checksum;         /* checksum */
///     uint16 pd_flags;            /* flag bits, see below */
///     LocationIndex pd_lower;     /* offset to start of free space */
///     LocationIndex pd_upper;     /* offset to end of free space */
///     LocationIndex pd_special;   /* offset to start of special space */
///     uint16 pd_pagesize_version; /* page size and layout version number */
///     TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
/// } PageHeaderData;
/// ```
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct PageHeaderData {
    /// LSN: next byte after last byte of xlog record for last change.
    /// LSN high 32 bits.
    pub pd_lsn_hi: u32,
    /// LSN low 32 bits.
    pub pd_lsn_lo: u32,
    /// Page checksum (if checksums enabled).
    pub pd_checksum: u16,
    /// Flag bits (see `PageFlags`).
    pub pd_flags: u16,
    /// Offset to start of free space (end of line pointer array).
    pub pd_lower: LocationIndex,
    /// Offset to end of free space (start of tuple data).
    pub pd_upper: LocationIndex,
    /// Offset to start of special space.
    pub pd_special: LocationIndex,
    /// Page size and layout version number.
    /// Low 8 bits: version, high 8 bits: page size as multiple of 256.
    pub pd_pagesize_version: u16,
    /// Oldest prunable XID, or zero if none.
    pub pd_prune_xid: TransactionId,
}

const_assert_eq!(std::mem::size_of::<PageHeaderData>(), PAGE_HEADER_SIZE);

/// Page layout version number (matches PG 16.x).
pub const PG_PAGE_LAYOUT_VERSION: u8 = 4;

/// Encode page size and version into `pd_pagesize_version`.
#[must_use]
pub const fn make_pagesize_version(page_size: usize, version: u8) -> u16 {
    let size_field = (page_size / 256) as u16;
    (size_field << 8) | (version as u16)
}

/// Decode page size from `pd_pagesize_version`.
#[must_use]
pub const fn get_page_size(pagesize_version: u16) -> usize {
    ((pagesize_version >> 8) as usize) * 256
}

/// Decode version from `pd_pagesize_version`.
#[must_use]
pub const fn get_page_version(pagesize_version: u16) -> u8 {
    (pagesize_version & 0xFF) as u8
}

impl PageHeaderData {
    /// Create a new page header for an empty page.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pd_lsn_hi: 0,
            pd_lsn_lo: 0,
            pd_checksum: 0,
            pd_flags: 0,
            pd_lower: PAGE_HEADER_SIZE as LocationIndex,
            pd_upper: PAGE_SIZE as LocationIndex,
            pd_special: PAGE_SIZE as LocationIndex,
            pd_pagesize_version: make_pagesize_version(PAGE_SIZE, PG_PAGE_LAYOUT_VERSION),
            pd_prune_xid: 0,
        }
    }

    /// Get the LSN as a single u64.
    #[must_use]
    pub fn lsn(&self) -> u64 {
        (u64::from(self.pd_lsn_hi) << 32) | u64::from(self.pd_lsn_lo)
    }

    /// Set the LSN from a u64.
    pub fn set_lsn(&mut self, lsn: u64) {
        self.pd_lsn_hi = (lsn >> 32) as u32;
        self.pd_lsn_lo = lsn as u32;
    }

    /// Get the amount of free space on the page.
    #[must_use]
    pub fn free_space(&self) -> usize {
        if self.pd_upper > self.pd_lower {
            (self.pd_upper - self.pd_lower) as usize
        } else {
            0
        }
    }

    /// Check if the page is empty (no tuples).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pd_lower == PAGE_HEADER_SIZE as LocationIndex
    }

    /// Get the number of line pointers on the page.
    #[must_use]
    pub fn item_count(&self) -> usize {
        let line_pointer_area = (self.pd_lower as usize).saturating_sub(PAGE_HEADER_SIZE);
        line_pointer_area / std::mem::size_of::<ItemIdData>()
    }
}

impl Default for PageHeaderData {
    fn default() -> Self {
        Self::new()
    }
}

bitflags::bitflags! {
    /// Page flag bits (from `bufpage.h`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PageFlags: u16 {
        /// Page has free line pointers.
        const PD_HAS_FREE_LINES = 0x0001;
        /// Page is full (no free space for new tuples).
        const PD_PAGE_FULL = 0x0002;
        /// All tuples on page are visible to all transactions.
        const PD_ALL_VISIBLE = 0x0004;
    }
}

/// Item identifier data (line pointer).
///
/// Matches PostgreSQL's `ItemIdData` from `bufpage.h`:
/// ```c
/// typedef struct ItemIdData {
///     unsigned lp_off:15,  /* offset to tuple (from start of page) */
///              lp_flags:2, /* state of line pointer, see below */
///              lp_len:15;  /* byte length of tuple */
/// } ItemIdData;
/// ```
///
/// We store this as a u32 and provide accessors for the bit fields.
#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq)]
#[repr(C)]
pub struct ItemIdData(u32);

const_assert_eq!(std::mem::size_of::<ItemIdData>(), 4);

/// Line pointer flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LpFlags {
    /// Unused (available for allocation).
    Unused = 0,
    /// Normal (points to a tuple).
    Normal = 1,
    /// Redirect to another line pointer (HOT chains).
    Redirect = 2,
    /// Dead (tuple has been removed).
    Dead = 3,
}

impl From<u8> for LpFlags {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Normal,
            2 => Self::Redirect,
            3 => Self::Dead,
            _ => Self::Unused,
        }
    }
}

impl ItemIdData {
    /// Bit masks and shifts for the packed fields.
    const LP_OFF_MASK: u32 = 0x7FFF; // bits 0-14
    const LP_FLAGS_SHIFT: u32 = 15;
    const LP_FLAGS_MASK: u32 = 0x3; // bits 15-16
    const LP_LEN_SHIFT: u32 = 17;
    const LP_LEN_MASK: u32 = 0x7FFF; // bits 17-31

    /// Create a new unused line pointer.
    #[must_use]
    pub const fn unused() -> Self {
        Self(0)
    }

    /// Create a new normal line pointer.
    #[must_use]
    pub const fn new(offset: u16, length: u16) -> Self {
        let off = (offset as u32) & Self::LP_OFF_MASK;
        let flags = (LpFlags::Normal as u32) << Self::LP_FLAGS_SHIFT;
        let len = ((length as u32) & Self::LP_LEN_MASK) << Self::LP_LEN_SHIFT;
        Self(off | flags | len)
    }

    /// Create a redirect line pointer (for HOT chains).
    #[must_use]
    pub const fn redirect(target_offset: u16) -> Self {
        let off = (target_offset as u32) & Self::LP_OFF_MASK;
        let flags = (LpFlags::Redirect as u32) << Self::LP_FLAGS_SHIFT;
        Self(off | flags)
    }

    /// Create a dead line pointer.
    #[must_use]
    pub const fn dead() -> Self {
        let flags = (LpFlags::Dead as u32) << Self::LP_FLAGS_SHIFT;
        Self(flags)
    }

    /// Get the offset to the tuple.
    #[must_use]
    pub const fn lp_off(&self) -> u16 {
        (self.0 & Self::LP_OFF_MASK) as u16
    }

    /// Get the line pointer flags.
    #[must_use]
    pub const fn lp_flags(&self) -> LpFlags {
        let flags = ((self.0 >> Self::LP_FLAGS_SHIFT) & Self::LP_FLAGS_MASK) as u8;
        match flags {
            1 => LpFlags::Normal,
            2 => LpFlags::Redirect,
            3 => LpFlags::Dead,
            _ => LpFlags::Unused,
        }
    }

    /// Get the tuple length.
    #[must_use]
    pub const fn lp_len(&self) -> u16 {
        ((self.0 >> Self::LP_LEN_SHIFT) & Self::LP_LEN_MASK) as u16
    }

    /// Check if this line pointer is unused.
    #[must_use]
    pub const fn is_unused(&self) -> bool {
        matches!(self.lp_flags(), LpFlags::Unused)
    }

    /// Check if this line pointer points to a normal tuple.
    #[must_use]
    pub const fn is_normal(&self) -> bool {
        matches!(self.lp_flags(), LpFlags::Normal)
    }

    /// Check if this line pointer is a redirect.
    #[must_use]
    pub const fn is_redirect(&self) -> bool {
        matches!(self.lp_flags(), LpFlags::Redirect)
    }

    /// Check if this line pointer is dead.
    #[must_use]
    pub const fn is_dead(&self) -> bool {
        matches!(self.lp_flags(), LpFlags::Dead)
    }

    /// Set the offset.
    pub fn set_lp_off(&mut self, offset: u16) {
        self.0 = (self.0 & !Self::LP_OFF_MASK) | (u32::from(offset) & Self::LP_OFF_MASK);
    }

    /// Set the length.
    pub fn set_lp_len(&mut self, length: u16) {
        self.0 = (self.0 & !(Self::LP_LEN_MASK << Self::LP_LEN_SHIFT))
            | ((u32::from(length) & Self::LP_LEN_MASK) << Self::LP_LEN_SHIFT);
    }
}

/// A heap page with full PostgreSQL-compatible layout.
///
/// Provides methods for manipulating the page header, line pointers, and tuple data
/// while maintaining the invariants of the PostgreSQL page format.
pub struct Page {
    data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    /// Create a new, initialized empty page.
    #[must_use]
    pub fn new() -> Self {
        let mut page = Self {
            data: Box::new([0; PAGE_SIZE]),
        };
        page.init();
        page
    }

    /// Create a page from existing raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the page header is invalid.
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> StorageResult<Self> {
        let page = Self {
            data: Box::new(data),
        };
        page.validate()?;
        Ok(page)
    }

    /// Initialize the page with a fresh header.
    pub fn init(&mut self) {
        self.data.fill(0);
        let header = PageHeaderData::new();
        self.set_header(&header);
    }

    /// Get a reference to the raw page bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Get a mutable reference to the raw page bytes.
    ///
    /// # Safety
    ///
    /// Caller must ensure page invariants are maintained after modification.
    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }

    /// Get the page header.
    #[must_use]
    pub fn header(&self) -> &PageHeaderData {
        bytemuck::from_bytes(&self.data[..PAGE_HEADER_SIZE])
    }

    /// Get a mutable reference to the page header.
    fn header_mut(&mut self) -> &mut PageHeaderData {
        bytemuck::from_bytes_mut(&mut self.data[..PAGE_HEADER_SIZE])
    }

    /// Set the page header.
    pub fn set_header(&mut self, header: &PageHeaderData) {
        self.data[..PAGE_HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(header));
    }

    /// Get the number of line pointers (item IDs) on the page.
    #[must_use]
    pub fn item_count(&self) -> usize {
        self.header().item_count()
    }

    /// Get a line pointer by index (0-based).
    ///
    /// Returns `None` if the index is out of bounds.
    #[must_use]
    pub fn get_item_id(&self, index: usize) -> Option<ItemIdData> {
        if index >= self.item_count() {
            return None;
        }
        let offset = PAGE_HEADER_SIZE + index * std::mem::size_of::<ItemIdData>();
        let bytes = &self.data[offset..offset + std::mem::size_of::<ItemIdData>()];
        Some(*bytemuck::from_bytes(bytes))
    }

    /// Set a line pointer by index (0-based).
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds.
    pub fn set_item_id(&mut self, index: usize, item_id: ItemIdData) -> StorageResult<()> {
        if index >= self.item_count() {
            return Err(StorageError::InvalidItemIndex { index });
        }
        let offset = PAGE_HEADER_SIZE + index * std::mem::size_of::<ItemIdData>();
        self.data[offset..offset + std::mem::size_of::<ItemIdData>()]
            .copy_from_slice(bytemuck::bytes_of(&item_id));
        Ok(())
    }

    /// Get the amount of free space on the page.
    #[must_use]
    pub fn free_space(&self) -> usize {
        self.header().free_space()
    }

    /// Get the maximum tuple size that can be inserted.
    ///
    /// This accounts for the line pointer that will be added.
    #[must_use]
    pub fn max_tuple_size(&self) -> usize {
        let free = self.free_space();
        if free > std::mem::size_of::<ItemIdData>() {
            free - std::mem::size_of::<ItemIdData>()
        } else {
            0
        }
    }

    /// Insert a tuple into the page.
    ///
    /// Returns the item index (0-based) of the inserted tuple.
    ///
    /// # Errors
    ///
    /// Returns an error if there is not enough space for the tuple.
    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> StorageResult<usize> {
        let tuple_len = tuple_data.len();

        // Check if we have enough space
        let required_space = tuple_len + std::mem::size_of::<ItemIdData>();
        if self.free_space() < required_space {
            return Err(StorageError::PageFull {
                required: required_space,
                available: self.free_space(),
            });
        }

        // Allocate space for the tuple (grows downward from pd_upper)
        let header = self.header();
        let new_upper = header.pd_upper as usize - tuple_len;
        let item_index = header.item_count();

        // Write the tuple data
        self.data[new_upper..new_upper + tuple_len].copy_from_slice(tuple_data);

        // Create and write the line pointer
        let item_id = ItemIdData::new(new_upper as u16, tuple_len as u16);
        let lp_offset = PAGE_HEADER_SIZE + item_index * std::mem::size_of::<ItemIdData>();
        self.data[lp_offset..lp_offset + std::mem::size_of::<ItemIdData>()]
            .copy_from_slice(bytemuck::bytes_of(&item_id));

        // Update the header
        let header = self.header_mut();
        header.pd_upper = new_upper as LocationIndex;
        header.pd_lower += std::mem::size_of::<ItemIdData>() as LocationIndex;

        Ok(item_index)
    }

    /// Get a tuple by item index (0-based).
    ///
    /// Returns the raw tuple bytes, or `None` if the index is invalid or the
    /// line pointer doesn't point to a normal tuple.
    #[must_use]
    pub fn get_tuple(&self, index: usize) -> Option<&[u8]> {
        let item_id = self.get_item_id(index)?;
        if !item_id.is_normal() {
            return None;
        }
        let offset = item_id.lp_off() as usize;
        let len = item_id.lp_len() as usize;
        if offset + len > PAGE_SIZE {
            return None;
        }
        Some(&self.data[offset..offset + len])
    }

    /// Mark a tuple as dead.
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds.
    pub fn mark_dead(&mut self, index: usize) -> StorageResult<()> {
        if index >= self.item_count() {
            return Err(StorageError::InvalidItemIndex { index });
        }
        let offset = PAGE_HEADER_SIZE + index * std::mem::size_of::<ItemIdData>();
        let item_id = ItemIdData::dead();
        self.data[offset..offset + std::mem::size_of::<ItemIdData>()]
            .copy_from_slice(bytemuck::bytes_of(&item_id));
        Ok(())
    }

    /// Validate the page structure.
    ///
    /// # Errors
    ///
    /// Returns an error if the page structure is invalid.
    pub fn validate(&self) -> StorageResult<()> {
        let header = self.header();

        // Check page size/version
        let page_size = get_page_size(header.pd_pagesize_version);
        if page_size != PAGE_SIZE {
            return Err(StorageError::InvalidPageSize {
                expected: PAGE_SIZE,
                actual: page_size,
            });
        }

        // Check pd_lower bounds
        if (header.pd_lower as usize) < PAGE_HEADER_SIZE {
            return Err(StorageError::InvalidPageLayout {
                message: "pd_lower below header".to_string(),
            });
        }

        // Check pd_upper bounds
        if header.pd_upper > header.pd_special {
            return Err(StorageError::InvalidPageLayout {
                message: "pd_upper exceeds pd_special".to_string(),
            });
        }

        // Check pd_special bounds
        if header.pd_special as usize > PAGE_SIZE {
            return Err(StorageError::InvalidPageLayout {
                message: "pd_special exceeds page size".to_string(),
            });
        }

        // Check free space consistency
        if header.pd_lower > header.pd_upper {
            return Err(StorageError::InvalidPageLayout {
                message: "pd_lower exceeds pd_upper (negative free space)".to_string(),
            });
        }

        Ok(())
    }

    /// Get the LSN of the page.
    #[must_use]
    pub fn lsn(&self) -> u64 {
        self.header().lsn()
    }

    /// Set the LSN of the page.
    pub fn set_lsn(&mut self, lsn: u64) {
        self.header_mut().set_lsn(lsn);
    }

    /// Check if the page is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.header().is_empty()
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("header", self.header())
            .field("item_count", &self.item_count())
            .field("free_space", &self.free_space())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_header_size_is_correct() {
        assert_eq!(std::mem::size_of::<PageHeaderData>(), PAGE_HEADER_SIZE);
    }

    #[test]
    fn item_id_size_is_correct() {
        assert_eq!(std::mem::size_of::<ItemIdData>(), 4);
    }

    #[test]
    fn new_page_is_valid() {
        let page = Page::new();
        assert!(page.validate().is_ok());
        assert!(page.is_empty());
        assert_eq!(page.item_count(), 0);
    }

    #[test]
    fn page_header_defaults() {
        let page = Page::new();
        let header = page.header();
        assert_eq!(header.pd_lower as usize, PAGE_HEADER_SIZE);
        assert_eq!(header.pd_upper as usize, PAGE_SIZE);
        assert_eq!(header.pd_special as usize, PAGE_SIZE);
        assert_eq!(get_page_size(header.pd_pagesize_version), PAGE_SIZE);
        assert_eq!(get_page_version(header.pd_pagesize_version), PG_PAGE_LAYOUT_VERSION);
    }

    #[test]
    fn item_id_bit_packing() {
        let item = ItemIdData::new(1000, 500);
        assert_eq!(item.lp_off(), 1000);
        assert_eq!(item.lp_len(), 500);
        assert!(item.is_normal());
        assert!(!item.is_unused());
        assert!(!item.is_dead());
        assert!(!item.is_redirect());
    }

    #[test]
    fn item_id_flags() {
        let unused = ItemIdData::unused();
        assert!(unused.is_unused());

        let dead = ItemIdData::dead();
        assert!(dead.is_dead());

        let redirect = ItemIdData::redirect(42);
        assert!(redirect.is_redirect());
        assert_eq!(redirect.lp_off(), 42);
    }

    #[test]
    fn insert_and_get_tuple() {
        let mut page = Page::new();
        let tuple_data = b"hello, postgres!";

        let index = page.insert_tuple(tuple_data).unwrap();
        assert_eq!(index, 0);
        assert_eq!(page.item_count(), 1);

        let retrieved = page.get_tuple(0).unwrap();
        assert_eq!(retrieved, tuple_data);
    }

    #[test]
    fn insert_multiple_tuples() {
        let mut page = Page::new();

        let tuples: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("tuple number {i}").into_bytes())
            .collect();

        for (i, tuple) in tuples.iter().enumerate() {
            let index = page.insert_tuple(tuple).unwrap();
            assert_eq!(index, i);
        }

        assert_eq!(page.item_count(), 10);

        for (i, expected) in tuples.iter().enumerate() {
            let retrieved = page.get_tuple(i).unwrap();
            assert_eq!(retrieved, expected.as_slice());
        }
    }

    #[test]
    fn page_full_error() {
        let mut page = Page::new();

        // Fill the page with large tuples
        let large_tuple = vec![0u8; 1000];
        let mut count = 0;
        while page.insert_tuple(&large_tuple).is_ok() {
            count += 1;
            if count > 100 {
                panic!("inserted too many tuples");
            }
        }

        // Verify we get a PageFull error
        let result = page.insert_tuple(&large_tuple);
        assert!(matches!(result, Err(StorageError::PageFull { .. })));
    }

    #[test]
    fn mark_tuple_dead() {
        let mut page = Page::new();
        page.insert_tuple(b"test tuple").unwrap();

        assert!(page.get_tuple(0).is_some());

        page.mark_dead(0).unwrap();

        // Tuple should no longer be retrievable
        assert!(page.get_tuple(0).is_none());

        // But line pointer should still exist
        let item_id = page.get_item_id(0).unwrap();
        assert!(item_id.is_dead());
    }

    #[test]
    fn free_space_tracking() {
        let mut page = Page::new();
        let initial_free = page.free_space();

        let tuple = b"test data";
        page.insert_tuple(tuple).unwrap();

        let after_insert = page.free_space();
        let expected_reduction = tuple.len() + std::mem::size_of::<ItemIdData>();
        assert_eq!(initial_free - after_insert, expected_reduction);
    }

    #[test]
    fn lsn_operations() {
        let mut page = Page::new();
        assert_eq!(page.lsn(), 0);

        page.set_lsn(0x0001_0002_0003_0004);
        assert_eq!(page.lsn(), 0x0001_0002_0003_0004);

        let header = page.header();
        assert_eq!(header.pd_lsn_hi, 0x0001_0002);
        assert_eq!(header.pd_lsn_lo, 0x0003_0004);
    }

    #[test]
    fn pagesize_version_encoding() {
        let encoded = make_pagesize_version(8192, 4);
        assert_eq!(get_page_size(encoded), 8192);
        assert_eq!(get_page_version(encoded), 4);

        // Test other page sizes
        let encoded_16k = make_pagesize_version(16384, 4);
        assert_eq!(get_page_size(encoded_16k), 16384);
    }

    #[test]
    fn page_validation_catches_invalid_layout() {
        let mut page = Page::new();

        // Corrupt pd_lower
        page.header_mut().pd_lower = 0;
        assert!(page.validate().is_err());

        // Reset and corrupt pd_upper
        page.init();
        page.header_mut().pd_upper = PAGE_SIZE as LocationIndex + 100;
        assert!(page.validate().is_err());
    }
}
