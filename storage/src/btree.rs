//! B-tree index implementation.
//!
//! This module implements PostgreSQL's B-tree index structure as defined in
//! `src/backend/access/nbtree/`. The B-tree is the default index type in
//! PostgreSQL and supports equality and range queries.
//!
//! # Page Layout
//!
//! B-tree pages use the standard page layout with special space at the end:
//!
//! ```text
//! +----------------+-----------+----------------+----------------+------------+
//! | PageHeaderData | ItemIdData| ... free space | index tuples   | BTPageOpaque|
//! | (24 bytes)     | array     |                | (grow down)    | (16 bytes) |
//! +----------------+-----------+----------------+----------------+------------+
//! ^                ^           ^                ^                ^            ^
//! 0            pd_lower    pd_upper         (tuples)       pd_special   PAGE_SIZE
//! ```
//!
//! # Index Tuple Format
//!
//! ```text
//! +------------------+------------+
//! | IndexTupleData   | key data   |
//! | (8 bytes)        | (variable) |
//! +------------------+------------+
//! ```
//!
//! # Example
//!
//! ```
//! use storage::btree::{BTreePage, BTreePageType};
//! use storage::page::PAGE_SIZE;
//!
//! // Create a new B-tree leaf page
//! let mut page = BTreePage::new(BTreePageType::Leaf);
//! assert!(page.is_leaf());
//!
//! // Insert an index entry
//! let key = 42i32.to_be_bytes();
//! let heap_ptr = (1u32, 1u16); // (block, offset)
//! page.insert_entry(&key, heap_ptr).unwrap();
//! ```

use bytemuck::{Pod, Zeroable};
use static_assertions::const_assert_eq;

use crate::error::{StorageError, StorageResult};
use crate::page::{ItemIdData, Page, PageHeaderData, PAGE_HEADER_SIZE, PAGE_SIZE};
use crate::types::{BlockNumber, OffsetNumber, TransactionId, INVALID_BLOCK_NUMBER};

/// Size of the B-tree page opaque data.
pub const BT_PAGE_OPAQUE_SIZE: usize = 16;

/// Size of the index tuple header.
pub const INDEX_TUPLE_HEADER_SIZE: usize = 8;

/// Maximum size of an index tuple (must fit on a page with overhead).
pub const BT_MAX_ITEM_SIZE: usize =
    (PAGE_SIZE - PAGE_HEADER_SIZE - BT_PAGE_OPAQUE_SIZE - std::mem::size_of::<ItemIdData>()) / 3;

/// B-tree page opaque data.
///
/// Matches PostgreSQL's `BTPageOpaqueData` from `nbtree.h`:
/// ```c
/// typedef struct BTPageOpaqueData {
///     BlockNumber btpo_prev;      /* left sibling, or P_NONE if leftmost */
///     BlockNumber btpo_next;      /* right sibling, or P_NONE if rightmost */
///     uint32      btpo_level;     /* tree level --- zero for leaf pages */
///     uint16      btpo_flags;     /* flag bits, see below */
///     uint16      btpo_cycleid;   /* vacuum cycle ID of latest split */
/// } BTPageOpaqueData;
/// ```
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct BTPageOpaqueData {
    /// Left sibling block number, or `INVALID_BLOCK_NUMBER` if leftmost.
    pub btpo_prev: BlockNumber,
    /// Right sibling block number, or `INVALID_BLOCK_NUMBER` if rightmost.
    pub btpo_next: BlockNumber,
    /// Tree level (0 for leaf pages).
    pub btpo_level: u32,
    /// Flag bits.
    pub btpo_flags: u16,
    /// Vacuum cycle ID of latest split.
    pub btpo_cycleid: u16,
}

const_assert_eq!(std::mem::size_of::<BTPageOpaqueData>(), BT_PAGE_OPAQUE_SIZE);

bitflags::bitflags! {
    /// B-tree page flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct BTreePageFlags: u16 {
        /// Page is a leaf page.
        const BTP_LEAF = 1 << 0;
        /// Page is the root page.
        const BTP_ROOT = 1 << 1;
        /// Page has been deleted (is on free list).
        const BTP_DELETED = 1 << 2;
        /// Page is a meta page.
        const BTP_META = 1 << 3;
        /// Page is a half-dead page (being deleted).
        const BTP_HALF_DEAD = 1 << 4;
        /// Page has been split, need to follow right link.
        const BTP_SPLIT_END = 1 << 5;
        /// Page has high key.
        const BTP_HAS_GARBAGE = 1 << 6;
        /// Page is incomplete split.
        const BTP_INCOMPLETE_SPLIT = 1 << 7;
    }
}

/// B-tree page type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BTreePageType {
    /// Leaf page (contains actual index entries).
    Leaf,
    /// Internal page (contains downlinks to child pages).
    Internal,
    /// Root page that is also a leaf (single-page tree).
    RootLeaf,
    /// Root page that is internal.
    RootInternal,
    /// Meta page (first page of index, contains metadata).
    Meta,
}

impl BTreePageType {
    /// Get the flags for this page type.
    #[must_use]
    pub fn flags(self) -> BTreePageFlags {
        match self {
            Self::Leaf => BTreePageFlags::BTP_LEAF,
            Self::Internal => BTreePageFlags::empty(),
            Self::RootLeaf => BTreePageFlags::BTP_ROOT | BTreePageFlags::BTP_LEAF,
            Self::RootInternal => BTreePageFlags::BTP_ROOT,
            Self::Meta => BTreePageFlags::BTP_META,
        }
    }
}

/// Index tuple data header.
///
/// Matches PostgreSQL's `IndexTupleData` from `itup.h`:
/// ```c
/// typedef struct IndexTupleData {
///     ItemPointerData t_tid;  /* reference TID to heap tuple */
///     unsigned short t_info;  /* various info about tuple */
/// } IndexTupleData;
/// ```
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct IndexTupleData {
    /// Heap TID - block number.
    pub t_tid_block: BlockNumber,
    /// Heap TID - offset number.
    pub t_tid_offset: OffsetNumber,
    /// Tuple info (size and flags).
    ///
    /// Low 13 bits: size of tuple
    /// Bit 13: has nulls
    /// Bit 14: has var-width attrs
    /// Bit 15: unused
    pub t_info: u16,
}

const_assert_eq!(std::mem::size_of::<IndexTupleData>(), INDEX_TUPLE_HEADER_SIZE);

/// Mask for extracting tuple size from t_info.
const INDEX_SIZE_MASK: u16 = 0x1FFF;

/// Flag indicating tuple has null values.
const INDEX_NULL_MASK: u16 = 0x2000;

/// Flag indicating tuple has variable-width attributes.
const INDEX_VAR_MASK: u16 = 0x4000;

impl IndexTupleData {
    /// Create a new index tuple header.
    #[must_use]
    pub fn new(heap_block: BlockNumber, heap_offset: OffsetNumber, size: u16) -> Self {
        Self {
            t_tid_block: heap_block,
            t_tid_offset: heap_offset,
            t_info: size & INDEX_SIZE_MASK,
        }
    }

    /// Get the tuple size.
    #[must_use]
    pub const fn size(&self) -> u16 {
        self.t_info & INDEX_SIZE_MASK
    }

    /// Check if tuple has nulls.
    #[must_use]
    pub const fn has_nulls(&self) -> bool {
        (self.t_info & INDEX_NULL_MASK) != 0
    }

    /// Check if tuple has variable-width attributes.
    #[must_use]
    pub const fn has_varwidth(&self) -> bool {
        (self.t_info & INDEX_VAR_MASK) != 0
    }

    /// Get the heap TID as (block, offset).
    #[must_use]
    pub const fn heap_tid(&self) -> (BlockNumber, OffsetNumber) {
        (self.t_tid_block, self.t_tid_offset)
    }
}

/// B-tree meta page data.
///
/// Stored on the first page of a B-tree index.
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct BTMetaPageData {
    /// B-tree version number.
    pub btm_version: u32,
    /// Block number of root page, or `INVALID_BLOCK_NUMBER`.
    pub btm_root: BlockNumber,
    /// Tree level of root page.
    pub btm_level: u32,
    /// Block number of first page in leftmost chain.
    pub btm_fastroot: BlockNumber,
    /// Tree level of fast root.
    pub btm_fastlevel: u32,
    /// Oldest xact that deleted a page.
    pub btm_oldest_btpo_xact: TransactionId,
    /// Last cleanup vacuum's oldest xmin.
    pub btm_last_cleanup_num_heap_tuples: f32,
    /// Are there deleted pages?
    pub btm_allequalimage: u8,
    /// Padding.
    _padding: [u8; 3],
}

/// Current B-tree version.
pub const BTREE_VERSION: u32 = 4;

/// B-tree page wrapper providing B-tree-specific operations.
pub struct BTreePage {
    /// Underlying page.
    page: Page,
}

impl BTreePage {
    /// Create a new B-tree page of the specified type.
    #[must_use]
    pub fn new(page_type: BTreePageType) -> Self {
        let mut page = Page::new();

        // Set up special space for BTPageOpaqueData
        let special_offset = PAGE_SIZE - BT_PAGE_OPAQUE_SIZE;
        {
            let header = page.header_mut_internal();
            header.pd_special = special_offset as u16;
            header.pd_upper = special_offset as u16;
        }

        // Initialize opaque data
        let opaque = BTPageOpaqueData {
            btpo_prev: INVALID_BLOCK_NUMBER,
            btpo_next: INVALID_BLOCK_NUMBER,
            btpo_level: u32::from(
                !(page_type == BTreePageType::Leaf || page_type == BTreePageType::RootLeaf),
            ),
            btpo_flags: page_type.flags().bits(),
            btpo_cycleid: 0,
        };

        let mut btpage = Self { page };
        btpage.set_opaque(&opaque);
        btpage
    }

    /// Create a B-tree page from an existing page.
    ///
    /// # Errors
    ///
    /// Returns an error if the page is not a valid B-tree page.
    pub fn from_page(page: Page) -> StorageResult<Self> {
        let btpage = Self { page };
        // Validate it has proper special space
        let header = btpage.page.header();
        if header.pd_special as usize != PAGE_SIZE - BT_PAGE_OPAQUE_SIZE {
            return Err(StorageError::InvalidPageLayout {
                message: "not a B-tree page (wrong special size)".to_string(),
            });
        }
        Ok(btpage)
    }

    /// Get the underlying page.
    #[must_use]
    pub fn page(&self) -> &Page {
        &self.page
    }

    /// Get a mutable reference to the underlying page.
    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.page
    }

    /// Get the opaque data.
    #[must_use]
    pub fn opaque(&self) -> &BTPageOpaqueData {
        let special_offset = PAGE_SIZE - BT_PAGE_OPAQUE_SIZE;
        let bytes = &self.page.as_bytes()[special_offset..];
        bytemuck::from_bytes(bytes)
    }

    /// Set the opaque data.
    pub fn set_opaque(&mut self, opaque: &BTPageOpaqueData) {
        let special_offset = PAGE_SIZE - BT_PAGE_OPAQUE_SIZE;
        let bytes = self.page.as_bytes_mut();
        bytes[special_offset..].copy_from_slice(bytemuck::bytes_of(opaque));
    }

    /// Check if this is a leaf page.
    #[must_use]
    pub fn is_leaf(&self) -> bool {
        let flags = BTreePageFlags::from_bits_truncate(self.opaque().btpo_flags);
        flags.contains(BTreePageFlags::BTP_LEAF)
    }

    /// Check if this is the root page.
    #[must_use]
    pub fn is_root(&self) -> bool {
        let flags = BTreePageFlags::from_bits_truncate(self.opaque().btpo_flags);
        flags.contains(BTreePageFlags::BTP_ROOT)
    }

    /// Check if this is a meta page.
    #[must_use]
    pub fn is_meta(&self) -> bool {
        let flags = BTreePageFlags::from_bits_truncate(self.opaque().btpo_flags);
        flags.contains(BTreePageFlags::BTP_META)
    }

    /// Get the tree level of this page.
    #[must_use]
    pub fn level(&self) -> u32 {
        self.opaque().btpo_level
    }

    /// Get the left sibling block number.
    #[must_use]
    pub fn left_sibling(&self) -> Option<BlockNumber> {
        let prev = self.opaque().btpo_prev;
        if prev == INVALID_BLOCK_NUMBER {
            None
        } else {
            Some(prev)
        }
    }

    /// Get the right sibling block number.
    #[must_use]
    pub fn right_sibling(&self) -> Option<BlockNumber> {
        let next = self.opaque().btpo_next;
        if next == INVALID_BLOCK_NUMBER {
            None
        } else {
            Some(next)
        }
    }

    /// Set sibling pointers.
    pub fn set_siblings(&mut self, left: Option<BlockNumber>, right: Option<BlockNumber>) {
        let mut opaque = *self.opaque();
        opaque.btpo_prev = left.unwrap_or(INVALID_BLOCK_NUMBER);
        opaque.btpo_next = right.unwrap_or(INVALID_BLOCK_NUMBER);
        self.set_opaque(&opaque);
    }

    /// Get the number of index entries on this page.
    #[must_use]
    pub fn num_entries(&self) -> usize {
        self.page.item_count()
    }

    /// Get the amount of free space available for new entries.
    #[must_use]
    pub fn free_space(&self) -> usize {
        self.page.free_space()
    }

    /// Insert an index entry (for leaf pages).
    ///
    /// # Arguments
    ///
    /// * `key` - The index key data
    /// * `heap_ptr` - The heap tuple pointer (block, offset)
    ///
    /// # Errors
    ///
    /// Returns an error if there is not enough space.
    pub fn insert_entry(
        &mut self,
        key: &[u8],
        heap_ptr: (BlockNumber, OffsetNumber),
    ) -> StorageResult<usize> {
        let tuple_size = INDEX_TUPLE_HEADER_SIZE + key.len();

        // Build the index tuple
        let header = IndexTupleData::new(heap_ptr.0, heap_ptr.1, tuple_size as u16);
        let mut tuple_data = Vec::with_capacity(tuple_size);
        tuple_data.extend_from_slice(bytemuck::bytes_of(&header));
        tuple_data.extend_from_slice(key);

        // Insert into page
        self.page.insert_tuple(&tuple_data)
    }

    /// Insert a downlink entry (for internal pages).
    ///
    /// # Arguments
    ///
    /// * `key` - The index key data (high key of child page)
    /// * `child_block` - The child page block number
    ///
    /// # Errors
    ///
    /// Returns an error if there is not enough space.
    pub fn insert_downlink(
        &mut self,
        key: &[u8],
        child_block: BlockNumber,
    ) -> StorageResult<usize> {
        // For internal pages, we store the child block number in the TID fields
        // This is a simplification; PostgreSQL uses a more complex scheme
        self.insert_entry(key, (child_block, 0))
    }

    /// Get an index entry by position.
    ///
    /// Returns the key data and heap pointer.
    #[must_use]
    pub fn get_entry(&self, index: usize) -> Option<(Vec<u8>, (BlockNumber, OffsetNumber))> {
        let tuple_data = self.page.get_tuple(index)?;
        if tuple_data.len() < INDEX_TUPLE_HEADER_SIZE {
            return None;
        }

        let header: &IndexTupleData =
            bytemuck::from_bytes(&tuple_data[..INDEX_TUPLE_HEADER_SIZE]);
        let key = tuple_data[INDEX_TUPLE_HEADER_SIZE..].to_vec();

        Some((key, header.heap_tid()))
    }

    /// Search for a key in a leaf page.
    ///
    /// Returns the index of the first entry >= key, or `num_entries()` if all
    /// entries are less than the key.
    ///
    /// The comparison function should return `Ordering::Less` if the entry key
    /// is less than the search key.
    pub fn search_leaf<F>(&self, compare: F) -> usize
    where
        F: Fn(&[u8]) -> std::cmp::Ordering,
    {
        let n = self.num_entries();
        if n == 0 {
            return 0;
        }

        // Binary search
        let mut low = 0;
        let mut high = n;

        while low < high {
            let mid = low + (high - low) / 2;
            if let Some((key, _)) = self.get_entry(mid) {
                match compare(&key) {
                    std::cmp::Ordering::Less => low = mid + 1,
                    _ => high = mid,
                }
            } else {
                break;
            }
        }

        low
    }

    /// Search for a key in an internal page.
    ///
    /// Returns the child block number to follow for the given key.
    pub fn search_internal<F>(&self, compare: F) -> Option<BlockNumber>
    where
        F: Fn(&[u8]) -> std::cmp::Ordering,
    {
        let pos = self.search_leaf(compare);
        // In internal pages, we follow the pointer at position `pos`
        // If pos == 0, we follow the leftmost child
        // Otherwise, we follow the child at pos - 1
        let entry_pos = if pos == 0 { 0 } else { pos - 1 };
        self.get_entry(entry_pos).map(|(_, (block, _))| block)
    }
}

impl std::fmt::Debug for BTreePage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTreePage")
            .field("is_leaf", &self.is_leaf())
            .field("is_root", &self.is_root())
            .field("level", &self.level())
            .field("num_entries", &self.num_entries())
            .field("free_space", &self.free_space())
            .finish()
    }
}

impl Page {
    /// Get mutable access to header (internal use).
    fn header_mut_internal(&mut self) -> &mut PageHeaderData {
        bytemuck::from_bytes_mut(&mut self.as_bytes_mut()[..PAGE_HEADER_SIZE])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btpage_opaque_size() {
        assert_eq!(std::mem::size_of::<BTPageOpaqueData>(), BT_PAGE_OPAQUE_SIZE);
    }

    #[test]
    fn index_tuple_size() {
        assert_eq!(std::mem::size_of::<IndexTupleData>(), INDEX_TUPLE_HEADER_SIZE);
    }

    #[test]
    fn new_leaf_page() {
        let page = BTreePage::new(BTreePageType::Leaf);
        assert!(page.is_leaf());
        assert!(!page.is_root());
        assert!(!page.is_meta());
        assert_eq!(page.level(), 0);
        assert_eq!(page.num_entries(), 0);
    }

    #[test]
    fn new_root_leaf_page() {
        let page = BTreePage::new(BTreePageType::RootLeaf);
        assert!(page.is_leaf());
        assert!(page.is_root());
        assert_eq!(page.level(), 0);
    }

    #[test]
    fn new_internal_page() {
        let page = BTreePage::new(BTreePageType::Internal);
        assert!(!page.is_leaf());
        assert!(!page.is_root());
        assert_eq!(page.level(), 1);
    }

    #[test]
    fn sibling_pointers() {
        let mut page = BTreePage::new(BTreePageType::Leaf);
        assert!(page.left_sibling().is_none());
        assert!(page.right_sibling().is_none());

        page.set_siblings(Some(10), Some(20));
        assert_eq!(page.left_sibling(), Some(10));
        assert_eq!(page.right_sibling(), Some(20));
    }

    #[test]
    fn insert_and_get_entry() {
        let mut page = BTreePage::new(BTreePageType::Leaf);

        let key = 42i32.to_be_bytes();
        let heap_ptr = (100, 5);

        let index = page.insert_entry(&key, heap_ptr).unwrap();
        assert_eq!(index, 0);
        assert_eq!(page.num_entries(), 1);

        let (retrieved_key, retrieved_ptr) = page.get_entry(0).unwrap();
        assert_eq!(retrieved_key, key);
        assert_eq!(retrieved_ptr, heap_ptr);
    }

    #[test]
    fn insert_multiple_entries() {
        let mut page = BTreePage::new(BTreePageType::Leaf);

        for i in 0..10 {
            let key = (i as i32).to_be_bytes();
            page.insert_entry(&key, (i, 1)).unwrap();
        }

        assert_eq!(page.num_entries(), 10);

        for i in 0..10 {
            let (key, ptr) = page.get_entry(i).unwrap();
            let expected_key = (i as i32).to_be_bytes();
            assert_eq!(key, expected_key);
            assert_eq!(ptr.0, i as u32);
        }
    }

    #[test]
    fn search_leaf_page() {
        let mut page = BTreePage::new(BTreePageType::Leaf);

        // Insert sorted keys: 10, 20, 30, 40, 50
        for i in [10i32, 20, 30, 40, 50] {
            let key = i.to_be_bytes();
            page.insert_entry(&key, (i as u32, 1)).unwrap();
        }

        // Search for existing key
        let pos = page.search_leaf(|k| {
            let k_val = i32::from_be_bytes(k.try_into().unwrap());
            k_val.cmp(&30)
        });
        assert_eq!(pos, 2); // 30 is at index 2

        // Search for non-existing key (25 should return position of 30)
        let pos = page.search_leaf(|k| {
            let k_val = i32::from_be_bytes(k.try_into().unwrap());
            k_val.cmp(&25)
        });
        assert_eq!(pos, 2);

        // Search for key smaller than all
        let pos = page.search_leaf(|k| {
            let k_val = i32::from_be_bytes(k.try_into().unwrap());
            k_val.cmp(&5)
        });
        assert_eq!(pos, 0);

        // Search for key larger than all
        let pos = page.search_leaf(|k| {
            let k_val = i32::from_be_bytes(k.try_into().unwrap());
            k_val.cmp(&100)
        });
        assert_eq!(pos, 5);
    }

    #[test]
    fn index_tuple_data() {
        let tuple = IndexTupleData::new(100, 5, 24);
        assert_eq!(tuple.heap_tid(), (100, 5));
        assert_eq!(tuple.size(), 24);
        assert!(!tuple.has_nulls());
        assert!(!tuple.has_varwidth());
    }

    #[test]
    fn page_free_space() {
        let page = BTreePage::new(BTreePageType::Leaf);
        let initial_free = page.free_space();

        // Should have space for entries
        assert!(initial_free > 100);

        // After special space is reserved
        let expected_max = PAGE_SIZE - PAGE_HEADER_SIZE - BT_PAGE_OPAQUE_SIZE;
        assert!(initial_free <= expected_max);
    }

    #[test]
    fn internal_page_downlinks() {
        let mut page = BTreePage::new(BTreePageType::Internal);

        // Insert downlinks
        page.insert_downlink(&10i32.to_be_bytes(), 100).unwrap();
        page.insert_downlink(&20i32.to_be_bytes(), 200).unwrap();
        page.insert_downlink(&30i32.to_be_bytes(), 300).unwrap();

        assert_eq!(page.num_entries(), 3);

        // Search for child block
        let child = page.search_internal(|k| {
            let k_val = i32::from_be_bytes(k.try_into().unwrap());
            k_val.cmp(&15)
        });
        assert_eq!(child, Some(100)); // Should follow first downlink
    }
}
