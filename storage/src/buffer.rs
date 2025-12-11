//! Buffer manager implementation.
//!
//! This module implements PostgreSQL's buffer manager as defined in
//! `src/backend/storage/buffer/`. The buffer manager provides:
//!
//! - A shared buffer pool of fixed-size pages
//! - Pin/unpin semantics for safe concurrent access
//! - Clock sweep replacement algorithm
//! - Dirty page tracking for write-back
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      BufferPool                              │
//! │  ┌─────────┬─────────┬─────────┬─────────┬─────────┐        │
//! │  │ Buffer  │ Buffer  │ Buffer  │ Buffer  │  ...    │        │
//! │  │   0     │   1     │   2     │   3     │         │        │
//! │  └────┬────┴────┬────┴────┬────┴────┬────┴─────────┘        │
//! │       │         │         │         │                        │
//! │  ┌────▼────┬────▼────┬────▼────┬────▼────┬─────────┐        │
//! │  │BuffDesc │BuffDesc │BuffDesc │BuffDesc │  ...    │        │
//! │  │ tag,    │ tag,    │ tag,    │ tag,    │         │        │
//! │  │ state   │ state   │ state   │ state   │         │        │
//! │  └─────────┴─────────┴─────────┴─────────┴─────────┘        │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```
//! use storage::buffer::{BufferPool, BufferTag};
//!
//! // Create a buffer pool with 16 buffers
//! let mut pool = BufferPool::new(16);
//!
//! // Read a page (will allocate a new buffer)
//! let tag = BufferTag::new(1, 0, 0); // relation 1, fork 0, block 0
//! let buffer_id = pool.read_buffer(tag).unwrap();
//!
//! // Access the page
//! {
//!     let page = pool.get_page(buffer_id).unwrap();
//!     assert!(page.is_empty());
//! }
//!
//! // Unpin when done
//! pool.unpin_buffer(buffer_id);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::{StorageError, StorageResult};
use crate::page::Page;
use crate::types::{BlockNumber, Oid, INVALID_BLOCK_NUMBER};

/// Buffer ID type (index into buffer pool).
pub type BufferId = u32;

/// Invalid buffer ID.
pub const INVALID_BUFFER_ID: BufferId = u32::MAX;

/// Fork number for relation forks.
///
/// PostgreSQL stores different aspects of a relation in different "forks":
/// - Main fork (0): The actual table/index data
/// - FSM fork (1): Free space map
/// - VM fork (2): Visibility map
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ForkNumber {
    /// Main data fork.
    Main = 0,
    /// Free space map fork.
    Fsm = 1,
    /// Visibility map fork.
    Vm = 2,
    /// Initialization fork.
    Init = 3,
}

impl Default for ForkNumber {
    fn default() -> Self {
        Self::Main
    }
}

/// Buffer tag - uniquely identifies a disk block.
///
/// Matches PostgreSQL's `BufferTag` from `buf_internals.h`:
/// ```c
/// typedef struct buftag {
///     Oid         spcOid;     /* tablespace oid */
///     Oid         dbOid;      /* database oid */
///     RelFileNumber relNumber; /* relation file number */
///     ForkNumber  forkNum;
///     BlockNumber blockNum;
/// } BufferTag;
/// ```
///
/// We simplify this to just (relation_id, fork, block) for now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BufferTag {
    /// Relation OID (simplified from full RelFileLocator).
    pub relation_id: Oid,
    /// Fork number.
    pub fork: ForkNumber,
    /// Block number within the fork.
    pub block_num: BlockNumber,
}

impl BufferTag {
    /// Create a new buffer tag.
    #[must_use]
    pub const fn new(relation_id: Oid, fork_num: u8, block_num: BlockNumber) -> Self {
        let fork = match fork_num {
            1 => ForkNumber::Fsm,
            2 => ForkNumber::Vm,
            3 => ForkNumber::Init,
            _ => ForkNumber::Main,
        };
        Self {
            relation_id,
            fork,
            block_num,
        }
    }

    /// Create a tag for the main fork.
    #[must_use]
    pub const fn main(relation_id: Oid, block_num: BlockNumber) -> Self {
        Self {
            relation_id,
            fork: ForkNumber::Main,
            block_num,
        }
    }
}

impl Default for BufferTag {
    fn default() -> Self {
        Self {
            relation_id: 0,
            fork: ForkNumber::Main,
            block_num: INVALID_BLOCK_NUMBER,
        }
    }
}

bitflags::bitflags! {
    /// Buffer state flags.
    ///
    /// Based on PostgreSQL's buffer state flags from `buf_internals.h`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct BufferState: u32 {
        /// Buffer contains valid data.
        const BM_VALID = 1 << 0;
        /// Buffer has been modified.
        const BM_DIRTY = 1 << 1;
        /// Buffer is being written out.
        const BM_IO_IN_PROGRESS = 1 << 2;
        /// Buffer is locked for exclusive access.
        const BM_LOCKED = 1 << 3;
        /// Buffer was recently accessed (for clock sweep).
        const BM_RECENTLY_USED = 1 << 4;
    }
}

/// Buffer descriptor - metadata for a buffer.
///
/// Matches PostgreSQL's `BufferDesc` from `buf_internals.h`.
#[derive(Debug)]
pub struct BufferDesc {
    /// Tag identifying the disk block.
    pub tag: BufferTag,
    /// Buffer state flags.
    pub state: BufferState,
    /// Reference count (number of pins).
    pub refcount: AtomicU32,
    /// Usage count for clock sweep (0-5 in PG).
    pub usage_count: u8,
}

impl BufferDesc {
    /// Create a new empty buffer descriptor.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tag: BufferTag::default(),
            state: BufferState::empty(),
            refcount: AtomicU32::new(0),
            usage_count: 0,
        }
    }

    /// Check if the buffer is pinned.
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        self.refcount.load(Ordering::Acquire) > 0
    }

    /// Check if the buffer contains valid data.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.state.contains(BufferState::BM_VALID)
    }

    /// Check if the buffer is dirty.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.state.contains(BufferState::BM_DIRTY)
    }

    /// Pin the buffer (increment refcount).
    pub fn pin(&self) {
        self.refcount.fetch_add(1, Ordering::AcqRel);
    }

    /// Unpin the buffer (decrement refcount).
    ///
    /// # Panics
    ///
    /// Panics if the buffer is not pinned.
    pub fn unpin(&self) {
        let old = self.refcount.fetch_sub(1, Ordering::AcqRel);
        assert!(old > 0, "unpinning buffer that is not pinned");
    }

    /// Get the current pin count.
    #[must_use]
    pub fn pin_count(&self) -> u32 {
        self.refcount.load(Ordering::Acquire)
    }
}

impl Default for BufferDesc {
    fn default() -> Self {
        Self::new()
    }
}

/// Maximum usage count for clock sweep algorithm.
const MAX_USAGE_COUNT: u8 = 5;

/// Buffer pool - manages a collection of shared buffers.
///
/// Implements PostgreSQL's shared buffer pool with:
/// - Fixed number of buffer slots
/// - Hash table for tag -> buffer_id lookup
/// - Clock sweep replacement algorithm
pub struct BufferPool {
    /// Buffer descriptors.
    descriptors: Vec<BufferDesc>,
    /// Actual page data.
    pages: Vec<Page>,
    /// Tag to buffer ID mapping.
    tag_map: HashMap<BufferTag, BufferId>,
    /// Clock hand for replacement algorithm.
    clock_hand: usize,
    /// Number of buffers in the pool.
    num_buffers: usize,
}

impl BufferPool {
    /// Create a new buffer pool with the specified number of buffers.
    ///
    /// # Panics
    ///
    /// Panics if `num_buffers` is 0.
    #[must_use]
    pub fn new(num_buffers: usize) -> Self {
        assert!(num_buffers > 0, "buffer pool must have at least one buffer");

        let mut descriptors = Vec::with_capacity(num_buffers);
        let mut pages = Vec::with_capacity(num_buffers);

        for _ in 0..num_buffers {
            descriptors.push(BufferDesc::new());
            pages.push(Page::new());
        }

        Self {
            descriptors,
            pages,
            tag_map: HashMap::with_capacity(num_buffers),
            clock_hand: 0,
            num_buffers,
        }
    }

    /// Get the number of buffers in the pool.
    #[must_use]
    pub fn num_buffers(&self) -> usize {
        self.num_buffers
    }

    /// Read a buffer for the given tag.
    ///
    /// If the page is already in the buffer pool, returns the existing buffer.
    /// Otherwise, allocates a new buffer (possibly evicting an old one).
    ///
    /// The returned buffer is pinned and must be unpinned when done.
    ///
    /// # Errors
    ///
    /// Returns an error if no buffer can be allocated (all buffers pinned).
    pub fn read_buffer(&mut self, tag: BufferTag) -> StorageResult<BufferId> {
        // Check if already in pool
        if let Some(&buffer_id) = self.tag_map.get(&tag) {
            let desc = &mut self.descriptors[buffer_id as usize];
            desc.pin();
            desc.usage_count = MAX_USAGE_COUNT;
            return Ok(buffer_id);
        }

        // Need to allocate a new buffer
        let buffer_id = self.allocate_buffer()?;
        let desc = &mut self.descriptors[buffer_id as usize];

        // Remove old tag mapping if this buffer was previously used
        if desc.is_valid() {
            self.tag_map.remove(&desc.tag);
        }

        // Initialize the buffer
        desc.tag = tag;
        desc.state = BufferState::BM_VALID;
        desc.usage_count = MAX_USAGE_COUNT;
        desc.pin();

        // Initialize the page
        self.pages[buffer_id as usize].init();

        // Add to tag map
        self.tag_map.insert(tag, buffer_id);

        Ok(buffer_id)
    }

    /// Allocate a buffer using clock sweep algorithm.
    ///
    /// # Errors
    ///
    /// Returns an error if all buffers are pinned.
    fn allocate_buffer(&mut self) -> StorageResult<BufferId> {
        let start = self.clock_hand;
        let mut loops = 0;

        loop {
            let buffer_id = self.clock_hand;
            self.clock_hand = (self.clock_hand + 1) % self.num_buffers;

            let desc = &mut self.descriptors[buffer_id];

            // Skip pinned buffers
            if desc.is_pinned() {
                if self.clock_hand == start {
                    loops += 1;
                    if loops > 1 {
                        return Err(StorageError::BufferPoolExhausted);
                    }
                }
                continue;
            }

            // Check usage count
            if desc.usage_count > 0 {
                desc.usage_count -= 1;
                continue;
            }

            // Found a victim buffer
            // If dirty, would need to write out first (not implemented yet)
            if desc.is_dirty() {
                // In a real implementation, we'd write the page to disk here
                desc.state.remove(BufferState::BM_DIRTY);
            }

            return Ok(buffer_id as BufferId);
        }
    }

    /// Unpin a buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer ID is invalid.
    pub fn unpin_buffer(&mut self, buffer_id: BufferId) -> StorageResult<()> {
        self.validate_buffer_id(buffer_id)?;
        self.descriptors[buffer_id as usize].unpin();
        Ok(())
    }

    /// Get a reference to a page.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer ID is invalid or the buffer is not pinned.
    pub fn get_page(&self, buffer_id: BufferId) -> StorageResult<&Page> {
        self.validate_buffer_id(buffer_id)?;
        let desc = &self.descriptors[buffer_id as usize];
        if !desc.is_pinned() {
            return Err(StorageError::BufferNotPinned { buffer_id });
        }
        Ok(&self.pages[buffer_id as usize])
    }

    /// Get a mutable reference to a page.
    ///
    /// This also marks the buffer as dirty.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer ID is invalid or the buffer is not pinned.
    pub fn get_page_mut(&mut self, buffer_id: BufferId) -> StorageResult<&mut Page> {
        self.validate_buffer_id(buffer_id)?;
        let desc = &mut self.descriptors[buffer_id as usize];
        if !desc.is_pinned() {
            return Err(StorageError::BufferNotPinned { buffer_id });
        }
        desc.state.insert(BufferState::BM_DIRTY);
        Ok(&mut self.pages[buffer_id as usize])
    }

    /// Mark a buffer as dirty.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer ID is invalid.
    pub fn mark_dirty(&mut self, buffer_id: BufferId) -> StorageResult<()> {
        self.validate_buffer_id(buffer_id)?;
        self.descriptors[buffer_id as usize]
            .state
            .insert(BufferState::BM_DIRTY);
        Ok(())
    }

    /// Get the buffer descriptor for a buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer ID is invalid.
    pub fn get_descriptor(&self, buffer_id: BufferId) -> StorageResult<&BufferDesc> {
        self.validate_buffer_id(buffer_id)?;
        Ok(&self.descriptors[buffer_id as usize])
    }

    /// Find a buffer by tag without pinning.
    ///
    /// Returns `None` if the page is not in the buffer pool.
    #[must_use]
    pub fn lookup_buffer(&self, tag: &BufferTag) -> Option<BufferId> {
        self.tag_map.get(tag).copied()
    }

    /// Get statistics about the buffer pool.
    #[must_use]
    pub fn stats(&self) -> BufferPoolStats {
        let mut valid = 0;
        let mut dirty = 0;
        let mut pinned = 0;

        for desc in &self.descriptors {
            if desc.is_valid() {
                valid += 1;
            }
            if desc.is_dirty() {
                dirty += 1;
            }
            if desc.is_pinned() {
                pinned += 1;
            }
        }

        BufferPoolStats {
            total: self.num_buffers,
            valid,
            dirty,
            pinned,
        }
    }

    /// Validate a buffer ID.
    fn validate_buffer_id(&self, buffer_id: BufferId) -> StorageResult<()> {
        if buffer_id as usize >= self.num_buffers {
            return Err(StorageError::InvalidBufferId { buffer_id });
        }
        Ok(())
    }

    /// Flush all dirty buffers.
    ///
    /// In a real implementation, this would write pages to disk.
    /// For now, it just clears the dirty flags.
    pub fn flush_all(&mut self) {
        for desc in &mut self.descriptors {
            if desc.is_dirty() {
                // Would write to disk here
                desc.state.remove(BufferState::BM_DIRTY);
            }
        }
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("num_buffers", &self.num_buffers)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// Buffer pool statistics.
#[derive(Debug, Clone, Copy)]
pub struct BufferPoolStats {
    /// Total number of buffers.
    pub total: usize,
    /// Number of valid buffers.
    pub valid: usize,
    /// Number of dirty buffers.
    pub dirty: usize,
    /// Number of pinned buffers.
    pub pinned: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_pool_creation() {
        let pool = BufferPool::new(16);
        assert_eq!(pool.num_buffers(), 16);

        let stats = pool.stats();
        assert_eq!(stats.total, 16);
        assert_eq!(stats.valid, 0);
        assert_eq!(stats.dirty, 0);
        assert_eq!(stats.pinned, 0);
    }

    #[test]
    fn read_buffer_allocates_new() {
        let mut pool = BufferPool::new(16);
        let tag = BufferTag::main(1, 0);

        let buffer_id = pool.read_buffer(tag).unwrap();
        assert_eq!(buffer_id, 0);

        let desc = pool.get_descriptor(buffer_id).unwrap();
        assert!(desc.is_valid());
        assert!(desc.is_pinned());
        assert!(!desc.is_dirty());
    }

    #[test]
    fn read_buffer_returns_existing() {
        let mut pool = BufferPool::new(16);
        let tag = BufferTag::main(1, 0);

        let buffer_id1 = pool.read_buffer(tag).unwrap();
        pool.unpin_buffer(buffer_id1).unwrap();

        let buffer_id2 = pool.read_buffer(tag).unwrap();
        assert_eq!(buffer_id1, buffer_id2);
    }

    #[test]
    fn pin_unpin_semantics() {
        let mut pool = BufferPool::new(16);
        let tag = BufferTag::main(1, 0);

        let buffer_id = pool.read_buffer(tag).unwrap();
        assert_eq!(pool.get_descriptor(buffer_id).unwrap().pin_count(), 1);

        // Pin again
        pool.get_descriptor(buffer_id).unwrap().pin();
        assert_eq!(pool.get_descriptor(buffer_id).unwrap().pin_count(), 2);

        // Unpin twice
        pool.unpin_buffer(buffer_id).unwrap();
        assert_eq!(pool.get_descriptor(buffer_id).unwrap().pin_count(), 1);

        pool.unpin_buffer(buffer_id).unwrap();
        assert_eq!(pool.get_descriptor(buffer_id).unwrap().pin_count(), 0);
    }

    #[test]
    fn get_page_requires_pin() {
        let mut pool = BufferPool::new(16);
        let tag = BufferTag::main(1, 0);

        let buffer_id = pool.read_buffer(tag).unwrap();
        pool.unpin_buffer(buffer_id).unwrap();

        let result = pool.get_page(buffer_id);
        assert!(matches!(result, Err(StorageError::BufferNotPinned { .. })));
    }

    #[test]
    fn get_page_mut_marks_dirty() {
        let mut pool = BufferPool::new(16);
        let tag = BufferTag::main(1, 0);

        let buffer_id = pool.read_buffer(tag).unwrap();
        assert!(!pool.get_descriptor(buffer_id).unwrap().is_dirty());

        let _page = pool.get_page_mut(buffer_id).unwrap();
        assert!(pool.get_descriptor(buffer_id).unwrap().is_dirty());
    }

    #[test]
    fn clock_sweep_eviction() {
        let mut pool = BufferPool::new(4);

        // Fill the pool
        for i in 0..4 {
            let tag = BufferTag::main(1, i);
            let buffer_id = pool.read_buffer(tag).unwrap();
            pool.unpin_buffer(buffer_id).unwrap();
        }

        assert_eq!(pool.stats().valid, 4);

        // Read a new page - should evict one
        let tag = BufferTag::main(1, 100);
        let buffer_id = pool.read_buffer(tag).unwrap();

        // Should still have 4 valid buffers (one was replaced)
        assert_eq!(pool.stats().valid, 4);
        assert!(pool.get_descriptor(buffer_id).unwrap().is_pinned());
    }

    #[test]
    fn buffer_pool_exhausted() {
        let mut pool = BufferPool::new(2);

        // Pin all buffers
        let tag1 = BufferTag::main(1, 0);
        let tag2 = BufferTag::main(1, 1);
        let _b1 = pool.read_buffer(tag1).unwrap();
        let _b2 = pool.read_buffer(tag2).unwrap();

        // Try to read another - should fail
        let tag3 = BufferTag::main(1, 2);
        let result = pool.read_buffer(tag3);
        assert!(matches!(result, Err(StorageError::BufferPoolExhausted)));
    }

    #[test]
    fn lookup_buffer() {
        let mut pool = BufferPool::new(16);
        let tag = BufferTag::main(1, 0);

        assert!(pool.lookup_buffer(&tag).is_none());

        let buffer_id = pool.read_buffer(tag).unwrap();
        assert_eq!(pool.lookup_buffer(&tag), Some(buffer_id));
    }

    #[test]
    fn buffer_tag_creation() {
        let tag1 = BufferTag::new(1, 0, 42);
        assert_eq!(tag1.relation_id, 1);
        assert_eq!(tag1.fork, ForkNumber::Main);
        assert_eq!(tag1.block_num, 42);

        let tag2 = BufferTag::main(2, 100);
        assert_eq!(tag2.relation_id, 2);
        assert_eq!(tag2.fork, ForkNumber::Main);
        assert_eq!(tag2.block_num, 100);
    }

    #[test]
    fn insert_tuple_through_buffer() {
        let mut pool = BufferPool::new(16);
        let tag = BufferTag::main(1, 0);

        let buffer_id = pool.read_buffer(tag).unwrap();

        // Insert a tuple
        {
            let page = pool.get_page_mut(buffer_id).unwrap();
            let tuple_data = b"test tuple data";
            let index = page.insert_tuple(tuple_data).unwrap();
            assert_eq!(index, 0);
        }

        // Verify it's there
        {
            let page = pool.get_page(buffer_id).unwrap();
            let tuple = page.get_tuple(0).unwrap();
            assert_eq!(tuple, b"test tuple data");
        }

        // Buffer should be dirty
        assert!(pool.get_descriptor(buffer_id).unwrap().is_dirty());
    }

    #[test]
    fn flush_all_clears_dirty() {
        let mut pool = BufferPool::new(16);
        let tag = BufferTag::main(1, 0);

        let buffer_id = pool.read_buffer(tag).unwrap();
        pool.mark_dirty(buffer_id).unwrap();
        assert!(pool.get_descriptor(buffer_id).unwrap().is_dirty());

        pool.flush_all();
        assert!(!pool.get_descriptor(buffer_id).unwrap().is_dirty());
    }
}
