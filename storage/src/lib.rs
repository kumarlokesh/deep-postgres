//! PostgreSQL storage engine implementation.
//!
//! This crate implements the core storage subsystem of PostgreSQL, including:
//!
//! - **Page layout**: 8KB pages with headers, line pointers, and tuple data
//! - **Heap tuples**: Variable-length tuples with MVCC metadata
//! - **Buffer management**: Pin/unpin, clock sweep replacement (planned)
//! - **B-tree indexes**: Basic insert/search operations (planned)
//!
//! # Architecture
//!
//! The storage layer follows PostgreSQL's design closely:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    Buffer Manager                        │
//! │  (manages shared buffer pool, handles I/O)              │
//! └─────────────────────────────────────────────────────────┘
//!                            │
//!                            ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │                      Page Layer                          │
//! │  (8KB pages, headers, line pointers, free space mgmt)   │
//! └─────────────────────────────────────────────────────────┘
//!                            │
//!                            ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │                     Tuple Layer                          │
//! │  (heap tuples, MVCC fields, null bitmap, user data)     │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```
//! use storage::page::{Page, PAGE_SIZE};
//!
//! // Create a new page
//! let mut page = Page::new();
//! assert!(page.is_empty());
//!
//! // Insert a tuple
//! let tuple_data = b"hello, postgres!";
//! let index = page.insert_tuple(tuple_data).unwrap();
//!
//! // Retrieve the tuple
//! let retrieved = page.get_tuple(index).unwrap();
//! assert_eq!(retrieved, tuple_data);
//! ```

pub mod error;
pub mod page;
pub mod tuple;
pub mod types;

// Re-export commonly used items
pub use error::{StorageError, StorageResult};
pub use page::{ItemIdData, Page, PageHeaderData, PAGE_SIZE};
pub use tuple::{HeapTupleHeader, HEAP_TUPLE_HEADER_SIZE};
pub use types::{BlockNumber, CommandId, OffsetNumber, TransactionId};
