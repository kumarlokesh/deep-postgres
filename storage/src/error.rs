//! Error types for the storage crate.

use thiserror::Error;

/// Result type alias for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

/// Errors that can occur in storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Page is full and cannot accommodate the requested tuple.
    #[error("page full: required {required} bytes, available {available} bytes")]
    PageFull {
        /// Bytes required for the operation.
        required: usize,
        /// Bytes available on the page.
        available: usize,
    },

    /// Invalid item index.
    #[error("invalid item index: {index}")]
    InvalidItemIndex {
        /// The invalid index.
        index: usize,
    },

    /// Invalid page size.
    #[error("invalid page size: expected {expected}, got {actual}")]
    InvalidPageSize {
        /// Expected page size.
        expected: usize,
        /// Actual page size.
        actual: usize,
    },

    /// Invalid page layout.
    #[error("invalid page layout: {message}")]
    InvalidPageLayout {
        /// Description of the layout error.
        message: String,
    },

    /// Tuple too large for any page.
    #[error("tuple too large: {size} bytes exceeds maximum {max_size} bytes")]
    TupleTooLarge {
        /// Size of the tuple.
        size: usize,
        /// Maximum allowed size.
        max_size: usize,
    },

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
