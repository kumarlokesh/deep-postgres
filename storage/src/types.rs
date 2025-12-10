//! Common type definitions matching PostgreSQL conventions.
//!
//! These types mirror PostgreSQL's internal type definitions from various headers
//! like `c.h`, `transam.h`, etc.

/// Transaction ID type (matches PostgreSQL's `TransactionId`).
///
/// In PostgreSQL, this is a 32-bit unsigned integer that wraps around.
/// Special values:
/// - 0 (`InvalidTransactionId`): Invalid/unset
/// - 1 (`BootstrapTransactionId`): Bootstrap transaction
/// - 2 (`FrozenTransactionId`): Frozen (always visible)
/// - 3+ : Normal transaction IDs
pub type TransactionId = u32;

/// Invalid transaction ID.
pub const INVALID_TRANSACTION_ID: TransactionId = 0;

/// Bootstrap transaction ID.
pub const BOOTSTRAP_TRANSACTION_ID: TransactionId = 1;

/// Frozen transaction ID (tuple is always visible).
pub const FROZEN_TRANSACTION_ID: TransactionId = 2;

/// First normal transaction ID.
pub const FIRST_NORMAL_TRANSACTION_ID: TransactionId = 3;

/// Command ID type (matches PostgreSQL's `CommandId`).
///
/// Identifies commands within a transaction for visibility purposes.
pub type CommandId = u32;

/// Invalid command ID.
pub const INVALID_COMMAND_ID: CommandId = 0;

/// First valid command ID.
pub const FIRST_COMMAND_ID: CommandId = 0;

/// Location index type for page offsets (matches PostgreSQL's `LocationIndex`).
///
/// Used for `pd_lower`, `pd_upper`, `pd_special` in page headers.
pub type LocationIndex = u16;

/// Offset number type for item IDs (matches PostgreSQL's `OffsetNumber`).
///
/// This is 1-based in PostgreSQL (0 is invalid).
pub type OffsetNumber = u16;

/// Invalid offset number.
pub const INVALID_OFFSET_NUMBER: OffsetNumber = 0;

/// First valid offset number.
pub const FIRST_OFFSET_NUMBER: OffsetNumber = 1;

/// Maximum offset number.
pub const MAX_OFFSET_NUMBER: OffsetNumber = (u16::MAX / 4) as OffsetNumber;

/// Block number type (matches PostgreSQL's `BlockNumber`).
///
/// Identifies a page within a relation.
pub type BlockNumber = u32;

/// Invalid block number.
pub const INVALID_BLOCK_NUMBER: BlockNumber = u32::MAX;

/// Object ID type (matches PostgreSQL's `Oid`).
pub type Oid = u32;

/// Invalid OID.
pub const INVALID_OID: Oid = 0;

/// Check if a transaction ID is normal (not special).
#[must_use]
pub const fn transaction_id_is_normal(xid: TransactionId) -> bool {
    xid >= FIRST_NORMAL_TRANSACTION_ID
}

/// Check if a transaction ID is valid.
#[must_use]
pub const fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid != INVALID_TRANSACTION_ID
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_id_checks() {
        assert!(!transaction_id_is_valid(INVALID_TRANSACTION_ID));
        assert!(transaction_id_is_valid(BOOTSTRAP_TRANSACTION_ID));
        assert!(transaction_id_is_valid(FROZEN_TRANSACTION_ID));
        assert!(transaction_id_is_valid(FIRST_NORMAL_TRANSACTION_ID));

        assert!(!transaction_id_is_normal(INVALID_TRANSACTION_ID));
        assert!(!transaction_id_is_normal(BOOTSTRAP_TRANSACTION_ID));
        assert!(!transaction_id_is_normal(FROZEN_TRANSACTION_ID));
        assert!(transaction_id_is_normal(FIRST_NORMAL_TRANSACTION_ID));
        assert!(transaction_id_is_normal(100));
    }
}
