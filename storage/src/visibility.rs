//! Tuple visibility rules for MVCC.
//!
//! This module implements PostgreSQL's tuple visibility logic as defined in
//! `src/backend/access/heap/heapam_visibility.c`. The visibility rules determine
//! whether a tuple is visible to a given transaction based on:
//!
//! - The tuple's `xmin` (inserting transaction)
//! - The tuple's `xmax` (deleting transaction, if any)
//! - The current transaction's snapshot
//! - Hint bits in the tuple header
//!
//! # MVCC Basics
//!
//! PostgreSQL uses Multi-Version Concurrency Control (MVCC) to provide
//! transaction isolation without read locks. Each tuple has:
//!
//! - `xmin`: The transaction ID that inserted this tuple version
//! - `xmax`: The transaction ID that deleted/updated this tuple (0 if not deleted)
//!
//! A tuple is visible if:
//! 1. `xmin` is committed and visible to the snapshot
//! 2. `xmax` is either invalid, aborted, or not visible to the snapshot
//!
//! # Hint Bits
//!
//! To avoid repeated transaction status lookups, PostgreSQL sets "hint bits"
//! on tuples after determining their visibility status:
//!
//! - `HEAP_XMIN_COMMITTED`: xmin is known to be committed
//! - `HEAP_XMIN_INVALID`: xmin is known to be aborted
//! - `HEAP_XMAX_COMMITTED`: xmax is known to be committed
//! - `HEAP_XMAX_INVALID`: xmax is known to be invalid/aborted

use crate::tuple::{HeapTupleHeader, InfomaskFlags};
use crate::types::{
    CommandId, TransactionId, FIRST_NORMAL_TRANSACTION_ID, FROZEN_TRANSACTION_ID,
    INVALID_TRANSACTION_ID,
};

/// Transaction status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Transaction is still in progress.
    InProgress,
    /// Transaction has committed.
    Committed,
    /// Transaction has aborted.
    Aborted,
}

/// A snapshot represents a consistent view of the database at a point in time.
///
/// Matches PostgreSQL's `SnapshotData` from `snapshot.h`.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The transaction ID of the current transaction.
    pub xid: TransactionId,
    /// All transaction IDs >= this are invisible.
    pub xmax: TransactionId,
    /// All transaction IDs < this are visible (if committed).
    pub xmin: TransactionId,
    /// Transaction IDs of in-progress transactions (between xmin and xmax).
    pub xip: Vec<TransactionId>,
    /// Current command ID within the transaction.
    pub curcid: CommandId,
}

impl Snapshot {
    /// Create a new snapshot.
    #[must_use]
    pub fn new(xid: TransactionId, xmin: TransactionId, xmax: TransactionId) -> Self {
        Self {
            xid,
            xmax,
            xmin,
            xip: Vec::new(),
            curcid: 0,
        }
    }

    /// Create a snapshot that sees all committed transactions.
    ///
    /// This is useful for system operations that need to see everything.
    #[must_use]
    pub fn any() -> Self {
        Self {
            xid: FIRST_NORMAL_TRANSACTION_ID,
            xmax: TransactionId::MAX,
            xmin: FIRST_NORMAL_TRANSACTION_ID,
            xip: Vec::new(),
            curcid: 0,
        }
    }

    /// Check if a transaction ID is visible in this snapshot.
    ///
    /// A transaction is visible if:
    /// - It's less than xmin (definitely committed before snapshot)
    /// - It's between xmin and xmax and not in the xip list
    /// - It's the current transaction
    #[must_use]
    pub fn is_visible(&self, xid: TransactionId) -> bool {
        // Special transaction IDs
        if xid == INVALID_TRANSACTION_ID {
            return false;
        }
        if xid == FROZEN_TRANSACTION_ID {
            return true;
        }

        // Current transaction's own changes are visible
        if xid == self.xid {
            return true;
        }

        // Transaction IDs >= xmax are not visible
        if xid >= self.xmax {
            return false;
        }

        // Transaction IDs < xmin are visible (if committed)
        if xid < self.xmin {
            return true;
        }

        // Check if in the in-progress list
        !self.xip.contains(&xid)
    }

    /// Add an in-progress transaction to the snapshot.
    pub fn add_in_progress(&mut self, xid: TransactionId) {
        if !self.xip.contains(&xid) {
            self.xip.push(xid);
        }
    }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::any()
    }
}

/// Result of a visibility check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityResult {
    /// Tuple is visible to the snapshot.
    Visible,
    /// Tuple is not visible (inserted by uncommitted/aborted transaction).
    Invisible,
    /// Tuple is deleted and not visible.
    Deleted,
    /// Tuple is being deleted by current transaction.
    BeingDeleted,
    /// Tuple was inserted by current transaction.
    InsertedBySelf,
    /// Tuple was deleted by current transaction.
    DeletedBySelf,
}

impl VisibilityResult {
    /// Check if the tuple should be returned to the user.
    #[must_use]
    pub fn is_visible(self) -> bool {
        matches!(self, Self::Visible | Self::InsertedBySelf)
    }
}

/// Transaction status oracle - provides transaction commit status.
///
/// In a real implementation, this would consult pg_xact (clog).
/// For now, we use a simple in-memory map.
pub trait TransactionOracle {
    /// Get the status of a transaction.
    fn get_status(&self, xid: TransactionId) -> TransactionStatus;

    /// Check if a transaction is committed.
    fn is_committed(&self, xid: TransactionId) -> bool {
        self.get_status(xid) == TransactionStatus::Committed
    }

    /// Check if a transaction is aborted.
    fn is_aborted(&self, xid: TransactionId) -> bool {
        self.get_status(xid) == TransactionStatus::Aborted
    }

    /// Check if a transaction is in progress.
    fn is_in_progress(&self, xid: TransactionId) -> bool {
        self.get_status(xid) == TransactionStatus::InProgress
    }
}

/// Simple in-memory transaction oracle for testing.
#[derive(Debug, Default)]
pub struct SimpleTransactionOracle {
    /// Map of transaction ID to status.
    status: std::collections::HashMap<TransactionId, TransactionStatus>,
}

impl SimpleTransactionOracle {
    /// Create a new oracle.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the status of a transaction.
    pub fn set_status(&mut self, xid: TransactionId, status: TransactionStatus) {
        self.status.insert(xid, status);
    }

    /// Mark a transaction as committed.
    pub fn commit(&mut self, xid: TransactionId) {
        self.set_status(xid, TransactionStatus::Committed);
    }

    /// Mark a transaction as aborted.
    pub fn abort(&mut self, xid: TransactionId) {
        self.set_status(xid, TransactionStatus::Aborted);
    }
}

impl TransactionOracle for SimpleTransactionOracle {
    fn get_status(&self, xid: TransactionId) -> TransactionStatus {
        // Special cases
        if xid == INVALID_TRANSACTION_ID {
            return TransactionStatus::Aborted;
        }
        if xid == FROZEN_TRANSACTION_ID {
            return TransactionStatus::Committed;
        }

        self.status
            .get(&xid)
            .copied()
            .unwrap_or(TransactionStatus::InProgress)
    }
}

/// Check if a heap tuple is visible to a snapshot.
///
/// This is the main visibility check function, implementing the logic from
/// `HeapTupleSatisfiesMVCC` in PostgreSQL.
///
/// # Arguments
///
/// * `header` - The tuple header to check
/// * `snapshot` - The snapshot to check visibility against
/// * `oracle` - Transaction status oracle
///
/// # Returns
///
/// The visibility result for the tuple.
pub fn heap_tuple_satisfies_mvcc<O: TransactionOracle>(
    header: &HeapTupleHeader,
    snapshot: &Snapshot,
    oracle: &O,
) -> VisibilityResult {
    let infomask = header.infomask();

    // Check xmin (inserting transaction)
    if !check_xmin_visible(header, snapshot, oracle, infomask) {
        return VisibilityResult::Invisible;
    }

    // Check if inserted by current transaction
    if header.t_xmin == snapshot.xid {
        // Check command ID for visibility within same transaction
        if header.t_cid >= snapshot.curcid {
            return VisibilityResult::Invisible;
        }

        // Check xmax
        if header.t_xmax == INVALID_TRANSACTION_ID || infomask.contains(InfomaskFlags::HEAP_XMAX_INVALID) {
            return VisibilityResult::InsertedBySelf;
        }

        if header.t_xmax == snapshot.xid {
            return VisibilityResult::DeletedBySelf;
        }
    }

    // Check xmax (deleting transaction)
    check_xmax_visibility(header, snapshot, oracle, infomask)
}

/// Check if xmin makes the tuple visible.
fn check_xmin_visible<O: TransactionOracle>(
    header: &HeapTupleHeader,
    snapshot: &Snapshot,
    oracle: &O,
    infomask: InfomaskFlags,
) -> bool {
    let xmin = header.t_xmin;

    // Invalid xmin means tuple was never properly inserted
    if xmin == INVALID_TRANSACTION_ID {
        return false;
    }

    // Frozen tuples are always visible
    if xmin == FROZEN_TRANSACTION_ID {
        return true;
    }

    // Check hint bits first
    if infomask.contains(InfomaskFlags::HEAP_XMIN_COMMITTED) {
        // xmin is committed, check if visible to snapshot
        return snapshot.is_visible(xmin);
    }

    if infomask.contains(InfomaskFlags::HEAP_XMIN_INVALID) {
        // xmin is aborted
        return false;
    }

    // No hint bits, need to check transaction status
    match oracle.get_status(xmin) {
        TransactionStatus::Committed => snapshot.is_visible(xmin),
        TransactionStatus::Aborted => false,
        TransactionStatus::InProgress => {
            // In-progress transaction - only visible if it's our own
            xmin == snapshot.xid
        }
    }
}

/// Check xmax visibility and return final result.
fn check_xmax_visibility<O: TransactionOracle>(
    header: &HeapTupleHeader,
    snapshot: &Snapshot,
    oracle: &O,
    infomask: InfomaskFlags,
) -> VisibilityResult {
    let xmax = header.t_xmax;

    // No deleting transaction
    if xmax == INVALID_TRANSACTION_ID || infomask.contains(InfomaskFlags::HEAP_XMAX_INVALID) {
        return VisibilityResult::Visible;
    }

    // Check hint bits
    if infomask.contains(InfomaskFlags::HEAP_XMAX_COMMITTED) {
        // xmax is committed - tuple is deleted
        if snapshot.is_visible(xmax) {
            return VisibilityResult::Deleted;
        }
        // Deletion not visible to this snapshot
        return VisibilityResult::Visible;
    }

    // Check if being deleted by current transaction
    if xmax == snapshot.xid {
        return VisibilityResult::BeingDeleted;
    }

    // Check transaction status
    match oracle.get_status(xmax) {
        TransactionStatus::Committed => {
            if snapshot.is_visible(xmax) {
                VisibilityResult::Deleted
            } else {
                VisibilityResult::Visible
            }
        }
        TransactionStatus::Aborted => {
            // Deletion was aborted, tuple is visible
            VisibilityResult::Visible
        }
        TransactionStatus::InProgress => {
            // Deletion in progress by another transaction
            VisibilityResult::Visible
        }
    }
}

/// Set hint bits on a tuple header based on visibility check results.
///
/// This should be called after a visibility check to avoid repeated lookups.
pub fn set_hint_bits<O: TransactionOracle>(header: &mut HeapTupleHeader, oracle: &O) {
    let mut infomask = header.infomask();

    // Set xmin hints
    if !infomask.contains(InfomaskFlags::HEAP_XMIN_COMMITTED)
        && !infomask.contains(InfomaskFlags::HEAP_XMIN_INVALID)
    {
        match oracle.get_status(header.t_xmin) {
            TransactionStatus::Committed => {
                infomask.insert(InfomaskFlags::HEAP_XMIN_COMMITTED);
            }
            TransactionStatus::Aborted => {
                infomask.insert(InfomaskFlags::HEAP_XMIN_INVALID);
            }
            TransactionStatus::InProgress => {}
        }
    }

    // Set xmax hints
    if header.t_xmax != INVALID_TRANSACTION_ID
        && !infomask.contains(InfomaskFlags::HEAP_XMAX_COMMITTED)
        && !infomask.contains(InfomaskFlags::HEAP_XMAX_INVALID)
    {
        match oracle.get_status(header.t_xmax) {
            TransactionStatus::Committed => {
                infomask.insert(InfomaskFlags::HEAP_XMAX_COMMITTED);
            }
            TransactionStatus::Aborted => {
                infomask.insert(InfomaskFlags::HEAP_XMAX_INVALID);
            }
            TransactionStatus::InProgress => {}
        }
    }

    header.set_infomask(infomask);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tuple(xmin: TransactionId, xmax: TransactionId) -> HeapTupleHeader {
        let mut header = HeapTupleHeader::new(xmin, 0);
        header.t_xmax = xmax;
        // If xmax is set, clear the HEAP_XMAX_INVALID flag
        if xmax != INVALID_TRANSACTION_ID {
            let mut flags = header.infomask();
            flags.remove(InfomaskFlags::HEAP_XMAX_INVALID);
            header.set_infomask(flags);
        }
        header
    }

    #[test]
    fn snapshot_visibility_basic() {
        let snapshot = Snapshot::new(100, 50, 150);

        // Frozen is always visible
        assert!(snapshot.is_visible(FROZEN_TRANSACTION_ID));

        // Invalid is never visible
        assert!(!snapshot.is_visible(INVALID_TRANSACTION_ID));

        // Current transaction is visible
        assert!(snapshot.is_visible(100));

        // Old committed transactions are visible
        assert!(snapshot.is_visible(40));

        // Future transactions are not visible
        assert!(!snapshot.is_visible(200));
    }

    #[test]
    fn snapshot_with_in_progress() {
        let mut snapshot = Snapshot::new(100, 50, 150);
        snapshot.add_in_progress(75);
        snapshot.add_in_progress(80);

        // In-progress transactions are not visible
        assert!(!snapshot.is_visible(75));
        assert!(!snapshot.is_visible(80));

        // Other transactions in range are visible
        assert!(snapshot.is_visible(70));
        assert!(snapshot.is_visible(90));
    }

    #[test]
    fn simple_oracle() {
        let mut oracle = SimpleTransactionOracle::new();

        // Default is in-progress
        assert!(oracle.is_in_progress(100));

        oracle.commit(100);
        assert!(oracle.is_committed(100));
        assert!(!oracle.is_in_progress(100));

        oracle.abort(200);
        assert!(oracle.is_aborted(200));
    }

    #[test]
    fn visible_committed_tuple() {
        let mut oracle = SimpleTransactionOracle::new();
        oracle.commit(50);

        let header = make_tuple(50, INVALID_TRANSACTION_ID);
        let snapshot = Snapshot::new(100, 40, 150);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Visible);
    }

    #[test]
    fn invisible_uncommitted_tuple() {
        let oracle = SimpleTransactionOracle::new();

        let header = make_tuple(50, INVALID_TRANSACTION_ID);
        let snapshot = Snapshot::new(100, 40, 150);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Invisible);
    }

    #[test]
    fn invisible_aborted_tuple() {
        let mut oracle = SimpleTransactionOracle::new();
        oracle.abort(50);

        let header = make_tuple(50, INVALID_TRANSACTION_ID);
        let snapshot = Snapshot::new(100, 40, 150);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Invisible);
    }

    #[test]
    fn deleted_tuple() {
        let mut oracle = SimpleTransactionOracle::new();
        oracle.commit(50);
        oracle.commit(60);

        let header = make_tuple(50, 60);
        let snapshot = Snapshot::new(100, 40, 150);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Deleted);
    }

    #[test]
    fn tuple_with_uncommitted_delete() {
        let mut oracle = SimpleTransactionOracle::new();
        oracle.commit(50);
        // xmax (60) is still in progress

        let header = make_tuple(50, 60);
        let snapshot = Snapshot::new(100, 40, 150);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Visible);
    }

    #[test]
    fn tuple_with_aborted_delete() {
        let mut oracle = SimpleTransactionOracle::new();
        oracle.commit(50);
        oracle.abort(60);

        let header = make_tuple(50, 60);
        let snapshot = Snapshot::new(100, 40, 150);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Visible);
    }

    #[test]
    fn own_insert_visible() {
        let oracle = SimpleTransactionOracle::new();
        // Transaction 100 is current, so in-progress

        let mut header = make_tuple(100, INVALID_TRANSACTION_ID);
        header.t_cid = 0;

        let mut snapshot = Snapshot::new(100, 40, 150);
        snapshot.curcid = 5;

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::InsertedBySelf);
    }

    #[test]
    fn own_insert_not_visible_same_command() {
        let oracle = SimpleTransactionOracle::new();

        let mut header = make_tuple(100, INVALID_TRANSACTION_ID);
        header.t_cid = 5;

        let mut snapshot = Snapshot::new(100, 40, 150);
        snapshot.curcid = 5; // Same command ID

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Invisible);
    }

    #[test]
    fn own_delete() {
        let mut oracle = SimpleTransactionOracle::new();
        oracle.commit(50);

        let mut header = make_tuple(50, 100);
        header.t_cid = 0;

        let snapshot = Snapshot::new(100, 40, 150);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::BeingDeleted);
    }

    #[test]
    fn hint_bits_set() {
        let mut oracle = SimpleTransactionOracle::new();
        oracle.commit(50);
        oracle.abort(60);

        let mut header = make_tuple(50, 60);

        // Initially no hint bits
        assert!(!header.xmin_committed());
        assert!(!header.xmax_invalid());

        set_hint_bits(&mut header, &oracle);

        // Now hint bits should be set
        assert!(header.xmin_committed());
        assert!(header.xmax_invalid());
    }

    #[test]
    fn frozen_tuple_always_visible() {
        let oracle = SimpleTransactionOracle::new();

        let header = make_tuple(FROZEN_TRANSACTION_ID, INVALID_TRANSACTION_ID);
        let snapshot = Snapshot::new(100, 40, 150);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Visible);
    }

    #[test]
    fn delete_not_visible_to_old_snapshot() {
        let mut oracle = SimpleTransactionOracle::new();
        oracle.commit(50);
        oracle.commit(100); // Delete committed

        let header = make_tuple(50, 100);

        // Snapshot that can't see transaction 100
        let snapshot = Snapshot::new(80, 40, 90);

        let result = heap_tuple_satisfies_mvcc(&header, &snapshot, &oracle);
        assert_eq!(result, VisibilityResult::Visible);
    }
}
