// Package storage implements PostgreSQL storage engine internals:
// heap page layout, tuple format, buffer manager, B-tree pages, and
// MVCC visibility rules.
//
// Type definitions mirror PostgreSQL's internal types from c.h, transam.h,
// buf_internals.h, and related headers. The binary layouts are kept faithful
// to the PG source so page images can be compared against real Postgres files.
package storage

// TransactionId is a 32-bit unsigned integer that wraps around.
// Matches PostgreSQL's TransactionId (transam.h).
//
// Special values:
//   - 0 (InvalidTransactionId): invalid / unset
//   - 1 (BootstrapTransactionId): bootstrap transaction
//   - 2 (FrozenTransactionId): always visible
//   - 3+ : normal transaction IDs
type TransactionId = uint32

const (
	InvalidTransactionId     TransactionId = 0
	BootstrapTransactionId   TransactionId = 1
	FrozenTransactionId      TransactionId = 2
	FirstNormalTransactionId TransactionId = 3
)

// CommandId identifies commands within a transaction.
// Matches PostgreSQL's CommandId (c.h).
type CommandId = uint32

const (
	InvalidCommandId CommandId = 0xFFFFFFFF
	FirstCommandId   CommandId = 0
)

// LocationIndex is used for pd_lower, pd_upper, pd_special in page headers.
// Matches PostgreSQL's LocationIndex (bufpage.h).
type LocationIndex = uint16

// OffsetNumber is a 1-based index into the line pointer array on a page.
// 0 is InvalidOffsetNumber. Matches PostgreSQL's OffsetNumber (off.h).
type OffsetNumber = uint16

const (
	InvalidOffsetNumber OffsetNumber = 0
	FirstOffsetNumber   OffsetNumber = 1
	MaxOffsetNumber     OffsetNumber = (^uint16(0)) / 4
)

// BlockNumber identifies a page within a relation.
// Matches PostgreSQL's BlockNumber (block.h).
type BlockNumber = uint32

const InvalidBlockNumber BlockNumber = ^uint32(0)

// Oid is the object identifier type.
// Matches PostgreSQL's Oid (postgres_ext.h).
type Oid = uint32

const InvalidOid Oid = 0

// TransactionIdIsNormal reports whether xid is a normal (non-special) XID.
func TransactionIdIsNormal(xid TransactionId) bool {
	return xid >= FirstNormalTransactionId
}

// TransactionIdIsValid reports whether xid is valid (non-zero).
func TransactionIdIsValid(xid TransactionId) bool {
	return xid != InvalidTransactionId
}
