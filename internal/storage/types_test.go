package storage

import "testing"

func TestTransactionIdChecks(t *testing.T) {
	cases := []struct {
		xid     TransactionId
		valid   bool
		normal  bool
	}{
		{InvalidTransactionId, false, false},
		{BootstrapTransactionId, true, false},
		{FrozenTransactionId, true, false},
		{FirstNormalTransactionId, true, true},
		{100, true, true},
	}
	for _, c := range cases {
		if got := TransactionIdIsValid(c.xid); got != c.valid {
			t.Errorf("IsValid(%d) = %v, want %v", c.xid, got, c.valid)
		}
		if got := TransactionIdIsNormal(c.xid); got != c.normal {
			t.Errorf("IsNormal(%d) = %v, want %v", c.xid, got, c.normal)
		}
	}
}
