// Package domain holds the api-gateway's entities and value objects.
//
// It is the innermost ring of the onion: it imports nothing from our other
// packages (no platform, no pgx, no net/http). Business rules that must hold
// regardless of transport or storage live here.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Transfer is the value object describing a requested movement of funds.
// Amount is in minor units (e.g. cents) and must be strictly positive.
type Transfer struct {
	MerchantID string
	FromWallet string
	ToWallet   string
	Amount     int64
	Currency   string
	Reference  string
}

// Validate enforces the invariants a transfer must satisfy on any transport.
// It returns a ValidationError naming the offending field, never an HTTP status.
func (t Transfer) Validate() error {
	switch {
	case t.FromWallet == "":
		return ValidationError{Field: "from_wallet", Msg: "is required"}
	case t.ToWallet == "":
		return ValidationError{Field: "to_wallet", Msg: "is required"}
	case t.Amount <= 0:
		return ValidationError{Field: "amount", Msg: "must be positive"}
	case t.Currency == "":
		return ValidationError{Field: "currency", Msg: "is required"}
	case t.FromWallet == t.ToWallet:
		return ValidationError{Field: "to_wallet", Msg: "must differ from from_wallet"}
	}
	return nil
}

// Hash returns a stable fingerprint of the transfer's financial fields. Two
// requests under the same idempotency key must hash equal, or the second is a
// conflicting reuse of the key.
func (t Transfer) Hash() string {
	canonical := fmt.Sprintf("%s|%s|%s|%d|%s|%s",
		t.MerchantID, t.FromWallet, t.ToWallet, t.Amount, t.Currency, t.Reference)
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
