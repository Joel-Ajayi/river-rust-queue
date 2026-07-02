package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Transfer struct {
	MerchantID string
	FromWallet string
	ToWallet   string
	Amount     int64
	Currency   string
	Reference  string
}

func (t Transfer) Validate() error {
	switch {
	case t.FromWallet == "":
		return ValidationError{Field: "from_wallet", Msg: "is required"}
	case t.ToWallet == "":
		return ValidationError{Field: "to_wallet", Msg: "is required"}
	case t.Amount <= 0:
		return ValidationError{Field: "amount", Msg: "must be greater than 0"}
	case t.Currency == "":
		return ValidationError{Field: "currency", Msg: "is required"}
	case t.FromWallet == t.ToWallet:
		return ValidationError{Field: "to_wallet", Msg: "must differ from from_wallet"}
	default:
		return nil
	}
}

func (t Transfer) Hash() string {
	payload := fmt.Sprintf("%s|%s|%s|%d|%s|%s", t.MerchantID, t.FromWallet, t.ToWallet, t.Amount, t.Currency, t.Reference)
	h := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(h[:])
}
