package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Transfer struct {
	MerchantID   string
	ToMerchantID string
	FromWallet   string
	ToWallet     string
	Amount       int64
	Currency     string
	Reference    string
}

type CircuitBreakerWalletBalanceRes struct {
	Bal int64
	Cur string
}

func (t Transfer) Validate() error {
	switch {
	case t.FromWallet == "":
		return ErrInvalidFromWallet
	case t.ToWallet == "":
		return ErrInvalidToWallet
	case t.Amount <= 0:
		return ErrInvalidAmount
	case t.Currency == "":
		return ErrInvalidCurrency
	case t.FromWallet == t.ToWallet:
		return ErrSameWallet
	default:
		return nil
	}
}

func (t Transfer) Hash() string {
	payload := fmt.Sprintf("%s|%s|%s|%s|%d|%s|%s", t.MerchantID, t.ToMerchantID, t.FromWallet, t.ToWallet, t.Amount, t.Currency, t.Reference)
	h := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(h[:])
}
