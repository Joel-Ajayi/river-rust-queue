package domain

import "errors"

var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrWalletFrozen        = errors.New("wallet is frozen")
	ErrWalletClosed        = errors.New("wallet is closed")
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrSelfTransfer        = errors.New("self transfer is not allowed")
)

// IsTerminal returns true if the error should fail the transfer immediately
// without retries (e.g., business logic errors vs connection errors).
func IsTerminal(err error) bool {
	return errors.Is(err, ErrInsufficientBalance) ||
		errors.Is(err, ErrWalletFrozen) ||
		errors.Is(err, ErrWalletClosed) ||
		errors.Is(err, ErrWalletNotFound) ||
		errors.Is(err, ErrCurrencyMismatch) ||
		errors.Is(err, ErrSelfTransfer)
}
