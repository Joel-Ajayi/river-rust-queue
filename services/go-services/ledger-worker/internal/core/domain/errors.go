package domain

import "errors"

var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrWalletFrozen        = errors.New("wallet is frozen")
	ErrWalletClosed        = errors.New("wallet is closed")
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrSelfTransfer        = errors.New("self transfer is not allowed")
	ErrInvalidJobType      = errors.New("unsupported job type")
	ErrMissingTransferData = errors.New("missing transfer data")
	ErrInvalidBody         = errors.New("invalid payload body")
	ErrMerchantInactive    = errors.New("merchant is not active")
	ErrServiceUnavailable  = errors.New("service unavailable")
	ErrUnmarshal           = errors.New("unmarshal error")
	ErrPanic               = errors.New("panic during processing")
	ErrCrossShardTerminal  = errors.New("cross-shard terminal error")
)

// IsTerminalError returns true if the error should fail the transfer immediately
// without retries (e.g., business logic errors vs connection errors).
func IsTerminalError(err error) bool {
	return errors.Is(err, ErrInsufficientBalance) ||
		errors.Is(err, ErrWalletFrozen) ||
		errors.Is(err, ErrWalletClosed) ||
		errors.Is(err, ErrWalletNotFound) ||
		errors.Is(err, ErrCurrencyMismatch) ||
		errors.Is(err, ErrSelfTransfer) ||
		errors.Is(err, ErrInvalidJobType) ||
		errors.Is(err, ErrMissingTransferData) ||
		errors.Is(err, ErrInvalidBody) ||
		errors.Is(err, ErrMerchantInactive)
}
