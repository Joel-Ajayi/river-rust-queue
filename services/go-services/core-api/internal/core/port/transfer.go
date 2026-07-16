package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
)

// -- Incoming ports --

// TransferSubmitter is the driving port for accepting a transfer request.
type TransferSubmitter interface {
	Submit(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error)
	GetBalance(ctx context.Context, walletID, merchantID string) (int64, string, error)
}
