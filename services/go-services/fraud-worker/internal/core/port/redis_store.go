package port

import (
	"context"
)

type RedisStore interface {
	UpdateVelocity(ctx context.Context, walletID string, eventID string, timestampMs int64, windowSeconds int) (int, error)
}
