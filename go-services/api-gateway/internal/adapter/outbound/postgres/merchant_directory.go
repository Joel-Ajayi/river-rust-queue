package postgres

import (
	"context"

	"strings"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

// MerchantDirectory reads the global merchants database.
type MerchantDirectory struct {
	pools *platform.ShardPools
}

// compile time interface implementation check
var _ port.MerchantDirectory = (*MerchantDirectory)(nil)

// NewMerchantDirectory builds the adapter over the shared connection pools.
func NewMerchantDirectory(pools *platform.ShardPools) *MerchantDirectory {
	return &MerchantDirectory{pools}
}

// ShardFor returns the shard owning an active merchant.
func (md *MerchantDirectory) ShardFor(ctx context.Context, merchantID string) (string, error) {
	var shardID string
	err := md.pools.MerchantsPool().QueryRow(
		ctx,
		`SELECT shard_id FROM merchants WHERE id = $1 AND status = $2`,
		merchantID, platform.MerchantStatusActive,
	).Scan(&shardID)
	if err != nil || err == pgx.ErrNoRows {
		return "", domain.ErrMerchantInactive
	}

	return shardID, err
}

func (md *MerchantDirectory) AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error) {
	// A production API key format must be compound("merchant_id:secret_part")
	parts := strings.SplitN(apiKey, ":", 2)
	if len(parts) != 2 {
		return domain.Principal{}, domain.ErrInvalidAPIKey
	}
	merchantID := parts[0]
	secretPart := parts[1]

	var tier, status, hash string
	err := md.pools.MerchantsPool().QueryRow(ctx,
		`SELECT tier, status, api_key_hash FROM merchants WHERE id = $1 AND status != $2`,
		merchantID, platform.MerchantStatusClosed,
	).Scan(&tier, &status, &hash)

	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.Principal{}, domain.ErrInvalidCredentials
		}
		return domain.Principal{}, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(secretPart)); err != nil {
		return domain.Principal{}, domain.ErrInvalidCredentials
	}

	if status != platform.MerchantStatusActive {
		return domain.Principal{}, domain.ErrMerchantInactive
	}

	return domain.Principal{MerchantID: merchantID, Status: status, Tier: tier}, nil
}
