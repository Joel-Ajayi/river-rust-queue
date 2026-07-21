package postgres

import (
	"context"
	"errors"

	"strings"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/jackc/pgx/v5"
)

// MerchantDirectory reads the global merchants database.
type MerchantDirectory struct {
	pools *platform.ShardPools
}

// compile time interface implementation checks.
var (
	_ port.MerchantDirectory = (*MerchantDirectory)(nil)
	_ port.MerchantStore     = (*MerchantDirectory)(nil)
)

// NewMerchantDirectory builds the adapter over the shared connection pools.
func NewMerchantDirectory(pools *platform.ShardPools) *MerchantDirectory {
	return &MerchantDirectory{pools}
}

// Create inserts a new merchant into the global merchants table.
func (md *MerchantDirectory) Create(ctx context.Context, id, name, apiKeyHash, tier, status, shardID, webhookURL, webhookSecret string) error {
	var urlVal, secretVal *string
	if webhookURL != "" {
		urlVal = &webhookURL
	}
	if webhookSecret != "" {
		secretVal = &webhookSecret
	}
	_, err := md.pools.MerchantsPool().Exec(ctx, `
		INSERT INTO merchants (id, name, api_key_hash, webhook_url, webhook_secret, tier, status, shard_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, id, name, apiKeyHash, urlVal, secretVal, tier, status, shardID)
	return err
}

// ShardFor returns the shard owning an active merchant.
func (md *MerchantDirectory) ShardFor(ctx context.Context, merchantID string) (string, error) {
	var shardID string
	err := md.pools.MerchantsPoolRO().QueryRow(
		ctx,
		`SELECT shard_id FROM merchants WHERE id = $1 AND status = $2`,
		merchantID, platform.MerchantStatusActive,
	).Scan(&shardID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", domain.ErrMerchantInactive
		}
		return "", err
	}

	return shardID, nil
}

func (md *MerchantDirectory) AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error) {
	if !strings.HasPrefix(apiKey, platform.APIKeyPrefix) {
		return domain.Principal{}, domain.ErrInvalidAPIKey
	}

	rawKey := strings.TrimPrefix(apiKey, platform.APIKeyPrefix)
	parts := strings.SplitN(rawKey, platform.APIKeySeparator, 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return domain.Principal{}, domain.ErrInvalidAPIKey
	}
	merchantID := parts[0]
	secretPart := parts[1]

	var tier, status, hash string
	err := md.pools.MerchantsPoolRO().QueryRow(ctx,
		`SELECT tier, status, api_key_hash FROM merchants WHERE id = $1 AND status != $2`,
		merchantID, platform.MerchantStatusClosed,
	).Scan(&tier, &status, &hash)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Principal{}, domain.ErrInvalidCredentials
		}
		return domain.Principal{}, err
	}

	if !platform.CompareAPIKeySecret(hash, secretPart) {
		return domain.Principal{}, domain.ErrInvalidCredentials
	}

	if status != platform.MerchantStatusActive {
		return domain.Principal{}, domain.ErrMerchantInactive
	}

	return domain.Principal{MerchantID: merchantID, Status: status, Tier: tier}, nil
}
