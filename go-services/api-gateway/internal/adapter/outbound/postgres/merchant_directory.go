// Package postgres is a driven adapter: it implements the gateway's outbound
// ports (port.MerchantDirectory, port.JobStore) on top of pgx. It is the only
// place in the service that knows SQL exists.
package postgres

import (
	"context"

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

var _ port.MerchantDirectory = (*MerchantDirectory)(nil)

// NewMerchantDirectory builds the adapter over the shared connection pools.
func NewMerchantDirectory(pools *platform.ShardPools) *MerchantDirectory {
	return &MerchantDirectory{pools: pools}
}

// ShardFor returns the shard owning an active merchant.
func (d *MerchantDirectory) ShardFor(ctx context.Context, merchantID string) (string, error) {
	var shardID string
	err := d.pools.MerchantsPool().QueryRow(ctx,
		`SELECT shard_id FROM merchants WHERE id = $1 AND status = 'active'`, merchantID,
	).Scan(&shardID)
	if err == pgx.ErrNoRows {
		return "", domain.ErrMerchantInactive
	}
	return shardID, err
}

// AuthenticateAPIKey compares the presented key against stored bcrypt hashes.
func (d *MerchantDirectory) AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error) {
	rows, err := d.pools.MerchantsPool().Query(ctx,
		`SELECT id, tier, status, api_key_hash FROM merchants WHERE status != 'closed'`)
	if err != nil {
		return domain.Principal{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var id, tier, status, hash string
		if err := rows.Scan(&id, &tier, &status, &hash); err != nil {
			return domain.Principal{}, err
		}
		if bcrypt.CompareHashAndPassword([]byte(hash), []byte(apiKey)) == nil {
			if status != "active" {
				return domain.Principal{}, domain.ErrMerchantInactive
			}
			return domain.Principal{MerchantID: id, Tier: tier}, nil
		}
	}
	return domain.Principal{}, domain.ErrInvalidCredentials
}
