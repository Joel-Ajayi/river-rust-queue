package testutil

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// SeedMerchantAndWallets inserts a fresh Merchant into the global merchants DB
// and provisions its wallets into the designated Shard DB.
// It returns the dynamically generated merchant ID for true hermetic isolation.
func SeedMerchantAndWallets(t *testing.T, merchantsDB, shardDB TestDB) string {
	t.Helper()
	
	// Seed a test merchant with a unique random ID for true hermetic isolation
	merchantID := "m_" + uuid.NewString()[:8]
	
	// Create a real bcrypt hash for "secret-123" so we can test AuthToken exchange
	hashBytes, err := bcrypt.GenerateFromPassword([]byte("secret-123"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}

	_, err = merchantsDB.Pool.Exec(context.Background(),
		`INSERT INTO merchants (id, name, tier, status, shard_id, api_key_hash) VALUES ($1, 'Test', 'starter', 'active', 'shard-a', $2)`, merchantID, string(hashBytes))
	if err != nil {
		t.Fatalf("failed to seed merchant: %v", err)
	}

	// Seed wallets for the merchant in the shard DB
	_, err = shardDB.Pool.Exec(context.Background(),
		`INSERT INTO wallets (id, merchant_id, currency) VALUES
		($1 || '.wal_A', $1, 'NGN'),
		($1 || '.wal_B', $1, 'NGN'),
		('m_999.wal_foreign', 'm_999', 'NGN')`, merchantID)
	if err != nil {
		t.Fatalf("failed to seed wallets: %v", err)
	}

	return merchantID
}
