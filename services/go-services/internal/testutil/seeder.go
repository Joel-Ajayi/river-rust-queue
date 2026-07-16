package testutil

import (
	"context"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"golang.org/x/crypto/bcrypt"
)

// SeedMerchantAndWallets inserts a fresh Merchant into the global merchants DB
// and provisions its wallets into the designated Shard DB.
// It returns the dynamically generated merchant ID for true hermetic isolation.
func SeedMerchantAndWallets(t *testing.T, merchantsDB, shardDB TestDB) string {
	t.Helper()
	
	// Seed a test merchant with a unique random ID for true hermetic isolation
	merchantID := platform.NewMerchantID()
	
	// Create a real bcrypt hash for "secret-123" so we can test AuthToken exchange
	hashBytes, err := bcrypt.GenerateFromPassword([]byte("secret-123"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}

	_, err = merchantsDB.Pool.Exec(context.Background(),
		`INSERT INTO merchants (id, name, tier, status, shard_id, api_key_hash) VALUES ($1, 'Test', 'starter', $2, 'shard-a', $3)`, merchantID, platform.MerchantStatusActive, string(hashBytes))
	if err != nil {
		t.Fatalf("failed to seed merchant: %v", err)
	}

	// Use deterministic UUIDs for test assertions
	walA := "01905335-9781-7000-8000-000000000001"
	walB := "01905335-9781-7000-8000-000000000002"
	foreignM := "m_01905335-9781-7000-8000-000000000003"
	foreignWal := "01905335-9781-7000-8000-000000000004"

	// Seed wallets for the merchant in the shard DB
	_, err = shardDB.Pool.Exec(context.Background(),
		`INSERT INTO wallets (id, merchant_id, currency) VALUES
		($1 || '.' || $2, $1, 'NGN'),
		($1 || '.' || $3, $1, 'NGN'),
		($4 || '.' || $5, $4, 'NGN')`, merchantID, walA, walB, foreignM, foreignWal)
	if err != nil {
		t.Fatalf("failed to seed wallets: %v", err)
	}

	return merchantID
}
