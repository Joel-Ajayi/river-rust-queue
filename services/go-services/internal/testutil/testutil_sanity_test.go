//go:build integration

package testutil

import (
	"context"
	"testing"
)

func TestContainerDB_SetupAndSeed(t *testing.T) {
	cluster := SetupTestDB(t)

	// Verify merchant seeded from deploy/db/seed/global.sql
	var merchantCount int
	err := cluster.MerchantsDB.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM merchants WHERE id = 'merchant_00000000-0000-0000-0000-000000000001'`).Scan(&merchantCount)
	if err != nil {
		t.Fatalf("failed to query seeded platform merchant: %v", err)
	}
	if merchantCount != 1 {
		t.Fatalf("expected platform merchant from deploy/db/seed/global.sql, found %d", merchantCount)
	}

	// Verify system wallets seeded from deploy/db/seed/shard.sql
	var walletCount int
	err = cluster.ShardA.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wallets WHERE merchant_id = 'merchant_00000000-0000-0000-0000-000000000001'`).Scan(&walletCount)
	if err != nil {
		t.Fatalf("failed to query seeded system wallets on shard-a: %v", err)
	}
	if walletCount < 2 {
		t.Fatalf("expected system wallets from deploy/db/seed/shard.sql, found %d", walletCount)
	}
}
