-- Seed system clearing and fiat vault wallets for this shard.
INSERT INTO wallets (id, merchant_id, currency, wallet_type, status)
VALUES (
  'merchant_00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000000',
  'merchant_00000000-0000-0000-0000-000000000001',
  'NGN',
  'system',
  'active'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO wallets (id, merchant_id, currency, wallet_type, status)
VALUES (
  'merchant_00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000001',
  'merchant_00000000-0000-0000-0000-000000000001',
  'NGN',
  'system_fiat_vault',
  'active'
) ON CONFLICT (id) DO NOTHING;

-- Seed initial balances of 0 for system wallets in balance cache.
INSERT INTO wallet_balance_cache (wallet_id, balance, last_entry_id, updated_at)
VALUES (
  'merchant_00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000000',
  0,
  0,
  NOW()
) ON CONFLICT (wallet_id) DO NOTHING;

INSERT INTO wallet_balance_cache (wallet_id, balance, last_entry_id, updated_at)
VALUES (
  'merchant_00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000001',
  0,
  0,
  NOW()
) ON CONFLICT (wallet_id) DO NOTHING;
