-- Seed the single platform premium merchant.
INSERT INTO merchants (id, name, api_key_hash, tier, status, shard_id)
VALUES (
  'merchant_00000000-0000-0000-0000-000000000001',
  'Platform Premium Merchant',
  '$2b$04$EqX2GZ.vWk7aCVYMbVpRoug3rpu3Hi1Mtqdy5EPTI.vXPS36IaX1G',
  'premium',
  'active',
  '__platform__'
) ON CONFLICT (id) DO NOTHING;
