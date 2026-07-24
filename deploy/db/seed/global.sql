-- Seed the single platform premium merchant.
INSERT INTO merchants (id, name, api_key_hash, tier, status, shard_id)
VALUES (
  'merchant_00000000-0000-0000-0000-000000000001',
  'Platform Premium Merchant',
  '$argon2id$v=19$m=65536,t=1,p=4$DkQfRDtWT3sIErR0nvzk/g$/4en1aTRWdG65p5NA5+9zlqY87jl1FezreZoqC26Gmo',
  'premium',
  'active',
  '__platform__'
) ON CONFLICT (id) DO NOTHING;
