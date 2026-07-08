-- 000011_relax_sharded_fks.up.sql
-- Drop the foreign keys on transfers to allow cross-shard recording.
-- A cross-shard transfer involves a remote wallet that doesn't exist on the local shard.
ALTER TABLE transfers DROP CONSTRAINT IF EXISTS transfers_from_wallet_fkey;
ALTER TABLE transfers DROP CONSTRAINT IF EXISTS transfers_to_wallet_fkey;
