-- Revert: restore original wallet_type CHECK constraint.
ALTER TABLE wallets DROP CONSTRAINT IF EXISTS wallets_wallet_type_check;
ALTER TABLE wallets ADD CONSTRAINT wallets_wallet_type_check
  CHECK (wallet_type IN ('merchant_operational', 'customer', 'system'));
