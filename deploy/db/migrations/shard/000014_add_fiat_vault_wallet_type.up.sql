-- Add system_fiat_vault wallet type and remove unused merchant_operational type.
ALTER TABLE wallets DROP CONSTRAINT IF EXISTS wallets_wallet_type_check;
ALTER TABLE wallets ADD CONSTRAINT wallets_wallet_type_check
  CHECK (wallet_type IN ('customer', 'system', 'system_fiat_vault'));
