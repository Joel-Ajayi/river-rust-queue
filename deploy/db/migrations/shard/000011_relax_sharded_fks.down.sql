-- 000011_relax_sharded_fks.down.sql
ALTER TABLE transfers ADD CONSTRAINT transfers_from_wallet_fkey FOREIGN KEY (from_wallet) REFERENCES wallets(id);
ALTER TABLE transfers ADD CONSTRAINT transfers_to_wallet_fkey FOREIGN KEY (to_wallet) REFERENCES wallets(id);
