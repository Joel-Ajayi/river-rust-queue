-- wallet_balance_cache: a read projection of ledger_entries. NOT the source of truth;
-- never used by the posting txn or by reconciliation.
CREATE TABLE wallet_balance_cache (
    wallet_id      TEXT PRIMARY KEY REFERENCES wallets(id),
    balance        BIGINT NOT NULL,
    last_entry_id  BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
