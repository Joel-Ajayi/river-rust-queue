-- wallet_balance_cache: a read projection of ledger_entries, maintained
-- in-STEP by the posting txn (PostTransfer, CreditFromClearingAccount,
-- ReverseCrossShardTransfer). Used for the authorization (insufficient-balance)
-- check and as a fast-path balance source. NOT the source of truth;
-- ledger_entries is the source of truth.
CREATE TABLE wallet_balance_cache (
    wallet_id      TEXT PRIMARY KEY REFERENCES wallets(id),
    balance        BIGINT NOT NULL,
    last_entry_id  BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
