-- ledger_entries: the financial source of truth. UNIQUE(transfer_id, leg) = redelivery
-- guard + conservation check (I1); id is the per-wallet ordering key (I4).
CREATE TABLE ledger_entries (
    id             BIGSERIAL PRIMARY KEY,
    wallet_id      TEXT NOT NULL REFERENCES wallets(id),
    transfer_id    TEXT NOT NULL REFERENCES transfers(id),
    leg            TEXT NOT NULL CHECK (leg IN ('debit', 'credit')),
    amount         BIGINT NOT NULL,
    balance_after  BIGINT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (transfer_id, leg)
);

CREATE INDEX ledger_entries_wallet_idx ON ledger_entries (wallet_id, id);
