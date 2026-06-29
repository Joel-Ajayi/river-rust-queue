-- wallets: per-shard. merchant_id is a plain TEXT (merchants lives in the global DB,
-- so no cross-database FK); the gateway validates ownership for tenant isolation (I9).
CREATE TABLE wallets (
    id             TEXT PRIMARY KEY,
    merchant_id    TEXT NOT NULL,
    currency       TEXT NOT NULL,
    wallet_type    TEXT NOT NULL DEFAULT 'merchant_operational'
                   CHECK (wallet_type IN ('merchant_operational', 'customer', 'system')),
    external_ref   TEXT,
    status         TEXT NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'frozen', 'closed')),
    frozen_reason  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX wallets_merchant_idx ON wallets (merchant_id);
CREATE INDEX wallets_status_idx   ON wallets (status);
CREATE INDEX wallets_external_ref_idx ON wallets (merchant_id, external_ref)
    WHERE external_ref IS NOT NULL;
