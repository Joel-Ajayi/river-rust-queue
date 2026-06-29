-- cross_shard_transfer: the clearing-account saga state, on the SOURCE shard only.
-- Intra-shard transfers have no row here (one local txn, nothing to track).
CREATE TABLE cross_shard_transfer (
    transfer_id   TEXT PRIMARY KEY,        -- the tf_ id; identical on both shards
    job_id        TEXT NOT NULL,
    src_shard     TEXT NOT NULL,
    dst_shard     TEXT NOT NULL,
    from_wallet   TEXT NOT NULL,
    to_wallet     TEXT NOT NULL,
    amount        BIGINT NOT NULL CHECK (amount > 0),
    currency      TEXT NOT NULL,
    state         TEXT NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending', 'completed', 'reversed')),
    reason        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at    TIMESTAMPTZ
);

-- The stuck-transfer detector's hot query.
CREATE INDEX cross_shard_transfer_pending_idx
    ON cross_shard_transfer (created_at) WHERE state = 'pending';
