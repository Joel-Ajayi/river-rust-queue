-- merchants: the one GLOBAL table. Its shard_id column is the routing directory.
CREATE TABLE merchants (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    api_key_hash    TEXT NOT NULL UNIQUE,
    webhook_url     TEXT,
    webhook_secret  TEXT,
    tier            TEXT NOT NULL DEFAULT 'standard',
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'frozen', 'closed')),
    shard_id        TEXT NOT NULL,
    shard_state     TEXT NOT NULL DEFAULT 'active'
                    CHECK (shard_state IN ('active', 'migrating')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX merchants_status_idx ON merchants (status);
CREATE INDEX merchants_shard_idx  ON merchants (shard_id);
