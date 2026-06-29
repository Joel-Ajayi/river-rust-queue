-- transfers: one atomic money movement, created in the same txn as its ledger legs.
CREATE TABLE transfers (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL REFERENCES jobs(id),
    from_wallet     TEXT NOT NULL REFERENCES wallets(id),
    to_wallet       TEXT NOT NULL REFERENCES wallets(id),
    amount          BIGINT NOT NULL CHECK (amount > 0),
    currency        TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('completed', 'failed')),
    failure_reason  TEXT,
    posted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX transfers_job_idx  ON transfers (job_id);
CREATE INDEX transfers_from_idx ON transfers (from_wallet, posted_at);
CREATE INDEX transfers_to_idx   ON transfers (to_wallet, posted_at);
