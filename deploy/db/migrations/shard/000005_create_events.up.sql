-- events: append-only fact log AND the transactional outbox. The relay publishes rows
-- whose publish_topic is set, then stamps published_at (the only allowed UPDATE).
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    correlation_id  TEXT,
    payload         JSONB NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    publish_topic   TEXT,
    published_at    TIMESTAMPTZ
);

CREATE INDEX events_aggregate_idx   ON events (aggregate_id, id);
CREATE INDEX events_correlation_idx ON events (correlation_id) WHERE correlation_id IS NOT NULL;
-- The outbox relay's hot query: unpublished rows, in order.
CREATE INDEX events_outbox_idx ON events (id) WHERE publish_topic IS NOT NULL AND published_at IS NULL;
