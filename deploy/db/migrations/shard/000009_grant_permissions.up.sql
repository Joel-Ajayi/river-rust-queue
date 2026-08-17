-- I6 immutability: events and ledger_entries are append-only. The app role may only
-- INSERT/SELECT; the relay role additionally updates events.published_at. Nobody UPDATEs
-- or DELETEs history. Guarded so dev (non-privileged owner) no-ops instead of failing.
DO $$
BEGIN
    BEGIN
        CREATE ROLE rrq_relay NOLOGIN;
    EXCEPTION
        WHEN duplicate_object THEN NULL;          -- already created
        WHEN insufficient_privilege THEN
            RAISE NOTICE 'skipping role grants (needs CREATEROLE; dev)';
            RETURN;
    END;

    -- App role: write-once on the immutable tables, nothing more.
    REVOKE UPDATE, DELETE ON events, ledger_entries FROM rrq_app;
    GRANT  INSERT, SELECT ON events, ledger_entries TO   rrq_app;

    -- Relay role: the one narrow exception — stamp published_at only.
    GRANT SELECT (id, event_id, event_type, payload, publish_topic, published_at) ON events TO rrq_relay;
    GRANT UPDATE (published_at) ON events TO rrq_relay;
END $$;
