-- Best-effort revert; ignores missing role in dev.
DO $$
BEGIN
    GRANT UPDATE, DELETE ON events, ledger_entries TO rrq_app;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
