-- PG role separation runbook (PLAN-13 D13).
--
-- Restricts read + write access to the credential and API-token
-- tables to a dedicated `rektifier_app` role; the analytics /
-- operator role `rektifier_ro` can read user-data tables but is
-- explicitly REVOKE'd from the auth tables.
--
-- Run this once after `ensure_metadata_tables` + the rekt-auth
-- bootstrap have created the tables. Idempotent (CREATE ROLE IF
-- NOT EXISTS is not PG-standard, so the role creation is guarded by
-- a DO block).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rektifier_app') THEN
        CREATE ROLE rektifier_app NOINHERIT LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rektifier_ro') THEN
        CREATE ROLE rektifier_ro NOINHERIT LOGIN;
    END IF;
END;
$$;

-- The rektifier server connects as `rektifier_app`.
GRANT CONNECT ON DATABASE rektifier TO rektifier_app;

-- App role gets full DML on every rektifier-managed table.
GRANT SELECT, INSERT, UPDATE, DELETE
   ON _rektifier_aws_credentials,
      _rektifier_api_tokens,
      _rektifier_tables,
      _rektifier_gsi_state
   TO rektifier_app;

-- Analytics / ops role: SELECT on user-data + catalog tables ONLY.
-- The credential and API-token tables are explicitly REVOKED.
GRANT SELECT ON _rektifier_tables TO rektifier_ro;
REVOKE ALL ON _rektifier_aws_credentials, _rektifier_api_tokens FROM rektifier_ro;
REVOKE ALL ON _rektifier_aws_credentials, _rektifier_api_tokens FROM PUBLIC;

-- Belt-and-suspenders: row-level security policies that only the app
-- role passes.
ALTER TABLE _rektifier_aws_credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE _rektifier_api_tokens     ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS rektifier_app_only ON _rektifier_aws_credentials;
CREATE POLICY rektifier_app_only
    ON _rektifier_aws_credentials
    FOR ALL
    TO rektifier_app
    USING (true)
    WITH CHECK (true);

DROP POLICY IF EXISTS rektifier_app_only ON _rektifier_api_tokens;
CREATE POLICY rektifier_app_only
    ON _rektifier_api_tokens
    FOR ALL
    TO rektifier_app
    USING (true)
    WITH CHECK (true);
