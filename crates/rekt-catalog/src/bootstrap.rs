//! Idempotent creation of the catalog's internal PG tables.
//!
//! Runs on every rektifier startup. `_rektifier_tables` and
//! `_rektifier_gsi_state` are owned by rektifier; operators introspect
//! them via `psql` but should not hand-edit while the server runs.
//! See PLAN-10 KD1 / KD12.

use crate::CatalogError;
use deadpool_postgres::Pool;

const CREATE_TABLES_SQL: &str = "\
CREATE TABLE IF NOT EXISTS _rektifier_tables (
    table_name              text PRIMARY KEY,
    pg_table                text NOT NULL,
    jsonb_col               text NOT NULL DEFAULT 'data',
    pk_attr                 text NOT NULL,
    pk_type                 text NOT NULL,
    sk_attr                 text,
    sk_type                 text,
    status                  text NOT NULL,
    serveable               boolean NOT NULL DEFAULT false,
    unserveable_reason      text,
    last_modified_at_ms     bigint NOT NULL,
    last_modified_by        text NOT NULL,
    creation_date_ms        bigint NOT NULL,
    tags                    jsonb NOT NULL DEFAULT '{}'::jsonb,
    billing_mode            text,
    provisioned_rcu         bigint,
    provisioned_wcu         bigint,
    lsi_specs               jsonb NOT NULL DEFAULT '[]'::jsonb,
    UNIQUE (pg_table)
);

-- PLAN-11 L3. Idempotent for catalogs created before LSI support; rows
-- pre-dating the column default into the empty-list shape so they
-- round-trip cleanly through the reconciler.
ALTER TABLE _rektifier_tables
    ADD COLUMN IF NOT EXISTS lsi_specs jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS _rektifier_tables_status_idx
    ON _rektifier_tables (status);
";

/// Create `_rektifier_tables` and its indexes if absent. Safe to call on
/// every boot; the `IF NOT EXISTS` clauses make this a no-op after the
/// first run. Does NOT create `_rektifier_gsi_state` — that lands with
/// PLAN-9.
pub async fn ensure_metadata_tables(pool: &Pool) -> Result<(), CatalogError> {
    let client = pool
        .get()
        .await
        .map_err(|e| CatalogError::Pool(format!("pool get failed: {e}")))?;
    client.batch_execute(CREATE_TABLES_SQL).await?;
    tracing::debug!("_rektifier_tables ensured");
    Ok(())
}
