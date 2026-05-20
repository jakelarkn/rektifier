//! Startup seeder: idempotently upserts TOML `[[tables]]` rows into
//! `_rektifier_tables`.
//!
//! Transitional (PLAN-10 D2-D7). The seeder is deleted in D8 when the
//! `[[tables]]` block is removed from TOML and `CreateTable` becomes
//! the sole provisioning surface.
//!
//! `INSERT ... ON CONFLICT (table_name) DO NOTHING` is critical: once a
//! row exists in `_rektifier_tables`, the runtime is authoritative and
//! TOML edits don't overwrite operator-driven mutations. This matters
//! during the transition window because operators may CreateTable a
//! row that *also* exists in TOML — the seeder must not clobber it.

use crate::{metadata::key_type_str, metadata::now_ms, CatalogError};
use deadpool_postgres::Pool;
use rekt_config::TableConfig;

const SEED_SQL: &str = "\
INSERT INTO _rektifier_tables (
    table_name, pg_table, jsonb_col, pk_attr, pk_type,
    sk_attr, sk_type, status, serveable,
    creation_date_ms, last_modified_at_ms, last_modified_by
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, 'ACTIVE', true,
    $8, $9, 'seeder'
)
ON CONFLICT (table_name) DO NOTHING
";

/// For each TOML-declared table, attempt to seed a row. Idempotent on
/// re-run: existing rows are left alone. Returns the number of new
/// rows inserted (0 when every table already existed).
///
/// The rows are inserted with `status = ACTIVE` and `serveable = true`
/// because `rekt_meta::verify` runs *before* the seeder and has already
/// confirmed the PG shape. D3's reconciler will be the ongoing
/// authority for `serveable`.
pub async fn seed_from_config(
    pool: &Pool,
    tables: &[TableConfig],
) -> Result<usize, CatalogError> {
    if tables.is_empty() {
        return Ok(0);
    }
    let client = pool
        .get()
        .await
        .map_err(|e| CatalogError::Pool(format!("pool get failed: {e}")))?;
    let stmt = client.prepare(SEED_SQL).await?;
    let mut inserted = 0usize;
    let now = now_ms();
    for t in tables {
        let pk_type = key_type_str(t.pk_type);
        let sk_type = t.sk_type.map(key_type_str);
        let rows_affected = client
            .execute(
                &stmt,
                &[
                    &t.name,
                    &t.pg_table,
                    &t.jsonb_col,
                    &t.pk_attr,
                    &pk_type,
                    &t.sk_attr,
                    &sk_type,
                    &now,
                    &now,
                ],
            )
            .await?;
        if rows_affected > 0 {
            inserted += 1;
            tracing::info!(
                table = %t.name,
                pg_table = %t.pg_table,
                "seeded into _rektifier_tables from TOML"
            );
        } else {
            tracing::debug!(
                table = %t.name,
                "seeder: row already present in _rektifier_tables, leaving as-is"
            );
        }
    }
    Ok(inserted)
}
