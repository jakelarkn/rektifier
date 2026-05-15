//! `batch_write_raw`: multi-row upsert + multi-key delete against a
//! single table, in one transaction.
//!
//! SQL shape (per call; the VALUES list scales with `ops.len()`):
//!
//! ```sql
//! BEGIN;
//!
//! -- Puts (one statement, regardless of count):
//! INSERT INTO <table> (<jsonb_col>) VALUES
//!   ($1::jsonb), ($2::jsonb), ...
//! ON CONFLICT (<pk_col>[, <sk_col>])
//! DO UPDATE SET <jsonb_col> = EXCLUDED.<jsonb_col>;
//!
//! -- Deletes (one statement; hash-only uses `ANY`, composite uses `IN VALUES`):
//! DELETE FROM <table> WHERE <pk_col> = ANY($1::<pk_type>[]);
//!   -- or --
//! DELETE FROM <table>
//!   WHERE (<pk_col>, <sk_col>) IN (($1::pk_t, $2::sk_t), ...);
//!
//! COMMIT;
//! ```
//!
//! Both statements run inside one transaction so a SQL-level failure
//! rolls back the table's writes cleanly. Cross-table tables are
//! run in *separate* transactions (caller iterates table-by-table)
//! — matches DDB's "not atomic" guarantee.
//!
//! The Put statement is identical in shape to single-row PutItem's
//! `INSERT … ON CONFLICT DO UPDATE` but without the `WITH prev` CTE
//! that captures `ALL_OLD` — BatchWriteItem has no `ReturnValues`.
//! Per D2b in PLAN-6, this reuses the same `Bound` / cast / type-
//! mapping helpers as the single-row path; we don't reinvent them.
//!
//! Caller is responsible for per-table deduplication (translator
//! rejects "same key twice in one request" before we reach this
//! method). With no dupes, `DO UPDATE SET data = EXCLUDED.data` is
//! deterministic.

use crate::types::{cast_for_keytype, map_pg_err, pg_type_for_keytype, quote_ident, Bound};
use crate::PgBackend;
use rekt_storage::{BackendError, BatchWriteOutcome, KeyType, KeyValue, TableShape, WriteOp};
use tokio_postgres::types::{Json, ToSql, Type};
use tracing::Instrument;

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "pg.batch_write_raw",
    fields(table = %shape.table, ops = ops.len())
)]
pub(crate) async fn batch_write_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    ops: &[WriteOp],
) -> Result<BatchWriteOutcome, BackendError> {
    if ops.is_empty() {
        return Ok(BatchWriteOutcome::default());
    }

    // Partition into Puts and Deletes once. Keep references to the
    // original `WriteOp`s so we don't copy the jsonb payloads.
    let mut puts: Vec<&serde_json::Value> = Vec::new();
    let mut deletes: Vec<(&KeyValue, Option<&KeyValue>)> = Vec::new();
    for op in ops {
        match op {
            WriteOp::Put { sk, item, .. } => {
                // Shape consistency: composite shape must have sk; hash
                // must not. The translator validates this per request
                // but a backend-side guard catches future direct callers.
                match (shape.sk_col, sk) {
                    (Some(_), Some(_)) | (None, None) => {}
                    (Some(_), None) => {
                        return Err(BackendError::MissingSortKey {
                            name: shape.table.to_string(),
                        });
                    }
                    (None, Some(_)) => {
                        return Err(BackendError::UnexpectedSortKey {
                            name: shape.table.to_string(),
                        });
                    }
                }
                puts.push(item);
            }
            WriteOp::Delete { pk, sk } => {
                match (shape.sk_col, sk) {
                    (Some(_), Some(_)) | (None, None) => {}
                    (Some(_), None) => {
                        return Err(BackendError::MissingSortKey {
                            name: shape.table.to_string(),
                        });
                    }
                    (None, Some(_)) => {
                        return Err(BackendError::UnexpectedSortKey {
                            name: shape.table.to_string(),
                        });
                    }
                }
                deletes.push((pk, sk.as_ref()));
            }
        }
    }

    let mut client = backend.client().await?;
    let tx = client
        .transaction()
        .instrument(tracing::debug_span!("pg.begin"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    if !puts.is_empty() {
        execute_puts(&tx, shape, &puts).await?;
    }
    if !deletes.is_empty() {
        execute_deletes(&tx, shape, &deletes).await?;
    }

    tx.commit()
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    Ok(BatchWriteOutcome::default())
}

/// Emit and run the per-table multi-row upsert. One statement, N
/// `($k::jsonb)` value rows, one `ON CONFLICT (pk[,sk]) DO UPDATE SET
/// jsonb_col = EXCLUDED.jsonb_col`. Each row's jsonb is bound through
/// `Json<&Value>` — the same wrapper single-row PutItem uses.
async fn execute_puts(
    tx: &deadpool_postgres::Transaction<'_>,
    shape: &TableShape<'_>,
    puts: &[&serde_json::Value],
) -> Result<(), BackendError> {
    let table = quote_ident(shape.table);
    let pk_col = quote_ident(shape.pk_col);
    let jsonb_col = quote_ident(shape.jsonb_col);
    let conflict_target = match shape.sk_col {
        None => pk_col.clone(),
        Some(sk_col) => format!("{pk_col}, {}", quote_ident(sk_col)),
    };

    // Build the `($1::jsonb), ($2::jsonb), …` value list.
    let mut values_clause = String::with_capacity(puts.len() * 12);
    for i in 0..puts.len() {
        if i > 0 {
            values_clause.push_str(", ");
        }
        values_clause.push_str(&format!("(${}::jsonb)", i + 1));
    }
    let sql = format!(
        "INSERT INTO {table} ({jsonb_col}) VALUES {values_clause} \
         ON CONFLICT ({conflict_target}) \
         DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
    );
    let param_types: Vec<Type> = vec![Type::JSONB; puts.len()];

    let stmt = tx
        .prepare_typed_cached(&sql, &param_types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let jsons: Vec<Json<&serde_json::Value>> = puts.iter().map(|v| Json(*v)).collect();
    let params: Vec<&(dyn ToSql + Sync)> =
        jsons.iter().map(|j| j as &(dyn ToSql + Sync)).collect();
    tx.execute(&stmt, &params)
        .instrument(tracing::debug_span!("pg.execute"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    Ok(())
}

/// Emit and run the per-table multi-key delete. Hash-only uses
/// `ANY($1::type[])`; composite uses `(pk, sk) IN (VALUES ...)` —
/// same primitives as `batch_get_raw`.
async fn execute_deletes(
    tx: &deadpool_postgres::Transaction<'_>,
    shape: &TableShape<'_>,
    deletes: &[(&KeyValue, Option<&KeyValue>)],
) -> Result<(), BackendError> {
    let table = quote_ident(shape.table);
    let pk_col_q = quote_ident(shape.pk_col);

    match (shape.sk_col, shape.sk_type) {
        (None, _) => {
            // Hash-only: bind a single array of pk values.
            match shape.pk_type {
                KeyType::S => {
                    let pks: Vec<&str> = deletes
                        .iter()
                        .map(|(pk, _)| match pk {
                            KeyValue::S(s) => s.as_str(),
                            _ => unreachable!("translator validated pk type"),
                        })
                        .collect();
                    let sql = format!("DELETE FROM {table} WHERE {pk_col_q} = ANY($1)");
                    let stmt = tx
                        .prepare_typed_cached(&sql, &[Type::TEXT_ARRAY])
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    tx.execute(&stmt, &[&pks])
                        .instrument(tracing::debug_span!("pg.delete"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                }
                KeyType::N => {
                    let pks: Vec<&str> = deletes
                        .iter()
                        .map(|(pk, _)| match pk {
                            KeyValue::N(s) => s.as_str(),
                            _ => unreachable!("translator validated pk type"),
                        })
                        .collect();
                    let sql = format!(
                        "DELETE FROM {table} WHERE {pk_col_q} = ANY($1::numeric[])"
                    );
                    let stmt = tx
                        .prepare_typed_cached(&sql, &[Type::TEXT_ARRAY])
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    tx.execute(&stmt, &[&pks])
                        .instrument(tracing::debug_span!("pg.delete"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                }
                KeyType::B => {
                    let pks: Vec<&[u8]> = deletes
                        .iter()
                        .map(|(pk, _)| match pk {
                            KeyValue::B(b) => b.as_ref(),
                            _ => unreachable!("translator validated pk type"),
                        })
                        .collect();
                    let sql = format!("DELETE FROM {table} WHERE {pk_col_q} = ANY($1)");
                    let stmt = tx
                        .prepare_typed_cached(&sql, &[Type::BYTEA_ARRAY])
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    tx.execute(&stmt, &[&pks])
                        .instrument(tracing::debug_span!("pg.delete"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                }
            }
        }
        (Some(sk_col_name), Some(sk_type)) => {
            let sk_col_q = quote_ident(sk_col_name);
            let pk_cast = cast_for_keytype(shape.pk_type);
            let sk_cast = cast_for_keytype(sk_type);
            let pk_pg = pg_type_for_keytype(shape.pk_type);
            let sk_pg = pg_type_for_keytype(sk_type);

            let mut values_clause = String::with_capacity(deletes.len() * 32);
            let mut param_types: Vec<Type> = Vec::with_capacity(deletes.len() * 2);
            for i in 0..deletes.len() {
                if i > 0 {
                    values_clause.push_str(", ");
                }
                let pk_idx = i * 2 + 1;
                let sk_idx = i * 2 + 2;
                values_clause.push_str(&format!("(${pk_idx}{pk_cast}, ${sk_idx}{sk_cast})"));
                param_types.push(pk_pg.clone());
                param_types.push(sk_pg.clone());
            }
            let sql = format!(
                "DELETE FROM {table} WHERE ({pk_col_q}, {sk_col_q}) IN ({values_clause})"
            );

            let bounds: Vec<Bound<'_>> = deletes
                .iter()
                .flat_map(|(pk, sk)| {
                    let sk_ref = sk.expect("composite shape implies sk present");
                    [Bound(pk), Bound(sk_ref)]
                })
                .collect();
            let params: Vec<&(dyn ToSql + Sync)> =
                bounds.iter().map(|b| b as &(dyn ToSql + Sync)).collect();

            let stmt = tx
                .prepare_typed_cached(&sql, &param_types)
                .await
                .map_err(|e| map_pg_err(shape.table, e))?;
            tx.execute(&stmt, &params)
                .instrument(tracing::debug_span!("pg.delete"))
                .await
                .map_err(|e| map_pg_err(shape.table, e))?;
        }
        (Some(_), None) => unreachable!(
            "TableShape `{}`: sk_col without sk_type",
            shape.table
        ),
    }
    Ok(())
}
