//! `batch_get_raw`: multi-key SELECT against a single table.
//!
//! SQL shape (one statement, regardless of key count):
//!
//! ```sql
//! -- Hash-only:
//! SELECT <jsonb_col> FROM <table> WHERE <pk_col> = ANY($1::<pk_type>[])
//!
//! -- Composite:
//! SELECT <jsonb_col> FROM <table>
//!   WHERE (<pk_col>, <sk_col>) IN (
//!     ($1::<pk_type>, $2::<sk_type>),
//!     ($3::<pk_type>, $4::<sk_type>),
//!     ...
//!   )
//! ```
//!
//! Hash-only uses `pk = ANY($1::type[])`: one bound parameter (an
//! array). Composite uses an `IN (VALUES …)` list with per-key
//! parameters so each row's pk/sk are bound independently — there's
//! no portable two-column-array idiom in PG that's cleaner.
//!
//! Missing rows are silently absent from the response (PG returns no
//! row for them). Caller is responsible for deduplication; the
//! translator does this so the SQL emitter doesn't have to guard
//! against duplicate `(pk, sk)` pairs.
//!
//! No `ORDER BY` is emitted — DDB itself doesn't guarantee response
//! order for BatchGetItem, and clients are required to not depend on
//! one (D9 in `PLAN-6`).

use crate::types::{cast_for_keytype, map_pg_err, pg_type_for_keytype, quote_ident, Bound};
use crate::PgBackend;
use rekt_storage::{BackendError, BatchGetOutcome, KeyType, KeyValue, TableShape};
use tokio_postgres::types::{Json, ToSql, Type};
use tracing::Instrument;

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "pg.batch_get_raw",
    fields(table = %shape.table, keys = keys.len())
)]
pub(crate) async fn batch_get_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    keys: &[(KeyValue, Option<KeyValue>)],
) -> Result<BatchGetOutcome, BackendError> {
    // Empty `keys` is filtered out by the translator (EmptyKeysInBatchGet).
    // Be defensive anyway — an empty `IN (VALUES …)` is a SQL syntax error.
    if keys.is_empty() {
        return Ok(BatchGetOutcome::default());
    }

    // Shape consistency: every key tuple must agree with the table's
    // composite/hash configuration. The translator already enforces this
    // per-key via `extract_key`, but a backend-side check guards against
    // a future caller that builds a `Vec<(KeyValue, Option<KeyValue>)>`
    // by hand without going through the translator.
    for (_, sk) in keys {
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
    }

    let table = quote_ident(shape.table);
    let pk_col_q = quote_ident(shape.pk_col);
    let jsonb_col = quote_ident(shape.jsonb_col);
    let pk_pg = pg_type_for_keytype(shape.pk_type);

    let client = backend.client().await?;

    match (shape.sk_col, shape.sk_type) {
        // ---- Hash-only: WHERE pk = ANY($1::type[]) ------------------------
        (None, _) => {
            // ANY's element type matches the PG column type so PG can
            // index-probe directly. Array is owned for the duration of
            // the call.
            match shape.pk_type {
                KeyType::S => {
                    let pk_values: Vec<&str> = keys
                        .iter()
                        .map(|(pk, _)| match pk {
                            KeyValue::S(s) => s.as_str(),
                            _ => unreachable!(
                                "translator validated pk type before batch_get_raw"
                            ),
                        })
                        .collect();
                    let sql = format!(
                        "SELECT {jsonb_col} FROM {table} WHERE {pk_col_q} = ANY($1)"
                    );
                    let stmt = client
                        .prepare_typed_cached(&sql, &[Type::TEXT_ARRAY])
                        .instrument(tracing::debug_span!("pg.prepare"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    let rows = client
                        .query(&stmt, &[&pk_values])
                        .instrument(tracing::debug_span!("pg.query"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    Ok(BatchGetOutcome {
                        items: collect_items(rows),
                    })
                }
                KeyType::N => {
                    // N binds as text; the array element type is TEXT and
                    // the SQL casts the entire array via `::numeric[]`.
                    let pk_values: Vec<&str> = keys
                        .iter()
                        .map(|(pk, _)| match pk {
                            KeyValue::N(s) => s.as_str(),
                            _ => unreachable!(
                                "translator validated pk type before batch_get_raw"
                            ),
                        })
                        .collect();
                    let sql = format!(
                        "SELECT {jsonb_col} FROM {table} \
                         WHERE {pk_col_q} = ANY($1::numeric[])"
                    );
                    let stmt = client
                        .prepare_typed_cached(&sql, &[Type::TEXT_ARRAY])
                        .instrument(tracing::debug_span!("pg.prepare"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    let rows = client
                        .query(&stmt, &[&pk_values])
                        .instrument(tracing::debug_span!("pg.query"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    Ok(BatchGetOutcome {
                        items: collect_items(rows),
                    })
                }
                KeyType::B => {
                    let pk_values: Vec<&[u8]> = keys
                        .iter()
                        .map(|(pk, _)| match pk {
                            KeyValue::B(b) => b.as_ref(),
                            _ => unreachable!(
                                "translator validated pk type before batch_get_raw"
                            ),
                        })
                        .collect();
                    let sql = format!(
                        "SELECT {jsonb_col} FROM {table} WHERE {pk_col_q} = ANY($1)"
                    );
                    let stmt = client
                        .prepare_typed_cached(&sql, &[Type::BYTEA_ARRAY])
                        .instrument(tracing::debug_span!("pg.prepare"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    let rows = client
                        .query(&stmt, &[&pk_values])
                        .instrument(tracing::debug_span!("pg.query"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    Ok(BatchGetOutcome {
                        items: collect_items(rows),
                    })
                }
            }
        }

        // ---- Composite: WHERE (pk, sk) IN ((..), (..), ...) ----------------
        (Some(sk_col_name), Some(sk_type)) => {
            let sk_col_q = quote_ident(sk_col_name);
            let pk_cast = cast_for_keytype(shape.pk_type);
            let sk_cast = cast_for_keytype(sk_type);
            let sk_pg = pg_type_for_keytype(sk_type);

            // Build the VALUES list with two parameters per key tuple.
            // `next_idx` starts at 1 (PG uses 1-based positional params).
            let mut values_clause = String::with_capacity(keys.len() * 32);
            let mut param_types: Vec<Type> = Vec::with_capacity(keys.len() * 2);
            for i in 0..keys.len() {
                if i > 0 {
                    values_clause.push_str(", ");
                }
                let pk_idx = i * 2 + 1;
                let sk_idx = i * 2 + 2;
                values_clause.push_str(&format!(
                    "(${pk_idx}{pk_cast}, ${sk_idx}{sk_cast})"
                ));
                param_types.push(pk_pg.clone());
                param_types.push(sk_pg.clone());
            }
            let sql = format!(
                "SELECT {jsonb_col} FROM {table} \
                 WHERE ({pk_col_q}, {sk_col_q}) IN ({values_clause})"
            );

            // Build the bound-parameter slice. `Bound<'a>` borrows from
            // the keys vec, so the lifetimes here are all tied to the
            // caller's input.
            let bounds: Vec<Bound<'_>> = keys
                .iter()
                .flat_map(|(pk, sk)| {
                    // sk is `Some` for all entries — checked above.
                    let sk_ref = sk.as_ref().expect("composite shape implies sk present");
                    [Bound(pk), Bound(sk_ref)]
                })
                .collect();
            let params: Vec<&(dyn ToSql + Sync)> =
                bounds.iter().map(|b| b as &(dyn ToSql + Sync)).collect();

            let stmt = client
                .prepare_typed_cached(&sql, &param_types)
                .instrument(tracing::debug_span!("pg.prepare"))
                .await
                .map_err(|e| map_pg_err(shape.table, e))?;
            let rows = client
                .query(&stmt, &params)
                .instrument(tracing::debug_span!("pg.query"))
                .await
                .map_err(|e| map_pg_err(shape.table, e))?;
            Ok(BatchGetOutcome {
                items: collect_items(rows),
            })
        }
        (Some(_), None) => unreachable!(
            "TableShape `{}`: sk_col without sk_type — config validates against this",
            shape.table
        ),
    }
}

fn collect_items(rows: Vec<tokio_postgres::Row>) -> Vec<serde_json::Value> {
    rows.into_iter()
        .map(|r| {
            let Json(v): Json<serde_json::Value> = r.get(0);
            v
        })
        .collect()
}
