//! TransactGetItems — atomic-snapshot multi-key SELECT across one or
//! more tables. Single PG transaction at `REPEATABLE READ READ ONLY`
//! so every per-item read sees the same snapshot of the database.
//!
//! Per-item SQL reuses [`crate::shape::ShapeSql::get_sql`] (the same
//! statement single-row `get_item_raw` issues), so the prepared-
//! statement cache shares entries between transactional and single-
//! row reads.
//!
//! The transaction is committed at the end (no-op for read-only but
//! kept for symmetry); on any error the RAII `Drop` on the
//! `Transaction<'_>` aborts cleanly.

use crate::types::{check_sk_shape, map_pg_err, Bound};
use crate::PgBackend;
use rekt_storage::{BackendError, TransactGetOp, TransactGetOutcome};
use tokio_postgres::types::Json;
use tokio_postgres::IsolationLevel;
use tracing::Instrument;

#[tracing::instrument(level = "debug", skip_all, name = "pg.transact_get_raw", fields(items = items.len()))]
pub(crate) async fn transact_get_raw(
    backend: &PgBackend,
    items: &[TransactGetOp<'_>],
) -> Result<TransactGetOutcome, BackendError> {
    // Validate per-item shape before any SQL fires. Cheap, and lets us
    // map shape mismatches to the same MissingSortKey /
    // UnexpectedSortKey errors the single-row path uses.
    for op in items {
        check_sk_shape(&op.shape, op.sk.as_ref())?;
    }

    let mut client = backend.client().await?;

    // T1: build the snapshot transaction. The `READ ONLY` lets PG skip
    // xid allocation; `REPEATABLE READ` ensures every SELECT below
    // sees the same snapshot (D2 in PLAN-8).
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .instrument(tracing::debug_span!("pg.begin"))
        .await
        .map_err(|e| map_pg_err("<transact>", e))?;

    let mut out: Vec<Option<serde_json::Value>> = Vec::with_capacity(items.len());
    for op in items {
        let sql = backend.shape_sql(&op.shape);
        let stmt = tx
            .prepare_typed_cached(&sql.get_sql, &sql.key_types)
            .instrument(tracing::debug_span!("pg.prepare"))
            .await
            .map_err(|e| map_pg_err(op.shape.table, e))?;

        let pk_bound = Bound(&op.pk);
        let row_opt = match op.sk.as_ref() {
            None => {
                tx.query_opt(&stmt, &[&pk_bound])
                    .instrument(tracing::debug_span!("pg.query"))
                    .await
            }
            Some(sk_val) => {
                let sk_bound = Bound(sk_val);
                tx.query_opt(&stmt, &[&pk_bound, &sk_bound])
                    .instrument(tracing::debug_span!("pg.query"))
                    .await
            }
        }
        .map_err(|e| map_pg_err(op.shape.table, e))?;

        out.push(row_opt.map(|r| {
            let Json(v): Json<serde_json::Value> = r.get(0);
            v
        }));
    }

    tx.commit()
        .instrument(tracing::debug_span!("pg.commit"))
        .await
        .map_err(|e| map_pg_err("<transact>", e))?;

    Ok(TransactGetOutcome { items: out })
}
