//! `scan_raw`: unbounded read over the whole table.
//!
//! SQL shape:
//!
//! ```sql
//! SELECT <jsonb_col> FROM <table>
//!   ORDER BY <pk_col> [, <sk_col>]
//!   LIMIT $1
//! ```
//!
//! Q4 binds `LIMIT` as a soft cap (1000 items — see
//! `COMPATIBILITY_NOTES.md`) and exposes no other knobs. Q5 will lift
//! `Limit`, `ExclusiveStartKey`, `FilterExpression`, and parallel-scan
//! state through here.
//!
//! ORDER BY is deterministic so Q5's keyset pagination has a stable
//! row order to resume against. DDB's Scan order is officially
//! undefined; the diff tests assert set equality, not list equality.

use crate::types::{map_pg_err, quote_ident};
use crate::PgBackend;
use rekt_storage::{BackendError, ScanOutcome, TableShape};
use tokio_postgres::types::{Json, Type};
use tracing::Instrument;

const DEFAULT_LIMIT: u32 = 1000;

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "pg.scan_raw",
    fields(table = %shape.table)
)]
pub(crate) async fn scan_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
) -> Result<ScanOutcome, BackendError> {
    let table = quote_ident(shape.table);
    let jsonb_col = quote_ident(shape.jsonb_col);
    let pk_col = quote_ident(shape.pk_col);

    let order_by = match shape.sk_col {
        Some(sk) => format!("{pk_col}, {}", quote_ident(sk)),
        None => pk_col.clone(),
    };

    let sql = format!(
        "SELECT {jsonb_col} FROM {table} ORDER BY {order_by} LIMIT $1"
    );

    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql, &[Type::INT8])
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let lim = DEFAULT_LIMIT as i64;
    let rows = client
        .query(&stmt, &[&lim])
        .instrument(tracing::debug_span!("pg.query"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let mut items: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
    for row in rows {
        let Json(v): Json<serde_json::Value> = row.get(0);
        items.push(v);
    }
    let n = items.len() as u32;
    Ok(ScanOutcome {
        items,
        count: n,
        scanned_count: n,
        last_evaluated_key: None,
    })
}
