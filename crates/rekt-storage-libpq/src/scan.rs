//! `scan_raw`: full-table read with keyset pagination + optional
//! filter.
//!
//! SQL shape:
//!
//! ```sql
//! SELECT <jsonb_col> FROM <table>
//!   [ WHERE <pk_col> > $1::<pk_type> ]                         -- hash-only ESK
//!   [ WHERE (<pk_col>, <sk_col>) > ($1, $2)::<pk_type, sk_type> ]  -- composite ESK
//!   ORDER BY <pk_col> [, <sk_col>]
//!   LIMIT $N+1
//! ```
//!
//! The `L+1` sentinel pattern mirrors `query_raw`: read one extra
//! row to detect "more remain"; LEK is derived from the L-th
//! *scanned* (pre-filter) row's keys so paging continues even when
//! a filter drops everything in the current page.

use crate::types::{cast_for_keytype, map_pg_err, pg_type_for_keytype, quote_ident, Bound};
use crate::PgBackend;
use bytes::Bytes;
use rekt_storage::{
    BackendError, FilterEvalFn, KeyType, KeyValue, ScanOutcome, TableShape,
};
use tokio_postgres::types::{Json, ToSql, Type};
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
    filter: Option<FilterEvalFn<'_>>,
    limit: Option<u32>,
    exclusive_start_key: Option<(&KeyValue, Option<&KeyValue>)>,
) -> Result<ScanOutcome, BackendError> {
    let table = quote_ident(shape.table);
    let jsonb_col = quote_ident(shape.jsonb_col);
    let pk_col_q = quote_ident(shape.pk_col);
    let pk_cast = cast_for_keytype(shape.pk_type);
    let pk_pg = pg_type_for_keytype(shape.pk_type);

    let sk_col_q = shape.sk_col.map(quote_ident);
    let sk_cast = shape.sk_type.map(cast_for_keytype).unwrap_or("");
    let sk_pg = shape.sk_type.map(pg_type_for_keytype);

    // ---- Assemble WHERE + params for ESK ----
    let mut where_sql = String::new();
    let mut types: Vec<Type> = vec![];
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
    let mut next_idx: usize = 1;

    let esk_pk_bound: Option<Bound<'_>>;
    let esk_sk_bound: Option<Bound<'_>>;
    match exclusive_start_key {
        None => {
            esk_pk_bound = None;
            esk_sk_bound = None;
        }
        Some((esk_pk, esk_sk)) => match (sk_col_q.as_deref(), sk_pg.clone(), esk_sk) {
            (Some(sk_col), Some(sk_type), Some(sk_val)) => {
                // Composite: row-constructor comparison.
                where_sql.push_str(&format!(
                    " WHERE ({pk_col_q}, {sk_col}) > (${}{pk_cast}, ${}{sk_cast})",
                    next_idx,
                    next_idx + 1
                ));
                types.push(pk_pg.clone());
                types.push(sk_type);
                esk_pk_bound = Some(Bound(esk_pk));
                esk_sk_bound = Some(Bound(sk_val));
                next_idx += 2;
            }
            (None, _, None) => {
                // Hash-only: just `pk > $1`.
                where_sql.push_str(&format!(
                    " WHERE {pk_col_q} > ${}{pk_cast}",
                    next_idx
                ));
                types.push(pk_pg.clone());
                esk_pk_bound = Some(Bound(esk_pk));
                esk_sk_bound = None;
                next_idx += 1;
            }
            // Shape/ESK mismatch — translator-layer bug; treat as no ESK.
            _ => {
                esk_pk_bound = None;
                esk_sk_bound = None;
            }
        },
    }
    if let Some(b) = esk_pk_bound.as_ref() {
        params.push(b);
    }
    if let Some(b) = esk_sk_bound.as_ref() {
        params.push(b);
    }

    // ---- ORDER BY ----
    let order_by = match shape.sk_col {
        Some(sk) => format!("{pk_col_q}, {}", quote_ident(sk)),
        None => pk_col_q.clone(),
    };

    // ---- LIMIT ----
    // Match DDB's LEK semantic: set LastEvaluatedKey iff the page
    // filled exactly to the requested Limit (whether or not more
    // data exists past it). Read exactly `Limit` rows — no L+1
    // sentinel.
    let lim_input = limit.unwrap_or(DEFAULT_LIMIT);
    let lim_bound = lim_input as i64;
    types.push(Type::INT8);
    params.push(&lim_bound);
    let limit_idx = next_idx;

    let sql = format!(
        "SELECT {jsonb_col} FROM {table}{where_sql} ORDER BY {order_by} LIMIT ${limit_idx}"
    );

    // ---- Execute ----
    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql, &types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let rows = client
        .query(&stmt, &params)
        .instrument(tracing::debug_span!("pg.query"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    // ---- Decode + filter ----
    //
    // Same two-pass shape as query.rs: decode all scanned rows so
    // the L-th row is known for LEK before filter drops it, then
    // apply the filter to produce the items list. LEK is set
    // whenever the page filled exactly to Limit (DDB parity).
    let mut scanned_rows: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
    for row in &rows {
        let Json(v): Json<serde_json::Value> = row.get(0);
        scanned_rows.push(v);
    }
    let filled_to_limit = scanned_rows.len() as u32 == lim_input;
    let last_evaluated_key = if filled_to_limit {
        scanned_rows
            .last()
            .and_then(|row| lek_from_row(row, shape))
    } else {
        None
    };
    let scanned_count = scanned_rows.len() as u32;
    let items: Vec<serde_json::Value> = match filter {
        None => scanned_rows,
        Some(f) => scanned_rows.into_iter().filter(|row| f(row)).collect(),
    };
    let count = items.len() as u32;
    Ok(ScanOutcome {
        items,
        count,
        scanned_count,
        last_evaluated_key,
    })
}

/// Pull `(pk, Option<sk>)` out of a stored row's DDB-JSON. Same shape
/// as the Query helper; duplicated here so the two modules stay
/// decoupled.
fn lek_from_row(
    row: &serde_json::Value,
    shape: &TableShape<'_>,
) -> Option<(KeyValue, Option<KeyValue>)> {
    let pk = extract_key_from_jsonb(row, shape.pk_col, shape.pk_type)?;
    let sk = match (shape.sk_col, shape.sk_type) {
        (Some(col), Some(ty)) => Some(extract_key_from_jsonb(row, col, ty)?),
        _ => None,
    };
    Some((pk, sk))
}

fn extract_key_from_jsonb(
    row: &serde_json::Value,
    attr: &str,
    ty: KeyType,
) -> Option<KeyValue> {
    let tagged = row.get(attr)?;
    match ty {
        KeyType::S => tagged
            .get("S")?
            .as_str()
            .map(|s| KeyValue::S(s.to_string())),
        KeyType::N => tagged
            .get("N")?
            .as_str()
            .map(|s| KeyValue::N(s.to_string())),
        KeyType::B => {
            use base64::Engine;
            let s = tagged.get("B")?.as_str()?;
            base64::engine::general_purpose::STANDARD
                .decode(s)
                .ok()
                .map(|b| KeyValue::B(Bytes::from(b)))
        }
    }
}
