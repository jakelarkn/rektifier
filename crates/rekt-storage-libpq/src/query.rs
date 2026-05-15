//! `query_raw`: bounded read over one partition with keyset pagination.
//!
//! SQL shape (per call; assembled programmatically since the WHERE
//! varies with both `sk_condition` and the presence of an
//! `exclusive_start_key`):
//!
//! ```sql
//! SELECT <jsonb_col> FROM <table>
//!   WHERE <pk_col> = $1::<pk_type>
//!     [ AND <sk_col> <op>      $N::<sk_type> ]      -- Eq/Lt/Le/Gt/Ge
//!     [ AND <sk_col> BETWEEN   $N::<sk_type> AND $M::<sk_type> ]
//!     [ AND <sk_col> LIKE      $N ESCAPE '\' ]      -- BeginsWithS
//!     [ AND <sk_col> > $N::<sk_type> ]              -- ExclusiveStartKey
//!   ORDER BY <sk_col> ASC
//!   LIMIT $LAST                                     -- bound as L+1
//! ```
//!
//! `LIMIT` is bound separately so the same prepared statement can serve
//! every call; the dispatcher applies a soft cap of 1000 when the caller
//! omits one (see `COMPATIBILITY_NOTES.md`). The backend reads `L+1`
//! rows; if `L+1` came back, the L-th row's key becomes the
//! `LastEvaluatedKey` and the sentinel is dropped before returning.

use crate::types::{cast_for_keytype, map_pg_err, pg_type_for_keytype, quote_ident, Bound};
use crate::PgBackend;
use bytes::Bytes;
use rekt_storage::{BackendError, KeyType, KeyValue, QueryOutcome, SkCondition, TableShape};
use tokio_postgres::types::{Json, ToSql, Type};
use tracing::Instrument;

const DEFAULT_LIMIT: u32 = 1000;

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "pg.query_raw",
    fields(table = %shape.table)
)]
pub(crate) async fn query_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk_condition: Option<&SkCondition>,
    limit: Option<u32>,
    exclusive_start_key: Option<(&KeyValue, Option<&KeyValue>)>,
) -> Result<QueryOutcome, BackendError> {
    if sk_condition.is_some() && shape.sk_col.is_none() {
        return Err(BackendError::UnexpectedSortKey {
            name: shape.table.to_string(),
        });
    }

    // Hash-only Query with ESK present always returns empty + no LEK:
    // each partition has at most one row, so "after this key" never
    // matches anything. DDB has the same behavior. Short-circuit to
    // skip SQL entirely.
    if shape.sk_col.is_none() && exclusive_start_key.is_some() {
        return Ok(QueryOutcome::default());
    }

    let table = quote_ident(shape.table);
    let pk_col_q = quote_ident(shape.pk_col);
    let jsonb_col = quote_ident(shape.jsonb_col);
    let pk_cast = cast_for_keytype(shape.pk_type);
    let pk_pg = pg_type_for_keytype(shape.pk_type);

    // Common per-shape values for SK construction (composite only).
    let sk_col_q = shape.sk_col.map(quote_ident);
    let sk_cast = shape.sk_type.map(cast_for_keytype).unwrap_or("");
    let sk_pg = shape.sk_type.map(pg_type_for_keytype);

    // ---- Assemble WHERE clauses + param-type list + param values ----
    //
    // `$1` is always the PK. `next_idx` advances as each subsequent
    // clause is appended. `params` collects `&dyn ToSql` references in
    // bind order; the underlying values are held in this function's
    // locals (precomputed_like_pattern, etc.) so they live long enough.
    let mut where_sql = format!("{pk_col_q} = $1{pk_cast}");
    let mut types: Vec<Type> = vec![pk_pg];
    let pk_bound = Bound(pk);
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&pk_bound];
    let mut next_idx: usize = 2;

    // SK condition. Build the optional Bound storage values as locals so
    // their references can be pushed into `params` after the matcher.
    let sk_eq_bound: Option<Bound<'_>>;
    let sk_between_lo: Option<Bound<'_>>;
    let sk_between_hi: Option<Bound<'_>>;
    let sk_like_pattern: Option<String>;
    match sk_condition {
        None => {
            sk_eq_bound = None;
            sk_between_lo = None;
            sk_between_hi = None;
            sk_like_pattern = None;
        }
        Some(cond) => {
            // Safety: validated above that sk_col is Some when sk_condition
            // is Some; sk_pg / sk_col_q / sk_cast all aligned.
            let sk_col = sk_col_q.as_deref().expect("sk_condition implies sk_col");
            let sk_type = sk_pg.clone().expect("sk_condition implies sk_type");
            match cond {
                SkCondition::Eq(v)
                | SkCondition::Lt(v)
                | SkCondition::Le(v)
                | SkCondition::Gt(v)
                | SkCondition::Ge(v) => {
                    let op = match cond {
                        SkCondition::Eq(_) => "=",
                        SkCondition::Lt(_) => "<",
                        SkCondition::Le(_) => "<=",
                        SkCondition::Gt(_) => ">",
                        SkCondition::Ge(_) => ">=",
                        _ => unreachable!(),
                    };
                    where_sql.push_str(&format!(
                        " AND {sk_col} {op} ${next_idx}{sk_cast}"
                    ));
                    types.push(sk_type);
                    sk_eq_bound = Some(Bound(v));
                    sk_between_lo = None;
                    sk_between_hi = None;
                    sk_like_pattern = None;
                    next_idx += 1;
                }
                SkCondition::Between(lo, hi) => {
                    where_sql.push_str(&format!(
                        " AND {sk_col} BETWEEN ${next_idx}{sk_cast} AND ${}{sk_cast}",
                        next_idx + 1
                    ));
                    types.push(sk_type.clone());
                    types.push(sk_type);
                    sk_eq_bound = None;
                    sk_between_lo = Some(Bound(lo));
                    sk_between_hi = Some(Bound(hi));
                    sk_like_pattern = None;
                    next_idx += 2;
                }
                SkCondition::BeginsWithS(prefix) => {
                    where_sql.push_str(&format!(
                        " AND {sk_col} LIKE ${next_idx} ESCAPE '\\'"
                    ));
                    types.push(Type::TEXT);
                    sk_eq_bound = None;
                    sk_between_lo = None;
                    sk_between_hi = None;
                    sk_like_pattern = Some(format!("{}%", escape_like(prefix)));
                    next_idx += 1;
                }
            }
        }
    }
    if let Some(b) = sk_eq_bound.as_ref() {
        params.push(b);
    }
    if let (Some(lo), Some(hi)) = (sk_between_lo.as_ref(), sk_between_hi.as_ref()) {
        params.push(lo);
        params.push(hi);
    }
    if let Some(pat) = sk_like_pattern.as_ref() {
        params.push(pat);
    }

    // ExclusiveStartKey (composite tables only; hash-only short-
    // circuited above). For forward order, we want rows strictly
    // greater than the ESK's sk component. The ESK's pk is required to
    // equal the query pk in v1 (translator-enforced); we don't re-bind
    // it here since the PK equality is already in the WHERE.
    let esk_sk_bound: Option<Bound<'_>>;
    match exclusive_start_key {
        None => {
            esk_sk_bound = None;
        }
        Some((_esk_pk, Some(esk_sk))) => {
            let sk_col = sk_col_q.as_deref().expect("ESK with composite");
            let sk_type = sk_pg.clone().expect("ESK with composite");
            where_sql.push_str(&format!(" AND {sk_col} > ${next_idx}{sk_cast}"));
            types.push(sk_type);
            esk_sk_bound = Some(Bound(esk_sk));
            next_idx += 1;
        }
        Some((_, None)) => {
            // Composite shape but ESK missing sk component is a
            // translator-layer bug; treat as no ESK.
            esk_sk_bound = None;
        }
    }
    if let Some(b) = esk_sk_bound.as_ref() {
        params.push(b);
    }

    // LIMIT (always last). Bind as L+1 so we can detect "more remain"
    // by counting returned rows.
    let lim_input = limit.unwrap_or(DEFAULT_LIMIT);
    let lim_plus_one = (lim_input as i64).saturating_add(1);
    let order_by = match shape.sk_col {
        Some(sk_col) => format!(" ORDER BY {} ASC", quote_ident(sk_col)),
        None => String::new(),
    };
    types.push(Type::INT8);
    params.push(&lim_plus_one);

    let sql = format!(
        "SELECT {jsonb_col} FROM {table} WHERE {where_sql}{order_by} LIMIT ${next_idx}"
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

    // ---- Decode + apply L+1 sentinel ----
    let mut items: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
    for row in &rows {
        let Json(v): Json<serde_json::Value> = row.get(0);
        items.push(v);
    }
    let has_more = items.len() as i64 == lim_plus_one;
    let last_evaluated_key = if has_more {
        // Drop the sentinel; the L-th row's key becomes the LEK.
        items.truncate(lim_input as usize);
        items
            .last()
            .and_then(|row| lek_from_row(row, shape))
    } else {
        None
    };
    let scanned = items.len() as u32;
    Ok(QueryOutcome {
        count: scanned,
        scanned_count: scanned,
        items,
        last_evaluated_key,
    })
}

/// Pull `(pk, Option<sk>)` out of a stored row's DDB-JSON so the
/// caller can re-encode it as `LastEvaluatedKey`.
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

/// Escape SQL `LIKE` wildcards in a literal prefix so the resulting
/// pattern matches only what the caller intended. Uses `\` as the
/// escape character (paired with `ESCAPE '\'` in the SQL).
fn escape_like(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\\' || c == '%' || c == '_' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}
