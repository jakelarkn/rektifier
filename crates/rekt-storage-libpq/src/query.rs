//! `query_raw`: bounded read over one partition.
//!
//! SQL shape (per call; not cached because the WHERE varies with
//! `sk_condition`):
//!
//! ```sql
//! SELECT <jsonb_col> FROM <table>
//!   WHERE <pk_col> = $1::<pk_type>
//!     [ AND <sk_col> <op>      $2::<sk_type> ]      -- Eq/Lt/Le/Gt/Ge
//!     [ AND <sk_col> BETWEEN   $2::<sk_type> AND $3::<sk_type> ]
//!     [ AND <sk_col> LIKE      $2 || '%' ESCAPE '\\' ]  -- BeginsWithS
//!   ORDER BY <sk_col> ASC
//!   LIMIT $N
//! ```
//!
//! `LIMIT` is bound separately so the same prepared statement can serve
//! every call; for Q1 the dispatcher applies a soft cap of 1000 when
//! the caller doesn't supply one.

use crate::types::{cast_for_keytype, map_pg_err, pg_type_for_keytype, quote_ident, Bound};
use crate::PgBackend;
use rekt_storage::{BackendError, KeyValue, QueryOutcome, SkCondition, TableShape};
use tokio_postgres::types::{Json, Type};
use tracing::Instrument;

/// Soft cap when no `Limit` is supplied. DDB's real cap is 1 MB per
/// page (variable item count); we cap by item count instead. See
/// `COMPATIBILITY_NOTES.md`.
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
) -> Result<QueryOutcome, BackendError> {
    // A sort-key predicate against a hash-only table is a translator-
    // detectable bug; double-check at the storage boundary so we don't
    // silently emit "AND NULL = $2" or similar.
    if sk_condition.is_some() && shape.sk_col.is_none() {
        return Err(BackendError::UnexpectedSortKey {
            name: shape.table.to_string(),
        });
    }
    // Hash-only tables can be queried (PK equality with no SK predicate)
    // — that's just a 0-or-1-row return. Composite tables can also be
    // queried without an SK predicate (returns the whole partition).
    // `check_sk_shape` is too strict for Query because the SK is bound
    // via the predicate, not as a separate KeyValue.

    let table = quote_ident(shape.table);
    let pk_col = quote_ident(shape.pk_col);
    let jsonb_col = quote_ident(shape.jsonb_col);
    let pk_cast = cast_for_keytype(shape.pk_type);
    let pk_pg = pg_type_for_keytype(shape.pk_type);

    let order_by = match shape.sk_col {
        Some(sk_col) => format!(" ORDER BY {} ASC", quote_ident(sk_col)),
        None => String::new(),
    };

    let lim = limit.unwrap_or(DEFAULT_LIMIT) as i64;

    // Build the SQL fragment for the SK predicate, the parameter types,
    // and the bound parameters in lockstep. `$1` is always the PK; `$N`
    // for the SK operands; `${N+1}` is the LIMIT. Doing this in one pass
    // keeps the param numbering stable.
    //
    // We bind LIMIT through a parameter (not a literal in the SQL) so
    // tokio-postgres can cache the prepared statement across distinct
    // limit values.
    enum SkBuild<'a> {
        None,
        Cmp {
            sk_col: String,
            op: &'static str,
            sk_cast: &'static str,
            sk_type: Type,
            value: Bound<'a>,
        },
        Between {
            sk_col: String,
            sk_cast: &'static str,
            sk_type: Type,
            lo: Bound<'a>,
            hi: Bound<'a>,
        },
        BeginsWith {
            sk_col: String,
            prefix: &'a str,
        },
    }

    let sk_build = match (shape.sk_col, shape.sk_type, sk_condition) {
        (_, _, None) => SkBuild::None,
        (Some(sk_col_name), Some(sk_type), Some(cond)) => {
            let sk_col = quote_ident(sk_col_name);
            let sk_cast = cast_for_keytype(sk_type);
            let sk_pg = pg_type_for_keytype(sk_type);
            match cond {
                SkCondition::Eq(v) => SkBuild::Cmp {
                    sk_col,
                    op: "=",
                    sk_cast,
                    sk_type: sk_pg,
                    value: Bound(v),
                },
                SkCondition::Lt(v) => SkBuild::Cmp {
                    sk_col,
                    op: "<",
                    sk_cast,
                    sk_type: sk_pg,
                    value: Bound(v),
                },
                SkCondition::Le(v) => SkBuild::Cmp {
                    sk_col,
                    op: "<=",
                    sk_cast,
                    sk_type: sk_pg,
                    value: Bound(v),
                },
                SkCondition::Gt(v) => SkBuild::Cmp {
                    sk_col,
                    op: ">",
                    sk_cast,
                    sk_type: sk_pg,
                    value: Bound(v),
                },
                SkCondition::Ge(v) => SkBuild::Cmp {
                    sk_col,
                    op: ">=",
                    sk_cast,
                    sk_type: sk_pg,
                    value: Bound(v),
                },
                SkCondition::Between(lo, hi) => SkBuild::Between {
                    sk_col,
                    sk_cast,
                    sk_type: sk_pg,
                    lo: Bound(lo),
                    hi: Bound(hi),
                },
                SkCondition::BeginsWithS(prefix) => SkBuild::BeginsWith {
                    sk_col,
                    prefix: prefix.as_str(),
                },
            }
        }
        // The two impossible cases (sk_condition Some on hash-only, or
        // sk_col Some without sk_type) are caught above / unreachable.
        (None, _, Some(_)) => unreachable!("sk_condition without sk_col"),
        (Some(_), None, _) => unreachable!(
            "TableShape `{}`: sk_col without sk_type",
            shape.table
        ),
    };

    // Assemble SQL, parameter types, and LIKE-escaped prefix (if any).
    // The LIKE branch escapes `\`, `%`, `_` in the prefix so the user-
    // supplied string is matched literally (modulo the trailing `%`).
    let (sql, mut types): (String, Vec<Type>) = match &sk_build {
        SkBuild::None => (
            format!(
                "SELECT {jsonb_col} FROM {table} \
                 WHERE {pk_col} = $1{pk_cast}{order_by} LIMIT $2"
            ),
            vec![pk_pg, Type::INT8],
        ),
        SkBuild::Cmp {
            sk_col,
            op,
            sk_cast,
            sk_type,
            ..
        } => (
            format!(
                "SELECT {jsonb_col} FROM {table} \
                 WHERE {pk_col} = $1{pk_cast} AND {sk_col} {op} $2{sk_cast}{order_by} LIMIT $3"
            ),
            vec![pk_pg, sk_type.clone(), Type::INT8],
        ),
        SkBuild::Between {
            sk_col,
            sk_cast,
            sk_type,
            ..
        } => (
            format!(
                "SELECT {jsonb_col} FROM {table} \
                 WHERE {pk_col} = $1{pk_cast} \
                   AND {sk_col} BETWEEN $2{sk_cast} AND $3{sk_cast}{order_by} LIMIT $4"
            ),
            vec![pk_pg, sk_type.clone(), sk_type.clone(), Type::INT8],
        ),
        SkBuild::BeginsWith { sk_col, .. } => (
            // ESCAPE '\' lets us use `\` to neutralize wildcard chars in
            // the prefix. We pass the already-escaped string + a literal
            // `%` suffix as a single TEXT parameter.
            format!(
                "SELECT {jsonb_col} FROM {table} \
                 WHERE {pk_col} = $1{pk_cast} AND {sk_col} LIKE $2 ESCAPE '\\'{order_by} LIMIT $3"
            ),
            vec![pk_pg, Type::TEXT, Type::INT8],
        ),
    };
    // ensure pk_pg is unused warning quieted — already used in types.
    let _ = &mut types;

    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql, &types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let pk_bound = Bound(pk);
    let like_pattern: Option<String> = match &sk_build {
        SkBuild::BeginsWith { prefix, .. } => Some(format!("{}%", escape_like(prefix))),
        _ => None,
    };

    let rows = match &sk_build {
        SkBuild::None => {
            client
                .query(&stmt, &[&pk_bound, &lim])
                .instrument(tracing::debug_span!("pg.query"))
                .await
        }
        SkBuild::Cmp { value, .. } => {
            client
                .query(&stmt, &[&pk_bound, value, &lim])
                .instrument(tracing::debug_span!("pg.query"))
                .await
        }
        SkBuild::Between { lo, hi, .. } => {
            client
                .query(&stmt, &[&pk_bound, lo, hi, &lim])
                .instrument(tracing::debug_span!("pg.query"))
                .await
        }
        SkBuild::BeginsWith { .. } => {
            let pat = like_pattern.as_deref().expect("BeginsWith built above");
            client
                .query(&stmt, &[&pk_bound, &pat, &lim])
                .instrument(tracing::debug_span!("pg.query"))
                .await
        }
    }
    .map_err(|e| map_pg_err(shape.table, e))?;

    let mut items: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
    for row in rows {
        let Json(v): Json<serde_json::Value> = row.get(0);
        items.push(v);
    }
    let scanned = items.len() as u32;
    Ok(QueryOutcome {
        count: scanned,
        scanned_count: scanned,
        items,
        last_evaluated_key: None,
    })
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
