//! `ShapeSql`: per-table SQL templates + parameter type vectors built
//! once per distinct [`TableShape`] and cached on `PgBackend`. The shape
//! covers the always-the-same statements (`put_sql`, `get_sql`,
//! `delete_sql`); shape-variant SQL (UpdateItem with a per-call number
//! of SET/REMOVE clauses, conditional Put/Delete RMW prep) lives in
//! [`crate::update`].

use crate::types::{cast_for_keytype, pg_type_for_keytype, quote_ident};
use crate::PgBackend;
use rekt_storage::{DualWriteCol, KeyType, TableShape};
use std::sync::Arc;
use tokio_postgres::types::Type;

/// PLAN-9 G3. Build the SQL fragment that extracts a DualWrite GSI
/// column's value from the JSONB parameter `$N`. PG performs the
/// extraction inside the same statement as the JSONB write, so the
/// column and JSONB stay in sync atomically without rektifier needing
/// to compute the value Rust-side. Equivalent semantics to a GENERATED
/// expression, but living in the INSERT/UPDATE rather than in the
/// column definition (so ADD COLUMN stays catalog-only / online).
pub(crate) fn dual_write_extract_expr(item_param: usize, col: &DualWriteCol<'_>) -> String {
    let attr = col.attr;
    match col.key_type {
        KeyType::S => format!("(${item_param}::jsonb #>> '{{{attr},S}}')"),
        KeyType::N => format!("(${item_param}::jsonb #>> '{{{attr},N}}')::numeric"),
        // DDB encodes B as base64 inside the JSONB representation.
        // The decode() returns bytea, which matches the column type.
        KeyType::B => format!("decode((${item_param}::jsonb #>> '{{{attr},B}}'), 'base64')"),
    }
}

/// SET-clause fragment for one DualWrite column in an UPDATE …
/// EXCLUDED-style upsert path. The new row's JSONB is read from the
/// EXCLUDED pseudo-table; the column is recomputed from it.
pub(crate) fn dual_write_set_excluded(col: &DualWriteCol<'_>, jsonb_col: &str) -> String {
    let pg_col = quote_ident(col.col);
    let extract = match col.key_type {
        KeyType::S => format!("EXCLUDED.{jc} #>> '{{{},S}}'", col.attr, jc = jsonb_col),
        KeyType::N => format!(
            "(EXCLUDED.{jc} #>> '{{{},N}}')::numeric",
            col.attr,
            jc = jsonb_col
        ),
        KeyType::B => format!(
            "decode(EXCLUDED.{jc} #>> '{{{},B}}', 'base64')",
            col.attr,
            jc = jsonb_col
        ),
    };
    format!("{pg_col} = {extract}")
}

/// SET-clause fragment for one DualWrite column in an UPDATE path where
/// the new JSONB is bound as `$N::jsonb`. Used by the RMW slow path
/// and update_with_simple_condition_raw.
pub(crate) fn dual_write_set_from_param(
    param: usize,
    col: &DualWriteCol<'_>,
) -> String {
    let pg_col = quote_ident(col.col);
    let extract = dual_write_extract_expr(param, col);
    format!("{pg_col} = {extract}")
}

/// SET-clause fragment that recomputes the DualWrite column from a SQL
/// expression that produces the *new* JSONB. Used by `build_update_sql`
/// (Phase 3a/4d simple-update path) where the new JSONB is computed
/// via nested `jsonb_set(...)` calls and is the same fragment for
/// `data = expr`.
pub(crate) fn dual_write_set_from_expr(
    expr_for_new_jsonb: &str,
    col: &DualWriteCol<'_>,
) -> String {
    let pg_col = quote_ident(col.col);
    let attr = col.attr;
    let extract = match col.key_type {
        KeyType::S => format!("({expr_for_new_jsonb}) #>> '{{{attr},S}}'"),
        KeyType::N => format!("(({expr_for_new_jsonb}) #>> '{{{attr},N}}')::numeric"),
        KeyType::B => format!(
            "decode(({expr_for_new_jsonb}) #>> '{{{attr},B}}', 'base64')"
        ),
    };
    format!("{pg_col} = {extract}")
}

/// All the per-shape SQL state that used to be rebuilt every call.
/// Computed once per distinct `TableShape`, then handed out as an `Arc`.
pub(crate) struct ShapeSql {
    pub(crate) put_sql: String,
    pub(crate) get_sql: String,
    pub(crate) delete_sql: String,
    /// Parameter types for `put_item_raw`'s `prepare_typed_cached`:
    /// the key types first (bound to the pre-image CTE) followed by
    /// `JSONB` for the row being upserted.
    pub(crate) put_types: Vec<Type>,
    /// Parameter types for `get_item_raw` and `delete_item_raw`: `[pk]` for
    /// hash-only, `[pk, sk]` for composite. Each is `TEXT` (for `S`/`N` keys;
    /// the SQL contains the `::numeric` cast) or `BYTEA` (for `B`).
    pub(crate) key_types: Vec<Type>,
}

impl ShapeSql {
    pub(crate) fn build(shape: &TableShape<'_>) -> Self {
        let table = quote_ident(shape.table);
        let pk_col = quote_ident(shape.pk_col);
        let jsonb_col_raw = shape.jsonb_col;
        let jsonb_col = quote_ident(shape.jsonb_col);
        let pk_cast = cast_for_keytype(shape.pk_type);
        let pk_pg = pg_type_for_keytype(shape.pk_type);

        // PLAN-9 G3. DualWrite-mode GSI columns: for each, build the
        // (column, extraction-expr) pair the INSERT widens by, and the
        // matching ON CONFLICT DO UPDATE SET fragment.
        let dwc = shape.dual_write_cols;
        let dual_write_insert_cols: String = dwc
            .iter()
            .map(|c| format!(", {}", quote_ident(c.col)))
            .collect();
        let dual_write_set_cols: String = dwc
            .iter()
            .map(|c| format!(", {}", dual_write_set_excluded(c, jsonb_col_raw)))
            .collect();

        match (shape.sk_col, shape.sk_type) {
            (None, _) => {
                // PARAM ORDER: $1 = pk, $2 = jsonb item (also referenced
                // by DualWrite extraction exprs via $2).
                let dual_write_values: String = dwc
                    .iter()
                    .map(|c| format!(", {}", dual_write_extract_expr(2, c)))
                    .collect();
                Self {
                    put_sql: format!(
                        "WITH prev AS (SELECT {jsonb_col} AS old_data FROM {table} \
                                       WHERE {pk_col} = $1{pk_cast}) \
                         INSERT INTO {table} ({jsonb_col}{dual_write_insert_cols}) \
                         VALUES ($2{dual_write_values}) \
                         ON CONFLICT ({pk_col}) \
                         DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}\
                             {dual_write_set_cols} \
                         RETURNING (SELECT old_data FROM prev) AS old_data"
                    ),
                    get_sql: format!(
                        "SELECT {jsonb_col} FROM {table} WHERE {pk_col} = $1{pk_cast}"
                    ),
                    delete_sql: format!(
                        "DELETE FROM {table} WHERE {pk_col} = $1{pk_cast} RETURNING {jsonb_col}"
                    ),
                    put_types: vec![pk_pg.clone(), Type::JSONB],
                    key_types: vec![pk_pg],
                }
            }
            (Some(sk_col_name), Some(sk_type)) => {
                let sk_col = quote_ident(sk_col_name);
                let sk_cast = cast_for_keytype(sk_type);
                let sk_pg = pg_type_for_keytype(sk_type);
                let dual_write_values: String = dwc
                    .iter()
                    .map(|c| format!(", {}", dual_write_extract_expr(3, c)))
                    .collect();
                Self {
                    put_sql: format!(
                        "WITH prev AS (SELECT {jsonb_col} AS old_data FROM {table} \
                                       WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast}) \
                         INSERT INTO {table} ({jsonb_col}{dual_write_insert_cols}) \
                         VALUES ($3{dual_write_values}) \
                         ON CONFLICT ({pk_col}, {sk_col}) \
                         DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}\
                             {dual_write_set_cols} \
                         RETURNING (SELECT old_data FROM prev) AS old_data"
                    ),
                    get_sql: format!(
                        "SELECT {jsonb_col} FROM {table} \
                         WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast}"
                    ),
                    delete_sql: format!(
                        "DELETE FROM {table} \
                         WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast} \
                         RETURNING {jsonb_col}"
                    ),
                    put_types: vec![pk_pg.clone(), sk_pg.clone(), Type::JSONB],
                    key_types: vec![pk_pg, sk_pg],
                }
            }
            (Some(_), None) => panic!(
                "TableShape `{}`: sk_col is Some but sk_type is None — invariant violation",
                shape.table
            ),
        }
    }
}

/// PLAN-9 G3. Cache key for `ShapeSql`: table name + a signature
/// covering the DualWrite GSI columns (which materially change the SQL
/// emitted). Generated-mode GSIs do not affect the SQL — PG owns the
/// column — so they're excluded from the signature.
fn shape_cache_key(shape: &TableShape<'_>) -> String {
    if shape.dual_write_cols.is_empty() {
        return shape.table.to_string();
    }
    let mut parts: Vec<String> = shape
        .dual_write_cols
        .iter()
        .map(|c| {
            let t = match c.key_type {
                rekt_storage::KeyType::S => 'S',
                rekt_storage::KeyType::N => 'N',
                rekt_storage::KeyType::B => 'B',
            };
            format!("{}:{}:{t}", c.col, c.attr)
        })
        .collect();
    parts.sort();
    format!("{}|dw={}", shape.table, parts.join(","))
}

impl PgBackend {
    /// Get or lazily-build the cached SQL for `shape`. Read-locks for the
    /// common (hit) case; takes the write lock only on first touch per table.
    ///
    /// Recovers transparently from `PoisonError` — the cache contents
    /// are append-only `Arc<ShapeSql>` values, so a poisoned lock can't
    /// have corrupted the data. Before Obs-5 we `.unwrap()`'d the
    /// guards, which meant a panic-while-holding-the-lock (anywhere
    /// inside `ShapeSql::build`, in theory) would permanently take
    /// down every subsequent request with a downstream panic. Now we
    /// just `into_inner()` the poison and proceed.
    pub(crate) fn shape_sql(&self, shape: &TableShape<'_>) -> Arc<ShapeSql> {
        let key = shape_cache_key(shape);
        {
            let r = self
                .sql_cache
                .read()
                .unwrap_or_else(|e| {
                    tracing::warn!("shape_sql cache read lock was poisoned; recovering");
                    e.into_inner()
                });
            if let Some(s) = r.get(&key) {
                return Arc::clone(s);
            }
        }
        let built = Arc::new(ShapeSql::build(shape));
        let mut w = self
            .sql_cache
            .write()
            .unwrap_or_else(|e| {
                tracing::warn!("shape_sql cache write lock was poisoned; recovering");
                e.into_inner()
            });
        // Double-check: another thread may have inserted while we were
        // acquiring the write lock.
        Arc::clone(w.entry(key).or_insert_with(|| built.clone()))
    }
}
