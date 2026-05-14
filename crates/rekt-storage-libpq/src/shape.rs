//! `ShapeSql`: per-table SQL templates + parameter type vectors built
//! once per distinct [`TableShape`] and cached on `PgBackend`. The shape
//! covers the always-the-same statements (`put_sql`, `get_sql`,
//! `delete_sql`); shape-variant SQL (UpdateItem with a per-call number
//! of SET/REMOVE clauses, conditional Put/Delete RMW prep) lives in
//! [`crate::update`].

use crate::types::{cast_for_keytype, pg_type_for_keytype, quote_ident};
use crate::PgBackend;
use rekt_storage::TableShape;
use std::sync::Arc;
use tokio_postgres::types::Type;

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
        let jsonb_col = quote_ident(shape.jsonb_col);
        let pk_cast = cast_for_keytype(shape.pk_type);
        let pk_pg = pg_type_for_keytype(shape.pk_type);

        match (shape.sk_col, shape.sk_type) {
            (None, _) => Self {
                // CTE-prefix snapshots the pre-update row at the same MVCC
                // point as the upsert; RETURNING surfaces both the new row
                // and the old (NULL if it didn't exist). Param order:
                // $1 = pk, $2 = jsonb item.
                put_sql: format!(
                    "WITH prev AS (SELECT {jsonb_col} AS old_data FROM {table} \
                                   WHERE {pk_col} = $1{pk_cast}) \
                     INSERT INTO {table} ({jsonb_col}) VALUES ($2) \
                     ON CONFLICT ({pk_col}) \
                     DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col} \
                     RETURNING (SELECT old_data FROM prev) AS old_data"
                ),
                get_sql: format!("SELECT {jsonb_col} FROM {table} WHERE {pk_col} = $1{pk_cast}"),
                // RETURNING gives us the deleted row's jsonb for ALL_OLD;
                // 0 rows when nothing matched (Ok(None)).
                delete_sql: format!(
                    "DELETE FROM {table} WHERE {pk_col} = $1{pk_cast} RETURNING {jsonb_col}"
                ),
                put_types: vec![pk_pg.clone(), Type::JSONB],
                key_types: vec![pk_pg],
            },
            (Some(sk_col_name), Some(sk_type)) => {
                let sk_col = quote_ident(sk_col_name);
                let sk_cast = cast_for_keytype(sk_type);
                let sk_pg = pg_type_for_keytype(sk_type);
                Self {
                    // $1 = pk, $2 = sk, $3 = jsonb item.
                    put_sql: format!(
                        "WITH prev AS (SELECT {jsonb_col} AS old_data FROM {table} \
                                       WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast}) \
                         INSERT INTO {table} ({jsonb_col}) VALUES ($3) \
                         ON CONFLICT ({pk_col}, {sk_col}) \
                         DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col} \
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

impl PgBackend {
    /// Get or lazily-build the cached SQL for `shape`. Read-locks for the
    /// common (hit) case; takes the write lock only on first touch per table.
    pub(crate) fn shape_sql(&self, shape: &TableShape<'_>) -> Arc<ShapeSql> {
        if let Some(s) = self.sql_cache.read().unwrap().get(shape.table) {
            return Arc::clone(s);
        }
        let built = Arc::new(ShapeSql::build(shape));
        let mut w = self.sql_cache.write().unwrap();
        // Double-check: another thread may have inserted while we were
        // acquiring the write lock.
        Arc::clone(
            w.entry(shape.table.to_string())
                .or_insert_with(|| built.clone()),
        )
    }
}
