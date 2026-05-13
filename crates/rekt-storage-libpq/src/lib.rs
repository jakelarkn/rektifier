//! `tokio-postgres` + `deadpool-postgres` backed implementation of [`Backend`].
//!
//! The operator owns the PG schema. Rektifier reads `TableShape` (column
//! names) and `KeyValue` (typed key data) on every call, builds SQL with
//! the right column identifiers spliced in, and binds parameters by their
//! Rust type so PG receives `text` for `S`, `numeric` for `N` (parsed from
//! the verbatim DDB number string), and `bytea` for `B`.
//!
//! Column identifiers are quoted with `"..."` and any embedded quotes are
//! doubled — Postgres can't bind identifiers as parameters, so quoting is
//! what keeps SQL injection off the table.

use async_trait::async_trait;
use bytes::BytesMut;
use deadpool_postgres::{Manager, Pool};
use rekt_storage::{Backend, BackendError, KeyType, KeyValue, TableShape};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{IsNull, Json, ToSql, Type};
use tokio_postgres::NoTls;
use tracing::Instrument;

pub struct PgBackend {
    pool: Pool,
    /// Per-table cache of pre-formatted SQL and prepared-statement type
    /// vectors. Populated lazily on first touch from a `TableShape`. Keyed
    /// by `shape.table` (the PG table name).
    sql_cache: RwLock<HashMap<String, Arc<ShapeSql>>>,
}

/// All the per-shape SQL state that used to be rebuilt every call.
/// Computed once per distinct `TableShape`, then handed out as an `Arc`.
struct ShapeSql {
    put_sql: String,
    get_sql: String,
    delete_sql: String,
    /// Parameter types for `put_item_raw`'s `prepare_typed_cached`: always
    /// `[JSONB]` since writes only bind the jsonb column.
    put_types: Vec<Type>,
    /// Parameter types for `get_item_raw` and `delete_item_raw`: `[pk]` for
    /// hash-only, `[pk, sk]` for composite. Each is `TEXT` (for `S`/`N` keys;
    /// the SQL contains the `::numeric` cast) or `BYTEA` (for `B`).
    key_types: Vec<Type>,
}

impl PgBackend {
    /// Wrap an existing pool. Prefer this when the caller wants to share a
    /// `Pool` with other components (e.g. `rekt-meta::verify`).
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            sql_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn connect(connection_string: &str) -> Result<Self, BackendError> {
        let pg_config: tokio_postgres::Config =
            connection_string
                .parse()
                .map_err(|e: tokio_postgres::Error| {
                    BackendError::Other(format!("invalid connection string: {e}"))
                })?;
        let manager = Manager::new(pg_config, NoTls);
        let pool = Pool::builder(manager)
            .max_size(16)
            .build()
            .map_err(|e| BackendError::Other(format!("pool build failed: {e}")))?;
        Ok(Self::new(pool))
    }

    async fn client(&self) -> Result<deadpool_postgres::Object, BackendError> {
        self.pool
            .get()
            .instrument(tracing::debug_span!("pg.pool_get"))
            .await
            .map_err(|e| BackendError::Other(format!("pool get failed: {e}")))
    }

    /// Get or lazily-build the cached SQL for `shape`. Read-locks for the
    /// common (hit) case; takes the write lock only on first touch per table.
    fn shape_sql(&self, shape: &TableShape<'_>) -> Arc<ShapeSql> {
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

/// Quote a SQL identifier per Postgres rules: wrap in `"..."` and double
/// any embedded `"`. Combined with the operator-owned schema (rektifier
/// never invents identifier names), this is sufficient to block injection.
fn quote_ident(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('"');
    for c in ident.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// Newtype that lets us pass a `&KeyValue` straight into tokio-postgres'
/// parameter slot. We can't `impl ToSql for KeyValue` directly — orphan
/// rules — and a `&dyn ToSql` returning helper has lifetime trouble for
/// the `B` variant, so a tiny wrapper is the cleanest path.
#[derive(Debug)]
struct Bound<'a>(&'a KeyValue);

impl ToSql for Bound<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.0 {
            // Both `S` and `N` write UTF-8 text bytes. For `N`, the parameter
            // is bound as TEXT at the prepare-typed layer and the SQL contains
            // an explicit `$N::numeric` cast; PG converts text→numeric at the
            // SQL level. DDB N is already a string on the wire, so we pass it
            // through verbatim.
            KeyValue::S(s) | KeyValue::N(s) => s.to_sql(ty, out),
            KeyValue::B(b) => {
                let slice: &[u8] = b.as_ref();
                slice.to_sql(ty, out)
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        // We bind the underlying value as either text (for `S` / `N`) or as a
        // byte slice (for `B`). For non-text columns (`numeric`), we force the
        // parameter's PG type to TEXT via `prepare_typed` at the call site and
        // apply an explicit `$N::numeric` cast in the SQL — that way PG never
        // asks us to bind into a NUMERIC parameter directly and we don't have
        // to encode PG's numeric binary wire format ourselves.
        matches!(
            *ty,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN | Type::BYTEA
        )
    }

    tokio_postgres::types::to_sql_checked!();
}

/// SQL-level cast suffix for a parameter declared as `KeyType`. `S` and `B`
/// match their target column types natively; `N` is bound as text and cast
/// to numeric at the SQL level.
fn cast_for_keytype(t: KeyType) -> &'static str {
    match t {
        KeyType::S | KeyType::B => "",
        KeyType::N => "::numeric",
    }
}

/// PG `prepare_typed` type for a declared `KeyType`. `S`/`N` both bind as
/// TEXT (the SQL contains the `::numeric` cast for `N`); `B` binds as BYTEA.
fn pg_type_for_keytype(t: KeyType) -> Type {
    match t {
        KeyType::S | KeyType::N => Type::TEXT,
        KeyType::B => Type::BYTEA,
    }
}

impl ShapeSql {
    fn build(shape: &TableShape<'_>) -> Self {
        let table = quote_ident(shape.table);
        let pk_col = quote_ident(shape.pk_col);
        let jsonb_col = quote_ident(shape.jsonb_col);
        let pk_cast = cast_for_keytype(shape.pk_type);
        let pk_pg = pg_type_for_keytype(shape.pk_type);

        match (shape.sk_col, shape.sk_type) {
            (None, _) => Self {
                put_sql: format!(
                    "INSERT INTO {table} ({jsonb_col}) VALUES ($1) \
                     ON CONFLICT ({pk_col}) \
                     DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
                ),
                get_sql: format!("SELECT {jsonb_col} FROM {table} WHERE {pk_col} = $1{pk_cast}"),
                delete_sql: format!("DELETE FROM {table} WHERE {pk_col} = $1{pk_cast}"),
                put_types: vec![Type::JSONB],
                key_types: vec![pk_pg],
            },
            (Some(sk_col_name), Some(sk_type)) => {
                let sk_col = quote_ident(sk_col_name);
                let sk_cast = cast_for_keytype(sk_type);
                let sk_pg = pg_type_for_keytype(sk_type);
                Self {
                    put_sql: format!(
                        "INSERT INTO {table} ({jsonb_col}) VALUES ($1) \
                         ON CONFLICT ({pk_col}, {sk_col}) \
                         DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
                    ),
                    get_sql: format!(
                        "SELECT {jsonb_col} FROM {table} \
                         WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast}"
                    ),
                    delete_sql: format!(
                        "DELETE FROM {table} \
                         WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast}"
                    ),
                    put_types: vec![Type::JSONB],
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

/// Common precondition: caller passed `sk` iff shape has `sk_col`.
fn check_sk_shape(shape: &TableShape<'_>, sk: Option<&KeyValue>) -> Result<(), BackendError> {
    match (shape.sk_col, sk) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => Err(BackendError::MissingSortKey {
            name: shape.table.to_string(),
        }),
        (None, Some(_)) => Err(BackendError::UnexpectedSortKey {
            name: shape.table.to_string(),
        }),
    }
}

fn map_pg_err(table: &str, e: tokio_postgres::Error) -> BackendError {
    if e.code() == Some(&SqlState::UNDEFINED_TABLE) {
        return BackendError::TableNotFound {
            name: table.to_string(),
        };
    }
    // `e.to_string()` collapses to "db error"; drill into the underlying
    // DbError for the actual PG message + SQLSTATE.
    let detail = match e.as_db_error() {
        Some(db) => format!("{} ({})", db.message(), db.code().code()),
        None => e.to_string(),
    };
    BackendError::Other(detail)
}

#[async_trait]
impl Backend for PgBackend {
    #[tracing::instrument(level = "debug", skip_all, name = "pg.put_item_raw", fields(table = %shape.table))]
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError> {
        let sql = self.shape_sql(shape);
        let client = self.client().await?;
        let stmt = client
            .prepare_typed_cached(&sql.put_sql, &sql.put_types)
            .instrument(tracing::debug_span!("pg.prepare"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        client
            .execute(&stmt, &[&Json(item)])
            .instrument(tracing::debug_span!("pg.execute"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, name = "pg.get_item_raw", fields(table = %shape.table))]
    async fn get_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        check_sk_shape(shape, sk)?;
        let sql = self.shape_sql(shape);
        let client = self.client().await?;
        let stmt = client
            .prepare_typed_cached(&sql.get_sql, &sql.key_types)
            .instrument(tracing::debug_span!("pg.prepare"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;

        let pk_bound = Bound(pk);
        let query_result = match sk {
            None => {
                client
                    .query_opt(&stmt, &[&pk_bound])
                    .instrument(tracing::debug_span!("pg.query"))
                    .await
            }
            Some(sk_val) => {
                let sk_bound = Bound(sk_val);
                client
                    .query_opt(&stmt, &[&pk_bound, &sk_bound])
                    .instrument(tracing::debug_span!("pg.query"))
                    .await
            }
        };

        let row = query_result.map_err(|e| map_pg_err(shape.table, e))?;
        Ok(row.map(|r| {
            let Json(v): Json<serde_json::Value> = r.get(0);
            v
        }))
    }

    #[tracing::instrument(level = "debug", skip_all, name = "pg.delete_item_raw", fields(table = %shape.table))]
    async fn delete_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<(), BackendError> {
        check_sk_shape(shape, sk)?;
        let sql = self.shape_sql(shape);
        let client = self.client().await?;
        let stmt = client
            .prepare_typed_cached(&sql.delete_sql, &sql.key_types)
            .instrument(tracing::debug_span!("pg.prepare"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;

        let pk_bound = Bound(pk);
        let exec_result = match sk {
            None => {
                client
                    .execute(&stmt, &[&pk_bound])
                    .instrument(tracing::debug_span!("pg.execute"))
                    .await
            }
            Some(sk_val) => {
                let sk_bound = Bound(sk_val);
                client
                    .execute(&stmt, &[&pk_bound, &sk_bound])
                    .instrument(tracing::debug_span!("pg.execute"))
                    .await
            }
        };

        // DDB DeleteItem is idempotent — `Ok(())` whether 0 or 1 rows deleted.
        // Errors map normally (TableNotFound, etc.).
        exec_result.map_err(|e| map_pg_err(shape.table, e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn quote_ident_basic() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("user_id"), "\"user_id\"");
    }

    #[test]
    fn quote_ident_escapes_embedded_quote() {
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }

    fn database_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
    }

    /// End-to-end round trip against a running Postgres. Skipped by default;
    /// run with `cargo test -p rekt-storage-libpq -- --ignored` after `just up`.
    ///
    /// Schemas use `GENERATED ALWAYS AS ... STORED` columns derived from the
    /// JSONB — that's the contract rektifier expects from operator-owned tables.
    /// Writes bind only the JSONB; PG fills in the key columns.
    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn round_trip_hash_only_string_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_hash_s;
                 CREATE TABLE rekt_t_hash_s (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_hash_s",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let item = serde_json::json!({"id":{"S":"u1"},"name":{"S":"alice"}});
        backend.put_item_raw(&shape, &item).await.unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(item.clone()));

        // Upsert: same key, different value, should replace.
        let item2 = serde_json::json!({"id":{"S":"u1"},"name":{"S":"alice2"}});
        backend.put_item_raw(&shape, &item2).await.unwrap();
        let got2 = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got2, Some(item2));

        // Missing pk on read -> Ok(None).
        let missing = backend
            .get_item_raw(&shape, &KeyValue::S("nope".into()), None)
            .await
            .unwrap();
        assert_eq!(missing, None);

        // Sort key passed but shape is hash-only -> UnexpectedSortKey on read.
        let err = backend
            .get_item_raw(
                &shape,
                &KeyValue::S("u1".into()),
                Some(&KeyValue::S("oops".into())),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::UnexpectedSortKey { .. }));

        client
            .batch_execute("DROP TABLE rekt_t_hash_s;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn round_trip_composite_numeric_sk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_comp_sn;
                 CREATE TABLE rekt_t_comp_sn (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')           STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)       STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_comp_sn",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };

        // 38-digit precision number.
        let big_ts = "12345678901234567890.12345678901234567";
        let item1 =
            serde_json::json!({"device_id":{"S":"dev-1"},"ts":{"N":"1000"},"value":{"N":"1"}});
        let item2 =
            serde_json::json!({"device_id":{"S":"dev-1"},"ts":{"N":big_ts},"value":{"N":"2"}});
        backend.put_item_raw(&shape, &item1).await.unwrap();
        backend.put_item_raw(&shape, &item2).await.unwrap();

        assert_eq!(
            backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("dev-1".into()),
                    Some(&KeyValue::N("1000".into()))
                )
                .await
                .unwrap(),
            Some(item1)
        );
        assert_eq!(
            backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("dev-1".into()),
                    Some(&KeyValue::N(big_ts.into()))
                )
                .await
                .unwrap(),
            Some(item2)
        );

        // Missing sk on composite shape read -> MissingSortKey.
        let err = backend
            .get_item_raw(&shape, &KeyValue::S("dev-1".into()), None)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::MissingSortKey { .. }));

        client
            .batch_execute("DROP TABLE rekt_t_comp_sn;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn round_trip_binary_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_hash_b;
                 CREATE TABLE rekt_t_hash_b (
                   meta jsonb NOT NULL,
                   hash bytea GENERATED ALWAYS AS (decode(meta#>>'{hash,B}', 'base64')) STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_hash_b",
            pk_col: "hash",
            pk_type: KeyType::B,
            sk_col: None,
            sk_type: None,
            jsonb_col: "meta",
        };
        // base64 of "\x00\x01\x02\xff" is "AAEC/w==".
        let item = serde_json::json!({"hash":{"B":"AAEC/w=="},"size":{"N":"4"}});
        backend.put_item_raw(&shape, &item).await.unwrap();

        let got = backend
            .get_item_raw(
                &shape,
                &KeyValue::B(Bytes::from_static(b"\x00\x01\x02\xff")),
                None,
            )
            .await
            .unwrap();
        assert_eq!(got, Some(item));

        client
            .batch_execute("DROP TABLE rekt_t_hash_b;")
            .await
            .unwrap();
    }

    /// PG generated columns + NOT NULL primary key act as the backstop when a
    /// caller hands us an item whose JSONB doesn't yield the expected key.
    /// (Translator-level validation will catch this first in production, but
    /// the backstop matters for correctness.)
    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn put_with_missing_key_attr_in_jsonb_is_rejected() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_pk_required;
                 CREATE TABLE rekt_t_pk_required (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_pk_required",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        // JSONB has no `id` attr — generated column yields NULL — PRIMARY KEY rejects.
        let bad_item = serde_json::json!({"name":{"S":"alice"}});
        let err = backend.put_item_raw(&shape, &bad_item).await.unwrap_err();
        // PG raises a not-null-violation; we map to Other for now (translator
        // should catch this case earlier in the real call path).
        assert!(matches!(err, BackendError::Other(_)));

        client
            .batch_execute("DROP TABLE rekt_t_pk_required;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn delete_round_trip_hash_only() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_delete_s;
                 CREATE TABLE rekt_t_delete_s (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_delete_s",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let item = serde_json::json!({"id":{"S":"alice"},"name":{"S":"A"}});
        backend.put_item_raw(&shape, &item).await.unwrap();
        // Sanity: item is present.
        assert_eq!(
            backend
                .get_item_raw(&shape, &KeyValue::S("alice".into()), None)
                .await
                .unwrap(),
            Some(item)
        );

        // Delete: item gone.
        backend
            .delete_item_raw(&shape, &KeyValue::S("alice".into()), None)
            .await
            .unwrap();
        assert_eq!(
            backend
                .get_item_raw(&shape, &KeyValue::S("alice".into()), None)
                .await
                .unwrap(),
            None
        );

        // Idempotent: deleting again is Ok.
        backend
            .delete_item_raw(&shape, &KeyValue::S("alice".into()), None)
            .await
            .unwrap();

        client
            .batch_execute("DROP TABLE rekt_t_delete_s;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn delete_round_trip_composite() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_delete_sn;
                 CREATE TABLE rekt_t_delete_sn (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')           STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)       STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_delete_sn",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        let item_a = serde_json::json!({"device_id":{"S":"d1"},"ts":{"N":"100"},"v":{"N":"1"}});
        let item_b = serde_json::json!({"device_id":{"S":"d1"},"ts":{"N":"200"},"v":{"N":"2"}});
        backend.put_item_raw(&shape, &item_a).await.unwrap();
        backend.put_item_raw(&shape, &item_b).await.unwrap();

        // Delete only the (d1, 100) row; (d1, 200) survives.
        backend
            .delete_item_raw(
                &shape,
                &KeyValue::S("d1".into()),
                Some(&KeyValue::N("100".into())),
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("d1".into()),
                    Some(&KeyValue::N("100".into()))
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("d1".into()),
                    Some(&KeyValue::N("200".into()))
                )
                .await
                .unwrap(),
            Some(item_b)
        );

        client
            .batch_execute("DROP TABLE rekt_t_delete_sn;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn missing_table_maps_to_table_not_found() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let shape = TableShape {
            table: "rekt_does_not_exist_anywhere",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let err = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::TableNotFound { .. }));
    }
}
