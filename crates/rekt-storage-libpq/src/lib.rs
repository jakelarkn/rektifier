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
use rekt_storage::{Backend, BackendError, KeyValue, TableShape};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{IsNull, Json, ToSql, Type};
use tokio_postgres::NoTls;

pub struct PgBackend {
    pool: Pool,
}

impl PgBackend {
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
        Ok(Self { pool })
    }

    async fn client(&self) -> Result<deadpool_postgres::Object, BackendError> {
        self.pool
            .get()
            .await
            .map_err(|e| BackendError::Other(format!("pool get failed: {e}")))
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

/// SQL-level cast a parameter placeholder needs so PG accepts our text-formatted
/// bytes against a non-text column. `S` binds as text → matches `text` natively;
/// `B` binds as `&[u8]` → matches `bytea` natively; `N` binds as text but the
/// column is `numeric`, so we have to spell `$N::numeric` to coerce.
fn param_cast(kv: &KeyValue) -> &'static str {
    match kv {
        KeyValue::S(_) | KeyValue::B(_) => "",
        KeyValue::N(_) => "::numeric",
    }
}

/// PG type to declare for a `prepare_typed` slot bound from a `KeyValue`.
/// We force `N` to bind as TEXT (paired with a `::numeric` SQL cast) instead
/// of letting PG infer NUMERIC, because the text→numeric path is much simpler
/// than implementing PG's numeric binary wire format in Rust.
fn param_pg_type(kv: &KeyValue) -> Type {
    match kv {
        KeyValue::S(_) | KeyValue::N(_) => Type::TEXT,
        KeyValue::B(_) => Type::BYTEA,
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
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError> {
        let client = self.client().await?;

        let table = quote_ident(shape.table);
        let pk_col = quote_ident(shape.pk_col);
        let jsonb_col = quote_ident(shape.jsonb_col);

        // Writes never bind key values. PG derives the generated columns from
        // the JSONB on INSERT; the ON CONFLICT target is just column names.
        let sql = match shape.sk_col {
            None => format!(
                "INSERT INTO {table} ({jsonb_col}) VALUES ($1) \
                 ON CONFLICT ({pk_col}) \
                 DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
            ),
            Some(sk_col_name) => {
                let sk_col = quote_ident(sk_col_name);
                format!(
                    "INSERT INTO {table} ({jsonb_col}) VALUES ($1) \
                     ON CONFLICT ({pk_col}, {sk_col}) \
                     DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
                )
            }
        };

        let stmt = client
            .prepare_typed_cached(&sql, &[Type::JSONB])
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        client
            .execute(&stmt, &[&Json(item)])
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        Ok(())
    }

    async fn get_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        check_sk_shape(shape, sk)?;
        let client = self.client().await?;

        let table = quote_ident(shape.table);
        let pk_col = quote_ident(shape.pk_col);
        let jsonb_col = quote_ident(shape.jsonb_col);

        let pk_bound = Bound(pk);
        let pk_cast = param_cast(pk);
        let pk_pg = param_pg_type(pk);

        let query_result = match (shape.sk_col, sk) {
            (None, _) => {
                let sql = format!("SELECT {jsonb_col} FROM {table} WHERE {pk_col} = $1{pk_cast}");
                let stmt = client
                    .prepare_typed_cached(&sql, &[pk_pg])
                    .await
                    .map_err(|e| map_pg_err(shape.table, e))?;
                client.query_opt(&stmt, &[&pk_bound]).await
            }
            (Some(sk_col_name), Some(sk_val)) => {
                let sk_col = quote_ident(sk_col_name);
                let sk_bound = Bound(sk_val);
                let sk_cast = param_cast(sk_val);
                let sk_pg = param_pg_type(sk_val);
                let sql = format!(
                    "SELECT {jsonb_col} FROM {table} \
                     WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast}"
                );
                let stmt = client
                    .prepare_typed_cached(&sql, &[pk_pg, sk_pg])
                    .await
                    .map_err(|e| map_pg_err(shape.table, e))?;
                client.query_opt(&stmt, &[&pk_bound, &sk_bound]).await
            }
            (Some(_), None) => unreachable!("rejected by check_sk_shape above"),
        };

        let row = query_result.map_err(|e| map_pg_err(shape.table, e))?;
        Ok(row.map(|r| {
            let Json(v): Json<serde_json::Value> = r.get(0);
            v
        }))
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
            sk_col: None,
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
            sk_col: Some("ts"),
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
            sk_col: None,
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
            sk_col: None,
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
    async fn missing_table_maps_to_table_not_found() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let shape = TableShape {
            table: "rekt_does_not_exist_anywhere",
            pk_col: "id",
            sk_col: None,
            jsonb_col: "data",
        };
        let err = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::TableNotFound { .. }));
    }
}
