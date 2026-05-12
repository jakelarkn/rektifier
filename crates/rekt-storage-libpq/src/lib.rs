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
            // `S` -> text. `N` -> numeric on the PG side: tokio-postgres binds
            // the &str as text and PG implicitly casts to numeric via the
            // column's declared type. (DDB N is sent over the wire as a
            // string already, so we pass it through verbatim.)
            KeyValue::S(s) | KeyValue::N(s) => s.to_sql(ty, out),
            KeyValue::B(b) => {
                let slice: &[u8] = b.as_ref();
                slice.to_sql(ty, out)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        // We trust upstream schema validation (translator + boot-time PG
        // check) to ensure the column type matches the value variant; if
        // it doesn't, PG will reject at execute time with a clear error.
        true
    }

    tokio_postgres::types::to_sql_checked!();
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
    BackendError::Other(e.to_string())
}

#[async_trait]
impl Backend for PgBackend {
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError> {
        check_sk_shape(shape, sk)?;
        let client = self.client().await?;

        let table = quote_ident(shape.table);
        let pk_col = quote_ident(shape.pk_col);
        let jsonb_col = quote_ident(shape.jsonb_col);

        let item_json = Json(item);
        let pk_bound = Bound(pk);

        let exec_result = match (shape.sk_col, sk) {
            (None, _) => {
                let sql = format!(
                    "INSERT INTO {table} ({pk_col}, {jsonb_col}) \
                     VALUES ($1, $2) \
                     ON CONFLICT ({pk_col}) \
                     DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
                );
                client.execute(&sql, &[&pk_bound, &item_json]).await
            }
            (Some(sk_col_name), Some(sk_val)) => {
                let sk_col = quote_ident(sk_col_name);
                let sk_bound = Bound(sk_val);
                let sql = format!(
                    "INSERT INTO {table} ({pk_col}, {sk_col}, {jsonb_col}) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT ({pk_col}, {sk_col}) \
                     DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
                );
                client
                    .execute(&sql, &[&pk_bound, &sk_bound, &item_json])
                    .await
            }
            (Some(_), None) => unreachable!("rejected by check_sk_shape above"),
        };

        exec_result.map_err(|e| map_pg_err(shape.table, e))?;
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

        let query_result = match (shape.sk_col, sk) {
            (None, _) => {
                let sql = format!("SELECT {jsonb_col} FROM {table} WHERE {pk_col} = $1");
                client.query_opt(&sql, &[&pk_bound]).await
            }
            (Some(sk_col_name), Some(sk_val)) => {
                let sk_col = quote_ident(sk_col_name);
                let sk_bound = Bound(sk_val);
                let sql = format!(
                    "SELECT {jsonb_col} FROM {table} \
                     WHERE {pk_col} = $1 AND {sk_col} = $2"
                );
                client.query_opt(&sql, &[&pk_bound, &sk_bound]).await
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
    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn round_trip_hash_only_string_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_hash_s;
                 CREATE TABLE rekt_t_hash_s (
                   id   text  NOT NULL PRIMARY KEY,
                   data jsonb NOT NULL
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
        backend
            .put_item_raw(&shape, &KeyValue::S("u1".into()), None, &item)
            .await
            .unwrap();
        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(item));

        // Missing pk -> Ok(None).
        let missing = backend
            .get_item_raw(&shape, &KeyValue::S("nope".into()), None)
            .await
            .unwrap();
        assert_eq!(missing, None);

        // Sort key passed but shape is hash-only -> UnexpectedSortKey.
        let err = backend
            .put_item_raw(
                &shape,
                &KeyValue::S("u1".into()),
                Some(&KeyValue::S("oops".into())),
                &serde_json::json!({}),
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
                   device_id text    NOT NULL,
                   ts        numeric NOT NULL,
                   doc       jsonb   NOT NULL,
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
        let item1 = serde_json::json!({"value": 1});
        let item2 = serde_json::json!({"value": 2});
        backend
            .put_item_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                Some(&KeyValue::N("1000".into())),
                &item1,
            )
            .await
            .unwrap();
        backend
            .put_item_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                Some(&KeyValue::N(big_ts.into())),
                &item2,
            )
            .await
            .unwrap();

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

        // Missing sk on composite shape -> MissingSortKey.
        let err = backend
            .put_item_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                None,
                &serde_json::json!({}),
            )
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
                   hash bytea NOT NULL PRIMARY KEY,
                   meta jsonb NOT NULL
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
        let pk_bytes = Bytes::from_static(b"\x00\x01\x02\xff");
        let item = serde_json::json!({"size": 4});
        backend
            .put_item_raw(&shape, &KeyValue::B(pk_bytes.clone()), None, &item)
            .await
            .unwrap();
        let got = backend
            .get_item_raw(&shape, &KeyValue::B(pk_bytes), None)
            .await
            .unwrap();
        assert_eq!(got, Some(item));

        client
            .batch_execute("DROP TABLE rekt_t_hash_b;")
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
