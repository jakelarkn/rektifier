//! `tokio-postgres` + `deadpool-postgres` backed implementation of [`Backend`].
//!
//! Each rektifier-side table `<name>` maps to a Postgres table `ddb_<name>`.
//! Names are validated against DynamoDB's character class (`[A-Za-z0-9_.-]{3,255}`)
//! before interpolation — Postgres does not bind identifiers as parameters,
//! so validation is what keeps SQL injection off the table here.

use async_trait::async_trait;
use deadpool_postgres::{Manager, Pool};
use rekt_storage::{Backend, BackendError};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Json;
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

    /// Validate a DynamoDB table name (`[A-Za-z0-9_.-]{3,255}`). Returns the
    /// fully-qualified PG table name `ddb_<table>` ready to splice into SQL.
    fn pg_table(table: &str) -> Result<String, BackendError> {
        let valid_len = (3..=255).contains(&table.len());
        let valid_chars = table
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.');
        if !valid_len || !valid_chars {
            return Err(BackendError::InvalidTableName {
                name: table.to_string(),
            });
        }
        Ok(format!("ddb_{table}"))
    }

    async fn client(&self) -> Result<deadpool_postgres::Object, BackendError> {
        self.pool
            .get()
            .await
            .map_err(|e| BackendError::Other(format!("pool get failed: {e}")))
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
        table: &str,
        pk: &str,
        sk: Option<&str>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError> {
        let pg_table = Self::pg_table(table)?;
        let sk = sk.unwrap_or("");
        let client = self.client().await?;
        let sql = format!(
            "INSERT INTO {pg_table} (pk, sk, item) VALUES ($1, $2, $3) \
             ON CONFLICT (pk, sk) DO UPDATE SET item = EXCLUDED.item"
        );
        client
            .execute(&sql, &[&pk, &sk, &Json(item)])
            .await
            .map_err(|e| map_pg_err(table, e))?;
        Ok(())
    }

    async fn get_item_raw(
        &self,
        table: &str,
        pk: &str,
        sk: Option<&str>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        let pg_table = Self::pg_table(table)?;
        let sk = sk.unwrap_or("");
        let client = self.client().await?;
        let sql = format!("SELECT item FROM {pg_table} WHERE pk = $1 AND sk = $2");
        let row = client
            .query_opt(&sql, &[&pk, &sk])
            .await
            .map_err(|e| map_pg_err(table, e))?;
        Ok(row.map(|r| {
            let Json(v): Json<serde_json::Value> = r.get(0);
            v
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_table_accepts_valid_names() {
        assert_eq!(PgBackend::pg_table("users").unwrap(), "ddb_users");
        assert_eq!(PgBackend::pg_table("a.b-c_d").unwrap(), "ddb_a.b-c_d");
        assert_eq!(
            PgBackend::pg_table(&"x".repeat(255)).unwrap(),
            format!("ddb_{}", "x".repeat(255))
        );
    }

    #[test]
    fn pg_table_rejects_invalid_names() {
        for bad in [
            "",
            "ab",
            "x".repeat(256).as_str(),
            "has space",
            "drop;--",
            "a/b",
        ] {
            let err = PgBackend::pg_table(bad).unwrap_err();
            assert!(
                matches!(err, BackendError::InvalidTableName { .. }),
                "expected InvalidTableName for {bad:?}, got {err:?}"
            );
        }
    }

    fn database_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
    }

    /// End-to-end round trip against a running Postgres. Skipped by default;
    /// run with `cargo test -p rekt-storage-libpq -- --ignored` after `just up`.
    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn round_trip_against_real_postgres() {
        let backend = PgBackend::connect(&database_url()).unwrap();

        let table = "rekt_test_storage_roundtrip";
        let pg_table = format!("ddb_{table}");
        let client = backend.client().await.unwrap();

        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {pg_table};
                 CREATE TABLE {pg_table} (
                   pk   text  NOT NULL,
                   sk   text  NOT NULL DEFAULT '',
                   item jsonb NOT NULL,
                   PRIMARY KEY (pk, sk)
                 );"
            ))
            .await
            .unwrap();

        // Hash-only table style: sk = None.
        let item = serde_json::json!({"id":{"S":"u1"},"name":{"S":"alice"}});
        backend
            .put_item_raw(table, "u1", None, &item)
            .await
            .unwrap();

        let got = backend.get_item_raw(table, "u1", None).await.unwrap();
        assert_eq!(got, Some(item.clone()));

        // Upsert: same key, different value, should replace.
        let item2 = serde_json::json!({"id":{"S":"u1"},"name":{"S":"alice2"}});
        backend
            .put_item_raw(table, "u1", None, &item2)
            .await
            .unwrap();
        let got2 = backend.get_item_raw(table, "u1", None).await.unwrap();
        assert_eq!(got2, Some(item2));

        // Composite-key style: same pk, different sk, two distinct rows.
        let order_a = serde_json::json!({"order":"A"});
        let order_b = serde_json::json!({"order":"B"});
        backend
            .put_item_raw(table, "u2", Some("order#001"), &order_a)
            .await
            .unwrap();
        backend
            .put_item_raw(table, "u2", Some("order#002"), &order_b)
            .await
            .unwrap();
        assert_eq!(
            backend
                .get_item_raw(table, "u2", Some("order#001"))
                .await
                .unwrap(),
            Some(order_a)
        );
        assert_eq!(
            backend
                .get_item_raw(table, "u2", Some("order#002"))
                .await
                .unwrap(),
            Some(order_b)
        );

        // Missing pk -> Ok(None).
        let missing = backend.get_item_raw(table, "nope", None).await.unwrap();
        assert_eq!(missing, None);

        // Same pk but wrong sk -> Ok(None) (sk is part of the key).
        let wrong_sk = backend
            .get_item_raw(table, "u2", Some("missing"))
            .await
            .unwrap();
        assert_eq!(wrong_sk, None);

        // Missing table -> TableNotFound.
        let err = backend
            .get_item_raw("rekt_test_does_not_exist", "u1", None)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::TableNotFound { .. }));

        client
            .batch_execute(&format!("DROP TABLE {pg_table};"))
            .await
            .unwrap();
    }
}
