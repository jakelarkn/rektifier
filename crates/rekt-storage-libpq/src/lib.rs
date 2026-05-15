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
//!
//! ## Module layout
//!
//! `lib.rs` defines [`PgBackend`] and the `impl Backend for PgBackend`
//! trampoline; each method delegates to a `pub(crate)` function in one of
//! the topic modules below:
//!
//! - [`types`] — foundational helpers ([`types::Bound`],
//!   [`types::quote_ident`], `cast_for_keytype`, `map_pg_err`,
//!   `check_sk_shape`).
//! - [`shape`] — per-table cached SQL ([`shape::ShapeSql`] +
//!   [`PgBackend::shape_sql`]) for put / get / delete.
//! - [`condition_sql`] — Phase 4d ConditionExpression → SQL WHERE compiler
//!   ([`condition_sql::ParamBuilder`], `compile_condition`).
//! - [`update`] — UpdateItem SQL builders + the RMW prep
//!   ([`update::RmwSqlPrep`]) + the four `update_*_raw` method bodies.
//! - [`put_delete`] — bodies for `put_item_raw`, `get_item_raw`,
//!   `delete_item_raw`, and the two `_with_condition_raw` siblings.

mod batch_get;
mod batch_write;
mod condition_sql;
mod put_delete;
mod query;
mod scan;
mod shape;
mod types;
mod update;

use async_trait::async_trait;
use deadpool_postgres::{Manager, Pool};
use rekt_expressions::Condition;
use rekt_storage::{
    Backend, BackendError, BatchGetOutcome, BatchWriteOutcome, ConditionEvalFn, FilterEvalFn,
    GeneralUpdateFn, KeyValue, QueryOutcome, ScanOutcome, SkCondition, TableShape, UpdateOutcome,
    WriteOp,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio_postgres::NoTls;
use tracing::Instrument;

pub struct PgBackend {
    pool: Pool,
    /// Per-table cache of pre-formatted SQL and prepared-statement type
    /// vectors. Populated lazily on first touch from a `TableShape`. Keyed
    /// by `shape.table` (the PG table name).
    pub(crate) sql_cache: RwLock<HashMap<String, Arc<shape::ShapeSql>>>,
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

    pub(crate) async fn client(&self) -> Result<deadpool_postgres::Object, BackendError> {
        self.pool
            .get()
            .instrument(tracing::debug_span!("pg.pool_get"))
            .await
            .map_err(|e| {
                // Pool-get failures are typically PG unreachable, pool
                // exhaustion, or timeout (none of which we can
                // discriminate at this layer until Obs-3 lands the
                // explicit variants). Warn-level: operator-actionable
                // and rate-limited by the pool's own internal retries.
                tracing::warn!(
                    error = %e,
                    "PG pool get failed"
                );
                BackendError::Other(format!("pool get failed: {e}"))
            })
    }
}

#[async_trait]
impl Backend for PgBackend {
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        item: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        put_delete::put_item_raw(self, shape, pk, sk, item).await
    }

    async fn get_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        put_delete::get_item_raw(self, shape, pk, sk).await
    }

    async fn delete_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        put_delete::delete_item_raw(self, shape, pk, sk).await
    }

    async fn put_with_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        item: &serde_json::Value,
        condition: ConditionEvalFn<'_>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        put_delete::put_with_condition_raw(self, shape, pk, sk, item, condition).await
    }

    async fn delete_with_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        condition: ConditionEvalFn<'_>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        put_delete::delete_with_condition_raw(self, shape, pk, sk, condition).await
    }

    async fn update_simple_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        insert_item: &serde_json::Value,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
    ) -> Result<UpdateOutcome, BackendError> {
        update::update_simple_raw(self, shape, pk, sk, insert_item, sets, removes).await
    }

    async fn update_with_simple_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
        condition: &Condition,
    ) -> Result<UpdateOutcome, BackendError> {
        update::update_with_simple_condition_raw(self, shape, pk, sk, sets, removes, condition).await
    }

    async fn update_general_rmw_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        apply: GeneralUpdateFn<'_>,
    ) -> Result<UpdateOutcome, BackendError> {
        update::update_general_rmw_raw(self, shape, pk, sk, apply).await
    }

    async fn update_insert_only_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
    ) -> Result<UpdateOutcome, BackendError> {
        update::update_insert_only_raw(self, shape, insert_item).await
    }

    async fn query_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk_condition: Option<&SkCondition>,
        limit: Option<u32>,
        exclusive_start_key: Option<(&KeyValue, Option<&KeyValue>)>,
        filter: Option<FilterEvalFn<'_>>,
        forward: bool,
    ) -> Result<QueryOutcome, BackendError> {
        query::query_raw(
            self,
            shape,
            pk,
            sk_condition,
            limit,
            exclusive_start_key,
            filter,
            forward,
        )
        .await
    }

    async fn scan_raw(
        &self,
        shape: &TableShape<'_>,
        filter: Option<FilterEvalFn<'_>>,
        limit: Option<u32>,
        exclusive_start_key: Option<(&KeyValue, Option<&KeyValue>)>,
    ) -> Result<ScanOutcome, BackendError> {
        scan::scan_raw(self, shape, filter, limit, exclusive_start_key).await
    }

    async fn batch_get_raw(
        &self,
        shape: &TableShape<'_>,
        keys: &[(KeyValue, Option<KeyValue>)],
    ) -> Result<BatchGetOutcome, BackendError> {
        batch_get::batch_get_raw(self, shape, keys).await
    }

    async fn batch_write_raw(
        &self,
        shape: &TableShape<'_>,
        ops: &[WriteOp],
    ) -> Result<BatchWriteOutcome, BackendError> {
        batch_write::batch_write_raw(self, shape, ops).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::quote_ident;
    use bytes::Bytes;
    use rekt_storage::{KeyType, UpdateDecision};

    /// Test helper: derive `KeyValue`s from an item JSON per a shape so
    /// tests can stay terse. The CTE-based `put_item_raw` needs pk(+sk)
    /// bound explicitly; deriving from the item we're inserting matches
    /// what the translator does at runtime.
    fn keys_from_item(
        shape: &TableShape<'_>,
        item: &serde_json::Value,
    ) -> (KeyValue, Option<KeyValue>) {
        fn kv(t: KeyType, v: &serde_json::Value) -> KeyValue {
            match t {
                KeyType::S => KeyValue::S(v["S"].as_str().expect("S attr").to_string()),
                KeyType::N => KeyValue::N(v["N"].as_str().expect("N attr").to_string()),
                KeyType::B => {
                    use base64::Engine;
                    let b = base64::engine::general_purpose::STANDARD
                        .decode(v["B"].as_str().expect("B attr"))
                        .expect("B base64");
                    KeyValue::B(Bytes::from(b))
                }
            }
        }
        let pk = kv(shape.pk_type, &item[shape.pk_col]);
        let sk = shape
            .sk_col
            .zip(shape.sk_type)
            .map(|(c, t)| kv(t, &item[c]));
        (pk, sk)
    }

    /// Convenience wrapper around the now-CTE-flavored `put_item_raw`
    /// that derives pk/sk from the item. Tests that don't care about
    /// the returned pre-image use this; tests that do call the trait
    /// method directly.
    async fn pg_put(
        backend: &PgBackend,
        shape: &TableShape<'_>,
        item: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        let (pk, sk) = keys_from_item(shape, item);
        backend
            .put_item_raw(shape, &pk, sk.as_ref(), item)
            .await
            .unwrap()
    }

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
        let item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        let _ = pg_put(&backend, &shape, &item).await;

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(item.clone()));

        // Upsert: same key, different value, should replace.
        let item2 = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice2"}});
        let _ = pg_put(&backend, &shape, &item2).await;
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
            serde_json::json!({"device_id":{"S":"dev-1"},"ts":{"N":"1000"},"val":{"N":"1"}});
        let item2 =
            serde_json::json!({"device_id":{"S":"dev-1"},"ts":{"N":big_ts},"val":{"N":"2"}});
        let _ = pg_put(&backend, &shape, &item1).await;
        let _ = pg_put(&backend, &shape, &item2).await;

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
        let item = serde_json::json!({"hash":{"B":"AAEC/w=="},"extent":{"N":"4"}});
        let _ = pg_put(&backend, &shape, &item).await;

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
        // JSONB has no `id` attr — generated column yields NULL — PRIMARY KEY
        // rejects. The pk_value we bind to the CTE is a separate concern (it
        // only feeds the snapshot read, not the INSERT path).
        let bad_item = serde_json::json!({"label":{"S":"alice"}});
        let err = backend
            .put_item_raw(&shape, &KeyValue::S("anything".into()), None, &bad_item)
            .await
            .unwrap_err();
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
        let item = serde_json::json!({"id":{"S":"alice"},"label":{"S":"A"}});
        let _ = pg_put(&backend, &shape, &item).await;
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
        let _ = pg_put(&backend, &shape, &item_a).await;
        let _ = pg_put(&backend, &shape, &item_b).await;

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

    // ============================================================
    // update_simple_raw: fast-path UpdateItem (top-level SET literal +
    // REMOVE) against a real PG. All gated by `#[ignore]`.
    // ============================================================

    /// Set up a hash-only string-PK table identical to the one in the
    /// other tests, but with a unique name so parallel #[ignore] runs
    /// don't conflict.
    async fn setup_update_hash_table(
        backend: &PgBackend,
        table: &str,
    ) -> deadpool_postgres::Object {
        let client = backend.client().await.unwrap();
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {table};
                 CREATE TABLE {table} (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY
                 );"
            ))
            .await
            .unwrap();
        client
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_set_on_missing_row_creates() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_create").await;

        let shape = TableShape {
            table: "rekt_t_upd_create",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let insert_item =
            serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"},"score":{"N":"10"}});
        let name_v = serde_json::json!({"S":"alice"});
        let score_v = serde_json::json!({"N":"10"});
        backend
            .update_simple_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &insert_item,
                &[("label", &name_v), ("score", &score_v)],
                &[],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(insert_item));

        client
            .batch_execute("DROP TABLE rekt_t_upd_create;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_set_on_existing_row_merges() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_merge").await;

        let shape = TableShape {
            table: "rekt_t_upd_merge",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        // Seed an item that has `name` and `untouched`.
        let original = serde_json::json!({
            "id":{"S":"u1"},
            "label":{"S":"alice"},
            "untouched":{"S":"keep_me"},
        });
        let _ = pg_put(&backend, &shape, &original).await;

        // Update: replace `name`, add a new `score`. Sibling `untouched`
        // must survive — that's the merge contract.
        let insert_item =
            serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice2"},"score":{"N":"42"}});
        let name_v = serde_json::json!({"S":"alice2"});
        let score_v = serde_json::json!({"N":"42"});
        backend
            .update_simple_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &insert_item,
                &[("label", &name_v), ("score", &score_v)],
                &[],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({
                "id":{"S":"u1"},
                "label":{"S":"alice2"},
                "score":{"N":"42"},
                "untouched":{"S":"keep_me"},
            }))
        );

        client
            .batch_execute("DROP TABLE rekt_t_upd_merge;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_remove_existing_and_absent() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_remove").await;

        let shape = TableShape {
            table: "rekt_t_upd_remove",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({
            "id":{"S":"u1"},
            "label":{"S":"alice"},
            "deprecated":{"S":"gone"},
        });
        let _ = pg_put(&backend, &shape, &original).await;

        // REMOVE `deprecated` (present) and `never_existed` (absent).
        // Absent path is a no-op, must not error.
        backend
            .update_simple_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &serde_json::json!({"id":{"S":"u1"}}),
                &[],
                &["deprecated", "never_existed"],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({
                "id":{"S":"u1"},
                "label":{"S":"alice"},
            }))
        );

        client
            .batch_execute("DROP TABLE rekt_t_upd_remove;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_set_and_remove_combined() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_combined").await;

        let shape = TableShape {
            table: "rekt_t_upd_combined",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({
            "id":{"S":"u1"},
            "flag":{"S":"old"},
            "deprecated":{"S":"x"},
        });
        let _ = pg_put(&backend, &shape, &original).await;

        let status_v = serde_json::json!({"S":"new"});
        backend
            .update_simple_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &serde_json::json!({"id":{"S":"u1"},"flag":{"S":"new"}}),
                &[("flag", &status_v)],
                &["deprecated"],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({"id":{"S":"u1"},"flag":{"S":"new"}}))
        );

        client
            .batch_execute("DROP TABLE rekt_t_upd_combined;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_composite_key() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_upd_comp;
                 CREATE TABLE rekt_t_upd_comp (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')          STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)       STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_upd_comp",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };

        // Upsert via update on a missing composite-key row.
        let value_v = serde_json::json!({"N":"7"});
        backend
            .update_simple_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                Some(&KeyValue::N("1000".into())),
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N":"1000"},
                    "val":{"N":"7"},
                }),
                &[("val", &value_v)],
                &[],
            )
            .await
            .unwrap();

        // And again on an existing row — merge preserves other attrs.
        let extra = serde_json::json!({
            "device_id":{"S":"dev-1"},
            "ts":{"N":"2000"},
            "val":{"N":"1"},
            "label":{"S":"keep_me"},
        });
        let _ = pg_put(&backend, &shape, &extra).await;

        let value_v2 = serde_json::json!({"N":"99"});
        backend
            .update_simple_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                Some(&KeyValue::N("2000".into())),
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N":"2000"},
                    "val":{"N":"99"},
                }),
                &[("val", &value_v2)],
                &[],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                Some(&KeyValue::N("2000".into())),
            )
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({
                "device_id":{"S":"dev-1"},
                "ts":{"N":"2000"},
                "val":{"N":"99"},
                "label":{"S":"keep_me"},
            }))
        );

        client
            .batch_execute("DROP TABLE rekt_t_upd_comp;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_supports_all_attribute_value_variants() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_variants").await;

        let shape = TableShape {
            table: "rekt_t_upd_variants",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        // Cover S, N, B, BOOL, NULL, L, M, SS, NS, BS — every variant.
        let s_v = serde_json::json!({"S":"hello"});
        let n_v = serde_json::json!({"N":"42.5"});
        let b_v = serde_json::json!({"B":"AAEC"});
        let bool_v = serde_json::json!({"BOOL":true});
        let null_v = serde_json::json!({"NULL":true});
        let l_v = serde_json::json!({"L":[{"S":"a"},{"N":"1"}]});
        let m_v = serde_json::json!({"M":{"k":{"S":"v"}}});
        let ss_v = serde_json::json!({"SS":["a","b"]});
        let ns_v = serde_json::json!({"NS":["1","2"]});
        let bs_v = serde_json::json!({"BS":["AAE="]});

        let insert_item = serde_json::json!({
            "id":{"S":"u1"},
            "s":s_v, "n":n_v, "b":b_v, "bool":bool_v, "null":null_v,
            "l":l_v, "m":m_v, "ss":ss_v, "ns":ns_v, "bs":bs_v,
        });

        backend
            .update_simple_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &insert_item,
                &[
                    ("s", &s_v),
                    ("n", &n_v),
                    ("b", &b_v),
                    ("bool", &bool_v),
                    ("null", &null_v),
                    ("l", &l_v),
                    ("m", &m_v),
                    ("ss", &ss_v),
                    ("ns", &ns_v),
                    ("bs", &bs_v),
                ],
                &[],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(insert_item));

        client
            .batch_execute("DROP TABLE rekt_t_upd_variants;")
            .await
            .unwrap();
    }

    // ============================================================
    // update_insert_only_raw: Phase 4c, `attribute_not_exists(pk)`
    // path. Missing row → row inserted; existing row → CCFE.
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_insert_only_creates_when_row_absent() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_inserto_new").await;

        let shape = TableShape {
            table: "rekt_t_inserto_new",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        backend.update_insert_only_raw(&shape, &item).await.unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(item));

        client
            .batch_execute("DROP TABLE rekt_t_inserto_new;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_insert_only_returns_ccfe_when_row_exists() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_inserto_clash").await;

        let shape = TableShape {
            table: "rekt_t_inserto_clash",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        let _ = pg_put(&backend, &shape, &original).await;

        // Second insert-only call on the same key must fail and leave
        // the original row untouched.
        let proposed = serde_json::json!({"id":{"S":"u1"},"label":{"S":"NEW"}});
        let err = backend
            .update_insert_only_raw(&shape, &proposed)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(original));

        client
            .batch_execute("DROP TABLE rekt_t_inserto_clash;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_insert_only_composite_key() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_inserto_comp;
                 CREATE TABLE rekt_t_inserto_comp (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')         STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)     STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_inserto_comp",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };

        let first = serde_json::json!({
            "device_id":{"S":"d1"},
            "ts":{"N":"1000"},
            "v":{"N":"1"},
        });
        backend
            .update_insert_only_raw(&shape, &first)
            .await
            .unwrap();

        // Same composite key again → CCFE.
        let second = serde_json::json!({
            "device_id":{"S":"d1"},
            "ts":{"N":"1000"},
            "v":{"N":"99"},
        });
        let err = backend
            .update_insert_only_raw(&shape, &second)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        // Different ts on same device works (different composite key).
        let third = serde_json::json!({
            "device_id":{"S":"d1"},
            "ts":{"N":"2000"},
            "v":{"N":"7"},
        });
        backend
            .update_insert_only_raw(&shape, &third)
            .await
            .unwrap();

        client
            .batch_execute("DROP TABLE rekt_t_inserto_comp;")
            .await
            .unwrap();
    }

    // ============================================================
    // update_with_simple_condition_raw: Phase 4d, the SQL-WHERE path.
    // Compiles the resolved Condition AST to a SQL WHERE fragment
    // against `data` and emits UPDATE … WHERE.
    // ============================================================

    use rekt_expressions::{ComparisonOp, Condition, Operand, Path, PathSegment};

    fn cond_path(name: &str) -> Path {
        Path {
            segments: vec![PathSegment::Name(name.into())],
        }
    }
    fn cond_attr_exists(name: &str) -> Condition {
        Condition::AttributeExists(cond_path(name))
    }
    fn cond_eq(name: &str, value: rekt_protocol::AttributeValue) -> Condition {
        Condition::Compare {
            op: ComparisonOp::Eq,
            left: Operand::Path(cond_path(name)),
            right: Operand::Value(value),
        }
    }
    fn cond_cmp(name: &str, op: ComparisonOp, value: rekt_protocol::AttributeValue) -> Condition {
        Condition::Compare {
            op,
            left: Operand::Path(cond_path(name)),
            right: Operand::Value(value),
        }
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_attribute_exists_succeeds_when_row_present() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_ae").await;
        let shape = TableShape {
            table: "rekt_t_cond_ae",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}})).await;

        let new_name = serde_json::json!({"S":"alice2"});
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("label", &new_name)],
                &[],
                &cond_attr_exists("id"),
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice2"}}))
        );

        client
            .batch_execute("DROP TABLE rekt_t_cond_ae;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_attribute_exists_fails_when_row_missing() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_ae_miss").await;
        let shape = TableShape {
            table: "rekt_t_cond_ae_miss",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let new_name = serde_json::json!({"S":"x"});
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("missing".into()),
                None,
                &[("label", &new_name)],
                &[],
                &cond_attr_exists("id"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        client
            .batch_execute("DROP TABLE rekt_t_cond_ae_miss;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_eq_optimistic_lock() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_eq").await;
        let shape = TableShape {
            table: "rekt_t_cond_eq",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({"id":{"S":"u1"},"version":{"N":"3"}})).await;

        // Matching version → update applies (set version to 4).
        let new_v = serde_json::json!({"N":"4"});
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("version", &new_v)],
                &[],
                &cond_eq("version", rekt_protocol::AttributeValue::N("3".into())),
            )
            .await
            .unwrap();

        // Now version is 4; condition `version = 3` fails → CCFE.
        let new_v2 = serde_json::json!({"N":"5"});
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("version", &new_v2)],
                &[],
                &cond_eq("version", rekt_protocol::AttributeValue::N("3".into())),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        // Row was not mutated by the failed call — version is still 4.
        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({"id":{"S":"u1"},"version":{"N":"4"}}))
        );

        client
            .batch_execute("DROP TABLE rekt_t_cond_eq;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_numeric_ordering() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_n").await;
        let shape = TableShape {
            table: "rekt_t_cond_n",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({"id":{"S":"u1"},"score":{"N":"42"}})).await;

        // score < 100 → true.
        let new_score = serde_json::json!({"N":"50"});
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("score", &new_score)],
                &[],
                &cond_cmp(
                    "score",
                    ComparisonOp::Lt,
                    rek_n("100"),
                ),
            )
            .await
            .unwrap();

        // score (now 50) > 100 → false → CCFE.
        let new_score2 = serde_json::json!({"N":"99"});
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("score", &new_score2)],
                &[],
                &cond_cmp("score", ComparisonOp::Gt, rek_n("100")),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        client
            .batch_execute("DROP TABLE rekt_t_cond_n;")
            .await
            .unwrap();
    }

    fn rek_n(s: &str) -> rekt_protocol::AttributeValue {
        rekt_protocol::AttributeValue::N(s.into())
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_boolean_and() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_and").await;
        let shape = TableShape {
            table: "rekt_t_cond_and",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({ "id":{"S":"u1"}, "flag":{"S":"active"}, "version":{"N":"1"}, })).await;

        // Both terms true → update applies.
        let new_v = serde_json::json!({"N":"2"});
        let cond_both = Condition::And(
            Box::new(cond_eq(
                "flag",
                rekt_protocol::AttributeValue::S("active".into()),
            )),
            Box::new(cond_eq("version", rek_n("1"))),
        );
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("version", &new_v)],
                &[],
                &cond_both,
            )
            .await
            .unwrap();

        // Second term false → CCFE.
        let new_v2 = serde_json::json!({"N":"3"});
        let cond_bad = Condition::And(
            Box::new(cond_eq(
                "flag",
                rekt_protocol::AttributeValue::S("active".into()),
            )),
            Box::new(cond_eq("version", rek_n("999"))),
        );
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("version", &new_v2)],
                &[],
                &cond_bad,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        client
            .batch_execute("DROP TABLE rekt_t_cond_and;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_attribute_not_exists_on_non_pk_attr() {
        // `attribute_not_exists` on a non-pk attribute is SimpleSql, not
        // InsertOnlyOnPk — exercises the SQL `data ? key` predicate path.
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_ane").await;
        let shape = TableShape {
            table: "rekt_t_cond_ane",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}})).await;

        // attribute_not_exists(score) on a row that has no `score` → true.
        let v = serde_json::json!({"N":"7"});
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("score", &v)],
                &[],
                &Condition::AttributeNotExists(cond_path("score")),
            )
            .await
            .unwrap();

        // Now score exists; same condition → false → CCFE.
        let v2 = serde_json::json!({"N":"10"});
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("score", &v2)],
                &[],
                &Condition::AttributeNotExists(cond_path("score")),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        client
            .batch_execute("DROP TABLE rekt_t_cond_ane;")
            .await
            .unwrap();
    }

    // ============================================================
    // update_general_rmw_raw: Phase 3b slow path. Tests use a hand-
    // crafted closure to exercise the BEGIN / SELECT FOR UPDATE /
    // UPDATE-or-INSERT-DO-NOTHING / retry flow without depending on
    // translator wiring.
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn rmw_inserts_when_row_absent() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_rmw_insert").await;
        let shape = TableShape {
            table: "rekt_t_rmw_insert",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let new_item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
        let captured_clone = captured.clone();
        let new_clone = new_item.clone();
        let apply: GeneralUpdateFn<'_> = Box::new(move |existing| {
            // Record what we saw so the test can assert it was None.
            *captured_clone.lock().unwrap() = Some(existing.cloned());
            Ok(UpdateDecision::Apply(new_clone.clone()))
        });
        backend
            .update_general_rmw_raw(&shape, &KeyValue::S("u1".into()), None, apply)
            .await
            .unwrap();

        assert_eq!(*captured.lock().unwrap(), Some(None));
        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(new_item));

        client
            .batch_execute("DROP TABLE rekt_t_rmw_insert;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn rmw_updates_when_row_present() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_rmw_update").await;
        let shape = TableShape {
            table: "rekt_t_rmw_update",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({"id":{"S":"u1"},"tally":{"N":"5"}});
        let _ = pg_put(&backend, &shape, &original).await;

        // Closure: read counter, increment, write back.
        let apply: GeneralUpdateFn<'_> = Box::new(move |existing| {
            let existing = existing.expect("row should be present");
            let cur: i64 = existing["tally"]["N"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let new_item = serde_json::json!({
                "id":{"S":"u1"},
                "tally":{"N":format!("{}", cur + 1)},
            });
            Ok(UpdateDecision::Apply(new_item))
        });
        backend
            .update_general_rmw_raw(&shape, &KeyValue::S("u1".into()), None, apply)
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({"id":{"S":"u1"},"tally":{"N":"6"}}))
        );

        client
            .batch_execute("DROP TABLE rekt_t_rmw_update;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn rmw_fail_decision_returns_ccfe_no_mutation() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_rmw_fail").await;
        let shape = TableShape {
            table: "rekt_t_rmw_fail",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        let _ = pg_put(&backend, &shape, &original).await;

        let apply: GeneralUpdateFn<'_> =
            Box::new(move |_existing| Ok(UpdateDecision::Fail));
        let err = backend
            .update_general_rmw_raw(&shape, &KeyValue::S("u1".into()), None, apply)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        // Row unchanged.
        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(original));

        client
            .batch_execute("DROP TABLE rekt_t_rmw_fail;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn rmw_concurrent_inc_serializes() {
        // Two concurrent increments on the same row must both apply
        // serially, not lose-write. `SELECT FOR UPDATE` provides the
        // row-level lock.
        let backend = std::sync::Arc::new(PgBackend::connect(&database_url()).unwrap());
        let client = setup_update_hash_table(&backend, "rekt_t_rmw_race").await;
        let shape_owned = (
            "rekt_t_rmw_race".to_string(),
            "id".to_string(),
            "data".to_string(),
        );

        let init_shape = TableShape {
            table: &shape_owned.0,
            pk_col: &shape_owned.1,
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: &shape_owned.2,
        };
        let init_item = serde_json::json!({"id":{"S":"u1"},"tally":{"N":"0"}});
        backend
            .put_item_raw(
                &init_shape,
                &KeyValue::S("u1".into()),
                None,
                &init_item,
            )
            .await
            .unwrap();

        let inc_fn = || {
            let backend = backend.clone();
            let shape_owned = shape_owned.clone();
            async move {
                let shape = TableShape {
                    table: &shape_owned.0,
                    pk_col: &shape_owned.1,
                    pk_type: KeyType::S,
                    sk_col: None,
                    sk_type: None,
                    jsonb_col: &shape_owned.2,
                };
                let apply: GeneralUpdateFn<'_> = Box::new(move |existing| {
                    let existing = existing.expect("row should be present");
                    let cur: i64 = existing["tally"]["N"]
                        .as_str()
                        .unwrap()
                        .parse()
                        .unwrap();
                    Ok(UpdateDecision::Apply(serde_json::json!({
                        "id":{"S":"u1"},
                        "tally":{"N": format!("{}", cur + 1)},
                    })))
                });
                backend
                    .update_general_rmw_raw(&shape, &KeyValue::S("u1".into()), None, apply)
                    .await
            }
        };

        // Spawn N concurrent increments.
        const N: usize = 8;
        let mut handles = vec![];
        for _ in 0..N {
            handles.push(tokio::spawn(inc_fn()));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        let got = backend
            .get_item_raw(
                &TableShape {
                    table: &shape_owned.0,
                    pk_col: &shape_owned.1,
                    pk_type: KeyType::S,
                    sk_col: None,
                    sk_type: None,
                    jsonb_col: &shape_owned.2,
                },
                &KeyValue::S("u1".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            got.unwrap()["tally"]["N"].as_str().unwrap(),
            &N.to_string(),
            "lost-write: N concurrent increments should serialize"
        );

        client
            .batch_execute("DROP TABLE rekt_t_rmw_race;")
            .await
            .unwrap();
    }

    // ============================================================
    // Phase 4e/5/6: slow-path routes via update_general_rmw_raw with
    // a closure that combines evaluator + condition eval. Tests use
    // the rekt-translator helpers to drive the closure exactly as
    // the server would.
    // ============================================================

    fn build_apply<'a>(
        expr: rekt_expressions::UpdateExpression,
        condition: Option<rekt_expressions::Condition>,
        key: rekt_protocol::Item,
    ) -> GeneralUpdateFn<'a> {
        Box::new(move |existing| {
            if let Some(c) = &condition {
                if !rekt_translator::evaluate_condition(existing, c) {
                    return Ok(UpdateDecision::Fail);
                }
            }
            match rekt_translator::apply_update_expression(existing, &expr, &key) {
                Ok(new) => Ok(UpdateDecision::Apply(new)),
                Err(e) => Err(BackendError::Other(e.to_string())),
            }
        })
    }

    fn parse_upd_e2e(
        src: &str,
        values: &[(&str, rekt_protocol::AttributeValue)],
    ) -> rekt_expressions::UpdateExpression {
        let raw = rekt_expressions::parse_update_expression(src).unwrap();
        let names = std::collections::BTreeMap::new();
        let vmap: std::collections::BTreeMap<String, rekt_protocol::AttributeValue> =
            values.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
        rekt_expressions::substitute_update(raw, &names, &vmap).unwrap()
    }

    fn parse_cond_e2e(
        src: &str,
        values: &[(&str, rekt_protocol::AttributeValue)],
    ) -> rekt_expressions::Condition {
        let raw = rekt_expressions::parse_condition_expression(src).unwrap();
        let names = std::collections::BTreeMap::new();
        let vmap: std::collections::BTreeMap<String, rekt_protocol::AttributeValue> =
            values.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
        rekt_expressions::substitute_condition(raw, &names, &vmap).unwrap()
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn slow_path_add_numeric_increments() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_addnum").await;
        let shape = TableShape {
            table: "rekt_t_addnum",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({"id":{"S":"u1"},"tally":{"N":"10"}})).await;

        let key: rekt_protocol::Item = [(
            "id".to_string(),
            rekt_protocol::AttributeValue::S("u1".into()),
        )]
        .into_iter()
        .collect();
        let expr = parse_upd_e2e(
            "ADD tally :inc",
            &[(":inc", rekt_protocol::AttributeValue::N("5".into()))],
        );
        backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr, None, key),
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got.unwrap()["tally"], serde_json::json!({"N":"15"}));

        client
            .batch_execute("DROP TABLE rekt_t_addnum;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn slow_path_add_set_union() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_addset").await;
        let shape = TableShape {
            table: "rekt_t_addset",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({"id":{"S":"u1"},"tags":{"SS":["a","b"]}})).await;

        let key: rekt_protocol::Item = [(
            "id".to_string(),
            rekt_protocol::AttributeValue::S("u1".into()),
        )]
        .into_iter()
        .collect();
        let expr = parse_upd_e2e(
            "ADD tags :new",
            &[(
                ":new",
                rekt_protocol::AttributeValue::Ss(vec!["b".into(), "c".into()]),
            )],
        );
        backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr, None, key),
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        let tags = got.unwrap()["tags"]["SS"].as_array().unwrap().clone();
        assert_eq!(tags.len(), 3);
        for s in ["a", "b", "c"] {
            assert!(tags.iter().any(|v| v == s), "missing {s}");
        }

        client
            .batch_execute("DROP TABLE rekt_t_addset;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn slow_path_delete_set_emptied_removes_attribute() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_del_empty").await;
        let shape = TableShape {
            table: "rekt_t_del_empty",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({"id":{"S":"u1"},"tags":{"SS":["a","b"]},"keep":{"S":"x"}})).await;

        let key: rekt_protocol::Item = [(
            "id".to_string(),
            rekt_protocol::AttributeValue::S("u1".into()),
        )]
        .into_iter()
        .collect();
        let expr = parse_upd_e2e(
            "DELETE tags :rm",
            &[(
                ":rm",
                rekt_protocol::AttributeValue::Ss(vec!["a".into(), "b".into()]),
            )],
        );
        backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr, None, key),
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        let item = got.unwrap();
        assert!(item.get("tags").is_none(), "tags should be removed");
        assert_eq!(item["keep"], serde_json::json!({"S":"x"}));

        client
            .batch_execute("DROP TABLE rekt_t_del_empty;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn slow_path_begins_with_condition() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_bw").await;
        let shape = TableShape {
            table: "rekt_t_bw",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let _ = pg_put(&backend, &shape, &serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}})).await;

        let key: rekt_protocol::Item = [(
            "id".to_string(),
            rekt_protocol::AttributeValue::S("u1".into()),
        )]
        .into_iter()
        .collect();

        // Matching prefix → update applies.
        let expr = parse_upd_e2e(
            "SET marker = :m",
            &[(":m", rekt_protocol::AttributeValue::S("hit".into()))],
        );
        let cond = parse_cond_e2e(
            "begins_with(label, :p)",
            &[(":p", rekt_protocol::AttributeValue::S("ali".into()))],
        );
        backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr, Some(cond), key.clone()),
            )
            .await
            .unwrap();

        // Non-matching prefix → CCFE.
        let expr2 = parse_upd_e2e(
            "SET marker = :m",
            &[(":m", rekt_protocol::AttributeValue::S("nope".into()))],
        );
        let cond2 = parse_cond_e2e(
            "begins_with(label, :p)",
            &[(":p", rekt_protocol::AttributeValue::S("bob".into()))],
        );
        let err = backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr2, Some(cond2), key),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got.unwrap()["marker"], serde_json::json!({"S":"hit"}));

        client
            .batch_execute("DROP TABLE rekt_t_bw;")
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

    // ============================================================
    // Q1 — query_raw against real PG
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_pk_only_returns_partition_ordered() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_pk_only;
                 CREATE TABLE rekt_t_q_pk_only (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_pk_only",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in ["3000", "1000", "2000"] {
            let item =
                serde_json::json!({"device_id":{"S":"dev-1"},"ts":{"N":ts},"val":{"N":ts}});
            pg_put(&backend, &shape, &item).await;
        }

        let out = backend
            .query_raw(&shape, &KeyValue::S("dev-1".into()), None, None, None, None, true)
            .await
            .unwrap();
        assert_eq!(out.count, 3);
        assert_eq!(out.scanned_count, 3);
        let ts_seq: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts_seq, vec!["1000", "2000", "3000"]);

        client
            .batch_execute("DROP TABLE rekt_t_q_pk_only;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_sk_predicates() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_sk;
                 CREATE TABLE rekt_t_q_sk (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_sk",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in ["100", "200", "300", "400", "500"] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"device_id":{"S":"dev-1"},"ts":{"N":ts},"val":{"S":"x"}}),
            )
            .await;
        }

        let pk = KeyValue::S("dev-1".into());

        // Eq
        let out = backend
            .query_raw(
                &shape,
                &pk,
                Some(&SkCondition::Eq(KeyValue::N("300".into()))),
                None,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(out.count, 1);
        assert_eq!(out.items[0]["ts"]["N"], "300");

        // Lt → 100, 200
        let out = backend
            .query_raw(
                &shape,
                &pk,
                Some(&SkCondition::Lt(KeyValue::N("300".into()))),
                None,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["100", "200"]);

        // Ge → 300, 400, 500
        let out = backend
            .query_raw(
                &shape,
                &pk,
                Some(&SkCondition::Ge(KeyValue::N("300".into()))),
                None,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["300", "400", "500"]);

        // Between (inclusive)
        let out = backend
            .query_raw(
                &shape,
                &pk,
                Some(&SkCondition::Between(
                    KeyValue::N("200".into()),
                    KeyValue::N("400".into()),
                )),
                None,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["200", "300", "400"]);

        client
            .batch_execute("DROP TABLE rekt_t_q_sk;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_sk_begins_with_string() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_bw;
                 CREATE TABLE rekt_t_q_bw (
                   data   jsonb NOT NULL,
                   thread text GENERATED ALWAYS AS (data#>>'{thread,S}') STORED,
                   ts     text GENERATED ALWAYS AS (data#>>'{ts,S}')     STORED,
                   PRIMARY KEY (thread, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_bw",
            pk_col: "thread",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::S),
            jsonb_col: "data",
        };
        for ts in ["2026-01-01", "2026-01-02", "2026-02-01", "2027-01-01"] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"thread":{"S":"t-1"},"ts":{"S":ts},"body":{"S":"x"}}),
            )
            .await;
        }
        let out = backend
            .query_raw(
                &shape,
                &KeyValue::S("t-1".into()),
                Some(&SkCondition::BeginsWithS("2026-01".into())),
                None,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["S"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["2026-01-01", "2026-01-02"]);

        // Verify LIKE-special chars in the prefix are escaped: a prefix
        // of `%` should match nothing (not "everything").
        let none = backend
            .query_raw(
                &shape,
                &KeyValue::S("t-1".into()),
                Some(&SkCondition::BeginsWithS("%".into())),
                None,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(none.count, 0);

        client
            .batch_execute("DROP TABLE rekt_t_q_bw;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_limit_honored() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_limit;
                 CREATE TABLE rekt_t_q_limit (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_limit",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in 1..=10 {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N": ts.to_string()},
                    "val":{"N": ts.to_string()}
                }),
            )
            .await;
        }
        let out = backend
            .query_raw(&shape, &KeyValue::S("dev-1".into()), None, Some(3), None, None, true)
            .await
            .unwrap();
        assert_eq!(out.count, 3);
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["1", "2", "3"]);

        client
            .batch_execute("DROP TABLE rekt_t_q_limit;")
            .await
            .unwrap();
    }

    // ============================================================
    // Q2 — Limit + ExclusiveStartKey/LastEvaluatedKey paging
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_limit_sets_lek_when_more_remain() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_lek;
                 CREATE TABLE rekt_t_q_lek (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_lek",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in 1..=5 {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N": ts.to_string()},
                    "val":{"N": ts.to_string()}
                }),
            )
            .await;
        }
        let out = backend
            .query_raw(&shape, &KeyValue::S("dev-1".into()), None, Some(2), None, None, true)
            .await
            .unwrap();
        assert_eq!(out.count, 2);
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["1", "2"]);
        let (lek_pk, lek_sk) = out.last_evaluated_key.expect("LEK present");
        assert_eq!(lek_pk, KeyValue::S("dev-1".into()));
        assert_eq!(lek_sk, Some(KeyValue::N("2".into())));

        // Last page: no LEK.
        let out = backend
            .query_raw(&shape, &KeyValue::S("dev-1".into()), None, Some(10), None, None, true)
            .await
            .unwrap();
        assert_eq!(out.count, 5);
        assert!(out.last_evaluated_key.is_none());

        client
            .batch_execute("DROP TABLE rekt_t_q_lek;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_multi_page_reassembles_partition() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_page;
                 CREATE TABLE rekt_t_q_page (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_page",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in 1..=10 {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N": ts.to_string()},
                    "val":{"N":"x"}
                }),
            )
            .await;
        }

        let pk = KeyValue::S("dev-1".into());
        let mut all_ts: Vec<String> = Vec::new();
        let mut esk: Option<(KeyValue, Option<KeyValue>)> = None;
        let mut iter = 0;
        loop {
            iter += 1;
            assert!(iter <= 10, "pagination didn't terminate");
            let esk_ref = esk
                .as_ref()
                .map(|(p, s)| (p, s.as_ref()));
            let out = backend
                .query_raw(&shape, &pk, None, Some(3), esk_ref, None, true)
                .await
                .unwrap();
            for item in &out.items {
                all_ts.push(item["ts"]["N"].as_str().unwrap().to_string());
            }
            match out.last_evaluated_key {
                Some(next) => esk = Some(next),
                None => break,
            }
        }
        assert_eq!(
            all_ts,
            (1..=10).map(|n| n.to_string()).collect::<Vec<_>>()
        );

        client
            .batch_execute("DROP TABLE rekt_t_q_page;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_esk_with_sk_predicate_combines() {
        // ESK + sk_condition: caller passes ESK to resume, but the
        // original sk_condition (e.g. begins_with) still applies.
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_esk_bw;
                 CREATE TABLE rekt_t_q_esk_bw (
                   data   jsonb NOT NULL,
                   thread text GENERATED ALWAYS AS (data#>>'{thread,S}') STORED,
                   ts     text GENERATED ALWAYS AS (data#>>'{ts,S}')     STORED,
                   PRIMARY KEY (thread, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_esk_bw",
            pk_col: "thread",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::S),
            jsonb_col: "data",
        };
        for ts in [
            "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-02-01",
        ] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"thread":{"S":"t-1"},"ts":{"S":ts},"body":{"S":"x"}}),
            )
            .await;
        }

        // First page: begins_with("2026-01"), L=2 → ts in [01, 02], LEK at 02.
        let pk = KeyValue::S("t-1".into());
        let out = backend
            .query_raw(
                &shape,
                &pk,
                Some(&SkCondition::BeginsWithS("2026-01".into())),
                Some(2),
                None,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(out.count, 2);
        let (_, lek_sk) = out.last_evaluated_key.expect("LEK present");
        assert_eq!(lek_sk, Some(KeyValue::S("2026-01-02".into())));

        // Second page: same begins_with, ESK = ("t-1", "2026-01-02")
        // → ts in [03, 04]. No LEK (4 items total in the prefix).
        let esk_sk = KeyValue::S("2026-01-02".into());
        let esk = (&pk, Some(&esk_sk));
        let out = backend
            .query_raw(
                &shape,
                &pk,
                Some(&SkCondition::BeginsWithS("2026-01".into())),
                Some(2),
                Some(esk),
                None,
                true,
            )
            .await
            .unwrap();
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["S"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["2026-01-03", "2026-01-04"]);
        // DDB parity: LEK is present whenever the page fills exactly
        // to Limit, even if no more matching rows remain. The next
        // call would return 0 items + no LEK.
        let (_, lek_sk) = out.last_evaluated_key.expect("LEK present on filled page");
        assert_eq!(lek_sk, Some(KeyValue::S("2026-01-04".into())));

        client
            .batch_execute("DROP TABLE rekt_t_q_esk_bw;")
            .await
            .unwrap();
    }

    // ============================================================
    // Q3 — FilterExpression (per-row Rust eval against real PG)
    // ============================================================

    /// Build a FilterEvalFn from a hand-written predicate so the PG
    /// integration test doesn't have to depend on rekt-translator's
    /// parser. The dispatcher uses `evaluate_condition` against the
    /// parsed AST in production; this just exercises the storage
    /// layer's filter contract.
    fn flag_eq_filter(want: &'static str) -> rekt_storage::FilterEvalFn<'static> {
        Box::new(move |row| {
            row.get("flag")
                .and_then(|v| v.get("S"))
                .and_then(|v| v.as_str())
                == Some(want)
        })
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_filter_drops_some_rows() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_filt;
                 CREATE TABLE rekt_t_q_filt (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_filt",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in 1..=5 {
            let flag = if ts % 2 == 1 { "on" } else { "off" };
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N": ts.to_string()},
                    "flag":{"S": flag}
                }),
            )
            .await;
        }
        let out = backend
            .query_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                None,
                None,
                None,
                Some(flag_eq_filter("on")),
                true,
            )
            .await
            .unwrap();
        assert_eq!(out.scanned_count, 5);
        assert_eq!(out.count, 3);
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["1", "3", "5"]);
        assert!(out.last_evaluated_key.is_none());

        client
            .batch_execute("DROP TABLE rekt_t_q_filt;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_filter_with_limit_sets_lek_even_when_count_zero() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_filt_lim;
                 CREATE TABLE rekt_t_q_filt_lim (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_filt_lim",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in 1..=5 {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N": ts.to_string()},
                    "flag":{"S":"off"}
                }),
            )
            .await;
        }
        let out = backend
            .query_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                None,
                Some(2),
                None,
                Some(flag_eq_filter("NO_MATCH")),
                true,
            )
            .await
            .unwrap();
        assert_eq!(out.scanned_count, 2);
        assert_eq!(out.count, 0);
        let (_, lek_sk) = out.last_evaluated_key.expect("LEK present");
        assert_eq!(lek_sk, Some(KeyValue::N("2".into())));

        client
            .batch_execute("DROP TABLE rekt_t_q_filt_lim;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_filter_with_sk_predicate_combines() {
        // Sort-key predicate runs in SQL; filter runs in Rust over the
        // resulting rows. Both must be honored.
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_filt_sk;
                 CREATE TABLE rekt_t_q_filt_sk (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_filt_sk",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in 1..=10 {
            let flag = if ts % 2 == 0 { "on" } else { "off" };
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N": ts.to_string()},
                    "flag":{"S": flag}
                }),
            )
            .await;
        }
        // ts >= 5 → 5..=10 (scanned); flag = "on" → ts 6, 8, 10.
        let out = backend
            .query_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                Some(&SkCondition::Ge(KeyValue::N("5".into()))),
                None,
                None,
                Some(flag_eq_filter("on")),
                true,
            )
            .await
            .unwrap();
        assert_eq!(out.scanned_count, 6);
        assert_eq!(out.count, 3);
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["6", "8", "10"]);

        client
            .batch_execute("DROP TABLE rekt_t_q_filt_sk;")
            .await
            .unwrap();
    }

    // ============================================================
    // Q4 — scan_raw against real PG
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn scan_returns_all_rows_hash_only() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_scan_h;
                 CREATE TABLE rekt_t_scan_h (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_scan_h",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        for n in 1..=4 {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"id":{"S":format!("u{n}")},"label":{"S":format!("v{n}")}}),
            )
            .await;
        }
        let out = backend.scan_raw(&shape, None, None, None).await.unwrap();
        assert_eq!(out.count, 4);
        assert_eq!(out.scanned_count, 4);
        assert!(out.last_evaluated_key.is_none());
        // Ordered by pk_col ascending.
        let ids: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["id"]["S"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["u1", "u2", "u3", "u4"]);

        client
            .batch_execute("DROP TABLE rekt_t_scan_h;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn scan_returns_all_rows_composite() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_scan_c;
                 CREATE TABLE rekt_t_scan_c (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_scan_c",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for (dev, ts) in [("d2", "10"), ("d1", "20"), ("d1", "10"), ("d2", "5")] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({
                    "device_id":{"S":dev},
                    "ts":{"N":ts},
                    "val":{"S":"x"}
                }),
            )
            .await;
        }
        let out = backend.scan_raw(&shape, None, None, None).await.unwrap();
        assert_eq!(out.count, 4);
        // PG ORDER BY (device_id, ts) ascending.
        let keys: Vec<(String, String)> = out
            .items
            .iter()
            .map(|i| {
                (
                    i["device_id"]["S"].as_str().unwrap().to_string(),
                    i["ts"]["N"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                ("d1".into(), "10".into()),
                ("d1".into(), "20".into()),
                ("d2".into(), "5".into()),
                ("d2".into(), "10".into()),
            ]
        );

        client
            .batch_execute("DROP TABLE rekt_t_scan_c;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn scan_empty_table_returns_no_items() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_scan_empty;
                 CREATE TABLE rekt_t_scan_empty (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_scan_empty",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let out = backend.scan_raw(&shape, None, None, None).await.unwrap();
        assert_eq!(out.count, 0);
        assert_eq!(out.scanned_count, 0);
        assert!(out.items.is_empty());

        client
            .batch_execute("DROP TABLE rekt_t_scan_empty;")
            .await
            .unwrap();
    }

    // ============================================================
    // Q5 — Scan Limit/ESK/Filter + Query forward=false
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn scan_limit_sets_lek_and_resumes() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_scan_lek;
                 CREATE TABLE rekt_t_scan_lek (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_scan_lek",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        // 6 rows across 2 partitions; expected scan order is
        // (d1, 1), (d1, 2), (d1, 3), (d2, 1), (d2, 2), (d2, 3).
        for dev in ["d1", "d2"] {
            for ts in 1..=3 {
                pg_put(
                    &backend,
                    &shape,
                    &serde_json::json!({
                        "device_id":{"S":dev},
                        "ts":{"N":ts.to_string()},
                        "val":{"S":"x"}
                    }),
                )
                .await;
            }
        }
        // First page: L=2 → (d1,1), (d1,2); LEK at (d1, 2).
        let out = backend
            .scan_raw(&shape, None, Some(2), None)
            .await
            .unwrap();
        assert_eq!(out.count, 2);
        let (lek_pk, lek_sk) = out.last_evaluated_key.clone().expect("LEK present");
        assert_eq!(lek_pk, KeyValue::S("d1".into()));
        assert_eq!(lek_sk, Some(KeyValue::N("2".into())));

        // Resume from LEK. Should see (d1,3), (d2,1) next with L=2.
        let esk_pk = lek_pk;
        let esk_sk = lek_sk;
        let esk = (&esk_pk, esk_sk.as_ref());
        let out = backend
            .scan_raw(&shape, None, Some(2), Some(esk))
            .await
            .unwrap();
        let pairs: Vec<(String, String)> = out
            .items
            .iter()
            .map(|i| {
                (
                    i["device_id"]["S"].as_str().unwrap().to_string(),
                    i["ts"]["N"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("d1".into(), "3".into()),
                ("d2".into(), "1".into()),
            ]
        );

        client
            .batch_execute("DROP TABLE rekt_t_scan_lek;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn scan_filter_drops_some_rows() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_scan_filt;
                 CREATE TABLE rekt_t_scan_filt (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_scan_filt",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        for n in 1..=4 {
            let flag = if n % 2 == 0 { "on" } else { "off" };
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"id":{"S":format!("u{n}")},"flag":{"S":flag}}),
            )
            .await;
        }
        let filter: rekt_storage::FilterEvalFn<'static> = Box::new(|row| {
            row.get("flag")
                .and_then(|v| v.get("S"))
                .and_then(|v| v.as_str())
                == Some("on")
        });
        let out = backend
            .scan_raw(&shape, Some(filter), None, None)
            .await
            .unwrap();
        assert_eq!(out.scanned_count, 4);
        assert_eq!(out.count, 2);

        client
            .batch_execute("DROP TABLE rekt_t_scan_filt;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_forward_false_returns_descending_paginated() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_desc;
                 CREATE TABLE rekt_t_q_desc (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')     STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_desc",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        for ts in 1..=6 {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"d1"},
                    "ts":{"N":ts.to_string()},
                    "val":{"S":"x"}
                }),
            )
            .await;
        }
        let pk = KeyValue::S("d1".into());

        // Descending, L=2: ts 6, 5; LEK at sk=5.
        let out = backend
            .query_raw(&shape, &pk, None, Some(2), None, None, false)
            .await
            .unwrap();
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["6", "5"]);
        let (_, lek_sk) = out.last_evaluated_key.clone().expect("LEK");
        assert_eq!(lek_sk, Some(KeyValue::N("5".into())));

        // Continue descending with ESK: ts 4, 3.
        let esk_sk_val = KeyValue::N("5".into());
        let esk = (&pk, Some(&esk_sk_val));
        let out = backend
            .query_raw(&shape, &pk, None, Some(2), Some(esk), None, false)
            .await
            .unwrap();
        let ts: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["ts"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(ts, vec!["4", "3"]);

        client
            .batch_execute("DROP TABLE rekt_t_q_desc;")
            .await
            .unwrap();
    }

    // ============================================================
    // Integration coverage: Query/Scan on B-PK, N-PK, S+B composite
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn scan_returns_all_rows_b_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_scan_b;
                 CREATE TABLE rekt_t_scan_b (
                   data    jsonb NOT NULL,
                   binmark bytea GENERATED ALWAYS AS (decode(data#>>'{binmark,B}', 'base64')) STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_scan_b",
            pk_col: "binmark",
            pk_type: KeyType::B,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        for k in ["AAEC", "AAED", "AAEE"] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"binmark":{"B":k},"label":{"S":"v"}}),
            )
            .await;
        }
        let out = backend.scan_raw(&shape, None, None, None).await.unwrap();
        assert_eq!(out.count, 3);
        // Ordered by binmark bytes ascending → "AAEC" < "AAED" < "AAEE".
        let ks: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["binmark"]["B"].as_str().unwrap())
            .collect();
        assert_eq!(ks, vec!["AAEC", "AAED", "AAEE"]);

        client
            .batch_execute("DROP TABLE rekt_t_scan_b;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_b_pk_equality() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_b;
                 CREATE TABLE rekt_t_q_b (
                   data    jsonb NOT NULL,
                   binmark bytea GENERATED ALWAYS AS (decode(data#>>'{binmark,B}', 'base64')) STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_b",
            pk_col: "binmark",
            pk_type: KeyType::B,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        pg_put(
            &backend,
            &shape,
            &serde_json::json!({"binmark":{"B":"AAFF"},"label":{"S":"alpha"}}),
        )
        .await;
        // Decode base64 to bytes for the KeyValue::B used by query_raw.
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode("AAFF")
            .unwrap();
        let pk = KeyValue::B(Bytes::from(bytes));
        let out = backend
            .query_raw(&shape, &pk, None, None, None, None, true)
            .await
            .unwrap();
        assert_eq!(out.count, 1);
        assert_eq!(out.items[0]["label"]["S"], "alpha");

        client
            .batch_execute("DROP TABLE rekt_t_q_b;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_n_pk_equality() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_n;
                 CREATE TABLE rekt_t_q_n (
                   data jsonb   NOT NULL,
                   id   numeric GENERATED ALWAYS AS ((data#>>'{id,N}')::numeric) STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_n",
            pk_col: "id",
            pk_type: KeyType::N,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        for n in ["10", "1", "20", "2"] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"id":{"N":n},"val":{"N":n}}),
            )
            .await;
        }
        // Query each one — hash-only returns 0 or 1 rows.
        for n in ["1", "2", "10", "20"] {
            let out = backend
                .query_raw(
                    &shape,
                    &KeyValue::N(n.to_string()),
                    None,
                    None,
                    None,
                    None,
                    true,
                )
                .await
                .unwrap();
            assert_eq!(out.count, 1, "N-PK Query missed id={n}");
        }

        client
            .batch_execute("DROP TABLE rekt_t_q_n;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn scan_n_pk_ordering_is_numeric() {
        // Regression test: PG numeric ORDER BY is numeric, not lexical.
        // Insert "1","2","10","20" and assert ascending order is
        // 1, 2, 10, 20 (NOT 1, 10, 2, 20).
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_scan_n_ord;
                 CREATE TABLE rekt_t_scan_n_ord (
                   data jsonb   NOT NULL,
                   id   numeric GENERATED ALWAYS AS ((data#>>'{id,N}')::numeric) STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_scan_n_ord",
            pk_col: "id",
            pk_type: KeyType::N,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        for n in ["10", "1", "20", "2"] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"id":{"N":n},"val":{"N":n}}),
            )
            .await;
        }
        let out = backend.scan_raw(&shape, None, None, None).await.unwrap();
        let ids: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["id"]["N"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids,
            vec!["1", "2", "10", "20"],
            "N-PK Scan ordering must be numeric (not lexical)"
        );

        client
            .batch_execute("DROP TABLE rekt_t_scan_n_ord;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn query_sb_composite_pk_only() {
        // S+B composite (id S, binmark B). Pure pk-only KCE returns
        // all rows in the partition ordered by B ascending.
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_q_sb;
                 CREATE TABLE rekt_t_q_sb (
                   data    jsonb NOT NULL,
                   id      text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED,
                   binmark bytea GENERATED ALWAYS AS (decode(data#>>'{binmark,B}', 'base64')) STORED,
                   PRIMARY KEY (id, binmark)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_q_sb",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: Some("binmark"),
            sk_type: Some(KeyType::B),
            jsonb_col: "data",
        };
        for b in ["AAA=", "AAE=", "AAI="] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"id":{"S":"t1"},"binmark":{"B":b},"val":{"S":"v"}}),
            )
            .await;
        }
        let out = backend
            .query_raw(
                &shape,
                &KeyValue::S("t1".into()),
                None,
                None,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(out.count, 3);
        let bs: Vec<&str> = out
            .items
            .iter()
            .map(|i| i["binmark"]["B"].as_str().unwrap())
            .collect();
        assert_eq!(bs, vec!["AAA=", "AAE=", "AAI="]);

        client
            .batch_execute("DROP TABLE rekt_t_q_sb;")
            .await
            .unwrap();
    }

    // ===== BatchGetItem (B1) =================================================
    //
    // Live-PG validation of `batch_get_raw`'s two SQL shapes:
    //   - Hash-only:  `WHERE pk = ANY($1::type[])`
    //   - Composite:  `WHERE (pk, sk) IN (VALUES …)`

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_get_hash_string_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bg_hash_s;
                 CREATE TABLE rekt_t_bg_hash_s (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bg_hash_s",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        for n in ["a", "b", "c"] {
            let item = serde_json::json!({"id":{"S":n},"label":{"S":format!("L-{n}")}});
            pg_put(&backend, &shape, &item).await;
        }

        // 3 hits + 2 misses; misses are silently dropped.
        let keys = vec![
            (KeyValue::S("a".into()), None),
            (KeyValue::S("miss-1".into()), None),
            (KeyValue::S("b".into()), None),
            (KeyValue::S("miss-2".into()), None),
            (KeyValue::S("c".into()), None),
        ];
        let outcome = backend.batch_get_raw(&shape, &keys).await.unwrap();
        assert_eq!(outcome.items.len(), 3);
        let mut got_ids: Vec<&str> = outcome
            .items
            .iter()
            .map(|i| i["id"]["S"].as_str().unwrap())
            .collect();
        got_ids.sort();
        assert_eq!(got_ids, vec!["a", "b", "c"]);

        // Empty input → empty output, no SQL syntax error.
        let outcome = backend.batch_get_raw(&shape, &[]).await.unwrap();
        assert_eq!(outcome.items.len(), 0);

        client
            .batch_execute("DROP TABLE rekt_t_bg_hash_s;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_get_hash_numeric_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bg_hash_n;
                 CREATE TABLE rekt_t_bg_hash_n (
                   data jsonb NOT NULL,
                   id   numeric GENERATED ALWAYS AS ((data#>>'{id,N}')::numeric) STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bg_hash_n",
            pk_col: "id",
            pk_type: KeyType::N,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        for n in ["1", "2", "3"] {
            pg_put(&backend, &shape, &serde_json::json!({"id":{"N":n}})).await;
        }
        let keys = vec![
            (KeyValue::N("1".into()), None),
            (KeyValue::N("3".into()), None),
            (KeyValue::N("999".into()), None), // miss
        ];
        let outcome = backend.batch_get_raw(&shape, &keys).await.unwrap();
        assert_eq!(outcome.items.len(), 2);

        client
            .batch_execute("DROP TABLE rekt_t_bg_hash_n;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_get_hash_binary_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bg_hash_b;
                 CREATE TABLE rekt_t_bg_hash_b (
                   data jsonb NOT NULL,
                   hash bytea GENERATED ALWAYS AS (decode(data#>>'{hash,B}', 'base64')) STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bg_hash_b",
            pk_col: "hash",
            pk_type: KeyType::B,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        // Insert items with B keys via DDB-base64-wire form.
        use base64::Engine;
        for raw in [b"\x00\x01" as &[u8], b"\x00\x02", b"\x00\x03"] {
            let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"hash":{"B":b64}}),
            )
            .await;
        }
        let keys = vec![
            (KeyValue::B(Bytes::from_static(b"\x00\x01")), None),
            (KeyValue::B(Bytes::from_static(b"\x00\x03")), None),
            (KeyValue::B(Bytes::from_static(b"\x00\x99")), None), // miss
        ];
        let outcome = backend.batch_get_raw(&shape, &keys).await.unwrap();
        assert_eq!(outcome.items.len(), 2);

        client
            .batch_execute("DROP TABLE rekt_t_bg_hash_b;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_get_composite_pk_sk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bg_comp;
                 CREATE TABLE rekt_t_bg_comp (
                   data      jsonb   NOT NULL,
                   device_id text    GENERATED ALWAYS AS (data#>>'{device_id,S}') STORED,
                   ts        numeric GENERATED ALWAYS AS ((data#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bg_comp",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "data",
        };
        // Two partitions, three SK each.
        for dev in ["d1", "d2"] {
            for ts in ["10", "20", "30"] {
                pg_put(
                    &backend,
                    &shape,
                    &serde_json::json!({"device_id":{"S":dev},"ts":{"N":ts}}),
                )
                .await;
            }
        }
        // Cherry-pick across partitions; include a miss.
        let keys = vec![
            (KeyValue::S("d1".into()), Some(KeyValue::N("10".into()))),
            (KeyValue::S("d1".into()), Some(KeyValue::N("30".into()))),
            (KeyValue::S("d2".into()), Some(KeyValue::N("20".into()))),
            (KeyValue::S("d2".into()), Some(KeyValue::N("999".into()))), // miss
        ];
        let outcome = backend.batch_get_raw(&shape, &keys).await.unwrap();
        assert_eq!(outcome.items.len(), 3);

        client
            .batch_execute("DROP TABLE rekt_t_bg_comp;")
            .await
            .unwrap();
    }

    // ===== BatchWriteItem (B4–B5) ============================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_write_put_only_hash() {
        use rekt_storage::WriteOp;
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bw_hash_s;
                 CREATE TABLE rekt_t_bw_hash_s (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bw_hash_s",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let ops = vec![
            WriteOp::Put {
                pk: KeyValue::S("a".into()),
                sk: None,
                item: serde_json::json!({"id":{"S":"a"},"label":{"S":"A"}}),
            },
            WriteOp::Put {
                pk: KeyValue::S("b".into()),
                sk: None,
                item: serde_json::json!({"id":{"S":"b"},"label":{"S":"B"}}),
            },
        ];
        backend.batch_write_raw(&shape, &ops).await.unwrap();

        // Verify via GetItem.
        for (id, lab) in [("a","A"),("b","B")] {
            let got = backend
                .get_item_raw(&shape, &KeyValue::S(id.into()), None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(got["label"]["S"].as_str(), Some(lab));
        }

        // Upsert: re-Put with new label.
        let ops2 = vec![WriteOp::Put {
            pk: KeyValue::S("a".into()),
            sk: None,
            item: serde_json::json!({"id":{"S":"a"},"label":{"S":"A2"}}),
        }];
        backend.batch_write_raw(&shape, &ops2).await.unwrap();
        let got = backend
            .get_item_raw(&shape, &KeyValue::S("a".into()), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got["label"]["S"].as_str(), Some("A2"));

        client
            .batch_execute("DROP TABLE rekt_t_bw_hash_s;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_write_put_composite() {
        use rekt_storage::WriteOp;
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bw_comp;
                 CREATE TABLE rekt_t_bw_comp (
                   data      jsonb   NOT NULL,
                   device_id text    GENERATED ALWAYS AS (data#>>'{device_id,S}') STORED,
                   ts        numeric GENERATED ALWAYS AS ((data#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bw_comp",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "data",
        };
        let ops = vec![
            WriteOp::Put {
                pk: KeyValue::S("d1".into()),
                sk: Some(KeyValue::N("1".into())),
                item: serde_json::json!({"device_id":{"S":"d1"},"ts":{"N":"1"}}),
            },
            WriteOp::Put {
                pk: KeyValue::S("d1".into()),
                sk: Some(KeyValue::N("2".into())),
                item: serde_json::json!({"device_id":{"S":"d1"},"ts":{"N":"2"}}),
            },
            WriteOp::Put {
                pk: KeyValue::S("d2".into()),
                sk: Some(KeyValue::N("1".into())),
                item: serde_json::json!({"device_id":{"S":"d2"},"ts":{"N":"1"}}),
            },
        ];
        backend.batch_write_raw(&shape, &ops).await.unwrap();

        let got = backend
            .get_item_raw(
                &shape,
                &KeyValue::S("d1".into()),
                Some(&KeyValue::N("2".into())),
            )
            .await
            .unwrap();
        assert!(got.is_some());

        client
            .batch_execute("DROP TABLE rekt_t_bw_comp;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_write_put_and_delete_hash() {
        use rekt_storage::WriteOp;
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bw_mix;
                 CREATE TABLE rekt_t_bw_mix (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bw_mix",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        // Pre-seed two rows that will get deleted.
        for id in ["del-1","del-2"] {
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"id":{"S":id}}),
            )
            .await;
        }
        // Mix Put + Delete in one batch.
        let ops = vec![
            WriteOp::Put {
                pk: KeyValue::S("new-1".into()),
                sk: None,
                item: serde_json::json!({"id":{"S":"new-1"},"label":{"S":"x"}}),
            },
            WriteOp::Delete {
                pk: KeyValue::S("del-1".into()),
                sk: None,
            },
            WriteOp::Delete {
                pk: KeyValue::S("del-2".into()),
                sk: None,
            },
            WriteOp::Put {
                pk: KeyValue::S("new-2".into()),
                sk: None,
                item: serde_json::json!({"id":{"S":"new-2"},"label":{"S":"y"}}),
            },
        ];
        backend.batch_write_raw(&shape, &ops).await.unwrap();

        // new-1, new-2 exist; del-1, del-2 gone.
        assert!(backend.get_item_raw(&shape, &KeyValue::S("new-1".into()), None).await.unwrap().is_some());
        assert!(backend.get_item_raw(&shape, &KeyValue::S("new-2".into()), None).await.unwrap().is_some());
        assert!(backend.get_item_raw(&shape, &KeyValue::S("del-1".into()), None).await.unwrap().is_none());
        assert!(backend.get_item_raw(&shape, &KeyValue::S("del-2".into()), None).await.unwrap().is_none());

        // Delete of a non-existent row is a no-op (no error).
        backend
            .batch_write_raw(
                &shape,
                &[WriteOp::Delete {
                    pk: KeyValue::S("never-existed".into()),
                    sk: None,
                }],
            )
            .await
            .unwrap();

        client
            .batch_execute("DROP TABLE rekt_t_bw_mix;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_write_unknown_table_surfaces_table_not_found() {
        use rekt_storage::WriteOp;
        let backend = PgBackend::connect(&database_url()).unwrap();
        let shape = TableShape {
            table: "rekt_t_bw_no_such_table_xyz",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let err = backend
            .batch_write_raw(
                &shape,
                &[WriteOp::Put {
                    pk: KeyValue::S("x".into()),
                    sk: None,
                    item: serde_json::json!({"id":{"S":"x"}}),
                }],
            )
            .await
            .unwrap_err();
        match err {
            BackendError::TableNotFound { .. } => {}
            other => panic!("expected TableNotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_get_unknown_table_surfaces_table_not_found() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let shape = TableShape {
            table: "rekt_t_bg_no_such_table_xyz",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let keys = vec![(KeyValue::S("x".into()), None)];
        let err = backend.batch_get_raw(&shape, &keys).await.unwrap_err();
        match err {
            BackendError::TableNotFound { .. } => {}
            other => panic!("expected TableNotFound, got {other:?}"),
        }
    }

    // ===== Batch op boundary coverage (high + medium gaps) ==================
    //
    // The dispatch-layer mock can run 100-key / 25-write batches in
    // isolation, but the real SQL emitter's `IN (VALUES ...)` and
    // multi-row `INSERT ... ON CONFLICT DO UPDATE` only see those
    // shapes here. Catches bound-parameter / prepared-statement-cache
    // edge cases that pure mock tests can't.

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_get_100_keys_composite_at_cap() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bg_100;
                 CREATE TABLE rekt_t_bg_100 (
                   data      jsonb   NOT NULL,
                   device_id text    GENERATED ALWAYS AS (data#>>'{device_id,S}') STORED,
                   ts        numeric GENERATED ALWAYS AS ((data#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bg_100",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "data",
        };
        // Seed 100 rows across two partitions.
        for i in 0..100 {
            let dev = if i < 50 { "p1" } else { "p2" };
            let ts = (i % 50).to_string();
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"device_id":{"S":dev},"ts":{"N":ts}}),
            )
            .await;
        }
        let keys: Vec<(KeyValue, Option<KeyValue>)> = (0..100)
            .map(|i| {
                let dev = if i < 50 { "p1" } else { "p2" };
                let ts = (i % 50).to_string();
                (KeyValue::S(dev.into()), Some(KeyValue::N(ts)))
            })
            .collect();
        // 100 rows = 200 bound parameters. PG's default limit is
        // 65535 (i16 max) so this is well within bounds, but the
        // prepared-statement cache hasn't seen a 200-param entry
        // for this shape before — exercises that path.
        let outcome = backend.batch_get_raw(&shape, &keys).await.unwrap();
        assert_eq!(outcome.items.len(), 100);

        client
            .batch_execute("DROP TABLE rekt_t_bg_100;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_write_25_puts_composite_at_cap() {
        use rekt_storage::WriteOp;
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bw_25;
                 CREATE TABLE rekt_t_bw_25 (
                   data      jsonb   NOT NULL,
                   device_id text    GENERATED ALWAYS AS (data#>>'{device_id,S}') STORED,
                   ts        numeric GENERATED ALWAYS AS ((data#>>'{ts,N}')::numeric) STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bw_25",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "data",
        };
        let ops: Vec<WriteOp> = (0..25)
            .map(|i| WriteOp::Put {
                pk: KeyValue::S("p".into()),
                sk: Some(KeyValue::N(i.to_string())),
                item: serde_json::json!({"device_id":{"S":"p"},"ts":{"N":i.to_string()}}),
            })
            .collect();
        backend.batch_write_raw(&shape, &ops).await.unwrap();
        // Verify a sampling of rows persisted.
        for i in [0, 12, 24] {
            let got = backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("p".into()),
                    Some(&KeyValue::N(i.to_string())),
                )
                .await
                .unwrap();
            assert!(got.is_some(), "row ts={i} missing");
        }

        client
            .batch_execute("DROP TABLE rekt_t_bw_25;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn batch_get_composite_binary_sk() {
        // Composite S+B — exercises the per-row B cast in the
        // IN (VALUES ...) emitter. Single-row Get covers binsorted
        // shape via Q tests; batch's per-row binary bind is new.
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_bg_sb;
                 CREATE TABLE rekt_t_bg_sb (
                   data    jsonb NOT NULL,
                   id      text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED,
                   binmark bytea GENERATED ALWAYS AS (decode(data#>>'{binmark,B}', 'base64')) STORED,
                   PRIMARY KEY (id, binmark)
                 );",
            )
            .await
            .unwrap();
        let shape = TableShape {
            table: "rekt_t_bg_sb",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: Some("binmark"),
            sk_type: Some(KeyType::B),
            jsonb_col: "data",
        };
        use base64::Engine;
        for raw in [b"\x00\x01" as &[u8], b"\x00\x02", b"\x00\x03"] {
            let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
            pg_put(
                &backend,
                &shape,
                &serde_json::json!({"id":{"S":"part"},"binmark":{"B":b64}}),
            )
            .await;
        }
        let keys = vec![
            (
                KeyValue::S("part".into()),
                Some(KeyValue::B(Bytes::from_static(b"\x00\x01"))),
            ),
            (
                KeyValue::S("part".into()),
                Some(KeyValue::B(Bytes::from_static(b"\x00\x03"))),
            ),
            (
                KeyValue::S("part".into()),
                Some(KeyValue::B(Bytes::from_static(b"\x00\x99"))), // miss
            ),
        ];
        let outcome = backend.batch_get_raw(&shape, &keys).await.unwrap();
        assert_eq!(outcome.items.len(), 2);

        client
            .batch_execute("DROP TABLE rekt_t_bg_sb;")
            .await
            .unwrap();
    }
}
