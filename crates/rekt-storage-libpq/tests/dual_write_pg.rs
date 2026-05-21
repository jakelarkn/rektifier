//! PLAN-9 G3 PG-live verification. Run with:
//!   cargo test -p rekt-storage-libpq --test dual_write_pg -- --ignored
//! after `just up`.
//!
//! Asserts that when `TableShape::dual_write_cols` is non-empty, the
//! emitted INSERT/UPDATE SQL populates each DualWrite column from the
//! JSONB body in the same statement. PG performs the extraction;
//! rektifier does not bind separate values.
//!
//! Coverage: hash-only and composite tables; PutItem (insert + upsert);
//! DeleteItem (no-op on dual-write); UpdateItem slow path (RMW).

use bytes::Bytes;
use deadpool_postgres::{Config, Runtime};
use rekt_storage::{Backend, DualWriteCol, KeyType, KeyValue, TableShape};
use rekt_storage_libpq::PgBackend;
use tokio_postgres::NoTls;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
}

async fn build_backend() -> (PgBackend, deadpool_postgres::Pool) {
    let mut cfg = Config::new();
    cfg.url = Some(database_url());
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool");
    let backend = PgBackend::new(pool.clone());
    (backend, pool)
}

async fn seed_table(pool: &deadpool_postgres::Pool, pg_table: &str, ddl: &str) {
    let client = pool.get().await.expect("client");
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {pg_table}; {ddl}"))
        .await
        .expect("seed");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn put_populates_dual_write_columns_hash_only() {
    let (backend, pool) = build_backend().await;
    let pg_table = "rekt_t_g3_hashonly";
    seed_table(
        &pool,
        pg_table,
        &format!(
            "CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 tier text NULL, \
                 score numeric NULL, \
                 data jsonb NOT NULL \
             );"
        ),
    )
    .await;

    let cols = [
        DualWriteCol {
            col: "tier",
            attr: "tier",
            key_type: KeyType::S,
        },
        DualWriteCol {
            col: "score",
            attr: "score",
            key_type: KeyType::N,
        },
    ];
    let shape = TableShape {
        table: pg_table,
        pk_col: "id",
        pk_type: KeyType::S,
        sk_col: None,
        sk_type: None,
        jsonb_col: "data",
        dual_write_cols: &cols,
    };

    let item = serde_json::json!({
        "id":   {"S":"u1"},
        "tier": {"S":"gold"},
        "score":{"N":"42"},
    });
    backend
        .put_item_raw(&shape, &KeyValue::S("u1".into()), None, &item)
        .await
        .expect("put");

    // Verify columns auto-populated.
    let client = pool.get().await.expect("client");
    let row = client
        .query_one(
            &format!("SELECT tier, score::text FROM {pg_table} WHERE id = 'u1'"),
            &[],
        )
        .await
        .expect("select");
    let tier: String = row.get(0);
    let score: String = row.get(1);
    assert_eq!(tier, "gold");
    assert_eq!(score, "42");

    // Upsert updates both the JSONB and the DualWrite cols.
    let item2 = serde_json::json!({
        "id":   {"S":"u1"},
        "tier": {"S":"platinum"},
        "score":{"N":"99"},
    });
    backend
        .put_item_raw(&shape, &KeyValue::S("u1".into()), None, &item2)
        .await
        .expect("upsert");
    let row = client
        .query_one(
            &format!("SELECT tier, score::text FROM {pg_table} WHERE id = 'u1'"),
            &[],
        )
        .await
        .expect("select after upsert");
    let tier: String = row.get(0);
    let score: String = row.get(1);
    assert_eq!(tier, "platinum");
    assert_eq!(score, "99");

    // Sparse: item missing the GSI attribute leaves the column NULL.
    let item3 = serde_json::json!({"id":{"S":"u2"}});
    backend
        .put_item_raw(&shape, &KeyValue::S("u2".into()), None, &item3)
        .await
        .expect("sparse put");
    let row = client
        .query_one(
            &format!("SELECT tier, score::text FROM {pg_table} WHERE id = 'u2'"),
            &[],
        )
        .await
        .expect("select sparse");
    let tier: Option<String> = row.get(0);
    let score: Option<String> = row.get(1);
    assert!(tier.is_none());
    assert!(score.is_none());

    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn put_populates_dual_write_columns_binary() {
    let (backend, pool) = build_backend().await;
    let pg_table = "rekt_t_g3_binary";
    seed_table(
        &pool,
        pg_table,
        &format!(
            "CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 payload bytea NULL, \
                 data jsonb NOT NULL \
             );"
        ),
    )
    .await;

    let cols = [DualWriteCol {
        col: "payload",
        attr: "payload",
        key_type: KeyType::B,
    }];
    let shape = TableShape {
        table: pg_table,
        pk_col: "id",
        pk_type: KeyType::S,
        sk_col: None,
        sk_type: None,
        jsonb_col: "data",
        dual_write_cols: &cols,
    };

    // DDB encodes B values as base64 inside the JSONB.
    use base64::Engine;
    let raw = b"\x00\xff hello \x10";
    let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
    let item = serde_json::json!({
        "id":      {"S":"bin1"},
        "payload": {"B": encoded},
    });
    backend
        .put_item_raw(&shape, &KeyValue::S("bin1".into()), None, &item)
        .await
        .expect("put");

    let client = pool.get().await.expect("client");
    let row = client
        .query_one(
            &format!("SELECT payload FROM {pg_table} WHERE id = 'bin1'"),
            &[],
        )
        .await
        .expect("select");
    let bytes: Vec<u8> = row.get(0);
    assert_eq!(bytes, raw);

    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn put_populates_dual_write_columns_composite_table() {
    let (backend, pool) = build_backend().await;
    let pg_table = "rekt_t_g3_composite";
    seed_table(
        &pool,
        pg_table,
        &format!(
            "CREATE TABLE {pg_table} ( \
                 device text GENERATED ALWAYS AS (data#>>'{{device,S}}') STORED, \
                 ts numeric GENERATED ALWAYS AS ((data#>>'{{ts,N}}')::numeric) STORED, \
                 tier text NULL, \
                 data jsonb NOT NULL, \
                 PRIMARY KEY (device, ts) \
             );"
        ),
    )
    .await;

    let cols = [DualWriteCol {
        col: "tier",
        attr: "tier",
        key_type: KeyType::S,
    }];
    let shape = TableShape {
        table: pg_table,
        pk_col: "device",
        pk_type: KeyType::S,
        sk_col: Some("ts"),
        sk_type: Some(KeyType::N),
        jsonb_col: "data",
        dual_write_cols: &cols,
    };

    let item = serde_json::json!({
        "device":{"S":"d1"},
        "ts":    {"N":"1"},
        "tier":  {"S":"bronze"},
    });
    backend
        .put_item_raw(
            &shape,
            &KeyValue::S("d1".into()),
            Some(&KeyValue::N("1".into())),
            &item,
        )
        .await
        .expect("put");

    let client = pool.get().await.expect("client");
    let row = client
        .query_one(
            &format!("SELECT tier FROM {pg_table} WHERE device='d1' AND ts=1"),
            &[],
        )
        .await
        .expect("select");
    let tier: String = row.get(0);
    assert_eq!(tier, "bronze");

    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}

/// UpdateItem RMW slow path picks up changes to a dual-write attribute.
#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn update_rmw_repopulates_dual_write_column() {
    use rekt_storage::{GeneralUpdateFn, UpdateDecision};
    let (backend, pool) = build_backend().await;
    let pg_table = "rekt_t_g3_update";
    seed_table(
        &pool,
        pg_table,
        &format!(
            "CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 tier text NULL, \
                 data jsonb NOT NULL \
             );"
        ),
    )
    .await;

    let cols = [DualWriteCol {
        col: "tier",
        attr: "tier",
        key_type: KeyType::S,
    }];
    let shape = TableShape {
        table: pg_table,
        pk_col: "id",
        pk_type: KeyType::S,
        sk_col: None,
        sk_type: None,
        jsonb_col: "data",
        dual_write_cols: &cols,
    };

    // Initial put.
    backend
        .put_item_raw(
            &shape,
            &KeyValue::S("u".into()),
            None,
            &serde_json::json!({"id":{"S":"u"},"tier":{"S":"silver"}}),
        )
        .await
        .expect("put");

    // RMW that flips tier to gold.
    let apply: GeneralUpdateFn<'_> = Box::new(|existing| {
        let mut m = existing
            .expect("row present")
            .clone();
        m["tier"] = serde_json::json!({"S":"gold"});
        Ok(UpdateDecision::Apply(m))
    });
    backend
        .update_general_rmw_raw(&shape, &KeyValue::S("u".into()), None, apply)
        .await
        .expect("rmw");

    let client = pool.get().await.expect("client");
    let row = client
        .query_one(
            &format!("SELECT tier FROM {pg_table} WHERE id='u'"),
            &[],
        )
        .await
        .expect("select");
    let tier: String = row.get(0);
    assert_eq!(tier, "gold", "RMW upsert path should repopulate dual-write column");

    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}

// Suppress unused warning when base64 isn't pulled by other tests.
#[allow(dead_code)]
fn _bytes_marker(_b: Bytes) {}
