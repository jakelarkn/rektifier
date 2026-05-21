//! PLAN-9 G4 PG-live verification. Run with:
//!   cargo test -p rekt-storage-libpq --test dual_write_batch_transact_pg -- --ignored
//! after `just up`.
//!
//! Confirms `batch_write_raw` and `transact_write_raw` populate
//! DualWrite GSI columns on every touched row, identically to the
//! single-row paths covered in `dual_write_pg`.

use deadpool_postgres::{Config, Runtime};
use rekt_storage::{
    Backend, DualWriteCol, KeyType, KeyValue, TableShape, TransactWriteOp, WriteOp,
};
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

async fn seed_table(pool: &deadpool_postgres::Pool, pg_table: &str) {
    let client = pool.get().await.expect("client");
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {pg_table}; \
             CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 tier text NULL, \
                 data jsonb NOT NULL \
             );"
        ))
        .await
        .expect("seed");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn batch_write_populates_dual_write_columns() {
    let (backend, pool) = build_backend().await;
    let pg_table = "rekt_t_g4_batch";
    seed_table(&pool, pg_table).await;

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

    let items: Vec<serde_json::Value> = vec![
        serde_json::json!({"id":{"S":"a"},"tier":{"S":"bronze"}}),
        serde_json::json!({"id":{"S":"b"},"tier":{"S":"silver"}}),
        serde_json::json!({"id":{"S":"c"},"tier":{"S":"gold"}}),
        // Sparse: no tier attribute.
        serde_json::json!({"id":{"S":"d"}}),
    ];
    let ops: Vec<WriteOp> = items
        .iter()
        .map(|it| WriteOp::Put {
            pk: KeyValue::S(it["id"]["S"].as_str().unwrap().to_string()),
            sk: None,
            item: it.clone(),
        })
        .collect();

    backend
        .batch_write_raw(&shape, &ops)
        .await
        .expect("batch_write");

    let client = pool.get().await.expect("client");
    let rows = client
        .query(
            &format!("SELECT id, tier FROM {pg_table} ORDER BY id"),
            &[],
        )
        .await
        .expect("select");
    let mut got: Vec<(String, Option<String>)> = rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("a".into(), Some("bronze".into())),
            ("b".into(), Some("silver".into())),
            ("c".into(), Some("gold".into())),
            ("d".into(), None),
        ]
    );

    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn transact_write_populates_dual_write_columns_across_ops() {
    let (backend, pool) = build_backend().await;
    let pg_table = "rekt_t_g4_transact";
    seed_table(&pool, pg_table).await;

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

    let put_item = serde_json::json!({"id":{"S":"u1"},"tier":{"S":"bronze"}});
    let ops: Vec<TransactWriteOp<'_>> = vec![TransactWriteOp::Put {
        shape: shape.clone(),
        pk: KeyValue::S("u1".into()),
        sk: None,
        item: put_item.clone(),
        condition: None,
        return_old_on_failure: false,
    }];
    backend.transact_write_raw(ops).await.expect("transact 1");

    let client = pool.get().await.expect("client");
    let tier: String = client
        .query_one(
            &format!("SELECT tier FROM {pg_table} WHERE id='u1'"),
            &[],
        )
        .await
        .expect("select")
        .get(0);
    assert_eq!(tier, "bronze");

    // Now an Update inside transact: flip tier.
    use rekt_storage::{GeneralUpdateFn, UpdateDecision};
    let apply: GeneralUpdateFn<'_> = Box::new(|existing| {
        let mut m = existing.expect("present").clone();
        m["tier"] = serde_json::json!({"S":"platinum"});
        Ok(UpdateDecision::Apply(m))
    });
    let ops: Vec<TransactWriteOp<'_>> = vec![TransactWriteOp::Update {
        shape: shape.clone(),
        pk: KeyValue::S("u1".into()),
        sk: None,
        apply,
        return_old_on_failure: false,
    }];
    backend.transact_write_raw(ops).await.expect("transact 2");

    let tier: String = client
        .query_one(
            &format!("SELECT tier FROM {pg_table} WHERE id='u1'"),
            &[],
        )
        .await
        .expect("select")
        .get(0);
    assert_eq!(tier, "platinum");

    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}
