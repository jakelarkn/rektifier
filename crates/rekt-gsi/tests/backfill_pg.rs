//! PLAN-9 G5 PG-live verification. Run with:
//!   cargo test -p rekt-gsi --test backfill_pg -- --ignored
//! after `just up`.

use deadpool_postgres::{Config, Runtime};
use rekt_catalog::{GsiMode, GsiSpec};
use rekt_gsi::{
    create_dual_write_gsi_in_tx, create_gsi_state_table, fetch_state, run_backfill,
    BackfillConfig, GsiPhase,
};
use rekt_storage::KeyType;
use std::time::Duration;
use tokio_postgres::NoTls;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
}

async fn build_pool() -> deadpool_postgres::Pool {
    let mut cfg = Config::new();
    cfg.url = Some(database_url());
    cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool")
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn backfill_populates_existing_rows() {
    let pool = build_pool().await;
    create_gsi_state_table(&pool).await.expect("bootstrap");

    let mut client = pool.get().await.expect("client");
    let pg_table = "rekt_gsi_test_g5";
    // Seed a populated table.
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = 'GsiTestG5'; \
             CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 data jsonb NOT NULL \
             ); \
             INSERT INTO {pg_table} (data) VALUES \
                ('{{\"id\":{{\"S\":\"a\"}},\"tier\":{{\"S\":\"bronze\"}}}}'::jsonb), \
                ('{{\"id\":{{\"S\":\"b\"}},\"tier\":{{\"S\":\"silver\"}}}}'::jsonb), \
                ('{{\"id\":{{\"S\":\"c\"}},\"tier\":{{\"S\":\"gold\"}}}}'::jsonb), \
                ('{{\"id\":{{\"S\":\"d\"}}}}'::jsonb);"
        ))
        .await
        .expect("seed");

    // Run the orchestrator's synchronous part (ADD COLUMN → backfilling
    // phase). This is the same code path UpdateTable.Create would take.
    let spec = GsiSpec {
        name: "by_tier".into(),
        partition_attr: "tier".into(),
        partition_type: KeyType::S,
        partition_pg_col: "tier".into(),
        sort_attr: None,
        sort_type: None,
        sort_pg_col: None,
        index_name: format!("{pg_table}_gsi_by_tier_idx"),
        projection_type: Some("ALL".into()),
        projection_non_key_attrs: None,
        mode: GsiMode::DualWrite,
        serveable: false,
        unserveable_reason: Some("DualWrite GSI in CREATING phase".into()),
    };
    let tx = client.transaction().await.expect("tx");
    create_dual_write_gsi_in_tx(&tx, "GsiTestG5", pg_table, &spec)
        .await
        .expect("orchestrator");
    tx.commit().await.expect("commit");

    // Pre-condition: column landed, populated by NO rows (all NULL).
    let null_count: i64 = client
        .query_one(
            &format!("SELECT COUNT(*)::bigint FROM {pg_table} WHERE tier IS NULL"),
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(
        null_count, 4,
        "post-ADD COLUMN, the new column is NULL for all existing rows"
    );

    // Run backfill.
    run_backfill(
        &pool,
        "GsiTestG5::by_tier",
        BackfillConfig {
            chunk_size: 2, // exercise multi-chunk loop
            throttle: Duration::from_millis(0),
        },
    )
    .await
    .expect("backfill");

    // Three rows had a tier; the fourth (sparse) should stay NULL.
    let rows = client
        .query(
            &format!("SELECT id, tier FROM {pg_table} ORDER BY id"),
            &[],
        )
        .await
        .expect("select");
    let got: Vec<(String, Option<String>)> = rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("a".into(), Some("bronze".into())),
            ("b".into(), Some("silver".into())),
            ("c".into(), Some("gold".into())),
            ("d".into(), None),
        ]
    );

    // State row remains in `backfilling` — G6 promotes to `indexing`.
    let st = fetch_state(&client, "GsiTestG5::by_tier")
        .await
        .expect("fetch")
        .expect("present");
    assert_eq!(st.phase, GsiPhase::Backfilling);

    client
        .batch_execute(&format!(
            "DROP TABLE {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = 'GsiTestG5';"
        ))
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn backfill_is_resumable_from_last_pk_copied() {
    let pool = build_pool().await;
    create_gsi_state_table(&pool).await.expect("bootstrap");

    let mut client = pool.get().await.expect("client");
    let pg_table = "rekt_gsi_test_g5_resume";
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = 'GsiTestG5Resume'; \
             CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 data jsonb NOT NULL \
             ); \
             INSERT INTO {pg_table} (data) VALUES \
                ('{{\"id\":{{\"S\":\"a\"}},\"tier\":{{\"S\":\"x\"}}}}'::jsonb), \
                ('{{\"id\":{{\"S\":\"b\"}},\"tier\":{{\"S\":\"x\"}}}}'::jsonb), \
                ('{{\"id\":{{\"S\":\"c\"}},\"tier\":{{\"S\":\"x\"}}}}'::jsonb), \
                ('{{\"id\":{{\"S\":\"d\"}},\"tier\":{{\"S\":\"x\"}}}}'::jsonb);"
        ))
        .await
        .expect("seed");

    let spec = GsiSpec {
        name: "by_tier".into(),
        partition_attr: "tier".into(),
        partition_type: KeyType::S,
        partition_pg_col: "tier".into(),
        sort_attr: None,
        sort_type: None,
        sort_pg_col: None,
        index_name: format!("{pg_table}_gsi_by_tier_idx"),
        projection_type: Some("ALL".into()),
        projection_non_key_attrs: None,
        mode: GsiMode::DualWrite,
        serveable: false,
        unserveable_reason: Some("DualWrite GSI in CREATING phase".into()),
    };
    let tx = client.transaction().await.expect("tx");
    create_dual_write_gsi_in_tx(&tx, "GsiTestG5Resume", pg_table, &spec)
        .await
        .expect("orchestrator");
    tx.commit().await.expect("commit");

    // Simulate a crash mid-backfill: hand-set last_pk_copied to "b"
    // so the worker should only touch rows with id > "b" (c, d).
    client
        .execute(
            "UPDATE _rektifier_gsi_state \
                SET last_pk_copied = $2::jsonb \
              WHERE gsi_id = $1",
            &[
                &"GsiTestG5Resume::by_tier",
                &serde_json::json!("b"),
            ],
        )
        .await
        .expect("set resume point");

    // Manually populate a and b to mimic the partial-completion state.
    client
        .batch_execute(&format!(
            "UPDATE {pg_table} SET tier = 'pre' WHERE id IN ('a','b')"
        ))
        .await
        .expect("simulate pre-crash chunks");

    run_backfill(
        &pool,
        "GsiTestG5Resume::by_tier",
        BackfillConfig::default(),
    )
    .await
    .expect("backfill");

    // a + b keep the pre-populated value (worker didn't touch them
    // thanks to last_pk_copied + WHERE tier IS NULL); c + d are
    // backfilled to 'x'.
    let rows = client
        .query(
            &format!("SELECT id, tier FROM {pg_table} ORDER BY id"),
            &[],
        )
        .await
        .expect("select");
    let got: Vec<(String, Option<String>)> = rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("a".into(), Some("pre".into())),
            ("b".into(), Some("pre".into())),
            ("c".into(), Some("x".into())),
            ("d".into(), Some("x".into())),
        ]
    );

    client
        .batch_execute(&format!(
            "DROP TABLE {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = 'GsiTestG5Resume';"
        ))
        .await
        .expect("cleanup");
}
