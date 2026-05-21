//! PLAN-9 G9 PG-live verification. Run with:
//!   cargo test -p rekt-gsi --test remove_pg -- --ignored
//! after `just up`.
//!
//! Generated-mode removal is exercised through the in-tx helper.
//! DualWrite-mode removal exercises the full coordinator
//! (start_dual_write_removal + run_removal_to_gone) including
//! DROP INDEX CONCURRENTLY + DROP COLUMN + state-row archival.

use deadpool_postgres::{Config, Runtime};
use rekt_catalog::{ensure_metadata_tables, GsiMode, GsiSpec};
use rekt_gsi::{
    create_dual_write_gsi_in_tx, create_gsi_state_table, drop_generated_gsi_in_tx, fetch_state,
    run_lifecycle_to_active, run_removal_to_gone, start_dual_write_removal, GsiPhase,
};
use rekt_storage::KeyType;
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
async fn generated_mode_removal_drops_index_and_column_in_tx() {
    let pool = build_pool().await;
    ensure_metadata_tables(&pool).await.expect("meta");

    let mut client = pool.get().await.expect("client");
    let pg_table = "rekt_gsi_test_g9_gen";

    // Seed a table with a Generated-mode GSI (column + index already
    // present, matching CT-time emission).
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {pg_table}; \
             CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 tier text GENERATED ALWAYS AS (data#>>'{{tier,S}}') STORED, \
                 data jsonb NOT NULL \
             ); \
             CREATE INDEX IF NOT EXISTS {pg_table}_gsi_by_tier_idx ON {pg_table} (tier);"
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
        mode: GsiMode::Generated,
        serveable: true,
        unserveable_reason: None,
    };
    let tx = client.transaction().await.expect("tx");
    drop_generated_gsi_in_tx(&tx, pg_table, &spec, &[], "id", None, &[])
        .await
        .expect("drop");
    tx.commit().await.expect("commit");

    // Index + column both gone.
    let n_idx: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pg_indexes \
              WHERE schemaname = current_schema() AND indexname = $1",
            &[&format!("{pg_table}_gsi_by_tier_idx")],
        )
        .await
        .expect("idx")
        .get(0);
    assert_eq!(n_idx, 0);
    let n_col: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM information_schema.columns \
              WHERE table_schema = current_schema() \
                AND table_name = $1 AND column_name = 'tier'",
            &[&pg_table],
        )
        .await
        .expect("col")
        .get(0);
    assert_eq!(n_col, 0);

    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn dual_write_mode_removal_to_gone() {
    let pool = build_pool().await;
    ensure_metadata_tables(&pool).await.expect("meta");
    create_gsi_state_table(&pool).await.expect("state");

    let mut client = pool.get().await.expect("client");
    let pg_table = "rekt_gsi_test_g9_dw";
    let table_name = "GsiTestG9Dw";

    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = '{table_name}'; \
             DELETE FROM _rektifier_tables WHERE table_name = '{table_name}'; \
             CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 data jsonb NOT NULL \
             ); \
             INSERT INTO {pg_table} (data) VALUES \
                ('{{\"id\":{{\"S\":\"a\"}},\"tier\":{{\"S\":\"bronze\"}}}}'::jsonb);"
        ))
        .await
        .expect("seed");

    let now = rekt_catalog::metadata::now_ms();
    let initial_gsi_specs = serde_json::json!([{
        "name": "by_tier",
        "partition_attr": "tier",
        "partition_type": "S",
        "partition_pg_col": "tier",
        "sort_attr": null,
        "sort_type": null,
        "sort_pg_col": null,
        "index_name": format!("{pg_table}_gsi_by_tier_idx"),
        "projection_type": "ALL",
        "projection_non_key_attrs": null,
        "mode": "dual_write",
        "serveable": false,
        "unserveable_reason": "DualWrite GSI in CREATING phase",
    }]);
    client
        .execute(
            "INSERT INTO _rektifier_tables \
                (table_name, pg_table, jsonb_col, pk_attr, pk_type, \
                 status, serveable, creation_date_ms, last_modified_at_ms, \
                 last_modified_by, gsi_specs) \
             VALUES ($1, $2, 'data', 'id', 'S', 'ACTIVE', true, $3, $3, 'test', $4::jsonb)",
            &[&table_name, &pg_table, &now, &initial_gsi_specs],
        )
        .await
        .expect("meta row");

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
    create_dual_write_gsi_in_tx(&tx, table_name, pg_table, &spec)
        .await
        .expect("orchestrator");
    tx.commit().await.expect("commit");

    // Drive to ACTIVE so we have a real index to drop.
    let gsi_id = format!("{table_name}::by_tier");
    run_lifecycle_to_active(pool.clone(), gsi_id.clone()).await;

    // Initiate removal: state row → removing_index.
    let tx = client.transaction().await.expect("tx2");
    start_dual_write_removal(&tx, &gsi_id)
        .await
        .expect("start removal");
    tx.commit().await.expect("commit removal start");

    // Run the async removal coordinator (synchronously here).
    run_removal_to_gone(&pool, &gsi_id)
        .await
        .expect("removal");

    // State row archived as gone.
    let st = fetch_state(&client, &gsi_id)
        .await
        .expect("fetch")
        .expect("present");
    assert_eq!(st.phase, GsiPhase::Gone);

    // Index gone, column gone.
    let n_idx: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pg_indexes \
              WHERE schemaname = current_schema() AND indexname = $1",
            &[&format!("{pg_table}_gsi_by_tier_idx")],
        )
        .await
        .expect("idx")
        .get(0);
    assert_eq!(n_idx, 0);
    let n_col: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM information_schema.columns \
              WHERE table_schema = current_schema() \
                AND table_name = $1 AND column_name = 'tier'",
            &[&pg_table],
        )
        .await
        .expect("col")
        .get(0);
    assert_eq!(n_col, 0);

    // Catalog entry removed from gsi_specs.
    let row = client
        .query_one(
            "SELECT gsi_specs FROM _rektifier_tables WHERE table_name = $1",
            &[&table_name],
        )
        .await
        .expect("meta row read");
    let arr: serde_json::Value = row.get(0);
    assert!(arr.as_array().expect("array").is_empty());

    client
        .batch_execute(&format!(
            "DROP TABLE {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = '{table_name}'; \
             DELETE FROM _rektifier_tables WHERE table_name = '{table_name}';"
        ))
        .await
        .expect("cleanup");
}
