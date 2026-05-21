//! PLAN-9 G8 PG-live verification. Run with:
//!   cargo test -p rekt-gsi --test verify_pg -- --ignored
//! after `just up`.

use deadpool_postgres::{Config, Runtime};
use rekt_catalog::{ensure_metadata_tables, GsiMode, GsiSpec};
use rekt_gsi::{
    create_dual_write_gsi_in_tx, create_gsi_state_table, fetch_state, run_drift_check,
    run_lifecycle_to_active, GsiPhase,
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

async fn seed_active_dual_write_gsi(
    pool: &deadpool_postgres::Pool,
    pg_table: &str,
    table_name: &str,
) {
    ensure_metadata_tables(pool).await.expect("metadata");
    create_gsi_state_table(pool).await.expect("state");

    let mut client = pool.get().await.expect("client");
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
                ('{{\"id\":{{\"S\":\"a\"}},\"tier\":{{\"S\":\"bronze\"}}}}'::jsonb), \
                ('{{\"id\":{{\"S\":\"b\"}},\"tier\":{{\"S\":\"silver\"}}}}'::jsonb);"
        ))
        .await
        .expect("seed table");

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
        .expect("seed metadata");

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

    let gsi_id = format!("{table_name}::by_tier");
    run_lifecycle_to_active(pool.clone(), gsi_id).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn drift_check_clean_table_finds_no_mismatches() {
    let pool = build_pool().await;
    let pg_table = "rekt_gsi_test_g8_clean";
    let table_name = "GsiTestG8Clean";
    seed_active_dual_write_gsi(&pool, pg_table, table_name).await;

    let gsi_id = format!("{table_name}::by_tier");
    let report = run_drift_check(&pool, &gsi_id, 100)
        .await
        .expect("verify");
    assert!(
        report.mismatches.is_empty(),
        "clean table should have no drift; got {:?}",
        report.mismatches
    );
    assert!(report.sampled >= 2);

    // State row still active; last_verified_at_ms now set.
    let client = pool.get().await.expect("client");
    let st = fetch_state(&client, &gsi_id)
        .await
        .expect("fetch")
        .expect("present");
    assert_eq!(st.phase, GsiPhase::Active);
    assert!(st.last_verified_at_ms.is_some());

    client
        .batch_execute(&format!(
            "DROP TABLE {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = '{table_name}'; \
             DELETE FROM _rektifier_tables WHERE table_name = '{table_name}';"
        ))
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn drift_check_demotes_gsi_when_column_diverges() {
    let pool = build_pool().await;
    let pg_table = "rekt_gsi_test_g8_drift";
    let table_name = "GsiTestG8Drift";
    seed_active_dual_write_gsi(&pool, pg_table, table_name).await;

    // Simulate drift: tamper with the column directly (something a
    // foreign psql writer might do — D6's "single-writer" violation).
    let client = pool.get().await.expect("client");
    client
        .batch_execute(&format!(
            "UPDATE {pg_table} SET tier = 'TAMPERED' WHERE id = 'a';"
        ))
        .await
        .expect("tamper");

    let gsi_id = format!("{table_name}::by_tier");
    let report = run_drift_check(&pool, &gsi_id, 100)
        .await
        .expect("verify");
    assert!(
        !report.mismatches.is_empty(),
        "should detect tampered row; got 0 mismatches across {} samples",
        report.sampled
    );

    // State row demoted to degraded.
    let st = fetch_state(&client, &gsi_id)
        .await
        .expect("fetch")
        .expect("present");
    assert_eq!(st.phase, GsiPhase::Degraded);
    assert!(st.error.is_some());

    // Catalog spec: serveable flipped to false.
    let row = client
        .query_one(
            "SELECT gsi_specs FROM _rektifier_tables WHERE table_name = $1",
            &[&table_name],
        )
        .await
        .expect("read");
    let arr: serde_json::Value = row.get(0);
    let entry = &arr.as_array().unwrap()[0];
    assert_eq!(entry["serveable"], serde_json::json!(false));
    assert_eq!(
        entry["unserveable_reason"],
        serde_json::json!("sampled drift detected")
    );

    client
        .batch_execute(&format!(
            "DROP TABLE {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = '{table_name}'; \
             DELETE FROM _rektifier_tables WHERE table_name = '{table_name}';"
        ))
        .await
        .expect("cleanup");
}
