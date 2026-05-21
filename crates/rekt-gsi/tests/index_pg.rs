//! PLAN-9 G6 PG-live verification. Run with:
//!   cargo test -p rekt-gsi --test index_pg -- --ignored
//! after `just up`.
//!
//! Covers the full DualWrite lifecycle: orchestrator (G2b) → backfill
//! (G5) → CREATE INDEX CONCURRENTLY → active (G6). The lifecycle
//! coordinator (`run_lifecycle_to_active`) drives backfill then index
//! creation; on success the state row phase is `active` and the
//! `_rektifier_tables.gsi_specs[*].serveable` flips to true.

use deadpool_postgres::{Config, Runtime};
use rekt_catalog::{ensure_metadata_tables, GsiMode, GsiSpec};
use rekt_gsi::{
    create_dual_write_gsi_in_tx, create_gsi_state_table, fetch_state, run_lifecycle_to_active,
    GsiPhase,
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
async fn full_lifecycle_drives_to_active() {
    let pool = build_pool().await;
    ensure_metadata_tables(&pool).await.expect("metadata");
    create_gsi_state_table(&pool).await.expect("state");

    let mut client = pool.get().await.expect("client");
    let pg_table = "rekt_gsi_test_g6";
    let table_name = "GsiTestG6";

    // Seed a populated table + matching `_rektifier_tables` row so the
    // catalog-flip step has a row to update.
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
                ('{{\"id\":{{\"S\":\"b\"}},\"tier\":{{\"S\":\"silver\"}}}}'::jsonb), \
                ('{{\"id\":{{\"S\":\"c\"}},\"tier\":{{\"S\":\"gold\"}}}}'::jsonb);"
        ))
        .await
        .expect("seed table");

    // Seed the metadata row with an empty gsi_specs; the orchestrator
    // does NOT touch this (the dispatcher does), so we mimic the
    // post-UpdateTable state where the GSI entry has already been
    // appended with serveable=false.
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

    // Run the orchestrator's synchronous part (ADD COLUMN → backfilling).
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

    // Run the lifecycle coordinator (backfill + CREATE INDEX
    // CONCURRENTLY + active promotion).
    let gsi_id = format!("{table_name}::by_tier");
    run_lifecycle_to_active(pool.clone(), gsi_id.clone()).await;

    // State row: phase = active, error = null.
    let st = fetch_state(&client, &gsi_id)
        .await
        .expect("fetch")
        .expect("present");
    assert_eq!(st.phase, GsiPhase::Active, "phase must be active; state={st:?}");
    assert!(
        st.activated_at_ms.is_some(),
        "activated_at_ms must be set on active phase"
    );

    // Index exists + is valid.
    let row = client
        .query_one(
            "SELECT indisvalid FROM pg_index i \
               JOIN pg_class c ON c.oid = i.indexrelid \
              WHERE c.relname = $1",
            &[&format!("{pg_table}_gsi_by_tier_idx")],
        )
        .await
        .expect("pg_index lookup");
    let indisvalid: bool = row.get(0);
    assert!(indisvalid, "index must be VALID after G6");
    // PLAN-12 D1: DualWrite-mode CREATE INDEX CONCURRENTLY also
    // emits INCLUDE (data) so the GSI Query can use index-only scans.
    let row = client
        .query_one(
            "SELECT indexdef FROM pg_indexes \
              WHERE schemaname = current_schema() AND indexname = $1",
            &[&format!("{pg_table}_gsi_by_tier_idx")],
        )
        .await
        .expect("pg_indexes lookup");
    let indexdef: String = row.get(0);
    assert!(
        indexdef.contains("INCLUDE (data)"),
        "DualWrite GSI index must INCLUDE (data); got: {indexdef}"
    );

    // Catalog row: gsi_specs[0].serveable now true.
    let row = client
        .query_one(
            "SELECT gsi_specs FROM _rektifier_tables WHERE table_name = $1",
            &[&table_name],
        )
        .await
        .expect("read metadata");
    let specs: serde_json::Value = row.get(0);
    let arr = specs.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    let entry = &arr[0];
    assert_eq!(
        entry["serveable"], serde_json::json!(true),
        "gsi_specs[0].serveable must flip to true after G6 active promotion"
    );
    assert!(
        entry["unserveable_reason"].is_null(),
        "unserveable_reason must clear when flipping to serveable=true"
    );

    // Indexed query should hit the new btree.
    let rows = client
        .query(
            &format!("SELECT id FROM {pg_table} WHERE tier = 'gold'"),
            &[],
        )
        .await
        .expect("indexed query");
    let got: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(got, vec!["c"]);

    client
        .batch_execute(&format!(
            "DROP TABLE {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = '{table_name}'; \
             DELETE FROM _rektifier_tables WHERE table_name = '{table_name}';"
        ))
        .await
        .expect("cleanup");
}
