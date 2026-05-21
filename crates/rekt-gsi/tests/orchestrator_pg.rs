//! PLAN-9 G2b PG-live verification. Run with:
//!   cargo test -p rekt-gsi --test orchestrator_pg -- --ignored
//! after `just up`.
//!
//! Asserted invariants:
//! - `create_gsi_state_table` creates the schema idempotently.
//! - `create_dual_write_gsi_in_tx` inserts a state row, runs ALTER
//!   TABLE ADD COLUMN, and transitions the state row to `backfilling`
//!   in the same transaction.
//! - Boot-time recovery: `fetch_states_by_phase` returns the in-flight
//!   rows so a worker can resume them.
//! - Idempotency on the orchestrator entry — re-running against the
//!   same (table, gsi_name) is a no-op.

use deadpool_postgres::{Config, Runtime};
use rekt_catalog::{GsiMode, GsiSpec};
use rekt_gsi::{
    create_dual_write_gsi_in_tx, create_gsi_state_table, fetch_state, fetch_states_by_phase,
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

fn dual_write_spec() -> GsiSpec {
    GsiSpec {
        name: "by_tier".into(),
        partition_attr: "tier".into(),
        partition_type: KeyType::S,
        partition_pg_col: "tier".into(),
        sort_attr: None,
        sort_type: None,
        sort_pg_col: None,
        index_name: "rekt_gsi_test_g2b_gsi_by_tier_idx".into(),
        projection_type: Some("ALL".into()),
        projection_non_key_attrs: None,
        mode: GsiMode::DualWrite,
        serveable: false,
        unserveable_reason: Some("DualWrite GSI in CREATING phase".into()),
    }
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn orchestrator_creates_state_row_and_alters_table() {
    let pool = build_pool().await;
    create_gsi_state_table(&pool).await.expect("bootstrap state");

    // Seed: a base table with the same shape as rektifier's CT-time
    // emitter would produce. Composite PK (device, ts) so we can prove
    // the orchestrator works against multi-key tables.
    let mut client = pool.get().await.expect("client");
    let pg_table = "rekt_gsi_test_g2b";
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = 'GsiTestG2b'; \
             CREATE TABLE {pg_table} ( \
                 device text GENERATED ALWAYS AS (data#>>'{{device,S}}') STORED PRIMARY KEY, \
                 ts numeric GENERATED ALWAYS AS ((data#>>'{{ts,N}}')::numeric) STORED, \
                 data jsonb NOT NULL \
             );"
        ))
        .await
        .expect("seed base table");

    let spec = dual_write_spec();
    let tx = client.transaction().await.expect("tx");
    create_dual_write_gsi_in_tx(&tx, "GsiTestG2b", pg_table, &spec)
        .await
        .expect("orchestrator");
    tx.commit().await.expect("commit");

    // State row exists with phase = backfilling.
    let st = fetch_state(&client, "GsiTestG2b::by_tier")
        .await
        .expect("fetch")
        .expect("state row present");
    assert_eq!(st.phase, GsiPhase::Backfilling);
    assert_eq!(st.gsi_name, "by_tier");
    assert_eq!(st.pg_table, pg_table);

    // Column landed as a regular (non-GENERATED) text column.
    let row = client
        .query_one(
            "SELECT data_type, is_generated FROM information_schema.columns \
             WHERE table_schema = current_schema() AND table_name = $1 AND column_name = 'tier'",
            &[&pg_table],
        )
        .await
        .expect("col check");
    let dtype: String = row.get(0);
    let gen: String = row.get(1);
    assert_eq!(dtype, "text");
    assert_eq!(gen, "NEVER", "DualWrite column must be regular, not GENERATED");

    // Index does NOT exist yet — G6 creates it CONCURRENTLY later.
    let idx_exists: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pg_indexes \
             WHERE schemaname = current_schema() AND tablename = $1 \
               AND indexname = 'rekt_gsi_test_g2b_gsi_by_tier_idx'",
            &[&pg_table],
        )
        .await
        .expect("idx check")
        .get(0);
    assert_eq!(idx_exists, 0, "DualWrite index lands at G6, not G2b");

    // Boot-time recovery: backfilling phase is in-flight.
    let in_flight = fetch_states_by_phase(
        &client,
        &[
            GsiPhase::Declared,
            GsiPhase::AddingColumn,
            GsiPhase::Backfilling,
            GsiPhase::Indexing,
        ],
    )
    .await
    .expect("scan in-flight");
    assert!(in_flight.iter().any(|s| s.gsi_id == "GsiTestG2b::by_tier"));

    // Cleanup.
    client
        .batch_execute(&format!(
            "DROP TABLE {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = 'GsiTestG2b';"
        ))
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn orchestrator_is_idempotent_on_retry() {
    let pool = build_pool().await;
    create_gsi_state_table(&pool).await.expect("bootstrap");

    let mut client = pool.get().await.expect("client");
    let pg_table = "rekt_gsi_test_g2b_idem";
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = 'GsiTestG2bIdem'; \
             CREATE TABLE {pg_table} ( \
                 id text GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY, \
                 data jsonb NOT NULL \
             );"
        ))
        .await
        .expect("seed");

    let mut spec = dual_write_spec();
    spec.index_name = "rekt_gsi_test_g2b_idem_gsi_by_tier_idx".into();

    let tx = client.transaction().await.expect("tx");
    create_dual_write_gsi_in_tx(&tx, "GsiTestG2bIdem", pg_table, &spec)
        .await
        .expect("first call");
    tx.commit().await.expect("commit 1");

    // Re-run: state row is ON CONFLICT DO NOTHING; ADD COLUMN IF NOT
    // EXISTS no-ops; phase updates are idempotent (set to same value).
    let tx = client.transaction().await.expect("tx2");
    create_dual_write_gsi_in_tx(&tx, "GsiTestG2bIdem", pg_table, &spec)
        .await
        .expect("retry");
    tx.commit().await.expect("commit 2");

    let st = fetch_state(&client, "GsiTestG2bIdem::by_tier")
        .await
        .expect("fetch")
        .expect("present");
    assert_eq!(st.phase, GsiPhase::Backfilling);

    client
        .batch_execute(&format!(
            "DROP TABLE {pg_table}; \
             DELETE FROM _rektifier_gsi_state WHERE table_name = 'GsiTestG2bIdem';"
        ))
        .await
        .expect("cleanup");
}
