//! PLAN-11 L3 PG-live tests: catalog round-trip for LSI specs and the
//! reconciler's per-LSI verdict logic.
//!
//! Run with `cargo test -p rekt-catalog --test lsi_reconciler_pg -- --ignored`
//! after `just up`.
//!
//! Asserted invariants:
//! - `ensure_metadata_tables` brings `_rektifier_tables.lsi_specs` into
//!   existence (idempotent on re-issue).
//! - LSI specs survive a write→read round-trip through `_rektifier_tables`.
//! - The reconciler verifies per-LSI column + index presence and flips
//!   the individual LSI's `serveable` bit on drift, leaving the base
//!   table's `serveable` bit untouched (open Q4 decision).

use deadpool_postgres::{Config, Pool, Runtime};
use rekt_catalog::{
    ensure_metadata_tables, lsi_specs_to_json, LsiSpec, Reconciler, TableCatalog, TableStatus,
};
use rekt_storage::KeyType;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
}

fn pool() -> Pool {
    let mut cfg = Config::new();
    cfg.url = Some(database_url());
    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("create pool")
}

/// Build a composite-key table with a matching `_rektifier_tables` row
/// declaring one LSI on `priority` (numeric). Returns (pool, pg_table,
/// table_name).
async fn setup_lsi_table(table_name: &str) -> (Pool, String, String) {
    let pool = pool();
    ensure_metadata_tables(&pool).await.expect("bootstrap");

    let pg_table = format!("rekt_t_{table_name}");
    let lsi_idx = format!("{pg_table}_lsi_by_priority_idx");

    let client = pool.get().await.expect("pool get");
    client
        .batch_execute(&format!(
            "DELETE FROM _rektifier_tables WHERE table_name = '{table_name}';
             DROP TABLE IF EXISTS {pg_table};"
        ))
        .await
        .expect("cleanup");

    // Mirror what L2's create_table_sql emits.
    client
        .batch_execute(&format!(
            "CREATE TABLE {pg_table} (
                data jsonb NOT NULL,
                device text GENERATED ALWAYS AS ((data #>> '{{device,S}}')) STORED NOT NULL,
                ts numeric GENERATED ALWAYS AS (((data #>> '{{ts,N}}')::numeric)) STORED NOT NULL,
                priority numeric GENERATED ALWAYS AS (((data #>> '{{priority,N}}')::numeric)) STORED,
                PRIMARY KEY (device, ts)
            );
             CREATE INDEX {lsi_idx} ON {pg_table} (device, priority);"
        ))
        .await
        .expect("create pg table");

    // Insert the catalog row with one LSI spec.
    let lsi_specs = lsi_specs_to_json(&[LsiSpec {
        name: "by_priority".into(),
        sort_attr: "priority".into(),
        sort_type: KeyType::N,
        sort_pg_col: "priority".into(),
        index_name: lsi_idx,
        projection_type: Some("ALL".into()),
        projection_non_key_attrs: None,
        // serveable=false initially — let the reconciler confirm it.
        serveable: false,
        unserveable_reason: Some("pre-reconcile".into()),
    }]);
    client
        .execute(
            "INSERT INTO _rektifier_tables (
                table_name, pg_table, jsonb_col, pk_attr, pk_type,
                sk_attr, sk_type, status, serveable,
                creation_date_ms, last_modified_at_ms, last_modified_by,
                tags, billing_mode, provisioned_rcu, provisioned_wcu, lsi_specs
             ) VALUES (
                $1, $2, 'data', 'device', 'S',
                'ts', 'N', 'ACTIVE', true,
                1700000000000, 1700000000000, 'test',
                '{}'::jsonb, NULL, NULL, NULL, $3::jsonb
             )",
            &[&table_name, &pg_table, &lsi_specs],
        )
        .await
        .expect("insert catalog row");

    (pool, pg_table, table_name.to_string())
}

async fn teardown(pool: &Pool, pg_table: &str, table_name: &str) {
    let client = pool.get().await.expect("pool get");
    let _ = client
        .batch_execute(&format!(
            "DELETE FROM _rektifier_tables WHERE table_name = '{table_name}';
             DROP TABLE IF EXISTS {pg_table};"
        ))
        .await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn reconciler_marks_lsi_serveable_when_column_and_index_present() {
    let (pool, pg_table, table_name) = setup_lsi_table("rekt_lsi_recon_ok").await;
    let catalog = Arc::new(TableCatalog::empty());
    let reconciler = Reconciler::new(pool.clone(), catalog.clone(), Duration::from_secs(60));

    reconciler.reconcile_now().await.expect("reconcile");

    let entry = catalog.get(&table_name).expect("entry in catalog");
    assert!(entry.serveable, "base table must be serveable");
    assert_eq!(entry.status, TableStatus::Active);
    assert_eq!(entry.lsis.len(), 1);
    assert!(
        entry.lsis[0].serveable,
        "LSI must be serveable after reconcile; reason: {:?}",
        entry.lsis[0].unserveable_reason
    );
    assert!(entry.lsis[0].unserveable_reason.is_none());

    teardown(&pool, &pg_table, &table_name).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn reconciler_demotes_lsi_when_index_missing_keeps_table_serveable() {
    let (pool, pg_table, table_name) =
        setup_lsi_table("rekt_lsi_recon_no_idx").await;
    // Drop the LSI index — simulates drift (operator hand-edit / PG
    // restore from a stale dump).
    let client = pool.get().await.expect("pool");
    client
        .batch_execute(&format!(
            "DROP INDEX {pg_table}_lsi_by_priority_idx;"
        ))
        .await
        .expect("drop lsi index");

    let catalog = Arc::new(TableCatalog::empty());
    let reconciler = Reconciler::new(pool.clone(), catalog.clone(), Duration::from_secs(60));
    reconciler.reconcile_now().await.expect("reconcile");

    let entry = catalog.get(&table_name).expect("entry");
    // PLAN-11 open Q4 decision: per-LSI granularity. Base-table reads
    // are unaffected by LSI drift.
    assert!(
        entry.serveable,
        "base table must remain serveable when only the LSI index is missing"
    );
    assert_eq!(entry.status, TableStatus::Active);
    assert_eq!(entry.lsis.len(), 1);
    assert!(
        !entry.lsis[0].serveable,
        "LSI must flip to serveable=false on missing index"
    );
    let reason = entry.lsis[0]
        .unserveable_reason
        .as_deref()
        .unwrap_or("");
    assert!(
        reason.contains("missing"),
        "reason should mention missing index; got: {reason}"
    );

    teardown(&pool, &pg_table, &table_name).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn reconciler_demotes_lsi_when_sort_column_missing() {
    let (pool, pg_table, table_name) =
        setup_lsi_table("rekt_lsi_recon_no_col").await;
    // Drop the LSI column. With PG, dropping the GENERATED column also
    // drops the index that referenced it, so this exercises both arms
    // of the verdict at once.
    let client = pool.get().await.expect("pool");
    client
        .batch_execute(&format!(
            "ALTER TABLE {pg_table} DROP COLUMN priority;"
        ))
        .await
        .expect("drop lsi column");

    let catalog = Arc::new(TableCatalog::empty());
    let reconciler = Reconciler::new(pool.clone(), catalog.clone(), Duration::from_secs(60));
    reconciler.reconcile_now().await.expect("reconcile");

    let entry = catalog.get(&table_name).expect("entry");
    assert!(entry.serveable, "base table remains serveable");
    assert_eq!(entry.lsis.len(), 1);
    assert!(!entry.lsis[0].serveable);
    let reason = entry.lsis[0]
        .unserveable_reason
        .as_deref()
        .unwrap_or("");
    assert!(
        reason.contains("missing") || reason.contains("LSI"),
        "reason should mention LSI/missing; got: {reason}"
    );

    teardown(&pool, &pg_table, &table_name).await;
}

