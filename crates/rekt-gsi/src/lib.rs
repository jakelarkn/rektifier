//! Async GSI lifecycle orchestrator (PLAN-9 G2b → G9).
//!
//! Owns the DualWrite-mode GSI state machine described in PLAN-9 D5:
//! `declared → adding_column → backfilling → indexing → active` (plus
//! removal + failure transitions). Generated-mode GSIs do not flow
//! through this crate — their lifecycle is collapsed into the
//! CreateTable DDL transaction (rekt-ddl).
//!
//! Layering: this crate depends on `rekt-catalog` (state-row schema +
//! catalog refresh hooks) and `rekt-storage` (KeyType for SQL emission).
//! It does *not* depend on `rekt-translator` or `rekt-storage-libpq`
//! (it issues raw SQL through the connection pool directly).
//!
//! The orchestrator is reentrant: each lifecycle step is one PG
//! transaction that updates the state row before committing the
//! observable side effect, so a crash mid-step is recovered by reading
//! the persisted phase on boot.

pub mod backfill;
pub mod state;

pub use backfill::{run_backfill, BackfillConfig};
pub use state::{
    create_gsi_state_table, fetch_state, fetch_states_by_phase, insert_state_row, update_phase,
    GsiPhase, GsiState, OrchestratorError,
};
pub use rekt_catalog::GsiMode;

use deadpool_postgres::{GenericClient, Pool};
use rekt_catalog::{GsiSpec, TableCatalog};

/// Orchestrator handle. Cheap to clone — holds an `Arc<Pool>`-shaped
/// pool reference and a catalog handle for snapshot refreshes.
#[derive(Clone)]
pub struct GsiOrchestrator {
    pool: Pool,
    catalog: std::sync::Arc<TableCatalog>,
}

impl GsiOrchestrator {
    pub fn new(pool: Pool, catalog: std::sync::Arc<TableCatalog>) -> Self {
        Self { pool, catalog }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub fn catalog(&self) -> &TableCatalog {
        &self.catalog
    }
}

/// G2b. Synchronous transition for an `UpdateTable.GlobalSecondaryIndexUpdates.
/// Create`. Runs `declared → adding_column → backfilling` inside the
/// caller's transaction (so the new GsiSpec + state row + ADD COLUMN
/// land atomically). The backfill worker (G5) picks up from
/// `backfilling`; index creation (G6) follows once backfill completes.
///
/// `pg_table` is the base table's PG identifier. `gsi` is the
/// DualWrite-mode GsiSpec the caller has just constructed (mode
/// must be DualWrite — the orchestrator panics otherwise to catch
/// caller bugs early; the wire-level guarantee is D17).
pub async fn create_dual_write_gsi_in_tx<C: GenericClient>(
    client: &C,
    table_name: &str,
    pg_table: &str,
    gsi: &GsiSpec,
) -> Result<(), OrchestratorError> {
    assert!(
        matches!(gsi.mode, rekt_catalog::GsiMode::DualWrite),
        "create_dual_write_gsi_in_tx called with Generated-mode GSI; \
         violates PLAN-9 D17"
    );
    let gsi_id = state::gsi_id(table_name, &gsi.name);
    let now = rekt_catalog::metadata::now_ms();

    state::insert_state_row(
        client,
        &GsiState {
            gsi_id: gsi_id.clone(),
            table_name: table_name.into(),
            gsi_name: gsi.name.clone(),
            pg_table: pg_table.into(),
            column_specs: state::column_specs_json(gsi),
            index_name: gsi.index_name.clone(),
            phase: GsiPhase::Declared,
            last_pk_copied: None,
            declared_at_ms: now,
            activated_at_ms: None,
            last_verified_at_ms: None,
            last_modified_at_ms: now,
            error: None,
        },
    )
    .await?;

    // ALTER TABLE ADD COLUMN <col> <type> NULL. Catalog-only change on
    // PG; near-instant. Idempotent — `ADD COLUMN IF NOT EXISTS` no-ops
    // on retry. ALTER TABLE inside a tx is legal and briefly holds
    // ACCESS EXCLUSIVE until commit.
    let part_type = key_type_sql(gsi.partition_type);
    let sort_clause = match (&gsi.sort_pg_col, gsi.sort_type) {
        (Some(col), Some(t)) => format!(
            ", ADD COLUMN IF NOT EXISTS {col} {ty} NULL",
            ty = key_type_sql(t)
        ),
        _ => String::new(),
    };
    let alter_sql = format!(
        "ALTER TABLE {pg_table} \
         ADD COLUMN IF NOT EXISTS {part_col} {part_type} NULL{sort_clause}",
        part_col = gsi.partition_pg_col,
    );
    client
        .batch_execute(&alter_sql)
        .await
        .map_err(|e| OrchestratorError::AlterTable(format!("ADD COLUMN failed: {e}")))?;

    state::update_phase(client, &gsi_id, GsiPhase::AddingColumn, None).await?;
    state::update_phase(client, &gsi_id, GsiPhase::Backfilling, None).await?;

    tracing::info!(
        table = %table_name,
        gsi = %gsi.name,
        "GSI orchestrator: declared → adding_column → backfilling \
         (ADD COLUMN landed, awaiting backfill)"
    );

    Ok(())
}

/// SQL type literal for a GSI key column (matches DDL emission's
/// per-KeyType mapping).
fn key_type_sql(t: rekt_storage::KeyType) -> &'static str {
    match t {
        rekt_storage::KeyType::S => "text",
        rekt_storage::KeyType::N => "numeric",
        rekt_storage::KeyType::B => "bytea",
    }
}
