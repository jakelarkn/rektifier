//! `_rektifier_gsi_state` row CRUD + phase enum (PLAN-9 D5).
//!
//! One row per DualWrite-mode GSI in the system. Generated-mode GSIs do
//! not get a row — they live entirely in `_rektifier_tables.gsi_specs`.

use deadpool_postgres::GenericClient;
use rekt_catalog::{metadata::now_ms, GsiSpec};

#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("pool error: {0}")]
    Pool(String),
    #[error("database error: {source}")]
    Db {
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("ALTER TABLE failed: {0}")]
    AlterTable(String),
    #[error("CREATE INDEX failed: {0}")]
    CreateIndex(String),
    #[error("malformed state row `{gsi_id}`: {reason}")]
    MalformedRow { gsi_id: String, reason: String },
    #[error("unknown phase `{0}`")]
    UnknownPhase(String),
}

impl From<tokio_postgres::Error> for OrchestratorError {
    fn from(source: tokio_postgres::Error) -> Self {
        Self::Db { source }
    }
}

/// State-machine phases per PLAN-9 D5. Persisted as the lowercase
/// variant name in `_rektifier_gsi_state.phase`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GsiPhase {
    Declared,
    AddingColumn,
    Backfilling,
    Indexing,
    Active,
    RemovingIndex,
    RemovingColumn,
    Gone,
    Degraded,
    Failed,
}

impl GsiPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Declared => "declared",
            Self::AddingColumn => "adding_column",
            Self::Backfilling => "backfilling",
            Self::Indexing => "indexing",
            Self::Active => "active",
            Self::RemovingIndex => "removing_index",
            Self::RemovingColumn => "removing_column",
            Self::Gone => "gone",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "declared" => Some(Self::Declared),
            "adding_column" => Some(Self::AddingColumn),
            "backfilling" => Some(Self::Backfilling),
            "indexing" => Some(Self::Indexing),
            "active" => Some(Self::Active),
            "removing_index" => Some(Self::RemovingIndex),
            "removing_column" => Some(Self::RemovingColumn),
            "gone" => Some(Self::Gone),
            "degraded" => Some(Self::Degraded),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// True for any non-terminal phase — boot-time scan picks these up
    /// and resumes the corresponding workers.
    pub fn is_in_flight(self) -> bool {
        matches!(
            self,
            Self::Declared
                | Self::AddingColumn
                | Self::Backfilling
                | Self::Indexing
                | Self::RemovingIndex
                | Self::RemovingColumn
        )
    }
}

#[derive(Debug, Clone)]
pub struct GsiState {
    pub gsi_id: String,
    pub table_name: String,
    pub gsi_name: String,
    pub pg_table: String,
    pub column_specs: serde_json::Value,
    pub index_name: String,
    pub phase: GsiPhase,
    pub last_pk_copied: Option<serde_json::Value>,
    pub declared_at_ms: i64,
    pub activated_at_ms: Option<i64>,
    pub last_verified_at_ms: Option<i64>,
    pub last_modified_at_ms: i64,
    pub error: Option<String>,
}

pub fn gsi_id(table_name: &str, gsi_name: &str) -> String {
    format!("{table_name}::{gsi_name}")
}

/// Materialize the (col, type, path) entries the orchestrator needs to
/// populate at backfill time. Encoded as a JSONB array on the state row
/// so reads are self-describing.
pub fn column_specs_json(gsi: &GsiSpec) -> serde_json::Value {
    let mut arr = vec![serde_json::json!({
        "col":  gsi.partition_pg_col,
        "type": rekt_catalog::metadata::key_type_str(gsi.partition_type),
        "attr": gsi.partition_attr,
    })];
    if let (Some(col), Some(t), Some(attr)) =
        (&gsi.sort_pg_col, gsi.sort_type, &gsi.sort_attr)
    {
        arr.push(serde_json::json!({
            "col":  col,
            "type": rekt_catalog::metadata::key_type_str(t),
            "attr": attr,
        }));
    }
    serde_json::Value::Array(arr)
}

// Wrapped in a session-level advisory lock so parallel test runs
// (multiple processes each issuing `CREATE TABLE IF NOT EXISTS`) don't
// race on `pg_type_typname_nsp_index` — PG's IF NOT EXISTS check
// happens *before* the implicit row-type insert, leaving a tiny
// window where two concurrent transactions can both proceed past
// the existence check and then both fail at the type-insert step.
// The advisory lock key is rektifier-namespaced (matches the
// `rektifier::table::` family used elsewhere).
const CREATE_STATE_SQL: &str = "\
SELECT pg_advisory_lock(hashtext('rektifier::bootstrap::_rektifier_gsi_state'));

CREATE TABLE IF NOT EXISTS _rektifier_gsi_state (
    gsi_id              text PRIMARY KEY,
    table_name          text NOT NULL,
    gsi_name            text NOT NULL,
    pg_table            text NOT NULL,
    column_specs        jsonb NOT NULL,
    index_name          text NOT NULL,
    phase               text NOT NULL,
    last_pk_copied      jsonb,
    declared_at_ms      bigint NOT NULL,
    activated_at_ms     bigint,
    last_verified_at_ms bigint,
    last_modified_at_ms bigint NOT NULL,
    error               text,
    UNIQUE (table_name, gsi_name)
);

CREATE INDEX IF NOT EXISTS _rektifier_gsi_state_phase_idx
    ON _rektifier_gsi_state (phase);

SELECT pg_advisory_unlock(hashtext('rektifier::bootstrap::_rektifier_gsi_state'));
";

const INSERT_STATE_SQL: &str = "\
INSERT INTO _rektifier_gsi_state
    (gsi_id, table_name, gsi_name, pg_table, column_specs, index_name,
     phase, last_pk_copied,
     declared_at_ms, activated_at_ms, last_verified_at_ms,
     last_modified_at_ms, error)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (gsi_id) DO NOTHING";

const UPDATE_PHASE_SQL: &str = "\
UPDATE _rektifier_gsi_state
   SET phase = $2,
       activated_at_ms = CASE WHEN $2 = 'active' THEN $4 ELSE activated_at_ms END,
       last_modified_at_ms = $4,
       error = $3
 WHERE gsi_id = $1";

const SELECT_STATE_SQL: &str = "\
SELECT gsi_id, table_name, gsi_name, pg_table, column_specs, index_name,
       phase, last_pk_copied,
       declared_at_ms, activated_at_ms, last_verified_at_ms,
       last_modified_at_ms, error
  FROM _rektifier_gsi_state
 WHERE gsi_id = $1";

const SELECT_STATES_BY_PHASE_SQL: &str = "\
SELECT gsi_id, table_name, gsi_name, pg_table, column_specs, index_name,
       phase, last_pk_copied,
       declared_at_ms, activated_at_ms, last_verified_at_ms,
       last_modified_at_ms, error
  FROM _rektifier_gsi_state
 WHERE phase = ANY($1::text[])";

/// Bootstrap the state table. Safe to call on every boot.
pub async fn create_gsi_state_table(
    pool: &deadpool_postgres::Pool,
) -> Result<(), OrchestratorError> {
    let client = pool
        .get()
        .await
        .map_err(|e| OrchestratorError::Pool(format!("pool get: {e}")))?;
    client.batch_execute(CREATE_STATE_SQL).await?;
    Ok(())
}

/// Insert a new state row. Idempotent on `gsi_id`.
pub async fn insert_state_row<C: GenericClient>(
    client: &C,
    s: &GsiState,
) -> Result<(), OrchestratorError> {
    client
        .execute(
            INSERT_STATE_SQL,
            &[
                &s.gsi_id,
                &s.table_name,
                &s.gsi_name,
                &s.pg_table,
                &s.column_specs,
                &s.index_name,
                &s.phase.as_str(),
                &s.last_pk_copied,
                &s.declared_at_ms,
                &s.activated_at_ms,
                &s.last_verified_at_ms,
                &s.last_modified_at_ms,
                &s.error,
            ],
        )
        .await?;
    Ok(())
}

/// Update phase + (optional) error message. `activated_at_ms` is
/// stamped automatically by the SQL when phase = active.
pub async fn update_phase<C: GenericClient>(
    client: &C,
    gsi_id: &str,
    phase: GsiPhase,
    error: Option<String>,
) -> Result<(), OrchestratorError> {
    let now = now_ms();
    client
        .execute(
            UPDATE_PHASE_SQL,
            &[&gsi_id, &phase.as_str(), &error, &now],
        )
        .await?;
    Ok(())
}

pub async fn fetch_state<C: GenericClient>(
    client: &C,
    gsi_id: &str,
) -> Result<Option<GsiState>, OrchestratorError> {
    let row = client.query_opt(SELECT_STATE_SQL, &[&gsi_id]).await?;
    row.map(row_to_state).transpose()
}

/// Boot-time recovery — return every state row whose phase is in the
/// supplied set. Typical usage: scan for in-flight phases on startup
/// and resume the corresponding workers.
pub async fn fetch_states_by_phase<C: GenericClient>(
    client: &C,
    phases: &[GsiPhase],
) -> Result<Vec<GsiState>, OrchestratorError> {
    let phase_strs: Vec<&str> = phases.iter().map(|p| p.as_str()).collect();
    let rows = client
        .query(SELECT_STATES_BY_PHASE_SQL, &[&phase_strs])
        .await?;
    rows.into_iter().map(row_to_state).collect()
}

fn row_to_state(r: tokio_postgres::Row) -> Result<GsiState, OrchestratorError> {
    let phase_str: String = r.get("phase");
    let phase = GsiPhase::parse(&phase_str).ok_or_else(|| {
        OrchestratorError::UnknownPhase(phase_str.clone())
    })?;
    Ok(GsiState {
        gsi_id: r.get("gsi_id"),
        table_name: r.get("table_name"),
        gsi_name: r.get("gsi_name"),
        pg_table: r.get("pg_table"),
        column_specs: r.get("column_specs"),
        index_name: r.get("index_name"),
        phase,
        last_pk_copied: r.get("last_pk_copied"),
        declared_at_ms: r.get("declared_at_ms"),
        activated_at_ms: r.get("activated_at_ms"),
        last_verified_at_ms: r.get("last_verified_at_ms"),
        last_modified_at_ms: r.get("last_modified_at_ms"),
        error: r.get("error"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_round_trip() {
        for &p in &[
            GsiPhase::Declared,
            GsiPhase::AddingColumn,
            GsiPhase::Backfilling,
            GsiPhase::Indexing,
            GsiPhase::Active,
            GsiPhase::RemovingIndex,
            GsiPhase::RemovingColumn,
            GsiPhase::Gone,
            GsiPhase::Degraded,
            GsiPhase::Failed,
        ] {
            assert_eq!(GsiPhase::parse(p.as_str()), Some(p));
        }
        assert_eq!(GsiPhase::parse("nonsense"), None);
    }

    #[test]
    fn in_flight_predicate() {
        assert!(GsiPhase::Declared.is_in_flight());
        assert!(GsiPhase::Backfilling.is_in_flight());
        assert!(GsiPhase::RemovingIndex.is_in_flight());
        assert!(!GsiPhase::Active.is_in_flight());
        assert!(!GsiPhase::Gone.is_in_flight());
        assert!(!GsiPhase::Failed.is_in_flight());
        assert!(!GsiPhase::Degraded.is_in_flight());
    }
}
