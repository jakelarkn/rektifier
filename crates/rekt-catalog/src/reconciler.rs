//! Reconciler worker: drift detection + cache refresh.
//!
//! PLAN-10 KD3 / KD7. Runs on a configurable interval (and, post-D6, on
//! local-DDL completion). For every metadata row in a non-transitional
//! status (ACTIVE / DEGRADED / FAILED_*), the reconciler:
//!
//! 1. Reads `information_schema` for the row's `pg_table`.
//! 2. Computes the expected shape from the metadata row's pk/sk attrs
//!    and types.
//! 3. Compares — sets `serveable=true / status=ACTIVE` on match,
//!    `serveable=false / status=DEGRADED` plus a reason on mismatch.
//! 4. Persists any change back to `_rektifier_tables`.
//! 5. Builds a fresh `CatalogSnapshot` from the post-update rows and
//!    swaps it into the `TableCatalog` (single refresher per KD3).
//!
//! Transitional statuses (`CREATING` / `UPDATING` / `DELETING`) are owned
//! by the DDL worker (PLAN-10 D6 / D7); the reconciler doesn't touch
//! their `serveable` flag. It still reads them when assembling the
//! snapshot so the cache stays comprehensive.
//!
//! Subsumes `rekt_meta::verify` (deleted in this phase). The legacy
//! verifier hard-failed startup on any drift; the reconciler flags
//! per-table serveability instead so drift on table X doesn't block
//! ops on table Y.

use crate::{
    metadata, CatalogError, CatalogSnapshot, LsiSpec, TableCatalog, TableEntry, TableStatus,
};
use deadpool_postgres::Pool;
use rekt_storage::KeyType;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const SELECT_COLUMNS_SQL: &str = "\
SELECT column_name, data_type, is_generated
  FROM information_schema.columns
 WHERE table_schema = current_schema()
   AND table_name = $1
";

const SELECT_PRIMARY_KEY_SQL: &str = "\
SELECT kcu.column_name
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema    = kcu.table_schema
 WHERE tc.constraint_type = 'PRIMARY KEY'
   AND tc.table_schema    = current_schema()
   AND tc.table_name      = $1
 ORDER BY kcu.ordinal_position
";

/// Apply the reconciler's verdict to `_rektifier_tables`. The reconciler
/// touches three fields together — status, serveable, unserveable_reason
/// — plus lsi_specs (PLAN-11 L3: per-LSI serveable/reason updated in the
/// same write). All five columns move together so a partial write can't
/// leave the row in a half-consistent state.
const UPDATE_VERDICT_SQL: &str = "\
UPDATE _rektifier_tables
   SET status = $2,
       serveable = $3,
       unserveable_reason = $4,
       lsi_specs = $5::jsonb,
       last_modified_at_ms = $6,
       last_modified_by = 'reconciler'
 WHERE table_name = $1
";

/// PLAN-11 L3. Index introspection: indexname + indexdef so the
/// reconciler can confirm an LSI's btree exists with the right column
/// ordering.
const SELECT_INDEXES_SQL: &str = "\
SELECT indexname, indexdef
  FROM pg_indexes
 WHERE schemaname = current_schema()
   AND tablename = $1
";

/// PG-side row description used during introspection.
#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    data_type: String,
    is_generated: String, // "ALWAYS" | "NEVER"
}

/// Verdict on a single table — what the reconciler decided after
/// comparing metadata to live PG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileVerdict {
    pub status: TableStatus,
    pub serveable: bool,
    pub reason: Option<String>,
    /// PLAN-11 L3 / open Q4. Per-LSI verdict carried alongside the
    /// table-level verdict. The base table's `serveable` bit reflects
    /// only base-table drift; an LSI with `serveable=false` here
    /// causes Query/Scan with `IndexName` targeting it to return RNF
    /// at dispatch, while base-table requests stay served.
    pub lsis: Vec<LsiSpec>,
}

/// Periodic worker. The single `replace`-er of the catalog cache (KD3).
/// Local-DDL-driven invalidation (D6 / D7) calls `reconcile_now()`
/// directly to refresh between timer ticks.
pub struct Reconciler {
    pool: Pool,
    catalog: Arc<TableCatalog>,
    interval: Duration,
}

impl Reconciler {
    pub fn new(pool: Pool, catalog: Arc<TableCatalog>, interval: Duration) -> Self {
        Self {
            pool,
            catalog,
            interval,
        }
    }

    /// One full reconcile pass. Returns the number of rows whose stored
    /// (status, serveable, reason) verdict changed. Useful for tests
    /// and for telemetry.
    pub async fn reconcile_now(&self) -> Result<usize, CatalogError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| CatalogError::Pool(format!("pool get failed: {e}")))?;

        // Load every catalog row — even rows in transitional states are
        // included in the snapshot, just not in the verdict-write loop.
        let stored = metadata::load_snapshot(&self.pool).await?;
        let now = metadata::now_ms();

        let mut updates_written = 0usize;
        let mut next_entries: HashMap<String, Arc<TableEntry>> =
            HashMap::with_capacity(stored.len());

        for (name, entry) in stored.iter() {
            if !is_reconciler_owned(entry.status) {
                // DDL worker owns this row — pass through.
                next_entries.insert(name.clone(), entry.clone());
                continue;
            }

            let verdict = match introspect(&client, entry).await {
                Ok(v) => v,
                Err(CatalogError::Db { source }) => {
                    // A connection-level / query error: log loudly,
                    // skip the row for this cycle. We do NOT flip
                    // serveable=false on a DB blip — that would cause
                    // a serving outage on transient PG hiccups. Better
                    // to leave the prior verdict in place and retry
                    // on the next cycle.
                    tracing::warn!(
                        table = %name,
                        error = %source,
                        "reconciler skipping table due to introspection error"
                    );
                    next_entries.insert(name.clone(), entry.clone());
                    continue;
                }
                Err(e) => return Err(e),
            };

            let stored_verdict = ReconcileVerdict {
                status: entry.status,
                serveable: entry.serveable,
                reason: entry.unserveable_reason.clone(),
                lsis: entry.lsis.clone(),
            };

            if verdict != stored_verdict {
                let lsis_json = metadata::lsi_specs_to_json(&verdict.lsis);
                client
                    .execute(
                        UPDATE_VERDICT_SQL,
                        &[
                            &entry.schema.name,
                            &verdict.status.as_internal_str(),
                            &verdict.serveable,
                            &verdict.reason,
                            &lsis_json,
                            &now,
                        ],
                    )
                    .await?;
                updates_written += 1;
                if verdict.serveable && !stored_verdict.serveable {
                    tracing::info!(
                        table = %name,
                        prior_status = stored_verdict.status.as_internal_str(),
                        new_status = verdict.status.as_internal_str(),
                        "table flipped to serveable=true"
                    );
                } else if !verdict.serveable && stored_verdict.serveable {
                    tracing::error!(
                        table = %name,
                        reason = verdict.reason.as_deref().unwrap_or("unknown"),
                        new_status = verdict.status.as_internal_str(),
                        "table flipped to serveable=false"
                    );
                }
                for (prior, new) in stored_verdict.lsis.iter().zip(verdict.lsis.iter()) {
                    if prior.serveable != new.serveable {
                        if new.serveable {
                            tracing::info!(
                                table = %name,
                                lsi = %new.name,
                                "LSI flipped to serveable=true"
                            );
                        } else {
                            tracing::error!(
                                table = %name,
                                lsi = %new.name,
                                reason = new.unserveable_reason.as_deref().unwrap_or("unknown"),
                                "LSI flipped to serveable=false"
                            );
                        }
                    }
                }
            }

            let mut new_entry = (**entry).clone();
            new_entry.status = verdict.status;
            new_entry.serveable = verdict.serveable;
            new_entry.unserveable_reason = verdict.reason;
            new_entry.lsis = verdict.lsis;
            if new_entry.status != entry.status
                || new_entry.serveable != entry.serveable
                || new_entry.unserveable_reason != entry.unserveable_reason
                || new_entry.lsis != entry.lsis
            {
                new_entry.last_modified_at_ms = now;
                new_entry.last_modified_by = "reconciler".into();
            }
            next_entries.insert(name.clone(), Arc::new(new_entry));
        }

        self.catalog
            .replace(CatalogSnapshot::from_entries(next_entries));
        Ok(updates_written)
    }

    /// Spawn the periodic-timer task. Caller `.await`s the first
    /// `reconcile_now()` themselves so the cache is hot before serving.
    pub fn spawn_periodic(self: Arc<Self>) {
        if self.interval.is_zero() {
            tracing::warn!(
                "catalog reconcile_interval_ms = 0 — periodic refresh disabled \
                 (only local-DDL invalidation will refresh the cache)"
            );
            return;
        }
        let interval = self.interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // First tick fires immediately by default — skip it because
            // main.rs has already run one pass synchronously before binding.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match self.reconcile_now().await {
                    Ok(n) if n > 0 => tracing::debug!(updates_written = n, "reconciler cycle"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "reconciler cycle failed"),
                }
            }
        });
    }
}

/// Returns true for rows whose serveable flag is the reconciler's
/// concern. Transitional states (CREATING / UPDATING / DELETING) are
/// owned by the DDL worker and skipped.
fn is_reconciler_owned(status: TableStatus) -> bool {
    !matches!(
        status,
        TableStatus::Creating | TableStatus::Updating | TableStatus::Deleting
    )
}

async fn introspect(
    client: &deadpool_postgres::Object,
    entry: &TableEntry,
) -> Result<ReconcileVerdict, CatalogError> {
    let pg_table = &entry.schema.pg_table;
    let columns = fetch_columns(client, pg_table).await?;

    if columns.is_empty() {
        // No table at all: every LSI inherits the same degraded reason
        // (the column can't exist without its parent table).
        let dead_lsis = degrade_all_lsis(&entry.lsis, "parent PG table does not exist");
        return Ok(ReconcileVerdict {
            status: TableStatus::Degraded,
            serveable: false,
            reason: Some(format!("PG table `{pg_table}` does not exist")),
            lsis: dead_lsis,
        });
    }

    if let Some(reason) = check_jsonb_column(&columns, &entry.schema.jsonb_col) {
        return Ok(degraded_with_lsis(reason, &entry.lsis));
    }
    if let Some(reason) = check_key_column(&columns, &entry.schema.pk_attr, entry.schema.pk_type) {
        return Ok(degraded_with_lsis(reason, &entry.lsis));
    }
    if let (Some(sk_attr), Some(sk_type)) = (&entry.schema.sk_attr, entry.schema.sk_type) {
        if let Some(reason) = check_key_column(&columns, sk_attr, sk_type) {
            return Ok(degraded_with_lsis(reason, &entry.lsis));
        }
    }

    let pk_cols = fetch_primary_key(client, pg_table).await?;
    let expected_pk: Vec<String> = match &entry.schema.sk_attr {
        Some(sk) => vec![entry.schema.pk_attr.clone(), sk.clone()],
        None => vec![entry.schema.pk_attr.clone()],
    };
    if pk_cols != expected_pk {
        return Ok(degraded_with_lsis(
            format!(
                "primary key shape mismatch: expected {expected_pk:?}, got {pk_cols:?}"
            ),
            &entry.lsis,
        ));
    }

    // PLAN-11 L3 / open Q4. Per-LSI introspection. Base-table verdict
    // stays ACTIVE+serveable; per-LSI serveable flag set independently.
    let index_defs = if entry.lsis.is_empty() {
        HashMap::new()
    } else {
        fetch_indexes(client, pg_table).await?
    };
    let lsi_verdicts = entry
        .lsis
        .iter()
        .map(|lsi| check_lsi(lsi, &columns, &index_defs, &entry.schema.pk_attr))
        .collect();

    Ok(ReconcileVerdict {
        status: TableStatus::Active,
        serveable: true,
        reason: None,
        lsis: lsi_verdicts,
    })
}

/// Build an LSI verdict by checking the GENERATED column + composite
/// btree index. A degraded LSI flips its own `serveable` bit; the
/// table-level `serveable` is untouched (PLAN-11 open Q4).
fn check_lsi(
    lsi: &LsiSpec,
    columns: &[ColumnInfo],
    index_defs: &HashMap<String, String>,
    base_pk_col: &str,
) -> LsiSpec {
    if let Some(reason) = check_key_column(columns, &lsi.sort_pg_col, lsi.sort_type) {
        return LsiSpec {
            serveable: false,
            unserveable_reason: Some(format!("LSI `{}` column: {reason}", lsi.name)),
            ..lsi.clone()
        };
    }
    let Some(def) = index_defs.get(&lsi.index_name) else {
        return LsiSpec {
            serveable: false,
            unserveable_reason: Some(format!(
                "LSI `{}` index `{}` missing from pg_indexes",
                lsi.name, lsi.index_name
            )),
            ..lsi.clone()
        };
    };
    // The CREATE INDEX statement always contains "(<pk>, <sort_col>)";
    // a textual contains-check is good enough as a shape verifier.
    let expected_columns = format!("({base_pk_col}, {})", lsi.sort_pg_col);
    if !def.contains(&expected_columns) {
        return LsiSpec {
            serveable: false,
            unserveable_reason: Some(format!(
                "LSI `{}` index columns mismatch: expected `{expected_columns}` in indexdef, got: {def}",
                lsi.name
            )),
            ..lsi.clone()
        };
    }
    LsiSpec {
        serveable: true,
        unserveable_reason: None,
        ..lsi.clone()
    }
}

/// Mark every LSI in a slice degraded with the same reason. Used when a
/// base-table mismatch is severe enough that all LSIs are
/// transitively-unserveable too.
fn degrade_all_lsis(lsis: &[LsiSpec], reason: &str) -> Vec<LsiSpec> {
    lsis.iter()
        .map(|l| LsiSpec {
            serveable: false,
            unserveable_reason: Some(reason.into()),
            ..l.clone()
        })
        .collect()
}

fn degraded_with_lsis(reason: String, lsis: &[LsiSpec]) -> ReconcileVerdict {
    let dead = degrade_all_lsis(lsis, &reason);
    ReconcileVerdict {
        status: TableStatus::Degraded,
        serveable: false,
        reason: Some(reason),
        lsis: dead,
    }
}

fn check_jsonb_column(columns: &[ColumnInfo], jsonb_col: &str) -> Option<String> {
    let col = columns.iter().find(|c| c.name == jsonb_col)?;
    if col.data_type != "jsonb" {
        return Some(format!(
            "column `{}`: expected jsonb, got `{}`",
            jsonb_col, col.data_type
        ));
    }
    if col.is_generated == "ALWAYS" {
        return Some(format!(
            "column `{jsonb_col}`: jsonb column must not be GENERATED ALWAYS"
        ));
    }
    None
}

/// Same shape check the legacy `rekt_meta::verify` did, but returns the
/// reason string instead of constructing a typed error: the reconciler
/// stores it in `_rektifier_tables.unserveable_reason`.
fn check_key_column(columns: &[ColumnInfo], col_name: &str, key_type: KeyType) -> Option<String> {
    let Some(col) = columns.iter().find(|c| c.name == col_name) else {
        return Some(format!("column `{col_name}`: missing"));
    };
    let expected_type = match key_type {
        KeyType::S => "text",
        KeyType::N => "numeric",
        KeyType::B => "bytea",
    };
    if col.data_type != expected_type {
        return Some(format!(
            "column `{col_name}`: expected `{expected_type}`, got `{}`",
            col.data_type
        ));
    }
    if col.is_generated != "ALWAYS" {
        return Some(format!(
            "column `{col_name}`: must be GENERATED ALWAYS AS (...) STORED (is_generated = {})",
            col.is_generated
        ));
    }
    None
}

async fn fetch_columns(
    client: &deadpool_postgres::Object,
    pg_table: &str,
) -> Result<Vec<ColumnInfo>, CatalogError> {
    let rows = client.query(SELECT_COLUMNS_SQL, &[&pg_table]).await?;
    Ok(rows
        .into_iter()
        .map(|r| ColumnInfo {
            name: r.get(0),
            data_type: r.get(1),
            is_generated: r.get(2),
        })
        .collect())
}

async fn fetch_primary_key(
    client: &deadpool_postgres::Object,
    pg_table: &str,
) -> Result<Vec<String>, CatalogError> {
    let rows = client.query(SELECT_PRIMARY_KEY_SQL, &[&pg_table]).await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// PLAN-11 L3. Per-table pg_indexes lookup, returning a
/// `indexname → indexdef` map. Used to confirm each LSI's btree exists
/// with the expected (base_pk, lsi_sort_col) shape.
async fn fetch_indexes(
    client: &deadpool_postgres::Object,
    pg_table: &str,
) -> Result<HashMap<String, String>, CatalogError> {
    let rows = client.query(SELECT_INDEXES_SQL, &[&pg_table]).await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(specs: &[(&str, &str, &str)]) -> Vec<ColumnInfo> {
        specs
            .iter()
            .map(|(n, dt, ig)| ColumnInfo {
                name: (*n).into(),
                data_type: (*dt).into(),
                is_generated: (*ig).into(),
            })
            .collect()
    }

    /// Happy path: jsonb column is a real jsonb that isn't generated.
    #[test]
    fn jsonb_check_passes_for_real_jsonb() {
        let cols = cols(&[("data", "jsonb", "NEVER")]);
        assert!(check_jsonb_column(&cols, "data").is_none());
    }

    #[test]
    fn jsonb_check_flags_wrong_type() {
        let cols = cols(&[("data", "text", "NEVER")]);
        let r = check_jsonb_column(&cols, "data").unwrap();
        assert!(r.contains("expected jsonb"));
    }

    /// Generated jsonb columns aren't writable by the storage layer —
    /// that's a hard mismatch.
    #[test]
    fn jsonb_check_flags_generated_jsonb() {
        let cols = cols(&[("data", "jsonb", "ALWAYS")]);
        let r = check_jsonb_column(&cols, "data").unwrap();
        assert!(r.contains("must not be GENERATED"));
    }

    /// Key columns must be GENERATED ALWAYS (rektifier extracts them
    /// from the jsonb blob at write time).
    #[test]
    fn key_check_requires_generated() {
        let cols = cols(&[("id", "text", "NEVER")]);
        let r = check_key_column(&cols, "id", KeyType::S).unwrap();
        assert!(r.contains("must be GENERATED"));
    }

    #[test]
    fn key_check_requires_correct_type() {
        let cols = cols(&[("id", "numeric", "ALWAYS")]);
        let r = check_key_column(&cols, "id", KeyType::S).unwrap();
        assert!(r.contains("expected `text`"));
    }

    #[test]
    fn key_check_passes_when_aligned() {
        let c = cols(&[("id", "text", "ALWAYS")]);
        assert!(check_key_column(&c, "id", KeyType::S).is_none());

        let c = cols(&[("ts", "numeric", "ALWAYS")]);
        assert!(check_key_column(&c, "ts", KeyType::N).is_none());

        let c = cols(&[("payload", "bytea", "ALWAYS")]);
        assert!(check_key_column(&c, "payload", KeyType::B).is_none());
    }

    #[test]
    fn key_check_flags_missing_column() {
        let cols = cols(&[("data", "jsonb", "NEVER")]);
        let r = check_key_column(&cols, "id", KeyType::S).unwrap();
        assert!(r.contains("missing"));
    }

    /// Transitional statuses aren't the reconciler's job — the DDL
    /// worker owns those rows.
    #[test]
    fn reconciler_skips_transitional_statuses() {
        assert!(!is_reconciler_owned(TableStatus::Creating));
        assert!(!is_reconciler_owned(TableStatus::Updating));
        assert!(!is_reconciler_owned(TableStatus::Deleting));
        assert!(is_reconciler_owned(TableStatus::Active));
        assert!(is_reconciler_owned(TableStatus::Degraded));
        assert!(is_reconciler_owned(TableStatus::FailedCreating));
        assert!(is_reconciler_owned(TableStatus::FailedUpdating));
        assert!(is_reconciler_owned(TableStatus::FailedDeleting));
    }
}
