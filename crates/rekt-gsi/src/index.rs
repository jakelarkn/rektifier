//! PLAN-9 G6: CREATE INDEX CONCURRENTLY + active promotion.
//!
//! Runs after the backfill worker has filled the DualWrite column on
//! every existing row. The index build itself is online (takes
//! `SHARE UPDATE EXCLUSIVE` rather than `ACCESS EXCLUSIVE`) so live
//! writes continue to flow through G3's dual-write SQL during the
//! build. On success the state row promotes to `active` and the
//! catalog's `gsi_specs[*].serveable` flips to true; the next
//! reconciler pass refreshes the in-memory snapshot, after which
//! Query requests against the GSI succeed.

use crate::state::{self, GsiPhase, OrchestratorError};
use deadpool_postgres::Pool;
use std::time::Duration;

/// Maximum time we'll wait for `pg_index.indisvalid` to flip true.
/// `CREATE INDEX CONCURRENTLY` scans the table twice and processes the
/// build in stages; on a billion-row table this can take hours. The
/// default ceiling is intentionally generous so the worker doesn't
/// give up on a legitimate build; operators with smaller tables can
/// tune via `IndexConfig::poll_timeout`.
const DEFAULT_INDEX_POLL_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24);

/// Poll cadence on `pg_index.indisvalid`. Cheap query against a system
/// catalog; not load-bearing on cost.
const INDEX_POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy)]
pub struct IndexConfig {
    pub poll_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            poll_timeout: DEFAULT_INDEX_POLL_TIMEOUT,
            poll_interval: INDEX_POLL_INTERVAL,
        }
    }
}

/// Run the CREATE INDEX CONCURRENTLY + promote-to-active phase. Caller
/// must have driven the state row to `backfilling` (typically by
/// running `run_backfill` first).
///
/// Steps:
/// 1. Transition `backfilling → indexing` in its own short tx.
/// 2. Issue `CREATE INDEX CONCURRENTLY ... (cols)` outside any tx.
/// 3. Poll `pg_index.indisvalid` until VALID (or timeout).
/// 4. Transition `indexing → active` and flip the catalog's
///    `gsi_specs[*].serveable = true` in one short tx so the next
///    reconciler pass surfaces the GSI as queryable.
pub async fn run_index_creation(
    pool: &Pool,
    gsi_id: &str,
    cfg: IndexConfig,
) -> Result<(), OrchestratorError> {
    let client = pool
        .get()
        .await
        .map_err(|e| OrchestratorError::Pool(format!("pool get: {e}")))?;
    let st = state::fetch_state(&client, gsi_id).await?.ok_or_else(|| {
        OrchestratorError::MalformedRow {
            gsi_id: gsi_id.into(),
            reason: "no state row".into(),
        }
    })?;
    if st.phase == GsiPhase::Active {
        // Idempotent: already done.
        return Ok(());
    }
    if st.phase != GsiPhase::Backfilling && st.phase != GsiPhase::Indexing {
        return Err(OrchestratorError::MalformedRow {
            gsi_id: gsi_id.into(),
            reason: format!(
                "cannot run index creation: phase is `{}`, expected `backfilling` or `indexing`",
                st.phase.as_str()
            ),
        });
    }

    // 1) Move to indexing — committed so a crash here resumes
    //    correctly (the CREATE INDEX CONCURRENTLY below cannot run
    //    inside a transaction, so phase progression must commit first).
    if st.phase != GsiPhase::Indexing {
        state::update_phase(&client, gsi_id, GsiPhase::Indexing, None).await?;
    }

    // 2) Build the column list for the index. The state row carries
    //    `column_specs` as a JSON array; the order is partition then
    //    sort, matching the catalog's GsiSpec emission.
    let cols = parse_index_cols(&st)?;
    let pg_table = quote(&st.pg_table);
    let idx_name = quote(&st.index_name);
    let cols_sql = cols
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    let create_sql = format!(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {pg_table} ({cols_sql})"
    );
    // `CREATE INDEX CONCURRENTLY` cannot run inside a transaction
    // block. `batch_execute` runs each statement as its own
    // implicit-tx, which is what we want here.
    if let Err(e) = client.batch_execute(&create_sql).await {
        // INVALID index sometimes left behind; per D4, drop + retry
        // once, then mark `failed`.
        tracing::warn!(
            gsi_id = %gsi_id,
            error = %e,
            "CREATE INDEX CONCURRENTLY failed; dropping and retrying once"
        );
        let drop_sql = format!("DROP INDEX IF EXISTS {idx_name}");
        let _ = client.batch_execute(&drop_sql).await;
        if let Err(e2) = client.batch_execute(&create_sql).await {
            let msg = format!("CREATE INDEX CONCURRENTLY failed twice: {e2}");
            state::update_phase(&client, gsi_id, GsiPhase::Failed, Some(msg.clone())).await?;
            return Err(OrchestratorError::CreateIndex(msg));
        }
    }

    // 3) Poll pg_index.indisvalid. CONCURRENTLY builds may finish their
    //    SQL command long before the index is fully valid, so polling
    //    is the only reliable signal.
    let deadline = std::time::Instant::now() + cfg.poll_timeout;
    loop {
        let row = client
            .query_opt(
                "SELECT indisvalid FROM pg_index i \
                   JOIN pg_class c ON c.oid = i.indexrelid \
                  WHERE c.relname = $1 \
                    AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())",
                &[&st.index_name],
            )
            .await?;
        match row {
            Some(r) => {
                let valid: bool = r.get(0);
                if valid {
                    break;
                }
            }
            None => {
                // Index disappeared between CREATE and poll? Operator
                // intervention or a concurrent DROP. Mark failed.
                let msg = format!(
                    "index `{}` vanished after CREATE INDEX CONCURRENTLY",
                    st.index_name
                );
                state::update_phase(&client, gsi_id, GsiPhase::Failed, Some(msg.clone())).await?;
                return Err(OrchestratorError::CreateIndex(msg));
            }
        }
        if std::time::Instant::now() >= deadline {
            let msg = format!(
                "index `{}` did not reach indisvalid=true within {:?}",
                st.index_name, cfg.poll_timeout
            );
            state::update_phase(&client, gsi_id, GsiPhase::Failed, Some(msg.clone())).await?;
            return Err(OrchestratorError::CreateIndex(msg));
        }
        tokio::time::sleep(cfg.poll_interval).await;
    }

    // 4) Promote state row + catalog spec in one tx. The
    //    `_rektifier_tables` UPDATE is a read-modify-write of the
    //    jsonb gsi_specs entry; the per-table advisory lock matches
    //    rekt-ddl's lock to avoid racing UpdateTable mutations.
    let mut client = pool
        .get()
        .await
        .map_err(|e| OrchestratorError::Pool(format!("pool get 2: {e}")))?;
    let tx = client.transaction().await?;
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtext('rektifier::table::' || $1))",
        &[&st.table_name],
    )
    .await?;
    state::update_phase(&tx, gsi_id, GsiPhase::Active, None).await?;
    flip_gsi_serveable(&tx, &st.table_name, &st.gsi_name).await?;
    tx.commit().await?;

    tracing::info!(
        table = %st.table_name,
        gsi = %st.gsi_name,
        "GSI lifecycle complete: indexing → active (serveable=true)"
    );
    Ok(())
}

/// Update one GSI's `serveable=true` + clear `unserveable_reason`
/// inside `_rektifier_tables.gsi_specs`. Reads the current jsonb,
/// rewrites the matching entry, writes it back. The reconciler's
/// existence check still gates on actual PG state, but having the
/// catalog row pre-flipped means the next reconcile pass surfaces the
/// GSI as queryable without delay.
async fn flip_gsi_serveable(
    tx: &deadpool_postgres::Transaction<'_>,
    table_name: &str,
    gsi_name: &str,
) -> Result<(), OrchestratorError> {
    // jsonb manipulation done in PG: for each array element whose
    // `name` matches, set `serveable=true` and `unserveable_reason=null`.
    // Equivalent to a Rust-side parse + serialize but avoids two
    // round-trips.
    let n = tx
        .execute(
            "UPDATE _rektifier_tables SET \
                gsi_specs = ( \
                    SELECT jsonb_agg(\
                        CASE WHEN elem->>'name' = $2 \
                             THEN elem || jsonb_build_object('serveable', true, 'unserveable_reason', null) \
                             ELSE elem \
                        END \
                    ) FROM jsonb_array_elements(gsi_specs) elem \
                ), \
                last_modified_at_ms = $3, \
                last_modified_by = 'gsi-orchestrator' \
              WHERE table_name = $1",
            &[
                &table_name,
                &gsi_name,
                &rekt_catalog::metadata::now_ms(),
            ],
        )
        .await?;
    if n == 0 {
        return Err(OrchestratorError::MalformedRow {
            gsi_id: format!("{table_name}::{gsi_name}"),
            reason: "table row missing from _rektifier_tables".into(),
        });
    }
    Ok(())
}

fn parse_index_cols(st: &crate::state::GsiState) -> Result<Vec<String>, OrchestratorError> {
    let arr = st.column_specs.as_array().ok_or_else(|| {
        OrchestratorError::MalformedRow {
            gsi_id: st.gsi_id.clone(),
            reason: "column_specs not array".into(),
        }
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        let s = v
            .as_object()
            .and_then(|m| m.get("col"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::MalformedRow {
                gsi_id: st.gsi_id.clone(),
                reason: "column_specs entry missing `col`".into(),
            })?;
        out.push(s.to_string());
    }
    Ok(out)
}

fn quote(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('"');
    for c in ident.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}
