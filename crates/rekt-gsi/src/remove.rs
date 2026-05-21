//! PLAN-9 G9: GSI removal for both modes.
//!
//! Generated mode: single DDL transaction (`DROP INDEX IF EXISTS` +
//! `ALTER TABLE DROP COLUMN`). Inline; the caller's tx commits the
//! `_rektifier_tables.gsi_specs` entry deletion alongside.
//!
//! DualWrite mode: async lifecycle inverse of G2b → G6. The
//! synchronous handler transitions the state row to `removing_index`
//! and flips serveable=false in the catalog; a background worker
//! issues `DROP INDEX CONCURRENTLY`, then `ALTER TABLE DROP COLUMN`
//! (gated by the D9 refcount), then archives the state row to `gone`
//! and removes the catalog entry.

use crate::state::{self, GsiPhase, OrchestratorError};
use deadpool_postgres::{GenericClient, Pool};
use rekt_catalog::GsiSpec;

/// Generated-mode GSI removal. Runs entirely inside the caller's
/// transaction. The caller is responsible for *also* removing the
/// GsiSpec from `_rektifier_tables.gsi_specs` in the same tx — this
/// helper only handles the PG-level DDL.
pub async fn drop_generated_gsi_in_tx<C: GenericClient>(
    client: &C,
    pg_table: &str,
    gsi: &GsiSpec,
    sibling_specs: &[GsiSpec],
    base_pk_col: &str,
    base_sk_col: Option<&str>,
    lsi_cols: &[&str],
) -> Result<(), OrchestratorError> {
    let drop_index = format!(
        "DROP INDEX IF EXISTS {}",
        quote(&gsi.index_name),
    );
    client.batch_execute(&drop_index).await?;

    // D9 refcount: drop each GSI key column only when no other index
    // references it. The set of "still in use" columns covers base
    // PK/SK, all LSI sort columns, and every sibling GSI's partition/
    // sort columns.
    let referenced = build_referenced_columns(
        sibling_specs,
        base_pk_col,
        base_sk_col,
        lsi_cols,
    );
    for col in [
        Some(gsi.partition_pg_col.as_str()),
        gsi.sort_pg_col.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if !referenced.contains(col) {
            let drop_col = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                quote(pg_table),
                quote(col),
            );
            client.batch_execute(&drop_col).await?;
        }
    }
    Ok(())
}

/// DualWrite-mode synchronous removal entry. Flips state row to
/// `removing_index` so the background coordinator picks it up. The
/// caller is responsible for flipping serveable=false in
/// `_rektifier_tables.gsi_specs` so the data-plane gate kicks in
/// immediately.
pub async fn start_dual_write_removal<C: GenericClient>(
    client: &C,
    gsi_id: &str,
) -> Result<(), OrchestratorError> {
    state::update_phase(client, gsi_id, GsiPhase::RemovingIndex, None).await?;
    Ok(())
}

/// DualWrite-mode async removal coordinator. Runs to terminal phase
/// `gone` (success) or `failed`. Drives:
/// - `removing_index → DROP INDEX CONCURRENTLY → removing_column`
/// - `removing_column → DROP COLUMN (refcount-gated) → gone`
/// - Removes the GSI entry from `_rektifier_tables.gsi_specs`.
pub async fn run_removal_to_gone(
    pool: &Pool,
    gsi_id: &str,
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
    if st.phase == GsiPhase::Gone {
        return Ok(());
    }
    if !matches!(
        st.phase,
        GsiPhase::RemovingIndex | GsiPhase::RemovingColumn
    ) {
        return Err(OrchestratorError::MalformedRow {
            gsi_id: gsi_id.into(),
            reason: format!(
                "cannot run removal: phase is `{}`, expected removing_*",
                st.phase.as_str()
            ),
        });
    }

    if st.phase == GsiPhase::RemovingIndex {
        let drop_sql = format!(
            "DROP INDEX CONCURRENTLY IF EXISTS {}",
            quote(&st.index_name),
        );
        if let Err(e) = client.batch_execute(&drop_sql).await {
            state::update_phase(
                &client,
                gsi_id,
                GsiPhase::Failed,
                Some(format!("DROP INDEX CONCURRENTLY: {e}")),
            )
            .await?;
            return Err(OrchestratorError::CreateIndex(format!(
                "DROP INDEX CONCURRENTLY failed: {e}"
            )));
        }
        state::update_phase(&client, gsi_id, GsiPhase::RemovingColumn, None).await?;
    }

    // RemovingColumn: refcount-gate the DROP COLUMN against sibling
    // GSIs / LSIs / base PK-SK still using it. Read the catalog row
    // fresh, decide, then do the drop + spec removal in one tx.
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

    // Re-read gsi_specs under the lock so refcount uses current state.
    let row = tx
        .query_one(
            "SELECT pk_attr, sk_attr, lsi_specs, gsi_specs \
               FROM _rektifier_tables WHERE table_name = $1",
            &[&st.table_name],
        )
        .await?;
    let base_pk: String = row.get("pk_attr");
    let base_sk: Option<String> = row.get("sk_attr");
    let lsi_json: serde_json::Value = row.get("lsi_specs");
    let gsi_json: serde_json::Value = row.get("gsi_specs");

    // Sibling GSIs (everyone except the one being dropped).
    let cols_referenced = refcount_referenced_columns(&base_pk, base_sk.as_deref(), &lsi_json, &gsi_json, &st.gsi_name);

    // Drop the column(s) owned only by this GSI.
    let dropped_cols: Vec<String> = derive_owned_columns(&gsi_json, &st.gsi_name)
        .into_iter()
        .filter(|c| !cols_referenced.contains(c.as_str()))
        .collect();
    for col in &dropped_cols {
        let sql = format!(
            "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
            quote(&st.pg_table),
            quote(col),
        );
        tx.batch_execute(&sql).await?;
    }

    // Remove this GSI's entry from gsi_specs.
    tx.execute(
        "UPDATE _rektifier_tables SET \
            gsi_specs = COALESCE(\
                (SELECT jsonb_agg(elem) FROM jsonb_array_elements(gsi_specs) elem \
                  WHERE elem->>'name' <> $2), \
                '[]'::jsonb), \
            last_modified_at_ms = $3, \
            last_modified_by = 'gsi-removal' \
          WHERE table_name = $1",
        &[
            &st.table_name,
            &st.gsi_name,
            &rekt_catalog::metadata::now_ms(),
        ],
    )
    .await?;

    // Archive the state row.
    state::update_phase(&tx, gsi_id, GsiPhase::Gone, None).await?;
    tx.commit().await?;

    tracing::info!(
        gsi_id = %gsi_id,
        dropped_columns = ?dropped_cols,
        "GSI removal complete (gone)"
    );
    Ok(())
}

/// Spawn `run_removal_to_gone` as a background tokio task.
pub fn spawn_removal_to_gone(pool: Pool, gsi_id: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_removal_to_gone(&pool, &gsi_id).await {
            tracing::error!(gsi_id = %gsi_id, error = %e, "GSI removal failed");
        }
    })
}

fn build_referenced_columns<'a>(
    siblings: &'a [GsiSpec],
    base_pk: &'a str,
    base_sk: Option<&'a str>,
    lsi_cols: &[&'a str],
) -> std::collections::HashSet<&'a str> {
    let mut set: std::collections::HashSet<&str> = std::collections::HashSet::new();
    set.insert(base_pk);
    if let Some(s) = base_sk {
        set.insert(s);
    }
    for col in lsi_cols {
        set.insert(col);
    }
    for g in siblings {
        set.insert(g.partition_pg_col.as_str());
        if let Some(s) = g.sort_pg_col.as_deref() {
            set.insert(s);
        }
    }
    set
}

fn refcount_referenced_columns(
    base_pk: &str,
    base_sk: Option<&str>,
    lsi_json: &serde_json::Value,
    gsi_json: &serde_json::Value,
    excluding_gsi: &str,
) -> std::collections::HashSet<String> {
    let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
    set.insert(base_pk.into());
    if let Some(s) = base_sk {
        set.insert(s.into());
    }
    if let Some(arr) = lsi_json.as_array() {
        for v in arr {
            if let Some(col) = v.get("sort_pg_col").and_then(|s| s.as_str()) {
                set.insert(col.into());
            }
        }
    }
    if let Some(arr) = gsi_json.as_array() {
        for v in arr {
            if v.get("name").and_then(|n| n.as_str()) == Some(excluding_gsi) {
                continue;
            }
            if let Some(c) = v.get("partition_pg_col").and_then(|s| s.as_str()) {
                set.insert(c.into());
            }
            if let Some(c) = v.get("sort_pg_col").and_then(|s| s.as_str()) {
                set.insert(c.into());
            }
        }
    }
    set
}

fn derive_owned_columns(
    gsi_json: &serde_json::Value,
    target_gsi: &str,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(arr) = gsi_json.as_array() {
        for v in arr {
            if v.get("name").and_then(|n| n.as_str()) != Some(target_gsi) {
                continue;
            }
            if let Some(c) = v.get("partition_pg_col").and_then(|s| s.as_str()) {
                out.push(c.into());
            }
            if let Some(c) = v.get("sort_pg_col").and_then(|s| s.as_str()) {
                out.push(c.into());
            }
        }
    }
    out
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
