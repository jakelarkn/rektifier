//! PLAN-9 G8: sampled drift detection for DualWrite-mode GSIs.
//!
//! Periodically samples N rows from each active DualWrite GSI's table
//! and compares the DualWrite column's stored value against a freshly-
//! computed extraction from the JSONB. Any mismatch demotes the GSI
//! to `degraded` (state row + catalog `serveable=false`) so dispatch
//! returns RNF until an operator rebuilds.
//!
//! Generated-mode GSIs skip this check — PG enforces the
//! JSONB→column invariant directly, so sampled JSONB-vs-column drift
//! can only be caused by PG-level corruption (out of scope for the
//! rektifier orchestrator).

use crate::state::{self, GsiPhase, GsiState, OrchestratorError};
use deadpool_postgres::Pool;

#[derive(Debug, Clone)]
pub struct DriftReport {
    pub gsi_id: String,
    pub sampled: usize,
    pub mismatches: Vec<DriftMismatch>,
}

#[derive(Debug, Clone)]
pub struct DriftMismatch {
    pub pk_text: String,
    pub column_value: Option<String>,
    pub expected_from_jsonb: Option<String>,
    pub attr: String,
}

/// Run one drift-check pass on an active DualWrite GSI. Samples up to
/// `sample_size` rows (uniformly at random) and compares each
/// DualWrite column's value against `data#>>'{attr,T}'`. Returns the
/// report; on any mismatch, also marks the state row `degraded` and
/// flips `_rektifier_tables.gsi_specs[*].serveable = false`.
pub async fn run_drift_check(
    pool: &Pool,
    gsi_id: &str,
    sample_size: i64,
) -> Result<DriftReport, OrchestratorError> {
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
    if st.phase != GsiPhase::Active && st.phase != GsiPhase::Degraded {
        // Pre-ACTIVE GSIs are not yet load-bearing; skip silently so
        // the scheduler can blanket-call this without per-phase
        // filtering.
        return Ok(DriftReport {
            gsi_id: gsi_id.into(),
            sampled: 0,
            mismatches: vec![],
        });
    }

    let specs = parse_column_specs(&st)?;
    if specs.is_empty() {
        return Ok(DriftReport {
            gsi_id: gsi_id.into(),
            sampled: 0,
            mismatches: vec![],
        });
    }

    // For each DualWrite column, sample N rows via TABLESAMPLE — fast
    // and PG-managed (no requirement that the column has any kind of
    // distribution). For tables smaller than the sample size,
    // TABLESAMPLE may return fewer rows; that's fine, the check is
    // best-effort.
    let mut mismatches: Vec<DriftMismatch> = Vec::new();
    let mut sampled_total: usize = 0;
    for spec in &specs {
        let extract = extract_sql(&spec.attr, spec.key_type);
        let column = quote(&spec.col);
        // ORDER BY random() + LIMIT is portable; TABLESAMPLE SYSTEM
        // would be faster but is page-granular and less uniform for
        // small-table workloads.
        let sql = format!(
            "SELECT \
                CAST(data AS text) AS data_text, \
                {column}::text AS col_text, \
                ({extract})::text AS expected_text \
              FROM {tbl} \
             ORDER BY random() \
             LIMIT {sample_size}",
            tbl = quote(&st.pg_table),
        );
        let rows = client.query(&sql, &[]).await?;
        sampled_total += rows.len();
        for r in rows {
            let col_text: Option<String> = r.get("col_text");
            let expected: Option<String> = r.get("expected_text");
            if col_text != expected {
                let data_text: String = r.get("data_text");
                mismatches.push(DriftMismatch {
                    pk_text: data_text,
                    column_value: col_text,
                    expected_from_jsonb: expected,
                    attr: spec.attr.clone(),
                });
            }
        }
    }

    if !mismatches.is_empty() {
        tracing::error!(
            gsi_id = %gsi_id,
            mismatches = mismatches.len(),
            sampled = sampled_total,
            "GSI drift detected — demoting to degraded"
        );
        // Mark state row + catalog spec degraded. Idempotent — re-runs
        // overwrite the same fields.
        let mut client = pool
            .get()
            .await
            .map_err(|e| OrchestratorError::Pool(format!("pool get: {e}")))?;
        let tx = client.transaction().await?;
        state::update_phase(
            &tx,
            gsi_id,
            GsiPhase::Degraded,
            Some(format!(
                "sampled drift check: {n} mismatches out of {s} samples",
                n = mismatches.len(),
                s = sampled_total
            )),
        )
        .await?;
        demote_gsi_serveable(&tx, &st.table_name, &st.gsi_name, "sampled drift detected").await?;
        tx.commit().await?;
    } else {
        // Stamp last_verified_at_ms so operators tailing the row see
        // recent verification activity.
        let _ = client
            .execute(
                "UPDATE _rektifier_gsi_state SET last_verified_at_ms = $2 WHERE gsi_id = $1",
                &[&gsi_id, &rekt_catalog::metadata::now_ms()],
            )
            .await;
    }

    Ok(DriftReport {
        gsi_id: gsi_id.into(),
        sampled: sampled_total,
        mismatches,
    })
}

/// Reverse of `flip_gsi_serveable` from G6 — demotes one GSI's
/// serveable bit in `_rektifier_tables.gsi_specs`.
async fn demote_gsi_serveable(
    tx: &deadpool_postgres::Transaction<'_>,
    table_name: &str,
    gsi_name: &str,
    reason: &str,
) -> Result<(), OrchestratorError> {
    tx.execute(
        "UPDATE _rektifier_tables SET \
            gsi_specs = ( \
                SELECT jsonb_agg(\
                    CASE WHEN elem->>'name' = $2 \
                         THEN elem || jsonb_build_object('serveable', false, 'unserveable_reason', $4::text) \
                         ELSE elem \
                    END \
                ) FROM jsonb_array_elements(gsi_specs) elem \
            ), \
            last_modified_at_ms = $3, \
            last_modified_by = 'gsi-verify' \
          WHERE table_name = $1",
        &[
            &table_name,
            &gsi_name,
            &rekt_catalog::metadata::now_ms(),
            &reason,
        ],
    )
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct ColSpec {
    col: String,
    attr: String,
    key_type: char,
}

fn parse_column_specs(st: &GsiState) -> Result<Vec<ColSpec>, OrchestratorError> {
    let arr = st.column_specs.as_array().ok_or_else(|| {
        OrchestratorError::MalformedRow {
            gsi_id: st.gsi_id.clone(),
            reason: "column_specs not an array".into(),
        }
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        let m = v.as_object().ok_or_else(|| OrchestratorError::MalformedRow {
            gsi_id: st.gsi_id.clone(),
            reason: "column_specs entry not an object".into(),
        })?;
        let col = m
            .get("col")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::MalformedRow {
                gsi_id: st.gsi_id.clone(),
                reason: "column_specs.col missing".into(),
            })?
            .to_string();
        let attr = m
            .get("attr")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::MalformedRow {
                gsi_id: st.gsi_id.clone(),
                reason: "column_specs.attr missing".into(),
            })?
            .to_string();
        let kt = m
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::MalformedRow {
                gsi_id: st.gsi_id.clone(),
                reason: "column_specs.type missing".into(),
            })?;
        let key_type = match kt {
            "S" => 'S',
            "N" => 'N',
            "B" => 'B',
            _ => {
                return Err(OrchestratorError::MalformedRow {
                    gsi_id: st.gsi_id.clone(),
                    reason: format!("unknown type `{kt}`"),
                });
            }
        };
        out.push(ColSpec {
            col,
            attr,
            key_type,
        });
    }
    Ok(out)
}

fn extract_sql(attr: &str, key_type: char) -> String {
    match key_type {
        'S' => format!("data #>> '{{{attr},S}}'"),
        'N' => format!("(data #>> '{{{attr},N}}')::numeric"),
        'B' => format!("decode(data #>> '{{{attr},B}}', 'base64')"),
        _ => unreachable!(),
    }
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
