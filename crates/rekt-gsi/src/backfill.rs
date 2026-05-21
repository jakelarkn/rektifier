//! PLAN-9 G5: chunked backfill worker.
//!
//! One tokio task per DualWrite-mode GSI in `backfilling` phase. The
//! SQL is a row-limited UPDATE driven by a PK-ordered CTE; each chunk
//! is one transaction. The worker persists `last_pk_copied` after each
//! committed chunk so a rektifier crash resumes from the next row.
//!
//! Idempotent on `<col> IS NULL` — a row already populated by the
//! live dual-write path (G3) is skipped. If a concurrent dual-write
//! PUT lands on a row the worker just picked up, both paths compute
//! the same value from the same JSONB so the second writer's
//! UPDATE is a no-op or wins the race; correctness is preserved.

use crate::state::{self, GsiPhase, GsiState, OrchestratorError};
use deadpool_postgres::Pool;
use std::time::Duration;

/// Knobs per backfill run. Defaults match the plan's D3 recommendation.
#[derive(Debug, Clone, Copy)]
pub struct BackfillConfig {
    pub chunk_size: i64,
    pub throttle: Duration,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            chunk_size: 1000,
            throttle: Duration::from_millis(100),
        }
    }
}

/// Run the backfill loop for one GSI to completion. Returns when
/// `RETURNING` yields zero rows (no remaining NULL column values).
/// On error, the state row is marked `failed` with the error string;
/// the caller may choose to retry by resetting phase to `backfilling`.
pub async fn run_backfill(
    pool: &Pool,
    gsi_id: &str,
    cfg: BackfillConfig,
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
    if st.phase != GsiPhase::Backfilling {
        // Idempotent: another worker already finished or moved past
        // backfilling. Bail without error.
        return Ok(());
    }

    let composite = has_composite_pk(&client, &st.pg_table).await?;
    let cols = parse_column_specs(&st)?;

    // In-memory cursor that advances every chunk regardless of NULL
    // state. Without this, a sparse row whose JSONB has no GSI attr
    // would keep its column NULL after the UPDATE (the expression
    // yields NULL) and the next chunk's `WHERE tier IS NULL` would
    // pick it up again — infinite loop. We seed from the persisted
    // `last_pk_copied` so a crash mid-backfill resumes correctly.
    let mut cursor: ResumeCursor = if composite {
        ResumeCursor::Composite(
            st.last_pk_copied
                .as_ref()
                .and_then(|v| v.as_object())
                .and_then(|obj| {
                    let p = obj.get("pk").and_then(|v| v.as_str()).map(str::to_string);
                    let s = obj.get("sk").and_then(|v| v.as_str()).map(str::to_string);
                    match (p, s) {
                        (Some(p), Some(s)) => Some((p, s)),
                        _ => None,
                    }
                }),
        )
    } else {
        ResumeCursor::Simple(
            st.last_pk_copied
                .as_ref()
                .and_then(|v| v.as_str())
                .map(str::to_string),
        )
    };

    loop {
        let n = run_chunk(pool, &st, &cols, &mut cursor, cfg.chunk_size).await?;
        if n == 0 {
            break;
        }
        if !cfg.throttle.is_zero() {
            tokio::time::sleep(cfg.throttle).await;
        }
    }

    // Backfill complete — caller (G6 worker) takes it from here. The
    // state row stays at `backfilling` until the index-build worker
    // promotes it via `update_phase(.., Indexing)`.
    tracing::info!(
        gsi_id = %st.gsi_id,
        "GSI backfill complete (RETURNING empty); awaiting G6 CREATE INDEX CONCURRENTLY"
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct ColSpec {
    col: String,
    attr: String,
    key_type: char, // 'S' | 'N' | 'B'
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
        let obj = v.as_object().ok_or_else(|| OrchestratorError::MalformedRow {
            gsi_id: st.gsi_id.clone(),
            reason: "column_specs entry not an object".into(),
        })?;
        let col = obj
            .get("col")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::MalformedRow {
                gsi_id: st.gsi_id.clone(),
                reason: "column_specs.col missing".into(),
            })?
            .to_string();
        let attr = obj
            .get("attr")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::MalformedRow {
                gsi_id: st.gsi_id.clone(),
                reason: "column_specs.attr missing".into(),
            })?
            .to_string();
        let kt_str = obj
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrchestratorError::MalformedRow {
                gsi_id: st.gsi_id.clone(),
                reason: "column_specs.type missing".into(),
            })?;
        let key_type = match kt_str {
            "S" => 'S',
            "N" => 'N',
            "B" => 'B',
            other => {
                return Err(OrchestratorError::MalformedRow {
                    gsi_id: st.gsi_id.clone(),
                    reason: format!("column_specs.type unknown: `{other}`"),
                })
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
        _ => unreachable!("validated in parse_column_specs"),
    }
}

async fn has_composite_pk(
    client: &deadpool_postgres::Object,
    pg_table: &str,
) -> Result<bool, OrchestratorError> {
    // PK shape: composite when the table has > 1 PK column.
    let row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM information_schema.key_column_usage \
             WHERE table_schema = current_schema() AND table_name = $1 \
               AND constraint_name LIKE '%pkey'",
            &[&pg_table],
        )
        .await?;
    let n: i64 = row.get(0);
    Ok(n > 1)
}

#[derive(Debug, Clone)]
enum ResumeCursor {
    Simple(Option<String>),
    Composite(Option<(String, String)>),
}

/// Run one backfill chunk. Returns the number of rows touched (0 →
/// loop exits). The CTE filters on the first DualWrite column being
/// NULL AND on `pk > cursor`; the cursor advances every chunk so
/// sparse rows (where the GSI attr is absent and the extraction
/// yields NULL after UPDATE) don't infinite-loop the worker.
async fn run_chunk(
    pool: &Pool,
    st: &GsiState,
    cols: &[ColSpec],
    cursor: &mut ResumeCursor,
    chunk_size: i64,
) -> Result<u64, OrchestratorError> {
    if cols.is_empty() {
        return Ok(0);
    }
    let mut client = pool
        .get()
        .await
        .map_err(|e| OrchestratorError::Pool(format!("pool get: {e}")))?;

    let pk_cols = pk_column_names(&client, &st.pg_table).await?;
    if pk_cols.is_empty() {
        return Err(OrchestratorError::MalformedRow {
            gsi_id: st.gsi_id.clone(),
            reason: format!(
                "table `{}` has no PRIMARY KEY columns — backfill cannot proceed",
                st.pg_table
            ),
        });
    }

    // SET clause: every DualWrite column recomputed from JSONB.
    let set_clause: String = cols
        .iter()
        .map(|c| format!("{} = {}", quote(&c.col), extract_sql(&c.attr, c.key_type)))
        .collect::<Vec<_>>()
        .join(", ");
    let null_filter = format!("{} IS NULL", quote(&cols[0].col));

    let tx = client
        .transaction()
        .await
        .map_err(OrchestratorError::from)?;

    let n = match cursor {
        ResumeCursor::Composite(prev) if pk_cols.len() >= 2 => {
            let (pk, sk) = (&pk_cols[0], &pk_cols[1]);
            let resume_pred = if prev.is_some() {
                format!(
                    "({pk}::text, {sk}::text) > ($1, $2)",
                    pk = quote(pk),
                    sk = quote(sk)
                )
            } else {
                "TRUE".to_string()
            };
            let pk_sql = format!(
                "WITH chunk AS ( \
                     SELECT {pk}, {sk} FROM {tbl} \
                     WHERE {resume_pred} AND {null_filter} \
                     ORDER BY {pk}, {sk} \
                     LIMIT {chunk_size} \
                 ) \
                 UPDATE {tbl} \
                    SET {set_clause} \
                   FROM chunk \
                  WHERE {tbl}.{pk} = chunk.{pk} AND {tbl}.{sk} = chunk.{sk} \
                  RETURNING {tbl}.{pk}::text AS pk, {tbl}.{sk}::text AS sk",
                pk = quote(pk),
                sk = quote(sk),
                tbl = quote(&st.pg_table),
            );
            let rows = match prev.clone() {
                Some((p, s)) => tx.query(&pk_sql, &[&p, &s]).await?,
                None => tx.query(&pk_sql, &[]).await?,
            };
            let last = rows.last().map(|r| {
                let pk_s: String = r.get("pk");
                let sk_s: String = r.get("sk");
                (pk_s, sk_s)
            });
            let n = rows.len() as u64;
            if let Some((pk_v, sk_v)) = last {
                let j = serde_json::json!({"pk": pk_v, "sk": sk_v});
                tx.execute(
                    "UPDATE _rektifier_gsi_state \
                        SET last_pk_copied = $2::jsonb, last_modified_at_ms = $3 \
                      WHERE gsi_id = $1",
                    &[&st.gsi_id, &j, &rekt_catalog::metadata::now_ms()],
                )
                .await?;
                *prev = Some((pk_v.clone(), sk_v.clone()));
                *cursor = ResumeCursor::Composite(Some((pk_v, sk_v)));
            }
            n
        }
        _ => {
            let prev = match cursor {
                ResumeCursor::Simple(p) => p.clone(),
                ResumeCursor::Composite(_) => None,
            };
            let pk = &pk_cols[0];
            let resume_pred = if prev.is_some() {
                format!("{pk}::text > $1", pk = quote(pk))
            } else {
                "TRUE".to_string()
            };
            let pk_sql = format!(
                "WITH chunk AS ( \
                     SELECT {pk} FROM {tbl} \
                     WHERE {resume_pred} AND {null_filter} \
                     ORDER BY {pk} \
                     LIMIT {chunk_size} \
                 ) \
                 UPDATE {tbl} \
                    SET {set_clause} \
                   FROM chunk \
                  WHERE {tbl}.{pk} = chunk.{pk} \
                  RETURNING {tbl}.{pk}::text AS pk",
                pk = quote(pk),
                tbl = quote(&st.pg_table),
            );
            let rows = match prev {
                Some(p) => tx.query(&pk_sql, &[&p]).await?,
                None => tx.query(&pk_sql, &[]).await?,
            };
            let last_pk = rows.last().map(|r| r.get::<_, String>("pk"));
            let n = rows.len() as u64;
            if let Some(pk_v) = last_pk {
                let j = serde_json::json!(&pk_v);
                tx.execute(
                    "UPDATE _rektifier_gsi_state \
                        SET last_pk_copied = $2::jsonb, last_modified_at_ms = $3 \
                      WHERE gsi_id = $1",
                    &[&st.gsi_id, &j, &rekt_catalog::metadata::now_ms()],
                )
                .await?;
                *cursor = ResumeCursor::Simple(Some(pk_v));
            }
            n
        }
    };
    tx.commit().await?;
    tracing::debug!(gsi_id = %st.gsi_id, rows = n, "backfill chunk");
    Ok(n)
}

async fn pk_column_names(
    client: &deadpool_postgres::Object,
    pg_table: &str,
) -> Result<Vec<String>, OrchestratorError> {
    // Order by ordinal_position so composite PKs come back (pk, sk).
    let rows = client
        .query(
            "SELECT kc.column_name \
               FROM information_schema.key_column_usage kc \
               JOIN information_schema.table_constraints tc \
                 ON tc.constraint_name = kc.constraint_name \
                AND tc.table_schema = kc.table_schema \
              WHERE kc.table_schema = current_schema() \
                AND kc.table_name = $1 \
                AND tc.constraint_type = 'PRIMARY KEY' \
              ORDER BY kc.ordinal_position",
            &[&pg_table],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

fn quote(ident: &str) -> String {
    // Same convention as rekt-storage-libpq::types::quote_ident — wrap
    // identifiers in double quotes and escape embedded quotes.
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
