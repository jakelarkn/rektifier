//! PG storage for the `_rektifier_api_tokens` table.

use deadpool_postgres::{GenericClient, Pool};

const CREATE_SQL: &str = "
CREATE TABLE IF NOT EXISTS _rektifier_api_tokens (
    token_hash    bytea PRIMARY KEY,
    principal     text NOT NULL,
    enabled       bool NOT NULL DEFAULT true,
    expires_at_ms bigint,
    created_at_ms bigint NOT NULL,
    last_used_ms  bigint
);
";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("pool error: {0}")]
    Pool(String),
    #[error("postgres error: {0}")]
    Pg(#[from] tokio_postgres::Error),
}

#[derive(Debug, Clone)]
pub struct ApiTokenRow {
    pub principal: String,
    pub enabled: bool,
    pub expires_at_ms: Option<i64>,
}

pub async fn ensure_api_tokens_table(pool: &Pool) -> Result<(), StoreError> {
    let client = pool.get().await.map_err(|e| StoreError::Pool(e.to_string()))?;
    client.batch_execute(CREATE_SQL).await?;
    Ok(())
}

pub async fn insert_token_hash(
    pool: &Pool,
    token_hash: &[u8; 32],
    principal: &str,
    expires_at_ms: Option<i64>,
) -> Result<(), StoreError> {
    let client = pool.get().await.map_err(|e| StoreError::Pool(e.to_string()))?;
    let now_ms = now_ms();
    let hash_slice: &[u8] = token_hash;
    client
        .execute(
            "INSERT INTO _rektifier_api_tokens \
             (token_hash, principal, enabled, expires_at_ms, created_at_ms, last_used_ms) \
             VALUES ($1, $2, true, $3, $4, NULL)",
            &[&hash_slice, &principal, &expires_at_ms, &now_ms],
        )
        .await?;
    Ok(())
}

pub async fn fetch_token_row(
    pool: &Pool,
    token_hash: &[u8; 32],
) -> Result<Option<ApiTokenRow>, StoreError> {
    let client = pool.get().await.map_err(|e| StoreError::Pool(e.to_string()))?;
    let hash_slice: &[u8] = token_hash;
    let opt = client
        .query_opt(
            "SELECT principal, enabled, expires_at_ms \
             FROM _rektifier_api_tokens WHERE token_hash = $1",
            &[&hash_slice],
        )
        .await?;
    let Some(row) = opt else {
        return Ok(None);
    };
    Ok(Some(ApiTokenRow {
        principal: row.get(0),
        enabled: row.get(1),
        expires_at_ms: row.get(2),
    }))
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
