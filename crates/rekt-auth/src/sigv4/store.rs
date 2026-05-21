//! PG storage for the `_rektifier_aws_credentials` table.
//!
//! Schema (PLAN-13 D5):
//! ```sql
//! CREATE TABLE _rektifier_aws_credentials (
//!     access_key_id  text PRIMARY KEY,
//!     secret_aes_enc bytea NOT NULL,
//!     secret_nonce   bytea NOT NULL,
//!     principal      text NOT NULL,
//!     enabled        bool NOT NULL DEFAULT true,
//!     created_at_ms  bigint NOT NULL,
//!     last_used_ms   bigint
//! );
//! ```
//!
//! Rotation = INSERT new + DELETE old; the cache TTL bounds propagation.
//! No live AWS IAM lookup — see PLAN-13 D5.

use crate::crypto::{decrypt, encrypt, Ciphertext, CryptoError};
use deadpool_postgres::{GenericClient, Pool};
use tokio_postgres::types::ToSql;

const CREATE_SQL: &str = "
CREATE TABLE IF NOT EXISTS _rektifier_aws_credentials (
    access_key_id  text PRIMARY KEY,
    secret_aes_enc bytea NOT NULL,
    secret_nonce   bytea NOT NULL,
    principal      text NOT NULL,
    enabled        bool NOT NULL DEFAULT true,
    created_at_ms  bigint NOT NULL,
    last_used_ms   bigint
);
";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("pool error: {0}")]
    Pool(String),
    #[error("postgres error: {0}")]
    Pg(#[from] tokio_postgres::Error),
    #[error("crypto error: {0}")]
    Crypto(#[from] CryptoError),
    #[error("malformed credential row for AKID {0}: {1}")]
    Malformed(String, &'static str),
}

#[derive(Debug, Clone)]
pub struct CredentialRow {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub principal: String,
    pub enabled: bool,
}

/// Idempotent. Run at boot, before the HTTP listener binds (PLAN-13 D14).
pub async fn ensure_credentials_table(pool: &Pool) -> Result<(), StoreError> {
    let client = pool.get().await.map_err(|e| StoreError::Pool(e.to_string()))?;
    client.batch_execute(CREATE_SQL).await?;
    Ok(())
}

/// Insert a new credential row. The plaintext `secret_access_key` is
/// AES-GCM encrypted with the supplied 32-byte data-encryption key.
pub async fn insert_credential_row(
    pool: &Pool,
    aes_key: &[u8; 32],
    row: &CredentialRow,
) -> Result<(), StoreError> {
    let ct = encrypt(aes_key, row.secret_access_key.as_bytes())?;
    let client = pool.get().await.map_err(|e| StoreError::Pool(e.to_string()))?;
    let now_ms = now_ms();
    let nonce_bytes: &[u8] = &ct.nonce;
    let params: [&(dyn ToSql + Sync); 7] = [
        &row.access_key_id,
        &ct.ciphertext,
        &nonce_bytes,
        &row.principal,
        &row.enabled,
        &now_ms,
        &None::<i64>,
    ];
    client
        .execute(
            "INSERT INTO _rektifier_aws_credentials \
             (access_key_id, secret_aes_enc, secret_nonce, principal, enabled, created_at_ms, last_used_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &params,
        )
        .await?;
    Ok(())
}

/// Fetch a credential by AKID. Returns `None` if the row does not
/// exist; the verifier maps that to a negative-cache entry.
pub async fn fetch_credential_row(
    pool: &Pool,
    aes_key: &[u8; 32],
    akid: &str,
) -> Result<Option<CredentialRow>, StoreError> {
    let client = pool.get().await.map_err(|e| StoreError::Pool(e.to_string()))?;
    let opt = client
        .query_opt(
            "SELECT secret_aes_enc, secret_nonce, principal, enabled \
             FROM _rektifier_aws_credentials WHERE access_key_id = $1",
            &[&akid],
        )
        .await?;
    let Some(row) = opt else {
        return Ok(None);
    };
    let enc: Vec<u8> = row.get(0);
    let nonce_vec: Vec<u8> = row.get(1);
    let principal: String = row.get(2);
    let enabled: bool = row.get(3);
    if nonce_vec.len() != 12 {
        return Err(StoreError::Malformed(akid.into(), "secret_nonce must be 12 bytes"));
    }
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&nonce_vec);
    let ct = Ciphertext {
        nonce,
        ciphertext: enc,
    };
    let pt = decrypt(aes_key, &ct)?;
    let secret_access_key = String::from_utf8(pt)
        .map_err(|_| StoreError::Malformed(akid.into(), "decrypted secret is not UTF-8"))?;
    Ok(Some(CredentialRow {
        access_key_id: akid.to_string(),
        secret_access_key,
        principal,
        enabled,
    }))
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    dur.as_millis() as i64
}
