//! Storage backend trait for rektifier.
//!
//! The MVP shape is task-shaped (one method per DynamoDB op) rather than
//! SQL-shaped (`execute` / `query` / `transaction`). When the translator
//! grows past three or four ops the trait will widen — but until then we
//! don't have enough call sites to design the abstraction correctly, so
//! the methods stay literal.
//!
//! Every backing table has a composite primary key `(pk, sk)`. For
//! hash-only DynamoDB tables, callers pass `sk = None`; the impl maps that
//! to the empty string at the SQL boundary, so the storage layer stays
//! oblivious to whether the table is "really" composite.

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("table not found: {name}")]
    TableNotFound { name: String },

    #[error("invalid table name: {name}")]
    InvalidTableName { name: String },

    #[error("storage error: {0}")]
    Other(String),
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Upsert: replace the whole `item` for a given `(pk, sk)`. Matches
    /// DynamoDB's default `PutItem` semantics (no `ConditionExpression`).
    /// For hash-only tables, pass `sk = None`.
    async fn put_item_raw(
        &self,
        table: &str,
        pk: &str,
        sk: Option<&str>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError>;

    /// Returns `Ok(None)` when the table exists but no row matches `(pk, sk)`.
    /// Returns `Err(BackendError::TableNotFound)` when the table itself is
    /// missing — the server maps that to `ResourceNotFoundException`.
    /// For hash-only tables, pass `sk = None`.
    async fn get_item_raw(
        &self,
        table: &str,
        pk: &str,
        sk: Option<&str>,
    ) -> Result<Option<serde_json::Value>, BackendError>;
}
