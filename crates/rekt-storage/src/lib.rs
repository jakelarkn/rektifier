//! Storage backend trait for rektifier.
//!
//! The MVP shape is task-shaped (one method per DynamoDB op) rather than
//! SQL-shaped (`execute` / `query` / `transaction`). When the translator
//! grows past three or four ops the trait will widen — but until then we
//! don't have enough call sites to design the abstraction correctly, so
//! the methods stay literal.
//!
//! Callers always pass a [`TableShape`] (PG table name + PK / SK / JSONB
//! column names) and typed [`KeyValue`]s. The underlying PG schema is
//! operator-owned: rektifier doesn't create tables, it just reads/writes
//! against tables whose shape the operator declared in config.

use async_trait::async_trait;
use bytes::Bytes;

/// DynamoDB-declared type of a key attribute (PK or SK).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// String — PG column type `text`.
    S,
    /// Number — PG column type `numeric`. DDB N is sent as a decimal string;
    /// rektifier passes it verbatim to PG, which parses it losslessly.
    N,
    /// Binary — PG column type `bytea`.
    B,
}

/// Typed key value, ready to bind to a PG parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyValue {
    /// PG column `text`. Stored as-is.
    S(String),
    /// PG column `numeric`. Held as a decimal string; PG parses on insert.
    N(String),
    /// PG column `bytea`. Raw bytes.
    B(Bytes),
}

impl KeyValue {
    pub fn key_type(&self) -> KeyType {
        match self {
            Self::S(_) => KeyType::S,
            Self::N(_) => KeyType::N,
            Self::B(_) => KeyType::B,
        }
    }
}

/// Underlying-backend table shape. For PG: real table name + column names.
/// `sk_col = None` means hash-only — no sort-key column in the PG schema.
#[derive(Debug, Clone)]
pub struct TableShape<'a> {
    pub table: &'a str,
    pub pk_col: &'a str,
    pub sk_col: Option<&'a str>,
    pub jsonb_col: &'a str,
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("table not found: {name}")]
    TableNotFound { name: String },

    #[error("key type mismatch on `{col}`: shape declares {expected:?}, got {actual:?}")]
    KeyTypeMismatch {
        col: String,
        expected: KeyType,
        actual: KeyType,
    },

    #[error("sort key required for composite table `{name}` but caller passed None")]
    MissingSortKey { name: String },

    #[error("sort key passed for hash-only table `{name}`")]
    UnexpectedSortKey { name: String },

    #[error("storage error: {0}")]
    Other(String),
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Upsert: replace the whole `item` for a given `(pk, sk)`. Matches
    /// DynamoDB's default `PutItem` semantics (no `ConditionExpression`).
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError>;

    /// Returns `Ok(None)` when the table exists but no row matches.
    /// Returns `Err(BackendError::TableNotFound)` when the table itself
    /// is missing — the server maps that to `ResourceNotFoundException`.
    async fn get_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError>;
}
