//! TOML config loader for rektifier.
//!
//! Parses a config file into validated, typed structures the binary
//! consumes. Pure parsing + offline validation — no Postgres calls, no
//! environment lookups. The binary owns those steps.
//!
//! Config shape (see `tests/valid.rs` for examples):
//! ```toml
//! [server]
//! listen_addr  = "127.0.0.1:9000"
//! database_url = "postgres://..."
//!
//! [limits]                       # server-level defaults; all optional
//! max_item_size_bytes      = 409600
//! max_partition_key_bytes  = 2048
//! max_sort_key_bytes       = 1024
//! max_attribute_name_bytes = 65536
//! max_nesting_depth        = 32
//!
//! [[tables]]
//! name      = "users"
//! pk_attr   = "id"
//! jsonb_col = "data"
//!
//! [[tables]]
//! name      = "documents"
//! pk_attr   = "doc_id"
//! jsonb_col = "data"
//!   [tables.limits]              # per-table override; "unlimited" removes a cap
//!   max_item_size_bytes = 16777216
//!   max_attribute_name_bytes = "unlimited"
//! ```

use rekt_storage::KeyType;
use serde::{Deserialize, Deserializer};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;

// DDB-spec defaults applied when the server-level [limits] field is omitted.
const DDB_DEFAULT_ITEM_SIZE_BYTES: u64 = 409_600; // 400 KB
const DDB_DEFAULT_PARTITION_KEY_BYTES: u64 = 2_048; // 2 KB
const DDB_DEFAULT_SORT_KEY_BYTES: u64 = 1_024; // 1 KB
const DDB_DEFAULT_ATTRIBUTE_NAME_BYTES: u64 = 65_536; // 64 KB
const DDB_DEFAULT_NESTING_DEPTH: u32 = 32;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to parse TOML: {0}")]
    ParseToml(#[from] toml::de::Error),

    #[error("invalid config: {reason}")]
    Validation { reason: String },
}

impl ConfigError {
    fn validation(reason: impl Into<String>) -> Self {
        Self::Validation {
            reason: reason.into(),
        }
    }
}

/// Fully-resolved config: defaults applied, per-table limits merged with
/// server-level defaults, validation rules checked.
#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    pub database_url: String,
}

/// Size limits. `None` = unlimited (the operator disabled that cap).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_item_size_bytes: Option<u64>,
    pub max_partition_key_bytes: Option<u64>,
    pub max_sort_key_bytes: Option<u64>,
    pub max_attribute_name_bytes: Option<u64>,
    pub max_nesting_depth: Option<u32>,
}

impl Limits {
    /// DDB-spec defaults — used as the server-level baseline when `[limits]`
    /// is omitted entirely from the config.
    pub fn ddb_defaults() -> Self {
        Self {
            max_item_size_bytes: Some(DDB_DEFAULT_ITEM_SIZE_BYTES),
            max_partition_key_bytes: Some(DDB_DEFAULT_PARTITION_KEY_BYTES),
            max_sort_key_bytes: Some(DDB_DEFAULT_SORT_KEY_BYTES),
            max_attribute_name_bytes: Some(DDB_DEFAULT_ATTRIBUTE_NAME_BYTES),
            max_nesting_depth: Some(DDB_DEFAULT_NESTING_DEPTH),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableConfig {
    /// DDB-side table name (what clients send).
    pub name: String,
    /// Postgres table name. Defaults to `name` if omitted.
    pub pg_table: String,
    pub pk_attr: String,
    pub pk_type: KeyType,
    pub sk_attr: Option<String>,
    pub sk_type: Option<KeyType>,
    pub jsonb_col: String,
    /// Fully-resolved limits: server defaults merged with per-table overrides.
    pub limits: Limits,
}

// ===== Public entry points =====================================================

pub fn load_from_path(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path)?;
    load_from_str(&text)
}

pub fn load_from_str(s: &str) -> Result<Config, ConfigError> {
    let raw: RawConfig = toml::from_str(s)?;
    raw.resolve()
}

// ===== Raw (parse-only) types ==================================================
//
// These types mirror the TOML structure literally. `Option<>` distinguishes
// "user supplied" from "use default" so we can apply DDB defaults in `resolve`
// only where the user didn't override.

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    server: RawServer,
    #[serde(default)]
    limits: Option<RawLimits>,
    #[serde(default)]
    tables: Vec<RawTable>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServer {
    listen_addr: String,
    database_url: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLimits {
    #[serde(default, deserialize_with = "de_u64_or_unlimited")]
    max_item_size_bytes: Option<LimitField<u64>>,
    #[serde(default, deserialize_with = "de_u64_or_unlimited")]
    max_partition_key_bytes: Option<LimitField<u64>>,
    #[serde(default, deserialize_with = "de_u64_or_unlimited")]
    max_sort_key_bytes: Option<LimitField<u64>>,
    #[serde(default, deserialize_with = "de_u64_or_unlimited")]
    max_attribute_name_bytes: Option<LimitField<u64>>,
    #[serde(default, deserialize_with = "de_u32_or_unlimited")]
    max_nesting_depth: Option<LimitField<u32>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTable {
    name: String,
    #[serde(default)]
    pg_table: Option<String>,
    pk_attr: String,
    #[serde(default)]
    pk_type: Option<RawKeyType>,
    #[serde(default)]
    sk_attr: Option<String>,
    #[serde(default)]
    sk_type: Option<RawKeyType>,
    jsonb_col: String,
    #[serde(default)]
    limits: Option<RawLimits>,
}

/// Wire representation of a key-type tag. Kept separate from `rekt_storage::KeyType`
/// so the config can present clearer error messages for bad input.
#[derive(Debug, Clone, Copy, Deserialize)]
enum RawKeyType {
    S,
    N,
    B,
}

impl From<RawKeyType> for KeyType {
    fn from(r: RawKeyType) -> Self {
        match r {
            RawKeyType::S => KeyType::S,
            RawKeyType::N => KeyType::N,
            RawKeyType::B => KeyType::B,
        }
    }
}

/// A single limit field as parsed from TOML: either a positive integer or
/// the literal string "unlimited".
#[derive(Debug, Clone, Copy)]
enum LimitField<N> {
    Number(N),
    Unlimited,
}

// ===== Custom deserializers for the "unlimited" literal ========================

fn de_u64_or_unlimited<'de, D>(de: D) -> Result<Option<LimitField<u64>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Number(u64),
        Tag(String),
    }
    match Option::<Repr>::deserialize(de)? {
        None => Ok(None),
        Some(Repr::Number(n)) => Ok(Some(LimitField::Number(n))),
        Some(Repr::Tag(s)) if s == "unlimited" => Ok(Some(LimitField::Unlimited)),
        Some(Repr::Tag(s)) => Err(serde::de::Error::custom(format!(
            r#"expected positive integer or the string "unlimited", got "{s}""#
        ))),
    }
}

fn de_u32_or_unlimited<'de, D>(de: D) -> Result<Option<LimitField<u32>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Number(u32),
        Tag(String),
    }
    match Option::<Repr>::deserialize(de)? {
        None => Ok(None),
        Some(Repr::Number(n)) => Ok(Some(LimitField::Number(n))),
        Some(Repr::Tag(s)) if s == "unlimited" => Ok(Some(LimitField::Unlimited)),
        Some(Repr::Tag(s)) => Err(serde::de::Error::custom(format!(
            r#"expected positive integer or the string "unlimited", got "{s}""#
        ))),
    }
}

// ===== Resolution: raw -> validated, with merges and defaults applied ==========

impl RawConfig {
    fn resolve(self) -> Result<Config, ConfigError> {
        let listen_addr: SocketAddr = self.server.listen_addr.parse().map_err(|e| {
            ConfigError::validation(format!(
                "server.listen_addr `{}` is not a valid socket address: {e}",
                self.server.listen_addr
            ))
        })?;

        if self.server.database_url.is_empty() {
            return Err(ConfigError::validation(
                "server.database_url must not be empty",
            ));
        }

        let server_limits = merge_limits(Limits::ddb_defaults(), self.limits.as_ref());

        let mut tables = Vec::with_capacity(self.tables.len());
        let mut seen_names: HashSet<String> = HashSet::new();
        let mut seen_pg_tables: HashSet<String> = HashSet::new();

        for raw in self.tables {
            // sk_attr + sk_type must be both-present or both-absent.
            match (&raw.sk_attr, raw.sk_type) {
                (Some(_), Some(_)) | (None, None) => {}
                (Some(_), None) => {
                    return Err(ConfigError::validation(format!(
                        "table `{}`: sk_attr is set but sk_type is missing — \
                         specify both or neither",
                        raw.name
                    )));
                }
                (None, Some(_)) => {
                    return Err(ConfigError::validation(format!(
                        "table `{}`: sk_type is set but sk_attr is missing — \
                         specify both or neither",
                        raw.name
                    )));
                }
            }

            if !seen_names.insert(raw.name.clone()) {
                return Err(ConfigError::validation(format!(
                    "duplicate table name `{}` — each DDB table name must be unique",
                    raw.name
                )));
            }

            let pg_table = raw.pg_table.unwrap_or_else(|| raw.name.clone());
            if !seen_pg_tables.insert(pg_table.clone()) {
                return Err(ConfigError::validation(format!(
                    "duplicate pg_table `{}` — two DDB tables would route to the \
                     same Postgres table",
                    pg_table
                )));
            }

            let pk_type = raw.pk_type.map(KeyType::from).unwrap_or(KeyType::S);
            let sk_type = raw.sk_type.map(KeyType::from);

            let limits = merge_limits(server_limits, raw.limits.as_ref());

            tables.push(TableConfig {
                name: raw.name,
                pg_table,
                pk_attr: raw.pk_attr,
                pk_type,
                sk_attr: raw.sk_attr,
                sk_type,
                jsonb_col: raw.jsonb_col,
                limits,
            });
        }

        Ok(Config {
            server: ServerConfig {
                listen_addr,
                database_url: self.server.database_url,
            },
            tables,
        })
    }
}

/// Apply an optional `RawLimits` block on top of a base `Limits`.
/// - Fields the raw block doesn't mention → inherit from base.
/// - Fields with `LimitField::Number(n)` → set to `Some(n)`.
/// - Fields with `LimitField::Unlimited` → set to `None` (cap disabled).
fn merge_limits(base: Limits, raw: Option<&RawLimits>) -> Limits {
    let Some(raw) = raw else { return base };
    Limits {
        max_item_size_bytes: apply_u64(base.max_item_size_bytes, raw.max_item_size_bytes),
        max_partition_key_bytes: apply_u64(
            base.max_partition_key_bytes,
            raw.max_partition_key_bytes,
        ),
        max_sort_key_bytes: apply_u64(base.max_sort_key_bytes, raw.max_sort_key_bytes),
        max_attribute_name_bytes: apply_u64(
            base.max_attribute_name_bytes,
            raw.max_attribute_name_bytes,
        ),
        max_nesting_depth: apply_u32(base.max_nesting_depth, raw.max_nesting_depth),
    }
}

fn apply_u64(base: Option<u64>, override_: Option<LimitField<u64>>) -> Option<u64> {
    match override_ {
        None => base,
        Some(LimitField::Number(n)) => Some(n),
        Some(LimitField::Unlimited) => None,
    }
}

fn apply_u32(base: Option<u32>, override_: Option<LimitField<u32>>) -> Option<u32> {
    match override_ {
        None => base,
        Some(LimitField::Number(n)) => Some(n),
        Some(LimitField::Unlimited) => None,
    }
}
