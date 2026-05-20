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

// DDB-spec defaults for batch-op quantity caps. Configurable via
// `[batch_limits]`; raising past these moves away from DDB parity
// (see `COMPATIBILITY_NOTES.md`).
const DDB_DEFAULT_BATCH_GET_MAX_KEYS: u32 = 100;
const DDB_DEFAULT_BATCH_WRITE_MAX_REQUESTS: u32 = 25;
const DDB_DEFAULT_TRANSACT_GET_MAX_ITEMS: u32 = 100;
const DDB_DEFAULT_TRANSACT_WRITE_MAX_ITEMS: u32 = 100;

/// Default per-request hard timeout. The DDB CLI client times out at
/// 60s; we set a 30s budget so a hung backend produces a 408 well
/// before the client gives up.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

/// Default reconciler cadence: every 30s a fresh `_rektifier_tables`
/// snapshot is built and swapped into the catalog cache. PLAN-10 KD3
/// — also serves as the single cache-refresh mechanism. `0` disables
/// the periodic timer (only local-DDL invalidation refreshes; debugging
/// only, see KD4 caveat).
const DEFAULT_CATALOG_RECONCILE_INTERVAL_MS: u64 = 30_000;

/// Default deadpool max pool size. Matches the previous hard-coded
/// value so operators upgrading without a `[pg]` block see no change.
const DEFAULT_PG_MAX_POOL_SIZE: u32 = 16;
/// Fail fast when the pool is saturated instead of blocking the
/// handler. The TimeoutLayer (Obs-2) catches anything that escapes
/// this, but explicit pool-level timeout produces a clearer error
/// (`PoolExhausted` vs generic `RequestTimeout`).
const DEFAULT_PG_WAIT_TIMEOUT_MS: u64 = 5_000;
/// Fail fast when opening a new connection — defends against PG being
/// reachable-but-unresponsive.
const DEFAULT_PG_CREATE_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PG_RECYCLE_TIMEOUT_MS: u64 = 1_000;

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
    pub batch_limits: BatchLimits,
    pub pg: PgConfig,
    pub catalog: CatalogConfig,
}

/// Tunables for the runtime table catalog (PLAN-10 KD4). Sensible
/// defaults — operators can omit the `[catalog]` block entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogConfig {
    /// Reconciler cadence in milliseconds. Doubles as the cache-refresh
    /// interval (the reconciler is the single refresher per KD3). `0`
    /// disables the periodic timer; only local-DDL invalidation will
    /// refresh — debugging mode, dangerous in prod.
    pub reconcile_interval_ms: u64,
    /// When a rektifier-issued CreateTable / UpdateTable / DeleteTable
    /// completes, trigger an immediate reconcile so the cache reflects
    /// the new state before the handler returns. PLAN-10 KD3.
    pub invalidate_on_local_ddl: bool,
}

impl CatalogConfig {
    pub fn defaults() -> Self {
        Self {
            reconcile_interval_ms: DEFAULT_CATALOG_RECONCILE_INTERVAL_MS,
            invalidate_on_local_ddl: true,
        }
    }
}

/// Postgres pool + retry config. All fields have DDB-/deadpool-
/// reasonable defaults so an upgrading deployment can omit the
/// `[pg]` section entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgConfig {
    pub max_pool_size: u32,
    pub wait_timeout_ms: u64,
    pub create_timeout_ms: u64,
    pub recycle_timeout_ms: u64,
    pub recycling_method: RecyclingMethod,
    pub retry: PgRetryConfig,
}

/// Mirrors `deadpool_postgres::RecyclingMethod` but kept in this
/// crate so `rekt-config` doesn't depend on `deadpool-postgres`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecyclingMethod {
    /// Don't validate on recycle. Fast but stale connections after
    /// a PG restart surface on the next query rather than
    /// transparently reconnecting.
    Fast,
    /// Issue a trivial query (`SELECT 1`) on recycle. ~1ms per
    /// recycle; transparent PG restart.
    Verified,
    /// `Verified` + `DISCARD ALL` to drop session state. Heavier;
    /// recommended only when callers rely on session-state isolation.
    Clean,
}

/// Retry policy for transient PG errors (08*, 57P0[1-5], 40001,
/// 40P01) and `BackendError::PoolExhausted` / `ConnectFailed`.
/// Wired by Obs-4; parsed here so Obs-3's config diff lands once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgRetryConfig {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub jitter_pct: u32,
}

impl PgConfig {
    pub fn defaults() -> Self {
        Self {
            max_pool_size: DEFAULT_PG_MAX_POOL_SIZE,
            wait_timeout_ms: DEFAULT_PG_WAIT_TIMEOUT_MS,
            create_timeout_ms: DEFAULT_PG_CREATE_TIMEOUT_MS,
            recycle_timeout_ms: DEFAULT_PG_RECYCLE_TIMEOUT_MS,
            recycling_method: RecyclingMethod::Fast,
            retry: PgRetryConfig::defaults(),
        }
    }
}

impl PgRetryConfig {
    pub fn defaults() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 50,
            max_backoff_ms: 1_000,
            jitter_pct: 25,
        }
    }
}

/// Server-level quantity caps on the batch ops. Defaults match DDB
/// exactly; raising past the default means rektifier accepts requests
/// DDB would reject (Lenient divergence — see `COMPATIBILITY_NOTES.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    pub batch_get_max_keys: u32,
    pub batch_write_max_requests: u32,
    pub transact_get_max_items: u32,
    pub transact_write_max_items: u32,
}

impl BatchLimits {
    pub fn ddb_defaults() -> Self {
        Self {
            batch_get_max_keys: DDB_DEFAULT_BATCH_GET_MAX_KEYS,
            batch_write_max_requests: DDB_DEFAULT_BATCH_WRITE_MAX_REQUESTS,
            transact_get_max_items: DDB_DEFAULT_TRANSACT_GET_MAX_ITEMS,
            transact_write_max_items: DDB_DEFAULT_TRANSACT_WRITE_MAX_ITEMS,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    pub database_url: String,
    /// Hard per-request timeout applied by tower-http's TimeoutLayer.
    /// Defaults to 30s. Operators can override via
    /// `server.request_timeout_ms` in TOML.
    pub request_timeout_ms: u64,
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
    batch_limits: Option<RawBatchLimits>,
    #[serde(default)]
    pg: Option<RawPg>,
    #[serde(default)]
    catalog: Option<RawCatalog>,
    #[serde(default)]
    tables: Vec<RawTable>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCatalog {
    #[serde(default)]
    reconcile_interval_ms: Option<u64>,
    #[serde(default)]
    invalidate_on_local_ddl: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPg {
    #[serde(default)]
    max_pool_size: Option<u32>,
    #[serde(default)]
    wait_timeout_ms: Option<u64>,
    #[serde(default)]
    create_timeout_ms: Option<u64>,
    #[serde(default)]
    recycle_timeout_ms: Option<u64>,
    #[serde(default)]
    recycling_method: Option<String>,
    #[serde(default)]
    retry: Option<RawPgRetry>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPgRetry {
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    initial_backoff_ms: Option<u64>,
    #[serde(default)]
    max_backoff_ms: Option<u64>,
    #[serde(default)]
    jitter_pct: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBatchLimits {
    #[serde(default)]
    batch_get_max_keys: Option<u32>,
    #[serde(default)]
    batch_write_max_requests: Option<u32>,
    #[serde(default)]
    transact_get_max_items: Option<u32>,
    #[serde(default)]
    transact_write_max_items: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServer {
    listen_addr: String,
    database_url: String,
    #[serde(default)]
    request_timeout_ms: Option<u64>,
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

        let batch_limits = match self.batch_limits {
            None => BatchLimits::ddb_defaults(),
            Some(raw) => {
                let mut bl = BatchLimits::ddb_defaults();
                if let Some(n) = raw.batch_get_max_keys {
                    if n == 0 {
                        return Err(ConfigError::validation(
                            "batch_limits.batch_get_max_keys must be > 0",
                        ));
                    }
                    bl.batch_get_max_keys = n;
                }
                if let Some(n) = raw.batch_write_max_requests {
                    if n == 0 {
                        return Err(ConfigError::validation(
                            "batch_limits.batch_write_max_requests must be > 0",
                        ));
                    }
                    bl.batch_write_max_requests = n;
                }
                if let Some(n) = raw.transact_get_max_items {
                    if n == 0 {
                        return Err(ConfigError::validation(
                            "batch_limits.transact_get_max_items must be > 0",
                        ));
                    }
                    bl.transact_get_max_items = n;
                }
                if let Some(n) = raw.transact_write_max_items {
                    if n == 0 {
                        return Err(ConfigError::validation(
                            "batch_limits.transact_write_max_items must be > 0",
                        ));
                    }
                    bl.transact_write_max_items = n;
                }
                bl
            }
        };

        let request_timeout_ms = match self.server.request_timeout_ms {
            None => DEFAULT_REQUEST_TIMEOUT_MS,
            Some(0) => {
                return Err(ConfigError::validation(
                    "server.request_timeout_ms must be > 0",
                ));
            }
            Some(n) => n,
        };

        let pg = resolve_pg(self.pg.as_ref())?;
        let catalog = resolve_catalog(self.catalog.as_ref());

        Ok(Config {
            server: ServerConfig {
                listen_addr,
                database_url: self.server.database_url,
                request_timeout_ms,
            },
            tables,
            batch_limits,
            pg,
            catalog,
        })
    }
}

fn resolve_catalog(raw: Option<&RawCatalog>) -> CatalogConfig {
    let mut out = CatalogConfig::defaults();
    let Some(raw) = raw else { return out };
    // `reconcile_interval_ms = 0` is meaningful (disables the timer);
    // accept verbatim instead of treating zero as "use default".
    if let Some(ms) = raw.reconcile_interval_ms {
        out.reconcile_interval_ms = ms;
    }
    if let Some(b) = raw.invalidate_on_local_ddl {
        out.invalidate_on_local_ddl = b;
    }
    out
}

/// Apply an optional `RawLimits` block on top of a base `Limits`.
/// - Fields the raw block doesn't mention → inherit from base.
/// - Fields with `LimitField::Number(n)` → set to `Some(n)`.
/// - Fields with `LimitField::Unlimited` → set to `None` (cap disabled).
fn resolve_pg(raw: Option<&RawPg>) -> Result<PgConfig, ConfigError> {
    let mut out = PgConfig::defaults();
    let Some(raw) = raw else { return Ok(out) };
    if let Some(n) = raw.max_pool_size {
        if n == 0 {
            return Err(ConfigError::validation("pg.max_pool_size must be > 0"));
        }
        out.max_pool_size = n;
    }
    if let Some(n) = raw.wait_timeout_ms {
        if n == 0 {
            return Err(ConfigError::validation("pg.wait_timeout_ms must be > 0"));
        }
        out.wait_timeout_ms = n;
    }
    if let Some(n) = raw.create_timeout_ms {
        if n == 0 {
            return Err(ConfigError::validation("pg.create_timeout_ms must be > 0"));
        }
        out.create_timeout_ms = n;
    }
    if let Some(n) = raw.recycle_timeout_ms {
        if n == 0 {
            return Err(ConfigError::validation("pg.recycle_timeout_ms must be > 0"));
        }
        out.recycle_timeout_ms = n;
    }
    if let Some(m) = raw.recycling_method.as_deref() {
        out.recycling_method = match m {
            "fast" => RecyclingMethod::Fast,
            "verified" => RecyclingMethod::Verified,
            "clean" => RecyclingMethod::Clean,
            other => {
                return Err(ConfigError::validation(format!(
                    "pg.recycling_method `{other}` is not one of fast | verified | clean"
                )));
            }
        };
    }
    if let Some(rraw) = raw.retry.as_ref() {
        if let Some(n) = rraw.max_attempts {
            if n == 0 {
                return Err(ConfigError::validation(
                    "pg.retry.max_attempts must be > 0 (use 1 to disable retries)",
                ));
            }
            out.retry.max_attempts = n;
        }
        if let Some(n) = rraw.initial_backoff_ms {
            out.retry.initial_backoff_ms = n;
        }
        if let Some(n) = rraw.max_backoff_ms {
            out.retry.max_backoff_ms = n;
        }
        if let Some(n) = rraw.jitter_pct {
            if n > 100 {
                return Err(ConfigError::validation(
                    "pg.retry.jitter_pct must be in 0..=100",
                ));
            }
            out.retry.jitter_pct = n;
        }
        if out.retry.initial_backoff_ms > out.retry.max_backoff_ms {
            return Err(ConfigError::validation(
                "pg.retry.initial_backoff_ms must be <= max_backoff_ms",
            ));
        }
    }
    Ok(out)
}

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
