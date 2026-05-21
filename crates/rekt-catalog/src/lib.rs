//! Runtime catalog of DDB tables for rektifier.
//!
//! The catalog is the in-memory cache that dispatch handlers consult on
//! every request. Its authoritative source is the PG metadata table
//! `_rektifier_tables`; the catalog is refreshed by the reconciler
//! (PLAN-10 D3) and on local DDL completion (PLAN-10 D6 / D7).
//!
//! Today (D2 scaffolding) the cache has two write paths:
//! 1. **Seeder** — startup pass that upserts TOML `[[tables]]` rows into
//!    `_rektifier_tables` with `ON CONFLICT DO NOTHING`. Transitional;
//!    deleted in D8.
//! 2. **Initial snapshot** — one-shot read of `_rektifier_tables` after
//!    the seeder runs, pushed into the cache so handlers can resolve
//!    tables. In D3 the reconciler becomes the only refresher.
//!
//! Read path (lock-free, sub-microsecond): `TableCatalog::get` /
//! `::snapshot` perform a single `ArcSwap::load`, cloning either an
//! `Arc<TableEntry>` (single-table handlers) or an `Arc<CatalogSnapshot>`
//! (multi-table translators that need a `HashMap<String, TableSchema>`).
//!
//! The `serveable` gate (PLAN-10 D4) lives in the handler layer, not
//! here — `TableCatalog::get` returns the entry regardless of
//! `serveable`; callers must check.

pub mod bootstrap;
pub mod metadata;
pub mod reconciler;

use arc_swap::ArcSwap;
use rekt_storage::KeyType;
use rekt_translator::TableSchema;
use std::collections::HashMap;
use std::sync::Arc;

pub use bootstrap::ensure_metadata_tables;
pub use metadata::{
    gsi_specs_from_json, gsi_specs_to_json, load_snapshot, lsi_specs_from_json, lsi_specs_to_json,
    MetadataRow,
};
pub use reconciler::{ReconcileVerdict, Reconciler};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("pool error: {0}")]
    Pool(String),

    #[error("database error: {source}")]
    Db {
        #[source]
        source: tokio_postgres::Error,
    },

    /// A row read from `_rektifier_tables` carries a value rektifier
    /// doesn't know how to interpret (unknown status, malformed key
    /// type). The operator either hand-edited the table or rektifier
    /// shipped a regression; this fires loudly.
    #[error("malformed metadata row for `{table_name}`: {reason}")]
    MalformedRow { table_name: String, reason: String },
}

impl From<tokio_postgres::Error> for CatalogError {
    fn from(source: tokio_postgres::Error) -> Self {
        Self::Db { source }
    }
}

/// Internal table-lifecycle state. Persisted in `_rektifier_tables.status`
/// as the variant's `as_internal_str()` name. The wire-visible
/// `TableStatus` (returned to SDK clients by DescribeTable) is the
/// `to_wire_str()` projection — see PLAN-10 KD6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableStatus {
    Creating,
    FailedCreating,
    Active,
    /// Reconciler detected drift between metadata and live PG; operator
    /// must intervene. Wire-visible as `ACTIVE` (KD6).
    Degraded,
    Updating,
    FailedUpdating,
    Deleting,
    FailedDeleting,
}

impl TableStatus {
    pub fn as_internal_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::FailedCreating => "FAILED_CREATING",
            Self::Active => "ACTIVE",
            Self::Degraded => "DEGRADED",
            Self::Updating => "UPDATING",
            Self::FailedUpdating => "FAILED_UPDATING",
            Self::Deleting => "DELETING",
            Self::FailedDeleting => "FAILED_DELETING",
        }
    }

    /// Map internal status to the DDB wire `TableStatus` vocabulary.
    /// KD6: FAILED_* collapse to in-flight; DEGRADED collapses to ACTIVE.
    pub fn to_wire_str(self) -> &'static str {
        match self {
            Self::Creating | Self::FailedCreating => "CREATING",
            Self::Active | Self::Degraded => "ACTIVE",
            Self::Updating | Self::FailedUpdating => "UPDATING",
            Self::Deleting | Self::FailedDeleting => "DELETING",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "CREATING" => Self::Creating,
            "FAILED_CREATING" => Self::FailedCreating,
            "ACTIVE" => Self::Active,
            "DEGRADED" => Self::Degraded,
            "UPDATING" => Self::Updating,
            "FAILED_UPDATING" => Self::FailedUpdating,
            "DELETING" => Self::Deleting,
            "FAILED_DELETING" => Self::FailedDeleting,
            _ => return None,
        })
    }
}

/// One catalog entry. The `schema` field is what existing translators
/// consume; everything else is for the dispatch gate, DescribeTable, and
/// PLAN-9 GSI integration. Wrapped in `Arc` inside the snapshot so
/// per-call clones are pointer-sized.
#[derive(Debug, Clone)]
pub struct TableEntry {
    pub schema: TableSchema,
    pub status: TableStatus,
    /// Whether dispatch should serve requests against this table. False
    /// when the reconciler has detected drift, when a DDL operation is
    /// in-flight (CREATING / UPDATING / DELETING), or when a worker
    /// has failed (FAILED_*).
    pub serveable: bool,
    pub unserveable_reason: Option<String>,
    pub creation_date_ms: i64,
    pub last_modified_at_ms: i64,
    /// `"ddl" | "seeder" | "reconciler"` — telemetry / debugging only.
    pub last_modified_by: String,
    pub billing_mode: Option<String>,
    pub provisioned_rcu: Option<i64>,
    pub provisioned_wcu: Option<i64>,
    pub tags: serde_json::Value,
    /// Per-GSI cache state. Empty until PLAN-9 lands.
    pub gsis: HashMap<String, GsiCacheEntry>,
    /// PLAN-11 L3. Per-LSI cache state, populated from
    /// `_rektifier_tables.lsi_specs` at every catalog refresh. Empty for
    /// tables that declared no LSIs at CreateTable time. Each LSI carries
    /// its own `serveable` bit (PLAN-11 open Q4) so drift on one LSI
    /// doesn't render the base table unserveable.
    pub lsis: Vec<LsiSpec>,
}

/// Post-validation, persistence-friendly form of an LSI declaration as
/// it lives in the catalog. The `name`, `sort_attr`, and `sort_type`
/// echo the validator's output; the rest is reconciler-driven state.
///
/// `sort_pg_col` is intentionally distinct from `sort_attr` even though
/// today they always agree (same as PK/SK — rektifier uses the raw DDB
/// attribute name verbatim). Keeping them separate lets a future rename
/// or column-collision-suffix scheme land without touching every
/// consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsiSpec {
    pub name: String,
    pub sort_attr: String,
    pub sort_type: KeyType,
    pub sort_pg_col: String,
    pub index_name: String,
    pub projection_type: Option<String>,
    pub projection_non_key_attrs: Option<Vec<String>>,
    /// Reconciler verdict for this LSI. `false` when the column or
    /// index is missing / shape-mismatched. Base-table Queries are
    /// unaffected — only Query/Scan with `IndexName` targeting this
    /// LSI return ResourceNotFoundException at the dispatch gate.
    pub serveable: bool,
    pub unserveable_reason: Option<String>,
}

/// PLAN-9 D17. Per-GSI column-population mode, selected at GSI birth
/// from the request origin and immutable thereafter.
///
/// - `Generated`: CreateTable-time GSI. PG owns the JSONB→column
///   invariant via `GENERATED ALWAYS AS (data#>>{path,T}) STORED`.
///   No app-side dual-write code is needed. The GSI is ACTIVE
///   immediately on CreateTable return.
/// - `DualWrite`: UpdateTable.Create-time GSI. Regular column;
///   rektifier populates it on every INSERT/UPDATE. The async
///   lifecycle in `_rektifier_gsi_state` (declared → adding_column
///   → backfilling → indexing → active) governs queryability.
///
/// Persisted as the lowercase variant name in
/// `_rektifier_tables.gsi_specs[*].mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GsiMode {
    Generated,
    DualWrite,
}

impl GsiMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Generated => "generated",
            Self::DualWrite => "dual_write",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "generated" => Some(Self::Generated),
            "dual_write" => Some(Self::DualWrite),
            _ => None,
        }
    }
}

/// PLAN-9 G1. Per-GSI cache state — the GSI analog of `LsiSpec`.
/// `partition_attr` is required (every GSI has a HASH); `sort_attr`
/// is optional (DDB permits hash-only GSIs). Each attribute name
/// doubles as the PG column name (matches PK/SK and LSI sort-attr
/// convention).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GsiSpec {
    pub name: String,
    pub partition_attr: String,
    pub partition_type: KeyType,
    pub partition_pg_col: String,
    pub sort_attr: Option<String>,
    pub sort_type: Option<KeyType>,
    pub sort_pg_col: Option<String>,
    pub index_name: String,
    pub projection_type: Option<String>,
    pub projection_non_key_attrs: Option<Vec<String>>,
    /// PLAN-9 D17. Mode at GSI birth — Generated for CT-time,
    /// DualWrite for UpdateTable.Create. Immutable.
    pub mode: GsiMode,
    /// Reconciler-driven gate (PLAN-9 D7 / G8). When `false`,
    /// Query/Scan with this IndexName returns RNF at dispatch.
    pub serveable: bool,
    pub unserveable_reason: Option<String>,
}

/// PLAN-9 G1 prior placeholder. Retained as a thin alias for the
/// transitional period while DescribeTable / dispatch sites are
/// migrated; new code uses `GsiSpec` directly.
pub type GsiCacheEntry = GsiSpec;

/// Immutable snapshot swapped wholesale by writers. Holds two views of
/// the same data:
/// - `entries`: rich `TableEntry` per name, for single-table handlers
///   and DescribeTable.
/// - `schemas`: thin `TableSchema` map, the legacy shape consumed by
///   multi-table translators (BatchGet/Write, TransactGet/Write).
///   Derived from `entries` at snapshot-build time — readers never pay
///   to assemble it on each call.
#[derive(Debug, Clone, Default)]
pub struct CatalogSnapshot {
    pub entries: HashMap<String, Arc<TableEntry>>,
    pub schemas: HashMap<String, TableSchema>,
}

impl CatalogSnapshot {
    pub fn from_entries(entries: HashMap<String, Arc<TableEntry>>) -> Self {
        let schemas = entries
            .iter()
            .map(|(name, entry)| (name.clone(), entry.schema.clone()))
            .collect();
        Self { entries, schemas }
    }
}

/// Lock-free, read-mostly cache. The reconciler (D3) and DDL workers
/// (D6/D7) call `replace` with a freshly-built `CatalogSnapshot`;
/// readers do `get` / `snapshot` for a single `ArcSwap::load`.
pub struct TableCatalog {
    inner: ArcSwap<CatalogSnapshot>,
}

impl Default for TableCatalog {
    fn default() -> Self {
        Self::empty()
    }
}

impl TableCatalog {
    pub fn empty() -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(CatalogSnapshot::default())),
        }
    }

    pub fn from_snapshot(snapshot: CatalogSnapshot) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(snapshot)),
        }
    }

    /// Single-table lookup. Returns `None` for unknown names (used by
    /// the dispatch gate to emit ResourceNotFoundException).
    pub fn get(&self, name: &str) -> Option<Arc<TableEntry>> {
        self.inner.load().entries.get(name).cloned()
    }

    /// Snapshot for multi-table translators that want a single coherent
    /// view across the request. `Arc<CatalogSnapshot>` is cheap to clone
    /// and stable for the duration of the handler call.
    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.inner.load_full()
    }

    /// Atomic swap. Single writer (reconciler) per PLAN-10 KD3.
    pub fn replace(&self, snapshot: CatalogSnapshot) {
        self.inner.store(Arc::new(snapshot));
    }

    /// Lex-sorted ASC table names. Used by ListTables (D5).
    pub fn sorted_table_names(&self) -> Vec<String> {
        let snap = self.inner.load();
        let mut names: Vec<String> = snap.entries.keys().cloned().collect();
        names.sort();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekt_storage::KeyType;

    fn mk_entry(name: &str) -> Arc<TableEntry> {
        Arc::new(TableEntry {
            schema: TableSchema {
                name: name.into(),
                pg_table: format!("rekt_t_{name}"),
                pk_attr: "id".into(),
                pk_type: KeyType::S,
                sk_attr: None,
                sk_type: None,
                jsonb_col: "data".into(),
                lsis: Default::default(),
            gsis: Default::default(),
            },
            status: TableStatus::Active,
            serveable: true,
            unserveable_reason: None,
            creation_date_ms: 1_700_000_000_000,
            last_modified_at_ms: 1_700_000_000_000,
            last_modified_by: "seeder".into(),
            billing_mode: None,
            provisioned_rcu: None,
            provisioned_wcu: None,
            tags: serde_json::json!({}),
            gsis: HashMap::new(),
            lsis: Vec::new(),
        })
    }

    /// Wire-status mapping per KD6. Every internal variant collapses to
    /// one of the four DDB wire values; FAILED_* hide behind their
    /// in-flight counterpart; DEGRADED hides behind ACTIVE.
    #[test]
    fn status_internal_to_wire_mapping() {
        assert_eq!(TableStatus::Creating.to_wire_str(), "CREATING");
        assert_eq!(TableStatus::FailedCreating.to_wire_str(), "CREATING");
        assert_eq!(TableStatus::Active.to_wire_str(), "ACTIVE");
        assert_eq!(TableStatus::Degraded.to_wire_str(), "ACTIVE");
        assert_eq!(TableStatus::Updating.to_wire_str(), "UPDATING");
        assert_eq!(TableStatus::FailedUpdating.to_wire_str(), "UPDATING");
        assert_eq!(TableStatus::Deleting.to_wire_str(), "DELETING");
        assert_eq!(TableStatus::FailedDeleting.to_wire_str(), "DELETING");
    }

    #[test]
    fn status_round_trips_internal_string() {
        for s in [
            TableStatus::Creating,
            TableStatus::FailedCreating,
            TableStatus::Active,
            TableStatus::Degraded,
            TableStatus::Updating,
            TableStatus::FailedUpdating,
            TableStatus::Deleting,
            TableStatus::FailedDeleting,
        ] {
            assert_eq!(TableStatus::parse(s.as_internal_str()), Some(s));
        }
        assert_eq!(TableStatus::parse("nope"), None);
    }

    /// Empty catalog: get returns None; snapshot is well-shaped.
    #[test]
    fn empty_catalog_returns_none() {
        let c = TableCatalog::empty();
        assert!(c.get("foo").is_none());
        let snap = c.snapshot();
        assert!(snap.entries.is_empty());
        assert!(snap.schemas.is_empty());
        assert!(c.sorted_table_names().is_empty());
    }

    /// Snapshot derives the `schemas` map from entries — multi-table
    /// translators reading `snap.schemas` see the same key set as
    /// single-table handlers reading `snap.entries`.
    #[test]
    fn snapshot_schemas_view_mirrors_entries() {
        let mut entries = HashMap::new();
        entries.insert("alpha".into(), mk_entry("alpha"));
        entries.insert("beta".into(), mk_entry("beta"));
        let snap = CatalogSnapshot::from_entries(entries);
        assert_eq!(snap.entries.len(), 2);
        assert_eq!(snap.schemas.len(), 2);
        assert!(snap.schemas.contains_key("alpha"));
        assert!(snap.schemas.contains_key("beta"));
        assert_eq!(snap.schemas["alpha"].pg_table, "rekt_t_alpha");
    }

    /// Replace atomically swaps the snapshot. Old readers holding an
    /// Arc<CatalogSnapshot> from before the swap continue to see the
    /// old state — that's the whole point of arc-swap.
    #[test]
    fn replace_is_atomic_and_isolates_pre_swap_readers() {
        let c = TableCatalog::empty();
        let mut entries_v1 = HashMap::new();
        entries_v1.insert("alpha".into(), mk_entry("alpha"));
        c.replace(CatalogSnapshot::from_entries(entries_v1));

        let snap_v1 = c.snapshot();
        assert!(snap_v1.entries.contains_key("alpha"));
        assert!(!snap_v1.entries.contains_key("beta"));

        let mut entries_v2 = HashMap::new();
        entries_v2.insert("alpha".into(), mk_entry("alpha"));
        entries_v2.insert("beta".into(), mk_entry("beta"));
        c.replace(CatalogSnapshot::from_entries(entries_v2));

        // Old guard still sees v1 — no torn read.
        assert!(!snap_v1.entries.contains_key("beta"));

        // Fresh load sees v2.
        let snap_v2 = c.snapshot();
        assert!(snap_v2.entries.contains_key("beta"));
        assert_eq!(c.sorted_table_names(), vec!["alpha", "beta"]);
    }

    #[test]
    fn get_clones_arc_not_entry() {
        let c = TableCatalog::empty();
        let mut entries = HashMap::new();
        entries.insert("alpha".into(), mk_entry("alpha"));
        c.replace(CatalogSnapshot::from_entries(entries));

        let a = c.get("alpha").unwrap();
        let b = c.get("alpha").unwrap();
        // Same underlying Arc payload — get is pointer-cheap.
        assert!(Arc::ptr_eq(&a, &b));
    }

    /// PLAN-9 D17. Mode round-trips through the wire string used to
    /// persist it inside `_rektifier_tables.gsi_specs[*].mode`.
    #[test]
    fn gsi_mode_string_round_trip() {
        assert_eq!(GsiMode::Generated.as_str(), "generated");
        assert_eq!(GsiMode::DualWrite.as_str(), "dual_write");
        assert_eq!(GsiMode::parse("generated"), Some(GsiMode::Generated));
        assert_eq!(GsiMode::parse("dual_write"), Some(GsiMode::DualWrite));
        assert_eq!(GsiMode::parse("nonsense"), None);
    }
}
