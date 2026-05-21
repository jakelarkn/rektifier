//! Table-schema metadata shared between the translator and the storage layer.

use rekt_storage::{DualWriteCol, KeyType, TableShape};
use std::collections::HashMap;

/// Description of a table that the translator and the storage layer agree on.
/// Built once at server startup from config (+ PG verification).
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// DynamoDB-side table name (what clients send).
    pub name: String,
    /// Postgres table name. May differ from `name`.
    pub pg_table: String,
    /// Partition-key attribute name. Doubles as the PG column name.
    pub pk_attr: String,
    pub pk_type: KeyType,
    /// Sort-key attribute name, if the table is composite. Doubles as the
    /// PG column name.
    pub sk_attr: Option<String>,
    pub sk_type: Option<KeyType>,
    /// PG column name for the full DynamoDB-JSON item blob (`jsonb` type).
    pub jsonb_col: String,
    /// PLAN-11 L4. Declared LSIs, keyed by DDB IndexName. Populated by
    /// the catalog's snapshot builder from the per-row `lsi_specs` JSONB
    /// blob. Empty when the table declares no LSIs.
    pub lsis: HashMap<String, LsiSchema>,
    /// PLAN-9 G1. Declared GSIs, keyed by IndexName. Same population
    /// pattern as `lsis`.
    pub gsis: HashMap<String, GsiSchema>,
}

/// PLAN-9 G1. Translator-side view of a declared GSI. The GSI's HASH
/// attribute replaces the base table's partition key in the synthetic
/// schema used by KCE resolution.
#[derive(Debug, Clone)]
pub struct GsiSchema {
    pub name: String,
    pub partition_attr: String,
    pub partition_type: KeyType,
    pub partition_pg_col: String,
    pub sort_attr: Option<String>,
    pub sort_type: Option<KeyType>,
    pub sort_pg_col: Option<String>,
    /// PLAN-9 D17 / G3. True when this GSI was created via
    /// UpdateTable.Create (regular column populated by rektifier's
    /// SQL at every INSERT/UPDATE). False for Generated-mode GSIs
    /// (PG owns the column via GENERATED ALWAYS AS ... STORED;
    /// write-path SQL ignores them).
    pub is_dual_write: bool,
    pub serveable: bool,
    pub unserveable_reason: Option<String>,
}

/// Translator-side view of a declared LSI. The catalog's `LsiSpec`
/// carries identical data plus persistence-only fields; this projection
/// is what the translator and dispatch handlers consult.
#[derive(Debug, Clone)]
pub struct LsiSchema {
    pub name: String,
    /// LSI sort attribute name — appears in KeyConditionExpression in
    /// place of the base table's sk_attr.
    pub sort_attr: String,
    pub sort_type: KeyType,
    /// PG column name for the LSI sort attribute (matches `sort_attr`
    /// today; kept separate so a future renamed-column scheme would
    /// drop in here without touching the translator).
    pub sort_pg_col: String,
    /// Reconciler-driven gate (PLAN-11 D12 + open Q4). When `false`,
    /// Query/Scan with this IndexName returns ResourceNotFoundException
    /// at dispatch.
    pub serveable: bool,
    pub unserveable_reason: Option<String>,
}

impl TableSchema {
    /// Borrow as a `TableShape` for handing to the storage layer.
    /// Caller supplies the borrowed DualWriteCol slice; use
    /// `dual_write_cols()` to materialize it from the GSI map.
    pub fn shape_with_dual_write<'a>(
        &'a self,
        dual_write_cols: &'a [DualWriteCol<'a>],
    ) -> TableShape<'a> {
        TableShape {
            table: &self.pg_table,
            pk_col: &self.pk_attr,
            pk_type: self.pk_type,
            sk_col: self.sk_attr.as_deref(),
            sk_type: self.sk_type,
            jsonb_col: &self.jsonb_col,
            dual_write_cols,
        }
    }

    /// Convenience: empty DualWrite slice — for read-path operations
    /// (Get, Query, Scan) where DualWrite columns are read but never
    /// written, the slice is irrelevant.
    pub fn shape(&self) -> TableShape<'_> {
        TableShape {
            table: &self.pg_table,
            pk_col: &self.pk_attr,
            pk_type: self.pk_type,
            sk_col: self.sk_attr.as_deref(),
            sk_type: self.sk_type,
            jsonb_col: &self.jsonb_col,
            dual_write_cols: &[],
        }
    }

    /// PLAN-9 G3. Materialize the DualWrite-mode GSI columns into a
    /// Vec the caller borrows from when building a write-path
    /// TableShape. Each entry is one DualWriteCol per GSI key column
    /// (partition + optional sort). Skipped: Generated-mode GSIs
    /// (PG owns), and DualWrite GSIs that share an attribute with the
    /// base PK/SK or any other already-emitted DualWrite column
    /// (refcount dedup per D9).
    pub fn dual_write_cols(&self) -> Vec<DualWriteCol<'_>> {
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        // Reserve names that PG already owns (GENERATED on base + LSI +
        // Generated-mode GSI columns). Those columns cannot be in
        // INSERT/UPDATE column lists.
        seen.insert(self.pk_attr.as_str());
        if let Some(sk) = self.sk_attr.as_deref() {
            seen.insert(sk);
        }
        for lsi in self.lsis.values() {
            seen.insert(lsi.sort_pg_col.as_str());
        }
        for gsi in self.gsis.values() {
            if !gsi.is_dual_write {
                // Generated-mode columns are PG-owned; never written.
                seen.insert(gsi.partition_pg_col.as_str());
                if let Some(s) = gsi.sort_pg_col.as_deref() {
                    seen.insert(s);
                }
            }
        }
        let mut out: Vec<DualWriteCol<'_>> = Vec::new();
        for gsi in self.gsis.values() {
            if !gsi.is_dual_write {
                continue;
            }
            if seen.insert(gsi.partition_pg_col.as_str()) {
                out.push(DualWriteCol {
                    col: &gsi.partition_pg_col,
                    attr: &gsi.partition_attr,
                    key_type: gsi.partition_type,
                });
            }
            if let (Some(col), Some(attr), Some(t)) = (
                gsi.sort_pg_col.as_deref(),
                gsi.sort_attr.as_deref(),
                gsi.sort_type,
            ) {
                if seen.insert(col) {
                    out.push(DualWriteCol {
                        col,
                        attr,
                        key_type: t,
                    });
                }
            }
        }
        // Stable order — column name lex sort. Keeps SQL deterministic
        // for cache-key derivation and easier debugging.
        out.sort_by(|a, b| a.col.cmp(b.col));
        out
    }

    /// PLAN-11 L4. Build a `TableShape` that targets an LSI. PK stays
    /// the base-table partition column; SK is substituted for the LSI's
    /// sort column. PG's planner picks the LSI btree (which is on
    /// `(base_pk, lsi_sort)`) because the WHERE clause references those
    /// columns in order.
    pub fn shape_for_lsi<'a>(&'a self, lsi: &'a LsiSchema) -> TableShape<'a> {
        TableShape {
            table: &self.pg_table,
            pk_col: &self.pk_attr,
            pk_type: self.pk_type,
            sk_col: Some(&lsi.sort_pg_col),
            sk_type: Some(lsi.sort_type),
            jsonb_col: &self.jsonb_col,
            dual_write_cols: &[],
        }
    }

    /// PLAN-9 G7. Build a `TableShape` that targets a GSI. Both PK and
    /// SK columns are substituted with the GSI's attrs. For hash-only
    /// GSIs the sk_col is None.
    pub fn shape_for_gsi<'a>(&'a self, gsi: &'a GsiSchema) -> TableShape<'a> {
        TableShape {
            table: &self.pg_table,
            pk_col: &gsi.partition_pg_col,
            pk_type: gsi.partition_type,
            sk_col: gsi.sort_pg_col.as_deref(),
            sk_type: gsi.sort_type,
            jsonb_col: &self.jsonb_col,
            dual_write_cols: &[],
        }
    }
}
