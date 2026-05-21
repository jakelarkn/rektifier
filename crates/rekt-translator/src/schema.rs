//! Table-schema metadata shared between the translator and the storage layer.

use rekt_storage::{KeyType, TableShape};
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
    pub fn shape(&self) -> TableShape<'_> {
        TableShape {
            table: &self.pg_table,
            pk_col: &self.pk_attr,
            pk_type: self.pk_type,
            sk_col: self.sk_attr.as_deref(),
            sk_type: self.sk_type,
            jsonb_col: &self.jsonb_col,
        }
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
        }
    }
}
