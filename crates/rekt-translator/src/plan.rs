//! Backend-neutral plan structs produced by `translate_*_item`.
//!
//! - [`PutItemPlan`] / [`GetItemPlan`] / [`DeleteItemPlan`] / [`UpdateItemPlan`] —
//!   per-op shapes the dispatcher hands to the storage layer.
//! - [`ReturnValuesMode`] — DDB's full `ReturnValues` enum.
//! - [`ConditionPlan`] / [`ConditionRouting`] — parsed condition + the
//!   routing decision the classifier reached.
//! - [`SimpleUpdatePrimitives`] / [`SimpleSqlUpdatePrimitives`] —
//!   pre-flattened storage-ready output of the simple-subset
//!   materializers (see [`crate::materialize`]).
//! - [`touched_paths`] — top-level attrs an UpdateExpression writes;
//!   feeds `ReturnValues=UPDATED_*` projection.

use rekt_expressions::{Condition, UpdateExpression};
use rekt_storage::{KeyValue, SkCondition};

#[derive(Debug, Clone)]
pub struct PutItemPlan {
    /// Partition-key value, needed by conditional / ALL_OLD paths to
    /// look up the pre-update row. The unconditional fast path ignores
    /// it (PG derives the key columns from the JSONB).
    pub pk: KeyValue,
    pub sk: Option<KeyValue>,
    /// Full item in DynamoDB-JSON form, source of truth for the jsonb column.
    /// PG derives the typed key columns from this via `GENERATED ALWAYS AS`,
    /// so writes don't bind pk/sk separately. The translator still validates
    /// that the expected key attributes are present and well-typed — that's
    /// protocol-level validation, independent of how PG stores them.
    pub item_json: serde_json::Value,
    /// Resolved ConditionExpression, if any. PutItem accepts every
    /// shape UpdateItem does; routing is the same classifier.
    pub condition: Option<ConditionPlan>,
    /// PutItem accepts only `None` (default) and `AllOld`. Other variants
    /// are rejected at translate time.
    pub return_values: ReturnValuesMode,
}

#[derive(Debug, Clone)]
pub struct GetItemPlan {
    pub pk: KeyValue,
    pub sk: Option<KeyValue>,
}

#[derive(Debug, Clone)]
pub struct DeleteItemPlan {
    pub pk: KeyValue,
    pub sk: Option<KeyValue>,
    /// Resolved ConditionExpression, if any.
    pub condition: Option<ConditionPlan>,
    /// DeleteItem accepts only `None` (default) and `AllOld`. Other
    /// variants are rejected at translate time.
    pub return_values: ReturnValuesMode,
}

#[derive(Debug, Clone)]
pub struct UpdateItemPlan {
    pub pk: KeyValue,
    pub sk: Option<KeyValue>,
    /// The fully-resolved, schema-validated UpdateExpression. Storage
    /// interprets this directly — the simple-path predicate
    /// (`is_simple_update`) decides whether to emit pure-SQL or fall
    /// back to read-modify-write.
    pub expression: UpdateExpression,
    /// The fully-resolved, schema-validated ConditionExpression, if the
    /// request carried one. Storage uses `routing` to pick the SQL shape:
    /// `InsertOnlyOnPk` becomes `INSERT … ON CONFLICT DO NOTHING` (Phase 4c);
    /// `SimpleSql` becomes `UPDATE … WHERE cond-sql` (Phase 4d);
    /// `NeedsTx` falls to the slow path with in-Rust evaluation
    /// (Phase 4e). Always `None` in Phases 1–3.
    pub condition: Option<ConditionPlan>,
    /// What the response body should carry. `None` means an empty
    /// response (DDB default). `AllNew` means the full post-update item.
    /// Other DDB variants (`ALL_OLD` / `UPDATED_OLD` / `UPDATED_NEW`)
    /// are Phase 7b/7c and are rejected at translate time.
    pub return_values: ReturnValuesMode,
}

/// DDB's `ReturnValues` enum, fully supported. `None` (default) returns
/// an empty body; `AllNew`/`AllOld` return the full post-/pre-update
/// item; `UpdatedNew`/`UpdatedOld` return the same restricted to the
/// top-level attributes the UpdateExpression touched (computed by
/// [`touched_paths`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReturnValuesMode {
    #[default]
    None,
    AllNew,
    AllOld,
    UpdatedNew,
    UpdatedOld,
}

/// Top-level attribute names the UpdateExpression touches: union of
/// SET targets, REMOVE paths, ADD targets, DELETE targets. Used by
/// the dispatcher to project `UpdateOutcome` for `UPDATED_*`
/// `ReturnValues` modes. Top-level only — Phase 8 nested-path lift
/// will need to extend this to track the deepest projected ancestor.
pub fn touched_paths(expr: &UpdateExpression) -> std::collections::BTreeSet<String> {
    let mut paths = std::collections::BTreeSet::new();
    for c in &expr.set {
        if let Some(n) = c.path.top_name() {
            paths.insert(n.to_string());
        }
    }
    for p in &expr.remove {
        if let Some(n) = p.top_name() {
            paths.insert(n.to_string());
        }
    }
    for a in &expr.add {
        if let Some(n) = a.path.top_name() {
            paths.insert(n.to_string());
        }
    }
    for d in &expr.delete {
        if let Some(n) = d.path.top_name() {
            paths.insert(n.to_string());
        }
    }
    paths
}

/// Scan plan: full-table read.
///
/// `limit` is the per-call cap (soft default 1000 — see
/// `COMPATIBILITY_NOTES.md`). `esk_pk` + `esk_sk` together form the
/// keyset cursor — both come from the previous response's LEK.
/// `filter` is the parsed FilterExpression (per-row Rust eval, same
/// pattern as Query — PLAN-4 D2).
#[derive(Debug, Clone, Default)]
pub struct ScanPlan {
    pub limit: Option<u32>,
    pub esk_pk: Option<KeyValue>,
    pub esk_sk: Option<KeyValue>,
    pub filter: Option<ConditionPlan>,
}

/// Query plan: bounded read of one partition.
///
/// `pk` is the resolved partition-key value (KCE always pins exactly
/// one partition via `pk = :v`). `sk_condition` is the optional sort-
/// key predicate. `limit` is the caller's per-call cap (Q2). `esk_sk`
/// is the sort-key half of the `ExclusiveStartKey` cursor; the PK half
/// is `pk` (translator-enforced equality with the query PK) so we only
/// carry the SK component through to storage. `filter` (Q3) is the
/// parsed FilterExpression evaluated per-row in Rust by the dispatcher.
/// `forward` (Q5) toggles the sort order — `true` is the DDB default;
/// `false` is `ScanIndexForward=false` (descending SK, flipped ESK
/// comparison).
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub pk: KeyValue,
    pub sk_condition: Option<SkCondition>,
    pub limit: Option<u32>,
    pub esk_sk: Option<KeyValue>,
    pub filter: Option<ConditionPlan>,
    pub forward: bool,
}

#[derive(Debug, Clone)]
pub struct ConditionPlan {
    pub routing: ConditionRouting,
    pub condition: Condition,
}

/// Where in the storage layer's dispatch tree this condition belongs.
/// Phase 4b populates this; Phases 4c / 4d / 4e wire each variant
/// through to a concrete backend path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionRouting {
    /// Exactly `attribute_not_exists(<pk_attr>)`, no boolean composition.
    /// The fast-path `INSERT … ON CONFLICT DO NOTHING` case.
    InsertOnlyOnPk,
    /// Any other v1 condition (compositions of `attribute_exists` /
    /// `attribute_not_exists` / `=` / `<>` / `<` / `<=` / `>` / `>=`
    /// via AND / OR / NOT). Compiles to a SQL WHERE fragment in 4d.
    SimpleSql,
    /// Conditions that can't be expressed in plain SQL (`begins_with`,
    /// `contains`, `BETWEEN`, etc.). Reserved for Phase 4e; the parser
    /// doesn't yet emit shapes that land here.
    NeedsTx,
}

/// Storage-ready primitives for the simple-subset UpdateItem fast path.
/// Produced by [`crate::materialize::materialize_simple_update`] from a
/// translated plan + the request's `Key`.
#[derive(Debug, Clone)]
pub struct SimpleUpdatePrimitives {
    /// DDB-JSON for the INSERT branch: the request's key attrs merged
    /// with the SET literals.
    pub insert_item: serde_json::Value,
    /// One `(top-level attr name, DDB-JSON value)` per SET clause.
    pub sets: Vec<(String, serde_json::Value)>,
    /// One top-level attr name per REMOVE clause.
    pub removes: Vec<String>,
}

/// Storage-ready primitives for an UpdateItem with a `SimpleSql`-
/// classified ConditionExpression. The condition is *not* compiled
/// here — the backend's SQL compiler does that — but the SET/REMOVE
/// pieces are pre-flattened identically to the no-condition fast path.
/// Produced by [`crate::materialize::materialize_simple_sql_update`].
#[derive(Debug, Clone)]
pub struct SimpleSqlUpdatePrimitives {
    pub sets: Vec<(String, serde_json::Value)>,
    pub removes: Vec<String>,
}
