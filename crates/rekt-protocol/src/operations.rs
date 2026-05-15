//! Request and response types for DynamoDB operations.
//!
//! Field names are PascalCase on the wire; we apply `rename_all = "PascalCase"`
//! at the struct level. Unknown fields on requests are ignored (serde default)
//! so that AWS-CLI-only fields like `ReturnConsumedCapacity` don't 400 us.

use crate::Item;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutItemRequest {
    pub table_name: String,
    pub item: Item,
    #[serde(default)]
    pub expression_attribute_names: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    pub expression_attribute_values:
        Option<std::collections::BTreeMap<String, crate::AttributeValue>>,
    #[serde(default)]
    pub condition_expression: Option<String>,
    /// PutItem accepts only `NONE` (default) and `ALL_OLD`. Absent
    /// means `NONE`. The translator rejects other values.
    #[serde(default)]
    pub return_values: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutItemResponse {
    /// Populated only when `ReturnValues=ALL_OLD` and a prior row
    /// existed at the same key. Omitted from the wire body otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Item>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetItemRequest {
    pub table_name: String,
    pub key: Item,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetItemResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteItemRequest {
    pub table_name: String,
    pub key: Item,
    #[serde(default)]
    pub expression_attribute_names: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    pub expression_attribute_values:
        Option<std::collections::BTreeMap<String, crate::AttributeValue>>,
    #[serde(default)]
    pub condition_expression: Option<String>,
    /// DeleteItem accepts only `NONE` (default) and `ALL_OLD`. Absent
    /// means `NONE`. The translator rejects other values.
    #[serde(default)]
    pub return_values: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteItemResponse {
    /// Populated only when `ReturnValues=ALL_OLD` and a row was
    /// actually deleted. Omitted from the wire body otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Item>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateItemRequest {
    pub table_name: String,
    pub key: Item,
    pub update_expression: String,
    #[serde(default)]
    pub expression_attribute_names: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    pub expression_attribute_values:
        Option<std::collections::BTreeMap<String, crate::AttributeValue>>,
    #[serde(default)]
    pub condition_expression: Option<String>,
    /// One of `NONE` | `ALL_OLD` | `UPDATED_OLD` | `ALL_NEW` | `UPDATED_NEW`.
    /// Absent means `NONE`. Phase 2 supports `NONE` only; the translator
    /// rejects other values with a "deferred-feature" error.
    #[serde(default)]
    pub return_values: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateItemResponse {
    /// Populated only when `ReturnValues` was something other than `NONE`.
    /// Phase 2 always returns `None` because only `NONE` is supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Item>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryRequest {
    pub table_name: String,
    pub key_condition_expression: String,
    #[serde(default)]
    pub expression_attribute_names: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    pub expression_attribute_values:
        Option<std::collections::BTreeMap<String, crate::AttributeValue>>,
    /// Silently honored — every PG read is already at least as strong as
    /// DDB's strongly-consistent mode. Surfaced via the request tracing
    /// span; see `COMPATIBILITY_NOTES.md`.
    #[serde(default)]
    pub consistent_read: Option<bool>,
    /// Maximum items to scan from PG before returning. DDB caps at 1 MB
    /// per page (variable item count); rektifier caps by item count
    /// (soft default 1000) — see `COMPATIBILITY_NOTES.md`.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Pagination cursor: the `LastEvaluatedKey` from the previous
    /// response. Caller passes it back verbatim; rektifier resumes
    /// strictly after this key.
    #[serde(default)]
    pub exclusive_start_key: Option<Item>,
    /// Optional post-key filter (DDB's full v1 ConditionExpression
    /// grammar over non-key attrs). Applied per row in Rust after PG
    /// returns the partition slice — `Count` tracks post-filter
    /// items; `ScannedCount` tracks pre-filter items (which may
    /// differ).
    #[serde(default)]
    pub filter_expression: Option<String>,
    /// Sort direction. `Some(false)` reverses to descending — items
    /// are returned in sort-key DESC order and pagination resumes
    /// strictly *before* the `ExclusiveStartKey`. `None` and
    /// `Some(true)` are both ascending (DDB default).
    #[serde(default)]
    pub scan_index_forward: Option<bool>,
    /// `ALL_ATTRIBUTES` (default), `COUNT`, or `SPECIFIC_ATTRIBUTES`
    /// (implied when `ProjectionExpression` is set). Q6 accepts these
    /// three; `ALL_PROJECTED_ATTRIBUTES` is GSI-only and rejected
    /// until GSI work lands.
    #[serde(default)]
    pub select: Option<String>,
    /// Comma-separated attribute paths to return in each item. v1
    /// supports top-level paths only (nested paths land with Phase 8
    /// in `PLAN-2`).
    #[serde(default)]
    pub projection_expression: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryResponse {
    pub items: Vec<Item>,
    pub count: u32,
    pub scanned_count: u32,
    /// Present when more items remain in the partition than fit under
    /// `Limit`. Caller passes back verbatim as `ExclusiveStartKey` to
    /// fetch the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<Item>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ScanRequest {
    pub table_name: String,
    /// Silently honored — see `COMPATIBILITY_NOTES.md`.
    #[serde(default)]
    pub consistent_read: Option<bool>,
    /// GSI/LSI scan target. Rejected in Q4 (no GSI support yet) —
    /// `PLAN-2` "Indexes" tracks the prereq work.
    #[serde(default)]
    pub index_name: Option<String>,
    /// Parallel-scan partition index (`0..TotalSegments-1`). Rejected
    /// in Q4; deferred per `PLAN-4` D4.
    #[serde(default)]
    pub segment: Option<u32>,
    #[serde(default)]
    pub total_segments: Option<u32>,
    /// Max items to scan from PG per call. Soft default 1000 — see
    /// `COMPATIBILITY_NOTES.md`.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Pagination cursor (the `LastEvaluatedKey` from the previous
    /// response). For Scan, the cursor contains the table's full
    /// key set (pk for hash-only; pk + sk for composite) and rektifier
    /// resumes strictly after this key in `(pk, sk)` order.
    #[serde(default)]
    pub exclusive_start_key: Option<Item>,
    /// Optional post-scan filter. Same v1 ConditionExpression grammar
    /// as Query; same per-row Rust evaluation in Q5 (SQL push-down is
    /// a deferred perf phase, PLAN-4 D2).
    #[serde(default)]
    pub filter_expression: Option<String>,
    #[serde(default)]
    pub expression_attribute_names: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    pub expression_attribute_values:
        Option<std::collections::BTreeMap<String, crate::AttributeValue>>,
    /// See `QueryRequest::select`.
    #[serde(default)]
    pub select: Option<String>,
    /// See `QueryRequest::projection_expression`.
    #[serde(default)]
    pub projection_expression: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ScanResponse {
    pub items: Vec<Item>,
    pub count: u32,
    pub scanned_count: u32,
    /// Present when Q5 lands pagination; always omitted in Q4.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<Item>,
}

/// Per-table read spec inside a `BatchGetItem` request. `Keys` is the
/// only required field; `ProjectionExpression` / `ConsistentRead` /
/// `AttributesToGet` are accepted on the wire and validated by the
/// translator (v1: AttributesToGet rejected; the others land in B3).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KeysAndAttributes {
    pub keys: Vec<Item>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistent_read: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<std::collections::BTreeMap<String, String>>,
    /// Legacy projection format. Rektifier rejects it — see `D8` in
    /// `PLAN-6` and the matching entry in `COMPATIBILITY_NOTES.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes_to_get: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BatchGetItemRequest {
    pub request_items: std::collections::HashMap<String, KeysAndAttributes>,
    /// Accepted-and-dropped per `COMPATIBILITY_NOTES.md` "Cross-cutting"
    /// stance on metering fields.
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct BatchGetItemResponse {
    /// Per-table items; outer map key is the DDB table name. The map
    /// is always present (`{}` when empty) — never omitted, never null
    /// (D10). Per-table item lists are emitted whether empty or not.
    pub responses: std::collections::HashMap<String, Vec<Item>>,
    /// Always `{}` in v1 — rektifier has no throttle / partial-failure
    /// surface (D11). Field is present so SDK retry loops have a
    /// well-shaped value to interrogate.
    pub unprocessed_keys: std::collections::HashMap<String, KeysAndAttributes>,
}
