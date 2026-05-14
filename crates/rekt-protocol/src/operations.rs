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
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryResponse {
    pub items: Vec<Item>,
    pub count: u32,
    pub scanned_count: u32,
}
