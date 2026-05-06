//! Request and response types for DynamoDB operations.
//!
//! Field names are PascalCase on the wire; we apply `rename_all = "PascalCase"`
//! at the struct level. Unknown fields on requests are ignored (serde default)
//! so that AWS-CLI-only fields like `ReturnConsumedCapacity` don't 400 us.

use crate::Item;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutItemRequest {
    pub table_name: String,
    pub item: Item,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PutItemResponse {}

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
