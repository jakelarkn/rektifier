//! Request and response types for DynamoDB DDL operations.
//!
//! `CreateTable`, `UpdateTable`, `DeleteTable`, `DescribeTable`, and
//! `ListTables`. Wire-shape parity with DDB: PascalCase serde rename, no
//! rektifier-specific extension fields. Unknown fields on requests are
//! ignored so SDK-emitted shapes that carry tomorrow's DDB additions
//! continue to deserialize.
//!
//! `TableDescription` is the shared response shape returned in
//! CreateTable, DeleteTable, DescribeTable, and UpdateTable responses
//! (DescribeTable wraps it in `Table`, the others in `TableDescription`).
//! See PLAN-10 KD6 for the internal-status to wire-status mapping and
//! KD13 for the `TableArn` format.

use serde::{Deserialize, Serialize};

// ===== Shared primitives =====================================================

/// One entry in a `KeySchema` — pairs an attribute name with the role it
/// plays (`HASH` partition key or `RANGE` sort key). DDB wire shape;
/// rektifier validates the (cardinality, KeyType vocabulary) tuple at
/// translate time.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct KeySchemaElement {
    pub attribute_name: String,
    pub key_type: String,
}

/// Attribute typing block referenced by `KeySchema` / GSI `KeySchema`.
/// `AttributeType` is one of `S` | `N` | `B`; the translator rejects
/// other values with `ValidationException`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: String,
}

/// Throughput knobs. Accepted-and-stored only — rektifier has no
/// metering or throttle surface (logged as Inert in COMPATIBILITY_NOTES).
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ProvisionedThroughput {
    pub read_capacity_units: i64,
    pub write_capacity_units: i64,
}

/// Response shape for throughput. DDB adds `NumberOfDecreasesToday`,
/// `LastIncreaseDateTime`, `LastDecreaseDateTime`; rektifier echoes only
/// the configured RCU/WCU values and omits the operational counters.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct ProvisionedThroughputDescription {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_capacity_units: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_capacity_units: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub number_of_decreases_today: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_increase_date_time: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_decrease_date_time: Option<f64>,
}

/// Echoed in `TableDescription.BillingModeSummary`. `BillingMode` is one
/// of `PROVISIONED` | `PAY_PER_REQUEST`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct BillingModeSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_update_to_pay_per_request_date_time: Option<f64>,
}

/// GSI / LSI projection: `ProjectionType` is `ALL` | `KEYS_ONLY` |
/// `INCLUDE`. When `INCLUDE`, `NonKeyAttributes` lists the extra
/// attributes. PLAN-9 owns enforcement; here we just carry the shape.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Projection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_key_attributes: Option<Vec<String>>,
}

/// GSI declaration inside `CreateTable.GlobalSecondaryIndexes` and
/// `UpdateTable.GlobalSecondaryIndexUpdates[].Create`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct GlobalSecondaryIndex {
    pub index_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_demand_throughput: Option<serde_json::Value>,
}

/// GSI sub-description inside `TableDescription`. `IndexStatus`
/// reflects the per-GSI lifecycle phase (PLAN-9 `_rektifier_gsi_state`)
/// mapped to the DDB wire vocabulary.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct GlobalSecondaryIndexDescription {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_schema: Option<Vec<KeySchemaElement>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<Projection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfilling: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughputDescription>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_size_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
}

/// One mutation in `UpdateTable.GlobalSecondaryIndexUpdates`.
/// Externally-tagged in JSON (`{"Create": ...}` / `{"Update": ...}` /
/// `{"Delete": ...}`); the translator validates "exactly one set".
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct GlobalSecondaryIndexUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<GlobalSecondaryIndex>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateGlobalSecondaryIndexAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete: Option<DeleteGlobalSecondaryIndexAction>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateGlobalSecondaryIndexAction {
    pub index_name: String,
    pub provisioned_throughput: ProvisionedThroughput,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteGlobalSecondaryIndexAction {
    pub index_name: String,
}

/// Stream config on `CreateTable` / `UpdateTable`. Rektifier rejects
/// `StreamEnabled = true` (Streams deferred to PLAN-10 D10); the rest
/// round-trips.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct StreamSpecification {
    pub stream_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_view_type: Option<String>,
}

/// Tag pair on `CreateTable` and `UpdateTable`. Stored verbatim in the
/// catalog as JSONB. Tag-mutation ops (TagResource / UntagResource)
/// land later (D10 deferred).
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// `TableClass`-related sub-shape. Accept-and-ignore.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct TableClassSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_update_date_time: Option<f64>,
}

/// Shared response payload: returned inside `CreateTable`, `UpdateTable`,
/// `DeleteTable`, and `DescribeTable` responses. Most fields are Option
/// so synthesis can omit anything rektifier doesn't track yet.
///
/// `TableStatus` is the wire-mapped value per PLAN-10 KD6 (internal
/// `FAILED_*` / `DEGRADED` states are collapsed to their in-flight /
/// `ACTIVE` counterparts). `TableArn` is synthesized as
/// `arn:aws:dynamodb:::rektifier-table/<name>` (KD13).
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct TableDescription {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_arn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation_date_time: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_schema: Option<Vec<KeySchemaElement>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughputDescription>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_mode_summary: Option<BillingModeSummary>,
    /// Always `0` in rektifier — see COMPATIBILITY_NOTES (Inert).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_count: Option<i64>,
    /// Always `0` in rektifier — see COMPATIBILITY_NOTES (Inert).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_size_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_secondary_indexes: Option<Vec<GlobalSecondaryIndexDescription>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_stream_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_stream_arn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_class_summary: Option<TableClassSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_protection_enabled: Option<bool>,
}

// ===== CreateTable ===========================================================

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTableRequest {
    pub table_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_secondary_indexes: Option<Vec<GlobalSecondaryIndex>>,
    /// Rektifier does not support LSIs in v1; the translator rejects
    /// non-empty values with `ValidationException`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_secondary_indexes: Option<Vec<serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<Tag>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_protection_enabled: Option<bool>,
    /// Server-side encryption config. Rektifier accepts and ignores —
    /// PG-level encryption is the operator's responsibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_specification: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_policy: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_demand_throughput: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTableResponse {
    pub table_description: TableDescription,
}

// ===== DeleteTable ===========================================================

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteTableRequest {
    pub table_name: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteTableResponse {
    pub table_description: TableDescription,
}

// ===== DescribeTable =========================================================

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeTableRequest {
    pub table_name: String,
}

/// DescribeTable's response wraps the description in `Table`, not
/// `TableDescription`. Differs from CreateTable/DeleteTable/UpdateTable.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeTableResponse {
    pub table: TableDescription,
}

// ===== ListTables ============================================================

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListTablesRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclusive_start_table_name: Option<String>,
    /// 1..=100; DDB defaults to 100. The translator clamps + validates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListTablesResponse {
    pub table_names: Vec<String>,
    /// Present when more pages remain; caller passes back verbatim as
    /// `ExclusiveStartTableName`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_evaluated_table_name: Option<String>,
}

// ===== UpdateTable ===========================================================

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateTableRequest {
    pub table_name: String,
    /// Required when GSI updates reference attributes not already in
    /// the table's existing AttributeDefinitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_secondary_index_updates: Option<Vec<GlobalSecondaryIndexUpdate>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_specification: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_updates: Option<Vec<serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_protection_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_demand_throughput: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateTableResponse {
    pub table_description: TableDescription,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// CreateTable request body from a typical `aws dynamodb
    /// create-table` invocation against a hash-only table. Verifies
    /// PascalCase field mapping and that we accept unknown top-level
    /// fields (`ReturnConsumedCapacity` would be one in practice).
    #[test]
    fn create_table_request_round_trip() {
        let raw = json!({
            "TableName": "Orders",
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "Tags": [
                {"Key": "env", "Value": "test"}
            ]
        });
        let req: CreateTableRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.table_name, "Orders");
        assert_eq!(req.key_schema.len(), 1);
        assert_eq!(req.key_schema[0].attribute_name, "id");
        assert_eq!(req.key_schema[0].key_type, "HASH");
        assert_eq!(req.attribute_definitions.len(), 1);
        assert_eq!(req.attribute_definitions[0].attribute_type, "S");
        assert_eq!(req.billing_mode.as_deref(), Some("PAY_PER_REQUEST"));
        assert!(req.provisioned_throughput.is_none());
        let tags = req.tags.as_ref().unwrap();
        assert_eq!(tags[0].key, "env");
        assert_eq!(tags[0].value, "test");
    }

    /// Composite-key shape with a single GSI declaration.
    #[test]
    fn create_table_request_with_gsi_round_trip() {
        let raw = json!({
            "TableName": "Events",
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "ts", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "ts", "AttributeType": "N"},
                {"AttributeName": "tier", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "by_tier",
                    "KeySchema": [
                        {"AttributeName": "tier", "KeyType": "HASH"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ],
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        });
        let req: CreateTableRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.key_schema.len(), 2);
        let gsis = req.global_secondary_indexes.as_ref().unwrap();
        assert_eq!(gsis.len(), 1);
        assert_eq!(gsis[0].index_name, "by_tier");
        assert_eq!(
            gsis[0].projection.projection_type.as_deref(),
            Some("ALL")
        );
        let pt = req.provisioned_throughput.as_ref().unwrap();
        assert_eq!(pt.read_capacity_units, 5);
    }

    /// CreateTable response with a synthesized TableDescription. Verifies
    /// Optional-field omission (skip_serializing_if = Option::is_none).
    #[test]
    fn create_table_response_serialize_omits_none() {
        let resp = CreateTableResponse {
            table_description: TableDescription {
                table_name: Some("Orders".into()),
                table_status: Some("CREATING".into()),
                table_arn: Some("arn:aws:dynamodb:::rektifier-table/Orders".into()),
                creation_date_time: Some(1_700_000_000.0),
                key_schema: Some(vec![KeySchemaElement {
                    attribute_name: "id".into(),
                    key_type: "HASH".into(),
                }]),
                attribute_definitions: Some(vec![AttributeDefinition {
                    attribute_name: "id".into(),
                    attribute_type: "S".into(),
                }]),
                item_count: Some(0),
                table_size_bytes: Some(0),
                ..Default::default()
            },
        };
        let v = serde_json::to_value(&resp).unwrap();
        let td = &v["TableDescription"];
        assert_eq!(td["TableName"], "Orders");
        assert_eq!(td["TableStatus"], "CREATING");
        assert_eq!(td["TableArn"], "arn:aws:dynamodb:::rektifier-table/Orders");
        assert_eq!(td["CreationDateTime"], 1_700_000_000.0);
        assert!(td.get("ProvisionedThroughput").is_none());
        assert!(td.get("GlobalSecondaryIndexes").is_none());
        assert!(td.get("BillingModeSummary").is_none());
    }

    #[test]
    fn describe_table_request_round_trip() {
        let raw = json!({"TableName": "Orders"});
        let req: DescribeTableRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.table_name, "Orders");
    }

    /// DescribeTable wraps in `Table`, not `TableDescription`. Easy to
    /// get wrong; lock it down here.
    #[test]
    fn describe_table_response_uses_table_key() {
        let resp = DescribeTableResponse {
            table: TableDescription {
                table_name: Some("Orders".into()),
                table_status: Some("ACTIVE".into()),
                ..Default::default()
            },
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert!(v.get("Table").is_some());
        assert!(v.get("TableDescription").is_none());
        assert_eq!(v["Table"]["TableName"], "Orders");
    }

    #[test]
    fn delete_table_round_trip() {
        let raw = json!({"TableName": "Orders"});
        let req: DeleteTableRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.table_name, "Orders");
        let resp = DeleteTableResponse {
            table_description: TableDescription {
                table_name: Some("Orders".into()),
                table_status: Some("DELETING".into()),
                ..Default::default()
            },
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["TableDescription"]["TableStatus"], "DELETING");
    }

    #[test]
    fn list_tables_request_optional_fields() {
        let raw = json!({});
        let req: ListTablesRequest = serde_json::from_value(raw).unwrap();
        assert!(req.exclusive_start_table_name.is_none());
        assert!(req.limit.is_none());

        let raw = json!({
            "ExclusiveStartTableName": "Foo",
            "Limit": 10
        });
        let req: ListTablesRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.exclusive_start_table_name.as_deref(), Some("Foo"));
        assert_eq!(req.limit, Some(10));
    }

    #[test]
    fn list_tables_response_round_trip() {
        let resp = ListTablesResponse {
            table_names: vec!["A".into(), "B".into()],
            last_evaluated_table_name: Some("B".into()),
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["TableNames"][0], "A");
        assert_eq!(v["LastEvaluatedTableName"], "B");

        let resp = ListTablesResponse {
            table_names: vec![],
            last_evaluated_table_name: None,
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert!(v.get("LastEvaluatedTableName").is_none());
    }

    /// UpdateTable carrying a GSI Create sub-action. The
    /// AttributeDefinitions block accompanies it because the new key
    /// attribute may not exist in the table's prior shape.
    #[test]
    fn update_table_create_gsi_round_trip() {
        let raw = json!({
            "TableName": "Events",
            "AttributeDefinitions": [
                {"AttributeName": "tier", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexUpdates": [
                {
                    "Create": {
                        "IndexName": "by_tier",
                        "KeySchema": [
                            {"AttributeName": "tier", "KeyType": "HASH"}
                        ],
                        "Projection": {"ProjectionType": "ALL"}
                    }
                }
            ]
        });
        let req: UpdateTableRequest = serde_json::from_value(raw).unwrap();
        let updates = req.global_secondary_index_updates.as_ref().unwrap();
        assert_eq!(updates.len(), 1);
        let create = updates[0].create.as_ref().unwrap();
        assert_eq!(create.index_name, "by_tier");
        assert!(updates[0].update.is_none());
        assert!(updates[0].delete.is_none());
    }

    #[test]
    fn update_table_delete_gsi_round_trip() {
        let raw = json!({
            "TableName": "Events",
            "GlobalSecondaryIndexUpdates": [
                {"Delete": {"IndexName": "by_tier"}}
            ]
        });
        let req: UpdateTableRequest = serde_json::from_value(raw).unwrap();
        let updates = req.global_secondary_index_updates.as_ref().unwrap();
        let delete = updates[0].delete.as_ref().unwrap();
        assert_eq!(delete.index_name, "by_tier");
    }

    /// Unknown top-level fields on requests must not block deserialization
    /// — SDKs send `ReturnConsumedCapacity` and friends on a variety of
    /// endpoints.
    #[test]
    fn create_table_ignores_unknown_fields() {
        let raw = json!({
            "TableName": "Orders",
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "FutureField": "ignored",
            "ReturnConsumedCapacity": "TOTAL"
        });
        let req: CreateTableRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.table_name, "Orders");
    }
}
