//! DDL operation dispatch handlers: DescribeTable, ListTables (D5);
//! CreateTable (D6); DeleteTable + UpdateTable (D7).
//!
//! These handlers are deliberately ungated by `serveable`: an operator
//! whose table has drifted needs DescribeTable to work so they can
//! diagnose. ListTables likewise returns all catalog entries regardless
//! of per-table health — matches DDB, which doesn't filter on internal
//! health either.
//!
//! D5 today; D6 / D7 grow this module.

use crate::{json_ok, parse_request, ApiError, AppState};
use bytes::Bytes;
use rekt_catalog::TableEntry;
use rekt_protocol::{
    AttributeDefinition, CreateTableRequest, CreateTableResponse, DeleteTableRequest,
    DeleteTableResponse, DescribeTableRequest, DescribeTableResponse,
    GlobalSecondaryIndexDescription, KeySchemaElement, ListTablesRequest, ListTablesResponse,
    Projection, ProvisionedThroughputDescription, TableDescription, UpdateTableRequest,
    UpdateTableResponse,
};
use rekt_storage::KeyType;

// ===== Limits & defaults ====================================================

/// DDB's documented default + max for `ListTables.Limit`. Translator
/// clamps requests above this with a ValidationException; absent
/// `Limit` defaults to this value.
const LIST_TABLES_DEFAULT_LIMIT: u32 = 100;
const LIST_TABLES_MAX_LIMIT: u32 = 100;

// ===== Helpers ==============================================================

/// Synthesize a DDB-shaped ARN for a rektifier table. PLAN-10 KD13:
/// empty region/account; the `rektifier-table` resource segment signals
/// "this is not a real AWS table" to operator tooling that parses
/// ARNs strictly.
pub(crate) fn synthesize_table_arn(table_name: &str) -> String {
    format!("arn:aws:dynamodb:::rektifier-table/{table_name}")
}

fn key_type_wire(k: KeyType) -> &'static str {
    match k {
        KeyType::S => "S",
        KeyType::N => "N",
        KeyType::B => "B",
    }
}

/// Project a catalog `TableEntry` into the wire-shape `TableDescription`
/// per PLAN-10 KD6 (status mapping) and KD13 (TableArn). Reusable across
/// CreateTable / DescribeTable / DeleteTable / UpdateTable response
/// synthesis when D6 / D7 land.
pub(crate) fn table_description_from_entry(entry: &TableEntry) -> TableDescription {
    let schema = &entry.schema;
    let mut key_schema = vec![KeySchemaElement {
        attribute_name: schema.pk_attr.clone(),
        key_type: "HASH".into(),
    }];
    let mut attr_defs = vec![AttributeDefinition {
        attribute_name: schema.pk_attr.clone(),
        attribute_type: key_type_wire(schema.pk_type).into(),
    }];
    if let (Some(sk_attr), Some(sk_type)) = (&schema.sk_attr, schema.sk_type) {
        key_schema.push(KeySchemaElement {
            attribute_name: sk_attr.clone(),
            key_type: "RANGE".into(),
        });
        attr_defs.push(AttributeDefinition {
            attribute_name: sk_attr.clone(),
            attribute_type: key_type_wire(sk_type).into(),
        });
    }

    // GSI descriptions — joined from the catalog's per-GSI cache (PLAN-9
    // populates these once it lands). Today the map is always empty.
    let gsi_descs = if entry.gsis.is_empty() {
        None
    } else {
        let mut descs: Vec<GlobalSecondaryIndexDescription> = entry
            .gsis
            .iter()
            .map(|(name, g)| GlobalSecondaryIndexDescription {
                index_name: Some(name.clone()),
                index_status: Some(g.status.clone()),
                key_schema: Some(
                    g.key_schema
                        .iter()
                        .map(|(attr, kt)| KeySchemaElement {
                            attribute_name: attr.clone(),
                            key_type: kt.clone(),
                        })
                        .collect(),
                ),
                projection: Some(Projection {
                    projection_type: Some("ALL".into()),
                    non_key_attributes: None,
                }),
                index_size_bytes: Some(0),
                item_count: Some(0),
                index_arn: None,
                backfilling: None,
                provisioned_throughput: None,
            })
            .collect();
        // Stable ordering — DDB sorts by IndexName; mirror that so
        // response diffs stay deterministic.
        descs.sort_by(|a, b| a.index_name.cmp(&b.index_name));
        Some(descs)
    };

    let provisioned = if entry.provisioned_rcu.is_some() || entry.provisioned_wcu.is_some() {
        Some(ProvisionedThroughputDescription {
            read_capacity_units: entry.provisioned_rcu,
            write_capacity_units: entry.provisioned_wcu,
            number_of_decreases_today: None,
            last_increase_date_time: None,
            last_decrease_date_time: None,
        })
    } else {
        None
    };

    TableDescription {
        table_name: Some(schema.name.clone()),
        table_status: Some(entry.status.to_wire_str().into()),
        table_arn: Some(synthesize_table_arn(&schema.name)),
        table_id: None,
        // DDB's CreationDateTime is epoch seconds with fractional ms.
        // We store ms; divide by 1000.0 so the conversion is exact for
        // any whole millisecond.
        creation_date_time: Some(entry.creation_date_ms as f64 / 1000.0),
        key_schema: Some(key_schema),
        attribute_definitions: Some(attr_defs),
        provisioned_throughput: provisioned,
        billing_mode_summary: entry.billing_mode.as_ref().map(|bm| {
            rekt_protocol::BillingModeSummary {
                billing_mode: Some(bm.clone()),
                last_update_to_pay_per_request_date_time: None,
            }
        }),
        // Inert — see COMPATIBILITY_NOTES.
        item_count: Some(0),
        table_size_bytes: Some(0),
        global_secondary_indexes: gsi_descs,
        // PLAN-11 L5 wires this from entry.lsis once the catalog grows
        // the field. For now (L1) — None.
        local_secondary_indexes: None,
        stream_specification: None,
        latest_stream_label: None,
        latest_stream_arn: None,
        table_class_summary: None,
        deletion_protection_enabled: None,
    }
}

// ===== CreateTable ===========================================================

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.create_table",
    fields(table = tracing::field::Empty)
)]
pub(crate) async fn handle_create_table(
    state: &AppState,
    body: &Bytes,
) -> Result<axum::response::Response, ApiError> {
    let req: CreateTableRequest = parse_request(body, "CreateTable")?;
    tracing::Span::current().record("table", req.table_name.as_str());
    let description = state.ddl.create_table(&req).await?;
    Ok(json_ok(&CreateTableResponse {
        table_description: description,
    }))
}

// ===== DeleteTable ===========================================================

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.delete_table",
    fields(table = tracing::field::Empty)
)]
pub(crate) async fn handle_delete_table(
    state: &AppState,
    body: &Bytes,
) -> Result<axum::response::Response, ApiError> {
    let req: DeleteTableRequest = parse_request(body, "DeleteTable")?;
    tracing::Span::current().record("table", req.table_name.as_str());
    let description = state.ddl.delete_table(&req).await?;
    Ok(json_ok(&DeleteTableResponse {
        table_description: description,
    }))
}

// ===== UpdateTable ===========================================================

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.update_table",
    fields(table = tracing::field::Empty)
)]
pub(crate) async fn handle_update_table(
    state: &AppState,
    body: &Bytes,
) -> Result<axum::response::Response, ApiError> {
    let req: UpdateTableRequest = parse_request(body, "UpdateTable")?;
    tracing::Span::current().record("table", req.table_name.as_str());
    let description = state.ddl.update_table(&req).await?;
    Ok(json_ok(&UpdateTableResponse {
        table_description: description,
    }))
}

// ===== DescribeTable =========================================================

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.describe_table",
    fields(table = tracing::field::Empty)
)]
pub(crate) async fn handle_describe_table(
    state: &AppState,
    body: &Bytes,
) -> Result<axum::response::Response, ApiError> {
    let req: DescribeTableRequest = parse_request(body, "DescribeTable")?;
    tracing::Span::current().record("table", req.table_name.as_str());

    // DescribeTable is intentionally ungated: an operator whose table
    // is unserveable still needs the description to diagnose. The wire
    // status reflects KD6's mapping (DEGRADED → ACTIVE), so the
    // response still says ACTIVE even when serveable=false. Logs +
    // direct `_rektifier_tables` reads expose the serveability detail.
    let entry = state.catalog.get(&req.table_name).ok_or_else(|| {
        ApiError::ResourceNotFound(format!("Table not found: {}", req.table_name))
    })?;
    if !entry.serveable {
        // Telemetry hint so the operator tailing logs sees DescribeTable
        // was called on a degraded table — useful when the wire response
        // hides the failure.
        tracing::warn!(
            table = %req.table_name,
            reason = entry.unserveable_reason.as_deref().unwrap_or("unknown"),
            wire_status = entry.status.to_wire_str(),
            internal_status = entry.status.as_internal_str(),
            "DescribeTable called on unserveable table; wire status hides internal state"
        );
    }
    Ok(json_ok(&DescribeTableResponse {
        table: table_description_from_entry(&entry),
    }))
}

// ===== ListTables ============================================================

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.list_tables",
    fields(limit = tracing::field::Empty, returned = tracing::field::Empty)
)]
pub(crate) async fn handle_list_tables(
    state: &AppState,
    body: &Bytes,
) -> Result<axum::response::Response, ApiError> {
    let req: ListTablesRequest = parse_request(body, "ListTables")?;

    let limit = match req.limit {
        None => LIST_TABLES_DEFAULT_LIMIT,
        Some(0) => {
            return Err(ApiError::Validation(
                "ListTables.Limit must be between 1 and 100".into(),
            ));
        }
        Some(n) if n > LIST_TABLES_MAX_LIMIT => {
            return Err(ApiError::Validation(format!(
                "ListTables.Limit must be between 1 and {LIST_TABLES_MAX_LIMIT}"
            )));
        }
        Some(n) => n,
    };
    tracing::Span::current().record("limit", limit);

    // Catalog snapshot already returns sorted ASC names — matches
    // DDB's ListTables ordering. Internal `_rektifier_*` tables are
    // never present (KD12) so no filter is needed here.
    let all_names = state.catalog.sorted_table_names();

    // ExclusiveStartTableName is a lex-boundary cursor — start strictly
    // after the named table.
    let start_idx = match req.exclusive_start_table_name.as_deref() {
        None => 0,
        Some(after) => all_names
            .iter()
            .position(|n| n.as_str() > after)
            .unwrap_or(all_names.len()),
    };
    let end_idx = start_idx
        .saturating_add(limit as usize)
        .min(all_names.len());
    let table_names = all_names[start_idx..end_idx].to_vec();
    let last_evaluated_table_name = if end_idx < all_names.len() {
        table_names.last().cloned()
    } else {
        None
    };
    tracing::Span::current().record("returned", table_names.len());

    Ok(json_ok(&ListTablesResponse {
        table_names,
        last_evaluated_table_name,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekt_catalog::TableStatus;
    use rekt_translator::TableSchema;
    use std::collections::HashMap;

    fn mk_entry(name: &str, sk: Option<(&str, KeyType)>, status: TableStatus) -> TableEntry {
        TableEntry {
            schema: TableSchema {
                name: name.into(),
                pg_table: format!("rekt_t_{name}"),
                pk_attr: "id".into(),
                pk_type: KeyType::S,
                sk_attr: sk.map(|(a, _)| a.into()),
                sk_type: sk.map(|(_, t)| t),
                jsonb_col: "data".into(),
            },
            status,
            serveable: status == TableStatus::Active,
            unserveable_reason: None,
            creation_date_ms: 1_700_000_000_000,
            last_modified_at_ms: 1_700_000_000_000,
            last_modified_by: "test".into(),
            billing_mode: None,
            provisioned_rcu: None,
            provisioned_wcu: None,
            tags: serde_json::json!({}),
            gsis: HashMap::new(),
        }
    }

    /// KD13: arn format is `arn:aws:dynamodb:::rektifier-table/<name>`
    /// — empty region+account, `rektifier-table` (not `table`) as the
    /// resource segment.
    #[test]
    fn arn_format_matches_kd13() {
        assert_eq!(
            synthesize_table_arn("Orders"),
            "arn:aws:dynamodb:::rektifier-table/Orders"
        );
    }

    /// Hash-only table: KeySchema has one HASH entry; AttributeDefinitions
    /// has the matching attr; no sort key fields emitted.
    #[test]
    fn description_hash_only() {
        let entry = mk_entry("users", None, TableStatus::Active);
        let td = table_description_from_entry(&entry);
        assert_eq!(td.table_name.as_deref(), Some("users"));
        assert_eq!(td.table_status.as_deref(), Some("ACTIVE"));
        assert_eq!(
            td.table_arn.as_deref(),
            Some("arn:aws:dynamodb:::rektifier-table/users")
        );
        let ks = td.key_schema.as_ref().unwrap();
        assert_eq!(ks.len(), 1);
        assert_eq!(ks[0].attribute_name, "id");
        assert_eq!(ks[0].key_type, "HASH");
        let ad = td.attribute_definitions.as_ref().unwrap();
        assert_eq!(ad.len(), 1);
        assert_eq!(ad[0].attribute_type, "S");
        assert!(td.global_secondary_indexes.is_none());
    }

    /// Composite-key table: KeySchema has HASH + RANGE; AttributeDefinitions
    /// has both entries with correct AttributeType.
    #[test]
    fn description_composite_key() {
        let entry = mk_entry("events", Some(("ts", KeyType::N)), TableStatus::Active);
        let td = table_description_from_entry(&entry);
        let ks = td.key_schema.as_ref().unwrap();
        assert_eq!(ks.len(), 2);
        assert_eq!(ks[0].key_type, "HASH");
        assert_eq!(ks[1].attribute_name, "ts");
        assert_eq!(ks[1].key_type, "RANGE");
        let ad = td.attribute_definitions.as_ref().unwrap();
        assert_eq!(ad.len(), 2);
        assert_eq!(ad[1].attribute_type, "N");
    }

    /// KD6 wire-status mapping: every internal variant collapses to
    /// one of the four DDB wire values; FAILED_* hide behind their
    /// in-flight counterpart; DEGRADED hides behind ACTIVE.
    #[test]
    fn description_wire_status_per_kd6() {
        for (internal, wire) in [
            (TableStatus::Creating, "CREATING"),
            (TableStatus::FailedCreating, "CREATING"),
            (TableStatus::Active, "ACTIVE"),
            (TableStatus::Degraded, "ACTIVE"),
            (TableStatus::Updating, "UPDATING"),
            (TableStatus::FailedUpdating, "UPDATING"),
            (TableStatus::Deleting, "DELETING"),
            (TableStatus::FailedDeleting, "DELETING"),
        ] {
            let entry = mk_entry("t", None, internal);
            let td = table_description_from_entry(&entry);
            assert_eq!(
                td.table_status.as_deref(),
                Some(wire),
                "internal {internal:?} should map to wire {wire}"
            );
        }
    }

    /// CreationDateTime: bigint ms → f64 seconds via division. Exact
    /// for any whole millisecond.
    #[test]
    fn creation_date_time_is_seconds_with_fraction() {
        let mut entry = mk_entry("t", None, TableStatus::Active);
        entry.creation_date_ms = 1_700_000_001_234;
        let td = table_description_from_entry(&entry);
        assert_eq!(td.creation_date_time, Some(1_700_000_001.234));
    }

    /// ItemCount and TableSizeBytes are always 0 (Inert). Document this
    /// in COMPATIBILITY_NOTES; lock it down here.
    #[test]
    fn item_count_and_table_size_are_inert_zero() {
        let entry = mk_entry("t", None, TableStatus::Active);
        let td = table_description_from_entry(&entry);
        assert_eq!(td.item_count, Some(0));
        assert_eq!(td.table_size_bytes, Some(0));
    }
}
