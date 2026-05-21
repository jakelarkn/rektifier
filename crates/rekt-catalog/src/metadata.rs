//! CRUD against `_rektifier_tables`.
//!
//! All SQL lives here, isolated from the cache mechanics. Callers
//! (seeder, reconciler, DDL workers) compose these primitives.

use crate::{CatalogError, GsiMode, GsiSpec, LsiSpec, TableEntry, TableStatus};
use deadpool_postgres::Pool;
use rekt_storage::KeyType;
use rekt_translator::TableSchema;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock now in Unix-epoch milliseconds. Single helper so every
/// catalog write stamps timestamps with the same source; the
/// `SystemTime::UNIX_EPOCH` baseline is the lifetime our `i64` field
/// agrees on.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        // Pre-1970 system clock can't really happen on a server, but if
        // it does we'd rather log 0 than panic.
        .unwrap_or(0)
}

/// Raw row shape returned by SELECT — narrower than `TableEntry`
/// because it doesn't yet carry GSI cache state (joined separately
/// when PLAN-9 lands).
#[derive(Debug, Clone)]
pub struct MetadataRow {
    pub table_name: String,
    pub pg_table: String,
    pub jsonb_col: String,
    pub pk_attr: String,
    pub pk_type: String,
    pub sk_attr: Option<String>,
    pub sk_type: Option<String>,
    pub status: String,
    pub serveable: bool,
    pub unserveable_reason: Option<String>,
    pub last_modified_at_ms: i64,
    pub last_modified_by: String,
    pub creation_date_ms: i64,
    pub tags: serde_json::Value,
    pub billing_mode: Option<String>,
    pub provisioned_rcu: Option<i64>,
    pub provisioned_wcu: Option<i64>,
    /// PLAN-11 L3: per-LSI specs, stored as a JSONB array. Empty array
    /// when the table declared no LSIs.
    pub lsi_specs: serde_json::Value,
    /// PLAN-9 G1: per-GSI specs, stored as a JSONB array.
    pub gsi_specs: serde_json::Value,
}

const SELECT_ALL_SQL: &str = "\
SELECT table_name, pg_table, jsonb_col, pk_attr, pk_type, sk_attr, sk_type,
       status, serveable, unserveable_reason,
       last_modified_at_ms, last_modified_by, creation_date_ms,
       tags, billing_mode, provisioned_rcu, provisioned_wcu,
       lsi_specs, gsi_specs
  FROM _rektifier_tables
";

/// Read every row of `_rektifier_tables` and assemble a
/// `CatalogSnapshot`-friendly entry map. Single SQL round-trip; the
/// reconciler / startup path calls this directly.
pub async fn load_snapshot(
    pool: &Pool,
) -> Result<HashMap<String, Arc<TableEntry>>, CatalogError> {
    let client = pool
        .get()
        .await
        .map_err(|e| CatalogError::Pool(format!("pool get failed: {e}")))?;
    let rows = client.query(SELECT_ALL_SQL, &[]).await?;
    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        let raw = MetadataRow {
            table_name: row.get("table_name"),
            pg_table: row.get("pg_table"),
            jsonb_col: row.get("jsonb_col"),
            pk_attr: row.get("pk_attr"),
            pk_type: row.get("pk_type"),
            sk_attr: row.get("sk_attr"),
            sk_type: row.get("sk_type"),
            status: row.get("status"),
            serveable: row.get("serveable"),
            unserveable_reason: row.get("unserveable_reason"),
            last_modified_at_ms: row.get("last_modified_at_ms"),
            last_modified_by: row.get("last_modified_by"),
            creation_date_ms: row.get("creation_date_ms"),
            tags: row.get("tags"),
            billing_mode: row.get("billing_mode"),
            provisioned_rcu: row.get("provisioned_rcu"),
            provisioned_wcu: row.get("provisioned_wcu"),
            lsi_specs: row.get("lsi_specs"),
            gsi_specs: row.get("gsi_specs"),
        };
        let entry = row_to_entry(raw)?;
        out.insert(entry.schema.name.clone(), Arc::new(entry));
    }
    Ok(out)
}

/// Project a raw row into the cache-friendly `TableEntry`. Hard-fails
/// on rows with malformed status / key-type strings — those signal
/// either operator hand-edit or rektifier regression. Better to
/// surface than silently drop.
pub fn row_to_entry(row: MetadataRow) -> Result<TableEntry, CatalogError> {
    let status =
        TableStatus::parse(&row.status).ok_or_else(|| CatalogError::MalformedRow {
            table_name: row.table_name.clone(),
            reason: format!("unknown status `{}`", row.status),
        })?;
    let pk_type = parse_key_type(&row.table_name, &row.pk_type)?;
    let sk_type = row
        .sk_type
        .as_deref()
        .map(|s| parse_key_type(&row.table_name, s))
        .transpose()?;
    if row.sk_attr.is_some() != sk_type.is_some() {
        return Err(CatalogError::MalformedRow {
            table_name: row.table_name.clone(),
            reason: "sk_attr and sk_type must be both-present or both-absent".into(),
        });
    }
    let lsis = lsi_specs_from_json(&row.table_name, &row.lsi_specs)?;
    let gsis_vec = gsi_specs_from_json(&row.table_name, &row.gsi_specs)?;
    // PLAN-11 L4. Mirror the cache's typed LsiSpec list into the
    // translator's LsiSchema map so the translator/dispatch layer can
    // resolve IndexName without re-reading the catalog row.
    let schema_lsis: std::collections::HashMap<String, rekt_translator::LsiSchema> = lsis
        .iter()
        .map(|s| {
            (
                s.name.clone(),
                rekt_translator::LsiSchema {
                    name: s.name.clone(),
                    sort_attr: s.sort_attr.clone(),
                    sort_type: s.sort_type,
                    sort_pg_col: s.sort_pg_col.clone(),
                    serveable: s.serveable,
                    unserveable_reason: s.unserveable_reason.clone(),
                },
            )
        })
        .collect();
    // PLAN-9 G1. Same shape for GSIs.
    let schema_gsis: std::collections::HashMap<String, rekt_translator::GsiSchema> = gsis_vec
        .iter()
        .map(|s| {
            (
                s.name.clone(),
                rekt_translator::GsiSchema {
                    name: s.name.clone(),
                    partition_attr: s.partition_attr.clone(),
                    partition_type: s.partition_type,
                    partition_pg_col: s.partition_pg_col.clone(),
                    sort_attr: s.sort_attr.clone(),
                    sort_type: s.sort_type,
                    sort_pg_col: s.sort_pg_col.clone(),
                    is_dual_write: matches!(s.mode, crate::GsiMode::DualWrite),
                    serveable: s.serveable,
                    unserveable_reason: s.unserveable_reason.clone(),
                },
            )
        })
        .collect();
    let schema = TableSchema {
        name: row.table_name,
        pg_table: row.pg_table,
        pk_attr: row.pk_attr,
        pk_type,
        sk_attr: row.sk_attr,
        sk_type,
        jsonb_col: row.jsonb_col,
        lsis: schema_lsis,
        gsis: schema_gsis,
    };
    // PLAN-9 G1. Materialize the GSI vec into the catalog's
    // HashMap<IndexName, GsiSpec> shape used at TableEntry storage.
    let gsis: HashMap<String, GsiSpec> = gsis_vec
        .into_iter()
        .map(|g| (g.name.clone(), g))
        .collect();
    Ok(TableEntry {
        schema,
        status,
        serveable: row.serveable,
        unserveable_reason: row.unserveable_reason,
        creation_date_ms: row.creation_date_ms,
        last_modified_at_ms: row.last_modified_at_ms,
        last_modified_by: row.last_modified_by,
        billing_mode: row.billing_mode,
        provisioned_rcu: row.provisioned_rcu,
        provisioned_wcu: row.provisioned_wcu,
        tags: row.tags,
        gsis,
        lsis,
    })
}

/// PLAN-11 L3. Serialize a `[LsiSpec]` slice to the JSONB shape
/// persisted in `_rektifier_tables.lsi_specs`. The shape is internal —
/// owned by the catalog at both write and read time — so a flat
/// hand-rolled object is fine and avoids dragging serde derives onto
/// `KeyType` (which would propagate into the storage layer).
pub fn lsi_specs_to_json(specs: &[LsiSpec]) -> serde_json::Value {
    serde_json::Value::Array(
        specs
            .iter()
            .map(|s| {
                serde_json::json!({
                    "name":        s.name,
                    "sort_attr":   s.sort_attr,
                    "sort_type":   key_type_str(s.sort_type),
                    "sort_pg_col": s.sort_pg_col,
                    "index_name":  s.index_name,
                    "projection_type":      s.projection_type,
                    "projection_non_key_attrs": s.projection_non_key_attrs,
                    "serveable":   s.serveable,
                    "unserveable_reason": s.unserveable_reason,
                })
            })
            .collect(),
    )
}

/// Inverse of `lsi_specs_to_json`. Malformed entries get surfaced as
/// `MalformedRow` so an operator-edited row hard-fails loudly instead
/// of silently dropping LSIs from the cache.
pub fn lsi_specs_from_json(
    table_name: &str,
    json: &serde_json::Value,
) -> Result<Vec<LsiSpec>, CatalogError> {
    let arr = json.as_array().ok_or_else(|| CatalogError::MalformedRow {
        table_name: table_name.into(),
        reason: "lsi_specs must be a JSON array".into(),
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for (idx, v) in arr.iter().enumerate() {
        let obj = v.as_object().ok_or_else(|| CatalogError::MalformedRow {
            table_name: table_name.into(),
            reason: format!("lsi_specs[{idx}]: expected object"),
        })?;
        let name = read_str(obj, "name", table_name, idx)?;
        let sort_attr = read_str(obj, "sort_attr", table_name, idx)?;
        let sort_type_s = read_str(obj, "sort_type", table_name, idx)?;
        let sort_type = parse_key_type(table_name, &sort_type_s)?;
        let sort_pg_col = read_str(obj, "sort_pg_col", table_name, idx)?;
        let index_name = read_str(obj, "index_name", table_name, idx)?;
        let projection_type = obj
            .get("projection_type")
            .and_then(|v| if v.is_null() { None } else { v.as_str() })
            .map(str::to_string);
        let projection_non_key_attrs = obj
            .get("projection_non_key_attrs")
            .and_then(|v| if v.is_null() { None } else { v.as_array() })
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            });
        let serveable = obj
            .get("serveable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let unserveable_reason = obj
            .get("unserveable_reason")
            .and_then(|v| if v.is_null() { None } else { v.as_str() })
            .map(str::to_string);
        out.push(LsiSpec {
            name,
            sort_attr,
            sort_type,
            sort_pg_col,
            index_name,
            projection_type,
            projection_non_key_attrs,
            serveable,
            unserveable_reason,
        });
    }
    Ok(out)
}

fn read_str(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    table_name: &str,
    idx: usize,
) -> Result<String, CatalogError> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| CatalogError::MalformedRow {
            table_name: table_name.into(),
            reason: format!("lsi_specs[{idx}]: missing or non-string `{key}`"),
        })
}

/// PLAN-9 G1. GSI analog of `lsi_specs_to_json`. Shape is internal
/// (same writer and reader own both ends) so a flat hand-rolled object
/// keeps the catalog free of serde derives on `KeyType`.
pub fn gsi_specs_to_json(specs: &[GsiSpec]) -> serde_json::Value {
    serde_json::Value::Array(
        specs
            .iter()
            .map(|s| {
                serde_json::json!({
                    "name":                 s.name,
                    "partition_attr":       s.partition_attr,
                    "partition_type":       key_type_str(s.partition_type),
                    "partition_pg_col":     s.partition_pg_col,
                    "sort_attr":            s.sort_attr,
                    "sort_type":            s.sort_type.map(key_type_str),
                    "sort_pg_col":          s.sort_pg_col,
                    "index_name":           s.index_name,
                    "projection_type":      s.projection_type,
                    "projection_non_key_attrs": s.projection_non_key_attrs,
                    "mode":                 s.mode.as_str(),
                    "serveable":            s.serveable,
                    "unserveable_reason":   s.unserveable_reason,
                })
            })
            .collect(),
    )
}

/// Inverse of `gsi_specs_to_json`. Hard-fails on malformed shape so
/// operator hand-edits don't silently drop GSIs.
pub fn gsi_specs_from_json(
    table_name: &str,
    json: &serde_json::Value,
) -> Result<Vec<GsiSpec>, CatalogError> {
    let arr = json.as_array().ok_or_else(|| CatalogError::MalformedRow {
        table_name: table_name.into(),
        reason: "gsi_specs must be a JSON array".into(),
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for (idx, v) in arr.iter().enumerate() {
        let obj = v.as_object().ok_or_else(|| CatalogError::MalformedRow {
            table_name: table_name.into(),
            reason: format!("gsi_specs[{idx}]: expected object"),
        })?;
        let name = read_str_g(obj, "name", table_name, idx)?;
        let partition_attr = read_str_g(obj, "partition_attr", table_name, idx)?;
        let partition_type_s = read_str_g(obj, "partition_type", table_name, idx)?;
        let partition_type = parse_key_type(table_name, &partition_type_s)?;
        let partition_pg_col = read_str_g(obj, "partition_pg_col", table_name, idx)?;
        let sort_attr = obj
            .get("sort_attr")
            .and_then(|v| if v.is_null() { None } else { v.as_str() })
            .map(str::to_string);
        let sort_type = match obj.get("sort_type") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(s)) => Some(parse_key_type(table_name, s)?),
            _ => {
                return Err(CatalogError::MalformedRow {
                    table_name: table_name.into(),
                    reason: format!("gsi_specs[{idx}]: `sort_type` must be string or null"),
                });
            }
        };
        let sort_pg_col = obj
            .get("sort_pg_col")
            .and_then(|v| if v.is_null() { None } else { v.as_str() })
            .map(str::to_string);
        let index_name = read_str_g(obj, "index_name", table_name, idx)?;
        let projection_type = obj
            .get("projection_type")
            .and_then(|v| if v.is_null() { None } else { v.as_str() })
            .map(str::to_string);
        let projection_non_key_attrs = obj
            .get("projection_non_key_attrs")
            .and_then(|v| if v.is_null() { None } else { v.as_array() })
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            });
        let serveable = obj
            .get("serveable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let unserveable_reason = obj
            .get("unserveable_reason")
            .and_then(|v| if v.is_null() { None } else { v.as_str() })
            .map(str::to_string);
        // PLAN-9 D17. `mode` defaults to Generated when absent so
        // pre-revision rows round-trip cleanly (every shipped GSI was
        // Generated-mode by construction). DualWrite is only written
        // when the orchestrator inserts a new GSI from UpdateTable.
        let mode = match obj.get("mode") {
            None | Some(serde_json::Value::Null) => GsiMode::Generated,
            Some(serde_json::Value::String(s)) => {
                GsiMode::parse(s).ok_or_else(|| CatalogError::MalformedRow {
                    table_name: table_name.into(),
                    reason: format!(
                        "gsi_specs[{idx}]: `mode` must be `generated` or `dual_write`, got `{s}`"
                    ),
                })?
            }
            _ => {
                return Err(CatalogError::MalformedRow {
                    table_name: table_name.into(),
                    reason: format!("gsi_specs[{idx}]: `mode` must be string"),
                });
            }
        };
        out.push(GsiSpec {
            name,
            partition_attr,
            partition_type,
            partition_pg_col,
            sort_attr,
            sort_type,
            sort_pg_col,
            index_name,
            projection_type,
            projection_non_key_attrs,
            mode,
            serveable,
            unserveable_reason,
        });
    }
    Ok(out)
}

fn read_str_g(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    table_name: &str,
    idx: usize,
) -> Result<String, CatalogError> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| CatalogError::MalformedRow {
            table_name: table_name.into(),
            reason: format!("gsi_specs[{idx}]: missing or non-string `{key}`"),
        })
}

fn parse_key_type(table_name: &str, s: &str) -> Result<KeyType, CatalogError> {
    match s {
        "S" => Ok(KeyType::S),
        "N" => Ok(KeyType::N),
        "B" => Ok(KeyType::B),
        other => Err(CatalogError::MalformedRow {
            table_name: table_name.into(),
            reason: format!("unknown key type `{other}` (expected S | N | B)"),
        }),
    }
}

/// Serialize a `KeyType` back to the wire-shape string used in
/// `_rektifier_tables.pk_type` / `.sk_type`.
pub fn key_type_str(t: KeyType) -> &'static str {
    match t {
        KeyType::S => "S",
        KeyType::N => "N",
        KeyType::B => "B",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_to_entry_hash_only() {
        let row = MetadataRow {
            table_name: "users".into(),
            pg_table: "rekt_t_users".into(),
            jsonb_col: "data".into(),
            pk_attr: "id".into(),
            pk_type: "S".into(),
            sk_attr: None,
            sk_type: None,
            status: "ACTIVE".into(),
            serveable: true,
            unserveable_reason: None,
            last_modified_at_ms: 1_700_000_000_000,
            last_modified_by: "seeder".into(),
            creation_date_ms: 1_700_000_000_000,
            tags: serde_json::json!({}),
            billing_mode: None,
            provisioned_rcu: None,
            provisioned_wcu: None,
            lsi_specs: serde_json::json!([]),
            gsi_specs: serde_json::json!([]),
        };
        let entry = row_to_entry(row).unwrap();
        assert_eq!(entry.schema.name, "users");
        assert_eq!(entry.schema.pk_type, KeyType::S);
        assert!(entry.schema.sk_attr.is_none());
        assert_eq!(entry.status, TableStatus::Active);
        assert!(entry.serveable);
    }

    #[test]
    fn row_to_entry_composite() {
        let row = MetadataRow {
            table_name: "events".into(),
            pg_table: "rekt_t_events".into(),
            jsonb_col: "doc".into(),
            pk_attr: "device".into(),
            pk_type: "S".into(),
            sk_attr: Some("ts".into()),
            sk_type: Some("N".into()),
            status: "ACTIVE".into(),
            serveable: true,
            unserveable_reason: None,
            last_modified_at_ms: 1,
            last_modified_by: "seeder".into(),
            creation_date_ms: 1,
            tags: serde_json::json!({}),
            billing_mode: None,
            provisioned_rcu: None,
            provisioned_wcu: None,
            lsi_specs: serde_json::json!([]),
            gsi_specs: serde_json::json!([]),
        };
        let entry = row_to_entry(row).unwrap();
        assert_eq!(entry.schema.sk_attr.as_deref(), Some("ts"));
        assert_eq!(entry.schema.sk_type, Some(KeyType::N));
    }

    #[test]
    fn row_to_entry_rejects_unknown_status() {
        let row = MetadataRow {
            table_name: "foo".into(),
            pg_table: "rekt_t_foo".into(),
            jsonb_col: "data".into(),
            pk_attr: "id".into(),
            pk_type: "S".into(),
            sk_attr: None,
            sk_type: None,
            status: "NOPE".into(),
            serveable: true,
            unserveable_reason: None,
            last_modified_at_ms: 1,
            last_modified_by: "x".into(),
            creation_date_ms: 1,
            tags: serde_json::json!({}),
            billing_mode: None,
            provisioned_rcu: None,
            provisioned_wcu: None,
            lsi_specs: serde_json::json!([]),
            gsi_specs: serde_json::json!([]),
        };
        let err = row_to_entry(row).unwrap_err();
        match err {
            CatalogError::MalformedRow { table_name, reason } => {
                assert_eq!(table_name, "foo");
                assert!(reason.contains("NOPE"));
            }
            _ => panic!("expected MalformedRow"),
        }
    }

    #[test]
    fn row_to_entry_rejects_half_specified_sort_key() {
        let row = MetadataRow {
            table_name: "broken".into(),
            pg_table: "rekt_t_broken".into(),
            jsonb_col: "data".into(),
            pk_attr: "id".into(),
            pk_type: "S".into(),
            sk_attr: Some("ts".into()),
            sk_type: None,
            status: "ACTIVE".into(),
            serveable: true,
            unserveable_reason: None,
            last_modified_at_ms: 1,
            last_modified_by: "x".into(),
            creation_date_ms: 1,
            tags: serde_json::json!({}),
            billing_mode: None,
            provisioned_rcu: None,
            provisioned_wcu: None,
            lsi_specs: serde_json::json!([]),
            gsi_specs: serde_json::json!([]),
        };
        let err = row_to_entry(row).unwrap_err();
        assert!(matches!(err, CatalogError::MalformedRow { .. }));
    }
}
