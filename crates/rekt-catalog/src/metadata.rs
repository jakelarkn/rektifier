//! CRUD against `_rektifier_tables`.
//!
//! All SQL lives here, isolated from the cache mechanics. Callers
//! (seeder, reconciler, DDL workers) compose these primitives.

use crate::{CatalogError, GsiCacheEntry, TableEntry, TableStatus};
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
}

const SELECT_ALL_SQL: &str = "\
SELECT table_name, pg_table, jsonb_col, pk_attr, pk_type, sk_attr, sk_type,
       status, serveable, unserveable_reason,
       last_modified_at_ms, last_modified_by, creation_date_ms,
       tags, billing_mode, provisioned_rcu, provisioned_wcu
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
    let schema = TableSchema {
        name: row.table_name,
        pg_table: row.pg_table,
        pk_attr: row.pk_attr,
        pk_type,
        sk_attr: row.sk_attr,
        sk_type,
        jsonb_col: row.jsonb_col,
    };
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
        gsis: HashMap::<String, GsiCacheEntry>::new(),
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
        };
        let err = row_to_entry(row).unwrap_err();
        assert!(matches!(err, CatalogError::MalformedRow { .. }));
    }
}
