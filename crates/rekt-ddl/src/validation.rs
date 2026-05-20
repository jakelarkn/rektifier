//! Pure CreateTable request validation. No PG access — all rules apply
//! to the body shape alone. PLAN-10 KD11 / KD12 + the "CreateTable
//! request validation" section.

use crate::errors::DdlError;
use rekt_protocol::CreateTableRequest;
use rekt_storage::KeyType;
use std::collections::{BTreeSet, HashSet};

/// DDB's documented table-name regex: `[a-zA-Z0-9_.-]{3,255}`.
fn is_valid_table_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-'
}

/// Reserved internal-namespace prefix. KD12: CreateTable rejects names
/// starting with this so operators can't shadow `_rektifier_tables` or
/// any sibling internal table.
const RESERVED_PREFIX: &str = "_rektifier_";

/// What survives validation: a derived plan ready for the worker.
/// Carries the inputs in their post-validation typed form so the SQL
/// generator and the metadata writer don't have to re-parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub pk_attr: String,
    pub pk_type: KeyType,
    pub sk_attr: Option<String>,
    pub sk_type: Option<KeyType>,
    /// "data" by default — rektifier doesn't expose this on the wire so
    /// the value is always the default for CreateTable-emitted tables.
    pub jsonb_col: String,
    pub billing_mode: Option<String>,
    pub provisioned_rcu: Option<i64>,
    pub provisioned_wcu: Option<i64>,
    pub tags: serde_json::Value,
}

pub fn validate_create_table(req: &CreateTableRequest) -> Result<CreateTablePlan, DdlError> {
    validate_table_name(&req.table_name)?;
    let (pk_attr, pk_type, sk_attr, sk_type) =
        validate_key_schema_and_attrs(&req.table_name, req)?;
    validate_lsi_not_supported(req)?;
    validate_stream_not_enabled(req)?;
    validate_billing_mode(req)?;

    let provisioned = req.provisioned_throughput.as_ref();
    Ok(CreateTablePlan {
        table_name: req.table_name.clone(),
        pk_attr,
        pk_type,
        sk_attr,
        sk_type,
        jsonb_col: "data".into(),
        billing_mode: req.billing_mode.clone(),
        provisioned_rcu: provisioned.map(|p| p.read_capacity_units),
        provisioned_wcu: provisioned.map(|p| p.write_capacity_units),
        tags: tags_to_json(req.tags.as_ref()),
    })
}

fn validate_table_name(name: &str) -> Result<(), DdlError> {
    // 3..=255 chars from DDB's table-name grammar.
    if name.len() < 3 || name.len() > 255 {
        return Err(DdlError::Validation(format!(
            "table name `{name}` must be between 3 and 255 characters"
        )));
    }
    if !name.chars().all(is_valid_table_name_char) {
        return Err(DdlError::Validation(format!(
            "table name `{name}` contains characters outside [a-zA-Z0-9_.-]"
        )));
    }
    // KD12: internal-namespace prefix rejection. Case-insensitive
    // because PG would lower the PG-side name, and operators have been
    // known to send `_REKTIFIER_xyz` in test scripts.
    if name.to_ascii_lowercase().starts_with(RESERVED_PREFIX) {
        return Err(DdlError::Validation(format!(
            "table names with the `_rektifier_` prefix are reserved \
             (got `{name}`)"
        )));
    }
    Ok(())
}

fn validate_key_schema_and_attrs(
    table_name: &str,
    req: &CreateTableRequest,
) -> Result<(String, KeyType, Option<String>, Option<KeyType>), DdlError> {
    let mut pk: Option<&str> = None;
    let mut sk: Option<&str> = None;
    for kse in &req.key_schema {
        match kse.key_type.as_str() {
            "HASH" => {
                if pk.is_some() {
                    return Err(DdlError::Validation(format!(
                        "table `{table_name}`: KeySchema has more than one HASH entry"
                    )));
                }
                pk = Some(&kse.attribute_name);
            }
            "RANGE" => {
                if sk.is_some() {
                    return Err(DdlError::Validation(format!(
                        "table `{table_name}`: KeySchema has more than one RANGE entry"
                    )));
                }
                sk = Some(&kse.attribute_name);
            }
            other => {
                return Err(DdlError::Validation(format!(
                    "table `{table_name}`: KeySchema KeyType must be HASH or RANGE \
                     (got `{other}`)"
                )));
            }
        }
    }
    let pk_attr = pk.ok_or_else(|| {
        DdlError::Validation(format!(
            "table `{table_name}`: KeySchema must contain exactly one HASH entry"
        ))
    })?;

    // Build attribute-name → KeyType map from AttributeDefinitions.
    // Reject unknown attribute types.
    let mut defs: std::collections::HashMap<&str, KeyType> = Default::default();
    let mut defs_order: Vec<&str> = Vec::new();
    for ad in &req.attribute_definitions {
        let kt = parse_attribute_type(table_name, &ad.attribute_name, &ad.attribute_type)?;
        if defs.insert(ad.attribute_name.as_str(), kt).is_some() {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: AttributeDefinitions has duplicate \
                 entry for `{}`",
                ad.attribute_name
            )));
        }
        defs_order.push(ad.attribute_name.as_str());
    }

    // Every key attribute (base + GSI) must appear in AttributeDefinitions.
    let mut keyed: HashSet<&str> = HashSet::new();
    keyed.insert(pk_attr);
    if let Some(s) = sk {
        keyed.insert(s);
    }
    if let Some(gsis) = req.global_secondary_indexes.as_ref() {
        for gsi in gsis {
            for kse in &gsi.key_schema {
                keyed.insert(kse.attribute_name.as_str());
            }
        }
    }
    for k in &keyed {
        if !defs.contains_key(*k) {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: KeySchema references attribute `{k}` \
                 which has no AttributeDefinitions entry"
            )));
        }
    }

    // DDB parity: every AttributeDefinitions entry must be referenced
    // by *some* key schema. Unused definitions are rejected. Ordered
    // BTreeSet so error messages are stable.
    let key_set: BTreeSet<&str> = keyed.iter().copied().collect();
    let unused: Vec<&str> = defs_order
        .iter()
        .copied()
        .filter(|n| !key_set.contains(n))
        .collect();
    if !unused.is_empty() {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: AttributeDefinitions has unused entries: \
             {unused:?}"
        )));
    }

    let pk_type = defs[pk_attr];
    let sk_type = sk.map(|s| defs[s]);
    Ok((
        pk_attr.to_string(),
        pk_type,
        sk.map(|s| s.to_string()),
        sk_type,
    ))
}

fn parse_attribute_type(
    table_name: &str,
    attr_name: &str,
    raw: &str,
) -> Result<KeyType, DdlError> {
    match raw {
        "S" => Ok(KeyType::S),
        "N" => Ok(KeyType::N),
        "B" => Ok(KeyType::B),
        other => Err(DdlError::Validation(format!(
            "table `{table_name}`: attribute `{attr_name}` has invalid \
             AttributeType `{other}` (expected S | N | B)"
        ))),
    }
}

fn validate_lsi_not_supported(req: &CreateTableRequest) -> Result<(), DdlError> {
    if let Some(lsi) = req.local_secondary_indexes.as_ref() {
        if !lsi.is_empty() {
            return Err(DdlError::Validation(
                "LocalSecondaryIndexes are not supported by rektifier; \
                 use GlobalSecondaryIndexes instead"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn validate_stream_not_enabled(req: &CreateTableRequest) -> Result<(), DdlError> {
    if let Some(spec) = req.stream_specification.as_ref() {
        if spec.stream_enabled {
            return Err(DdlError::Validation(
                "StreamSpecification.StreamEnabled = true is not supported; \
                 DDB Streams parity is deferred (PLAN-10 D10)"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn validate_billing_mode(req: &CreateTableRequest) -> Result<(), DdlError> {
    if let Some(bm) = req.billing_mode.as_deref() {
        if bm != "PROVISIONED" && bm != "PAY_PER_REQUEST" {
            return Err(DdlError::Validation(format!(
                "BillingMode `{bm}` is not one of PROVISIONED | PAY_PER_REQUEST"
            )));
        }
    }
    Ok(())
}

fn tags_to_json(tags: Option<&Vec<rekt_protocol::Tag>>) -> serde_json::Value {
    let Some(tags) = tags else {
        return serde_json::json!({});
    };
    let mut m = serde_json::Map::new();
    for t in tags {
        m.insert(t.key.clone(), serde_json::Value::String(t.value.clone()));
    }
    serde_json::Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekt_protocol::{AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, Projection};

    fn req(table_name: &str) -> CreateTableRequest {
        CreateTableRequest {
            table_name: table_name.into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "id".into(),
                key_type: "HASH".into(),
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "id".into(),
                attribute_type: "S".into(),
            }],
            ..Default::default()
        }
    }

    /// Happy path: simple hash-only table validates and surfaces the
    /// plan with the right shape.
    #[test]
    fn hash_only_validates() {
        let plan = validate_create_table(&req("Orders")).unwrap();
        assert_eq!(plan.table_name, "Orders");
        assert_eq!(plan.pk_attr, "id");
        assert_eq!(plan.pk_type, KeyType::S);
        assert!(plan.sk_attr.is_none());
        assert_eq!(plan.jsonb_col, "data");
    }

    /// Composite key with all three S/N/B attribute types validates.
    #[test]
    fn composite_key_validates() {
        let r = CreateTableRequest {
            table_name: "Events".into(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "device".into(),
                    key_type: "HASH".into(),
                },
                KeySchemaElement {
                    attribute_name: "ts".into(),
                    key_type: "RANGE".into(),
                },
            ],
            attribute_definitions: vec![
                AttributeDefinition {
                    attribute_name: "device".into(),
                    attribute_type: "S".into(),
                },
                AttributeDefinition {
                    attribute_name: "ts".into(),
                    attribute_type: "N".into(),
                },
            ],
            ..Default::default()
        };
        let plan = validate_create_table(&r).unwrap();
        assert_eq!(plan.sk_attr.as_deref(), Some("ts"));
        assert_eq!(plan.sk_type, Some(KeyType::N));
    }

    /// KD12: `_rektifier_*` is reserved. Case-insensitive.
    #[test]
    fn rejects_reserved_prefix() {
        let err = validate_create_table(&req("_rektifier_my_table")).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("reserved")));
        let err = validate_create_table(&req("_REKTIFIER_X")).unwrap_err();
        assert!(matches!(err, DdlError::Validation(_)));
    }

    #[test]
    fn rejects_short_name() {
        let err = validate_create_table(&req("ab")).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("between 3 and 255")));
    }

    #[test]
    fn rejects_disallowed_chars() {
        let err = validate_create_table(&req("has space")).unwrap_err();
        assert!(matches!(err, DdlError::Validation(_)));
    }

    /// Every AttributeDefinitions entry must be referenced by KeySchema
    /// (base or GSI). DDB-parity rejection.
    #[test]
    fn rejects_unused_attribute_definition() {
        let mut r = req("Orders");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "extra".into(),
            attribute_type: "S".into(),
        });
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("unused")));
    }

    /// Every KeySchema attribute name must appear in AttributeDefinitions.
    #[test]
    fn rejects_key_attr_without_definition() {
        let mut r = req("Orders");
        r.key_schema.push(KeySchemaElement {
            attribute_name: "missing".into(),
            key_type: "RANGE".into(),
        });
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("no AttributeDefinitions")));
    }

    /// KeyType outside {HASH, RANGE} rejected.
    #[test]
    fn rejects_invalid_key_type() {
        let mut r = req("Orders");
        r.key_schema[0].key_type = "WEIRD".into();
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("HASH or RANGE")));
    }

    /// AttributeType outside {S,N,B} rejected.
    #[test]
    fn rejects_invalid_attribute_type() {
        let mut r = req("Orders");
        r.attribute_definitions[0].attribute_type = "X".into();
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("S | N | B")));
    }

    /// LSI rejection — rektifier doesn't support LSIs (PLAN-9 covers
    /// GSIs only).
    #[test]
    fn rejects_local_secondary_indexes() {
        let mut r = req("Orders");
        r.local_secondary_indexes = Some(vec![serde_json::json!({"IndexName": "lsi"})]);
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("LocalSecondaryIndexes")));
    }

    /// Empty LSI list is fine — operators may send `LocalSecondaryIndexes: []`
    /// from SDKs that always include the field.
    #[test]
    fn accepts_empty_local_secondary_indexes_list() {
        let mut r = req("Orders");
        r.local_secondary_indexes = Some(vec![]);
        validate_create_table(&r).unwrap();
    }

    /// Streams deferred to PLAN-10 D10.
    #[test]
    fn rejects_stream_enabled_true() {
        let mut r = req("Orders");
        r.stream_specification = Some(rekt_protocol::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some("NEW_AND_OLD_IMAGES".into()),
        });
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("Streams")));
    }

    #[test]
    fn accepts_stream_enabled_false() {
        let mut r = req("Orders");
        r.stream_specification = Some(rekt_protocol::StreamSpecification {
            stream_enabled: false,
            stream_view_type: None,
        });
        validate_create_table(&r).unwrap();
    }

    /// GSI key attributes must be present in AttributeDefinitions —
    /// same rule as base-table keys.
    #[test]
    fn gsi_attrs_must_be_in_attribute_definitions() {
        let mut r = req("Orders");
        r.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
            index_name: "by_tier".into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "tier".into(),
                key_type: "HASH".into(),
            }],
            projection: Projection {
                projection_type: Some("ALL".into()),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
            on_demand_throughput: None,
        }]);
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("no AttributeDefinitions")));
    }

    /// Tags round-trip as a JSON object keyed by tag.Key. The catalog
    /// stores this verbatim in `_rektifier_tables.tags`.
    #[test]
    fn tags_serialize_to_json_object() {
        let mut r = req("Orders");
        r.tags = Some(vec![
            rekt_protocol::Tag {
                key: "env".into(),
                value: "test".into(),
            },
            rekt_protocol::Tag {
                key: "owner".into(),
                value: "team-x".into(),
            },
        ]);
        let plan = validate_create_table(&r).unwrap();
        assert_eq!(plan.tags["env"], "test");
        assert_eq!(plan.tags["owner"], "team-x");
    }

    /// BillingMode passes through; non-{PROVISIONED, PAY_PER_REQUEST}
    /// rejected.
    #[test]
    fn billing_mode_validation() {
        let mut r = req("Orders");
        r.billing_mode = Some("PAY_PER_REQUEST".into());
        let plan = validate_create_table(&r).unwrap();
        assert_eq!(plan.billing_mode.as_deref(), Some("PAY_PER_REQUEST"));

        r.billing_mode = Some("WEIRD".into());
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("PAY_PER_REQUEST")));
    }
}
