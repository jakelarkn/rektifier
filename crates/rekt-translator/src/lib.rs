//! Translate DynamoDB operations into backend-neutral plans.
//!
//! For MVP this is a pair of free functions covering `PutItem` and `GetItem`.
//! Schemas may declare PK and SK with type S, N, or B; the translator
//! enforces that the incoming attribute matches the declared type.
//!
//! No expressions, no projections, no conditions yet — those live in their
//! own crate.

use rekt_expressions::{
    parse_update_expression, substitute_update, Operand, ParseError, Path, PathSegment, SetRhs,
    SubstituteError, UpdateExpression,
};
use rekt_protocol::{
    AttributeValue, DeleteItemRequest, GetItemRequest, Item, PutItemRequest, UpdateItemRequest,
};
use rekt_storage::{KeyType, KeyValue, TableShape};
use std::collections::BTreeMap;

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
}

#[derive(Debug, Clone)]
pub struct PutItemPlan {
    /// Full item in DynamoDB-JSON form, source of truth for the jsonb column.
    /// PG derives the typed key columns from this via `GENERATED ALWAYS AS`,
    /// so writes don't bind pk/sk separately. The translator still validates
    /// that the expected key attributes are present and well-typed — that's
    /// protocol-level validation, independent of how PG stores them.
    pub item_json: serde_json::Value,
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
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TranslateError {
    #[error("missing partition key attribute `{attr}` in item")]
    MissingPartitionKey { attr: String },

    #[error("missing sort key attribute `{attr}` in item")]
    MissingSortKey { attr: String },

    #[error("partition key `{attr}` has wrong type: schema says {expected:?}, got {got}")]
    PartitionKeyTypeMismatch {
        attr: String,
        expected: KeyType,
        got: &'static str,
    },

    #[error("sort key `{attr}` has wrong type: schema says {expected:?}, got {got}")]
    SortKeyTypeMismatch {
        attr: String,
        expected: KeyType,
        got: &'static str,
    },

    #[error("Key contains unexpected attribute `{attr}` (not a key attribute)")]
    ExtraKeyAttribute { attr: String },

    // ----- UpdateItem-specific -----
    #[error("UpdateExpression is empty (must contain at least one clause)")]
    EmptyUpdateExpression,

    #[error("invalid UpdateExpression: {0}")]
    InvalidUpdateExpression(String),

    #[error("UpdateExpression references unknown placeholder: {0}")]
    UnknownPlaceholder(String),

    #[error(
        "UpdateExpression touches key attribute `{attr}` — key attributes are determined by the `Key` field, not by `UpdateExpression`"
    )]
    UpdateTouchesKey { attr: String },

    /// Nested paths (e.g. `meta.score`, `items[3]`) are valid DDB but
    /// the translator only supports top-level paths in this phase. Phase
    /// 8 lifts this restriction.
    #[error(
        "nested paths are not yet supported (got `{path}`) — Phase 8 will lift this restriction"
    )]
    UnsupportedNestedPath { path: String },

    /// Phase 2 only supports `SET attr = :value` (literal). Phase 3 will
    /// add path-refs / arithmetic / if_not_exists / list_append via the
    /// read-modify-write fallback.
    #[error("only literal SET right-hand sides are supported in this phase (got `{shape}`)")]
    UnsupportedSetRhs { shape: &'static str },

    #[error("ADD clauses are not yet supported (phase 5 will add them)")]
    UnsupportedAddClause,

    #[error("DELETE clauses are not yet supported (phase 6 will add them)")]
    UnsupportedDeleteClause,

    #[error("ConditionExpression is not yet supported (phase 4 will add it)")]
    UnsupportedConditionExpression,

    #[error("only `ReturnValues=NONE` is supported in this phase (got `{got}`)")]
    UnsupportedReturnValues { got: String },
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.put_item", fields(table = %schema.name))]
pub fn translate_put_item(
    req: &PutItemRequest,
    schema: &TableSchema,
) -> Result<PutItemPlan, TranslateError> {
    // Validate the key attributes for protocol correctness. We discard the
    // extracted KeyValues — PG derives the actual key columns from the JSONB
    // via GENERATED ALWAYS AS, so the storage layer never binds them on writes.
    let _ = extract_key(&req.item, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    if let (Some(attr), Some(t)) = (&schema.sk_attr, schema.sk_type) {
        let _ = extract_key(&req.item, attr, t, KeyRole::Sk)?;
    }
    let item_json =
        serde_json::to_value(&req.item).expect("AttributeValue Serialize is infallible");
    Ok(PutItemPlan { item_json })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.get_item", fields(table = %schema.name))]
pub fn translate_get_item(
    req: &GetItemRequest,
    schema: &TableSchema,
) -> Result<GetItemPlan, TranslateError> {
    let pk = extract_key(&req.key, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    let sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(t)) => Some(extract_key(&req.key, attr, t, KeyRole::Sk)?),
        _ => None,
    };
    reject_extra_key_attrs(&req.key, schema)?;
    Ok(GetItemPlan { pk, sk })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.delete_item", fields(table = %schema.name))]
pub fn translate_delete_item(
    req: &DeleteItemRequest,
    schema: &TableSchema,
) -> Result<DeleteItemPlan, TranslateError> {
    let pk = extract_key(&req.key, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    let sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(t)) => Some(extract_key(&req.key, attr, t, KeyRole::Sk)?),
        _ => None,
    };
    reject_extra_key_attrs(&req.key, schema)?;
    Ok(DeleteItemPlan { pk, sk })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.update_item", fields(table = %schema.name))]
pub fn translate_update_item(
    req: &UpdateItemRequest,
    schema: &TableSchema,
) -> Result<UpdateItemPlan, TranslateError> {
    // 1. Disallow features not yet supported in this phase. Reject early so
    //    the rest of the translator only deals with the supported subset.
    if let Some(c) = req.condition_expression.as_deref() {
        if !c.is_empty() {
            return Err(TranslateError::UnsupportedConditionExpression);
        }
    }
    match req.return_values.as_deref() {
        None | Some("NONE") => {}
        Some(other) => {
            return Err(TranslateError::UnsupportedReturnValues {
                got: other.to_string(),
            });
        }
    }

    // 2. Validate the Key against the schema (same machinery as GetItem /
    //    DeleteItem). Caller must supply pk (and sk if composite); no
    //    foreign attributes in Key.
    let pk = extract_key(&req.key, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    let sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(t)) => Some(extract_key(&req.key, attr, t, KeyRole::Sk)?),
        _ => None,
    };
    reject_extra_key_attrs(&req.key, schema)?;

    // 3. Parse + resolve the UpdateExpression. ExpressionAttributeNames /
    //    ExpressionAttributeValues are optional in the request.
    let raw = parse_update_expression(&req.update_expression)?;
    let empty_names: BTreeMap<String, String> = BTreeMap::new();
    let empty_values: BTreeMap<String, AttributeValue> = BTreeMap::new();
    let names = req
        .expression_attribute_names
        .as_ref()
        .unwrap_or(&empty_names);
    let values = req
        .expression_attribute_values
        .as_ref()
        .unwrap_or(&empty_values);
    let expression = substitute_update(raw, names, values)?;

    // 4. Reject empty expressions and clauses we haven't implemented yet.
    if expression.set.is_empty() && expression.remove.is_empty() {
        if !expression.add.is_empty() {
            return Err(TranslateError::UnsupportedAddClause);
        }
        if !expression.delete.is_empty() {
            return Err(TranslateError::UnsupportedDeleteClause);
        }
        return Err(TranslateError::EmptyUpdateExpression);
    }
    if !expression.add.is_empty() {
        return Err(TranslateError::UnsupportedAddClause);
    }
    if !expression.delete.is_empty() {
        return Err(TranslateError::UnsupportedDeleteClause);
    }

    // 5. Per-clause validation:
    //    - paths must be top-level
    //    - paths must not name a key attribute
    //    - SET RHS must be a literal (Operand::Value)
    for clause in &expression.set {
        check_top_level(&clause.path)?;
        check_not_key(&clause.path, schema)?;
        check_set_rhs_is_literal(&clause.value)?;
    }
    for path in &expression.remove {
        check_top_level(path)?;
        check_not_key(path, schema)?;
    }

    Ok(UpdateItemPlan { pk, sk, expression })
}

fn check_top_level(p: &Path) -> Result<(), TranslateError> {
    if p.is_top_level() {
        Ok(())
    } else {
        Err(TranslateError::UnsupportedNestedPath {
            path: format_path(p),
        })
    }
}

fn check_not_key(p: &Path, schema: &TableSchema) -> Result<(), TranslateError> {
    let name = match p.top_name() {
        Some(n) => n,
        None => return Ok(()),
    };
    if name == schema.pk_attr || schema.sk_attr.as_deref() == Some(name) {
        return Err(TranslateError::UpdateTouchesKey {
            attr: name.to_string(),
        });
    }
    Ok(())
}

fn check_set_rhs_is_literal(rhs: &SetRhs) -> Result<(), TranslateError> {
    match rhs {
        SetRhs::Operand(Operand::Value(_)) => Ok(()),
        SetRhs::Operand(Operand::Path(_)) => Err(TranslateError::UnsupportedSetRhs {
            shape: "path reference (e.g. `SET a = b`)",
        }),
        SetRhs::Plus(_, _) => Err(TranslateError::UnsupportedSetRhs {
            shape: "arithmetic `+`",
        }),
        SetRhs::Minus(_, _) => Err(TranslateError::UnsupportedSetRhs {
            shape: "arithmetic `-`",
        }),
        SetRhs::IfNotExists(_, _) => Err(TranslateError::UnsupportedSetRhs {
            shape: "if_not_exists(...)",
        }),
        SetRhs::ListAppend(_, _) => Err(TranslateError::UnsupportedSetRhs {
            shape: "list_append(...)",
        }),
    }
}

fn format_path(p: &Path) -> String {
    let mut out = String::new();
    for (i, seg) in p.segments.iter().enumerate() {
        match seg {
            PathSegment::Name(n) => {
                if i > 0 {
                    out.push('.');
                }
                out.push_str(n);
            }
            PathSegment::Index(n) => {
                out.push('[');
                out.push_str(&n.to_string());
                out.push(']');
            }
        }
    }
    out
}

impl From<ParseError> for TranslateError {
    fn from(e: ParseError) -> Self {
        TranslateError::InvalidUpdateExpression(e.to_string())
    }
}

impl From<SubstituteError> for TranslateError {
    fn from(e: SubstituteError) -> Self {
        match e {
            SubstituteError::UnknownName { name } => {
                TranslateError::UnknownPlaceholder(format!("#{name}"))
            }
            SubstituteError::UnknownValue { name } => {
                TranslateError::UnknownPlaceholder(format!(":{name}"))
            }
        }
    }
}

#[derive(Copy, Clone)]
enum KeyRole {
    Pk,
    Sk,
}

fn extract_key(
    map: &Item,
    attr: &str,
    expected: KeyType,
    role: KeyRole,
) -> Result<KeyValue, TranslateError> {
    let av = map.get(attr).ok_or_else(|| match role {
        KeyRole::Pk => TranslateError::MissingPartitionKey {
            attr: attr.to_string(),
        },
        KeyRole::Sk => TranslateError::MissingSortKey {
            attr: attr.to_string(),
        },
    })?;

    match (expected, av) {
        (KeyType::S, AttributeValue::S(s)) => Ok(KeyValue::S(s.clone())),
        (KeyType::N, AttributeValue::N(s)) => Ok(KeyValue::N(s.clone())),
        (KeyType::B, AttributeValue::B(b)) => Ok(KeyValue::B(b.clone())),
        (_, other) => Err(match role {
            KeyRole::Pk => TranslateError::PartitionKeyTypeMismatch {
                attr: attr.to_string(),
                expected,
                got: other.type_name(),
            },
            KeyRole::Sk => TranslateError::SortKeyTypeMismatch {
                attr: attr.to_string(),
                expected,
                got: other.type_name(),
            },
        }),
    }
}

fn reject_extra_key_attrs(key: &Item, schema: &TableSchema) -> Result<(), TranslateError> {
    for attr in key.keys() {
        let allowed = attr == &schema.pk_attr || schema.sk_attr.as_deref() == Some(attr.as_str());
        if !allowed {
            return Err(TranslateError::ExtraKeyAttribute { attr: attr.clone() });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::BTreeMap;

    fn schema_hash_s() -> TableSchema {
        TableSchema {
            name: "users".into(),
            pg_table: "users".into(),
            pk_attr: "id".into(),
            pk_type: KeyType::S,
            sk_attr: None,
            sk_type: None,
            jsonb_col: "data".into(),
        }
    }

    fn schema_composite_s_n() -> TableSchema {
        TableSchema {
            name: "events".into(),
            pg_table: "events".into(),
            pk_attr: "device_id".into(),
            pk_type: KeyType::S,
            sk_attr: Some("ts".into()),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc".into(),
        }
    }

    fn schema_hash_b() -> TableSchema {
        TableSchema {
            name: "blobs".into(),
            pg_table: "blobs".into(),
            pk_attr: "hash".into(),
            pk_type: KeyType::B,
            sk_attr: None,
            sk_type: None,
            jsonb_col: "meta".into(),
        }
    }

    fn item_of(pairs: &[(&str, AttributeValue)]) -> Item {
        let mut m: BTreeMap<String, AttributeValue> = BTreeMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), v.clone());
        }
        m
    }

    #[test]
    fn put_hash_s_ok() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[
                ("id", AttributeValue::S("u1".into())),
                ("name", AttributeValue::S("alice".into())),
            ]),
        };
        let plan = translate_put_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            plan.item_json,
            serde_json::json!({"id":{"S":"u1"},"name":{"S":"alice"}})
        );
    }

    #[test]
    fn put_composite_s_n_ok() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::N("1234567890".into())),
                ("value", AttributeValue::N("99".into())),
            ]),
        };
        let plan = translate_put_item(&req, &schema_composite_s_n()).unwrap();
        // PutItemPlan no longer carries pk/sk; PG derives them from the JSONB.
        // Translator's job is still to validate the attrs are present and
        // typed correctly (this call would error if not).
        let stored = plan.item_json;
        assert_eq!(stored["device_id"], serde_json::json!({"S":"dev-1"}));
        assert_eq!(stored["ts"], serde_json::json!({"N":"1234567890"}));
    }

    #[test]
    fn put_hash_b_ok() {
        let req = PutItemRequest {
            table_name: "blobs".into(),
            item: item_of(&[
                ("hash", AttributeValue::B(Bytes::from_static(b"\x00\x01"))),
                ("size", AttributeValue::N("2".into())),
            ]),
        };
        let plan = translate_put_item(&req, &schema_hash_b()).unwrap();
        // base64 of {0x00, 0x01} is "AAE=".
        assert_eq!(plan.item_json["hash"], serde_json::json!({"B":"AAE="}));
    }

    #[test]
    fn put_pk_wrong_type() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("id", AttributeValue::N("42".into()))]),
        };
        let err = translate_put_item(&req, &schema_hash_s()).unwrap_err();
        match err {
            TranslateError::PartitionKeyTypeMismatch {
                attr,
                expected,
                got,
            } => {
                assert_eq!(attr, "id");
                assert_eq!(expected, KeyType::S);
                assert_eq!(got, "N");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn put_sk_wrong_type() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::S("not a number".into())),
            ]),
        };
        let err = translate_put_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::SortKeyTypeMismatch { .. }));
    }

    #[test]
    fn put_missing_pk() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("name", AttributeValue::S("alice".into()))]),
        };
        let err = translate_put_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingPartitionKey { .. }));
    }

    #[test]
    fn put_composite_missing_sk() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[("device_id", AttributeValue::S("dev-1".into()))]),
        };
        let err = translate_put_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingSortKey { .. }));
    }

    #[test]
    fn get_composite_ok() {
        let req = GetItemRequest {
            table_name: "events".into(),
            key: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::N("1234".into())),
            ]),
        };
        let plan = translate_get_item(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("dev-1".into()));
        assert_eq!(plan.sk, Some(KeyValue::N("1234".into())));
    }

    #[test]
    fn get_hash_ok() {
        let req = GetItemRequest {
            table_name: "users".into(),
            key: item_of(&[("id", AttributeValue::S("u1".into()))]),
        };
        let plan = translate_get_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("u1".into()));
        assert_eq!(plan.sk, None);
    }

    #[test]
    fn get_extra_key_attr_rejected() {
        let req = GetItemRequest {
            table_name: "users".into(),
            key: item_of(&[
                ("id", AttributeValue::S("u1".into())),
                ("name", AttributeValue::S("alice".into())),
            ]),
        };
        let err = translate_get_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn schema_shape_borrow() {
        let s = schema_composite_s_n();
        let shape = s.shape();
        assert_eq!(shape.table, "events");
        assert_eq!(shape.pk_col, "device_id");
        assert_eq!(shape.sk_col, Some("ts"));
        assert_eq!(shape.jsonb_col, "doc");
    }

    // ============================================================
    // UpdateItem translator tests
    // ============================================================

    fn upd(
        table: &str,
        key: &[(&str, AttributeValue)],
        expr: &str,
        values: &[(&str, AttributeValue)],
        names: &[(&str, &str)],
    ) -> UpdateItemRequest {
        UpdateItemRequest {
            table_name: table.into(),
            key: item_of(key),
            update_expression: expr.into(),
            expression_attribute_names: if names.is_empty() {
                None
            } else {
                Some(
                    names
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                )
            },
            expression_attribute_values: if values.is_empty() {
                None
            } else {
                Some(
                    values
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                )
            },
            condition_expression: None,
            return_values: None,
        }
    }

    fn set_literal_value(plan: &UpdateItemPlan, attr: &str) -> AttributeValue {
        let c = plan
            .expression
            .set
            .iter()
            .find(|c| c.path.top_name() == Some(attr))
            .unwrap_or_else(|| panic!("no SET clause for `{attr}`"));
        match &c.value {
            SetRhs::Operand(Operand::Value(v)) => v.clone(),
            other => panic!("expected literal SET RHS for `{attr}`, got {other:?}"),
        }
    }

    // ----- Happy paths: SET literal across every AttributeValue variant ----

    #[test]
    fn upd_set_string_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET name = :v",
            &[(":v", AttributeValue::S("alice".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("u1".into()));
        assert_eq!(
            set_literal_value(&plan, "name"),
            AttributeValue::S("alice".into())
        );
    }

    #[test]
    fn upd_set_number_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET score = :v",
            &[(":v", AttributeValue::N("42.5".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            set_literal_value(&plan, "score"),
            AttributeValue::N("42.5".into())
        );
    }

    #[test]
    fn upd_set_binary_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET blob = :v",
            &[(":v", AttributeValue::B(Bytes::from_static(b"\x00\x01\x02")))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(matches!(
            set_literal_value(&plan, "blob"),
            AttributeValue::B(_)
        ));
    }

    #[test]
    fn upd_set_bool_value() {
        for b in [true, false] {
            let req = upd(
                "users",
                &[("id", AttributeValue::S("u1".into()))],
                "SET active = :v",
                &[(":v", AttributeValue::Bool(b))],
                &[],
            );
            let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
            assert_eq!(set_literal_value(&plan, "active"), AttributeValue::Bool(b));
        }
    }

    #[test]
    fn upd_set_null_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET deleted = :v",
            &[(":v", AttributeValue::Null)],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "deleted"), AttributeValue::Null);
    }

    #[test]
    fn upd_set_list_value() {
        let list = AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::N("1".into()),
            AttributeValue::Bool(true),
        ]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET items = :v",
            &[(":v", list.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "items"), list);
    }

    #[test]
    fn upd_set_map_value() {
        let mut inner = BTreeMap::new();
        inner.insert("vip".into(), AttributeValue::Bool(true));
        inner.insert("score".into(), AttributeValue::N("99".into()));
        let map = AttributeValue::M(inner);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET meta = :v",
            &[(":v", map.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "meta"), map);
    }

    #[test]
    fn upd_set_string_set_value() {
        let ss = AttributeValue::Ss(vec!["a".into(), "b".into()]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET tags = :v",
            &[(":v", ss.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "tags"), ss);
    }

    #[test]
    fn upd_set_number_set_value() {
        let ns = AttributeValue::Ns(vec!["1".into(), "2".into()]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET scores = :v",
            &[(":v", ns.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "scores"), ns);
    }

    #[test]
    fn upd_set_binary_set_value() {
        let bs = AttributeValue::Bs(vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET chunks = :v",
            &[(":v", bs.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "chunks"), bs);
    }

    // ----- Multi-clause & composite keys -----------------------------------

    #[test]
    fn upd_set_multiple_attributes() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :s, score = :n, active = :b",
            &[
                (":s", AttributeValue::S("active".into())),
                (":n", AttributeValue::N("42".into())),
                (":b", AttributeValue::Bool(true)),
            ],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set.len(), 3);
        assert_eq!(
            set_literal_value(&plan, "status"),
            AttributeValue::S("active".into())
        );
        assert_eq!(
            set_literal_value(&plan, "score"),
            AttributeValue::N("42".into())
        );
        assert_eq!(
            set_literal_value(&plan, "active"),
            AttributeValue::Bool(true)
        );
    }

    #[test]
    fn upd_remove_single() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE deprecated_field",
            &[],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.remove.len(), 1);
        assert_eq!(
            plan.expression.remove[0].top_name(),
            Some("deprecated_field")
        );
    }

    #[test]
    fn upd_remove_multiple() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE a, b, c",
            &[],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.remove.len(), 3);
    }

    #[test]
    fn upd_set_and_remove_combined() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :s REMOVE deprecated_field",
            &[(":s", AttributeValue::S("active".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set.len(), 1);
        assert_eq!(plan.expression.remove.len(), 1);
    }

    #[test]
    fn upd_composite_key_passthrough() {
        let req = upd(
            "events",
            &[
                ("device_id", AttributeValue::S("d1".into())),
                ("ts", AttributeValue::N("1000".into())),
            ],
            "SET value = :v",
            &[(":v", AttributeValue::N("42".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("d1".into()));
        assert_eq!(plan.sk, Some(KeyValue::N("1000".into())));
    }

    #[test]
    fn upd_name_placeholder_substitutes() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET #s = :v",
            &[(":v", AttributeValue::S("ok".into()))],
            &[("#s", "status")],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set[0].path.top_name(), Some("status"));
    }

    // ----- Reject paths that touch key attributes --------------------------

    #[test]
    fn upd_reject_set_on_pk_attr() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET id = :v",
            &[(":v", AttributeValue::S("u2".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "id"));
    }

    #[test]
    fn upd_reject_remove_on_pk_attr() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE id",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "id"));
    }

    #[test]
    fn upd_reject_set_on_sk_attr() {
        let req = upd(
            "events",
            &[
                ("device_id", AttributeValue::S("d1".into())),
                ("ts", AttributeValue::N("1000".into())),
            ],
            "SET ts = :v",
            &[(":v", AttributeValue::N("2000".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "ts"));
    }

    // ----- Reject nested / indexed paths (deferred to Phase 8) ------------

    #[test]
    fn upd_reject_dotted_path() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET meta.score = :v",
            &[(":v", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedNestedPath { ref path } if path == "meta.score")
        );
    }

    #[test]
    fn upd_reject_indexed_path() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET items[3] = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedNestedPath { ref path } if path == "items[3]")
        );
    }

    // ----- Reject SET RHS shapes deferred to later phases -----------------

    #[test]
    fn upd_reject_set_path_ref() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET a = b",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedSetRhs { .. }));
    }

    #[test]
    fn upd_reject_set_arithmetic() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET total = subtotal + :tax",
            &[(":tax", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedSetRhs { .. }));
    }

    #[test]
    fn upd_reject_if_not_exists() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET created_at = if_not_exists(created_at, :now)",
            &[(":now", AttributeValue::N("1700000000".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedSetRhs { .. }));
    }

    #[test]
    fn upd_reject_list_append() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET items = list_append(items, :new)",
            &[(":new", AttributeValue::L(vec![]))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedSetRhs { .. }));
    }

    // ----- Reject deferred clauses (ADD, DELETE, ConditionExpression, etc.)

    #[test]
    fn upd_reject_add_clause() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD count :one",
            &[(":one", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedAddClause));
    }

    #[test]
    fn upd_reject_delete_clause() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "DELETE tags :rm",
            &[(":rm", AttributeValue::Ss(vec!["x".into()]))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedDeleteClause));
    }

    #[test]
    fn upd_reject_condition_expression() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.condition_expression = Some("attribute_exists(id)".into());
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionExpression
        ));
    }

    #[test]
    fn upd_accept_explicit_return_values_none() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("NONE".into());
        assert!(translate_update_item(&req, &schema_hash_s()).is_ok());
    }

    #[test]
    fn upd_reject_return_values_all_old() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("ALL_OLD".into());
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedReturnValues { ref got } if got == "ALL_OLD")
        );
    }

    // ----- Parse / substitute errors propagate -----------------------------

    #[test]
    fn upd_propagates_parse_error() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = ",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidUpdateExpression(_)));
    }

    #[test]
    fn upd_propagates_unknown_value_placeholder() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :v",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnknownPlaceholder(ref p) if p == ":v"));
    }

    #[test]
    fn upd_propagates_unknown_name_placeholder() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET #x = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnknownPlaceholder(ref p) if p == "#x"));
    }

    // ----- Key validation reuses existing machinery -----------------------

    #[test]
    fn upd_reject_missing_key() {
        // Same as Get/Delete: missing pk -> MissingPartitionKey.
        let req = upd(
            "users",
            &[],
            "SET status = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingPartitionKey { .. }));
    }

    #[test]
    fn upd_reject_extra_key_attr() {
        let req = upd(
            "users",
            &[
                ("id", AttributeValue::S("u1".into())),
                ("rogue", AttributeValue::S("x".into())),
            ],
            "SET status = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn upd_reject_empty_expression() {
        // Empty after parsing (the parser itself rejects this earlier; we
        // also have to handle "expression parsed but has no clauses we
        // support" — though in practice an empty post-parse expression is
        // unreachable for now since the parser requires at least one clause).
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        // Parser rejects empty string with ParseError::Empty, which we
        // wrap as InvalidUpdateExpression.
        assert!(matches!(err, TranslateError::InvalidUpdateExpression(_)));
    }
}
