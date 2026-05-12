//! Translate DynamoDB operations into backend-neutral plans.
//!
//! For MVP this is a pair of free functions covering `PutItem` and `GetItem`.
//! Schemas may declare PK and SK with type S, N, or B; the translator
//! enforces that the incoming attribute matches the declared type.
//!
//! No expressions, no projections, no conditions yet — those live in their
//! own crate.

use rekt_protocol::{AttributeValue, GetItemRequest, Item, PutItemRequest};
use rekt_storage::{KeyType, KeyValue, TableShape};

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
            sk_col: self.sk_attr.as_deref(),
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
}

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
}
