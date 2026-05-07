//! Translate DynamoDB operations into backend-neutral plans.
//!
//! For MVP this is a pair of free functions covering `PutItem` and `GetItem`
//! against schemas with at most one partition key (S type) and one sort key
//! (S type). No expressions, no projections, no conditions.
//!
//! The translator owns the rules that "make sense at the DynamoDB layer":
//! the key attribute exists, has the right type, and (for `GetItem`) no
//! foreign attributes appear in `Key`. Anything past that is the storage
//! layer's problem.

use rekt_protocol::{AttributeValue, GetItemRequest, Item, PutItemRequest};

/// Description of a table that the translator and the storage layer agree on.
/// Built once at server startup (or, post-MVP, looked up via `DescribeTable`).
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub pk_attr: String,
    pub sk_attr: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PutItemPlan {
    pub pk: String,
    pub sk: Option<String>,
    /// The full item in DynamoDB-JSON form, ready to drop into a `jsonb`
    /// column. Source of truth — extracted columns derive from it.
    pub item_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct GetItemPlan {
    pub pk: String,
    pub sk: Option<String>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TranslateError {
    #[error("missing partition key attribute `{attr}` in item")]
    MissingPartitionKey { attr: String },

    #[error("missing sort key attribute `{attr}` in item")]
    MissingSortKey { attr: String },

    #[error("partition key `{attr}` must be a string (type S), got {got}")]
    PartitionKeyNotString { attr: String, got: &'static str },

    #[error("sort key `{attr}` must be a string (type S), got {got}")]
    SortKeyNotString { attr: String, got: &'static str },

    #[error("Key contains unexpected attribute `{attr}` (not a key attribute)")]
    ExtraKeyAttribute { attr: String },
}

pub fn translate_put_item(
    req: &PutItemRequest,
    schema: &TableSchema,
) -> Result<PutItemPlan, TranslateError> {
    let pk = extract_pk(&req.item, &schema.pk_attr)?;
    let sk = match &schema.sk_attr {
        Some(attr) => Some(extract_sk(&req.item, attr)?),
        None => None,
    };
    let item_json =
        serde_json::to_value(&req.item).expect("AttributeValue Serialize is infallible");
    Ok(PutItemPlan { pk, sk, item_json })
}

pub fn translate_get_item(
    req: &GetItemRequest,
    schema: &TableSchema,
) -> Result<GetItemPlan, TranslateError> {
    let pk = extract_pk(&req.key, &schema.pk_attr)?;
    let sk = match &schema.sk_attr {
        Some(attr) => Some(extract_sk(&req.key, attr)?),
        None => None,
    };
    reject_extra_key_attrs(&req.key, schema)?;
    Ok(GetItemPlan { pk, sk })
}

fn extract_pk(map: &Item, attr: &str) -> Result<String, TranslateError> {
    match map.get(attr) {
        None => Err(TranslateError::MissingPartitionKey {
            attr: attr.to_string(),
        }),
        Some(AttributeValue::S(s)) => Ok(s.clone()),
        Some(other) => Err(TranslateError::PartitionKeyNotString {
            attr: attr.to_string(),
            got: other.type_name(),
        }),
    }
}

fn extract_sk(map: &Item, attr: &str) -> Result<String, TranslateError> {
    match map.get(attr) {
        None => Err(TranslateError::MissingSortKey {
            attr: attr.to_string(),
        }),
        Some(AttributeValue::S(s)) => Ok(s.clone()),
        Some(other) => Err(TranslateError::SortKeyNotString {
            attr: attr.to_string(),
            got: other.type_name(),
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
    use std::collections::BTreeMap;

    fn hash_only_schema() -> TableSchema {
        TableSchema {
            name: "users".into(),
            pk_attr: "id".into(),
            sk_attr: None,
        }
    }

    fn composite_schema() -> TableSchema {
        TableSchema {
            name: "orders".into(),
            pk_attr: "user_id".into(),
            sk_attr: Some("order_id".into()),
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
    fn put_hash_only_ok() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[
                ("id", AttributeValue::S("u1".into())),
                ("name", AttributeValue::S("alice".into())),
            ]),
        };
        let plan = translate_put_item(&req, &hash_only_schema()).unwrap();
        assert_eq!(plan.pk, "u1");
        assert_eq!(plan.sk, None);
        assert_eq!(
            plan.item_json,
            serde_json::json!({"id":{"S":"u1"},"name":{"S":"alice"}})
        );
    }

    #[test]
    fn put_composite_ok() {
        let req = PutItemRequest {
            table_name: "orders".into(),
            item: item_of(&[
                ("user_id", AttributeValue::S("u1".into())),
                ("order_id", AttributeValue::S("o#001".into())),
                ("total", AttributeValue::N("42.50".into())),
            ]),
        };
        let plan = translate_put_item(&req, &composite_schema()).unwrap();
        assert_eq!(plan.pk, "u1");
        assert_eq!(plan.sk.as_deref(), Some("o#001"));
    }

    #[test]
    fn put_missing_pk() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("name", AttributeValue::S("alice".into()))]),
        };
        let err = translate_put_item(&req, &hash_only_schema()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingPartitionKey { .. }));
    }

    #[test]
    fn put_pk_wrong_type() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("id", AttributeValue::N("42".into()))]),
        };
        let err = translate_put_item(&req, &hash_only_schema()).unwrap_err();
        match err {
            TranslateError::PartitionKeyNotString { attr, got } => {
                assert_eq!(attr, "id");
                assert_eq!(got, "N");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn put_composite_missing_sk() {
        let req = PutItemRequest {
            table_name: "orders".into(),
            item: item_of(&[("user_id", AttributeValue::S("u1".into()))]),
        };
        let err = translate_put_item(&req, &composite_schema()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingSortKey { .. }));
    }

    #[test]
    fn get_hash_only_ok() {
        let req = GetItemRequest {
            table_name: "users".into(),
            key: item_of(&[("id", AttributeValue::S("u1".into()))]),
        };
        let plan = translate_get_item(&req, &hash_only_schema()).unwrap();
        assert_eq!(plan.pk, "u1");
        assert_eq!(plan.sk, None);
    }

    #[test]
    fn get_composite_ok() {
        let req = GetItemRequest {
            table_name: "orders".into(),
            key: item_of(&[
                ("user_id", AttributeValue::S("u1".into())),
                ("order_id", AttributeValue::S("o#001".into())),
            ]),
        };
        let plan = translate_get_item(&req, &composite_schema()).unwrap();
        assert_eq!(plan.pk, "u1");
        assert_eq!(plan.sk.as_deref(), Some("o#001"));
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
        let err = translate_get_item(&req, &hash_only_schema()).unwrap_err();
        match err {
            TranslateError::ExtraKeyAttribute { attr } => assert_eq!(attr, "name"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn get_composite_missing_sk() {
        let req = GetItemRequest {
            table_name: "orders".into(),
            key: item_of(&[("user_id", AttributeValue::S("u1".into()))]),
        };
        let err = translate_get_item(&req, &composite_schema()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingSortKey { .. }));
    }
}
