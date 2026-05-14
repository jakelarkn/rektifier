//! Key extraction + validation utilities shared by `translate_*_item`.

use crate::error::TranslateError;
use crate::schema::TableSchema;
use rekt_protocol::{AttributeValue, Item};
use rekt_storage::{KeyType, KeyValue};

#[derive(Copy, Clone)]
pub(crate) enum KeyRole {
    Pk,
    Sk,
}

pub(crate) fn extract_key(
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

pub(crate) fn reject_extra_key_attrs(
    key: &Item,
    schema: &TableSchema,
) -> Result<(), TranslateError> {
    for attr in key.keys() {
        let allowed = attr == &schema.pk_attr || schema.sk_attr.as_deref() == Some(attr.as_str());
        if !allowed {
            return Err(TranslateError::ExtraKeyAttribute { attr: attr.clone() });
        }
    }
    Ok(())
}
