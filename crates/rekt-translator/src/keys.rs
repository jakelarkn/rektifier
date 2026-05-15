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

/// Extract `(pk, Some(sk))` for composite tables or `(pk, None)` for
/// hash-only, validating each attribute's declared type. Consolidates
/// the "pk + optional sk per the table's shape" pattern that every
/// translator entry point repeats — single-row (Put/Get/Delete/Update)
/// and the per-key loops in BatchGetItem / BatchWriteItem.
///
/// Does *not* check for extra attributes — callers that need that
/// (single-row reads / Delete keys; BatchGetItem keys) follow with
/// `reject_extra_key_attrs`. Item-shaped callers (PutItem.Item /
/// BatchWriteItem PutRequest.Item) don't, since items carry non-key
/// attrs by design.
///
/// Not used by `paging::decode_esk` / `decode_scan_esk` — those wrap
/// the underlying `extract_key` error into `InvalidExclusiveStartKey`,
/// which this helper's generic `TranslateError` return can't preserve.
/// `extract_key` stays exported for those direct callers.
pub(crate) fn extract_key_pair(
    item: &Item,
    schema: &TableSchema,
) -> Result<(KeyValue, Option<KeyValue>), TranslateError> {
    let pk = extract_key(item, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    let sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(t)) => Some(extract_key(item, attr, t, KeyRole::Sk)?),
        _ => None,
    };
    Ok((pk, sk))
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
