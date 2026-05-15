//! Encode / decode the DDB pagination cursor (`ExclusiveStartKey` in,
//! `LastEvaluatedKey` out). DDB renders LEK as a regular `Item`
//! containing just the key attrs; clients pass it back verbatim as
//! ESK. Rektifier's storage layer carries the cursor as
//! `(KeyValue, Option<KeyValue>)` — this module bridges the two.

use crate::error::TranslateError;
use crate::keys::{extract_key, KeyRole};
use crate::schema::TableSchema;
use rekt_protocol::{AttributeValue, Item};
use rekt_storage::KeyValue;
use std::collections::BTreeMap;

/// Decode an `ExclusiveStartKey` `Item` into typed key values.
///
/// Validates that the ESK contains exactly the key attrs for the table
/// (pk for hash-only; pk + sk for composite), that each attribute's
/// type matches the schema, and that the ESK's PK equals the
/// `query_pk` (DDB requires the cursor to belong to the queried
/// partition).
pub(crate) fn decode_esk(
    esk: &Item,
    schema: &TableSchema,
    query_pk: &KeyValue,
) -> Result<Option<KeyValue>, TranslateError> {
    let esk_pk = extract_key(esk, &schema.pk_attr, schema.pk_type, KeyRole::Pk)
        .map_err(|_| TranslateError::InvalidExclusiveStartKey {
            reason: format!(
                "missing or wrong-typed partition key `{}`",
                schema.pk_attr
            ),
        })?;
    if &esk_pk != query_pk {
        return Err(TranslateError::ExclusiveStartKeyPkMismatch {
            attr: schema.pk_attr.clone(),
        });
    }

    let esk_sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(ty)) => Some(
            extract_key(esk, attr, ty, KeyRole::Sk).map_err(|_| {
                TranslateError::InvalidExclusiveStartKey {
                    reason: format!("missing or wrong-typed sort key `{attr}`"),
                }
            })?,
        ),
        _ => None,
    };

    // Reject extra attrs in the ESK so clients can't accidentally
    // round-trip a full item.
    let allowed: &[&str] = match &schema.sk_attr {
        Some(sk) => &[schema.pk_attr.as_str(), sk.as_str()][..],
        None => &[schema.pk_attr.as_str()][..],
    };
    let allowed_owned: std::collections::BTreeSet<&str> = allowed.iter().copied().collect();
    for attr in esk.keys() {
        if !allowed_owned.contains(attr.as_str()) {
            return Err(TranslateError::InvalidExclusiveStartKey {
                reason: format!("unexpected attribute `{attr}` in ExclusiveStartKey"),
            });
        }
    }

    Ok(esk_sk)
}

/// Encode a `(pk, sk)` cursor as a DDB-JSON `Item` for the
/// `LastEvaluatedKey` response field. The shape matches what DDB
/// returns and what `decode_esk` accepts on the next call.
pub fn encode_lek(pk: &KeyValue, sk: Option<&KeyValue>, schema: &TableSchema) -> Item {
    let mut m: BTreeMap<String, AttributeValue> = BTreeMap::new();
    m.insert(schema.pk_attr.clone(), kv_to_av(pk));
    if let (Some(sk_attr), Some(sk_val)) = (&schema.sk_attr, sk) {
        m.insert(sk_attr.clone(), kv_to_av(sk_val));
    }
    m
}

fn kv_to_av(kv: &KeyValue) -> AttributeValue {
    match kv {
        KeyValue::S(s) => AttributeValue::S(s.clone()),
        KeyValue::N(s) => AttributeValue::N(s.clone()),
        KeyValue::B(b) => AttributeValue::B(b.clone()),
    }
}

/// Validate caller-supplied `Limit`. DDB accepts `1..=1_000_000` but
/// caps real per-page returns at 1 MB; rektifier caps at 1000 items
/// (see `COMPATIBILITY_NOTES.md` — soft default of 1000).
pub(crate) fn validate_limit(limit: u32) -> Result<u32, TranslateError> {
    if !(1..=1000).contains(&limit) {
        return Err(TranslateError::InvalidLimit { got: limit });
    }
    Ok(limit)
}

