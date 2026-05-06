//! Round-trip tests for AttributeValue and request/response types.
//!
//! For each scenario: parse JSON -> serialize -> parse again, assert that the
//! two parsed values are equal. This catches asymmetry between Serialize and
//! Deserialize without depending on exact byte equality of the JSON output.

use bytes::Bytes;
use rekt_protocol::{AttributeValue, GetItemRequest, GetItemResponse, Item, PutItemRequest};
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn roundtrip(s: &str) -> AttributeValue {
    let first: AttributeValue = serde_json::from_str(s).expect("first parse");
    let reserialized = serde_json::to_string(&first).expect("serialize");
    let second: AttributeValue = serde_json::from_str(&reserialized).expect("second parse");
    assert_eq!(first, second, "round-trip mismatch for input: {s}");
    first
}

#[test]
fn av_string() {
    assert_eq!(
        roundtrip(r#"{"S":"hello"}"#),
        AttributeValue::S("hello".into())
    );
    assert_eq!(roundtrip(r#"{"S":""}"#), AttributeValue::S(String::new()));
}

#[test]
fn av_number_preserves_string() {
    // 38 digits — `f64` would mangle this. AttributeValue::N keeps it verbatim.
    let big = r#"{"N":"123456789012345678901234567890.12345678"}"#;
    let av = roundtrip(big);
    assert_eq!(
        av,
        AttributeValue::N("123456789012345678901234567890.12345678".into())
    );
}

#[test]
fn av_binary_base64() {
    // base64("hello") == "aGVsbG8="
    let av = roundtrip(r#"{"B":"aGVsbG8="}"#);
    assert_eq!(av, AttributeValue::B(Bytes::from_static(b"hello")));
}

#[test]
fn av_bool() {
    assert_eq!(roundtrip(r#"{"BOOL":true}"#), AttributeValue::Bool(true));
    assert_eq!(roundtrip(r#"{"BOOL":false}"#), AttributeValue::Bool(false));
}

#[test]
fn av_null_only_accepts_true() {
    assert_eq!(roundtrip(r#"{"NULL":true}"#), AttributeValue::Null);
    let err: Result<AttributeValue, _> = serde_json::from_str(r#"{"NULL":false}"#);
    assert!(err.is_err(), "NULL:false must be rejected");
}

#[test]
fn av_list_mixed() {
    let s = r#"{"L":[{"S":"a"},{"N":"1"},{"BOOL":true},{"NULL":true}]}"#;
    let av = roundtrip(s);
    assert_eq!(
        av,
        AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::N("1".into()),
            AttributeValue::Bool(true),
            AttributeValue::Null,
        ])
    );
}

#[test]
fn av_map_nested() {
    let s = r#"{"M":{"name":{"S":"alice"},"age":{"N":"30"},"meta":{"M":{"vip":{"BOOL":true}}}}}"#;
    let av = roundtrip(s);
    let mut inner = BTreeMap::new();
    inner.insert("vip".to_string(), AttributeValue::Bool(true));
    let mut outer = BTreeMap::new();
    outer.insert("name".to_string(), AttributeValue::S("alice".into()));
    outer.insert("age".to_string(), AttributeValue::N("30".into()));
    outer.insert("meta".to_string(), AttributeValue::M(inner));
    assert_eq!(av, AttributeValue::M(outer));
}

#[test]
fn av_string_set() {
    let av = roundtrip(r#"{"SS":["a","b","c"]}"#);
    assert_eq!(
        av,
        AttributeValue::Ss(vec!["a".into(), "b".into(), "c".into()])
    );
}

#[test]
fn av_number_set() {
    let av = roundtrip(r#"{"NS":["1","2","3"]}"#);
    assert_eq!(
        av,
        AttributeValue::Ns(vec!["1".into(), "2".into(), "3".into()])
    );
}

#[test]
fn av_binary_set() {
    // base64("a") == "YQ==", base64("bb") == "YmI="
    let av = roundtrip(r#"{"BS":["YQ==","YmI="]}"#);
    assert_eq!(
        av,
        AttributeValue::Bs(vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb"),])
    );
}

#[test]
fn av_rejects_two_keys() {
    let err: Result<AttributeValue, _> = serde_json::from_str(r#"{"S":"x","N":"1"}"#);
    assert!(err.is_err(), "two-key AttributeValue must be rejected");
}

#[test]
fn av_rejects_empty_object() {
    let err: Result<AttributeValue, _> = serde_json::from_str(r#"{}"#);
    assert!(err.is_err(), "empty AttributeValue object must be rejected");
}

#[test]
fn av_rejects_unknown_variant() {
    let err: Result<AttributeValue, _> = serde_json::from_str(r#"{"X":"oops"}"#);
    assert!(err.is_err(), "unknown variant must be rejected");
}

#[test]
fn put_item_request_parses_and_ignores_extra_fields() {
    // The AWS CLI sends ReturnValues, ConditionExpression, etc. We accept and ignore.
    let body = r#"{
        "TableName": "users",
        "Item": {"id": {"S": "u1"}, "name": {"S": "alice"}},
        "ReturnValues": "NONE",
        "ConditionExpression": "attribute_not_exists(id)"
    }"#;
    let req: PutItemRequest = serde_json::from_str(body).unwrap();
    assert_eq!(req.table_name, "users");
    assert_eq!(req.item.len(), 2);
    assert_eq!(req.item.get("id"), Some(&AttributeValue::S("u1".into())));
    assert_eq!(
        req.item.get("name"),
        Some(&AttributeValue::S("alice".into()))
    );
}

#[test]
fn get_item_request_parses() {
    let body = r#"{"TableName":"users","Key":{"id":{"S":"u1"}},"ConsistentRead":true}"#;
    let req: GetItemRequest = serde_json::from_str(body).unwrap();
    assert_eq!(req.table_name, "users");
    assert_eq!(req.key.get("id"), Some(&AttributeValue::S("u1".into())));
}

#[test]
fn get_item_response_omits_item_when_none() {
    let resp = GetItemResponse { item: None };
    let json = serde_json::to_string(&resp).unwrap();
    assert_eq!(json, "{}", "missing Item must be omitted entirely");
}

#[test]
fn get_item_response_emits_item_when_some() {
    let mut item: Item = BTreeMap::new();
    item.insert("id".into(), AttributeValue::S("u1".into()));
    let resp = GetItemResponse { item: Some(item) };
    let v: Value = serde_json::to_value(&resp).unwrap();
    assert_eq!(v, json!({"Item": {"id": {"S": "u1"}}}));
}

#[test]
fn put_item_response_serializes_to_empty_object() {
    let resp = rekt_protocol::PutItemResponse::default();
    let json = serde_json::to_string(&resp).unwrap();
    assert_eq!(json, "{}");
}
