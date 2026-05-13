//! End-to-end tests of the axum router with an in-memory mock backend.
//! No Postgres required.

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use rekt_server::{router, AppState};
use rekt_sigv4::{PermissiveVerifier, Verifier};
use rekt_storage::{Backend, BackendError, KeyType, KeyValue, TableShape};
use rekt_translator::TableSchema;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tower::ServiceExt;

// ===== Mock backend ============================================================

#[derive(Default)]
struct MockBackend {
    /// Keyed by (table, pk_string, sk_string-or-empty). String keys make
    /// hashing simple regardless of KeyType.
    store: Mutex<HashMap<(String, String, String), serde_json::Value>>,
}

fn kv_to_string(kv: &KeyValue) -> String {
    match kv {
        KeyValue::S(s) | KeyValue::N(s) => s.clone(),
        KeyValue::B(b) => format!("{:?}", b.as_ref()),
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError> {
        // The mock doesn't actually evaluate the generated-column expressions.
        // Production code does this in PG; for tests we cheat: look at the
        // item's DDB-JSON shape to derive pk/sk values, since by convention
        // pk_attr/sk_attr in the shape match attribute names.
        let pk_value = item
            .get(shape.pk_col)
            .and_then(extract_key_string)
            .ok_or_else(|| {
                BackendError::Other(format!(
                    "mock backend: item missing pk attribute `{}`",
                    shape.pk_col
                ))
            })?;
        let sk_value = match shape.sk_col {
            Some(sk_col) => item
                .get(sk_col)
                .and_then(extract_key_string)
                .ok_or_else(|| {
                    BackendError::Other(format!(
                        "mock backend: item missing sk attribute `{sk_col}`"
                    ))
                })?,
            None => String::new(),
        };
        self.store
            .lock()
            .unwrap()
            .insert((shape.table.to_string(), pk_value, sk_value), item.clone());
        Ok(())
    }

    async fn get_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        let pk_value = kv_to_string(pk);
        let sk_value = sk.map(kv_to_string).unwrap_or_default();
        Ok(self
            .store
            .lock()
            .unwrap()
            .get(&(shape.table.to_string(), pk_value, sk_value))
            .cloned())
    }

    async fn delete_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<(), BackendError> {
        let pk_value = kv_to_string(pk);
        let sk_value = sk.map(kv_to_string).unwrap_or_default();
        self.store
            .lock()
            .unwrap()
            .remove(&(shape.table.to_string(), pk_value, sk_value));
        // DDB DeleteItem is idempotent — Ok regardless of whether the row existed.
        Ok(())
    }
}

/// Pull the string value out of a DDB-JSON AttributeValue like `{"S":"u1"}`.
fn extract_key_string(av: &Value) -> Option<String> {
    av.as_object()
        .and_then(|m| m.iter().next())
        .and_then(|(_tag, v)| v.as_str())
        .map(|s| s.to_string())
}

// ===== Helpers ================================================================

fn schemas() -> HashMap<String, TableSchema> {
    let users = TableSchema {
        name: "users".into(),
        pg_table: "users".into(),
        pk_attr: "id".into(),
        pk_type: KeyType::S,
        sk_attr: None,
        sk_type: None,
        jsonb_col: "data".into(),
    };
    let events = TableSchema {
        name: "device_events".into(),
        pg_table: "device_events".into(),
        pk_attr: "device_id".into(),
        pk_type: KeyType::S,
        sk_attr: Some("ts".into()),
        sk_type: Some(KeyType::N),
        jsonb_col: "doc".into(),
    };
    let mut m = HashMap::new();
    m.insert(users.name.clone(), users);
    m.insert(events.name.clone(), events);
    m
}

fn app() -> axum::Router {
    let state = AppState {
        verifier: Arc::new(PermissiveVerifier) as Arc<dyn Verifier>,
        backend: Arc::new(MockBackend::default()) as Arc<dyn Backend>,
        schemas: Arc::new(schemas()),
    };
    router(state)
}

fn ddb_request(target: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", format!("DynamoDB_20120810.{target}"))
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

// ===== Tests ==================================================================

#[tokio::test]
async fn put_then_get_round_trip() {
    let app = app();

    // PutItem
    let put_body = json!({
        "TableName": "users",
        "Item": {"id": {"S": "u1"}, "name": {"S": "alice"}}
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("PutItem", put_body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let put_resp = body_json(resp).await;
    assert_eq!(put_resp, json!({}));

    // GetItem returning the same item
    let get_body = json!({
        "TableName": "users",
        "Key": {"id": {"S": "u1"}}
    });
    let resp = app.oneshot(ddb_request("GetItem", get_body)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({"Item": {"id": {"S": "u1"}, "name": {"S": "alice"}}})
    );
}

#[tokio::test]
async fn get_missing_item_returns_empty_object() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"nope"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body, json!({}));
}

#[tokio::test]
async fn unknown_table_is_resource_not_found() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"unknown","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(
        ty.ends_with("#ResourceNotFoundException"),
        "expected ResourceNotFoundException, got {ty}"
    );
}

#[tokio::test]
async fn unsupported_op_is_unknown_operation() {
    let app = app();
    let resp = app
        .oneshot(ddb_request("Scan", json!({"TableName":"users"})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(
        ty.ends_with("#UnknownOperationException"),
        "expected UnknownOperationException, got {ty}"
    );
}

#[tokio::test]
async fn missing_x_amz_target_is_serialization_error() {
    let app = app();
    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/x-amz-json-1.0")
        .body(Body::from(json!({}).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#SerializationException"));
}

#[tokio::test]
async fn put_with_missing_pk_attr_is_validation_error() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"name":{"S":"alice"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ValidationException"), "got: {ty}");
}

#[tokio::test]
async fn put_with_wrong_pk_type_is_validation_error() {
    let app = app();
    // Schema says id is S but client sent N.
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"N":"42"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ValidationException"));
}

#[tokio::test]
async fn get_with_extra_key_attr_is_validation_error() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"},"extra":{"S":"x"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ValidationException"));
}

#[tokio::test]
async fn composite_table_put_then_get() {
    let app = app();
    let item = json!({
        "device_id": {"S": "d1"},
        "ts": {"N": "1000"},
        "value": {"N": "42"}
    });
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"device_events","Item": item.clone()}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({
                "TableName": "device_events",
                "Key": {"device_id": {"S": "d1"}, "ts": {"N": "1000"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    assert_eq!(got, json!({"Item": item}));
}

#[tokio::test]
async fn put_item_returns_empty_object_and_correct_content_type() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_eq!(ct, "application/x-amz-json-1.0");
    let body = body_json(resp).await;
    assert_eq!(body, json!({}));
}

#[tokio::test]
async fn delete_then_get_returns_empty() {
    let app = app();
    // Put first
    let item = json!({"id":{"S":"to_delete"},"name":{"S":"alice"}});
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item": item}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Confirm present
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"to_delete"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert!(
        got.get("Item").is_some(),
        "expected Item before delete: {got}"
    );

    // Delete
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "DeleteItem",
            json!({"TableName":"users","Key":{"id":{"S":"to_delete"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let del_body = body_json(resp).await;
    assert_eq!(del_body, json!({}));

    // Confirm gone
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"to_delete"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(got, json!({}), "expected empty Item after delete: {got}");
}

#[tokio::test]
async fn delete_nonexistent_is_ok() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "DeleteItem",
            json!({"TableName":"users","Key":{"id":{"S":"never_existed"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, json!({}));
}

#[tokio::test]
async fn delete_with_extra_key_attr_is_validation_error() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "DeleteItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"},"extra":{"S":"x"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ValidationException"), "got: {ty}");
}

#[tokio::test]
async fn bad_json_is_serialization_error() {
    let app = app();
    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", "DynamoDB_20120810.PutItem")
        .body(Body::from("{this is not valid json"))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#SerializationException"));
}
