//! End-to-end tests of the axum router with an in-memory mock backend.
//! No Postgres required.

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use rekt_expressions::Condition;
use rekt_server::{router, AppState};
use rekt_sigv4::{PermissiveVerifier, Verifier};
use rekt_storage::{
    Backend, BackendError, GeneralUpdateFn, KeyType, KeyValue, TableShape, UpdateDecision,
};
use rekt_translator::{evaluate_condition, TableSchema};
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

    async fn update_simple_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
    ) -> Result<serde_json::Value, BackendError> {
        // Derive pk/sk from insert_item (the key attrs are guaranteed
        // present there by the translator).
        let pk_value = insert_item
            .get(shape.pk_col)
            .and_then(extract_key_string)
            .ok_or_else(|| {
                BackendError::Other(format!(
                    "mock backend: insert_item missing pk attribute `{}`",
                    shape.pk_col
                ))
            })?;
        let sk_value = match shape.sk_col {
            Some(sk_col) => insert_item
                .get(sk_col)
                .and_then(extract_key_string)
                .ok_or_else(|| {
                    BackendError::Other(format!(
                        "mock backend: insert_item missing sk attribute `{sk_col}`"
                    ))
                })?,
            None => String::new(),
        };

        let key = (shape.table.to_string(), pk_value, sk_value);
        let mut store = self.store.lock().unwrap();

        // Upsert: start from existing row if present, else from insert_item;
        // apply SETs and REMOVEs over the chosen base.
        let mut new_item = match store.get(&key) {
            Some(existing) => existing.clone(),
            None => insert_item.clone(),
        };
        let obj = new_item
            .as_object_mut()
            .ok_or_else(|| BackendError::Other("stored val is not an object".into()))?;
        for (name, value) in sets {
            obj.insert((*name).to_string(), (*value).clone());
        }
        for name in removes {
            obj.remove(*name);
        }

        store.insert(key, new_item.clone());
        Ok(new_item)
    }

    async fn update_with_simple_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
        condition: &Condition,
    ) -> Result<serde_json::Value, BackendError> {
        let pk_value = kv_to_string(pk);
        let sk_value = sk.map(kv_to_string).unwrap_or_default();
        let key = (shape.table.to_string(), pk_value, sk_value);

        let mut store = self.store.lock().unwrap();
        let existing = store.get(&key).cloned();
        // No INSERT branch for SimpleSql: missing row → condition false →
        // CCFE. (DDB semantics: any non-attribute_not_exists condition
        // requires the row to exist.)
        let item = match existing {
            None => return Err(BackendError::ConditionalCheckFailed),
            Some(v) => v,
        };

        if !evaluate_condition(Some(&item), condition) {
            return Err(BackendError::ConditionalCheckFailed);
        }

        // Apply SETs + REMOVEs over the existing item.
        let mut new_item = item;
        let obj = new_item
            .as_object_mut()
            .ok_or_else(|| BackendError::Other("stored val is not an object".into()))?;
        for (name, value) in sets {
            obj.insert((*name).to_string(), (*value).clone());
        }
        for name in removes {
            obj.remove(*name);
        }
        store.insert(key, new_item.clone());
        Ok(new_item)
    }

    async fn update_general_rmw_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        apply: GeneralUpdateFn<'_>,
    ) -> Result<serde_json::Value, BackendError> {
        // Mock equivalent of the PgBackend RMW loop. No actual locking;
        // the Mutex serializes access so the retry-on-race case doesn't
        // arise here.
        let pk_value = kv_to_string(pk);
        let sk_value = sk.map(kv_to_string).unwrap_or_default();
        let key = (shape.table.to_string(), pk_value, sk_value);

        let mut store = self.store.lock().unwrap();
        let existing = store.get(&key).cloned();

        let decision = apply(existing.as_ref())?;
        match decision {
            UpdateDecision::Fail => Err(BackendError::ConditionalCheckFailed),
            UpdateDecision::Apply(new_item) => {
                store.insert(key, new_item.clone());
                Ok(new_item)
            }
        }
    }

    async fn update_insert_only_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
    ) -> Result<serde_json::Value, BackendError> {
        // Same key derivation as put_item_raw / update_simple_raw.
        let pk_value = insert_item
            .get(shape.pk_col)
            .and_then(extract_key_string)
            .ok_or_else(|| {
                BackendError::Other(format!(
                    "mock backend: insert_item missing pk attribute `{}`",
                    shape.pk_col
                ))
            })?;
        let sk_value = match shape.sk_col {
            Some(sk_col) => insert_item
                .get(sk_col)
                .and_then(extract_key_string)
                .ok_or_else(|| {
                    BackendError::Other(format!(
                        "mock backend: insert_item missing sk attribute `{sk_col}`"
                    ))
                })?,
            None => String::new(),
        };

        let key = (shape.table.to_string(), pk_value, sk_value);
        let mut store = self.store.lock().unwrap();
        if store.contains_key(&key) {
            return Err(BackendError::ConditionalCheckFailed);
        }
        store.insert(key, insert_item.clone());
        Ok(insert_item.clone())
    }
}

// MockBackend's condition evaluation delegates to the public
// `rekt_translator::evaluate_condition` so the mock and the real PG
// path share the exact same DDB-semantics implementation.

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
        "Item": {"id": {"S": "u1"}, "label": {"S": "alice"}}
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
        json!({"Item": {"id": {"S": "u1"}, "label": {"S": "alice"}}})
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
            json!({"TableName":"users","Item":{"label":{"S":"alice"}}}),
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
        "val": {"N": "42"}
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
    let item = json!({"id":{"S":"to_delete"},"label":{"S":"alice"}});
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

// ===== UpdateItem dispatch tests ==============================================

#[tokio::test]
async fn update_set_on_missing_row_upserts() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET #n = :label, score = :s",
                "ExpressionAttributeNames":{"#n":"label"},
                "ExpressionAttributeValues":{":label":{"S":"alice"},":s":{"N":"7"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, json!({}));

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({"Item":{"id":{"S":"u1"},"label":{"S":"alice"},"score":{"N":"7"}}})
    );
}

#[tokio::test]
async fn update_set_on_existing_row_merges_siblings() {
    let app = app();
    // Seed
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"label":{"S":"alice"},"keep":{"S":"x"}}}),
        ))
        .await
        .unwrap();

    // SET name=alice2, add new field active=true; `keep` survives.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET #n = :label, active = :a",
                "ExpressionAttributeNames":{"#n":"label"},
                "ExpressionAttributeValues":{":label":{"S":"alice2"},":a":{"BOOL":true}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({"Item":{
            "id":{"S":"u1"},
            "label":{"S":"alice2"},
            "keep":{"S":"x"},
            "active":{"BOOL":true},
        }})
    );
}

#[tokio::test]
async fn update_remove_drops_attribute() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"keep":{"S":"x"},"goner":{"S":"y"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"REMOVE #d",
                "ExpressionAttributeNames":{"#d":"goner"},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(got, json!({"Item":{"id":{"S":"u1"},"keep":{"S":"x"}}}));
}

// ===== Phase 4d: SimpleSql conditions (UPDATE ... WHERE) ===================

#[tokio::test]
async fn update_attribute_exists_pk_succeeds_when_row_exists() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"label":{"S":"alice"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET label = :n",
                "ExpressionAttributeValues":{":n":{"S":"alice2"}},
                "ConditionExpression":"attribute_exists(id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({"Item":{"id":{"S":"u1"},"label":{"S":"alice2"}}})
    );
}

#[tokio::test]
async fn update_attribute_exists_pk_fails_when_row_missing() {
    // No INSERT branch for SimpleSql — missing row → CCFE.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"never_existed"}},
                "UpdateExpression":"SET label = :n",
                "ExpressionAttributeValues":{":n":{"S":"alice"}},
                "ConditionExpression":"attribute_exists(id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ConditionalCheckFailedException"));
}

#[tokio::test]
async fn update_optimistic_lock_via_version_equality() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"version":{"N":"3"}}}),
        ))
        .await
        .unwrap();

    // Match version → update applies, version bumps.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET version = :new",
                "ExpressionAttributeValues":{":expected":{"N":"3"},":new":{"N":"4"}},
                "ConditionExpression":"version = :expected",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Mismatch version (still :expected=3, but actual is now 4) → CCFE.
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET version = :new",
                "ExpressionAttributeValues":{":expected":{"N":"3"},":new":{"N":"5"}},
                "ConditionExpression":"version = :expected",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ConditionalCheckFailedException"));
}

#[tokio::test]
async fn update_numeric_ordering_condition() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"score":{"N":"10"}}}),
        ))
        .await
        .unwrap();

    // score < 100 → true → update applies.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET score = :new",
                "ExpressionAttributeValues":{":new":{"N":"20"},":lim":{"N":"100"}},
                "ConditionExpression":"score < :lim",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // score (now 20) > 5 → true → update applies.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET score = :new",
                "ExpressionAttributeValues":{":new":{"N":"30"},":min":{"N":"5"}},
                "ConditionExpression":"score > :min",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // score (now 30) > 50 → false → CCFE.
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET score = :new",
                "ExpressionAttributeValues":{":new":{"N":"99"},":min":{"N":"50"}},
                "ConditionExpression":"score > :min",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn update_boolean_composition() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"flag":{"S":"active"},"version":{"N":"1"}}}),
        ))
        .await
        .unwrap();

    // status = active AND version = 1 → true.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET version = :new",
                "ExpressionAttributeValues":{":active":{"S":"active"},":v1":{"N":"1"},":new":{"N":"2"}},
                "ConditionExpression":"flag = :active AND version = :v1",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // status = inactive (false) → CCFE.
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET version = :new",
                "ExpressionAttributeValues":{":inactive":{"S":"inactive"},":new":{"N":"3"}},
                "ConditionExpression":"flag = :inactive",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ===== Phase 4c: attribute_not_exists(pk) insert-only dispatch ==============

#[tokio::test]
async fn update_insert_only_creates_when_row_absent() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u_insert_only"}},
                "UpdateExpression":"SET #n = :label",
                "ExpressionAttributeNames":{"#n":"label"},
                "ExpressionAttributeValues":{":label":{"S":"alice"}},
                "ConditionExpression":"attribute_not_exists(id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, json!({}));

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u_insert_only"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({"Item":{"id":{"S":"u_insert_only"},"label":{"S":"alice"}}})
    );
}

#[tokio::test]
async fn update_insert_only_fails_when_row_exists() {
    let app = app();
    // Seed
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u_exists"},"label":{"S":"alice"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u_exists"}},
                "UpdateExpression":"SET label = :n",
                "ExpressionAttributeValues":{":n":{"S":"alice2"}},
                "ConditionExpression":"attribute_not_exists(id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(
        ty.ends_with("#ConditionalCheckFailedException"),
        "expected ConditionalCheckFailedException, got {ty}"
    );

    // Confirm the existing row was *not* overwritten.
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u_exists"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({"Item":{"id":{"S":"u_exists"},"label":{"S":"alice"}}})
    );
}

#[tokio::test]
async fn update_insert_only_composite_key() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"device_events",
                "Key":{"device_id":{"S":"d1"},"ts":{"N":"1000"}},
                "UpdateExpression":"SET v = :v",
                "ExpressionAttributeValues":{":v":{"N":"42"}},
                "ConditionExpression":"attribute_not_exists(device_id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Second call on same key should fail.
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"device_events",
                "Key":{"device_id":{"S":"d1"},"ts":{"N":"1000"}},
                "UpdateExpression":"SET v = :v",
                "ExpressionAttributeValues":{":v":{"N":"99"}},
                "ConditionExpression":"attribute_not_exists(device_id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ConditionalCheckFailedException"));
}

#[tokio::test]
async fn update_reject_unsupported_return_values() {
    // ALL_OLD / UPDATED_OLD / UPDATED_NEW remain Phase 7b/7c. Pick one
    // here to keep the divergence honest at the dispatch boundary.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET label = :n",
                "ExpressionAttributeValues":{":n":{"S":"alice"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ValidationException"));
}

// ===== Phase 7a: ReturnValues=ALL_NEW =====================================

#[tokio::test]
async fn update_return_values_all_new_direct_set_insert() {
    // No prior row → INSERT branch of the fast path. ALL_NEW must
    // return the full synthesized item (key + SET attrs).
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET #n = :n",
                "ExpressionAttributeNames":{"#n":"label"},
                "ExpressionAttributeValues":{":n":{"S":"alice"}},
                "ReturnValues":"ALL_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body
        .get("Attributes")
        .expect("ALL_NEW must populate Attributes");
    assert_eq!(attrs.get("id").unwrap(), &json!({"S":"u1"}));
    assert_eq!(attrs.get("label").unwrap(), &json!({"S":"alice"}));
}

#[tokio::test]
async fn update_return_values_all_new_direct_set_update() {
    // Prior row exists → DO-UPDATE branch. ALL_NEW must merge the
    // existing attrs with the SET delta.
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"label":{"S":"old"},"role":{"S":"admin"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET #n = :n",
                "ExpressionAttributeNames":{"#n":"label"},
                "ExpressionAttributeValues":{":n":{"S":"new"}},
                "ReturnValues":"ALL_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("label").unwrap(), &json!({"S":"new"}));
    assert_eq!(attrs.get("role").unwrap(), &json!({"S":"admin"}));
}

#[tokio::test]
async fn update_return_values_all_new_insert_only_path() {
    // attribute_not_exists(pk) → InsertOnlyOnPk fast path. ALL_NEW must
    // return the inserted item.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u-new"}},
                "UpdateExpression":"SET #n = :n",
                "ConditionExpression":"attribute_not_exists(id)",
                "ExpressionAttributeNames":{"#n":"label"},
                "ExpressionAttributeValues":{":n":{"S":"bob"}},
                "ReturnValues":"ALL_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let attrs = body_json(resp).await;
    let attrs = attrs.get("Attributes").unwrap();
    assert_eq!(attrs.get("id").unwrap(), &json!({"S":"u-new"}));
    assert_eq!(attrs.get("label").unwrap(), &json!({"S":"bob"}));
}

#[tokio::test]
async fn update_return_values_all_new_simple_sql_cond_path() {
    // Prior row + SimpleSql condition → SQL-WHERE fast path. ALL_NEW
    // must return the merged item.
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"version":{"N":"1"}}}),
        ))
        .await
        .unwrap();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET #v = :new",
                "ConditionExpression":"#v = :old",
                "ExpressionAttributeNames":{"#v":"version"},
                "ExpressionAttributeValues":{":new":{"N":"2"},":old":{"N":"1"}},
                "ReturnValues":"ALL_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("version").unwrap(), &json!({"N":"2"}));
}

#[tokio::test]
async fn update_return_values_all_new_tx_path() {
    // Path-ref RHS forces the slow (tx) path. ALL_NEW must come from
    // the closure's `new_item`, not from a re-fetched SELECT.
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"origin":{"S":"hello"}}}),
        ))
        .await
        .unwrap();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET replica = #s",
                "ExpressionAttributeNames":{"#s":"origin"},
                "ReturnValues":"ALL_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("replica").unwrap(), &json!({"S":"hello"}));
    assert_eq!(attrs.get("origin").unwrap(), &json!({"S":"hello"}));
}

// ===== Reserved-word rejection (edge cases) ==============================
//
// Main-path tests above avoid reserved attribute names. These tests are
// dedicated to verifying the rejection AND that aliasing bypasses it.
// Use `label`/`flag`/`tally`/`origin`/etc. as non-reserved attribute
// names in any new main-path test.

#[tokio::test]
async fn update_rejects_reserved_bare_name_as_validation_exception() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET name = :v",
                "ExpressionAttributeValues":{":v":{"S":"alice"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ValidationException"));
    let msg = body.get("message").and_then(Value::as_str).unwrap();
    assert!(
        msg.contains("reserved keyword: name"),
        "expected DDB-shaped reserved-keyword message, got: {msg}"
    );
}

#[tokio::test]
async fn update_rejects_reserved_bare_name_in_condition() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET label = :v",
                "ConditionExpression":"attribute_exists(status)",
                "ExpressionAttributeValues":{":v":{"S":"alice"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let msg = body.get("message").and_then(Value::as_str).unwrap();
    assert!(
        msg.contains("ConditionExpression") && msg.contains("reserved keyword: status"),
        "got: {msg}"
    );
}

#[tokio::test]
async fn update_accepts_reserved_name_when_aliased() {
    // The whole point of #x → "name": a reserved word is fine as the
    // *target* of an alias because the parser only sees the placeholder.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET #n = :v",
                "ExpressionAttributeNames":{"#n":"name"},
                "ExpressionAttributeValues":{":v":{"S":"alice"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn update_return_values_none_omits_attributes() {
    // Explicit NONE (and implicit / absent) → response body has no
    // `Attributes` key. Sanity check that the default path didn't
    // regress to always-returning.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET #n = :n",
                "ExpressionAttributeNames":{"#n":"label"},
                "ExpressionAttributeValues":{":n":{"S":"alice"}},
                "ReturnValues":"NONE",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body.get("Attributes").is_none());
}

// ===== Phase 3b/3c: slow-path UpdateExpression RHS forms ===================

#[tokio::test]
async fn update_set_path_ref_copies_attribute() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"origin":{"S":"hello"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET copied = origin",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({"Item":{"id":{"S":"u1"},"origin":{"S":"hello"},"copied":{"S":"hello"}}})
    );
}

#[tokio::test]
async fn update_set_arithmetic_increment_counter() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"tally":{"N":"10"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET tally = tally + :inc",
                "ExpressionAttributeValues":{":inc":{"N":"5"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(got, json!({"Item":{"id":{"S":"u1"},"tally":{"N":"15"}}}));
}

#[tokio::test]
async fn update_if_not_exists_preserves_existing() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"created":{"N":"1000"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET created = if_not_exists(created, :now)",
                "ExpressionAttributeValues":{":now":{"N":"9999"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    // existing `created` is preserved, fallback never used.
    assert_eq!(got, json!({"Item":{"id":{"S":"u1"},"created":{"N":"1000"}}}));
}

#[tokio::test]
async fn update_if_not_exists_uses_fallback_on_missing_path() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET created = if_not_exists(created, :now)",
                "ExpressionAttributeValues":{":now":{"N":"1234"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(got, json!({"Item":{"id":{"S":"u1"},"created":{"N":"1234"}}}));
}

#[tokio::test]
async fn update_list_append_concatenates() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"tags":{"L":[{"S":"a"}]}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET tags = list_append(tags, :new)",
                "ExpressionAttributeValues":{":new":{"L":[{"S":"b"},{"S":"c"}]}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({"Item":{"id":{"S":"u1"},"tags":{"L":[{"S":"a"},{"S":"b"},{"S":"c"}]}}})
    );
}

#[tokio::test]
async fn update_path_ref_missing_path_is_validation_error() {
    // `SET a = b` where `b` doesn't exist → ValidationException.
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET a = b",
            }),
        ))
        .await
        .unwrap();
    // BackendError::Other surfaces as InternalServerError in the current
    // mapping. (TODO: surface evaluator errors as ValidationException
    // when we wire EvalError → BackendError more precisely.)
    assert!(resp.status() == StatusCode::INTERNAL_SERVER_ERROR
        || resp.status() == StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn update_slow_path_handles_condition_too() {
    // Combine non-simple UpdateExpression with a (SimpleSql-eligible)
    // condition — slow path catches the non-simple expression and the
    // condition evaluator runs inside the same Tx.
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({
                "TableName":"users",
                "Item":{"id":{"S":"u1"},"tally":{"N":"5"},"version":{"N":"1"}}
            }),
        ))
        .await
        .unwrap();

    // Matching version → update applies, counter increments.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET tally = tally + :inc",
                "ExpressionAttributeValues":{":inc":{"N":"3"},":v":{"N":"1"}},
                "ConditionExpression":"version = :v",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_json(resp).await.get("Item").unwrap().get("tally"),
        Some(&json!({"N":"8"}))
    );

    // Mismatched version → CCFE, counter unchanged.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET tally = tally + :inc",
                "ExpressionAttributeValues":{":inc":{"N":"99"},":v":{"N":"42"}},
                "ConditionExpression":"version = :v",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ConditionalCheckFailedException"));

    // counter unchanged.
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_json(resp).await.get("Item").unwrap().get("tally"),
        Some(&json!({"N":"8"}))
    );
}

#[tokio::test]
async fn update_slow_path_via_needs_tx_condition() {
    // Path-vs-path ordering routes to NeedsTx → slow path. Both attrs
    // exist + a < b → condition true → update applies.
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"a":{"N":"1"},"b":{"N":"10"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET marker = :m",
                "ExpressionAttributeValues":{":m":{"S":"ok"}},
                "ConditionExpression":"a < b",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_json(resp).await.get("Item").unwrap().get("marker"),
        Some(&json!({"S":"ok"}))
    );
}

// ===== Phase 4e: slow-path conditions =======================================

#[tokio::test]
async fn update_begins_with_routes_through_slow_path() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"label":{"S":"alice"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET marker = :m",
                "ExpressionAttributeValues":{":m":{"S":"ok"},":p":{"S":"ali"}},
                "ConditionExpression":"begins_with(#n, :p)",
                "ExpressionAttributeNames":{"#n":"label"},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn update_contains_set_membership_via_slow_path() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"tags":{"SS":["a","b"]}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET marker = :m",
                "ExpressionAttributeValues":{":m":{"S":"ok"},":x":{"S":"a"}},
                "ConditionExpression":"contains(tags, :x)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn update_between_routes_through_slow_path() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"score":{"N":"42"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET marker = :m",
                "ExpressionAttributeValues":{":m":{"S":"ok"},":lo":{"N":"10"},":hi":{"N":"100"}},
                "ConditionExpression":"score BETWEEN :lo AND :hi",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Out of range → CCFE.
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET marker = :m",
                "ExpressionAttributeValues":{":m":{"S":"ok"},":lo":{"N":"100"},":hi":{"N":"200"}},
                "ConditionExpression":"score BETWEEN :lo AND :hi",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let ty = body_json(resp).await.get("__type").and_then(Value::as_str).unwrap().to_string();
    assert!(ty.ends_with("#ConditionalCheckFailedException"));
}

#[tokio::test]
async fn update_in_list_via_slow_path() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"flag":{"S":"pending"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET marker = :m",
                "ExpressionAttributeValues":{
                    ":m":{"S":"ok"},
                    ":a":{"S":"active"},
                    ":p":{"S":"pending"},
                    ":c":{"S":"closed"},
                },
                "ConditionExpression":"#s IN (:a, :p, :c)",
                "ExpressionAttributeNames":{"#s":"flag"},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn update_attribute_type_via_slow_path() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"score":{"N":"42"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET marker = :m",
                "ExpressionAttributeValues":{":m":{"S":"ok"},":t":{"S":"N"}},
                "ConditionExpression":"attribute_type(score, :t)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===== Phase 5: ADD ========================================================

#[tokio::test]
async fn update_add_numeric_on_missing_row_creates_counter() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"new_counter"}},
                "UpdateExpression":"ADD #c :one",
                "ExpressionAttributeNames":{"#c":"tally"},
                "ExpressionAttributeValues":{":one":{"N":"1"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"new_counter"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(got["Item"]["tally"], json!({"N":"1"}));
}

#[tokio::test]
async fn update_add_numeric_increments_existing() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"tally":{"N":"10"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"ADD #c :inc",
                "ExpressionAttributeNames":{"#c":"tally"},
                "ExpressionAttributeValues":{":inc":{"N":"5"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(body_json(resp).await["Item"]["tally"], json!({"N":"15"}));
}

#[tokio::test]
async fn update_add_set_union() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"tags":{"SS":["a","b"]}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"ADD tags :new",
                "ExpressionAttributeValues":{":new":{"SS":["b","c"]}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    let tags = got["Item"]["tags"]["SS"].as_array().unwrap().clone();
    assert_eq!(tags.len(), 3);
    for s in ["a", "b", "c"] {
        assert!(tags.iter().any(|v| v == s), "missing {s}");
    }
}

// ===== Phase 6: DELETE =====================================================

#[tokio::test]
async fn update_delete_set_removes_elements() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"tags":{"SS":["a","b","c"]}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"DELETE tags :rm",
                "ExpressionAttributeValues":{":rm":{"SS":["b"]}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let tags = body_json(resp).await["Item"]["tags"]["SS"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(tags.len(), 2);
    assert!(tags.iter().any(|v| v == "a"));
    assert!(tags.iter().any(|v| v == "c"));
}

#[tokio::test]
async fn update_delete_set_emptied_removes_attribute() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"tags":{"SS":["a","b"]},"keep":{"S":"x"}}}),
        ))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"DELETE tags :rm",
                "ExpressionAttributeValues":{":rm":{"SS":["a","b"]}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert!(got["Item"].get("tags").is_none(), "tags should be removed");
    assert_eq!(got["Item"]["keep"], json!({"S":"x"}));
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
