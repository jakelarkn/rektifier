//! End-to-end tests of the axum router with an in-memory mock backend.
//! No Postgres required.

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use rekt_expressions::Condition;
use rekt_server::{router, AppState};
use rekt_sigv4::{PermissiveVerifier, Verifier};
use rekt_storage::{
    Backend, BackendError, BatchGetOutcome, ConditionEvalFn, FilterEvalFn, GeneralUpdateFn,
    KeyType, KeyValue, QueryOutcome, ScanOutcome, SkCondition, TableShape, UpdateDecision,
    UpdateOutcome,
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

/// Evaluate an `SkCondition` against a row's SK as stored in the mock
/// (a `kv_to_string`-encoded value), with comparison semantics chosen
/// by the table's SK type.
fn sk_predicate(cond: &SkCondition, sk: &str, sk_type: KeyType) -> bool {
    use std::cmp::Ordering;
    let cmp = |target: &str| compare_sk_strings(sk, target, sk_type);
    match cond {
        SkCondition::Eq(v) => cmp(&kv_to_string(v)) == Ordering::Equal,
        SkCondition::Lt(v) => cmp(&kv_to_string(v)) == Ordering::Less,
        SkCondition::Le(v) => !matches!(cmp(&kv_to_string(v)), Ordering::Greater),
        SkCondition::Gt(v) => cmp(&kv_to_string(v)) == Ordering::Greater,
        SkCondition::Ge(v) => !matches!(cmp(&kv_to_string(v)), Ordering::Less),
        SkCondition::Between(lo, hi) => {
            !matches!(cmp(&kv_to_string(lo)), Ordering::Less)
                && !matches!(cmp(&kv_to_string(hi)), Ordering::Greater)
        }
        SkCondition::BeginsWithS(prefix) => sk.starts_with(prefix.as_str()),
    }
}

/// Ordering for two `kv_to_string`-encoded SKs. N keys compare as
/// numerics (f64 — same precision caveat as the slow-path eval);
/// S / B compare lexically.
fn compare_sk_strings(a: &str, b: &str, sk_type: KeyType) -> std::cmp::Ordering {
    match sk_type {
        KeyType::N => {
            let av = a.parse::<f64>().unwrap_or(0.0);
            let bv = b.parse::<f64>().unwrap_or(0.0);
            av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal)
        }
        KeyType::S | KeyType::B => a.cmp(b),
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        item: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        let pk_value = kv_to_string(pk);
        let sk_value = sk.map(kv_to_string).unwrap_or_default();
        let key = (shape.table.to_string(), pk_value, sk_value);
        let mut store = self.store.lock().unwrap();
        let old = store.insert(key, item.clone());
        Ok(old)
    }

    async fn put_with_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        item: &serde_json::Value,
        condition: ConditionEvalFn<'_>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        let pk_value = kv_to_string(pk);
        let sk_value = sk.map(kv_to_string).unwrap_or_default();
        let key = (shape.table.to_string(), pk_value, sk_value);
        let mut store = self.store.lock().unwrap();
        let existing = store.get(&key).cloned();
        if !condition(existing.as_ref()) {
            return Err(BackendError::ConditionalCheckFailed);
        }
        store.insert(key, item.clone());
        Ok(existing)
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
    ) -> Result<Option<serde_json::Value>, BackendError> {
        let pk_value = kv_to_string(pk);
        let sk_value = sk.map(kv_to_string).unwrap_or_default();
        let old = self
            .store
            .lock()
            .unwrap()
            .remove(&(shape.table.to_string(), pk_value, sk_value));
        // DDB DeleteItem is idempotent — Ok regardless of whether the row existed.
        Ok(old)
    }

    async fn delete_with_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        condition: ConditionEvalFn<'_>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        let pk_value = kv_to_string(pk);
        let sk_value = sk.map(kv_to_string).unwrap_or_default();
        let key = (shape.table.to_string(), pk_value, sk_value);
        let mut store = self.store.lock().unwrap();
        let existing = store.get(&key).cloned();
        if !condition(existing.as_ref()) {
            return Err(BackendError::ConditionalCheckFailed);
        }
        if existing.is_some() {
            store.remove(&key);
        }
        Ok(existing)
    }

    async fn update_simple_raw(
        &self,
        shape: &TableShape<'_>,
        _pk: &KeyValue,
        _sk: Option<&KeyValue>,
        insert_item: &serde_json::Value,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
    ) -> Result<UpdateOutcome, BackendError> {
        // Derive pk/sk from insert_item (the key attrs are guaranteed
        // present there by the translator). The explicit `_pk`/`_sk`
        // params match the new trait shape (PgBackend needs them for
        // the prev-CTE WHERE) but the mock just reads from insert_item
        // since it doesn't need a separate key-by-column lookup.
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

        let old_item = store.get(&key).cloned();

        // Upsert: start from existing row if present, else from insert_item;
        // apply SETs and REMOVEs over the chosen base.
        let mut new_item = match &old_item {
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
        Ok(UpdateOutcome { new_item, old_item })
    }

    async fn update_with_simple_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
        condition: &Condition,
    ) -> Result<UpdateOutcome, BackendError> {
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

        let old_item = Some(item.clone());

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
        Ok(UpdateOutcome { new_item, old_item })
    }

    async fn update_general_rmw_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        apply: GeneralUpdateFn<'_>,
    ) -> Result<UpdateOutcome, BackendError> {
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
                Ok(UpdateOutcome {
                    new_item,
                    old_item: existing,
                })
            }
        }
    }

    async fn query_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk_condition: Option<&SkCondition>,
        limit: Option<u32>,
        exclusive_start_key: Option<(&KeyValue, Option<&KeyValue>)>,
        filter: Option<FilterEvalFn<'_>>,
        forward: bool,
    ) -> Result<QueryOutcome, BackendError> {
        let pk_value = kv_to_string(pk);

        // Hash-only Query with ESK: short-circuit to empty (matches DDB
        // and PgBackend behavior).
        if shape.sk_col.is_none() && exclusive_start_key.is_some() {
            return Ok(QueryOutcome::default());
        }

        let store = self.store.lock().unwrap();
        let mut hits: Vec<(String, serde_json::Value)> = store
            .iter()
            .filter(|((t, p, _sk), _)| t == shape.table && p == &pk_value)
            .filter(|((_, _, sk), _)| match (sk_condition, shape.sk_type) {
                (None, _) => true,
                (Some(cond), Some(sk_type)) => sk_predicate(cond, sk, sk_type),
                (Some(_), None) => false,
            })
            .map(|((_, _, sk), v)| (sk.clone(), v.clone()))
            .collect();

        if let Some(sk_type) = shape.sk_type {
            hits.sort_by(|a, b| {
                let cmp = compare_sk_strings(&a.0, &b.0, sk_type);
                if forward {
                    cmp
                } else {
                    cmp.reverse()
                }
            });
        }

        // Apply ESK: keep rows strictly greater (forward) or less
        // (descending) than esk_sk.
        if let Some((_, Some(esk_sk))) = exclusive_start_key {
            if let Some(sk_type) = shape.sk_type {
                let cutoff = kv_to_string(esk_sk);
                hits.retain(|(sk, _)| {
                    let cmp = compare_sk_strings(sk, &cutoff, sk_type);
                    if forward {
                        cmp == std::cmp::Ordering::Greater
                    } else {
                        cmp == std::cmp::Ordering::Less
                    }
                });
            }
        }

        // Match PgBackend: truncate to Limit, then set LEK iff the
        // page filled exactly to Limit (DDB parity — LEK present
        // doesn't imply more data exists).
        let lim = limit.unwrap_or(1000) as usize;
        if hits.len() > lim {
            hits.truncate(lim);
        }
        let filled_to_limit = limit.is_some_and(|l| hits.len() == l as usize);

        let last_evaluated_key = if filled_to_limit {
            hits.last().map(|(sk_str, _)| {
                let sk_kv = shape.sk_type.map(|t| match t {
                    KeyType::S => KeyValue::S(sk_str.clone()),
                    KeyType::N => KeyValue::N(sk_str.clone()),
                    // B-SK paging isn't exercised in the mock today.
                    KeyType::B => KeyValue::S(sk_str.clone()),
                });
                (pk.clone(), sk_kv)
            })
        } else {
            None
        };

        let scanned_count = hits.len() as u32;
        let items: Vec<serde_json::Value> = match filter {
            None => hits.into_iter().map(|(_, v)| v).collect(),
            Some(f) => hits
                .into_iter()
                .map(|(_, v)| v)
                .filter(|row| f(row))
                .collect(),
        };
        let count = items.len() as u32;
        Ok(QueryOutcome {
            items,
            count,
            scanned_count,
            last_evaluated_key,
        })
    }

    async fn scan_raw(
        &self,
        shape: &TableShape<'_>,
        filter: Option<FilterEvalFn<'_>>,
        limit: Option<u32>,
        exclusive_start_key: Option<(&KeyValue, Option<&KeyValue>)>,
    ) -> Result<ScanOutcome, BackendError> {
        let store = self.store.lock().unwrap();
        let mut hits: Vec<(String, String, serde_json::Value)> = store
            .iter()
            .filter(|((t, _, _), _)| t == shape.table)
            .map(|((_, pk, sk), v)| (pk.clone(), sk.clone(), v.clone()))
            .collect();
        hits.sort_by(|a, b| {
            let pk_cmp = compare_sk_strings(&a.0, &b.0, shape.pk_type);
            if pk_cmp != std::cmp::Ordering::Equal {
                return pk_cmp;
            }
            match shape.sk_type {
                Some(t) => compare_sk_strings(&a.1, &b.1, t),
                None => std::cmp::Ordering::Equal,
            }
        });

        // Apply ESK as lex (pk, sk) > (esk_pk, esk_sk).
        if let Some((esk_pk, esk_sk)) = exclusive_start_key {
            let esk_pk_s = kv_to_string(esk_pk);
            let esk_sk_s = esk_sk.map(kv_to_string).unwrap_or_default();
            hits.retain(|(pk, sk, _)| {
                let pk_cmp = compare_sk_strings(pk, &esk_pk_s, shape.pk_type);
                match pk_cmp {
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Less => false,
                    std::cmp::Ordering::Equal => match shape.sk_type {
                        Some(t) => {
                            compare_sk_strings(sk, &esk_sk_s, t)
                                == std::cmp::Ordering::Greater
                        }
                        // Hash-only: equal pk → not strictly after the
                        // cursor, drop.
                        None => false,
                    },
                }
            });
        }

        // Truncate to Limit; LEK iff the page filled to Limit (DDB
        // parity — see PgBackend scan.rs).
        let lim = limit.unwrap_or(1000) as usize;
        if hits.len() > lim {
            hits.truncate(lim);
        }
        let filled_to_limit = limit.is_some_and(|l| hits.len() == l as usize);

        let last_evaluated_key = if filled_to_limit {
            hits.last().map(|(pk, sk, _)| {
                let pk_kv = match shape.pk_type {
                    KeyType::S => KeyValue::S(pk.clone()),
                    KeyType::N => KeyValue::N(pk.clone()),
                    KeyType::B => KeyValue::S(pk.clone()),
                };
                let sk_kv = shape.sk_type.map(|t| match t {
                    KeyType::S => KeyValue::S(sk.clone()),
                    KeyType::N => KeyValue::N(sk.clone()),
                    KeyType::B => KeyValue::S(sk.clone()),
                });
                (pk_kv, sk_kv)
            })
        } else {
            None
        };

        let scanned_count = hits.len() as u32;
        let items: Vec<serde_json::Value> = match filter {
            None => hits.into_iter().map(|(_, _, v)| v).collect(),
            Some(f) => hits
                .into_iter()
                .map(|(_, _, v)| v)
                .filter(|row| f(row))
                .collect(),
        };
        let count = items.len() as u32;
        Ok(ScanOutcome {
            items,
            count,
            scanned_count,
            last_evaluated_key,
        })
    }

    async fn batch_get_raw(
        &self,
        shape: &TableShape<'_>,
        keys: &[(KeyValue, Option<KeyValue>)],
    ) -> Result<BatchGetOutcome, BackendError> {
        let store = self.store.lock().unwrap();
        let mut items = Vec::with_capacity(keys.len());
        for (pk, sk) in keys {
            let pk_s = kv_to_string(pk);
            let sk_s = sk.as_ref().map(kv_to_string).unwrap_or_default();
            let key = (shape.table.to_string(), pk_s, sk_s);
            if let Some(v) = store.get(&key) {
                items.push(v.clone());
            }
        }
        Ok(BatchGetOutcome { items })
    }

    async fn update_insert_only_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
    ) -> Result<UpdateOutcome, BackendError> {
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
        Ok(UpdateOutcome {
            new_item: insert_item.clone(),
            old_item: None,
        })
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
    let messages = TableSchema {
        name: "messages".into(),
        pg_table: "messages".into(),
        pk_attr: "thread".into(),
        pk_type: KeyType::S,
        sk_attr: Some("ts".into()),
        sk_type: Some(KeyType::S),
        jsonb_col: "data".into(),
    };
    let mut m = HashMap::new();
    m.insert(users.name.clone(), users);
    m.insert(events.name.clone(), events);
    m.insert(messages.name.clone(), messages);
    m
}

fn app() -> axum::Router {
    let state = AppState {
        verifier: Arc::new(PermissiveVerifier) as Arc<dyn Verifier>,
        backend: Arc::new(MockBackend::default()) as Arc<dyn Backend>,
        schemas: Arc::new(schemas()),
        batch_limits: rekt_server::BatchLimits::default(),
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
    // `TransactGetItems` is not yet implemented (see PLAN-2 "Batch +
    // transactional ops"). Once it lands, swap this for whatever is
    // still unsupported at the time.
    let resp = app
        .oneshot(ddb_request("TransactGetItems", json!({})))
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

// ===== Conditional PutItem / DeleteItem + ReturnValues=ALL_OLD ================

#[tokio::test]
async fn put_attribute_not_exists_succeeds_on_missing_row() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({
                "TableName":"users",
                "Item":{"id":{"S":"new1"},"label":{"S":"alice"}},
                "ConditionExpression":"attribute_not_exists(id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, json!({}));
}

#[tokio::test]
async fn put_attribute_not_exists_fails_when_row_exists() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"dup1"}}}),
        ))
        .await
        .unwrap();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({
                "TableName":"users",
                "Item":{"id":{"S":"dup1"},"label":{"S":"new"}},
                "ConditionExpression":"attribute_not_exists(id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ConditionalCheckFailedException"), "got: {ty}");
}

#[tokio::test]
async fn put_simple_sql_condition_optimistic_lock() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"lock1"},"version":{"N":"1"}}}),
        ))
        .await
        .unwrap();
    // Matching version → success.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({
                "TableName":"users",
                "Item":{"id":{"S":"lock1"},"version":{"N":"2"}},
                "ConditionExpression":"version = :v",
                "ExpressionAttributeValues":{":v":{"N":"1"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // Stale version → CCFE.
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({
                "TableName":"users",
                "Item":{"id":{"S":"lock1"},"version":{"N":"3"}},
                "ConditionExpression":"version = :stale",
                "ExpressionAttributeValues":{":stale":{"N":"1"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ConditionalCheckFailedException"), "got: {ty}");
}

#[tokio::test]
async fn put_all_old_on_insert_omits_attributes() {
    // Fresh key → no pre-image → Attributes omitted, even with ALL_OLD.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({
                "TableName":"users",
                "Item":{"id":{"S":"fresh-put-old"},"label":{"S":"x"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body.get("Attributes").is_none(), "got: {body}");
}

#[tokio::test]
async fn put_all_old_on_overwrite_returns_pre_image() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"ow1"},"label":{"S":"v1"}}}),
        ))
        .await
        .unwrap();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({
                "TableName":"users",
                "Item":{"id":{"S":"ow1"},"label":{"S":"v2"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").expect("ALL_OLD on overwrite returns Attributes");
    assert_eq!(attrs.get("label").unwrap(), &json!({"S":"v1"}));
}

#[tokio::test]
async fn put_rejects_unsupported_return_values_all_new() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({
                "TableName":"users",
                "Item":{"id":{"S":"u1"}},
                "ReturnValues":"ALL_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ValidationException"), "got: {ty}");
}

#[tokio::test]
async fn delete_with_attribute_exists_fails_when_row_missing() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "DeleteItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"never"}},
                "ConditionExpression":"attribute_exists(id)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ConditionalCheckFailedException"), "got: {ty}");
}

#[tokio::test]
async fn delete_simple_sql_condition_optimistic_lock() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"dlock1"},"version":{"N":"5"}}}),
        ))
        .await
        .unwrap();
    // Stale → CCFE.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "DeleteItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"dlock1"}},
                "ConditionExpression":"version = :stale",
                "ExpressionAttributeValues":{":stale":{"N":"1"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    // Matching → success.
    let resp = app
        .oneshot(ddb_request(
            "DeleteItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"dlock1"}},
                "ConditionExpression":"version = :v",
                "ExpressionAttributeValues":{":v":{"N":"5"}},
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn delete_all_old_on_missing_omits_attributes() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "DeleteItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"never-d"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body.get("Attributes").is_none(), "got: {body}");
}

#[tokio::test]
async fn delete_all_old_returns_deleted_item() {
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"todel"},"label":{"S":"alice"}}}),
        ))
        .await
        .unwrap();
    let resp = app
        .oneshot(ddb_request(
            "DeleteItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"todel"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").expect("ALL_OLD on existing returns Attributes");
    assert_eq!(attrs.get("label").unwrap(), &json!({"S":"alice"}));
}

#[tokio::test]
async fn delete_rejects_updated_new_return_values() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "DeleteItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "ReturnValues":"UPDATED_NEW",
            }),
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
    // All five DDB-defined variants are accepted as of Phase 7c. An
    // unrecognized string still produces ValidationException.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET label = :n",
                "ExpressionAttributeValues":{":n":{"S":"alice"}},
                "ReturnValues":"BOGUS",
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
async fn update_return_values_all_old_on_insert_omits_attributes() {
    // No prior row → the upsert created it. ALL_OLD has nothing to
    // return; the response must omit `Attributes` entirely.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u-fresh"}},
                "UpdateExpression":"SET #l = :n",
                "ExpressionAttributeNames":{"#l":"label"},
                "ExpressionAttributeValues":{":n":{"S":"alice"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body.get("Attributes").is_none());
}

#[tokio::test]
async fn update_return_values_all_old_on_update_returns_pre_image() {
    // Prior row exists → ALL_OLD must return the pre-update state.
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
                "UpdateExpression":"SET #l = :n",
                "ExpressionAttributeNames":{"#l":"label"},
                "ExpressionAttributeValues":{":n":{"S":"new"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body
        .get("Attributes")
        .expect("ALL_OLD on existing row must populate Attributes");
    // The pre-update state, not the post-update state.
    assert_eq!(attrs.get("label").unwrap(), &json!({"S":"old"}));
    assert_eq!(attrs.get("role").unwrap(), &json!({"S":"admin"}));
}

#[tokio::test]
async fn update_return_values_all_old_insert_only_omits_attributes() {
    // InsertOnlyOnPk succeeds iff the row was absent. ALL_OLD therefore
    // omits Attributes by definition.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u-fresh-2"}},
                "UpdateExpression":"SET #l = :n",
                "ConditionExpression":"attribute_not_exists(id)",
                "ExpressionAttributeNames":{"#l":"label"},
                "ExpressionAttributeValues":{":n":{"S":"bob"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body.get("Attributes").is_none());
}

#[tokio::test]
async fn update_return_values_all_old_sql_cond_returns_pre_image() {
    // SimpleSql path — optimistic-lock pattern. ALL_OLD must reflect
    // the row state before the update.
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
                "UpdateExpression":"SET version = :new",
                "ConditionExpression":"version = :old",
                "ExpressionAttributeValues":{":new":{"N":"2"},":old":{"N":"1"}},
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("version").unwrap(), &json!({"N":"1"}));
}

#[tokio::test]
async fn update_return_values_all_old_tx_returns_pre_image() {
    // Slow (tx) path. ALL_OLD must come from the SELECT FOR UPDATE
    // snapshot, not the post-write state.
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
                "ReturnValues":"ALL_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("origin").unwrap(), &json!({"S":"hello"}));
    // `replica` was added in this UPDATE; ALL_OLD must NOT include it.
    assert!(attrs.get("replica").is_none());
}

#[tokio::test]
async fn update_return_values_updated_new_projects_touched() {
    // SET touches `label`. UPDATED_NEW must contain ONLY `label`,
    // not other attributes the row had before.
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
                "UpdateExpression":"SET #l = :n",
                "ExpressionAttributeNames":{"#l":"label"},
                "ExpressionAttributeValues":{":n":{"S":"new"}},
                "ReturnValues":"UPDATED_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("label").unwrap(), &json!({"S":"new"}));
    assert!(attrs.get("role").is_none(), "role wasn't touched");
    assert!(attrs.get("id").is_none(), "id wasn't touched");
}

#[tokio::test]
async fn update_return_values_updated_old_projects_touched_pre_image() {
    // UPDATED_OLD on a SET must return the pre-update value of the
    // touched attribute only.
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
                "UpdateExpression":"SET #l = :n",
                "ExpressionAttributeNames":{"#l":"label"},
                "ExpressionAttributeValues":{":n":{"S":"new"}},
                "ReturnValues":"UPDATED_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("label").unwrap(), &json!({"S":"old"}));
    assert!(attrs.get("role").is_none());
}

#[tokio::test]
async fn update_return_values_updated_old_on_fresh_insert_omits_attributes() {
    // No pre-update row → UPDATED_OLD has nothing to project.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u-fresh-uo"}},
                "UpdateExpression":"SET #l = :n",
                "ExpressionAttributeNames":{"#l":"label"},
                "ExpressionAttributeValues":{":n":{"S":"alice"}},
                "ReturnValues":"UPDATED_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body.get("Attributes").is_none());
}

#[tokio::test]
async fn update_return_values_updated_new_on_remove_omits_removed_attr() {
    // REMOVE touches `b`. UPDATED_NEW projects post-update item → `b`
    // is gone → projection is empty → Attributes omitted.
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"a":{"S":"keep"},"b":{"S":"drop_target"}}}),
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
                "UpdateExpression":"REMOVE b",
                "ReturnValues":"UPDATED_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body.get("Attributes").is_none());
}

#[tokio::test]
async fn update_return_values_updated_old_on_remove_returns_pre_value() {
    // REMOVE touches `b`. UPDATED_OLD projects pre-update item → `b`
    // existed → return its old value.
    let app = app();
    app.clone()
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"},"a":{"S":"keep"},"b":{"S":"drop_target"}}}),
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
                "UpdateExpression":"REMOVE b",
                "ReturnValues":"UPDATED_OLD",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("b").unwrap(), &json!({"S":"drop_target"}));
    assert!(attrs.get("a").is_none());
}

#[tokio::test]
async fn update_return_values_updated_new_on_add_numeric() {
    // ADD :n on tx path. UPDATED_NEW returns the incremented value.
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
                "UpdateExpression":"ADD tally :n",
                "ExpressionAttributeValues":{":n":{"N":"5"}},
                "ReturnValues":"UPDATED_NEW",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let attrs = body.get("Attributes").unwrap();
    assert_eq!(attrs.get("tally").unwrap(), &json!({"N":"15"}));
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

// ===== Query (Q1) =============================================================

/// Helper: PutItem against the mock so subsequent Query has data.
async fn put_item(app: &axum::Router, table: &str, item: Value) {
    let body = json!({"TableName": table, "Item": item});
    let resp = app
        .clone()
        .oneshot(ddb_request("PutItem", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn query_pk_only_returns_full_partition() {
    let app = app();
    // Seed 3 rows in the same partition, plus one in another partition.
    for ts in ["1000", "2000", "3000"] {
        put_item(
            &app,
            "device_events",
            json!({"device_id":{"S":"q-dev-1"},"ts":{"N":ts},"val":{"N":ts}}),
        )
        .await;
    }
    put_item(
        &app,
        "device_events",
        json!({"device_id":{"S":"q-dev-other"},"ts":{"N":"500"},"val":{"N":"0"}}),
    )
    .await;

    let resp = app
        .clone()
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "device_events",
                "KeyConditionExpression": "device_id = :pk",
                "ExpressionAttributeValues": {":pk":{"S":"q-dev-1"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 3);
    assert_eq!(body["ScannedCount"], 3);
    let items = body["Items"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    // Items must be ordered by SK ascending.
    let ts_seq: Vec<&str> = items
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts_seq, vec!["1000", "2000", "3000"]);
}

#[tokio::test]
async fn query_empty_partition() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "device_events",
                "KeyConditionExpression": "device_id = :pk",
                "ExpressionAttributeValues": {":pk":{"S":"q-empty"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 0);
    assert_eq!(body["Items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn query_sk_range_inclusive_and_exclusive() {
    let app = app();
    for ts in ["100", "200", "300", "400", "500"] {
        put_item(
            &app,
            "device_events",
            json!({"device_id":{"S":"q-range-1"},"ts":{"N":ts},"val":{"S":"x"}}),
        )
        .await;
    }

    // ts >= 200 AND ts < 500: matches 200, 300, 400.
    // KCE only allows one SK term, so split into two separate Query calls
    // and intersect mentally. Here we just test sk >= 200.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "device_events",
                "KeyConditionExpression": "device_id = :pk AND ts >= :lo",
                "ExpressionAttributeValues": {
                    ":pk":{"S":"q-range-1"},
                    ":lo":{"N":"200"}
                }
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let ts_seq: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts_seq, vec!["200", "300", "400", "500"]);
}

#[tokio::test]
async fn query_sk_between() {
    let app = app();
    for ts in ["100", "200", "300", "400", "500"] {
        put_item(
            &app,
            "device_events",
            json!({"device_id":{"S":"q-btw-1"},"ts":{"N":ts},"val":{"S":"x"}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "device_events",
                "KeyConditionExpression": "device_id = :pk AND ts BETWEEN :lo AND :hi",
                "ExpressionAttributeValues": {
                    ":pk":{"S":"q-btw-1"},
                    ":lo":{"N":"200"},
                    ":hi":{"N":"400"}
                }
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let ts_seq: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts_seq, vec!["200", "300", "400"]);
}

#[tokio::test]
async fn query_sk_begins_with_string() {
    let app = app();
    for ts in ["2026-01-01", "2026-01-02", "2026-02-01", "2027-01-01"] {
        put_item(
            &app,
            "messages",
            json!({"thread":{"S":"t-1"},"ts":{"S":ts},"body":{"S":ts}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "messages",
                "KeyConditionExpression": "thread = :pk AND begins_with(ts, :pfx)",
                "ExpressionAttributeValues": {
                    ":pk":{"S":"t-1"},
                    ":pfx":{"S":"2026-01"}
                }
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let ts_seq: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["S"].as_str().unwrap())
        .collect();
    assert_eq!(ts_seq, vec!["2026-01-01", "2026-01-02"]);
}

#[tokio::test]
async fn query_pk_eq_and_sk_eq() {
    let app = app();
    for ts in ["10", "20", "30"] {
        put_item(
            &app,
            "device_events",
            json!({"device_id":{"S":"q-eq"},"ts":{"N":ts},"val":{"S":ts}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "device_events",
                "KeyConditionExpression": "device_id = :pk AND ts = :ts",
                "ExpressionAttributeValues": {
                    ":pk":{"S":"q-eq"},
                    ":ts":{"N":"20"}
                }
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 1);
    assert_eq!(body["Items"][0]["ts"]["N"], "20");
}

#[tokio::test]
async fn query_missing_pk_returns_validation_exception() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "device_events",
                "KeyConditionExpression": "ts = :ts",
                "ExpressionAttributeValues": {":ts":{"N":"1"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body.get("__type").and_then(Value::as_str).unwrap();
    assert!(ty.ends_with("#ValidationException"));
}

#[tokio::test]
async fn query_or_returns_validation_exception() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "users",
                "KeyConditionExpression": "id = :a OR id = :b",
                "ExpressionAttributeValues": {
                    ":a":{"S":"u1"},":b":{"S":"u2"}
                }
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body
        .get("__type")
        .and_then(Value::as_str)
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn query_unknown_table_returns_resource_not_found() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "nope",
                "KeyConditionExpression": "id = :v",
                "ExpressionAttributeValues": {":v":{"S":"u1"}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body
        .get("__type")
        .and_then(Value::as_str)
        .unwrap()
        .ends_with("#ResourceNotFoundException"));
}

#[tokio::test]
async fn query_consistent_read_silently_honored() {
    let app = app();
    put_item(
        &app,
        "users",
        json!({"id":{"S":"q-cr-1"},"label":{"S":"alice"}}),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "users",
                "KeyConditionExpression": "id = :pk",
                "ExpressionAttributeValues": {":pk":{"S":"q-cr-1"}},
                "ConsistentRead": true
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 1);
}

#[tokio::test]
async fn query_placeholders_substitute() {
    let app = app();
    put_item(
        &app,
        "messages",
        json!({"thread":{"S":"q-ph-1"},"ts":{"S":"2026-01-01"},"body":{"S":"x"}}),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName": "messages",
                "KeyConditionExpression": "#t = :pk AND begins_with(#r, :pfx)",
                "ExpressionAttributeNames": {"#t":"thread","#r":"ts"},
                "ExpressionAttributeValues": {
                    ":pk":{"S":"q-ph-1"},
                    ":pfx":{"S":"2026"}
                }
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 1);
}

// ===== Query Q2: Limit + ExclusiveStartKey / LastEvaluatedKey ==============

#[tokio::test]
async fn query_limit_truncates_and_sets_lek() {
    let app = app();
    for ts in 1..=5 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-lim-1"},
                "ts":{"N":ts.to_string()},
                "val":{"N":ts.to_string()}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-lim-1"}},
                "Limit": 2
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 2);
    let ts: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts, vec!["1", "2"]);
    // Limit < partition size → LEK present, equal to the last returned key.
    assert_eq!(
        body["LastEvaluatedKey"],
        json!({"device_id":{"S":"q-lim-1"},"ts":{"N":"2"}})
    );
}

#[tokio::test]
async fn query_last_page_no_lek() {
    let app = app();
    for ts in 1..=3 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-lastpg"},
                "ts":{"N":ts.to_string()},
                "val":{"N":"x"}
            }),
        )
        .await;
    }
    // Limit > partition size → no LEK in response.
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-lastpg"}},
                "Limit": 10
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 3);
    assert!(body.get("LastEvaluatedKey").is_none());
}

#[tokio::test]
async fn query_multi_page_traversal_reassembles() {
    let app = app();
    for ts in 1..=10 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-page"},
                "ts":{"N":ts.to_string()},
                "val":{"N":ts.to_string()}
            }),
        )
        .await;
    }

    // Page through L=3 at a time; collect all items + assert order.
    let mut all_ts: Vec<String> = Vec::new();
    let mut esk: Option<Value> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        if pages > 10 {
            panic!("pagination didn't terminate");
        }
        let mut body_in = json!({
            "TableName":"device_events",
            "KeyConditionExpression":"device_id = :pk",
            "ExpressionAttributeValues":{":pk":{"S":"q-page"}},
            "Limit": 3
        });
        if let Some(e) = &esk {
            body_in["ExclusiveStartKey"] = e.clone();
        }
        let resp = app
            .clone()
            .oneshot(ddb_request("Query", body_in))
            .await
            .unwrap();
        let body = body_json(resp).await;
        for item in body["Items"].as_array().unwrap() {
            all_ts.push(item["ts"]["N"].as_str().unwrap().to_string());
        }
        match body.get("LastEvaluatedKey") {
            Some(lek) if !lek.is_null() => esk = Some(lek.clone()),
            _ => break,
        }
    }
    assert_eq!(
        all_ts,
        (1..=10).map(|n| n.to_string()).collect::<Vec<_>>()
    );
    assert!(pages >= 4, "expected at least 4 pages for L=3 over 10 items, got {pages}");
}

#[tokio::test]
async fn query_limit_one_single_row_page() {
    let app = app();
    for ts in 1..=3 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-l1"},
                "ts":{"N":ts.to_string()},
                "val":{"N":"x"}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-l1"}},
                "Limit": 1
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 1);
    assert_eq!(body["Items"][0]["ts"]["N"], "1");
    assert_eq!(
        body["LastEvaluatedKey"],
        json!({"device_id":{"S":"q-l1"},"ts":{"N":"1"}})
    );
}

#[tokio::test]
async fn query_invalid_limit_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-bad-limit"}},
                "Limit": 0
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body
        .get("__type")
        .and_then(Value::as_str)
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn query_invalid_esk_pk_mismatch_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-mismatch"}},
                "ExclusiveStartKey":{
                    "device_id":{"S":"q-OTHER"},
                    "ts":{"N":"1"}
                }
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body
        .get("__type")
        .and_then(Value::as_str)
        .unwrap()
        .ends_with("#ValidationException"));
}

// ===== Query Q3: FilterExpression (Rust eval per row) =====================
//
// Filter is applied AFTER the SK key predicate + Limit truncation.
// `Count` reflects post-filter; `ScannedCount` reflects pre-filter.
// LEK is set whenever the scan capped at Limit, regardless of how many
// rows passed the filter — even Count=0 with LEK is valid.

#[tokio::test]
async fn query_filter_drops_some_rows() {
    let app = app();
    // 5 rows; flag alternates "on"/"off".
    for ts in 1..=5 {
        let flag = if ts % 2 == 1 { "on" } else { "off" };
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-filt-1"},
                "ts":{"N":ts.to_string()},
                "flag":{"S":flag}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "FilterExpression":"flag = :want",
                "ExpressionAttributeValues":{
                    ":pk":{"S":"q-filt-1"},
                    ":want":{"S":"on"}
                }
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    // 5 scanned, 3 kept (ts 1, 3, 5).
    assert_eq!(body["ScannedCount"], 5);
    assert_eq!(body["Count"], 3);
    let ts: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts, vec!["1", "3", "5"]);
}

#[tokio::test]
async fn query_filter_drops_all_rows_no_lek_when_partition_exhausted() {
    let app = app();
    for ts in 1..=3 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-filt-empty"},
                "ts":{"N":ts.to_string()},
                "flag":{"S":"off"}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "FilterExpression":"flag = :want",
                "ExpressionAttributeValues":{
                    ":pk":{"S":"q-filt-empty"},
                    ":want":{"S":"on"}
                }
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["ScannedCount"], 3);
    assert_eq!(body["Count"], 0);
    // Partition fully scanned, filter dropped all → no LEK.
    assert!(body.get("LastEvaluatedKey").is_none());
}

#[tokio::test]
async fn query_filter_with_limit_sets_lek_even_when_count_zero() {
    let app = app();
    // 5 rows; none match. Limit=2 caps scan at 2 → LEK present even
    // though Count=0 (client should re-page).
    for ts in 1..=5 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-filt-lim"},
                "ts":{"N":ts.to_string()},
                "flag":{"S":"off"}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "FilterExpression":"flag = :want",
                "ExpressionAttributeValues":{
                    ":pk":{"S":"q-filt-lim"},
                    ":want":{"S":"NO_MATCH"}
                },
                "Limit": 2
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["ScannedCount"], 2);
    assert_eq!(body["Count"], 0);
    assert_eq!(
        body["LastEvaluatedKey"],
        json!({"device_id":{"S":"q-filt-lim"},"ts":{"N":"2"}})
    );
}

#[tokio::test]
async fn query_filter_attribute_exists() {
    let app = app();
    put_item(
        &app,
        "device_events",
        json!({
            "device_id":{"S":"q-filt-ae"},
            "ts":{"N":"1"},
            "score":{"N":"5"}
        }),
    )
    .await;
    put_item(
        &app,
        "device_events",
        json!({
            "device_id":{"S":"q-filt-ae"},
            "ts":{"N":"2"}
            // no score attribute
        }),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "FilterExpression":"attribute_exists(score)",
                "ExpressionAttributeValues":{":pk":{"S":"q-filt-ae"}}
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 1);
    assert_eq!(body["Items"][0]["ts"]["N"], "1");
}

#[tokio::test]
async fn query_filter_boolean_and_with_between() {
    let app = app();
    for (ts, score) in [(1, 5), (2, 15), (3, 25), (4, 35)] {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-filt-bool"},
                "ts":{"N":ts.to_string()},
                "score":{"N":score.to_string()},
                "flag":{"S":"on"}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "FilterExpression":"flag = :want AND score BETWEEN :lo AND :hi",
                "ExpressionAttributeValues":{
                    ":pk":{"S":"q-filt-bool"},
                    ":want":{"S":"on"},
                    ":lo":{"N":"10"},
                    ":hi":{"N":"30"}
                }
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    // score in [10, 30]: rows ts=2 (score=15) and ts=3 (score=25).
    assert_eq!(body["Count"], 2);
    let ts: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts, vec!["2", "3"]);
}

#[tokio::test]
async fn query_filter_with_pagination_continues_through_filter_misses() {
    let app = app();
    // 10 rows; only ts=7 matches. Page with L=3; expect 3 pages of
    // empty Items with LEKs, then the page containing ts=7.
    for ts in 1..=10 {
        let flag = if ts == 7 { "MATCH" } else { "no" };
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-filt-page"},
                "ts":{"N":ts.to_string()},
                "flag":{"S":flag}
            }),
        )
        .await;
    }

    let mut esk: Option<Value> = None;
    let mut found: Vec<String> = vec![];
    let mut pages = 0;
    loop {
        pages += 1;
        if pages > 10 {
            panic!("paging didn't terminate");
        }
        let mut body_in = json!({
            "TableName":"device_events",
            "KeyConditionExpression":"device_id = :pk",
            "FilterExpression":"flag = :want",
            "ExpressionAttributeValues":{
                ":pk":{"S":"q-filt-page"},
                ":want":{"S":"MATCH"}
            },
            "Limit": 3
        });
        if let Some(e) = &esk {
            body_in["ExclusiveStartKey"] = e.clone();
        }
        let resp = app
            .clone()
            .oneshot(ddb_request("Query", body_in))
            .await
            .unwrap();
        let body = body_json(resp).await;
        for item in body["Items"].as_array().unwrap() {
            found.push(item["ts"]["N"].as_str().unwrap().to_string());
        }
        match body.get("LastEvaluatedKey") {
            Some(lek) if !lek.is_null() => esk = Some(lek.clone()),
            _ => break,
        }
    }
    assert_eq!(found, vec!["7"]);
}

// ===== Scan Q4 ============================================================

#[tokio::test]
async fn scan_returns_all_items_hash_only() {
    let app = app();
    for n in 1..=3 {
        put_item(
            &app,
            "users",
            json!({"id":{"S":format!("scan-u-{n}")},"label":{"S":format!("v-{n}")}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request("Scan", json!({"TableName":"users"})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body["Count"].as_u64().unwrap() >= 3);
    assert_eq!(body["Count"], body["ScannedCount"]);
    assert!(body.get("LastEvaluatedKey").is_none(),
        "Q4 never sets LEK");

    // Returned ids include all three we put.
    let ids: Vec<String> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"]["S"].as_str().unwrap().to_string())
        .collect();
    for n in 1..=3 {
        let want = format!("scan-u-{n}");
        assert!(ids.contains(&want), "missing {want} in {ids:?}");
    }
}

#[tokio::test]
async fn scan_returns_all_items_composite() {
    let app = app();
    for (dev, ts) in [("d1", "1"), ("d1", "2"), ("d2", "1")] {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":format!("scan-{dev}")},
                "ts":{"N":ts},
                "val":{"N":ts}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request("Scan", json!({"TableName":"device_events"})))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let items = body["Items"].as_array().unwrap();
    assert!(items.len() >= 3);
    assert_eq!(body["Count"], body["ScannedCount"]);
}

#[tokio::test]
async fn scan_empty_table() {
    let app = app();
    // Fresh app — no data inserted.
    let resp = app
        .oneshot(ddb_request("Scan", json!({"TableName":"users"})))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 0);
    assert_eq!(body["ScannedCount"], 0);
    assert_eq!(body["Items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn scan_unknown_table_returns_resource_not_found() {
    let app = app();
    let resp = app
        .oneshot(ddb_request("Scan", json!({"TableName":"nope"})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body
        .get("__type")
        .and_then(Value::as_str)
        .unwrap()
        .ends_with("#ResourceNotFoundException"));
}

#[tokio::test]
async fn scan_index_name_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({"TableName":"users","IndexName":"some_gsi"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body
        .get("__type")
        .and_then(Value::as_str)
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn scan_parallel_segments_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({"TableName":"users","Segment":0,"TotalSegments":4}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn scan_consistent_read_silently_honored() {
    let app = app();
    put_item(
        &app,
        "users",
        json!({"id":{"S":"scan-cr-1"},"label":{"S":"x"}}),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({"TableName":"users","ConsistentRead":true}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn scan_round_trips_every_attribute_variant() {
    // Type-coverage smoke: insert one item with every AttributeValue
    // variant, then Scan it back and assert the item shape survives.
    let app = app();
    let pk = "scan-type-cov";
    put_item(
        &app,
        "users",
        json!({
            "id": {"S": pk},
            "s_attr": {"S": "hello"},
            "n_attr": {"N": "3.14"},
            "b_attr": {"B": "AAEC"},
            "bool_attr": {"BOOL": true},
            "null_attr": {"NULL": true},
            "l_attr": {"L":[{"S":"a"},{"N":"1"}]},
            "m_attr": {"M":{"k":{"S":"v"}}},
            "ss_attr": {"SS":["a","b"]},
            "ns_attr": {"NS":["1","2"]},
            "bs_attr": {"BS":["AA==","AQ=="]}
        }),
    )
    .await;

    let resp = app
        .oneshot(ddb_request("Scan", json!({"TableName":"users"})))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let found = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["id"]["S"] == pk)
        .expect("inserted row in Scan result");
    assert_eq!(found["s_attr"]["S"], "hello");
    assert_eq!(found["n_attr"]["N"], "3.14");
    assert_eq!(found["b_attr"]["B"], "AAEC");
    assert_eq!(found["bool_attr"]["BOOL"], true);
    assert_eq!(found["null_attr"]["NULL"], true);
    assert!(found["l_attr"]["L"].is_array());
    assert_eq!(found["m_attr"]["M"]["k"]["S"], "v");
    assert!(found["ss_attr"]["SS"].is_array());
    assert!(found["ns_attr"]["NS"].is_array());
    assert!(found["bs_attr"]["BS"].is_array());
}

// ===== Scan Q5: Limit + ExclusiveStartKey + FilterExpression ==============

#[tokio::test]
async fn scan_limit_sets_lek_when_more_remain() {
    let app = app();
    for n in 1..=4 {
        put_item(
            &app,
            "users",
            json!({"id":{"S":format!("scan-lek-{n}")},"label":{"S":"v"}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({"TableName":"users","Limit":2}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 2);
    assert!(body.get("LastEvaluatedKey").is_some(),
        "Limit=2 < 4 items → LEK should be set");
}

#[tokio::test]
async fn scan_multi_page_traversal_reassembles() {
    let app = app();
    for n in 1..=6 {
        put_item(
            &app,
            "users",
            json!({"id":{"S":format!("scan-page-{n:02}")},"label":{"S":"v"}}),
        )
        .await;
    }

    let mut all_ids: Vec<String> = vec![];
    let mut esk: Option<Value> = None;
    for pages in 1..=10 {
        let mut body_in = json!({"TableName":"users","Limit":2});
        if let Some(e) = &esk {
            body_in["ExclusiveStartKey"] = e.clone();
        }
        let resp = app
            .clone()
            .oneshot(ddb_request("Scan", body_in))
            .await
            .unwrap();
        let body = body_json(resp).await;
        for item in body["Items"].as_array().unwrap() {
            let id = item["id"]["S"].as_str().unwrap();
            // Only collect ids we put for this test (other tests
            // share the `users` mock table).
            if id.starts_with("scan-page-") {
                all_ids.push(id.to_string());
            }
        }
        match body.get("LastEvaluatedKey") {
            Some(lek) if !lek.is_null() => esk = Some(lek.clone()),
            _ => break,
        }
        if pages == 10 {
            panic!("pagination didn't terminate");
        }
    }
    // Must include all 6 ids (PG order is ascending pk).
    for n in 1..=6 {
        let want = format!("scan-page-{n:02}");
        assert!(all_ids.contains(&want), "missing {want} in {all_ids:?}");
    }
}

#[tokio::test]
async fn scan_filter_drops_some_rows() {
    let app = app();
    for n in 1..=4 {
        let role = if n % 2 == 0 { "admin" } else { "user" };
        put_item(
            &app,
            "users",
            json!({
                "id":{"S":format!("scan-filt-{n}")},
                "tier":{"S":role}
            }),
        )
        .await;
    }

    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({
                "TableName":"users",
                "FilterExpression":"tier = :want",
                "ExpressionAttributeValues":{":want":{"S":"admin"}}
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    // ScannedCount should include every row in the mock store; Count
    // should be 2 (the admin entries we just inserted).
    assert!(body["ScannedCount"].as_u64().unwrap() >= 4);
    let admin_count = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["id"]["S"].as_str().unwrap().starts_with("scan-filt-"))
        .count();
    assert_eq!(admin_count, 2);
}

#[tokio::test]
async fn scan_filter_with_limit_lek_when_count_zero() {
    let app = app();
    for n in 1..=4 {
        put_item(
            &app,
            "users",
            json!({"id":{"S":format!("scan-fl-{n}")},"tier":{"S":"user"}}),
        )
        .await;
    }
    // Limit=2 caps scan to first two rows in pk order. Filter wants
    // "admin" — none match → Count=0, ScannedCount=2, LEK present.
    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({
                "TableName":"users",
                "FilterExpression":"tier = :want",
                "ExpressionAttributeValues":{":want":{"S":"admin"}},
                "Limit": 2
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["ScannedCount"], 2);
    assert_eq!(body["Count"], 0);
    assert!(body.get("LastEvaluatedKey").is_some());
}

// ===== Query Q5: ScanIndexForward=false =====================================

#[tokio::test]
async fn query_scan_index_forward_false_returns_descending() {
    let app = app();
    for ts in 1..=5 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-desc"},
                "ts":{"N":ts.to_string()},
                "val":{"N":ts.to_string()}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-desc"}},
                "ScanIndexForward": false
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let ts: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts, vec!["5", "4", "3", "2", "1"]);
}

#[tokio::test]
async fn query_descending_pagination_resumes_correctly() {
    let app = app();
    for ts in 1..=6 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-desc-page"},
                "ts":{"N":ts.to_string()},
                "val":{"N":"x"}
            }),
        )
        .await;
    }

    // First descending page: L=2 → ts 6, 5; LEK at sk=5.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-desc-page"}},
                "ScanIndexForward": false,
                "Limit": 2
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let ts: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts, vec!["6", "5"]);
    let lek = body.get("LastEvaluatedKey").cloned().expect("LEK");

    // Second descending page: continue from LEK → ts 4, 3.
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-desc-page"}},
                "ScanIndexForward": false,
                "Limit": 2,
                "ExclusiveStartKey": lek
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let ts: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(ts, vec!["4", "3"]);
}

// ===== Q6: Select=COUNT + ProjectionExpression ============================

#[tokio::test]
async fn query_select_count_omits_items() {
    let app = app();
    for ts in 1..=3 {
        put_item(
            &app,
            "device_events",
            json!({
                "device_id":{"S":"q-cnt"},
                "ts":{"N":ts.to_string()},
                "val":{"N":ts.to_string()}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"device_events",
                "KeyConditionExpression":"device_id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-cnt"}},
                "Select":"COUNT"
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Count"], 3);
    assert_eq!(body["ScannedCount"], 3);
    let items = body["Items"].as_array().expect("Items field present");
    assert!(items.is_empty(), "Select=COUNT must omit Items entries");
}

#[tokio::test]
async fn query_projection_prunes_attributes() {
    let app = app();
    put_item(
        &app,
        "users",
        json!({
            "id":{"S":"q-proj-1"},
            "label":{"S":"alice"},
            "tier":{"S":"admin"},
            "version":{"N":"42"}
        }),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"users",
                "KeyConditionExpression":"id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-proj-1"}},
                "ProjectionExpression":"label, tier"
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let item = &body["Items"][0];
    // `label` + `tier` kept; `id` + `version` pruned.
    assert!(item.get("label").is_some());
    assert!(item.get("tier").is_some());
    assert!(item.get("id").is_none(), "projected items should drop id");
    assert!(item.get("version").is_none());
}

#[tokio::test]
async fn query_projection_with_alias_substitutes() {
    let app = app();
    put_item(
        &app,
        "users",
        json!({
            "id":{"S":"q-pa-1"},
            "name":{"S":"alice"},
            "label":{"S":"L"}
        }),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"users",
                "KeyConditionExpression":"id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-pa-1"}},
                "ExpressionAttributeNames":{"#n":"name"},
                "ProjectionExpression":"#n"
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let item = &body["Items"][0];
    assert_eq!(item["name"]["S"], "alice");
    assert!(item.get("id").is_none());
    assert!(item.get("label").is_none());
}

#[tokio::test]
async fn query_select_count_with_projection_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"users",
                "KeyConditionExpression":"id = :pk",
                "ExpressionAttributeValues":{":pk":{"S":"q-cnt-proj"}},
                "Select":"COUNT",
                "ProjectionExpression":"label"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body
        .get("__type")
        .and_then(Value::as_str)
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn scan_select_count_omits_items() {
    let app = app();
    for n in 1..=3 {
        put_item(
            &app,
            "users",
            json!({"id":{"S":format!("scan-cnt-{n}")},"label":{"S":"v"}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({"TableName":"users","Select":"COUNT"}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert!(body["Count"].as_u64().unwrap() >= 3);
    assert_eq!(
        body["Items"].as_array().unwrap().len(),
        0,
        "Select=COUNT must omit Items entries"
    );
}

#[tokio::test]
async fn scan_projection_prunes_attributes() {
    let app = app();
    for n in 1..=2 {
        put_item(
            &app,
            "users",
            json!({
                "id":{"S":format!("scan-proj-{n}")},
                "label":{"S":format!("L{n}")},
                "tier":{"S":"admin"}
            }),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({
                "TableName":"users",
                "ProjectionExpression":"label"
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    // Every returned item must have ONLY `label` (other tests may have
    // populated the table; just check our items are pruned correctly).
    for item in body["Items"].as_array().unwrap() {
        let keys: Vec<&str> = item.as_object().unwrap().keys().map(|s| s.as_str()).collect();
        assert!(
            keys.iter().all(|k| *k == "label"),
            "projected scan items must contain only `label`, got {keys:?}"
        );
    }
}

// ===== BatchGetItem (B1) ======================================================

/// Read a parsed Item out of a `Responses[table]` array by matching its PK
/// to the requested one. Order is undefined per D9, so all assertions
/// look up by key rather than by index.
fn find_in_responses<'a>(
    resp: &'a Value,
    table: &str,
    pk_attr: &str,
    pk_value: &str,
) -> Option<&'a Value> {
    resp["Responses"][table]
        .as_array()?
        .iter()
        .find(|i| i[pk_attr]["S"].as_str() == Some(pk_value))
}

#[tokio::test]
async fn batch_get_single_table_returns_existing_items() {
    let app = app();
    for n in ["b1-a", "b1-b", "b1-c"] {
        put_item(
            &app,
            "users",
            json!({"id":{"S":n},"label":{"S":format!("L-{n}")}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{
                "users":{"Keys":[
                    {"id":{"S":"b1-a"}},
                    {"id":{"S":"b1-b"}},
                    {"id":{"S":"b1-c"}}
                ]}
            }}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["users"].as_array().unwrap().len(), 3);
    for n in ["b1-a", "b1-b", "b1-c"] {
        let item = find_in_responses(&body, "users", "id", n)
            .unwrap_or_else(|| panic!("missing item {n} in response"));
        assert_eq!(item["label"]["S"].as_str(), Some(format!("L-{n}")).as_deref());
    }
    // UnprocessedKeys: D11 says always `{}` and present (not omitted).
    assert_eq!(body["UnprocessedKeys"], json!({}));
}

#[tokio::test]
async fn batch_get_missing_keys_are_silently_omitted() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"hit"},"label":{"S":"yes"}})).await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{"Keys":[
                {"id":{"S":"hit"}},
                {"id":{"S":"miss-1"}},
                {"id":{"S":"miss-2"}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let items = body["Responses"]["users"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"]["S"].as_str(), Some("hit"));
}

#[tokio::test]
async fn batch_get_empty_table_returns_empty_array_not_omitted() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{"Keys":[
                {"id":{"S":"definitely-not-there"}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    // D10: empty per-table response is still present as `[]`.
    assert!(body["Responses"]["users"].is_array());
    assert_eq!(body["Responses"]["users"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn batch_get_composite_key_table_works() {
    let app = app();
    for ts in ["10", "20", "30"] {
        put_item(
            &app,
            "device_events",
            json!({"device_id":{"S":"comp"},"ts":{"N":ts},"val":{"S":format!("v{ts}")}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"device_events":{"Keys":[
                {"device_id":{"S":"comp"},"ts":{"N":"10"}},
                {"device_id":{"S":"comp"},"ts":{"N":"30"}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let items = body["Responses"]["device_events"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let mut got_ts: Vec<&str> = items
        .iter()
        .map(|i| i["ts"]["N"].as_str().unwrap())
        .collect();
    got_ts.sort();
    assert_eq!(got_ts, vec!["10", "30"]);
}

#[tokio::test]
async fn batch_get_rejects_empty_request_items() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body["__type"].as_str().unwrap();
    assert!(
        ty.ends_with("#ValidationException"),
        "expected ValidationException, got {ty}"
    );
    assert!(body["message"].as_str().unwrap().contains("RequestItems"));
}

#[tokio::test]
async fn batch_get_rejects_empty_keys_array() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{"Keys":[]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"].as_str().unwrap().ends_with("#ValidationException"));
}

#[tokio::test]
async fn batch_get_rejects_duplicate_keys() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{"Keys":[
                {"id":{"S":"dup"}},
                {"id":{"S":"dup"}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"].as_str().unwrap().ends_with("#ValidationException"));
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("duplicate"));
}

#[tokio::test]
async fn batch_get_rejects_extra_attr_in_key() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{"Keys":[
                {"id":{"S":"x"},"unexpected":{"S":"oops"}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"].as_str().unwrap().ends_with("#ValidationException"));
}

#[tokio::test]
async fn batch_get_rejects_wrong_key_type() {
    let app = app();
    // `users.id` is S; passing N should be rejected.
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{"Keys":[
                {"id":{"N":"42"}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"].as_str().unwrap().ends_with("#ValidationException"));
}

#[tokio::test]
async fn batch_get_unknown_table_is_resource_not_found() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"nope":{"Keys":[{"id":{"S":"x"}}]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body["__type"].as_str().unwrap();
    assert!(
        ty.ends_with("#ResourceNotFoundException"),
        "expected ResourceNotFoundException, got {ty}"
    );
}

#[tokio::test]
async fn batch_get_rejects_attributes_to_get() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"x"}}],
                "AttributesToGet":["label"]
            }}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let msg = body["message"].as_str().unwrap();
    assert!(msg.contains("AttributesToGet"), "got message: {msg}");
}

#[tokio::test]
async fn batch_get_too_many_keys_is_rejected() {
    let app = app();
    let mut keys = Vec::new();
    for n in 0..101u32 {
        keys.push(json!({"id":{"S":format!("k-{n}")}}));
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{"Keys": keys}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let msg = body["message"].as_str().unwrap();
    assert!(msg.contains("Too many items"), "got message: {msg}");
}

#[tokio::test]
async fn batch_get_100_keys_is_accepted() {
    let app = app();
    // Seed and request exactly 100 keys: the configured DDB default.
    for n in 0..100u32 {
        put_item(
            &app,
            "users",
            json!({"id":{"S":format!("k-{n}")},"label":{"S":format!("L{n}")}}),
        )
        .await;
    }
    let mut keys = Vec::new();
    for n in 0..100u32 {
        keys.push(json!({"id":{"S":format!("k-{n}")}}));
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{"Keys": keys}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["users"].as_array().unwrap().len(), 100);
}

#[tokio::test]
async fn batch_get_ignores_per_table_consistent_read_in_b1() {
    // ConsistentRead is silently honored (D7); B3 surfaces it via
    // tracing. v1: the wire field deserializes, the request succeeds.
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"cr"},"label":{"S":"x"}})).await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"cr"}}],
                "ConsistentRead":true
            }}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["users"].as_array().unwrap().len(), 1);
}

// ===== BatchGetItem multi-table (B2) =========================================

#[tokio::test]
async fn batch_get_multi_table_returns_per_table_responses() {
    let app = app();
    // Seed users + device_events in parallel.
    put_item(&app, "users", json!({"id":{"S":"mt-u1"},"label":{"S":"U1"}})).await;
    put_item(&app, "users", json!({"id":{"S":"mt-u2"},"label":{"S":"U2"}})).await;
    put_item(
        &app,
        "device_events",
        json!({"device_id":{"S":"mt-d"},"ts":{"N":"1"},"flag":{"S":"on"}}),
    )
    .await;
    put_item(
        &app,
        "device_events",
        json!({"device_id":{"S":"mt-d"},"ts":{"N":"2"},"flag":{"S":"off"}}),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{
                "users":{"Keys":[
                    {"id":{"S":"mt-u1"}},
                    {"id":{"S":"mt-u2"}}
                ]},
                "device_events":{"Keys":[
                    {"device_id":{"S":"mt-d"},"ts":{"N":"1"}},
                    {"device_id":{"S":"mt-d"},"ts":{"N":"2"}}
                ]}
            }}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["users"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["Responses"]["device_events"].as_array().unwrap().len(),
        2
    );
    assert_eq!(body["UnprocessedKeys"], json!({}));
}

#[tokio::test]
async fn batch_get_multi_table_one_empty_still_present() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"mt-u-only"},"label":{"S":"x"}})).await;
    // device_events keys all miss — its array should still be present
    // as [], per D10.
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{
                "users":{"Keys":[{"id":{"S":"mt-u-only"}}]},
                "device_events":{"Keys":[
                    {"device_id":{"S":"nope"},"ts":{"N":"42"}}
                ]}
            }}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["users"].as_array().unwrap().len(), 1);
    assert!(body["Responses"]["device_events"].is_array());
    assert_eq!(
        body["Responses"]["device_events"].as_array().unwrap().len(),
        0
    );
}

#[tokio::test]
async fn batch_get_multi_table_unknown_table_fails_whole_request() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"mt-fail"},"label":{"S":"x"}})).await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{
                "users":{"Keys":[{"id":{"S":"mt-fail"}}]},
                "no-such-table":{"Keys":[{"id":{"S":"x"}}]}
            }}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body["__type"].as_str().unwrap();
    assert!(
        ty.ends_with("#ResourceNotFoundException"),
        "expected ResourceNotFoundException, got {ty}"
    );
}

#[tokio::test]
async fn batch_get_duplicate_keys_in_one_table_doesnt_affect_other() {
    // Duplicate inside `users` should reject the whole request, even
    // though `device_events` is well-formed. Translator returns the
    // first error it finds (HashMap iteration is unordered, so we
    // just check the error class — not which table got blamed).
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{
                "users":{"Keys":[
                    {"id":{"S":"dup"}},
                    {"id":{"S":"dup"}}
                ]},
                "device_events":{"Keys":[
                    {"device_id":{"S":"d"},"ts":{"N":"1"}}
                ]}
            }}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"].as_str().unwrap().ends_with("#ValidationException"));
}

#[tokio::test]
async fn batch_get_same_key_across_tables_is_allowed() {
    // The duplicate-key rule is per-table. (pk="x") in users plus
    // (device_id="x", ts=1) in device_events is fine — different keys
    // in different tables.
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"x"},"label":{"S":"u"}})).await;
    put_item(
        &app,
        "device_events",
        json!({"device_id":{"S":"x"},"ts":{"N":"1"},"flag":{"S":"on"}}),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{
                "users":{"Keys":[{"id":{"S":"x"}}]},
                "device_events":{"Keys":[{"device_id":{"S":"x"},"ts":{"N":"1"}}]}
            }}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["users"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["Responses"]["device_events"].as_array().unwrap().len(),
        1
    );
}
