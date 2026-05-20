//! End-to-end tests of the axum router with an in-memory mock backend.
//! No Postgres required.

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use rekt_expressions::Condition;
use rekt_catalog::{CatalogSnapshot, TableCatalog, TableEntry, TableStatus};
use rekt_server::{router, AppState};
use rekt_sigv4::{PermissiveVerifier, Verifier};
use rekt_storage::{
    Backend, BackendError, BatchGetOutcome, BatchWriteOutcome, ConditionEvalFn, FilterEvalFn,
    GeneralUpdateFn, KeyType, KeyValue, QueryOutcome, ScanOutcome, SkCondition, TableShape,
    TransactCancelReason, TransactGetOp, TransactGetOutcome, TransactWriteOp,
    TransactWriteOutcome, UpdateDecision, UpdateOutcome, WriteOp,
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
    /// Test hook (Obs-2): when set, `get_item_raw` panics with this
    /// payload — exercises the `CatchPanicLayer` middleware.
    panic_on_get: Option<&'static str>,
    /// Test hook (Obs-2): when set, `get_item_raw` sleeps for this
    /// duration before responding — exercises the `TimeoutLayer`
    /// middleware. Combined with a short request_timeout in
    /// `app_with`, surfaces a 408.
    sleep_on_get: Option<std::time::Duration>,
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
        // Obs-2 test hooks. Panic-on-get exercises CatchPanicLayer;
        // sleep-on-get exercises TimeoutLayer.
        if let Some(msg) = self.panic_on_get {
            panic!("{msg}");
        }
        if let Some(d) = self.sleep_on_get {
            tokio::time::sleep(d).await;
        }
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

    async fn batch_write_raw(
        &self,
        shape: &TableShape<'_>,
        ops: &[WriteOp],
    ) -> Result<BatchWriteOutcome, BackendError> {
        // Mirror the PG path's "Puts first, then Deletes" ordering so
        // mock + libpq produce identical state under a same-key
        // Put+Delete in one request (rare; translator rejects exactly
        // that case anyway, but let's not diverge).
        let mut store = self.store.lock().unwrap();
        for op in ops {
            if let WriteOp::Put { pk, sk, item } = op {
                let key = (
                    shape.table.to_string(),
                    kv_to_string(pk),
                    sk.as_ref().map(kv_to_string).unwrap_or_default(),
                );
                store.insert(key, item.clone());
            }
        }
        for op in ops {
            if let WriteOp::Delete { pk, sk } = op {
                let key = (
                    shape.table.to_string(),
                    kv_to_string(pk),
                    sk.as_ref().map(kv_to_string).unwrap_or_default(),
                );
                store.remove(&key);
            }
        }
        Ok(BatchWriteOutcome::default())
    }

    async fn transact_get_raw(
        &self,
        items: &[TransactGetOp<'_>],
    ) -> Result<TransactGetOutcome, BackendError> {
        // Atomic-snapshot semantics: hold the mutex for the whole call
        // so the in-memory store can't change between per-item reads.
        let store = self.store.lock().unwrap();
        let mut out: Vec<Option<serde_json::Value>> = Vec::with_capacity(items.len());
        for op in items {
            let pk_s = kv_to_string(&op.pk);
            let sk_s = op.sk.as_ref().map(kv_to_string).unwrap_or_default();
            let key = (op.shape.table.to_string(), pk_s, sk_s);
            out.push(store.get(&key).cloned());
        }
        Ok(TransactGetOutcome { items: out })
    }

    async fn transact_write_raw(
        &self,
        items: Vec<TransactWriteOp<'_>>,
    ) -> Result<TransactWriteOutcome, BackendError> {
        // Two-pass: check all conditions first (under one mutex hold),
        // then apply all writes. This preserves the atomic semantic —
        // if any condition fails we don't mutate anything.
        let mut store = self.store.lock().unwrap();
        let n = items.len();
        let mut failed_at: Option<(usize, &'static str)> = None;
        for (i, op) in items.iter().enumerate() {
            // Capture the (table, pk, sk) lookup key for both phases.
            let (shape_table, pk, sk): (&str, &KeyValue, Option<&KeyValue>) = match op {
                TransactWriteOp::Put { shape, pk, sk, .. } => (shape.table, pk, sk.as_ref()),
                TransactWriteOp::Delete { shape, pk, sk, .. } => (shape.table, pk, sk.as_ref()),
                TransactWriteOp::ConditionCheck { shape, pk, sk, .. } => {
                    (shape.table, pk, sk.as_ref())
                }
                TransactWriteOp::Update { shape, pk, sk, .. } => (shape.table, pk, sk.as_ref()),
            };
            let store_key = (
                shape_table.to_string(),
                kv_to_string(pk),
                sk.map(kv_to_string).unwrap_or_default(),
            );
            let existing = store.get(&store_key).cloned();

            // Evaluate per-op condition (Put/Delete optional;
            // ConditionCheck mandatory; Update via the apply closure
            // which embeds its condition).
            let cond_passes = match op {
                TransactWriteOp::Put { condition, .. } => {
                    condition.as_ref().is_none_or(|c| c(existing.as_ref()))
                }
                TransactWriteOp::Delete { condition, .. } => {
                    condition.as_ref().is_none_or(|c| c(existing.as_ref()))
                }
                TransactWriteOp::ConditionCheck { condition, .. } => {
                    condition(existing.as_ref())
                }
                TransactWriteOp::Update { apply, .. } => {
                    match apply(existing.as_ref()) {
                        Ok(UpdateDecision::Apply(_)) => true,
                        Ok(UpdateDecision::Fail) => false,
                        Err(_) => false,
                    }
                }
            };
            if !cond_passes {
                failed_at = Some((i, "ConditionalCheckFailed"));
                break;
            }
        }
        if let Some((idx, code)) = failed_at {
            let mut reasons: Vec<TransactCancelReason> = (0..n)
                .map(|_| TransactCancelReason {
                    code: "None",
                    message: None,
                    item: None,
                })
                .collect();
            reasons[idx] = TransactCancelReason {
                code,
                message: Some("The conditional request failed".into()),
                item: None,
            };
            return Err(BackendError::TransactionCancelled { reasons });
        }

        // All conditions passed — apply.
        for op in items {
            match op {
                TransactWriteOp::Put { shape, pk, sk, item, .. } => {
                    let key = (
                        shape.table.to_string(),
                        kv_to_string(&pk),
                        sk.as_ref().map(kv_to_string).unwrap_or_default(),
                    );
                    store.insert(key, item);
                }
                TransactWriteOp::Delete { shape, pk, sk, .. } => {
                    let key = (
                        shape.table.to_string(),
                        kv_to_string(&pk),
                        sk.as_ref().map(kv_to_string).unwrap_or_default(),
                    );
                    store.remove(&key);
                }
                TransactWriteOp::ConditionCheck { .. } => {
                    // Read-only guard; no mutation.
                }
                TransactWriteOp::Update { shape, pk, sk, apply, .. } => {
                    let key = (
                        shape.table.to_string(),
                        kv_to_string(&pk),
                        sk.as_ref().map(kv_to_string).unwrap_or_default(),
                    );
                    let existing = store.get(&key).cloned();
                    match apply(existing.as_ref()) {
                        Ok(UpdateDecision::Apply(new_item)) => {
                            store.insert(key, new_item);
                        }
                        Ok(UpdateDecision::Fail) => {
                            // Should have been caught in phase 1; if we
                            // get here it's a non-deterministic apply
                            // closure (shouldn't happen).
                            unreachable!("phase 1 already validated apply")
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Ok(TransactWriteOutcome::default())
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
    // N-PK, B-PK, and S+B composite — matches rektifier.toml.example
    // so dispatch tests can exercise the same key-type matrix the
    // diff harness does. The MockBackend stringifies KeyValues so
    // every key type round-trips through the same in-memory path.
    let counters = TableSchema {
        name: "counters".into(),
        pg_table: "counters".into(),
        pk_attr: "id".into(),
        pk_type: KeyType::N,
        sk_attr: None,
        sk_type: None,
        jsonb_col: "data".into(),
    };
    let blobs = TableSchema {
        name: "blobs".into(),
        pg_table: "blobs".into(),
        pk_attr: "binmark".into(),
        pk_type: KeyType::B,
        sk_attr: None,
        sk_type: None,
        jsonb_col: "data".into(),
    };
    let binsorted = TableSchema {
        name: "binsorted".into(),
        pg_table: "binsorted".into(),
        pk_attr: "id".into(),
        pk_type: KeyType::S,
        sk_attr: Some("binmark".into()),
        sk_type: Some(KeyType::B),
        jsonb_col: "data".into(),
    };
    let mut m = HashMap::new();
    m.insert(users.name.clone(), users);
    m.insert(events.name.clone(), events);
    m.insert(messages.name.clone(), messages);
    m.insert(counters.name.clone(), counters);
    m.insert(blobs.name.clone(), blobs);
    m.insert(binsorted.name.clone(), binsorted);
    m
}

/// Build a fully-active catalog from the TableSchema fixtures used by
/// every dispatch test. Mirrors the post-D2 cache shape: every table
/// is `ACTIVE` + `serveable = true` so existing tests behave as before.
/// D4 will add explicit unserveable-gate tests by mutating this.
fn catalog() -> Arc<TableCatalog> {
    let mut entries = HashMap::new();
    for (name, schema) in schemas() {
        entries.insert(
            name.clone(),
            Arc::new(TableEntry {
                schema,
                status: TableStatus::Active,
                serveable: true,
                unserveable_reason: None,
                creation_date_ms: 0,
                last_modified_at_ms: 0,
                last_modified_by: "dispatch-test-fixture".into(),
                billing_mode: None,
                provisioned_rcu: None,
                provisioned_wcu: None,
                tags: serde_json::json!({}),
                gsis: HashMap::new(),
            }),
        );
    }
    Arc::new(TableCatalog::from_snapshot(CatalogSnapshot::from_entries(
        entries,
    )))
}

fn app() -> axum::Router {
    app_with(MockBackend::default(), std::time::Duration::from_secs(30))
}

/// Build an app whose `users` table is flagged unserveable (drift
/// scenario). Every op against `users` should return RNF with the
/// drift reason in the message; other tables remain serveable.
fn app_users_unserveable() -> axum::Router {
    let mut entries = HashMap::new();
    for (name, schema) in schemas() {
        let serveable = name != "users";
        let reason = if serveable {
            None
        } else {
            Some("expected jsonb column `data`, got `text`".to_string())
        };
        entries.insert(
            name.clone(),
            Arc::new(TableEntry {
                schema,
                status: if serveable {
                    TableStatus::Active
                } else {
                    TableStatus::Degraded
                },
                serveable,
                unserveable_reason: reason,
                creation_date_ms: 0,
                last_modified_at_ms: 0,
                last_modified_by: "dispatch-test-fixture".into(),
                billing_mode: None,
                provisioned_rcu: None,
                provisioned_wcu: None,
                tags: serde_json::json!({}),
                gsis: HashMap::new(),
            }),
        );
    }
    let catalog = Arc::new(TableCatalog::from_snapshot(CatalogSnapshot::from_entries(
        entries,
    )));
    let state = AppState {
        verifier: Arc::new(PermissiveVerifier) as Arc<dyn Verifier>,
        backend: Arc::new(MockBackend::default()) as Arc<dyn Backend>,
        catalog,
        batch_limits: rekt_server::BatchLimits::default(),
        request_timeout: std::time::Duration::from_secs(30),
    };
    router(state)
}

/// Builder for tests that need a non-default backend (e.g. one that
/// panics or sleeps to exercise the resilience middleware) or a
/// custom request timeout.
fn app_with(backend: MockBackend, request_timeout: std::time::Duration) -> axum::Router {
    let state = AppState {
        verifier: Arc::new(PermissiveVerifier) as Arc<dyn Verifier>,
        backend: Arc::new(backend) as Arc<dyn Backend>,
        catalog: catalog(),
        batch_limits: rekt_server::BatchLimits::default(),
        request_timeout,
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
    // `CreateTable` is not yet implemented (PLAN-10 D6). Swap this for
    // whatever is still unsupported as more ops land.
    let resp = app
        .oneshot(ddb_request(
            "CreateTable",
            json!({
                "TableName":"new",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]
            }),
        ))
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

// ===== BatchGetItem projection + ConsistentRead (B3) =========================

#[tokio::test]
async fn batch_get_with_projection_prunes_items() {
    let app = app();
    for n in ["pj-a", "pj-b"] {
        put_item(
            &app,
            "users",
            json!({"id":{"S":n},"label":{"S":format!("L-{n}")},"tier":{"S":"admin"}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"pj-a"}},{"id":{"S":"pj-b"}}],
                "ProjectionExpression":"label"
            }}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let items = body["Responses"]["users"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    for it in items {
        let keys: Vec<&str> = it.as_object().unwrap().keys().map(|s| s.as_str()).collect();
        assert_eq!(keys, vec!["label"], "expected only `label` attribute, got {keys:?}");
    }
}

#[tokio::test]
async fn batch_get_with_projection_including_pk_and_other_attrs() {
    let app = app();
    put_item(
        &app,
        "users",
        json!({"id":{"S":"pj-multi"},"label":{"S":"L"},"tier":{"S":"x"},"flag":{"S":"on"}}),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"pj-multi"}}],
                "ProjectionExpression":"id, label, tier"
            }}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let it = &body["Responses"]["users"][0];
    let mut keys: Vec<&str> = it.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["id", "label", "tier"]);
    assert!(it["flag"].is_null(), "flag should have been pruned");
}

#[tokio::test]
async fn batch_get_projection_with_ean_alias_for_reserved_word() {
    // `name` is a DDB reserved word; must use EAN alias.
    let app = app();
    put_item(
        &app,
        "users",
        json!({"id":{"S":"pj-rw"},"label":{"S":"L"}}),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"pj-rw"}}],
                "ProjectionExpression":"#n, id",
                "ExpressionAttributeNames":{"#n":"label"}
            }}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let it = &body["Responses"]["users"][0];
    let mut keys: Vec<&str> = it.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["id", "label"]);
}

#[tokio::test]
async fn batch_get_projection_of_missing_attr_silently_absent() {
    // Asking for an attr the item doesn't have: silently dropped on
    // that item (no error). Matches DDB.
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"pj-miss"},"label":{"S":"L"}})).await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"pj-miss"}}],
                "ProjectionExpression":"label, nothere"
            }}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let it = &body["Responses"]["users"][0];
    let keys: Vec<&str> = it.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    assert_eq!(keys, vec!["label"]);
}

#[tokio::test]
async fn batch_get_consistent_read_silently_accepted() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"cr-2"},"label":{"S":"x"}})).await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"cr-2"}}],
                "ConsistentRead":true
            }}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["users"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn batch_get_per_table_consistent_read_differs_across_tables() {
    // ConsistentRead is a per-table flag in DDB; rektifier just
    // surfaces it via tracing per-table. Smoke-test that mixing
    // per-table values doesn't cross-contaminate.
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"cr-mix"},"label":{"S":"u"}})).await;
    put_item(
        &app,
        "device_events",
        json!({"device_id":{"S":"d"},"ts":{"N":"1"},"flag":{"S":"on"}}),
    )
    .await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{
                "users":{
                    "Keys":[{"id":{"S":"cr-mix"}}],
                    "ConsistentRead":true
                },
                "device_events":{
                    "Keys":[{"device_id":{"S":"d"},"ts":{"N":"1"}}],
                    "ConsistentRead":false
                }
            }}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn batch_get_invalid_projection_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"x"}}],
                "ProjectionExpression":"label, , bad"
            }}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"].as_str().unwrap().ends_with("#ValidationException"));
}

#[tokio::test]
async fn batch_get_reserved_word_in_bare_projection_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"users":{
                "Keys":[{"id":{"S":"x"}}],
                "ProjectionExpression":"name"
            }}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.contains("reserved keyword") || msg.contains("Reserved"),
        "expected reserved-word rejection, got: {msg}"
    );
}

// ===== BatchWriteItem Put-only single-table (B4) =============================

/// Helper: GetItem against the mock to verify what BatchWriteItem
/// actually persisted (or removed).
async fn get_item_value(app: &axum::Router, table: &str, key: Value) -> Value {
    let body = json!({"TableName": table, "Key": key});
    let resp = app
        .clone()
        .oneshot(ddb_request("GetItem", body))
        .await
        .unwrap();
    body_json(resp).await
}

#[tokio::test]
async fn batch_write_put_only_persists_items() {
    let app = app();
    let req_items = json!({"users":[
        {"PutRequest":{"Item":{"id":{"S":"bw-a"},"label":{"S":"A"}}}},
        {"PutRequest":{"Item":{"id":{"S":"bw-b"},"label":{"S":"B"}}}},
        {"PutRequest":{"Item":{"id":{"S":"bw-c"},"label":{"S":"C"}}}}
    ]});
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems": req_items}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["UnprocessedItems"], json!({}));

    // Verify via GetItem.
    for (id, label) in [("bw-a","A"),("bw-b","B"),("bw-c","C")] {
        let got = get_item_value(&app, "users", json!({"id":{"S":id}})).await;
        assert_eq!(got["Item"]["label"]["S"].as_str(), Some(label));
    }
}

#[tokio::test]
async fn batch_write_put_overwrites_existing() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"bw-over"},"label":{"S":"old"}})).await;
    let req_items = json!({"users":[
        {"PutRequest":{"Item":{"id":{"S":"bw-over"},"label":{"S":"new"}}}}
    ]});
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems": req_items}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = get_item_value(&app, "users", json!({"id":{"S":"bw-over"}})).await;
    assert_eq!(got["Item"]["label"]["S"].as_str(), Some("new"));
}

#[tokio::test]
async fn batch_write_composite_key_puts() {
    let app = app();
    let req_items = json!({"device_events":[
        {"PutRequest":{"Item":{
            "device_id":{"S":"bw-d"},"ts":{"N":"1"},"flag":{"S":"on"}
        }}},
        {"PutRequest":{"Item":{
            "device_id":{"S":"bw-d"},"ts":{"N":"2"},"flag":{"S":"off"}
        }}}
    ]});
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems": req_items}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got1 = get_item_value(&app, "device_events", json!({"device_id":{"S":"bw-d"},"ts":{"N":"1"}})).await;
    assert_eq!(got1["Item"]["flag"]["S"].as_str(), Some("on"));
    let got2 = get_item_value(&app, "device_events", json!({"device_id":{"S":"bw-d"},"ts":{"N":"2"}})).await;
    assert_eq!(got2["Item"]["flag"]["S"].as_str(), Some("off"));
}

#[tokio::test]
async fn batch_write_rejects_empty_request_items() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"].as_str().unwrap().ends_with("#ValidationException"));
    assert!(body["message"].as_str().unwrap().contains("RequestItems"));
}

#[tokio::test]
async fn batch_write_rejects_empty_per_table_writes() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_write_rejects_duplicate_put_keys() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"PutRequest":{"Item":{"id":{"S":"bw-dup"},"label":{"S":"a"}}}},
                {"PutRequest":{"Item":{"id":{"S":"bw-dup"},"label":{"S":"b"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["message"].as_str().unwrap().contains("duplicate"));
}

#[tokio::test]
async fn batch_write_rejects_malformed_write_request() {
    // Both PutRequest and DeleteRequest present.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"PutRequest":{"Item":{"id":{"S":"x"}}},
                 "DeleteRequest":{"Key":{"id":{"S":"y"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("exactly one"));
}

#[tokio::test]
async fn batch_write_rejects_neither_put_nor_delete() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[{}]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_write_unknown_table_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"nope":[
                {"PutRequest":{"Item":{"id":{"S":"x"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ResourceNotFoundException"));
}

#[tokio::test]
async fn batch_write_too_many_requests_rejected() {
    let app = app();
    let mut writes = Vec::new();
    for n in 0..26u32 {
        writes.push(json!({"PutRequest":{"Item":{"id":{"S":format!("over-{n}")}}}}));
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users": writes}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["message"].as_str().unwrap().contains("Too many"));
}

#[tokio::test]
async fn batch_write_25_requests_accepted() {
    let app = app();
    let mut writes = Vec::new();
    for n in 0..25u32 {
        writes.push(json!({"PutRequest":{"Item":{"id":{"S":format!("at-cap-{n}")}}}}));
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users": writes}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn batch_write_put_missing_pk_attr_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"PutRequest":{"Item":{"label":{"S":"no-pk"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_write_put_wrong_pk_type_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"PutRequest":{"Item":{"id":{"N":"42"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ===== BatchWriteItem Put+Delete mix + multi-table (B5) =======================

#[tokio::test]
async fn batch_write_mixed_put_and_delete_single_table() {
    let app = app();
    // Pre-seed two rows that we'll delete.
    put_item(&app, "users", json!({"id":{"S":"bw-mx-old1"},"label":{"S":"a"}})).await;
    put_item(&app, "users", json!({"id":{"S":"bw-mx-old2"},"label":{"S":"b"}})).await;

    let req_items = json!({"users":[
        {"PutRequest":{"Item":{"id":{"S":"bw-mx-new1"},"label":{"S":"X"}}}},
        {"DeleteRequest":{"Key":{"id":{"S":"bw-mx-old1"}}}},
        {"DeleteRequest":{"Key":{"id":{"S":"bw-mx-old2"}}}},
        {"PutRequest":{"Item":{"id":{"S":"bw-mx-new2"},"label":{"S":"Y"}}}}
    ]});
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems": req_items}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // new1, new2 present; old1, old2 gone.
    let n1 = get_item_value(&app, "users", json!({"id":{"S":"bw-mx-new1"}})).await;
    assert_eq!(n1["Item"]["label"]["S"].as_str(), Some("X"));
    let n2 = get_item_value(&app, "users", json!({"id":{"S":"bw-mx-new2"}})).await;
    assert_eq!(n2["Item"]["label"]["S"].as_str(), Some("Y"));
    let g_old1 = get_item_value(&app, "users", json!({"id":{"S":"bw-mx-old1"}})).await;
    assert!(g_old1.get("Item").is_none());
    let g_old2 = get_item_value(&app, "users", json!({"id":{"S":"bw-mx-old2"}})).await;
    assert!(g_old2.get("Item").is_none());
}

#[tokio::test]
async fn batch_write_delete_of_missing_row_is_noop() {
    let app = app();
    // No pre-seed; delete a key that doesn't exist.
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"DeleteRequest":{"Key":{"id":{"S":"never-existed"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn batch_write_rejects_put_plus_delete_of_same_key() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"PutRequest":{"Item":{"id":{"S":"bw-mx-clash"},"label":{"S":"x"}}}},
                {"DeleteRequest":{"Key":{"id":{"S":"bw-mx-clash"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["message"].as_str().unwrap().contains("duplicate"));
}

#[tokio::test]
async fn batch_write_multi_table_fan_out() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"bw-mt-u-del"},"label":{"S":"x"}})).await;
    let req_items = json!({
        "users":[
            {"PutRequest":{"Item":{"id":{"S":"bw-mt-u-new"},"label":{"S":"new"}}}},
            {"DeleteRequest":{"Key":{"id":{"S":"bw-mt-u-del"}}}}
        ],
        "device_events":[
            {"PutRequest":{"Item":{
                "device_id":{"S":"bw-mt-d"},"ts":{"N":"1"},"flag":{"S":"on"}
            }}}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems": req_items}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // users: new present, del gone.
    let n = get_item_value(&app, "users", json!({"id":{"S":"bw-mt-u-new"}})).await;
    assert_eq!(n["Item"]["label"]["S"].as_str(), Some("new"));
    let d = get_item_value(&app, "users", json!({"id":{"S":"bw-mt-u-del"}})).await;
    assert!(d.get("Item").is_none());
    // device_events: row present.
    let e = get_item_value(
        &app,
        "device_events",
        json!({"device_id":{"S":"bw-mt-d"},"ts":{"N":"1"}}),
    )
    .await;
    assert_eq!(e["Item"]["flag"]["S"].as_str(), Some("on"));
}

#[tokio::test]
async fn batch_write_multi_table_unknown_table_fails_whole_request() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"bw-mt-fail"},"label":{"S":"alive"}})).await;
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{
                "users":[
                    {"PutRequest":{"Item":{"id":{"S":"bw-mt-should-not-write"},"label":{"S":"nope"}}}}
                ],
                "no-such-table":[
                    {"PutRequest":{"Item":{"id":{"S":"x"}}}}
                ]
            }}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    // No partial write should have happened — translator rejects
    // before any SQL fires.
    let pre = get_item_value(&app, "users", json!({"id":{"S":"bw-mt-should-not-write"}})).await;
    assert!(pre.get("Item").is_none(),
        "translator must reject the whole batch before issuing SQL");
    // The pre-existing row should still be alive (no rollback artifact).
    let alive = get_item_value(&app, "users", json!({"id":{"S":"bw-mt-fail"}})).await;
    assert_eq!(alive["Item"]["label"]["S"].as_str(), Some("alive"));
}

#[tokio::test]
async fn batch_write_multi_table_total_cap_summed() {
    let app = app();
    // 13 users + 13 events = 26 > 25.
    let mut us = Vec::new();
    let mut es = Vec::new();
    for n in 0..13 {
        us.push(json!({"PutRequest":{"Item":{"id":{"S":format!("bw-tc-u{n}")}}}}));
        es.push(json!({"PutRequest":{"Item":{
            "device_id":{"S":format!("bw-tc-d{n}")},"ts":{"N":"1"}
        }}}));
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users": us, "device_events": es}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["message"].as_str().unwrap().contains("Too many"));
}

// ===== BatchWriteItem validation polish (B6) ==================================

#[tokio::test]
async fn batch_write_rejects_condition_expression_in_put_request() {
    // D6: PutRequest only carries Item. DDB rejects extra fields;
    // we match via serde(deny_unknown_fields), surfacing as
    // SerializationException.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"PutRequest":{
                    "Item":{"id":{"S":"x"}},
                    "ConditionExpression":"attribute_not_exists(id)"
                }}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    let ty = body["__type"].as_str().unwrap();
    assert!(
        ty.ends_with("#SerializationException") || ty.ends_with("#ValidationException"),
        "expected Ser/ValidationException, got {ty}"
    );
}

#[tokio::test]
async fn batch_write_rejects_return_values_in_put_request() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"PutRequest":{
                    "Item":{"id":{"S":"x"}},
                    "ReturnValues":"ALL_OLD"
                }}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_write_rejects_extra_field_in_delete_request() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"users":[
                {"DeleteRequest":{
                    "Key":{"id":{"S":"x"}},
                    "ConditionExpression":"attribute_exists(id)"
                }}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_write_accepts_return_consumed_capacity_envelope() {
    // ReturnConsumedCapacity at the *envelope* level is accepted-and-
    // dropped (we have no metering). Should not error.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({
                "RequestItems":{"users":[
                    {"PutRequest":{"Item":{"id":{"S":"bw-rcc"}}}}
                ]},
                "ReturnConsumedCapacity":"TOTAL"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn batch_write_accepts_return_item_collection_metrics_envelope() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({
                "RequestItems":{"users":[
                    {"PutRequest":{"Item":{"id":{"S":"bw-ricm"}}}}
                ]},
                "ReturnItemCollectionMetrics":"SIZE"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn batch_get_accepts_return_consumed_capacity_envelope() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"bg-rcc"},"label":{"S":"x"}})).await;
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({
                "RequestItems":{"users":{"Keys":[{"id":{"S":"bg-rcc"}}]}},
                "ReturnConsumedCapacity":"TOTAL"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===== Batch op coverage closure (high + medium gaps) ========================
//
// Audit follow-up — closes the per-key-type-matrix gap (N-PK / B-PK
// at dispatch layer) and the B-typed SK in BatchGetItem gap that
// PG-live + diff couldn't catch alone. Per-attribute-variant
// round-trips and PG-level boundary tests live in the libpq + diff
// surfaces; this file focuses on dispatch-layer key-type coverage.

#[tokio::test]
async fn batch_get_numeric_pk_via_counters() {
    let app = app();
    for n in ["1", "2", "3"] {
        put_item(
            &app,
            "counters",
            json!({"id":{"N":n},"tally":{"N":n}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"counters":{"Keys":[
                {"id":{"N":"1"}},
                {"id":{"N":"3"}},
                {"id":{"N":"999"}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let items = body["Responses"]["counters"].as_array().unwrap();
    assert_eq!(items.len(), 2, "expected 2 hits, got {items:?}");
}

#[tokio::test]
async fn batch_get_binary_pk_via_blobs() {
    let app = app();
    // DDB-JSON binary values are base64-encoded on the wire.
    for b64 in ["YQ==", "Yg==", "Yw=="] {
        put_item(
            &app,
            "blobs",
            json!({"binmark":{"B":b64},"label":{"S":format!("L-{b64}")}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"blobs":{"Keys":[
                {"binmark":{"B":"YQ=="}},
                {"binmark":{"B":"Yw=="}},
                {"binmark":{"B":"enp6"}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["blobs"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn batch_get_binary_sk_via_binsorted() {
    let app = app();
    // S PK + B SK — the composite SQL path's binary SK binding is
    // unexercised elsewhere at the dispatch layer.
    for b64 in ["AAE=", "AAI=", "AAM="] {
        put_item(
            &app,
            "binsorted",
            json!({"id":{"S":"part-1"},"binmark":{"B":b64}}),
        )
        .await;
    }
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{"binsorted":{"Keys":[
                {"id":{"S":"part-1"},"binmark":{"B":"AAE="}},
                {"id":{"S":"part-1"},"binmark":{"B":"AAM="}}
            ]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["Responses"]["binsorted"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn batch_write_numeric_pk_via_counters() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"counters":[
                {"PutRequest":{"Item":{"id":{"N":"42"},"tally":{"N":"100"}}}},
                {"PutRequest":{"Item":{"id":{"N":"43"},"tally":{"N":"200"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // Verify both rows persisted.
    let got = get_item_value(&app, "counters", json!({"id":{"N":"42"}})).await;
    assert_eq!(got["Item"]["tally"]["N"].as_str(), Some("100"));
    let got2 = get_item_value(&app, "counters", json!({"id":{"N":"43"}})).await;
    assert_eq!(got2["Item"]["tally"]["N"].as_str(), Some("200"));
}

#[tokio::test]
async fn batch_write_binary_pk_via_blobs() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"blobs":[
                {"PutRequest":{"Item":{"binmark":{"B":"YnctYg=="},"label":{"S":"x"}}}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = get_item_value(&app, "blobs", json!({"binmark":{"B":"YnctYg=="}})).await;
    assert_eq!(got["Item"]["label"]["S"].as_str(), Some("x"));
}

#[tokio::test]
async fn batch_write_binary_sk_via_binsorted() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{"binsorted":[
                {"PutRequest":{"Item":{
                    "id":{"S":"bw-bsk"},"binmark":{"B":"AAEC"},"label":{"S":"x"}
                }}},
                {"DeleteRequest":{"Key":{
                    "id":{"S":"bw-bsk"},"binmark":{"B":"AAED"}
                }}}
            ]}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===== Obs-2: resilience middleware (CatchPanic + Timeout) ===================

#[tokio::test]
async fn panic_in_handler_returns_500_via_catch_panic_layer() {
    let backend = MockBackend {
        panic_on_get: Some("forced panic for Obs-2 test"),
        ..Default::default()
    };
    let app = app_with(backend, std::time::Duration::from_secs(30));
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"x"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = body_json(resp).await;
    let ty = body["__type"].as_str().unwrap();
    assert!(
        ty.ends_with("#InternalServerError"),
        "expected InternalServerError shape, got {ty}"
    );
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.contains("panic"),
        "panic detail must appear in body, got: {msg}"
    );
}

#[tokio::test]
async fn slow_handler_exceeds_timeout_returns_408() {
    // Backend sleeps 200ms; timeout is 50ms — TimeoutLayer must
    // interrupt with REQUEST_TIMEOUT.
    let backend = MockBackend {
        sleep_on_get: Some(std::time::Duration::from_millis(200)),
        ..Default::default()
    };
    let app = app_with(backend, std::time::Duration::from_millis(50));
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"x"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
}

#[tokio::test]
async fn fast_handler_under_timeout_passes() {
    // Sanity check: a quick request under the timeout still succeeds
    // — confirms TimeoutLayer isn't tripping on every request.
    let backend = MockBackend {
        sleep_on_get: Some(std::time::Duration::from_millis(10)),
        ..Default::default()
    };
    let app = app_with(backend, std::time::Duration::from_millis(500));
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"x"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===== TransactGetItems (T1) ================================================

/// Helper: seed a row directly into the mock store.
async fn seed_put(app: axum::Router, table: &str, item: Value) {
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({ "TableName": table, "Item": item }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn transact_get_single_table_happy_path() {
    let app = app();
    seed_put(
        app.clone(),
        "users",
        json!({"id":{"S":"u1"},"label":{"S":"alice"}}),
    )
    .await;
    seed_put(
        app.clone(),
        "users",
        json!({"id":{"S":"u2"},"label":{"S":"bob"}}),
    )
    .await;

    let body = json!({
        "TransactItems": [
            {"Get": {"TableName":"users","Key":{"id":{"S":"u1"}}}},
            {"Get": {"TableName":"users","Key":{"id":{"S":"u2"}}}},
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    assert_eq!(
        got,
        json!({
            "Responses": [
                {"Item": {"id":{"S":"u1"},"label":{"S":"alice"}}},
                {"Item": {"id":{"S":"u2"},"label":{"S":"bob"}}},
            ]
        })
    );
}

#[tokio::test]
async fn transact_get_missing_item_returns_empty_slot() {
    let app = app();
    seed_put(
        app.clone(),
        "users",
        json!({"id":{"S":"present"},"label":{"S":"yo"}}),
    )
    .await;
    let body = json!({
        "TransactItems": [
            {"Get": {"TableName":"users","Key":{"id":{"S":"present"}}}},
            {"Get": {"TableName":"users","Key":{"id":{"S":"missing"}}}},
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    let got = body_json(resp).await;
    // D8: position-preserving; D10/T1: missing slot is `{}` (Item field absent).
    let responses = got.get("Responses").and_then(Value::as_array).unwrap();
    assert_eq!(responses.len(), 2);
    assert!(responses[0].get("Item").is_some());
    assert!(responses[1].get("Item").is_none());
    assert_eq!(responses[1], json!({}));
}

#[tokio::test]
async fn transact_get_position_preserves_request_order() {
    // Even with seeded reverse-order keys, the response must come back in
    // the request's order. D8.
    let app = app();
    for n in ["b", "a", "c"] {
        seed_put(
            app.clone(),
            "users",
            json!({"id":{"S":n},"order":{"S":n}}),
        )
        .await;
    }
    let body = json!({
        "TransactItems": [
            {"Get": {"TableName":"users","Key":{"id":{"S":"c"}}}},
            {"Get": {"TableName":"users","Key":{"id":{"S":"a"}}}},
            {"Get": {"TableName":"users","Key":{"id":{"S":"b"}}}},
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    let got = body_json(resp).await;
    let responses = got.get("Responses").and_then(Value::as_array).unwrap();
    assert_eq!(
        responses[0]["Item"]["id"]["S"].as_str().unwrap(),
        "c"
    );
    assert_eq!(
        responses[1]["Item"]["id"]["S"].as_str().unwrap(),
        "a"
    );
    assert_eq!(
        responses[2]["Item"]["id"]["S"].as_str().unwrap(),
        "b"
    );
}

#[tokio::test]
async fn transact_get_composite_table_round_trip() {
    let app = app();
    seed_put(
        app.clone(),
        "device_events",
        json!({
            "device_id":{"S":"d1"},
            "ts":{"N":"42"},
            "label":{"S":"event42"}
        }),
    )
    .await;
    let body = json!({
        "TransactItems": [{
            "Get": {
                "TableName":"device_events",
                "Key":{"device_id":{"S":"d1"},"ts":{"N":"42"}}
            }
        }]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    let got = body_json(resp).await;
    assert_eq!(
        got["Responses"][0]["Item"]["label"]["S"]
            .as_str()
            .unwrap(),
        "event42"
    );
}

#[tokio::test]
async fn transact_get_empty_request_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactGetItems",
            json!({"TransactItems": []}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn transact_get_missing_table_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName":"nope","Key":{"id":{"S":"x"}}}}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ResourceNotFoundException"));
}

#[tokio::test]
async fn transact_get_duplicate_target_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName":"users","Key":{"id":{"S":"a"}}}},
                {"Get": {"TableName":"users","Key":{"id":{"S":"a"}}}},
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("multiple operations"));
}

#[tokio::test]
async fn transact_get_malformed_item_missing_get_rejected() {
    let app = app();
    // TransactItem with no inner key at all.
    let resp = app
        .oneshot(ddb_request(
            "TransactGetItems",
            json!({"TransactItems": [{}]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn transact_get_unknown_field_inside_get_rejected() {
    // serde deny_unknown_fields on Get.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName":"users","Key":{"id":{"S":"a"}},
                         "Bogus":"x"}}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    // serde rejects unknown field with SerializationException.
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#SerializationException"));
}

#[tokio::test]
async fn transact_get_extra_key_attr_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName":"users","Key":{
                    "id":{"S":"a"},
                    "extra":{"S":"nope"}
                }}}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn transact_get_wrong_key_type_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName":"users","Key":{"id":{"N":"42"}}}}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
}

// ===== TransactGetItems T2 — multi-table + projection ========================

#[tokio::test]
async fn transact_get_multi_table_happy_path() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"mt-u1"},"label":{"S":"alice"}})).await;
    put_item(
        &app,
        "device_events",
        json!({
            "device_id":{"S":"mt-d1"},
            "ts":{"N":"99"},
            "label":{"S":"e99"}
        }),
    )
    .await;

    let body = json!({
        "TransactItems": [
            {"Get":{"TableName":"users","Key":{"id":{"S":"mt-u1"}}}},
            {"Get":{"TableName":"device_events",
                    "Key":{"device_id":{"S":"mt-d1"},"ts":{"N":"99"}}}}
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    // D8: positional. Slot 0 is users; slot 1 is device_events.
    assert_eq!(got["Responses"][0]["Item"]["label"]["S"], "alice");
    assert_eq!(got["Responses"][1]["Item"]["label"]["S"], "e99");
}

#[tokio::test]
async fn transact_get_multi_table_mixed_hit_miss() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"mt-hit"}})).await;
    // events: nothing seeded
    let body = json!({
        "TransactItems": [
            {"Get":{"TableName":"users","Key":{"id":{"S":"mt-hit"}}}},
            {"Get":{"TableName":"users","Key":{"id":{"S":"mt-not"}}}},
            {"Get":{"TableName":"device_events",
                    "Key":{"device_id":{"S":"d"},"ts":{"N":"1"}}}}
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    assert!(got["Responses"][0].get("Item").is_some());
    assert!(got["Responses"][1].get("Item").is_none());
    assert!(got["Responses"][2].get("Item").is_none());
}

#[tokio::test]
async fn transact_get_missing_table_of_n_rejects_whole_request() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"x"}})).await;
    let body = json!({
        "TransactItems": [
            {"Get":{"TableName":"users","Key":{"id":{"S":"x"}}}},
            {"Get":{"TableName":"does_not_exist","Key":{"id":{"S":"y"}}}}
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ResourceNotFoundException"));
}

#[tokio::test]
async fn transact_get_with_projection_prunes_item() {
    let app = app();
    put_item(
        &app,
        "users",
        json!({
            "id":{"S":"tg-pj"},
            "label":{"S":"L"},
            "tier":{"S":"admin"},
            "flag":{"S":"on"}
        }),
    )
    .await;
    let body = json!({
        "TransactItems": [{
            "Get": {
                "TableName":"users",
                "Key":{"id":{"S":"tg-pj"}},
                "ProjectionExpression":"id, label"
            }
        }]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    let it = &got["Responses"][0]["Item"];
    let mut keys: Vec<&str> = it.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["id", "label"]);
}

#[tokio::test]
async fn transact_get_projection_per_item_independent() {
    // Different items in the same TransactGet may carry different
    // ProjectionExpressions; T2's per-item pruning honors each
    // independently.
    let app = app();
    put_item(
        &app,
        "users",
        json!({"id":{"S":"pi-1"},"label":{"S":"L1"},"flag":{"S":"x"}}),
    )
    .await;
    put_item(
        &app,
        "users",
        json!({"id":{"S":"pi-2"},"label":{"S":"L2"},"flag":{"S":"y"}}),
    )
    .await;
    let body = json!({
        "TransactItems": [
            {"Get":{"TableName":"users","Key":{"id":{"S":"pi-1"}},
                    "ProjectionExpression":"label"}},
            {"Get":{"TableName":"users","Key":{"id":{"S":"pi-2"}},
                    "ProjectionExpression":"id, flag"}}
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    let got = body_json(resp).await;
    let it0 = &got["Responses"][0]["Item"];
    let it1 = &got["Responses"][1]["Item"];
    let k0: Vec<&str> = it0.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    let mut k1: Vec<&str> = it1.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    k1.sort();
    assert_eq!(k0, vec!["label"]);
    assert_eq!(k1, vec!["flag", "id"]);
}

#[tokio::test]
async fn transact_get_projection_with_ean_alias() {
    let app = app();
    put_item(
        &app,
        "users",
        json!({"id":{"S":"pj-rw"},"label":{"S":"L"}}),
    )
    .await;
    let body = json!({
        "TransactItems": [{
            "Get": {
                "TableName":"users",
                "Key":{"id":{"S":"pj-rw"}},
                "ProjectionExpression":"#n, id",
                "ExpressionAttributeNames":{"#n":"label"}
            }
        }]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    let it = &got["Responses"][0]["Item"];
    let mut keys: Vec<&str> = it.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["id", "label"]);
}

#[tokio::test]
async fn transact_get_projection_missing_attr_silently_dropped() {
    // ProjectionExpression names an attr that isn't on the item.
    // DDB: silently drops the missing attr (same as Query/Scan).
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"pj-miss"},"label":{"S":"L"}})).await;
    let body = json!({
        "TransactItems": [{
            "Get": {
                "TableName":"users",
                "Key":{"id":{"S":"pj-miss"}},
                "ProjectionExpression":"label, not_present"
            }
        }]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    let got = body_json(resp).await;
    let it = &got["Responses"][0]["Item"];
    let keys: Vec<&str> = it.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    assert_eq!(keys, vec!["label"]);
}

#[tokio::test]
async fn transact_get_projection_nested_path_rejected() {
    // v1 = top-level paths only.
    let app = app();
    let body = json!({
        "TransactItems": [{
            "Get": {
                "TableName":"users",
                "Key":{"id":{"S":"x"}},
                "ProjectionExpression":"meta.nested"
            }
        }]
    });
    let resp = app
        .oneshot(ddb_request("TransactGetItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
}

// ===== TransactWriteItems T3 — Put-only =====================================

#[tokio::test]
async fn transact_write_put_only_round_trip() {
    let app = app();
    let body = json!({
        "TransactItems": [
            {"Put":{"TableName":"users","Item":{"id":{"S":"tw-u1"},"label":{"S":"alice"}}}},
            {"Put":{"TableName":"users","Item":{"id":{"S":"tw-u2"},"label":{"S":"bob"}}}}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_json(resp).await;
    // DDB emits empty body on success.
    assert_eq!(got, json!({}));

    // Verify both rows present via GetItem.
    for id in ["tw-u1", "tw-u2"] {
        let resp = app
            .clone()
            .oneshot(ddb_request(
                "GetItem",
                json!({"TableName":"users","Key":{"id":{"S":id}}}),
            ))
            .await
            .unwrap();
        let body = body_json(resp).await;
        assert!(body.get("Item").is_some(), "missing {id}");
    }
}

#[tokio::test]
async fn transact_write_conditional_put_succeeds() {
    let app = app();
    let body = json!({
        "TransactItems": [{
            "Put": {
                "TableName":"users",
                "Item":{"id":{"S":"tw-cond-ok"},"label":{"S":"L"}},
                "ConditionExpression":"attribute_not_exists(id)"
            }
        }]
    });
    let resp = app
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn transact_write_conditional_put_fails_returns_cancelled() {
    let app = app();
    // Seed row.
    put_item(&app, "users", json!({"id":{"S":"tw-cond-fail"}})).await;
    // attribute_not_exists should fail.
    let body = json!({
        "TransactItems": [{
            "Put": {
                "TableName":"users",
                "Item":{"id":{"S":"tw-cond-fail"},"label":{"S":"new"}},
                "ConditionExpression":"attribute_not_exists(id)"
            }
        }]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#TransactionCanceledException"));
    // CancellationReasons[0] should be ConditionalCheckFailed.
    assert_eq!(
        body["CancellationReasons"][0]["Code"].as_str().unwrap(),
        "ConditionalCheckFailed"
    );
}

#[tokio::test]
async fn transact_write_one_failure_rolls_back_others() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"tw-already"}})).await;
    // Two puts: first OK, second has a condition that will fail.
    let body = json!({
        "TransactItems": [
            {"Put":{"TableName":"users","Item":{"id":{"S":"tw-side-effect"},"label":{"S":"x"}}}},
            {"Put":{
                "TableName":"users",
                "Item":{"id":{"S":"tw-already"},"label":{"S":"y"}},
                "ConditionExpression":"attribute_not_exists(id)"
            }}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    // CancellationReasons indexed by position: slot 0 = None, slot 1 = failure.
    assert_eq!(body["CancellationReasons"][0]["Code"].as_str().unwrap(), "None");
    assert_eq!(
        body["CancellationReasons"][1]["Code"].as_str().unwrap(),
        "ConditionalCheckFailed"
    );

    // Critical assertion: the would-have-been-Put at slot 0 must NOT
    // have landed — atomicity.
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"tw-side-effect"}}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert!(body.get("Item").is_none(), "atomicity violation: {body}");
}

#[tokio::test]
async fn transact_write_empty_request_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn transact_write_missing_table_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[
                {"Put":{"TableName":"nope","Item":{"id":{"S":"x"}}}}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ResourceNotFoundException"));
}

#[tokio::test]
async fn transact_write_duplicate_target_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[
                {"Put":{"TableName":"users","Item":{"id":{"S":"dup"}}}},
                {"Put":{"TableName":"users","Item":{"id":{"S":"dup"},"label":{"S":"x"}}}}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("multiple operations"));
}

#[tokio::test]
async fn transact_write_client_request_token_accepted_and_dropped() {
    // T8 will implement real idempotency; v1 accepts and discards.
    let app = app();
    let body = json!({
        "TransactItems": [{
            "Put":{"TableName":"users","Item":{"id":{"S":"tw-crt"}}}
        }],
        "ClientRequestToken": "abc-123"
    });
    let resp = app
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn transact_write_client_request_token_too_long_rejected() {
    let app = app();
    let body = json!({
        "TransactItems": [{"Put":{"TableName":"users","Item":{"id":{"S":"x"}}}}],
        "ClientRequestToken": "x".repeat(37)
    });
    let resp = app
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn transact_write_unknown_field_in_put_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[
                {"Put":{"TableName":"users","Item":{"id":{"S":"x"}},"Bogus":"q"}}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#SerializationException"));
}

#[tokio::test]
async fn transact_write_update_invalid_expression_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[
                {"Update":{
                    "TableName":"users",
                    "Key":{"id":{"S":"x"}},
                    "UpdateExpression":"GARBAGE"
                }}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn transact_write_composite_table_put() {
    let app = app();
    let body = json!({
        "TransactItems": [{
            "Put":{
                "TableName":"device_events",
                "Item":{"device_id":{"S":"tw-d1"},"ts":{"N":"42"},"label":{"S":"E"}}
            }
        }]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // Verify via GetItem.
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({
                "TableName":"device_events",
                "Key":{"device_id":{"S":"tw-d1"},"ts":{"N":"42"}}
            }),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Item"]["label"]["S"], "E");
}

// ===== TransactWriteItems T4 — Delete + ConditionCheck =======================

#[tokio::test]
async fn transact_write_delete_round_trip() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"tw-d-1"},"label":{"S":"L"}})).await;
    put_item(&app, "users", json!({"id":{"S":"tw-d-2"},"label":{"S":"L"}})).await;
    let body = json!({
        "TransactItems": [
            {"Delete":{"TableName":"users","Key":{"id":{"S":"tw-d-1"}}}},
            {"Delete":{"TableName":"users","Key":{"id":{"S":"tw-d-2"}}}}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // Both rows gone.
    for id in ["tw-d-1", "tw-d-2"] {
        let resp = app
            .clone()
            .oneshot(ddb_request(
                "GetItem",
                json!({"TableName":"users","Key":{"id":{"S":id}}}),
            ))
            .await
            .unwrap();
        let body = body_json(resp).await;
        assert!(body.get("Item").is_none(), "{id} not deleted");
    }
}

#[tokio::test]
async fn transact_write_delete_missing_row_is_noop() {
    let app = app();
    let body = json!({
        "TransactItems": [
            {"Delete":{"TableName":"users","Key":{"id":{"S":"never-existed"}}}}
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn transact_write_conditional_delete_succeeds() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"tw-cd-1"}})).await;
    let body = json!({
        "TransactItems": [{
            "Delete":{
                "TableName":"users",
                "Key":{"id":{"S":"tw-cd-1"}},
                "ConditionExpression":"attribute_exists(id)"
            }
        }]
    });
    let resp = app
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn transact_write_conditional_delete_fails_returns_cancelled() {
    let app = app();
    // No seed → attribute_exists fails.
    let body = json!({
        "TransactItems": [{
            "Delete":{
                "TableName":"users",
                "Key":{"id":{"S":"missing"}},
                "ConditionExpression":"attribute_exists(id)"
            }
        }]
    });
    let resp = app
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(
        body["CancellationReasons"][0]["Code"].as_str().unwrap(),
        "ConditionalCheckFailed"
    );
}

#[tokio::test]
async fn transact_write_condition_check_guards_sibling_put() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"tw-cc-guard"},"flag":{"S":"on"}})).await;

    // ConditionCheck "flag=on" should pass; sibling Put then lands.
    let body = json!({
        "TransactItems": [
            {"ConditionCheck":{
                "TableName":"users",
                "Key":{"id":{"S":"tw-cc-guard"}},
                "ConditionExpression":"flag = :v",
                "ExpressionAttributeValues":{":v":{"S":"on"}}
            }},
            {"Put":{"TableName":"users","Item":{"id":{"S":"tw-cc-side"},"label":{"S":"x"}}}}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // tw-cc-side should now exist.
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"tw-cc-side"}}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert!(body.get("Item").is_some());
}

#[tokio::test]
async fn transact_write_condition_check_failure_rolls_back_sibling_put() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"tw-cc-fail"},"flag":{"S":"off"}})).await;

    let body = json!({
        "TransactItems": [
            {"Put":{"TableName":"users","Item":{"id":{"S":"tw-cc-blocked"},"label":{"S":"x"}}}},
            {"ConditionCheck":{
                "TableName":"users",
                "Key":{"id":{"S":"tw-cc-fail"}},
                "ConditionExpression":"flag = :v",
                "ExpressionAttributeValues":{":v":{"S":"on"}}
            }}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(
        body["CancellationReasons"][1]["Code"].as_str().unwrap(),
        "ConditionalCheckFailed"
    );

    // Atomicity: tw-cc-blocked should NOT exist.
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"tw-cc-blocked"}}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert!(body.get("Item").is_none());
}

#[tokio::test]
async fn transact_write_mixed_put_delete_condition_check() {
    let app = app();
    put_item(
        &app,
        "users",
        json!({"id":{"S":"mix-guard"},"flag":{"S":"alive"}}),
    )
    .await;
    put_item(&app, "users", json!({"id":{"S":"mix-victim"},"label":{"S":"X"}})).await;
    let body = json!({
        "TransactItems": [
            {"ConditionCheck":{
                "TableName":"users",
                "Key":{"id":{"S":"mix-guard"}},
                "ConditionExpression":"flag = :v",
                "ExpressionAttributeValues":{":v":{"S":"alive"}}
            }},
            {"Put":{"TableName":"users","Item":{"id":{"S":"mix-new"},"label":{"S":"L"}}}},
            {"Delete":{"TableName":"users","Key":{"id":{"S":"mix-victim"}}}}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // mix-new exists, mix-victim deleted.
    let r1 = app
        .clone()
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"mix-new"}}}),
        ))
        .await
        .unwrap();
    assert!(body_json(r1).await.get("Item").is_some());
    let r2 = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"mix-victim"}}}),
        ))
        .await
        .unwrap();
    assert!(body_json(r2).await.get("Item").is_none());
}

#[tokio::test]
async fn transact_write_condition_check_without_expression_rejected() {
    // The struct requires ConditionExpression to be present (serde). An
    // empty string passes serde but the translator rejects.
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[
                {"ConditionCheck":{
                    "TableName":"users",
                    "Key":{"id":{"S":"x"}},
                    "ConditionExpression":""
                }}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ===== TransactWriteItems T5 — Update =======================================

#[tokio::test]
async fn transact_write_update_existing_row() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"upd-1"},"label":{"S":"old"}})).await;
    let body = json!({
        "TransactItems": [{
            "Update":{
                "TableName":"users",
                "Key":{"id":{"S":"upd-1"}},
                "UpdateExpression":"SET label = :v",
                "ExpressionAttributeValues":{":v":{"S":"new"}}
            }
        }]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"upd-1"}}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Item"]["label"]["S"], "new");
}

#[tokio::test]
async fn transact_write_update_creates_missing_row() {
    let app = app();
    let body = json!({
        "TransactItems": [{
            "Update":{
                "TableName":"users",
                "Key":{"id":{"S":"upd-new"}},
                "UpdateExpression":"SET label = :v",
                "ExpressionAttributeValues":{":v":{"S":"fresh"}}
            }
        }]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"upd-new"}}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Item"]["label"]["S"], "fresh");
}

#[tokio::test]
async fn transact_write_conditional_update_fails_rolls_back() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"upd-cf"},"label":{"S":"alive"}})).await;
    let body = json!({
        "TransactItems": [
            {"Put":{"TableName":"users","Item":{"id":{"S":"upd-side"},"label":{"S":"x"}}}},
            {"Update":{
                "TableName":"users",
                "Key":{"id":{"S":"upd-cf"}},
                "UpdateExpression":"SET label = :new",
                "ConditionExpression":"label = :old",
                "ExpressionAttributeValues":{
                    ":new":{"S":"updated"},
                    ":old":{"S":"different"}
                }
            }}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(
        body["CancellationReasons"][1]["Code"].as_str().unwrap(),
        "ConditionalCheckFailed"
    );

    // Atomicity: sibling Put didn't land.
    let resp = app
        .clone()
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"upd-side"}}}),
        ))
        .await
        .unwrap();
    assert!(body_json(resp).await.get("Item").is_none());

    // Original row label unchanged.
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"upd-cf"}}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Item"]["label"]["S"], "alive");
}

#[tokio::test]
async fn transact_write_update_add_numeric() {
    let app = app();
    // Use the counters table (N-PK) and ADD a numeric.
    let body = json!({
        "TransactItems": [{
            "Update":{
                "TableName":"counters",
                "Key":{"id":{"N":"100"}},
                "UpdateExpression":"ADD tally :n",
                "ExpressionAttributeValues":{":n":{"N":"7"}}
            }
        }]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"counters","Key":{"id":{"N":"100"}}}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    assert_eq!(body["Item"]["tally"]["N"], "7");
}

#[tokio::test]
async fn transact_write_update_empty_expression_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[
                {"Update":{
                    "TableName":"users",
                    "Key":{"id":{"S":"x"}},
                    "UpdateExpression":"SET"
                }}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn transact_write_update_touches_key_rejected() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[
                {"Update":{
                    "TableName":"users",
                    "Key":{"id":{"S":"x"}},
                    "UpdateExpression":"SET id = :v",
                    "ExpressionAttributeValues":{":v":{"S":"y"}}
                }}
            ]}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("key attribute"));
}

// ===== TransactWriteItems T6 — Multi-table + CancellationReasons polish ======

#[tokio::test]
async fn transact_write_multi_table_happy_path() {
    let app = app();
    let body = json!({
        "TransactItems": [
            {"Put":{"TableName":"users","Item":{"id":{"S":"mt-u"},"label":{"S":"U"}}}},
            {"Put":{"TableName":"device_events",
                    "Item":{"device_id":{"S":"mt-d"},"ts":{"N":"1"},"label":{"S":"E"}}}}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Both rows present.
    let r1 = app
        .clone()
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"mt-u"}}}),
        ))
        .await
        .unwrap();
    assert!(body_json(r1).await.get("Item").is_some());
    let r2 = app
        .oneshot(ddb_request(
            "GetItem",
            json!({
                "TableName":"device_events",
                "Key":{"device_id":{"S":"mt-d"},"ts":{"N":"1"}}
            }),
        ))
        .await
        .unwrap();
    assert!(body_json(r2).await.get("Item").is_some());
}

#[tokio::test]
async fn transact_write_multi_table_failure_rolls_back_across_tables() {
    let app = app();
    // Seed a guard row in `users` that exists.
    put_item(&app, "users", json!({"id":{"S":"mt-x-guard"}})).await;
    // Tx puts to device_events, then ConditionChecks against a missing
    // row in users — that fails, so the device_events Put must roll back.
    let body = json!({
        "TransactItems": [
            {"Put":{"TableName":"device_events",
                    "Item":{"device_id":{"S":"mt-x-blocked"},"ts":{"N":"1"}}}},
            {"ConditionCheck":{
                "TableName":"users",
                "Key":{"id":{"S":"mt-x-missing"}},
                "ConditionExpression":"attribute_exists(id)"
            }}
        ]
    });
    let resp = app
        .clone()
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    // CancellationReasons positionally indexed (D9).
    assert_eq!(body["CancellationReasons"][0]["Code"], "None");
    assert_eq!(body["CancellationReasons"][1]["Code"], "ConditionalCheckFailed");

    // Atomicity across tables: device_events Put rolled back.
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({
                "TableName":"device_events",
                "Key":{"device_id":{"S":"mt-x-blocked"},"ts":{"N":"1"}}
            }),
        ))
        .await
        .unwrap();
    assert!(body_json(resp).await.get("Item").is_none());
}

#[tokio::test]
async fn transact_write_same_key_across_tables_allowed() {
    // D6 dedupe is keyed on (table, pk, sk) — same key in different
    // tables is fine.
    let app = app();
    let body = json!({
        "TransactItems": [
            {"Put":{"TableName":"users","Item":{"id":{"S":"shared"}}}},
            {"Put":{"TableName":"device_events",
                    "Item":{"device_id":{"S":"shared"},"ts":{"N":"1"}}}}
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Pin the full DDB-shape of TransactionCanceledException. The
/// wire format is non-obvious: __type at the top, capital-M
/// `Message` (not lowercase `message` as the standard ApiError
/// uses), parallel `CancellationReasons` array. SDK retry code
/// reads `CancellationReasons[i].Code` directly.
#[tokio::test]
async fn transact_write_cancellation_wire_shape() {
    let app = app();
    put_item(&app, "users", json!({"id":{"S":"shape-row"}})).await;
    let body = json!({
        "TransactItems": [
            {"Put":{"TableName":"users","Item":{"id":{"S":"shape-1"}}}},
            {"Put":{"TableName":"users","Item":{"id":{"S":"shape-row"}},
                    "ConditionExpression":"attribute_not_exists(id)"}},
            {"Put":{"TableName":"users","Item":{"id":{"S":"shape-3"}}}}
        ]
    });
    let resp = app
        .oneshot(ddb_request("TransactWriteItems", body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#TransactionCanceledException"));
    assert!(body["Message"]
        .as_str()
        .unwrap()
        .starts_with("Transaction cancelled"));
    let reasons = body["CancellationReasons"].as_array().unwrap();
    assert_eq!(reasons.len(), 3);
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[1]["Code"], "ConditionalCheckFailed");
    assert_eq!(reasons[1]["Message"], "The conditional request failed");
    assert_eq!(reasons[2]["Code"], "None");
}

#[tokio::test]
async fn transact_write_too_many_items_rejected() {
    let app = app();
    let many: Vec<Value> = (0..101)
        .map(|n| json!({
            "Put":{"TableName":"users","Item":{"id":{"S":format!("toomany-{n}")}}}
        }))
        .collect();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems": many}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ValidationException"));
}

#[tokio::test]
async fn transact_write_at_cap_succeeds() {
    let app = app();
    let many: Vec<Value> = (0..100)
        .map(|n| json!({
            "Put":{"TableName":"users","Item":{"id":{"S":format!("atcap-{n}")}}}
        }))
        .collect();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems": many}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===== D4: serveable-gate (PLAN-10 KD5) ======================================
//
// An unserveable table — drift detected by the reconciler, or DDL in
// flight — must surface to clients as `ResourceNotFoundException` (not
// a bespoke variant), with the unserveable reason folded into the
// response message. This holds for every op type, and for batch /
// transact ops it fails the whole request if ANY referenced table is
// unserveable.

async fn expect_rnf_with_reason(resp: axum::response::Response, table_name: &str) -> Value {
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .ends_with("#ResourceNotFoundException"),
        "expected RNF, got {body}"
    );
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.contains("not currently serveable") && msg.contains(table_name),
        "message should name the unserveable table + signal the reason: {msg}"
    );
    body
}

#[tokio::test]
async fn put_item_rnf_when_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"users","Item":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

#[tokio::test]
async fn get_item_rnf_when_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

#[tokio::test]
async fn delete_item_rnf_when_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "DeleteItem",
            json!({"TableName":"users","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

#[tokio::test]
async fn update_item_rnf_when_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "UpdateItem",
            json!({
                "TableName":"users",
                "Key":{"id":{"S":"u1"}},
                "UpdateExpression":"SET #v = :x",
                "ExpressionAttributeNames":{"#v":"label"},
                "ExpressionAttributeValues":{":x":{"S":"y"}}
            }),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

#[tokio::test]
async fn query_rnf_when_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "Query",
            json!({
                "TableName":"users",
                "KeyConditionExpression":"id = :v",
                "ExpressionAttributeValues":{":v":{"S":"u1"}}
            }),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

#[tokio::test]
async fn scan_rnf_when_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "Scan",
            json!({"TableName":"users"}),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

/// BatchGetItem: any unserveable referenced table fails the whole
/// request — KD5. The well-shaped (serveable) `messages` table on its
/// own would succeed; mixing in `users` poisons the batch.
#[tokio::test]
async fn batch_get_rnf_if_any_referenced_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "BatchGetItem",
            json!({"RequestItems":{
                "users":{"Keys":[{"id":{"S":"u1"}}]},
                "messages":{"Keys":[{"thread":{"S":"t1"},"ts":{"S":"1"}}]}
            }}),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

#[tokio::test]
async fn batch_write_rnf_if_any_referenced_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "BatchWriteItem",
            json!({"RequestItems":{
                "users":[{"PutRequest":{"Item":{"id":{"S":"u1"}}}}],
                "messages":[{"PutRequest":{"Item":{"thread":{"S":"t1"},"ts":{"S":"1"}}}}]
            }}),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

#[tokio::test]
async fn transact_get_rnf_if_any_referenced_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "TransactGetItems",
            json!({"TransactItems":[
                {"Get":{"TableName":"messages","Key":{"thread":{"S":"t1"},"ts":{"S":"1"}}}},
                {"Get":{"TableName":"users","Key":{"id":{"S":"u1"}}}}
            ]}),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

#[tokio::test]
async fn transact_write_rnf_if_any_referenced_table_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "TransactWriteItems",
            json!({"TransactItems":[
                {"Put":{"TableName":"messages","Item":{"thread":{"S":"t1"},"ts":{"S":"1"}}}},
                {"Delete":{"TableName":"users","Key":{"id":{"S":"u1"}}}}
            ]}),
        ))
        .await
        .unwrap();
    expect_rnf_with_reason(resp, "users").await;
}

/// Serveable sibling tables still work when one is degraded — drift on
/// table X doesn't block ops on table Y (PLAN-10 motivation #2).
#[tokio::test]
async fn other_tables_still_serve_when_one_is_unserveable() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "PutItem",
            json!({"TableName":"messages","Item":{"thread":{"S":"t1"},"ts":{"S":"1"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Unknown tables already returned RNF; the gate keeps that contract.
/// The wire shape stays identical so SDKs that retry on RNF behave
/// the same for both unknown and unserveable.
#[tokio::test]
async fn unknown_table_still_returns_rnf_not_unserveable_message() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "GetItem",
            json!({"TableName":"nope","Key":{"id":{"S":"u1"}}}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ResourceNotFoundException"));
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("Table not found"));
}

// ===== D5: DescribeTable + ListTables =======================================

#[tokio::test]
async fn describe_table_returns_arn_status_keyschema() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "DescribeTable",
            json!({"TableName":"users"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let table = &body["Table"];
    assert_eq!(table["TableName"], "users");
    assert_eq!(table["TableStatus"], "ACTIVE");
    assert_eq!(
        table["TableArn"],
        "arn:aws:dynamodb:::rektifier-table/users"
    );
    let ks = table["KeySchema"].as_array().unwrap();
    assert_eq!(ks.len(), 1);
    assert_eq!(ks[0]["AttributeName"], "id");
    assert_eq!(ks[0]["KeyType"], "HASH");
}

/// Composite-key table — KeySchema carries both HASH + RANGE in that
/// order; AttributeDefinitions covers both attributes.
#[tokio::test]
async fn describe_table_composite_key_round_trip() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "DescribeTable",
            json!({"TableName":"device_events"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let table = &body["Table"];
    let ks = table["KeySchema"].as_array().unwrap();
    assert_eq!(ks.len(), 2);
    assert_eq!(ks[0]["KeyType"], "HASH");
    assert_eq!(ks[1]["AttributeName"], "ts");
    assert_eq!(ks[1]["KeyType"], "RANGE");
    let ad = table["AttributeDefinitions"].as_array().unwrap();
    assert_eq!(ad.len(), 2);
    assert!(ad
        .iter()
        .any(|d| d["AttributeName"] == "ts" && d["AttributeType"] == "N"));
}

/// PLAN-10 KD5: DescribeTable on an unserveable table still returns
/// the description (operators need it to diagnose). Wire status hides
/// the internal failure per KD6.
#[tokio::test]
async fn describe_table_works_on_unserveable_table() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request(
            "DescribeTable",
            json!({"TableName":"users"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let table = &body["Table"];
    // The catalog row carries status=Degraded; KD6 collapses that to ACTIVE.
    assert_eq!(table["TableStatus"], "ACTIVE");
    assert_eq!(table["TableName"], "users");
}

/// Unknown table → RNF (no serveable mention, table truly doesn't exist).
#[tokio::test]
async fn describe_table_unknown_returns_rnf() {
    let app = app();
    let resp = app
        .oneshot(ddb_request(
            "DescribeTable",
            json!({"TableName":"nope"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(body["__type"]
        .as_str()
        .unwrap()
        .ends_with("#ResourceNotFoundException"));
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("Table not found"));
}

#[tokio::test]
async fn list_tables_returns_sorted_names() {
    let app = app();
    let resp = app
        .oneshot(ddb_request("ListTables", json!({})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let names: Vec<&str> = body["TableNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    // Test fixture has six tables; verify sort.
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "ListTables must return ASC-sorted names");
    assert!(body.get("LastEvaluatedTableName").is_none());
}

/// Pagination: Limit=2 returns first two names + LastEvaluatedTableName
/// equal to the last returned name. Resumed page starts strictly after.
#[tokio::test]
async fn list_tables_pagination() {
    let app1 = app();
    let resp = app1
        .oneshot(ddb_request("ListTables", json!({"Limit": 2})))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let names: Vec<String> = body["TableNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(names.len(), 2);
    let last = body["LastEvaluatedTableName"].as_str().unwrap();
    assert_eq!(last, names.last().unwrap());

    // Resume strictly after the cursor.
    let app2 = app();
    let resp = app2
        .oneshot(ddb_request(
            "ListTables",
            json!({"ExclusiveStartTableName": last, "Limit": 2}),
        ))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let next_names: Vec<&str> = body["TableNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(next_names.iter().all(|n| *n > last));
}

/// Limit=0 and Limit>100 both rejected per DDB's 1..=100 bound.
#[tokio::test]
async fn list_tables_invalid_limit_rejected() {
    let app1 = app();
    let resp = app1
        .oneshot(ddb_request("ListTables", json!({"Limit": 0})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let app2 = app();
    let resp = app2
        .oneshot(ddb_request("ListTables", json!({"Limit": 101})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// ListTables includes unserveable tables — same as DDB, which doesn't
/// filter on internal health. Operators see degraded tables in the
/// listing and can DescribeTable / read `_rektifier_tables` to diagnose.
#[tokio::test]
async fn list_tables_includes_unserveable_entries() {
    let app = app_users_unserveable();
    let resp = app
        .oneshot(ddb_request("ListTables", json!({})))
        .await
        .unwrap();
    let body = body_json(resp).await;
    let names: Vec<&str> = body["TableNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(names.contains(&"users"), "unserveable `users` must appear");
}
