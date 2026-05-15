//! The four public `translate_*_item` entry points + shared helpers
//! for resolving `ReturnValues` and parsing `ConditionExpression`.

use crate::checks::{
    check_add_value_type, check_condition_paths_top_level, check_delete_value_type, check_not_key,
    check_set_rhs_paths_top_level, check_top_level,
};
use crate::classify::classify_condition;
use crate::error::{map_substitute_for_condition, TranslateError};
use crate::key_condition::extract_key_condition;
use crate::keys::{extract_key_pair, reject_extra_key_attrs};
use crate::paging::{decode_esk, decode_scan_esk, validate_limit};
use crate::projection::resolve_select_and_projection;
use crate::plan::{
    BatchGetItemPlan, BatchGetPerTable, BatchWriteItemPlan, BatchWritePerTable, ConditionPlan,
    DeleteItemPlan, GetItemPlan, PutItemPlan, QueryPlan, ReturnValuesMode, ScanPlan,
    TransactGetItemsPlan, TransactGetPlanItem, TransactWriteItemsPlan, TransactWriteKind,
    TransactWritePlanItem, UpdateItemPlan,
};
use crate::schema::TableSchema;
use rekt_expressions::{
    parse_condition_expression, parse_update_expression, substitute_condition, substitute_update,
};
use rekt_protocol::{
    AttributeValue, BatchGetItemRequest, BatchWriteItemRequest, DeleteItemRequest, GetItemRequest,
    PutItemRequest, QueryRequest, ScanRequest, TransactGetItemsRequest, TransactWriteItemsRequest,
    UpdateItemRequest,
};
use rekt_storage::{KeyValue, WriteOp};
use std::collections::{BTreeMap, HashMap, HashSet};

#[tracing::instrument(level = "debug", skip_all, name = "translate.put_item", fields(table = %schema.name))]
pub fn translate_put_item(
    req: &PutItemRequest,
    schema: &TableSchema,
) -> Result<PutItemPlan, TranslateError> {
    // 1. Validate key attributes; capture pk/sk for the conditional path.
    //    The unconditional fast path ignores them (PG derives via
    //    GENERATED ALWAYS AS), but the conditional / ALL_OLD paths need
    //    them to look up the pre-update row.
    let (pk, sk) = extract_key_pair(&req.item, schema)?;

    // 2. Resolve ReturnValues. PutItem accepts only NONE and ALL_OLD.
    let return_values = resolve_put_delete_return_values(req.return_values.as_deref(), "PutItem")?;

    // 3. Parse + validate + classify the ConditionExpression, if any.
    let empty_names: BTreeMap<String, String> = BTreeMap::new();
    let empty_values: BTreeMap<String, AttributeValue> = BTreeMap::new();
    let names = req
        .expression_attribute_names
        .as_ref()
        .unwrap_or(&empty_names);
    let values = req
        .expression_attribute_values
        .as_ref()
        .unwrap_or(&empty_values);
    let condition = translate_condition(req.condition_expression.as_deref(), names, values, schema)?;

    let item_json =
        serde_json::to_value(&req.item).expect("AttributeValue Serialize is infallible");
    Ok(PutItemPlan {
        pk,
        sk,
        item_json,
        condition,
        return_values,
    })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.get_item", fields(table = %schema.name))]
pub fn translate_get_item(
    req: &GetItemRequest,
    schema: &TableSchema,
) -> Result<GetItemPlan, TranslateError> {
    let (pk, sk) = extract_key_pair(&req.key, schema)?;
    reject_extra_key_attrs(&req.key, schema)?;
    Ok(GetItemPlan { pk, sk })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.delete_item", fields(table = %schema.name))]
pub fn translate_delete_item(
    req: &DeleteItemRequest,
    schema: &TableSchema,
) -> Result<DeleteItemPlan, TranslateError> {
    let (pk, sk) = extract_key_pair(&req.key, schema)?;
    reject_extra_key_attrs(&req.key, schema)?;

    let return_values =
        resolve_put_delete_return_values(req.return_values.as_deref(), "DeleteItem")?;

    let empty_names: BTreeMap<String, String> = BTreeMap::new();
    let empty_values: BTreeMap<String, AttributeValue> = BTreeMap::new();
    let names = req
        .expression_attribute_names
        .as_ref()
        .unwrap_or(&empty_names);
    let values = req
        .expression_attribute_values
        .as_ref()
        .unwrap_or(&empty_values);
    let condition = translate_condition(req.condition_expression.as_deref(), names, values, schema)?;

    Ok(DeleteItemPlan {
        pk,
        sk,
        condition,
        return_values,
    })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.update_item", fields(table = %schema.name))]
pub fn translate_update_item(
    req: &UpdateItemRequest,
    schema: &TableSchema,
) -> Result<UpdateItemPlan, TranslateError> {
    // 1. Resolve ReturnValues. All five DDB variants are supported.
    let return_values = match req.return_values.as_deref() {
        None | Some("NONE") => ReturnValuesMode::None,
        Some("ALL_NEW") => ReturnValuesMode::AllNew,
        Some("ALL_OLD") => ReturnValuesMode::AllOld,
        Some("UPDATED_NEW") => ReturnValuesMode::UpdatedNew,
        Some("UPDATED_OLD") => ReturnValuesMode::UpdatedOld,
        Some(other) => {
            return Err(TranslateError::UnsupportedReturnValues {
                got: other.to_string(),
            });
        }
    };

    // 2. Validate the Key against the schema (same machinery as GetItem /
    //    DeleteItem). Caller must supply pk (and sk if composite); no
    //    foreign attributes in Key.
    let (pk, sk) = extract_key_pair(&req.key, schema)?;
    reject_extra_key_attrs(&req.key, schema)?;

    // 3. Parse + resolve the UpdateExpression. ExpressionAttributeNames /
    //    ExpressionAttributeValues are optional in the request.
    let raw = parse_update_expression(&req.update_expression)?;
    let empty_names: BTreeMap<String, String> = BTreeMap::new();
    let empty_values: BTreeMap<String, AttributeValue> = BTreeMap::new();
    let names = req
        .expression_attribute_names
        .as_ref()
        .unwrap_or(&empty_names);
    let values = req
        .expression_attribute_values
        .as_ref()
        .unwrap_or(&empty_values);
    let expression = substitute_update(raw, names, values)?;

    // 4. Reject empty expressions.
    if expression.set.is_empty()
        && expression.remove.is_empty()
        && expression.add.is_empty()
        && expression.delete.is_empty()
    {
        return Err(TranslateError::EmptyUpdateExpression);
    }

    // 5. Per-clause validation. Top-level paths only across all clauses
    //    (Phase 8 lifts); key attrs disallowed as the LHS of any write
    //    (SET / REMOVE / ADD / DELETE). RHS embedded paths can read key
    //    attrs (`SET a = id` is permitted).
    //
    // Fast path requires `is_simple()` (top-level SET = literal + REMOVE
    // only); non-simple SET RHS, ADD, DELETE, and slow-path-bound
    // conditions all route through the Phase 3b read-modify-write path.
    for clause in &expression.set {
        check_top_level(&clause.path)?;
        check_not_key(&clause.path, schema)?;
        check_set_rhs_paths_top_level(&clause.value)?;
    }
    for path in &expression.remove {
        check_top_level(path)?;
        check_not_key(path, schema)?;
    }
    for action in &expression.add {
        check_top_level(&action.path)?;
        check_not_key(&action.path, schema)?;
        check_add_value_type(&action.value)?;
    }
    for action in &expression.delete {
        check_top_level(&action.path)?;
        check_not_key(&action.path, schema)?;
        check_delete_value_type(&action.value)?;
    }

    // 6. Parse + validate + classify the ConditionExpression, if any.
    //    Unlike SET / REMOVE paths, condition paths *may* reference key
    //    attributes (that's how `attribute_not_exists(pk)` works).
    let condition = translate_condition(req.condition_expression.as_deref(), names, values, schema)?;

    Ok(UpdateItemPlan {
        pk,
        sk,
        expression,
        condition,
        return_values,
    })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.query", fields(table = %schema.name))]
pub fn translate_query(
    req: &QueryRequest,
    schema: &TableSchema,
) -> Result<QueryPlan, TranslateError> {
    if req.key_condition_expression.trim().is_empty() {
        return Err(TranslateError::KeyConditionMissingPkEquality {
            attr: schema.pk_attr.clone(),
        });
    }

    let raw = parse_condition_expression(&req.key_condition_expression)?;
    let empty_names: BTreeMap<String, String> = BTreeMap::new();
    let empty_values: BTreeMap<String, AttributeValue> = BTreeMap::new();
    let names = req
        .expression_attribute_names
        .as_ref()
        .unwrap_or(&empty_names);
    let values = req
        .expression_attribute_values
        .as_ref()
        .unwrap_or(&empty_values);
    let cond = substitute_condition(raw, names, values).map_err(map_substitute_for_condition)?;
    let bounds = extract_key_condition(&cond, schema)?;

    let limit = req.limit.map(validate_limit).transpose()?;

    let esk_sk = match &req.exclusive_start_key {
        None => None,
        Some(esk) => decode_esk(esk, schema, &bounds.pk)?,
    };

    // FilterExpression: same parse/substitute/classify pipeline as
    // Put/Delete/Update conditions. The classifier still runs (so
    // future SQL push-down can branch on routing) but Q3 evaluates
    // every filter shape in Rust — see PLAN-4 D2.
    let filter = translate_condition(
        req.filter_expression.as_deref(),
        names,
        values,
        schema,
    )?;

    // ScanIndexForward defaults to true (DDB default). Q5 honors
    // false → DESC sort + flipped ESK comparison.
    let forward = req.scan_index_forward.unwrap_or(true);

    let (select_count_only, projection) = resolve_select_and_projection(
        req.select.as_deref(),
        req.projection_expression.as_deref(),
        names,
    )?;

    Ok(QueryPlan {
        pk: bounds.pk,
        sk_condition: bounds.sk_condition,
        limit,
        esk_sk,
        filter,
        forward,
        select_count_only,
        projection,
    })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.scan", fields(table = %schema.name))]
pub fn translate_scan(
    req: &ScanRequest,
    schema: &TableSchema,
) -> Result<ScanPlan, TranslateError> {
    // Reject features deferred per PLAN-4 (D4 / D5 / Q7).
    if req.index_name.is_some() {
        return Err(TranslateError::ScanFeatureNotSupported {
            what: "IndexName (GSI/LSI scan)",
        });
    }
    if req.segment.is_some() || req.total_segments.is_some() {
        return Err(TranslateError::ScanFeatureNotSupported {
            what: "Segment / TotalSegments (parallel scan)",
        });
    }

    let limit = req.limit.map(validate_limit).transpose()?;

    let (esk_pk, esk_sk) = match &req.exclusive_start_key {
        None => (None, None),
        Some(esk) => {
            let (pk, sk) = decode_scan_esk(esk, schema)?;
            (Some(pk), sk)
        }
    };

    let empty_names: BTreeMap<String, String> = BTreeMap::new();
    let empty_values: BTreeMap<String, AttributeValue> = BTreeMap::new();
    let names = req
        .expression_attribute_names
        .as_ref()
        .unwrap_or(&empty_names);
    let values = req
        .expression_attribute_values
        .as_ref()
        .unwrap_or(&empty_values);
    let filter = translate_condition(
        req.filter_expression.as_deref(),
        names,
        values,
        schema,
    )?;

    let (select_count_only, projection) = resolve_select_and_projection(
        req.select.as_deref(),
        req.projection_expression.as_deref(),
        names,
    )?;

    Ok(ScanPlan {
        limit,
        esk_pk,
        esk_sk,
        filter,
        select_count_only,
        projection,
    })
}

/// BatchGetItem — per-table key validation + cross-table cap enforcement.
///
/// Unlike the single-row entry points, this function does its own
/// schema lookup (the request can name many tables, so the server
/// can't pre-resolve "the" schema). Missing tables surface as
/// `ResourceNotFoundForBatch` — same wire-shape as DDB's per-batch
/// `ResourceNotFoundException`.
///
/// `max_keys` is the configured per-call cap (DDB default 100); the
/// translator enforces the sum across all tables, not per-table.
#[tracing::instrument(level = "debug", skip_all, name = "translate.batch_get_item")]
pub fn translate_batch_get_item(
    req: &BatchGetItemRequest,
    schemas: &HashMap<String, TableSchema>,
    max_keys: u32,
) -> Result<BatchGetItemPlan, TranslateError> {
    if req.request_items.is_empty() {
        return Err(TranslateError::EmptyBatchGetRequest);
    }

    // Sum across tables first so a request like {"t1": 80, "t2": 30}
    // is rejected before any per-key validation cost.
    let total: usize = req.request_items.values().map(|v| v.keys.len()).sum();
    if total > max_keys as usize {
        return Err(TranslateError::TooManyKeysInBatchGet {
            got: total as u32,
            max: max_keys,
        });
    }

    let mut per_table: Vec<BatchGetPerTable> = Vec::with_capacity(req.request_items.len());
    for (table_name, ka) in &req.request_items {
        let schema = schemas
            .get(table_name)
            .ok_or_else(|| TranslateError::ResourceNotFoundForBatch {
                table: table_name.clone(),
            })?;

        if ka.attributes_to_get.is_some() {
            return Err(TranslateError::AttributesToGetNotSupported);
        }
        if ka.keys.is_empty() {
            return Err(TranslateError::EmptyKeysInBatchGet {
                table: table_name.clone(),
            });
        }

        // B3: ProjectionExpression is parsed via the same helper Query
        // / Scan use; ConsistentRead is carried through for the
        // dispatcher's per-table tracing field but has no behavioral
        // effect (D7).
        let empty_names: BTreeMap<String, String> = BTreeMap::new();
        let names = ka.expression_attribute_names.as_ref().unwrap_or(&empty_names);
        let projection =
            crate::projection::parse_projection(ka.projection_expression.as_deref(), names)?;
        let consistent_read = ka.consistent_read.unwrap_or(false);

        let mut keys: Vec<(KeyValue, Option<KeyValue>)> = Vec::with_capacity(ka.keys.len());
        let mut seen: HashSet<(KeyValue, Option<KeyValue>)> =
            HashSet::with_capacity(ka.keys.len());
        for key_item in &ka.keys {
            let tuple = extract_key_pair(key_item, schema)?;
            reject_extra_key_attrs(key_item, schema)?;
            if !seen.insert(tuple.clone()) {
                return Err(TranslateError::DuplicateKeyInBatch);
            }
            keys.push(tuple);
        }
        per_table.push(BatchGetPerTable {
            table_name: table_name.clone(),
            keys,
            projection,
            consistent_read,
        });
    }

    Ok(BatchGetItemPlan { per_table })
}

/// BatchWriteItem — per-table key validation + cross-table cap enforcement.
///
/// Mirrors `translate_batch_get_item`'s shape: per-table schema lookup,
/// per-key/per-item type validation, HashSet dedupe across the whole
/// table's writes (Put + Delete combined — DDB rejects "same key in
/// one request" regardless of op kind), and a total-write cap (DDB
/// default 25; operator-tunable).
///
/// `WriteOp::Put` carries the full DDB-JSON item; the translator
/// validates the item contains the schema's key attrs at the right
/// types, then the storage layer's `INSERT ... ON CONFLICT DO UPDATE`
/// uses the generated key columns to upsert.
///
/// `WriteOp::Delete` carries the typed key tuple only.
#[tracing::instrument(level = "debug", skip_all, name = "translate.batch_write_item")]
pub fn translate_batch_write_item(
    req: &BatchWriteItemRequest,
    schemas: &HashMap<String, TableSchema>,
    max_writes: u32,
) -> Result<BatchWriteItemPlan, TranslateError> {
    if req.request_items.is_empty() {
        return Err(TranslateError::EmptyBatchWriteRequest);
    }
    let total: usize = req.request_items.values().map(|v| v.len()).sum();
    if total > max_writes as usize {
        return Err(TranslateError::TooManyWritesInBatch {
            got: total as u32,
            max: max_writes,
        });
    }

    let mut per_table: Vec<BatchWritePerTable> = Vec::with_capacity(req.request_items.len());
    for (table_name, writes) in &req.request_items {
        let schema = schemas
            .get(table_name)
            .ok_or_else(|| TranslateError::ResourceNotFoundForBatch {
                table: table_name.clone(),
            })?;
        if writes.is_empty() {
            return Err(TranslateError::EmptyWritesInBatch {
                table: table_name.clone(),
            });
        }

        let mut ops: Vec<WriteOp> = Vec::with_capacity(writes.len());
        let mut seen: HashSet<(KeyValue, Option<KeyValue>)> = HashSet::with_capacity(writes.len());
        for w in writes {
            // Exactly-one-of: Put XOR Delete.
            match (&w.put_request, &w.delete_request) {
                (Some(p), None) => {
                    // Validate the Item contains the right key attrs at
                    // the right types — same machinery PutItem uses.
                    let (pk, sk) = extract_key_pair(&p.item, schema)?;
                    let tuple = (pk.clone(), sk.clone());
                    if !seen.insert(tuple) {
                        return Err(TranslateError::DuplicateKeyInBatch);
                    }
                    let item_json = serde_json::to_value(&p.item)
                        .expect("AttributeValue Serialize is infallible");
                    ops.push(WriteOp::Put {
                        pk,
                        sk,
                        item: item_json,
                    });
                }
                (None, Some(d)) => {
                    let (pk, sk) = extract_key_pair(&d.key, schema)?;
                    reject_extra_key_attrs(&d.key, schema)?;
                    let tuple = (pk.clone(), sk.clone());
                    if !seen.insert(tuple) {
                        return Err(TranslateError::DuplicateKeyInBatch);
                    }
                    ops.push(WriteOp::Delete { pk, sk });
                }
                _ => return Err(TranslateError::MalformedWriteRequest),
            }
        }
        per_table.push(BatchWritePerTable {
            table_name: table_name.clone(),
            ops,
        });
    }

    Ok(BatchWriteItemPlan { per_table })
}

/// TransactGetItems — atomic-snapshot multi-key read across one or more
/// tables. Position-preserving plan (D8 in PLAN-8): the returned
/// `items[i]` corresponds to the request's `transact_items[i]`. Each
/// item carries its own `TableName` + `Key`; duplicate `(table, key)`
/// pairs across items are rejected (D6).
///
/// T1 ignores `ProjectionExpression`; T2 wires it through.
#[tracing::instrument(level = "debug", skip_all, name = "translate.transact_get_items")]
pub fn translate_transact_get_items(
    req: &TransactGetItemsRequest,
    schemas: &HashMap<String, TableSchema>,
    max_items: u32,
) -> Result<TransactGetItemsPlan, TranslateError> {
    if req.transact_items.is_empty() {
        return Err(TranslateError::EmptyTransactRequest);
    }
    let total = req.transact_items.len();
    if total > max_items as usize {
        return Err(TranslateError::TooManyTransactItems {
            got: total as u32,
            max: max_items,
        });
    }

    let mut items: Vec<TransactGetPlanItem> = Vec::with_capacity(total);
    // Same-target dedupe across the whole request (cross-table is fine —
    // dedupe key includes table name). D6.
    let mut seen: HashSet<(String, KeyValue, Option<KeyValue>)> = HashSet::with_capacity(total);

    for ti in &req.transact_items {
        let get = ti.get.as_ref().ok_or(TranslateError::MalformedTransactItem {
            expected: "Get",
        })?;
        let schema = schemas
            .get(&get.table_name)
            .ok_or_else(|| TranslateError::ResourceNotFoundForTransact {
                table: get.table_name.clone(),
            })?;
        let (pk, sk) = extract_key_pair(&get.key, schema)?;
        reject_extra_key_attrs(&get.key, schema)?;
        let target = (get.table_name.clone(), pk.clone(), sk.clone());
        if !seen.insert(target) {
            return Err(TranslateError::DuplicateTransactTarget);
        }

        // T2: ProjectionExpression. Reuse the Query/Scan helper so a
        // future nested-path lift propagates here too. v1 = top-level
        // names only.
        let empty_names: BTreeMap<String, String> = BTreeMap::new();
        let names = get.expression_attribute_names.as_ref().unwrap_or(&empty_names);
        let projection =
            crate::projection::parse_projection(get.projection_expression.as_deref(), names)?;

        items.push(TransactGetPlanItem {
            table_name: get.table_name.clone(),
            pk,
            sk,
            projection,
        });
    }

    Ok(TransactGetItemsPlan { items })
}

/// TransactWriteItems — atomic cross-table write. T3 implements
/// `Put` only; T4 lifts Delete + ConditionCheck; T5 lifts Update.
///
/// Position-preserving (D8): the returned `items[i]` corresponds to
/// the request's `transact_items[i]`. Same-target dedupe across the
/// whole request keyed by `(table, pk, sk)` per D6.
///
/// `ClientRequestToken` is validated (≤36 bytes) and otherwise
/// discarded — v1 has no idempotency store (D10; T8 implements).
#[tracing::instrument(level = "debug", skip_all, name = "translate.transact_write_items")]
pub fn translate_transact_write_items(
    req: &TransactWriteItemsRequest,
    schemas: &HashMap<String, TableSchema>,
    max_items: u32,
) -> Result<TransactWriteItemsPlan, TranslateError> {
    if req.transact_items.is_empty() {
        return Err(TranslateError::EmptyTransactRequest);
    }
    let total = req.transact_items.len();
    if total > max_items as usize {
        return Err(TranslateError::TooManyTransactItems {
            got: total as u32,
            max: max_items,
        });
    }
    // ClientRequestToken length cap. DDB rejects > 36 chars with
    // ValidationException; we match.
    if let Some(t) = req.client_request_token.as_deref() {
        if t.len() > 36 {
            return Err(TranslateError::InvalidConditionExpression(
                "ClientRequestToken must be <= 36 chars".into(),
            ));
        }
    }

    let mut items: Vec<TransactWritePlanItem> = Vec::with_capacity(total);
    let mut seen: HashSet<(String, KeyValue, Option<KeyValue>)> = HashSet::with_capacity(total);

    for ti in &req.transact_items {
        // Exactly-one rule: precisely one of Put/Delete/Update/ConditionCheck.
        let set_count = [
            ti.put.is_some(),
            ti.delete.is_some(),
            ti.update.is_some(),
            ti.condition_check.is_some(),
        ]
        .iter()
        .filter(|x| **x)
        .count();
        if set_count != 1 {
            return Err(TranslateError::MalformedTransactItem {
                expected: "Put | Delete | Update | ConditionCheck",
            });
        }

        let empty_names: BTreeMap<String, String> = BTreeMap::new();
        let empty_values: BTreeMap<String, AttributeValue> = BTreeMap::new();
        if let Some(put) = ti.put.as_ref() {
            let schema = schemas.get(&put.table_name).ok_or_else(|| {
                TranslateError::ResourceNotFoundForTransact {
                    table: put.table_name.clone(),
                }
            })?;
            let (pk, sk) = extract_key_pair(&put.item, schema)?;
            let target = (put.table_name.clone(), pk.clone(), sk.clone());
            if !seen.insert(target) {
                return Err(TranslateError::DuplicateTransactTarget);
            }
            let names = put.expression_attribute_names.as_ref().unwrap_or(&empty_names);
            let values = put
                .expression_attribute_values
                .as_ref()
                .unwrap_or(&empty_values);
            let condition = translate_condition(
                put.condition_expression.as_deref(),
                names,
                values,
                schema,
            )?;
            let return_old_on_failure = parse_return_old_on_failure(
                put.return_values_on_condition_check_failure.as_deref(),
            )?;
            let item_json = serde_json::to_value(&put.item)
                .expect("AttributeValue Serialize is infallible");
            items.push(TransactWritePlanItem {
                table_name: put.table_name.clone(),
                pk,
                sk,
                kind: TransactWriteKind::Put { item_json },
                condition,
                return_old_on_failure,
            });
        } else if let Some(del) = ti.delete.as_ref() {
            let schema = schemas.get(&del.table_name).ok_or_else(|| {
                TranslateError::ResourceNotFoundForTransact {
                    table: del.table_name.clone(),
                }
            })?;
            let (pk, sk) = extract_key_pair(&del.key, schema)?;
            reject_extra_key_attrs(&del.key, schema)?;
            let target = (del.table_name.clone(), pk.clone(), sk.clone());
            if !seen.insert(target) {
                return Err(TranslateError::DuplicateTransactTarget);
            }
            let names = del.expression_attribute_names.as_ref().unwrap_or(&empty_names);
            let values = del
                .expression_attribute_values
                .as_ref()
                .unwrap_or(&empty_values);
            let condition = translate_condition(
                del.condition_expression.as_deref(),
                names,
                values,
                schema,
            )?;
            let return_old_on_failure = parse_return_old_on_failure(
                del.return_values_on_condition_check_failure.as_deref(),
            )?;
            items.push(TransactWritePlanItem {
                table_name: del.table_name.clone(),
                pk,
                sk,
                kind: TransactWriteKind::Delete,
                condition,
                return_old_on_failure,
            });
        } else if let Some(cc) = ti.condition_check.as_ref() {
            let schema = schemas.get(&cc.table_name).ok_or_else(|| {
                TranslateError::ResourceNotFoundForTransact {
                    table: cc.table_name.clone(),
                }
            })?;
            let (pk, sk) = extract_key_pair(&cc.key, schema)?;
            reject_extra_key_attrs(&cc.key, schema)?;
            let target = (cc.table_name.clone(), pk.clone(), sk.clone());
            if !seen.insert(target) {
                return Err(TranslateError::DuplicateTransactTarget);
            }
            let names = cc.expression_attribute_names.as_ref().unwrap_or(&empty_names);
            let values = cc
                .expression_attribute_values
                .as_ref()
                .unwrap_or(&empty_values);
            // ConditionExpression is required for ConditionCheck —
            // the wire struct typed it as String, so we already
            // have it.
            let condition = translate_condition(
                Some(cc.condition_expression.as_str()),
                names,
                values,
                schema,
            )?
            .ok_or(TranslateError::MalformedTransactItem {
                expected: "ConditionCheck requires non-empty ConditionExpression",
            })?;
            let return_old_on_failure = parse_return_old_on_failure(
                cc.return_values_on_condition_check_failure.as_deref(),
            )?;
            items.push(TransactWritePlanItem {
                table_name: cc.table_name.clone(),
                pk,
                sk,
                kind: TransactWriteKind::ConditionCheck,
                condition: Some(condition),
                return_old_on_failure,
            });
        } else {
            // Update lands in T5.
            return Err(TranslateError::MalformedTransactItem {
                expected: "Put / Delete / ConditionCheck (Update lands in PLAN-8 T5)",
            });
        }
    }

    Ok(TransactWriteItemsPlan { items })
}

/// Validate `ReturnValuesOnConditionCheckFailure`. DDB accepts `NONE`
/// (default) or `ALL_OLD`; anything else is a ValidationException.
fn parse_return_old_on_failure(raw: Option<&str>) -> Result<bool, TranslateError> {
    match raw {
        None | Some("NONE") => Ok(false),
        Some("ALL_OLD") => Ok(true),
        Some(other) => Err(TranslateError::UnsupportedReturnValuesForOp {
            op: "TransactWriteItems",
            got: other.to_string(),
        }),
    }
}

/// Shared `ReturnValues` resolver for PutItem and DeleteItem. DDB accepts
/// only `NONE` and `ALL_OLD` on these ops (`ALL_NEW` / `UPDATED_*` are
/// UpdateItem-only). The op name flows into the error message so callers
/// see the right context.
fn resolve_put_delete_return_values(
    raw: Option<&str>,
    op: &'static str,
) -> Result<ReturnValuesMode, TranslateError> {
    match raw {
        None | Some("NONE") => Ok(ReturnValuesMode::None),
        Some("ALL_OLD") => Ok(ReturnValuesMode::AllOld),
        Some(other) => Err(TranslateError::UnsupportedReturnValuesForOp {
            op,
            got: other.to_string(),
        }),
    }
}

/// Shared ConditionExpression resolver for PutItem and DeleteItem. Same
/// parse / substitute / classify pipeline as UpdateItem, minus the
/// UpdateExpression-specific bits. Returns `Ok(None)` for absent or
/// blank input.
fn translate_condition(
    raw: Option<&str>,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
    schema: &TableSchema,
) -> Result<Option<ConditionPlan>, TranslateError> {
    match raw {
        None => Ok(None),
        Some(c) if c.trim().is_empty() => Ok(None),
        Some(c) => {
            let raw_ast = parse_condition_expression(c)?;
            let cond = substitute_condition(raw_ast, names, values)
                .map_err(map_substitute_for_condition)?;
            check_condition_paths_top_level(&cond)?;
            let routing = classify_condition(&cond, schema);
            Ok(Some(ConditionPlan {
                routing,
                condition: cond,
            }))
        }
    }
}
