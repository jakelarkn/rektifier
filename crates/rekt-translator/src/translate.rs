//! The four public `translate_*_item` entry points + shared helpers
//! for resolving `ReturnValues` and parsing `ConditionExpression`.

use crate::checks::{
    check_add_value_type, check_condition_paths_top_level, check_delete_value_type, check_not_key,
    check_set_rhs_paths_top_level, check_top_level,
};
use crate::classify::classify_condition;
use crate::error::{map_substitute_for_condition, TranslateError};
use crate::key_condition::extract_key_condition;
use crate::keys::{extract_key, reject_extra_key_attrs, KeyRole};
use crate::paging::{decode_esk, validate_limit};
use crate::plan::{
    ConditionPlan, DeleteItemPlan, GetItemPlan, PutItemPlan, QueryPlan, ReturnValuesMode,
    UpdateItemPlan,
};
use crate::schema::TableSchema;
use rekt_expressions::{
    parse_condition_expression, parse_update_expression, substitute_condition, substitute_update,
};
use rekt_protocol::{
    AttributeValue, DeleteItemRequest, GetItemRequest, PutItemRequest, QueryRequest,
    UpdateItemRequest,
};
use std::collections::BTreeMap;

#[tracing::instrument(level = "debug", skip_all, name = "translate.put_item", fields(table = %schema.name))]
pub fn translate_put_item(
    req: &PutItemRequest,
    schema: &TableSchema,
) -> Result<PutItemPlan, TranslateError> {
    // 1. Validate key attributes; capture pk/sk for the conditional path.
    //    The unconditional fast path ignores them (PG derives via
    //    GENERATED ALWAYS AS), but the conditional / ALL_OLD paths need
    //    them to look up the pre-update row.
    let pk = extract_key(&req.item, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    let sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(t)) => Some(extract_key(&req.item, attr, t, KeyRole::Sk)?),
        _ => None,
    };

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
    let pk = extract_key(&req.key, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    let sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(t)) => Some(extract_key(&req.key, attr, t, KeyRole::Sk)?),
        _ => None,
    };
    reject_extra_key_attrs(&req.key, schema)?;
    Ok(GetItemPlan { pk, sk })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.delete_item", fields(table = %schema.name))]
pub fn translate_delete_item(
    req: &DeleteItemRequest,
    schema: &TableSchema,
) -> Result<DeleteItemPlan, TranslateError> {
    let pk = extract_key(&req.key, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    let sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(t)) => Some(extract_key(&req.key, attr, t, KeyRole::Sk)?),
        _ => None,
    };
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
    let pk = extract_key(&req.key, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    let sk = match (&schema.sk_attr, schema.sk_type) {
        (Some(attr), Some(t)) => Some(extract_key(&req.key, attr, t, KeyRole::Sk)?),
        _ => None,
    };
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

    Ok(QueryPlan {
        pk: bounds.pk,
        sk_condition: bounds.sk_condition,
        limit,
        esk_sk,
        filter,
    })
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
