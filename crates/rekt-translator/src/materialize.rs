//! Pre-flatten a simple-subset UpdateItemPlan into storage-ready
//! primitives. Three flavors:
//!
//! - [`materialize_simple_update`] — no-condition fast path; produces
//!   the INSERT-branch JSONB + SET/REMOVE flat lists.
//! - [`materialize_insert_only_update`] — `attribute_not_exists(pk)`
//!   fast path; produces just the INSERT-branch JSONB.
//! - [`materialize_simple_sql_update`] — `SimpleSql` fast path;
//!   produces SET/REMOVE flat lists only (no INSERT branch — the
//!   storage layer's compiled WHERE clause demands the row exists).

use crate::error::TranslateError;
use crate::plan::{
    ConditionRouting, SimpleSqlUpdatePrimitives, SimpleUpdatePrimitives, UpdateItemPlan,
};
use rekt_expressions::{Operand, SetRhs};
use rekt_protocol::Item;

/// Pre-decompose a Phase-2 UpdateItemPlan into the storage layer's
/// fast-path primitives. The caller must pass the same `Key` it gave to
/// `translate_update_item` so we can build the INSERT-branch JSONB.
///
/// Returns an error if the plan's expression is outside the simple
/// subset — this is a defensive check; `translate_update_item` rejects
/// such expressions already, but we'd rather not silently mis-route.
pub fn materialize_simple_update(
    plan: &UpdateItemPlan,
    key: &Item,
) -> Result<SimpleUpdatePrimitives, TranslateError> {
    if !plan.expression.is_simple() {
        return Err(TranslateError::UnsupportedSetRhs {
            shape: "non-simple expression reached materializer",
        });
    }
    // Condition handling is Phase 4c/4d/4e. Until then we refuse to route
    // a conditional plan through the no-condition fast path (which would
    // silently apply the update unconditionally — wrong).
    if plan.condition.is_some() {
        return Err(TranslateError::UnsupportedConditionInFastPath);
    }

    // Start the INSERT-branch jsonb at the request's key, then layer SETs
    // on top so the new row has `{key + SET attrs}` when no row exists.
    let mut insert_item =
        serde_json::to_value(key).expect("AttributeValue Serialize is infallible");
    let obj = insert_item
        .as_object_mut()
        .expect("Item serializes to a JSON object");

    let mut sets: Vec<(String, serde_json::Value)> = Vec::with_capacity(plan.expression.set.len());
    for clause in &plan.expression.set {
        let attr = clause
            .path
            .top_name()
            .expect("is_simple() guarantees top-level path")
            .to_string();
        let value = match &clause.value {
            SetRhs::Operand(Operand::Value(v)) => {
                serde_json::to_value(v).expect("AttributeValue Serialize is infallible")
            }
            _ => unreachable!("is_simple() guarantees Operand::Value RHS"),
        };
        obj.insert(attr.clone(), value.clone());
        sets.push((attr, value));
    }

    let removes: Vec<String> = plan
        .expression
        .remove
        .iter()
        .map(|p| {
            p.top_name()
                .expect("is_simple() guarantees top-level path")
                .to_string()
        })
        .collect();

    Ok(SimpleUpdatePrimitives {
        insert_item,
        sets,
        removes,
    })
}

/// Materialize the JSONB to insert when routing an UpdateItem with
/// `ConditionExpression: attribute_not_exists(pk)` through the storage
/// layer's insert-only fast path (Phase 4c). The result is the request's
/// key attrs merged with the SET-clause literals — exactly what
/// DDB would create when the condition holds. REMOVE clauses don't
/// contribute (they're no-ops against a row that doesn't exist yet).
///
/// Errors if the plan's update expression isn't simple, or if the
/// classified condition routing isn't `InsertOnlyOnPk`.
pub fn materialize_insert_only_update(
    plan: &UpdateItemPlan,
    key: &Item,
) -> Result<serde_json::Value, TranslateError> {
    if !plan.expression.is_simple() {
        return Err(TranslateError::UnsupportedSetRhs {
            shape: "non-simple expression reached insert-only materializer",
        });
    }
    let cp = plan.condition.as_ref().ok_or(TranslateError::UnsupportedSetRhs {
        shape: "insert-only materializer called without a condition",
    })?;
    if cp.routing != ConditionRouting::InsertOnlyOnPk {
        return Err(TranslateError::UnsupportedSetRhs {
            shape: "insert-only materializer called with non-InsertOnlyOnPk routing",
        });
    }
    Ok(build_insert_item(plan, key))
}

/// Pre-flatten the SET/REMOVE clauses of a simple-subset
/// UpdateItem into the (attr-name, ddb-json value) and attr-name
/// lists the storage backends expect. Used for the SimpleSql fast
/// path (which doesn't need an insert_item because there's no INSERT
/// branch).
///
/// Errors if the plan's expression isn't simple, or if the classified
/// condition routing isn't `SimpleSql` — the caller is dispatching to
/// the wrong materializer in that case.
pub fn materialize_simple_sql_update(
    plan: &UpdateItemPlan,
) -> Result<SimpleSqlUpdatePrimitives, TranslateError> {
    if !plan.expression.is_simple() {
        return Err(TranslateError::UnsupportedSetRhs {
            shape: "non-simple expression reached SimpleSql materializer",
        });
    }
    let cp = plan.condition.as_ref().ok_or(TranslateError::UnsupportedSetRhs {
        shape: "SimpleSql materializer called without a condition",
    })?;
    if cp.routing != ConditionRouting::SimpleSql {
        return Err(TranslateError::UnsupportedSetRhs {
            shape: "SimpleSql materializer called with non-SimpleSql routing",
        });
    }
    let (sets, removes) = build_sets_removes(plan);
    Ok(SimpleSqlUpdatePrimitives { sets, removes })
}

/// Pre-flatten just the SET / REMOVE clauses of a simple-subset
/// UpdateExpression. Caller must have already established
/// `plan.expression.is_simple()`.
fn build_sets_removes(
    plan: &UpdateItemPlan,
) -> (Vec<(String, serde_json::Value)>, Vec<String>) {
    let sets: Vec<(String, serde_json::Value)> = plan
        .expression
        .set
        .iter()
        .map(|clause| {
            let attr = clause
                .path
                .top_name()
                .expect("is_simple() guarantees top-level path")
                .to_string();
            let value = match &clause.value {
                SetRhs::Operand(Operand::Value(v)) => {
                    serde_json::to_value(v).expect("AttributeValue Serialize is infallible")
                }
                _ => unreachable!("is_simple() guarantees Operand::Value RHS"),
            };
            (attr, value)
        })
        .collect();
    let removes: Vec<String> = plan
        .expression
        .remove
        .iter()
        .map(|p| {
            p.top_name()
                .expect("is_simple() guarantees top-level path")
                .to_string()
        })
        .collect();
    (sets, removes)
}

/// Build `{key attrs ∪ SET literals}` as DDB-JSON. Shared by both
/// fast-path materializers. Caller must have already established
/// `plan.expression.is_simple()`.
fn build_insert_item(plan: &UpdateItemPlan, key: &Item) -> serde_json::Value {
    let mut insert_item =
        serde_json::to_value(key).expect("AttributeValue Serialize is infallible");
    let obj = insert_item
        .as_object_mut()
        .expect("Item serializes to a JSON object");
    for clause in &plan.expression.set {
        let attr = clause
            .path
            .top_name()
            .expect("is_simple() guarantees top-level path")
            .to_string();
        let value = match &clause.value {
            SetRhs::Operand(Operand::Value(v)) => {
                serde_json::to_value(v).expect("AttributeValue Serialize is infallible")
            }
            _ => unreachable!("is_simple() guarantees Operand::Value RHS"),
        };
        obj.insert(attr, value);
    }
    insert_item
}
