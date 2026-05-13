//! Translate DynamoDB operations into backend-neutral plans.
//!
//! For MVP this is a pair of free functions covering `PutItem` and `GetItem`.
//! Schemas may declare PK and SK with type S, N, or B; the translator
//! enforces that the incoming attribute matches the declared type.
//!
//! No expressions, no projections, no conditions yet — those live in their
//! own crate.

pub mod eval;
pub use eval::{apply_update_expression, evaluate_condition, EvalError};

use rekt_expressions::{
    parse_condition_expression, parse_update_expression, substitute_condition, substitute_update,
    ComparisonOp, Condition, Operand, ParseError, Path, PathSegment, SetRhs, SubstituteError,
    UpdateExpression,
};
use rekt_protocol::{
    AttributeValue, DeleteItemRequest, GetItemRequest, Item, PutItemRequest, UpdateItemRequest,
};
use rekt_storage::{KeyType, KeyValue, TableShape};
use std::collections::BTreeMap;

/// Description of a table that the translator and the storage layer agree on.
/// Built once at server startup from config (+ PG verification).
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// DynamoDB-side table name (what clients send).
    pub name: String,
    /// Postgres table name. May differ from `name`.
    pub pg_table: String,
    /// Partition-key attribute name. Doubles as the PG column name.
    pub pk_attr: String,
    pub pk_type: KeyType,
    /// Sort-key attribute name, if the table is composite. Doubles as the
    /// PG column name.
    pub sk_attr: Option<String>,
    pub sk_type: Option<KeyType>,
    /// PG column name for the full DynamoDB-JSON item blob (`jsonb` type).
    pub jsonb_col: String,
}

impl TableSchema {
    /// Borrow as a `TableShape` for handing to the storage layer.
    pub fn shape(&self) -> TableShape<'_> {
        TableShape {
            table: &self.pg_table,
            pk_col: &self.pk_attr,
            pk_type: self.pk_type,
            sk_col: self.sk_attr.as_deref(),
            sk_type: self.sk_type,
            jsonb_col: &self.jsonb_col,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PutItemPlan {
    /// Full item in DynamoDB-JSON form, source of truth for the jsonb column.
    /// PG derives the typed key columns from this via `GENERATED ALWAYS AS`,
    /// so writes don't bind pk/sk separately. The translator still validates
    /// that the expected key attributes are present and well-typed — that's
    /// protocol-level validation, independent of how PG stores them.
    pub item_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct GetItemPlan {
    pub pk: KeyValue,
    pub sk: Option<KeyValue>,
}

#[derive(Debug, Clone)]
pub struct DeleteItemPlan {
    pub pk: KeyValue,
    pub sk: Option<KeyValue>,
}

#[derive(Debug, Clone)]
pub struct UpdateItemPlan {
    pub pk: KeyValue,
    pub sk: Option<KeyValue>,
    /// The fully-resolved, schema-validated UpdateExpression. Storage
    /// interprets this directly — the simple-path predicate
    /// (`is_simple_update`) decides whether to emit pure-SQL or fall
    /// back to read-modify-write.
    pub expression: UpdateExpression,
    /// The fully-resolved, schema-validated ConditionExpression, if the
    /// request carried one. Storage uses `routing` to pick the SQL shape:
    /// `InsertOnlyOnPk` becomes `INSERT … ON CONFLICT DO NOTHING` (Phase 4c);
    /// `SimpleSql` becomes `UPDATE … WHERE cond-sql` (Phase 4d);
    /// `NeedsTx` falls to the slow path with in-Rust evaluation
    /// (Phase 4e). Always `None` in Phases 1–3.
    pub condition: Option<ConditionPlan>,
}

#[derive(Debug, Clone)]
pub struct ConditionPlan {
    pub routing: ConditionRouting,
    pub condition: Condition,
}

/// Where in the storage layer's dispatch tree this condition belongs.
/// Phase 4b populates this; Phases 4c / 4d / 4e wire each variant
/// through to a concrete backend path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionRouting {
    /// Exactly `attribute_not_exists(<pk_attr>)`, no boolean composition.
    /// The fast-path `INSERT … ON CONFLICT DO NOTHING` case.
    InsertOnlyOnPk,
    /// Any other v1 condition (compositions of `attribute_exists` /
    /// `attribute_not_exists` / `=` / `<>` / `<` / `<=` / `>` / `>=`
    /// via AND / OR / NOT). Compiles to a SQL WHERE fragment in 4d.
    SimpleSql,
    /// Conditions that can't be expressed in plain SQL (`begins_with`,
    /// `contains`, `BETWEEN`, etc.). Reserved for Phase 4e; the parser
    /// doesn't yet emit shapes that land here.
    NeedsTx,
}

/// Storage-ready primitives for the simple-subset UpdateItem fast path.
/// Produced by [`materialize_simple_update`] from a translated plan +
/// the request's `Key`.
#[derive(Debug, Clone)]
pub struct SimpleUpdatePrimitives {
    /// DDB-JSON for the INSERT branch: the request's key attrs merged
    /// with the SET literals.
    pub insert_item: serde_json::Value,
    /// One `(top-level attr name, DDB-JSON value)` per SET clause.
    pub sets: Vec<(String, serde_json::Value)>,
    /// One top-level attr name per REMOVE clause.
    pub removes: Vec<String>,
}

/// Storage-ready primitives for an UpdateItem with a `SimpleSql`-
/// classified ConditionExpression. The condition is *not* compiled
/// here — the backend's SQL compiler does that — but the SET/REMOVE
/// pieces are pre-flattened identically to the no-condition fast path.
/// Produced by [`materialize_simple_sql_update`].
#[derive(Debug, Clone)]
pub struct SimpleSqlUpdatePrimitives {
    pub sets: Vec<(String, serde_json::Value)>,
    pub removes: Vec<String>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TranslateError {
    #[error("missing partition key attribute `{attr}` in item")]
    MissingPartitionKey { attr: String },

    #[error("missing sort key attribute `{attr}` in item")]
    MissingSortKey { attr: String },

    #[error("partition key `{attr}` has wrong type: schema says {expected:?}, got {got}")]
    PartitionKeyTypeMismatch {
        attr: String,
        expected: KeyType,
        got: &'static str,
    },

    #[error("sort key `{attr}` has wrong type: schema says {expected:?}, got {got}")]
    SortKeyTypeMismatch {
        attr: String,
        expected: KeyType,
        got: &'static str,
    },

    #[error("Key contains unexpected attribute `{attr}` (not a key attribute)")]
    ExtraKeyAttribute { attr: String },

    // ----- UpdateItem-specific -----
    #[error("UpdateExpression is empty (must contain at least one clause)")]
    EmptyUpdateExpression,

    #[error("invalid UpdateExpression: {0}")]
    InvalidUpdateExpression(String),

    #[error("UpdateExpression references unknown placeholder: {0}")]
    UnknownPlaceholder(String),

    #[error(
        "UpdateExpression touches key attribute `{attr}` — key attributes are determined by the `Key` field, not by `UpdateExpression`"
    )]
    UpdateTouchesKey { attr: String },

    /// Nested paths (e.g. `meta.score`, `items[3]`) are valid DDB but
    /// the translator only supports top-level paths in this phase. Phase
    /// 8 lifts this restriction.
    #[error(
        "nested paths are not yet supported (got `{path}`) — Phase 8 will lift this restriction"
    )]
    UnsupportedNestedPath { path: String },

    /// Phase 2 only supports `SET attr = :value` (literal). Phase 3 will
    /// add path-refs / arithmetic / if_not_exists / list_append via the
    /// read-modify-write fallback.
    #[error("only literal SET right-hand sides are supported in this phase (got `{shape}`)")]
    UnsupportedSetRhs { shape: &'static str },

    /// ADD requires a numeric or set value. `ADD foo :s` where `:s` is
    /// `{"S":"x"}` raises `ValidationException`.
    #[error(
        "An operand in the update expression has an incorrect data type: ADD requires N or SS/NS/BS (got `{got}`)"
    )]
    InvalidAddValueType { got: &'static str },

    /// DELETE requires a set value. `DELETE foo :s` where `:s` isn't a
    /// `SS`/`NS`/`BS` raises `ValidationException`.
    #[error(
        "An operand in the update expression has an incorrect data type: DELETE requires SS/NS/BS (got `{got}`)"
    )]
    InvalidDeleteValueType { got: &'static str },

    #[error("invalid ConditionExpression: {0}")]
    InvalidConditionExpression(String),

    /// ConditionExpression handling lives in Phases 4c/4d/4e; the
    /// no-condition fast path can't be safely reused for conditional
    /// requests. Surfaced through the storage materializer until those
    /// phases wire their own backend paths.
    #[error(
        "ConditionExpression is parsed and validated but storage execution is not yet implemented — Phase 4c will land `attribute_not_exists(pk)`; Phase 4d adds SQL-WHERE; Phase 4e adds the slow-path evaluator"
    )]
    UnsupportedConditionInFastPath,

    /// Nested paths in conditions (e.g. `meta.score = :v`) are valid DDB
    /// but Phase 4's v1 grammar is top-level paths only. Phase 8 lifts.
    #[error(
        "nested paths in ConditionExpression are not yet supported (got `{path}`) — Phase 8 will lift this restriction"
    )]
    UnsupportedConditionNestedPath { path: String },

    #[error("only `ReturnValues=NONE` is supported in this phase (got `{got}`)")]
    UnsupportedReturnValues { got: String },
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.put_item", fields(table = %schema.name))]
pub fn translate_put_item(
    req: &PutItemRequest,
    schema: &TableSchema,
) -> Result<PutItemPlan, TranslateError> {
    // Validate the key attributes for protocol correctness. We discard the
    // extracted KeyValues — PG derives the actual key columns from the JSONB
    // via GENERATED ALWAYS AS, so the storage layer never binds them on writes.
    let _ = extract_key(&req.item, &schema.pk_attr, schema.pk_type, KeyRole::Pk)?;
    if let (Some(attr), Some(t)) = (&schema.sk_attr, schema.sk_type) {
        let _ = extract_key(&req.item, attr, t, KeyRole::Sk)?;
    }
    let item_json =
        serde_json::to_value(&req.item).expect("AttributeValue Serialize is infallible");
    Ok(PutItemPlan { item_json })
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
    Ok(DeleteItemPlan { pk, sk })
}

#[tracing::instrument(level = "debug", skip_all, name = "translate.update_item", fields(table = %schema.name))]
pub fn translate_update_item(
    req: &UpdateItemRequest,
    schema: &TableSchema,
) -> Result<UpdateItemPlan, TranslateError> {
    // 1. Disallow features not yet supported in this phase. Reject early so
    //    the rest of the translator only deals with the supported subset.
    match req.return_values.as_deref() {
        None | Some("NONE") => {}
        Some(other) => {
            return Err(TranslateError::UnsupportedReturnValues {
                got: other.to_string(),
            });
        }
    }

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
    let condition = match req.condition_expression.as_deref() {
        None => None,
        Some(c) if c.trim().is_empty() => None,
        Some(c) => {
            let raw = parse_condition_expression(c)?;
            let cond = substitute_condition(raw, names, values)?;
            check_condition_paths_top_level(&cond)?;
            let routing = classify_condition(&cond, schema);
            Some(ConditionPlan {
                routing,
                condition: cond,
            })
        }
    };

    Ok(UpdateItemPlan {
        pk,
        sk,
        expression,
        condition,
    })
}

/// Classify a ConditionExpression into a routing variant the storage
/// layer can dispatch on. The shape is determined by the AST plus the
/// schema's pk attribute name. Phase 4c/4d/4e wire each variant.
pub fn classify_condition(cond: &Condition, schema: &TableSchema) -> ConditionRouting {
    if let Condition::AttributeNotExists(p) = cond {
        if p.top_name() == Some(&schema.pk_attr) {
            return ConditionRouting::InsertOnlyOnPk;
        }
    }
    if condition_fits_sql(cond) {
        ConditionRouting::SimpleSql
    } else {
        ConditionRouting::NeedsTx
    }
}

/// True iff every comparison in `cond` can be expressed against a PG
/// jsonb column without consulting the existing row in Rust. The
/// concrete rules — equality works on any operand shapes; ordering
/// requires one Path operand + one Value operand of type N or S so the
/// SQL compiler can pick an extraction strategy — match what the
/// libpq backend's compiler emits.
fn condition_fits_sql(cond: &Condition) -> bool {
    match cond {
        Condition::AttributeExists(_) | Condition::AttributeNotExists(_) => true,
        Condition::Compare { op, left, right } => match op {
            ComparisonOp::Eq | ComparisonOp::Ne => true,
            ComparisonOp::Lt | ComparisonOp::Le | ComparisonOp::Gt | ComparisonOp::Ge => {
                ordering_has_typed_extraction(left, right)
            }
        },
        // Phase 4e additions all live on the slow path. The Rust
        // evaluator handles them in one place; promoting any to SQL
        // can come later behind a benchmark number.
        Condition::BeginsWith(_, _)
        | Condition::Contains(_, _)
        | Condition::Between(_, _, _)
        | Condition::In(_, _)
        | Condition::AttributeType(_, _) => false,
        Condition::And(a, b) | Condition::Or(a, b) => condition_fits_sql(a) && condition_fits_sql(b),
        Condition::Not(inner) => condition_fits_sql(inner),
    }
}

fn ordering_has_typed_extraction(left: &Operand, right: &Operand) -> bool {
    // One Path, one Value (in either order). The Value's AttributeValue
    // type picks the extraction (N → numeric, S → text). Anything else
    // (path-vs-path, value-vs-value, or a non-N/S value) lacks a
    // well-defined SQL ordering and routes to NeedsTx.
    let value = match (left, right) {
        (Operand::Path(_), Operand::Value(v)) | (Operand::Value(v), Operand::Path(_)) => v,
        _ => return false,
    };
    matches!(value, AttributeValue::N(_) | AttributeValue::S(_))
}

fn check_condition_paths_top_level(cond: &Condition) -> Result<(), TranslateError> {
    match cond {
        Condition::AttributeExists(p) | Condition::AttributeNotExists(p) => {
            check_condition_path(p)
        }
        Condition::Compare { left, right, .. } => {
            check_condition_operand(left)?;
            check_condition_operand(right)
        }
        Condition::BeginsWith(p, o)
        | Condition::Contains(p, o)
        | Condition::AttributeType(p, o) => {
            check_condition_path(p)?;
            check_condition_operand(o)
        }
        Condition::Between(v, lo, hi) => {
            check_condition_operand(v)?;
            check_condition_operand(lo)?;
            check_condition_operand(hi)
        }
        Condition::In(v, items) => {
            check_condition_operand(v)?;
            for item in items {
                check_condition_operand(item)?;
            }
            Ok(())
        }
        Condition::And(a, b) | Condition::Or(a, b) => {
            check_condition_paths_top_level(a)?;
            check_condition_paths_top_level(b)
        }
        Condition::Not(inner) => check_condition_paths_top_level(inner),
    }
}

fn check_condition_operand(o: &Operand) -> Result<(), TranslateError> {
    match o {
        Operand::Path(p) => check_condition_path(p),
        Operand::Value(_) => Ok(()),
    }
}

fn check_condition_path(p: &Path) -> Result<(), TranslateError> {
    if p.is_top_level() {
        Ok(())
    } else {
        Err(TranslateError::UnsupportedConditionNestedPath {
            path: format_path(p),
        })
    }
}

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

fn check_top_level(p: &Path) -> Result<(), TranslateError> {
    if p.is_top_level() {
        Ok(())
    } else {
        Err(TranslateError::UnsupportedNestedPath {
            path: format_path(p),
        })
    }
}

fn check_not_key(p: &Path, schema: &TableSchema) -> Result<(), TranslateError> {
    let name = match p.top_name() {
        Some(n) => n,
        None => return Ok(()),
    };
    if name == schema.pk_attr || schema.sk_attr.as_deref() == Some(name) {
        return Err(TranslateError::UpdateTouchesKey {
            attr: name.to_string(),
        });
    }
    Ok(())
}

/// Verify every embedded path in a SET RHS is top-level. The translator
/// permits non-literal RHS forms (path-ref, arithmetic, if_not_exists,
/// list_append) as of Phase 3c — they route to the slow path — but
/// nested paths are still a Phase 8 concern.
fn check_set_rhs_paths_top_level(rhs: &SetRhs) -> Result<(), TranslateError> {
    match rhs {
        SetRhs::Operand(o) => check_operand_path_top_level(o),
        SetRhs::Plus(a, b) | SetRhs::Minus(a, b) => {
            check_operand_path_top_level(a)?;
            check_operand_path_top_level(b)
        }
        SetRhs::IfNotExists(p, inner) => {
            check_top_level(p)?;
            check_set_rhs_paths_top_level(inner)
        }
        SetRhs::ListAppend(a, b) => {
            check_set_rhs_paths_top_level(a)?;
            check_set_rhs_paths_top_level(b)
        }
    }
}

fn check_operand_path_top_level(o: &Operand) -> Result<(), TranslateError> {
    match o {
        Operand::Path(p) => check_top_level(p),
        Operand::Value(_) => Ok(()),
    }
}

/// `ADD path :v` requires `:v` to be a Number (numeric add) or one of
/// the set types (set union). DDB raises ValidationException on any
/// other type at request time, so we surface the same rejection
/// pre-storage.
fn check_add_value_type(v: &AttributeValue) -> Result<(), TranslateError> {
    match v {
        AttributeValue::N(_)
        | AttributeValue::Ss(_)
        | AttributeValue::Ns(_)
        | AttributeValue::Bs(_) => Ok(()),
        other => Err(TranslateError::InvalidAddValueType {
            got: other.type_name(),
        }),
    }
}

/// `DELETE path :v` requires `:v` to be a set type — DELETE is set
/// subtraction only.
fn check_delete_value_type(v: &AttributeValue) -> Result<(), TranslateError> {
    match v {
        AttributeValue::Ss(_) | AttributeValue::Ns(_) | AttributeValue::Bs(_) => Ok(()),
        other => Err(TranslateError::InvalidDeleteValueType {
            got: other.type_name(),
        }),
    }
}

fn format_path(p: &Path) -> String {
    let mut out = String::new();
    for (i, seg) in p.segments.iter().enumerate() {
        match seg {
            PathSegment::Name(n) => {
                if i > 0 {
                    out.push('.');
                }
                out.push_str(n);
            }
            PathSegment::Index(n) => {
                out.push('[');
                out.push_str(&n.to_string());
                out.push(']');
            }
        }
    }
    out
}

impl From<ParseError> for TranslateError {
    fn from(e: ParseError) -> Self {
        // Discriminate Update vs Condition by the `kind` carried on the
        // parse error. Stays in sync with `rekt-expressions::parser`,
        // which tags every emitted error with its grammar.
        let is_condition = match &e {
            ParseError::Invalid { kind, .. } => *kind == "ConditionExpression",
            ParseError::Empty { kind } => *kind == "ConditionExpression",
            ParseError::DuplicateKeyword { .. } => false,
        };
        if is_condition {
            TranslateError::InvalidConditionExpression(e.to_string())
        } else {
            TranslateError::InvalidUpdateExpression(e.to_string())
        }
    }
}

impl From<SubstituteError> for TranslateError {
    fn from(e: SubstituteError) -> Self {
        match e {
            SubstituteError::UnknownName { name } => {
                TranslateError::UnknownPlaceholder(format!("#{name}"))
            }
            SubstituteError::UnknownValue { name } => {
                TranslateError::UnknownPlaceholder(format!(":{name}"))
            }
        }
    }
}

#[derive(Copy, Clone)]
enum KeyRole {
    Pk,
    Sk,
}

fn extract_key(
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

fn reject_extra_key_attrs(key: &Item, schema: &TableSchema) -> Result<(), TranslateError> {
    for attr in key.keys() {
        let allowed = attr == &schema.pk_attr || schema.sk_attr.as_deref() == Some(attr.as_str());
        if !allowed {
            return Err(TranslateError::ExtraKeyAttribute { attr: attr.clone() });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::BTreeMap;

    fn schema_hash_s() -> TableSchema {
        TableSchema {
            name: "users".into(),
            pg_table: "users".into(),
            pk_attr: "id".into(),
            pk_type: KeyType::S,
            sk_attr: None,
            sk_type: None,
            jsonb_col: "data".into(),
        }
    }

    fn schema_composite_s_n() -> TableSchema {
        TableSchema {
            name: "events".into(),
            pg_table: "events".into(),
            pk_attr: "device_id".into(),
            pk_type: KeyType::S,
            sk_attr: Some("ts".into()),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc".into(),
        }
    }

    fn schema_hash_b() -> TableSchema {
        TableSchema {
            name: "blobs".into(),
            pg_table: "blobs".into(),
            pk_attr: "hash".into(),
            pk_type: KeyType::B,
            sk_attr: None,
            sk_type: None,
            jsonb_col: "meta".into(),
        }
    }

    fn item_of(pairs: &[(&str, AttributeValue)]) -> Item {
        let mut m: BTreeMap<String, AttributeValue> = BTreeMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), v.clone());
        }
        m
    }

    #[test]
    fn put_hash_s_ok() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[
                ("id", AttributeValue::S("u1".into())),
                ("name", AttributeValue::S("alice".into())),
            ]),
        };
        let plan = translate_put_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            plan.item_json,
            serde_json::json!({"id":{"S":"u1"},"name":{"S":"alice"}})
        );
    }

    #[test]
    fn put_composite_s_n_ok() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::N("1234567890".into())),
                ("value", AttributeValue::N("99".into())),
            ]),
        };
        let plan = translate_put_item(&req, &schema_composite_s_n()).unwrap();
        // PutItemPlan no longer carries pk/sk; PG derives them from the JSONB.
        // Translator's job is still to validate the attrs are present and
        // typed correctly (this call would error if not).
        let stored = plan.item_json;
        assert_eq!(stored["device_id"], serde_json::json!({"S":"dev-1"}));
        assert_eq!(stored["ts"], serde_json::json!({"N":"1234567890"}));
    }

    #[test]
    fn put_hash_b_ok() {
        let req = PutItemRequest {
            table_name: "blobs".into(),
            item: item_of(&[
                ("hash", AttributeValue::B(Bytes::from_static(b"\x00\x01"))),
                ("size", AttributeValue::N("2".into())),
            ]),
        };
        let plan = translate_put_item(&req, &schema_hash_b()).unwrap();
        // base64 of {0x00, 0x01} is "AAE=".
        assert_eq!(plan.item_json["hash"], serde_json::json!({"B":"AAE="}));
    }

    #[test]
    fn put_pk_wrong_type() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("id", AttributeValue::N("42".into()))]),
        };
        let err = translate_put_item(&req, &schema_hash_s()).unwrap_err();
        match err {
            TranslateError::PartitionKeyTypeMismatch {
                attr,
                expected,
                got,
            } => {
                assert_eq!(attr, "id");
                assert_eq!(expected, KeyType::S);
                assert_eq!(got, "N");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn put_sk_wrong_type() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::S("not a number".into())),
            ]),
        };
        let err = translate_put_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::SortKeyTypeMismatch { .. }));
    }

    #[test]
    fn put_missing_pk() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("name", AttributeValue::S("alice".into()))]),
        };
        let err = translate_put_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingPartitionKey { .. }));
    }

    #[test]
    fn put_composite_missing_sk() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[("device_id", AttributeValue::S("dev-1".into()))]),
        };
        let err = translate_put_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingSortKey { .. }));
    }

    #[test]
    fn get_composite_ok() {
        let req = GetItemRequest {
            table_name: "events".into(),
            key: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::N("1234".into())),
            ]),
        };
        let plan = translate_get_item(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("dev-1".into()));
        assert_eq!(plan.sk, Some(KeyValue::N("1234".into())));
    }

    #[test]
    fn get_hash_ok() {
        let req = GetItemRequest {
            table_name: "users".into(),
            key: item_of(&[("id", AttributeValue::S("u1".into()))]),
        };
        let plan = translate_get_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("u1".into()));
        assert_eq!(plan.sk, None);
    }

    #[test]
    fn get_extra_key_attr_rejected() {
        let req = GetItemRequest {
            table_name: "users".into(),
            key: item_of(&[
                ("id", AttributeValue::S("u1".into())),
                ("name", AttributeValue::S("alice".into())),
            ]),
        };
        let err = translate_get_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn schema_shape_borrow() {
        let s = schema_composite_s_n();
        let shape = s.shape();
        assert_eq!(shape.table, "events");
        assert_eq!(shape.pk_col, "device_id");
        assert_eq!(shape.sk_col, Some("ts"));
        assert_eq!(shape.jsonb_col, "doc");
    }

    // ============================================================
    // UpdateItem translator tests
    // ============================================================

    fn upd(
        table: &str,
        key: &[(&str, AttributeValue)],
        expr: &str,
        values: &[(&str, AttributeValue)],
        names: &[(&str, &str)],
    ) -> UpdateItemRequest {
        UpdateItemRequest {
            table_name: table.into(),
            key: item_of(key),
            update_expression: expr.into(),
            expression_attribute_names: if names.is_empty() {
                None
            } else {
                Some(
                    names
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                )
            },
            expression_attribute_values: if values.is_empty() {
                None
            } else {
                Some(
                    values
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                )
            },
            condition_expression: None,
            return_values: None,
        }
    }

    fn set_literal_value(plan: &UpdateItemPlan, attr: &str) -> AttributeValue {
        let c = plan
            .expression
            .set
            .iter()
            .find(|c| c.path.top_name() == Some(attr))
            .unwrap_or_else(|| panic!("no SET clause for `{attr}`"));
        match &c.value {
            SetRhs::Operand(Operand::Value(v)) => v.clone(),
            other => panic!("expected literal SET RHS for `{attr}`, got {other:?}"),
        }
    }

    // ----- Happy paths: SET literal across every AttributeValue variant ----

    #[test]
    fn upd_set_string_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET name = :v",
            &[(":v", AttributeValue::S("alice".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("u1".into()));
        assert_eq!(
            set_literal_value(&plan, "name"),
            AttributeValue::S("alice".into())
        );
    }

    #[test]
    fn upd_set_number_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET score = :v",
            &[(":v", AttributeValue::N("42.5".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            set_literal_value(&plan, "score"),
            AttributeValue::N("42.5".into())
        );
    }

    #[test]
    fn upd_set_binary_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET blob = :v",
            &[(":v", AttributeValue::B(Bytes::from_static(b"\x00\x01\x02")))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(matches!(
            set_literal_value(&plan, "blob"),
            AttributeValue::B(_)
        ));
    }

    #[test]
    fn upd_set_bool_value() {
        for b in [true, false] {
            let req = upd(
                "users",
                &[("id", AttributeValue::S("u1".into()))],
                "SET active = :v",
                &[(":v", AttributeValue::Bool(b))],
                &[],
            );
            let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
            assert_eq!(set_literal_value(&plan, "active"), AttributeValue::Bool(b));
        }
    }

    #[test]
    fn upd_set_null_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET deleted = :v",
            &[(":v", AttributeValue::Null)],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "deleted"), AttributeValue::Null);
    }

    #[test]
    fn upd_set_list_value() {
        let list = AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::N("1".into()),
            AttributeValue::Bool(true),
        ]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET items = :v",
            &[(":v", list.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "items"), list);
    }

    #[test]
    fn upd_set_map_value() {
        let mut inner = BTreeMap::new();
        inner.insert("vip".into(), AttributeValue::Bool(true));
        inner.insert("score".into(), AttributeValue::N("99".into()));
        let map = AttributeValue::M(inner);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET meta = :v",
            &[(":v", map.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "meta"), map);
    }

    #[test]
    fn upd_set_string_set_value() {
        let ss = AttributeValue::Ss(vec!["a".into(), "b".into()]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET tags = :v",
            &[(":v", ss.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "tags"), ss);
    }

    #[test]
    fn upd_set_number_set_value() {
        let ns = AttributeValue::Ns(vec!["1".into(), "2".into()]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET scores = :v",
            &[(":v", ns.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "scores"), ns);
    }

    #[test]
    fn upd_set_binary_set_value() {
        let bs = AttributeValue::Bs(vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET chunks = :v",
            &[(":v", bs.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "chunks"), bs);
    }

    // ----- Multi-clause & composite keys -----------------------------------

    #[test]
    fn upd_set_multiple_attributes() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :s, score = :n, active = :b",
            &[
                (":s", AttributeValue::S("active".into())),
                (":n", AttributeValue::N("42".into())),
                (":b", AttributeValue::Bool(true)),
            ],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set.len(), 3);
        assert_eq!(
            set_literal_value(&plan, "status"),
            AttributeValue::S("active".into())
        );
        assert_eq!(
            set_literal_value(&plan, "score"),
            AttributeValue::N("42".into())
        );
        assert_eq!(
            set_literal_value(&plan, "active"),
            AttributeValue::Bool(true)
        );
    }

    #[test]
    fn upd_remove_single() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE deprecated_field",
            &[],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.remove.len(), 1);
        assert_eq!(
            plan.expression.remove[0].top_name(),
            Some("deprecated_field")
        );
    }

    #[test]
    fn upd_remove_multiple() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE a, b, c",
            &[],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.remove.len(), 3);
    }

    #[test]
    fn upd_set_and_remove_combined() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :s REMOVE deprecated_field",
            &[(":s", AttributeValue::S("active".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set.len(), 1);
        assert_eq!(plan.expression.remove.len(), 1);
    }

    #[test]
    fn upd_composite_key_passthrough() {
        let req = upd(
            "events",
            &[
                ("device_id", AttributeValue::S("d1".into())),
                ("ts", AttributeValue::N("1000".into())),
            ],
            "SET value = :v",
            &[(":v", AttributeValue::N("42".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("d1".into()));
        assert_eq!(plan.sk, Some(KeyValue::N("1000".into())));
    }

    #[test]
    fn upd_name_placeholder_substitutes() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET #s = :v",
            &[(":v", AttributeValue::S("ok".into()))],
            &[("#s", "status")],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set[0].path.top_name(), Some("status"));
    }

    // ----- Reject paths that touch key attributes --------------------------

    #[test]
    fn upd_reject_set_on_pk_attr() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET id = :v",
            &[(":v", AttributeValue::S("u2".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "id"));
    }

    #[test]
    fn upd_reject_remove_on_pk_attr() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE id",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "id"));
    }

    #[test]
    fn upd_reject_set_on_sk_attr() {
        let req = upd(
            "events",
            &[
                ("device_id", AttributeValue::S("d1".into())),
                ("ts", AttributeValue::N("1000".into())),
            ],
            "SET ts = :v",
            &[(":v", AttributeValue::N("2000".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "ts"));
    }

    // ----- Reject nested / indexed paths (deferred to Phase 8) ------------

    #[test]
    fn upd_reject_dotted_path() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET meta.score = :v",
            &[(":v", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedNestedPath { ref path } if path == "meta.score")
        );
    }

    #[test]
    fn upd_reject_indexed_path() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET items[3] = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedNestedPath { ref path } if path == "items[3]")
        );
    }

    // ----- Phase 3c: non-literal SET RHS shapes are now accepted -----
    //
    // The translator no longer rejects path-ref, arithmetic,
    // if_not_exists, or list_append. They route to the slow path (via
    // `!is_simple()`) at the server. Tests here just confirm acceptance
    // + the `is_simple()` predicate flips to false.

    #[test]
    fn upd_accept_set_path_ref() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET a = b",
            &[],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_accept_set_arithmetic() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET total = subtotal + :tax",
            &[(":tax", AttributeValue::N("1".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_accept_if_not_exists() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET created_at = if_not_exists(created_at, :now)",
            &[(":now", AttributeValue::N("1700000000".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_accept_list_append() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET items = list_append(items, :new)",
            &[(":new", AttributeValue::L(vec![]))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_reject_nested_path_in_set_rhs() {
        // Nested paths in RHS are still rejected — they're Phase 8.
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET a = b.nested",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedNestedPath { .. }));
    }

    // ----- Phase 5/6: ADD and DELETE accepted (route to slow path) -----

    #[test]
    fn upd_accept_add_numeric() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD count :one",
            &[(":one", AttributeValue::N("1".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.add.len(), 1);
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_accept_add_set() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD tags :new",
            &[(":new", AttributeValue::Ss(vec!["a".into(), "b".into()]))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.add.len(), 1);
    }

    #[test]
    fn upd_reject_add_with_non_numeric_non_set_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD count :one",
            &[(":one", AttributeValue::S("not_a_number".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::InvalidAddValueType { got } if got == "S"),
            "got: {err:?}"
        );
    }

    #[test]
    fn upd_reject_add_on_key_attr() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD id :one",
            &[(":one", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "id"));
    }

    #[test]
    fn upd_accept_delete_set() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "DELETE tags :rm",
            &[(":rm", AttributeValue::Ss(vec!["x".into()]))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.delete.len(), 1);
    }

    #[test]
    fn upd_reject_delete_with_non_set_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "DELETE tags :v",
            &[(":v", AttributeValue::S("not_a_set".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::InvalidDeleteValueType { got } if got == "S"),
            "got: {err:?}"
        );
    }

    // ----- ConditionExpression: parsed + classified (Phase 4b) -----------
    //
    // The translator now accepts ConditionExpression and produces a
    // `ConditionPlan` on `UpdateItemPlan.condition`. Storage execution is
    // wired in Phases 4c (insert-only), 4d (SQL WHERE), 4e (slow path);
    // until then, `materialize_simple_update` refuses conditional plans
    // with `UnsupportedConditionInFastPath`.

    fn upd_with_condition(
        cond: &str,
        values: &[(&str, AttributeValue)],
        names: &[(&str, &str)],
    ) -> UpdateItemRequest {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :s",
            // Merge `:s` placeholder with any caller-supplied ones.
            &{
                let mut v: Vec<(&str, AttributeValue)> =
                    vec![(":s", AttributeValue::S("active".into()))];
                v.extend(values.iter().cloned());
                v
            },
            names,
        );
        req.condition_expression = Some(cond.into());
        req
    }

    #[test]
    fn upd_condition_classified_as_insert_only_on_pk() {
        let req = upd_with_condition("attribute_not_exists(id)", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let cp = plan.condition.expect("condition should be present");
        assert_eq!(cp.routing, ConditionRouting::InsertOnlyOnPk);
        assert!(matches!(cp.condition, Condition::AttributeNotExists(_)));
    }

    #[test]
    fn upd_condition_attribute_not_exists_on_non_pk_is_simple_sql() {
        let req = upd_with_condition("attribute_not_exists(status)", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            plan.condition.unwrap().routing,
            ConditionRouting::SimpleSql
        );
    }

    #[test]
    fn upd_condition_attribute_exists_on_pk_is_simple_sql() {
        // `attribute_exists(pk)` is "update-only" semantics — handled as
        // SimpleSql, not InsertOnlyOnPk (which is only for *not*-exists).
        let req = upd_with_condition("attribute_exists(id)", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            plan.condition.unwrap().routing,
            ConditionRouting::SimpleSql
        );
    }

    #[test]
    fn upd_condition_comparison_is_simple_sql() {
        let req = upd_with_condition(
            "version = :v",
            &[(":v", AttributeValue::N("3".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let cp = plan.condition.unwrap();
        assert_eq!(cp.routing, ConditionRouting::SimpleSql);
        assert!(matches!(cp.condition, Condition::Compare { .. }));
    }

    #[test]
    fn upd_condition_boolean_composition_is_simple_sql() {
        // Even when one term is `attribute_not_exists(pk)`, the routing is
        // SimpleSql once it's inside AND/OR — InsertOnlyOnPk requires the
        // condition to be *exactly* that one shape.
        let req = upd_with_condition(
            "attribute_not_exists(id) OR version = :v",
            &[(":v", AttributeValue::N("1".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let cp = plan.condition.unwrap();
        assert_eq!(cp.routing, ConditionRouting::SimpleSql);
        assert!(matches!(cp.condition, Condition::Or(_, _)));
    }

    #[test]
    fn upd_condition_resolves_name_and_value_placeholders() {
        let req = upd_with_condition(
            "#k = :want",
            &[(":want", AttributeValue::S("active".into()))],
            &[("#k", "status")],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let cp = plan.condition.unwrap();
        match cp.condition {
            Condition::Compare { left, right, .. } => {
                assert!(matches!(
                    left,
                    Operand::Path(p) if p.top_name() == Some("status")
                ));
                assert!(matches!(right, Operand::Value(AttributeValue::S(ref s)) if s == "active"));
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    #[test]
    fn upd_condition_empty_string_is_treated_as_absent() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.condition_expression = Some("".into());
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(plan.condition.is_none());
    }

    #[test]
    fn upd_condition_reject_nested_path() {
        let req = upd_with_condition("meta.score = :v", &[(":v", AttributeValue::N("1".into()))], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedConditionNestedPath { ref path } if path == "meta.score")
        );
    }

    #[test]
    fn upd_condition_reject_indexed_path() {
        let req = upd_with_condition("items[0] = :v", &[(":v", AttributeValue::N("1".into()))], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionNestedPath { .. }
        ));
    }

    #[test]
    fn upd_condition_reject_in_attribute_exists() {
        let req = upd_with_condition("attribute_exists(meta.score)", &[], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionNestedPath { .. }
        ));
    }

    #[test]
    fn upd_condition_parse_error_surfaces_as_invalid_condition() {
        let req = upd_with_condition("a = ", &[], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::InvalidConditionExpression(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn upd_condition_unknown_placeholder_surfaces_clearly() {
        let req = upd_with_condition("a = :missing", &[], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnknownPlaceholder(ref p) if p == ":missing"));
    }

    #[test]
    fn upd_condition_composite_key_pk_only_classified_correctly() {
        // For a composite-key table, InsertOnlyOnPk requires
        // `attribute_not_exists(pk_attr)` — the sk attr alone wouldn't
        // be enough to scope insert-only-on-the-row semantics.
        let req = {
            let mut r = upd(
                "events",
                &[
                    ("device_id", AttributeValue::S("d1".into())),
                    ("ts", AttributeValue::N("1000".into())),
                ],
                "SET v = :v",
                &[(":v", AttributeValue::N("1".into()))],
                &[],
            );
            r.condition_expression = Some("attribute_not_exists(device_id)".into());
            r
        };
        let plan = translate_update_item(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(
            plan.condition.unwrap().routing,
            ConditionRouting::InsertOnlyOnPk
        );
    }

    // ----- NeedsTx routing (Phase 4e fall-through) -----
    //
    // Ordering on path-vs-path, value-vs-value, or against
    // non-N/non-S RHS values can't compile to SQL without a typed
    // extraction strategy. Those shapes must reach the slow path.

    #[test]
    fn upd_condition_path_vs_path_ordering_is_needs_tx() {
        let req = upd_with_condition("a < b", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::NeedsTx);
    }

    #[test]
    fn upd_condition_ordering_against_bool_is_needs_tx() {
        let req = upd_with_condition("a > :b", &[(":b", AttributeValue::Bool(true))], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::NeedsTx);
    }

    #[test]
    fn upd_condition_ordering_against_binary_is_needs_tx() {
        let req = upd_with_condition(
            "a >= :b",
            &[(":b", AttributeValue::B(bytes::Bytes::from_static(b"hi")))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::NeedsTx);
    }

    #[test]
    fn upd_condition_ordering_against_n_value_is_simple_sql() {
        let req = upd_with_condition("a < :n", &[(":n", AttributeValue::N("5".into()))], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::SimpleSql);
    }

    #[test]
    fn upd_condition_ordering_against_s_value_is_simple_sql() {
        let req = upd_with_condition(
            "name <= :s",
            &[(":s", AttributeValue::S("z".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::SimpleSql);
    }

    #[test]
    fn upd_condition_equality_path_vs_path_is_simple_sql() {
        // Equality on any operand shape (JSONB equality handles it).
        let req = upd_with_condition("a = b", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::SimpleSql);
    }

    #[test]
    fn upd_condition_needs_tx_propagates_through_boolean_composition() {
        // One NeedsTx term inside an AND/OR → whole condition is NeedsTx.
        let req = upd_with_condition(
            "version = :v AND a < b",
            &[(":v", AttributeValue::N("1".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::NeedsTx);
    }

    // ----- Phase 4b boundary: materialize refuses conditional plans -----

    #[test]
    fn materialize_refuses_conditional_plan_until_4c() {
        let req = upd_with_condition("attribute_not_exists(id)", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let err = materialize_simple_update(&plan, &req.key).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionInFastPath
        ));
    }

    #[test]
    fn upd_accept_explicit_return_values_none() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("NONE".into());
        assert!(translate_update_item(&req, &schema_hash_s()).is_ok());
    }

    #[test]
    fn upd_reject_return_values_all_old() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("ALL_OLD".into());
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedReturnValues { ref got } if got == "ALL_OLD")
        );
    }

    // ----- Parse / substitute errors propagate -----------------------------

    #[test]
    fn upd_propagates_parse_error() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = ",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidUpdateExpression(_)));
    }

    #[test]
    fn upd_propagates_unknown_value_placeholder() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET status = :v",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnknownPlaceholder(ref p) if p == ":v"));
    }

    #[test]
    fn upd_propagates_unknown_name_placeholder() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET #x = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnknownPlaceholder(ref p) if p == "#x"));
    }

    // ----- Key validation reuses existing machinery -----------------------

    #[test]
    fn upd_reject_missing_key() {
        // Same as Get/Delete: missing pk -> MissingPartitionKey.
        let req = upd(
            "users",
            &[],
            "SET status = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingPartitionKey { .. }));
    }

    #[test]
    fn upd_reject_extra_key_attr() {
        let req = upd(
            "users",
            &[
                ("id", AttributeValue::S("u1".into())),
                ("rogue", AttributeValue::S("x".into())),
            ],
            "SET status = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn upd_reject_empty_expression() {
        // Empty after parsing (the parser itself rejects this earlier; we
        // also have to handle "expression parsed but has no clauses we
        // support" — though in practice an empty post-parse expression is
        // unreachable for now since the parser requires at least one clause).
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        // Parser rejects empty string with ParseError::Empty, which we
        // wrap as InvalidUpdateExpression.
        assert!(matches!(err, TranslateError::InvalidUpdateExpression(_)));
    }
}
