//! axum HTTP server that dispatches DynamoDB-JSON-1.0 requests to the
//! translator + backend.
//!
//! DDB has a single endpoint (`POST /`); the operation is selected by the
//! `X-Amz-Target` header (`DynamoDB_20120810.PutItem`, etc). The body is
//! a JSON document whose shape depends on the op.
//!
//! For MVP we support `PutItem` and `GetItem`. Other ops return
//! `UnknownOperationException` 400.
//!
//! Error responses match DDB's wire format exactly:
//!
//! ```json
//! {
//!   "__type": "com.amazonaws.dynamodb.v20120810#ValidationException",
//!   "message": "..."
//! }
//! ```
//!
//! HTTP status is **400 for almost everything** (per DDB convention) and
//! **500** only for genuine internal errors.

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use rekt_protocol::{
    DeleteItemRequest, DeleteItemResponse, GetItemRequest, GetItemResponse, Item, PutItemRequest,
    PutItemResponse, QueryRequest, QueryResponse, UpdateItemRequest, UpdateItemResponse,
};
use rekt_sigv4::{SigV4Error, Verifier};
use rekt_storage::{Backend, BackendError, UpdateDecision, UpdateOutcome};
use rekt_translator::{
    apply_update_expression, encode_lek, evaluate_condition, materialize_insert_only_update,
    materialize_simple_sql_update, materialize_simple_update, touched_paths,
    translate_delete_item, translate_get_item, translate_put_item, translate_query,
    translate_update_item, ConditionRouting, ReturnValuesMode, TableSchema, TranslateError,
};
use std::collections::HashMap;
use std::sync::Arc;
use tower_http::limit::RequestBodyLimitLayer;

/// Hardcoded HTTP body-size ceiling. Defense-in-depth before any parsing;
/// the per-table item-size limit (translator-enforced) lives on top of this.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// DDB JSON-1.0 ServiceID prefix on `X-Amz-Target`.
const TARGET_PREFIX: &str = "DynamoDB_20120810.";

/// Content type for DDB JSON-1.0 requests and responses.
const CONTENT_TYPE: &str = "application/x-amz-json-1.0";

#[derive(Clone)]
pub struct AppState {
    pub verifier: Arc<dyn Verifier>,
    pub backend: Arc<dyn Backend>,
    pub schemas: Arc<HashMap<String, TableSchema>>,
}

/// Build the axum router. Apply the body-size limit at the outermost layer
/// so it triggers before any parsing or extraction.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", post(dispatch))
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .with_state(state)
}

// ===== Error type ==============================================================
//
// The variants map to DDB error shapes. `IntoResponse` writes the
// `{"__type":"...#<Name>","message":"..."}` body with the right HTTP status
// and `Content-Type` header.

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("UnknownOperationException: {0}")]
    UnknownOperation(String),

    #[error("SerializationException: {0}")]
    Serialization(String),

    #[error("ValidationException: {0}")]
    Validation(String),

    #[error("ResourceNotFoundException: {0}")]
    ResourceNotFound(String),

    #[error("AccessDeniedException: {0}")]
    AccessDenied(String),

    #[error("ConditionalCheckFailedException: {0}")]
    ConditionalCheckFailed(String),

    #[error("InternalServerError: {0}")]
    Internal(String),
}

impl ApiError {
    fn ddb_error_name(&self) -> &'static str {
        match self {
            Self::UnknownOperation(_) => "UnknownOperationException",
            Self::Serialization(_) => "SerializationException",
            Self::Validation(_) => "ValidationException",
            Self::ResourceNotFound(_) => "ResourceNotFoundException",
            Self::AccessDenied(_) => "AccessDeniedException",
            Self::ConditionalCheckFailed(_) => "ConditionalCheckFailedException",
            Self::Internal(_) => "InternalServerError",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    fn message(&self) -> String {
        // Strip the variant-name prefix our Display attaches; the wire
        // message field carries just the human-readable reason.
        match self {
            Self::UnknownOperation(m)
            | Self::Serialization(m)
            | Self::Validation(m)
            | Self::ResourceNotFound(m)
            | Self::AccessDenied(m)
            | Self::ConditionalCheckFailed(m)
            | Self::Internal(m) => m.clone(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "__type": format!("com.amazonaws.dynamodb.v20120810#{}", self.ddb_error_name()),
            "message": self.message(),
        });
        let body_bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());

        let mut response = (self.http_status(), body_bytes).into_response();
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static(CONTENT_TYPE),
        );
        response
    }
}

impl From<TranslateError> for ApiError {
    fn from(e: TranslateError) -> Self {
        ApiError::Validation(e.to_string())
    }
}

impl From<BackendError> for ApiError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::TableNotFound { name } => {
                ApiError::ResourceNotFound(format!("Table not found: {name}"))
            }
            BackendError::MissingSortKey { name } => {
                ApiError::Validation(format!("Sort key required for `{name}`"))
            }
            BackendError::UnexpectedSortKey { name } => ApiError::Validation(format!(
                "Sort key not allowed for `{name}` (hash-only table)"
            )),
            BackendError::KeyTypeMismatch {
                col,
                expected,
                actual,
            } => ApiError::Validation(format!(
                "Key column `{col}`: expected {expected:?}, got {actual:?}"
            )),
            BackendError::ConditionalCheckFailed => {
                ApiError::ConditionalCheckFailed("The conditional request failed".into())
            }
            BackendError::Other(m) => ApiError::Internal(m),
        }
    }
}

impl From<SigV4Error> for ApiError {
    fn from(e: SigV4Error) -> Self {
        ApiError::AccessDenied(e.to_string())
    }
}

// ===== Dispatch ================================================================

#[tracing::instrument(level = "debug", skip_all, name = "server.dispatch", fields(op = tracing::field::Empty))]
async fn dispatch(State(state): State<AppState>, req: Request) -> Result<Response, ApiError> {
    // Split the request into its real Parts + Body. Passing the real Parts
    // straight to the verifier saves a HeaderMap::clone vs the earlier
    // `HeaderMap + Bytes` extractor pair, which built a synthetic Parts and
    // cloned headers into it.
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, MAX_BODY_BYTES)
        .await
        .map_err(|e| ApiError::Serialization(format!("body read failed: {e}")))?;

    // 1. Verify the request. PermissiveVerifier is the MVP impl — see
    //    rekt-sigv4. We do this before peeking at any body content so the
    //    auth boundary stays at the top.
    state.verifier.verify(&parts, &body)?;

    // 2. Identify the operation from the X-Amz-Target header.
    let target = parts
        .headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::Serialization("missing X-Amz-Target header".into()))?;
    let op = target
        .strip_prefix(TARGET_PREFIX)
        .ok_or_else(|| ApiError::UnknownOperation(format!("unsupported target: {target}")))?;
    tracing::Span::current().record("op", op);

    // 3. Dispatch on op name.
    match op {
        "PutItem" => handle_put_item(&state, &body).await,
        "GetItem" => handle_get_item(&state, &body).await,
        "DeleteItem" => handle_delete_item(&state, &body).await,
        "UpdateItem" => handle_update_item(&state, &body).await,
        "Query" => handle_query(&state, &body).await,
        _ => Err(ApiError::UnknownOperation(format!(
            "operation `{op}` not implemented"
        ))),
    }
}

#[tracing::instrument(level = "debug", skip_all, name = "server.put_item", fields(table = tracing::field::Empty))]
async fn handle_put_item(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: PutItemRequest = serde_json::from_slice(body)
        .map_err(|e| ApiError::Serialization(format!("invalid PutItem body: {e}")))?;
    tracing::Span::current().record("table", req.table_name.as_str());

    let schema = state.schemas.get(&req.table_name).ok_or_else(|| {
        ApiError::ResourceNotFound(format!("Table not found: {}", req.table_name))
    })?;

    let plan = translate_put_item(&req, schema)?;
    let shape = schema.shape();

    // Conditional + unconditional both come back through the same
    // "(pre-image, item)" surface; the only branch is which backend
    // method we call. The slow path is fine for now — a fast-path
    // optimization for `attribute_not_exists(pk)` mirroring
    // `update_insert_only_raw` could come later behind benchmark
    // numbers.
    let old_item = match plan.condition.as_ref() {
        None => {
            state
                .backend
                .put_item_raw(&shape, &plan.pk, plan.sk.as_ref(), &plan.item_json)
                .await?
        }
        Some(cond_plan) => {
            let cond = cond_plan.condition.clone();
            let eval: rekt_storage::ConditionEvalFn<'_> =
                Box::new(move |existing| evaluate_condition(existing, &cond));
            state
                .backend
                .put_with_condition_raw(
                    &shape,
                    &plan.pk,
                    plan.sk.as_ref(),
                    &plan.item_json,
                    eval,
                )
                .await?
        }
    };

    let attributes = put_delete_attributes(plan.return_values, old_item)?;
    Ok(json_ok(&PutItemResponse { attributes }))
}

/// Project the backend-returned pre-image into the `Attributes` field
/// per `ReturnValues`. Returns `None` for `ReturnValuesMode::None` and
/// for `ALL_OLD` when no row existed. The other `ReturnValuesMode`
/// variants are UpdateItem-only and rejected at translate time, so
/// reaching them here is a translator/dispatcher drift bug.
fn put_delete_attributes(
    mode: ReturnValuesMode,
    old_item: Option<serde_json::Value>,
) -> Result<Option<Item>, ApiError> {
    match mode {
        ReturnValuesMode::None => Ok(None),
        ReturnValuesMode::AllOld => match old_item {
            None => Ok(None),
            Some(v) => Ok(Some(serde_json::from_value(v).map_err(|e| {
                ApiError::Internal(format!("stored item is not valid DynamoDB-JSON: {e}"))
            })?)),
        },
        ReturnValuesMode::AllNew | ReturnValuesMode::UpdatedNew | ReturnValuesMode::UpdatedOld => {
            Err(ApiError::Internal(format!(
                "unreachable ReturnValues variant for Put/Delete: {mode:?}"
            )))
        }
    }
}

#[tracing::instrument(level = "debug", skip_all, name = "server.get_item", fields(table = tracing::field::Empty))]
async fn handle_get_item(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: GetItemRequest = serde_json::from_slice(body)
        .map_err(|e| ApiError::Serialization(format!("invalid GetItem body: {e}")))?;
    tracing::Span::current().record("table", req.table_name.as_str());

    let schema = state.schemas.get(&req.table_name).ok_or_else(|| {
        ApiError::ResourceNotFound(format!("Table not found: {}", req.table_name))
    })?;

    let plan = translate_get_item(&req, schema)?;
    let stored = state
        .backend
        .get_item_raw(&schema.shape(), &plan.pk, plan.sk.as_ref())
        .await?;

    let item: Option<Item> = match stored {
        Some(v) => Some(serde_json::from_value(v).map_err(|e| {
            ApiError::Internal(format!("stored item is not valid DynamoDB-JSON: {e}"))
        })?),
        None => None,
    };

    let resp = GetItemResponse { item };
    Ok(json_ok(&resp))
}

#[tracing::instrument(level = "debug", skip_all, name = "server.delete_item", fields(table = tracing::field::Empty))]
async fn handle_delete_item(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: DeleteItemRequest = serde_json::from_slice(body)
        .map_err(|e| ApiError::Serialization(format!("invalid DeleteItem body: {e}")))?;
    tracing::Span::current().record("table", req.table_name.as_str());

    let schema = state.schemas.get(&req.table_name).ok_or_else(|| {
        ApiError::ResourceNotFound(format!("Table not found: {}", req.table_name))
    })?;

    let plan = translate_delete_item(&req, schema)?;
    let shape = schema.shape();

    let old_item = match plan.condition.as_ref() {
        None => {
            state
                .backend
                .delete_item_raw(&shape, &plan.pk, plan.sk.as_ref())
                .await?
        }
        Some(cond_plan) => {
            let cond = cond_plan.condition.clone();
            let eval: rekt_storage::ConditionEvalFn<'_> =
                Box::new(move |existing| evaluate_condition(existing, &cond));
            state
                .backend
                .delete_with_condition_raw(&shape, &plan.pk, plan.sk.as_ref(), eval)
                .await?
        }
    };

    let attributes = put_delete_attributes(plan.return_values, old_item)?;
    Ok(json_ok(&DeleteItemResponse { attributes }))
}

#[tracing::instrument(level = "debug", skip_all, name = "server.update_item", fields(table = tracing::field::Empty))]
async fn handle_update_item(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: UpdateItemRequest = serde_json::from_slice(body)
        .map_err(|e| ApiError::Serialization(format!("invalid UpdateItem body: {e}")))?;
    tracing::Span::current().record("table", req.table_name.as_str());

    let schema = state.schemas.get(&req.table_name).ok_or_else(|| {
        ApiError::ResourceNotFound(format!("Table not found: {}", req.table_name))
    })?;

    let plan = translate_update_item(&req, schema)?;

    // Slow-path predicate: the fast paths require *both* a simple
    // UpdateExpression (top-level SET=literal + REMOVE only) *and* a
    // SQL-expressible condition shape. Anything outside that —
    // non-simple SET RHS or NeedsTx-classified condition — falls to
    // the Tx-based read-modify-write path.
    let needs_slow_path = !plan.expression.is_simple()
        || plan
            .condition
            .as_ref()
            .is_some_and(|c| c.routing == ConditionRouting::NeedsTx);

    if needs_slow_path {
        return dispatch_slow_path(state, schema, &plan, &req.key).await;
    }

    // Dispatch on ConditionExpression routing. Phase 3a handles the no-
    // condition case; Phase 4c handles `attribute_not_exists(pk)`; Phase
    // 4d handles SimpleSql; the slow-path branch above caught NeedsTx.
    let outcome = match plan.condition.as_ref().map(|c| c.routing) {
        None => {
            let prims = materialize_simple_update(&plan, &req.key)?;
            let sets_borrowed: Vec<(&str, &serde_json::Value)> = prims
                .sets
                .iter()
                .map(|(name, val)| (name.as_str(), val))
                .collect();
            let removes_borrowed: Vec<&str> = prims.removes.iter().map(String::as_str).collect();
            state
                .backend
                .update_simple_raw(
                    &schema.shape(),
                    &plan.pk,
                    plan.sk.as_ref(),
                    &prims.insert_item,
                    &sets_borrowed,
                    &removes_borrowed,
                )
                .await?
        }
        Some(ConditionRouting::InsertOnlyOnPk) => {
            let insert_item = materialize_insert_only_update(&plan, &req.key)?;
            state
                .backend
                .update_insert_only_raw(&schema.shape(), &insert_item)
                .await?
        }
        Some(ConditionRouting::SimpleSql) => {
            let prims = materialize_simple_sql_update(&plan)?;
            let sets_borrowed: Vec<(&str, &serde_json::Value)> = prims
                .sets
                .iter()
                .map(|(name, val)| (name.as_str(), val))
                .collect();
            let removes_borrowed: Vec<&str> = prims.removes.iter().map(String::as_str).collect();
            // `plan.condition` is guaranteed Some by the outer match arm;
            // unwrap is safe.
            let cond = &plan
                .condition
                .as_ref()
                .expect("SimpleSql arm implies condition.is_some()")
                .condition;
            state
                .backend
                .update_with_simple_condition_raw(
                    &schema.shape(),
                    &plan.pk,
                    plan.sk.as_ref(),
                    &sets_borrowed,
                    &removes_borrowed,
                    cond,
                )
                .await?
        }
        Some(ConditionRouting::NeedsTx) => unreachable!(
            "NeedsTx is handled by the slow-path branch above"
        ),
    };

    let paths = touched_paths(&plan.expression);
    Ok(json_ok(&build_update_response(
        plan.return_values,
        outcome,
        &paths,
    )?))
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.query",
    fields(table = tracing::field::Empty, consistent_read = tracing::field::Empty)
)]
async fn handle_query(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: QueryRequest = serde_json::from_slice(body)
        .map_err(|e| ApiError::Serialization(format!("invalid Query body: {e}")))?;
    tracing::Span::current().record("table", req.table_name.as_str());
    // ConsistentRead is silently honored — PG's snapshot isolation is
    // already at least as strong as DDB's strongly-consistent mode for
    // single-statement reads. Surface the flag in tracing so parallel-
    // deployment debugging can see it landed; see COMPATIBILITY_NOTES.md.
    if let Some(cr) = req.consistent_read {
        tracing::Span::current().record("consistent_read", cr);
    }

    let schema = state.schemas.get(&req.table_name).ok_or_else(|| {
        ApiError::ResourceNotFound(format!("Table not found: {}", req.table_name))
    })?;

    let plan = translate_query(&req, schema)?;
    let esk_pair = plan
        .esk_sk
        .as_ref()
        .map(|sk| (&plan.pk, Some(sk)))
        .or_else(|| {
            // Hash-only ESK: the translator decodes esk_sk = None for a
            // hash-only table, but the storage layer still needs to see
            // "ESK present" to short-circuit. Detect via the raw request.
            req.exclusive_start_key
                .as_ref()
                .map(|_| (&plan.pk, None::<&rekt_storage::KeyValue>))
        });
    // FilterExpression: wrap evaluate_condition into a `Fn(&Value)`
    // closure (Query rows are never None; the underlying eval accepts
    // Option<&Value>, so we lift each row into Some). Same pattern as
    // the conditional Put/Delete closures.
    let filter_fn: Option<rekt_storage::FilterEvalFn<'_>> =
        plan.filter.as_ref().map(|cp| {
            let cond = cp.condition.clone();
            let f: rekt_storage::FilterEvalFn<'_> =
                Box::new(move |row| evaluate_condition(Some(row), &cond));
            f
        });
    let outcome = state
        .backend
        .query_raw(
            &schema.shape(),
            &plan.pk,
            plan.sk_condition.as_ref(),
            plan.limit,
            esk_pair,
            filter_fn,
        )
        .await?;

    let items: Vec<Item> = outcome
        .items
        .into_iter()
        .map(|v| {
            serde_json::from_value::<Item>(v).map_err(|e| {
                ApiError::Internal(format!("stored item is not valid DynamoDB-JSON: {e}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let last_evaluated_key = outcome
        .last_evaluated_key
        .as_ref()
        .map(|(pk, sk)| encode_lek(pk, sk.as_ref(), schema));

    Ok(json_ok(&QueryResponse {
        items,
        count: outcome.count,
        scanned_count: outcome.scanned_count,
        last_evaluated_key,
    }))
}

/// Shape the UpdateItem response according to `ReturnValues`. Supports
/// all five DDB variants. The `touched` set is consulted only for the
/// `UPDATED_*` projected modes — caller can pass an empty set when the
/// mode doesn't need it.
fn build_update_response(
    mode: ReturnValuesMode,
    outcome: UpdateOutcome,
    touched: &std::collections::BTreeSet<String>,
) -> Result<UpdateItemResponse, ApiError> {
    let to_item = |label: &'static str, v: serde_json::Value| -> Result<Item, ApiError> {
        serde_json::from_value(v).map_err(|e| {
            ApiError::Internal(format!(
                "{label}: stored item is not valid DynamoDB-JSON: {e}"
            ))
        })
    };
    match mode {
        ReturnValuesMode::None => Ok(UpdateItemResponse::default()),
        ReturnValuesMode::AllNew => Ok(UpdateItemResponse {
            attributes: Some(to_item("ReturnValues=ALL_NEW", outcome.new_item)?),
        }),
        ReturnValuesMode::AllOld => Ok(UpdateItemResponse {
            // DDB returns no `Attributes` if the call created a new
            // row — there was no pre-update item to surface.
            attributes: outcome
                .old_item
                .map(|v| to_item("ReturnValues=ALL_OLD", v))
                .transpose()?,
        }),
        ReturnValuesMode::UpdatedNew => Ok(UpdateItemResponse {
            attributes: project_attributes(
                "ReturnValues=UPDATED_NEW",
                Some(outcome.new_item),
                touched,
            )?,
        }),
        ReturnValuesMode::UpdatedOld => Ok(UpdateItemResponse {
            attributes: project_attributes(
                "ReturnValues=UPDATED_OLD",
                outcome.old_item,
                touched,
            )?,
        }),
    }
}

/// Restrict `source` (a stored DDB-JSON item or `None`) to the
/// `touched` top-level attribute set. Returns `None` (so the response
/// omits `Attributes`) when the source is `None` OR when none of the
/// touched attributes are present — matches DDB's behavior on a
/// REMOVE-only UpdateExpression's `UPDATED_NEW` (the removed attrs no
/// longer exist) and a fresh-insert's `UPDATED_OLD`.
fn project_attributes(
    label: &'static str,
    source: Option<serde_json::Value>,
    touched: &std::collections::BTreeSet<String>,
) -> Result<Option<Item>, ApiError> {
    let Some(v) = source else { return Ok(None) };
    let obj = match v {
        serde_json::Value::Object(o) => o,
        // Stored values are always JSON objects in this codebase.
        other => {
            return Err(ApiError::Internal(format!(
                "{label}: stored value is not a JSON object (got {other:?})"
            )));
        }
    };
    let mut projected = serde_json::Map::new();
    for name in touched {
        if let Some(val) = obj.get(name) {
            projected.insert(name.clone(), val.clone());
        }
    }
    if projected.is_empty() {
        return Ok(None);
    }
    let item: Item = serde_json::from_value(serde_json::Value::Object(projected))
        .map_err(|e| ApiError::Internal(format!("{label}: projection decode failed: {e}")))?;
    Ok(Some(item))
}

/// Slow-path orchestration (Phase 3b): builds a closure that, given the
/// row the backend reads under `SELECT FOR UPDATE`, evaluates the
/// ConditionExpression (if any) and applies the UpdateExpression. The
/// backend's transaction handles atomicity + race-on-insert retry.
async fn dispatch_slow_path(
    state: &AppState,
    schema: &TableSchema,
    plan: &rekt_translator::UpdateItemPlan,
    key: &rekt_protocol::Item,
) -> Result<Response, ApiError> {
    // Move-by-clone into the closure; the backend may invoke it multiple
    // times on a race. Borrowed lifetimes are awkward through dyn Fn, so
    // we pay the small clone cost.
    let expr = plan.expression.clone();
    let condition = plan.condition.as_ref().map(|c| c.condition.clone());
    let key_owned = key.clone();

    let apply = move |existing: Option<&serde_json::Value>| {
        if let Some(c) = &condition {
            if !evaluate_condition(existing, c) {
                return Ok(UpdateDecision::Fail);
            }
        }
        match apply_update_expression(existing, &expr, &key_owned) {
            Ok(new_item) => Ok(UpdateDecision::Apply(new_item)),
            Err(e) => Err(BackendError::Other(e.to_string())),
        }
    };

    let outcome = state
        .backend
        .update_general_rmw_raw(&schema.shape(), &plan.pk, plan.sk.as_ref(), Box::new(apply))
        .await?;

    let paths = touched_paths(&plan.expression);
    Ok(json_ok(&build_update_response(
        plan.return_values,
        outcome,
        &paths,
    )?))
}

fn json_ok<T: serde::Serialize>(value: &T) -> Response {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(CONTENT_TYPE),
    );
    response
}
