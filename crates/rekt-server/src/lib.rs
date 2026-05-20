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
    BatchGetItemRequest, BatchGetItemResponse, BatchWriteItemRequest, BatchWriteItemResponse,
    DeleteItemRequest, DeleteItemResponse, GetItemRequest, GetItemResponse, Item, PutItemRequest,
    PutItemResponse, QueryRequest, QueryResponse, ScanRequest, ScanResponse,
    TransactGetItemResponse, TransactGetItemsRequest, TransactGetItemsResponse,
    TransactWriteItemsRequest, TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse,
    WriteRequest,
};
use rekt_catalog::{CatalogSnapshot, TableCatalog, TableEntry};
use rekt_sigv4::{SigV4Error, Verifier};
use rekt_storage::{Backend, BackendError, TransactGetOp, TransactWriteOp, UpdateDecision, UpdateOutcome};
use rekt_translator::{
    apply_update_expression, encode_lek, evaluate_condition, materialize_insert_only_update,
    materialize_simple_sql_update, materialize_simple_update, touched_paths,
    translate_batch_get_item, translate_batch_write_item, translate_delete_item,
    translate_get_item, translate_put_item, translate_query, translate_scan,
    translate_transact_get_items, translate_transact_write_items, translate_update_item,
    ConditionRouting, ReturnValuesMode, TableSchema, TransactWriteKind, TranslateError,
};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

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
    /// Runtime table catalog: the single source of truth for which
    /// tables exist and what shape they have. Refreshed by the
    /// reconciler (PLAN-10 D3) and on local DDL completion. Replaces
    /// the immutable `Arc<HashMap<String, TableSchema>>` of pre-D2 —
    /// see PLAN-10 KD4.
    pub catalog: Arc<TableCatalog>,
    /// Per-call quantity caps on the batch ops. Defaults to DDB
    /// values (100 / 25); operator can override via `[batch_limits]`
    /// in `rektifier.toml`. See `COMPATIBILITY_NOTES.md`.
    pub batch_limits: BatchLimits,
    /// Per-request hard timeout. Wired into a `TimeoutLayer`; a
    /// handler exceeding this returns 503 (tower-http's default) and
    /// `TraceLayer::on_failure` logs the timeout. Defaults to 30s
    /// for tests; the binary reads `[server].request_timeout_ms`
    /// from `rektifier.toml`.
    pub request_timeout: Duration,
}

/// Server-side projection of `rekt_config::BatchLimits`. Carried in
/// `AppState` and threaded into per-call translators that need it
/// (currently `translate_batch_get_item`; `translate_batch_write_item`
/// will plug in at B4).
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub batch_get_max_keys: u32,
    pub batch_write_max_requests: u32,
    pub transact_get_max_items: u32,
    pub transact_write_max_items: u32,
}

impl Default for BatchLimits {
    /// DDB defaults — matches `rekt_config::BatchLimits::ddb_defaults()`.
    /// Useful in tests that construct an `AppState` without going
    /// through `rektifier.toml`.
    fn default() -> Self {
        Self {
            batch_get_max_keys: 100,
            batch_write_max_requests: 25,
            transact_get_max_items: 100,
            transact_write_max_items: 100,
        }
    }
}

/// Build the axum router with the resilience middleware stack.
///
/// Layer order (axum applies outer-to-inner):
/// 1. `CatchPanicLayer` (outermost) — a panic anywhere inside is
///    caught and rendered as a structured 500 with an error log,
///    rather than dropping the TCP connection.
/// 2. `TraceLayer` — HTTP-level access logging at info; emits a log
///    line per request including method/path/status/duration.
/// 3. `TimeoutLayer` — per-request hard cap. tower-http converts
///    excess into a 408 Request Timeout.
/// 4. `RequestBodyLimitLayer` (innermost) — runs before any
///    extraction so over-size bodies are rejected before parsing.
pub fn router(state: AppState) -> Router {
    let timeout = state.request_timeout;
    Router::new()
        .route("/", post(dispatch))
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(CatchPanicLayer::custom(panic_to_500))
        .with_state(state)
}

/// `CatchPanicLayer` handler: render the panic as a DDB-shaped
/// InternalServerError + log at error. Without this, axum drops the
/// connection on panic and the SDK sees a transport error with no
/// way to correlate to the cause.
fn panic_to_500(err: Box<dyn Any + Send + 'static>) -> Response {
    // Best-effort downcast: the panic payload is typically String or
    // &str. Anything else gets a generic placeholder.
    let detail: String = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        "panic with non-string payload".to_string()
    };
    tracing::error!(panic = %detail, "handler panicked — returning 500");
    ApiError::Internal(format!("panic: {detail}")).into_response()
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

    #[error("TransactionCanceledException")]
    TransactionCancelled {
        reasons: Vec<rekt_storage::TransactCancelReason>,
    },

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
            Self::TransactionCancelled { .. } => "TransactionCanceledException",
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
            Self::TransactionCancelled { reasons } => {
                // DDB's verbatim wording — references the populated
                // failure codes so clients can read it directly.
                let codes: Vec<&str> = reasons.iter().map(|r| r.code).collect();
                format!(
                    "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
                    codes.join(", ")
                )
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = match &self {
            ApiError::TransactionCancelled { reasons } => {
                let cancel_array: Vec<serde_json::Value> = reasons
                    .iter()
                    .map(|r| {
                        let mut obj = serde_json::Map::new();
                        obj.insert("Code".into(), serde_json::Value::String(r.code.into()));
                        if let Some(msg) = &r.message {
                            obj.insert(
                                "Message".into(),
                                serde_json::Value::String(msg.clone()),
                            );
                        }
                        if let Some(item) = &r.item {
                            obj.insert("Item".into(), item.clone());
                        }
                        serde_json::Value::Object(obj)
                    })
                    .collect();
                serde_json::json!({
                    "__type": format!(
                        "com.amazonaws.dynamodb.v20120810#{}",
                        self.ddb_error_name()
                    ),
                    "Message": self.message(),
                    "CancellationReasons": cancel_array,
                })
            }
            _ => serde_json::json!({
                "__type": format!("com.amazonaws.dynamodb.v20120810#{}", self.ddb_error_name()),
                "message": self.message(),
            }),
        };
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
        match e {
            // BatchGetItem can name many tables; the per-table missing
            // lookup happens inside the translator (the dispatcher
            // can't pre-resolve a single schema). Map back to the
            // ResourceNotFoundException wire-shape DDB uses for the
            // single-row ops' equivalent error.
            TranslateError::ResourceNotFoundForBatch { table } => {
                ApiError::ResourceNotFound(format!("Table not found: {table}"))
            }
            TranslateError::ResourceNotFoundForTransact { table } => {
                ApiError::ResourceNotFound(format!("Table not found: {table}"))
            }
            other => ApiError::Validation(other.to_string()),
        }
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
            // Pool starvation: the storage layer already logged at
            // warn in classify_pool_error. Surface as 500 with a
            // hint-shaped message — operators reading the wire
            // response can correlate to the pool log line by the
            // waited_ms field.
            BackendError::PoolExhausted { waited_ms } => {
                tracing::error!(
                    waited_ms,
                    "PG pool exhausted -> InternalServerError (500)"
                );
                ApiError::Internal(format!(
                    "PG connection pool exhausted after {waited_ms} ms"
                ))
            }
            BackendError::ConnectFailed { reason } => {
                tracing::error!(
                    reason = %reason,
                    "PG connect failed -> InternalServerError (500)"
                );
                ApiError::Internal(format!("PG connection failed: {reason}"))
            }
            // PG-layer error reaches us after the retry layer
            // already exhausted its budget. map_pg_err logged it at
            // the per-attempt level; emit a final 500-boundary log
            // so operators see the surfaced-to-client form.
            BackendError::PgError { sqlstate, message } => {
                tracing::error!(
                    sqlstate = %sqlstate,
                    pg_message = %message,
                    "PG error -> InternalServerError (500) after retries exhausted"
                );
                ApiError::Internal(format!("PG error ({sqlstate}): {message}"))
            }
            // Every Other -> ApiError::Internal becomes a 500. The PG
            // layer already logged the detailed cause in `map_pg_err`;
            // this site emits an error-level boundary log so operators
            // reading the API log can correlate a 500 response with
            // the storage-layer error that produced it.
            BackendError::Other(m) => {
                tracing::error!(
                    backend_error = %m,
                    "backend error surfaced as InternalServerError (500)"
                );
                ApiError::Internal(m)
            }
            BackendError::TransactionCancelled { reasons } => {
                tracing::warn!(
                    failing_codes = ?reasons.iter().map(|r| r.code).collect::<Vec<_>>(),
                    "TransactWriteItems cancelled -> TransactionCanceledException"
                );
                ApiError::TransactionCancelled { reasons }
            }
        }
    }
}

impl From<SigV4Error> for ApiError {
    fn from(e: SigV4Error) -> Self {
        ApiError::AccessDenied(e.to_string())
    }
}

/// Catalog gate (PLAN-10 D4 / KD5). Single-table handlers call this
/// instead of fetching the entry directly: unknown OR unserveable both
/// surface as `ResourceNotFoundException`, with the unserveable reason
/// folded into the message so operators tailing logs / running the AWS
/// CLI can see what's actually wrong.
///
/// KD5 rationale: from the client's perspective, an unserveable table
/// is functionally equivalent to a missing one. DDB SDK retry logic
/// for RNF often does the right thing — by the time the retry lands,
/// the reconciler may have flipped serveable back.
fn require_serveable(
    catalog: &TableCatalog,
    table_name: &str,
) -> Result<Arc<TableEntry>, ApiError> {
    let entry = catalog.get(table_name).ok_or_else(|| {
        ApiError::ResourceNotFound(format!("Table not found: {table_name}"))
    })?;
    gate_serveable(&entry, table_name)?;
    Ok(entry)
}

/// Multi-table variant: same wire shape (RNF on miss or unserveable)
/// but reads from a `CatalogSnapshot` so the multi-table handler can
/// pin a single coherent view across all referenced tables.
fn require_serveable_in_snapshot<'a>(
    snapshot: &'a CatalogSnapshot,
    table_name: &str,
) -> Result<&'a Arc<TableEntry>, ApiError> {
    let entry = snapshot.entries.get(table_name).ok_or_else(|| {
        ApiError::ResourceNotFound(format!("Table not found: {table_name}"))
    })?;
    gate_serveable(entry, table_name)?;
    Ok(entry)
}

fn gate_serveable(entry: &TableEntry, table_name: &str) -> Result<(), ApiError> {
    if entry.serveable {
        return Ok(());
    }
    Err(ApiError::ResourceNotFound(format!(
        "Table not currently serveable: {table_name} ({})",
        entry
            .unserveable_reason
            .as_deref()
            .unwrap_or("unknown reason")
    )))
}

/// Multi-table gate: every referenced table must exist AND be serveable.
/// Used pre-translate by BatchGet/Write and TransactGet/Write so the
/// "not currently serveable" wire message wins over the translator's
/// generic ResourceNotFound (which would say "table not found" instead,
/// hiding the actionable reason from the operator).
///
/// PLAN-10 KD5: any unserveable referenced table fails the whole batch
/// with RNF — pending diff-harness verification on whether DDB emits
/// per-item UnprocessedKeys / CancellationReasons instead. Logged as
/// Parity-unverified in COMPATIBILITY_NOTES.
fn gate_referenced_tables<'a, I>(
    snapshot: &CatalogSnapshot,
    names: I,
) -> Result<(), ApiError>
where
    I: IntoIterator<Item = &'a str>,
{
    for name in names {
        require_serveable_in_snapshot(snapshot, name)?;
    }
    Ok(())
}

/// Deserialize a request body and log warn-level on failure. Centralizes
/// the "invalid <Op> body: <reason>" message + structured tracing field
/// for every handler so a misbehaving SDK shows up in logs even when
/// the response is just a 4xx SerializationException.
fn parse_request<T: serde::de::DeserializeOwned>(
    body: &Bytes,
    op: &'static str,
) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| {
        tracing::warn!(
            op = op,
            error = %e,
            body_bytes = body.len(),
            "request body parse failed"
        );
        ApiError::Serialization(format!("invalid {op} body: {e}"))
    })
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
        .map_err(|e| {
            // Client cut the connection, exceeded MAX_BODY_BYTES, or
            // an upstream proxy misbehaved. Warn-level — operator
            // wants to see frequency in case a client regresses.
            tracing::warn!(
                error = %e,
                max_body_bytes = MAX_BODY_BYTES,
                "request body read failed"
            );
            ApiError::Serialization(format!("body read failed: {e}"))
        })?;

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
        "Scan" => handle_scan(&state, &body).await,
        "BatchGetItem" => handle_batch_get_item(&state, &body).await,
        "BatchWriteItem" => handle_batch_write_item(&state, &body).await,
        "TransactGetItems" => handle_transact_get_items(&state, &body).await,
        "TransactWriteItems" => handle_transact_write_items(&state, &body).await,
        _ => Err(ApiError::UnknownOperation(format!(
            "operation `{op}` not implemented"
        ))),
    }
}

#[tracing::instrument(level = "debug", skip_all, name = "server.put_item", fields(table = tracing::field::Empty))]
async fn handle_put_item(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: PutItemRequest = parse_request(body, "PutItem")?;
    tracing::Span::current().record("table", req.table_name.as_str());

    let entry = require_serveable(&state.catalog, &req.table_name)?;
    let schema = &entry.schema;

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
    let req: GetItemRequest = parse_request(body, "GetItem")?;
    tracing::Span::current().record("table", req.table_name.as_str());

    let entry = require_serveable(&state.catalog, &req.table_name)?;
    let schema = &entry.schema;

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
    let req: DeleteItemRequest = parse_request(body, "DeleteItem")?;
    tracing::Span::current().record("table", req.table_name.as_str());

    let entry = require_serveable(&state.catalog, &req.table_name)?;
    let schema = &entry.schema;

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
    let req: UpdateItemRequest = parse_request(body, "UpdateItem")?;
    tracing::Span::current().record("table", req.table_name.as_str());

    let entry = require_serveable(&state.catalog, &req.table_name)?;
    let schema = &entry.schema;

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
    let req: QueryRequest = parse_request(body, "Query")?;
    tracing::Span::current().record("table", req.table_name.as_str());
    // ConsistentRead is silently honored — PG's snapshot isolation is
    // already at least as strong as DDB's strongly-consistent mode for
    // single-statement reads. Surface the flag in tracing so parallel-
    // deployment debugging can see it landed; see COMPATIBILITY_NOTES.md.
    if let Some(cr) = req.consistent_read {
        tracing::Span::current().record("consistent_read", cr);
    }

    let entry = require_serveable(&state.catalog, &req.table_name)?;
    let schema = &entry.schema;

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
            plan.forward,
        )
        .await?;

    let items: Vec<Item> = items_to_response(
        outcome.items,
        plan.select_count_only,
        plan.projection.as_ref(),
    )?;

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

/// Apply Q6's count-only + projection post-filter to the backend's
/// returned items. `count_only` drops every item (the wire response
/// will carry `Count` / `ScannedCount` only). `projection` retains
/// only the listed top-level attribute names per item.
fn items_to_response(
    rows: Vec<serde_json::Value>,
    count_only: bool,
    projection: Option<&std::collections::BTreeSet<String>>,
) -> Result<Vec<Item>, ApiError> {
    if count_only {
        return Ok(Vec::new());
    }
    rows.into_iter()
        .map(|v| {
            let v = match projection {
                None => v,
                Some(keep) => prune_to_projection(v, keep),
            };
            serde_json::from_value::<Item>(v).map_err(|e| {
                ApiError::Internal(format!("stored item is not valid DynamoDB-JSON: {e}"))
            })
        })
        .collect()
}

fn prune_to_projection(
    v: serde_json::Value,
    keep: &std::collections::BTreeSet<String>,
) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let pruned: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .filter(|(k, _)| keep.contains(k))
                .collect();
            serde_json::Value::Object(pruned)
        }
        other => other,
    }
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.scan",
    fields(table = tracing::field::Empty, consistent_read = tracing::field::Empty)
)]
async fn handle_scan(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: ScanRequest = parse_request(body, "Scan")?;
    tracing::Span::current().record("table", req.table_name.as_str());
    if let Some(cr) = req.consistent_read {
        tracing::Span::current().record("consistent_read", cr);
    }

    let entry = require_serveable(&state.catalog, &req.table_name)?;
    let schema = &entry.schema;

    let plan = translate_scan(&req, schema)?;

    // ESK: assemble (pk, sk) pair from the plan's decoded keys.
    let esk_pair: Option<(&rekt_storage::KeyValue, Option<&rekt_storage::KeyValue>)> =
        plan.esk_pk.as_ref().map(|pk| (pk, plan.esk_sk.as_ref()));

    // Filter: same closure pattern as handle_query.
    let filter_fn: Option<rekt_storage::FilterEvalFn<'_>> =
        plan.filter.as_ref().map(|cp| {
            let cond = cp.condition.clone();
            let f: rekt_storage::FilterEvalFn<'_> =
                Box::new(move |row| evaluate_condition(Some(row), &cond));
            f
        });

    let outcome = state
        .backend
        .scan_raw(&schema.shape(), filter_fn, plan.limit, esk_pair)
        .await?;

    let items: Vec<Item> = items_to_response(
        outcome.items,
        plan.select_count_only,
        plan.projection.as_ref(),
    )?;

    let last_evaluated_key = outcome
        .last_evaluated_key
        .as_ref()
        .map(|(pk, sk)| encode_lek(pk, sk.as_ref(), schema));

    Ok(json_ok(&ScanResponse {
        items,
        count: outcome.count,
        scanned_count: outcome.scanned_count,
        last_evaluated_key,
    }))
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.batch_get_item",
    fields(tables = tracing::field::Empty, total_keys = tracing::field::Empty)
)]
async fn handle_batch_get_item(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: BatchGetItemRequest = parse_request(body, "BatchGetItem")?;

    let snapshot = state.catalog.snapshot();
    gate_referenced_tables(
        &snapshot,
        req.request_items.keys().map(String::as_str),
    )?;
    let plan = translate_batch_get_item(
        &req,
        &snapshot.schemas,
        state.batch_limits.batch_get_max_keys,
    )?;

    tracing::Span::current().record("tables", plan.per_table.len());
    let total_keys: usize = plan.per_table.iter().map(|t| t.keys.len()).sum();
    tracing::Span::current().record("total_keys", total_keys);

    // Per-table serial fan-out (D1 in PLAN-6). Each table issues one
    // multi-key SQL statement. Missing rows are silently absent from
    // the response. ProjectionExpression / ConsistentRead are applied
    // per-table (B3): the former via the shared `items_to_response`
    // pruning helper, the latter as a tracing field only (D7).
    let mut responses: HashMap<String, Vec<Item>> = HashMap::with_capacity(plan.per_table.len());
    for table_plan in &plan.per_table {
        let schema = snapshot
            .schemas
            .get(&table_plan.table_name)
            .ok_or_else(|| {
                // Translator already validated this against the same
                // snapshot; this branch is a defensive guard against
                // the (impossible in v1) case where the snapshot changed
                // between translate and dispatch.
                ApiError::ResourceNotFound(format!(
                    "Table not found: {}",
                    table_plan.table_name
                ))
            })?;

        if table_plan.consistent_read {
            tracing::debug!(
                table = %table_plan.table_name,
                consistent_read = true,
                "BatchGetItem per-table ConsistentRead silently honored"
            );
        }

        let outcome = state
            .backend
            .batch_get_raw(&schema.shape(), &table_plan.keys)
            .await?;

        // Reuse the same projection-pruning path Query/Scan use, so a
        // future enhancement (nested-path projection from PLAN-2
        // Phase 8) automatically lifts here too.
        let items = items_to_response(
            outcome.items,
            false, /* count_only — BatchGetItem has no Select=COUNT */
            table_plan.projection.as_ref(),
        )?;
        responses.insert(table_plan.table_name.clone(), items);
    }

    Ok(json_ok(&BatchGetItemResponse {
        responses,
        // Always `{}` in v1 — D11 in PLAN-6.
        unprocessed_keys: HashMap::new(),
    }))
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.batch_write_item",
    fields(tables = tracing::field::Empty, total_ops = tracing::field::Empty)
)]
async fn handle_batch_write_item(state: &AppState, body: &Bytes) -> Result<Response, ApiError> {
    let req: BatchWriteItemRequest = parse_request(body, "BatchWriteItem")?;

    let snapshot = state.catalog.snapshot();
    gate_referenced_tables(
        &snapshot,
        req.request_items.keys().map(String::as_str),
    )?;
    let plan = translate_batch_write_item(
        &req,
        &snapshot.schemas,
        state.batch_limits.batch_write_max_requests,
    )?;

    tracing::Span::current().record("tables", plan.per_table.len());
    let total_ops: usize = plan.per_table.iter().map(|t| t.ops.len()).sum();
    tracing::Span::current().record("total_ops", total_ops);

    // Per-table serial fan-out (D1). Each table opens its own
    // transaction inside `batch_write_raw`; cross-table writes are
    // *not* atomic (matches DDB).
    for table_plan in &plan.per_table {
        let schema = snapshot
            .schemas
            .get(&table_plan.table_name)
            .ok_or_else(|| {
                ApiError::ResourceNotFound(format!(
                    "Table not found: {}",
                    table_plan.table_name
                ))
            })?;
        state
            .backend
            .batch_write_raw(&schema.shape(), &table_plan.ops)
            .await?;
    }

    Ok(json_ok(&BatchWriteItemResponse {
        // Always `{}` in v1 — D11 in PLAN-6.
        unprocessed_items: HashMap::<String, Vec<WriteRequest>>::new(),
    }))
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.transact_get_items",
    fields(items = tracing::field::Empty)
)]
async fn handle_transact_get_items(
    state: &AppState,
    body: &Bytes,
) -> Result<Response, ApiError> {
    let req: TransactGetItemsRequest = parse_request(body, "TransactGetItems")?;

    let snapshot = state.catalog.snapshot();
    gate_referenced_tables(
        &snapshot,
        req.transact_items
            .iter()
            .filter_map(|it| it.get.as_ref().map(|g| g.table_name.as_str())),
    )?;
    let plan = translate_transact_get_items(
        &req,
        &snapshot.schemas,
        state.batch_limits.transact_get_max_items,
    )?;

    tracing::Span::current().record("items", plan.items.len());

    // Build the storage-layer op list. Each item carries its own
    // TableShape borrowed from the snapshot's schemas map; the
    // Arc<CatalogSnapshot> keeps the borrow valid for the duration of
    // the call.
    let shapes: Vec<rekt_storage::TableShape<'_>> = plan
        .items
        .iter()
        .map(|p| {
            snapshot
                .schemas
                .get(&p.table_name)
                .map(|s| s.shape())
                .ok_or_else(|| {
                    // Translator already validated this; defensive only.
                    ApiError::ResourceNotFound(format!("Table not found: {}", p.table_name))
                })
        })
        .collect::<Result<_, _>>()?;

    let ops: Vec<TransactGetOp<'_>> = plan
        .items
        .iter()
        .zip(shapes.into_iter())
        .map(|(p, shape)| TransactGetOp {
            shape,
            pk: p.pk.clone(),
            sk: p.sk.clone(),
        })
        .collect();

    let outcome = state.backend.transact_get_raw(&ops).await?;

    // Position-preserving response (D8). Each slot is `{"Item": ...}`
    // on hit or `{}` on miss. Projection (T2) prunes per-item before
    // shaping into the wire `Item`.
    let mut responses: Vec<TransactGetItemResponse> = Vec::with_capacity(outcome.items.len());
    for (slot, plan_item) in outcome.items.into_iter().zip(plan.items.iter()) {
        let item = match slot {
            None => None,
            Some(v) => {
                let pruned = match plan_item.projection.as_ref() {
                    None => v,
                    Some(keep) => prune_to_projection(v, keep),
                };
                Some(serde_json::from_value::<Item>(pruned).map_err(|e| {
                    ApiError::Internal(format!(
                        "TransactGetItems: stored item is not valid DynamoDB-JSON: {e}"
                    ))
                })?)
            }
        };
        responses.push(TransactGetItemResponse { item });
    }

    Ok(json_ok(&TransactGetItemsResponse { responses }))
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    name = "server.transact_write_items",
    fields(items = tracing::field::Empty)
)]
async fn handle_transact_write_items(
    state: &AppState,
    body: &Bytes,
) -> Result<Response, ApiError> {
    let req: TransactWriteItemsRequest = parse_request(body, "TransactWriteItems")?;

    if let Some(t) = req.client_request_token.as_deref() {
        tracing::debug!(
            client_request_token_len = t.len(),
            "TransactWriteItems client_request_token accepted-and-dropped (T8 implements real idempotency)"
        );
    }

    let snapshot = state.catalog.snapshot();
    gate_referenced_tables(
        &snapshot,
        req.transact_items.iter().filter_map(|it| {
            if let Some(p) = it.put.as_ref() {
                Some(p.table_name.as_str())
            } else if let Some(d) = it.delete.as_ref() {
                Some(d.table_name.as_str())
            } else if let Some(u) = it.update.as_ref() {
                Some(u.table_name.as_str())
            } else {
                it.condition_check.as_ref().map(|c| c.table_name.as_str())
            }
        }),
    )?;
    let plan = translate_transact_write_items(
        &req,
        &snapshot.schemas,
        state.batch_limits.transact_write_max_items,
    )?;
    tracing::Span::current().record("items", plan.items.len());

    // Build the storage-level ops, attaching ConditionEvalFn closures
    // per-item. Each shape is borrowed from the snapshot's schemas
    // map; the Arc<CatalogSnapshot> keeps the borrow valid for the
    // call's lifetime, which is also the ConditionEvalFn lifetime ('a).
    let mut ops: Vec<TransactWriteOp<'_>> = Vec::with_capacity(plan.items.len());
    for plan_item in plan.items.into_iter() {
        let schema = snapshot.schemas.get(&plan_item.table_name).ok_or_else(|| {
            ApiError::ResourceNotFound(format!("Table not found: {}", plan_item.table_name))
        })?;
        let shape = schema.shape();
        let condition_fn: Option<rekt_storage::ConditionEvalFn<'_>> =
            plan_item.condition.as_ref().map(|cp| {
                let cond = cp.condition.clone();
                let f: rekt_storage::ConditionEvalFn<'_> =
                    Box::new(move |existing| evaluate_condition(existing, &cond));
                f
            });
        match plan_item.kind {
            TransactWriteKind::Put { item_json } => {
                ops.push(TransactWriteOp::Put {
                    shape,
                    pk: plan_item.pk,
                    sk: plan_item.sk,
                    item: item_json,
                    condition: condition_fn,
                    return_old_on_failure: plan_item.return_old_on_failure,
                });
            }
            TransactWriteKind::Delete => {
                ops.push(TransactWriteOp::Delete {
                    shape,
                    pk: plan_item.pk,
                    sk: plan_item.sk,
                    condition: condition_fn,
                    return_old_on_failure: plan_item.return_old_on_failure,
                });
            }
            TransactWriteKind::ConditionCheck => {
                let cond = condition_fn.expect(
                    "translator guarantees ConditionCheck has a condition",
                );
                ops.push(TransactWriteOp::ConditionCheck {
                    shape,
                    pk: plan_item.pk,
                    sk: plan_item.sk,
                    condition: cond,
                    return_old_on_failure: plan_item.return_old_on_failure,
                });
            }
            TransactWriteKind::Update { expression, key } => {
                // Build a GeneralUpdateFn that evaluates the optional
                // ConditionExpression (Fail on false) and then applies
                // the parsed UpdateExpression to produce the new row.
                // `Fn` (not `FnOnce`) so the storage layer could call
                // it inside a retry loop — though the in-tx variant
                // is single-pass.
                let condition_for_closure = plan_item.condition.as_ref().map(|c| c.condition.clone());
                let apply: rekt_storage::GeneralUpdateFn<'_> = Box::new(move |existing| {
                    if let Some(cond) = &condition_for_closure {
                        if !evaluate_condition(existing, cond) {
                            return Ok(UpdateDecision::Fail);
                        }
                    }
                    let new_item = apply_update_expression(existing, &expression, &key)
                        .map_err(|e| BackendError::Other(format!("UpdateExpression eval failed: {e}")))?;
                    Ok(UpdateDecision::Apply(new_item))
                });
                ops.push(TransactWriteOp::Update {
                    shape,
                    pk: plan_item.pk,
                    sk: plan_item.sk,
                    apply,
                    return_old_on_failure: plan_item.return_old_on_failure,
                });
            }
        }
    }

    let _outcome = state.backend.transact_write_raw(ops).await?;

    // DDB emits an empty body on success.
    Ok(json_ok(&TransactWriteItemsResponse::default()))
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
