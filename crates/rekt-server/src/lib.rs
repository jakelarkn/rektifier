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
    PutItemResponse,
};
use rekt_sigv4::{SigV4Error, Verifier};
use rekt_storage::{Backend, BackendError};
use rekt_translator::{
    translate_delete_item, translate_get_item, translate_put_item, TableSchema, TranslateError,
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
    state
        .backend
        .put_item_raw(&schema.shape(), &plan.item_json)
        .await?;

    let resp = PutItemResponse::default();
    Ok(json_ok(&resp))
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
    state
        .backend
        .delete_item_raw(&schema.shape(), &plan.pk, plan.sk.as_ref())
        .await?;

    let resp = DeleteItemResponse::default();
    Ok(json_ok(&resp))
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
