//! TransactWriteItems — atomic cross-table write inside a single PG
//! transaction at `REPEATABLE READ`. Per-item dispatch:
//!
//! - `Put` (unconditional): `put_item_inside_tx`
//! - `Put` (conditional):   `put_with_condition_inside_tx`
//! - `Delete`, `ConditionCheck`, `Update`: future phases (T4 / T5).
//!
//! On the first per-op failure, the whole tx is aborted (RAII drop
//! on `Transaction<'_>`) and we return
//! `BackendError::TransactionCancelled` with a position-indexed
//! `Vec<TransactCancelReason>` so the HTTP layer can render DDB's
//! `TransactionCanceledException` wire shape (T6).

use crate::put_delete::{
    condition_check_inside_tx, delete_item_inside_tx, delete_with_condition_inside_tx,
    put_item_inside_tx, put_with_condition_inside_tx,
};
use crate::update::update_general_rmw_inside_tx;
use crate::types::map_pg_err;
use crate::PgBackend;
use rekt_storage::{
    BackendError, TransactCancelReason, TransactWriteOp, TransactWriteOutcome,
};
use tokio_postgres::IsolationLevel;
use tracing::Instrument;

#[tracing::instrument(level = "debug", skip_all, name = "pg.transact_write_raw", fields(items = items.len()))]
pub(crate) async fn transact_write_raw(
    backend: &PgBackend,
    items: Vec<TransactWriteOp<'_>>,
) -> Result<TransactWriteOutcome, BackendError> {
    let n = items.len();
    let mut client = backend.client().await?;

    // D2 in PLAN-8: REPEATABLE READ for cross-row consistency. Our
    // explicit SELECT FOR UPDATE inside every condition-bearing op
    // closes the write-skew gap that REPEATABLE READ leaves open.
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .instrument(tracing::debug_span!("pg.begin"))
        .await
        .map_err(|e| map_pg_err("<transact>", e))?;

    // Iterate in request order; on first failure, build the
    // CancellationReasons[] array and bail.
    for (i, op) in items.iter().enumerate() {
        let result: Result<(), BackendError> = match op {
            TransactWriteOp::Put {
                shape,
                pk,
                sk,
                item,
                condition,
                ..
            } => match condition.as_ref() {
                None => {
                    put_item_inside_tx(backend, &tx, shape, pk, sk.as_ref(), item)
                        .await
                        .map(|_| ())
                }
                Some(cond) => {
                    put_with_condition_inside_tx(&tx, shape, pk, sk.as_ref(), item, cond)
                        .await
                        .map(|_| ())
                }
            },
            TransactWriteOp::Delete {
                shape,
                pk,
                sk,
                condition,
                ..
            } => match condition.as_ref() {
                None => {
                    delete_item_inside_tx(backend, &tx, shape, pk, sk.as_ref())
                        .await
                        .map(|_| ())
                }
                Some(cond) => {
                    delete_with_condition_inside_tx(&tx, shape, pk, sk.as_ref(), cond)
                        .await
                        .map(|_| ())
                }
            },
            TransactWriteOp::ConditionCheck {
                shape,
                pk,
                sk,
                condition,
                ..
            } => condition_check_inside_tx(&tx, shape, pk, sk.as_ref(), condition)
                .await
                .map(|_| ()),
            TransactWriteOp::Update {
                shape, pk, sk, apply, ..
            } => update_general_rmw_inside_tx(&tx, shape, pk, sk.as_ref(), apply)
                .await
                .map(|_| ()),
        };
        if let Err(err) = result {
            return Err(into_cancelled(err, i, n, false, items));
        }
    }

    tx.commit()
        .instrument(tracing::debug_span!("pg.commit"))
        .await
        .map_err(|e| map_pg_err("<transact>", e))?;

    Ok(TransactWriteOutcome::default())
}

/// Map a single-op `BackendError` to a `TransactionCancelled` with a
/// fully-populated `CancellationReasons` array. The failing slot
/// `failed_idx` carries the specific code; every other slot is
/// `code: "None"` so the wire array is parallel to the request.
///
/// `return_old_on_failure` is honored on the failing slot only:
/// when set AND the failure was `ConditionalCheckFailed`, the
/// pre-image — which the inside_tx helper already captured before
/// invoking the condition — would normally be threaded back here.
/// In this v1 we don't surface it through the BackendError →
/// CancellationReason path (the closure-eval signature doesn't
/// return the row on failure); T6's wire-shape polish revisits this.
fn into_cancelled(
    err: BackendError,
    failed_idx: usize,
    n: usize,
    _return_old_on_failure: bool,
    _items: Vec<TransactWriteOp<'_>>,
) -> BackendError {
    let (code, message) = classify_for_cancel(&err);
    let mut reasons: Vec<TransactCancelReason> = (0..n)
        .map(|_| TransactCancelReason {
            code: "None",
            message: None,
            item: None,
        })
        .collect();
    reasons[failed_idx] = TransactCancelReason {
        code,
        message: Some(message),
        item: None,
    };
    BackendError::TransactionCancelled { reasons }
}

/// Pick the right CancellationReason `Code` for a given storage
/// error. The strings match the DDB error codes documented in the
/// `TransactionCanceledException` spec.
fn classify_for_cancel(err: &BackendError) -> (&'static str, String) {
    match err {
        BackendError::ConditionalCheckFailed => (
            "ConditionalCheckFailed",
            "The conditional request failed".to_string(),
        ),
        BackendError::PgError { sqlstate, message } => {
            // 40001 / 40P01 → TransactionConflict.
            let code: &'static str =
                if sqlstate == "40001" || sqlstate == "40P01" {
                    "TransactionConflict"
                } else {
                    "InternalServerError"
                };
            (code, format!("PG error ({sqlstate}): {message}"))
        }
        BackendError::TableNotFound { name } => (
            "ResourceNotFound",
            format!("Table not found: {name}"),
        ),
        other => ("InternalServerError", other.to_string()),
    }
}
