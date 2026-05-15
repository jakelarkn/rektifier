//! PutItem / GetItem / DeleteItem method bodies (unconditional + their
//! `_with_condition_raw` siblings). The unconditional paths drive their
//! cached SQL off [`crate::shape::ShapeSql`]; the conditional paths run
//! a SELECT-FOR-UPDATE tx via [`crate::update::RmwSqlPrep`].

use crate::types::{cast_for_keytype, check_sk_shape, map_pg_err, quote_ident, Bound};
use crate::update::RmwSqlPrep;
use crate::PgBackend;
use rekt_storage::{BackendError, ConditionEvalFn, KeyValue, TableShape};
use tokio_postgres::types::{Json, Type};
use tracing::Instrument;

#[tracing::instrument(level = "debug", skip_all, name = "pg.put_item_raw", fields(table = %shape.table))]
pub(crate) async fn put_item_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    item: &serde_json::Value,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let sql = backend.shape_sql(shape);
    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql.put_sql, &sql.put_types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let pk_bound = Bound(pk);
    let item_json = Json(item);
    let row = match sk {
        None => {
            client
                .query_one(&stmt, &[&pk_bound, &item_json])
                .instrument(tracing::debug_span!("pg.execute"))
                .await
        }
        Some(sk_val) => {
            let sk_bound = Bound(sk_val);
            client
                .query_one(&stmt, &[&pk_bound, &sk_bound, &item_json])
                .instrument(tracing::debug_span!("pg.execute"))
                .await
        }
    }
    .map_err(|e| map_pg_err(shape.table, e))?;
    // The single returned column is `old_data`: jsonb of the prior
    // row if the upsert hit an existing one, NULL otherwise.
    let old_item: Option<Json<serde_json::Value>> = row.get(0);
    Ok(old_item.map(|Json(v)| v))
}

#[tracing::instrument(level = "debug", skip_all, name = "pg.get_item_raw", fields(table = %shape.table))]
pub(crate) async fn get_item_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let sql = backend.shape_sql(shape);
    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql.get_sql, &sql.key_types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let pk_bound = Bound(pk);
    let query_result = match sk {
        None => {
            client
                .query_opt(&stmt, &[&pk_bound])
                .instrument(tracing::debug_span!("pg.query"))
                .await
        }
        Some(sk_val) => {
            let sk_bound = Bound(sk_val);
            client
                .query_opt(&stmt, &[&pk_bound, &sk_bound])
                .instrument(tracing::debug_span!("pg.query"))
                .await
        }
    };

    let row = query_result.map_err(|e| map_pg_err(shape.table, e))?;
    Ok(row.map(|r| {
        let Json(v): Json<serde_json::Value> = r.get(0);
        v
    }))
}

#[tracing::instrument(level = "debug", skip_all, name = "pg.delete_item_raw", fields(table = %shape.table))]
pub(crate) async fn delete_item_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let sql = backend.shape_sql(shape);
    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql.delete_sql, &sql.key_types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let pk_bound = Bound(pk);
    // `RETURNING` yields one row per deleted row — `query_opt`
    // distinguishes "deleted 1" (Some) from "deleted 0" (None).
    let row = match sk {
        None => {
            client
                .query_opt(&stmt, &[&pk_bound])
                .instrument(tracing::debug_span!("pg.execute"))
                .await
        }
        Some(sk_val) => {
            let sk_bound = Bound(sk_val);
            client
                .query_opt(&stmt, &[&pk_bound, &sk_bound])
                .instrument(tracing::debug_span!("pg.execute"))
                .await
        }
    }
    .map_err(|e| map_pg_err(shape.table, e))?;

    Ok(row.map(|r| {
        let Json(v): Json<serde_json::Value> = r.get(0);
        v
    }))
}

#[tracing::instrument(level = "debug", skip_all, name = "pg.put_with_condition_raw", fields(table = %shape.table))]
pub(crate) async fn put_with_condition_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    item: &serde_json::Value,
    condition: ConditionEvalFn<'_>,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let prep = RmwSqlPrep::build(shape);

    let mut client = backend.client().await?;
    let tx = client
        .transaction()
        .instrument(tracing::debug_span!("pg.begin"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let select_stmt = tx
        .prepare_typed_cached(&prep.select_sql, &prep.key_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let update_stmt = tx
        .prepare_typed_cached(&prep.update_sql, &prep.update_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let insert_stmt = tx
        .prepare_typed_cached(&prep.insert_sql, &[Type::JSONB])
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    // Retry budget identical to update_general_rmw_raw: the
    // race-on-INSERT case converges in ≤2 iterations.
    const MAX_ITERS: usize = 3;
    for _ in 0..MAX_ITERS {
        let existing = prep.select_for_update(&tx, &select_stmt, pk, sk).await?;
        let exists = existing.is_some();
        if !condition(existing.as_ref()) {
            tx.rollback()
                .await
                .map_err(|e| map_pg_err(shape.table, e))?;
            return Err(BackendError::ConditionalCheckFailed);
        }
        let new_json = Json(item);
        let rows = if exists {
            prep.exec_update(&tx, &update_stmt, &new_json, pk, sk).await?
        } else {
            tx.execute(&insert_stmt, &[&new_json])
                .instrument(tracing::debug_span!("pg.insert_on_conflict"))
                .await
                .map_err(|e| map_pg_err(shape.table, e))?
        };
        if rows > 0 {
            tx.commit()
                .await
                .map_err(|e| map_pg_err(shape.table, e))?;
            return Ok(existing);
        }
        // 0 rows from INSERT-DO-NOTHING: a concurrent INSERT won.
        // Loop; next SELECT will see the row.
    }
    tx.rollback()
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    Err(BackendError::Other(
        "put_with_condition_raw exceeded retry budget (possible runaway race contention)".into(),
    ))
}

/// Tx-scoped upsert. Runs the same CTE-based `put_sql` from
/// [`crate::shape::ShapeSql`] against an existing `Transaction` and
/// returns the pre-update row (or `None`). Used by `transact_write_raw`
/// (T3) so the upsert participates in the caller's transaction; the
/// trait-method `put_item_raw` keeps its existing direct-client path
/// for the single-row hot path.
pub(crate) async fn put_item_inside_tx(
    backend: &PgBackend,
    tx: &deadpool_postgres::Transaction<'_>,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    item: &serde_json::Value,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let sql = backend.shape_sql(shape);
    let stmt = tx
        .prepare_typed_cached(&sql.put_sql, &sql.put_types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let pk_bound = Bound(pk);
    let item_json = Json(item);
    let row = match sk {
        None => {
            tx.query_one(&stmt, &[&pk_bound, &item_json])
                .instrument(tracing::debug_span!("pg.execute"))
                .await
        }
        Some(sk_val) => {
            let sk_bound = Bound(sk_val);
            tx.query_one(&stmt, &[&pk_bound, &sk_bound, &item_json])
                .instrument(tracing::debug_span!("pg.execute"))
                .await
        }
    }
    .map_err(|e| map_pg_err(shape.table, e))?;
    let old_item: Option<Json<serde_json::Value>> = row.get(0);
    Ok(old_item.map(|Json(v)| v))
}

/// Tx-scoped unconditional delete. Idempotent — returns Ok whether
/// the row existed or not, matching DDB DeleteItem semantics.
pub(crate) async fn delete_item_inside_tx(
    backend: &PgBackend,
    tx: &deadpool_postgres::Transaction<'_>,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let sql = backend.shape_sql(shape);
    let stmt = tx
        .prepare_typed_cached(&sql.delete_sql, &sql.key_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let pk_bound = Bound(pk);
    let row = match sk {
        None => tx
            .query_opt(&stmt, &[&pk_bound])
            .instrument(tracing::debug_span!("pg.execute"))
            .await,
        Some(sk_val) => {
            let sk_bound = Bound(sk_val);
            tx.query_opt(&stmt, &[&pk_bound, &sk_bound])
                .instrument(tracing::debug_span!("pg.execute"))
                .await
        }
    }
    .map_err(|e| map_pg_err(shape.table, e))?;
    Ok(row.map(|r| {
        let Json(v): Json<serde_json::Value> = r.get(0);
        v
    }))
}

/// Tx-scoped conditional delete. SELECT FOR UPDATE → evaluate
/// condition → on true DELETE, on false ConditionalCheckFailed.
/// Matches `delete_with_condition_raw`'s logic but doesn't open/own
/// the transaction.
pub(crate) async fn delete_with_condition_inside_tx(
    tx: &deadpool_postgres::Transaction<'_>,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    condition: &ConditionEvalFn<'_>,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let prep = RmwSqlPrep::build(shape);

    let table = quote_ident(shape.table);
    let pk_col = quote_ident(shape.pk_col);
    let pk_cast = cast_for_keytype(shape.pk_type);
    let delete_sql = match (shape.sk_col, shape.sk_type) {
        (None, _) => format!("DELETE FROM {table} WHERE {pk_col} = $1{pk_cast}"),
        (Some(sk_col), Some(sk_type)) => {
            let sk_col_q = quote_ident(sk_col);
            let sk_cast = cast_for_keytype(sk_type);
            format!(
                "DELETE FROM {table} \
                 WHERE {pk_col} = $1{pk_cast} AND {sk_col_q} = $2{sk_cast}"
            )
        }
        (Some(_), None) => unreachable!(
            "TableShape `{}`: sk_col without sk_type",
            shape.table
        ),
    };

    let select_stmt = tx
        .prepare_typed_cached(&prep.select_sql, &prep.key_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let delete_stmt = tx
        .prepare_typed_cached(&delete_sql, &prep.key_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let existing = prep.select_for_update(tx, &select_stmt, pk, sk).await?;
    if !condition(existing.as_ref()) {
        return Err(BackendError::ConditionalCheckFailed);
    }
    if existing.is_some() {
        let pk_bound = Bound(pk);
        match sk {
            None => tx
                .execute(&delete_stmt, &[&pk_bound])
                .instrument(tracing::debug_span!("pg.delete"))
                .await,
            Some(sk_val) => {
                let sk_bound = Bound(sk_val);
                tx.execute(&delete_stmt, &[&pk_bound, &sk_bound])
                    .instrument(tracing::debug_span!("pg.delete"))
                    .await
            }
        }
        .map_err(|e| map_pg_err(shape.table, e))?;
    }
    Ok(existing)
}

/// Tx-scoped read-only guard. SELECT FOR UPDATE → evaluate
/// condition → on true return Ok(()), on false ConditionalCheckFailed.
/// No mutation. Backs `TransactWriteOp::ConditionCheck`.
///
/// The FOR UPDATE lock is held for the duration of the outer
/// transaction — preventing concurrent writes to the row until the
/// whole TransactWriteItems either commits or aborts (the cross-row
/// invariant guarantee).
pub(crate) async fn condition_check_inside_tx(
    tx: &deadpool_postgres::Transaction<'_>,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    condition: &ConditionEvalFn<'_>,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let prep = RmwSqlPrep::build(shape);
    let select_stmt = tx
        .prepare_typed_cached(&prep.select_sql, &prep.key_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let existing = prep.select_for_update(tx, &select_stmt, pk, sk).await?;
    if !condition(existing.as_ref()) {
        return Err(BackendError::ConditionalCheckFailed);
    }
    Ok(existing)
}

/// Tx-scoped conditional upsert — single-pass: SELECT FOR UPDATE,
/// evaluate condition, INSERT/UPDATE accordingly. Returns the
/// pre-update row on success. On condition false, returns
/// [`BackendError::ConditionalCheckFailed`] (caller drops the tx to
/// roll back).
///
/// Unlike `put_with_condition_raw`, this helper does NOT retry on
/// race-on-INSERT — under the caller's REPEATABLE READ tx, a
/// concurrent INSERT after our snapshot raises SQLSTATE `40001`,
/// which propagates and aborts the whole TransactWriteItems request
/// (mapped to `TransactionConflict` in the cancellation reasons).
pub(crate) async fn put_with_condition_inside_tx(
    tx: &deadpool_postgres::Transaction<'_>,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    item: &serde_json::Value,
    condition: &ConditionEvalFn<'_>,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let prep = RmwSqlPrep::build(shape);

    let select_stmt = tx
        .prepare_typed_cached(&prep.select_sql, &prep.key_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let update_stmt = tx
        .prepare_typed_cached(&prep.update_sql, &prep.update_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let insert_stmt = tx
        .prepare_typed_cached(&prep.insert_sql, &[Type::JSONB])
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let existing = prep.select_for_update(tx, &select_stmt, pk, sk).await?;
    let exists = existing.is_some();
    if !condition(existing.as_ref()) {
        return Err(BackendError::ConditionalCheckFailed);
    }
    let new_json = Json(item);
    if exists {
        prep.exec_update(tx, &update_stmt, &new_json, pk, sk).await?;
    } else {
        tx.execute(&insert_stmt, &[&new_json])
            .instrument(tracing::debug_span!("pg.insert_on_conflict"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
    }
    Ok(existing)
}

#[tracing::instrument(level = "debug", skip_all, name = "pg.delete_with_condition_raw", fields(table = %shape.table))]
pub(crate) async fn delete_with_condition_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    condition: ConditionEvalFn<'_>,
) -> Result<Option<serde_json::Value>, BackendError> {
    check_sk_shape(shape, sk)?;
    let prep = RmwSqlPrep::build(shape);

    // Build the DELETE statement on the fly — it's not shared with
    // update_general_rmw_raw, and the shape is shape-stable so
    // prepare_typed_cached will cache it after first use.
    let table = quote_ident(shape.table);
    let pk_col = quote_ident(shape.pk_col);
    let pk_cast = cast_for_keytype(shape.pk_type);
    let delete_sql = match (shape.sk_col, shape.sk_type) {
        (None, _) => format!("DELETE FROM {table} WHERE {pk_col} = $1{pk_cast}"),
        (Some(sk_col), Some(sk_type)) => {
            let sk_col_q = quote_ident(sk_col);
            let sk_cast = cast_for_keytype(sk_type);
            format!(
                "DELETE FROM {table} \
                 WHERE {pk_col} = $1{pk_cast} AND {sk_col_q} = $2{sk_cast}"
            )
        }
        (Some(_), None) => unreachable!(
            "TableShape `{}`: sk_col without sk_type",
            shape.table
        ),
    };

    let mut client = backend.client().await?;
    let tx = client
        .transaction()
        .instrument(tracing::debug_span!("pg.begin"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let select_stmt = tx
        .prepare_typed_cached(&prep.select_sql, &prep.key_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    let delete_stmt = tx
        .prepare_typed_cached(&delete_sql, &prep.key_types)
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let existing = prep.select_for_update(&tx, &select_stmt, pk, sk).await?;

    if !condition(existing.as_ref()) {
        tx.rollback()
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        return Err(BackendError::ConditionalCheckFailed);
    }

    // Condition passed. If the row existed, delete it; otherwise the
    // "deleted nothing" branch is still a success per DDB DeleteItem
    // semantics (conditions like attribute_not_exists(pk) can hold
    // on a missing row).
    let pk_bound = Bound(pk);
    if existing.is_some() {
        match sk {
            None => tx
                .execute(&delete_stmt, &[&pk_bound])
                .instrument(tracing::debug_span!("pg.delete"))
                .await,
            Some(sk_val) => {
                let sk_bound = Bound(sk_val);
                tx.execute(&delete_stmt, &[&pk_bound, &sk_bound])
                    .instrument(tracing::debug_span!("pg.delete"))
                    .await
            }
        }
        .map_err(|e| map_pg_err(shape.table, e))?;
    }
    tx.commit()
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    Ok(existing)
}
