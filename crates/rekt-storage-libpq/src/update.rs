//! UpdateItem PG paths: SQL builders (`build_update_sql`,
//! `build_insert_only_sql`), the shared SELECT-FOR-UPDATE → mutate prep
//! ([`RmwSqlPrep`]), and the four `update_*_raw` method bodies the
//! `Backend` trait impl trampolines into.

use crate::condition_sql::{compile_condition, ParamBuilder};
use crate::types::{
    cast_for_keytype, check_sk_shape, map_pg_err, pg_type_for_keytype, quote_ident, Bound,
};
use crate::PgBackend;
use rekt_expressions::Condition;
use rekt_storage::{
    BackendError, GeneralUpdateFn, KeyType, KeyValue, TableShape, UpdateDecision, UpdateOutcome,
};
use std::fmt::Write;
use tokio_postgres::types::{Json, ToSql, Type};
use tracing::Instrument;

/// Pre-built SQL shapes for the SELECT-FOR-UPDATE → mutate transaction
/// pattern used by `update_general_rmw_raw`, `put_with_condition_raw`,
/// and `delete_with_condition_raw`. Building these once per call keeps
/// the per-statement code paths free of branching on key shape.
pub(crate) struct RmwSqlPrep {
    pub(crate) select_sql: String,
    pub(crate) update_sql: String,
    pub(crate) insert_sql: String,
    pub(crate) key_types: Vec<Type>,
    pub(crate) update_types: Vec<Type>,
}

impl RmwSqlPrep {
    pub(crate) fn build(shape: &TableShape<'_>) -> Self {
        let table = quote_ident(shape.table);
        let pk_col = quote_ident(shape.pk_col);
        let jsonb_col = quote_ident(shape.jsonb_col);
        let pk_cast = cast_for_keytype(shape.pk_type);
        let pk_pg = pg_type_for_keytype(shape.pk_type);

        let (select_sql, update_sql, key_types) = match (shape.sk_col, shape.sk_type) {
            (None, _) => (
                format!(
                    "SELECT {jsonb_col} FROM {table} \
                     WHERE {pk_col} = $1{pk_cast} FOR UPDATE"
                ),
                format!(
                    "UPDATE {table} SET {jsonb_col} = $1 \
                     WHERE {pk_col} = $2{pk_cast}"
                ),
                vec![pk_pg.clone()],
            ),
            (Some(sk_col_name), Some(sk_type)) => {
                let sk_col_q = quote_ident(sk_col_name);
                let sk_cast = cast_for_keytype(sk_type);
                let sk_pg = pg_type_for_keytype(sk_type);
                (
                    format!(
                        "SELECT {jsonb_col} FROM {table} \
                         WHERE {pk_col} = $1{pk_cast} AND {sk_col_q} = $2{sk_cast} FOR UPDATE"
                    ),
                    format!(
                        "UPDATE {table} SET {jsonb_col} = $1 \
                         WHERE {pk_col} = $2{pk_cast} AND {sk_col_q} = $3{sk_cast}"
                    ),
                    vec![pk_pg, sk_pg],
                )
            }
            (Some(_), None) => unreachable!(
                "TableShape `{}`: sk_col without sk_type",
                shape.table
            ),
        };
        let insert_sql = build_insert_only_sql(shape);
        let mut update_types: Vec<Type> = Vec::with_capacity(1 + key_types.len());
        update_types.push(Type::JSONB);
        update_types.extend(key_types.iter().cloned());

        Self {
            select_sql,
            update_sql,
            insert_sql,
            key_types,
            update_types,
        }
    }

    pub(crate) async fn select_for_update(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        stmt: &tokio_postgres::Statement,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        let pk_bound = Bound(pk);
        let table_name = "<unknown>"; // map_pg_err only uses it for TableNotFound; UNDEFINED_TABLE can't surface here.
        let row = match sk {
            None => tx
                .query_opt(stmt, &[&pk_bound])
                .instrument(tracing::debug_span!("pg.select_for_update"))
                .await,
            Some(sk_val) => {
                let sk_bound = Bound(sk_val);
                tx.query_opt(stmt, &[&pk_bound, &sk_bound])
                    .instrument(tracing::debug_span!("pg.select_for_update"))
                    .await
            }
        }
        .map_err(|e| map_pg_err(table_name, e))?;
        Ok(row.map(|r| {
            let Json(v): Json<serde_json::Value> = r.get(0);
            v
        }))
    }

    pub(crate) async fn exec_update(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        stmt: &tokio_postgres::Statement,
        new_json: &Json<&serde_json::Value>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<u64, BackendError> {
        let pk_bound = Bound(pk);
        let table_name = "<unknown>";
        match sk {
            None => tx
                .execute(stmt, &[new_json, &pk_bound])
                .instrument(tracing::debug_span!("pg.update"))
                .await,
            Some(sk_val) => {
                let sk_bound = Bound(sk_val);
                tx.execute(stmt, &[new_json, &pk_bound, &sk_bound])
                    .instrument(tracing::debug_span!("pg.update"))
                    .await
            }
        }
        .map_err(|e| map_pg_err(table_name, e))
    }
}

/// Build the per-call SQL for the simple-subset UpdateItem. The shape
/// varies by `n_sets` and `n_removes`, so we can't precompute it per
/// table — but `prepare_typed_cached` keys on the SQL string, so warm
/// shapes still hit the prepared-statement cache.
///
/// Parameter layout:
///   $1[, $2]    — pk[, sk] for the `prev` CTE's WHERE clause
///   $K+1        — JSONB insert_item (K = 1 or 2)
///   $K+2,$K+3   — per-SET (TEXT name, JSONB value) pairs
///   ...         — per-REMOVE TEXT names
///
/// Adding the CTE costs one snapshot lookup; the upsert itself still
/// derives its key columns from JSONB via `GENERATED ALWAYS AS`.
pub(crate) fn build_update_sql(
    shape: &TableShape<'_>,
    n_sets: usize,
    n_removes: usize,
) -> String {
    let table = quote_ident(shape.table);
    let pk_col = quote_ident(shape.pk_col);
    let jsonb_col = quote_ident(shape.jsonb_col);
    let pk_cast = cast_for_keytype(shape.pk_type);

    let conflict_cols = match shape.sk_col {
        None => pk_col.clone(),
        Some(sk) => format!("{pk_col}, {sk}", sk = quote_ident(sk)),
    };

    // Build the WHERE clause for the snapshot CTE and assign the
    // insert-jsonb param index right after the key params.
    let (cte_where, insert_param_idx) = match (shape.sk_col, shape.sk_type) {
        (None, _) => (format!("{pk_col} = $1{pk_cast}"), 2usize),
        (Some(sk_col), Some(sk_type)) => {
            let sk_quoted = quote_ident(sk_col);
            let sk_cast = cast_for_keytype(sk_type);
            (
                format!("{pk_col} = $1{pk_cast} AND {sk_quoted} = $2{sk_cast}"),
                3,
            )
        }
        (Some(_), None) => unreachable!(
            "TableShape `{}`: sk_col without sk_type",
            shape.table
        ),
    };

    // Build the DO UPDATE expression: starts at `t.jsonb_col`; wrap
    // with `jsonb_set(_, ARRAY[$name], $value)` per SET; append
    // `- $name` per REMOVE.
    let mut param_idx: usize = insert_param_idx + 1;
    let mut expr = format!("{table}.{jsonb_col}");
    for _ in 0..n_sets {
        let name_p = param_idx;
        let val_p = param_idx + 1;
        param_idx += 2;
        expr = format!("jsonb_set({expr}, ARRAY[${name_p}::text], ${val_p}::jsonb)");
    }
    for _ in 0..n_removes {
        let name_p = param_idx;
        param_idx += 1;
        expr = format!("({expr}) - ${name_p}::text");
    }

    // The `prev` CTE reads the pre-update row at the same statement
    // snapshot as the upsert, so old_data is the value that existed
    // *before* this statement ran.
    format!(
        "WITH prev AS (SELECT {jsonb_col} AS old_data FROM {table} WHERE {cte_where}) \
         INSERT INTO {table} ({jsonb_col}) VALUES (${insert_param_idx}::jsonb) \
         ON CONFLICT ({conflict_cols}) \
         DO UPDATE SET {jsonb_col} = {expr} \
         RETURNING {jsonb_col} AS new_data, (SELECT old_data FROM prev) AS old_data"
    )
}

/// SQL for the insert-only fast path (Phase 4c). Differs from
/// `build_update_sql` only in `DO NOTHING` vs `DO UPDATE SET …`.
/// One bound param: `$1::jsonb` = the synthesized full row.
pub(crate) fn build_insert_only_sql(shape: &TableShape<'_>) -> String {
    let table = quote_ident(shape.table);
    let pk_col = quote_ident(shape.pk_col);
    let jsonb_col = quote_ident(shape.jsonb_col);

    let conflict_cols = match shape.sk_col {
        None => pk_col,
        Some(sk) => format!("{pk_col}, {sk}", sk = quote_ident(sk)),
    };

    format!(
        "INSERT INTO {table} ({jsonb_col}) VALUES ($1::jsonb) \
         ON CONFLICT ({conflict_cols}) DO NOTHING \
         RETURNING {jsonb_col}"
    )
}

// =====================================================================
// Method bodies — the Backend trait impl in lib.rs trampolines into these.
// =====================================================================

#[tracing::instrument(level = "debug", skip_all, name = "pg.update_simple_raw", fields(table = %shape.table))]
pub(crate) async fn update_simple_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    insert_item: &serde_json::Value,
    sets: &[(&str, &serde_json::Value)],
    removes: &[&str],
) -> Result<UpdateOutcome, BackendError> {
    check_sk_shape(shape, sk)?;
    let sql = build_update_sql(shape, sets.len(), removes.len());

    // Parameter type vector mirrors the SQL produced by
    // build_update_sql: the leading key params (1 or 2) feed the
    // `prev` CTE; then $N+1 = JSONB (insert_item); then per-SET
    // (TEXT attr name, JSONB value) pairs; then per-REMOVE TEXT
    // attr names.
    let pk_pg = pg_type_for_keytype(shape.pk_type);
    let mut types: Vec<Type> =
        Vec::with_capacity(1 + sk.is_some() as usize + 1 + 2 * sets.len() + removes.len());
    types.push(pk_pg);
    if let Some(sk_v) = sk {
        types.push(pg_type_for_keytype(match sk_v {
            KeyValue::S(_) => KeyType::S,
            KeyValue::N(_) => KeyType::N,
            KeyValue::B(_) => KeyType::B,
        }));
    }
    types.push(Type::JSONB);
    for _ in sets {
        types.push(Type::TEXT);
        types.push(Type::JSONB);
    }
    for _ in removes {
        types.push(Type::TEXT);
    }

    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql, &types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let pk_bound = Bound(pk);
    let sk_bound = sk.map(Bound);
    let insert_json = Json(insert_item);
    let set_value_jsons: Vec<Json<&serde_json::Value>> =
        sets.iter().map(|(_, v)| Json(*v)).collect();

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(types.len());
    params.push(&pk_bound);
    if let Some(b) = sk_bound.as_ref() {
        params.push(b);
    }
    params.push(&insert_json);
    for (i, (name, _)) in sets.iter().enumerate() {
        params.push(name);
        params.push(&set_value_jsons[i]);
    }
    for name in removes {
        params.push(name);
    }

    let row = client
        .query_one(&stmt, &params)
        .instrument(tracing::debug_span!("pg.execute"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    // Column 0: new_data (post-update). Column 1: old_data (pre-update,
    // NULL if the row didn't exist).
    let Json(new_item): Json<serde_json::Value> = row.get(0);
    let old_item: Option<Json<serde_json::Value>> = row.get(1);
    Ok(UpdateOutcome {
        new_item,
        old_item: old_item.map(|Json(v)| v),
    })
}

#[tracing::instrument(level = "debug", skip_all, name = "pg.update_with_simple_condition_raw", fields(table = %shape.table))]
pub(crate) async fn update_with_simple_condition_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    sets: &[(&str, &serde_json::Value)],
    removes: &[&str],
    condition: &Condition,
) -> Result<UpdateOutcome, BackendError> {
    // Pre-flight: sk shape must match the table shape.
    check_sk_shape(shape, sk)?;

    let mut builder = ParamBuilder::new();

    // 1) pk (and sk) — $1, $2. Same param indices feed both the
    //    `prev` CTE's WHERE and the UPDATE's WHERE.
    let pk_idx = builder.bind_key(pk);
    let sk_idx = sk.map(|s| builder.bind_key(s));

    // 2) SET-expression chain over `t.data`, then REMOVE chain.
    let table_ident = quote_ident(shape.table);
    let jsonb_ident = quote_ident(shape.jsonb_col);
    let data_ref = format!("{table_ident}.{jsonb_ident}");
    let mut expr = data_ref.clone();
    for (name, value) in sets {
        let name_idx = builder.bind_text((*name).to_string());
        let value_idx = builder.bind_jsonb((*value).clone());
        expr = format!(
            "jsonb_set({expr}, ARRAY[${name_idx}::text], ${value_idx}::jsonb)"
        );
    }
    for name in removes {
        let name_idx = builder.bind_text((*name).to_string());
        expr = format!("({expr}) - ${name_idx}::text");
    }

    // 3) key predicate, reused by both the `prev` CTE and the UPDATE.
    let mut key_predicate = format!(
        "{} = ${}{}",
        quote_ident(shape.pk_col),
        pk_idx,
        cast_for_keytype(shape.pk_type),
    );
    if let (Some(sk_col), Some(sk_type), Some(idx)) =
        (shape.sk_col, shape.sk_type, sk_idx)
    {
        let _ = write!(
            key_predicate,
            " AND {} = ${}{}",
            quote_ident(sk_col),
            idx,
            cast_for_keytype(sk_type),
        );
    }
    let cond_sql = compile_condition(&mut builder, &data_ref, condition)?;

    // The `prev` CTE captures pre-update state at the same
    // statement snapshot. The UPDATE adds the user's condition;
    // the CTE intentionally does NOT — we want the old item even
    // if the condition would have failed (though in that case
    // the UPDATE returns 0 rows and we map to CCFE, never reading
    // prev).
    let sql = format!(
        "WITH prev AS (SELECT {jsonb_ident} AS old_data FROM {table_ident} WHERE {key_predicate}) \
         UPDATE {table_ident} SET {jsonb_ident} = {expr} \
         WHERE {key_predicate} AND ({cond_sql}) \
         RETURNING {jsonb_ident} AS new_data, (SELECT old_data FROM prev) AS old_data"
    );

    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql, &builder.types)
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    let params_refs: Vec<&(dyn ToSql + Sync)> =
        builder.params.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
    let row = client
        .query_opt(&stmt, &params_refs)
        .instrument(tracing::debug_span!("pg.execute"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    match row {
        None => Err(BackendError::ConditionalCheckFailed),
        Some(r) => {
            let Json(new_item): Json<serde_json::Value> = r.get(0);
            let old_item: Option<Json<serde_json::Value>> = r.get(1);
            Ok(UpdateOutcome {
                new_item,
                old_item: old_item.map(|Json(v)| v),
            })
        }
    }
}

/// Tx-scoped general RMW: SELECT FOR UPDATE → call `apply(existing)` →
/// UPDATE / INSERT. Single-pass (no retry loop) because the caller's
/// REPEATABLE READ tx surfaces any race-on-INSERT as SQLSTATE 40001
/// (serialization_failure), which propagates up to the transact_write
/// CancellationReasons array as "TransactionConflict".
///
/// Caller controls the tx; this helper does not begin/commit/rollback.
pub(crate) async fn update_general_rmw_inside_tx(
    tx: &deadpool_postgres::Transaction<'_>,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    apply: &GeneralUpdateFn<'_>,
) -> Result<UpdateOutcome, BackendError> {
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
    match apply(existing.as_ref())? {
        UpdateDecision::Fail => Err(BackendError::ConditionalCheckFailed),
        UpdateDecision::Apply(new_item) => {
            let new_json = Json(&new_item);
            let rows = if existing.is_some() {
                prep.exec_update(tx, &update_stmt, &new_json, pk, sk).await?
            } else {
                tx.execute(&insert_stmt, &[&new_json])
                    .instrument(tracing::debug_span!("pg.insert_on_conflict"))
                    .await
                    .map_err(|e| map_pg_err(shape.table, e))?
            };
            if rows > 0 {
                Ok(UpdateOutcome {
                    new_item,
                    old_item: existing,
                })
            } else {
                // INSERT-DO-NOTHING returned 0 rows: a concurrent INSERT
                // landed between our SELECT FOR UPDATE and our INSERT.
                // Under REPEATABLE READ this is rare (40001 usually
                // surfaces first) but possible at READ COMMITTED. The
                // single-row trait method retries; the in-tx variant
                // can't (caller owns the tx), so surface as
                // ConditionalCheckFailed — the caller may retry the
                // whole TransactWriteItems.
                Err(BackendError::ConditionalCheckFailed)
            }
        }
    }
}

#[tracing::instrument(level = "debug", skip_all, name = "pg.update_general_rmw_raw", fields(table = %shape.table))]
pub(crate) async fn update_general_rmw_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    pk: &KeyValue,
    sk: Option<&KeyValue>,
    apply: GeneralUpdateFn<'_>,
) -> Result<UpdateOutcome, BackendError> {
    check_sk_shape(shape, sk)?;

    let prep = RmwSqlPrep::build(shape);

    let mut client = backend.client().await?;
    let tx = client
        .transaction()
        .instrument(tracing::debug_span!("pg.begin"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;

    // Prepare the three statements once; they're cached per
    // (connection, sql) so repeated calls hit the same plan.
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

    // Retry budget. The race-on-insert case converges in ≤2 iterations
    // (once a row exists it can't be un-inserted, so the next SELECT
    // finds it and goes down the UPDATE branch). 3 leaves margin.
    const MAX_ITERS: usize = 3;
    for _ in 0..MAX_ITERS {
        let existing = prep.select_for_update(&tx, &select_stmt, pk, sk).await?;

        let decision = apply(existing.as_ref())?;
        match decision {
            UpdateDecision::Fail => {
                tx.rollback()
                    .await
                    .map_err(|e| map_pg_err(shape.table, e))?;
                return Err(BackendError::ConditionalCheckFailed);
            }
            UpdateDecision::Apply(new_item) => {
                let new_json = Json(&new_item);
                let rows = if existing.is_some() {
                    // Row was present (and we hold its lock): plain UPDATE
                    // by key always affects exactly 1 row.
                    prep.exec_update(&tx, &update_stmt, &new_json, pk, sk).await?
                } else {
                    // Row was absent: race-guarded INSERT. 0 rows means a
                    // concurrent transaction inserted between our SELECT
                    // and this INSERT — loop and retry.
                    tx.execute(&insert_stmt, &[&new_json])
                        .instrument(tracing::debug_span!("pg.insert_on_conflict"))
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?
                };
                if rows > 0 {
                    tx.commit()
                        .await
                        .map_err(|e| map_pg_err(shape.table, e))?;
                    return Ok(UpdateOutcome {
                        new_item,
                        old_item: existing,
                    });
                }
                // 0 rows from the INSERT-DO-NOTHING branch only — loop.
            }
        }
    }

    tx.rollback()
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    Err(BackendError::Other(
        "update_general_rmw_raw exceeded retry budget (possible runaway race contention)".into(),
    ))
}

#[tracing::instrument(level = "debug", skip_all, name = "pg.update_insert_only_raw", fields(table = %shape.table))]
pub(crate) async fn update_insert_only_raw(
    backend: &PgBackend,
    shape: &TableShape<'_>,
    insert_item: &serde_json::Value,
) -> Result<UpdateOutcome, BackendError> {
    let sql = build_insert_only_sql(shape);
    let client = backend.client().await?;
    let stmt = client
        .prepare_typed_cached(&sql, &[Type::JSONB])
        .instrument(tracing::debug_span!("pg.prepare"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    // INSERT … ON CONFLICT DO NOTHING RETURNING data: returns 1 row
    // on successful insert, 0 rows when the conflict suppressed the
    // INSERT — which means `attribute_not_exists(pk)` was false.
    let row = client
        .query_opt(&stmt, &[&Json(insert_item)])
        .instrument(tracing::debug_span!("pg.execute"))
        .await
        .map_err(|e| map_pg_err(shape.table, e))?;
    match row {
        None => Err(BackendError::ConditionalCheckFailed),
        Some(r) => {
            let Json(new_item): Json<serde_json::Value> = r.get(0);
            // Insert-only success means the row did NOT exist
            // beforehand, so there's no pre-update item.
            Ok(UpdateOutcome {
                new_item,
                old_item: None,
            })
        }
    }
}
