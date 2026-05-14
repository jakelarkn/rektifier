//! `tokio-postgres` + `deadpool-postgres` backed implementation of [`Backend`].
//!
//! The operator owns the PG schema. Rektifier reads `TableShape` (column
//! names) and `KeyValue` (typed key data) on every call, builds SQL with
//! the right column identifiers spliced in, and binds parameters by their
//! Rust type so PG receives `text` for `S`, `numeric` for `N` (parsed from
//! the verbatim DDB number string), and `bytea` for `B`.
//!
//! Column identifiers are quoted with `"..."` and any embedded quotes are
//! doubled — Postgres can't bind identifiers as parameters, so quoting is
//! what keeps SQL injection off the table.

use async_trait::async_trait;
use bytes::BytesMut;
use deadpool_postgres::{Manager, Pool};
use rekt_expressions::{ComparisonOp, Condition, Operand, Path};
use rekt_protocol::AttributeValue;
use rekt_storage::{
    Backend, BackendError, GeneralUpdateFn, KeyType, KeyValue, TableShape, UpdateDecision,
};
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, RwLock};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{IsNull, Json, ToSql, Type};
use tokio_postgres::NoTls;
use tracing::Instrument;

pub struct PgBackend {
    pool: Pool,
    /// Per-table cache of pre-formatted SQL and prepared-statement type
    /// vectors. Populated lazily on first touch from a `TableShape`. Keyed
    /// by `shape.table` (the PG table name).
    sql_cache: RwLock<HashMap<String, Arc<ShapeSql>>>,
}

/// All the per-shape SQL state that used to be rebuilt every call.
/// Computed once per distinct `TableShape`, then handed out as an `Arc`.
struct ShapeSql {
    put_sql: String,
    get_sql: String,
    delete_sql: String,
    /// Parameter types for `put_item_raw`'s `prepare_typed_cached`: always
    /// `[JSONB]` since writes only bind the jsonb column.
    put_types: Vec<Type>,
    /// Parameter types for `get_item_raw` and `delete_item_raw`: `[pk]` for
    /// hash-only, `[pk, sk]` for composite. Each is `TEXT` (for `S`/`N` keys;
    /// the SQL contains the `::numeric` cast) or `BYTEA` (for `B`).
    key_types: Vec<Type>,
}

impl PgBackend {
    /// Wrap an existing pool. Prefer this when the caller wants to share a
    /// `Pool` with other components (e.g. `rekt-meta::verify`).
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            sql_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn connect(connection_string: &str) -> Result<Self, BackendError> {
        let pg_config: tokio_postgres::Config =
            connection_string
                .parse()
                .map_err(|e: tokio_postgres::Error| {
                    BackendError::Other(format!("invalid connection string: {e}"))
                })?;
        let manager = Manager::new(pg_config, NoTls);
        let pool = Pool::builder(manager)
            .max_size(16)
            .build()
            .map_err(|e| BackendError::Other(format!("pool build failed: {e}")))?;
        Ok(Self::new(pool))
    }

    async fn client(&self) -> Result<deadpool_postgres::Object, BackendError> {
        self.pool
            .get()
            .instrument(tracing::debug_span!("pg.pool_get"))
            .await
            .map_err(|e| BackendError::Other(format!("pool get failed: {e}")))
    }

    /// Get or lazily-build the cached SQL for `shape`. Read-locks for the
    /// common (hit) case; takes the write lock only on first touch per table.
    fn shape_sql(&self, shape: &TableShape<'_>) -> Arc<ShapeSql> {
        if let Some(s) = self.sql_cache.read().unwrap().get(shape.table) {
            return Arc::clone(s);
        }
        let built = Arc::new(ShapeSql::build(shape));
        let mut w = self.sql_cache.write().unwrap();
        // Double-check: another thread may have inserted while we were
        // acquiring the write lock.
        Arc::clone(
            w.entry(shape.table.to_string())
                .or_insert_with(|| built.clone()),
        )
    }
}

/// Quote a SQL identifier per Postgres rules: wrap in `"..."` and double
/// any embedded `"`. Combined with the operator-owned schema (rektifier
/// never invents identifier names), this is sufficient to block injection.
fn quote_ident(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('"');
    for c in ident.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// Newtype that lets us pass a `&KeyValue` straight into tokio-postgres'
/// parameter slot. We can't `impl ToSql for KeyValue` directly — orphan
/// rules — and a `&dyn ToSql` returning helper has lifetime trouble for
/// the `B` variant, so a tiny wrapper is the cleanest path.
#[derive(Debug)]
struct Bound<'a>(&'a KeyValue);

impl ToSql for Bound<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.0 {
            // Both `S` and `N` write UTF-8 text bytes. For `N`, the parameter
            // is bound as TEXT at the prepare-typed layer and the SQL contains
            // an explicit `$N::numeric` cast; PG converts text→numeric at the
            // SQL level. DDB N is already a string on the wire, so we pass it
            // through verbatim.
            KeyValue::S(s) | KeyValue::N(s) => s.to_sql(ty, out),
            KeyValue::B(b) => {
                let slice: &[u8] = b.as_ref();
                slice.to_sql(ty, out)
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        // We bind the underlying value as either text (for `S` / `N`) or as a
        // byte slice (for `B`). For non-text columns (`numeric`), we force the
        // parameter's PG type to TEXT via `prepare_typed` at the call site and
        // apply an explicit `$N::numeric` cast in the SQL — that way PG never
        // asks us to bind into a NUMERIC parameter directly and we don't have
        // to encode PG's numeric binary wire format ourselves.
        matches!(
            *ty,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN | Type::BYTEA
        )
    }

    tokio_postgres::types::to_sql_checked!();
}

/// SQL-level cast suffix for a parameter declared as `KeyType`. `S` and `B`
/// match their target column types natively; `N` is bound as text and cast
/// to numeric at the SQL level.
fn cast_for_keytype(t: KeyType) -> &'static str {
    match t {
        KeyType::S | KeyType::B => "",
        KeyType::N => "::numeric",
    }
}

/// PG `prepare_typed` type for a declared `KeyType`. `S`/`N` both bind as
/// TEXT (the SQL contains the `::numeric` cast for `N`); `B` binds as BYTEA.
fn pg_type_for_keytype(t: KeyType) -> Type {
    match t {
        KeyType::S | KeyType::N => Type::TEXT,
        KeyType::B => Type::BYTEA,
    }
}

impl ShapeSql {
    fn build(shape: &TableShape<'_>) -> Self {
        let table = quote_ident(shape.table);
        let pk_col = quote_ident(shape.pk_col);
        let jsonb_col = quote_ident(shape.jsonb_col);
        let pk_cast = cast_for_keytype(shape.pk_type);
        let pk_pg = pg_type_for_keytype(shape.pk_type);

        match (shape.sk_col, shape.sk_type) {
            (None, _) => Self {
                put_sql: format!(
                    "INSERT INTO {table} ({jsonb_col}) VALUES ($1) \
                     ON CONFLICT ({pk_col}) \
                     DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
                ),
                get_sql: format!("SELECT {jsonb_col} FROM {table} WHERE {pk_col} = $1{pk_cast}"),
                delete_sql: format!("DELETE FROM {table} WHERE {pk_col} = $1{pk_cast}"),
                put_types: vec![Type::JSONB],
                key_types: vec![pk_pg],
            },
            (Some(sk_col_name), Some(sk_type)) => {
                let sk_col = quote_ident(sk_col_name);
                let sk_cast = cast_for_keytype(sk_type);
                let sk_pg = pg_type_for_keytype(sk_type);
                Self {
                    put_sql: format!(
                        "INSERT INTO {table} ({jsonb_col}) VALUES ($1) \
                         ON CONFLICT ({pk_col}, {sk_col}) \
                         DO UPDATE SET {jsonb_col} = EXCLUDED.{jsonb_col}"
                    ),
                    get_sql: format!(
                        "SELECT {jsonb_col} FROM {table} \
                         WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast}"
                    ),
                    delete_sql: format!(
                        "DELETE FROM {table} \
                         WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast}"
                    ),
                    put_types: vec![Type::JSONB],
                    key_types: vec![pk_pg, sk_pg],
                }
            }
            (Some(_), None) => panic!(
                "TableShape `{}`: sk_col is Some but sk_type is None — invariant violation",
                shape.table
            ),
        }
    }
}

/// Common precondition: caller passed `sk` iff shape has `sk_col`.
fn check_sk_shape(shape: &TableShape<'_>, sk: Option<&KeyValue>) -> Result<(), BackendError> {
    match (shape.sk_col, sk) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => Err(BackendError::MissingSortKey {
            name: shape.table.to_string(),
        }),
        (None, Some(_)) => Err(BackendError::UnexpectedSortKey {
            name: shape.table.to_string(),
        }),
    }
}

fn map_pg_err(table: &str, e: tokio_postgres::Error) -> BackendError {
    if e.code() == Some(&SqlState::UNDEFINED_TABLE) {
        return BackendError::TableNotFound {
            name: table.to_string(),
        };
    }
    // `e.to_string()` collapses to "db error"; drill into the underlying
    // DbError for the actual PG message + SQLSTATE.
    let detail = match e.as_db_error() {
        Some(db) => format!("{} ({})", db.message(), db.code().code()),
        None => e.to_string(),
    };
    BackendError::Other(detail)
}

#[async_trait]
impl Backend for PgBackend {
    #[tracing::instrument(level = "debug", skip_all, name = "pg.put_item_raw", fields(table = %shape.table))]
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError> {
        let sql = self.shape_sql(shape);
        let client = self.client().await?;
        let stmt = client
            .prepare_typed_cached(&sql.put_sql, &sql.put_types)
            .instrument(tracing::debug_span!("pg.prepare"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        client
            .execute(&stmt, &[&Json(item)])
            .instrument(tracing::debug_span!("pg.execute"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, name = "pg.get_item_raw", fields(table = %shape.table))]
    async fn get_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError> {
        check_sk_shape(shape, sk)?;
        let sql = self.shape_sql(shape);
        let client = self.client().await?;
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
    async fn delete_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<(), BackendError> {
        check_sk_shape(shape, sk)?;
        let sql = self.shape_sql(shape);
        let client = self.client().await?;
        let stmt = client
            .prepare_typed_cached(&sql.delete_sql, &sql.key_types)
            .instrument(tracing::debug_span!("pg.prepare"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;

        let pk_bound = Bound(pk);
        let exec_result = match sk {
            None => {
                client
                    .execute(&stmt, &[&pk_bound])
                    .instrument(tracing::debug_span!("pg.execute"))
                    .await
            }
            Some(sk_val) => {
                let sk_bound = Bound(sk_val);
                client
                    .execute(&stmt, &[&pk_bound, &sk_bound])
                    .instrument(tracing::debug_span!("pg.execute"))
                    .await
            }
        };

        // DDB DeleteItem is idempotent — `Ok(())` whether 0 or 1 rows deleted.
        // Errors map normally (TableNotFound, etc.).
        exec_result.map_err(|e| map_pg_err(shape.table, e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, name = "pg.update_simple_raw", fields(table = %shape.table))]
    async fn update_simple_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
    ) -> Result<serde_json::Value, BackendError> {
        let sql = build_update_sql(shape, sets.len(), removes.len());

        // Parameter type vector mirrors the SQL: $1 = JSONB (insert_item),
        // then (TEXT attr name, JSONB value) per SET, then TEXT attr name
        // per REMOVE.
        let mut types: Vec<Type> = Vec::with_capacity(1 + 2 * sets.len() + removes.len());
        types.push(Type::JSONB);
        for _ in sets {
            types.push(Type::TEXT);
            types.push(Type::JSONB);
        }
        for _ in removes {
            types.push(Type::TEXT);
        }

        let client = self.client().await?;
        let stmt = client
            .prepare_typed_cached(&sql, &types)
            .instrument(tracing::debug_span!("pg.prepare"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;

        // Build the param slice. SET values get wrapped in `Json(&Value)`
        // owned in a side Vec so the borrows survive until execute().
        let insert_json = Json(insert_item);
        let set_value_jsons: Vec<Json<&serde_json::Value>> =
            sets.iter().map(|(_, v)| Json(*v)).collect();

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(types.len());
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
        let Json(new_item): Json<serde_json::Value> = row.get(0);
        Ok(new_item)
    }

    #[tracing::instrument(level = "debug", skip_all, name = "pg.update_with_simple_condition_raw", fields(table = %shape.table))]
    async fn update_with_simple_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
        condition: &Condition,
    ) -> Result<serde_json::Value, BackendError> {
        // Pre-flight: sk shape must match the table shape.
        check_sk_shape(shape, sk)?;

        let mut builder = ParamBuilder::new();

        // 1) pk (and sk) — $1, $2.
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

        // 3) WHERE: pk = $1 [AND sk = $2] AND (compiled condition).
        let mut where_clause = format!(
            "{} = ${}{}",
            quote_ident(shape.pk_col),
            pk_idx,
            cast_for_keytype(shape.pk_type),
        );
        if let (Some(sk_col), Some(sk_type), Some(idx)) =
            (shape.sk_col, shape.sk_type, sk_idx)
        {
            let _ = write!(
                where_clause,
                " AND {} = ${}{}",
                quote_ident(sk_col),
                idx,
                cast_for_keytype(sk_type),
            );
        }
        let cond_sql = compile_condition(&mut builder, &data_ref, condition)?;
        let _ = write!(where_clause, " AND ({cond_sql})");

        let sql = format!(
            "UPDATE {table_ident} SET {jsonb_ident} = {expr} \
             WHERE {where_clause} RETURNING {jsonb_ident}"
        );

        let client = self.client().await?;
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
                Ok(new_item)
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, name = "pg.update_general_rmw_raw", fields(table = %shape.table))]
    async fn update_general_rmw_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        apply: GeneralUpdateFn<'_>,
    ) -> Result<serde_json::Value, BackendError> {
        check_sk_shape(shape, sk)?;

        // Pre-compute the three SQL shapes (SELECT, UPDATE, INSERT) once.
        let table = quote_ident(shape.table);
        let pk_col = quote_ident(shape.pk_col);
        let jsonb_col = quote_ident(shape.jsonb_col);
        let pk_cast = cast_for_keytype(shape.pk_type);
        let pk_pg = pg_type_for_keytype(shape.pk_type);

        let (select_sql, update_sql, key_types): (String, String, Vec<Type>) =
            match (shape.sk_col, shape.sk_type) {
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
                    let sk_col = quote_ident(sk_col_name);
                    let sk_cast = cast_for_keytype(sk_type);
                    let sk_pg = pg_type_for_keytype(sk_type);
                    (
                        format!(
                            "SELECT {jsonb_col} FROM {table} \
                             WHERE {pk_col} = $1{pk_cast} AND {sk_col} = $2{sk_cast} FOR UPDATE"
                        ),
                        format!(
                            "UPDATE {table} SET {jsonb_col} = $1 \
                             WHERE {pk_col} = $2{pk_cast} AND {sk_col} = $3{sk_cast}"
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
        // UPDATE param types are [JSONB, then the key types].
        let mut update_types: Vec<Type> = Vec::with_capacity(1 + key_types.len());
        update_types.push(Type::JSONB);
        update_types.extend(key_types.iter().cloned());

        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .instrument(tracing::debug_span!("pg.begin"))
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;

        // Prepare the three statements once; they're cached per
        // (connection, sql) so repeated calls hit the same plan.
        let select_stmt = tx
            .prepare_typed_cached(&select_sql, &key_types)
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        let update_stmt = tx
            .prepare_typed_cached(&update_sql, &update_types)
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        let insert_stmt = tx
            .prepare_typed_cached(&insert_sql, &[Type::JSONB])
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;

        // Retry budget. The race-on-insert case converges in ≤2 iterations
        // (once a row exists it can't be un-inserted, so the next SELECT
        // finds it and goes down the UPDATE branch). 3 leaves margin.
        const MAX_ITERS: usize = 3;
        for _ in 0..MAX_ITERS {
            // SELECT FOR UPDATE.
            let pk_bound = Bound(pk);
            let row = match sk {
                None => {
                    tx.query_opt(&select_stmt, &[&pk_bound])
                        .instrument(tracing::debug_span!("pg.select_for_update"))
                        .await
                }
                Some(sk_val) => {
                    let sk_bound = Bound(sk_val);
                    tx.query_opt(&select_stmt, &[&pk_bound, &sk_bound])
                        .instrument(tracing::debug_span!("pg.select_for_update"))
                        .await
                }
            }
            .map_err(|e| map_pg_err(shape.table, e))?;

            let existing: Option<serde_json::Value> = row.map(|r| {
                let Json(v): Json<serde_json::Value> = r.get(0);
                v
            });

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
                        let pk_bound = Bound(pk);
                        match sk {
                            None => tx
                                .execute(&update_stmt, &[&new_json, &pk_bound])
                                .instrument(tracing::debug_span!("pg.update"))
                                .await,
                            Some(sk_val) => {
                                let sk_bound = Bound(sk_val);
                                tx.execute(&update_stmt, &[&new_json, &pk_bound, &sk_bound])
                                    .instrument(tracing::debug_span!("pg.update"))
                                    .await
                            }
                        }
                        .map_err(|e| map_pg_err(shape.table, e))?
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
                        return Ok(new_item);
                    }
                    // 0 rows from the INSERT-DO-NOTHING branch only — loop.
                }
            }
        }

        tx.rollback()
            .await
            .map_err(|e| map_pg_err(shape.table, e))?;
        Err(BackendError::Other(
            "update_general_rmw_raw exceeded retry budget (possible runaway race contention)"
                .into(),
        ))
    }

    #[tracing::instrument(level = "debug", skip_all, name = "pg.update_insert_only_raw", fields(table = %shape.table))]
    async fn update_insert_only_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
    ) -> Result<serde_json::Value, BackendError> {
        let sql = build_insert_only_sql(shape);
        let client = self.client().await?;
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
                Ok(new_item)
            }
        }
    }
}

/// Build the per-call SQL for the simple-subset UpdateItem. The shape
/// varies by `n_sets` and `n_removes`, so we can't precompute it per
/// table — but `prepare_typed_cached` keys on the SQL string, so warm
/// shapes still hit the prepared-statement cache.
fn build_update_sql(shape: &TableShape<'_>, n_sets: usize, n_removes: usize) -> String {
    let table = quote_ident(shape.table);
    let pk_col = quote_ident(shape.pk_col);
    let jsonb_col = quote_ident(shape.jsonb_col);

    let conflict_cols = match shape.sk_col {
        None => pk_col.clone(),
        Some(sk) => format!("{pk_col}, {sk}", sk = quote_ident(sk)),
    };

    // Start the DO-UPDATE expression at `t.jsonb_col`; wrap with a
    // `jsonb_set(_, ARRAY[$name], $value)` per SET; then append `- $name`
    // per REMOVE.
    //
    // Params: $1 = INSERT-branch JSONB, then for each SET (name, value),
    // then for each REMOVE name.
    let mut param_idx: usize = 2;
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

    format!(
        "INSERT INTO {table} ({jsonb_col}) VALUES ($1::jsonb) \
         ON CONFLICT ({conflict_cols}) \
         DO UPDATE SET {jsonb_col} = {expr} \
         RETURNING {jsonb_col}"
    )
}

/// SQL for the insert-only fast path (Phase 4c). Differs from
/// `build_update_sql` only in `DO NOTHING` vs `DO UPDATE SET …`.
/// One bound param: `$1::jsonb` = the synthesized full row.
fn build_insert_only_sql(shape: &TableShape<'_>) -> String {
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

// =============================================================================
// ConditionExpression → SQL WHERE compiler (Phase 4d)
// =============================================================================

/// Accumulates positional parameters as we build the SQL string. The
/// compiler binds attr names as TEXT, DDB-JSON values as JSONB, and the
/// raw textual N/S form for typed ordering as TEXT (the SQL applies the
/// `::numeric`/`::text` cast).
struct ParamBuilder {
    params: Vec<Box<dyn ToSql + Sync + Send>>,
    types: Vec<Type>,
}

impl ParamBuilder {
    fn new() -> Self {
        Self {
            params: Vec::new(),
            types: Vec::new(),
        }
    }

    fn next_idx(&self) -> usize {
        self.params.len() + 1
    }

    fn bind_text(&mut self, s: String) -> usize {
        let idx = self.next_idx();
        self.params.push(Box::new(s));
        self.types.push(Type::TEXT);
        idx
    }

    fn bind_jsonb(&mut self, v: serde_json::Value) -> usize {
        let idx = self.next_idx();
        self.params.push(Box::new(Json(v)));
        self.types.push(Type::JSONB);
        idx
    }

    fn bind_key(&mut self, kv: &KeyValue) -> usize {
        let idx = self.next_idx();
        match kv {
            KeyValue::S(s) | KeyValue::N(s) => {
                self.params.push(Box::new(s.clone()));
                self.types.push(Type::TEXT);
            }
            KeyValue::B(b) => {
                self.params.push(Box::new(b.to_vec()));
                self.types.push(Type::BYTEA);
            }
        }
        idx
    }
}

/// Walk a SimpleSql-classified `Condition` and emit a SQL WHERE fragment
/// against `data_ref` (typically `"t".data`). Caller guarantees that
/// every comparison fits the `condition_fits_sql` predicate from the
/// translator — i.e., equality on any operand shapes, or ordering with
/// one Path operand + one Value operand of type N or S. Anything else
/// returns a `BackendError::Other("classifier divergence: …")` rather
/// than a panic, so a translator/storage drift is loud.
fn compile_condition(
    b: &mut ParamBuilder,
    data_ref: &str,
    cond: &Condition,
) -> Result<String, BackendError> {
    match cond {
        Condition::AttributeExists(p) => {
            let idx = b.bind_text(top_attr(p)?);
            Ok(format!("({data_ref} ? ${idx}::text)"))
        }
        Condition::AttributeNotExists(p) => {
            let idx = b.bind_text(top_attr(p)?);
            Ok(format!("(NOT ({data_ref} ? ${idx}::text))"))
        }
        Condition::Compare { op, left, right } => match op {
            ComparisonOp::Eq | ComparisonOp::Ne => {
                let l_sql = operand_as_jsonb(b, data_ref, left)?;
                let r_sql = operand_as_jsonb(b, data_ref, right)?;
                let op_str = if *op == ComparisonOp::Eq { "=" } else { "<>" };
                // `IS [NOT] DISTINCT FROM` would treat NULL as a value; DDB
                // semantics say "missing attr → comparison false", which is
                // exactly what plain `=` / `<>` give us (NULL → false in WHERE).
                Ok(format!("({l_sql} {op_str} {r_sql})"))
            }
            ComparisonOp::Lt | ComparisonOp::Le | ComparisonOp::Gt | ComparisonOp::Ge => {
                compile_typed_ordering(b, data_ref, *op, left, right)
            }
        },
        Condition::And(a, c) => {
            let l = compile_condition(b, data_ref, a)?;
            let r = compile_condition(b, data_ref, c)?;
            Ok(format!("({l} AND {r})"))
        }
        Condition::Or(a, c) => {
            let l = compile_condition(b, data_ref, a)?;
            let r = compile_condition(b, data_ref, c)?;
            Ok(format!("({l} OR {r})"))
        }
        Condition::Not(inner) => {
            let s = compile_condition(b, data_ref, inner)?;
            Ok(format!("(NOT {s})"))
        }
        // Phase 4e shapes route to the slow path via the translator's
        // `condition_fits_sql == false`; reaching the SQL compiler with
        // one of these is a classifier divergence.
        Condition::BeginsWith(_, _)
        | Condition::Contains(_, _)
        | Condition::Between(_, _, _)
        | Condition::In(_, _)
        | Condition::AttributeType(_, _) => Err(BackendError::Other(
            "classifier divergence: Phase 4e shape reached SQL compiler".into(),
        )),
    }
}

fn top_attr(p: &Path) -> Result<String, BackendError> {
    p.top_name().map(str::to_string).ok_or_else(|| {
        BackendError::Other("classifier divergence: non-top-level path reached SQL compiler".into())
    })
}

/// Render an operand as a jsonb-typed SQL expression. For paths this is
/// `data->attr`; for values it's `$N::jsonb` bound to the DDB-JSON form.
fn operand_as_jsonb(
    b: &mut ParamBuilder,
    data_ref: &str,
    op: &Operand,
) -> Result<String, BackendError> {
    match op {
        Operand::Path(p) => {
            let idx = b.bind_text(top_attr(p)?);
            Ok(format!("{data_ref}->${idx}::text"))
        }
        Operand::Value(v) => {
            let json_v =
                serde_json::to_value(v).expect("AttributeValue Serialize is infallible");
            let idx = b.bind_jsonb(json_v);
            Ok(format!("${idx}::jsonb"))
        }
    }
}

/// Compile an ordering compare: one operand is a Path, the other is a
/// Value of type N or S. Extract the typed scalar from the stored DDB
/// JSON (`data#>>ARRAY['attr','N']` etc.), cast to numeric / text, and
/// compare with a likewise-cast bound parameter.
fn compile_typed_ordering(
    b: &mut ParamBuilder,
    data_ref: &str,
    op: ComparisonOp,
    left: &Operand,
    right: &Operand,
) -> Result<String, BackendError> {
    // Normalize to (path, value) with the operator possibly flipped so
    // the path is always on the LHS in the emitted SQL.
    let (path, value, sql_op_str) = match (left, right) {
        (Operand::Path(p), Operand::Value(v)) => (p, v, sql_op(op, false)),
        (Operand::Value(v), Operand::Path(p)) => (p, v, sql_op(op, true)),
        _ => {
            return Err(BackendError::Other(
                "classifier divergence: ordering must be Path/Value".into(),
            ));
        }
    };
    let attr_idx = b.bind_text(top_attr(path)?);
    match value {
        AttributeValue::N(n) => {
            let val_idx = b.bind_text(n.clone());
            Ok(format!(
                "(({data_ref}#>>ARRAY[${attr_idx}::text, 'N']::text[])::numeric \
                 {sql_op_str} ${val_idx}::numeric)"
            ))
        }
        AttributeValue::S(s) => {
            let val_idx = b.bind_text(s.clone());
            Ok(format!(
                "({data_ref}#>>ARRAY[${attr_idx}::text, 'S']::text[] \
                 {sql_op_str} ${val_idx}::text)"
            ))
        }
        _ => Err(BackendError::Other(
            "classifier divergence: ordering RHS must be N or S".into(),
        )),
    }
}

fn sql_op(op: ComparisonOp, swap: bool) -> &'static str {
    use ComparisonOp::*;
    match (op, swap) {
        (Lt, false) | (Gt, true) => "<",
        (Le, false) | (Ge, true) => "<=",
        (Gt, false) | (Lt, true) => ">",
        (Ge, false) | (Le, true) => ">=",
        _ => unreachable!("non-ordering op reached sql_op"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn quote_ident_basic() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("user_id"), "\"user_id\"");
    }

    #[test]
    fn quote_ident_escapes_embedded_quote() {
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }

    fn database_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
    }

    /// End-to-end round trip against a running Postgres. Skipped by default;
    /// run with `cargo test -p rekt-storage-libpq -- --ignored` after `just up`.
    ///
    /// Schemas use `GENERATED ALWAYS AS ... STORED` columns derived from the
    /// JSONB — that's the contract rektifier expects from operator-owned tables.
    /// Writes bind only the JSONB; PG fills in the key columns.
    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn round_trip_hash_only_string_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_hash_s;
                 CREATE TABLE rekt_t_hash_s (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_hash_s",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        backend.put_item_raw(&shape, &item).await.unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(item.clone()));

        // Upsert: same key, different value, should replace.
        let item2 = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice2"}});
        backend.put_item_raw(&shape, &item2).await.unwrap();
        let got2 = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got2, Some(item2));

        // Missing pk on read -> Ok(None).
        let missing = backend
            .get_item_raw(&shape, &KeyValue::S("nope".into()), None)
            .await
            .unwrap();
        assert_eq!(missing, None);

        // Sort key passed but shape is hash-only -> UnexpectedSortKey on read.
        let err = backend
            .get_item_raw(
                &shape,
                &KeyValue::S("u1".into()),
                Some(&KeyValue::S("oops".into())),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::UnexpectedSortKey { .. }));

        client
            .batch_execute("DROP TABLE rekt_t_hash_s;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn round_trip_composite_numeric_sk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_comp_sn;
                 CREATE TABLE rekt_t_comp_sn (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')           STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)       STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_comp_sn",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };

        // 38-digit precision number.
        let big_ts = "12345678901234567890.12345678901234567";
        let item1 =
            serde_json::json!({"device_id":{"S":"dev-1"},"ts":{"N":"1000"},"val":{"N":"1"}});
        let item2 =
            serde_json::json!({"device_id":{"S":"dev-1"},"ts":{"N":big_ts},"val":{"N":"2"}});
        backend.put_item_raw(&shape, &item1).await.unwrap();
        backend.put_item_raw(&shape, &item2).await.unwrap();

        assert_eq!(
            backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("dev-1".into()),
                    Some(&KeyValue::N("1000".into()))
                )
                .await
                .unwrap(),
            Some(item1)
        );
        assert_eq!(
            backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("dev-1".into()),
                    Some(&KeyValue::N(big_ts.into()))
                )
                .await
                .unwrap(),
            Some(item2)
        );

        // Missing sk on composite shape read -> MissingSortKey.
        let err = backend
            .get_item_raw(&shape, &KeyValue::S("dev-1".into()), None)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::MissingSortKey { .. }));

        client
            .batch_execute("DROP TABLE rekt_t_comp_sn;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn round_trip_binary_pk() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_hash_b;
                 CREATE TABLE rekt_t_hash_b (
                   meta jsonb NOT NULL,
                   hash bytea GENERATED ALWAYS AS (decode(meta#>>'{hash,B}', 'base64')) STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_hash_b",
            pk_col: "hash",
            pk_type: KeyType::B,
            sk_col: None,
            sk_type: None,
            jsonb_col: "meta",
        };
        // base64 of "\x00\x01\x02\xff" is "AAEC/w==".
        let item = serde_json::json!({"hash":{"B":"AAEC/w=="},"extent":{"N":"4"}});
        backend.put_item_raw(&shape, &item).await.unwrap();

        let got = backend
            .get_item_raw(
                &shape,
                &KeyValue::B(Bytes::from_static(b"\x00\x01\x02\xff")),
                None,
            )
            .await
            .unwrap();
        assert_eq!(got, Some(item));

        client
            .batch_execute("DROP TABLE rekt_t_hash_b;")
            .await
            .unwrap();
    }

    /// PG generated columns + NOT NULL primary key act as the backstop when a
    /// caller hands us an item whose JSONB doesn't yield the expected key.
    /// (Translator-level validation will catch this first in production, but
    /// the backstop matters for correctness.)
    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn put_with_missing_key_attr_in_jsonb_is_rejected() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_pk_required;
                 CREATE TABLE rekt_t_pk_required (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_pk_required",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        // JSONB has no `id` attr — generated column yields NULL — PRIMARY KEY rejects.
        let bad_item = serde_json::json!({"label":{"S":"alice"}});
        let err = backend.put_item_raw(&shape, &bad_item).await.unwrap_err();
        // PG raises a not-null-violation; we map to Other for now (translator
        // should catch this case earlier in the real call path).
        assert!(matches!(err, BackendError::Other(_)));

        client
            .batch_execute("DROP TABLE rekt_t_pk_required;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn delete_round_trip_hash_only() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_delete_s;
                 CREATE TABLE rekt_t_delete_s (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_delete_s",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let item = serde_json::json!({"id":{"S":"alice"},"label":{"S":"A"}});
        backend.put_item_raw(&shape, &item).await.unwrap();
        // Sanity: item is present.
        assert_eq!(
            backend
                .get_item_raw(&shape, &KeyValue::S("alice".into()), None)
                .await
                .unwrap(),
            Some(item)
        );

        // Delete: item gone.
        backend
            .delete_item_raw(&shape, &KeyValue::S("alice".into()), None)
            .await
            .unwrap();
        assert_eq!(
            backend
                .get_item_raw(&shape, &KeyValue::S("alice".into()), None)
                .await
                .unwrap(),
            None
        );

        // Idempotent: deleting again is Ok.
        backend
            .delete_item_raw(&shape, &KeyValue::S("alice".into()), None)
            .await
            .unwrap();

        client
            .batch_execute("DROP TABLE rekt_t_delete_s;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn delete_round_trip_composite() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_delete_sn;
                 CREATE TABLE rekt_t_delete_sn (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')           STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)       STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_delete_sn",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };
        let item_a = serde_json::json!({"device_id":{"S":"d1"},"ts":{"N":"100"},"v":{"N":"1"}});
        let item_b = serde_json::json!({"device_id":{"S":"d1"},"ts":{"N":"200"},"v":{"N":"2"}});
        backend.put_item_raw(&shape, &item_a).await.unwrap();
        backend.put_item_raw(&shape, &item_b).await.unwrap();

        // Delete only the (d1, 100) row; (d1, 200) survives.
        backend
            .delete_item_raw(
                &shape,
                &KeyValue::S("d1".into()),
                Some(&KeyValue::N("100".into())),
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("d1".into()),
                    Some(&KeyValue::N("100".into()))
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            backend
                .get_item_raw(
                    &shape,
                    &KeyValue::S("d1".into()),
                    Some(&KeyValue::N("200".into()))
                )
                .await
                .unwrap(),
            Some(item_b)
        );

        client
            .batch_execute("DROP TABLE rekt_t_delete_sn;")
            .await
            .unwrap();
    }

    // ============================================================
    // update_simple_raw: fast-path UpdateItem (top-level SET literal +
    // REMOVE) against a real PG. All gated by `#[ignore]`.
    // ============================================================

    /// Set up a hash-only string-PK table identical to the one in the
    /// other tests, but with a unique name so parallel #[ignore] runs
    /// don't conflict.
    async fn setup_update_hash_table(
        backend: &PgBackend,
        table: &str,
    ) -> deadpool_postgres::Object {
        let client = backend.client().await.unwrap();
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {table};
                 CREATE TABLE {table} (
                   data jsonb NOT NULL,
                   id   text  GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY
                 );"
            ))
            .await
            .unwrap();
        client
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_set_on_missing_row_creates() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_create").await;

        let shape = TableShape {
            table: "rekt_t_upd_create",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let insert_item =
            serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"},"score":{"N":"10"}});
        let name_v = serde_json::json!({"S":"alice"});
        let score_v = serde_json::json!({"N":"10"});
        backend
            .update_simple_raw(
                &shape,
                &insert_item,
                &[("label", &name_v), ("score", &score_v)],
                &[],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(insert_item));

        client
            .batch_execute("DROP TABLE rekt_t_upd_create;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_set_on_existing_row_merges() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_merge").await;

        let shape = TableShape {
            table: "rekt_t_upd_merge",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        // Seed an item that has `name` and `untouched`.
        let original = serde_json::json!({
            "id":{"S":"u1"},
            "label":{"S":"alice"},
            "untouched":{"S":"keep_me"},
        });
        backend.put_item_raw(&shape, &original).await.unwrap();

        // Update: replace `name`, add a new `score`. Sibling `untouched`
        // must survive — that's the merge contract.
        let insert_item =
            serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice2"},"score":{"N":"42"}});
        let name_v = serde_json::json!({"S":"alice2"});
        let score_v = serde_json::json!({"N":"42"});
        backend
            .update_simple_raw(
                &shape,
                &insert_item,
                &[("label", &name_v), ("score", &score_v)],
                &[],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({
                "id":{"S":"u1"},
                "label":{"S":"alice2"},
                "score":{"N":"42"},
                "untouched":{"S":"keep_me"},
            }))
        );

        client
            .batch_execute("DROP TABLE rekt_t_upd_merge;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_remove_existing_and_absent() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_remove").await;

        let shape = TableShape {
            table: "rekt_t_upd_remove",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({
            "id":{"S":"u1"},
            "label":{"S":"alice"},
            "deprecated":{"S":"gone"},
        });
        backend.put_item_raw(&shape, &original).await.unwrap();

        // REMOVE `deprecated` (present) and `never_existed` (absent).
        // Absent path is a no-op, must not error.
        backend
            .update_simple_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"}}),
                &[],
                &["deprecated", "never_existed"],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({
                "id":{"S":"u1"},
                "label":{"S":"alice"},
            }))
        );

        client
            .batch_execute("DROP TABLE rekt_t_upd_remove;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_set_and_remove_combined() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_combined").await;

        let shape = TableShape {
            table: "rekt_t_upd_combined",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({
            "id":{"S":"u1"},
            "flag":{"S":"old"},
            "deprecated":{"S":"x"},
        });
        backend.put_item_raw(&shape, &original).await.unwrap();

        let status_v = serde_json::json!({"S":"new"});
        backend
            .update_simple_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"flag":{"S":"new"}}),
                &[("flag", &status_v)],
                &["deprecated"],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({"id":{"S":"u1"},"flag":{"S":"new"}}))
        );

        client
            .batch_execute("DROP TABLE rekt_t_upd_combined;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_composite_key() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_upd_comp;
                 CREATE TABLE rekt_t_upd_comp (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')          STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)       STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_upd_comp",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };

        // Upsert via update on a missing composite-key row.
        let value_v = serde_json::json!({"N":"7"});
        backend
            .update_simple_raw(
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N":"1000"},
                    "val":{"N":"7"},
                }),
                &[("val", &value_v)],
                &[],
            )
            .await
            .unwrap();

        // And again on an existing row — merge preserves other attrs.
        let extra = serde_json::json!({
            "device_id":{"S":"dev-1"},
            "ts":{"N":"2000"},
            "val":{"N":"1"},
            "label":{"S":"keep_me"},
        });
        backend.put_item_raw(&shape, &extra).await.unwrap();

        let value_v2 = serde_json::json!({"N":"99"});
        backend
            .update_simple_raw(
                &shape,
                &serde_json::json!({
                    "device_id":{"S":"dev-1"},
                    "ts":{"N":"2000"},
                    "val":{"N":"99"},
                }),
                &[("val", &value_v2)],
                &[],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(
                &shape,
                &KeyValue::S("dev-1".into()),
                Some(&KeyValue::N("2000".into())),
            )
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({
                "device_id":{"S":"dev-1"},
                "ts":{"N":"2000"},
                "val":{"N":"99"},
                "label":{"S":"keep_me"},
            }))
        );

        client
            .batch_execute("DROP TABLE rekt_t_upd_comp;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_simple_supports_all_attribute_value_variants() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_upd_variants").await;

        let shape = TableShape {
            table: "rekt_t_upd_variants",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        // Cover S, N, B, BOOL, NULL, L, M, SS, NS, BS — every variant.
        let s_v = serde_json::json!({"S":"hello"});
        let n_v = serde_json::json!({"N":"42.5"});
        let b_v = serde_json::json!({"B":"AAEC"});
        let bool_v = serde_json::json!({"BOOL":true});
        let null_v = serde_json::json!({"NULL":true});
        let l_v = serde_json::json!({"L":[{"S":"a"},{"N":"1"}]});
        let m_v = serde_json::json!({"M":{"k":{"S":"v"}}});
        let ss_v = serde_json::json!({"SS":["a","b"]});
        let ns_v = serde_json::json!({"NS":["1","2"]});
        let bs_v = serde_json::json!({"BS":["AAE="]});

        let insert_item = serde_json::json!({
            "id":{"S":"u1"},
            "s":s_v, "n":n_v, "b":b_v, "bool":bool_v, "null":null_v,
            "l":l_v, "m":m_v, "ss":ss_v, "ns":ns_v, "bs":bs_v,
        });

        backend
            .update_simple_raw(
                &shape,
                &insert_item,
                &[
                    ("s", &s_v),
                    ("n", &n_v),
                    ("b", &b_v),
                    ("bool", &bool_v),
                    ("null", &null_v),
                    ("l", &l_v),
                    ("m", &m_v),
                    ("ss", &ss_v),
                    ("ns", &ns_v),
                    ("bs", &bs_v),
                ],
                &[],
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(insert_item));

        client
            .batch_execute("DROP TABLE rekt_t_upd_variants;")
            .await
            .unwrap();
    }

    // ============================================================
    // update_insert_only_raw: Phase 4c, `attribute_not_exists(pk)`
    // path. Missing row → row inserted; existing row → CCFE.
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_insert_only_creates_when_row_absent() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_inserto_new").await;

        let shape = TableShape {
            table: "rekt_t_inserto_new",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        backend.update_insert_only_raw(&shape, &item).await.unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(item));

        client
            .batch_execute("DROP TABLE rekt_t_inserto_new;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_insert_only_returns_ccfe_when_row_exists() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_inserto_clash").await;

        let shape = TableShape {
            table: "rekt_t_inserto_clash",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        backend.put_item_raw(&shape, &original).await.unwrap();

        // Second insert-only call on the same key must fail and leave
        // the original row untouched.
        let proposed = serde_json::json!({"id":{"S":"u1"},"label":{"S":"NEW"}});
        let err = backend
            .update_insert_only_raw(&shape, &proposed)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(original));

        client
            .batch_execute("DROP TABLE rekt_t_inserto_clash;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_insert_only_composite_key() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = backend.client().await.unwrap();

        client
            .batch_execute(
                "DROP TABLE IF EXISTS rekt_t_inserto_comp;
                 CREATE TABLE rekt_t_inserto_comp (
                   doc       jsonb NOT NULL,
                   device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')         STORED,
                   ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)     STORED,
                   PRIMARY KEY (device_id, ts)
                 );",
            )
            .await
            .unwrap();

        let shape = TableShape {
            table: "rekt_t_inserto_comp",
            pk_col: "device_id",
            pk_type: KeyType::S,
            sk_col: Some("ts"),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc",
        };

        let first = serde_json::json!({
            "device_id":{"S":"d1"},
            "ts":{"N":"1000"},
            "v":{"N":"1"},
        });
        backend
            .update_insert_only_raw(&shape, &first)
            .await
            .unwrap();

        // Same composite key again → CCFE.
        let second = serde_json::json!({
            "device_id":{"S":"d1"},
            "ts":{"N":"1000"},
            "v":{"N":"99"},
        });
        let err = backend
            .update_insert_only_raw(&shape, &second)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        // Different ts on same device works (different composite key).
        let third = serde_json::json!({
            "device_id":{"S":"d1"},
            "ts":{"N":"2000"},
            "v":{"N":"7"},
        });
        backend
            .update_insert_only_raw(&shape, &third)
            .await
            .unwrap();

        client
            .batch_execute("DROP TABLE rekt_t_inserto_comp;")
            .await
            .unwrap();
    }

    // ============================================================
    // update_with_simple_condition_raw: Phase 4d, the SQL-WHERE path.
    // Compiles the resolved Condition AST to a SQL WHERE fragment
    // against `data` and emits UPDATE … WHERE.
    // ============================================================

    use rekt_expressions::{ComparisonOp, Condition, Operand, Path, PathSegment};

    fn cond_path(name: &str) -> Path {
        Path {
            segments: vec![PathSegment::Name(name.into())],
        }
    }
    fn cond_attr_exists(name: &str) -> Condition {
        Condition::AttributeExists(cond_path(name))
    }
    fn cond_eq(name: &str, value: rekt_protocol::AttributeValue) -> Condition {
        Condition::Compare {
            op: ComparisonOp::Eq,
            left: Operand::Path(cond_path(name)),
            right: Operand::Value(value),
        }
    }
    fn cond_cmp(name: &str, op: ComparisonOp, value: rekt_protocol::AttributeValue) -> Condition {
        Condition::Compare {
            op,
            left: Operand::Path(cond_path(name)),
            right: Operand::Value(value),
        }
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_attribute_exists_succeeds_when_row_present() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_ae").await;
        let shape = TableShape {
            table: "rekt_t_cond_ae",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}}),
            )
            .await
            .unwrap();

        let new_name = serde_json::json!({"S":"alice2"});
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("label", &new_name)],
                &[],
                &cond_attr_exists("id"),
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice2"}}))
        );

        client
            .batch_execute("DROP TABLE rekt_t_cond_ae;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_attribute_exists_fails_when_row_missing() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_ae_miss").await;
        let shape = TableShape {
            table: "rekt_t_cond_ae_miss",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let new_name = serde_json::json!({"S":"x"});
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("missing".into()),
                None,
                &[("label", &new_name)],
                &[],
                &cond_attr_exists("id"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        client
            .batch_execute("DROP TABLE rekt_t_cond_ae_miss;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_eq_optimistic_lock() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_eq").await;
        let shape = TableShape {
            table: "rekt_t_cond_eq",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"version":{"N":"3"}}),
            )
            .await
            .unwrap();

        // Matching version → update applies (set version to 4).
        let new_v = serde_json::json!({"N":"4"});
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("version", &new_v)],
                &[],
                &cond_eq("version", rekt_protocol::AttributeValue::N("3".into())),
            )
            .await
            .unwrap();

        // Now version is 4; condition `version = 3` fails → CCFE.
        let new_v2 = serde_json::json!({"N":"5"});
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("version", &new_v2)],
                &[],
                &cond_eq("version", rekt_protocol::AttributeValue::N("3".into())),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        // Row was not mutated by the failed call — version is still 4.
        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({"id":{"S":"u1"},"version":{"N":"4"}}))
        );

        client
            .batch_execute("DROP TABLE rekt_t_cond_eq;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_numeric_ordering() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_n").await;
        let shape = TableShape {
            table: "rekt_t_cond_n",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"score":{"N":"42"}}),
            )
            .await
            .unwrap();

        // score < 100 → true.
        let new_score = serde_json::json!({"N":"50"});
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("score", &new_score)],
                &[],
                &cond_cmp(
                    "score",
                    ComparisonOp::Lt,
                    rek_n("100"),
                ),
            )
            .await
            .unwrap();

        // score (now 50) > 100 → false → CCFE.
        let new_score2 = serde_json::json!({"N":"99"});
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("score", &new_score2)],
                &[],
                &cond_cmp("score", ComparisonOp::Gt, rek_n("100")),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        client
            .batch_execute("DROP TABLE rekt_t_cond_n;")
            .await
            .unwrap();
    }

    fn rek_n(s: &str) -> rekt_protocol::AttributeValue {
        rekt_protocol::AttributeValue::N(s.into())
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_boolean_and() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_and").await;
        let shape = TableShape {
            table: "rekt_t_cond_and",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({
                    "id":{"S":"u1"},
                    "flag":{"S":"active"},
                    "version":{"N":"1"},
                }),
            )
            .await
            .unwrap();

        // Both terms true → update applies.
        let new_v = serde_json::json!({"N":"2"});
        let cond_both = Condition::And(
            Box::new(cond_eq(
                "flag",
                rekt_protocol::AttributeValue::S("active".into()),
            )),
            Box::new(cond_eq("version", rek_n("1"))),
        );
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("version", &new_v)],
                &[],
                &cond_both,
            )
            .await
            .unwrap();

        // Second term false → CCFE.
        let new_v2 = serde_json::json!({"N":"3"});
        let cond_bad = Condition::And(
            Box::new(cond_eq(
                "flag",
                rekt_protocol::AttributeValue::S("active".into()),
            )),
            Box::new(cond_eq("version", rek_n("999"))),
        );
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("version", &new_v2)],
                &[],
                &cond_bad,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        client
            .batch_execute("DROP TABLE rekt_t_cond_and;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn update_with_cond_attribute_not_exists_on_non_pk_attr() {
        // `attribute_not_exists` on a non-pk attribute is SimpleSql, not
        // InsertOnlyOnPk — exercises the SQL `data ? key` predicate path.
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_cond_ane").await;
        let shape = TableShape {
            table: "rekt_t_cond_ane",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}}),
            )
            .await
            .unwrap();

        // attribute_not_exists(score) on a row that has no `score` → true.
        let v = serde_json::json!({"N":"7"});
        backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("score", &v)],
                &[],
                &Condition::AttributeNotExists(cond_path("score")),
            )
            .await
            .unwrap();

        // Now score exists; same condition → false → CCFE.
        let v2 = serde_json::json!({"N":"10"});
        let err = backend
            .update_with_simple_condition_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                &[("score", &v2)],
                &[],
                &Condition::AttributeNotExists(cond_path("score")),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        client
            .batch_execute("DROP TABLE rekt_t_cond_ane;")
            .await
            .unwrap();
    }

    // ============================================================
    // update_general_rmw_raw: Phase 3b slow path. Tests use a hand-
    // crafted closure to exercise the BEGIN / SELECT FOR UPDATE /
    // UPDATE-or-INSERT-DO-NOTHING / retry flow without depending on
    // translator wiring.
    // ============================================================

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn rmw_inserts_when_row_absent() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_rmw_insert").await;
        let shape = TableShape {
            table: "rekt_t_rmw_insert",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let new_item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
        let captured_clone = captured.clone();
        let new_clone = new_item.clone();
        let apply: GeneralUpdateFn<'_> = Box::new(move |existing| {
            // Record what we saw so the test can assert it was None.
            *captured_clone.lock().unwrap() = Some(existing.cloned());
            Ok(UpdateDecision::Apply(new_clone.clone()))
        });
        backend
            .update_general_rmw_raw(&shape, &KeyValue::S("u1".into()), None, apply)
            .await
            .unwrap();

        assert_eq!(*captured.lock().unwrap(), Some(None));
        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(new_item));

        client
            .batch_execute("DROP TABLE rekt_t_rmw_insert;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn rmw_updates_when_row_present() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_rmw_update").await;
        let shape = TableShape {
            table: "rekt_t_rmw_update",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({"id":{"S":"u1"},"tally":{"N":"5"}});
        backend.put_item_raw(&shape, &original).await.unwrap();

        // Closure: read counter, increment, write back.
        let apply: GeneralUpdateFn<'_> = Box::new(move |existing| {
            let existing = existing.expect("row should be present");
            let cur: i64 = existing["tally"]["N"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let new_item = serde_json::json!({
                "id":{"S":"u1"},
                "tally":{"N":format!("{}", cur + 1)},
            });
            Ok(UpdateDecision::Apply(new_item))
        });
        backend
            .update_general_rmw_raw(&shape, &KeyValue::S("u1".into()), None, apply)
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(serde_json::json!({"id":{"S":"u1"},"tally":{"N":"6"}}))
        );

        client
            .batch_execute("DROP TABLE rekt_t_rmw_update;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn rmw_fail_decision_returns_ccfe_no_mutation() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_rmw_fail").await;
        let shape = TableShape {
            table: "rekt_t_rmw_fail",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        let original = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        backend.put_item_raw(&shape, &original).await.unwrap();

        let apply: GeneralUpdateFn<'_> =
            Box::new(move |_existing| Ok(UpdateDecision::Fail));
        let err = backend
            .update_general_rmw_raw(&shape, &KeyValue::S("u1".into()), None, apply)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        // Row unchanged.
        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got, Some(original));

        client
            .batch_execute("DROP TABLE rekt_t_rmw_fail;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn rmw_concurrent_inc_serializes() {
        // Two concurrent increments on the same row must both apply
        // serially, not lose-write. `SELECT FOR UPDATE` provides the
        // row-level lock.
        let backend = std::sync::Arc::new(PgBackend::connect(&database_url()).unwrap());
        let client = setup_update_hash_table(&backend, "rekt_t_rmw_race").await;
        let shape_owned = (
            "rekt_t_rmw_race".to_string(),
            "id".to_string(),
            "data".to_string(),
        );

        backend
            .put_item_raw(
                &TableShape {
                    table: &shape_owned.0,
                    pk_col: &shape_owned.1,
                    pk_type: KeyType::S,
                    sk_col: None,
                    sk_type: None,
                    jsonb_col: &shape_owned.2,
                },
                &serde_json::json!({"id":{"S":"u1"},"tally":{"N":"0"}}),
            )
            .await
            .unwrap();

        let inc_fn = || {
            let backend = backend.clone();
            let shape_owned = shape_owned.clone();
            async move {
                let shape = TableShape {
                    table: &shape_owned.0,
                    pk_col: &shape_owned.1,
                    pk_type: KeyType::S,
                    sk_col: None,
                    sk_type: None,
                    jsonb_col: &shape_owned.2,
                };
                let apply: GeneralUpdateFn<'_> = Box::new(move |existing| {
                    let existing = existing.expect("row should be present");
                    let cur: i64 = existing["tally"]["N"]
                        .as_str()
                        .unwrap()
                        .parse()
                        .unwrap();
                    Ok(UpdateDecision::Apply(serde_json::json!({
                        "id":{"S":"u1"},
                        "tally":{"N": format!("{}", cur + 1)},
                    })))
                });
                backend
                    .update_general_rmw_raw(&shape, &KeyValue::S("u1".into()), None, apply)
                    .await
            }
        };

        // Spawn N concurrent increments.
        const N: usize = 8;
        let mut handles = vec![];
        for _ in 0..N {
            handles.push(tokio::spawn(inc_fn()));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        let got = backend
            .get_item_raw(
                &TableShape {
                    table: &shape_owned.0,
                    pk_col: &shape_owned.1,
                    pk_type: KeyType::S,
                    sk_col: None,
                    sk_type: None,
                    jsonb_col: &shape_owned.2,
                },
                &KeyValue::S("u1".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            got.unwrap()["tally"]["N"].as_str().unwrap(),
            &N.to_string(),
            "lost-write: N concurrent increments should serialize"
        );

        client
            .batch_execute("DROP TABLE rekt_t_rmw_race;")
            .await
            .unwrap();
    }

    // ============================================================
    // Phase 4e/5/6: slow-path routes via update_general_rmw_raw with
    // a closure that combines evaluator + condition eval. Tests use
    // the rekt-translator helpers to drive the closure exactly as
    // the server would.
    // ============================================================

    fn build_apply<'a>(
        expr: rekt_expressions::UpdateExpression,
        condition: Option<rekt_expressions::Condition>,
        key: rekt_protocol::Item,
    ) -> GeneralUpdateFn<'a> {
        Box::new(move |existing| {
            if let Some(c) = &condition {
                if !rekt_translator::evaluate_condition(existing, c) {
                    return Ok(UpdateDecision::Fail);
                }
            }
            match rekt_translator::apply_update_expression(existing, &expr, &key) {
                Ok(new) => Ok(UpdateDecision::Apply(new)),
                Err(e) => Err(BackendError::Other(e.to_string())),
            }
        })
    }

    fn parse_upd_e2e(
        src: &str,
        values: &[(&str, rekt_protocol::AttributeValue)],
    ) -> rekt_expressions::UpdateExpression {
        let raw = rekt_expressions::parse_update_expression(src).unwrap();
        let names = std::collections::BTreeMap::new();
        let vmap: std::collections::BTreeMap<String, rekt_protocol::AttributeValue> =
            values.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
        rekt_expressions::substitute_update(raw, &names, &vmap).unwrap()
    }

    fn parse_cond_e2e(
        src: &str,
        values: &[(&str, rekt_protocol::AttributeValue)],
    ) -> rekt_expressions::Condition {
        let raw = rekt_expressions::parse_condition_expression(src).unwrap();
        let names = std::collections::BTreeMap::new();
        let vmap: std::collections::BTreeMap<String, rekt_protocol::AttributeValue> =
            values.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
        rekt_expressions::substitute_condition(raw, &names, &vmap).unwrap()
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn slow_path_add_numeric_increments() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_addnum").await;
        let shape = TableShape {
            table: "rekt_t_addnum",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"tally":{"N":"10"}}),
            )
            .await
            .unwrap();

        let key: rekt_protocol::Item = [(
            "id".to_string(),
            rekt_protocol::AttributeValue::S("u1".into()),
        )]
        .into_iter()
        .collect();
        let expr = parse_upd_e2e(
            "ADD tally :inc",
            &[(":inc", rekt_protocol::AttributeValue::N("5".into()))],
        );
        backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr, None, key),
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got.unwrap()["tally"], serde_json::json!({"N":"15"}));

        client
            .batch_execute("DROP TABLE rekt_t_addnum;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn slow_path_add_set_union() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_addset").await;
        let shape = TableShape {
            table: "rekt_t_addset",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"tags":{"SS":["a","b"]}}),
            )
            .await
            .unwrap();

        let key: rekt_protocol::Item = [(
            "id".to_string(),
            rekt_protocol::AttributeValue::S("u1".into()),
        )]
        .into_iter()
        .collect();
        let expr = parse_upd_e2e(
            "ADD tags :new",
            &[(
                ":new",
                rekt_protocol::AttributeValue::Ss(vec!["b".into(), "c".into()]),
            )],
        );
        backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr, None, key),
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        let tags = got.unwrap()["tags"]["SS"].as_array().unwrap().clone();
        assert_eq!(tags.len(), 3);
        for s in ["a", "b", "c"] {
            assert!(tags.iter().any(|v| v == s), "missing {s}");
        }

        client
            .batch_execute("DROP TABLE rekt_t_addset;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn slow_path_delete_set_emptied_removes_attribute() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_del_empty").await;
        let shape = TableShape {
            table: "rekt_t_del_empty",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"tags":{"SS":["a","b"]},"keep":{"S":"x"}}),
            )
            .await
            .unwrap();

        let key: rekt_protocol::Item = [(
            "id".to_string(),
            rekt_protocol::AttributeValue::S("u1".into()),
        )]
        .into_iter()
        .collect();
        let expr = parse_upd_e2e(
            "DELETE tags :rm",
            &[(
                ":rm",
                rekt_protocol::AttributeValue::Ss(vec!["a".into(), "b".into()]),
            )],
        );
        backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr, None, key),
            )
            .await
            .unwrap();

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        let item = got.unwrap();
        assert!(item.get("tags").is_none(), "tags should be removed");
        assert_eq!(item["keep"], serde_json::json!({"S":"x"}));

        client
            .batch_execute("DROP TABLE rekt_t_del_empty;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn slow_path_begins_with_condition() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let client = setup_update_hash_table(&backend, "rekt_t_bw").await;
        let shape = TableShape {
            table: "rekt_t_bw",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };

        backend
            .put_item_raw(
                &shape,
                &serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}}),
            )
            .await
            .unwrap();

        let key: rekt_protocol::Item = [(
            "id".to_string(),
            rekt_protocol::AttributeValue::S("u1".into()),
        )]
        .into_iter()
        .collect();

        // Matching prefix → update applies.
        let expr = parse_upd_e2e(
            "SET marker = :m",
            &[(":m", rekt_protocol::AttributeValue::S("hit".into()))],
        );
        let cond = parse_cond_e2e(
            "begins_with(label, :p)",
            &[(":p", rekt_protocol::AttributeValue::S("ali".into()))],
        );
        backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr, Some(cond), key.clone()),
            )
            .await
            .unwrap();

        // Non-matching prefix → CCFE.
        let expr2 = parse_upd_e2e(
            "SET marker = :m",
            &[(":m", rekt_protocol::AttributeValue::S("nope".into()))],
        );
        let cond2 = parse_cond_e2e(
            "begins_with(label, :p)",
            &[(":p", rekt_protocol::AttributeValue::S("bob".into()))],
        );
        let err = backend
            .update_general_rmw_raw(
                &shape,
                &KeyValue::S("u1".into()),
                None,
                build_apply(expr2, Some(cond2), key),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ConditionalCheckFailed));

        let got = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap();
        assert_eq!(got.unwrap()["marker"], serde_json::json!({"S":"hit"}));

        client
            .batch_execute("DROP TABLE rekt_t_bw;")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires postgres on localhost:5432 (just up)"]
    async fn missing_table_maps_to_table_not_found() {
        let backend = PgBackend::connect(&database_url()).unwrap();
        let shape = TableShape {
            table: "rekt_does_not_exist_anywhere",
            pk_col: "id",
            pk_type: KeyType::S,
            sk_col: None,
            sk_type: None,
            jsonb_col: "data",
        };
        let err = backend
            .get_item_raw(&shape, &KeyValue::S("u1".into()), None)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::TableNotFound { .. }));
    }
}
