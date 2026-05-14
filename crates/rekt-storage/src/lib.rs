//! Storage backend trait for rektifier.
//!
//! The MVP shape is task-shaped (one method per DynamoDB op) rather than
//! SQL-shaped (`execute` / `query` / `transaction`). When the translator
//! grows past three or four ops the trait will widen — but until then we
//! don't have enough call sites to design the abstraction correctly, so
//! the methods stay literal.
//!
//! Callers always pass a [`TableShape`] (PG table name + PK / SK / JSONB
//! column names) and typed [`KeyValue`]s. The underlying PG schema is
//! operator-owned: rektifier doesn't create tables, it just reads/writes
//! against tables whose shape the operator declared in config.

use async_trait::async_trait;
use bytes::Bytes;
use rekt_expressions::Condition;

/// DynamoDB-declared type of a key attribute (PK or SK).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// String — PG column type `text`.
    S,
    /// Number — PG column type `numeric`. DDB N is sent as a decimal string;
    /// rektifier passes it verbatim to PG, which parses it losslessly.
    N,
    /// Binary — PG column type `bytea`.
    B,
}

/// Typed key value, ready to bind to a PG parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyValue {
    /// PG column `text`. Stored as-is.
    S(String),
    /// PG column `numeric`. Held as a decimal string; PG parses on insert.
    N(String),
    /// PG column `bytea`. Raw bytes.
    B(Bytes),
}

impl KeyValue {
    pub fn key_type(&self) -> KeyType {
        match self {
            Self::S(_) => KeyType::S,
            Self::N(_) => KeyType::N,
            Self::B(_) => KeyType::B,
        }
    }
}

/// Underlying-backend table shape. For PG: real table name + column names
/// + declared key types. `sk_col = None` means hash-only.
///
/// `pk_type` and `sk_type` are present so backends can pre-resolve SQL
/// (parameter casts, prepared-statement type vectors) at shape-load time
/// rather than per-call from a runtime `KeyValue`.
#[derive(Debug, Clone)]
pub struct TableShape<'a> {
    pub table: &'a str,
    pub pk_col: &'a str,
    pub pk_type: KeyType,
    pub sk_col: Option<&'a str>,
    pub sk_type: Option<KeyType>,
    pub jsonb_col: &'a str,
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("table not found: {name}")]
    TableNotFound { name: String },

    #[error("key type mismatch on `{col}`: shape declares {expected:?}, got {actual:?}")]
    KeyTypeMismatch {
        col: String,
        expected: KeyType,
        actual: KeyType,
    },

    #[error("sort key required for composite table `{name}` but caller passed None")]
    MissingSortKey { name: String },

    #[error("sort key passed for hash-only table `{name}`")]
    UnexpectedSortKey { name: String },

    /// The DDB-level `ConditionExpression` evaluated to false. Surfaces
    /// as `ConditionalCheckFailedException` at the HTTP boundary. The
    /// concrete reason (row exists when caller demanded it not, vs.
    /// row missing when caller demanded it does, vs. arbitrary
    /// predicate false) isn't carried — DDB itself doesn't distinguish.
    #[error("the conditional request failed")]
    ConditionalCheckFailed,

    #[error("storage error: {0}")]
    Other(String),
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Upsert: write `item` into `shape.jsonb_col`. The PG table is expected
    /// to derive `pk_col` (and `sk_col` if present) via `GENERATED ALWAYS
    /// AS ... STORED` columns — rektifier never binds key values on writes.
    /// Matches DynamoDB's default `PutItem` semantics (no `ConditionExpression`).
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        item: &serde_json::Value,
    ) -> Result<(), BackendError>;

    /// Returns `Ok(None)` when the table exists but no row matches.
    /// Returns `Err(BackendError::TableNotFound)` when the table itself
    /// is missing — the server maps that to `ResourceNotFoundException`.
    async fn get_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError>;

    /// Delete one row by full key. Idempotent: returns `Ok(())` whether
    /// the row existed or not, matching DynamoDB's default DeleteItem
    /// semantics (`ReturnValues=NONE` and no `ConditionExpression`).
    async fn delete_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<(), BackendError>;

    /// Apply the "simple subset" of a DynamoDB UpdateExpression to a single
    /// row, atomically. Emits one statement of the shape:
    ///
    /// ```sql
    /// INSERT INTO t (data) VALUES ($insert_item::jsonb)
    /// ON CONFLICT (pk[, sk]) DO UPDATE
    /// SET data = ((jsonb_set(t.data, ARRAY[$a], $v) ...) - $k ...)
    /// ```
    ///
    /// The INSERT branch runs when the row doesn't exist; the DO UPDATE
    /// branch folds `jsonb_set` over `t.data` once per SET clause and
    /// chains `- 'key'` once per REMOVE clause. Single-statement PG MVCC
    /// makes the operation atomic at the row level — no transaction, no
    /// row lock.
    ///
    /// `insert_item` is the DDB-JSON item used for the INSERT branch
    /// (must contain the Key attrs + the SET attr values). `sets` are
    /// `(top-level attribute name, AttributeValue serialized as
    /// DDB-JSON)` pairs. `removes` are top-level attribute names.
    ///
    /// Anything outside the simple subset (path references, arithmetic,
    /// `if_not_exists` / `list_append`, nested paths, ADD / DELETE) is
    /// out of scope for this method — those will route to a future
    /// read-modify-write method.
    ///
    /// Returns the post-update item as DDB-JSON (the value of the `data`
    /// column after the write). Callers that need it for
    /// `ReturnValues=ALL_NEW` use it directly; callers that don't can
    /// discard it. The cost on the wire is one extra row in the
    /// `RETURNING` clause — negligible relative to the round-trip.
    async fn update_simple_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
    ) -> Result<serde_json::Value, BackendError>;

    /// Insert-only-if-the-row-doesn't-exist. Backs the
    /// `ConditionExpression: attribute_not_exists(pk)` case of UpdateItem
    /// (and the same shape from PutItem in a later phase). Emits
    /// `INSERT … ON CONFLICT (pk[,sk]) DO NOTHING`: a successful INSERT
    /// affects 1 row, a conflict leaves 0 rows affected — which the impl
    /// surfaces as [`BackendError::ConditionalCheckFailed`].
    ///
    /// `insert_item` is the DDB-JSON for the new row (caller has already
    /// merged the request's key attrs with whatever SET clauses the
    /// UpdateExpression specified; REMOVE clauses are no-ops here because
    /// the row didn't exist).
    ///
    /// Returns the inserted item on success (same shape as
    /// `update_simple_raw`'s return).
    async fn update_insert_only_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
    ) -> Result<serde_json::Value, BackendError>;

    /// Apply the simple update subset to a single row gated by a
    /// `SimpleSql`-classified ConditionExpression. Emits:
    ///
    /// ```sql
    /// UPDATE t SET data = <jsonb_set chain> - 'k' …
    ///   WHERE pk = $1 [AND sk = $2] AND <compiled cond on data>
    /// ```
    ///
    /// No INSERT branch — DDB semantics treat any condition other than
    /// `attribute_not_exists(pk)` (handled by `update_insert_only_raw`)
    /// as implicitly demanding the row exist. 0 rows affected → either
    /// the row doesn't exist OR the condition was false; both surface
    /// as [`BackendError::ConditionalCheckFailed`].
    ///
    /// Caller guarantees the AST shape is `SimpleSql`-eligible (every
    /// comparison is either equality on any operand shapes, or
    /// ordering with a Path operand and a Value operand of type N or
    /// S). Anything outside that subset is the `NeedsTx` slow path,
    /// which is a future phase.
    ///
    /// Returns the post-update item on success.
    async fn update_with_simple_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
        condition: &Condition,
    ) -> Result<serde_json::Value, BackendError>;

    /// Slow-path read-modify-write for any UpdateItem shape that can't
    /// run on the fast paths: non-simple UpdateExpression RHS forms
    /// (path-ref / arithmetic / `if_not_exists` / `list_append`) and
    /// ConditionExpression shapes the SQL compiler can't express
    /// (`NeedsTx` routing — e.g., path-vs-path ordering, future
    /// `begins_with` / `contains` / `BETWEEN` / `IN`).
    ///
    /// The backend runs:
    ///
    /// 1. `BEGIN`
    /// 2. `SELECT data FROM t WHERE pk = $1 [AND sk = $2] FOR UPDATE`
    /// 3. Call `apply(existing)` — caller's closure decides
    ///    [`UpdateDecision`].
    /// 4. `Apply(new)` → `UPDATE` if the row was present, else
    ///    `INSERT … ON CONFLICT DO NOTHING` (race-guarded; 0 rows
    ///    affected means a concurrent insert won — loop back to step 2,
    ///    which now finds the row and goes down the UPDATE branch).
    ///    `Fail` → `BackendError::ConditionalCheckFailed`.
    /// 5. `COMMIT`
    ///
    /// The closure is `Fn` (not `FnOnce`) so the retry loop can call it
    /// again on a race. It's pure — no I/O — and runs entirely against
    /// the JSON view of the row.
    ///
    /// Returns the post-update item — the value the closure returned
    /// from its successful `UpdateDecision::Apply(new)`.
    async fn update_general_rmw_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        apply: GeneralUpdateFn<'_>,
    ) -> Result<serde_json::Value, BackendError>;
}

/// Caller-supplied decision function for the slow path. Synchronous —
/// runs against a freshly-fetched row inside the backend's transaction.
pub type GeneralUpdateFn<'a> = Box<
    dyn Fn(Option<&serde_json::Value>) -> Result<UpdateDecision, BackendError>
        + Send
        + Sync
        + 'a,
>;

/// What the slow path should do given the row state.
#[derive(Debug, Clone)]
pub enum UpdateDecision {
    /// Write `new` as the row's new state (replaces if the row existed,
    /// inserts otherwise).
    Apply(serde_json::Value),
    /// ConditionExpression evaluated to false — surface
    /// `ConditionalCheckFailed` and roll back.
    Fail,
}
