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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    /// Pool exhausted: every connection is in use and the
    /// `wait_timeout` elapsed before one became available. Retryable
    /// (Obs-4): the request may succeed on a fresh attempt once load
    /// drops.
    #[error("PG connection pool exhausted (waited {waited_ms} ms)")]
    PoolExhausted { waited_ms: u64 },

    /// Could not open a connection to PG (TCP refused, auth failure,
    /// timeout, etc). Retryable for the TCP / timeout subclasses;
    /// auth failures are operator-actionable. v1 doesn't discriminate
    /// — the retry layer treats every `ConnectFailed` as transient.
    #[error("PG connection failed: {reason}")]
    ConnectFailed { reason: String },

    #[error("storage error: {0}")]
    Other(String),
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Upsert: write `item` into `shape.jsonb_col`. The PG table is expected
    /// to derive `pk_col` (and `sk_col` if present) via `GENERATED ALWAYS
    /// AS ... STORED` columns — rektifier never binds key values on writes.
    /// Matches DynamoDB's default `PutItem` semantics (no `ConditionExpression`).
    ///
    /// Returns the pre-existing row at the same key, if any. Callers that
    /// don't need it (e.g. `ReturnValues=NONE`) ignore the value; callers
    /// implementing `ReturnValues=ALL_OLD` return it as the response's
    /// `Attributes` field. `pk` (+ optional `sk`) is bound to a CTE that
    /// snapshots the prior row at the same MVCC point as the upsert.
    async fn put_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        item: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>, BackendError>;

    /// Conditional PutItem. Reads the current row under `SELECT FOR UPDATE`,
    /// invokes `condition` against it (with `None` for a missing row),
    /// and on `true` replaces it with `item`. On `false` returns
    /// [`BackendError::ConditionalCheckFailed`] and rolls back. Returns
    /// the pre-update row (or `None` if it didn't exist) on success —
    /// same shape as [`Backend::put_item_raw`].
    ///
    /// The condition is passed as a closure (rather than the AST
    /// directly) so the storage layer doesn't have to depend on the
    /// expression evaluator. The caller wraps
    /// `rekt_translator::evaluate_condition` to match the trait shape.
    async fn put_with_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        item: &serde_json::Value,
        condition: ConditionEvalFn<'_>,
    ) -> Result<Option<serde_json::Value>, BackendError>;

    /// Returns `Ok(None)` when the table exists but no row matches.
    /// Returns `Err(BackendError::TableNotFound)` when the table itself
    /// is missing — the server maps that to `ResourceNotFoundException`.
    async fn get_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError>;

    /// Delete one row by full key. Idempotent: returns `Ok(None)` whether
    /// the row existed or not, matching DynamoDB's default DeleteItem
    /// semantics. The returned `Option` carries the deleted row (when
    /// one was removed) for `ReturnValues=ALL_OLD`; `ReturnValues=NONE`
    /// callers ignore it.
    async fn delete_item_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
    ) -> Result<Option<serde_json::Value>, BackendError>;

    /// Conditional DeleteItem. Reads the current row under `SELECT FOR
    /// UPDATE`, invokes `condition` against it (with `None` for a
    /// missing row — so `attribute_not_exists(pk)` evaluates true,
    /// `attribute_exists(pk)` false). On `true` → DELETE and return the
    /// deleted row (or `None` if the row genuinely didn't exist and the
    /// condition still passed, e.g. `attribute_not_exists(pk)`). On
    /// `false` → [`BackendError::ConditionalCheckFailed`].
    async fn delete_with_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        condition: ConditionEvalFn<'_>,
    ) -> Result<Option<serde_json::Value>, BackendError>;

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
    /// Returns an [`UpdateOutcome`] carrying both the post-update item
    /// (always present on success) and the pre-update item (`None` if
    /// this call inserted a new row; `Some` if it merged into an
    /// existing row). Callers that need `ReturnValues=ALL_NEW` /
    /// `ALL_OLD` read the corresponding field; callers that need
    /// `NONE` can ignore both. The CTE that snapshots the old row
    /// runs in the same statement as the upsert so the old/new pair
    /// is read at a consistent MVCC snapshot.
    ///
    /// `pk` (+ optional `sk`) is bound for the CTE's WHERE clause —
    /// the upsert itself still derives its key columns from the
    /// JSONB via `GENERATED ALWAYS AS`, so passing keys explicitly
    /// only adds the snapshot read.
    async fn update_simple_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        insert_item: &serde_json::Value,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
    ) -> Result<UpdateOutcome, BackendError>;

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
    /// Returns the inserted item as `new_item` and `old_item = None`
    /// (the call succeeds iff the row didn't exist beforehand, so by
    /// definition there's no pre-update item to surface).
    async fn update_insert_only_raw(
        &self,
        shape: &TableShape<'_>,
        insert_item: &serde_json::Value,
    ) -> Result<UpdateOutcome, BackendError>;

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
    /// Returns the pre-update item (always `Some` because UPDATE
    /// requires the row to exist) and the post-update item. The
    /// snapshot is read in the same statement via a CTE, so the
    /// pair is consistent.
    async fn update_with_simple_condition_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        sets: &[(&str, &serde_json::Value)],
        removes: &[&str],
        condition: &Condition,
    ) -> Result<UpdateOutcome, BackendError>;

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
    /// Returns the `existing` row read via `SELECT FOR UPDATE` as the
    /// pre-update item (`None` if the row didn't exist), and the
    /// `new_item` produced by the closure's successful
    /// `UpdateDecision::Apply(new)` as the post-update item.
    async fn update_general_rmw_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk: Option<&KeyValue>,
        apply: GeneralUpdateFn<'_>,
    ) -> Result<UpdateOutcome, BackendError>;

    /// Query — bounded read over a single partition.
    ///
    /// Emits a `SELECT … FROM <table> WHERE pk_col = $1 [AND <sk-cond>]
    /// [AND <sk-col> > <esk-sk>] ORDER BY sk_col ASC LIMIT $L+1`
    /// against the table's indexed key columns. `pk` always
    /// equality-matches; `sk_condition` (when set) adds the SK
    /// predicate. `exclusive_start_key` (when set) adds a keyset-
    /// pagination predicate (`sk > esk_sk` for forward order in v1; Q5
    /// will add descending).
    ///
    /// The backend reads `limit + 1` rows; if `limit + 1` came back, it
    /// drops the sentinel, populates `last_evaluated_key` from the
    /// L-th row's keys, and the caller passes that back as the next
    /// ESK. `count` and `scanned_count` are equal until Q3 adds
    /// FilterExpression.
    #[allow(clippy::too_many_arguments)]
    async fn query_raw(
        &self,
        shape: &TableShape<'_>,
        pk: &KeyValue,
        sk_condition: Option<&SkCondition>,
        limit: Option<u32>,
        exclusive_start_key: Option<(&KeyValue, Option<&KeyValue>)>,
        filter: Option<FilterEvalFn<'_>>,
        forward: bool,
    ) -> Result<QueryOutcome, BackendError>;

    /// Scan — unbounded read over the whole table.
    ///
    /// Emits `SELECT <jsonb_col> FROM <table>
    ///   [WHERE (<pk_col>, <sk_col>) > ($esk_pk, $esk_sk)]
    ///   ORDER BY <pk_col> [, <sk_col>] LIMIT $L+1`.
    /// Reads `L+1` rows so the backend can populate
    /// `last_evaluated_key` from the L-th row when more rows remain.
    /// Filter (when supplied) runs per row after the read — same
    /// pattern as Query, with `scanned_count` (pre-filter) vs `count`
    /// (post-filter).
    async fn scan_raw(
        &self,
        shape: &TableShape<'_>,
        filter: Option<FilterEvalFn<'_>>,
        limit: Option<u32>,
        exclusive_start_key: Option<(&KeyValue, Option<&KeyValue>)>,
    ) -> Result<ScanOutcome, BackendError>;

    /// BatchWriteItem — multi-row upsert + multi-key delete against
    /// a single table, in one transaction.
    ///
    /// `ops` may freely mix `WriteOp::Put` and `WriteOp::Delete`. The
    /// Put-statement runs before the Delete-statement within the
    /// transaction; request-position interleaving within the input
    /// `&[WriteOp]` slice is not preserved across the two emitted
    /// statements (matches DDB observationally — see PLAN-6 D3).
    ///
    /// Caller is responsible for deduplication: the translator
    /// rejects "same key twice in one table" and "Put + Delete of the
    /// same key" before this method is reached.
    ///
    /// Returns a `BatchWriteOutcome` (currently always empty — v1
    /// never produces `UnprocessedItems`).
    async fn batch_write_raw(
        &self,
        shape: &TableShape<'_>,
        ops: &[WriteOp],
    ) -> Result<BatchWriteOutcome, BackendError>;

    /// BatchGetItem — multi-key SELECT against a single table.
    ///
    /// Emits one SQL statement, regardless of `keys.len()`. Hash-only
    /// tables use `WHERE pk_col = ANY($1::<pk_type>[])`; composite-key
    /// tables use `WHERE (pk_col, sk_col) IN (VALUES ($1, $2), ($3,
    /// $4), ...)`. Missing rows are silently absent from the returned
    /// vec — DDB returns hits, not "miss" sentinels.
    ///
    /// `keys` is caller-deduplicated; the translator rejects duplicates
    /// before this method is reached, so the SQL emitter doesn't need
    /// to compensate. Caller must not depend on response order (no
    /// `ORDER BY` is emitted — D9 in `PLAN-6`).
    async fn batch_get_raw(
        &self,
        shape: &TableShape<'_>,
        keys: &[(KeyValue, Option<KeyValue>)],
    ) -> Result<BatchGetOutcome, BackendError>;
}

/// A single write request inside a per-table BatchWriteItem batch.
/// Mirrors the wire shape but pre-typed for the storage layer; the
/// translator extracts `(pk, sk)` from the request's `Item` (Put) or
/// `Key` (Delete) before this enum is constructed.
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// PutItem-equivalent upsert: replaces any existing row at the
    /// derived `(pk[, sk])`. `item` is the full DDB-JSON item that
    /// will be written to the jsonb column; PG derives the key
    /// columns from it via `GENERATED ALWAYS AS`.
    Put {
        pk: KeyValue,
        sk: Option<KeyValue>,
        item: serde_json::Value,
    },
    /// DeleteItem-equivalent: idempotent on missing rows.
    Delete {
        pk: KeyValue,
        sk: Option<KeyValue>,
    },
}

/// Result of a `batch_write_raw` call. Always `Default` in v1
/// (success is the only signalled outcome — any SQL-level failure
/// surfaces as a `BackendError`). The struct exists so B7's
/// `unprocessed` field can be added without widening the trait.
#[derive(Debug, Clone, Default)]
pub struct BatchWriteOutcome {
    // Future fields (unused in v1):
    //   pub unprocessed: Vec<WriteOp>,
}

/// Result of a `batch_get_raw` call. v1 consumes only `items.len()`
/// and the items themselves; the struct exists so B7 can add
/// per-call metrics (e.g. `scanned_count` for PG-level row counts)
/// or partial-failure markers (e.g. `unprocessed` for the
/// `UnprocessedKeys` response field) without widening the trait.
#[derive(Debug, Clone, Default)]
pub struct BatchGetOutcome {
    pub items: Vec<serde_json::Value>,
    // Future fields (unused in v1, retained as comments so the shape
    // is visible at the trait surface):
    //   pub scanned_count: u32,
    //   pub unprocessed: Vec<(KeyValue, Option<KeyValue>)>,
}

/// Result of a `scan_raw` call. Structurally identical to
/// [`QueryOutcome`] for now; kept separate so the two can evolve
/// independently as Q5/Q7 lift their respective feature sets.
#[derive(Debug, Clone, Default)]
pub struct ScanOutcome {
    pub items: Vec<serde_json::Value>,
    pub count: u32,
    pub scanned_count: u32,
    pub last_evaluated_key: Option<(KeyValue, Option<KeyValue>)>,
}

/// Sort-key predicate the translator hands to `query_raw`. PK is always
/// equality-matched (DDB requires it), so it doesn't appear here.
/// `BeginsWithS` carries the prefix string directly because Q1 only
/// supports S-typed SK prefix matches — D8 in `PLAN-4` defers B-typed
/// prefix to a follow-up.
#[derive(Debug, Clone)]
pub enum SkCondition {
    Eq(KeyValue),
    Lt(KeyValue),
    Le(KeyValue),
    Gt(KeyValue),
    Ge(KeyValue),
    Between(KeyValue, KeyValue),
    BeginsWithS(String),
}

/// Result of a `query_raw` call. `count` is the post-filter item count
/// (`= items.len()` in Q1; will diverge from `scanned_count` when Q3
/// adds FilterExpression). `last_evaluated_key` is `None` in Q1 — Q2
/// populates it for keyset pagination.
#[derive(Debug, Clone, Default)]
pub struct QueryOutcome {
    pub items: Vec<serde_json::Value>,
    pub count: u32,
    pub scanned_count: u32,
    pub last_evaluated_key: Option<(KeyValue, Option<KeyValue>)>,
}

/// Pre- and post-update view of a row, returned by the four
/// `update_*_raw` methods on success. `new_item` is always populated;
/// `old_item` is `Some` iff the row existed before the call.
///
/// Phase 7a uses `new_item` for `ReturnValues=ALL_NEW`; Phase 7b uses
/// `old_item` for `ALL_OLD`. Future Phase 7c (`UPDATED_*`) can be
/// computed by projecting the pair against translator-tracked touched
/// paths without additional backend work.
#[derive(Debug, Clone)]
pub struct UpdateOutcome {
    pub new_item: serde_json::Value,
    pub old_item: Option<serde_json::Value>,
}

/// Caller-supplied decision function for the slow path. Synchronous —
/// runs against a freshly-fetched row inside the backend's transaction.
pub type GeneralUpdateFn<'a> = Box<
    dyn Fn(Option<&serde_json::Value>) -> Result<UpdateDecision, BackendError>
        + Send
        + Sync
        + 'a,
>;

/// Caller-supplied condition evaluator for conditional Put / Delete.
/// Receives the current row (`None` if missing) and returns `true` to
/// proceed, `false` to reject with [`BackendError::ConditionalCheckFailed`].
/// The dispatcher typically wraps `rekt_translator::evaluate_condition`
/// over the parsed AST so the storage layer doesn't have to depend on
/// the expression evaluator.
pub type ConditionEvalFn<'a> =
    Box<dyn Fn(Option<&serde_json::Value>) -> bool + Send + Sync + 'a>;

/// Caller-supplied row filter for Query / Scan `FilterExpression`.
/// Runs against each row already fetched from PG; returns `true` to
/// include the row in the response, `false` to drop it. Unlike
/// [`ConditionEvalFn`], the row is always present (never `None`) —
/// Query/Scan only ever see real rows. Tracked separately to keep
/// `scanned_count` (pre-filter, derived from PG read) distinct from
/// `count` (post-filter).
pub type FilterEvalFn<'a> =
    Box<dyn Fn(&serde_json::Value) -> bool + Send + Sync + 'a>;

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
