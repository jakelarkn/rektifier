//! `TranslateError` + `From` impls for upstream parser/substituter errors.

use rekt_expressions::{ParseError, SubstituteError};
use rekt_storage::KeyType;

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

    /// Bare attribute name in an UpdateExpression matched a DDB reserved
    /// word. Caller should rewrite the expression to use an
    /// ExpressionAttributeName alias (`#x` → "actualName"). Message
    /// format mirrors DDB-local's wire output.
    #[error(
        "Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: {word}"
    )]
    ReservedWordInUpdateExpression { word: String },

    /// Same as above but for ConditionExpression.
    #[error(
        "Invalid ConditionExpression: Attribute name is a reserved keyword; reserved keyword: {word}"
    )]
    ReservedWordInConditionExpression { word: String },

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

    /// All five DDB `ReturnValues` variants (`NONE`, `ALL_NEW`,
    /// `ALL_OLD`, `UPDATED_NEW`, `UPDATED_OLD`) are accepted as of
    /// Phase 7c; this variant remains for any future unknown string.
    #[error("unknown `ReturnValues` value: `{got}`")]
    UnsupportedReturnValues { got: String },

    /// PutItem and DeleteItem accept only `NONE` and `ALL_OLD` per DDB
    /// — `ALL_NEW` and the `UPDATED_*` variants are UpdateItem-only.
    #[error("`ReturnValues={got}` is not valid for {op}: expected `NONE` or `ALL_OLD`")]
    UnsupportedReturnValuesForOp { op: &'static str, got: String },

    // ----- Query / KeyConditionExpression -----
    /// KCE didn't supply `<pk_attr> = :v`. DDB requires exactly one PK
    /// equality term in every KeyConditionExpression.
    #[error(
        "Query condition missed key schema element: `{attr}` (KeyConditionExpression must include `{attr} = :v`)"
    )]
    KeyConditionMissingPkEquality { attr: String },

    /// KCE referenced a non-key attribute. DDB rejects this with a
    /// ValidationException; non-key predicates belong in FilterExpression.
    #[error(
        "Query condition referenced attribute `{attr}` which is not a key attribute (use FilterExpression for non-key predicates)"
    )]
    KeyConditionNonKeyAttr { attr: String },

    /// KCE used an unsupported shape: OR / NOT, function-call (other
    /// than `begins_with`), `attribute_exists` / `attribute_not_exists`,
    /// path-vs-path operands, `IN`, `attribute_type`, `contains` — none
    /// of these are valid KeyConditionExpression. AND is the only
    /// supported boolean combinator and the only legal binary shape is
    /// AND'ing a PK equality with one SK predicate.
    #[error("Query condition has unsupported shape ({reason}) — KeyConditionExpression accepts `pk = :v [AND <sk-predicate>]` only")]
    KeyConditionUnsupportedShape { reason: &'static str },

    /// KCE's PK term used something other than `=` (e.g. `pk < :v`).
    /// DDB requires PK equality.
    #[error("Query condition on partition key `{attr}` must be equality (got `{op}`)")]
    KeyConditionPkMustBeEquality { attr: String, op: &'static str },

    /// KCE's RHS operand was a path reference (e.g. `pk = sk`). DDB
    /// requires the RHS to be a value (or `:v` placeholder).
    #[error("Query condition right-hand side must be a value (path references are not allowed)")]
    KeyConditionRhsMustBeValue,

    /// KCE's `begins_with(sk, ...)` was used against a non-S sort key.
    /// D8 in PLAN-4 defers B-typed prefix matching to a follow-up.
    #[error(
        "begins_with on sort key `{attr}` requires an S-typed sort key (got `{got:?}`) — B-typed prefix matching is a deferred feature"
    )]
    KeyConditionBeginsWithRequiresStringSk {
        attr: String,
        got: rekt_storage::KeyType,
    },

    /// KCE provided an operand whose AttributeValue type doesn't match
    /// the sort-key's declared type (e.g. SK declared as N, KCE used S).
    #[error(
        "Query condition on sort key `{attr}` has wrong type: schema says {expected:?}, got {got}"
    )]
    KeyConditionSkTypeMismatch {
        attr: String,
        expected: rekt_storage::KeyType,
        got: &'static str,
    },

    /// Same as above for the partition key.
    #[error(
        "Query condition on partition key `{attr}` has wrong type: schema says {expected:?}, got {got}"
    )]
    KeyConditionPkTypeMismatch {
        attr: String,
        expected: rekt_storage::KeyType,
        got: &'static str,
    },

    /// Caller-supplied `Limit` was 0 or above rektifier's per-call cap
    /// (1000). DDB's real cap is 1 MB per page (variable item count);
    /// see `COMPATIBILITY_NOTES.md`.
    #[error("Invalid Limit: {got} (must be 1..=1000)")]
    InvalidLimit { got: u32 },

    /// `ExclusiveStartKey` couldn't be decoded as a valid cursor for
    /// this table — missing/extra attrs, wrong types, etc.
    #[error("Invalid ExclusiveStartKey: {reason}")]
    InvalidExclusiveStartKey { reason: String },

    /// `ExclusiveStartKey` carries a partition key that doesn't match
    /// the query's PK. DDB requires the cursor to belong to the
    /// queried partition.
    #[error("ExclusiveStartKey partition key `{attr}` does not match the Query's KeyConditionExpression PK")]
    ExclusiveStartKeyPkMismatch { attr: String },

    // ----- Scan -----
    /// Scan request used a feature rektifier doesn't yet implement
    /// (`IndexName` → GSI; `Segment`/`TotalSegments` → parallel scan).
    /// See `PLAN-4` D4/D5.
    #[error("Scan feature not supported: {what}")]
    ScanFeatureNotSupported { what: &'static str },
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
            // The blanket `From` defaults to the UpdateExpression
            // phrasing; call sites that resolve a ConditionExpression
            // remap it via `map_substitute_for_condition` so the wire
            // message names the right expression.
            SubstituteError::ReservedWord { word } => {
                TranslateError::ReservedWordInUpdateExpression { word }
            }
        }
    }
}

/// Remap a `SubstituteError::ReservedWord` raised during
/// ConditionExpression resolution to the condition-specific variant,
/// leaving placeholder errors unchanged.
pub(crate) fn map_substitute_for_condition(e: SubstituteError) -> TranslateError {
    match e {
        SubstituteError::ReservedWord { word } => {
            TranslateError::ReservedWordInConditionExpression { word }
        }
        other => other.into(),
    }
}
