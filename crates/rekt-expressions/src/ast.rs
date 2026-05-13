//! AST types for `UpdateExpression`. Two flavors:
//!
//! - `Raw*` types carry placeholder references (`:v`, `#n`) verbatim from
//!   the source text. They're what the parser produces.
//! - The resolved types (`UpdateExpression`, `SetClause`, etc.) carry
//!   actual `AttributeValue`s and attribute-name strings. They're what
//!   `substitute_update` produces from a raw AST plus the
//!   `ExpressionAttributeNames` / `ExpressionAttributeValues` maps.
//!
//! Downstream code (translator, storage planner) consumes the resolved
//! types only.

use rekt_protocol::AttributeValue;

// ===== Resolved AST (post-substitution) ======================================

#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpdateExpression {
    pub set: Vec<SetClause>,
    pub remove: Vec<Path>,
    pub add: Vec<AddAction>,
    pub delete: Vec<DeleteAction>,
}

impl UpdateExpression {
    /// True iff every clause fits the storage layer's pure-SQL fast path:
    /// a top-level `SET attr = literal` (RHS is a resolved `:v` value of
    /// any AttributeValue variant) or `REMOVE attr`. ADD / DELETE / nested
    /// paths / path-reference / arithmetic / function-call RHS all return
    /// false — those route to the read-modify-write fallback in a later
    /// phase.
    pub fn is_simple(&self) -> bool {
        self.add.is_empty()
            && self.delete.is_empty()
            && self
                .set
                .iter()
                .all(|c| c.path.is_top_level() && matches!(c.value, SetRhs::Operand(Operand::Value(_))))
            && self.remove.iter().all(|p| p.is_top_level())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    pub path: Path,
    pub value: SetRhs,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetRhs {
    /// Either a literal value (from `:v`) or a path reference (`SET a = b`).
    Operand(Operand),
    /// `operand + operand` (numeric addition)
    Plus(Operand, Operand),
    /// `operand - operand` (numeric subtraction)
    Minus(Operand, Operand),
    /// `if_not_exists(path, value)`
    IfNotExists(Path, Box<SetRhs>),
    /// `list_append(value, value)` — values can be paths or list literals.
    ListAppend(Box<SetRhs>, Box<SetRhs>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Path(Path),
    Value(AttributeValue),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddAction {
    pub path: Path,
    pub value: AttributeValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteAction {
    pub path: Path,
    pub value: AttributeValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Path {
    pub segments: Vec<PathSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// A named attribute (post-substitution, so `#n` is resolved).
    Name(String),
    /// A list-index segment, e.g., `items[3]`.
    Index(usize),
}

impl Path {
    /// True if the path is a single name with no `.` or `[]` accessors —
    /// i.e., a bare top-level attribute. Used by the storage planner to
    /// detect whether an UpdateExpression is "simple enough" for the
    /// fast-path SQL emitter.
    pub fn is_top_level(&self) -> bool {
        matches!(self.segments.as_slice(), [PathSegment::Name(_)])
    }

    /// First segment as a name; useful for the simple-case predicate.
    pub fn top_name(&self) -> Option<&str> {
        match self.segments.first() {
            Some(PathSegment::Name(n)) => Some(n),
            _ => None,
        }
    }
}

// ===== ConditionExpression (resolved) ========================================
//
// Used by UpdateItem / DeleteItem / PutItem / TransactWriteItems. Reuses
// the shared `Path` and `Operand` types from above.
//
// v1 grammar covers: attribute_exists / attribute_not_exists, comparison
// operators, AND / OR / NOT, parens. Deferred: BETWEEN, IN, begins_with,
// contains, size, attribute_type, nested paths — Phase 4e and later.

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    /// `op1 op op2`
    Compare {
        op: ComparisonOp,
        left: Operand,
        right: Operand,
    },
    /// `attribute_exists(path)`
    AttributeExists(Path),
    /// `attribute_not_exists(path)`
    AttributeNotExists(Path),
    /// `cond AND cond`
    And(Box<Condition>, Box<Condition>),
    /// `cond OR cond`
    Or(Box<Condition>, Box<Condition>),
    /// `NOT cond`
    Not(Box<Condition>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq, // =
    Ne, // <>
    Lt, // <
    Le, // <=
    Gt, // >
    Ge, // >=
}

impl ComparisonOp {
    /// The on-wire token form, useful for error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        }
    }
}

// ===== Raw AST (pre-substitution; what the parser emits) =====================

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RawUpdateExpression {
    pub set: Vec<RawSetClause>,
    pub remove: Vec<RawPath>,
    pub add: Vec<RawAddAction>,
    pub delete: Vec<RawDeleteAction>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawSetClause {
    pub path: RawPath,
    pub value: RawSetRhs,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RawSetRhs {
    Operand(RawOperand),
    Plus(RawOperand, RawOperand),
    Minus(RawOperand, RawOperand),
    IfNotExists(RawPath, Box<RawSetRhs>),
    ListAppend(Box<RawSetRhs>, Box<RawSetRhs>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RawOperand {
    Path(RawPath),
    /// `:v` value placeholder, contains the identifier (without the leading `:`).
    ValueRef(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawAddAction {
    pub path: RawPath,
    pub value: RawOperand,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawDeleteAction {
    pub path: RawPath,
    pub value: RawOperand,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPath {
    pub segments: Vec<RawPathSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawPathSegment {
    /// A bare identifier path segment.
    Name(String),
    /// `#x` placeholder for an attribute name.
    NameRef(String),
    /// `[N]` list index.
    Index(usize),
}

// ===== ConditionExpression (raw, pre-substitution) ===========================

#[derive(Debug, Clone, PartialEq)]
pub enum RawCondition {
    Compare {
        op: ComparisonOp,
        left: RawOperand,
        right: RawOperand,
    },
    AttributeExists(RawPath),
    AttributeNotExists(RawPath),
    And(Box<RawCondition>, Box<RawCondition>),
    Or(Box<RawCondition>, Box<RawCondition>),
    Not(Box<RawCondition>),
}
