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
