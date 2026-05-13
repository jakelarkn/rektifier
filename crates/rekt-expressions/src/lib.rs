//! Parser + AST for DynamoDB expression languages.
//!
//! Phase 1 covers `UpdateExpression`. `FilterExpression`,
//! `ConditionExpression`, and `KeyConditionExpression` follow in later
//! phases — they share much of the same grammar (paths, operands,
//! placeholders) so the helpers are designed to be reused.
//!
//! ## Two-step model
//!
//! 1. **Parse** the expression source string into a `RawUpdateExpression`
//!    that contains `:v` and `#n` placeholders.
//! 2. **Substitute** `ExpressionAttributeNames` (#n → actual name) and
//!    `ExpressionAttributeValues` (:v → AttributeValue) into the raw AST
//!    to produce a fully-resolved `UpdateExpression`.
//!
//! Splitting parse from substitute lets us reuse the parser and gives
//! callers a chance to validate the placeholder maps before resolving.
//!
//! ## DynamoDB grammar (UpdateExpression subset)
//!
//! ```text
//! UpdateExpression  ::= ( SET    set_action    (',' set_action)*    )?
//!                       ( REMOVE remove_action (',' remove_action)* )?
//!                       ( ADD    add_action    (',' add_action)*    )?
//!                       ( DELETE delete_action (',' delete_action)* )?
//!
//! set_action        ::= path '=' set_rhs
//! remove_action     ::= path
//! add_action        ::= path operand
//! delete_action     ::= path operand
//!
//! set_rhs           ::= operand
//!                     | operand '+' operand
//!                     | operand '-' operand
//!                     | 'if_not_exists' '(' path ',' set_rhs ')'
//!                     | 'list_append'  '(' set_rhs ',' set_rhs ')'
//!
//! operand           ::= path | value_ref
//! path              ::= path_seg ( '.' path_seg | '[' INDEX ']' )*
//! path_seg          ::= NAME | name_ref
//! name_ref          ::= '#' ident
//! value_ref         ::= ':' ident
//! ```

pub mod ast;
mod parser;
mod substitute;

pub use ast::{
    AddAction, DeleteAction, Operand, Path, PathSegment, RawOperand, RawPath, RawPathSegment,
    RawSetClause, RawSetRhs, RawUpdateExpression, SetClause, SetRhs, UpdateExpression,
};
pub use parser::{parse_update_expression, ParseError};
pub use substitute::{substitute_update, SubstituteError};
