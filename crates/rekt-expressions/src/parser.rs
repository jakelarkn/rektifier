//! winnow-based parsers for DDB expression languages: `UpdateExpression`
//! (`parse_update_expression`) and `ConditionExpression`
//! (`parse_condition_expression`). Both produce raw ASTs with `:v` and
//! `#n` placeholders verbatim; downstream `substitute_*` functions
//! resolve them.

use winnow::ascii::{digit1, multispace0, multispace1, Caseless};
use winnow::combinator::{alt, delimited, fail, opt, preceded, repeat, separated, terminated};
use winnow::error::{ContextError, ParseError as WinnowParseError};
use winnow::token::take_while;
use winnow::ModalResult;
use winnow::Parser;

use crate::ast::*;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("{kind} parse error at position {position}: {message}")]
    Invalid {
        kind: &'static str,
        position: usize,
        message: String,
    },

    #[error("{kind} is empty")]
    Empty { kind: &'static str },

    #[error(
        "UpdateExpression contains duplicate `{clause}` keyword — DDB requires actions of one kind to be grouped under a single `SET`/`REMOVE`/`ADD`/`DELETE` keyword"
    )]
    DuplicateKeyword { clause: &'static str },
}

impl ParseError {
    fn from_winnow(
        kind: &'static str,
        input_orig: &str,
        e: WinnowParseError<&str, ContextError>,
    ) -> Self {
        let position = input_orig.len() - e.input().len();
        Self::Invalid {
            kind,
            position,
            message: e.inner().to_string(),
        }
    }
}

pub fn parse_update_expression(input: &str) -> Result<RawUpdateExpression, ParseError> {
    if input.trim().is_empty() {
        return Err(ParseError::Empty {
            kind: "UpdateExpression",
        });
    }
    let raw_blocks = full_expression
        .parse(input)
        .map_err(|e| ParseError::from_winnow("UpdateExpression", input, e))?;

    let mut expr = RawUpdateExpression::default();
    let mut seen = [false; 4];
    for c in raw_blocks {
        match c {
            RawClauseBlock::Set(mut v) => {
                if seen[0] {
                    return Err(ParseError::DuplicateKeyword { clause: "SET" });
                }
                seen[0] = true;
                expr.set.append(&mut v);
            }
            RawClauseBlock::Remove(mut v) => {
                if seen[1] {
                    return Err(ParseError::DuplicateKeyword { clause: "REMOVE" });
                }
                seen[1] = true;
                expr.remove.append(&mut v);
            }
            RawClauseBlock::Add(mut v) => {
                if seen[2] {
                    return Err(ParseError::DuplicateKeyword { clause: "ADD" });
                }
                seen[2] = true;
                expr.add.append(&mut v);
            }
            RawClauseBlock::Delete(mut v) => {
                if seen[3] {
                    return Err(ParseError::DuplicateKeyword { clause: "DELETE" });
                }
                seen[3] = true;
                expr.delete.append(&mut v);
            }
        }
    }

    Ok(expr)
}

// ===== Top-level grammar =====================================================

#[derive(Debug)]
enum RawClauseBlock {
    Set(Vec<RawSetClause>),
    Remove(Vec<RawPath>),
    Add(Vec<RawAddAction>),
    Delete(Vec<RawDeleteAction>),
}

fn full_expression(input: &mut &str) -> ModalResult<Vec<RawClauseBlock>> {
    let _ = multispace0.parse_next(input)?;
    let blocks: Vec<RawClauseBlock> =
        repeat(1.., terminated(clause_block, multispace0)).parse_next(input)?;
    Ok(blocks)
}

fn clause_block(input: &mut &str) -> ModalResult<RawClauseBlock> {
    alt((
        set_block.map(RawClauseBlock::Set),
        remove_block.map(RawClauseBlock::Remove),
        add_block.map(RawClauseBlock::Add),
        delete_block.map(RawClauseBlock::Delete),
    ))
    .parse_next(input)
}

// ===== Individual clause blocks ==============================================

fn set_block(input: &mut &str) -> ModalResult<Vec<RawSetClause>> {
    let _ = Caseless("SET").parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    separated(1.., set_action, comma_sep).parse_next(input)
}

fn remove_block(input: &mut &str) -> ModalResult<Vec<RawPath>> {
    let _ = Caseless("REMOVE").parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    separated(1.., path, comma_sep).parse_next(input)
}

fn add_block(input: &mut &str) -> ModalResult<Vec<RawAddAction>> {
    let _ = Caseless("ADD").parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    separated(1.., add_action, comma_sep).parse_next(input)
}

fn delete_block(input: &mut &str) -> ModalResult<Vec<RawDeleteAction>> {
    let _ = Caseless("DELETE").parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    separated(1.., delete_action, comma_sep).parse_next(input)
}

fn comma_sep(input: &mut &str) -> ModalResult<()> {
    let _ = (multispace0, ',', multispace0).parse_next(input)?;
    Ok(())
}

// ===== Action shapes =========================================================

fn set_action(input: &mut &str) -> ModalResult<RawSetClause> {
    let p = path.parse_next(input)?;
    let _ = (multispace0, '=', multispace0).parse_next(input)?;
    let v = set_rhs.parse_next(input)?;
    Ok(RawSetClause { path: p, value: v })
}

fn add_action(input: &mut &str) -> ModalResult<RawAddAction> {
    let p = path.parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    let v = operand.parse_next(input)?;
    Ok(RawAddAction { path: p, value: v })
}

fn delete_action(input: &mut &str) -> ModalResult<RawDeleteAction> {
    let p = path.parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    let v = operand.parse_next(input)?;
    Ok(RawDeleteAction { path: p, value: v })
}

// ===== SET right-hand side ===================================================
//
// set_rhs := function_call | operand ( ('+' | '-') operand )?

fn set_rhs(input: &mut &str) -> ModalResult<RawSetRhs> {
    alt((if_not_exists_call, list_append_call, operand_or_arith)).parse_next(input)
}

fn if_not_exists_call(input: &mut &str) -> ModalResult<RawSetRhs> {
    let _ = Caseless("if_not_exists").parse_next(input)?;
    let _ = (multispace0, '(', multispace0).parse_next(input)?;
    let p = path.parse_next(input)?;
    let _ = (multispace0, ',', multispace0).parse_next(input)?;
    let v = set_rhs.parse_next(input)?;
    let _ = (multispace0, ')').parse_next(input)?;
    Ok(RawSetRhs::IfNotExists(p, Box::new(v)))
}

fn list_append_call(input: &mut &str) -> ModalResult<RawSetRhs> {
    let _ = Caseless("list_append").parse_next(input)?;
    let _ = (multispace0, '(', multispace0).parse_next(input)?;
    let a = set_rhs.parse_next(input)?;
    let _ = (multispace0, ',', multispace0).parse_next(input)?;
    let b = set_rhs.parse_next(input)?;
    let _ = (multispace0, ')').parse_next(input)?;
    Ok(RawSetRhs::ListAppend(Box::new(a), Box::new(b)))
}

fn operand_or_arith(input: &mut &str) -> ModalResult<RawSetRhs> {
    let a = operand.parse_next(input)?;
    let tail = opt(arith_tail).parse_next(input)?;
    match tail {
        None => Ok(RawSetRhs::Operand(a)),
        Some((b'+', b)) => Ok(RawSetRhs::Plus(a, b)),
        Some((b'-', b)) => Ok(RawSetRhs::Minus(a, b)),
        Some(_) => fail.parse_next(input),
    }
}

fn arith_tail(input: &mut &str) -> ModalResult<(u8, RawOperand)> {
    let _ = multispace0.parse_next(input)?;
    let op = alt(('+', '-')).parse_next(input)?;
    let _ = multispace0.parse_next(input)?;
    let b = operand.parse_next(input)?;
    Ok((op as u8, b))
}

fn operand(input: &mut &str) -> ModalResult<RawOperand> {
    alt((
        value_ref.map(RawOperand::ValueRef),
        path.map(RawOperand::Path),
    ))
    .parse_next(input)
}

// ===== Paths, names, values ==================================================

fn path(input: &mut &str) -> ModalResult<RawPath> {
    let first = path_seg.parse_next(input)?;
    let rest: Vec<RawPathSegment> = repeat(0.., path_continuation).parse_next(input)?;
    let mut segments = Vec::with_capacity(1 + rest.len());
    segments.push(first);
    segments.extend(rest);
    Ok(RawPath { segments })
}

fn path_seg(input: &mut &str) -> ModalResult<RawPathSegment> {
    alt((
        name_ref.map(RawPathSegment::NameRef),
        ident.map(RawPathSegment::Name),
    ))
    .parse_next(input)
}

fn path_continuation(input: &mut &str) -> ModalResult<RawPathSegment> {
    alt((dot_segment, bracket_index)).parse_next(input)
}

fn dot_segment(input: &mut &str) -> ModalResult<RawPathSegment> {
    let _ = '.'.parse_next(input)?;
    path_seg.parse_next(input)
}

fn bracket_index(input: &mut &str) -> ModalResult<RawPathSegment> {
    let n: &str = delimited('[', digit1, ']').parse_next(input)?;
    n.parse::<usize>()
        .map(RawPathSegment::Index)
        .or_else(|_| fail.parse_next(input))
}

fn name_ref(input: &mut &str) -> ModalResult<String> {
    preceded('#', ident).parse_next(input)
}

fn value_ref(input: &mut &str) -> ModalResult<String> {
    preceded(':', ident).parse_next(input)
}

fn ident(input: &mut &str) -> ModalResult<String> {
    let first: &str =
        take_while(1..=1, |c: char| c.is_ascii_alphabetic() || c == '_').parse_next(input)?;
    let rest: &str =
        take_while(0.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)?;
    let mut s = String::with_capacity(first.len() + rest.len());
    s.push_str(first);
    s.push_str(rest);
    Ok(s)
}

// =============================================================================
// ConditionExpression parser
// =============================================================================
//
// v1 grammar:
//
//   condition  := or_expr
//   or_expr    := and_expr ('OR' and_expr)*
//   and_expr   := not_expr ('AND' not_expr)*
//   not_expr   := 'NOT' not_expr | atom
//   atom       := '(' condition ')'
//                | 'attribute_exists' '(' path ')'
//                | 'attribute_not_exists' '(' path ')'
//                | operand comparator operand
//   comparator := '=' | '<>' | '<=' | '>=' | '<' | '>'
//
// Precedence: OR (lowest) < AND < NOT < parens/atom. Keywords are
// case-insensitive (matches DDB behavior). Deferred to later phases:
// BETWEEN, IN, begins_with, contains, size, attribute_type.

pub fn parse_condition_expression(input: &str) -> Result<RawCondition, ParseError> {
    if input.trim().is_empty() {
        return Err(ParseError::Empty {
            kind: "ConditionExpression",
        });
    }
    cond_top
        .parse(input)
        .map_err(|e| ParseError::from_winnow("ConditionExpression", input, e))
}

fn cond_top(input: &mut &str) -> ModalResult<RawCondition> {
    let _ = multispace0.parse_next(input)?;
    let c = or_expr.parse_next(input)?;
    let _ = multispace0.parse_next(input)?;
    Ok(c)
}

fn or_expr(input: &mut &str) -> ModalResult<RawCondition> {
    let mut left = and_expr.parse_next(input)?;
    while opt(or_keyword).parse_next(input)?.is_some() {
        let right = and_expr.parse_next(input)?;
        left = RawCondition::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn and_expr(input: &mut &str) -> ModalResult<RawCondition> {
    let mut left = not_expr.parse_next(input)?;
    while opt(and_keyword).parse_next(input)?.is_some() {
        let right = not_expr.parse_next(input)?;
        left = RawCondition::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn not_expr(input: &mut &str) -> ModalResult<RawCondition> {
    let _ = multispace0.parse_next(input)?;
    if opt(not_keyword).parse_next(input)?.is_some() {
        let inner = not_expr.parse_next(input)?;
        return Ok(RawCondition::Not(Box::new(inner)));
    }
    cond_atom.parse_next(input)
}

fn cond_atom(input: &mut &str) -> ModalResult<RawCondition> {
    let _ = multispace0.parse_next(input)?;
    alt((paren_group, attribute_exists_fn, attribute_not_exists_fn, comparison)).parse_next(input)
}

fn paren_group(input: &mut &str) -> ModalResult<RawCondition> {
    let _ = '('.parse_next(input)?;
    let inner = or_expr.parse_next(input)?;
    let _ = (multispace0, ')').parse_next(input)?;
    Ok(inner)
}

fn attribute_exists_fn(input: &mut &str) -> ModalResult<RawCondition> {
    let _ = Caseless("attribute_exists").parse_next(input)?;
    let _ = (multispace0, '(', multispace0).parse_next(input)?;
    let p = path.parse_next(input)?;
    let _ = (multispace0, ')').parse_next(input)?;
    Ok(RawCondition::AttributeExists(p))
}

fn attribute_not_exists_fn(input: &mut &str) -> ModalResult<RawCondition> {
    let _ = Caseless("attribute_not_exists").parse_next(input)?;
    let _ = (multispace0, '(', multispace0).parse_next(input)?;
    let p = path.parse_next(input)?;
    let _ = (multispace0, ')').parse_next(input)?;
    Ok(RawCondition::AttributeNotExists(p))
}

fn comparison(input: &mut &str) -> ModalResult<RawCondition> {
    let left = operand.parse_next(input)?;
    let _ = multispace0.parse_next(input)?;
    let op = comparator.parse_next(input)?;
    let _ = multispace0.parse_next(input)?;
    let right = operand.parse_next(input)?;
    Ok(RawCondition::Compare { op, left, right })
}

fn comparator(input: &mut &str) -> ModalResult<ComparisonOp> {
    // Order matters: `<=` and `<>` must be tried before `<`; `>=` before `>`.
    alt((
        "<=".map(|_| ComparisonOp::Le),
        ">=".map(|_| ComparisonOp::Ge),
        "<>".map(|_| ComparisonOp::Ne),
        "=".map(|_| ComparisonOp::Eq),
        "<".map(|_| ComparisonOp::Lt),
        ">".map(|_| ComparisonOp::Gt),
    ))
    .parse_next(input)
}

/// Match the `AND` keyword as a whole word — i.e., followed by whitespace
/// or end-of-input — so paths whose names happen to start with `and` (e.g.
/// `andrew`) don't get mis-eaten by the OR loop's lookahead.
fn and_keyword(input: &mut &str) -> ModalResult<()> {
    let _ = multispace1.parse_next(input)?;
    let _ = Caseless("AND").parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    Ok(())
}

fn or_keyword(input: &mut &str) -> ModalResult<()> {
    let _ = multispace1.parse_next(input)?;
    let _ = Caseless("OR").parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    Ok(())
}

fn not_keyword(input: &mut &str) -> ModalResult<()> {
    let _ = Caseless("NOT").parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    Ok(())
}
