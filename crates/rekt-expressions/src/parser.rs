//! winnow-based parser for `UpdateExpression`. Produces a `RawUpdateExpression`
//! with `:v` and `#n` placeholders verbatim; downstream `substitute_update`
//! resolves them.

use winnow::ascii::{digit1, multispace0, multispace1, Caseless};
use winnow::combinator::{alt, delimited, fail, opt, preceded, repeat, separated, terminated};
use winnow::error::{ContextError, ParseError as WinnowParseError};
use winnow::token::take_while;
use winnow::ModalResult;
use winnow::Parser;

use crate::ast::*;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("UpdateExpression parse error at position {position}: {message}")]
    Invalid { position: usize, message: String },

    #[error("UpdateExpression is empty")]
    Empty,

    #[error(
        "UpdateExpression contains duplicate `{clause}` keyword — DDB requires actions of one kind to be grouped under a single `SET`/`REMOVE`/`ADD`/`DELETE` keyword"
    )]
    DuplicateKeyword { clause: &'static str },
}

impl ParseError {
    fn from_winnow(input_orig: &str, e: WinnowParseError<&str, ContextError>) -> Self {
        let position = input_orig.len() - e.input().len();
        Self::Invalid {
            position,
            message: e.inner().to_string(),
        }
    }
}

pub fn parse_update_expression(input: &str) -> Result<RawUpdateExpression, ParseError> {
    if input.trim().is_empty() {
        return Err(ParseError::Empty);
    }
    let raw_blocks = full_expression
        .parse(input)
        .map_err(|e| ParseError::from_winnow(input, e))?;

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
