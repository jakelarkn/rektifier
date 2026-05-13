//! Substitute `ExpressionAttributeNames` (`#x` → "actualName") and
//! `ExpressionAttributeValues` (`:v` → AttributeValue) into a parsed
//! `RawUpdateExpression`, producing a fully-resolved `UpdateExpression`.
//!
//! The placeholder maps are passed in by the caller (the DDB request
//! carries `ExpressionAttributeNames` / `ExpressionAttributeValues`).
//! Missing keys are surfaced as `SubstituteError::UnknownName` /
//! `UnknownValue`.

use std::collections::BTreeMap;

use rekt_protocol::AttributeValue;

use crate::ast::*;

#[derive(Debug, thiserror::Error)]
pub enum SubstituteError {
    #[error("ExpressionAttributeNames is missing entry `#{name}` referenced by the expression")]
    UnknownName { name: String },

    #[error("ExpressionAttributeValues is missing entry `:{name}` referenced by the expression")]
    UnknownValue { name: String },
}

pub fn substitute_update(
    raw: RawUpdateExpression,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<UpdateExpression, SubstituteError> {
    Ok(UpdateExpression {
        set: raw
            .set
            .into_iter()
            .map(|c| resolve_set_clause(c, names, values))
            .collect::<Result<_, _>>()?,
        remove: raw
            .remove
            .into_iter()
            .map(|p| resolve_path(p, names))
            .collect::<Result<_, _>>()?,
        add: raw
            .add
            .into_iter()
            .map(|a| {
                Ok(AddAction {
                    path: resolve_path(a.path, names)?,
                    value: resolve_operand_to_value(a.value, values)?,
                })
            })
            .collect::<Result<_, _>>()?,
        delete: raw
            .delete
            .into_iter()
            .map(|d| {
                Ok(DeleteAction {
                    path: resolve_path(d.path, names)?,
                    value: resolve_operand_to_value(d.value, values)?,
                })
            })
            .collect::<Result<_, _>>()?,
    })
}

fn resolve_set_clause(
    c: RawSetClause,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<SetClause, SubstituteError> {
    Ok(SetClause {
        path: resolve_path(c.path, names)?,
        value: resolve_set_rhs(c.value, names, values)?,
    })
}

fn resolve_set_rhs(
    rhs: RawSetRhs,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<SetRhs, SubstituteError> {
    Ok(match rhs {
        RawSetRhs::Operand(o) => SetRhs::Operand(resolve_operand(o, names, values)?),
        RawSetRhs::Plus(a, b) => SetRhs::Plus(
            resolve_operand(a, names, values)?,
            resolve_operand(b, names, values)?,
        ),
        RawSetRhs::Minus(a, b) => SetRhs::Minus(
            resolve_operand(a, names, values)?,
            resolve_operand(b, names, values)?,
        ),
        RawSetRhs::IfNotExists(p, inner) => SetRhs::IfNotExists(
            resolve_path(p, names)?,
            Box::new(resolve_set_rhs(*inner, names, values)?),
        ),
        RawSetRhs::ListAppend(a, b) => SetRhs::ListAppend(
            Box::new(resolve_set_rhs(*a, names, values)?),
            Box::new(resolve_set_rhs(*b, names, values)?),
        ),
    })
}

fn resolve_operand(
    op: RawOperand,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<Operand, SubstituteError> {
    Ok(match op {
        RawOperand::Path(p) => Operand::Path(resolve_path(p, names)?),
        RawOperand::ValueRef(v) => {
            let av = values
                .get(&format!(":{v}"))
                .or_else(|| values.get(&v))
                .cloned()
                .ok_or(SubstituteError::UnknownValue { name: v })?;
            Operand::Value(av)
        }
    })
}

/// Specialized form for `ADD` and `DELETE`, which can only have a value
/// (not a path reference) per DDB's grammar.
fn resolve_operand_to_value(
    op: RawOperand,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<AttributeValue, SubstituteError> {
    match op {
        RawOperand::Path(p) => Err(SubstituteError::UnknownValue {
            name: format!(
                "ADD/DELETE require a value reference (`:v`), got path `{}`",
                debug_path(&p)
            ),
        }),
        RawOperand::ValueRef(v) => values
            .get(&format!(":{v}"))
            .or_else(|| values.get(&v))
            .cloned()
            .ok_or(SubstituteError::UnknownValue { name: v }),
    }
}

fn resolve_path(p: RawPath, names: &BTreeMap<String, String>) -> Result<Path, SubstituteError> {
    Ok(Path {
        segments: p
            .segments
            .into_iter()
            .map(|s| {
                Ok(match s {
                    RawPathSegment::Name(n) => PathSegment::Name(n),
                    RawPathSegment::Index(i) => PathSegment::Index(i),
                    RawPathSegment::NameRef(n) => {
                        let actual = names
                            .get(&format!("#{n}"))
                            .or_else(|| names.get(&n))
                            .cloned()
                            .ok_or(SubstituteError::UnknownName { name: n })?;
                        PathSegment::Name(actual)
                    }
                })
            })
            .collect::<Result<_, _>>()?,
    })
}

fn debug_path(p: &RawPath) -> String {
    let mut out = String::new();
    for (i, seg) in p.segments.iter().enumerate() {
        match seg {
            RawPathSegment::Name(n) => {
                if i > 0 {
                    out.push('.');
                }
                out.push_str(n);
            }
            RawPathSegment::NameRef(n) => {
                if i > 0 {
                    out.push('.');
                }
                out.push('#');
                out.push_str(n);
            }
            RawPathSegment::Index(i) => {
                out.push('[');
                out.push_str(&i.to_string());
                out.push(']');
            }
        }
    }
    out
}
