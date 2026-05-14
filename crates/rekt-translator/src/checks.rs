//! Path / operand / value-type validation helpers used by the
//! per-clause walk in `translate_update_item` and the condition walker.

use crate::error::TranslateError;
use crate::schema::TableSchema;
use rekt_expressions::{Condition, Operand, Path, PathSegment, SetRhs};
use rekt_protocol::AttributeValue;

pub(crate) fn check_top_level(p: &Path) -> Result<(), TranslateError> {
    if p.is_top_level() {
        Ok(())
    } else {
        Err(TranslateError::UnsupportedNestedPath {
            path: format_path(p),
        })
    }
}

pub(crate) fn check_not_key(p: &Path, schema: &TableSchema) -> Result<(), TranslateError> {
    let name = match p.top_name() {
        Some(n) => n,
        None => return Ok(()),
    };
    if name == schema.pk_attr || schema.sk_attr.as_deref() == Some(name) {
        return Err(TranslateError::UpdateTouchesKey {
            attr: name.to_string(),
        });
    }
    Ok(())
}

/// Verify every embedded path in a SET RHS is top-level. The translator
/// permits non-literal RHS forms (path-ref, arithmetic, if_not_exists,
/// list_append) as of Phase 3c — they route to the slow path — but
/// nested paths are still a Phase 8 concern.
pub(crate) fn check_set_rhs_paths_top_level(rhs: &SetRhs) -> Result<(), TranslateError> {
    match rhs {
        SetRhs::Operand(o) => check_operand_path_top_level(o),
        SetRhs::Plus(a, b) | SetRhs::Minus(a, b) => {
            check_operand_path_top_level(a)?;
            check_operand_path_top_level(b)
        }
        SetRhs::IfNotExists(p, inner) => {
            check_top_level(p)?;
            check_set_rhs_paths_top_level(inner)
        }
        SetRhs::ListAppend(a, b) => {
            check_set_rhs_paths_top_level(a)?;
            check_set_rhs_paths_top_level(b)
        }
    }
}

fn check_operand_path_top_level(o: &Operand) -> Result<(), TranslateError> {
    match o {
        Operand::Path(p) => check_top_level(p),
        Operand::Value(_) => Ok(()),
    }
}

/// `ADD path :v` requires `:v` to be a Number (numeric add) or one of
/// the set types (set union). DDB raises ValidationException on any
/// other type at request time, so we surface the same rejection
/// pre-storage.
pub(crate) fn check_add_value_type(v: &AttributeValue) -> Result<(), TranslateError> {
    match v {
        AttributeValue::N(_)
        | AttributeValue::Ss(_)
        | AttributeValue::Ns(_)
        | AttributeValue::Bs(_) => Ok(()),
        other => Err(TranslateError::InvalidAddValueType {
            got: other.type_name(),
        }),
    }
}

/// `DELETE path :v` requires `:v` to be a set type — DELETE is set
/// subtraction only.
pub(crate) fn check_delete_value_type(v: &AttributeValue) -> Result<(), TranslateError> {
    match v {
        AttributeValue::Ss(_) | AttributeValue::Ns(_) | AttributeValue::Bs(_) => Ok(()),
        other => Err(TranslateError::InvalidDeleteValueType {
            got: other.type_name(),
        }),
    }
}

pub(crate) fn check_condition_paths_top_level(cond: &Condition) -> Result<(), TranslateError> {
    match cond {
        Condition::AttributeExists(p) | Condition::AttributeNotExists(p) => {
            check_condition_path(p)
        }
        Condition::Compare { left, right, .. } => {
            check_condition_operand(left)?;
            check_condition_operand(right)
        }
        Condition::BeginsWith(p, o)
        | Condition::Contains(p, o)
        | Condition::AttributeType(p, o) => {
            check_condition_path(p)?;
            check_condition_operand(o)
        }
        Condition::Between(v, lo, hi) => {
            check_condition_operand(v)?;
            check_condition_operand(lo)?;
            check_condition_operand(hi)
        }
        Condition::In(v, items) => {
            check_condition_operand(v)?;
            for item in items {
                check_condition_operand(item)?;
            }
            Ok(())
        }
        Condition::And(a, b) | Condition::Or(a, b) => {
            check_condition_paths_top_level(a)?;
            check_condition_paths_top_level(b)
        }
        Condition::Not(inner) => check_condition_paths_top_level(inner),
    }
}

fn check_condition_operand(o: &Operand) -> Result<(), TranslateError> {
    match o {
        Operand::Path(p) => check_condition_path(p),
        Operand::Value(_) => Ok(()),
    }
}

fn check_condition_path(p: &Path) -> Result<(), TranslateError> {
    if p.is_top_level() {
        Ok(())
    } else {
        Err(TranslateError::UnsupportedConditionNestedPath {
            path: format_path(p),
        })
    }
}

pub(crate) fn format_path(p: &Path) -> String {
    let mut out = String::new();
    for (i, seg) in p.segments.iter().enumerate() {
        match seg {
            PathSegment::Name(n) => {
                if i > 0 {
                    out.push('.');
                }
                out.push_str(n);
            }
            PathSegment::Index(n) => {
                out.push('[');
                out.push_str(&n.to_string());
                out.push(']');
            }
        }
    }
    out
}
