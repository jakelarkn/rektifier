//! Lower a resolved `ConditionExpression` AST into a `QueryKeyBounds`
//! suitable for storage. Q1's KeyConditionExpression support reuses the
//! existing condition parser/substitute pipeline (PLAN-4 D1) and
//! validates the shape here.
//!
//! Legal KCE shapes:
//!
//! ```text
//! pk_attr = :v                                  // hash-only
//! pk_attr = :v AND sk_attr = :v                 // composite, sk equality
//! pk_attr = :v AND sk_attr <  :v                // sk ordering: <, <=, >, >=
//! pk_attr = :v AND sk_attr BETWEEN :lo AND :hi
//! pk_attr = :v AND begins_with(sk_attr, :v)     // S-typed sk only (PLAN-4 D8)
//! ```
//!
//! Everything else (OR, NOT, `attribute_*`, `contains`, `IN`,
//! `attribute_type`, path-vs-path operands, non-key attrs, deeper
//! nesting) is rejected with a specific `TranslateError`.

use crate::error::TranslateError;
use crate::schema::TableSchema;
use rekt_expressions::{ComparisonOp, Condition, Operand};
use rekt_protocol::AttributeValue;
use rekt_storage::{KeyType, KeyValue, SkCondition};

pub(crate) struct QueryKeyBounds {
    pub pk: KeyValue,
    pub sk_condition: Option<SkCondition>,
}

/// Walk the resolved Condition AST, validate KCE-shape, and lower into
/// storage-typed bounds. The schema is consulted for key attr names + types.
pub(crate) fn extract_key_condition(
    cond: &Condition,
    schema: &TableSchema,
) -> Result<QueryKeyBounds, TranslateError> {
    // Top level is either a single PK equality OR an AND of (PK equality, SK predicate).
    let (pk_term, sk_term): (&Condition, Option<&Condition>) = match cond {
        Condition::And(a, b) => (a.as_ref(), Some(b.as_ref())),
        single => (single, None),
    };

    // Resolve PK first so we always emit a stable error if it's missing.
    let pk = match_pk_equality(pk_term, schema)?;

    let sk_condition = match sk_term {
        None => None,
        Some(term) => Some(match_sk_predicate(term, schema)?),
    };

    Ok(QueryKeyBounds { pk, sk_condition })
}

fn match_pk_equality(
    cond: &Condition,
    schema: &TableSchema,
) -> Result<KeyValue, TranslateError> {
    let (op, left, right) = match cond {
        Condition::Compare { op, left, right } => (op, left, right),
        Condition::And(_, _) => {
            return Err(TranslateError::KeyConditionUnsupportedShape {
                reason: "nested AND not allowed on the PK side",
            });
        }
        Condition::Or(_, _) | Condition::Not(_) => {
            return Err(TranslateError::KeyConditionUnsupportedShape {
                reason: "OR / NOT are not allowed",
            });
        }
        Condition::AttributeExists(_) | Condition::AttributeNotExists(_) => {
            return Err(TranslateError::KeyConditionUnsupportedShape {
                reason: "attribute_exists / attribute_not_exists are not allowed",
            });
        }
        Condition::BeginsWith(_, _)
        | Condition::Contains(_, _)
        | Condition::Between(_, _, _)
        | Condition::In(_, _)
        | Condition::AttributeType(_, _) => {
            return Err(TranslateError::KeyConditionUnsupportedShape {
                reason: "function-call shape used on the partition key term",
            });
        }
    };

    let (attr, value) = path_and_value(left, right)?;
    if attr != schema.pk_attr {
        // Either it's a non-key attr, OR it's the SK attr in the wrong
        // slot (no PK equality at all). DDB phrases both as "missed key
        // schema element"; we differentiate to give clearer messages.
        if Some(attr) == schema.sk_attr.as_deref() {
            return Err(TranslateError::KeyConditionMissingPkEquality {
                attr: schema.pk_attr.clone(),
            });
        }
        return Err(TranslateError::KeyConditionNonKeyAttr {
            attr: attr.to_string(),
        });
    }

    if *op != ComparisonOp::Eq {
        return Err(TranslateError::KeyConditionPkMustBeEquality {
            attr: attr.to_string(),
            op: op.as_str(),
        });
    }

    av_to_keyvalue(value, schema.pk_type).map_err(|got| {
        TranslateError::KeyConditionPkTypeMismatch {
            attr: attr.to_string(),
            expected: schema.pk_type,
            got,
        }
    })
}

fn match_sk_predicate(
    cond: &Condition,
    schema: &TableSchema,
) -> Result<SkCondition, TranslateError> {
    let sk_attr = schema.sk_attr.as_deref().ok_or(
        TranslateError::KeyConditionUnsupportedShape {
            reason: "table is hash-only; AND'ing a sort-key predicate is not allowed",
        },
    )?;
    let sk_type = schema.sk_type.expect("sk_attr Some implies sk_type Some");

    match cond {
        Condition::Compare { op, left, right } => {
            let (attr, value) = path_and_value(left, right)?;
            if attr != sk_attr {
                return Err(TranslateError::KeyConditionNonKeyAttr {
                    attr: attr.to_string(),
                });
            }
            let kv = av_to_keyvalue(value, sk_type).map_err(|got| {
                TranslateError::KeyConditionSkTypeMismatch {
                    attr: attr.to_string(),
                    expected: sk_type,
                    got,
                }
            })?;
            Ok(match op {
                ComparisonOp::Eq => SkCondition::Eq(kv),
                ComparisonOp::Lt => SkCondition::Lt(kv),
                ComparisonOp::Le => SkCondition::Le(kv),
                ComparisonOp::Gt => SkCondition::Gt(kv),
                ComparisonOp::Ge => SkCondition::Ge(kv),
                ComparisonOp::Ne => {
                    return Err(TranslateError::KeyConditionUnsupportedShape {
                        reason: "`<>` is not valid on the sort key",
                    });
                }
            })
        }
        Condition::Between(target, lo, hi) => {
            let attr = path_name(target)?;
            if attr != sk_attr {
                return Err(TranslateError::KeyConditionNonKeyAttr {
                    attr: attr.to_string(),
                });
            }
            let lo_av = operand_value(lo)?;
            let hi_av = operand_value(hi)?;
            let lo_kv = av_to_keyvalue(lo_av, sk_type).map_err(|got| {
                TranslateError::KeyConditionSkTypeMismatch {
                    attr: attr.to_string(),
                    expected: sk_type,
                    got,
                }
            })?;
            let hi_kv = av_to_keyvalue(hi_av, sk_type).map_err(|got| {
                TranslateError::KeyConditionSkTypeMismatch {
                    attr: attr.to_string(),
                    expected: sk_type,
                    got,
                }
            })?;
            Ok(SkCondition::Between(lo_kv, hi_kv))
        }
        Condition::BeginsWith(path, operand) => {
            let attr = path.top_name().ok_or(
                TranslateError::KeyConditionUnsupportedShape {
                    reason: "begins_with target is not a top-level name",
                },
            )?;
            if attr != sk_attr {
                return Err(TranslateError::KeyConditionNonKeyAttr {
                    attr: attr.to_string(),
                });
            }
            if sk_type != KeyType::S {
                return Err(TranslateError::KeyConditionBeginsWithRequiresStringSk {
                    attr: attr.to_string(),
                    got: sk_type,
                });
            }
            let av = operand_value(operand)?;
            match av {
                AttributeValue::S(s) => Ok(SkCondition::BeginsWithS(s.clone())),
                other => Err(TranslateError::KeyConditionSkTypeMismatch {
                    attr: attr.to_string(),
                    expected: KeyType::S,
                    got: other.type_name(),
                }),
            }
        }
        Condition::And(_, _) | Condition::Or(_, _) | Condition::Not(_) => Err(
            TranslateError::KeyConditionUnsupportedShape {
                reason: "boolean combinators are not allowed on the sort-key term",
            },
        ),
        Condition::AttributeExists(_) | Condition::AttributeNotExists(_) => Err(
            TranslateError::KeyConditionUnsupportedShape {
                reason: "attribute_exists / attribute_not_exists are not allowed",
            },
        ),
        Condition::Contains(_, _) => Err(TranslateError::KeyConditionUnsupportedShape {
            reason: "contains is not allowed in KeyConditionExpression",
        }),
        Condition::In(_, _) => Err(TranslateError::KeyConditionUnsupportedShape {
            reason: "IN is not allowed in KeyConditionExpression",
        }),
        Condition::AttributeType(_, _) => Err(TranslateError::KeyConditionUnsupportedShape {
            reason: "attribute_type is not allowed in KeyConditionExpression",
        }),
    }
}

/// Pull a `(path-top-name, value)` pair out of a comparison's left/right
/// operands, requiring exactly one path side and one value side.
fn path_and_value<'a>(
    left: &'a Operand,
    right: &'a Operand,
) -> Result<(&'a str, &'a AttributeValue), TranslateError> {
    match (left, right) {
        (Operand::Path(p), Operand::Value(v)) | (Operand::Value(v), Operand::Path(p)) => {
            let name =
                p.top_name()
                    .ok_or(TranslateError::KeyConditionUnsupportedShape {
                        reason: "operand path is not a top-level name",
                    })?;
            Ok((name, v))
        }
        (Operand::Path(_), Operand::Path(_)) => Err(TranslateError::KeyConditionRhsMustBeValue),
        (Operand::Value(_), Operand::Value(_)) => {
            Err(TranslateError::KeyConditionUnsupportedShape {
                reason: "both operands are values (must reference a key attribute)",
            })
        }
    }
}

fn path_name(op: &Operand) -> Result<&str, TranslateError> {
    match op {
        Operand::Path(p) => p
            .top_name()
            .ok_or(TranslateError::KeyConditionUnsupportedShape {
                reason: "operand path is not a top-level name",
            }),
        Operand::Value(_) => Err(TranslateError::KeyConditionUnsupportedShape {
            reason: "BETWEEN target must be a path",
        }),
    }
}

fn operand_value(op: &Operand) -> Result<&AttributeValue, TranslateError> {
    match op {
        Operand::Value(v) => Ok(v),
        Operand::Path(_) => Err(TranslateError::KeyConditionRhsMustBeValue),
    }
}

fn av_to_keyvalue(av: &AttributeValue, expected: KeyType) -> Result<KeyValue, &'static str> {
    match (expected, av) {
        (KeyType::S, AttributeValue::S(s)) => Ok(KeyValue::S(s.clone())),
        (KeyType::N, AttributeValue::N(s)) => Ok(KeyValue::N(s.clone())),
        (KeyType::B, AttributeValue::B(b)) => Ok(KeyValue::B(b.clone())),
        (_, other) => Err(other.type_name()),
    }
}
