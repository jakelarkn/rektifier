//! In-Rust evaluator for `UpdateExpression`: takes the existing row (or
//! `None` for upsert) plus the request's `Key`, walks the AST, and
//! produces the new row state. Mirrors DDB's "read all RHS values
//! first, then write" semantics so `SET a = b, b = a` correctly swaps.

use super::values::single_tagged;
use super::EvalError;
use rekt_expressions::{Operand, Path, SetRhs, UpdateExpression};
use rekt_protocol::{AttributeValue, Item};
use serde_json::Value;

/// Apply an UpdateExpression to an optional existing row + request key.
/// When `existing` is `None`, the new row is built from `key` + the
/// values produced by SET clauses (REMOVE clauses are no-ops on a
/// missing row). Returns the new full row state as DDB-JSON.
pub fn apply_update_expression(
    existing: Option<&Value>,
    expr: &UpdateExpression,
    key: &Item,
) -> Result<Value, EvalError> {
    // Phase 1: compute every SET RHS *before* writing anything, so
    // `SET a = b, b = a` correctly swaps the two — values read from the
    // ORIGINAL existing row, not from a partially-mutated one.
    let mut staged: Vec<(String, Value)> = Vec::with_capacity(expr.set.len());
    for clause in &expr.set {
        let attr = top_name_or_unreachable(&clause.path);
        let value = compute_set_rhs(&clause.value, existing)?;
        staged.push((attr.to_string(), value));
    }

    // Phase 2: start from `existing` (or synthesize `{key attrs}` for
    // an upsert), apply SETs, then REMOVEs, then ADDs, then DELETEs.
    // The ordering of SET → REMOVE → ADD → DELETE matches DDB's
    // documented semantics (REMOVE happens after SET; ADD/DELETE
    // operate on the post-REMOVE state).
    let mut item: Value = match existing {
        Some(v) => v.clone(),
        None => serde_json::to_value(key).expect("Item Serialize is infallible"),
    };
    let obj = item.as_object_mut().ok_or(EvalError::ItemNotObject)?;

    for (attr, value) in staged {
        obj.insert(attr, value);
    }
    for path in &expr.remove {
        let attr = top_name_or_unreachable(path);
        obj.remove(attr);
    }
    for action in &expr.add {
        let attr = top_name_or_unreachable(&action.path);
        let new_val = apply_add(obj.get(attr), &action.value)?;
        obj.insert(attr.to_string(), new_val);
    }
    for action in &expr.delete {
        let attr = top_name_or_unreachable(&action.path);
        match obj.get(attr) {
            // Path absent → no action (DDB semantics).
            None => continue,
            Some(existing) => match apply_delete(existing, &action.value)? {
                Some(new_val) => {
                    obj.insert(attr.to_string(), new_val);
                }
                None => {
                    // Set became empty → attribute removed.
                    obj.remove(attr);
                }
            },
        }
    }
    Ok(item)
}

/// `ADD path :v` over the current value of `path` (or `None` if absent).
///
/// - Numeric ADD: existing must be `N` (or absent → start at 0); result
///   is `{"N": str(existing + addend)}`.
/// - Set ADD: existing must be the same set type (or absent → start
///   from empty); result is the union, order-preserving with dedup.
fn apply_add(existing: Option<&Value>, addend: &AttributeValue) -> Result<Value, EvalError> {
    match addend {
        AttributeValue::N(n) => {
            let lhs = match existing {
                None => 0.0,
                Some(v) => match single_tagged(v) {
                    Some(("N", s)) => match s.as_str().and_then(|t| t.parse::<f64>().ok()) {
                        Some(f) => f,
                        None => return Err(EvalError::ArithmeticParseFailed {
                            value: s.to_string(),
                        }),
                    },
                    Some((tag, _)) => {
                        return Err(EvalError::ArithmeticOperandNotNumber {
                            got: static_tag(tag),
                        });
                    }
                    None => {
                        return Err(EvalError::ArithmeticOperandNotNumber {
                            got: "non-DDB-tagged value",
                        });
                    }
                },
            };
            let rhs = parse_decimal(n)?;
            Ok(serde_json::json!({"N": format_decimal(lhs + rhs)}))
        }
        AttributeValue::Ss(addend_items) => {
            set_union(existing, "SS", addend_items.iter().cloned())
        }
        AttributeValue::Ns(addend_items) => {
            set_union(existing, "NS", addend_items.iter().cloned())
        }
        AttributeValue::Bs(addend_items) => {
            // base64 the addend's bytes for the wire form so we can
            // dedup against existing in the same encoding.
            use base64::engine::general_purpose::STANDARD as B64;
            use base64::Engine;
            let strs: Vec<String> = addend_items.iter().map(|b| B64.encode(b)).collect();
            set_union(existing, "BS", strs)
        }
        // Other types blocked by `check_add_value_type` in the translator;
        // reaching here is a translator divergence.
        _ => Err(EvalError::ArithmeticOperandNotNumber {
            got: addend.type_name(),
        }),
    }
}

fn set_union<I>(existing: Option<&Value>, tag: &str, addend: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = String>,
{
    let mut combined: Vec<String> = match existing {
        None => Vec::new(),
        Some(v) => match single_tagged(v) {
            Some((existing_tag, arr)) if existing_tag == tag => arr
                .as_array()
                .map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_string)).collect())
                .unwrap_or_default(),
            Some((existing_tag, _)) => {
                return Err(EvalError::ListAppendOperandNotList {
                    got: static_tag(existing_tag),
                });
            }
            None => Vec::new(),
        },
    };
    for item in addend {
        if !combined.contains(&item) {
            combined.push(item);
        }
    }
    Ok(serde_json::json!({ tag: combined }))
}

/// Apply a `DELETE path :v` action against a present `existing` value
/// (set difference). Returns `Some(new)` when there are still elements
/// left, or `None` when the set has been fully drained — DDB removes
/// the attribute outright in that case. Caller has already handled the
/// "path absent" no-op before calling.
fn apply_delete(
    existing: &Value,
    subset: &AttributeValue,
) -> Result<Option<Value>, EvalError> {
    let (tag, to_remove): (&str, Vec<String>) = match subset {
        AttributeValue::Ss(items) => ("SS", items.clone()),
        AttributeValue::Ns(items) => ("NS", items.clone()),
        AttributeValue::Bs(items) => {
            use base64::engine::general_purpose::STANDARD as B64;
            use base64::Engine;
            ("BS", items.iter().map(|b| B64.encode(b)).collect())
        }
        other => {
            return Err(EvalError::ListAppendOperandNotList {
                got: other.type_name(),
            })
        }
    };
    let (existing_tag, arr) = single_tagged(existing).ok_or(
        EvalError::ListAppendOperandNotList {
            got: "non-DDB-tagged value",
        },
    )?;
    if existing_tag != tag {
        return Err(EvalError::ListAppendOperandNotList {
            got: static_tag(existing_tag),
        });
    }
    let arr = arr.as_array().ok_or(EvalError::ListAppendOperandNotList {
        got: "non-array set wrapper",
    })?;
    let remaining: Vec<String> = arr
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .filter(|s| !to_remove.contains(s))
        .collect();
    if remaining.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::json!({ tag: remaining })))
    }
}

fn compute_set_rhs(rhs: &SetRhs, existing: Option<&Value>) -> Result<Value, EvalError> {
    match rhs {
        SetRhs::Operand(Operand::Value(v)) => {
            Ok(serde_json::to_value(v).expect("AttributeValue Serialize is infallible"))
        }
        SetRhs::Operand(Operand::Path(p)) => {
            let attr = top_name_or_unreachable(p);
            existing
                .and_then(|v| v.get(attr))
                .cloned()
                .ok_or_else(|| EvalError::PathDoesNotExist {
                    path: attr.to_string(),
                })
        }
        SetRhs::Plus(a, b) => arith(a, b, existing, /*plus=*/ true),
        SetRhs::Minus(a, b) => arith(a, b, existing, /*plus=*/ false),
        SetRhs::IfNotExists(path, fallback) => {
            let attr = top_name_or_unreachable(path);
            match existing.and_then(|v| v.get(attr)) {
                Some(existing_val) => Ok(existing_val.clone()),
                None => compute_set_rhs(fallback, existing),
            }
        }
        SetRhs::ListAppend(a, b) => {
            let av = compute_set_rhs(a, existing)?;
            let bv = compute_set_rhs(b, existing)?;
            list_concat(&av, &bv)
        }
    }
}

/// `+` / `-` over DDB `N` operands. Each operand is either a path
/// (looked up in `existing`) or a literal value. Both must resolve to
/// `{"N": "..."}` shape; result is `{"N": "..."}`.
fn arith(
    a: &Operand,
    b: &Operand,
    existing: Option<&Value>,
    plus: bool,
) -> Result<Value, EvalError> {
    let av = operand_value(a, existing)?;
    let bv = operand_value(b, existing)?;
    let an = ddb_n_value(&av)?;
    let bn = ddb_n_value(&bv)?;
    let (lhs, rhs) = (parse_decimal(an)?, parse_decimal(bn)?);
    let result = if plus { lhs + rhs } else { lhs - rhs };
    Ok(serde_json::json!({ "N": format_decimal(result) }))
}

/// Resolve an operand to its DDB-JSON value form, looking up paths in
/// `existing`. A missing path raises `PathDoesNotExist`, matching DDB
/// behavior for arithmetic on non-existent attributes.
fn operand_value(op: &Operand, existing: Option<&Value>) -> Result<Value, EvalError> {
    match op {
        Operand::Value(v) => {
            Ok(serde_json::to_value(v).expect("AttributeValue Serialize is infallible"))
        }
        Operand::Path(p) => {
            let attr = top_name_or_unreachable(p);
            existing
                .and_then(|v| v.get(attr))
                .cloned()
                .ok_or_else(|| EvalError::PathDoesNotExist {
                    path: attr.to_string(),
                })
        }
    }
}

fn ddb_n_value(v: &Value) -> Result<&str, EvalError> {
    let (tag, inner) = single_tagged(v).ok_or(EvalError::ArithmeticOperandNotNumber {
        got: "non-DDB-tagged value",
    })?;
    if tag != "N" {
        return Err(EvalError::ArithmeticOperandNotNumber {
            got: static_tag(tag),
        });
    }
    inner
        .as_str()
        .ok_or(EvalError::ArithmeticOperandNotNumber { got: "non-string N" })
}

fn parse_decimal(s: &str) -> Result<f64, EvalError> {
    // f64 is sufficient for typical DDB N use; arbitrary-precision is a
    // known limitation we'd want to revisit if a customer reports drift
    // on 38-digit precision sums.
    s.parse::<f64>().map_err(|_| EvalError::ArithmeticParseFailed {
        value: s.to_string(),
    })
}

fn format_decimal(f: f64) -> String {
    // Mirror DDB's "trim trailing zeros after a decimal point" idiom
    // when the result is integer-valued. Otherwise use default fmt.
    if f.fract() == 0.0 && f.abs() < 1e16 {
        format!("{}", f as i64)
    } else {
        format!("{f}")
    }
}

fn list_concat(a: &Value, b: &Value) -> Result<Value, EvalError> {
    let av = ddb_list(a)?;
    let bv = ddb_list(b)?;
    let mut combined = Vec::with_capacity(av.len() + bv.len());
    combined.extend_from_slice(av);
    combined.extend_from_slice(bv);
    Ok(serde_json::json!({ "L": combined }))
}

fn ddb_list(v: &Value) -> Result<&[Value], EvalError> {
    let (tag, inner) = single_tagged(v).ok_or(EvalError::ListAppendOperandNotList {
        got: "non-DDB-tagged value",
    })?;
    if tag != "L" {
        return Err(EvalError::ListAppendOperandNotList {
            got: static_tag(tag),
        });
    }
    inner
        .as_array()
        .map(Vec::as_slice)
        .ok_or(EvalError::ListAppendOperandNotList { got: "non-array L" })
}

/// Translator validation guarantees every path in a translated plan is
/// top-level, so unwrap is justified here. If a caller bypasses the
/// translator and constructs a non-top-level path, we'd reach this — a
/// panic message rather than silent UB is the right fallback.
fn top_name_or_unreachable(p: &Path) -> &str {
    p.top_name()
        .expect("translator-validated path; expected top-level")
}

fn static_tag(tag: &str) -> &'static str {
    // Map dynamic DDB type tags to static strings for error messages.
    match tag {
        "S" => "S",
        "N" => "N",
        "B" => "B",
        "BOOL" => "BOOL",
        "NULL" => "NULL",
        "L" => "L",
        "M" => "M",
        "SS" => "SS",
        "NS" => "NS",
        "BS" => "BS",
        _ => "unknown",
    }
}
