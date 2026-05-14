//! Pure in-Rust evaluators for `UpdateExpression` and `ConditionExpression`,
//! used by the storage layer's slow path (Phase 3b/4e). Both operate
//! over `serde_json::Value`s in DDB-JSON form (`{"S":"alice"}` etc.) —
//! the same shape the backends store. No SQL, no async, no I/O.
//!
//! - [`apply_update_expression`] takes the existing row (or `None`
//!   for upsert) plus the request's `Key`, walks the expression AST,
//!   and produces the new row state. Mirrors DDB's "read all RHS
//!   values first, then write" semantics so `SET a = b, b = a` swaps.
//! - [`evaluate_condition`] runs the v1 ConditionExpression grammar
//!   against an `Option<&Value>` and returns a bool. Missing-attribute
//!   comparisons evaluate to false, matching DDB.

use rekt_expressions::{
    ComparisonOp, Condition, Operand, Path, SetRhs, UpdateExpression,
};
use rekt_protocol::{AttributeValue, Item};
use serde_json::{Map, Value};

#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    /// `SET a = b` where `b` doesn't exist in the row. DDB returns
    /// `ValidationException: The provided expression refers to an
    /// attribute that does not exist in the item: <name>`.
    #[error("The provided expression refers to an attribute that does not exist in the item: {path}")]
    PathDoesNotExist { path: String },

    /// Arithmetic (`+` / `-`) on an operand whose value isn't a DDB
    /// `N`-typed scalar. Surfaces as `ValidationException`.
    #[error("An operand in the update expression has an incorrect data type: arithmetic requires Number (got {got})")]
    ArithmeticOperandNotNumber { got: &'static str },

    /// Arithmetic value couldn't be parsed as a finite numeric (e.g.,
    /// `NaN` or unparseable string). Internal — shouldn't happen with
    /// well-formed DDB N values.
    #[error("arithmetic value `{value}` is not a valid number")]
    ArithmeticParseFailed { value: String },

    /// `list_append(a, b)` where one operand isn't an `L`. Maps to
    /// `ValidationException`.
    #[error("An operand in the update expression has an incorrect data type: list_append requires List (got {got})")]
    ListAppendOperandNotList { got: &'static str },

    /// Defensive — should be unreachable because storage rows are
    /// always JSON objects.
    #[error("stored item is not a JSON object")]
    ItemNotObject,
}

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

// =============================================================================
// ConditionExpression evaluation
// =============================================================================

/// Evaluate a resolved `Condition` AST against an `Option<&Value>`
/// (the existing row). Missing attributes make comparisons return
/// false (matches DDB).
pub fn evaluate_condition(existing: Option<&Value>, cond: &Condition) -> bool {
    match cond {
        Condition::AttributeExists(p) => p
            .top_name()
            .is_some_and(|n| existing.and_then(|v| v.get(n)).is_some()),
        Condition::AttributeNotExists(p) => p
            .top_name()
            .is_some_and(|n| existing.and_then(|v| v.get(n)).is_none()),
        Condition::Compare { op, left, right } => {
            let l = condition_operand_value(left, existing);
            let r = condition_operand_value(right, existing);
            match (l, r) {
                (Some(lv), Some(rv)) => compare_values(*op, &lv, &rv),
                _ => false,
            }
        }
        Condition::BeginsWith(p, o) => eval_begins_with(existing, p, o),
        Condition::Contains(p, o) => eval_contains(existing, p, o),
        Condition::Between(v, lo, hi) => eval_between(existing, v, lo, hi),
        Condition::In(v, items) => eval_in(existing, v, items),
        Condition::AttributeType(p, o) => eval_attribute_type(existing, p, o),
        Condition::And(a, b) => evaluate_condition(existing, a) && evaluate_condition(existing, b),
        Condition::Or(a, b) => evaluate_condition(existing, a) || evaluate_condition(existing, b),
        Condition::Not(inner) => !evaluate_condition(existing, inner),
    }
}

/// `begins_with(path, operand)` — true when `path` is `S` and starts
/// with the `operand`'s `S` value. Any type mismatch or missing path
/// returns false (DDB semantics).
fn eval_begins_with(existing: Option<&Value>, path: &Path, operand: &Operand) -> bool {
    let attr_val = match path
        .top_name()
        .and_then(|n| existing.and_then(|v| v.get(n)))
    {
        Some(v) => v,
        None => return false,
    };
    let needle = match condition_operand_value(operand, existing) {
        Some(v) => v,
        None => return false,
    };
    match (
        single_tagged(attr_val),
        single_tagged(&needle),
    ) {
        (Some(("S", a)), Some(("S", b))) => match (a.as_str(), b.as_str()) {
            (Some(haystack), Some(prefix)) => haystack.starts_with(prefix),
            _ => false,
        },
        // DDB also defines begins_with on B (binary prefix). Skip for
        // now — the slow path doesn't decode binary, and the use case
        // is rare. Returns false on mismatch, which is safe-by-default.
        _ => false,
    }
}

/// `contains(path, operand)` — three DDB modes:
/// - `path` is `S`: returns true iff operand is `S` and substring of path's value
/// - `path` is a set (`SS`/`NS`/`BS`): returns true iff operand is the matching scalar
///   (`S`/`N`/`B`) and is an element of the set
/// - `path` is `L`: returns true iff some element of the list equals operand by value
fn eval_contains(existing: Option<&Value>, path: &Path, operand: &Operand) -> bool {
    let attr_val = match path
        .top_name()
        .and_then(|n| existing.and_then(|v| v.get(n)))
    {
        Some(v) => v,
        None => return false,
    };
    let needle = match condition_operand_value(operand, existing) {
        Some(v) => v,
        None => return false,
    };

    match single_tagged(attr_val) {
        Some(("S", haystack_v)) => match (haystack_v.as_str(), single_tagged(&needle)) {
            (Some(haystack), Some(("S", needle_v))) => needle_v
                .as_str()
                .is_some_and(|n| haystack.contains(n)),
            _ => false,
        },
        Some(("SS", arr)) => contains_in_set(arr, &needle, "S"),
        Some(("NS", arr)) => contains_in_set(arr, &needle, "N"),
        Some(("BS", arr)) => contains_in_set(arr, &needle, "B"),
        Some(("L", arr)) => {
            // `L` membership: any list element JSON-equals the needle.
            arr.as_array().is_some_and(|items| items.iter().any(|i| i == &needle))
        }
        _ => false,
    }
}

/// Set membership helper: `arr` is the inner array of a set wrapper
/// (`{"SS":["a","b"]}` → `arr` is `["a","b"]`); `needle` should be a
/// `{tag: scalar}` object where tag is the corresponding scalar type
/// (`S` for `SS`, `N` for `NS`, `B` for `BS`).
fn contains_in_set(arr: &Value, needle: &Value, scalar_tag: &str) -> bool {
    let items = match arr.as_array() {
        Some(a) => a,
        None => return false,
    };
    let needle_scalar = match single_tagged(needle) {
        Some((tag, v)) if tag == scalar_tag => v,
        _ => return false,
    };
    items.iter().any(|item| item == needle_scalar)
}

/// `v BETWEEN lo AND hi` — inclusive range. Both bounds must share a
/// tag (N or S); cross-type comparisons return false.
fn eval_between(
    existing: Option<&Value>,
    v: &Operand,
    lo: &Operand,
    hi: &Operand,
) -> bool {
    let vv = match condition_operand_value(v, existing) {
        Some(v) => v,
        None => return false,
    };
    let lov = match condition_operand_value(lo, existing) {
        Some(v) => v,
        None => return false,
    };
    let hiv = match condition_operand_value(hi, existing) {
        Some(v) => v,
        None => return false,
    };
    // v >= lo AND v <= hi
    compare_values(ComparisonOp::Ge, &vv, &lov)
        && compare_values(ComparisonOp::Le, &vv, &hiv)
}

/// `v IN (op1, op2, ...)` — true iff any of the list members
/// JSON-equals `v`. The list must be non-empty per DDB grammar; we
/// don't enforce that here (parser does).
fn eval_in(existing: Option<&Value>, v: &Operand, items: &[Operand]) -> bool {
    let vv = match condition_operand_value(v, existing) {
        Some(v) => v,
        None => return false,
    };
    items.iter().any(|op| {
        condition_operand_value(op, existing)
            .as_ref()
            .is_some_and(|iv| iv == &vv)
    })
}

/// `attribute_type(path, :type)` — `:type` is one of the DDB tag
/// strings (`"S"`, `"N"`, ..., `"BS"`). Returns true iff the path's
/// top-level value uses that DDB tag.
fn eval_attribute_type(existing: Option<&Value>, path: &Path, operand: &Operand) -> bool {
    let attr_val = match path
        .top_name()
        .and_then(|n| existing.and_then(|v| v.get(n)))
    {
        Some(v) => v,
        None => return false,
    };
    let type_str = match condition_operand_value(operand, existing) {
        Some(v) => match single_tagged(&v) {
            Some(("S", s)) => match s.as_str() {
                Some(t) => t.to_string(),
                None => return false,
            },
            _ => return false,
        },
        None => return false,
    };
    matches!(single_tagged(attr_val), Some((tag, _)) if tag == type_str)
}

fn condition_operand_value(op: &Operand, existing: Option<&Value>) -> Option<Value> {
    match op {
        Operand::Path(p) => existing.and_then(|v| v.get(p.top_name()?)).cloned(),
        Operand::Value(v) => {
            Some(serde_json::to_value(v).expect("AttributeValue Serialize is infallible"))
        }
    }
}

fn compare_values(op: ComparisonOp, l: &Value, r: &Value) -> bool {
    match op {
        ComparisonOp::Eq => l == r,
        ComparisonOp::Ne => l != r,
        ord => {
            // Ordering: both operands must share a DDB tag and that tag
            // must be N or S. Anything else → false (matches the SQL
            // compiler's typed-extraction predicate).
            let (l_tag, l_val) = match single_tagged(l) {
                Some(p) => p,
                None => return false,
            };
            let (r_tag, r_val) = match single_tagged(r) {
                Some(p) => p,
                None => return false,
            };
            if l_tag != r_tag {
                return false;
            }
            match l_tag {
                "N" => match (
                    l_val.as_str().and_then(|s| s.parse::<f64>().ok()),
                    r_val.as_str().and_then(|s| s.parse::<f64>().ok()),
                ) {
                    (Some(a), Some(b)) => apply_ord(ord, a.partial_cmp(&b)),
                    _ => false,
                },
                "S" => match (l_val.as_str(), r_val.as_str()) {
                    (Some(a), Some(b)) => apply_ord(ord, Some(a.cmp(b))),
                    _ => false,
                },
                _ => false,
            }
        }
    }
}

fn apply_ord(op: ComparisonOp, ord: Option<std::cmp::Ordering>) -> bool {
    use std::cmp::Ordering::*;
    matches!(
        (op, ord),
        (ComparisonOp::Lt, Some(Less))
            | (ComparisonOp::Le, Some(Less | Equal))
            | (ComparisonOp::Gt, Some(Greater))
            | (ComparisonOp::Ge, Some(Greater | Equal))
    )
}

// =============================================================================
// Shared helpers
// =============================================================================

fn single_tagged(v: &Value) -> Option<(&str, &Value)> {
    let m: &Map<String, Value> = v.as_object()?;
    if m.len() != 1 {
        return None;
    }
    let (k, v) = m.iter().next()?;
    Some((k.as_str(), v))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn key_of(pairs: &[(&str, AttributeValue)]) -> Item {
        let mut m: BTreeMap<String, AttributeValue> = BTreeMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), v.clone());
        }
        m
    }

    fn parse_upd(src: &str, values: &[(&str, AttributeValue)]) -> UpdateExpression {
        let raw = rekt_expressions::parse_update_expression(src).unwrap();
        let names = BTreeMap::new();
        let vmap: BTreeMap<String, AttributeValue> = values
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        rekt_expressions::substitute_update(raw, &names, &vmap).unwrap()
    }

    fn parse_cond(src: &str, values: &[(&str, AttributeValue)]) -> Condition {
        let raw = rekt_expressions::parse_condition_expression(src).unwrap();
        let names = BTreeMap::new();
        let vmap: BTreeMap<String, AttributeValue> = values
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        rekt_expressions::substitute_condition(raw, &names, &vmap).unwrap()
    }

    // ----- SET literal (3a parity) ----------------------------------------

    #[test]
    fn set_literal_on_missing_row_synthesizes_from_key() {
        let key = key_of(&[("id", AttributeValue::S("u1".into()))]);
        let expr = parse_upd("SET label = :n", &[(":n", AttributeValue::S("alice".into()))]);
        let got = apply_update_expression(None, &expr, &key).unwrap();
        assert_eq!(
            got,
            serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}})
        );
    }

    #[test]
    fn set_literal_on_existing_row_merges() {
        let existing = serde_json::json!({"id":{"S":"u1"},"keep":{"S":"x"}});
        let expr = parse_upd("SET label = :n", &[(":n", AttributeValue::S("alice".into()))]);
        let got =
            apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(
            got,
            serde_json::json!({"id":{"S":"u1"},"keep":{"S":"x"},"label":{"S":"alice"}})
        );
    }

    #[test]
    fn remove_on_existing_row() {
        let existing = serde_json::json!({"id":{"S":"u1"},"goner":{"S":"x"},"keep":{"S":"y"}});
        let expr = parse_upd("REMOVE drop_me", &[]);
        // (renaming for clarity — "goner" is a Rust keyword in some contexts)
        let _ = expr;
        let expr = parse_upd("REMOVE goner", &[]);
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(
            got,
            serde_json::json!({"id":{"S":"u1"},"keep":{"S":"y"}})
        );
    }

    #[test]
    fn remove_absent_path_is_noop() {
        let existing = serde_json::json!({"id":{"S":"u1"}});
        let expr = parse_upd("REMOVE never_existed", &[]);
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(got, existing);
    }

    // ----- SET path-ref (3c-a) -------------------------------------------

    #[test]
    fn set_path_ref_copies_value() {
        let existing = serde_json::json!({"id":{"S":"u1"},"origin":{"S":"hello"}});
        let expr = parse_upd("SET copied = origin", &[]);
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(got.get("copied"), Some(&serde_json::json!({"S":"hello"})));
    }

    #[test]
    fn set_path_ref_missing_path_errors() {
        let existing = serde_json::json!({"id":{"S":"u1"}});
        let expr = parse_upd("SET copied = origin", &[]);
        let err =
            apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap_err();
        assert!(matches!(err, EvalError::PathDoesNotExist { ref path } if path == "origin"));
    }

    #[test]
    fn set_swap_reads_original_values() {
        // `SET a = b, b = a` swaps the values atomically — both RHS reads
        // happen against the pre-update row.
        let existing = serde_json::json!({"id":{"S":"u1"},"a":{"S":"AA"},"b":{"S":"BB"}});
        let expr = parse_upd("SET a = b, b = a", &[]);
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(got.get("a"), Some(&serde_json::json!({"S":"BB"})));
        assert_eq!(got.get("b"), Some(&serde_json::json!({"S":"AA"})));
    }

    // ----- SET arithmetic (3c-b) -----------------------------------------

    #[test]
    fn set_arith_plus_path_and_value() {
        let existing = serde_json::json!({"id":{"S":"u1"},"tally":{"N":"10"}});
        let expr = parse_upd(
            "SET tally = tally + :inc",
            &[(":inc", AttributeValue::N("5".into()))],
        );
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(got.get("tally"), Some(&serde_json::json!({"N":"15"})));
    }

    #[test]
    fn set_arith_minus() {
        let existing = serde_json::json!({"id":{"S":"u1"},"tally":{"N":"10"}});
        let expr = parse_upd(
            "SET tally = tally - :dec",
            &[(":dec", AttributeValue::N("3".into()))],
        );
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(got.get("tally"), Some(&serde_json::json!({"N":"7"})));
    }

    #[test]
    fn set_arith_rejects_non_numeric_operand() {
        let existing = serde_json::json!({"id":{"S":"u1"},"label":{"S":"hi"}});
        let expr = parse_upd(
            "SET tally = label + :v",
            &[(":v", AttributeValue::N("1".into()))],
        );
        let err =
            apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap_err();
        assert!(matches!(err, EvalError::ArithmeticOperandNotNumber { .. }));
    }

    // ----- if_not_exists (3c-c) -----------------------------------------

    #[test]
    fn if_not_exists_returns_fallback_when_path_absent() {
        let existing = serde_json::json!({"id":{"S":"u1"}});
        let expr = parse_upd(
            "SET created_at = if_not_exists(created_at, :now)",
            &[(":now", AttributeValue::N("1700000000".into()))],
        );
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(
            got.get("created_at"),
            Some(&serde_json::json!({"N":"1700000000"}))
        );
    }

    #[test]
    fn if_not_exists_preserves_existing_when_path_present() {
        let existing = serde_json::json!({"id":{"S":"u1"},"created_at":{"N":"1234567890"}});
        let expr = parse_upd(
            "SET created_at = if_not_exists(created_at, :now)",
            &[(":now", AttributeValue::N("9999999999".into()))],
        );
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(
            got.get("created_at"),
            Some(&serde_json::json!({"N":"1234567890"}))
        );
    }

    // ----- list_append (3c-d) -------------------------------------------

    #[test]
    fn list_append_path_then_value() {
        let existing = serde_json::json!({"id":{"S":"u1"},"entries":{"L":[{"S":"a"}]}});
        let expr = parse_upd(
            "SET entries = list_append(entries, :new)",
            &[(
                ":new",
                AttributeValue::L(vec![
                    AttributeValue::S("b".into()),
                    AttributeValue::S("c".into()),
                ]),
            )],
        );
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(
            got.get("entries"),
            Some(&serde_json::json!({"L":[{"S":"a"},{"S":"b"},{"S":"c"}]}))
        );
    }

    #[test]
    fn list_append_rejects_non_list_operand() {
        let existing = serde_json::json!({"id":{"S":"u1"},"entries":{"S":"oops"}});
        let expr = parse_upd(
            "SET entries = list_append(entries, :new)",
            &[(":new", AttributeValue::L(vec![]))],
        );
        let err =
            apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap_err();
        assert!(matches!(err, EvalError::ListAppendOperandNotList { .. }));
    }

    // ----- evaluate_condition ------------------------------------------

    #[test]
    fn cond_attribute_exists_true_when_present() {
        let item = serde_json::json!({"id":{"S":"u1"}});
        let cond = parse_cond("attribute_exists(id)", &[]);
        assert!(evaluate_condition(Some(&item), &cond));
    }

    #[test]
    fn cond_attribute_exists_false_when_absent() {
        let item = serde_json::json!({"id":{"S":"u1"}});
        let cond = parse_cond("attribute_exists(label)", &[]);
        assert!(!evaluate_condition(Some(&item), &cond));
    }

    #[test]
    fn cond_attribute_not_exists_true_on_missing_row() {
        let cond = parse_cond("attribute_not_exists(id)", &[]);
        assert!(evaluate_condition(None, &cond));
    }

    #[test]
    fn cond_equality_jsonb() {
        let item = serde_json::json!({"id":{"S":"u1"},"version":{"N":"3"}});
        let cond = parse_cond("version = :v", &[(":v", AttributeValue::N("3".into()))]);
        assert!(evaluate_condition(Some(&item), &cond));

        let cond_mismatch =
            parse_cond("version = :v", &[(":v", AttributeValue::N("4".into()))]);
        assert!(!evaluate_condition(Some(&item), &cond_mismatch));
    }

    #[test]
    fn cond_numeric_ordering() {
        let item = serde_json::json!({"id":{"S":"u1"},"score":{"N":"10"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("score < :v", &[(":v", AttributeValue::N("100".into()))])
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond("score > :v", &[(":v", AttributeValue::N("100".into()))])
        ));
    }

    #[test]
    fn cond_string_ordering() {
        let item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"bob"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("label >= :v", &[(":v", AttributeValue::S("a".into()))])
        ));
    }

    #[test]
    fn cond_and_or_not() {
        let item = serde_json::json!({"id":{"S":"u1"},"flag":{"S":"active"},"v":{"N":"1"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond(
                "flag = :s AND v = :v",
                &[
                    (":s", AttributeValue::S("active".into())),
                    (":v", AttributeValue::N("1".into())),
                ],
            )
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond(
                "flag = :s AND v = :v",
                &[
                    (":s", AttributeValue::S("active".into())),
                    (":v", AttributeValue::N("99".into())),
                ],
            )
        ));
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("NOT attribute_exists(deleted)", &[])
        ));
    }

    #[test]
    fn cond_path_vs_path_equality() {
        let item = serde_json::json!({"id":{"S":"u1"},"a":{"S":"x"},"b":{"S":"x"}});
        assert!(evaluate_condition(Some(&item), &parse_cond("a = b", &[])));
        let item2 = serde_json::json!({"id":{"S":"u1"},"a":{"S":"x"},"b":{"S":"y"}});
        assert!(!evaluate_condition(Some(&item2), &parse_cond("a = b", &[])));
    }

    // ----- Phase 4e: new condition shapes ---------------------------------

    #[test]
    fn cond_begins_with() {
        let item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("begins_with(label, :p)", &[(":p", AttributeValue::S("ali".into()))])
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond("begins_with(label, :p)", &[(":p", AttributeValue::S("bob".into()))])
        ));
        // Type mismatch (begins_with against N attribute) → false.
        let item_n = serde_json::json!({"id":{"S":"u1"},"x":{"N":"42"}});
        assert!(!evaluate_condition(
            Some(&item_n),
            &parse_cond("begins_with(x, :p)", &[(":p", AttributeValue::S("4".into()))])
        ));
    }

    #[test]
    fn cond_contains_string_substring() {
        let item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"hello world"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("contains(label, :p)", &[(":p", AttributeValue::S("lo wo".into()))])
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond("contains(label, :p)", &[(":p", AttributeValue::S("zzz".into()))])
        ));
    }

    #[test]
    fn cond_contains_set_membership() {
        let item = serde_json::json!({"id":{"S":"u1"},"tags":{"SS":["a","b","c"]}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("contains(tags, :p)", &[(":p", AttributeValue::S("b".into()))])
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond("contains(tags, :p)", &[(":p", AttributeValue::S("z".into()))])
        ));
    }

    #[test]
    fn cond_contains_list_element() {
        let item = serde_json::json!({"id":{"S":"u1"},"entries":{"L":[{"S":"a"},{"N":"7"}]}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("contains(entries, :v)", &[(":v", AttributeValue::N("7".into()))])
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond("contains(entries, :v)", &[(":v", AttributeValue::N("99".into()))])
        ));
    }

    #[test]
    fn cond_between_numeric() {
        let item = serde_json::json!({"id":{"S":"u1"},"score":{"N":"50"}});
        let lo = AttributeValue::N("10".into());
        let hi = AttributeValue::N("100".into());
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("score BETWEEN :lo AND :hi", &[(":lo", lo.clone()), (":hi", hi.clone())])
        ));
        // Out of range.
        let out_item = serde_json::json!({"id":{"S":"u1"},"score":{"N":"200"}});
        assert!(!evaluate_condition(
            Some(&out_item),
            &parse_cond("score BETWEEN :lo AND :hi", &[(":lo", lo), (":hi", hi)])
        ));
    }

    #[test]
    fn cond_between_string() {
        let item = serde_json::json!({"id":{"S":"u1"},"label":{"S":"charlie"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond(
                "label BETWEEN :a AND :z",
                &[
                    (":a", AttributeValue::S("a".into())),
                    (":z", AttributeValue::S("d".into())),
                ],
            )
        ));
    }

    #[test]
    fn cond_in_list() {
        let item = serde_json::json!({"id":{"S":"u1"},"flag":{"S":"pending"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond(
                "flag IN (:a, :b, :c)",
                &[
                    (":a", AttributeValue::S("active".into())),
                    (":b", AttributeValue::S("pending".into())),
                    (":c", AttributeValue::S("closed".into())),
                ],
            )
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond(
                "flag IN (:a, :b)",
                &[
                    (":a", AttributeValue::S("active".into())),
                    (":b", AttributeValue::S("closed".into())),
                ],
            )
        ));
    }

    #[test]
    fn cond_attribute_type_matches() {
        let item = serde_json::json!({"id":{"S":"u1"},"score":{"N":"42"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("attribute_type(score, :t)", &[(":t", AttributeValue::S("N".into()))])
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond("attribute_type(score, :t)", &[(":t", AttributeValue::S("S".into()))])
        ));
        // Missing path → false.
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond("attribute_type(absent, :t)", &[(":t", AttributeValue::S("S".into()))])
        ));
    }

    // ----- Phase 5: ADD ---------------------------------------------------

    #[test]
    fn add_numeric_on_missing_creates_with_value() {
        let key = key_of(&[("id", AttributeValue::S("u1".into()))]);
        let expr = parse_upd(
            "ADD tally :inc",
            &[(":inc", AttributeValue::N("5".into()))],
        );
        let got = apply_update_expression(None, &expr, &key).unwrap();
        assert_eq!(got.get("tally"), Some(&serde_json::json!({"N":"5"})));
    }

    #[test]
    fn add_numeric_on_existing_increments() {
        let existing = serde_json::json!({"id":{"S":"u1"},"tally":{"N":"10"}});
        let expr = parse_upd(
            "ADD tally :inc",
            &[(":inc", AttributeValue::N("3".into()))],
        );
        let got =
            apply_update_expression(Some(&existing), &expr, &std::collections::BTreeMap::new())
                .unwrap();
        assert_eq!(got.get("tally"), Some(&serde_json::json!({"N":"13"})));
    }

    #[test]
    fn add_numeric_existing_wrong_type_errors() {
        let existing = serde_json::json!({"id":{"S":"u1"},"tally":{"S":"oops"}});
        let expr = parse_upd(
            "ADD tally :inc",
            &[(":inc", AttributeValue::N("1".into()))],
        );
        let err =
            apply_update_expression(Some(&existing), &expr, &std::collections::BTreeMap::new())
                .unwrap_err();
        assert!(matches!(err, EvalError::ArithmeticOperandNotNumber { .. }));
    }

    #[test]
    fn add_set_on_missing_creates_with_set() {
        let key = key_of(&[("id", AttributeValue::S("u1".into()))]);
        let expr = parse_upd(
            "ADD tags :new",
            &[(":new", AttributeValue::Ss(vec!["a".into(), "b".into()]))],
        );
        let got = apply_update_expression(None, &expr, &key).unwrap();
        // Set elements preserve insertion order in our impl; test order-
        // insensitively by checking each item is present.
        let tags = got["tags"]["SS"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.iter().any(|v| v == "a"));
        assert!(tags.iter().any(|v| v == "b"));
    }

    #[test]
    fn add_set_unions_with_dedup() {
        let existing = serde_json::json!({"id":{"S":"u1"},"tags":{"SS":["a","b"]}});
        let expr = parse_upd(
            "ADD tags :new",
            &[(":new", AttributeValue::Ss(vec!["b".into(), "c".into()]))],
        );
        let got =
            apply_update_expression(Some(&existing), &expr, &std::collections::BTreeMap::new())
                .unwrap();
        let tags = got["tags"]["SS"].as_array().unwrap();
        assert_eq!(tags.len(), 3);
        for s in ["a", "b", "c"] {
            assert!(tags.iter().any(|v| v == s), "missing {s}");
        }
    }

    // ----- Phase 6: DELETE -----------------------------------------------

    #[test]
    fn delete_set_removes_elements() {
        let existing = serde_json::json!({"id":{"S":"u1"},"tags":{"SS":["a","b","c"]}});
        let expr = parse_upd(
            "DELETE tags :rm",
            &[(":rm", AttributeValue::Ss(vec!["b".into()]))],
        );
        let got =
            apply_update_expression(Some(&existing), &expr, &std::collections::BTreeMap::new())
                .unwrap();
        let tags = got["tags"]["SS"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.iter().any(|v| v == "a"));
        assert!(tags.iter().any(|v| v == "c"));
    }

    #[test]
    fn delete_set_empties_attribute_when_all_removed() {
        let existing = serde_json::json!({"id":{"S":"u1"},"tags":{"SS":["a","b"]}});
        let expr = parse_upd(
            "DELETE tags :rm",
            &[(":rm", AttributeValue::Ss(vec!["a".into(), "b".into()]))],
        );
        let got =
            apply_update_expression(Some(&existing), &expr, &std::collections::BTreeMap::new())
                .unwrap();
        // DDB: if DELETE empties the set, the attribute is removed entirely.
        assert!(got.get("tags").is_none());
    }

    #[test]
    fn delete_path_absent_is_noop() {
        let existing = serde_json::json!({"id":{"S":"u1"}});
        let expr = parse_upd(
            "DELETE tags :rm",
            &[(":rm", AttributeValue::Ss(vec!["x".into()]))],
        );
        let got =
            apply_update_expression(Some(&existing), &expr, &std::collections::BTreeMap::new())
                .unwrap();
        assert_eq!(got, existing);
    }

    #[test]
    fn delete_wrong_type_errors() {
        let existing = serde_json::json!({"id":{"S":"u1"},"tags":{"S":"not_a_set"}});
        let expr = parse_upd(
            "DELETE tags :rm",
            &[(":rm", AttributeValue::Ss(vec!["a".into()]))],
        );
        let err =
            apply_update_expression(Some(&existing), &expr, &std::collections::BTreeMap::new())
                .unwrap_err();
        assert!(matches!(err, EvalError::ListAppendOperandNotList { .. }));
    }
}
