//! In-Rust evaluator for `ConditionExpression`: runs the v1 grammar
//! against an `Option<&Value>` (the existing row) and returns a bool.
//! Missing-attribute comparisons evaluate to false, matching DDB.

use super::values::single_tagged;
use rekt_expressions::{ComparisonOp, Condition, Operand, Path};
use serde_json::Value;

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
