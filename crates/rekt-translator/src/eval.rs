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
use rekt_protocol::Item;
// `AttributeValue` (which `Item` aliases over) is also referenced from
// the test module; brought in there via `use super::*`.
#[cfg(test)]
use rekt_protocol::AttributeValue;
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
    // an upsert), apply SETs, then REMOVEs.
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
    Ok(item)
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
        Condition::And(a, b) => evaluate_condition(existing, a) && evaluate_condition(existing, b),
        Condition::Or(a, b) => evaluate_condition(existing, a) || evaluate_condition(existing, b),
        Condition::Not(inner) => !evaluate_condition(existing, inner),
    }
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
        let expr = parse_upd("SET name = :n", &[(":n", AttributeValue::S("alice".into()))]);
        let got = apply_update_expression(None, &expr, &key).unwrap();
        assert_eq!(
            got,
            serde_json::json!({"id":{"S":"u1"},"name":{"S":"alice"}})
        );
    }

    #[test]
    fn set_literal_on_existing_row_merges() {
        let existing = serde_json::json!({"id":{"S":"u1"},"keep":{"S":"x"}});
        let expr = parse_upd("SET name = :n", &[(":n", AttributeValue::S("alice".into()))]);
        let got =
            apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(
            got,
            serde_json::json!({"id":{"S":"u1"},"keep":{"S":"x"},"name":{"S":"alice"}})
        );
    }

    #[test]
    fn remove_on_existing_row() {
        let existing = serde_json::json!({"id":{"S":"u1"},"drop":{"S":"x"},"keep":{"S":"y"}});
        let expr = parse_upd("REMOVE drop_me", &[]);
        // (renaming for clarity — "drop" is a Rust keyword in some contexts)
        let _ = expr;
        let expr = parse_upd("REMOVE drop", &[]);
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
        let existing = serde_json::json!({"id":{"S":"u1"},"source":{"S":"hello"}});
        let expr = parse_upd("SET copied = source", &[]);
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(got.get("copied"), Some(&serde_json::json!({"S":"hello"})));
    }

    #[test]
    fn set_path_ref_missing_path_errors() {
        let existing = serde_json::json!({"id":{"S":"u1"}});
        let expr = parse_upd("SET copied = source", &[]);
        let err =
            apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap_err();
        assert!(matches!(err, EvalError::PathDoesNotExist { ref path } if path == "source"));
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
        let existing = serde_json::json!({"id":{"S":"u1"},"counter":{"N":"10"}});
        let expr = parse_upd(
            "SET counter = counter + :inc",
            &[(":inc", AttributeValue::N("5".into()))],
        );
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(got.get("counter"), Some(&serde_json::json!({"N":"15"})));
    }

    #[test]
    fn set_arith_minus() {
        let existing = serde_json::json!({"id":{"S":"u1"},"counter":{"N":"10"}});
        let expr = parse_upd(
            "SET counter = counter - :dec",
            &[(":dec", AttributeValue::N("3".into()))],
        );
        let got = apply_update_expression(Some(&existing), &expr, &BTreeMap::new()).unwrap();
        assert_eq!(got.get("counter"), Some(&serde_json::json!({"N":"7"})));
    }

    #[test]
    fn set_arith_rejects_non_numeric_operand() {
        let existing = serde_json::json!({"id":{"S":"u1"},"name":{"S":"hi"}});
        let expr = parse_upd(
            "SET counter = name + :v",
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
        let existing = serde_json::json!({"id":{"S":"u1"},"items":{"L":[{"S":"a"}]}});
        let expr = parse_upd(
            "SET items = list_append(items, :new)",
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
            got.get("items"),
            Some(&serde_json::json!({"L":[{"S":"a"},{"S":"b"},{"S":"c"}]}))
        );
    }

    #[test]
    fn list_append_rejects_non_list_operand() {
        let existing = serde_json::json!({"id":{"S":"u1"},"items":{"S":"oops"}});
        let expr = parse_upd(
            "SET items = list_append(items, :new)",
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
        let cond = parse_cond("attribute_exists(name)", &[]);
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
        let item = serde_json::json!({"id":{"S":"u1"},"name":{"S":"bob"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond("name >= :v", &[(":v", AttributeValue::S("a".into()))])
        ));
    }

    #[test]
    fn cond_and_or_not() {
        let item = serde_json::json!({"id":{"S":"u1"},"status":{"S":"active"},"v":{"N":"1"}});
        assert!(evaluate_condition(
            Some(&item),
            &parse_cond(
                "status = :s AND v = :v",
                &[
                    (":s", AttributeValue::S("active".into())),
                    (":v", AttributeValue::N("1".into())),
                ],
            )
        ));
        assert!(!evaluate_condition(
            Some(&item),
            &parse_cond(
                "status = :s AND v = :v",
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
}
