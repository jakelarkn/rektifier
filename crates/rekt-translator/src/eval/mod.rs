//! Pure in-Rust evaluators for `UpdateExpression` and `ConditionExpression`,
//! used by the storage layer's slow path (Phase 3b/4e). Both operate
//! over `serde_json::Value`s in DDB-JSON form (`{"S":"alice"}` etc.) —
//! the same shape the backends store. No SQL, no async, no I/O.
//!
//! ## Module layout
//!
//! - [`update`] — [`apply_update_expression`] + SET/REMOVE/ADD/DELETE
//!   helpers. Owns [`EvalError`]'s use sites.
//! - [`condition`] — [`evaluate_condition`] + every Phase 4e
//!   sub-predicate (`begins_with`, `contains`, `BETWEEN`, `IN`,
//!   `attribute_type`). Returns plain `bool` — no error type involved.
//! - `values` (private) — shared DDB-JSON shape introspection
//!   (`single_tagged`).

mod condition;
mod update;
mod values;

pub use condition::evaluate_condition;
pub use update::apply_update_expression;

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
#[cfg(test)]
mod tests {
    use super::*;
    use rekt_expressions::{Condition, UpdateExpression};
    use rekt_protocol::{AttributeValue, Item};
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
