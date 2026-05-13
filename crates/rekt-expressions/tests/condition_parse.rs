//! Parser + substitution tests for `ConditionExpression`. Mirrors the
//! structure of `update_parse.rs`:
//! - `parse_*`: raw AST shape from `parse_condition_expression`.
//! - `subst_*`: resolved AST shape after `substitute_condition`.
//! - `err_*`: parser / substitution error cases.

use std::collections::BTreeMap;

use rekt_expressions::ast::*;
use rekt_expressions::{
    parse_condition_expression, substitute_condition, ParseError, SubstituteError,
};
use rekt_protocol::AttributeValue;

// ---- helpers ----------------------------------------------------------------

fn pn(s: &str) -> RawPathSegment {
    RawPathSegment::Name(s.into())
}
fn pnref(s: &str) -> RawPathSegment {
    RawPathSegment::NameRef(s.into())
}
fn rpath(segs: &[RawPathSegment]) -> RawPath {
    RawPath {
        segments: segs.to_vec(),
    }
}
fn vref(s: &str) -> RawOperand {
    RawOperand::ValueRef(s.into())
}
fn rop_path(name: &str) -> RawOperand {
    RawOperand::Path(rpath(&[pn(name)]))
}
fn cmp(op: ComparisonOp, left: RawOperand, right: RawOperand) -> RawCondition {
    RawCondition::Compare { op, left, right }
}

fn names(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}
fn values(pairs: &[(&str, AttributeValue)]) -> BTreeMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

// ===== Atoms ================================================================

#[test]
fn parse_attribute_exists_simple() {
    let got = parse_condition_expression("attribute_exists(id)").unwrap();
    assert_eq!(got, RawCondition::AttributeExists(rpath(&[pn("id")])));
}

#[test]
fn parse_attribute_not_exists_simple() {
    let got = parse_condition_expression("attribute_not_exists(id)").unwrap();
    assert_eq!(got, RawCondition::AttributeNotExists(rpath(&[pn("id")])));
}

#[test]
fn parse_attribute_exists_case_insensitive() {
    for src in [
        "Attribute_Exists(id)",
        "ATTRIBUTE_EXISTS(id)",
        "attribute_EXISTS(id)",
    ] {
        let got = parse_condition_expression(src).unwrap();
        assert_eq!(got, RawCondition::AttributeExists(rpath(&[pn("id")])));
    }
}

#[test]
fn parse_attribute_exists_name_ref() {
    let got = parse_condition_expression("attribute_exists(#a)").unwrap();
    assert_eq!(
        got,
        RawCondition::AttributeExists(rpath(&[pnref("a")]))
    );
}

#[test]
fn parse_attribute_exists_whitespace_inside_parens() {
    let got = parse_condition_expression("attribute_exists ( id )").unwrap();
    assert_eq!(got, RawCondition::AttributeExists(rpath(&[pn("id")])));
}

// ===== Comparisons ===========================================================

#[test]
fn parse_eq_path_value() {
    let got = parse_condition_expression("version = :v").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Eq, rop_path("version"), vref("v")));
}

#[test]
fn parse_ne_path_value() {
    let got = parse_condition_expression("status <> :s").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Ne, rop_path("status"), vref("s")));
}

#[test]
fn parse_lt_le_gt_ge() {
    for (src, op) in [
        ("score < :n", ComparisonOp::Lt),
        ("score <= :n", ComparisonOp::Le),
        ("score > :n", ComparisonOp::Gt),
        ("score >= :n", ComparisonOp::Ge),
    ] {
        let got = parse_condition_expression(src).unwrap();
        assert_eq!(got, cmp(op, rop_path("score"), vref("n")), "src={src}");
    }
}

#[test]
fn parse_value_lhs_path_rhs() {
    // DDB allows either side to be a value or path. (The translator will
    // later restrict which combinations make sense; the parser is permissive.)
    let got = parse_condition_expression(":threshold < score").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Lt, vref("threshold"), rop_path("score")));
}

#[test]
fn parse_path_vs_path() {
    let got = parse_condition_expression("a = b").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Eq, rop_path("a"), rop_path("b")));
}

#[test]
fn parse_comparison_with_name_refs() {
    let got = parse_condition_expression("#status = :v").unwrap();
    assert_eq!(
        got,
        cmp(
            ComparisonOp::Eq,
            RawOperand::Path(rpath(&[pnref("status")])),
            vref("v")
        )
    );
}

#[test]
fn parse_comparator_no_whitespace_around() {
    let got = parse_condition_expression("a=:v").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Eq, rop_path("a"), vref("v")));
}

#[test]
fn parse_le_not_lt() {
    // Precedence in the comparator alt: `<=` is tried before `<`, so
    // `a <= :v` parses as Le, not Lt-then-syntax-error.
    let got = parse_condition_expression("a <= :v").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Le, rop_path("a"), vref("v")));
}

#[test]
fn parse_ne_not_lt_gt_pair() {
    // `<>` parses as Ne, not as `<` followed by `>`.
    let got = parse_condition_expression("a <> :v").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Ne, rop_path("a"), vref("v")));
}

// ===== Boolean composition ===================================================

#[test]
fn parse_and_two_terms() {
    let got = parse_condition_expression("a = :x AND b = :y").unwrap();
    assert_eq!(
        got,
        RawCondition::And(
            Box::new(cmp(ComparisonOp::Eq, rop_path("a"), vref("x"))),
            Box::new(cmp(ComparisonOp::Eq, rop_path("b"), vref("y"))),
        )
    );
}

#[test]
fn parse_or_two_terms() {
    let got = parse_condition_expression("a = :x OR b = :y").unwrap();
    assert_eq!(
        got,
        RawCondition::Or(
            Box::new(cmp(ComparisonOp::Eq, rop_path("a"), vref("x"))),
            Box::new(cmp(ComparisonOp::Eq, rop_path("b"), vref("y"))),
        )
    );
}

#[test]
fn parse_and_or_precedence() {
    // `a OR b AND c` is `a OR (b AND c)` because AND binds tighter than OR.
    let got = parse_condition_expression("a = :x OR b = :y AND c = :z").unwrap();
    let b_and_c = RawCondition::And(
        Box::new(cmp(ComparisonOp::Eq, rop_path("b"), vref("y"))),
        Box::new(cmp(ComparisonOp::Eq, rop_path("c"), vref("z"))),
    );
    assert_eq!(
        got,
        RawCondition::Or(
            Box::new(cmp(ComparisonOp::Eq, rop_path("a"), vref("x"))),
            Box::new(b_and_c),
        )
    );
}

#[test]
fn parse_not_negates_atom() {
    let got = parse_condition_expression("NOT attribute_exists(id)").unwrap();
    assert_eq!(
        got,
        RawCondition::Not(Box::new(RawCondition::AttributeExists(rpath(&[pn("id")]))))
    );
}

#[test]
fn parse_not_binds_tighter_than_and() {
    // `NOT a = :x AND b = :y` is `(NOT a = :x) AND (b = :y)`.
    let got = parse_condition_expression("NOT a = :x AND b = :y").unwrap();
    let not_a = RawCondition::Not(Box::new(cmp(ComparisonOp::Eq, rop_path("a"), vref("x"))));
    let b_eq = cmp(ComparisonOp::Eq, rop_path("b"), vref("y"));
    assert_eq!(got, RawCondition::And(Box::new(not_a), Box::new(b_eq)));
}

#[test]
fn parse_parens_override_precedence() {
    // `(a OR b) AND c` — parens flip OR to bind first.
    let got = parse_condition_expression("(a = :x OR b = :y) AND c = :z").unwrap();
    let a_or_b = RawCondition::Or(
        Box::new(cmp(ComparisonOp::Eq, rop_path("a"), vref("x"))),
        Box::new(cmp(ComparisonOp::Eq, rop_path("b"), vref("y"))),
    );
    let c_eq = cmp(ComparisonOp::Eq, rop_path("c"), vref("z"));
    assert_eq!(got, RawCondition::And(Box::new(a_or_b), Box::new(c_eq)));
}

#[test]
fn parse_nested_parens() {
    let got = parse_condition_expression("((a = :x))").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Eq, rop_path("a"), vref("x")));
}

#[test]
fn parse_left_associative_and_chain() {
    // `a AND b AND c` left-associates: ((a AND b) AND c).
    let got = parse_condition_expression(
        "attribute_exists(a) AND attribute_exists(b) AND attribute_exists(c)",
    )
    .unwrap();
    let a = RawCondition::AttributeExists(rpath(&[pn("a")]));
    let b = RawCondition::AttributeExists(rpath(&[pn("b")]));
    let c = RawCondition::AttributeExists(rpath(&[pn("c")]));
    assert_eq!(
        got,
        RawCondition::And(
            Box::new(RawCondition::And(Box::new(a), Box::new(b))),
            Box::new(c),
        )
    );
}

#[test]
fn parse_keywords_case_insensitive() {
    let got = parse_condition_expression("a = :x and b = :y or NOT attribute_exists(c)").unwrap();
    // and binds tighter than or; NOT binds tightest.
    let a_and_b = RawCondition::And(
        Box::new(cmp(ComparisonOp::Eq, rop_path("a"), vref("x"))),
        Box::new(cmp(ComparisonOp::Eq, rop_path("b"), vref("y"))),
    );
    let not_exists_c = RawCondition::Not(Box::new(RawCondition::AttributeExists(rpath(&[pn("c")]))));
    assert_eq!(
        got,
        RawCondition::Or(Box::new(a_and_b), Box::new(not_exists_c))
    );
}

#[test]
fn parse_name_starting_with_keyword_letters_is_identifier() {
    // `andrew` shouldn't be eaten as `AND` then unexpected `rew`. The AND
    // keyword matcher requires whitespace on both sides, so identifiers
    // starting with `and` parse as identifiers.
    let got = parse_condition_expression("andrew = :v").unwrap();
    assert_eq!(got, cmp(ComparisonOp::Eq, rop_path("andrew"), vref("v")));
}

// ===== Substitution ==========================================================

#[test]
fn subst_attribute_exists_resolves_name_ref() {
    let raw = parse_condition_expression("attribute_exists(#a)").unwrap();
    let resolved = substitute_condition(raw, &names(&[("#a", "status")]), &values(&[])).unwrap();
    assert_eq!(
        resolved,
        Condition::AttributeExists(Path {
            segments: vec![PathSegment::Name("status".into())],
        })
    );
}

#[test]
fn subst_comparison_resolves_value_ref() {
    let raw = parse_condition_expression("version = :v").unwrap();
    let resolved = substitute_condition(
        raw,
        &names(&[]),
        &values(&[(":v", AttributeValue::N("3".into()))]),
    )
    .unwrap();
    assert_eq!(
        resolved,
        Condition::Compare {
            op: ComparisonOp::Eq,
            left: Operand::Path(Path {
                segments: vec![PathSegment::Name("version".into())]
            }),
            right: Operand::Value(AttributeValue::N("3".into())),
        }
    );
}

#[test]
fn subst_boolean_composition() {
    let raw =
        parse_condition_expression("attribute_exists(#k) AND #k = :v").unwrap();
    let resolved = substitute_condition(
        raw,
        &names(&[("#k", "status")]),
        &values(&[(":v", AttributeValue::S("active".into()))]),
    )
    .unwrap();
    let status_path = Path {
        segments: vec![PathSegment::Name("status".into())],
    };
    assert_eq!(
        resolved,
        Condition::And(
            Box::new(Condition::AttributeExists(status_path.clone())),
            Box::new(Condition::Compare {
                op: ComparisonOp::Eq,
                left: Operand::Path(status_path),
                right: Operand::Value(AttributeValue::S("active".into())),
            }),
        )
    );
}

#[test]
fn subst_not() {
    let raw = parse_condition_expression("NOT attribute_exists(#a)").unwrap();
    let resolved =
        substitute_condition(raw, &names(&[("#a", "id")]), &values(&[])).unwrap();
    assert_eq!(
        resolved,
        Condition::Not(Box::new(Condition::AttributeExists(Path {
            segments: vec![PathSegment::Name("id".into())],
        })))
    );
}

// ===== Error cases ===========================================================

#[test]
fn err_empty_expression() {
    let err = parse_condition_expression("").unwrap_err();
    assert!(matches!(err, ParseError::Empty { kind } if kind == "ConditionExpression"));
}

#[test]
fn err_whitespace_only_expression() {
    let err = parse_condition_expression("   ").unwrap_err();
    assert!(matches!(err, ParseError::Empty { kind } if kind == "ConditionExpression"));
}

#[test]
fn err_unbalanced_paren() {
    let err = parse_condition_expression("(a = :v").unwrap_err();
    assert!(matches!(err, ParseError::Invalid { .. }));
}

#[test]
fn err_missing_comparator() {
    let err = parse_condition_expression("a b").unwrap_err();
    assert!(matches!(err, ParseError::Invalid { .. }));
}

#[test]
fn err_trailing_garbage() {
    let err = parse_condition_expression("a = :v AND").unwrap_err();
    assert!(matches!(err, ParseError::Invalid { .. }));
}

#[test]
fn err_attribute_exists_missing_arg() {
    let err = parse_condition_expression("attribute_exists()").unwrap_err();
    assert!(matches!(err, ParseError::Invalid { .. }));
}

#[test]
fn err_substitute_missing_name() {
    let raw = parse_condition_expression("attribute_exists(#x)").unwrap();
    let err = substitute_condition(raw, &names(&[]), &values(&[])).unwrap_err();
    assert!(matches!(err, SubstituteError::UnknownName { ref name } if name == "x"));
}

#[test]
fn err_substitute_missing_value() {
    let raw = parse_condition_expression("a = :missing").unwrap();
    let err = substitute_condition(raw, &names(&[]), &values(&[])).unwrap_err();
    assert!(matches!(err, SubstituteError::UnknownValue { ref name } if name == "missing"));
}

// ===== Phase 4e: extended grammar ============================================

#[test]
fn parse_begins_with() {
    let got = parse_condition_expression("begins_with(name, :p)").unwrap();
    assert_eq!(
        got,
        RawCondition::BeginsWith(rpath(&[pn("name")]), vref("p"))
    );
}

#[test]
fn parse_contains() {
    let got = parse_condition_expression("contains(tags, :s)").unwrap();
    assert_eq!(got, RawCondition::Contains(rpath(&[pn("tags")]), vref("s")));
}

#[test]
fn parse_attribute_type() {
    let got = parse_condition_expression("attribute_type(score, :t)").unwrap();
    assert_eq!(
        got,
        RawCondition::AttributeType(rpath(&[pn("score")]), vref("t"))
    );
}

#[test]
fn parse_between() {
    let got = parse_condition_expression("score BETWEEN :lo AND :hi").unwrap();
    assert_eq!(
        got,
        RawCondition::Between(rop_path("score"), vref("lo"), vref("hi"))
    );
}

#[test]
fn parse_in_list() {
    let got = parse_condition_expression("status IN (:a, :b, :c)").unwrap();
    assert_eq!(
        got,
        RawCondition::In(rop_path("status"), vec![vref("a"), vref("b"), vref("c")])
    );
}

#[test]
fn parse_in_single_item() {
    let got = parse_condition_expression("status IN (:a)").unwrap();
    assert_eq!(got, RawCondition::In(rop_path("status"), vec![vref("a")]));
}

#[test]
fn parse_keyword_word_boundary_in_not_consumed_by_inactive_ident() {
    // `a inactive = :v` would be invalid in DDB (two operands without
    // operator), but the parser should NOT mis-tokenize `inactive` as
    // `IN active`. Parse should fail cleanly rather than half-consume.
    assert!(parse_condition_expression("a inactive = :v").is_err());
}

#[test]
fn parse_between_inside_and_composition() {
    // The AND keyword inside BETWEEN's `BETWEEN lo AND hi` shouldn't be
    // confused with boolean AND when followed by a separate condition.
    let got =
        parse_condition_expression("score BETWEEN :lo AND :hi AND attribute_exists(id)").unwrap();
    let between = RawCondition::Between(rop_path("score"), vref("lo"), vref("hi"));
    let exists = RawCondition::AttributeExists(rpath(&[pn("id")]));
    assert_eq!(got, RawCondition::And(Box::new(between), Box::new(exists)));
}

#[test]
fn parse_in_with_paths() {
    // DDB allows paths in the IN list, not just values.
    let got = parse_condition_expression("a IN (b, c)").unwrap();
    assert_eq!(
        got,
        RawCondition::In(
            rop_path("a"),
            vec![rop_path("b"), rop_path("c")],
        )
    );
}

#[test]
fn parse_case_insensitive_extended_keywords() {
    // BETWEEN / IN / begins_with all case-insensitive.
    assert!(parse_condition_expression("score between :lo and :hi").is_ok());
    assert!(parse_condition_expression("status in (:a)").is_ok());
    assert!(parse_condition_expression("Begins_With(name, :p)").is_ok());
}

#[test]
fn err_in_with_empty_list() {
    // `path IN ()` — empty list is not valid grammar.
    assert!(parse_condition_expression("status IN ()").is_err());
}

#[test]
fn err_between_missing_and() {
    assert!(parse_condition_expression("score BETWEEN :lo :hi").is_err());
}
