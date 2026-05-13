//! Comprehensive parser tests for `UpdateExpression`. Covers every clause
//! type, every SET-RHS shape, every path form, and the placeholder-
//! substitution path including the full set of 10 AttributeValue variants.
//!
//! Tests are organized:
//! - `parse_*`: assert raw AST shape from `parse_update_expression`.
//! - `subst_*`: assert resolved AST shape after `substitute_update`.
//! - `err_*`: parser + substitution error cases.
//! - `roundtrip_*`: full end-to-end through real placeholder maps.

use std::collections::BTreeMap;

use bytes::Bytes;
use rekt_expressions::ast::*;
use rekt_expressions::{parse_update_expression, substitute_update, ParseError};
use rekt_protocol::AttributeValue;

fn pn(s: &str) -> RawPathSegment {
    RawPathSegment::Name(s.into())
}
fn pnref(s: &str) -> RawPathSegment {
    RawPathSegment::NameRef(s.into())
}
fn pidx(n: usize) -> RawPathSegment {
    RawPathSegment::Index(n)
}

fn rpath(segs: &[RawPathSegment]) -> RawPath {
    RawPath {
        segments: segs.to_vec(),
    }
}

fn vref(s: &str) -> RawOperand {
    RawOperand::ValueRef(s.into())
}

fn opath(segs: &[RawPathSegment]) -> RawOperand {
    RawOperand::Path(rpath(segs))
}

// ===== SET — every RHS shape ===============================================

#[test]
fn parse_set_top_level_literal() {
    let r = parse_update_expression("SET status = :v").unwrap();
    assert_eq!(
        r.set,
        vec![RawSetClause {
            path: rpath(&[pn("status")]),
            value: RawSetRhs::Operand(vref("v")),
        }]
    );
    assert!(r.remove.is_empty() && r.add.is_empty() && r.delete.is_empty());
}

#[test]
fn parse_set_multiple_clauses_one_keyword() {
    let r = parse_update_expression("SET a = :v, b = :w, c = :x").unwrap();
    assert_eq!(r.set.len(), 3);
    assert_eq!(r.set[0].path, rpath(&[pn("a")]));
    assert_eq!(r.set[1].path, rpath(&[pn("b")]));
    assert_eq!(r.set[2].path, rpath(&[pn("c")]));
}

#[test]
fn parse_set_path_reference() {
    // SET a = b — copy attribute b to a
    let r = parse_update_expression("SET a = b").unwrap();
    assert_eq!(
        r.set[0].value,
        RawSetRhs::Operand(RawOperand::Path(rpath(&[pn("b")])))
    );
}

#[test]
fn parse_set_arithmetic_plus() {
    let r = parse_update_expression("SET total = subtotal + :tax").unwrap();
    assert_eq!(
        r.set[0].value,
        RawSetRhs::Plus(opath(&[pn("subtotal")]), vref("tax"))
    );
}

#[test]
fn parse_set_arithmetic_minus() {
    let r = parse_update_expression("SET balance = balance - :amount").unwrap();
    assert_eq!(
        r.set[0].value,
        RawSetRhs::Minus(opath(&[pn("balance")]), vref("amount"))
    );
}

#[test]
fn parse_set_if_not_exists() {
    let r = parse_update_expression("SET created_at = if_not_exists(created_at, :now)").unwrap();
    match &r.set[0].value {
        RawSetRhs::IfNotExists(p, inner) => {
            assert_eq!(p, &rpath(&[pn("created_at")]));
            assert_eq!(**inner, RawSetRhs::Operand(vref("now")));
        }
        other => panic!("expected IfNotExists, got {other:?}"),
    }
}

#[test]
fn parse_set_list_append() {
    let r = parse_update_expression("SET items = list_append(items, :new_items)").unwrap();
    match &r.set[0].value {
        RawSetRhs::ListAppend(a, b) => {
            assert!(matches!(**a, RawSetRhs::Operand(RawOperand::Path(_))));
            assert!(matches!(**b, RawSetRhs::Operand(RawOperand::ValueRef(_))));
        }
        other => panic!("expected ListAppend, got {other:?}"),
    }
}

#[test]
fn parse_set_list_append_with_empty_initial() {
    // list_append wrapped in if_not_exists is the classic
    // "append-to-list-or-create-it" idiom.
    let r = parse_update_expression("SET items = list_append(if_not_exists(items, :empty), :v)")
        .unwrap();
    match &r.set[0].value {
        RawSetRhs::ListAppend(a, _) => {
            assert!(matches!(**a, RawSetRhs::IfNotExists(_, _)));
        }
        other => panic!("got {other:?}"),
    }
}

#[test]
fn parse_set_case_insensitive_keywords() {
    let r = parse_update_expression("set a = :v").unwrap();
    assert_eq!(r.set.len(), 1);
    let r = parse_update_expression("Set a = :v").unwrap();
    assert_eq!(r.set.len(), 1);
    // Function names are also case-insensitive in DDB.
    let r = parse_update_expression("SET a = IF_NOT_EXISTS(a, :v)").unwrap();
    assert!(matches!(&r.set[0].value, RawSetRhs::IfNotExists(_, _)));
}

// ===== Paths — top-level, nested, list-indexed, mixed =======================

#[test]
fn parse_path_dotted() {
    let r = parse_update_expression("SET meta.score = :v").unwrap();
    assert_eq!(r.set[0].path, rpath(&[pn("meta"), pn("score")]));
}

#[test]
fn parse_path_indexed() {
    let r = parse_update_expression("SET items[3] = :v").unwrap();
    assert_eq!(r.set[0].path, rpath(&[pn("items"), pidx(3)]));
}

#[test]
fn parse_path_mixed_dotted_and_indexed() {
    let r = parse_update_expression("SET events[0].kind.label = :v").unwrap();
    assert_eq!(
        r.set[0].path,
        rpath(&[pn("events"), pidx(0), pn("kind"), pn("label")])
    );
}

#[test]
fn parse_path_with_name_placeholders() {
    let r = parse_update_expression("SET #u.#name = :v").unwrap();
    assert_eq!(r.set[0].path, rpath(&[pnref("u"), pnref("name")]));
}

#[test]
fn parse_path_top_level_predicate() {
    let r = parse_update_expression("SET a = :v").unwrap();
    let _ = &r.set[0].path; // raw path; predicate is on resolved Path below
    let names = BTreeMap::<String, String>::new();
    let mut values = BTreeMap::new();
    values.insert(":v".into(), AttributeValue::S("x".into()));
    let resolved = substitute_update(
        parse_update_expression("SET a = :v").unwrap(),
        &names,
        &values,
    )
    .unwrap();
    assert!(resolved.set[0].path.is_top_level());
    assert_eq!(resolved.set[0].path.top_name(), Some("a"));

    // And the dotted form is NOT top-level:
    let r2 = parse_update_expression("SET m.x = :v").unwrap();
    let r2 = substitute_update(r2, &names, &values).unwrap();
    assert!(!r2.set[0].path.is_top_level());
}

// ===== REMOVE clause ========================================================

#[test]
fn parse_remove_single() {
    let r = parse_update_expression("REMOVE deprecated_field").unwrap();
    assert_eq!(r.remove, vec![rpath(&[pn("deprecated_field")])]);
    assert!(r.set.is_empty());
}

#[test]
fn parse_remove_multiple() {
    let r = parse_update_expression("REMOVE a, b, c.d, e[2]").unwrap();
    assert_eq!(r.remove.len(), 4);
    assert_eq!(r.remove[2], rpath(&[pn("c"), pn("d")]));
    assert_eq!(r.remove[3], rpath(&[pn("e"), pidx(2)]));
}

// ===== ADD clause ===========================================================

#[test]
fn parse_add_numeric() {
    let r = parse_update_expression("ADD count :one").unwrap();
    assert_eq!(r.add.len(), 1);
    assert_eq!(r.add[0].path, rpath(&[pn("count")]));
    assert_eq!(r.add[0].value, vref("one"));
}

#[test]
fn parse_add_multiple() {
    let r = parse_update_expression("ADD a :x, b :y, c :z").unwrap();
    assert_eq!(r.add.len(), 3);
}

#[test]
fn parse_add_to_nested_path() {
    let r = parse_update_expression("ADD stats.daily_count :one").unwrap();
    assert_eq!(r.add[0].path, rpath(&[pn("stats"), pn("daily_count")]));
}

// ===== DELETE clause ========================================================

#[test]
fn parse_delete_from_set() {
    let r = parse_update_expression("DELETE tags :to_remove").unwrap();
    assert_eq!(r.delete.len(), 1);
    assert_eq!(r.delete[0].path, rpath(&[pn("tags")]));
    assert_eq!(r.delete[0].value, vref("to_remove"));
}

// ===== Multi-clause expressions (the realistic shape) =======================

#[test]
fn parse_set_and_remove() {
    let r = parse_update_expression("SET status = :s REMOVE old_field").unwrap();
    assert_eq!(r.set.len(), 1);
    assert_eq!(r.remove.len(), 1);
}

#[test]
fn parse_all_four_clauses() {
    let r = parse_update_expression(
        "SET status = :s, version = version + :one REMOVE old_field ADD count :one DELETE tags :remove_set",
    )
    .unwrap();
    assert_eq!(r.set.len(), 2);
    assert_eq!(r.remove.len(), 1);
    assert_eq!(r.add.len(), 1);
    assert_eq!(r.delete.len(), 1);
}

#[test]
fn parse_clauses_out_of_doc_order_works() {
    // DDB's docs show SET / REMOVE / ADD / DELETE in that order, but the
    // grammar accepts any ordering as long as each keyword appears once.
    let r = parse_update_expression("REMOVE a SET b = :v ADD c :n").unwrap();
    assert_eq!(r.remove.len(), 1);
    assert_eq!(r.set.len(), 1);
    assert_eq!(r.add.len(), 1);
}

// ===== Whitespace + formatting tolerance ====================================

#[test]
fn parse_whitespace_around_punctuation() {
    let r1 = parse_update_expression("SET a=:v").unwrap();
    let r2 = parse_update_expression("SET   a   =   :v").unwrap();
    let r3 = parse_update_expression("SET\ta\n=\n:v").unwrap();
    assert_eq!(r1.set[0], r2.set[0]);
    assert_eq!(r1.set[0], r3.set[0]);
}

#[test]
fn parse_whitespace_around_commas() {
    let r = parse_update_expression("SET a = :v   ,   b = :w").unwrap();
    assert_eq!(r.set.len(), 2);
}

// ===== Substitution: every AttributeValue variant ===========================

fn names_map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn values_map(pairs: &[(&str, AttributeValue)]) -> BTreeMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[test]
fn subst_set_with_s_value() {
    let raw = parse_update_expression("SET status = :v").unwrap();
    let v = values_map(&[(":v", AttributeValue::S("active".into()))]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert_eq!(av, &AttributeValue::S("active".into()));
}

#[test]
fn subst_set_with_n_value() {
    let raw = parse_update_expression("SET score = :v").unwrap();
    let v = values_map(&[(":v", AttributeValue::N("42.5".into()))]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert_eq!(av, &AttributeValue::N("42.5".into()));
}

#[test]
fn subst_set_with_b_value() {
    let raw = parse_update_expression("SET blob = :v").unwrap();
    let v = values_map(&[(":v", AttributeValue::B(Bytes::from_static(b"\x00\x01\x02")))]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert!(matches!(av, AttributeValue::B(_)));
}

#[test]
fn subst_set_with_bool_value() {
    for b in [true, false] {
        let raw = parse_update_expression("SET active = :v").unwrap();
        let v = values_map(&[(":v", AttributeValue::Bool(b))]);
        let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
        let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
            panic!()
        };
        assert_eq!(av, &AttributeValue::Bool(b));
    }
}

#[test]
fn subst_set_with_null_value() {
    let raw = parse_update_expression("SET deleted = :v").unwrap();
    let v = values_map(&[(":v", AttributeValue::Null)]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert_eq!(av, &AttributeValue::Null);
}

#[test]
fn subst_set_with_list_value() {
    let raw = parse_update_expression("SET items = :v").unwrap();
    let list = AttributeValue::L(vec![
        AttributeValue::S("a".into()),
        AttributeValue::N("1".into()),
        AttributeValue::Bool(true),
    ]);
    let v = values_map(&[(":v", list.clone())]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert_eq!(av, &list);
}

#[test]
fn subst_set_with_map_value() {
    use std::collections::BTreeMap as Btm;
    let raw = parse_update_expression("SET meta = :v").unwrap();
    let mut inner = Btm::new();
    inner.insert("vip".to_string(), AttributeValue::Bool(true));
    inner.insert("score".to_string(), AttributeValue::N("99".into()));
    let map_val = AttributeValue::M(inner);
    let v = values_map(&[(":v", map_val.clone())]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert_eq!(av, &map_val);
}

#[test]
fn subst_set_with_ss_value() {
    let raw = parse_update_expression("SET tags = :v").unwrap();
    let v = values_map(&[(":v", AttributeValue::Ss(vec!["a".into(), "b".into()]))]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert_eq!(av, &AttributeValue::Ss(vec!["a".into(), "b".into()]));
}

#[test]
fn subst_set_with_ns_value() {
    let raw = parse_update_expression("SET scores = :v").unwrap();
    let v = values_map(&[(":v", AttributeValue::Ns(vec!["1".into(), "2".into()]))]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert_eq!(av, &AttributeValue::Ns(vec!["1".into(), "2".into()]));
}

#[test]
fn subst_set_with_bs_value() {
    let raw = parse_update_expression("SET chunks = :v").unwrap();
    let bs = AttributeValue::Bs(vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")]);
    let v = values_map(&[(":v", bs.clone())]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    let SetRhs::Operand(Operand::Value(av)) = &r.set[0].value else {
        panic!()
    };
    assert_eq!(av, &bs);
}

// ===== Substitution: name placeholders ======================================

#[test]
fn subst_name_ref_in_path() {
    let raw = parse_update_expression("SET #u = :v").unwrap();
    let n = names_map(&[("#u", "name")]);
    let v = values_map(&[(":v", AttributeValue::S("alice".into()))]);
    let r = substitute_update(raw, &n, &v).unwrap();
    assert_eq!(
        r.set[0].path,
        Path {
            segments: vec![PathSegment::Name("name".into())]
        }
    );
}

#[test]
fn subst_name_ref_in_dotted_path() {
    let raw = parse_update_expression("SET meta.#prop = :v").unwrap();
    let n = names_map(&[("#prop", "score")]);
    let v = values_map(&[(":v", AttributeValue::N("99".into()))]);
    let r = substitute_update(raw, &n, &v).unwrap();
    assert_eq!(
        r.set[0].path.segments,
        vec![
            PathSegment::Name("meta".into()),
            PathSegment::Name("score".into())
        ]
    );
}

// ===== Substitution: ADD / DELETE values ====================================

#[test]
fn subst_add_resolves_value() {
    let raw = parse_update_expression("ADD count :one").unwrap();
    let v = values_map(&[(":one", AttributeValue::N("1".into()))]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    assert_eq!(r.add[0].value, AttributeValue::N("1".into()));
}

#[test]
fn subst_delete_resolves_value() {
    let raw = parse_update_expression("DELETE tags :rm").unwrap();
    let v = values_map(&[(":rm", AttributeValue::Ss(vec!["x".into()]))]);
    let r = substitute_update(raw, &BTreeMap::new(), &v).unwrap();
    assert_eq!(r.delete[0].value, AttributeValue::Ss(vec!["x".into()]));
}

// ===== Roundtrip: realistic compound expression =============================

#[test]
fn roundtrip_compound_expression() {
    let src = "SET #s = :status, version = version + :one, \
               updated_at = if_not_exists(updated_at, :now), \
               items = list_append(items, :new_items) \
               REMOVE deprecated_field \
               ADD count :one \
               DELETE tags :to_remove";
    let raw = parse_update_expression(src).unwrap();
    let n = names_map(&[("#s", "status")]);
    let v = values_map(&[
        (":status", AttributeValue::S("active".into())),
        (":one", AttributeValue::N("1".into())),
        (":now", AttributeValue::N("1700000000".into())),
        (
            ":new_items",
            AttributeValue::L(vec![AttributeValue::S("x".into())]),
        ),
        (":to_remove", AttributeValue::Ss(vec!["old".into()])),
    ]);
    let r = substitute_update(raw, &n, &v).unwrap();
    assert_eq!(r.set.len(), 4);
    assert_eq!(r.remove.len(), 1);
    assert_eq!(r.add.len(), 1);
    assert_eq!(r.delete.len(), 1);

    // Spot-check resolutions.
    assert_eq!(
        r.set[0].path.segments,
        vec![PathSegment::Name("status".into())]
    );
    assert_eq!(
        r.set[2].value,
        SetRhs::IfNotExists(
            Path {
                segments: vec![PathSegment::Name("updated_at".into())]
            },
            Box::new(SetRhs::Operand(Operand::Value(AttributeValue::N(
                "1700000000".into()
            )))),
        )
    );
}

// ===== Error cases ==========================================================

#[test]
fn err_empty_expression() {
    assert!(matches!(
        parse_update_expression(""),
        Err(ParseError::Empty { kind }) if kind == "UpdateExpression"
    ));
    assert!(matches!(
        parse_update_expression("   "),
        Err(ParseError::Empty { kind }) if kind == "UpdateExpression"
    ));
}

#[test]
fn err_invalid_syntax() {
    // Bare value with no `=` after path.
    let e = parse_update_expression("SET status :v");
    assert!(matches!(e, Err(ParseError::Invalid { .. })));
}

#[test]
fn err_bad_keyword() {
    let e = parse_update_expression("UPDATE a = :v");
    assert!(matches!(e, Err(ParseError::Invalid { .. })));
}

#[test]
fn err_duplicate_set_keyword() {
    // DDB requires actions of one kind under a single keyword.
    let e = parse_update_expression("SET a = :v SET b = :w");
    assert!(matches!(
        e,
        Err(ParseError::DuplicateKeyword { clause: "SET" })
    ));
}

#[test]
fn err_unknown_value_placeholder() {
    let raw = parse_update_expression("SET a = :v").unwrap();
    let e = substitute_update(raw, &BTreeMap::new(), &BTreeMap::new());
    assert!(matches!(
        e,
        Err(rekt_expressions::SubstituteError::UnknownValue { ref name })
            if name == "v"
    ));
}

#[test]
fn err_unknown_name_placeholder() {
    let raw = parse_update_expression("SET #x = :v").unwrap();
    let v = values_map(&[(":v", AttributeValue::S("alice".into()))]);
    let e = substitute_update(raw, &BTreeMap::new(), &v);
    assert!(matches!(
        e,
        Err(rekt_expressions::SubstituteError::UnknownName { ref name })
            if name == "x"
    ));
}

#[test]
fn err_add_with_path_not_value() {
    // ADD only accepts a value ref, not a path reference.
    let raw = parse_update_expression("ADD count count2").unwrap();
    let e = substitute_update(raw, &BTreeMap::new(), &BTreeMap::new());
    assert!(matches!(
        e,
        Err(rekt_expressions::SubstituteError::UnknownValue { .. })
    ));
}
