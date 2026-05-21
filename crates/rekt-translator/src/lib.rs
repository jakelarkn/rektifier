//! Translate DynamoDB operations into backend-neutral plans.
//!
//! Schemas may declare PK and SK with type S, N, or B; the translator
//! enforces that the incoming attribute matches the declared type and
//! parses the request's UpdateExpression / ConditionExpression /
//! ReturnValues fields into [`plan`] structs the storage layer consumes.
//!
//! ## Module layout
//!
//! - [`schema`] — [`TableSchema`] + the `TableShape` adapter.
//! - [`plan`] — per-op plan structs, [`ReturnValuesMode`], condition
//!   routing, materialize-output primitives, [`touched_paths`].
//! - [`error`] — [`TranslateError`] + the upstream-error `From` impls.
//! - [`translate`] — the four public `translate_*_item` entry points
//!   + shared `ReturnValues` and `ConditionExpression` resolvers.
//! - [`classify`] — [`classify_condition`] (AST → [`ConditionRouting`]).
//! - [`checks`] — path / operand / value-type validation helpers.
//! - [`materialize`] — pre-flatten a simple-subset `UpdateItemPlan`
//!   into storage-ready primitives.
//! - [`keys`] (private) — key extraction / extra-attr rejection.
//! - [`eval`] — in-Rust evaluators for the slow path
//!   ([`apply_update_expression`], [`evaluate_condition`]).

pub mod eval;
pub use eval::{apply_update_expression, evaluate_condition, EvalError};

mod checks;
pub mod classify;
pub mod error;
mod key_condition;
mod keys;
pub mod materialize;
mod paging;
pub mod plan;
mod projection;
pub mod schema;
pub mod translate;

pub use paging::encode_lek;

// Public re-exports so external callers can `use rekt_translator::X;`
// for any of the headline types without knowing the submodule layout.
pub use classify::classify_condition;
pub use error::TranslateError;
pub use materialize::{
    materialize_insert_only_update, materialize_simple_sql_update, materialize_simple_update,
};
pub use plan::{
    touched_paths, BatchGetItemPlan, BatchGetPerTable, BatchWriteItemPlan, BatchWritePerTable,
    ConditionPlan, ConditionRouting, DeleteItemPlan, GetItemPlan, PutItemPlan, QueryPlan,
    ReturnValuesMode, ScanPlan, SimpleSqlUpdatePrimitives, SimpleUpdatePrimitives,
    TransactGetItemsPlan, TransactGetPlanItem, TransactWriteItemsPlan, TransactWriteKind,
    TransactWritePlanItem, UpdateItemPlan,
};
pub use schema::{GsiSchema, LsiSchema, TableSchema};
pub use translate::{
    translate_batch_get_item, translate_batch_write_item, translate_delete_item,
    translate_get_item, translate_put_item, translate_query, translate_scan,
    translate_transact_get_items, translate_transact_write_items, translate_update_item,
};
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rekt_expressions::{Condition, Operand, SetRhs};
    use rekt_protocol::{
        AttributeValue, DeleteItemRequest, GetItemRequest, Item, PutItemRequest, QueryRequest,
        UpdateItemRequest,
    };
    use rekt_storage::{KeyType, KeyValue, SkCondition};
    use std::collections::BTreeMap;

    fn schema_hash_s() -> TableSchema {
        TableSchema {
            name: "users".into(),
            pg_table: "users".into(),
            pk_attr: "id".into(),
            pk_type: KeyType::S,
            sk_attr: None,
            sk_type: None,
            jsonb_col: "data".into(),
            lsis: Default::default(),
            gsis: Default::default(),
        }
    }

    fn schema_composite_s_n() -> TableSchema {
        TableSchema {
            name: "events".into(),
            pg_table: "events".into(),
            pk_attr: "device_id".into(),
            pk_type: KeyType::S,
            sk_attr: Some("ts".into()),
            sk_type: Some(KeyType::N),
            jsonb_col: "doc".into(),
            lsis: Default::default(),
            gsis: Default::default(),
        }
    }

    fn schema_hash_b() -> TableSchema {
        TableSchema {
            name: "blobs".into(),
            pg_table: "blobs".into(),
            pk_attr: "hash".into(),
            pk_type: KeyType::B,
            sk_attr: None,
            sk_type: None,
            jsonb_col: "meta".into(),
            lsis: Default::default(),
            gsis: Default::default(),
        }
    }

    fn schema_composite_s_s() -> TableSchema {
        TableSchema {
            name: "messages".into(),
            pg_table: "messages".into(),
            pk_attr: "thread".into(),
            pk_type: KeyType::S,
            sk_attr: Some("ts".into()),
            sk_type: Some(KeyType::S),
            jsonb_col: "data".into(),
            lsis: Default::default(),
            gsis: Default::default(),
        }
    }

    fn item_of(pairs: &[(&str, AttributeValue)]) -> Item {
        let mut m: BTreeMap<String, AttributeValue> = BTreeMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), v.clone());
        }
        m
    }

    #[test]
    fn put_hash_s_ok() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[
                ("id", AttributeValue::S("u1".into())),
                ("label", AttributeValue::S("alice".into())),
            ]),
            ..Default::default()
        };
        let plan = translate_put_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            plan.item_json,
            serde_json::json!({"id":{"S":"u1"},"label":{"S":"alice"}})
        );
    }

    #[test]
    fn put_composite_s_n_ok() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::N("1234567890".into())),
                ("val", AttributeValue::N("99".into())),
            ]),
            ..Default::default()
        };
        let plan = translate_put_item(&req, &schema_composite_s_n()).unwrap();
        // PutItemPlan no longer carries pk/sk; PG derives them from the JSONB.
        // Translator's job is still to validate the attrs are present and
        // typed correctly (this call would error if not).
        let stored = plan.item_json;
        assert_eq!(stored["device_id"], serde_json::json!({"S":"dev-1"}));
        assert_eq!(stored["ts"], serde_json::json!({"N":"1234567890"}));
    }

    #[test]
    fn put_hash_b_ok() {
        let req = PutItemRequest {
            table_name: "blobs".into(),
            item: item_of(&[
                ("hash", AttributeValue::B(Bytes::from_static(b"\x00\x01"))),
                ("extent", AttributeValue::N("2".into())),
            ]),
            ..Default::default()
        };
        let plan = translate_put_item(&req, &schema_hash_b()).unwrap();
        // base64 of {0x00, 0x01} is "AAE=".
        assert_eq!(plan.item_json["hash"], serde_json::json!({"B":"AAE="}));
    }

    #[test]
    fn put_pk_wrong_type() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("id", AttributeValue::N("42".into()))]),
            ..Default::default()
        };
        let err = translate_put_item(&req, &schema_hash_s()).unwrap_err();
        match err {
            TranslateError::PartitionKeyTypeMismatch {
                attr,
                expected,
                got,
            } => {
                assert_eq!(attr, "id");
                assert_eq!(expected, KeyType::S);
                assert_eq!(got, "N");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn put_sk_wrong_type() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::S("not a number".into())),
            ]),
            ..Default::default()
        };
        let err = translate_put_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::SortKeyTypeMismatch { .. }));
    }

    #[test]
    fn put_missing_pk() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("label", AttributeValue::S("alice".into()))]),
            ..Default::default()
        };
        let err = translate_put_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingPartitionKey { .. }));
    }

    #[test]
    fn put_composite_missing_sk() {
        let req = PutItemRequest {
            table_name: "events".into(),
            item: item_of(&[("device_id", AttributeValue::S("dev-1".into()))]),
            ..Default::default()
        };
        let err = translate_put_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingSortKey { .. }));
    }

    #[test]
    fn put_carries_condition_and_return_values() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[
                ("id", AttributeValue::S("u1".into())),
                ("label", AttributeValue::S("alice".into())),
            ]),
            condition_expression: Some("attribute_not_exists(id)".into()),
            return_values: Some("ALL_OLD".into()),
            ..Default::default()
        };
        let plan = translate_put_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("u1".into()));
        assert_eq!(plan.return_values, ReturnValuesMode::AllOld);
        let cond = plan.condition.expect("condition present");
        assert_eq!(cond.routing, ConditionRouting::InsertOnlyOnPk);
    }

    #[test]
    fn put_rejects_all_new_return_values() {
        let req = PutItemRequest {
            table_name: "users".into(),
            item: item_of(&[("id", AttributeValue::S("u1".into()))]),
            return_values: Some("ALL_NEW".into()),
            ..Default::default()
        };
        match translate_put_item(&req, &schema_hash_s()).unwrap_err() {
            TranslateError::UnsupportedReturnValuesForOp { op, got } => {
                assert_eq!(op, "PutItem");
                assert_eq!(got, "ALL_NEW");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn delete_carries_condition_and_return_values() {
        let req = DeleteItemRequest {
            table_name: "users".into(),
            key: item_of(&[("id", AttributeValue::S("u1".into()))]),
            condition_expression: Some("attribute_exists(id)".into()),
            return_values: Some("ALL_OLD".into()),
            ..Default::default()
        };
        let plan = translate_delete_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.return_values, ReturnValuesMode::AllOld);
        let cond = plan.condition.expect("condition present");
        assert_eq!(cond.routing, ConditionRouting::SimpleSql);
    }

    #[test]
    fn delete_rejects_updated_new_return_values() {
        let req = DeleteItemRequest {
            table_name: "users".into(),
            key: item_of(&[("id", AttributeValue::S("u1".into()))]),
            return_values: Some("UPDATED_NEW".into()),
            ..Default::default()
        };
        match translate_delete_item(&req, &schema_hash_s()).unwrap_err() {
            TranslateError::UnsupportedReturnValuesForOp { op, got } => {
                assert_eq!(op, "DeleteItem");
                assert_eq!(got, "UPDATED_NEW");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn get_composite_ok() {
        let req = GetItemRequest {
            table_name: "events".into(),
            key: item_of(&[
                ("device_id", AttributeValue::S("dev-1".into())),
                ("ts", AttributeValue::N("1234".into())),
            ]),
        };
        let plan = translate_get_item(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("dev-1".into()));
        assert_eq!(plan.sk, Some(KeyValue::N("1234".into())));
    }

    #[test]
    fn get_hash_ok() {
        let req = GetItemRequest {
            table_name: "users".into(),
            key: item_of(&[("id", AttributeValue::S("u1".into()))]),
        };
        let plan = translate_get_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("u1".into()));
        assert_eq!(plan.sk, None);
    }

    #[test]
    fn get_extra_key_attr_rejected() {
        let req = GetItemRequest {
            table_name: "users".into(),
            key: item_of(&[
                ("id", AttributeValue::S("u1".into())),
                ("label", AttributeValue::S("alice".into())),
            ]),
        };
        let err = translate_get_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn schema_shape_borrow() {
        let s = schema_composite_s_n();
        let shape = s.shape();
        assert_eq!(shape.table, "events");
        assert_eq!(shape.pk_col, "device_id");
        assert_eq!(shape.sk_col, Some("ts"));
        assert_eq!(shape.jsonb_col, "doc");
    }

    // ============================================================
    // UpdateItem translator tests
    // ============================================================

    fn upd(
        table: &str,
        key: &[(&str, AttributeValue)],
        expr: &str,
        values: &[(&str, AttributeValue)],
        names: &[(&str, &str)],
    ) -> UpdateItemRequest {
        UpdateItemRequest {
            table_name: table.into(),
            key: item_of(key),
            update_expression: expr.into(),
            expression_attribute_names: if names.is_empty() {
                None
            } else {
                Some(
                    names
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                )
            },
            expression_attribute_values: if values.is_empty() {
                None
            } else {
                Some(
                    values
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                )
            },
            condition_expression: None,
            return_values: None,
        }
    }

    fn set_literal_value(plan: &UpdateItemPlan, attr: &str) -> AttributeValue {
        let c = plan
            .expression
            .set
            .iter()
            .find(|c| c.path.top_name() == Some(attr))
            .unwrap_or_else(|| panic!("no SET clause for `{attr}`"));
        match &c.value {
            SetRhs::Operand(Operand::Value(v)) => v.clone(),
            other => panic!("expected literal SET RHS for `{attr}`, got {other:?}"),
        }
    }

    // ----- Happy paths: SET literal across every AttributeValue variant ----

    #[test]
    fn upd_set_string_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET label = :v",
            &[(":v", AttributeValue::S("alice".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("u1".into()));
        assert_eq!(
            set_literal_value(&plan, "label"),
            AttributeValue::S("alice".into())
        );
    }

    #[test]
    fn upd_set_number_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET score = :v",
            &[(":v", AttributeValue::N("42.5".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            set_literal_value(&plan, "score"),
            AttributeValue::N("42.5".into())
        );
    }

    #[test]
    fn upd_set_binary_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET binmark = :v",
            &[(":v", AttributeValue::B(Bytes::from_static(b"\x00\x01\x02")))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(matches!(
            set_literal_value(&plan, "binmark"),
            AttributeValue::B(_)
        ));
    }

    #[test]
    fn upd_set_bool_value() {
        for b in [true, false] {
            let req = upd(
                "users",
                &[("id", AttributeValue::S("u1".into()))],
                "SET active = :v",
                &[(":v", AttributeValue::Bool(b))],
                &[],
            );
            let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
            assert_eq!(set_literal_value(&plan, "active"), AttributeValue::Bool(b));
        }
    }

    #[test]
    fn upd_set_null_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET deleted = :v",
            &[(":v", AttributeValue::Null)],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "deleted"), AttributeValue::Null);
    }

    #[test]
    fn upd_set_list_value() {
        let list = AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::N("1".into()),
            AttributeValue::Bool(true),
        ]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET entries = :v",
            &[(":v", list.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "entries"), list);
    }

    #[test]
    fn upd_set_map_value() {
        let mut inner = BTreeMap::new();
        inner.insert("vip".into(), AttributeValue::Bool(true));
        inner.insert("score".into(), AttributeValue::N("99".into()));
        let map = AttributeValue::M(inner);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET meta = :v",
            &[(":v", map.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "meta"), map);
    }

    #[test]
    fn upd_set_string_set_value() {
        let ss = AttributeValue::Ss(vec!["a".into(), "b".into()]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET tags = :v",
            &[(":v", ss.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "tags"), ss);
    }

    #[test]
    fn upd_set_number_set_value() {
        let ns = AttributeValue::Ns(vec!["1".into(), "2".into()]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET scores = :v",
            &[(":v", ns.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "scores"), ns);
    }

    #[test]
    fn upd_set_binary_set_value() {
        let bs = AttributeValue::Bs(vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")]);
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET chunks = :v",
            &[(":v", bs.clone())],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(set_literal_value(&plan, "chunks"), bs);
    }

    // ----- Multi-clause & composite keys -----------------------------------

    #[test]
    fn upd_set_multiple_attributes() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :s, score = :n, active = :b",
            &[
                (":s", AttributeValue::S("active".into())),
                (":n", AttributeValue::N("42".into())),
                (":b", AttributeValue::Bool(true)),
            ],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set.len(), 3);
        assert_eq!(
            set_literal_value(&plan, "flag"),
            AttributeValue::S("active".into())
        );
        assert_eq!(
            set_literal_value(&plan, "score"),
            AttributeValue::N("42".into())
        );
        assert_eq!(
            set_literal_value(&plan, "active"),
            AttributeValue::Bool(true)
        );
    }

    #[test]
    fn upd_remove_single() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE deprecated_field",
            &[],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.remove.len(), 1);
        assert_eq!(
            plan.expression.remove[0].top_name(),
            Some("deprecated_field")
        );
    }

    #[test]
    fn upd_remove_multiple() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE a, b, c",
            &[],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.remove.len(), 3);
    }

    #[test]
    fn upd_set_and_remove_combined() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :s REMOVE deprecated_field",
            &[(":s", AttributeValue::S("active".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set.len(), 1);
        assert_eq!(plan.expression.remove.len(), 1);
    }

    #[test]
    fn upd_composite_key_passthrough() {
        let req = upd(
            "events",
            &[
                ("device_id", AttributeValue::S("d1".into())),
                ("ts", AttributeValue::N("1000".into())),
            ],
            "SET val = :v",
            &[(":v", AttributeValue::N("42".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("d1".into()));
        assert_eq!(plan.sk, Some(KeyValue::N("1000".into())));
    }

    #[test]
    fn upd_name_placeholder_substitutes() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET #s = :v",
            &[(":v", AttributeValue::S("ok".into()))],
            &[("#s", "flag")],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set[0].path.top_name(), Some("flag"));
    }

    // ----- Reject paths that touch key attributes --------------------------

    #[test]
    fn upd_reject_set_on_pk_attr() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET id = :v",
            &[(":v", AttributeValue::S("u2".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "id"));
    }

    #[test]
    fn upd_reject_remove_on_pk_attr() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE id",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "id"));
    }

    #[test]
    fn upd_reject_set_on_sk_attr() {
        let req = upd(
            "events",
            &[
                ("device_id", AttributeValue::S("d1".into())),
                ("ts", AttributeValue::N("1000".into())),
            ],
            "SET ts = :v",
            &[(":v", AttributeValue::N("2000".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "ts"));
    }

    // ----- Reject nested / indexed paths (deferred to Phase 8) ------------

    #[test]
    fn upd_reject_dotted_path() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET meta.score = :v",
            &[(":v", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedNestedPath { ref path } if path == "meta.score")
        );
    }

    #[test]
    fn upd_reject_indexed_path() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET entries[3] = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedNestedPath { ref path } if path == "entries[3]")
        );
    }

    // ----- Phase 3c: non-literal SET RHS shapes are now accepted -----
    //
    // The translator no longer rejects path-ref, arithmetic,
    // if_not_exists, or list_append. They route to the slow path (via
    // `!is_simple()`) at the server. Tests here just confirm acceptance
    // + the `is_simple()` predicate flips to false.

    #[test]
    fn upd_accept_set_path_ref() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET a = b",
            &[],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_accept_set_arithmetic() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET summed = subtotal + :tax",
            &[(":tax", AttributeValue::N("1".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_accept_if_not_exists() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET created_at = if_not_exists(created_at, :now)",
            &[(":now", AttributeValue::N("1700000000".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_accept_list_append() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET entries = list_append(entries, :new)",
            &[(":new", AttributeValue::L(vec![]))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_reject_nested_path_in_set_rhs() {
        // Nested paths in RHS are still rejected — they're Phase 8.
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET a = b.nested",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedNestedPath { .. }));
    }

    // ----- Phase 5/6: ADD and DELETE accepted (route to slow path) -----

    #[test]
    fn upd_accept_add_numeric() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD tally2 :one",
            &[(":one", AttributeValue::N("1".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.add.len(), 1);
        assert!(!plan.expression.is_simple());
    }

    #[test]
    fn upd_accept_add_set() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD tags :new",
            &[(":new", AttributeValue::Ss(vec!["a".into(), "b".into()]))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.add.len(), 1);
    }

    #[test]
    fn upd_reject_add_with_non_numeric_non_set_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD tally2 :one",
            &[(":one", AttributeValue::S("not_a_number".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::InvalidAddValueType { got } if got == "S"),
            "got: {err:?}"
        );
    }

    #[test]
    fn upd_reject_add_on_key_attr() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD id :one",
            &[(":one", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { ref attr } if attr == "id"));
    }

    #[test]
    fn upd_accept_delete_set() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "DELETE tags :rm",
            &[(":rm", AttributeValue::Ss(vec!["x".into()]))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.delete.len(), 1);
    }

    #[test]
    fn upd_reject_delete_with_non_set_value() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "DELETE tags :v",
            &[(":v", AttributeValue::S("not_a_set".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::InvalidDeleteValueType { got } if got == "S"),
            "got: {err:?}"
        );
    }

    // ----- ConditionExpression: parsed + classified (Phase 4b) -----------
    //
    // The translator now accepts ConditionExpression and produces a
    // `ConditionPlan` on `UpdateItemPlan.condition`. Storage execution is
    // wired in Phases 4c (insert-only), 4d (SQL WHERE), 4e (slow path);
    // until then, `materialize_simple_update` refuses conditional plans
    // with `UnsupportedConditionInFastPath`.

    fn upd_with_condition(
        cond: &str,
        values: &[(&str, AttributeValue)],
        names: &[(&str, &str)],
    ) -> UpdateItemRequest {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :s",
            // Merge `:s` placeholder with any caller-supplied ones.
            &{
                let mut v: Vec<(&str, AttributeValue)> =
                    vec![(":s", AttributeValue::S("active".into()))];
                v.extend(values.iter().cloned());
                v
            },
            names,
        );
        req.condition_expression = Some(cond.into());
        req
    }

    #[test]
    fn upd_condition_classified_as_insert_only_on_pk() {
        let req = upd_with_condition("attribute_not_exists(id)", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let cp = plan.condition.expect("condition should be present");
        assert_eq!(cp.routing, ConditionRouting::InsertOnlyOnPk);
        assert!(matches!(cp.condition, Condition::AttributeNotExists(_)));
    }

    #[test]
    fn upd_condition_attribute_not_exists_on_non_pk_is_simple_sql() {
        let req = upd_with_condition("attribute_not_exists(flag)", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            plan.condition.unwrap().routing,
            ConditionRouting::SimpleSql
        );
    }

    #[test]
    fn upd_condition_attribute_exists_on_pk_is_simple_sql() {
        // `attribute_exists(pk)` is "update-only" semantics — handled as
        // SimpleSql, not InsertOnlyOnPk (which is only for *not*-exists).
        let req = upd_with_condition("attribute_exists(id)", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(
            plan.condition.unwrap().routing,
            ConditionRouting::SimpleSql
        );
    }

    #[test]
    fn upd_condition_comparison_is_simple_sql() {
        let req = upd_with_condition(
            "version = :v",
            &[(":v", AttributeValue::N("3".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let cp = plan.condition.unwrap();
        assert_eq!(cp.routing, ConditionRouting::SimpleSql);
        assert!(matches!(cp.condition, Condition::Compare { .. }));
    }

    #[test]
    fn upd_condition_boolean_composition_is_simple_sql() {
        // Even when one term is `attribute_not_exists(pk)`, the routing is
        // SimpleSql once it's inside AND/OR — InsertOnlyOnPk requires the
        // condition to be *exactly* that one shape.
        let req = upd_with_condition(
            "attribute_not_exists(id) OR version = :v",
            &[(":v", AttributeValue::N("1".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let cp = plan.condition.unwrap();
        assert_eq!(cp.routing, ConditionRouting::SimpleSql);
        assert!(matches!(cp.condition, Condition::Or(_, _)));
    }

    #[test]
    fn upd_condition_resolves_name_and_value_placeholders() {
        let req = upd_with_condition(
            "#k = :want",
            &[(":want", AttributeValue::S("active".into()))],
            &[("#k", "flag")],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let cp = plan.condition.unwrap();
        match cp.condition {
            Condition::Compare { left, right, .. } => {
                assert!(matches!(
                    left,
                    Operand::Path(p) if p.top_name() == Some("flag")
                ));
                assert!(matches!(right, Operand::Value(AttributeValue::S(ref s)) if s == "active"));
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    #[test]
    fn upd_condition_empty_string_is_treated_as_absent() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.condition_expression = Some("".into());
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert!(plan.condition.is_none());
    }

    #[test]
    fn upd_condition_reject_nested_path() {
        let req = upd_with_condition("meta.score = :v", &[(":v", AttributeValue::N("1".into()))], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedConditionNestedPath { ref path } if path == "meta.score")
        );
    }

    #[test]
    fn upd_condition_reject_indexed_path() {
        let req = upd_with_condition("entries[0] = :v", &[(":v", AttributeValue::N("1".into()))], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionNestedPath { .. }
        ));
    }

    #[test]
    fn upd_condition_reject_in_attribute_exists() {
        let req = upd_with_condition("attribute_exists(meta.score)", &[], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionNestedPath { .. }
        ));
    }

    #[test]
    fn upd_condition_parse_error_surfaces_as_invalid_condition() {
        let req = upd_with_condition("a = ", &[], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::InvalidConditionExpression(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn upd_condition_unknown_placeholder_surfaces_clearly() {
        let req = upd_with_condition("a = :missing", &[], &[]);
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnknownPlaceholder(ref p) if p == ":missing"));
    }

    #[test]
    fn upd_condition_composite_key_pk_only_classified_correctly() {
        // For a composite-key table, InsertOnlyOnPk requires
        // `attribute_not_exists(pk_attr)` — the sk attr alone wouldn't
        // be enough to scope insert-only-on-the-row semantics.
        let req = {
            let mut r = upd(
                "events",
                &[
                    ("device_id", AttributeValue::S("d1".into())),
                    ("ts", AttributeValue::N("1000".into())),
                ],
                "SET v = :v",
                &[(":v", AttributeValue::N("1".into()))],
                &[],
            );
            r.condition_expression = Some("attribute_not_exists(device_id)".into());
            r
        };
        let plan = translate_update_item(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(
            plan.condition.unwrap().routing,
            ConditionRouting::InsertOnlyOnPk
        );
    }

    // ----- NeedsTx routing (Phase 4e fall-through) -----
    //
    // Ordering on path-vs-path, value-vs-value, or against
    // non-N/non-S RHS values can't compile to SQL without a typed
    // extraction strategy. Those shapes must reach the slow path.

    #[test]
    fn upd_condition_path_vs_path_ordering_is_needs_tx() {
        let req = upd_with_condition("a < b", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::NeedsTx);
    }

    #[test]
    fn upd_condition_ordering_against_bool_is_needs_tx() {
        let req = upd_with_condition("a > :b", &[(":b", AttributeValue::Bool(true))], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::NeedsTx);
    }

    #[test]
    fn upd_condition_ordering_against_binary_is_needs_tx() {
        let req = upd_with_condition(
            "a >= :b",
            &[(":b", AttributeValue::B(bytes::Bytes::from_static(b"hi")))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::NeedsTx);
    }

    #[test]
    fn upd_condition_ordering_against_n_value_is_simple_sql() {
        let req = upd_with_condition("a < :n", &[(":n", AttributeValue::N("5".into()))], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::SimpleSql);
    }

    #[test]
    fn upd_condition_ordering_against_s_value_is_simple_sql() {
        let req = upd_with_condition(
            "label <= :s",
            &[(":s", AttributeValue::S("z".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::SimpleSql);
    }

    #[test]
    fn upd_condition_equality_path_vs_path_is_simple_sql() {
        // Equality on any operand shape (JSONB equality handles it).
        let req = upd_with_condition("a = b", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::SimpleSql);
    }

    #[test]
    fn upd_condition_needs_tx_propagates_through_boolean_composition() {
        // One NeedsTx term inside an AND/OR → whole condition is NeedsTx.
        let req = upd_with_condition(
            "version = :v AND a < b",
            &[(":v", AttributeValue::N("1".into()))],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.condition.unwrap().routing, ConditionRouting::NeedsTx);
    }

    // ----- Phase 4b boundary: materialize refuses conditional plans -----

    #[test]
    fn materialize_refuses_conditional_plan_until_4c() {
        let req = upd_with_condition("attribute_not_exists(id)", &[], &[]);
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let err = materialize_simple_update(&plan, &req.key).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionInFastPath
        ));
    }

    #[test]
    fn upd_accept_explicit_return_values_none() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("NONE".into());
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.return_values, ReturnValuesMode::None);
    }

    #[test]
    fn upd_accept_return_values_all_new() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("ALL_NEW".into());
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.return_values, ReturnValuesMode::AllNew);
    }

    #[test]
    fn upd_accept_return_values_all_old() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("ALL_OLD".into());
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.return_values, ReturnValuesMode::AllOld);
    }

    #[test]
    fn upd_accept_return_values_updated_new() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("UPDATED_NEW".into());
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.return_values, ReturnValuesMode::UpdatedNew);
    }

    #[test]
    fn upd_accept_return_values_updated_old() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("UPDATED_OLD".into());
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.return_values, ReturnValuesMode::UpdatedOld);
    }

    #[test]
    fn upd_reject_return_values_bogus_value() {
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :v",
            &[(":v", AttributeValue::S("active".into()))],
            &[],
        );
        req.return_values = Some("BOGUS".into());
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::UnsupportedReturnValues { ref got } if got == "BOGUS")
        );
    }

    #[test]
    fn touched_paths_unions_all_clauses() {
        // SET a, REMOVE b, ADD c, DELETE d → {a, b, c, d}.
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET a = :v REMOVE b ADD c :n DELETE d :s",
            &[
                (":v", AttributeValue::S("x".into())),
                (":n", AttributeValue::N("1".into())),
                (":s", AttributeValue::Ss(vec!["e".into()])),
            ],
            &[],
        );
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        let paths = touched_paths(&plan.expression);
        assert_eq!(
            paths,
            ["a", "b", "c", "d"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
    }

    // ----- Parse / substitute errors propagate -----------------------------

    #[test]
    fn upd_propagates_parse_error() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = ",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidUpdateExpression(_)));
    }

    #[test]
    fn upd_propagates_unknown_value_placeholder() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET flag = :v",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnknownPlaceholder(ref p) if p == ":v"));
    }

    #[test]
    fn upd_propagates_unknown_name_placeholder() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET #x = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::UnknownPlaceholder(ref p) if p == "#x"));
    }

    // ----- Key validation reuses existing machinery -----------------------

    #[test]
    fn upd_reject_missing_key() {
        // Same as Get/Delete: missing pk -> MissingPartitionKey.
        let req = upd(
            "users",
            &[],
            "SET flag = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::MissingPartitionKey { .. }));
    }

    #[test]
    fn upd_reject_extra_key_attr() {
        let req = upd(
            "users",
            &[
                ("id", AttributeValue::S("u1".into())),
                ("rogue", AttributeValue::S("x".into())),
            ],
            "SET flag = :v",
            &[(":v", AttributeValue::S("x".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn upd_reject_empty_expression() {
        // Empty after parsing (the parser itself rejects this earlier; we
        // also have to handle "expression parsed but has no clauses we
        // support" — though in practice an empty post-parse expression is
        // unreachable for now since the parser requires at least one clause).
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        // Parser rejects empty string with ParseError::Empty, which we
        // wrap as InvalidUpdateExpression.
        assert!(matches!(err, TranslateError::InvalidUpdateExpression(_)));
    }

    // ----- Reserved-word rejection (edge cases) ---------------------------
    //
    // These tests verify rektifier rejects bare DDB-reserved attribute
    // names exactly like DDB does, AND that the alias mechanism
    // (`ExpressionAttributeNames`) bypasses the check. Main-path tests
    // above use non-reserved names (`label`, `flag`, `tally`, ...) so
    // they exercise translator logic rather than reservation rules. If
    // you add a new test that wants to use `name`/`status`/etc. as an
    // attribute, alias it via `#x` or pick a non-reserved name.

    #[test]
    fn upd_reject_reserved_bare_name_in_set() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET name = :v",
            &[(":v", AttributeValue::S("alice".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::ReservedWordInUpdateExpression { ref word } if word == "name"),
            "got: {err:?}"
        );
    }

    #[test]
    fn upd_reject_reserved_bare_name_in_remove() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "REMOVE status",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::ReservedWordInUpdateExpression { ref word } if word == "status"),
            "got: {err:?}"
        );
    }

    #[test]
    fn upd_reject_reserved_bare_name_in_add() {
        let req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "ADD counter :n",
            &[(":n", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::ReservedWordInUpdateExpression { ref word } if word == "counter"),
            "got: {err:?}"
        );
    }

    #[test]
    fn upd_reject_reserved_bare_name_in_condition() {
        let req = upd_with_condition(
            "attribute_exists(status)",
            &[],
            &[],
        );
        let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
        assert!(
            matches!(err, TranslateError::ReservedWordInConditionExpression { ref word } if word == "status"),
            "got: {err:?}"
        );
    }

    #[test]
    fn upd_aliased_reserved_name_is_accepted() {
        // The whole point of ExpressionAttributeNames — `#n → "name"`
        // bypasses the reservation check because the resolved name
        // never lands back in the expression's token stream.
        let mut req = upd(
            "users",
            &[("id", AttributeValue::S("u1".into()))],
            "SET #n = :v",
            &[(":v", AttributeValue::S("alice".into()))],
            &[],
        );
        req.expression_attribute_names =
            Some(BTreeMap::from([("#n".to_string(), "name".to_string())]));
        let plan = translate_update_item(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.expression.set[0].path.top_name(), Some("name"));
    }

    #[test]
    fn upd_reserved_word_match_is_case_insensitive() {
        // DDB's reserved-word check is case-insensitive — `STATUS`,
        // `status`, and `Status` are all rejected.
        for upper in ["NAME", "Name", "nAmE"] {
            let expr = format!("SET {upper} = :v");
            let req = upd(
                "users",
                &[("id", AttributeValue::S("u1".into()))],
                &expr,
                &[(":v", AttributeValue::S("alice".into()))],
                &[],
            );
            let err = translate_update_item(&req, &schema_hash_s()).unwrap_err();
            assert!(
                matches!(err, TranslateError::ReservedWordInUpdateExpression { .. }),
                "expected reservation rejection for `{upper}`, got: {err:?}"
            );
        }
    }

    // ============================================================
    // translate_query (Q1)
    // ============================================================

    fn query_req(
        table: &str,
        kce: &str,
        values: &[(&str, AttributeValue)],
        names: &[(&str, &str)],
    ) -> QueryRequest {
        QueryRequest {
            table_name: table.into(),
            key_condition_expression: kce.into(),
            expression_attribute_names: if names.is_empty() {
                None
            } else {
                Some(
                    names
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                )
            },
            expression_attribute_values: if values.is_empty() {
                None
            } else {
                Some(
                    values
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                )
            },
            consistent_read: None,
            index_name: None,
            limit: None,
            exclusive_start_key: None,
            filter_expression: None,
            scan_index_forward: None,
            select: None,
            projection_expression: None,
        }
    }

    #[test]
    fn query_pk_only_hash() {
        let req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("u1".into()));
        assert!(plan.sk_condition.is_none());
    }

    #[test]
    fn query_pk_only_on_composite() {
        // Composite table, pk-only KCE → full-partition scan.
        let req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        let plan = translate_query(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("dev-1".into()));
        assert!(plan.sk_condition.is_none());
    }

    #[test]
    fn query_pk_eq_and_sk_eq() {
        let req = query_req(
            "events",
            "device_id = :pk AND ts = :ts",
            &[
                (":pk", AttributeValue::S("dev-1".into())),
                (":ts", AttributeValue::N("1000".into())),
            ],
            &[],
        );
        let plan = translate_query(&req, &schema_composite_s_n()).unwrap();
        assert!(matches!(
            plan.sk_condition,
            Some(SkCondition::Eq(KeyValue::N(ref s))) if s == "1000"
        ));
    }

    #[test]
    fn query_sk_ordering_variants() {
        for (op, expected_disc) in [
            ("<", "Lt"),
            ("<=", "Le"),
            (">", "Gt"),
            (">=", "Ge"),
        ] {
            let kce = format!("device_id = :pk AND ts {op} :ts");
            let req = query_req(
                "events",
                &kce,
                &[
                    (":pk", AttributeValue::S("dev-1".into())),
                    (":ts", AttributeValue::N("1000".into())),
                ],
                &[],
            );
            let plan = translate_query(&req, &schema_composite_s_n()).unwrap();
            let got = match plan.sk_condition {
                Some(SkCondition::Lt(_)) => "Lt",
                Some(SkCondition::Le(_)) => "Le",
                Some(SkCondition::Gt(_)) => "Gt",
                Some(SkCondition::Ge(_)) => "Ge",
                other => panic!("unexpected sk_condition: {other:?}"),
            };
            assert_eq!(got, expected_disc, "op={op}");
        }
    }

    #[test]
    fn query_sk_between() {
        let req = query_req(
            "events",
            "device_id = :pk AND ts BETWEEN :lo AND :hi",
            &[
                (":pk", AttributeValue::S("dev-1".into())),
                (":lo", AttributeValue::N("1000".into())),
                (":hi", AttributeValue::N("2000".into())),
            ],
            &[],
        );
        let plan = translate_query(&req, &schema_composite_s_n()).unwrap();
        assert!(matches!(
            plan.sk_condition,
            Some(SkCondition::Between(KeyValue::N(ref a), KeyValue::N(ref b)))
                if a == "1000" && b == "2000"
        ));
    }

    #[test]
    fn query_sk_begins_with_s() {
        let req = query_req(
            "messages",
            "thread = :pk AND begins_with(ts, :pfx)",
            &[
                (":pk", AttributeValue::S("t-1".into())),
                (":pfx", AttributeValue::S("2026-".into())),
            ],
            &[],
        );
        let plan = translate_query(&req, &schema_composite_s_s()).unwrap();
        assert!(matches!(
            plan.sk_condition,
            Some(SkCondition::BeginsWithS(ref s)) if s == "2026-"
        ));
    }

    #[test]
    fn query_begins_with_on_n_sk_rejected() {
        let req = query_req(
            "events",
            "device_id = :pk AND begins_with(ts, :pfx)",
            &[
                (":pk", AttributeValue::S("dev-1".into())),
                // The placeholder is S even though the SK is N — D8
                // tests reject the begins_with itself, not the value.
                (":pfx", AttributeValue::S("123".into())),
            ],
            &[],
        );
        let err = translate_query(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionBeginsWithRequiresStringSk { ref attr, .. }
                if attr == "ts"
        ));
    }

    #[test]
    fn query_empty_kce_rejected() {
        let req = query_req("users", "", &[], &[]);
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionMissingPkEquality { ref attr } if attr == "id"
        ));
    }

    #[test]
    fn query_missing_pk_equality() {
        // Composite table, KCE only references SK.
        let req = query_req(
            "events",
            "ts = :ts",
            &[(":ts", AttributeValue::N("1".into()))],
            &[],
        );
        let err = translate_query(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionMissingPkEquality { ref attr } if attr == "device_id"
        ));
    }

    #[test]
    fn query_pk_ordering_rejected() {
        let req = query_req(
            "users",
            "id < :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionPkMustBeEquality { ref attr, op: "<" } if attr == "id"
        ));
    }

    #[test]
    fn query_or_rejected() {
        let req = query_req(
            "users",
            "id = :a OR id = :b",
            &[
                (":a", AttributeValue::S("u1".into())),
                (":b", AttributeValue::S("u2".into())),
            ],
            &[],
        );
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionUnsupportedShape { .. }
        ));
    }

    #[test]
    fn query_attribute_exists_rejected() {
        let req = query_req("users", "attribute_exists(id)", &[], &[]);
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionUnsupportedShape { .. }
        ));
    }

    #[test]
    fn query_non_key_attr_rejected() {
        let req = query_req(
            "users",
            "label = :v",
            &[(":v", AttributeValue::S("alice".into()))],
            &[],
        );
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionNonKeyAttr { ref attr } if attr == "label"
        ));
    }

    #[test]
    fn query_pk_type_mismatch() {
        let req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::N("42".into()))],
            &[],
        );
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionPkTypeMismatch {
                ref attr, expected: KeyType::S, got: "N"
            } if attr == "id"
        ));
    }

    #[test]
    fn query_placeholders_substitute() {
        // `#t = :pk AND #r begins_with :pfx` style: both name and value
        // aliases resolve before validation.
        let req = query_req(
            "messages",
            "#t = :pk AND begins_with(#r, :pfx)",
            &[
                (":pk", AttributeValue::S("t-1".into())),
                (":pfx", AttributeValue::S("2026-".into())),
            ],
            &[("#t", "thread"), ("#r", "ts")],
        );
        let plan = translate_query(&req, &schema_composite_s_s()).unwrap();
        assert_eq!(plan.pk, KeyValue::S("t-1".into()));
        assert!(matches!(
            plan.sk_condition,
            Some(SkCondition::BeginsWithS(ref s)) if s == "2026-"
        ));
    }

    // ============================================================
    // translate_query Q2 — Limit + ExclusiveStartKey
    // ============================================================

    #[test]
    fn query_limit_carried_into_plan() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.limit = Some(50);
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.limit, Some(50));
    }

    #[test]
    fn query_limit_zero_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.limit = Some(0);
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidLimit { got: 0 }));
    }

    #[test]
    fn query_limit_above_cap_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.limit = Some(1001);
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidLimit { got: 1001 }));
    }

    #[test]
    fn query_esk_carries_sk_for_composite() {
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        req.exclusive_start_key = Some(item_of(&[
            ("device_id", AttributeValue::S("dev-1".into())),
            ("ts", AttributeValue::N("2000".into())),
        ]));
        let plan = translate_query(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.esk_sk, Some(KeyValue::N("2000".into())));
    }

    #[test]
    fn query_esk_hash_only_decodes_no_sk() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.exclusive_start_key =
            Some(item_of(&[("id", AttributeValue::S("u1".into()))]));
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert!(plan.esk_sk.is_none());
    }

    #[test]
    fn query_esk_missing_pk_rejected() {
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        req.exclusive_start_key =
            Some(item_of(&[("ts", AttributeValue::N("2000".into()))]));
        let err = translate_query(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::InvalidExclusiveStartKey { .. }
        ));
    }

    #[test]
    fn query_esk_pk_mismatch_rejected() {
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        req.exclusive_start_key = Some(item_of(&[
            ("device_id", AttributeValue::S("dev-OTHER".into())),
            ("ts", AttributeValue::N("2000".into())),
        ]));
        let err = translate_query(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::ExclusiveStartKeyPkMismatch { .. }
        ));
    }

    #[test]
    fn query_esk_extra_attr_rejected() {
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        req.exclusive_start_key = Some(item_of(&[
            ("device_id", AttributeValue::S("dev-1".into())),
            ("ts", AttributeValue::N("2000".into())),
            ("rogue", AttributeValue::S("nope".into())),
        ]));
        let err = translate_query(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::InvalidExclusiveStartKey { .. }
        ));
    }

    #[test]
    fn query_esk_wrong_sk_type_rejected() {
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        // device_events schema expects ts: N, give it S.
        req.exclusive_start_key = Some(item_of(&[
            ("device_id", AttributeValue::S("dev-1".into())),
            ("ts", AttributeValue::S("not-a-number".into())),
        ]));
        let err = translate_query(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::InvalidExclusiveStartKey { .. }
        ));
    }

    #[test]
    fn query_encode_lek_round_trip() {
        // encode_lek -> decode (via translate_query) should preserve the
        // sk component for composite tables.
        let pk = KeyValue::S("dev-1".into());
        let sk = KeyValue::N("2000".into());
        let lek = encode_lek(&pk, Some(&sk), &schema_composite_s_n());
        // Use the LEK as the next ESK.
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        req.exclusive_start_key = Some(lek);
        let plan = translate_query(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.esk_sk, Some(KeyValue::N("2000".into())));
    }

    // ============================================================
    // translate_query Q3 — FilterExpression
    // ============================================================

    #[test]
    fn query_filter_parses_into_plan() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[
                (":pk", AttributeValue::S("u1".into())),
                (":want", AttributeValue::S("active".into())),
            ],
            &[],
        );
        req.filter_expression = Some("flag = :want".into());
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        let fp = plan.filter.expect("filter present");
        assert!(matches!(fp.condition, Condition::Compare { .. }));
    }

    #[test]
    fn query_filter_absent_leaves_none() {
        let req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert!(plan.filter.is_none());
    }

    #[test]
    fn query_filter_blank_treated_as_absent() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.filter_expression = Some("".into());
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert!(plan.filter.is_none());
    }

    #[test]
    fn query_filter_nested_path_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[
                (":pk", AttributeValue::S("u1".into())),
                (":v", AttributeValue::S("x".into())),
            ],
            &[],
        );
        req.filter_expression = Some("meta.score = :v".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionNestedPath { .. }
        ));
    }

    #[test]
    fn query_filter_parse_error_surfaces() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.filter_expression = Some("flag =".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::InvalidConditionExpression(_)
        ));
    }

    #[test]
    fn query_filter_unknown_placeholder_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.filter_expression = Some("flag = :missing".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnknownPlaceholder(ref p) if p == ":missing"
        ));
    }

    #[test]
    fn query_filter_aliased_reserved_word_accepted() {
        // FilterExpression goes through the same reserved-word check as
        // ConditionExpression on Put/Delete/Update — aliased names
        // bypass it.
        let mut req = query_req(
            "users",
            "id = :pk",
            &[
                (":pk", AttributeValue::S("u1".into())),
                (":v", AttributeValue::S("alice".into())),
            ],
            &[("#n", "name")],
        );
        req.filter_expression = Some("#n = :v".into());
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert!(plan.filter.is_some());
    }

    #[test]
    fn query_filter_reserved_bare_word_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[
                (":pk", AttributeValue::S("u1".into())),
                (":v", AttributeValue::S("alice".into())),
            ],
            &[],
        );
        req.filter_expression = Some("name = :v".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::ReservedWordInConditionExpression { ref word } if word == "name"
        ));
    }

    // ============================================================
    // translate_scan (Q4)
    // ============================================================

    fn scan_req(table: &str) -> rekt_protocol::ScanRequest {
        rekt_protocol::ScanRequest {
            table_name: table.into(),
            ..Default::default()
        }
    }

    #[test]
    fn scan_basic_accepts() {
        translate_scan(&scan_req("users"), &schema_hash_s()).unwrap();
        translate_scan(&scan_req("events"), &schema_composite_s_n()).unwrap();
    }

    /// PLAN-11 L4: unknown IndexName on a table with no LSIs maps to
    /// IndexNotFound (→ RNF at the wire layer), not the legacy
    /// "feature not supported" ValidationException.
    #[test]
    fn scan_unknown_index_returns_index_not_found() {
        let mut req = scan_req("users");
        req.index_name = Some("an_index".into());
        let err = translate_scan(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::IndexNotFound { index } if index == "an_index"
        ));
    }

    #[test]
    fn scan_rejects_segment() {
        let mut req = scan_req("users");
        req.segment = Some(0);
        req.total_segments = Some(4);
        let err = translate_scan(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::ScanFeatureNotSupported { what }
                if what.contains("parallel scan")
        ));
    }

    /// PLAN-11 L4: LSI Query routes through the LSI's sort column.
    /// extract_key_condition resolves the KCE's sort-key term against
    /// the substituted SK, and the plan carries an index_sort the
    /// dispatcher uses to swap the storage shape.
    #[test]
    fn query_with_index_name_resolves_to_lsi() {
        let mut schema = schema_composite_s_n();
        schema.lsis.insert(
            "by_tier".into(),
            LsiSchema {
                name: "by_tier".into(),
                sort_attr: "tier".into(),
                sort_type: KeyType::S,
                sort_pg_col: "tier".into(),
                serveable: true,
                unserveable_reason: None,
            },
        );
        let mut req = query_req(
            "events",
            "device_id = :pk AND tier = :sk",
            &[
                (":pk", AttributeValue::S("dev-1".into())),
                (":sk", AttributeValue::S("gold".into())),
            ],
            &[],
        );
        req.index_name = Some("by_tier".into());
        let plan = translate_query(&req, &schema).unwrap();
        let idx = plan.index_sort.expect("index_sort populated");
        assert_eq!(idx.index_name, "by_tier");
        let sort = idx.sort.as_ref().expect("LSI must have sort");
        assert_eq!(sort.pg_col, "tier");
        assert_eq!(sort.key_type, KeyType::S);
        assert!(idx.partition_override.is_none(), "LSI shares base PK");
        // The plan's SK condition reflects the LSI's column, not the
        // base table's ts.
        match plan.sk_condition {
            Some(SkCondition::Eq(KeyValue::S(s))) => assert_eq!(s, "gold"),
            other => panic!("unexpected sk_condition: {other:?}"),
        }
    }

    /// PLAN-11 L4: KCE that pairs the base PK with the *base* SK while
    /// IndexName targets an LSI is a misuse — DDB rejects with a
    /// KeyCondition error because the LSI's SK is `tier`, not `ts`.
    #[test]
    fn query_lsi_kce_must_reference_lsi_sort_attr() {
        let mut schema = schema_composite_s_n();
        schema.lsis.insert(
            "by_tier".into(),
            LsiSchema {
                name: "by_tier".into(),
                sort_attr: "tier".into(),
                sort_type: KeyType::S,
                sort_pg_col: "tier".into(),
                serveable: true,
                unserveable_reason: None,
            },
        );
        let mut req = query_req(
            "events",
            "device_id = :pk AND ts = :sk",
            &[
                (":pk", AttributeValue::S("dev-1".into())),
                (":sk", AttributeValue::N("100".into())),
            ],
            &[],
        );
        req.index_name = Some("by_tier".into());
        let err = translate_query(&req, &schema).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::KeyConditionNonKeyAttr { attr } if attr == "ts"
        ));
    }

    #[test]
    fn query_with_unknown_index_name_returns_index_not_found() {
        let schema = schema_composite_s_n();
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        req.index_name = Some("by_phantom".into());
        let err = translate_query(&req, &schema).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::IndexNotFound { index } if index == "by_phantom"
        ));
    }

    /// PLAN-11 D12 / open Q4: an LSI with serveable=false (reconciler-
    /// detected drift) is rejected at translate time as IndexNotServeable.
    /// Maps to RNF at the wire layer to match DDB's "non-ACTIVE indexes
    /// aren't queryable" stance.
    #[test]
    fn query_unserveable_lsi_returns_index_not_serveable() {
        let mut schema = schema_composite_s_n();
        schema.lsis.insert(
            "by_tier".into(),
            LsiSchema {
                name: "by_tier".into(),
                sort_attr: "tier".into(),
                sort_type: KeyType::S,
                sort_pg_col: "tier".into(),
                serveable: false,
                unserveable_reason: Some("LSI index missing".into()),
            },
        );
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("dev-1".into()))],
            &[],
        );
        req.index_name = Some("by_tier".into());
        let err = translate_query(&req, &schema).unwrap_err();
        match err {
            TranslateError::IndexNotServeable { index, reason } => {
                assert_eq!(index, "by_tier");
                assert!(reason.contains("missing"));
            }
            other => panic!("expected IndexNotServeable, got {other:?}"),
        }
    }

    #[test]
    fn scan_rejects_segment_alone() {
        // Even with just Segment (no TotalSegments) we reject; DDB
        // requires both, but rektifier rejects either as a clear
        // unsupported-feature signal.
        let mut req = scan_req("users");
        req.segment = Some(0);
        let err = translate_scan(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::ScanFeatureNotSupported { .. }
        ));
    }

    // ============================================================
    // Q5 — Scan Limit/ESK/Filter + Query ScanIndexForward
    // ============================================================

    #[test]
    fn scan_carries_limit_into_plan() {
        let mut req = scan_req("users");
        req.limit = Some(50);
        let plan = translate_scan(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.limit, Some(50));
    }

    #[test]
    fn scan_invalid_limit_rejected() {
        let mut req = scan_req("users");
        req.limit = Some(1001);
        let err = translate_scan(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidLimit { got: 1001 }));
    }

    #[test]
    fn scan_esk_carries_pk_and_sk() {
        let mut req = scan_req("events");
        req.exclusive_start_key = Some(item_of(&[
            ("device_id", AttributeValue::S("dev-X".into())),
            ("ts", AttributeValue::N("100".into())),
        ]));
        let plan = translate_scan(&req, &schema_composite_s_n()).unwrap();
        assert_eq!(plan.esk_pk, Some(KeyValue::S("dev-X".into())));
        assert_eq!(plan.esk_sk, Some(KeyValue::N("100".into())));
    }

    #[test]
    fn scan_esk_hash_only() {
        let mut req = scan_req("users");
        req.exclusive_start_key =
            Some(item_of(&[("id", AttributeValue::S("u1".into()))]));
        let plan = translate_scan(&req, &schema_hash_s()).unwrap();
        assert_eq!(plan.esk_pk, Some(KeyValue::S("u1".into())));
        assert!(plan.esk_sk.is_none());
    }

    #[test]
    fn scan_esk_extra_attr_rejected() {
        let mut req = scan_req("events");
        req.exclusive_start_key = Some(item_of(&[
            ("device_id", AttributeValue::S("d".into())),
            ("ts", AttributeValue::N("1".into())),
            ("rogue", AttributeValue::S("x".into())),
        ]));
        let err = translate_scan(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::InvalidExclusiveStartKey { .. }
        ));
    }

    #[test]
    fn scan_esk_wrong_sk_type_rejected() {
        let mut req = scan_req("events");
        req.exclusive_start_key = Some(item_of(&[
            ("device_id", AttributeValue::S("d".into())),
            ("ts", AttributeValue::S("not-a-num".into())),
        ]));
        let err = translate_scan(&req, &schema_composite_s_n()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::InvalidExclusiveStartKey { .. }
        ));
    }

    #[test]
    fn scan_filter_parses_into_plan() {
        let mut req = scan_req("users");
        req.filter_expression = Some("flag = :want".into());
        req.expression_attribute_values = Some(BTreeMap::from([(
            ":want".to_string(),
            AttributeValue::S("active".into()),
        )]));
        let plan = translate_scan(&req, &schema_hash_s()).unwrap();
        assert!(plan.filter.is_some());
    }

    #[test]
    fn scan_filter_nested_path_rejected() {
        let mut req = scan_req("users");
        req.filter_expression = Some("meta.score = :v".into());
        req.expression_attribute_values = Some(BTreeMap::from([(
            ":v".to_string(),
            AttributeValue::N("1".into()),
        )]));
        let err = translate_scan(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedConditionNestedPath { .. }
        ));
    }

    #[test]
    fn query_forward_defaults_true() {
        let req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert!(plan.forward);
    }

    #[test]
    fn query_scan_index_forward_false_carries() {
        let mut req = query_req(
            "events",
            "device_id = :pk",
            &[(":pk", AttributeValue::S("d".into()))],
            &[],
        );
        req.scan_index_forward = Some(false);
        let plan = translate_query(&req, &schema_composite_s_n()).unwrap();
        assert!(!plan.forward);
    }

    // ============================================================
    // Q6 — Select + ProjectionExpression
    // ============================================================

    #[test]
    fn query_select_count_carries() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.select = Some("COUNT".into());
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert!(plan.select_count_only);
        assert!(plan.projection.is_none());
    }

    #[test]
    fn query_select_all_attributes_is_noop() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.select = Some("ALL_ATTRIBUTES".into());
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        assert!(!plan.select_count_only);
        assert!(plan.projection.is_none());
    }

    #[test]
    fn query_select_all_projected_attributes_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.select = Some("ALL_PROJECTED_ATTRIBUTES".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidSelectMode { .. }));
    }

    #[test]
    fn query_select_bogus_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.select = Some("BOGUS".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnsupportedSelect { ref got } if got == "BOGUS"
        ));
    }

    #[test]
    fn query_projection_parses_into_plan() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.projection_expression = Some("label, flag".into());
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        let proj = plan.projection.expect("projection set");
        assert!(proj.contains("label"));
        assert!(proj.contains("flag"));
        assert_eq!(proj.len(), 2);
    }

    #[test]
    fn query_projection_alias_substitutes() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[("#n", "name")],
        );
        req.projection_expression = Some("#n, label".into());
        let plan = translate_query(&req, &schema_hash_s()).unwrap();
        let proj = plan.projection.expect("projection set");
        // `#n` resolves to the reserved word "name" — aliases bypass
        // the reserved-word check just like in Update/Condition exprs.
        assert!(proj.contains("name"));
        assert!(proj.contains("label"));
    }

    #[test]
    fn query_projection_bare_reserved_word_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.projection_expression = Some("name, label".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::ReservedWordInProjectionExpression { ref word } if word == "name"
        ));
    }

    #[test]
    fn query_projection_nested_path_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.projection_expression = Some("meta.score".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::ProjectionNestedPathUnsupported { .. }
        ));
    }

    #[test]
    fn query_projection_indexed_path_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.projection_expression = Some("entries[0]".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::ProjectionNestedPathUnsupported { .. }
        ));
    }

    #[test]
    fn query_projection_unknown_alias_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.projection_expression = Some("#missing".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::UnknownPlaceholder(ref p) if p == "#missing"
        ));
    }

    #[test]
    fn query_select_count_with_projection_rejected() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.select = Some("COUNT".into());
        req.projection_expression = Some("label".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidSelectMode { .. }));
    }

    #[test]
    fn query_select_specific_attributes_requires_projection() {
        let mut req = query_req(
            "users",
            "id = :pk",
            &[(":pk", AttributeValue::S("u1".into()))],
            &[],
        );
        req.select = Some("SPECIFIC_ATTRIBUTES".into());
        let err = translate_query(&req, &schema_hash_s()).unwrap_err();
        assert!(matches!(err, TranslateError::InvalidSelectMode { .. }));
    }

    #[test]
    fn scan_select_count_carries() {
        let mut req = scan_req("users");
        req.select = Some("COUNT".into());
        let plan = translate_scan(&req, &schema_hash_s()).unwrap();
        assert!(plan.select_count_only);
    }

    #[test]
    fn scan_projection_parses_into_plan() {
        let mut req = scan_req("users");
        req.projection_expression = Some("label, flag, tier".into());
        let plan = translate_scan(&req, &schema_hash_s()).unwrap();
        let proj = plan.projection.expect("projection set");
        assert_eq!(proj.len(), 3);
    }

    // ===== BatchGetItem (B1) =================================================

    fn schemas_map() -> std::collections::HashMap<String, TableSchema> {
        let mut m = std::collections::HashMap::new();
        for s in [
            schema_hash_s(),
            schema_composite_s_n(),
            schema_hash_b(),
            schema_composite_s_s(),
        ] {
            m.insert(s.name.clone(), s);
        }
        m
    }

    fn bg_req(items: Vec<(&str, Vec<Item>)>) -> rekt_protocol::BatchGetItemRequest {
        let mut ri = std::collections::HashMap::new();
        for (table, keys) in items {
            ri.insert(
                table.to_string(),
                rekt_protocol::KeysAndAttributes {
                    keys,
                    ..Default::default()
                },
            );
        }
        rekt_protocol::BatchGetItemRequest {
            request_items: ri,
            return_consumed_capacity: None,
        }
    }

    #[test]
    fn batch_get_hash_table_ok() {
        let req = bg_req(vec![(
            "users",
            vec![
                item_of(&[("id", AttributeValue::S("a".into()))]),
                item_of(&[("id", AttributeValue::S("b".into()))]),
            ],
        )]);
        let plan = translate_batch_get_item(&req, &schemas_map(), 100).unwrap();
        assert_eq!(plan.per_table.len(), 1);
        assert_eq!(plan.per_table[0].keys.len(), 2);
        assert_eq!(plan.per_table[0].keys[0].0, KeyValue::S("a".into()));
        assert!(plan.per_table[0].keys[0].1.is_none());
    }

    #[test]
    fn batch_get_composite_table_extracts_sk() {
        let req = bg_req(vec![(
            "events",
            vec![
                item_of(&[
                    ("device_id", AttributeValue::S("d".into())),
                    ("ts", AttributeValue::N("1".into())),
                ]),
                item_of(&[
                    ("device_id", AttributeValue::S("d".into())),
                    ("ts", AttributeValue::N("2".into())),
                ]),
            ],
        )]);
        let plan = translate_batch_get_item(&req, &schemas_map(), 100).unwrap();
        let keys = &plan.per_table[0].keys;
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].1, Some(KeyValue::N("1".into())));
        assert_eq!(keys[1].1, Some(KeyValue::N("2".into())));
    }

    #[test]
    fn batch_get_empty_request_items_rejected() {
        let req = bg_req(vec![]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::EmptyBatchGetRequest));
    }

    #[test]
    fn batch_get_empty_keys_array_rejected() {
        let req = bg_req(vec![("users", vec![])]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::EmptyKeysInBatchGet { .. }));
    }

    #[test]
    fn batch_get_unknown_table_rejected() {
        let req = bg_req(vec![(
            "nope",
            vec![item_of(&[("id", AttributeValue::S("x".into()))])],
        )]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::ResourceNotFoundForBatch { .. }));
    }

    #[test]
    fn batch_get_dup_keys_rejected() {
        let req = bg_req(vec![(
            "users",
            vec![
                item_of(&[("id", AttributeValue::S("x".into()))]),
                item_of(&[("id", AttributeValue::S("x".into()))]),
            ],
        )]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::DuplicateKeyInBatch));
    }

    #[test]
    fn batch_get_dup_composite_keys_rejected() {
        // Two entries with identical (pk, sk).
        let same = || {
            item_of(&[
                ("device_id", AttributeValue::S("d".into())),
                ("ts", AttributeValue::N("5".into())),
            ])
        };
        let req = bg_req(vec![("events", vec![same(), same()])]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::DuplicateKeyInBatch));
    }

    #[test]
    fn batch_get_total_cap_summed_across_tables() {
        // 60 keys in users + 60 in events = 120 > 100. Sum-not-per-table.
        let mut users_keys = Vec::with_capacity(60);
        for n in 0..60 {
            users_keys.push(item_of(&[(
                "id",
                AttributeValue::S(format!("u{n}")),
            )]));
        }
        let mut event_keys = Vec::with_capacity(60);
        for n in 0..60 {
            event_keys.push(item_of(&[
                ("device_id", AttributeValue::S(format!("d{n}"))),
                ("ts", AttributeValue::N("1".into())),
            ]));
        }
        let req = bg_req(vec![("users", users_keys), ("events", event_keys)]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        match err {
            TranslateError::TooManyKeysInBatchGet { got, max } => {
                assert_eq!(got, 120);
                assert_eq!(max, 100);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn batch_get_custom_max_keys_lower() {
        // With max=5, asking for 6 should be rejected.
        let keys: Vec<Item> = (0..6)
            .map(|n| item_of(&[("id", AttributeValue::S(format!("u{n}")))]))
            .collect();
        let req = bg_req(vec![("users", keys)]);
        let err = translate_batch_get_item(&req, &schemas_map(), 5).unwrap_err();
        assert!(matches!(err, TranslateError::TooManyKeysInBatchGet { got: 6, max: 5 }));
    }

    #[test]
    fn batch_get_wrong_key_type_rejected() {
        // `users.id` is S; pass N.
        let req = bg_req(vec![(
            "users",
            vec![item_of(&[("id", AttributeValue::N("42".into()))])],
        )]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::PartitionKeyTypeMismatch { .. }
        ));
    }

    #[test]
    fn batch_get_extra_attr_rejected() {
        let req = bg_req(vec![(
            "users",
            vec![item_of(&[
                ("id", AttributeValue::S("x".into())),
                ("extra", AttributeValue::S("oops".into())),
            ])],
        )]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn batch_get_attributes_to_get_rejected() {
        let mut req = bg_req(vec![(
            "users",
            vec![item_of(&[("id", AttributeValue::S("x".into()))])],
        )]);
        req.request_items
            .get_mut("users")
            .unwrap()
            .attributes_to_get = Some(vec!["label".into()]);
        let err = translate_batch_get_item(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::AttributesToGetNotSupported));
    }

    #[test]
    fn batch_get_binary_pk_table_ok() {
        let req = bg_req(vec![(
            "blobs",
            vec![item_of(&[(
                "hash",
                AttributeValue::B(Bytes::from_static(b"abc")),
            )])],
        )]);
        let plan = translate_batch_get_item(&req, &schemas_map(), 100).unwrap();
        assert_eq!(plan.per_table[0].keys.len(), 1);
        match &plan.per_table[0].keys[0].0 {
            KeyValue::B(b) => assert_eq!(b.as_ref(), b"abc"),
            other => panic!("expected B key, got {other:?}"),
        }
    }

    // ===== BatchWriteItem (B4–B6) ============================================

    fn put_req(item: Item) -> rekt_protocol::WriteRequest {
        rekt_protocol::WriteRequest {
            put_request: Some(rekt_protocol::PutRequest { item }),
            delete_request: None,
        }
    }

    fn del_req(key: Item) -> rekt_protocol::WriteRequest {
        rekt_protocol::WriteRequest {
            put_request: None,
            delete_request: Some(rekt_protocol::DeleteRequest { key }),
        }
    }

    fn bw_req(
        items: Vec<(&str, Vec<rekt_protocol::WriteRequest>)>,
    ) -> rekt_protocol::BatchWriteItemRequest {
        let mut ri = std::collections::HashMap::new();
        for (table, writes) in items {
            ri.insert(table.to_string(), writes);
        }
        rekt_protocol::BatchWriteItemRequest {
            request_items: ri,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        }
    }

    #[test]
    fn batch_write_put_only_ok() {
        let req = bw_req(vec![(
            "users",
            vec![
                put_req(item_of(&[("id", AttributeValue::S("a".into()))])),
                put_req(item_of(&[("id", AttributeValue::S("b".into()))])),
            ],
        )]);
        let plan = translate_batch_write_item(&req, &schemas_map(), 25).unwrap();
        assert_eq!(plan.per_table[0].ops.len(), 2);
        for op in &plan.per_table[0].ops {
            assert!(matches!(op, rekt_storage::WriteOp::Put { .. }));
        }
    }

    #[test]
    fn batch_write_delete_only_ok() {
        let req = bw_req(vec![(
            "users",
            vec![del_req(item_of(&[("id", AttributeValue::S("a".into()))]))],
        )]);
        let plan = translate_batch_write_item(&req, &schemas_map(), 25).unwrap();
        assert!(matches!(
            plan.per_table[0].ops[0],
            rekt_storage::WriteOp::Delete { .. }
        ));
    }

    #[test]
    fn batch_write_mixed_put_delete_ok() {
        let req = bw_req(vec![(
            "users",
            vec![
                put_req(item_of(&[("id", AttributeValue::S("p".into()))])),
                del_req(item_of(&[("id", AttributeValue::S("d".into()))])),
            ],
        )]);
        let plan = translate_batch_write_item(&req, &schemas_map(), 25).unwrap();
        assert_eq!(plan.per_table[0].ops.len(), 2);
    }

    #[test]
    fn batch_write_composite_put_extracts_sk() {
        let req = bw_req(vec![(
            "events",
            vec![put_req(item_of(&[
                ("device_id", AttributeValue::S("d".into())),
                ("ts", AttributeValue::N("1".into())),
            ]))],
        )]);
        let plan = translate_batch_write_item(&req, &schemas_map(), 25).unwrap();
        match &plan.per_table[0].ops[0] {
            rekt_storage::WriteOp::Put { pk, sk, .. } => {
                assert_eq!(*pk, KeyValue::S("d".into()));
                assert_eq!(*sk, Some(KeyValue::N("1".into())));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn batch_write_empty_request_items_rejected() {
        let req = bw_req(vec![]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::EmptyBatchWriteRequest));
    }

    #[test]
    fn batch_write_empty_per_table_vec_rejected() {
        let req = bw_req(vec![("users", vec![])]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::EmptyWritesInBatch { .. }));
    }

    #[test]
    fn batch_write_unknown_table_rejected() {
        let req = bw_req(vec![("nope", vec![put_req(item_of(&[
            ("id", AttributeValue::S("x".into())),
        ]))])]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::ResourceNotFoundForBatch { .. }));
    }

    #[test]
    fn batch_write_dup_put_keys_rejected() {
        let req = bw_req(vec![(
            "users",
            vec![
                put_req(item_of(&[("id", AttributeValue::S("x".into()))])),
                put_req(item_of(&[("id", AttributeValue::S("x".into()))])),
            ],
        )]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::DuplicateKeyInBatch));
    }

    #[test]
    fn batch_write_put_plus_delete_same_key_rejected() {
        // Per-table HashSet spans Put + Delete combined.
        let req = bw_req(vec![(
            "users",
            vec![
                put_req(item_of(&[("id", AttributeValue::S("x".into()))])),
                del_req(item_of(&[("id", AttributeValue::S("x".into()))])),
            ],
        )]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::DuplicateKeyInBatch));
    }

    #[test]
    fn batch_write_malformed_neither_set_rejected() {
        let req = bw_req(vec![(
            "users",
            vec![rekt_protocol::WriteRequest {
                put_request: None,
                delete_request: None,
            }],
        )]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::MalformedWriteRequest));
    }

    #[test]
    fn batch_write_malformed_both_set_rejected() {
        let req = bw_req(vec![(
            "users",
            vec![rekt_protocol::WriteRequest {
                put_request: Some(rekt_protocol::PutRequest {
                    item: item_of(&[("id", AttributeValue::S("a".into()))]),
                }),
                delete_request: Some(rekt_protocol::DeleteRequest {
                    key: item_of(&[("id", AttributeValue::S("b".into()))]),
                }),
            }],
        )]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::MalformedWriteRequest));
    }

    #[test]
    fn batch_write_total_cap_summed_across_tables() {
        let mut us = Vec::new();
        let mut es = Vec::new();
        for n in 0..13 {
            us.push(put_req(item_of(&[
                ("id", AttributeValue::S(format!("u{n}"))),
            ])));
            es.push(put_req(item_of(&[
                ("device_id", AttributeValue::S(format!("d{n}"))),
                ("ts", AttributeValue::N("1".into())),
            ])));
        }
        let req = bw_req(vec![("users", us), ("events", es)]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        match err {
            TranslateError::TooManyWritesInBatch { got, max } => {
                assert_eq!(got, 26);
                assert_eq!(max, 25);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn batch_write_put_missing_pk_attr_rejected() {
        let req = bw_req(vec![(
            "users",
            vec![put_req(item_of(&[
                ("label", AttributeValue::S("no-pk".into())),
            ]))],
        )]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::MissingPartitionKey { .. }));
    }

    #[test]
    fn batch_write_put_wrong_pk_type_rejected() {
        let req = bw_req(vec![(
            "users",
            vec![put_req(item_of(&[
                ("id", AttributeValue::N("42".into())),
            ]))],
        )]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::PartitionKeyTypeMismatch { .. }
        ));
    }

    #[test]
    fn batch_write_delete_extra_attr_in_key_rejected() {
        let req = bw_req(vec![(
            "users",
            vec![del_req(item_of(&[
                ("id", AttributeValue::S("x".into())),
                ("extra", AttributeValue::S("oops".into())),
            ]))],
        )]);
        let err = translate_batch_write_item(&req, &schemas_map(), 25).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn batch_write_custom_max_writes_lower() {
        let req = bw_req(vec![(
            "users",
            (0..6)
                .map(|n| {
                    put_req(item_of(&[(
                        "id",
                        AttributeValue::S(format!("u{n}")),
                    )]))
                })
                .collect(),
        )]);
        let err = translate_batch_write_item(&req, &schemas_map(), 5).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::TooManyWritesInBatch { got: 6, max: 5 }
        ));
    }

    // ===== TransactGetItems (T1) =============================================

    fn tg_get(table: &str, key: Item) -> rekt_protocol::TransactGetItem {
        rekt_protocol::TransactGetItem {
            get: Some(rekt_protocol::TransactGet {
                table_name: table.into(),
                key,
                projection_expression: None,
                expression_attribute_names: None,
            }),
        }
    }

    fn tg_req(items: Vec<rekt_protocol::TransactGetItem>) -> rekt_protocol::TransactGetItemsRequest {
        rekt_protocol::TransactGetItemsRequest {
            transact_items: items,
            return_consumed_capacity: None,
        }
    }

    #[test]
    fn transact_get_hash_table_ok() {
        let req = tg_req(vec![
            tg_get("users", item_of(&[("id", AttributeValue::S("a".into()))])),
            tg_get("users", item_of(&[("id", AttributeValue::S("b".into()))])),
        ]);
        let plan = translate_transact_get_items(&req, &schemas_map(), 100).unwrap();
        assert_eq!(plan.items.len(), 2);
        assert_eq!(plan.items[0].table_name, "users");
        assert_eq!(plan.items[0].pk, KeyValue::S("a".into()));
        assert!(plan.items[0].sk.is_none());
        assert_eq!(plan.items[1].pk, KeyValue::S("b".into()));
    }

    #[test]
    fn transact_get_composite_table_extracts_sk() {
        let req = tg_req(vec![tg_get(
            "events",
            item_of(&[
                ("device_id", AttributeValue::S("d".into())),
                ("ts", AttributeValue::N("42".into())),
            ]),
        )]);
        let plan = translate_transact_get_items(&req, &schemas_map(), 100).unwrap();
        assert_eq!(plan.items[0].sk, Some(KeyValue::N("42".into())));
    }

    #[test]
    fn transact_get_empty_request_rejected() {
        let req = tg_req(vec![]);
        let err = translate_transact_get_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::EmptyTransactRequest));
    }

    #[test]
    fn transact_get_too_many_items_rejected() {
        let items: Vec<_> = (0..6)
            .map(|n| tg_get("users", item_of(&[("id", AttributeValue::S(format!("u{n}")))])))
            .collect();
        let req = tg_req(items);
        let err = translate_transact_get_items(&req, &schemas_map(), 5).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::TooManyTransactItems { got: 6, max: 5 }
        ));
    }

    #[test]
    fn transact_get_unknown_table_rejected() {
        let req = tg_req(vec![tg_get(
            "nope",
            item_of(&[("id", AttributeValue::S("x".into()))]),
        )]);
        let err = translate_transact_get_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::ResourceNotFoundForTransact { .. }));
    }

    #[test]
    fn transact_get_malformed_item_no_get_set_rejected() {
        let req = tg_req(vec![rekt_protocol::TransactGetItem { get: None }]);
        let err = translate_transact_get_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::MalformedTransactItem { .. }));
    }

    #[test]
    fn transact_get_duplicate_target_within_table_rejected() {
        let same = || item_of(&[("id", AttributeValue::S("x".into()))]);
        let req = tg_req(vec![tg_get("users", same()), tg_get("users", same())]);
        let err = translate_transact_get_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::DuplicateTransactTarget));
    }

    #[test]
    fn transact_get_same_key_across_tables_allowed() {
        // D6: dedupe key includes table name. Same pk in different tables OK.
        let req = tg_req(vec![
            tg_get("users", item_of(&[("id", AttributeValue::S("k".into()))])),
            tg_get(
                "blobs",
                item_of(&[("hash", AttributeValue::B(bytes::Bytes::from_static(b"k")))]),
            ),
        ]);
        let plan = translate_transact_get_items(&req, &schemas_map(), 100).unwrap();
        assert_eq!(plan.items.len(), 2);
    }

    #[test]
    fn transact_get_extra_key_attr_rejected() {
        let req = tg_req(vec![tg_get(
            "users",
            item_of(&[
                ("id", AttributeValue::S("a".into())),
                ("extra", AttributeValue::S("nope".into())),
            ]),
        )]);
        let err = translate_transact_get_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::ExtraKeyAttribute { .. }));
    }

    #[test]
    fn transact_get_wrong_pk_type_rejected() {
        let req = tg_req(vec![tg_get(
            "users",
            item_of(&[("id", AttributeValue::N("42".into()))]),
        )]);
        let err = translate_transact_get_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::PartitionKeyTypeMismatch { .. }));
    }

    #[test]
    fn transact_get_position_order_preserved() {
        // D8: response order tracks request order. Plan must preserve it.
        let req = tg_req(vec![
            tg_get("users", item_of(&[("id", AttributeValue::S("third".into()))])),
            tg_get("users", item_of(&[("id", AttributeValue::S("first".into()))])),
            tg_get("users", item_of(&[("id", AttributeValue::S("second".into()))])),
        ]);
        let plan = translate_transact_get_items(&req, &schemas_map(), 100).unwrap();
        assert_eq!(plan.items[0].pk, KeyValue::S("third".into()));
        assert_eq!(plan.items[1].pk, KeyValue::S("first".into()));
        assert_eq!(plan.items[2].pk, KeyValue::S("second".into()));
    }

    #[test]
    fn transact_get_projection_parses_into_plan() {
        let mut item = tg_get("users", item_of(&[("id", AttributeValue::S("a".into()))]));
        item.get.as_mut().unwrap().projection_expression = Some("id, label".into());
        let req = tg_req(vec![item]);
        let plan = translate_transact_get_items(&req, &schemas_map(), 100).unwrap();
        let proj = plan.items[0].projection.as_ref().expect("projection set");
        assert!(proj.contains("id"));
        assert!(proj.contains("label"));
    }

    // ===== TransactWriteItems (T3) ============================================

    fn tw_put(table: &str, item: Item) -> rekt_protocol::TransactWriteItem {
        rekt_protocol::TransactWriteItem {
            put: Some(rekt_protocol::TransactPut {
                table_name: table.into(),
                item,
                ..Default::default()
            }),
            delete: None,
            update: None,
            condition_check: None,
        }
    }

    fn tw_req(items: Vec<rekt_protocol::TransactWriteItem>) -> rekt_protocol::TransactWriteItemsRequest {
        rekt_protocol::TransactWriteItemsRequest {
            transact_items: items,
            ..Default::default()
        }
    }

    #[test]
    fn transact_write_put_only_ok() {
        let req = tw_req(vec![
            tw_put("users", item_of(&[("id", AttributeValue::S("a".into()))])),
            tw_put("users", item_of(&[("id", AttributeValue::S("b".into()))])),
        ]);
        let plan = translate_transact_write_items(&req, &schemas_map(), 100).unwrap();
        assert_eq!(plan.items.len(), 2);
        assert_eq!(plan.items[0].table_name, "users");
        assert_eq!(plan.items[0].pk, KeyValue::S("a".into()));
        assert!(matches!(plan.items[0].kind, TransactWriteKind::Put { .. }));
        assert!(plan.items[0].condition.is_none());
    }

    #[test]
    fn transact_write_put_with_condition_parses() {
        let mut item =
            tw_put("users", item_of(&[("id", AttributeValue::S("a".into()))]));
        item.put.as_mut().unwrap().condition_expression =
            Some("attribute_not_exists(id)".into());
        let req = tw_req(vec![item]);
        let plan = translate_transact_write_items(&req, &schemas_map(), 100).unwrap();
        assert!(plan.items[0].condition.is_some());
    }

    #[test]
    fn transact_write_empty_request_rejected() {
        let req = tw_req(vec![]);
        let err = translate_transact_write_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::EmptyTransactRequest));
    }

    #[test]
    fn transact_write_too_many_items_rejected() {
        let items: Vec<_> = (0..6)
            .map(|n| tw_put("users", item_of(&[("id", AttributeValue::S(format!("u{n}")))])))
            .collect();
        let req = tw_req(items);
        let err = translate_transact_write_items(&req, &schemas_map(), 5).unwrap_err();
        assert!(matches!(
            err,
            TranslateError::TooManyTransactItems { got: 6, max: 5 }
        ));
    }

    #[test]
    fn transact_write_unknown_table_rejected() {
        let req = tw_req(vec![tw_put(
            "nope",
            item_of(&[("id", AttributeValue::S("a".into()))]),
        )]);
        let err = translate_transact_write_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::ResourceNotFoundForTransact { .. }));
    }

    #[test]
    fn transact_write_duplicate_target_rejected() {
        let same = || item_of(&[("id", AttributeValue::S("dup".into()))]);
        let req = tw_req(vec![tw_put("users", same()), tw_put("users", same())]);
        let err = translate_transact_write_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::DuplicateTransactTarget));
    }

    #[test]
    fn transact_write_no_inner_set_rejected() {
        let req = tw_req(vec![rekt_protocol::TransactWriteItem::default()]);
        let err = translate_transact_write_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::MalformedTransactItem { .. }));
    }

    #[test]
    fn transact_write_delete_ok() {
        let req = tw_req(vec![rekt_protocol::TransactWriteItem {
            delete: Some(rekt_protocol::TransactDelete {
                table_name: "users".into(),
                key: item_of(&[("id", AttributeValue::S("a".into()))]),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        let plan = translate_transact_write_items(&req, &schemas_map(), 100).unwrap();
        assert!(matches!(plan.items[0].kind, TransactWriteKind::Delete));
    }

    #[test]
    fn transact_write_condition_check_ok() {
        let req = tw_req(vec![rekt_protocol::TransactWriteItem {
            condition_check: Some(rekt_protocol::TransactConditionCheck {
                table_name: "users".into(),
                key: item_of(&[("id", AttributeValue::S("a".into()))]),
                condition_expression: "attribute_exists(id)".into(),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        let plan = translate_transact_write_items(&req, &schemas_map(), 100).unwrap();
        assert!(matches!(
            plan.items[0].kind,
            TransactWriteKind::ConditionCheck
        ));
        assert!(plan.items[0].condition.is_some());
    }

    #[test]
    fn transact_write_update_ok() {
        let mut values = BTreeMap::new();
        values.insert(":v".into(), AttributeValue::S("new".into()));
        let req = tw_req(vec![rekt_protocol::TransactWriteItem {
            update: Some(rekt_protocol::TransactUpdate {
                table_name: "users".into(),
                key: item_of(&[("id", AttributeValue::S("a".into()))]),
                update_expression: "SET label = :v".into(),
                expression_attribute_values: Some(values),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        let plan = translate_transact_write_items(&req, &schemas_map(), 100).unwrap();
        assert!(matches!(plan.items[0].kind, TransactWriteKind::Update { .. }));
    }

    #[test]
    fn transact_write_update_empty_expression_rejected() {
        let req = tw_req(vec![rekt_protocol::TransactWriteItem {
            update: Some(rekt_protocol::TransactUpdate {
                table_name: "users".into(),
                key: item_of(&[("id", AttributeValue::S("a".into()))]),
                update_expression: "SET".into(),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        let err = translate_transact_write_items(&req, &schemas_map(), 100).unwrap_err();
        // Either parse error or EmptyUpdateExpression — anything but Ok.
        assert!(
            matches!(err, TranslateError::EmptyUpdateExpression)
                || matches!(err, TranslateError::InvalidUpdateExpression(_))
        );
    }

    #[test]
    fn transact_write_update_touches_key_rejected() {
        let mut values = BTreeMap::new();
        values.insert(":v".into(), AttributeValue::S("new".into()));
        let req = tw_req(vec![rekt_protocol::TransactWriteItem {
            update: Some(rekt_protocol::TransactUpdate {
                table_name: "users".into(),
                key: item_of(&[("id", AttributeValue::S("a".into()))]),
                update_expression: "SET id = :v".into(),
                expression_attribute_values: Some(values),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        let err = translate_transact_write_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::UpdateTouchesKey { .. }));
    }

    #[test]
    fn transact_write_return_values_unsupported_rejected() {
        let mut item =
            tw_put("users", item_of(&[("id", AttributeValue::S("a".into()))]));
        item.put.as_mut().unwrap().return_values_on_condition_check_failure =
            Some("BOGUS".into());
        let req = tw_req(vec![item]);
        let err = translate_transact_write_items(&req, &schemas_map(), 100).unwrap_err();
        assert!(matches!(err, TranslateError::UnsupportedReturnValuesForOp { .. }));
    }

    #[test]
    fn transact_write_return_values_all_old_accepted() {
        let mut item =
            tw_put("users", item_of(&[("id", AttributeValue::S("a".into()))]));
        item.put.as_mut().unwrap().return_values_on_condition_check_failure =
            Some("ALL_OLD".into());
        let req = tw_req(vec![item]);
        let plan = translate_transact_write_items(&req, &schemas_map(), 100).unwrap();
        assert!(plan.items[0].return_old_on_failure);
    }

    #[test]
    fn transact_write_client_request_token_too_long_rejected() {
        let mut req = tw_req(vec![tw_put(
            "users",
            item_of(&[("id", AttributeValue::S("a".into()))]),
        )]);
        req.client_request_token = Some("x".repeat(37));
        let err = translate_transact_write_items(&req, &schemas_map(), 100).unwrap_err();
        // Lands as InvalidConditionExpression in v1 (no dedicated variant).
        assert!(matches!(
            err,
            TranslateError::InvalidConditionExpression(_)
        ));
    }

    #[test]
    fn transact_write_position_preserved() {
        let req = tw_req(vec![
            tw_put("users", item_of(&[("id", AttributeValue::S("third".into()))])),
            tw_put("users", item_of(&[("id", AttributeValue::S("first".into()))])),
            tw_put("users", item_of(&[("id", AttributeValue::S("second".into()))])),
        ]);
        let plan = translate_transact_write_items(&req, &schemas_map(), 100).unwrap();
        assert_eq!(plan.items[0].pk, KeyValue::S("third".into()));
        assert_eq!(plan.items[1].pk, KeyValue::S("first".into()));
        assert_eq!(plan.items[2].pk, KeyValue::S("second".into()));
    }
}
