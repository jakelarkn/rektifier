//! Differential tests: send the same PutItem + GetItem pair to both
//! dynamodb-local (`:8000`) and rektifier (`:9000`); assert the responses
//! are semantically equal (`serde_json::Value` equality, order-insensitive
//! for objects).
//!
//! Setup:
//! 1. `just up` (docker compose for PG + dynamodb-local).
//! 2. `just bootstrap-pg` (creates the `users` and `device_events` PG tables).
//! 3. Start rektifier: `REKTIFIER_CONFIG=rektifier.toml.example cargo run --bin rektifier`.
//! 4. Run: `cargo test -p rekt-diff-tests -- --ignored`.
//!
//! The tests use a fixed list of items spanning every DDB AttributeValue
//! variant (S, N, B, BOOL, NULL, L, M, SS, NS, BS) plus a composite-key
//! case. Each test uses a unique PK so they're idempotent regardless of
//! whatever else is in the tables.

use serde_json::{json, Value};
use std::process::Command;

const REF: &str = "http://localhost:8000"; // dynamodb-local
const OURS: &str = "http://localhost:9000"; // rektifier

// ===== Shell-out helpers ======================================================

fn aws(endpoint: &str, args: &[&str]) -> Value {
    let output = Command::new("aws")
        .arg("--endpoint-url")
        .arg(endpoint)
        .args(args)
        .env("AWS_ACCESS_KEY_ID", "local")
        .env("AWS_SECRET_ACCESS_KEY", "local")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        // Force the AWS CLI to emit JSON (some shells override via env).
        .arg("--output")
        .arg("json")
        .output()
        .unwrap_or_else(|e| panic!("aws CLI invocation failed: {e}"));

    if !output.status.success() {
        panic!(
            "aws {} args={:?} failed:\nstdout: {}\nstderr: {}",
            endpoint,
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        // PutItem with ReturnValues=NONE (the default) prints nothing.
        json!({})
    } else {
        serde_json::from_str(trimmed)
            .unwrap_or_else(|e| panic!("aws output not JSON: {e}\nstdout: {stdout}"))
    }
}

/// Best-effort: try `DeleteItem` against both endpoints so each test starts
/// from a clean state. Failures are swallowed (the row may not exist).
fn delete_both(table: &str, key: &str) {
    let _ = Command::new("aws")
        .args(["--endpoint-url", REF, "dynamodb", "delete-item"])
        .args(["--table-name", table, "--key", key])
        .env("AWS_ACCESS_KEY_ID", "local")
        .env("AWS_SECRET_ACCESS_KEY", "local")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .output();
    let _ = Command::new("aws")
        .args(["--endpoint-url", OURS, "dynamodb", "delete-item"])
        .args(["--table-name", table, "--key", key])
        .env("AWS_ACCESS_KEY_ID", "local")
        .env("AWS_SECRET_ACCESS_KEY", "local")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .output();
}

/// Ensure a DDB-local table exists with the given schema. Idempotent.
fn ensure_ref_table(table: &str, attrs: &[(&str, &str)], key_schema: &[(&str, &str)]) {
    let attr_defs: Vec<String> = attrs
        .iter()
        .map(|(n, t)| format!("AttributeName={n},AttributeType={t}"))
        .collect();
    let key_def: Vec<String> = key_schema
        .iter()
        .map(|(n, t)| format!("AttributeName={n},KeyType={t}"))
        .collect();

    let mut args: Vec<&str> = vec!["dynamodb", "create-table", "--table-name", table];
    args.push("--attribute-definitions");
    for s in &attr_defs {
        args.push(s);
    }
    args.push("--key-schema");
    for s in &key_def {
        args.push(s);
    }
    args.push("--billing-mode");
    args.push("PAY_PER_REQUEST");

    // We don't use `aws()` here because a "table already exists" error is
    // success for our purposes — we just want it to be there.
    let _ = Command::new("aws")
        .arg("--endpoint-url")
        .arg(REF)
        .args(&args)
        .env("AWS_ACCESS_KEY_ID", "local")
        .env("AWS_SECRET_ACCESS_KEY", "local")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .output();
}

// ===== Core diff primitive ====================================================

/// Run a put-then-get round trip against both endpoints with the same item.
/// Asserts:
/// 1. Both PutItem responses are equal (both `{}` in MVP).
/// 2. Both GetItem responses are equal (object-equality via `serde_json::Value`).
fn assert_round_trip_matches(table: &str, item: &str, key: &str) {
    delete_both(table, key);

    let put_ref = aws(
        REF,
        &[
            "dynamodb",
            "put-item",
            "--table-name",
            table,
            "--item",
            item,
        ],
    );
    let put_ours = aws(
        OURS,
        &[
            "dynamodb",
            "put-item",
            "--table-name",
            table,
            "--item",
            item,
        ],
    );
    assert_eq!(
        put_ref, put_ours,
        "PutItem responses diverged for {table} / item={item}"
    );

    let mut get_ref = aws(
        REF,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    let mut get_ours = aws(
        OURS,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    // Sets are unordered by spec; normalize before comparing.
    sort_sets(&mut get_ref);
    sort_sets(&mut get_ours);
    assert_eq!(
        get_ref, get_ours,
        "GetItem responses diverged for {table} / key={key}\nref:  {get_ref}\nours: {get_ours}"
    );
}

/// Walk a DDB-JSON `Value` and sort the contents of every `SS`/`NS`/`BS`
/// set in place. DDB sets are unordered — comparing across implementations
/// requires canonicalizing the array order. Set elements are always
/// strings on the wire (numbers as decimal strings; binaries as base64),
/// so a lexical sort is unambiguous and identical across both endpoints.
fn sort_sets(v: &mut Value) {
    match v {
        Value::Object(map) => {
            if map.len() == 1 {
                for set_key in ["SS", "NS", "BS"] {
                    if let Some(Value::Array(arr)) = map.get_mut(set_key) {
                        arr.sort_by(|a, b| match (a.as_str(), b.as_str()) {
                            (Some(a), Some(b)) => a.cmp(b),
                            _ => std::cmp::Ordering::Equal,
                        });
                    }
                }
            }
            for (_, child) in map.iter_mut() {
                sort_sets(child);
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                sort_sets(child);
            }
        }
        _ => {}
    }
}

fn ensure_users_table() {
    ensure_ref_table("users", &[("id", "S")], &[("id", "HASH")]);
}

fn ensure_device_events_table() {
    ensure_ref_table(
        "device_events",
        &[("device_id", "S"), ("ts", "N")],
        &[("device_id", "HASH"), ("ts", "RANGE")],
    );
}

// ===== Test cases =============================================================

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_s_only() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_s"},"name":{"S":"alice"}}"#,
        r#"{"id":{"S":"diff_s"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_high_precision_number_attribute() {
    ensure_users_table();
    // 30 digits — within DDB's 38 limit. Verbatim text round-trip; no canonicalization.
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_n"},"score":{"N":"12345678901234567890.12345"}}"#,
        r#"{"id":{"S":"diff_n"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_binary_value() {
    ensure_users_table();
    // base64("\x00\x01\x02\xff") = "AAEC/w=="
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_b"},"blob":{"B":"AAEC/w=="}}"#,
        r#"{"id":{"S":"diff_b"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_bool_true() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_bool_t"},"active":{"BOOL":true}}"#,
        r#"{"id":{"S":"diff_bool_t"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_bool_false() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_bool_f"},"active":{"BOOL":false}}"#,
        r#"{"id":{"S":"diff_bool_f"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_null() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_null"},"deleted":{"NULL":true}}"#,
        r#"{"id":{"S":"diff_null"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_list_mixed_types() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_list"},"tags":{"L":[{"S":"a"},{"N":"1"},{"BOOL":true},{"NULL":true}]}}"#,
        r#"{"id":{"S":"diff_list"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_nested_map() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_map"},"meta":{"M":{"vip":{"BOOL":true},"score":{"N":"99"},"inner":{"M":{"x":{"S":"y"}}}}}}"#,
        r#"{"id":{"S":"diff_map"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_string_set() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_ss"},"tags":{"SS":["a","b","c"]}}"#,
        r#"{"id":{"S":"diff_ss"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_number_set() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_ns"},"scores":{"NS":["1","2","3.14","-7"]}}"#,
        r#"{"id":{"S":"diff_ns"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_binary_set() {
    ensure_users_table();
    // base64("a") == "YQ==", base64("bb") == "YmI=", base64("ccc") == "Y2Nj"
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_bs"},"chunks":{"BS":["YQ==","YmI=","Y2Nj"]}}"#,
        r#"{"id":{"S":"diff_bs"}}"#,
    );
}

// ===== Empty-collection edges ================================================

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_empty_string_value() {
    ensure_users_table();
    // DDB has allowed empty S values since 2020.
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_empty_s"},"note":{"S":""}}"#,
        r#"{"id":{"S":"diff_empty_s"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_empty_binary_value() {
    ensure_users_table();
    // Empty base64 -> 0 bytes. DDB has allowed empty B values since 2020.
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_empty_b"},"blob":{"B":""}}"#,
        r#"{"id":{"S":"diff_empty_b"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_empty_list() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_empty_l"},"items":{"L":[]}}"#,
        r#"{"id":{"S":"diff_empty_l"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_empty_map() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_empty_m"},"attrs":{"M":{}}}"#,
        r#"{"id":{"S":"diff_empty_m"}}"#,
    );
}

// ===== Two-layer nesting =====================================================
//
// `diff_nested_map` already covers M-in-M. These cover the other 2-layer
// shapes a DDB item commonly takes (list of lists, list of maps, map of lists).

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_list_of_lists() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_l_in_l"},"matrix":{"L":[{"L":[{"S":"a"},{"N":"1"}]},{"L":[{"BOOL":true},{"NULL":true}]}]}}"#,
        r#"{"id":{"S":"diff_l_in_l"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_list_of_maps() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_m_in_l"},"events":{"L":[{"M":{"kind":{"S":"click"},"n":{"N":"1"}}},{"M":{"kind":{"S":"view"},"n":{"N":"7"}}}]}}"#,
        r#"{"id":{"S":"diff_m_in_l"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_map_of_lists() {
    ensure_users_table();
    assert_round_trip_matches(
        "users",
        r#"{"id":{"S":"diff_l_in_m"},"meta":{"M":{"tags":{"L":[{"S":"a"},{"S":"b"}]},"flags":{"L":[{"BOOL":true},{"BOOL":false}]}}}}"#,
        r#"{"id":{"S":"diff_l_in_m"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_composite_key_with_numeric_sort_key() {
    ensure_device_events_table();
    assert_round_trip_matches(
        "device_events",
        r#"{"device_id":{"S":"diff_d1"},"ts":{"N":"1700000000"},"value":{"N":"42.5"}}"#,
        r#"{"device_id":{"S":"diff_d1"},"ts":{"N":"1700000000"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_delete_then_get_returns_empty() {
    ensure_users_table();
    let table = "users";
    let item = r#"{"id":{"S":"diff_delete"},"name":{"S":"alice"}}"#;
    let key = r#"{"id":{"S":"diff_delete"}}"#;

    // Clean start, then put.
    delete_both(table, key);
    let _ = aws(
        REF,
        &[
            "dynamodb",
            "put-item",
            "--table-name",
            table,
            "--item",
            item,
        ],
    );
    let _ = aws(
        OURS,
        &[
            "dynamodb",
            "put-item",
            "--table-name",
            table,
            "--item",
            item,
        ],
    );

    // Both delete responses should match (both should be `{}`).
    let del_ref = aws(
        REF,
        &[
            "dynamodb",
            "delete-item",
            "--table-name",
            table,
            "--key",
            key,
        ],
    );
    let del_ours = aws(
        OURS,
        &[
            "dynamodb",
            "delete-item",
            "--table-name",
            table,
            "--key",
            key,
        ],
    );
    assert_eq!(del_ref, del_ours, "DeleteItem responses diverged");

    // Both get-item responses should be the empty `{}` form.
    let g_ref = aws(
        REF,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    let g_ours = aws(
        OURS,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    assert_eq!(g_ref, g_ours, "post-delete GetItem responses diverged");
    assert_eq!(g_ref, json!({}), "expected empty post-delete: {g_ref}");
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_delete_nonexistent_is_idempotent() {
    ensure_users_table();
    let key = r#"{"id":{"S":"diff_delete_never_existed"}}"#;
    delete_both("users", key);
    let del_ref = strip_metadata(aws(
        REF,
        &[
            "dynamodb",
            "delete-item",
            "--table-name",
            "users",
            "--key",
            key,
        ],
    ));
    let del_ours = strip_metadata(aws(
        OURS,
        &[
            "dynamodb",
            "delete-item",
            "--table-name",
            "users",
            "--key",
            key,
        ],
    ));
    // Both should be the empty object after stripping. DDB-local sometimes
    // emits a stray ConsumedCapacity even without --return-consumed-capacity.
    assert_eq!(del_ref, del_ours, "idempotent-delete responses diverged");
    assert_eq!(del_ref, json!({}));
}

/// Strip bookkeeping metadata fields DDB-local sometimes includes but real
/// DDB doesn't (unless requested via `--return-consumed-capacity` /
/// `--return-item-collection-metrics`). We don't ship these from rektifier
/// and don't request them in tests; filter so diffs reflect data shape only.
fn strip_metadata(mut v: Value) -> Value {
    if let Some(obj) = v.as_object_mut() {
        obj.remove("ConsumedCapacity");
        obj.remove("ItemCollectionMetrics");
    }
    v
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_get_missing_returns_empty() {
    ensure_users_table();
    let key = r#"{"id":{"S":"diff_definitely_not_present_anywhere"}}"#;
    delete_both("users", key);

    let g_ref = aws(
        REF,
        &[
            "dynamodb",
            "get-item",
            "--table-name",
            "users",
            "--key",
            key,
        ],
    );
    let g_ours = aws(
        OURS,
        &[
            "dynamodb",
            "get-item",
            "--table-name",
            "users",
            "--key",
            key,
        ],
    );
    assert_eq!(g_ref, g_ours, "missing-item responses diverged");
    // Sanity: both should be `{}` (Item omitted).
    assert_eq!(g_ref, json!({}));
}
