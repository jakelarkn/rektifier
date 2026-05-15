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
//!
//! ## Attribute-name convention
//!
//! DDB has a ~573-word reserved-keyword list. Bare reserved names in
//! expressions are rejected with a `ValidationException` (rektifier
//! enforces this too, via `rekt_expressions::is_reserved`). To keep
//! main-path tests focused on the feature under test rather than the
//! reservation rules, use **non-reserved** attribute names in test
//! data: `label`, `flag`, `tally`, `origin`, `entries`, `replica`,
//! `score`, `version`, `tags`, `role`. If a test absolutely needs a
//! reserved name (e.g. for parity-with-DDB error testing), alias it
//! via `ExpressionAttributeNames` (`{"#n":"name"}`). Reserved-word
//! rejection itself has dedicated edge-case tests below — don't
//! duplicate that concern in main-path tests.

use serde_json::{json, Value};
use std::process::Command;

const REF: &str = "http://localhost:8000"; // dynamodb-local
const OURS: &str = "http://localhost:9000"; // rektifier

// ===== Shell-out helpers ======================================================

/// Like `aws`, but expects the CLI to exit non-zero. Returns the
/// stderr text so the test can match on the DDB error message. Panics
/// if the command unexpectedly succeeded.
fn aws_expecting_failure(endpoint: &str, args: &[&str]) -> String {
    let output = Command::new("aws")
        .arg("--endpoint-url")
        .arg(endpoint)
        .args(args)
        .env("AWS_ACCESS_KEY_ID", "local")
        .env("AWS_SECRET_ACCESS_KEY", "local")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .arg("--output")
        .arg("json")
        .output()
        .unwrap_or_else(|e| panic!("aws CLI invocation failed: {e}"));

    if output.status.success() {
        panic!(
            "aws {endpoint} args={args:?} unexpectedly succeeded:\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    let mut s = String::from_utf8_lossy(&output.stderr).into_owned();
    s.push_str(&String::from_utf8_lossy(&output.stdout));
    s
}

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

fn ensure_counters_table() {
    ensure_ref_table("counters", &[("id", "N")], &[("id", "HASH")]);
}

fn ensure_blobs_table() {
    ensure_ref_table("blobs", &[("binmark", "B")], &[("binmark", "HASH")]);
}

fn ensure_binsorted_table() {
    ensure_ref_table(
        "binsorted",
        &[("id", "S"), ("binmark", "B")],
        &[("id", "HASH"), ("binmark", "RANGE")],
    );
}

fn ensure_messages_table() {
    ensure_ref_table(
        "messages",
        &[("thread", "S"), ("ts", "S")],
        &[("thread", "HASH"), ("ts", "RANGE")],
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

// ===== UpdateItem diffs =======================================================
//
// Each test deletes the row on both endpoints, optionally seeds it via
// PutItem, runs UpdateItem on both, then GETs and compares. Phase 3
// supports the simple subset only: top-level `SET attr = literal` and
// `REMOVE attr`, no ConditionExpression, ReturnValues=NONE.

/// Run `UpdateItem` on both endpoints and compare. `seed_item` is an
/// optional PutItem run before the update (`None` exercises the upsert
/// path); the update arguments use `aws dynamodb update-item` flag form.
fn assert_update_round_trip_matches(
    table: &str,
    key: &str,
    seed_item: Option<&str>,
    update_args: &[&str],
) {
    delete_both(table, key);

    if let Some(item) = seed_item {
        aws(
            REF,
            &["dynamodb", "put-item", "--table-name", table, "--item", item],
        );
        aws(
            OURS,
            &["dynamodb", "put-item", "--table-name", table, "--item", item],
        );
    }

    let mut upd_args: Vec<&str> = vec!["dynamodb", "update-item", "--table-name", table, "--key", key];
    upd_args.extend_from_slice(update_args);

    let upd_ref = strip_metadata(aws(REF, &upd_args));
    let upd_ours = strip_metadata(aws(OURS, &upd_args));
    assert_eq!(
        upd_ref, upd_ours,
        "UpdateItem responses diverged for {table} / key={key}"
    );

    let mut get_ref = aws(
        REF,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    let mut get_ours = aws(
        OURS,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    sort_sets(&mut get_ref);
    sort_sets(&mut get_ours);
    assert_eq!(
        get_ref, get_ours,
        "GetItem after UpdateItem diverged for {table} / key={key}\nref:  {get_ref}\nours: {get_ours}"
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_set_on_missing_row_upserts() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_upsert"}}"##,
        None,
        &[
            "--update-expression",
            "SET #n = :name, score = :s",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":name":{"S":"alice"},":s":{"N":"7"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_set_on_existing_row_merges() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_merge"}}"##,
        Some(r##"{"id":{"S":"diff_upd_merge"},"name":{"S":"alice"},"keep":{"S":"x"}}"##),
        &[
            "--update-expression",
            "SET #n = :name, active = :a",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":name":{"S":"alice2"},":a":{"BOOL":true}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_remove_present_attribute() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_remove"}}"##,
        Some(r##"{"id":{"S":"diff_upd_remove"},"keep":{"S":"x"},"drop":{"S":"y"}}"##),
        &[
            "--update-expression",
            "REMOVE #d",
            "--expression-attribute-names",
            r##"{"#d":"drop"}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_remove_absent_attribute_is_noop() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_remove_absent"}}"##,
        Some(r##"{"id":{"S":"diff_upd_remove_absent"},"keep":{"S":"x"}}"##),
        &["--update-expression", "REMOVE never_existed"],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_set_and_remove_combined() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_combo"}}"##,
        Some(r##"{"id":{"S":"diff_upd_combo"},"status":{"S":"old"},"drop":{"S":"x"}}"##),
        &[
            "--update-expression",
            "SET #s = :new REMOVE #d",
            "--expression-attribute-names",
            r##"{"#s":"status","#d":"drop"}"##,
            "--expression-attribute-values",
            r##"{":new":{"S":"new"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_set_all_attribute_value_variants() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_variants"}}"##,
        None,
        &[
            "--update-expression",
            "SET s = :s, n = :n, b = :b, active = :bool, deleted = :null, l = :l, m = :m, tags = :ss, scores = :ns, chunks = :bs",
            "--expression-attribute-values",
            r##"{
                ":s":{"S":"hello"},
                ":n":{"N":"42.5"},
                ":b":{"B":"AAEC"},
                ":bool":{"BOOL":true},
                ":null":{"NULL":true},
                ":l":{"L":[{"S":"a"},{"N":"1"}]},
                ":m":{"M":{"k":{"S":"v"}}},
                ":ss":{"SS":["a","b"]},
                ":ns":{"NS":["1","2"]},
                ":bs":{"BS":["AAE="]}
            }"##,
        ],
    );
}

// ---- Phase 4c: attribute_not_exists(pk) -----------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_insert_only_succeeds_on_missing_row() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_io_new"}}"##,
        None,
        &[
            "--update-expression",
            "SET #n = :name",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":name":{"S":"alice"}}"##,
            "--condition-expression",
            "attribute_not_exists(id)",
        ],
    );
}

/// Insert-only condition on an existing row: DDB-local returns CCFE,
/// rektifier should too. We can't use `assert_update_round_trip_matches`
/// because both endpoints fail and the AWS CLI exits nonzero — we
/// invoke them directly and assert the error shape matches.
#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_insert_only_fails_on_existing_row() {
    ensure_users_table();
    let key = r##"{"id":{"S":"diff_io_clash"}}"##;
    delete_both("users", key);
    // Seed
    let seed = r##"{"id":{"S":"diff_io_clash"},"name":{"S":"alice"}}"##;
    aws(
        REF,
        &["dynamodb", "put-item", "--table-name", "users", "--item", seed],
    );
    aws(
        OURS,
        &["dynamodb", "put-item", "--table-name", "users", "--item", seed],
    );

    let upd_args = [
        "dynamodb",
        "update-item",
        "--table-name",
        "users",
        "--key",
        key,
        "--update-expression",
        "SET #n = :name",
        "--expression-attribute-names",
        r##"{"#n":"new_name"}"##,
        "--expression-attribute-values",
        r##"{":name":{"S":"NEW"}}"##,
        "--condition-expression",
        "attribute_not_exists(id)",
    ];

    let ref_err = run_for_error(REF, &upd_args);
    let ours_err = run_for_error(OURS, &upd_args);

    // Both should reject the request with a ConditionalCheckFailedException.
    // The exact wording of the message can drift; the error type tag is what
    // we care about.
    assert!(
        ref_err.contains("ConditionalCheckFailedException"),
        "DDB-local should reject with CCFE, got: {ref_err}"
    );
    assert!(
        ours_err.contains("ConditionalCheckFailedException"),
        "rektifier should reject with CCFE, got: {ours_err}"
    );

    // The row should be unchanged on both sides.
    let get_args = ["dynamodb", "get-item", "--table-name", "users", "--key", key];
    let mut ref_got = aws(REF, &get_args);
    let mut ours_got = aws(OURS, &get_args);
    sort_sets(&mut ref_got);
    sort_sets(&mut ours_got);
    assert_eq!(ref_got, ours_got, "post-CCFE GetItem diverged");
}

// ---- Phase 4d: SimpleSql conditions (UPDATE … WHERE) ---------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_attribute_exists_pk_succeeds_when_row_present() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_ae_present"}}"##,
        Some(r##"{"id":{"S":"diff_ae_present"},"name":{"S":"alice"}}"##),
        &[
            "--update-expression",
            "SET #n = :v",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":v":{"S":"alice2"}}"##,
            "--condition-expression",
            "attribute_exists(id)",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_attribute_exists_pk_fails_when_row_missing() {
    ensure_users_table();
    let key = r##"{"id":{"S":"diff_ae_missing"}}"##;
    delete_both("users", key);

    let upd_args = [
        "dynamodb",
        "update-item",
        "--table-name",
        "users",
        "--key",
        key,
        "--update-expression",
        "SET #n = :v",
        "--expression-attribute-names",
        r##"{"#n":"name"}"##,
        "--expression-attribute-values",
        r##"{":v":{"S":"x"}}"##,
        "--condition-expression",
        "attribute_exists(id)",
    ];
    let ref_err = run_for_error(REF, &upd_args);
    let ours_err = run_for_error(OURS, &upd_args);
    assert!(
        ref_err.contains("ConditionalCheckFailedException"),
        "DDB-local should reject with CCFE, got: {ref_err}"
    );
    assert!(
        ours_err.contains("ConditionalCheckFailedException"),
        "rektifier should reject with CCFE, got: {ours_err}"
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_optimistic_lock_via_version() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_oplock_ok"}}"##,
        Some(r##"{"id":{"S":"diff_oplock_ok"},"version":{"N":"3"}}"##),
        &[
            "--update-expression",
            "SET #v = :new",
            "--expression-attribute-names",
            r##"{"#v":"version"}"##,
            "--expression-attribute-values",
            r##"{":expected":{"N":"3"},":new":{"N":"4"}}"##,
            "--condition-expression",
            "#v = :expected",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_optimistic_lock_mismatched_version_is_ccfe() {
    ensure_users_table();
    let key = r##"{"id":{"S":"diff_oplock_mismatch"}}"##;
    delete_both("users", key);
    let seed = r##"{"id":{"S":"diff_oplock_mismatch"},"version":{"N":"5"}}"##;
    aws(
        REF,
        &["dynamodb", "put-item", "--table-name", "users", "--item", seed],
    );
    aws(
        OURS,
        &["dynamodb", "put-item", "--table-name", "users", "--item", seed],
    );

    let upd_args = [
        "dynamodb",
        "update-item",
        "--table-name",
        "users",
        "--key",
        key,
        "--update-expression",
        "SET #v = :new",
        "--expression-attribute-names",
        r##"{"#v":"version"}"##,
        "--expression-attribute-values",
        r##"{":expected":{"N":"1"},":new":{"N":"99"}}"##,
        "--condition-expression",
        "#v = :expected",
    ];
    let ref_err = run_for_error(REF, &upd_args);
    let ours_err = run_for_error(OURS, &upd_args);
    assert!(ref_err.contains("ConditionalCheckFailedException"));
    assert!(ours_err.contains("ConditionalCheckFailedException"));
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_numeric_ordering() {
    ensure_users_table();
    // score < 100 → applies; sets score=50.
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_ord_lt"}}"##,
        Some(r##"{"id":{"S":"diff_ord_lt"},"score":{"N":"10"}}"##),
        &[
            "--update-expression",
            "SET score = :new",
            "--expression-attribute-values",
            r##"{":new":{"N":"50"},":lim":{"N":"100"}}"##,
            "--condition-expression",
            "score < :lim",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_string_ordering() {
    ensure_users_table();
    // #n >= "a" → applies. (`name` is a DDB reserved keyword, must be
    // referenced via #alias to survive DDB-local's strict validator.)
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_ord_s"}}"##,
        Some(r##"{"id":{"S":"diff_ord_s"},"name":{"S":"bob"}}"##),
        &[
            "--update-expression",
            "SET #n = :new",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":new":{"S":"carol"},":floor":{"S":"a"}}"##,
            "--condition-expression",
            "#n >= :floor",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_boolean_and_composition() {
    ensure_users_table();
    // `status` and `version` are both DDB reserved keywords; alias both.
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_and"}}"##,
        Some(r##"{"id":{"S":"diff_and"},"status":{"S":"active"},"version":{"N":"1"}}"##),
        &[
            "--update-expression",
            "SET #v = :new",
            "--expression-attribute-names",
            r##"{"#v":"version","#s":"status"}"##,
            "--expression-attribute-values",
            r##"{":active":{"S":"active"},":v1":{"N":"1"},":new":{"N":"2"}}"##,
            "--condition-expression",
            "#s = :active AND #v = :v1",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_boolean_or_composition() {
    ensure_users_table();
    // version = 1 OR status = "active" → true (status matches). Both
    // attribute names are DDB reserved keywords; alias them.
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_or"}}"##,
        Some(r##"{"id":{"S":"diff_or"},"status":{"S":"active"},"version":{"N":"99"}}"##),
        &[
            "--update-expression",
            "SET marker = :m",
            "--expression-attribute-names",
            r##"{"#v":"version","#s":"status"}"##,
            "--expression-attribute-values",
            r##"{":v1":{"N":"1"},":active":{"S":"active"},":m":{"S":"updated"}}"##,
            "--condition-expression",
            "#v = :v1 OR #s = :active",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_not_composition() {
    ensure_users_table();
    // NOT attribute_exists(deleted) → true when no `deleted` attr.
    // `name` is reserved, alias it.
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_not"}}"##,
        Some(r##"{"id":{"S":"diff_not"},"name":{"S":"alice"}}"##),
        &[
            "--update-expression",
            "SET #n = :v",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":v":{"S":"alice2"}}"##,
            "--condition-expression",
            "NOT attribute_exists(deleted)",
        ],
    );
}

// ---- Phase 3b/3c: slow-path UpdateExpression RHS forms -------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_set_path_ref() {
    ensure_users_table();
    // `source` is DDB-reserved; alias it.
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_pathref"}}"##,
        Some(r##"{"id":{"S":"diff_pathref"},"source":{"S":"hello"}}"##),
        &[
            "--update-expression",
            "SET copied = #src",
            "--expression-attribute-names",
            r##"{"#src":"source"}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_arithmetic_increment() {
    ensure_users_table();
    // `counter` is DDB-reserved; alias it.
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_inc"}}"##,
        Some(r##"{"id":{"S":"diff_inc"},"counter":{"N":"5"}}"##),
        &[
            "--update-expression",
            "SET #c = #c + :inc",
            "--expression-attribute-names",
            r##"{"#c":"counter"}"##,
            "--expression-attribute-values",
            r##"{":inc":{"N":"3"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_arithmetic_decrement() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_dec"}}"##,
        Some(r##"{"id":{"S":"diff_dec"},"counter":{"N":"10"}}"##),
        &[
            "--update-expression",
            "SET #c = #c - :dec",
            "--expression-attribute-names",
            r##"{"#c":"counter"}"##,
            "--expression-attribute-values",
            r##"{":dec":{"N":"4"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_if_not_exists_preserves() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_ine_pres"}}"##,
        Some(r##"{"id":{"S":"diff_ine_pres"},"created":{"N":"1000"}}"##),
        &[
            "--update-expression",
            "SET created = if_not_exists(created, :now)",
            "--expression-attribute-values",
            r##"{":now":{"N":"9999"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_if_not_exists_uses_fallback() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_ine_fall"}}"##,
        Some(r##"{"id":{"S":"diff_ine_fall"}}"##),
        &[
            "--update-expression",
            "SET created = if_not_exists(created, :now)",
            "--expression-attribute-values",
            r##"{":now":{"N":"1234"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_list_append_appends() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_lapp"}}"##,
        Some(r##"{"id":{"S":"diff_lapp"},"tags":{"L":[{"S":"a"}]}}"##),
        &[
            "--update-expression",
            "SET tags = list_append(tags, :new)",
            "--expression-attribute-values",
            r##"{":new":{"L":[{"S":"b"},{"S":"c"}]}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_list_append_prepends() {
    ensure_users_table();
    // list_append(:new, tags) prepends.
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_lprep"}}"##,
        Some(r##"{"id":{"S":"diff_lprep"},"tags":{"L":[{"S":"c"}]}}"##),
        &[
            "--update-expression",
            "SET tags = list_append(:new, tags)",
            "--expression-attribute-values",
            r##"{":new":{"L":[{"S":"a"},{"S":"b"}]}}"##,
        ],
    );
}

// ---- Phase 4e: slow-path condition shapes ---------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_begins_with_condition() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_bw"}}"##,
        Some(r##"{"id":{"S":"diff_bw"},"name":{"S":"alice"}}"##),
        &[
            "--update-expression",
            "SET marker = :m",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":m":{"S":"hit"},":p":{"S":"ali"}}"##,
            "--condition-expression",
            "begins_with(#n, :p)",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_contains_set_membership() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_contains"}}"##,
        Some(r##"{"id":{"S":"diff_contains"},"tags":{"SS":["alpha","beta"]}}"##),
        &[
            "--update-expression",
            "SET marker = :m",
            "--expression-attribute-values",
            r##"{":m":{"S":"hit"},":x":{"S":"alpha"}}"##,
            "--condition-expression",
            "contains(tags, :x)",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_between_numeric() {
    ensure_users_table();
    // `score` is DDB-reserved → alias.
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_bw_n"}}"##,
        Some(r##"{"id":{"S":"diff_bw_n"},"score":{"N":"50"}}"##),
        &[
            "--update-expression",
            "SET marker = :m",
            "--expression-attribute-names",
            r##"{"#sc":"score"}"##,
            "--expression-attribute-values",
            r##"{":m":{"S":"hit"},":lo":{"N":"10"},":hi":{"N":"100"}}"##,
            "--condition-expression",
            "#sc BETWEEN :lo AND :hi",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_in_list() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_in"}}"##,
        Some(r##"{"id":{"S":"diff_in"},"status":{"S":"pending"}}"##),
        &[
            "--update-expression",
            "SET marker = :m",
            "--expression-attribute-names",
            r##"{"#s":"status"}"##,
            "--expression-attribute-values",
            r##"{":m":{"S":"hit"},":a":{"S":"active"},":p":{"S":"pending"},":c":{"S":"closed"}}"##,
            "--condition-expression",
            "#s IN (:a, :p, :c)",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_attribute_type() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_at"}}"##,
        Some(r##"{"id":{"S":"diff_at"},"score":{"N":"42"}}"##),
        &[
            "--update-expression",
            "SET marker = :m",
            "--expression-attribute-names",
            r##"{"#sc":"score"}"##,
            "--expression-attribute-values",
            r##"{":m":{"S":"hit"},":t":{"S":"N"}}"##,
            "--condition-expression",
            "attribute_type(#sc, :t)",
        ],
    );
}

// ---- Phase 5: ADD ---------------------------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_add_numeric_increments() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_add_n"}}"##,
        Some(r##"{"id":{"S":"diff_add_n"},"counter":{"N":"10"}}"##),
        &[
            "--update-expression",
            "ADD #c :inc",
            "--expression-attribute-names",
            r##"{"#c":"counter"}"##,
            "--expression-attribute-values",
            r##"{":inc":{"N":"5"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_add_numeric_on_missing_row() {
    ensure_users_table();
    let key = r##"{"id":{"S":"diff_add_new"}}"##;
    delete_both("users", key);
    // No seed — ADD-numeric on missing row should create the counter.
    let upd_args = [
        "dynamodb",
        "update-item",
        "--table-name",
        "users",
        "--key",
        key,
        "--update-expression",
        "ADD #c :inc",
        "--expression-attribute-names",
        r##"{"#c":"counter"}"##,
        "--expression-attribute-values",
        r##"{":inc":{"N":"7"}}"##,
    ];
    let ref_upd = strip_metadata(aws(REF, &upd_args));
    let ours_upd = strip_metadata(aws(OURS, &upd_args));
    assert_eq!(ref_upd, ours_upd);

    let mut ref_got = aws(
        REF,
        &["dynamodb", "get-item", "--table-name", "users", "--key", key],
    );
    let mut ours_got = aws(
        OURS,
        &["dynamodb", "get-item", "--table-name", "users", "--key", key],
    );
    sort_sets(&mut ref_got);
    sort_sets(&mut ours_got);
    assert_eq!(ref_got, ours_got);
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_add_set_union() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_add_set"}}"##,
        Some(r##"{"id":{"S":"diff_add_set"},"tags":{"SS":["a","b"]}}"##),
        &[
            "--update-expression",
            "ADD tags :new",
            "--expression-attribute-values",
            r##"{":new":{"SS":["b","c"]}}"##,
        ],
    );
}

// ---- Phase 6: DELETE ------------------------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_delete_set_removes_subset() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_del_set"}}"##,
        Some(r##"{"id":{"S":"diff_del_set"},"tags":{"SS":["a","b","c"]}}"##),
        &[
            "--update-expression",
            "DELETE tags :rm",
            "--expression-attribute-values",
            r##"{":rm":{"SS":["b"]}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_delete_set_emptied_removes_attribute() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_del_empty"}}"##,
        Some(r##"{"id":{"S":"diff_del_empty"},"tags":{"SS":["a","b"]},"keep":{"S":"x"}}"##),
        &[
            "--update-expression",
            "DELETE tags :rm",
            "--expression-attribute-values",
            r##"{":rm":{"SS":["a","b"]}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_delete_path_absent_noop() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_del_absent"}}"##,
        Some(r##"{"id":{"S":"diff_del_absent"},"keep":{"S":"x"}}"##),
        &[
            "--update-expression",
            "DELETE never_existed :rm",
            "--expression-attribute-values",
            r##"{":rm":{"SS":["a"]}}"##,
        ],
    );
}

/// Best-effort run that captures stderr (where the AWS CLI puts error
/// type names). Returns the concatenation of stdout and stderr so the
/// caller can grep for the expected `__type`.
fn run_for_error(endpoint: &str, args: &[&str]) -> String {
    let output = std::process::Command::new("aws")
        .arg("--endpoint-url")
        .arg(endpoint)
        .args(args)
        .arg("--output")
        .arg("json")
        .env("AWS_ACCESS_KEY_ID", "local")
        .env("AWS_SECRET_ACCESS_KEY", "local")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .output()
        .unwrap_or_else(|e| panic!("aws CLI invocation failed: {e}"));
    assert!(!output.status.success(), "command unexpectedly succeeded");
    let mut s = String::from_utf8_lossy(&output.stdout).into_owned();
    s.push_str(&String::from_utf8_lossy(&output.stderr));
    s
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_composite_key() {
    ensure_device_events_table();
    assert_update_round_trip_matches(
        "device_events",
        r##"{"device_id":{"S":"diff_upd_ck"},"ts":{"N":"1000"}}"##,
        None,
        &[
            "--update-expression",
            "SET v = :v",
            "--expression-attribute-values",
            r##"{":v":{"N":"42"}}"##,
        ],
    );
}

// ---- Phase 7a: ReturnValues=ALL_NEW --------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_new_insert() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvan_ins"}}"##,
        None,
        &[
            "--update-expression",
            "SET #n = :name",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":name":{"S":"alice"}}"##,
            "--return-values",
            "ALL_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_new_update_merge() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvan_upd"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvan_upd"},"name":{"S":"old"},"role":{"S":"admin"}}"##),
        &[
            "--update-expression",
            "SET #n = :name",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":name":{"S":"new"}}"##,
            "--return-values",
            "ALL_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_new_with_remove() {
    // Verifies the response reflects post-update state including REMOVE.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvan_rm"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvan_rm"},"a":{"S":"keep"},"b":{"S":"drop"}}"##),
        &[
            "--update-expression",
            "REMOVE b",
            "--return-values",
            "ALL_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_new_insert_only_path() {
    // InsertOnlyOnPk routing — attribute_not_exists(pk) on a missing row.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvan_ionly"}}"##,
        None,
        &[
            "--update-expression",
            "SET #n = :name",
            "--condition-expression",
            "attribute_not_exists(id)",
            "--expression-attribute-names",
            r##"{"#n":"name"}"##,
            "--expression-attribute-values",
            r##"{":name":{"S":"bob"}}"##,
            "--return-values",
            "ALL_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_new_sql_cond_path() {
    // SimpleSql routing — optimistic-lock pattern.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvan_sql"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvan_sql"},"version":{"N":"1"}}"##),
        &[
            "--update-expression",
            "SET #v = :new",
            "--condition-expression",
            "#v = :old",
            "--expression-attribute-names",
            r##"{"#v":"version"}"##,
            "--expression-attribute-values",
            r##"{":new":{"N":"2"},":old":{"N":"1"}}"##,
            "--return-values",
            "ALL_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_new_tx_path() {
    // Path-ref RHS forces the slow (tx) path.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvan_tx"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvan_tx"},"source":{"S":"hello"}}"##),
        &[
            "--update-expression",
            "SET #dst = #src",
            "--expression-attribute-names",
            r##"{"#src":"source","#dst":"copy"}"##,
            "--return-values",
            "ALL_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_new_add_numeric() {
    // ADD :n on the tx path; ALL_NEW must reflect the incremented value.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvan_add"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvan_add"},"counter":{"N":"10"}}"##),
        &[
            "--update-expression",
            "ADD #c :n",
            "--expression-attribute-names",
            r##"{"#c":"counter"}"##,
            "--expression-attribute-values",
            r##"{":n":{"N":"5"}}"##,
            "--return-values",
            "ALL_NEW",
        ],
    );
}

// ---- Phase 7b: ReturnValues=ALL_OLD --------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_old_on_insert_omits_attributes() {
    // No prior row → both endpoints must omit `Attributes`.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvao_ins"}}"##,
        None,
        &[
            "--update-expression", "SET #l = :v",
            "--expression-attribute-names", r##"{"#l":"label"}"##,
            "--expression-attribute-values", r##"{":v":{"S":"alice"}}"##,
            "--return-values", "ALL_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_old_on_update_returns_pre_image() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvao_upd"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvao_upd"},"label":{"S":"old"},"role":{"S":"admin"}}"##),
        &[
            "--update-expression", "SET #l = :v",
            "--expression-attribute-names", r##"{"#l":"label"}"##,
            "--expression-attribute-values", r##"{":v":{"S":"new"}}"##,
            "--return-values", "ALL_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_old_with_remove() {
    // ALL_OLD must include the attribute that this UPDATE removed.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvao_rm"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvao_rm"},"a":{"S":"keep"},"b":{"S":"drop_target"}}"##),
        &[
            "--update-expression", "REMOVE b",
            "--return-values", "ALL_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_old_insert_only_path() {
    // InsertOnlyOnPk succeeds only when row is absent → ALL_OLD has
    // nothing to return.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvao_ionly"}}"##,
        None,
        &[
            "--update-expression", "SET #l = :v",
            "--condition-expression", "attribute_not_exists(id)",
            "--expression-attribute-names", r##"{"#l":"label"}"##,
            "--expression-attribute-values", r##"{":v":{"S":"bob"}}"##,
            "--return-values", "ALL_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_old_sql_cond_path() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvao_sql"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvao_sql"},"version":{"N":"1"}}"##),
        &[
            "--update-expression", "SET version = :new",
            "--condition-expression", "version = :old",
            "--expression-attribute-values", r##"{":new":{"N":"2"},":old":{"N":"1"}}"##,
            "--return-values", "ALL_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_old_tx_path() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvao_tx"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvao_tx"},"origin":{"S":"hello"}}"##),
        &[
            "--update-expression", "SET #dst = #src",
            "--expression-attribute-names", r##"{"#src":"origin","#dst":"replica"}"##,
            "--return-values", "ALL_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_all_old_add_numeric() {
    // ADD :n on the tx path; ALL_OLD must reflect the pre-increment value.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_rvao_add"}}"##,
        Some(r##"{"id":{"S":"diff_upd_rvao_add"},"counter":{"N":"10"}}"##),
        &[
            "--update-expression", "ADD #c :n",
            "--expression-attribute-names", r##"{"#c":"counter"}"##,
            "--expression-attribute-values", r##"{":n":{"N":"5"}}"##,
            "--return-values", "ALL_OLD",
        ],
    );
}

// ---- Phase 7c: ReturnValues=UPDATED_NEW / UPDATED_OLD --------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_updated_new_set() {
    // UPDATED_NEW must contain only the touched attribute, with its
    // post-update value.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_un_set"}}"##,
        Some(r##"{"id":{"S":"diff_upd_un_set"},"label":{"S":"old"},"role":{"S":"admin"}}"##),
        &[
            "--update-expression", "SET #l = :v",
            "--expression-attribute-names", r##"{"#l":"label"}"##,
            "--expression-attribute-values", r##"{":v":{"S":"new"}}"##,
            "--return-values", "UPDATED_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_updated_old_set() {
    // UPDATED_OLD must contain only the touched attribute, with its
    // pre-update value.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_uo_set"}}"##,
        Some(r##"{"id":{"S":"diff_upd_uo_set"},"label":{"S":"old"},"role":{"S":"admin"}}"##),
        &[
            "--update-expression", "SET #l = :v",
            "--expression-attribute-names", r##"{"#l":"label"}"##,
            "--expression-attribute-values", r##"{":v":{"S":"new"}}"##,
            "--return-values", "UPDATED_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_updated_new_on_remove_omits_attrs() {
    // REMOVE → touched attr no longer exists → UPDATED_NEW omits
    // Attributes (or projection is empty — both endpoints should agree).
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_un_rm"}}"##,
        Some(r##"{"id":{"S":"diff_upd_un_rm"},"a":{"S":"keep"},"b":{"S":"drop_target"}}"##),
        &[
            "--update-expression", "REMOVE b",
            "--return-values", "UPDATED_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_updated_old_on_remove_returns_pre_value() {
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_uo_rm"}}"##,
        Some(r##"{"id":{"S":"diff_upd_uo_rm"},"a":{"S":"keep"},"b":{"S":"drop_target"}}"##),
        &[
            "--update-expression", "REMOVE b",
            "--return-values", "UPDATED_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_updated_old_on_fresh_insert_omits_attributes() {
    // No prior row → UPDATED_OLD has nothing.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_uo_fresh"}}"##,
        None,
        &[
            "--update-expression", "SET #l = :v",
            "--expression-attribute-names", r##"{"#l":"label"}"##,
            "--expression-attribute-values", r##"{":v":{"S":"alice"}}"##,
            "--return-values", "UPDATED_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_updated_new_add_numeric() {
    // ADD :n on tx path → UPDATED_NEW returns incremented value only.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_un_add"}}"##,
        Some(r##"{"id":{"S":"diff_upd_un_add"},"tally":{"N":"10"},"other":{"S":"untouched"}}"##),
        &[
            "--update-expression", "ADD tally :n",
            "--expression-attribute-values", r##"{":n":{"N":"5"}}"##,
            "--return-values", "UPDATED_NEW",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_update_return_values_updated_new_mixed_set_and_remove() {
    // Two touched paths — verifies the projection includes both.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_upd_un_mix"}}"##,
        Some(r##"{"id":{"S":"diff_upd_un_mix"},"a":{"S":"old_a"},"b":{"S":"drop_target"},"c":{"S":"untouched"}}"##),
        &[
            "--update-expression", "SET a = :v REMOVE b",
            "--expression-attribute-values", r##"{":v":{"S":"new_a"}}"##,
            "--return-values", "UPDATED_NEW",
        ],
    );
}

// ---- Reserved-word rejection parity (edge cases) -------------------------
//
// These verify both endpoints reject bare reserved-word attribute names
// in expressions with a similar error shape. They are deliberately
// scoped to the reservation behavior — main-path tests above use
// non-reserved attribute names (see the module header) so they stay
// focused on translator/storage logic.

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_reject_reserved_bare_name_in_update_expression() {
    ensure_users_table();
    let args = &[
        "dynamodb", "update-item",
        "--table-name", "users",
        "--key", r##"{"id":{"S":"diff_reserved_upd"}}"##,
        "--update-expression", "SET name = :v",
        "--expression-attribute-values", r##"{":v":{"S":"alice"}}"##,
    ];
    let ref_err = aws_expecting_failure(REF, args);
    let ours_err = aws_expecting_failure(OURS, args);
    for (label, msg) in [("ref", &ref_err), ("ours", &ours_err)] {
        assert!(
            msg.contains("ValidationException"),
            "{label}: expected ValidationException, got: {msg}"
        );
        assert!(
            msg.contains("reserved keyword: name"),
            "{label}: expected `reserved keyword: name`, got: {msg}"
        );
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_reject_reserved_bare_name_in_condition_expression() {
    ensure_users_table();
    let args = &[
        "dynamodb", "update-item",
        "--table-name", "users",
        "--key", r##"{"id":{"S":"diff_reserved_cond"}}"##,
        "--update-expression", "SET label = :v",
        "--condition-expression", "attribute_exists(status)",
        "--expression-attribute-values", r##"{":v":{"S":"alice"}}"##,
    ];
    let ref_err = aws_expecting_failure(REF, args);
    let ours_err = aws_expecting_failure(OURS, args);
    for (label, msg) in [("ref", &ref_err), ("ours", &ours_err)] {
        assert!(
            msg.contains("ValidationException"),
            "{label}: expected ValidationException, got: {msg}"
        );
        assert!(
            msg.contains("reserved keyword: status"),
            "{label}: expected `reserved keyword: status`, got: {msg}"
        );
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_aliased_reserved_name_is_accepted() {
    // Verifies the bypass: `#n → "name"` succeeds on both endpoints
    // because the parser only sees the placeholder.
    ensure_users_table();
    assert_update_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_reserved_aliased"}}"##,
        None,
        &[
            "--update-expression", "SET #n = :v",
            "--expression-attribute-names", r##"{"#n":"name"}"##,
            "--expression-attribute-values", r##"{":v":{"S":"alice"}}"##,
        ],
    );
}

// ===== Conditional PutItem / DeleteItem + ReturnValues=ALL_OLD =================

/// Run `PutItem` on both endpoints with optional seed and arbitrary
/// flags, compare responses, then GET and compare. Mirrors
/// `assert_update_round_trip_matches` for Put.
fn assert_put_round_trip_matches(
    table: &str,
    key: &str,
    seed_item: Option<&str>,
    item: &str,
    extra_args: &[&str],
) {
    delete_both(table, key);
    if let Some(s) = seed_item {
        aws(REF, &["dynamodb", "put-item", "--table-name", table, "--item", s]);
        aws(OURS, &["dynamodb", "put-item", "--table-name", table, "--item", s]);
    }
    let mut args: Vec<&str> = vec![
        "dynamodb", "put-item", "--table-name", table, "--item", item,
    ];
    args.extend_from_slice(extra_args);
    let put_ref = strip_metadata(aws(REF, &args));
    let put_ours = strip_metadata(aws(OURS, &args));
    assert_eq!(
        put_ref, put_ours,
        "PutItem responses diverged for {table} / key={key}"
    );
    let mut get_ref = aws(
        REF,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    let mut get_ours = aws(
        OURS,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    sort_sets(&mut get_ref);
    sort_sets(&mut get_ours);
    assert_eq!(
        get_ref, get_ours,
        "GetItem after PutItem diverged for {table} / key={key}"
    );
}

/// Run `DeleteItem` on both endpoints. Mirrors the helper above.
fn assert_delete_round_trip_matches(
    table: &str,
    key: &str,
    seed_item: Option<&str>,
    extra_args: &[&str],
) {
    delete_both(table, key);
    if let Some(s) = seed_item {
        aws(REF, &["dynamodb", "put-item", "--table-name", table, "--item", s]);
        aws(OURS, &["dynamodb", "put-item", "--table-name", table, "--item", s]);
    }
    let mut args: Vec<&str> = vec![
        "dynamodb", "delete-item", "--table-name", table, "--key", key,
    ];
    args.extend_from_slice(extra_args);
    let del_ref = strip_metadata(aws(REF, &args));
    let del_ours = strip_metadata(aws(OURS, &args));
    assert_eq!(
        del_ref, del_ours,
        "DeleteItem responses diverged for {table} / key={key}"
    );
    let mut get_ref = aws(
        REF,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    let mut get_ours = aws(
        OURS,
        &["dynamodb", "get-item", "--table-name", table, "--key", key],
    );
    sort_sets(&mut get_ref);
    sort_sets(&mut get_ours);
    assert_eq!(get_ref, get_ours, "GetItem after DeleteItem diverged");
}

// ---- Conditional PutItem -----------------------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_put_attribute_not_exists_succeeds_on_missing() {
    ensure_users_table();
    assert_put_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_put_ane_ok"}}"##,
        None,
        r##"{"id":{"S":"diff_put_ane_ok"},"label":{"S":"alice"}}"##,
        &["--condition-expression", "attribute_not_exists(id)"],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_put_attribute_not_exists_fails_when_row_exists() {
    ensure_users_table();
    let key = r##"{"id":{"S":"diff_put_ane_dup"}}"##;
    delete_both("users", key);
    let seed = r##"{"id":{"S":"diff_put_ane_dup"}}"##;
    aws(REF, &["dynamodb", "put-item", "--table-name", "users", "--item", seed]);
    aws(OURS, &["dynamodb", "put-item", "--table-name", "users", "--item", seed]);
    let args = [
        "dynamodb", "put-item",
        "--table-name", "users",
        "--item", r##"{"id":{"S":"diff_put_ane_dup"},"label":{"S":"new"}}"##,
        "--condition-expression", "attribute_not_exists(id)",
    ];
    let ref_err = run_for_error(REF, &args);
    let ours_err = run_for_error(OURS, &args);
    assert!(ref_err.contains("ConditionalCheckFailedException"));
    assert!(ours_err.contains("ConditionalCheckFailedException"));
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_put_optimistic_lock_matched() {
    ensure_users_table();
    assert_put_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_put_oplock"}}"##,
        Some(r##"{"id":{"S":"diff_put_oplock"},"version":{"N":"1"}}"##),
        r##"{"id":{"S":"diff_put_oplock"},"version":{"N":"2"}}"##,
        &[
            "--condition-expression", "version = :v",
            "--expression-attribute-values", r##"{":v":{"N":"1"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_put_optimistic_lock_stale_is_ccfe() {
    ensure_users_table();
    let key = r##"{"id":{"S":"diff_put_oplock_stale"}}"##;
    delete_both("users", key);
    let seed = r##"{"id":{"S":"diff_put_oplock_stale"},"version":{"N":"5"}}"##;
    aws(REF, &["dynamodb", "put-item", "--table-name", "users", "--item", seed]);
    aws(OURS, &["dynamodb", "put-item", "--table-name", "users", "--item", seed]);
    let args = [
        "dynamodb", "put-item",
        "--table-name", "users",
        "--item", r##"{"id":{"S":"diff_put_oplock_stale"},"version":{"N":"6"}}"##,
        "--condition-expression", "version = :v",
        "--expression-attribute-values", r##"{":v":{"N":"1"}}"##,
    ];
    let ref_err = run_for_error(REF, &args);
    let ours_err = run_for_error(OURS, &args);
    assert!(ref_err.contains("ConditionalCheckFailedException"));
    assert!(ours_err.contains("ConditionalCheckFailedException"));
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_put_all_old_on_overwrite_returns_pre_image() {
    ensure_users_table();
    assert_put_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_put_rvao_ow"}}"##,
        Some(r##"{"id":{"S":"diff_put_rvao_ow"},"label":{"S":"v1"},"role":{"S":"admin"}}"##),
        r##"{"id":{"S":"diff_put_rvao_ow"},"label":{"S":"v2"}}"##,
        &["--return-values", "ALL_OLD"],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_put_all_old_on_insert_omits_attributes() {
    ensure_users_table();
    assert_put_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_put_rvao_fresh"}}"##,
        None,
        r##"{"id":{"S":"diff_put_rvao_fresh"},"label":{"S":"x"}}"##,
        &["--return-values", "ALL_OLD"],
    );
}

// ---- Conditional DeleteItem --------------------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_delete_attribute_exists_succeeds_when_present() {
    ensure_users_table();
    assert_delete_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_del_ae_ok"}}"##,
        Some(r##"{"id":{"S":"diff_del_ae_ok"},"label":{"S":"x"}}"##),
        &["--condition-expression", "attribute_exists(id)"],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_delete_attribute_exists_fails_when_row_missing() {
    ensure_users_table();
    let key = r##"{"id":{"S":"diff_del_ae_missing"}}"##;
    delete_both("users", key);
    let args = [
        "dynamodb", "delete-item",
        "--table-name", "users",
        "--key", key,
        "--condition-expression", "attribute_exists(id)",
    ];
    let ref_err = run_for_error(REF, &args);
    let ours_err = run_for_error(OURS, &args);
    assert!(ref_err.contains("ConditionalCheckFailedException"));
    assert!(ours_err.contains("ConditionalCheckFailedException"));
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_delete_optimistic_lock_matched() {
    ensure_users_table();
    assert_delete_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_del_oplock"}}"##,
        Some(r##"{"id":{"S":"diff_del_oplock"},"version":{"N":"3"}}"##),
        &[
            "--condition-expression", "version = :v",
            "--expression-attribute-values", r##"{":v":{"N":"3"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_delete_all_old_returns_deleted_item() {
    ensure_users_table();
    assert_delete_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_del_rvao"}}"##,
        Some(r##"{"id":{"S":"diff_del_rvao"},"label":{"S":"alice"},"role":{"S":"admin"}}"##),
        &["--return-values", "ALL_OLD"],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_delete_all_old_on_missing_omits_attributes() {
    ensure_users_table();
    assert_delete_round_trip_matches(
        "users",
        r##"{"id":{"S":"diff_del_rvao_missing"}}"##,
        None,
        &["--return-values", "ALL_OLD"],
    );
}

// ===== Key-shape parity (N-PK, B-PK, S+B composite) ============================
//
// These tables let the diff layer cover what the PG-layer tests already
// did but the diff layer didn't: non-string partition keys, binary
// partition keys, and binary sort keys. The body of each test is
// intentionally small — the goal is parity-across-impls for the key
// machinery, not feature coverage that's already exercised against the
// `users` / `device_events` tables.

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_n_pk_round_trip() {
    ensure_counters_table();
    assert_round_trip_matches(
        "counters",
        r#"{"id":{"N":"42"},"tally":{"N":"7"},"label":{"S":"a"}}"#,
        r#"{"id":{"N":"42"}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_n_pk_update_increment() {
    ensure_counters_table();
    assert_update_round_trip_matches(
        "counters",
        r#"{"id":{"N":"100"}}"#,
        Some(r#"{"id":{"N":"100"},"tally":{"N":"5"}}"#),
        &[
            "--update-expression", "ADD tally :n",
            "--expression-attribute-values", r##"{":n":{"N":"3"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_n_pk_conditional_delete_all_old() {
    ensure_counters_table();
    assert_delete_round_trip_matches(
        "counters",
        r#"{"id":{"N":"200"}}"#,
        Some(r#"{"id":{"N":"200"},"label":{"S":"gone"}}"#),
        &[
            "--condition-expression", "attribute_exists(id)",
            "--return-values", "ALL_OLD",
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_b_pk_round_trip() {
    ensure_blobs_table();
    assert_round_trip_matches(
        "blobs",
        r#"{"binmark":{"B":"AAEC/w=="},"label":{"S":"first"}}"#,
        r#"{"binmark":{"B":"AAEC/w=="}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_b_pk_update_set() {
    ensure_blobs_table();
    assert_update_round_trip_matches(
        "blobs",
        r#"{"binmark":{"B":"EREREQ=="}}"#,
        None,
        &[
            "--update-expression", "SET label = :v",
            "--expression-attribute-values", r##"{":v":{"S":"set-on-b-pk"}}"##,
        ],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_b_pk_put_all_old_overwrites() {
    ensure_blobs_table();
    assert_put_round_trip_matches(
        "blobs",
        r#"{"binmark":{"B":"IiIiIg=="}}"#,
        Some(r#"{"binmark":{"B":"IiIiIg=="},"label":{"S":"v1"}}"#),
        r#"{"binmark":{"B":"IiIiIg=="},"label":{"S":"v2"}}"#,
        &["--return-values", "ALL_OLD"],
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_composite_b_sort_round_trip() {
    ensure_binsorted_table();
    assert_round_trip_matches(
        "binsorted",
        r#"{"id":{"S":"box-1"},"binmark":{"B":"MzM="},"label":{"S":"alpha"}}"#,
        r#"{"id":{"S":"box-1"},"binmark":{"B":"MzM="}}"#,
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_composite_b_sort_two_keys_in_same_partition() {
    // Two distinct items sharing the same partition but different B sort
    // keys must coexist under both endpoints.
    ensure_binsorted_table();
    let pk = "shared-1";
    let k1 = format!(r#"{{"id":{{"S":"{pk}"}},"binmark":{{"B":"AAA="}}}}"#);
    let k2 = format!(r#"{{"id":{{"S":"{pk}"}},"binmark":{{"B":"AQE="}}}}"#);
    delete_both("binsorted", &k1);
    delete_both("binsorted", &k2);

    let i1 = format!(r#"{{"id":{{"S":"{pk}"}},"binmark":{{"B":"AAA="}},"label":{{"S":"first"}}}}"#);
    let i2 = format!(r#"{{"id":{{"S":"{pk}"}},"binmark":{{"B":"AQE="}},"label":{{"S":"second"}}}}"#);
    aws(REF, &["dynamodb", "put-item", "--table-name", "binsorted", "--item", &i1]);
    aws(REF, &["dynamodb", "put-item", "--table-name", "binsorted", "--item", &i2]);
    aws(OURS, &["dynamodb", "put-item", "--table-name", "binsorted", "--item", &i1]);
    aws(OURS, &["dynamodb", "put-item", "--table-name", "binsorted", "--item", &i2]);

    for key in [&k1, &k2] {
        let g_ref = aws(REF, &["dynamodb", "get-item", "--table-name", "binsorted", "--key", key]);
        let g_ours = aws(OURS, &["dynamodb", "get-item", "--table-name", "binsorted", "--key", key]);
        assert_eq!(g_ref, g_ours, "GetItem diverged for binsorted/{key}");
    }
}

// ===== Query (Q1) =============================================================
//
// Differential parity tests for Query. Each test seeds a partition on both
// endpoints, issues a Query with the same KCE, and asserts the responses
// match. Sort-set normalization isn't needed since Items here have no
// SS/NS/BS attributes. We DO sort by SK on the rektifier side already (PG
// ORDER BY); DDB-local also sorts by sort-key for Query, so list equality
// holds.

/// Best-effort: delete an item from both endpoints. Allows repeated test
/// runs to reset state regardless of prior outcomes.
fn delete_both_composite(table: &str, pk_attr: &str, pk: &str, sk_attr: &str, sk_av: &str) {
    let key = format!(
        "{{\"{pk_attr}\":{{\"S\":\"{pk}\"}},\"{sk_attr}\":{sk_av}}}"
    );
    delete_both(table, &key);
}

/// Compact assertion: same KCE against both endpoints, expect identical
/// `Items`, `Count`, `ScannedCount`. Tests that supply EAV use the
/// `--expression-attribute-values` flag form via `extra_args`.
fn assert_query_matches(table: &str, kce: &str, eav: &str, extra: &[&str]) {
    let mut args: Vec<&str> = vec![
        "dynamodb",
        "query",
        "--table-name",
        table,
        "--key-condition-expression",
        kce,
        "--expression-attribute-values",
        eav,
    ];
    for a in extra {
        args.push(a);
    }
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(
        q_ref["Count"], q_ours["Count"],
        "Query Count diverged for {table}/{kce}\nref:  {q_ref}\nours: {q_ours}"
    );
    assert_eq!(
        q_ref["ScannedCount"], q_ours["ScannedCount"],
        "Query ScannedCount diverged for {table}/{kce}"
    );
    assert_eq!(
        q_ref["Items"], q_ours["Items"],
        "Query Items diverged for {table}/{kce}\nref:  {q_ref}\nours: {q_ours}"
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_pk_only() {
    ensure_device_events_table();
    // Use a unique pk so this test is idempotent across re-runs.
    let pk = "diff-q-pk-only";
    for ts in ["1000", "2000", "3000"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"N\":\"{ts}\"}}}}"
        );
        let _ = aws(
            REF,
            &["dynamodb", "put-item", "--table-name", "device_events", "--item", &item],
        );
        let _ = aws(
            OURS,
            &["dynamodb", "put-item", "--table-name", "device_events", "--item", &item],
        );
    }
    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    assert_query_matches("device_events", "device_id = :pk", &eav, &[]);

    // Cleanup.
    for ts in ["1000", "2000", "3000"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_sk_equality() {
    ensure_device_events_table();
    let pk = "diff-q-sk-eq";
    for ts in ["10", "20", "30"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"S\":\"x\"}}}}"
        );
        let _ = aws(REF, &["dynamodb", "put-item", "--table-name", "device_events", "--item", &item]);
        let _ = aws(OURS, &["dynamodb", "put-item", "--table-name", "device_events", "--item", &item]);
    }
    let eav = format!(
        "{{\":pk\":{{\"S\":\"{pk}\"}},\":ts\":{{\"N\":\"20\"}}}}"
    );
    assert_query_matches("device_events", "device_id = :pk AND ts = :ts", &eav, &[]);

    for ts in ["10", "20", "30"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_sk_range() {
    ensure_device_events_table();
    let pk = "diff-q-sk-range";
    for ts in ["100", "200", "300", "400", "500"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"S\":\"x\"}}}}"
        );
        let _ = aws(REF, &["dynamodb", "put-item", "--table-name", "device_events", "--item", &item]);
        let _ = aws(OURS, &["dynamodb", "put-item", "--table-name", "device_events", "--item", &item]);
    }
    let eav = format!(
        "{{\":pk\":{{\"S\":\"{pk}\"}},\":lo\":{{\"N\":\"200\"}}}}"
    );
    assert_query_matches("device_events", "device_id = :pk AND ts >= :lo", &eav, &[]);

    for ts in ["100", "200", "300", "400", "500"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_sk_between() {
    ensure_device_events_table();
    let pk = "diff-q-sk-btw";
    for ts in ["100", "200", "300", "400", "500"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"S\":\"x\"}}}}"
        );
        let _ = aws(REF, &["dynamodb", "put-item", "--table-name", "device_events", "--item", &item]);
        let _ = aws(OURS, &["dynamodb", "put-item", "--table-name", "device_events", "--item", &item]);
    }
    let eav = format!(
        "{{\":pk\":{{\"S\":\"{pk}\"}},\":lo\":{{\"N\":\"200\"}},\":hi\":{{\"N\":\"400\"}}}}"
    );
    assert_query_matches(
        "device_events",
        "device_id = :pk AND ts BETWEEN :lo AND :hi",
        &eav,
        &[],
    );

    for ts in ["100", "200", "300", "400", "500"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_sk_begins_with_string() {
    ensure_messages_table();
    let pk = "diff-q-bw";
    for ts in ["2026-01-01", "2026-01-02", "2026-02-01", "2027-01-01"] {
        let item = format!(
            "{{\"thread\":{{\"S\":\"{pk}\"}},\"ts\":{{\"S\":\"{ts}\"}},\"body\":{{\"S\":\"x\"}}}}"
        );
        let _ = aws(REF, &["dynamodb", "put-item", "--table-name", "messages", "--item", &item]);
        let _ = aws(OURS, &["dynamodb", "put-item", "--table-name", "messages", "--item", &item]);
    }
    let eav = format!(
        "{{\":pk\":{{\"S\":\"{pk}\"}},\":pfx\":{{\"S\":\"2026-01\"}}}}"
    );
    assert_query_matches(
        "messages",
        "thread = :pk AND begins_with(ts, :pfx)",
        &eav,
        &[],
    );

    for ts in ["2026-01-01", "2026-01-02", "2026-02-01", "2027-01-01"] {
        delete_both_composite("messages", "thread", pk, "ts", &format!("{{\"S\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_empty_partition() {
    ensure_device_events_table();
    let pk = "diff-q-empty-no-such-partition";
    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    assert_query_matches("device_events", "device_id = :pk", &eav, &[]);
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_unknown_table_both_reject() {
    // Both DDB-local and rektifier should reject a query against a
    // non-existent table — the error class matters more than wording.
    let stderr_ref = aws_expecting_failure(
        REF,
        &[
            "dynamodb",
            "query",
            "--table-name",
            "nope_no_such_table",
            "--key-condition-expression",
            "id = :pk",
            "--expression-attribute-values",
            "{\":pk\":{\"S\":\"x\"}}",
        ],
    );
    let stderr_ours = aws_expecting_failure(
        OURS,
        &[
            "dynamodb",
            "query",
            "--table-name",
            "nope_no_such_table",
            "--key-condition-expression",
            "id = :pk",
            "--expression-attribute-values",
            "{\":pk\":{\"S\":\"x\"}}",
        ],
    );
    assert!(
        stderr_ref.contains("ResourceNotFoundException") || stderr_ref.contains("not found"),
        "DDB-local error mention: {stderr_ref}"
    );
    assert!(
        stderr_ours.contains("ResourceNotFoundException") || stderr_ours.contains("not found"),
        "rektifier error mention: {stderr_ours}"
    );
}

// ===== Query Q2: Limit + ExclusiveStartKey / LastEvaluatedKey ==============

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_limit_sets_lek() {
    ensure_device_events_table();
    let pk = "diff-q-lek";
    for ts in ["1", "2", "3", "4", "5"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"N\":\"{ts}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }

    let q_ref = aws(REF, &[
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}"),
        "--limit","2",
    ]);
    let q_ours = aws(OURS, &[
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}"),
        "--limit","2",
    ]);
    assert_eq!(q_ref["Count"], q_ours["Count"], "Count diverged\nref: {q_ref}\nours: {q_ours}");
    assert_eq!(q_ref["Items"], q_ours["Items"], "Items diverged\nref: {q_ref}\nours: {q_ours}");
    assert_eq!(
        q_ref["LastEvaluatedKey"], q_ours["LastEvaluatedKey"],
        "LEK diverged\nref: {q_ref}\nours: {q_ours}"
    );

    for ts in ["1","2","3","4","5"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_multi_page_traversal() {
    ensure_device_events_table();
    let pk = "diff-q-page";
    for ts in 1..=9 {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"N\":\"{ts}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }

    // Walk both endpoints in lockstep with L=3; both should yield the same
    // Items in the same order and the same LEK at each step.
    let mut esk: Option<Value> = None;
    for page in 1..=5 {
        let mut common: Vec<String> = vec![
            "dynamodb".into(), "query".into(),
            "--table-name".into(), "device_events".into(),
            "--key-condition-expression".into(), "device_id = :pk".into(),
            "--expression-attribute-values".into(),
            format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}"),
            "--limit".into(), "3".into(),
        ];
        if let Some(e) = &esk {
            common.push("--exclusive-start-key".into());
            common.push(e.to_string());
        }
        let args: Vec<&str> = common.iter().map(String::as_str).collect();
        let q_ref = aws(REF, &args);
        let q_ours = aws(OURS, &args);
        assert_eq!(q_ref["Items"], q_ours["Items"], "page {page} Items diverged");
        assert_eq!(q_ref["LastEvaluatedKey"], q_ours["LastEvaluatedKey"], "page {page} LEK diverged");
        match q_ref.get("LastEvaluatedKey") {
            Some(lek) if !lek.is_null() => esk = Some(lek.clone()),
            _ => break,
        }
        if page == 5 {
            panic!("pagination didn't terminate");
        }
    }

    for ts in 1..=9 {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_last_page_no_lek() {
    ensure_device_events_table();
    let pk = "diff-q-lastpg";
    for ts in ["1", "2", "3"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"S\":\"x\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }
    let q_ref = aws(REF, &[
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}"),
        "--limit","10",
    ]);
    let q_ours = aws(OURS, &[
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}"),
        "--limit","10",
    ]);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["Items"], q_ours["Items"]);
    assert!(q_ref.get("LastEvaluatedKey").is_none(), "ref had LEK on last page");
    assert!(q_ours.get("LastEvaluatedKey").is_none(), "ours had LEK on last page");

    for ts in ["1","2","3"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

// ===== Query Q3: FilterExpression vs DDB-local =============================

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_filter_drops_some_rows() {
    ensure_device_events_table();
    let pk = "diff-q-filt-some";
    for ts in 1..=5 {
        let flag = if ts % 2 == 1 { "on" } else { "off" };
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"flag\":{{\"S\":\"{flag}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }

    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":want\":{{\"S\":\"on\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query",
        "--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--filter-expression","flag = :want",
        "--expression-attribute-values", &eav,
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["ScannedCount"], q_ours["ScannedCount"]);
    assert_eq!(q_ref["Items"], q_ours["Items"]);

    for ts in 1..=5 {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_filter_with_limit_lek_when_count_zero() {
    ensure_device_events_table();
    let pk = "diff-q-filt-lim";
    for ts in 1..=5 {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"flag\":{{\"S\":\"off\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }

    let eav = format!(
        "{{\":pk\":{{\"S\":\"{pk}\"}},\":want\":{{\"S\":\"NO_MATCH\"}}}}"
    );
    let args: Vec<&str> = vec![
        "dynamodb","query",
        "--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--filter-expression","flag = :want",
        "--expression-attribute-values",&eav,
        "--limit","2",
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"], "Count diverged\nref: {q_ref}\nours: {q_ours}");
    assert_eq!(q_ref["ScannedCount"], q_ours["ScannedCount"], "ScannedCount diverged");
    assert_eq!(
        q_ref["LastEvaluatedKey"], q_ours["LastEvaluatedKey"],
        "LEK diverged when filter dropped everything\nref: {q_ref}\nours: {q_ours}"
    );

    for ts in 1..=5 {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_filter_attribute_exists() {
    ensure_device_events_table();
    let pk = "diff-q-filt-ae";
    // ts=1 has `score`; ts=2 doesn't.
    let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",
        &format!("{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"1\"}},\"score\":{{\"N\":\"5\"}}}}")]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",
        &format!("{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"1\"}},\"score\":{{\"N\":\"5\"}}}}")]);
    let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",
        &format!("{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"2\"}}}}")]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",
        &format!("{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"2\"}}}}")]);

    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query",
        "--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--filter-expression","attribute_exists(score)",
        "--expression-attribute-values", &eav,
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["ScannedCount"], q_ours["ScannedCount"]);
    assert_eq!(q_ref["Items"], q_ours["Items"]);

    for ts in ["1","2"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

// ===== Scan Q4 vs DDB-local ================================================
//
// DDB Scan order is officially undefined; rektifier returns rows in
// (pk, sk) ascending order for keyset stability. These tests compare
// returned Items as a *set* (after sorting by pk[,sk]) so a divergence
// in row order doesn't fail the parity check.

fn sort_items_by_keys(items: &mut [Value], pk_attr: &str, sk_attr: Option<&str>) {
    items.sort_by(|a, b| {
        let pa = extract_keyish(a, pk_attr);
        let pb = extract_keyish(b, pk_attr);
        let pk_cmp = pa.cmp(&pb);
        if pk_cmp != std::cmp::Ordering::Equal {
            return pk_cmp;
        }
        match sk_attr {
            Some(sk) => extract_keyish(a, sk).cmp(&extract_keyish(b, sk)),
            None => std::cmp::Ordering::Equal,
        }
    });
}

fn extract_keyish(item: &Value, attr: &str) -> String {
    // Stringify the wrapped key value so sort works across S/N (we
    // don't have B keys in these scan fixtures).
    item.get(attr)
        .and_then(|v| v.as_object())
        .and_then(|m| m.iter().next())
        .and_then(|(_, v)| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_users_round_trip() {
    // Both endpoints accumulate state across many test runs (and
    // DDB-local in particular is never reset between sessions), so
    // we can't compare full-table Count/ScannedCount — the absolute
    // counts diverge based on leftover test data. Instead, filter
    // the returned Items down to ones we *just* inserted (matched
    // by pk prefix) and compare that scoped set.
    ensure_users_table();
    let prefix = "diff-scan-u-";
    for n in 1..=3 {
        let item = format!(
            "{{\"id\":{{\"S\":\"{prefix}{n}\"}},\"label\":{{\"S\":\"v{n}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","users","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","users","--item",&item]);
    }

    let q_ref = aws(REF, &["dynamodb","scan","--table-name","users"]);
    let q_ours = aws(OURS, &["dynamodb","scan","--table-name","users"]);

    let scope = |items: &Value, pfx: &str| -> Vec<Value> {
        let mut out: Vec<Value> = items
            .as_array()
            .unwrap()
            .iter()
            .filter(|i| {
                i["id"]["S"]
                    .as_str()
                    .is_some_and(|s| s.starts_with(pfx))
            })
            .cloned()
            .collect();
        sort_items_by_keys(&mut out, "id", None);
        out
    };
    let ref_scoped = scope(&q_ref["Items"], prefix);
    let our_scoped = scope(&q_ours["Items"], prefix);
    assert_eq!(
        ref_scoped, our_scoped,
        "Scan Items diverged after key-sort (scoped to {prefix}*)"
    );
    assert_eq!(ref_scoped.len(), 3);

    for n in 1..=3 {
        delete_both("users", &format!("{{\"id\":{{\"S\":\"{prefix}{n}\"}}}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_composite_round_trip() {
    // Scope to the partition we just seeded; see notes on
    // `diff_scan_users_round_trip` for why we can't compare absolute
    // table-wide counts.
    ensure_device_events_table();
    let pk = "diff-scan-c";
    for ts in ["1","2","3"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"N\":\"{ts}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }

    let q_ref = aws(REF, &["dynamodb","scan","--table-name","device_events"]);
    let q_ours = aws(OURS, &["dynamodb","scan","--table-name","device_events"]);

    let scope = |items: &Value, pk: &str| -> Vec<Value> {
        let mut out: Vec<Value> = items
            .as_array()
            .unwrap()
            .iter()
            .filter(|i| i["device_id"]["S"].as_str() == Some(pk))
            .cloned()
            .collect();
        sort_items_by_keys(&mut out, "device_id", Some("ts"));
        out
    };
    let ref_scoped = scope(&q_ref["Items"], pk);
    let our_scoped = scope(&q_ours["Items"], pk);
    assert_eq!(ref_scoped, our_scoped);
    assert_eq!(ref_scoped.len(), 3);

    for ts in ["1","2","3"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_unknown_table_both_reject() {
    let stderr_ref = aws_expecting_failure(
        REF,
        &["dynamodb","scan","--table-name","nope_no_such_table"],
    );
    let stderr_ours = aws_expecting_failure(
        OURS,
        &["dynamodb","scan","--table-name","nope_no_such_table"],
    );
    assert!(
        stderr_ref.contains("ResourceNotFoundException") || stderr_ref.contains("not found"),
        "DDB-local: {stderr_ref}"
    );
    assert!(
        stderr_ours.contains("ResourceNotFoundException") || stderr_ours.contains("not found"),
        "rektifier: {stderr_ours}"
    );
}

// ===== Q5: Scan paging + filter; Query descending vs DDB-local ============

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_limit_sets_lek() {
    ensure_device_events_table();
    let pk = "diff-scan-lim";
    for ts in 1..=4 {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"N\":\"{ts}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }

    // Both endpoints honor Limit=2. We can't compare LEK pk values
    // verbatim across endpoints — DDB-local picks an internal scan
    // order that may differ from PG's pk-asc order — so just check
    // that *both* set LEK (i.e. neither thinks the scan is done).
    let q_ref = aws(REF, &[
        "dynamodb","scan","--table-name","device_events","--limit","2",
    ]);
    let q_ours = aws(OURS, &[
        "dynamodb","scan","--table-name","device_events","--limit","2",
    ]);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["ScannedCount"], q_ours["ScannedCount"]);
    let ref_has_lek = q_ref.get("LastEvaluatedKey").is_some();
    let ours_has_lek = q_ours.get("LastEvaluatedKey").is_some();
    assert_eq!(
        ref_has_lek, ours_has_lek,
        "LEK presence diverged\nref: {q_ref}\nours: {q_ours}"
    );

    for ts in 1..=4 {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_filter_drops_some_rows() {
    ensure_device_events_table();
    let pk = "diff-scan-filt";
    for ts in 1..=4 {
        let flag = if ts % 2 == 1 { "on" } else { "off" };
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"flag\":{{\"S\":\"{flag}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }

    let eav = "{\":want\":{\"S\":\"on\"}}";
    let args: Vec<&str> = vec![
        "dynamodb","scan","--table-name","device_events",
        "--filter-expression","flag = :want",
        "--expression-attribute-values", eav,
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    // The filter target rows are only the four we inserted for this
    // test; sweeping comparison would pick up other tests' debris.
    // Filter both result sets down to our pk before comparing.
    let our_pk = pk.to_string();
    let count_ref = q_ref["Items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["device_id"]["S"] == our_pk)
        .count();
    let count_ours = q_ours["Items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["device_id"]["S"] == our_pk)
        .count();
    assert_eq!(count_ref, count_ours);
    assert_eq!(count_ref, 2, "expected 2 on/odd rows in our partition");

    for ts in 1..=4 {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_descending_traversal() {
    ensure_device_events_table();
    let pk = "diff-q-desc";
    for ts in 1..=4 {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"N\":\"{ts}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }
    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &eav,
        "--no-scan-index-forward",
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["Items"], q_ours["Items"]);

    for ts in 1..=4 {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

// ===== Q6: Select=COUNT + ProjectionExpression vs DDB-local ===============

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_select_count() {
    ensure_device_events_table();
    let pk = "diff-q-count";
    for ts in 1..=3 {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"val\":{{\"N\":\"{ts}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }

    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &eav,
        "--select","COUNT",
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["ScannedCount"], q_ours["ScannedCount"]);
    // Items: DDB-local returns an empty array; rektifier matches.
    let ref_items_empty = q_ref["Items"].as_array().is_none_or(|a| a.is_empty());
    let ours_items_empty = q_ours["Items"].as_array().is_none_or(|a| a.is_empty());
    assert!(
        ref_items_empty && ours_items_empty,
        "Select=COUNT should not return Items\nref: {q_ref}\nours: {q_ours}"
    );

    for ts in 1..=3 {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_projection_prunes_attributes() {
    ensure_device_events_table();
    let pk = "diff-q-proj";
    let item = format!(
        "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"1\"}},\"val\":{{\"S\":\"v\"}},\"label\":{{\"S\":\"L\"}}}}"
    );
    let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);

    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &eav,
        "--projection-expression","label, val",
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["Items"], q_ours["Items"]);

    delete_both_composite("device_events", "device_id", pk, "ts", "{\"N\":\"1\"}");
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_select_count() {
    // DDB-local accumulates across runs, so a bare Scan + COUNT
    // compares non-deterministic totals. Add a tier-tag attribute
    // unique to this test, then filter on it so both endpoints
    // count exactly the same scoped subset.
    ensure_users_table();
    let prefix = "diff-scan-cnt-";
    let tag = "diff-scan-cnt-tag";
    for n in 1..=3 {
        let item = format!(
            "{{\"id\":{{\"S\":\"{prefix}{n}\"}},\"tier\":{{\"S\":\"{tag}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","users","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","users","--item",&item]);
    }

    let eav = format!("{{\":t\":{{\"S\":\"{tag}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","scan","--table-name","users",
        "--filter-expression","tier = :t",
        "--expression-attribute-values", &eav,
        "--select","COUNT",
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["Count"], 3);
    let ref_empty = q_ref["Items"].as_array().is_none_or(|a| a.is_empty());
    let ours_empty = q_ours["Items"].as_array().is_none_or(|a| a.is_empty());
    assert!(ref_empty && ours_empty);

    for n in 1..=3 {
        delete_both("users", &format!("{{\"id\":{{\"S\":\"{prefix}{n}\"}}}}"));
    }
}

// ===== Integration test gap-closure (audit follow-up) =======================
//
// This block addresses the high-severity gaps surfaced by the
// integration-test audit (see PLAN-5-integration-testing.md). The
// audit-residual / low-severity gaps are tracked in that plan doc;
// the items here are the must-have parity coverage for v1.

// ----- Binary-key Query/Scan ------------------------------------------------

fn ensure_binsorted_table_q5() {
    ensure_binsorted_table();
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_blobs_b_pk_round_trip() {
    ensure_blobs_table();
    // Three binary keys; insert distinct base64 values.
    let keys = ["AAEC", "AAED", "AAEE"];
    for k in keys {
        let item = format!(
            "{{\"binmark\":{{\"B\":\"{k}\"}},\"val\":{{\"S\":\"v\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","blobs","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","blobs","--item",&item]);
    }

    let q_ref = aws(REF, &["dynamodb","scan","--table-name","blobs"]);
    let q_ours = aws(OURS, &["dynamodb","scan","--table-name","blobs"]);
    let mut ref_items = q_ref["Items"].as_array().unwrap().clone();
    let mut our_items = q_ours["Items"].as_array().unwrap().clone();
    // Sort by binmark (base64 string), since DDB Scan order is undefined.
    sort_items_by_keys(&mut ref_items, "binmark", None);
    sort_items_by_keys(&mut our_items, "binmark", None);
    assert_eq!(ref_items, our_items, "Scan on blobs diverged");

    for k in keys {
        delete_both("blobs", &format!("{{\"binmark\":{{\"B\":\"{k}\"}}}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_blobs_b_pk_round_trip() {
    ensure_blobs_table();
    let k = "AAFF";
    let item = format!(
        "{{\"binmark\":{{\"B\":\"{k}\"}},\"label\":{{\"S\":\"alpha\"}}}}"
    );
    let _ = aws(REF, &["dynamodb","put-item","--table-name","blobs","--item",&item]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","blobs","--item",&item]);

    // KCE: binmark = :pk (B-PK equality).
    let eav = format!("{{\":pk\":{{\"B\":\"{k}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","blobs",
        "--key-condition-expression","binmark = :pk",
        "--expression-attribute-values", &eav,
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["Items"], q_ours["Items"], "Query on blobs B-PK diverged");

    delete_both("blobs", &format!("{{\"binmark\":{{\"B\":\"{k}\"}}}}"));
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_binsorted_sb_composite() {
    ensure_binsorted_table_q5();
    let pk = "diff-binsorted";
    let bins = ["AAA=", "AAE=", "AAI="];
    for b in bins {
        let item = format!(
            "{{\"id\":{{\"S\":\"{pk}\"}},\"binmark\":{{\"B\":\"{b}\"}},\"val\":{{\"S\":\"v\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","binsorted","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","binsorted","--item",&item]);
    }

    // Pure pk-only KCE on composite S+B. Both endpoints should return
    // all three rows in B-asc order.
    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","binsorted",
        "--key-condition-expression","id = :pk",
        "--expression-attribute-values", &eav,
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["Items"], q_ours["Items"], "Query on binsorted diverged");

    for b in bins {
        delete_both_composite("binsorted", "id", pk, "binmark", &format!("{{\"B\":\"{b}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_binsorted_begins_with_b_sk_divergence() {
    // PLAN-4 D8: begins_with on a B-typed SK is deferred in rektifier.
    // DDB-local actually accepts the request (returns 0 items in a
    // genuinely empty partition; binary prefix matching is real).
    // This test pins the divergence so we notice if either side
    // changes: rektifier must reject; DDB-local must accept. See
    // COMPATIBILITY_NOTES.md "Query / Scan — begins_with on B-typed
    // sort key" entry.
    ensure_binsorted_table_q5();
    let eav = "{\":pk\":{\"S\":\"diff-binsorted-bw\"},\":pfx\":{\"B\":\"AA==\"}}";

    // rektifier rejects at translate time with a clear error.
    let stderr_ours = aws_expecting_failure(
        OURS,
        &[
            "dynamodb","query","--table-name","binsorted",
            "--key-condition-expression","id = :pk AND begins_with(binmark, :pfx)",
            "--expression-attribute-values", eav,
        ],
    );
    assert!(
        stderr_ours.contains("ValidationException")
            || stderr_ours.contains("begins_with"),
        "rektifier should reject begins_with on B-SK: {stderr_ours}"
    );

    // DDB-local accepts the request (returns Items: [] for an empty
    // partition). Verify the call succeeds — the actual response
    // shape isn't asserted; we just want to confirm the divergence
    // direction hasn't flipped.
    let q_ref = aws(
        REF,
        &[
            "dynamodb","query","--table-name","binsorted",
            "--key-condition-expression","id = :pk AND begins_with(binmark, :pfx)",
            "--expression-attribute-values", eav,
        ],
    );
    assert!(
        q_ref.get("Items").is_some(),
        "DDB-local should accept begins_with on B-SK (was: {q_ref})"
    );
}

// ----- N-PK Query/Scan + ordering ------------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_counters_n_pk() {
    ensure_counters_table();
    for n in [10, 1, 20, 2] {
        let item = format!(
            "{{\"id\":{{\"N\":\"{n}\"}},\"val\":{{\"N\":\"{n}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","counters","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","counters","--item",&item]);
    }
    // Query each one individually (hash-only — Query just returns the
    // single row for that pk).
    for n in [1, 2, 10, 20] {
        let eav = format!("{{\":pk\":{{\"N\":\"{n}\"}}}}");
        let args: Vec<&str> = vec![
            "dynamodb","query","--table-name","counters",
            "--key-condition-expression","id = :pk",
            "--expression-attribute-values", &eav,
        ];
        let q_ref = aws(REF, &args);
        let q_ours = aws(OURS, &args);
        assert_eq!(q_ref["Count"], q_ours["Count"], "N-PK Query Count diverged for {n}");
        assert_eq!(q_ref["Items"], q_ours["Items"], "N-PK Query Items diverged for {n}");
    }

    for n in [1, 2, 10, 20] {
        delete_both("counters", &format!("{{\"id\":{{\"N\":\"{n}\"}}}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_counters_n_pk_ordering() {
    ensure_counters_table();
    // Insert in a non-monotonic order to flush out any lex-vs-numeric
    // ordering bug. PG numeric and DDB N both sort numerically, so
    // `1, 2, 10, 20` (not `1, 10, 2, 20`).
    for n in [10, 1, 20, 2] {
        let item = format!(
            "{{\"id\":{{\"N\":\"{n}\"}},\"val\":{{\"N\":\"{n}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","counters","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","counters","--item",&item]);
    }
    let q_ref = aws(REF, &["dynamodb","scan","--table-name","counters"]);
    let q_ours = aws(OURS, &["dynamodb","scan","--table-name","counters"]);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    // Compare item sets (Scan order is undefined per DDB).
    let mut ref_items = q_ref["Items"].as_array().unwrap().clone();
    let mut our_items = q_ours["Items"].as_array().unwrap().clone();
    sort_items_by_keys(&mut ref_items, "id", None);
    sort_items_by_keys(&mut our_items, "id", None);
    assert_eq!(ref_items, our_items);

    for n in [1, 2, 10, 20] {
        delete_both("counters", &format!("{{\"id\":{{\"N\":\"{n}\"}}}}"));
    }
}

// ----- Type round-trip across all 10 AttributeValue variants ---------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_type_coverage_all_av_variants() {
    ensure_device_events_table();
    let pk = "diff-q-typecov";
    let item = format!(
        "{{\
            \"device_id\":{{\"S\":\"{pk}\"}},\
            \"ts\":{{\"N\":\"1\"}},\
            \"s_attr\":{{\"S\":\"hello\"}},\
            \"n_attr\":{{\"N\":\"3.14\"}},\
            \"b_attr\":{{\"B\":\"AAEC\"}},\
            \"bool_attr\":{{\"BOOL\":true}},\
            \"null_attr\":{{\"NULL\":true}},\
            \"l_attr\":{{\"L\":[{{\"S\":\"a\"}},{{\"N\":\"1\"}}]}},\
            \"m_attr\":{{\"M\":{{\"k\":{{\"S\":\"v\"}}}}}},\
            \"ss_attr\":{{\"SS\":[\"a\",\"b\"]}},\
            \"ns_attr\":{{\"NS\":[\"1\",\"2\"]}},\
            \"bs_attr\":{{\"BS\":[\"AA==\",\"AQ==\"]}}\
        }}"
    );
    let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);

    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    let mut q_ref = aws(REF, &[
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &eav,
    ]);
    let mut q_ours = aws(OURS, &[
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values", &eav,
    ]);
    sort_sets(&mut q_ref);
    sort_sets(&mut q_ours);
    assert_eq!(q_ref["Items"], q_ours["Items"], "Type round-trip via Query diverged");

    delete_both_composite("device_events", "device_id", pk, "ts", "{\"N\":\"1\"}");
}

// ----- FilterExpression operator breadth -----------------------------------
//
// One diff test per major operator family. The translator-side
// reservation rules already cover negative paths; these tests confirm
// each operator produces the same item set against DDB-local and
// rektifier.

fn seed_filter_corpus(pk: &str) {
    ensure_device_events_table();
    // 5 rows with varied attributes covering filter operator surface.
    let rows = [
        // (ts, flag_str, score_n, label_s)
        ("1", "on",  "5",  "alpha"),
        ("2", "off", "15", "beta"),
        ("3", "on",  "25", "gamma"),
        ("4", "off", "35", "delta"),
        ("5", "on",  "45", "epsilon"),
    ];
    for (ts, flag, score, label) in rows {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\
              \"flag\":{{\"S\":\"{flag}\"}},\"score\":{{\"N\":\"{score}\"}},\
              \"label\":{{\"S\":\"{label}\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }
}

fn drop_filter_corpus(pk: &str) {
    for ts in 1..=5 {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_filter_comparison_ops() {
    let pk = "diff-q-filt-cmp";
    seed_filter_corpus(pk);
    // Try four comparison operators against `score`.
    let cases = [
        ("score < :v",  "{\"N\":\"30\"}"),  // 5, 15, 25
        ("score <= :v", "{\"N\":\"25\"}"),  // 5, 15, 25
        ("score > :v",  "{\"N\":\"25\"}"),  // 35, 45
        ("score = :v",  "{\"N\":\"15\"}"),  // 15
    ];
    for (expr, val) in cases {
        let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":v\":{val}}}");
        let args: Vec<&str> = vec![
            "dynamodb","query","--table-name","device_events",
            "--key-condition-expression","device_id = :pk",
            "--filter-expression", expr,
            "--expression-attribute-values", &eav,
        ];
        let q_ref = aws(REF, &args);
        let q_ours = aws(OURS, &args);
        assert_eq!(q_ref["Count"], q_ours["Count"], "diverged for {expr}");
        assert_eq!(q_ref["Items"], q_ours["Items"], "items diverged for {expr}");
    }
    drop_filter_corpus(pk);
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_filter_and_or_not() {
    let pk = "diff-q-filt-bool";
    seed_filter_corpus(pk);
    // DDB rejects unused placeholders in EAV, so each filter
    // expression needs an EAV containing exactly the placeholders
    // it references. Pair filter + EAV per case.
    let cases: [(&str, String); 4] = [
        (
            "flag = :on AND score > :hi",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":on\":{{\"S\":\"on\"}},\":hi\":{{\"N\":\"20\"}}}}"),
        ),
        (
            "flag = :on OR score > :hi",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":on\":{{\"S\":\"on\"}},\":hi\":{{\"N\":\"20\"}}}}"),
        ),
        (
            "NOT (flag = :on)",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":on\":{{\"S\":\"on\"}}}}"),
        ),
        (
            "(flag = :on AND score > :lo) OR label = :tgt",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":on\":{{\"S\":\"on\"}},\":lo\":{{\"N\":\"10\"}},\":tgt\":{{\"S\":\"delta\"}}}}"),
        ),
    ];
    for (expr, eav) in &cases {
        let args: Vec<&str> = vec![
            "dynamodb","query","--table-name","device_events",
            "--key-condition-expression","device_id = :pk",
            "--filter-expression", expr,
            "--expression-attribute-values", eav,
        ];
        let q_ref = aws(REF, &args);
        let q_ours = aws(OURS, &args);
        assert_eq!(q_ref["Count"], q_ours["Count"], "diverged for {expr}");
        assert_eq!(q_ref["Items"], q_ours["Items"], "items diverged for {expr}");
    }
    drop_filter_corpus(pk);
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_filter_between_and_in() {
    let pk = "diff-q-filt-rangeset";
    seed_filter_corpus(pk);
    let cases: [(&str, String); 2] = [
        (
            "score BETWEEN :lo AND :hi",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":lo\":{{\"N\":\"10\"}},\":hi\":{{\"N\":\"30\"}}}}"),
        ),
        (
            "label IN (:a, :g, :e)",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":a\":{{\"S\":\"alpha\"}},\":g\":{{\"S\":\"gamma\"}},\":e\":{{\"S\":\"epsilon\"}}}}"),
        ),
    ];
    for (expr, eav) in &cases {
        let args: Vec<&str> = vec![
            "dynamodb","query","--table-name","device_events",
            "--key-condition-expression","device_id = :pk",
            "--filter-expression", expr,
            "--expression-attribute-values", eav,
        ];
        let q_ref = aws(REF, &args);
        let q_ours = aws(OURS, &args);
        assert_eq!(q_ref["Count"], q_ours["Count"], "diverged for {expr}");
        assert_eq!(q_ref["Items"], q_ours["Items"], "items diverged for {expr}");
    }
    drop_filter_corpus(pk);
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_filter_begins_with_contains_attr_type() {
    let pk = "diff-q-filt-fn";
    seed_filter_corpus(pk);
    let cases: [(&str, String); 3] = [
        (
            "begins_with(label, :pfx)",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":pfx\":{{\"S\":\"al\"}}}}"),
        ),
        (
            "contains(label, :sub)",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":sub\":{{\"S\":\"et\"}}}}"),
        ),
        (
            "attribute_type(label, :ty)",
            format!("{{\":pk\":{{\"S\":\"{pk}\"}},\":ty\":{{\"S\":\"S\"}}}}"),
        ),
    ];
    for (expr, eav) in &cases {
        let args: Vec<&str> = vec![
            "dynamodb","query","--table-name","device_events",
            "--key-condition-expression","device_id = :pk",
            "--filter-expression", expr,
            "--expression-attribute-values", eav,
        ];
        let q_ref = aws(REF, &args);
        let q_ours = aws(OURS, &args);
        assert_eq!(q_ref["Count"], q_ours["Count"], "diverged for {expr}");
        assert_eq!(q_ref["Items"], q_ours["Items"], "items diverged for {expr}");
    }
    drop_filter_corpus(pk);
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_filter_attribute_not_exists() {
    let pk = "diff-q-filt-nex";
    // 3 rows; some have `extra` set, some don't.
    for (ts, has_extra) in [("1", true), ("2", false), ("3", true)] {
        let extra = if has_extra { ",\"extra\":{\"S\":\"x\"}" } else { "" };
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"label\":{{\"S\":\"L\"}}{extra}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }
    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--filter-expression","attribute_not_exists(extra)",
        "--expression-attribute-values", &eav,
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["Items"], q_ours["Items"]);

    for ts in ["1","2","3"] {
        delete_both_composite("device_events", "device_id", pk, "ts", &format!("{{\"N\":\"{ts}\"}}"));
    }
}

// ----- Error / rejection parity --------------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_invalid_limit_zero_both_reject() {
    let stderr_ref = aws_expecting_failure(REF, &[
        "dynamodb","query","--table-name","users",
        "--key-condition-expression","id = :pk",
        "--expression-attribute-values","{\":pk\":{\"S\":\"x\"}}",
        "--limit","0",
    ]);
    let stderr_ours = aws_expecting_failure(OURS, &[
        "dynamodb","query","--table-name","users",
        "--key-condition-expression","id = :pk",
        "--expression-attribute-values","{\":pk\":{\"S\":\"x\"}}",
        "--limit","0",
    ]);
    assert!(
        stderr_ref.contains("ValidationException") || stderr_ref.contains("Limit"),
        "DDB-local didn't reject Limit=0: {stderr_ref}"
    );
    assert!(
        stderr_ours.contains("ValidationException") || stderr_ours.contains("Limit"),
        "rektifier didn't reject Limit=0: {stderr_ours}"
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_invalid_select_both_reject() {
    let stderr_ref = aws_expecting_failure(REF, &[
        "dynamodb","query","--table-name","users",
        "--key-condition-expression","id = :pk",
        "--expression-attribute-values","{\":pk\":{\"S\":\"x\"}}",
        "--select","BOGUS",
    ]);
    let stderr_ours = aws_expecting_failure(OURS, &[
        "dynamodb","query","--table-name","users",
        "--key-condition-expression","id = :pk",
        "--expression-attribute-values","{\":pk\":{\"S\":\"x\"}}",
        "--select","BOGUS",
    ]);
    // The AWS CLI itself may reject BOGUS at the client side; both
    // endpoints (CLI or server) should produce a non-zero exit.
    assert!(!stderr_ref.is_empty(), "DDB-local accepted invalid Select");
    assert!(!stderr_ours.is_empty(), "rektifier accepted invalid Select");
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_count_with_projection_both_reject() {
    // DDB rejects Select=COUNT combined with ProjectionExpression as
    // a ValidationException. Rektifier matches per Q6 validation.
    let stderr_ref = aws_expecting_failure(REF, &[
        "dynamodb","query","--table-name","users",
        "--key-condition-expression","id = :pk",
        "--expression-attribute-values","{\":pk\":{\"S\":\"x\"}}",
        "--select","COUNT",
        "--projection-expression","label",
    ]);
    let stderr_ours = aws_expecting_failure(OURS, &[
        "dynamodb","query","--table-name","users",
        "--key-condition-expression","id = :pk",
        "--expression-attribute-values","{\":pk\":{\"S\":\"x\"}}",
        "--select","COUNT",
        "--projection-expression","label",
    ]);
    assert!(
        stderr_ref.contains("ValidationException"),
        "DDB-local: {stderr_ref}"
    );
    assert!(
        stderr_ours.contains("ValidationException"),
        "rektifier: {stderr_ours}"
    );
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_malformed_esk_wrong_type_both_reject() {
    // Composite events table — provide an ESK with the wrong SK type.
    ensure_device_events_table();
    let bad_esk = "{\"device_id\":{\"S\":\"d\"},\"ts\":{\"S\":\"not-a-num\"}}";
    let stderr_ref = aws_expecting_failure(REF, &[
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values","{\":pk\":{\"S\":\"d\"}}",
        "--exclusive-start-key", bad_esk,
    ]);
    let stderr_ours = aws_expecting_failure(OURS, &[
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-values","{\":pk\":{\"S\":\"d\"}}",
        "--exclusive-start-key", bad_esk,
    ]);
    assert!(!stderr_ref.is_empty(), "DDB-local accepted bad ESK");
    assert!(!stderr_ours.is_empty(), "rektifier accepted bad ESK");
}

// ----- Projection + filter combination -------------------------------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_projection_with_filter() {
    let pk = "diff-q-proj-filt";
    seed_filter_corpus(pk);
    // Filter sees full item (so `score > 20` works), projection
    // returns only label.
    let eav = format!(
        "{{\":pk\":{{\"S\":\"{pk}\"}},\":n\":{{\"N\":\"20\"}}}}"
    );
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--filter-expression","score > :n",
        "--projection-expression","label",
        "--expression-attribute-values", &eav,
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["ScannedCount"], q_ours["ScannedCount"]);
    assert_eq!(q_ref["Items"], q_ours["Items"]);
    // Sanity: every item must have exactly the `label` field.
    for item in q_ours["Items"].as_array().unwrap() {
        let keys: Vec<&str> = item.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["label"], "projected item leaked extra attrs: {keys:?}");
    }
    drop_filter_corpus(pk);
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_projection_with_reserved_word_alias() {
    let pk = "diff-q-proj-resv";
    let item = format!(
        "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"1\"}},\
          \"name\":{{\"S\":\"alice\"}},\"label\":{{\"S\":\"L\"}}}}"
    );
    let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);

    let eav = format!("{{\":pk\":{{\"S\":\"{pk}\"}}}}");
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--expression-attribute-names","{\"#n\":\"name\"}",
        "--expression-attribute-values", &eav,
        "--projection-expression","#n",
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Items"], q_ours["Items"]);

    delete_both_composite("device_events", "device_id", pk, "ts", "{\"N\":\"1\"}");
}

// ----- Select=COUNT + filter (Count vs ScannedCount parity) ----------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_select_count_with_filter() {
    let pk = "diff-q-cnt-filt";
    seed_filter_corpus(pk);
    let eav = format!(
        "{{\":pk\":{{\"S\":\"{pk}\"}},\":on\":{{\"S\":\"on\"}}}}"
    );
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","device_events",
        "--key-condition-expression","device_id = :pk",
        "--filter-expression","flag = :on",
        "--expression-attribute-values", &eav,
        "--select","COUNT",
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    // Both should report Count=3 (on rows), ScannedCount=5 (all rows).
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    assert_eq!(q_ref["ScannedCount"], q_ours["ScannedCount"]);
    drop_filter_corpus(pk);
}

// ----- messages (S+S) — Scan + non-begins_with KCE operators ---------------

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_query_messages_sk_equality() {
    ensure_messages_table();
    let pk = "diff-msgs-eq";
    for ts in ["2026-01-01","2026-01-02","2026-01-03"] {
        let item = format!(
            "{{\"thread\":{{\"S\":\"{pk}\"}},\"ts\":{{\"S\":\"{ts}\"}},\"body\":{{\"S\":\"b\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","messages","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","messages","--item",&item]);
    }
    let eav = format!(
        "{{\":pk\":{{\"S\":\"{pk}\"}},\":ts\":{{\"S\":\"2026-01-02\"}}}}"
    );
    let args: Vec<&str> = vec![
        "dynamodb","query","--table-name","messages",
        "--key-condition-expression","thread = :pk AND ts = :ts",
        "--expression-attribute-values", &eav,
    ];
    let q_ref = aws(REF, &args);
    let q_ours = aws(OURS, &args);
    assert_eq!(q_ref["Items"], q_ours["Items"]);

    for ts in ["2026-01-01","2026-01-02","2026-01-03"] {
        delete_both_composite("messages", "thread", pk, "ts", &format!("{{\"S\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_scan_messages_round_trip() {
    ensure_messages_table();
    let pk = "diff-msgs-scan";
    for ts in ["a","b","c"] {
        let item = format!(
            "{{\"thread\":{{\"S\":\"{pk}\"}},\"ts\":{{\"S\":\"{ts}\"}},\"body\":{{\"S\":\"x\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","messages","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","messages","--item",&item]);
    }
    let q_ref = aws(REF, &["dynamodb","scan","--table-name","messages"]);
    let q_ours = aws(OURS, &["dynamodb","scan","--table-name","messages"]);
    assert_eq!(q_ref["Count"], q_ours["Count"]);
    let mut ref_items = q_ref["Items"].as_array().unwrap().clone();
    let mut our_items = q_ours["Items"].as_array().unwrap().clone();
    sort_items_by_keys(&mut ref_items, "thread", Some("ts"));
    sort_items_by_keys(&mut our_items, "thread", Some("ts"));
    assert_eq!(ref_items, our_items);

    for ts in ["a","b","c"] {
        delete_both_composite("messages", "thread", pk, "ts", &format!("{{\"S\":\"{ts}\"}}"));
    }
}

// ===== BatchGetItem (B1) ======================================================

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_single_table_hash_only() {
    ensure_users_table();
    // Seed three rows on both endpoints.
    let ids = ["bg-a", "bg-b", "bg-c"];
    for id in ids {
        let item = format!("{{\"id\":{{\"S\":\"{id}\"}},\"label\":{{\"S\":\"L-{id}\"}}}}");
        let _ = aws(REF, &["dynamodb","put-item","--table-name","users","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","users","--item",&item]);
    }
    let req_items = "{\"users\":{\"Keys\":[\
        {\"id\":{\"S\":\"bg-a\"}},\
        {\"id\":{\"S\":\"bg-b\"}},\
        {\"id\":{\"S\":\"bg-c\"}}\
    ]}}";
    let r_ref = aws(REF, &["dynamodb","batch-get-item","--request-items",req_items]);
    let r_ours = aws(OURS, &["dynamodb","batch-get-item","--request-items",req_items]);
    // Responses[table] is an unordered array per DDB (and per D9); sort
    // both sides before comparing.
    let mut ref_items = r_ref["Responses"]["users"].as_array().unwrap().clone();
    let mut our_items = r_ours["Responses"]["users"].as_array().unwrap().clone();
    sort_items_by_keys(&mut ref_items, "id", None);
    sort_items_by_keys(&mut our_items, "id", None);
    assert_eq!(ref_items, our_items, "BatchGetItem items diverged");

    for id in ids {
        delete_both("users", &format!("{{\"id\":{{\"S\":\"{id}\"}}}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_missing_keys_silently_omitted() {
    ensure_users_table();
    let item = "{\"id\":{\"S\":\"bg-only-hit\"},\"label\":{\"S\":\"present\"}}";
    let _ = aws(REF, &["dynamodb","put-item","--table-name","users","--item",item]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","users","--item",item]);

    // Mix one hit + two misses; both endpoints should return just the hit.
    let req_items = "{\"users\":{\"Keys\":[\
        {\"id\":{\"S\":\"bg-only-hit\"}},\
        {\"id\":{\"S\":\"bg-miss-1\"}},\
        {\"id\":{\"S\":\"bg-miss-2\"}}\
    ]}}";
    let r_ref = aws(REF, &["dynamodb","batch-get-item","--request-items",req_items]);
    let r_ours = aws(OURS, &["dynamodb","batch-get-item","--request-items",req_items]);
    let mut ref_items = r_ref["Responses"]["users"].as_array().unwrap().clone();
    let mut our_items = r_ours["Responses"]["users"].as_array().unwrap().clone();
    sort_items_by_keys(&mut ref_items, "id", None);
    sort_items_by_keys(&mut our_items, "id", None);
    assert_eq!(ref_items, our_items);
    assert_eq!(ref_items.len(), 1);

    delete_both("users", "{\"id\":{\"S\":\"bg-only-hit\"}}");
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_all_misses_returns_empty_array() {
    ensure_users_table();
    let req_items = "{\"users\":{\"Keys\":[\
        {\"id\":{\"S\":\"bg-miss-only-a\"}},\
        {\"id\":{\"S\":\"bg-miss-only-b\"}}\
    ]}}";
    let r_ref = aws(REF, &["dynamodb","batch-get-item","--request-items",req_items]);
    let r_ours = aws(OURS, &["dynamodb","batch-get-item","--request-items",req_items]);
    // D10: empty per-table response is still present as `[]` on both sides.
    assert!(r_ref["Responses"]["users"].is_array());
    assert!(r_ours["Responses"]["users"].is_array());
    assert_eq!(r_ref["Responses"]["users"].as_array().unwrap().len(), 0);
    assert_eq!(r_ours["Responses"]["users"].as_array().unwrap().len(), 0);
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_composite_key_table() {
    ensure_device_events_table();
    let pk = "bg-comp-pk";
    for ts in ["100","200","300"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"flag\":{{\"S\":\"on\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }
    let req_items = format!(
        "{{\"device_events\":{{\"Keys\":[\
            {{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"100\"}}}},\
            {{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"300\"}}}}\
        ]}}}}"
    );
    let r_ref = aws(REF, &["dynamodb","batch-get-item","--request-items",&req_items]);
    let r_ours = aws(OURS, &["dynamodb","batch-get-item","--request-items",&req_items]);
    let mut ref_items = r_ref["Responses"]["device_events"].as_array().unwrap().clone();
    let mut our_items = r_ours["Responses"]["device_events"].as_array().unwrap().clone();
    sort_items_by_keys(&mut ref_items, "device_id", Some("ts"));
    sort_items_by_keys(&mut our_items, "device_id", Some("ts"));
    assert_eq!(ref_items, our_items);

    for ts in ["100","200","300"] {
        delete_both_composite("device_events","device_id",pk,"ts",&format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_unprocessed_keys_always_empty() {
    ensure_users_table();
    let item = "{\"id\":{\"S\":\"bg-uk\"}}";
    let _ = aws(REF, &["dynamodb","put-item","--table-name","users","--item",item]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","users","--item",item]);

    let req_items = "{\"users\":{\"Keys\":[{\"id\":{\"S\":\"bg-uk\"}}]}}";
    let r_ref = aws(REF, &["dynamodb","batch-get-item","--request-items",req_items]);
    let r_ours = aws(OURS, &["dynamodb","batch-get-item","--request-items",req_items]);
    // D11: both endpoints emit `UnprocessedKeys: {}` on success.
    assert_eq!(r_ref["UnprocessedKeys"], json!({}));
    assert_eq!(r_ours["UnprocessedKeys"], json!({}));

    delete_both("users", "{\"id\":{\"S\":\"bg-uk\"}}");
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_duplicate_keys_both_reject() {
    ensure_users_table();
    let req_items = "{\"users\":{\"Keys\":[\
        {\"id\":{\"S\":\"bg-dup\"}},\
        {\"id\":{\"S\":\"bg-dup\"}}\
    ]}}";
    // Don't pin the message wording — DDB-local's vs ours diverge in
    // detail but both must reject as a 4xx ValidationException.
    let err_ref = aws_expecting_failure(REF, &["dynamodb","batch-get-item","--request-items",req_items]);
    let err_ours = aws_expecting_failure(OURS, &["dynamodb","batch-get-item","--request-items",req_items]);
    assert!(err_ref.to_lowercase().contains("dup") || err_ref.contains("duplicate"),
        "DDB ref didn't mention duplicates: {err_ref}");
    assert!(err_ours.contains("duplicate") || err_ours.contains("Duplicate"),
        "rektifier didn't mention duplicates: {err_ours}");
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_missing_table_both_reject() {
    let req_items = "{\"this-table-does-not-exist\":{\"Keys\":[{\"id\":{\"S\":\"x\"}}]}}";
    let err_ref = aws_expecting_failure(REF, &["dynamodb","batch-get-item","--request-items",req_items]);
    let err_ours = aws_expecting_failure(OURS, &["dynamodb","batch-get-item","--request-items",req_items]);
    // Both should surface ResourceNotFoundException-ish wire shape.
    let both_404ish = |s: &str| {
        s.contains("ResourceNotFound") || s.contains("not found") || s.contains("Cannot do operations")
    };
    assert!(both_404ish(&err_ref), "ref didn't reject missing table: {err_ref}");
    assert!(both_404ish(&err_ours), "ours didn't reject missing table: {err_ours}");
}

// ===== BatchGetItem multi-table (B2) =========================================

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_multi_table() {
    ensure_users_table();
    ensure_device_events_table();
    // Seed both tables on both endpoints.
    for id in ["bg-mt-a", "bg-mt-b"] {
        let item = format!("{{\"id\":{{\"S\":\"{id}\"}},\"label\":{{\"S\":\"L-{id}\"}}}}");
        let _ = aws(REF, &["dynamodb","put-item","--table-name","users","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","users","--item",&item]);
    }
    let pk = "bg-mt-dev";
    for ts in ["10","20"] {
        let item = format!(
            "{{\"device_id\":{{\"S\":\"{pk}\"}},\"ts\":{{\"N\":\"{ts}\"}},\"flag\":{{\"S\":\"on\"}}}}"
        );
        let _ = aws(REF, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
        let _ = aws(OURS, &["dynamodb","put-item","--table-name","device_events","--item",&item]);
    }
    let req_items = "{\"users\":{\"Keys\":[\
        {\"id\":{\"S\":\"bg-mt-a\"}},\
        {\"id\":{\"S\":\"bg-mt-b\"}}\
    ]},\"device_events\":{\"Keys\":[\
        {\"device_id\":{\"S\":\"bg-mt-dev\"},\"ts\":{\"N\":\"10\"}},\
        {\"device_id\":{\"S\":\"bg-mt-dev\"},\"ts\":{\"N\":\"20\"}}\
    ]}}";
    let r_ref = aws(REF, &["dynamodb","batch-get-item","--request-items",req_items]);
    let r_ours = aws(OURS, &["dynamodb","batch-get-item","--request-items",req_items]);

    // Compare each table independently (per-table set equality).
    let mut ref_users = r_ref["Responses"]["users"].as_array().unwrap().clone();
    let mut our_users = r_ours["Responses"]["users"].as_array().unwrap().clone();
    sort_items_by_keys(&mut ref_users, "id", None);
    sort_items_by_keys(&mut our_users, "id", None);
    assert_eq!(ref_users, our_users);

    let mut ref_events = r_ref["Responses"]["device_events"].as_array().unwrap().clone();
    let mut our_events = r_ours["Responses"]["device_events"].as_array().unwrap().clone();
    sort_items_by_keys(&mut ref_events, "device_id", Some("ts"));
    sort_items_by_keys(&mut our_events, "device_id", Some("ts"));
    assert_eq!(ref_events, our_events);

    for id in ["bg-mt-a", "bg-mt-b"] {
        delete_both("users", &format!("{{\"id\":{{\"S\":\"{id}\"}}}}"));
    }
    for ts in ["10","20"] {
        delete_both_composite("device_events","device_id",pk,"ts",&format!("{{\"N\":\"{ts}\"}}"));
    }
}

#[test]
#[ignore = "requires `just up` + `just bootstrap-pg` + a running rektifier on :9000"]
fn diff_batch_get_multi_table_one_empty() {
    ensure_users_table();
    ensure_device_events_table();
    let item = "{\"id\":{\"S\":\"bg-mt-one\"},\"label\":{\"S\":\"only\"}}";
    let _ = aws(REF, &["dynamodb","put-item","--table-name","users","--item",item]);
    let _ = aws(OURS, &["dynamodb","put-item","--table-name","users","--item",item]);
    // device_events keys all miss; expect a present, empty array.
    let req_items = "{\"users\":{\"Keys\":[{\"id\":{\"S\":\"bg-mt-one\"}}]},\
        \"device_events\":{\"Keys\":[\
            {\"device_id\":{\"S\":\"nope\"},\"ts\":{\"N\":\"1\"}}\
        ]}}";
    let r_ref = aws(REF, &["dynamodb","batch-get-item","--request-items",req_items]);
    let r_ours = aws(OURS, &["dynamodb","batch-get-item","--request-items",req_items]);

    // Both endpoints should emit Responses.device_events as an empty
    // array (not omit). D10 parity check.
    assert!(r_ref["Responses"]["device_events"].is_array());
    assert!(r_ours["Responses"]["device_events"].is_array());
    assert_eq!(r_ref["Responses"]["device_events"].as_array().unwrap().len(), 0);
    assert_eq!(r_ours["Responses"]["device_events"].as_array().unwrap().len(), 0);

    delete_both("users", "{\"id\":{\"S\":\"bg-mt-one\"}}");
}
