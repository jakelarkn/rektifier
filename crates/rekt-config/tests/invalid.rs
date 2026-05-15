//! Tests for invalid configs — every validation rule, one failing case each.

use rekt_config::{load_from_str, ConfigError};

fn err_msg(toml: &str) -> String {
    match load_from_str(toml) {
        Ok(_) => panic!("expected error, got Ok"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn missing_server_section() {
    let r = load_from_str(
        r#"
[[tables]]
name      = "t"
pk_attr   = "id"
jsonb_col = "data"
"#,
    );
    assert!(matches!(r, Err(ConfigError::ParseToml(_))));
}

#[test]
fn unknown_field_in_server() {
    let msg = err_msg(
        r#"
[server]
listen_addr   = "127.0.0.1:9000"
database_url  = "postgres://x"
mystery_field = true
"#,
    );
    assert!(
        msg.contains("unknown field") || msg.contains("mystery_field"),
        "got: {msg}"
    );
}

#[test]
fn unknown_field_in_table() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "t"
pk_attr   = "id"
jsonb_col = "data"
ttl_attr  = "expires_at"
"#,
    );
    assert!(
        msg.contains("unknown field") || msg.contains("ttl_attr"),
        "got: {msg}"
    );
}

#[test]
fn bad_listen_addr() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "not a socket addr"
database_url = "postgres://x"
"#,
    );
    assert!(msg.contains("listen_addr"), "got: {msg}");
}

#[test]
fn empty_database_url() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = ""
"#,
    );
    assert!(msg.contains("database_url"), "got: {msg}");
}

#[test]
fn sk_attr_without_sk_type() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "t"
pk_attr   = "id"
sk_attr   = "ts"
jsonb_col = "doc"
"#,
    );
    assert!(
        msg.contains("sk_attr") && msg.contains("sk_type"),
        "got: {msg}"
    );
}

#[test]
fn sk_type_without_sk_attr() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "t"
pk_attr   = "id"
sk_type   = "N"
jsonb_col = "doc"
"#,
    );
    assert!(
        msg.contains("sk_attr") && msg.contains("sk_type"),
        "got: {msg}"
    );
}

#[test]
fn duplicate_table_name() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "users"
pk_attr   = "id"
jsonb_col = "data"

[[tables]]
name      = "users"
pk_attr   = "id"
jsonb_col = "data2"
"#,
    );
    assert!(
        msg.contains("duplicate table name") && msg.contains("users"),
        "got: {msg}"
    );
}

#[test]
fn duplicate_pg_table() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "users_a"
pg_table  = "shared"
pk_attr   = "id"
jsonb_col = "data"

[[tables]]
name      = "users_b"
pg_table  = "shared"
pk_attr   = "id"
jsonb_col = "data"
"#,
    );
    assert!(
        msg.contains("duplicate pg_table") && msg.contains("shared"),
        "got: {msg}"
    );
}

#[test]
fn bad_pk_type_string() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "t"
pk_attr   = "id"
pk_type   = "Z"
jsonb_col = "data"
"#,
    );
    // serde reports unknown variant; just check the field name appears.
    assert!(
        msg.contains("Z") || msg.to_lowercase().contains("variant"),
        "got: {msg}"
    );
}

#[test]
fn bad_limit_string_is_rejected() {
    let msg = err_msg(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[limits]
max_item_size_bytes = "huge"
"#,
    );
    assert!(msg.contains("unlimited"), "got: {msg}");
}

#[test]
fn negative_limit_rejected() {
    let r = load_from_str(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[limits]
max_item_size_bytes = -100
"#,
    );
    // -100 isn't a u64 nor "unlimited"; serde rejects the integer at parse time.
    assert!(matches!(r, Err(ConfigError::ParseToml(_))));
}

// ===== Obs-3: [pg] validation =================================================

#[test]
fn pg_zero_max_pool_size_rejected() {
    let r = load_from_str(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[pg]
max_pool_size = 0
"#,
    );
    let msg = match r {
        Err(ConfigError::Validation { reason }) => reason,
        other => panic!("expected Validation, got {other:?}"),
    };
    assert!(msg.contains("max_pool_size"), "got: {msg}");
}

#[test]
fn pg_unknown_recycling_method_rejected() {
    let r = load_from_str(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[pg]
recycling_method = "wat"
"#,
    );
    let msg = match r {
        Err(ConfigError::Validation { reason }) => reason,
        other => panic!("expected Validation, got {other:?}"),
    };
    assert!(msg.contains("recycling_method"), "got: {msg}");
}

#[test]
fn pg_retry_initial_gt_max_rejected() {
    let r = load_from_str(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[pg.retry]
initial_backoff_ms = 5000
max_backoff_ms     = 1000
"#,
    );
    let msg = match r {
        Err(ConfigError::Validation { reason }) => reason,
        other => panic!("expected Validation, got {other:?}"),
    };
    assert!(msg.contains("initial_backoff_ms"), "got: {msg}");
}

#[test]
fn pg_retry_jitter_pct_over_100_rejected() {
    let r = load_from_str(
        r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[pg.retry]
jitter_pct = 200
"#,
    );
    let msg = match r {
        Err(ConfigError::Validation { reason }) => reason,
        other => panic!("expected Validation, got {other:?}"),
    };
    assert!(msg.contains("jitter_pct"), "got: {msg}");
}

#[test]
fn server_zero_request_timeout_rejected() {
    let r = load_from_str(
        r#"
[server]
listen_addr        = "127.0.0.1:9000"
database_url       = "postgres://x"
request_timeout_ms = 0
"#,
    );
    let msg = match r {
        Err(ConfigError::Validation { reason }) => reason,
        other => panic!("expected Validation, got {other:?}"),
    };
    assert!(msg.contains("request_timeout_ms"), "got: {msg}");
}
