//! Tests for valid configs — defaults, merges, and per-table overrides.

use rekt_config::{load_from_str, Config, Limits};
use rekt_storage::KeyType;

const FULL: &str = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://rektifier:rektifier@localhost:5432/rektifier"

[limits]
max_item_size_bytes      = 409600
max_partition_key_bytes  = 2048
max_sort_key_bytes       = 1024
max_attribute_name_bytes = 65536
max_nesting_depth        = 32

[[tables]]
name      = "users"
pk_attr   = "id"
jsonb_col = "data"

[[tables]]
name      = "device_events"
pk_attr   = "device_id"
sk_attr   = "ts"
sk_type   = "N"
jsonb_col = "doc"

[[tables]]
name      = "legacy_users"
pg_table  = "users_v2"
pk_attr   = "id"
jsonb_col = "doc"

[[tables]]
name      = "blobs"
pk_attr   = "hash"
pk_type   = "B"
jsonb_col = "meta"

[[tables]]
name      = "documents"
pk_attr   = "doc_id"
jsonb_col = "data"
  [tables.limits]
  max_item_size_bytes      = 16777216
  max_attribute_name_bytes = "unlimited"
"#;

#[test]
fn parses_full_example() {
    let cfg: Config = load_from_str(FULL).unwrap();

    assert_eq!(cfg.server.listen_addr.to_string(), "127.0.0.1:9000");
    assert_eq!(
        cfg.server.database_url,
        "postgres://rektifier:rektifier@localhost:5432/rektifier"
    );
    assert_eq!(cfg.tables.len(), 5);
}

#[test]
fn table_defaults_apply() {
    let cfg = load_from_str(FULL).unwrap();
    let users = cfg.tables.iter().find(|t| t.name == "users").unwrap();
    assert_eq!(users.pg_table, "users", "pg_table should default to name");
    assert_eq!(users.pk_type, KeyType::S, "pk_type should default to S");
    assert!(users.sk_attr.is_none());
    assert!(users.sk_type.is_none());
}

#[test]
fn pg_table_override() {
    let cfg = load_from_str(FULL).unwrap();
    let t = cfg
        .tables
        .iter()
        .find(|t| t.name == "legacy_users")
        .unwrap();
    assert_eq!(t.pg_table, "users_v2");
}

#[test]
fn composite_table_carries_sk() {
    let cfg = load_from_str(FULL).unwrap();
    let t = cfg
        .tables
        .iter()
        .find(|t| t.name == "device_events")
        .unwrap();
    assert_eq!(t.sk_attr.as_deref(), Some("ts"));
    assert_eq!(t.sk_type, Some(KeyType::N));
}

#[test]
fn binary_pk_type() {
    let cfg = load_from_str(FULL).unwrap();
    let t = cfg.tables.iter().find(|t| t.name == "blobs").unwrap();
    assert_eq!(t.pk_type, KeyType::B);
}

#[test]
fn server_limits_apply_to_tables_by_default() {
    let cfg = load_from_str(FULL).unwrap();
    let users = cfg.tables.iter().find(|t| t.name == "users").unwrap();
    assert_eq!(users.limits.max_item_size_bytes, Some(409600));
    assert_eq!(users.limits.max_partition_key_bytes, Some(2048));
    assert_eq!(users.limits.max_sort_key_bytes, Some(1024));
    assert_eq!(users.limits.max_attribute_name_bytes, Some(65536));
    assert_eq!(users.limits.max_nesting_depth, Some(32));
}

#[test]
fn per_table_limit_overrides_and_inheritance() {
    let cfg = load_from_str(FULL).unwrap();
    let docs = cfg.tables.iter().find(|t| t.name == "documents").unwrap();
    // Explicit override.
    assert_eq!(docs.limits.max_item_size_bytes, Some(16_777_216));
    // Explicit "unlimited" removes the cap for this table only.
    assert_eq!(docs.limits.max_attribute_name_bytes, None);
    // Inherited from server.
    assert_eq!(docs.limits.max_partition_key_bytes, Some(2048));
    assert_eq!(docs.limits.max_sort_key_bytes, Some(1024));
    assert_eq!(docs.limits.max_nesting_depth, Some(32));
}

#[test]
fn omitted_server_limits_get_ddb_defaults() {
    let minimal = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "t"
pk_attr   = "id"
jsonb_col = "data"
"#;
    let cfg = load_from_str(minimal).unwrap();
    let t = &cfg.tables[0];
    let ddb = Limits::ddb_defaults();
    assert_eq!(t.limits, ddb);
}

#[test]
fn empty_tables_list_is_allowed() {
    let no_tables = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"
"#;
    let cfg = load_from_str(no_tables).unwrap();
    assert!(cfg.tables.is_empty());
}

#[test]
fn per_table_unlimited_only_affects_that_table() {
    let toml = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "small"
pk_attr   = "id"
jsonb_col = "data"

[[tables]]
name      = "big"
pk_attr   = "id"
jsonb_col = "data"
  [tables.limits]
  max_item_size_bytes = "unlimited"
"#;
    let cfg = load_from_str(toml).unwrap();
    let small = cfg.tables.iter().find(|t| t.name == "small").unwrap();
    let big = cfg.tables.iter().find(|t| t.name == "big").unwrap();
    // Wait — both tables have the same name="id" pk + same jsonb_col, but
    // different table names, and different pg_tables (defaulted). That's
    // legal. Each gets its own limits.
    assert!(small.limits.max_item_size_bytes.is_some());
    assert_eq!(big.limits.max_item_size_bytes, None);
}

// ===== Obs-3: [pg] section ====================================================

#[test]
fn pg_section_omitted_yields_defaults() {
    let toml = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x@y/z"
"#;
    let cfg = load_from_str(toml).unwrap();
    let defaults = rekt_config::PgConfig::defaults();
    assert_eq!(cfg.pg, defaults);
    assert_eq!(cfg.server.request_timeout_ms, 30_000);
}

#[test]
fn pg_section_overrides_apply() {
    let toml = r#"
[server]
listen_addr        = "127.0.0.1:9000"
database_url       = "postgres://x@y/z"
request_timeout_ms = 15000

[pg]
max_pool_size      = 32
wait_timeout_ms    = 2000
create_timeout_ms  = 3000
recycle_timeout_ms = 500
recycling_method   = "verified"

[pg.retry]
max_attempts       = 5
initial_backoff_ms = 25
max_backoff_ms     = 2000
jitter_pct         = 10
"#;
    let cfg = load_from_str(toml).unwrap();
    assert_eq!(cfg.server.request_timeout_ms, 15_000);
    assert_eq!(cfg.pg.max_pool_size, 32);
    assert_eq!(cfg.pg.wait_timeout_ms, 2_000);
    assert_eq!(cfg.pg.create_timeout_ms, 3_000);
    assert_eq!(cfg.pg.recycle_timeout_ms, 500);
    assert_eq!(cfg.pg.recycling_method, rekt_config::RecyclingMethod::Verified);
    assert_eq!(cfg.pg.retry.max_attempts, 5);
    assert_eq!(cfg.pg.retry.initial_backoff_ms, 25);
    assert_eq!(cfg.pg.retry.max_backoff_ms, 2_000);
    assert_eq!(cfg.pg.retry.jitter_pct, 10);
}

#[test]
fn pg_partial_section_inherits_defaults() {
    let toml = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x@y/z"

[pg]
max_pool_size = 64
"#;
    let cfg = load_from_str(toml).unwrap();
    let defaults = rekt_config::PgConfig::defaults();
    assert_eq!(cfg.pg.max_pool_size, 64);
    assert_eq!(cfg.pg.wait_timeout_ms, defaults.wait_timeout_ms);
    assert_eq!(cfg.pg.recycling_method, defaults.recycling_method);
    assert_eq!(cfg.pg.retry, defaults.retry);
}
