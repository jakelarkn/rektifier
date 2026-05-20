//! Tests for valid configs — defaults, [pg] section, [catalog] section,
//! [limits] inheritance.
//!
//! Table declarations were removed from TOML in PLAN-10 D8; tables are
//! now runtime objects created via the DDB `CreateTable` wire API.

use rekt_config::{load_from_str, Config, Limits};

const FULL: &str = r#"
[server]
listen_addr        = "127.0.0.1:9000"
database_url       = "postgres://rektifier:rektifier@localhost:5432/rektifier"
request_timeout_ms = 15000

[limits]
max_item_size_bytes      = 409600
max_partition_key_bytes  = 2048
max_sort_key_bytes       = 1024
max_attribute_name_bytes = 65536
max_nesting_depth        = 32

[catalog]
reconcile_interval_ms   = 15000
invalidate_on_local_ddl = false
"#;

#[test]
fn parses_full_example() {
    let cfg: Config = load_from_str(FULL).unwrap();
    assert_eq!(cfg.server.listen_addr.to_string(), "127.0.0.1:9000");
    assert_eq!(
        cfg.server.database_url,
        "postgres://rektifier:rektifier@localhost:5432/rektifier"
    );
    assert_eq!(cfg.server.request_timeout_ms, 15_000);
}

#[test]
fn limits_section_applies_to_server_baseline() {
    let cfg = load_from_str(FULL).unwrap();
    assert_eq!(cfg.limits.max_item_size_bytes, Some(409_600));
    assert_eq!(cfg.limits.max_nesting_depth, Some(32));
}

#[test]
fn omitted_server_limits_get_ddb_defaults() {
    let minimal = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"
"#;
    let cfg = load_from_str(minimal).unwrap();
    let ddb = Limits::ddb_defaults();
    assert_eq!(cfg.limits, ddb);
}

#[test]
fn catalog_defaults_when_section_omitted() {
    let toml = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"
"#;
    let cfg = load_from_str(toml).unwrap();
    let defaults = rekt_config::CatalogConfig::defaults();
    assert_eq!(cfg.catalog, defaults);
}

#[test]
fn catalog_overrides_apply() {
    let cfg = load_from_str(FULL).unwrap();
    assert_eq!(cfg.catalog.reconcile_interval_ms, 15_000);
    assert!(!cfg.catalog.invalidate_on_local_ddl);
}

/// `reconcile_interval_ms = 0` is meaningful (disables the periodic
/// timer; KD3 caveat). Verify it round-trips verbatim.
#[test]
fn catalog_zero_interval_is_preserved() {
    let toml = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[catalog]
reconcile_interval_ms = 0
"#;
    let cfg = load_from_str(toml).unwrap();
    assert_eq!(cfg.catalog.reconcile_interval_ms, 0);
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

/// D8: configs no longer carry `[[tables]]`. Confirm a TOML body with
/// a `[[tables]]` block now fails to parse — `RawConfig` was tightened
/// to `deny_unknown_fields`, so the deletion is the regression net.
#[test]
fn legacy_tables_block_rejected_post_d8() {
    let toml = r#"
[server]
listen_addr  = "127.0.0.1:9000"
database_url = "postgres://x"

[[tables]]
name      = "users"
pk_attr   = "id"
jsonb_col = "data"
"#;
    let r = load_from_str(toml);
    assert!(r.is_err(), "expected parse error post-D8");
}
