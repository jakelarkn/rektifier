//! Smoke tests for the config-driven auth wiring. These don't spin up
//! a server; they exercise `build_auth` against a real PG pool +
//! various config shapes to confirm the wiring composes correctly.
//!
//! Run with:
//!   cargo test -p rektifier --test auth_wiring_smoke -- --ignored
//! after `just up`.

use deadpool_postgres::{Config as PgPoolConfig, Pool, Runtime};
use std::net::SocketAddr;
use tokio_postgres::NoTls;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
}

async fn pool() -> Pool {
    let mut cfg = PgPoolConfig::new();
    cfg.url = Some(database_url());
    cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
}

fn parse_config(toml_s: &str) -> rekt_config::Config {
    let full = format!(
        "[server]\nlisten_addr = \"127.0.0.1:9999\"\ndatabase_url = \"{}\"\n\n{toml_s}",
        database_url()
    );
    rekt_config::load_from_str(&full).expect("config parse")
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn sigv4_only_builds_chain() {
    std::env::set_var("REKTIFIER_AUTH_MASTER_KEY", "0".repeat(64));
    let cfg = parse_config(
        r#"
[auth]
master_key_env = "REKTIFIER_AUTH_MASTER_KEY"

[auth.sigv4]
enabled = true
"#,
    );
    let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let pool = pool().await;
    let built = rektifier::auth_wiring::build_auth(&pool, addr, &cfg.auth)
        .await
        .expect("build_auth");
    assert!(built.chain.sigv4.is_some());
    assert!(built.chain.jwt.is_none());
    assert!(built.chain.api_token.is_none());
    assert!(built.chain.permissive.is_none());
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn api_token_only_builds_chain() {
    std::env::set_var("REKTIFIER_AUTH_MASTER_KEY", "0".repeat(64));
    let cfg = parse_config(
        r#"
[auth]
master_key_env = "REKTIFIER_AUTH_MASTER_KEY"

[auth.api_token]
enabled = true
"#,
    );
    let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let pool = pool().await;
    let built = rektifier::auth_wiring::build_auth(&pool, addr, &cfg.auth)
        .await
        .expect("build_auth");
    assert!(built.chain.api_token.is_some());
}

/// Combined sentinel-on / sentinel-off test. The two cases share a
/// process-wide env var, so we run them serially in one tokio test
/// instead of relying on test-thread interleaving.
#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn permissive_sentinel_gate() {
    let cfg = parse_config(
        r#"
[auth.permissive]
enabled = true
"#,
    );
    let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let pool = pool().await;

    // Without sentinel → reject.
    std::env::remove_var("REKTIFIER_ALLOW_UNSAFE_AUTH");
    let err = match rektifier::auth_wiring::build_auth(&pool, addr, &cfg.auth).await {
        Err(e) => e,
        Ok(_) => panic!("expected build_auth to fail"),
    };
    assert!(
        format!("{err:?}").contains("REKTIFIER_ALLOW_UNSAFE_AUTH"),
        "expected sentinel error; got {err:?}"
    );

    // With sentinel → build succeeds.
    std::env::set_var(
        "REKTIFIER_ALLOW_UNSAFE_AUTH",
        "I_UNDERSTAND_THIS_IS_UNSAFE",
    );
    let built = rektifier::auth_wiring::build_auth(&pool, addr, &cfg.auth)
        .await
        .expect("build_auth with sentinel");
    assert!(built.chain.permissive.is_some());
    std::env::remove_var("REKTIFIER_ALLOW_UNSAFE_AUTH");
}

#[tokio::test]
#[ignore = "smoke test — no PG / no network"]
async fn empty_auth_rejects() {
    let cfg = parse_config("");
    let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let pool = pool().await;
    let err = match rektifier::auth_wiring::build_auth(&pool, addr, &cfg.auth).await {
        Err(e) => e,
        Ok(_) => panic!("expected build_auth to fail"),
    };
    let msg = format!("{err:?}");
    assert!(
        msg.contains("no verifier configured"),
        "expected no-verifier error; got {msg}"
    );
}
