//! rektifier — DynamoDB API in front of Postgres.
//!
//! Boot sequence:
//! 1. Initialize structured tracing (`REKTIFIER_LOG` env filter).
//! 2. Load config from `REKTIFIER_CONFIG` (path to TOML) — hard-exit on parse error.
//! 3. Build a deadpool-postgres `Pool` against the configured `database_url`.
//! 4. Verify every declared table's PG schema via `rekt_meta::verify` — hard-exit on mismatch.
//! 5. Build `HashMap<String, TableSchema>` from the verified configs.
//! 6. Build `AppState { verifier, backend, schemas }`.
//! 7. Bind on `listen_addr`, serve `rekt_server::router(state)`.
//!
//! The verifier wired in for MVP is `PermissiveVerifier` — SigV4 signatures
//! are NOT validated. Don't expose this to untrusted networks until
//! `StrictVerifier` lands.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, Pool};
use rekt_config::{Config, TableConfig};
use rekt_server::{router, AppState};
use rekt_sigv4::PermissiveVerifier;
use rekt_storage_libpq::PgBackend;
use rekt_translator::TableSchema;
use tokio_postgres::NoTls;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config_path = std::env::var("REKTIFIER_CONFIG")
        .context("REKTIFIER_CONFIG env var is required (path to TOML config)")?;
    let config_path = PathBuf::from(config_path);
    let config = rekt_config::load_from_path(&config_path)
        .with_context(|| format!("loading config from {}", config_path.display()))?;
    tracing::info!(
        path = %config_path.display(),
        tables = config.tables.len(),
        "config loaded"
    );

    let pool = build_pool(&config.server.database_url).context("building Postgres pool")?;

    rekt_meta::verify(&config.tables, &pool)
        .await
        .context("schema verification failed — refusing to start")?;
    tracing::info!("all declared tables verified against PG schema");

    let schemas = build_schema_map(&config);
    let backend = PgBackend::new(pool);
    let state = AppState {
        verifier: Arc::new(PermissiveVerifier),
        backend: Arc::new(backend),
        schemas: Arc::new(schemas),
    };

    let listener = tokio::net::TcpListener::bind(config.server.listen_addr)
        .await
        .with_context(|| format!("binding {}", config.server.listen_addr))?;
    tracing::warn!(
        addr = %config.server.listen_addr,
        "rektifier listening — PermissiveVerifier is active, do not expose this to untrusted networks"
    );

    axum::serve(listener, router(state))
        .await
        .context("axum server exited with error")?;

    Ok(())
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_env("REKTIFIER_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn build_pool(database_url: &str) -> Result<Pool> {
    let pg_config: tokio_postgres::Config = database_url
        .parse()
        .with_context(|| format!("parsing database_url `{database_url}`"))?;
    let manager = Manager::new(pg_config, NoTls);
    let pool = Pool::builder(manager)
        .max_size(16)
        .build()
        .context("deadpool Pool::builder")?;
    Ok(pool)
}

fn build_schema_map(config: &Config) -> HashMap<String, TableSchema> {
    config
        .tables
        .iter()
        .map(|t| (t.name.clone(), table_config_to_schema(t)))
        .collect()
}

fn table_config_to_schema(t: &TableConfig) -> TableSchema {
    TableSchema {
        name: t.name.clone(),
        pg_table: t.pg_table.clone(),
        pk_attr: t.pk_attr.clone(),
        pk_type: t.pk_type,
        sk_attr: t.sk_attr.clone(),
        sk_type: t.sk_type,
        jsonb_col: t.jsonb_col.clone(),
    }
}
