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
    let _tracing_guard = init_tracing();

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

    let pool = build_pool(&config.server.database_url, &config.pg)
        .context("building Postgres pool")?;
    tracing::info!(
        max_pool_size = config.pg.max_pool_size,
        wait_timeout_ms = config.pg.wait_timeout_ms,
        create_timeout_ms = config.pg.create_timeout_ms,
        recycling_method = ?config.pg.recycling_method,
        "PG pool configured"
    );

    rekt_meta::verify(&config.tables, &pool)
        .await
        .context("schema verification failed — refusing to start")?;
    tracing::info!("all declared tables verified against PG schema");

    let schemas = build_schema_map(&config);
    let backend = PgBackend::new(pool).with_retry_policy(rek_retry_policy(&config));
    tracing::info!(
        max_attempts = config.pg.retry.max_attempts,
        initial_backoff_ms = config.pg.retry.initial_backoff_ms,
        max_backoff_ms = config.pg.retry.max_backoff_ms,
        jitter_pct = config.pg.retry.jitter_pct,
        "PG retry policy configured"
    );
    let state = AppState {
        verifier: Arc::new(PermissiveVerifier),
        backend: Arc::new(backend),
        schemas: Arc::new(schemas),
        batch_limits: rek_batch_limits(&config),
        request_timeout: std::time::Duration::from_millis(config.server.request_timeout_ms),
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

/// Opaque guard returned from `init_tracing` that must be held for the
/// lifetime of the program. With the `flame` feature, dropping this guard
/// flushes the span samples to disk; without it, it's a no-op marker.
#[allow(dead_code)]
struct TracingGuard {
    #[cfg(feature = "flame")]
    flame: tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>>,
}

fn init_tracing() -> TracingGuard {
    let filter =
        EnvFilter::try_from_env("REKTIFIER_LOG").unwrap_or_else(|_| EnvFilter::new("info"));

    #[cfg(feature = "flame")]
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        let (flame_layer, flame_guard) = tracing_flame::FlameLayer::with_file("./tracing.folded")
            .expect("opening tracing.folded for write");
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .with(flame_layer)
            .init();
        tracing::info!("tracing-flame ENABLED — sampling to ./tracing.folded");
        return TracingGuard { flame: flame_guard };
    }

    #[cfg(not(feature = "flame"))]
    {
        tracing_subscriber::fmt().with_env_filter(filter).init();
        TracingGuard {}
    }
}

fn build_pool(database_url: &str, pg_cfg: &rekt_config::PgConfig) -> Result<Pool> {
    use deadpool_postgres::{ManagerConfig, RecyclingMethod as DpRecyclingMethod, Runtime};
    let pg_config: tokio_postgres::Config = database_url
        .parse()
        .with_context(|| format!("parsing database_url `{database_url}`"))?;
    let mgr_cfg = ManagerConfig {
        recycling_method: match pg_cfg.recycling_method {
            rekt_config::RecyclingMethod::Fast => DpRecyclingMethod::Fast,
            rekt_config::RecyclingMethod::Verified => DpRecyclingMethod::Verified,
            rekt_config::RecyclingMethod::Clean => DpRecyclingMethod::Clean,
        },
    };
    let manager = Manager::from_config(pg_config, NoTls, mgr_cfg);
    let pool = Pool::builder(manager)
        .runtime(Runtime::Tokio1)
        .max_size(pg_cfg.max_pool_size as usize)
        .wait_timeout(Some(std::time::Duration::from_millis(pg_cfg.wait_timeout_ms)))
        .create_timeout(Some(std::time::Duration::from_millis(
            pg_cfg.create_timeout_ms,
        )))
        .recycle_timeout(Some(std::time::Duration::from_millis(
            pg_cfg.recycle_timeout_ms,
        )))
        .build()
        .context("deadpool Pool::builder")?;
    Ok(pool)
}

fn rek_batch_limits(config: &Config) -> rekt_server::BatchLimits {
    rekt_server::BatchLimits {
        batch_get_max_keys: config.batch_limits.batch_get_max_keys,
        batch_write_max_requests: config.batch_limits.batch_write_max_requests,
        transact_get_max_items: config.batch_limits.transact_get_max_items,
        transact_write_max_items: config.batch_limits.transact_write_max_items,
    }
}

fn rek_retry_policy(config: &Config) -> rekt_storage_libpq::RetryPolicy {
    rekt_storage_libpq::RetryPolicy {
        max_attempts: config.pg.retry.max_attempts,
        initial_backoff: std::time::Duration::from_millis(config.pg.retry.initial_backoff_ms),
        max_backoff: std::time::Duration::from_millis(config.pg.retry.max_backoff_ms),
        jitter_pct: config.pg.retry.jitter_pct,
    }
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
