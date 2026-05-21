//! rektifier — DynamoDB API in front of Postgres.
//!
//! Boot sequence:
//! 1. Initialize structured tracing (`REKTIFIER_LOG` env filter).
//! 2. Load config from `REKTIFIER_CONFIG` (path to TOML) — hard-exit on parse error.
//! 3. Build a deadpool-postgres `Pool` against the configured `database_url`.
//! 4. Ensure `_rektifier_tables` exists (catalog bootstrap).
//! 5. Run one synchronous reconcile pass: introspect each cataloged
//!    table against `information_schema`, flip `serveable` per-table.
//!    Drift on table X no longer blocks startup; the reconciler emits
//!    error-level logs for any unserveable row.
//! 6. Build `AppState { verifier, backend, catalog, ddl }`.
//! 7. Spawn the periodic reconciler task (PLAN-10 KD3).
//! 8. Bind on `listen_addr`, serve `rekt_server::router(state)`.
//!
//! Tables are runtime objects, declared exclusively via the DDB
//! `CreateTable` wire API (PLAN-10 D8). A fresh rektifier process
//! against a fresh PG starts with an empty catalog; the operator
//! issues `CreateTable` to populate it.
//!
//! The auth chain wired in for MVP is `AuthChain::permissive_only()` —
//! SigV4 signatures are NOT validated. Don't expose this to untrusted
//! networks until A2 (strict SigV4) or A3 (JWT) is configured.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, Pool};
use rekt_catalog::{Reconciler, TableCatalog};
use rekt_ddl::PgDdlBackend;
use rekt_config::Config;
use rekt_server::{router, AppState};
use rekt_auth::AuthChain;
use rekt_storage_libpq::PgBackend;
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
    tracing::info!(path = %config_path.display(), "config loaded");

    let pool = build_pool(&config.server.database_url, &config.pg)
        .context("building Postgres pool")?;
    tracing::info!(
        max_pool_size = config.pg.max_pool_size,
        wait_timeout_ms = config.pg.wait_timeout_ms,
        create_timeout_ms = config.pg.create_timeout_ms,
        recycling_method = ?config.pg.recycling_method,
        "PG pool configured"
    );

    rekt_catalog::ensure_metadata_tables(&pool)
        .await
        .context("ensuring _rektifier_tables exists")?;
    rekt_gsi::create_gsi_state_table(&pool)
        .await
        .context("ensuring _rektifier_gsi_state exists")?;

    // PLAN-9 G5+G6: any DualWrite GSI in a non-terminal phase resumes
    // here. The lifecycle coordinator picks up from the persisted
    // `last_pk_copied` (backfilling) or from the recorded `indexing`
    // phase and drives the GSI to ACTIVE.
    let _ = rekt_gsi::boot_resume_in_flight(&pool)
        .await
        .context("resuming in-flight GSI lifecycles")?;

    let catalog = Arc::new(TableCatalog::empty());
    let reconciler = Arc::new(Reconciler::new(
        pool.clone(),
        catalog.clone(),
        std::time::Duration::from_millis(config.catalog.reconcile_interval_ms),
    ));
    // First pass synchronously so the cache is hot before we bind. Any
    // unserveable table is logged at error level inside reconcile_now;
    // we don't refuse to start the process.
    let updated = reconciler
        .reconcile_now()
        .await
        .context("initial catalog reconcile pass")?;
    tracing::info!(
        updated,
        entries = catalog.snapshot().entries.len(),
        "initial reconcile pass complete"
    );
    Arc::clone(&reconciler).spawn_periodic();

    let ddl = Arc::new(PgDdlBackend::new(
        pool.clone(),
        catalog.clone(),
        reconciler,
        config.catalog.invalidate_on_local_ddl,
    ));
    let backend = PgBackend::new(pool).with_retry_policy(rek_retry_policy(&config));
    tracing::info!(
        max_attempts = config.pg.retry.max_attempts,
        initial_backoff_ms = config.pg.retry.initial_backoff_ms,
        max_backoff_ms = config.pg.retry.max_backoff_ms,
        jitter_pct = config.pg.retry.jitter_pct,
        "PG retry policy configured"
    );

    let state = AppState {
        auth: Arc::new(AuthChain::permissive_only()),
        backend: Arc::new(backend),
        catalog,
        ddl,
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

