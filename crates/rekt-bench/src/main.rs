//! rekt-bench — closed-loop load + latency benchmark.
//!
//! Drives PutItem / GetItem / mixed workloads against one of three targets:
//!
//! - `rektifier` — raw HTTP DDB-JSON-1.0 against rektifier on :9000.
//! - `ddb-local` — same wire format against dynamodb-local on :8000.
//! - `direct-pg` — the equivalent SQL via tokio-postgres, no rektifier in
//!   the path. Establishes the latency floor for the rektifier+PG path.
//!
//! Output: per-target percentile latencies (p50/p90/p99/p999/max) and
//! sustained ops/sec, recorded into an HDR histogram during the run.
//!
//! Prereqs (operator owns; see README / justfile):
//!   - PG `users` table created via `just bootstrap-pg`
//!   - rektifier running with `rektifier.toml.example` (for `--target rektifier`)
//!   - dynamodb-local `users` table created (the diff tests' `ensure_ref_table`
//!     does this; the `setup-ddb-local` subcommand also does it here)
//!
//! Scope intentionally narrow: closed-loop only; single item size and working
//! set per run; no OTel/Prometheus export; no real-AWS comparison.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use deadpool_postgres::{Manager, Pool};
use hdrhistogram::Histogram;
use reqwest::Client as HttpClient;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_postgres::types::{Json, Type};
use tokio_postgres::NoTls;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a benchmark.
    Run(RunArgs),
    /// Create the bench table on dynamodb-local (idempotent).
    SetupDdbLocal {
        #[arg(long, default_value = "http://localhost:8000")]
        endpoint: String,
        #[arg(long, default_value = "users")]
        table: String,
    },
}

#[derive(Parser, Clone)]
struct RunArgs {
    #[arg(long, value_enum)]
    target: TargetKind,
    #[arg(long, value_enum)]
    workload: Workload,

    /// Number of concurrent worker tasks.
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
    /// Total run duration after warmup.
    #[arg(long, value_parser = parse_dur, default_value = "30s")]
    duration: Duration,
    /// Warmup duration; samples discarded.
    #[arg(long, value_parser = parse_dur, default_value = "5s")]
    warmup: Duration,
    /// Approximate item payload size in bytes (filler attribute padded to this).
    #[arg(long, default_value_t = 256)]
    item_size: usize,
    /// Number of pre-populated keys for get/mixed workloads.
    #[arg(long, default_value_t = 1_000)]
    working_set: usize,

    // ---- target endpoints ----
    #[arg(long, default_value = "http://localhost:9000")]
    rektifier_url: String,
    #[arg(long, default_value = "http://localhost:8000")]
    ddb_local_url: String,
    #[arg(
        long,
        default_value = "postgres://rektifier:rektifier@localhost:5432/rektifier"
    )]
    database_url: String,

    /// Logical DDB table name. The same name is expected to exist on the
    /// selected target (PG table for rektifier/direct-pg, DDB-local table
    /// for ddb-local). Must have a hash-only `S` PK named `id` and a `data`
    /// jsonb column on the PG side.
    #[arg(long, default_value = "users")]
    table: String,
}

#[derive(Clone, Copy, ValueEnum)]
enum TargetKind {
    Rektifier,
    DdbLocal,
    DirectPg,
}

#[derive(Clone, Copy, ValueEnum, PartialEq)]
enum Workload {
    Put,
    Get,
    Mixed,
}

fn parse_dur(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

// ============================== Target trait ===================================

#[async_trait::async_trait]
trait Target: Send + Sync {
    async fn put(&self, pk: &str, payload: &Value) -> Result<()>;
    async fn get(&self, pk: &str) -> Result<()>;
}

// (We pull `async-trait` only as a transitive macro through reqwest's tower;
// declare it explicitly so we control the version.)

struct HttpTarget {
    client: HttpClient,
    endpoint: String,
    table: String,
}

#[async_trait::async_trait]
impl Target for HttpTarget {
    async fn put(&self, pk: &str, payload: &Value) -> Result<()> {
        let body = json!({
            "TableName": self.table,
            "Item": full_item(pk, payload),
        });
        self.post("DynamoDB_20120810.PutItem", &body).await
    }

    async fn get(&self, pk: &str) -> Result<()> {
        let body = json!({
            "TableName": self.table,
            "Key": {"id": {"S": pk}},
        });
        self.post("DynamoDB_20120810.GetItem", &body).await
    }
}

impl HttpTarget {
    async fn post(&self, target: &str, body: &Value) -> Result<()> {
        let resp = self
            .client
            .post(&self.endpoint)
            .header("X-Amz-Target", target)
            .header("Content-Type", "application/x-amz-json-1.0")
            // Best-effort SigV4-ish header so rektifier / ddb-local accept
            // the request. PermissiveVerifier ignores the signature value;
            // ddb-local doesn't validate. For real AWS this'd be insufficient.
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=local/20260101/us-east-1/dynamodb/aws4_request, \
                 SignedHeaders=content-type;host;x-amz-target, Signature=deadbeef",
            )
            .body(body.to_string())
            .send()
            .await
            .context("HTTP send failed")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("{} on {}: {}", status, target, text);
        }
        // Drain body so the connection can be reused.
        let _ = resp.bytes().await;
        Ok(())
    }
}

struct PgTarget {
    pool: Pool,
    table: String,
}

#[async_trait::async_trait]
impl Target for PgTarget {
    async fn put(&self, pk: &str, payload: &Value) -> Result<()> {
        let item = full_item(pk, payload);
        let item_value = Value::Object(item.into_iter().collect());
        let client = self.pool.get().await.context("pool get")?;
        let sql = format!(
            "INSERT INTO \"{0}\" (data) VALUES ($1) \
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
            self.table
        );
        let stmt = client
            .prepare_typed_cached(&sql, &[Type::JSONB])
            .await
            .context("prepare put")?;
        client
            .execute(&stmt, &[&Json(&item_value)])
            .await
            .context("execute put")?;
        Ok(())
    }

    async fn get(&self, pk: &str) -> Result<()> {
        let client = self.pool.get().await.context("pool get")?;
        let sql = format!("SELECT data FROM \"{0}\" WHERE id = $1", self.table);
        let stmt = client
            .prepare_typed_cached(&sql, &[Type::TEXT])
            .await
            .context("prepare get")?;
        let _row = client
            .query_opt(&stmt, &[&pk])
            .await
            .context("execute get")?;
        Ok(())
    }
}

/// Build the full DDB-JSON item for `pk` with `payload` mixed in.
fn full_item(pk: &str, payload: &Value) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    m.insert("id".into(), json!({"S": pk}));
    if let Value::Object(p) = payload {
        for (k, v) in p {
            m.insert(k.clone(), v.clone());
        }
    }
    m
}

/// Build a payload of approximately `target_bytes` total wire size.
/// We add a single `filler` S-typed attribute padded with ASCII data so
/// the JSON-encoded item is in the requested ballpark.
fn make_payload(target_bytes: usize) -> Value {
    // Account for the wrapping `"filler":{"S":"..."}` (~16 bytes) and the
    // `id` attribute we'll add later (~40 bytes for typical pk).
    let overhead = 60usize;
    let fill_len = target_bytes.saturating_sub(overhead).max(1);
    let filler: String = "x".repeat(fill_len);
    json!({"filler": {"S": filler}})
}

// ============================== Bench runner ===================================

struct Stats {
    histogram: Histogram<u64>,
    errors: u64,
    ops: u64,
}

impl Stats {
    fn new() -> Self {
        Self {
            // 3 sig figs; range up to 60s in microseconds (60_000_000).
            histogram: Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
            errors: 0,
            ops: 0,
        }
    }
}

async fn run(args: RunArgs) -> Result<()> {
    let target: Arc<dyn Target> = build_target(&args).await?;

    // Pre-populate for get/mixed workloads.
    if matches!(args.workload, Workload::Get | Workload::Mixed) {
        eprintln!("populating working set ({} keys)...", args.working_set);
        let payload = make_payload(args.item_size);
        for i in 0..args.working_set {
            let pk = format!("bench-{i:08}");
            target
                .put(&pk, &payload)
                .await
                .with_context(|| format!("populating key {pk}"))?;
        }
    }

    // Build per-worker state.
    let payload = Arc::new(make_payload(args.item_size));
    let warmup_until = Instant::now() + args.warmup;
    let run_until = warmup_until + args.duration;
    let stats = Arc::new(Mutex::new(Stats::new()));
    let target_clone = target.clone();

    eprintln!(
        "running {} ops with concurrency={} for {} (after {} warmup)...",
        match args.workload {
            Workload::Put => "PutItem",
            Workload::Get => "GetItem",
            Workload::Mixed => "mixed 50/50",
        },
        args.concurrency,
        humantime::format_duration(args.duration),
        humantime::format_duration(args.warmup),
    );

    let mut handles = Vec::with_capacity(args.concurrency);
    for worker_id in 0..args.concurrency {
        let target = target_clone.clone();
        let payload = payload.clone();
        let stats = stats.clone();
        let workload = args.workload;
        let working_set = args.working_set;
        handles.push(tokio::spawn(async move {
            worker_loop(
                worker_id,
                target,
                payload,
                workload,
                working_set,
                warmup_until,
                run_until,
                stats,
            )
            .await
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    let stats = stats.lock().await;
    let elapsed = args.duration.as_secs_f64();
    print_report(&args, &stats, elapsed);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    worker_id: usize,
    target: Arc<dyn Target>,
    payload: Arc<Value>,
    workload: Workload,
    working_set: usize,
    warmup_until: Instant,
    run_until: Instant,
    stats: Arc<Mutex<Stats>>,
) {
    let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let mut local_errs = 0u64;
    let mut local_ops = 0u64;

    let mut counter: u64 = 0;
    loop {
        let now = Instant::now();
        if now >= run_until {
            break;
        }
        let recording = now >= warmup_until;

        // Pick the op + key.
        let do_put = match workload {
            Workload::Put => true,
            Workload::Get => false,
            Workload::Mixed => counter % 2 == 0,
        };
        let pk = if do_put {
            // Fresh PK per put avoids hot-key contention in the mixed mode.
            format!("bench-w{worker_id:02}-{counter:08}")
        } else {
            // Sample a populated key uniformly-ish.
            let idx = (counter as usize) % working_set;
            format!("bench-{idx:08}")
        };
        counter = counter.wrapping_add(1);

        let started = Instant::now();
        let result = if do_put {
            target.put(&pk, &payload).await
        } else {
            target.get(&pk).await
        };
        let elapsed_us = started.elapsed().as_micros() as u64;

        if recording {
            local_ops += 1;
            if result.is_err() {
                local_errs += 1;
            } else {
                local_hist.record(elapsed_us.max(1)).ok();
            }
        }
    }

    // Merge into the shared stats.
    let mut s = stats.lock().await;
    s.histogram.add(&local_hist).ok();
    s.errors += local_errs;
    s.ops += local_ops;
}

fn print_report(args: &RunArgs, stats: &Stats, elapsed_s: f64) {
    let ops_per_sec = if elapsed_s > 0.0 {
        stats.ops as f64 / elapsed_s
    } else {
        0.0
    };
    let h = &stats.histogram;
    let target_label = match args.target {
        TargetKind::Rektifier => "rektifier",
        TargetKind::DdbLocal => "ddb-local",
        TargetKind::DirectPg => "direct-pg",
    };
    let workload_label = match args.workload {
        Workload::Put => "PutItem",
        Workload::Get => "GetItem",
        Workload::Mixed => "mixed-50-50",
    };
    println!("\n=== rekt-bench report ===");
    println!("target       = {target_label}");
    println!("workload     = {workload_label}");
    println!("concurrency  = {}", args.concurrency);
    println!("item_size    = {} B", args.item_size);
    println!("working_set  = {}", args.working_set);
    println!(
        "duration     = {}",
        humantime::format_duration(args.duration)
    );
    println!("warmup       = {}", humantime::format_duration(args.warmup));
    println!("total_ops    = {}", stats.ops);
    println!("errors       = {}", stats.errors);
    println!("ops_per_sec  = {ops_per_sec:.1}");
    println!(
        "latency_p50  = {:>7.2} ms",
        us_to_ms(h.value_at_quantile(0.50))
    );
    println!(
        "latency_p90  = {:>7.2} ms",
        us_to_ms(h.value_at_quantile(0.90))
    );
    println!(
        "latency_p99  = {:>7.2} ms",
        us_to_ms(h.value_at_quantile(0.99))
    );
    println!(
        "latency_p999 = {:>7.2} ms",
        us_to_ms(h.value_at_quantile(0.999))
    );
    println!("latency_max  = {:>7.2} ms", us_to_ms(h.max()));
}

fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.0
}

async fn build_target(args: &RunArgs) -> Result<Arc<dyn Target>> {
    match args.target {
        TargetKind::Rektifier => Ok(Arc::new(HttpTarget {
            client: http_client(),
            endpoint: args.rektifier_url.clone(),
            table: args.table.clone(),
        })),
        TargetKind::DdbLocal => Ok(Arc::new(HttpTarget {
            client: http_client(),
            endpoint: args.ddb_local_url.clone(),
            table: args.table.clone(),
        })),
        TargetKind::DirectPg => {
            let pg_config: tokio_postgres::Config = args
                .database_url
                .parse()
                .with_context(|| format!("parsing database_url `{}`", args.database_url))?;
            let manager = Manager::new(pg_config, NoTls);
            let pool = Pool::builder(manager)
                .max_size(args.concurrency.max(4))
                .build()
                .context("pool build")?;
            Ok(Arc::new(PgTarget {
                pool,
                table: args.table.clone(),
            }))
        }
    }
}

fn http_client() -> HttpClient {
    HttpClient::builder()
        .pool_max_idle_per_host(256)
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build reqwest client")
}

async fn setup_ddb_local(endpoint: &str, table: &str) -> Result<()> {
    let client = http_client();
    let body = json!({
        "TableName": table,
        "AttributeDefinitions": [{"AttributeName":"id","AttributeType":"S"}],
        "KeySchema": [{"AttributeName":"id","KeyType":"HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
    });
    let resp = client
        .post(endpoint)
        .header("X-Amz-Target", "DynamoDB_20120810.CreateTable")
        .header("Content-Type", "application/x-amz-json-1.0")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=local/20260101/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=content-type;host;x-amz-target, Signature=deadbeef",
        )
        .body(body.to_string())
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.is_success() {
        eprintln!("created `{table}` on {endpoint}");
    } else if text.contains("ResourceInUseException") || text.contains("already exists") {
        eprintln!("table `{table}` already exists on {endpoint}");
    } else {
        bail!("create-table failed: {status}: {text}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run(args) => run(args).await,
        Cmd::SetupDdbLocal { endpoint, table } => setup_ddb_local(&endpoint, &table).await,
    }
}
