# rektifier

A DynamoDB-compatible API sidecar over PostgreSQL. Existing DynamoDB SDK
clients point at rektifier instead of DDB and store data in plain old
Postgres — no code changes.

A *rectifier* is the electrical component that converts one type of
current into another. **rektifier** does the same for protocols:
translates DynamoDB JSON-1.0 wire requests into PG operations so any
DDB SDK or CLI can talk, unmodified, to data living in Postgres.

> Status: pre-production. Not yet recommended for live workloads.
> Wire-level parity is tracked against DDB-local via a diff-test
> harness. Known divergences live in
> [`COMPATIBILITY_NOTES.md`](./COMPATIBILITY_NOTES.md).

## How it works

- HTTP/JSON DynamoDB protocol on the front (axum + tower-http).
- PostgreSQL on the back (tokio-postgres + deadpool-postgres).
- DDB tables are stored in PG as `jsonb` blobs with `GENERATED ALWAYS
  AS ... STORED` columns derived for partition + sort keys, giving the
  storage layer real PG indexes on key paths.
- Tables are *runtime objects*, declared via the DDB `CreateTable`
  wire API. The `_rektifier_tables` PG catalog is the source of truth;
  an in-memory snapshot is refreshed by a background reconciler.

## Supported wire surface

| Operation            | Status                                           |
|----------------------|--------------------------------------------------|
| `PutItem`            | Implemented                                      |
| `GetItem`            | Implemented                                      |
| `DeleteItem`         | Implemented                                      |
| `UpdateItem`         | Implemented (ConditionExpression, all clauses)   |
| `Query`              | Implemented (KeyCondition + FilterExpression)    |
| `Scan`               | Implemented (parallel scan deferred)             |
| `BatchGetItem`       | Implemented                                      |
| `BatchWriteItem`     | Implemented                                      |
| `TransactGetItems`   | Implemented                                      |
| `TransactWriteItems` | Implemented (Put / Delete / Update / Condition)  |
| `CreateTable`        | Implemented (LSIs + GSIs at create time)         |
| `DeleteTable`        | Implemented                                      |
| `DescribeTable`      | Implemented                                      |
| `ListTables`         | Implemented                                      |
| `UpdateTable`        | Implemented (metadata + GSI Create/Delete, online) |
| Streams              | Deferred                                         |
| Backups / TTL / Tags | Deferred                                         |

See [`COMPATIBILITY_NOTES.md`](./COMPATIBILITY_NOTES.md) for the
running list of behavioural divergences, with severity labels
(Lenient / Strict / Observable / Inert / Precision /
Parity-unverified).

## Prereqs

- Docker Desktop
- Rust 1.86 (`curl https://sh.rustup.rs -sSf | sh`)
- `just` (`brew install just`)
- AWS CLI v2 (`brew install awscli`)
- `direnv` (optional; `brew install direnv`)

## Quickstart

```sh
direnv allow              # if using direnv; otherwise `source .envrc`
just up                   # start postgres + dynamodb-local
cargo run --bin rektifier # in another shell
just bootstrap-tables     # provisions the example tables via CreateTable
just smoke-put-get        # PutItem + GetItem sanity check
just down                 # stop services
```

The default config is `rektifier.toml.example`; copy it to
`rektifier.toml` (gitignored) before customising.

## Ports

| Service          | Port  | Purpose                                          |
|------------------|-------|--------------------------------------------------|
| Postgres         | 5432  | Storage backend                                  |
| dynamodb-local   | 8000  | AWS reference emulator (for differential tests)  |
| rektifier        | 9000  | DynamoDB-compatible server                       |
| Adminer          | 8080  | Web UI for Postgres                              |

## Authentication

Clients pick a scheme by their `Authorization` header; rektifier
routes by prefix:

| `Authorization` header                              | Scheme         |
|-----------------------------------------------------|----------------|
| `AWS4-HMAC-SHA256 Credential=AKIA…, …, Signature=…` | Strict SigV4   |
| `Bearer eyJhbGciOi…<JWT>…`                          | JWT + JWKS     |
| `Bearer rekt_pat_…` / `Bearer rekt_svc_…`           | Opaque API token |
| *(missing)*                                         | Permissive (dev only) |

- **SigV4** — wire-compatible with AWS SDKs; secrets stored
  AES-GCM-encrypted in `_rektifier_aws_credentials`, cached in-process.
- **JWT** — multi-issuer with per-key alg pinning. Presets for GCP,
  Azure (Entra ID), Snowflake, Databricks, Neon, AWS Cognito.
- **API tokens** — type-prefixed bearer (`rekt_pat_…` / `rekt_svc_…`),
  HMAC-peppered in `_rektifier_api_tokens`.

Secret material derives from one operator-supplied master key
(`master_key_env` / `master_key_file` / `master_key_kms`). See
[`docs/auth/runbook.md`](./docs/auth/runbook.md) for workflows and
[`docs/auth/role_separation.sql`](./docs/auth/role_separation.sql)
for the PG-role lockdown script.

## Workspace layout

| Crate                  | Purpose                                                |
|------------------------|--------------------------------------------------------|
| `rekt-protocol`        | DDB JSON wire types — AttributeValue, request/response |
| `rekt-auth`            | SigV4 + JWT + opaque-token verifiers (PLAN-13)         |
| `rekt-expressions`     | KeyCondition / Filter / Update / Condition parsers     |
| `rekt-translator`      | DDB op + AST → SQL plan                                |
| `rekt-storage`         | Backend-neutral storage trait                          |
| `rekt-storage-libpq`   | Backend impl over tokio-postgres                       |
| `rekt-catalog`         | Runtime table catalog (cache + metadata + reconciler)  |
| `rekt-ddl`             | DDL orchestration (CreateTable / DeleteTable / UpdateTable) |
| `rekt-server`          | axum routes; dispatch to translator + backend          |
| `rekt-bench`           | Throughput benchmarks                                  |
| `rekt-config`          | TOML loader (server / pg / limits / catalog)           |
| `rektifier`            | Binary; wires everything into AppState                 |

## Performance

The sidecar tax: rektifier adds a roughly **fixed 0.2–0.4 ms per
call** of HTTP + JSON + translation overhead on top of whatever the
underlying PG operation costs. Closed-loop benchmarks against raw
`tokio-postgres` on the same host (PG 17, release build,
concurrency 16, 256-byte items). Full breakdown in
[`docs/perf/`](./docs/perf/).

| Workload                        | direct-pg       | rektifier       | overhead     |
|---------------------------------|-----------------|-----------------|--------------|
| PutItem                         | 14.1k @ 1.06 ms | 11.3k @ 1.37 ms | +0.31 ms p50 |
| GetItem                         | 23.6k @ 0.63 ms | 17.3k @ 0.87 ms | +0.24 ms p50 |
| Scan, bounded (20 rows)         | 15.1k @ 0.99 ms | 11.1k @ 1.34 ms | +0.35 ms p50 |
| Scan, full (1000 rows)          |  616  @ 25.5 ms |  554  @ 28.2 ms | +2.7 ms p50  |
| Update, fast path               | 15.3k @ 1.02 ms |  9.2k @ 1.70 ms | +0.68 ms p50 |
| Update, slow path (read+write)  |  4.8k @ 3.15 ms |  3.6k @ 4.18 ms | +1.03 ms p50 |

The headline: protocol overhead is **a near-constant per-call cost,
not a per-byte or per-row one**. For 1000-row Scans it's ~10% of
total latency; for tiny GetItems it's the larger share. As real PG
work dominates, the rektifier tax becomes statistical noise.

Worth highlighting:

- **Single-statement UpdateItem paths cost the same as PutItem**
  (1.7 ms p50). The translator compiles `SET attr = :v` straight to
  `INSERT … ON CONFLICT DO UPDATE` with no extra round trip.
- **Conditional / read-before-write paths add the expected `BEGIN-tx`
  envelope** (~3 ms direct, ~4 ms via rektifier).
- **Hot-key contention behaves correctly** — `update-tx-rmw-hot`
  shows the expected lock-serialization tail (p99 53 ms) without
  surprises.
- **Zero errors at any tested concurrency.**

Numbers are honest about pre-prod realities: single host, single PG
instance, `PermissiveVerifier` bypasses SigV4, 256-byte items.
Production deployments will hit PG-tuning ceilings well before
rektifier itself becomes the bottleneck.

## Comparison with ExtendDB

ExtendDB (v0.1.0, May 2026) is another Rust DDB-over-Postgres project
shipping a wider feature set: built-in IAM, mandatory SigV4 + TLS,
daemon lifecycle, management console, and Streams/TTL/Import-Export.
Keys are extracted by the engine into typed columns; GSIs are
separate PG tables with optional async maintenance. Rektifier extracts
keys via `GENERATED ALWAYS AS` (for CreateTable-time GSIs/LSIs) or via
a dual-write SQL path + async chunked backfill + `CREATE INDEX
CONCURRENTLY` (for GSIs added later via `UpdateTable`) — both online.
GSI indexes carry an `INCLUDE (data)` covering payload so GSI Query
runs as an index-only scan with no heap fetches. Per-request latency
runs roughly 3–5× lower than ExtendDB on the same PG 17 host, with
the largest gaps on ALL_OLD returns and single-statement UpdateItem
paths. Full breakdown:
[`docs/COMPARISON_VS_EXTENDDB.md`](./docs/COMPARISON_VS_EXTENDDB.md).

## Testing

Four parallel test surfaces:

- `cargo test --workspace` — unit + dispatch tests, no PG needed (uses
  an in-memory `MockBackend`).
- `cargo test -p rekt-storage-libpq -- --ignored` — `#[ignore]`-gated
  integration tests against real PG; needs `just up`.
- `cargo test -p rekt-diff-tests -- --ignored` — parity diff tests vs
  DDB-local; needs `just up` + `just bootstrap-tables` + a running
  rektifier on `:9000`.
- `cargo bench` (via `rekt-bench`) — throughput benchmarks against the
  live server.

## License

AGPL-3.0-or-later. See [`LICENSE`](./LICENSE) for the full text.

This is a copyleft license with the network-service clause (Section
13): if you run a modified rektifier as a service that users interact
with over a network, you must offer those users the corresponding
source. Contact the maintainer if your use case needs different terms.

## Contributing

Pre-production; the wire surface and module layout are still in flux.
Design happens in `docs/plan/PLAN-*.md` (gitignored — these are
local-only design notes, not stable interfaces). See
[`CLAUDE.md`](./CLAUDE.md) for project conventions, test discipline,
and the divergence-tracking workflow.
