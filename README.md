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
| `CreateTable`        | Implemented (GSI/LSI deferred — PLAN-9)          |
| `DeleteTable`        | Implemented                                      |
| `DescribeTable`      | Implemented                                      |
| `ListTables`         | Implemented                                      |
| `UpdateTable`        | Implemented (metadata fields; GSI ops PLAN-9)    |
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

## Workspace layout

| Crate                  | Purpose                                                |
|------------------------|--------------------------------------------------------|
| `rekt-protocol`        | DDB JSON wire types — AttributeValue, request/response |
| `rekt-sigv4`           | SigV4 request verification                             |
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
