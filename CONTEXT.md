# rektifier — project context

Resume notes for picking up cold. Captures decisions, not status.

## What this is

A **DynamoDB-compatible API in front of PostgreSQL**. Clients use the AWS DynamoDB SDK / CLI / boto3 unmodified; rektifier translates DynamoDB JSON-over-HTTP requests into SQL against a regular Postgres database.

Name: **rektifier** (deliberate `k`-spelling, plays on "rectifier" as the electrical component that converts one current type to another, plus internet "rekt" / mild vengeance-against-vendor-lock-in flavor).

## Architectural decision: sidecar, not Postgres extension

Two viable shapes were considered:

1. **Postgres extension** (C or Rust+pgrx) — bgworker exposes the DynamoDB API in-process, executes SQL via SPI. Tightest integration; can do logical-decoding for Streams cleanly.
2. **Sidecar proxy** (Rust standalone binary) — speaks DynamoDB HTTP on the front, talks to Postgres via the wire protocol on the back.

**Decision: sidecar.** Reasons:
- Works against managed Postgres (RDS, Cloud SQL, Aurora, Supabase) — those services do **not** allow loading custom extensions, which is most of the addressable market.
- Faster to iterate; no per-PG-major build matrix; no `unsafe` FFI surface.
- Rust async ecosystem (tokio + axum) is a much better fit than C/SPI for HTTP-protocol work.
- Modular code lets us add a pgrx extension later as a *deployment option*, not a rewrite.

We accept losing in-process performance and the cleanest path to DynamoDB Streams (logical-decoding output plugin) in exchange for portability.

## Tech stack

- **Rust** (1.82, pinned via `rust-toolchain.toml`)
- **tokio** — async runtime
- **axum** + **hyper** + **tower** — HTTP server stack
- **tokio-postgres** + **deadpool-postgres** — async PG client + pool
- **aws-sigv4** — request signature verification
- **serde / serde_json** — DynamoDB JSON in/out
- **winnow** or **chumsky** (TBD) — parser combinators for the four DynamoDB expression grammars
- **base64** — for binary `B` AttributeValue encoding and `LastEvaluatedKey` cursors
- **tracing** + **tracing-subscriber** — structured logs
- **thiserror** for library errors, **anyhow** at the binary edges

## Crate layout (Cargo workspace, planned)

Bottom-up. **Strict rule: nothing above `rekt-storage` may depend on `tokio-postgres` or any concrete backend.** This is what keeps the door open for a pgrx extension later.

```
rekt-protocol       AttributeValue enum, request/response types, JSON framing
rekt-sigv4          SigV4 verification (thin wrapper over aws-sigv4)
rekt-expressions    parsers + AST for KeyCondition / Filter / Update / Condition
rekt-translator     DynamoDB op + AST -> SQL fragment + bound params (backend-neutral)
rekt-storage        trait Backend { execute, query, transaction }
rekt-storage-libpq  Backend impl over tokio-postgres
rekt-server         axum routes; dispatches op -> translator -> backend
rekt-control        CreateTable / DescribeTable / DDL emission
rekt-streams        change events via logical replication (sidecar version)
rektifier           binary: wires libpq backend + axum server + sweeper

(LATER, optional extension shape:)
rekt-storage-spi    Backend impl using pgrx SPI
rekt-extension      pgrx bgworker hosting the same rekt-server
```

The `Backend` trait sketch:

```rust
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn execute(&self, sql: &str, params: &[Param]) -> Result<u64, BackendError>;
    async fn query(&self, sql: &str, params: &[Param]) -> Result<Rows, BackendError>;
    async fn transaction<F, R>(&self, f: F) -> Result<R, BackendError>
    where F: for<'tx> FnOnce(&'tx mut dyn Tx) -> BoxFuture<'tx, Result<R, BackendError>> + Send;
}
```

`Param` and `Rows` are our own types — never leak `tokio_postgres::Row` upward.

**Genuinely-leaky abstractions** (acknowledge, don't paper over):
- **Streams**: sidecar uses logical replication subscriber; extension would use logical-decoding output plugin. Different APIs. Share only the `StreamRecord` type and a `trait StreamSource`.
- **Transactions**: hidden behind `Backend::transaction` but the implementations are different shapes underneath.

## Data model

Per-DynamoDB-table Postgres table. Skeleton:

```sql
CREATE TABLE ddb_<tablename> (
  pk    text  NOT NULL,
  sk    text  NOT NULL,                 -- nullable column if table has no sort key
  item  jsonb NOT NULL,                 -- full DynamoDB item, source of truth
  -- generated columns for indexed / typed access (see below)
  PRIMARY KEY (pk, sk)
);
```

**Rule for extraction: extract a value if Postgres needs to reason about it; leave it in JSONB otherwise.**

Always extract:
- Primary key (pk, sk) — required for `PRIMARY KEY`
- GSI / LSI keys — indexes need typed columns; sort semantics matter (`(item->>'price')::numeric` is mandatory for numeric sort keys, since `->>` returns text and `"10" < "9"` lexicographically)
- TTL — `bigint`, indexed, partial WHERE-NOT-NULL

Maybe extract (after profiling):
- Hot atomic counters — to avoid full JSONB document rewrite on `ADD :n`
- Frequently-filtered attributes — push filter into index

Don't extract:
- Random user attributes — defeats the whole point of schemaless. A "Number" attribute may legitimately be a String in another item.
- Sparse / optional attributes — null-bitmap + alignment overhead with no query benefit.

**Use stored generated columns** to derive extracted columns from `item`:

```sql
ttl_epoch bigint    GENERATED ALWAYS AS ((item->>'ttl')::bigint)         STORED,
gsi1_pk   text      GENERATED ALWAYS AS (item->>'email')                 STORED,
gsi1_sk   text      GENERATED ALWAYS AS (item->>'created_at')            STORED,
```

This keeps `item` as the single source of truth — writes only set `item`, the columns derive themselves, no risk of drift.

**Binary handling**: open question. JSONB stores binary as base64 text (~33% bloat). If binary-heavy workloads matter, replace `B` values with `{"$bref": N}` placeholders in JSONB and store actual bytes in a side `bytea[]` column. Skip for v1; profile first.

## Implementation order

1. `rekt-protocol` — AttributeValue, request/response shapes, JSON. Round-trip test against boto3 payloads.
2. `rekt-sigv4` — verify a real signed request. Auth boundary first.
3. `rekt-storage` trait + `rekt-storage-libpq`. Trivial passthrough; `query("SELECT 1")` works.
4. `rekt-translator` for `PutItem`, `GetItem`, `DeleteItem`. Single PK only, no GSIs, no expressions.
5. `rekt-server` + `rektifier` binary. **MVP milestone: `aws dynamodb put-item` then `get-item` against rektifier work end-to-end.**
6. `rekt-expressions` for `KeyConditionExpression` only. Add `Query`.
7. `FilterExpression`, `UpdateExpression`, `ConditionExpression`. The parser long tail.
8. GSIs / LSIs. Adds DDL emission to `rekt-control`.
9. `BatchGetItem`, `BatchWriteItem`, `TransactWriteItems`, `Scan` with parallel segments.
10. TTL sweeper background task.
11. Streams via logical replication.
12. **Optional later**: `rekt-storage-spi` + `rekt-extension` (pgrx). By this point the trait is battle-tested.

## Dev environment

Files at repo root:

- `docker-compose.yml` — Postgres 17 (`rektifier-postgres`, port 5432, user/pass/db = `rektifier`), DynamoDB Local (`rektifier-ddblocal`, port 8000), Adminer (port 8080).
- `Cargo.toml` — empty workspace with shared dep versions.
- `rust-toolchain.toml` — pins Rust 1.82.
- `justfile` — `up`, `down`, `nuke`, `psql`, `build`, `test`, `watch`, `lint`, `fmt`, `run`, `smoke-ref`, `smoke-ours`.
- `.envrc` — `DATABASE_URL`, fake AWS creds, `AWS_ENDPOINT_URL=http://localhost:8000`, `REKTIFIER_LISTEN_ADDR=127.0.0.1:9000`.
- `.gitignore`, `.dockerignore`.
- `README.md` — project blurb + quickstart.

Port plan:
- 5432 — Postgres
- 8000 — `dynamodb-local` (AWS reference emulator)
- 9000 — rektifier (when running)
- 8080 — Adminer

**Differential testing is the gold standard.** Send the same request to both `localhost:8000` (reference) and `localhost:9000` (rektifier); compare responses. dynamodb-local is the official AWS Java emulator; trusting it as ground truth saves enormous effort vs. reading the AWS docs and guessing at edge cases.

Host tools to install (one-time):
```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
brew install just direnv awscli
cargo install cargo-nextest --locked cargo-watch
```

## Reference implementations to crib from

- **ScyllaDB Alternator** (`github.com/scylladb/scylladb`, `alternator/` directory) — the gold-standard OSS DynamoDB-compatible API. Written in C++/Seastar; storage backend is wrong for us (Cassandra-shaped) but the **request parser, SigV4 verifier, and `expressions.cc` / `expressions.g` (the four expression grammars)** are the best free reference for the hardest parts of the problem. Read it on GitHub; don't try to build it.
- **PolarDB-DDB** (Alibaba) — closed-source DynamoDB-on-Postgres in production. Validates the architecture; nothing to read.
- **AuroraWire** — indie dev.to writeup, same shape; design notes only.
- **AWS dynamodb-local** — the reference emulator we'll diff against. Closed source.

## Pending decisions / next steps when we resume

1. **Git init + initial commit** — blocked when the directory was renamed mid-session because the Bash tool's persistent CWD got wedged. After Claude Code restart this will work. Recommended commit:
   ```sh
   cd /Users/jake/swd/rektifier
   git init
   git add .dockerignore .envrc .gitignore Cargo.toml README.md docker-compose.yml justfile rust-toolchain.toml CONTEXT.md
   git commit -m "scaffold rektifier dev environment"
   ```
2. **Verify dev env loop works**: `just up` → `just smoke-ref` should print `{"TableNames": []}` from dynamodb-local.
3. **Create `rekt-protocol` crate** — first real code. Define `AttributeValue` enum (`S`/`N`/`B`/`BOOL`/`NULL`/`L`/`M`/`SS`/`NS`/`BS`), serde round-trip with DynamoDB JSON wire format, round-trip tests against captured boto3 payloads.
4. Then `rekt-sigv4`, then `rekt-storage` trait + libpq impl, then minimal server with `PutItem` / `GetItem`.

## Important constraints to remember

- **Schemaless is the feature.** Don't ever introduce strict typing on attributes (other than declared key/GSI/TTL attributes). DynamoDB lets `score` be a Number in one item and a String in another; rektifier must too.
- **No backend types in upper layers.** `rekt-server` doesn't `use tokio_postgres::*`. CI grep should enforce this.
- **DynamoDB error codes matter.** Clients expect `ResourceNotFoundException`, `ConditionalCheckFailedException`, etc. — not generic 500s. The HTTP layer maps internal errors to DDB error codes at the very top.
- **Test target is "AWS SDK works against rektifier."** Unit tests are necessary but not sufficient. The compatibility test suite (boto3 / aws-sdk-rust / aws CLI driving real requests) is the contract.
