# ExtendDB vs Rektifier

A detailed comparison of two Rust projects that put a DynamoDB-compatible
wire protocol in front of PostgreSQL. Both speak the same wire format,
both target Postgres as the storage backend, both are Apache-2.0
licensed. Past that, they make almost entirely different decisions about
ownership, operations, schema, and the trust model — and those decisions
show up in the code, the schema, and the latency numbers.

This document captures what we learned by reading both codebases
side-by-side, running both against the same Postgres 17 container, and
benchmarking extenddb with `extenddb-bench` (in this tree) against the
rektifier perf captures (in `../rektifier/docs/perf/`).

## Contents

- [Context](#context) — what each project is for
- [Feature matrix](#feature-matrix) — what they do and don't ship
- [Architecture](#architecture) — the meaningful design differences
  - [Topology](#topology)
  - [PG table shape](#pg-table-shape)
  - [Key extraction: engine vs PG](#key-extraction-engine-vs-pg)
  - [GSI strategy](#gsi-strategy)
  - [Authentication and authorization](#authentication-and-authorization)
  - [Operational model](#operational-model)
- [Performance](#performance) — measured numbers and what they mean
  - [Index workloads](#index-workloads)
- [Tradeoffs and when to pick which](#tradeoffs-and-when-to-pick-which)
- [Repro notes](#repro-notes)
- [References](#references)

## Context

### ExtendDB

> Apache-2.0 licensed clean-room implementation of the DynamoDB wire
> protocol, in Rust (edition 2024, MSRV 1.85), backed by Postgres. Speaks
> SigV4, ships built-in IAM (users/groups/roles/policies), runs as a
> daemonized service with mandatory TLS and a web management console.

The framing is **"replace the DynamoDB service for local, on-prem, and
multi-cloud workloads"**. The CLI looks like a database product:
`extenddb init`, `extenddb serve`, `extenddb stop`, `extenddb status`,
`extenddb migrate`, `extenddb destroy`, `extenddb manage`. The on-disk
storage is owned by extenddb — it creates its own catalog and data
databases on first init. Storage backends are pluggable via a trait
surface (`TableEngine` / `DataEngine` / `MetadataEngine` /
`StreamEngine` / `WorkerStore`) and a registry built on the `inventory`
crate. Postgres is the first implementation; the trait shape leaves the
door open to others.

Current release: v0.1.0, May 2026.

### Rektifier

> "DynamoDB-compatible API in front of PostgreSQL. Speak DynamoDB, store
> in Postgres." A sidecar proxy: HTTP/JSON in, Postgres wire out.

The framing is **"translation layer, not a database"**. The operator
already runs Postgres (managed RDS / Cloud SQL / Aurora / Supabase work
fine — the project rejected the `pgrx` extension shape specifically
because managed PG doesn't allow custom extensions). Rektifier reads
operator-declared tables from `_rektifier_tables` and translates DDB JSON
requests to SQL against those tables. The architectural rule is
explicit in `CONTEXT.md`: "nothing above `rekt-storage` may depend on
`tokio-postgres` or any concrete backend" — preserving a future pgrx
deployment option through a clean type-level boundary.

Current state: pre-1.0 scaffold, plan-document-driven development
(`docs/plan/PLAN-N-*.md`), diff-tested against `dynamodb-local` for
parity.

### The shared elevator pitch ends here

Same wire protocol, same backend, same language. From this point on
they take divergent paths.

## Feature matrix

| Surface | ExtendDB | Rektifier |
|---|---|---|
| DDB CRUD (Put/Get/Delete/Update) | yes | yes |
| Query / Scan | yes | yes |
| BatchGetItem / BatchWriteItem | yes (100 / 25 limits) | yes |
| TransactGetItems / TransactWriteItems | yes (100 / 100) | yes |
| Conditional expressions | yes | yes |
| FilterExpression | yes | yes |
| ProjectionExpression | yes | yes |
| UpdateExpression (SET, REMOVE, ADD, DELETE) | yes | yes (Phase 3a/3b/4c/4d/4e/5) |
| GSI / LSI | yes (separate PG tables, sync or async) | yes (LSI at create time; GSI at create time or online via UpdateTable) |
| Streams | yes (ListStreams, GetRecords, GetShardIterator) | planned (PLAN-X) |
| TTL | yes (background sweeper) | planned |
| Import / Export | yes (S3-shaped wire API; local-FS implementation) | no |
| Tagging | yes | partial |
| DescribeEndpoints / DescribeLimits | yes | yes |
| CreateTable / DeleteTable / UpdateTable | yes (engine-owned DDL) | yes (PLAN-10) — was operator-owned via TOML pre-D8 |
| **SigV4 verification** | mandatory, full | optional via `PermissiveVerifier` |
| **TLS** | mandatory, self-signed cert auto-generated | not enforced; runs HTTP by default |
| **Auth: IAM users/groups/roles/policies** | full implementation | none |
| Permissions boundaries | yes | none |
| Access keys with rotation | yes | none |
| **Multi-account isolation** | yes (`account_id` scopes everything) | none (one namespace) |
| **Management API + web console** | yes (`/console/`, IAM admin via CLI) | none |
| Daemon lifecycle (start/stop/status) | yes (`daemonize` crate) | foreground binary |
| Syslog logging | yes (`syslog-tracing`, journalctl) | tracing to stdout |
| Prometheus metrics | yes (`/metrics`) | none |
| CSRF, security headers, session management | yes (console) | n/a |
| Backup / restore | yes (`backups` / `backup_items` tables) | no |
| Compile-time storage backend selection | yes (cargo feature) | n/a (single backend) |

The asymmetry is large and intentional. Rektifier is a translation
layer that assumes the operator runs the operational surface
(database, auth, TLS termination, monitoring). ExtendDB is a product
that takes ownership of all of those.

## Architecture

### Topology

**ExtendDB** is seven crates:

```
extenddb (bin)              CLI, config, daemon lifecycle
  └─ extenddb-server        HTTP+TLS server, management API, console
       ├─ extenddb-engine   per-op handlers (PutItem, Query, Scan, …)
       │    ├─ extenddb-core      types, expressions, validation (sync)
       │    └─ extenddb-storage   trait definitions (TableEngine, …)
       ├─ extenddb-auth     SigV4, IAM policy engine
       └─ extenddb-storage-postgres   the PG TableEngine implementation
```

`extenddb-storage-postgres` registers itself with `inventory` at
compile time; the bin picks up the backend by name from config. Other
backends could be added without touching the engine or server.

**Rektifier** is twelve crates plus a diff-test crate:

```
rektifier (bin)             wires libpq backend + axum server + bench
  └─ rekt-server            axum routes; dispatches op → translator → backend
       ├─ rekt-protocol     AttributeValue + JSON wire framing
       ├─ rekt-sigv4        SigV4 verification (or permissive)
       ├─ rekt-config       TOML config loader
       ├─ rekt-catalog      runtime table catalog + reconciler
       ├─ rekt-ddl          CreateTable / DescribeTable / DDL emission
       ├─ rekt-translator   DDB op + AST → SQL fragment + bound params
       │    └─ rekt-expressions   parsers for the 4 expression grammars
       ├─ rekt-storage      trait Backend (backend-neutral, no PG types)
       └─ rekt-storage-libpq  Backend impl over tokio-postgres
```

Rektifier's signature move is the **`rekt-translator` crate**: 3.3K
lines of backend-neutral planning that produces typed plan structs
(`PutItemPlan`, `UpdateItemPlan`, …) which the libpq backend then
lowers to SQL. ExtendDB does not have an equivalent — its engine
crate calls storage traits directly, and SQL emission happens inside
`storage-postgres`. The translator layer is rektifier's *type-system
enforcement* of the "no PG types above storage" rule that CONTEXT.md
mandates. ExtendDB enforces the same separation by trait shape
(method signatures take typed Rust values, not SQL strings) but
doesn't materialize an intermediate plan IR.

### PG table shape

**This is the central design difference.** Both stacks store the
item as `JSONB`, but they extract keys completely differently.

#### Rektifier

One Postgres table per declared DDB table, named by the operator
(e.g. `users`). Skeleton:

```sql
CREATE TABLE users (
    item JSONB NOT NULL,
    -- generated columns derived from item (PK/SK + LSI sort cols +
    -- CreateTable-time GSI cols):
    pk    text    GENERATED ALWAYS AS (item->>'id')               STORED,
    sk_n  numeric GENERATED ALWAYS AS ((item->>'ts')::numeric)    STORED,  -- if N sort key
    gsi1_pk text  GENERATED ALWAYS AS (item->>'email')            STORED,  -- per-CT-time GSI
    -- GSIs added later via UpdateTable are regular columns,
    -- populated by dual-write SQL on every INSERT/UPDATE:
    gsi_late_pk text NULL,  -- per-UpdateTable-time GSI
    PRIMARY KEY (pk[, sk_*])
);
```

Postgres derives the typed key columns from the JSONB on every write.
Rektifier *never* binds key values on insert paths — it just writes
JSONB and the generated columns recompute. Numeric sort keys cast to
`numeric` so PG can sort them correctly. Binary keys cast to `bytea`.
String keys stay as `text`. The cast at DDL time is what keeps
schemaless workloads honest: a number stored as text would sort
lexicographically (`"10" < "9"`) which is wrong for DDB N keys.

#### ExtendDB

One PG table per DDB table, *one per index*, all named
`_ddb_<uuid>`. Skeleton for a single-SK base table:

```sql
CREATE TABLE "_ddb_<uuid>" (
    pk         TEXT NOT NULL,
    sk_s       TEXT,
    sk_n       NUMERIC,
    sk_b       BYTEA,
    item_data  JSONB NOT NULL,
    PRIMARY KEY (pk, sk_<chosen>)
);
```

Three typed sort-key columns (`sk_s`, `sk_n`, `sk_b`) exist on every
row; the engine populates only the one matching the declared type.
The PRIMARY KEY constraint references just the populated column.
Multi-part RANGE keys add `sk2_s/sk2_n/sk2_b`, `sk3_*`, etc.

Partition keys are *always* stored as TEXT regardless of declared
type. Numeric PKs are coerced to their decimal string representation;
binary PKs are base64-encoded. Multi-attribute partition keys are
netstring-encoded into one TEXT column
(`encode_netstring_composite` at `crates/storage/src/util/key.rs:132`).
The PK btree is a `text` btree, not a `numeric` btree even when the
DDB key type is N.

See `crates/storage-postgres/src/data/ddl.rs:42` for the DDL
template, and `crates/storage/src/util/key.rs` for the extraction
helpers.

### Key extraction: engine vs PG

The schema difference above is really about **who owns key
extraction**. PG, or the engine?

**Rektifier**: PG owns it. `GENERATED ALWAYS AS … STORED` columns
mean the database literally cannot have a row where the key columns
disagree with the JSONB. Every write is one SQL statement; PG
recomputes the generated columns on every insert/update. If a future
extenddb-style sidecar wanted to share rektifier-shaped tables, it
would inherit the same correctness guarantee for free, because the
guarantee is in the schema.

**ExtendDB**: the engine owns it. Every PutItem call extracts the
PK and SK values from the inbound `Item`, walks the key schema,
calls `composite_pk_to_text(item, key_schema)`, calls `parse_sk(av,
sk_type)`, picks the right `sk_<type>` column with `sk_column(type)`,
and binds the values as separate SQL parameters. The JSONB and the
key columns are populated by separate parameter binds in the same
INSERT. If a bug in `composite_pk_to_text` writes the wrong text,
the row's data and key columns disagree and PG cannot detect it.

#### What this trades

| | extenddb (engine-extracts) | rektifier (PG-derives at CT; SQL-derives at UpdateTable) |
|---|---|---|
| Invariant enforcement | engine code (testable, not load-bearing in PG) | PG schema for CT-time GSIs/LSIs/PK-SK; same INSERT statement for UpdateTable-time GSIs |
| Adding a GSI on a large table | `CREATE TABLE _ddb_<idx_uuid>` + backfill | `ALTER TABLE ADD COLUMN x text NULL` (catalog-only) + async chunked backfill + `CREATE INDEX CONCURRENTLY` — online |
| Schemaless attribute that's also a key | OK — engine converts whatever's there | sparse — items missing the attribute leave the column NULL; cast errors only when the attribute is present with the wrong DDB type |
| Renaming a table | catalog row update, O(1) | requires rewriting the PG table name |
| Multi-tenancy on shared PG | free — each account's tables live under unique UUIDs | name collisions in shared catalog |
| Per-row physical size | larger (typed-col triplets, base_pk on indexes) | base heap is small (one JSONB + generated leaves); each ALL-projection GSI's btree carries the JSONB payload in its leaves (PLAN-12 covering index), so total storage per GSI converges with extenddb's per-GSI-table shape |

Neither is obviously right. ExtendDB optimizes for operational
flexibility (account isolation, schemaless freedom, fast GSI add);
rektifier optimizes for correctness-by-construction (PG-enforced
key invariants on the base + LSI + Generated-mode columns) and a
single-namespace base-heap layout.

### GSI strategy

Both stacks treat GSIs as projections of the base table, but they
materialize the projection differently.

**Rektifier**: a GSI is one or more PG columns on the base table
plus a `CREATE INDEX` on those columns. Two modes, selected by origin:

- *CreateTable-time GSIs* use `GENERATED ALWAYS AS … STORED` columns
  (PG owns the JSONB→column invariant). Synchronous; ACTIVE before
  CreateTable returns.
- *UpdateTable.Create GSIs* use regular nullable columns populated
  by a dual-write SQL fragment (`($1::jsonb #>> '{attr,T}')`) inside
  the same INSERT/UPDATE that writes JSONB — equivalent semantics to
  GENERATED, but living in the statement rather than the column
  definition so `ADD COLUMN` stays catalog-only. Async lifecycle:
  chunked backfill on existing rows + `CREATE INDEX CONCURRENTLY` →
  ACTIVE; pre-ACTIVE Query returns RNF (matches DDB).

Reading via either mode is identical: both indexes are *covering*
via PG's `INCLUDE (data)` clause on `CREATE INDEX`. The index leaf
carries the full JSONB payload alongside the key columns, so PG's
planner picks an index-only scan and skips heap fetches entirely
(`Heap Fetches: 0` in `EXPLAIN ANALYZE`). The trade-off — larger
GSI btree, modest +0.05 ms p50 per-Put cost from the WAL'd payload —
is documented in PLAN-12. The cost-of-using-the-GSI promise the
operator sees on the wire (DDB's `Projection = ALL` semantic, which
rektifier honors universally today) is now materialized in the
on-disk shape.

**ExtendDB**: a GSI is a **separate PG table**, `_ddb_<index_uuid>`,
with the GSI keys as its primary identifier, the base table's keys
appended for uniqueness, and the *projected item* (KEYS_ONLY /
INCLUDE / ALL) stored as JSONB. See `crates/storage-postgres/src/
data/ddl.rs:139` (`create_index_data_table`) and
`crates/storage-postgres/src/data/index.rs:278`
(`insert_index_row_multi`).

Index maintenance is engine-driven on every put/update/delete. The
engine computes the old and new projected items and either deletes-
then-inserts (key changed), updates the projected `item_data` in
place (key unchanged), or no-ops (sparse index — the item lacks the
GSI key attributes, which DDB allows).

ExtendDB additionally distinguishes LSI from GSI by **consistency
contract**, not by table shape:

- **LSIs are always synchronous** — written in the same transaction
  as the base row, because DDB guarantees strong consistency for
  LSI reads. `TableKeyInfo.has_lsi` is cached so the put path knows
  to stay in tx.
- **GSIs can be asynchronous** — there's a `gsi_queue` table
  (`crates/storage-postgres/src/gsi_queue.rs`) and a worker
  (`crates/storage-postgres/src/workers.rs`) that drain the queue
  out-of-band. This matches DDB's documented "GSI reads are
  eventually consistent" semantics and lets the base write commit
  fast.

Rektifier's per-write GSI maintenance is implicit (PG btree update +
either the GENERATED expression or the dual-write SQL fragment runs
in the same statement as the JSONB write). The async piece exists
only at GSI *creation* on a populated table — chunked backfill of
the new column, then `CREATE INDEX CONCURRENTLY`. There is no
steady-state queue.

#### Trade summary for GSI

| | extenddb | rektifier |
|---|---|---|
| Storage per GSI (ALL projection) | full base item, duplicated | btree leaves carry the JSONB payload (covering index); ~2× heap size |
| Add a GSI on N-row table | `CREATE TABLE` + backfill (online) | `ALTER TABLE ADD COLUMN x text NULL` (catalog-only) + async chunked backfill + `CREATE INDEX CONCURRENTLY` (online) |
| Write amplification (3 GSIs sync) | 1 base + 3 (delete+insert) = up to 7 statements | 1 statement, 3 btree updates inline (each leaf carries the JSONB) |
| Write amplification (3 GSIs async) | 1 base + 1 queue insert | n/a — rektifier has no steady-state async path (only the one-shot backfill at GSI creation) |
| Read on GSI w/ ALL projection | index scan → JSONB at index | index-only scan over covering btree, no base heap fetch |
| Strong-consistency GSI read | depends on sync mode | always (same MVCC snapshot) |
| Sparse index support | yes (engine no-ops when keys missing) | yes — sparse semantics fall out of nullable columns + the `data#>>` extraction returning NULL when the attribute is absent |

The architectural cost is real on extenddb's side: writes that touch
many GSIs do meaningfully more work (delete-then-insert into the
projected table). On rektifier, the equivalent cost is the WAL'd
INCLUDE-payload growing each base-table btree leaf — measurable on
PutItem (+0.05 ms p50 per GSI) but smaller than the multi-statement
work extenddb does. On reads, the two stacks now converge: extenddb
returns rows from a single sequential read at the GSI table;
rektifier returns rows from an index-only scan over a covering btree.
DDB itself is built on the same trade — async, projected GSI tables —
which lets DDB-style workloads (`PutItem occasionally, Query-by-GSI
constantly`) work well on either stack.

### Authentication and authorization

**Rektifier**: SigV4 verification via `rekt-sigv4` (131 lines —
thin), with a `PermissiveVerifier` mode that accepts any signature
value. The default is permissive in the dev/test paths. There is no
IAM model, no policy evaluator, no user/group/role concept.
Operators handle authentication via whatever sits in front
(application gateway, mTLS, internal network only). The bench uses
`Signature=deadbeef` and it works.

**ExtendDB**: SigV4 is mandatory and full. The auth crate has 5
modules including a policy evaluator
(`crates/auth/src/policy/evaluator.rs`), signing key derivation,
canonical request construction, and timestamp validation. The full
DDB IAM evaluation algorithm runs on every request: explicit deny →
permissions boundary → session policy → identity allow → implicit
deny. Credentials live encrypted in PG (`access_keys` table, AES-GCM
per-key encryption with a master key in the `settings` table); the
secret key never persists in plaintext. Constant-time failure paths
prevent timing side-channels between "key doesn't exist," "key
inactive," and "signature mismatch." See `crates/auth/src/lib.rs`
and the `S-5` design comment around credential lookup.

Auth is so deeply integrated that the server **refuses to start
with `auth.provider = "none"`** (`crates/bin/src/cmd_serve.rs:57`).

We added a **benchmarking bypass** in this branch (also documented
in `docs/perf/extenddb-bench-2026-05-21.md`):

- `EXTENDDB_BYPASS_SIGV4=1` short-circuits all of: Authorization
  parsing, credential lookup, timestamp validation, signature
  verification.
- `EXTENDDB_BYPASS_ACCOUNT_ID=<id>` (default `bench`) names the
  synthetic account that bypassed requests act as.
- The IAM authorization layer
  (`crates/server/src/authorization.rs`) early-returns `Ok(())`
  when bypass is on.
- The server bootstraps the synthetic account row in the catalog at
  startup so `CreateTable` doesn't fail on the `accounts` FK.
- A loud `tracing::warn!` fires at server start and at every
  `BuiltinAuthProvider::new()` call.

This is the equivalent of rektifier's `PermissiveVerifier` — only
for apples-to-apples benchmarking. Production deployments do not
set this env var.

### Operational model

**Rektifier** runs as a foreground binary. Reads config from a TOML
file. Logs to stdout via `tracing`. Stopping is `Ctrl-C` or
`SIGTERM`. There is no init step (post PLAN-10) — tables are
created via the wire `CreateTable` API at runtime. Operator runs PG
separately and provides the connection string.

**ExtendDB** runs as a daemon (`daemonize` crate). PID files live in
`~/.extenddb/run/`. The CLI exposes `serve`, `stop`, `status`,
`verify`, `migrate`, `destroy`, `manage`. Logs go to syslog (Linux
journalctl, macOS unified log). `extenddb init` is a real
bootstrap: it creates the catalog database, the data database,
runs migrations, generates the AES-256-GCM encryption key, creates
the default account, creates the admin user, generates the
self-signed TLS cert (with SANs for `localhost` and 127.0.0.1), and
writes `extenddb.toml`. `extenddb destroy` tears all of that down.

The two stacks also handle catalog state differently:

- **Rektifier** keeps a runtime catalog in memory
  (`rekt-catalog::TableCatalog`, an `ArcSwap<Snapshot>`) that is
  refreshed by a reconciler from `_rektifier_tables` on a cadence
  and on local DDL completion. Reads are lock-free
  sub-microsecond.
- **ExtendDB** queries the catalog DB on every data operation that
  needs key schema. There is an in-process cache but no
  ArcSwap-style snapshot pattern; the trade-off is that the
  catalog is always strongly consistent across nodes in an HA
  deployment (one of the items in `docs/design/11-high-
  availability.md`).

## Performance

The numbers in this section are from the bench captures in:

- `docs/perf/extenddb-bench-2026-05-21.md` (this tree) — extenddb
- `../rektifier/docs/perf/baseline.md` — rektifier Put/Get
- `../rektifier/docs/perf/crud-conditional-baseline.md` — conditional
  CRUD + ALL_OLD
- `../rektifier/docs/perf/update-baseline.md` — UpdateItem dispatch tree
- `../rektifier/docs/perf/query-scan-baseline.md` — Query/Scan

### Methodology

Both stacks ran against the **same Postgres 17 docker container**
on the same host. Closed-loop driver, concurrency=16, 256 B items,
1000-key working set, 10s run + 2s warmup. ExtendDB ran with
`EXTENDDB_BYPASS_SIGV4=1` to remove crypto from the hot path,
matching rektifier's `PermissiveVerifier`. TLS stays on for
extenddb (mandatory); the bench passes `--ca-bundle` to trust the
self-signed cert through the AWS SDK's hyper-rustls client.

Bench drivers are different:

- **Rektifier**: hand-rolled `rekt-bench` using `reqwest` directly.
  Sends DDB JSON-1.0 with a fake `Signature=deadbeef` header.
- **ExtendDB**: `extenddb-bench` (this tree) using the official
  AWS Rust SDK (`aws-sdk-dynamodb` 1.112). Real SigV4 signing
  happens client-side then the bypass discards it server-side.

The SDK difference matters by ~50–100 µs per request (signing
cost, response deserialization through smithy types). It's not the
dominant cost in any of these numbers but it's worth flagging when
reading absolute latencies.

### Headline numbers

| Workload | extenddb ops/s | extenddb p50 | rektifier ops/s | rektifier p50 | ratio |
|---|---:|---:|---:|---:|---:|
| Put | 2,826 | 5.45 ms | 11,267 | 1.37 ms | 4.0× |
| Get | 4,746 | 3.18 ms | 17,344 | 0.87 ms | 3.7× |
| Mixed (50/50) | 3,465 | 4.26 ms | — | — | — |
| Delete | 3,491 | 4.35 ms | 11,718 | 1.09 ms | 3.4× |
| PutCondInsertOnly | 1,994 | 7.63 ms | 3,216 | 4.14 ms | 1.6× |
| PutAllOld | 2,023 | 7.53 ms | 6,029 | 2.05 ms | 3.0× |
| DeleteCondExists | 2,781 | 5.83 ms | 3,854 | 3.43 ms | 1.4× |
| DeleteAllOld | 2,549 | 5.89 ms | 8,553 | 1.64 ms | 3.4× |
| UpdateSet | 1,843 | 8.04 ms | 9,176 (direct) | 1.70 ms | 4.7× |
| UpdateRmw | 1,992 | 7.63 ms | 3,630 (tx) | 4.18 ms | 1.8× |
| UpdateCond | 1,973 | 7.73 ms | 9,027 (direct) | 1.70 ms | 4.5× |
| ScanFull | 190 | 80.6 ms | 554 | 28.2 ms | 2.9× |
| ScanLimit (20) | 3,585 | 4.27 ms | 11,125 | 1.34 ms | 3.2× |
| QueryPkOnly | 3,108 | 4.75 ms | 11,231 | 1.33 ms | 3.6× |
| QuerySkRange | 4,038 | 3.78 ms | 13,536 | 1.11 ms | 3.4× |
| PutLsi | 1,857 | 8.29 ms | 10,786 | 1.44 ms | 5.8× |
| PutGenGsi | 1,973 | 7.81 ms | 10,294 | 1.49 ms | 5.2× |
| PutDwGsi | 1,905 | 8.02 ms | 10,007 | 1.53 ms | 5.2× |
| PutMultiDwGsi (3 GSIs) | 1,891 | 7.96 ms | 9,803 | 1.57 ms | 5.1× |
| QueryLsi | 2,253 | 6.87 ms | 12,141 | 1.25 ms | 5.5× |
| QueryGenGsi | 1,748 | 8.89 ms | 8,250 | 1.85 ms | 4.8× |
| QueryDwGsi | 1,669 | 9.22 ms | 7,945 | 1.91 ms | 4.8× |

(`extenddb p50` ratios are extenddb / rektifier — higher means
extenddb is slower.)

The rektifier indexed-Put + indexed-Query numbers above are
post-PLAN-12 (`INCLUDE (data)` covering payload on every LSI + GSI
index leaf, GSI Query running as an index-only scan). Pre-PLAN-12
the GSI Query rows above measured ~895 ops/s / ~17 ms p50 each —
extenddb won that workload class by ~2×. The covering-index change
closed and reversed the gap: rektifier now leads on GSI Query for
the bench shape captured here.

### Index workloads

Numbers pair `docs/perf/index-bench.md` (rektifier) with a
matching capture against extenddb on the same Postgres container.
Same shapes: `bench_lsi`, `bench_gen_gsi`, `bench_dw_gsi`,
`bench_multi_dw_gsi`. Two shape-level findings, both consistent
with the architectural differences described above.

**Indexed Put cost is fixed-overhead on extenddb, per-index on
rektifier.** Adding *any* index drops extenddb Put throughput
from ~2,826 to ~1,900 ops/sec; the number of GSIs after that
barely moves the line (single LSI = 1,857; one CT-time GSI =
1,973; three UpdateTable GSIs = 1,891). Rektifier's PLAN-9 D15
budget holds — single-GSI Puts stay within ~5% of the no-index
baseline, three-GSI within ~7%. The extenddb pattern looks like
"every indexed-table Put goes through a transactional /
sync-maintenance routing path regardless of whether the GSIs
are async-eligible." That's a routing fix on extenddb's side,
not an architectural difference: GSI work is queued via
`gsi_queue` on extenddb (so per-GSI cost is ~zero, as expected)
but the request still pays a transactional envelope.

**Indexed Query on a GSI is ~4.8× faster on rektifier (after
PLAN-12).** This was originally going to be the one workload class
where extenddb won on absolute throughput. The architectural
prediction: rektifier's GSI is a btree on the base table, so a
50-row GSI Query should do 50 base-table heap fetches; extenddb's
GSI is a separate `_ddb_<idx_uuid>` table with the projected items
already there, so the same Query should be one sequential scan of
50 rows. The first capture (pre-PLAN-12) matched that prediction:
extenddb 1,748 ops/sec, rektifier 895 ops/sec.

PLAN-12 closed the gap by emitting `INCLUDE (data)` on every LSI +
GSI `CREATE INDEX`. PG's planner now picks index-only scans for
GSI Query (Heap Fetches = 0); the index leaf carries the JSONB
payload alongside the key columns, so the 50-row Query is served
by a btree walk without touching the base heap. Post-PLAN-12
numbers: rektifier 8,250 ops/sec / 1.85 ms p50 (`query-gen-gsi`)
and 7,945 ops/sec / 1.91 ms p50 (`query-dw-gsi`).

The architectural shapes converge: extenddb materializes the
projection in a side table; rektifier materializes the projection
inside the base table's GSI btree leaves. Both store the JSONB
twice (once in base, once in the GSI). Both serve the read from
contiguous pages. CT-time vs UpdateTable GSI is indistinguishable
on read for both stacks.

The win comes with two operational dependencies that don't bite
extenddb's design:
1. PG's visibility map must be current for the index-only scan to
   avoid the heap-fetch fallback. autovacuum keeps it current
   under typical load; intense write bursts can transiently push
   the planner back to the slow path. Documented in PLAN-12.
2. Index size roughly doubles vs the non-covering shape (the
   JSONB payload lands in every leaf). For multi-KB items the
   PG-level TOAST machinery kicks in and the covering benefit
   evaporates; PLAN-12's deferred follow-up (projection-aware
   INCLUDE — emit `INCLUDE` only over `NonKeyAttributes` for
   INCLUDE-projection GSIs) is the lever for large-item workloads.

LSI Query stays in the same ~5.5× band as every other extenddb
bounded read — both pre- and post-PLAN-12, since the LSI was
already fast on rektifier (matching rows clustered on adjacent
heap pages by base-PK construction). The covering payload on LSI
indexes is a small win at this row count + a hedge for non-
clustered LSI workloads.


### Where the gap comes from

The 3–5× gap is real and consistent, but it decomposes into three
mostly-additive layers.

**1. Per-request engine overhead** (~1.6–1.8× the lowest workloads
hit). The closest two stacks get is `PutCondInsertOnly` (1.6×) and
`UpdateRmw` (1.8×) — both running through the equivalent of
"BEGIN → SELECT FOR UPDATE → write → COMMIT" in both stacks. This
is the irreducible cost of extenddb's engine layer: dispatch
through `engine/<op>.rs` → `storage::DataEngine` trait method →
`storage-postgres/data/<op>.rs`, plus the per-request work of
fetching `TableKeyInfo` from the catalog DB, plus the AWS SDK
deserialization overhead.

**2. ALL_OLD path divergence** (~3× gap on `PutAllOld`,
`DeleteAllOld`). Rektifier folds the pre-image read into the
upsert via a CTE: `WITH prev AS (SELECT data FROM t WHERE pk = $1)
INSERT … ON CONFLICT DO UPDATE … RETURNING (SELECT old_data FROM
prev)`. One statement, single MVCC snapshot. ExtendDB opens a
transaction for any ALL_OLD request, even unconditional. The fix
is local to `storage-postgres/data/put_item.rs` and
`storage-postgres/data/delete_item.rs`: detect the
`return_old && condition.is_none()` case and use the CTE shape.
Worth ~3× on the affected workloads.

**3. UpdateItem fast-path classifier** (~4.5–4.7× gap on
`UpdateSet`, `UpdateCond`). Rektifier classifies each UpdateItem
expression and routes to one of:
- **direct** — `INSERT … ON CONFLICT DO UPDATE SET data =
  jsonb_set(data, ...)` for `SET attr = :literal` and similar
  shapes that don't need to read the prior row.
- **tx** — `BEGIN → SELECT FOR UPDATE → UPDATE → COMMIT` for
  shapes that need the prior row (`SET counter = counter + 1`,
  conditions over other attrs).

The direct path costs the same as PutItem; the tx path is ~2.3×
slower. ExtendDB has no such classifier — every UpdateItem goes
through the equivalent of rektifier's tx path. The fix is harder
than ALL_OLD (it touches expression analysis and SQL emission in
parallel) but the win on common patterns (`SET counter = 5`,
`SET status = "complete"`) is large.

The combination of (2) and (3) plus closing the SDK overhead would
bring extenddb to ~1.8× rektifier across the board — at which
point most of the remaining gap is architectural (typed-column
extraction, account-scoped catalog lookup, IAM bypass overhead).

### Things the headline numbers don't show

- **TLS stays on in extenddb measurements**, off in rektifier's.
  Keep-alive amortizes most of the cost, but there's a constant
  per-frame TLS cost (encryption + frame mac) that shows up on
  reads with large response bodies. Bigger items would
  proportionally hurt extenddb more.
- **Constant-time auth path runs even when bypassed.** When
  `EXTENDDB_BYPASS_SIGV4` is not set, every request additionally
  pays: credential lookup in PG (cached, but still goes through
  the pool), AES-GCM decrypt of the secret key, SigV4 canonical
  request construction, HMAC-SHA256 derive×4 chain, hash compare.
  Production-mode latency is meaningfully higher than the bypass
  numbers in this doc.

### Workloads where the GSI-table shape could still win

PLAN-12's covering-index approach closes the small-item gap, but
two scenarios still favor extenddb's projected-table design:

1. **Large items (>2 KB JSONB).** PG TOASTs the JSONB inline in
   the index leaf at that size — the leaf carries a TOAST pointer,
   and a covering scan still chases the TOAST relation. ExtendDB's
   GSI table stores the projected item directly (no TOAST chain in
   the read path for typical projections). The lever on rektifier's
   side is projection-aware INCLUDE — materialize only
   `NonKeyAttributes` columns into the index — which is a deferred
   PLAN-12 follow-up.
2. **Heavy write contention against the visibility map.** Sustained
   high-write workloads can lag the autovacuum visibility-map
   refresh, pushing the PG planner back to bitmap + heap-fetch.
   ExtendDB's projected table doesn't depend on the visibility map
   at all. The lever on rektifier's side is operator-side
   autovacuum tuning (lower thresholds for the GSI tables).

We haven't directly benchmarked either scenario; the bench items
are 256 B, well under TOAST, and the bench runs include an explicit
VACUUM step. Future work.

## Tradeoffs and when to pick which

The two stacks sit at different points on the same trade frontier.
A pithy summary:

> ExtendDB optimizes for a managed-product experience: it owns the
> database, the auth, the TLS, the daemon, the console, the
> migrations. You point it at a Postgres and it gives you a
> DynamoDB. The cost is per-request latency that, today, runs
> 3–5× a thin sidecar.
>
> Rektifier optimizes for a thin sidecar over Postgres-you-already-
> have: generated columns let PG enforce key invariants, the
> translator pipeline keeps the codebase backend-neutral, and a
> permissive auth mode keeps the dev loop fast. The cost is
> operational surface area that the operator has to provide:
> auth, TLS, account isolation, GSI migrations.

**Pick extenddb when:**

- You want a turnkey DynamoDB replacement (init, serve, stop).
- You need multi-account isolation in one deployment (CI fleets,
  multi-tenant test environments, shared dev clusters).
- Your GSI workload involves large items (>2 KB JSONB) where PG's
  TOAST chain erodes rektifier's covering-index benefit, or
  sustained write bursts that lag autovacuum's visibility-map
  refresh.
- You want sync-vs-async GSI policy as a knob (eventual-consistency
  GSI reads with a queue-drained worker).
- Schemaless attributes are also indexed (PG-derived generated
  columns can't handle the "this attribute is a number sometimes
  and a string other times" case for indexed keys).
- You need real IAM (policies, roles, permissions boundaries) at
  the wire, not at the application layer above.
- You need a built-in management console + Prometheus metrics +
  syslog.

**Pick rektifier when:**

- You already run Postgres and want a translation layer that adds
  the minimum operational surface.
- Your workload runs on typical DDB-sized items (sub-2 KB) where
  PG's index-only scans over covering GSI btrees deliver wire
  latency that's hard to beat — write throughput stays near
  baseline (PLAN-12 measured +0.05 ms p50 per GSI on PutItem).
- You want a single PG namespace shared with non-DDB workloads
  (rektifier doesn't claim ownership of the database the way
  extenddb does).
- You want a future migration path to a `pgrx` Postgres extension
  — the type system enforces backend-neutrality from day one.
- You're OK terminating TLS and authenticating users at a layer
  above the DDB protocol.

**Pick neither when:**

- You actually need DynamoDB (global tables, eventual-consistency
  semantics tuned over decades, AWS-managed throughput). Both
  stacks ship "DynamoDB-compatible" but not "DynamoDB-equivalent."
- Latency-critical workloads where 1–5 ms p50 isn't tight enough.

To run extenddb in production-equivalent mode (no bypass), drop the
`EXTENDDB_BYPASS_SIGV4=1` env var, provision real credentials via
`devtools/provision-test-credentials`, and use those as the SDK's
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY. Expect another 200–500 µs
of per-request latency from credential lookup + AES-GCM decrypt +
SigV4 canonicalization + HMAC chain + policy evaluation.

## References

### ExtendDB source

| File | Purpose |
|---|---|
| `crates/storage/src/lib.rs:79` | `TableEngine` trait — table lifecycle ops |
| `crates/storage/src/lib.rs:156` | `DataEngine` trait — Put / Get / Delete / Update / Query / Scan |
| `crates/storage/src/util/key.rs` | Key extraction helpers (`composite_pk_to_text`, `parse_sk`, `sk_column`) |
| `crates/storage-postgres/src/data/ddl.rs:42` | Base table DDL template |
| `crates/storage-postgres/src/data/ddl.rs:139` | Index (GSI/LSI) DDL template |
| `crates/storage-postgres/src/data/index.rs:278` | Index row insert (engine-driven maintenance) |
| `crates/storage-postgres/src/data/put_item.rs` | PutItem fast/slow path |
| `crates/storage-postgres/src/data/query_scan.rs:185` | Scan impl |
| `crates/storage-postgres/src/gsi_queue.rs` | Async GSI maintenance queue |
| `crates/storage-postgres/migrations/001_schema.sql` | Catalog schema with all FKs |
| `crates/auth/src/lib.rs` | `BuiltinAuthProvider` — SigV4 + IAM |
| `crates/server/src/authorization.rs` | IAM policy evaluation per request |
| `crates/bench/src/main.rs` | The bench driver |
| `docs/perf/extenddb-bench-2026-05-21.md` | This run's full perf capture + analysis |

### Rektifier source

| File | Purpose |
|---|---|
| `crates/rekt-storage/src/lib.rs` | `Backend` trait — task-shaped per-op methods |
| `crates/rekt-translator/src/plan.rs` | Backend-neutral plan structs |
| `crates/rekt-translator/src/translate.rs` | DDB op + AST → plan |
| `crates/rekt-storage-libpq/src/lib.rs` | PG backend — SQL emission |
| `crates/rekt-storage-libpq/src/put_delete.rs` | PutItem / DeleteItem SQL (CTE for ALL_OLD) |
| `crates/rekt-storage-libpq/src/update.rs` | UpdateItem direct + tx paths |
| `crates/rekt-catalog/src/lib.rs` | Runtime catalog with ArcSwap snapshots |
| `crates/rekt-sigv4/src/lib.rs` | SigV4 verification (and `PermissiveVerifier`) |
| `crates/rekt-bench/src/main.rs` | The other bench driver |
| `CONTEXT.md` | Decision log: sidecar-vs-extension, data model, implementation order |
| `CLAUDE.md` | Project conventions including plan-document discipline |
| `docs/perf/baseline.md` | Put/Get baseline |
| `docs/perf/update-baseline.md` | UpdateItem dispatch tree perf |
| `docs/perf/crud-conditional-baseline.md` | Conditional CRUD + ALL_OLD |
| `docs/perf/query-scan-baseline.md` | Query/Scan baseline |

### Design documents

- `docs/design/04-component-storage.md` — extenddb storage trait
  design, account scoping
- `docs/design/05-component-auth.md` / `05-component-auth-v3.md` —
  extenddb auth model
- `docs/design/07-component-streams.md` — extenddb streams (engine-
  driven, atomically captured with data writes)
- `docs/design/11-high-availability.md` — extenddb HA story
- `docs/design/12-backend-plugin-architecture.md` (referenced in
  CLAUDE.md) — pluggable storage backends
- `../rektifier/docs/plan/PLAN-*` — rektifier's per-feature plans
  (gitignored, but informs the public design)
