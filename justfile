set dotenv-load := true

# Default: list available recipes
default:
    @just --list

# Bring up postgres + dynamodb-local
up:
    docker compose up -d
    @echo "postgres:       localhost:5432  (user=rektifier pass=rektifier db=rektifier)"
    @echo "dynamodb-local: localhost:8000"
    @echo "adminer:        http://localhost:8080"

# Stop and remove containers (keeps the postgres volume)
down:
    docker compose down

# Stop AND wipe the postgres volume
nuke:
    docker compose down -v

# Tail service logs
logs service="":
    docker compose logs -f {{service}}

# Connect to postgres with psql
psql:
    docker compose exec postgres psql -U rektifier -d rektifier

# Build everything
build:
    cargo build --workspace

# Run all tests via nextest
test:
    cargo nextest run --workspace

# Watch and re-run tests on change
watch:
    cargo watch -x "nextest run --workspace"

# Lint
lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings

# Format
fmt:
    cargo fmt --all

# Apply the example PG schema (creates users + device_events tables)
bootstrap-pg:
    docker compose exec -T postgres psql -U rektifier rektifier < rektifier-bootstrap.sql

# Run rektifier with the example config
run:
    REKTIFIER_CONFIG=rektifier.toml.example cargo run --bin rektifier

# Smoke: PutItem then GetItem against rektifier (assumes bootstrap-pg was run).
smoke-put-get:
    aws dynamodb put-item \
        --endpoint-url http://localhost:9000 \
        --table-name users \
        --item '{"id":{"S":"u1"},"name":{"S":"alice"}}'
    aws dynamodb get-item \
        --endpoint-url http://localhost:9000 \
        --table-name users \
        --key '{"id":{"S":"u1"}}'

# Smoke test: list tables against the reference dynamodb-local
smoke-ref:
    aws dynamodb list-tables --endpoint-url http://localhost:8000

# Smoke test: list tables against rektifier (MVP doesn't implement ListTables;
# expect UnknownOperationException 400 — useful sanity check that the server is up)
smoke-ours:
    aws dynamodb list-tables --endpoint-url http://localhost:9000

# Build the release bench binary
bench-build:
    cargo build -p rekt-bench --release

# Profile a workload under tracing-flame and emit flamegraph.svg.
# Requires `cargo install inferno` once (for the inferno-flamegraph binary).
# Assumes docker + bootstrap-pg already applied, and ddb-local users table
# created via `./target/release/rekt-bench setup-ddb-local` (only needed
# if you switch the workload to --target ddb-local).
flame:
    cargo build -p rektifier --features flame --release
    cargo build -p rekt-bench --release
    rm -f tracing.folded flamegraph.svg
    REKTIFIER_CONFIG=rektifier.toml.example REKTIFIER_LOG=rekt=debug,info ./target/release/rektifier &
    sleep 3
    ./target/release/rekt-bench run --target rektifier --workload mixed --concurrency 16 --duration 15s --warmup 2s --working-set 1000
    pkill -f 'target/release/rektifier' || true
    sleep 1
    test -s tracing.folded || { echo 'no tracing data captured'; exit 1; }
    inferno-flamegraph < tracing.folded > flamegraph.svg
    @echo 'wrote flamegraph.svg — open it in a browser'

# Run a quick (10s/target/workload) bench across all three targets for put + get.
# Assumes: docker up + bootstrap-pg applied + rektifier running + ddb-local
# `users` table created (`./target/release/rekt-bench setup-ddb-local`).
bench-quick: bench-build
    ./target/release/rekt-bench setup-ddb-local
    @echo "=========================================="
    @echo "PutItem"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload put --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target rektifier  --workload put --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target ddb-local  --workload put --concurrency 16 --duration 10s --warmup 2s
    @echo "=========================================="
    @echo "GetItem"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload get --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    ./target/release/rekt-bench run --target rektifier  --workload get --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    ./target/release/rekt-bench run --target ddb-local  --workload get --concurrency 16 --duration 10s --warmup 2s --working-set 1000

# Bench the conditional Put/Delete + ALL_OLD paths plus a basic
# DeleteItem regression check. Working set sized for ~10s of throughput
# on the *-cond-exists* / *-all-old* delete workloads (they consume keys
# as they go).
#
# Assumes: same prereqs as bench-quick.
bench-conditional: bench-build
    ./target/release/rekt-bench setup-ddb-local
    @echo "=========================================="
    @echo "DeleteItem (unconditional — regression check on the new RETURNING SQL)"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload delete --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target rektifier  --workload delete --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target ddb-local  --workload delete --concurrency 16 --duration 10s --warmup 2s
    @echo "=========================================="
    @echo "PutItem with attribute_not_exists(id) (conditional Put slow path)"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload put-cond-insert-only --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target rektifier  --workload put-cond-insert-only --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target ddb-local  --workload put-cond-insert-only --concurrency 16 --duration 10s --warmup 2s
    @echo "=========================================="
    @echo "PutItem with ReturnValues=ALL_OLD (pre-image CTE)"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload put-all-old --concurrency 16 --duration 10s --warmup 2s --working-set 50000
    ./target/release/rekt-bench run --target rektifier  --workload put-all-old --concurrency 16 --duration 10s --warmup 2s --working-set 50000
    ./target/release/rekt-bench run --target ddb-local  --workload put-all-old --concurrency 16 --duration 10s --warmup 2s --working-set 50000
    @echo "=========================================="
    @echo "DeleteItem with attribute_exists(id) (conditional Delete slow path)"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload delete-cond-exists --concurrency 16 --duration 10s --warmup 2s --working-set 50000
    ./target/release/rekt-bench run --target rektifier  --workload delete-cond-exists --concurrency 16 --duration 10s --warmup 2s --working-set 50000
    ./target/release/rekt-bench run --target ddb-local  --workload delete-cond-exists --concurrency 16 --duration 10s --warmup 2s --working-set 50000
    @echo "=========================================="
    @echo "DeleteItem with ReturnValues=ALL_OLD"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload delete-all-old --concurrency 16 --duration 10s --warmup 2s --working-set 50000
    ./target/release/rekt-bench run --target rektifier  --workload delete-all-old --concurrency 16 --duration 10s --warmup 2s --working-set 50000
    ./target/release/rekt-bench run --target ddb-local  --workload delete-all-old --concurrency 16 --duration 10s --warmup 2s --working-set 50000

# Bench: Query + Scan dispatch tree (PLAN-4 Q1-Q6). Scan workloads use
# the `users` working set (sized in via --working-set). Query workloads
# seed a 50-row composite partition under device_events on bench start.
bench-query-scan: bench-build
    ./target/release/rekt-bench setup-ddb-local
    @echo "=========================================="
    @echo "Scan (no limit) — full-table read"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload scan-full --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    ./target/release/rekt-bench run --target rektifier  --workload scan-full --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    ./target/release/rekt-bench run --target ddb-local  --workload scan-full --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    @echo "=========================================="
    @echo "Scan (limit=20) — single-page bounded scan"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload scan-limit --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    ./target/release/rekt-bench run --target rektifier  --workload scan-limit --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    ./target/release/rekt-bench run --target ddb-local  --workload scan-limit --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    @echo "=========================================="
    @echo "Scan (limit=50 + filter) — per-row Rust filter cost"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload scan-filtered --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    ./target/release/rekt-bench run --target rektifier  --workload scan-filtered --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    ./target/release/rekt-bench run --target ddb-local  --workload scan-filtered --concurrency 16 --duration 10s --warmup 2s --working-set 1000
    @echo "=========================================="
    @echo "Query (pk-only, 50-row partition)"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload query-pk-only --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target rektifier  --workload query-pk-only --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target ddb-local  --workload query-pk-only --concurrency 16 --duration 10s --warmup 2s
    @echo "=========================================="
    @echo "Query (sk range BETWEEN — 20 of 50 rows)"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload query-sk-range --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target rektifier  --workload query-sk-range --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target ddb-local  --workload query-sk-range --concurrency 16 --duration 10s --warmup 2s
    @echo "=========================================="
    @echo "Query (pk + filter — Count vs ScannedCount divergence)"
    @echo "=========================================="
    ./target/release/rekt-bench run --target direct-pg  --workload query-filtered --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target rektifier  --workload query-filtered --concurrency 16 --duration 10s --warmup 2s
    ./target/release/rekt-bench run --target ddb-local  --workload query-filtered --concurrency 16 --duration 10s --warmup 2s

# SDK smoketests: validate real DynamoDB SDK clients can connect to
# rektifier. See smoketests/README.md for prerequisites. Requires
# Maven and JDK 17 installed.
smoke-java-v1:
    cd smoketests/java-sdk-v1 && mvn -q compile exec:java

smoke-java-v2:
    cd smoketests/java-sdk-v2 && mvn -q compile exec:java
