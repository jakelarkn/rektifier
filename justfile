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

# Run rektifier (once the binary exists)
run:
    cargo run --bin rektifier

# Smoke test: list tables against the reference dynamodb-local
smoke-ref:
    aws dynamodb list-tables --endpoint-url http://localhost:8000

# Smoke test: list tables against rektifier (once it runs on :9000)
smoke-ours:
    aws dynamodb list-tables --endpoint-url http://localhost:9000
