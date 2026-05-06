# rektifier

DynamoDB-compatible API in front of PostgreSQL. Speak DynamoDB, store in Postgres.

A *rectifier* is the electrical component that converts one type of current into another. **rektifier** does the same for protocols — converting DynamoDB API calls into Postgres operations, so any DynamoDB SDK or CLI can talk, unmodified, to data living in plain old Postgres.

Sidecar architecture: HTTP/JSON DynamoDB protocol on the front, PostgreSQL wire protocol on the back.

## Prereqs

- Docker Desktop
- Rust toolchain (`curl https://sh.rustup.rs -sSf | sh`)
- `just` (`brew install just`)
- `cargo-nextest` (`cargo install cargo-nextest --locked`)
- `cargo-watch` (`cargo install cargo-watch`)
- AWS CLI v2 (`brew install awscli`)
- `direnv` (optional but recommended; `brew install direnv`)

## Quickstart

```sh
direnv allow              # if using direnv; otherwise `source .envrc`
just up                   # start postgres + dynamodb-local
just smoke-ref            # sanity check: list tables on the reference emulator
just psql                 # poke at postgres
just down                 # stop services
```

## Ports

| Service          | Port  | Purpose                                          |
|------------------|-------|--------------------------------------------------|
| Postgres         | 5432  | Storage backend                                  |
| dynamodb-local   | 8000  | AWS reference emulator (for differential tests)  |
| rektifier        | 9000  | Our DynamoDB-compatible server (when running)    |
| Adminer          | 8080  | Web UI for Postgres                              |

## Architecture (planned)

Workspace crates, bottom-up:

- `rekt-protocol`       — AttributeValue, request/response types, JSON framing
- `rekt-sigv4`          — SigV4 verification
- `rekt-expressions`    — parsers for KeyCondition / Filter / Update / Condition
- `rekt-translator`     — DynamoDB op + AST -> SQL fragment + bound params
- `rekt-storage`        — `trait Backend` (backend-neutral)
- `rekt-storage-libpq`  — Backend impl over `tokio-postgres`
- `rekt-server`         — axum routes; dispatches to translator + backend
- `rekt-control`        — CreateTable / DescribeTable / DDL emission
- `rekt-streams`        — change events via logical replication
- `rektifier`           — binary, wires libpq backend + axum server

A future `rekt-storage-spi` + `rekt-extension` crate would let the same upper layers run as a Postgres extension via `pgrx`.
