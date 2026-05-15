# SDK smoketests

Standalone projects that exercise rektifier through real AWS SDK
clients. These confirm rektifier's wire shape is interoperable with
production-grade SDKs, not just the AWS CLI used by the diff harness.

Two SDK generations are covered:

| Project | SDK | Module |
|---|---|---|
| `java-sdk-v1` | AWS SDK for Java v1 (classic) | `com.amazonaws:aws-java-sdk-dynamodb` |
| `java-sdk-v2` | AWS SDK for Java v2 | `software.amazon.awssdk:dynamodb` |

Both projects exercise the same surface: PutItem, GetItem,
UpdateItem (with `SET counter = counter + :inc`), DeleteItem,
Query (`device_id = :pk` on `device_events` with 5 seeded rows),
Query + FilterExpression, Scan with Limit, and a conditional Put
that must surface `ConditionalCheckFailedException`. Each check
prints PASS / FAIL and the process exits non-zero on any failure.

## Prerequisites

- **JDK 17+** (`java -version`).
- **Maven 3.6+** (`mvn -version`). On macOS: `brew install maven`.
- **rektifier listening on `127.0.0.1:9000`** with the tables in
  `rektifier.toml.example` declared and `just bootstrap-pg` applied.

## Running

```sh
# rektifier must be up; e.g.
just bootstrap-pg
REKTIFIER_CONFIG=rektifier.toml.example cargo run --release --bin rektifier &

# v1
just smoke-java-v1

# v2
just smoke-java-v2
```

Or directly:

```sh
cd smoketests/java-sdk-v1
mvn -q compile exec:java
```

Override the endpoint via env vars:

```sh
REKTIFIER_URL=http://my-host:9000 just smoke-java-v1
```

## Output

```
=== rektifier-smoke-v2 ===
endpoint = http://localhost:9000
region   = us-east-1

  PASS  PutItem
  PASS  GetItem
  PASS  UpdateItem
  PASS  GetItem (post-update)
  PASS  DeleteItem
  PASS  Query (composite + KCE)
  PASS  Query with FilterExpression
  PASS  Scan with Limit
  PASS  Conditional Put rejection

=== 9 passed, 0 failed ===
```

## Adding a new check

Each `Main.java` has a `check("name", () -> ...)` harness. Add a
new method that throws on failure (or calls `require(...)`) and
register it with another `check(...)` call before `cleanup`.
Keep cleanup updated so re-runs don't leak state.
