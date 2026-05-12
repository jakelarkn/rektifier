# Performance baseline

Captured on 2026-05-12 with `rekt-bench` (Step 7).

## Setup

- Single host, all components on `localhost`.
- Postgres 17 in Docker (default config).
- Rektifier release build, `PermissiveVerifier`, max_size pool = 16.
- direct-pg uses tokio-postgres with deadpool pool max_size = 16 (matching concurrency).
- ddb-local in Docker, default Java config.
- Closed-loop driver, concurrency = 16, 10s run + 2s warmup, item size = 256 B,
  working set = 1000 keys, hash-only S-PK table (`users`).

## Numbers

### PutItem

| Target     |   ops/sec |  p50    |  p90    |  p99    |  p999   |   max   |
|------------|----------:|--------:|--------:|--------:|--------:|--------:|
| direct-pg  |   14,099  | 1.06 ms | 1.50 ms | 2.36 ms | 4.85 ms |  28 ms  |
| rektifier  |   11,267  | 1.37 ms | 1.82 ms | 2.54 ms | 4.30 ms |  19 ms  |
| ddb-local  |    4,671  | 3.54 ms | 4.62 ms | 7.13 ms | 9.44 ms |  17 ms  |

**Rektifier overhead vs direct-pg (PutItem):**
- p50: **+0.31 ms** (1.06 → 1.37)
- p99: **+0.18 ms** (2.36 → 2.54)
- throughput: **~20% loss** (14.1k → 11.3k ops/sec)

### GetItem

| Target     |   ops/sec |  p50    |  p90    |  p99    |  p999   |   max   |
|------------|----------:|--------:|--------:|--------:|--------:|--------:|
| direct-pg  |   23,649  | 0.63 ms | 0.89 ms | 1.52 ms | 4.54 ms |  15 ms  |
| rektifier  |   17,344  | 0.87 ms | 1.23 ms | 1.90 ms | 4.21 ms |  13 ms  |
| ddb-local  |   10,979  | 1.42 ms | 1.90 ms | 3.15 ms | 4.67 ms |  10 ms  |

**Rektifier overhead vs direct-pg (GetItem):**
- p50: **+0.24 ms** (0.63 → 0.87)
- p99: **+0.38 ms** (1.52 → 1.90)
- throughput: **~27% loss** (23.6k → 17.3k ops/sec)

## What this says

- **Rektifier's added latency is sub-millisecond at p50 and p99** for small items
  on a hash-only S-PK schema. The translation + HTTP-parse + JSON-decode +
  prepare-typed-cached overhead measures roughly 0.2–0.4 ms p50, and the same
  ballpark at p99.
- **Throughput costs ~20–27%** in this configuration — entirely consistent
  with the latency overhead (more time per op = fewer ops/sec).
- **Both rektifier and direct-pg outperform dynamodb-local** by a factor of
  2–3× on this workload. DDB-local is Java + slower-than-PG storage; not
  representative of real DDB.
- **No errors at this load on any target.**

## Reproducing

```sh
just up                                                            # docker
just bootstrap-pg                                                  # PG schema
REKTIFIER_CONFIG=rektifier.toml.example cargo run --release --bin rektifier &
just bench-quick                                                   # this very report's input
```

## Known caveats

- Closed-loop driver — no coordinated-omission correction. Numbers slightly
  optimistic vs a true open-loop measurement.
- Single host — no network. Real deployments will add network RTT.
- Small items + small working set — everything stays in PG's shared buffers.
  Big-item / large-working-set runs to follow.
- 10-second windows — long enough to be stable for this workload, too short to
  catch autovacuum / checkpoint pauses.

## Follow-ups (out of scope for Step 7)

- Open-loop driver with hdrhistogram coordinated-omission recording.
- Item-size sweep (256 B / 4 KB / 64 KB / 256 KB), the last one hitting PG's
  TOAST cliff.
- Mixed workload runs.
- Long-run stability (1h+) for autovacuum / checkpoint visibility.
- Profile + tune (probably worth: connection-pool sizing, prepared-statement
  cache hit rate, axum middleware overhead).
