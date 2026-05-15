# Query + Scan baseline

Captured 2026-05-15 with `rekt-bench` after the `PLAN-4` Q1–Q6 work
landed and the integration audit closure (commit `73323bd`). Companion
to `baseline.md` (Put/Get), `update-baseline.md` (UpdateItem), and
`crud-conditional-baseline.md` (conditional Put/Delete + ALL_OLD).

## Setup

- Same host config as prior baselines: PG 17 in Docker, rektifier
  release build, `PermissiveVerifier`, deadpool max_size = 16.
- Closed-loop driver, concurrency = 16, 10 s run + 2 s warmup,
  item_size = 256 B.
- Host 1-min load avg at capture time: ~2.5 (quiet — earlier captures
  hit elevated load).
- Scan workloads operate on the hash-only `users` working set
  (`--working-set 1000`).
- Query workloads operate on a fresh 50-row composite partition under
  `device_events` (PK = `q-bench-pk`, SK = ts 0..49 with alternating
  `flag: on|off`) seeded at bench start by `seed_query_partition`.
- `direct-pg` mirrors the actual SQL rektifier emits — same
  `SELECT … ORDER BY pk[, sk] LIMIT $N` for Scan; same
  `SELECT … WHERE device_id = $1 [AND ts BETWEEN $2 AND $3] ORDER BY ts ASC LIMIT $N`
  for Query. Filter benchmarks: direct-pg emits the un-filtered
  SELECT (per-row filter cost is paid in the rektifier dispatcher,
  not in the PG client), so the gap shows rektifier's filter
  overhead on top of identical SQL.

## What each workload exercises

| Workload | Rektifier path | SQL shape |
|---|---|---|
| `scan-full` | `Backend::scan_raw` with `Limit=1000` (soft default) | `SELECT data FROM users ORDER BY id LIMIT 1000` — entire working set per call |
| `scan-limit` | `scan_raw` with `Limit=20` | Same SELECT bounded at 20 — single-page Scan |
| `scan-filtered` | `scan_raw` with `Limit=50` + per-row filter (`counter < :half`, always true) | Same SELECT bounded at 50; rektifier evaluates filter per row in Rust |
| `query-pk-only` | `Backend::query_raw` with just pk | `SELECT doc FROM device_events WHERE device_id = $1 ORDER BY ts ASC LIMIT N` — full 50-row partition |
| `query-sk-range` | `query_raw` with `SkCondition::Between` | `… AND ts BETWEEN $2 AND $3 …` — 20 of 50 rows |
| `query-filtered` | `query_raw` + per-row filter (`flag = :on`, ~half match) | Same as `query-pk-only`; filter runs per row in dispatcher |

## Numbers

### Scan

#### Rektifier

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| scan-full | 554 | 28.18 | — | 47.30 | 53.82 |
| scan-limit (20) | 11,125 | 1.34 | — | 3.14 | 6.49 |
| scan-filtered (50) | 7,132 | 2.10 | — | 4.69 | 8.64 |

#### Direct PG

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| scan-full | 616 | 25.52 | — | 33.73 | 41.12 |
| scan-limit (20) | 15,051 | 0.99 | — | 2.39 | 4.13 |
| scan-filtered (50) | 9,589 | 1.57 | — | 3.38 | 5.99 |

#### DDB-local

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| scan-full | **2.1** | **7,688.19** | — | 8,495.10 | 8,495.10 |
| scan-limit (20) | 6,988 | 2.17 | — | 5.50 | 9.45 |
| scan-filtered (50) | 6,180 | 2.44 | — | 4.95 | 7.64 |

### Query

#### Rektifier

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| query-pk-only | 11,231 | 1.33 | — | 3.12 | 7.18 |
| query-sk-range | 13,536 | 1.11 | — | 2.66 | 6.50 |
| query-filtered | 10,309 | 1.43 | — | 3.88 | 8.35 |

#### Direct PG

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| query-pk-only | 16,927 | 0.87 | — | 2.46 | 5.01 |
| query-sk-range | 18,849 | 0.78 | — | 2.33 | 5.88 |
| query-filtered | 16,638 | 0.88 | — | 2.48 | 5.81 |

#### DDB-local

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| query-pk-only | 6,091 | 2.56 | — | 4.91 | 7.67 |
| query-sk-range | 7,428 | 2.15 | — | 4.28 | 5.65 |
| query-filtered | 5,572 | 2.78 | — | 5.16 | 7.45 |

## Observations

### Rektifier overhead is stable across Q/S — same as Get/Put

Latency delta vs direct-pg at p50:

| Workload | rektifier - direct-pg p50 |
|---|---:|
| scan-full | +2.66 ms |
| scan-limit (20) | +0.35 ms |
| scan-filtered (50) | +0.53 ms |
| query-pk-only | +0.46 ms |
| query-sk-range | +0.33 ms |
| query-filtered | +0.55 ms |

Bounded reads (Scan/Query at typical page sizes) sit in the
**+0.3 to +0.55 ms p50** band — exactly the range we see for
GetItem (+0.27 ms in today's run) and the conditional Put/Delete
paths (+0.3 to +0.5 ms in `crud-conditional-baseline.md`). No new
overhead from Q1–Q6's dispatcher additions; the cost is the same
"HTTP parse → translate → backend → HTTP serialize" envelope as
every other op.

### Per-row filter overhead is small but measurable

`scan-filtered` (50 rows scanned, filter always matches) costs
+0.18 ms p50 over `scan-limit` (20 rows scanned, no filter). That's
50 rows of `evaluate_condition(Some(&row), &cond)` plus the extra
30 rows of JSON deserialization. ~3.6 µs per row eval'd in Rust —
reasonable for the current implementation. The per-call
**rektifier-vs-direct-pg overhead** for `scan-filtered` (+0.53 ms)
is only ~0.18 ms more than `scan-limit` (+0.35 ms), so the filter
isn't pathological.

`query-filtered` (50 rows scanned, filter drops 25) is +0.55 ms
over direct-pg vs `query-pk-only`'s +0.46 ms — almost identical
overhead. Same per-row eval shape.

### sk-range Query is the cheapest read (vs pk-only)

Both PG and rektifier are faster on `query-sk-range` than
`query-pk-only` despite the extra WHERE clause. Why: the range
hits 20 of 50 rows, so fewer rows are deserialized + serialized.
The SQL parser/planner cost dominates, and 20 rows × JSON-encode
is cheaper than 50. Real-world Query workloads with selective SK
predicates will see this benefit.

### DDB-local catastrophically slow on full Scan

`scan-full` on DDB-local: **2.1 ops/sec, 7.6 s p50**. DDB-local
appears to single-thread or serialize unbounded Scan reads in a
way that breaks down under concurrency 16. With `Limit=20`
DDB-local recovers to ~7k ops/sec, so the problem is specific to
the unbounded scan path. Not a rektifier concern — flagging
because users running parallel diff tests against DDB-local should
avoid unbounded Scan as the high-traffic comparison shape.

Rektifier and direct-pg both stay under 30 ms p50 even on the
full 1000-row Scan. Throughput is similar (rektifier 554 vs
direct-pg 616 ops/sec) — the work is dominated by reading +
serializing 1000 rows × 256 B per call.

### No regression on existing paths

Same-run regression check against `baseline.md` / `crud-conditional-baseline.md`:

| Path | overhead p50 prior | overhead p50 now |
|---|---:|---:|
| PutItem | within noise | within noise (rektifier ran *faster* than direct-pg again — see Caveats in crud-conditional-baseline.md) |
| GetItem | +0.27 ms | +0.27 ms |
| DeleteItem | +0.31 ms | +0.24 ms |
| put-cond-insert-only | +0.62 ms | +0.12 ms |
| put-all-old | +0.75 ms | +0.41 ms |
| delete-cond-exists | +0.43 ms | +0.38 ms |

All numbers either match or improved (the May-14 capture was on an
elevated-load host). No architectural regression from the Q1–Q6 +
audit-closure work.

## Caveats

- DDB-local's `scan-full` collapse skews the printed comparison.
  Don't over-interpret the absolute numbers; rektifier vs direct-pg
  is the meaningful comparison.
- `scan-filtered` and `query-filtered` direct-pg emits the same SQL
  as the un-filtered variants because per-row filter cost is paid
  by the rektifier dispatcher, not the PG client. The rektifier
  overhead vs direct-pg shows the filter cost *on top of* identical
  backing SQL.
- The query-partition seed step (`seed_query_partition`) runs once
  per bench invocation. It calls 50 PutItems serially, so a few
  seconds of overhead is amortized into the warmup window and
  doesn't affect the steady-state numbers reported here.
- Soft `Limit` default is 1000 (matches rektifier's `DEFAULT_LIMIT`
  in `crates/rekt-storage-libpq/src/{query,scan}.rs`).

## Reproducing

```sh
just up                                                            # docker
just bootstrap-pg                                                  # PG schema
REKTIFIER_CONFIG=rektifier.toml.example cargo run --release --bin rektifier &
just bench-query-scan                                              # this report's input
```
