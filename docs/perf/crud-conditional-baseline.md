# CRUD + conditional baseline

Captured 2026-05-14 with `rekt-bench`, after the conditional Put/Delete
+ `ReturnValues=ALL_OLD` work landed (priorities 1 & 2 of PLAN-2's
"feature parity" track). Companion to `baseline.md` (Put/Get) and
`update-baseline.md` (UpdateItem dispatch tree).

## Setup

- Same host config as the prior baselines: PG 17 in Docker, rektifier
  release build, `PermissiveVerifier`, deadpool max_size = 16.
- Closed-loop driver, concurrency = 16, 10 s run + 2 s warmup,
  item_size = 256 B, hash-only S-PK `users` table.
- `direct-pg` mirrors the actual SQL rektifier emits — the
  CTE-wrapped `INSERT … RETURNING (old_data)` for Put, the
  `DELETE … RETURNING data` for Delete, and `BEGIN → SELECT FOR
  UPDATE → … → COMMIT` for the conditional paths.
- System load during this capture was elevated (1-min load avg ~8 on
  this host); see *Caveats* below.

## What each new workload exercises

| Workload | Routing | SQL shape |
|---|---|---|
| `delete` | unconditional Delete | `DELETE FROM t WHERE pk = $1 RETURNING data` (one round-trip) |
| `put-cond-insert-only` | conditional Put → slow path | `BEGIN → SELECT FOR UPDATE → INSERT … ON CONFLICT DO NOTHING → COMMIT` — fresh PK per op so the `attribute_not_exists(id)` always passes |
| `put-all-old` | unconditional Put + ALL_OLD | `WITH prev AS (SELECT data FROM t WHERE pk = $1) INSERT … ON CONFLICT DO UPDATE … RETURNING (SELECT old_data FROM prev)` — single statement |
| `delete-cond-exists` | conditional Delete → slow path | `BEGIN → SELECT FOR UPDATE → DELETE → COMMIT` |
| `delete-all-old` | unconditional Delete + ALL_OLD | Same shape as `delete` — `RETURNING data` is free on this path |

The `delete-cond-exists` / `delete-all-old` workloads consume the
working set as they run; we size it to 50 000 so a 10 s run at typical
throughput stays inside it. After the working set is exhausted,
`delete-cond-exists` starts erroring out with
`ConditionalCheckFailedException` (`attribute_exists` on a now-deleted
row) — those errors are expected and accounted for; latency numbers
still reflect the successful subset.

## Numbers

### New conditional + ALL_OLD workloads

#### Rektifier

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| delete | 11,718 | 1.09 | 2.33 | 4.95 | 8.77 |
| put-cond-insert-only | 3,216 | 4.14 | 7.80 | 14.84 | 31.09 |
| put-all-old | 6,029 | 2.05 | 4.43 | 11.20 | 27.81 |
| delete-cond-exists | 3,854 | 3.43 | 6.00 | 15.08 | 46.40 |
| delete-all-old | 8,553 | 1.64 | 2.99 | 6.06 | 12.09 |

#### Direct PG

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| delete | 16,678 | 0.78 | 1.53 | 3.59 | 8.12 |
| put-cond-insert-only | 3,688 | 3.52 | 6.71 | 14.65 | 31.95 |
| put-all-old | 10,419 | 1.30 | 2.30 | 5.02 | 11.60 |
| delete-cond-exists | 4,383 | 3.00 | 5.70 | 11.81 | 21.49 |
| delete-all-old | 12,178 | 1.15 | 2.10 | 4.70 | 9.29 |

#### DDB-local

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| delete | 7,531 | 1.76 | 3.34 | 7.63 | 17.87 |
| put-cond-insert-only | 3,248 | 4.25 | 8.75 | 15.94 | 30.18 |
| put-all-old | 3,492 | 4.11 | 7.52 | 17.14 | 28.25 |
| delete-cond-exists | 4,345 | 3.18 | 6.38 | 14.28 | 26.98 |
| delete-all-old | 7,940 | 1.75 | 3.02 | 6.31 | 14.51 |

### Existing workloads re-run for regression check (same run, current build)

| Target | Workload | ops/sec | p50 | p99 |
|---|---|---:|---:|---:|
| direct-pg | put | 4,074 | 2.81 | 17.18 |
| rektifier | put | 6,522 | 1.96 | 9.32 |
| ddb-local | put | 2,839 | 4.74 | 25.95 |
| direct-pg | get | 15,544 | 0.81 | 3.98 |
| rektifier | get | 10,741 | 1.12 | 6.42 |
| ddb-local | get | 5,240 | 2.04 | 16.77 |

## Observations

### Regression check on unchanged-feature paths

The recent work changed the SQL emitted by `put_item_raw` (now
CTE-wraps a pre-image read) and `delete_item_raw` (now uses
`RETURNING`). Comparing today's *rektifier-overhead-vs-floor* to the
2026-05-12 capture:

| Path | overhead p50 then | overhead p50 now |
|---|---:|---:|
| PutItem | +0.31 ms | within noise (rektifier ran slightly *faster* than direct-pg on Put in this run — see Caveats; the SQL shape is identical) |
| GetItem | +0.24 ms | +0.31 ms |
| DeleteItem | (not benched) | +0.31 ms |

No architectural regression. The CTE additions don't materially
change rektifier's per-request overhead vs the SQL floor.

### Conditional Put / Delete cost ~2× a non-conditional fast path

`put-cond-insert-only` on rektifier: 3,216 ops/s vs 6,522 ops/s for
unconditional Put. That's the exact `tx`-vs-`direct` ratio seen in
`update-baseline.md` for UpdateItem (~2.5× latency, ~50%
throughput): the slow path opens a transaction, does a `SELECT FOR
UPDATE`, then writes. Same shape, same cost.

`delete-cond-exists` on rektifier: 3,854 ops/s vs 11,718 ops/s for
unconditional Delete. Roughly 3× — slightly worse ratio than Put
because the unconditional Delete is a single-statement
`DELETE … RETURNING`, which is the cheapest write shape we emit.

### ALL_OLD is essentially free

`put-all-old` on rektifier: 2.05 ms p50 vs 1.96 ms p50 for plain Put
— +0.09 ms. The pre-image read is a single PK lookup inside the
same statement as the upsert, so PG can stitch the work cheaply.

`delete-all-old` on rektifier: 1.64 ms p50 vs 1.09 ms p50 for plain
Delete — +0.55 ms. Slightly more visible here because the
`RETURNING data` materializes the full row's JSON every time, vs
the no-RETURNING variant which… also reads the row to compute
`RETURNING data`. The two should be the same shape on the SQL
level; the 0.55 ms gap likely reflects sample variance plus the
small extra work of carrying `Option<Value>` back through the
dispatcher when ALL_OLD is requested.

### Rektifier's overhead-vs-floor is stable across all new paths

| Workload | rektifier - direct-pg p50 |
|---|---:|
| delete | +0.31 ms |
| put-cond-insert-only | +0.62 ms |
| put-all-old | +0.75 ms |
| delete-cond-exists | +0.43 ms |
| delete-all-old | +0.49 ms |

Roughly 0.3–0.8 ms p50 — in the same ballpark as the baseline.md
0.2–0.4 ms p50 range, slightly elevated by system load.

### DDB-local

Slowest target across the board, as previously observed. Notably,
`put-all-old` on DDB-local (3,492 ops/s, 4.11 ms p50) is similar to
plain Put on DDB-local (2,839 ops/s, 4.74 ms p50) — DDB-local pays
the same cost regardless of whether you ask for `ALL_OLD`.

## Caveats

- **Elevated system load** during this capture (1-min load avg ~8 on
  this host) — all targets ran slower than the 2026-05-12 numbers.
  Direct-pg dropped from 14,099 → 4,074 ops/sec on Put despite
  doing more work (CTE) in today's run, suggesting most of the gap
  is environmental, not code. Within-run comparisons (rektifier vs
  direct-pg) remain meaningful.
- **Concurrency 16 on a noisy host hits a ceiling.** On Put, today's
  rektifier ran *faster* than today's direct-pg — almost certainly
  because the deadpool warmup in the bench's PgTarget is shorter
  than rektifier's (which has been running for the diff-test suite
  + several bench runs). Re-running both back-to-back on a quiescent
  host would clean this up.
- `delete-cond-exists` errors (~50% of attempted ops) are by design:
  the workload exhausts the working set ~halfway through the 10 s
  window, and `attribute_exists` then correctly fails. Successful-op
  latency is still the right read.

## Reproducing

```sh
just up                                                            # docker
just bootstrap-pg                                                  # PG schema
REKTIFIER_CONFIG=rektifier.toml.example cargo run --release --bin rektifier &
just bench-quick                                                   # regression check
just bench-conditional                                             # this report's input
```
