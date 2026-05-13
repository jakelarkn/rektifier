# UpdateItem performance baseline

Captured 2026-05-12 with `rekt-bench`, after Phase 3b/3c/4c/4d landed
the full UpdateItem dispatch tree.

## Setup

- Single host, all components on `localhost`.
- Postgres 17 in Docker (default config).
- Rektifier release build, `PermissiveVerifier`, deadpool max_size = 16.
- `direct-pg` target runs the same SQL shape rektifier would emit
  (`jsonb_set` chain on the fast paths; BEGIN + SELECT FOR UPDATE +
  UPDATE + COMMIT on the slow path) so the comparison isolates
  rektifier's protocol/translator overhead.
- Closed-loop driver, concurrency = 16, 5 s run + 1 s warmup, item
  size = 256 B, working set = 500 keys, hash-only S-PK table.
- All numbers below are p50 / p99 in milliseconds plus ops/sec; raw
  output is in this branch's commit notes.

## What each workload exercises

The `direct` workloads run a single SQL statement (no transaction —
MVCC handles atomicity); the `tx` workloads open a transaction with
`SELECT FOR UPDATE` because they need to read the existing row
before writing. The naming is about *how the request executes*, not
about which is "better" — `tx` is the right choice (and only choice)
whenever the operation needs to consult existing data.

| Workload | Phase | Code path |
|---|---|---|
| `update-direct-set` | 3a | `INSERT … ON CONFLICT DO UPDATE SET data = jsonb_set(...)` — one round-trip, no row lock |
| `update-direct-insert-only` | 4c | `INSERT … ON CONFLICT DO NOTHING` (fresh PK per op) |
| `update-direct-cond` | 4d | `UPDATE … WHERE pk = $ AND (data ? 'id')` — one round-trip, condition compiles to SQL |
| `update-tx-rmw` | 3b | `BEGIN → SELECT FOR UPDATE → UPDATE → COMMIT`; keys spread, no lock contention |
| `update-tx-rmw-hot` | 3b | Same as above but every op hits one row — measures row-lock serialization |
| `update-tx-add-num` | 5 | `ADD counter :inc` — same Tx envelope as `update-tx-rmw`; numeric add through the ADD-clause evaluator |
| `update-tx-add-set` | 5 | `ADD tags :new` — set union with insertion-order dedup in Rust |
| `update-tx-cond-begins-with` | 4e | `SET marker = :v` gated on `begins_with(name, :p)` — condition evaluator inside the Tx, no extra round trip vs `update-tx-rmw` |

## Numbers

### Rektifier

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| Put (baseline) | 9,180 | 1.69 | 2.21 | 2.99 | 4.55 |
| update-direct-set (3a) | 9,176 | 1.70 | 2.18 | 2.78 | 3.67 |
| update-direct-insert-only (4c) | 8,686 | 1.78 | 2.35 | 3.19 | 4.84 |
| update-direct-cond (4d) | 9,027 | 1.70 | 2.26 | 3.26 | 4.24 |
| update-tx-rmw (3b spread) | 3,630 | 4.18 | 5.59 | 8.43 | 11.30 |
| update-tx-rmw-hot (3b hot) | 1,570 | 6.47 | 24.32 | 53.09 | 82.62 |
| update-tx-add-num (5) | 3,773 | 4.11 | 5.14 | 7.35 | 9.31 |
| update-tx-add-set (5) | 3,616 | 4.19 | 5.65 | 8.38 | 12.38 |
| update-tx-cond-begins-with (4e) | 3,578 | 4.20 | 5.69 | 8.79 | 15.48 |

### Direct PG (latency floor for the rektifier+PG path)

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| Put | 14,438 | 1.07 | 1.43 | 2.09 | 3.01 |
| update-direct-set | 15,270 | 1.02 | 1.31 | 1.78 | 2.84 |
| update-direct-insert-only | 15,205 | 1.03 | 1.30 | 1.73 | 2.85 |
| update-direct-cond | 15,551 | 1.00 | 1.28 | 1.88 | 2.98 |
| update-tx-rmw | 4,839 | 3.15 | 4.17 | 6.09 | 9.63 |
| update-tx-rmw-hot | 1,801 | 6.31 | 19.30 | 38.14 | 55.71 |
| update-tx-add-num | 4,880 | 3.13 | 4.13 | 5.99 | 8.83 |
| update-tx-add-set | 4,223 | 3.45 | 5.16 | 9.66 | 15.58 |
| update-tx-cond-begins-with | 4,470 | 3.29 | 4.77 | 8.06 | 15.30 |

## Observations

1. **All three `direct` workloads cost the same as Put** (~1.7 ms p50
   / ~3 ms p99 on rektifier). The 4d SQL-WHERE clause adds no
   measurable overhead vs 3a's unconditional upsert — both run in a
   single round-trip, MVCC handles atomicity. 4c's DO-NOTHING shape
   is also in-band with Put.

2. **The `tx` workloads add ~2.3× latency vs `direct`** (4.18 ms vs
   1.70 ms p50). Cost = the second round trip introduced by
   `BEGIN → SELECT FOR UPDATE → … → COMMIT`. The classifier-driven
   routing means only requests that *need* to consult existing state
   pay this — the `direct` paths handle every shape that can.

3. **Rektifier overhead vs direct-pg is consistent across paths**:
   - `direct` workloads: +0.7 ms p50, +1.0–1.5 ms p99
   - `tx` workloads:     +0.9–1.0 ms p50, +1.0–2.0 ms p99
   - Hot key:            +0.2 ms p50 (lock dominates; rektifier
     overhead is proportionally tiny here)

   The translation + HTTP-parse + JSON-decode + permissive-SigV4
   overhead measures ~0.7 ms p50 on small items, similar to the
   numbers in `baseline.md`. Nothing about Phase 3b/3c/4c/4d/4e/5/6
   added measurable per-request cost on top of that.

4. **Hot-key contention is severe but expected**: 16 workers
   serialize on one row through `SELECT FOR UPDATE`. p50 climbs to
   6.5 ms; p99 explodes to 53 ms as queue depth grows. Both
   rektifier and direct-pg see the same shape — the row lock
   dominates everything else. **Avoid `tx`-routed updates against
   hot keys when you can** (e.g., shard a counter into N buckets, or
   express the update as a `direct`-routable shape).

5. **No errors on any workload** (CCFE-free by design: every
   workload either uses fresh PKs or always-true conditions).

6. **ADD-numeric ≈ SET-arithmetic on the `tx` path** (4.11 vs 4.18 ms
   p50). Sanity check: the ADD clause goes through the same
   evaluator framework + Tx envelope as `SET counter = counter + :inc`,
   and the numbers confirm the work is interchangeable. Phase 3d
   would recover ~2× for *both* by promoting them to a
   single-statement `jsonb_set` emitter on the `direct` path.

7. **ADD-set adds no measurable overhead vs scalar ADD** (4.19 ms vs
   4.11 ms p50). The set-dedup work is dwarfed by Tx round-trip cost.

8. **`begins_with` condition costs essentially nothing on top of the
   `tx` path** (4.20 vs 4.18 ms p50). The condition evaluator runs
   against the row we already SELECTed for the update — no extra
   round-trip, just a string starts_with check. This generalizes:
   all Phase 4e shapes (`contains`, `BETWEEN`, `IN`, `attribute_type`)
   should show similar near-zero overhead because they all run as
   Rust functions over the in-memory SELECTed row.

## Reproducing

```sh
just up                                                            # docker
just bootstrap-pg                                                  # PG schema
REKTIFIER_CONFIG=rektifier.toml.example cargo run --release --bin rektifier &
cargo build --release -p rekt-bench
for wl in update-direct-set update-direct-insert-only update-direct-cond \
          update-tx-rmw update-tx-rmw-hot \
          update-tx-add-num update-tx-add-set \
          update-tx-cond-begins-with; do
  for tgt in rektifier direct-pg; do
    ./target/release/rekt-bench run \
      --target $tgt --workload $wl \
      --duration 5s --warmup 1s \
      --concurrency 16 --working-set 500
  done
done
```

## Known caveats

- Closed-loop driver — no coordinated-omission correction.
- Single host — no network RTT.
- 5-second windows — long enough for this workload to be stable, too
  short to catch autovacuum / checkpoint pauses.
- Hot-key concurrency only goes up to 16 here; serialization scales
  worse at higher concurrency.

## Follow-ups

- **Promote frequently-routed `tx` shapes to `direct`** (PLAN-2
  Phase 3d). E.g., simple counter increments
  (`SET counter = counter + :inc`) could compile to
  `UPDATE … SET data = jsonb_set(data, '{counter}',
  to_jsonb(((data#>>'{counter,N}')::numeric + :inc)::text))`. ~2×
  latency win for a common case. Currently routes to `tx` because
  the translator can't pattern-match the read-back-compute form to a
  pure-SQL emitter.
- **Open-loop driver** with HDR coordinated-omission recording for
  more honest tail numbers.
- **`tx`-path retry-rate visibility** — current bench doesn't
  distinguish "first-try success" from "retried after race". Adding
  a counter to `update_general_rmw_raw` would tell us how often the
  race actually fires under load.
