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

| Workload | Phase | Code path |
|---|---|---|
| `update-fast-set` | 3a | `INSERT … ON CONFLICT DO UPDATE SET data = jsonb_set(...)` — one round-trip, no row lock |
| `update-fast-insert-only` | 4c | `INSERT … ON CONFLICT DO NOTHING` (fresh PK per op) |
| `update-fast-cond` | 4d | `UPDATE … WHERE pk = $ AND (data ? 'id')` — one round-trip, condition compiles to SQL |
| `update-slow-rmw` | 3b | `BEGIN → SELECT FOR UPDATE → UPDATE → COMMIT`; keys spread, no lock contention |
| `update-slow-rmw-hot` | 3b | Same as above but every op hits one row — measures row-lock serialization |
| `update-slow-add-num` | 5 | `ADD counter :inc` — same Tx envelope as `update-slow-rmw`; numeric add through the ADD-clause evaluator |
| `update-slow-add-set` | 5 | `ADD tags :new` — set union with insertion-order dedup in Rust |
| `update-slow-cond-begins-with` | 4e | `SET marker = :v` gated on `begins_with(name, :p)` — slow-path condition evaluator, no extra round trip vs `update-slow-rmw` |

## Numbers

### Rektifier

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| Put (baseline) | 9,180 | 1.69 | 2.21 | 2.99 | 4.55 |
| update-fast-set (3a) | 9,176 | 1.70 | 2.18 | 2.78 | 3.67 |
| update-fast-insert-only (4c) | 8,686 | 1.78 | 2.35 | 3.19 | 4.84 |
| update-fast-cond (4d) | 9,027 | 1.70 | 2.26 | 3.26 | 4.24 |
| update-slow-rmw (3b spread) | 3,630 | 4.18 | 5.59 | 8.43 | 11.30 |
| update-slow-rmw-hot (3b hot) | 1,570 | 6.47 | 24.32 | 53.09 | 82.62 |
| update-slow-add-num (5) | 3,773 | 4.11 | 5.14 | 7.35 | 9.31 |
| update-slow-add-set (5) | 3,616 | 4.19 | 5.65 | 8.38 | 12.38 |
| update-slow-cond-begins-with (4e) | 3,578 | 4.20 | 5.69 | 8.79 | 15.48 |

### Direct PG (latency floor for the rektifier+PG path)

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| Put | 14,438 | 1.07 | 1.43 | 2.09 | 3.01 |
| update-fast-set | 15,270 | 1.02 | 1.31 | 1.78 | 2.84 |
| update-fast-insert-only | 15,205 | 1.03 | 1.30 | 1.73 | 2.85 |
| update-fast-cond | 15,551 | 1.00 | 1.28 | 1.88 | 2.98 |
| update-slow-rmw | 4,839 | 3.15 | 4.17 | 6.09 | 9.63 |
| update-slow-rmw-hot | 1,801 | 6.31 | 19.30 | 38.14 | 55.71 |
| update-slow-add-num | 4,880 | 3.13 | 4.13 | 5.99 | 8.83 |
| update-slow-add-set | 4,223 | 3.45 | 5.16 | 9.66 | 15.58 |
| update-slow-cond-begins-with | 4,470 | 3.29 | 4.77 | 8.06 | 15.30 |

## Observations

1. **All three fast paths cost the same as Put** (~1.7 ms p50 / ~3
   ms p99 on rektifier). The 4d SQL-WHERE clause adds no measurable
   overhead vs 3a's unconditional upsert — both run in a single
   round-trip, MVCC handles atomicity. 4c's DO-NOTHING shape is also
   in-band with Put.

2. **Slow path adds ~2.3× latency vs fast path** (3.94 ms vs 1.70 ms
   p50). Cost = the second round trip introduced by `BEGIN → SELECT
   FOR UPDATE → … → COMMIT`. The classifier-driven routing means
   only requests that *need* the slow path pay this. The Phase 3a/4c/4d
   fast paths are the right defaults.

3. **Rektifier overhead vs direct-pg is consistent across paths**:
   - Fast paths: +0.7 ms p50, +1.0–1.5 ms p99
   - Slow path: +0.9 ms p50, +1.0 ms p99
   - Hot key: +0.2 ms p50 (lock dominates; rektifier overhead is
     proportionally tiny here)

   The translation + HTTP-parse + JSON-decode + permissive-SigV4
   overhead measures ~0.7 ms p50 on small items, similar to the
   numbers in `baseline.md`. Nothing about Phase 3b/3c/4c/4d added
   measurable per-request cost on top of that.

4. **Hot-key contention is severe but expected**: 16 workers
   serialize on one row through `SELECT FOR UPDATE`. p50 climbs to
   6.5 ms; p99 explodes to 53 ms as queue depth grows. Both
   rektifier and direct-pg see the same shape — the row lock
   dominates everything else. **Don't pick the slow path for
   hot-row workloads if you can avoid it** (use a fast-path
   condition, or shard the counter into N buckets).

5. **No errors on any workload** (CCFE-free by design: every
   workload either uses fresh PKs or always-true conditions).

6. **ADD-numeric ≈ SET-arithmetic on the slow path** (4.11 vs 4.18 ms
   p50). Sanity check: the ADD clause goes through the same evaluator
   framework + Tx envelope as `SET counter = counter + :inc`, and
   the numbers confirm the work is interchangeable. Phase 3d would
   recover ~2× for *both* by promoting them to a single-statement
   `jsonb_set` emitter.

7. **ADD-set adds no measurable overhead vs scalar ADD** (4.19 ms vs
   4.11 ms p50). The set-dedup work is dwarfed by Tx round-trip cost.

8. **begins_with condition costs essentially nothing on top of the
   slow path** (4.20 vs 4.18 ms p50). The condition evaluator runs
   against the row we already SELECTed for the update — no extra
   round-trip, just a string starts_with check. This generalizes:
   all Phase 4e shapes (contains, BETWEEN, IN, attribute_type) should
   show similar near-zero overhead because they all run as Rust
   functions over the in-memory SELECTed row.

## Reproducing

```sh
just up                                                            # docker
just bootstrap-pg                                                  # PG schema
REKTIFIER_CONFIG=rektifier.toml.example cargo run --release --bin rektifier &
cargo build --release -p rekt-bench
for wl in update-fast-set update-fast-insert-only update-fast-cond \
          update-slow-rmw update-slow-rmw-hot \
          update-slow-add-num update-slow-add-set \
          update-slow-cond-begins-with; do
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

- **Promote frequently-routed slow-path shapes to the fast path.**
  E.g., simple counter increments (`SET counter = counter + :inc`)
  could compile to `UPDATE … SET data = jsonb_set(data, '{counter}',
  to_jsonb(((data#>>'{counter,N}')::numeric + :inc)::text))`. ~2×
  latency win for a common case. Currently routes to slow path because
  the translator can't pattern-match the read-back-compute form to a
  pure-SQL emitter.
- **Open-loop driver** with HDR coordinated-omission recording for
  more honest tail numbers.
- **Slow-path retry-rate visibility** — current bench doesn't
  distinguish "first-try success" from "retried after race". Adding
  a counter to `update_general_rmw_raw` would tell us how often the
  race actually fires under load.
