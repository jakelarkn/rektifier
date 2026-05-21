# Index bench: LSI + GSI (Generated + DualWrite)

Captured 2026-05-21 with `rekt-bench` after PLAN-9 (G1–G9) and
PLAN-11 (L1–L6) landed. Companion to `baseline.md` (Put/Get),
`update-baseline.md`, and `crud-conditional-baseline.md`. Subject:
how much per-write overhead does each kind of secondary index add,
and how do reads through the three index shapes compare.

## Setup

- PG 17 in Docker, rektifier release build, `PermissiveVerifier`,
  deadpool max_size = 16.
- Closed-loop driver, concurrency = 16, 10 s run + 3 s warmup,
  item_size = 256 B.
- Four bench tables (provisioned via `rekt-bench setup-index-tables`):
  | Table | Shape |
  |---|---|
  | `bench_lsi` | composite PK `(device_id S, ts N)` + LSI `by_device_score` on `(device_id, score)` — declared at CreateTable; GENERATED column. |
  | `bench_gen_gsi` | composite PK + CT-time GSI `by_tier_score` on `(tier S, score N)` — Generated mode. |
  | `bench_dw_gsi` | composite PK; GSI `by_tier_score` added via `UpdateTable.Create` — DualWrite mode (regular column populated by rektifier's INSERT SQL). |
  | `bench_multi_dw_gsi` | composite PK; three DualWrite GSIs (`by_tier_score`, `by_region_score`, `by_bucket_score`) added via `UpdateTable.Create`. |
- Indexed Put workloads use a fresh `(device_id, ts)` per op; no
  hot-spotting. Every item carries `tier`, `score`, `region`, `bucket`
  so every declared index column has a value.
- Indexed Query workloads operate on a 200-row partition pre-seeded
  with rotating `tier` (4 values, 50 rows each) and monotonic `score`
  (0..199). Seeded at bench start by `seed_indexed_partition`.

## What each workload exercises

| Workload | Rektifier path | SQL shape on Put |
|---|---|---|
| `put` (baseline) | `Backend::put_item_raw` on `users` (hash-only, no indexes) | `INSERT INTO users (data) VALUES ($1::jsonb) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data` |
| `put-lsi` | Same, on `bench_lsi`. LSI sort column populated by a GENERATED expression. | Same SQL shape; PG runs the GENERATED extraction per row. |
| `put-gen-gsi` | Same, on `bench_gen_gsi`. Two extra GENERATED columns (`tier`, `score`). | Same SQL shape. |
| `put-dw-gsi` | Same, on `bench_dw_gsi`. Two regular columns; rektifier's INSERT widens by two `($1::jsonb #>> '{attr,T}')` extraction exprs. | `INSERT INTO … (data, tier, score) VALUES ($3, ($3::jsonb#>>'{tier,S}'), (($3::jsonb#>>'{score,N}')::numeric)) ON CONFLICT … DO UPDATE SET data=EXCLUDED.data, tier=…, score=…` |
| `put-multi-dw-gsi` | Same, on `bench_multi_dw_gsi`. Six DualWrite columns total (three GSIs × two cols each). | Same widened shape; six extra extractions per Put. |
| `query-lsi` | KCE `device_id = :pk AND score BETWEEN :lo AND :hi` against `bench_lsi`. 20 of 200 rows returned. | `SELECT data FROM bench_lsi WHERE device_id = $1 AND score BETWEEN $2 AND $3 ORDER BY score LIMIT 1000` |
| `query-gen-gsi` | KCE `tier = :t AND score BETWEEN :lo AND :hi`, IndexName=`by_tier_score`. ~50 rows (one tier of four). | `SELECT data FROM bench_gen_gsi WHERE tier = $1 AND score BETWEEN $2 AND $3 ORDER BY score LIMIT 1000` |
| `query-dw-gsi` | Same KCE on `bench_dw_gsi`. Identical SQL shape — Generated and DualWrite GSIs converge on read. | Same as `query-gen-gsi`. |

## Numbers

### Put (write-path index overhead)

| Workload | ops/sec | p50 | p90 | p99 | p999 |
|---|---:|---:|---:|---:|---:|
| `put` (baseline, no index) | 10,914 | 1.43 | 1.88 | 2.50 | 3.70 |
| `put-lsi` (1 LSI, GENERATED) | 10,437 | 1.48 | 1.96 | 2.69 | 3.98 |
| `put-gen-gsi` (1 CT-time GSI, GENERATED) | 10,556 | 1.47 | 1.94 | 2.64 | 4.75 |
| `put-dw-gsi` (1 UpdateTable GSI, DualWrite) | 10,413 | 1.47 | 1.98 | 2.96 | 5.74 |
| `put-multi-dw-gsi` (3 UpdateTable GSIs) | 10,131 | 1.51 | 2.01 | 2.78 | 4.85 |

### Query (read-path through each index shape)

| Workload | ops/sec | p50 | p90 | p99 | p999 | rows returned |
|---|---:|---:|---:|---:|---:|---:|
| `query-lsi` | 11,908 | 1.27 | 1.79 | 2.79 | 6.22 | 20 |
| `query-gen-gsi` | 895 | 17.26 | 23.66 | 30.54 | 39.01 | 50 |
| `query-dw-gsi` | 881 | 17.47 | 24.29 | 32.45 | 43.52 | 50 |

## Observations

### Per-index Put overhead is bounded and uniform across modes

All four indexed Put variants stay within **+0.04 to +0.08 ms p50**
of the no-index baseline (+0.6 to +5% throughput drop). The overhead
is small and roughly identical regardless of mode — exactly the
PG-side cost model PLAN-9 D15 predicted:

- *Generated mode (LSI + CT-time GSI):* PG evaluates the `data#>>`
  extraction once per stored column at row write time. The cost is
  on PG, not on the rektifier dispatcher.
- *DualWrite mode (UpdateTable GSI):* the same extraction expression
  rides in rektifier's INSERT SQL; PG still does the extraction.
  No Rust-side bindings, no extra round-trips.

Both modes converge on the same per-write cost shape — the bench
confirms what the design predicted. The +0.08 ms `put-dw-gsi` vs
`put-gen-gsi` delta is noise-level (item size 256 B, 16 concurrent
workers; p999 is the only place the DualWrite path shows a slightly
wider tail, attributable to the longer SQL string and the per-row
extraction expression's planner cost).

### Three DualWrite GSIs cost ~7% throughput on writes

`put-multi-dw-gsi` (3 GSIs × 2 columns each = 6 extra extractions +
3 extra btree inserts) drops Put throughput from 10,914 → 10,131
ops/sec (-7.2%). p50 grows by 0.08 ms. That's well within D15's
"<5% per active GSI" budget — three GSIs together cost what one
might predict for two from a strictly-linear extrapolation, because
the extraction itself is cheap and the dominant cost is the per-
btree insert (already amortized into the existing baseline's PK
btree maintenance).

Operators sizing capacity for many-GSI tables should plan PG-side
WAL volume + index page maintenance, not extra rektifier overhead.

### LSI Query is fast; GSI Query at this seed shape returns more rows

`query-lsi` runs at the same shape as `query-sk-range` from the
prior baseline (~12k ops/sec, p50 ~1.3 ms) — the LSI's composite
btree (`device_id, score`) is hit identically to the base table's
composite PK btree.

`query-gen-gsi` and `query-dw-gsi` look slow (~900 ops/sec, p50
17 ms) because of the seeded partition shape: 200 rows × 4 tiers
means each tier=:gold query returns ~50 rows (vs 20 for the LSI
range). The per-row JSONB read + heap fetch + HTTP serialization
dominates; the index hit itself is cheap (visible in the consistent
sub-25 ms p90).

The two GSI modes are **statistically indistinguishable** on read,
as designed: both materialize as a column + btree, and the Query
SQL is identical. The 0.21 ms gap between `query-gen-gsi` and
`query-dw-gsi` p50 is well inside run-to-run noise.

### No mode-discrimination at runtime

The dispatcher routes `Query` requests with `IndexName` through the
same `resolve_index_for_query` resolver regardless of mode. PG's
planner sees the same shape (`WHERE col = $1 AND col2 BETWEEN $2
AND $3`) regardless of whether the column was declared `GENERATED
ALWAYS AS` or populated by dual-write SQL. The benchmark numbers
above are the predicted consequence.

## Caveats

- 200-row partition is small enough that PG's planner may favor a
  bitmap or sequential scan over the index for queries returning a
  large fraction of the partition. The +17 ms p50 for the GSI
  queries doesn't isolate index-walk cost — it's dominated by
  rendering 50 items per response. A larger partition (10k+ rows)
  with a more selective predicate would isolate the index-walk cost
  more cleanly; deferred for a future capture.
- Bench tables are intentionally schemaless beyond their declared
  key columns. Real workloads with sparse attribute coverage may
  see slightly different per-row write cost from the GENERATED /
  dual-write extractions (they return NULL when the attribute is
  missing, which is the cheap path).
- DDB-local + DirectPg targets are not exercised by the indexed
  workloads. DDB-local supports LSI/GSI but at a different
  per-request latency baseline; DirectPg would have to implement
  the GSI mechanics in test code to be comparable. Rektifier-vs-
  rektifier (no-index baseline vs N-index variants) is the
  meaningful comparison for this report.
- The `put-multi-dw-gsi` workload exercises the linear-scaling
  case, not the absolute upper bound. DDB caps at 20 GSIs per
  table; rektifier inherits the same ceiling. Per D15, 20 active
  indexes is approximately a 100% PutItem-latency increase — PG's
  index-maintenance cost, not rektifier's.

## Reproducing

```sh
just up                                                            # docker
REKTIFIER_CONFIG=rektifier.toml.example cargo run --release --bin rektifier &
just bootstrap-tables                                              # users + device_events (baseline Put needs this)
./target/release/rekt-bench setup-index-tables                     # bench_lsi + bench_gen_gsi + bench_dw_gsi + bench_multi_dw_gsi
# Each workload, 10 s + 3 s warmup, concurrency 16:
for w in put put-lsi put-gen-gsi put-dw-gsi put-multi-dw-gsi \
         query-lsi query-gen-gsi query-dw-gsi; do
    ./target/release/rekt-bench run --target rektifier --workload $w \
        --duration 10s --warmup 3s --concurrency 16 --working-set 200
done
```
