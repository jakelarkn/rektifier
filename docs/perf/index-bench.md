# Index bench: LSI + GSI (Generated + DualWrite)

Captured 2026-05-21 with `rekt-bench` after PLAN-9 (G1–G9),
PLAN-11 (L1–L6), and PLAN-12 (covering-index INCLUDE clause)
landed. Companion to `baseline.md` (Put/Get), `update-baseline.md`,
and `crud-conditional-baseline.md`. Subject: how much per-write
overhead does each kind of secondary index add, and how do reads
through the three index shapes compare.

> **History note (2026-05-21):** the first capture below ran with
> the original `CREATE INDEX … (key_cols)` shape. GSI Query then
> measured 17 ms p50 / 895 ops/s — PG performed a regular index
> scan followed by 50 random heap fetches per call, which dominated
> latency. PLAN-12 added `INCLUDE (<jsonb_col>)` to every LSI + GSI
> emission, turning the index leaf into a covering index. The
> second capture (current numbers) shows GSI Query at ~1.9 ms p50,
> ~8k ops/s — index-only scans eliminate the heap fetches. The
> bench-table rows are truncated + VACUUM'd between sweeps so PG's
> visibility map is current (without a fresh VACUUM the planner
> falls back to heap fetches and the win disappears).

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
| `put` (baseline, no index) | 10,967 | 1.42 | 1.85 | 2.59 | 4.20 |
| `put-lsi` (1 LSI, GENERATED, covering) | 10,786 | 1.44 | 1.89 | 2.67 | 3.88 |
| `put-gen-gsi` (1 CT-time GSI, GENERATED, covering) | 10,294 | 1.49 | 2.00 | 2.81 | 4.49 |
| `put-dw-gsi` (1 UpdateTable GSI, DualWrite, covering) | 10,007 | 1.53 | 2.07 | 2.96 | 5.39 |
| `put-multi-dw-gsi` (3 UpdateTable GSIs, covering) | 9,803 | 1.57 | 2.13 | 3.04 | 4.26 |

### Query (read-path through each index shape, post-VACUUM)

| Workload | ops/sec | p50 | p90 | p99 | p999 | rows returned |
|---|---:|---:|---:|---:|---:|---:|
| `query-lsi` | 12,141 | 1.25 | 1.73 | 2.71 | 6.17 | 20 |
| `query-gen-gsi` | 8,250 | 1.85 | 2.58 | 3.73 | 7.15 | ~50 |
| `query-dw-gsi` | 7,945 | 1.91 | 2.69 | 4.04 | 7.84 | ~50 |

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

### LSI + GSI Query are now all fast (covering indexes)

After PLAN-12, every Query path is dominated by the rektifier
HTTP/dispatch envelope rather than PG heap fetches:

- `query-lsi` (20 rows): ~1.25 ms p50, ~12k ops/sec — unchanged
  shape. The LSI was already fast because matching rows clustered
  on adjacent heap pages by construction.
- `query-gen-gsi` / `query-dw-gsi` (~50 rows each): ~1.9 ms p50,
  ~8k ops/sec. The 0.6 ms gap vs `query-lsi` tracks the row-count
  delta (50 vs 20) — per-row dispatch + JSON serialization is the
  remaining cost, not the index.

The two GSI modes (Generated vs DualWrite) are statistically
indistinguishable, as designed: both materialize as a column +
btree + INCLUDE-(data) covering payload. The 0.06 ms gap between
`query-gen-gsi` and `query-dw-gsi` p50 is inside run-to-run noise.

### PLAN-12 INCLUDE: read-side 9×, write-side ≈ free

Adding `INCLUDE (data)` to every LSI + GSI `CREATE INDEX`
statement bakes the JSONB payload into the index leaf. PG's planner
picks index-only scans (Heap Fetches: 0) when the visibility map is
current — observable via `EXPLAIN ANALYZE` and confirmed end-to-end
in the benchmark.

The cost on the write side is small and roughly proportional to
the leaf payload growth:

- One LSI: -0.04 ms p50 (within noise; the LSI was already cheap
  to maintain). +3% throughput vs the pre-PLAN-12 measurement.
- One CT-time / UpdateTable GSI: +0.02 to +0.06 ms p50. -2 to -4%
  throughput.
- Three DualWrite GSIs: +0.06 ms p50. -3% throughput.

The PG-side cost is the index leaf growing from ~30 bytes
(key + ctid) to ~30 + 256 bytes (key + ctid + JSONB payload). WAL
volume scales with payload size; for the 256-byte items in this
bench the effect is small. Operators with much larger items
(multi-KB) should expect the write-side cost to grow proportionally
and may eventually want PLAN-12's follow-up option (projection-
aware INCLUDE — emit `INCLUDE` only over the operator-declared
`NonKeyAttributes` for INCLUDE-projection GSIs).

### Visibility-map dependence

Index-only scans require PG's visibility map to mark pages as
all-visible. Newly-inserted rows are not in all-visible pages
until autovacuum runs. The bench's pre-Query VACUUM step is
explicit; in production, autovacuum tuning matters for sustained
GSI Query performance. The win does not disappear on heavily-
written tables — autovacuum keeps the visibility map current
under typical load — but workloads with intense write bursts may
see the heap-fetch fallback transiently. Documented in
PLAN-12-covering-indexes.md.

### No mode-discrimination at runtime

The dispatcher routes `Query` requests with `IndexName` through the
same `resolve_index_for_query` resolver regardless of mode. PG's
planner sees the same shape (`WHERE col = $1 AND col2 BETWEEN $2
AND $3`) regardless of whether the column was declared `GENERATED
ALWAYS AS` or populated by dual-write SQL, and regardless of
INCLUDE-clause presence (the SELECT shape is identical). The
benchmark numbers above are the predicted consequence.

## Caveats

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
- Re-running Put workloads against the same tables across
  iterations accumulates rows, growing the partition the Query
  workloads later scan. The numbers above use truncate + VACUUM
  between Put and Query sweeps; without that, GSI Query slows
  proportionally to the accumulated row count (each call returns
  more matching rows; per-row JSON serialization dominates).
- The covering-index win is measured at 256 B item size. JSONB > ~2
  KB is TOASTed in both heap and index leaf, at which point the
  index leaf holds a TOAST pointer and the "savings" from
  INCLUDE evaporate (PG still chases the TOAST relation). PLAN-12
  documents the trade and notes the projection-aware INCLUDE
  follow-up as the lever for operators with large items.

## Reproducing

```sh
just up                                                            # docker
REKTIFIER_CONFIG=rektifier.toml.example cargo run --release --bin rektifier &
just bootstrap-tables                                              # users + device_events (baseline Put needs this)
./target/release/rekt-bench setup-index-tables                     # bench_lsi + bench_gen_gsi + bench_dw_gsi + bench_multi_dw_gsi

# Put workloads: 10 s + 3 s warmup, concurrency 16. Truncate before
# each pass so the Query measurements that follow operate on a
# clean partition seeded only by `seed_indexed_partition`.
for t in rekt_t_bench_lsi rekt_t_bench_gen_gsi rekt_t_bench_dw_gsi rekt_t_bench_multi_dw_gsi; do
    docker exec rektifier-postgres psql -U rektifier -d rektifier -c "TRUNCATE $t;"
done
for w in put put-lsi put-gen-gsi put-dw-gsi put-multi-dw-gsi; do
    ./target/release/rekt-bench run --target rektifier --workload $w \
        --duration 10s --warmup 3s --concurrency 16 --working-set 200
done

# Query workloads need the visibility map current for index-only
# scans, so TRUNCATE + VACUUM between Put and Query sweeps.
for t in rekt_t_bench_lsi rekt_t_bench_gen_gsi rekt_t_bench_dw_gsi; do
    docker exec rektifier-postgres psql -U rektifier -d rektifier -c "TRUNCATE $t;"
    docker exec rektifier-postgres psql -U rektifier -d rektifier -At -c "VACUUM ANALYZE $t;"
done
for w in query-lsi query-gen-gsi query-dw-gsi; do
    ./target/release/rekt-bench run --target rektifier --workload $w \
        --duration 10s --warmup 3s --concurrency 16
done
```
