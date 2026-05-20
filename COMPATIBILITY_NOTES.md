# Rektifier — Compatibility Notes

Documented behavioral divergences from Amazon DynamoDB, scoped to the
APIs rektifier implements end-to-end (PutItem, GetItem, DeleteItem,
UpdateItem, Query, Scan) plus the BatchGetItem / BatchWriteItem
section, which pre-documents divergences for the in-progress
PLAN-6 work so the parity contract is locked when the implementation
lands. APIs not yet planned (TransactGetItems, TransactWriteItems,
CreateTable/DescribeTable/DeleteTable/ListTables, Streams) are tracked
in `docs/plan/PLAN-2-feature-completeness.md` rather than here.

This is a living document. Add an entry whenever we land a feature
that knowingly diverges from DDB, or whenever a diff-test reveals an
existing divergence.

---

## Severity legend

- **Lenient** — we accept input or behavior that DDB rejects. Code
  written against rektifier may fail when moved to DDB. *Migration
  risk.*
- **Strict** — we reject input or behavior that DDB accepts.
  Code written against DDB may fail against rektifier. *Onboarding
  risk.*
- **Observable** — same input, different observable response shape
  or value. *Correctness risk.*
- **Inert** — feature accepted on the wire / in config but has no
  effect.
- **Precision** — numeric semantics differ in edge cases.
- **Parity-unverified** — divergence is suspected but not yet
  confirmed via the diff harness. Tracked here to remember to verify.

---

## Cross-cutting

### `ConsistentRead` is silently honored, surfaced via tracing — Inert

DDB defaults to eventually-consistent reads and offers
`ConsistentRead=true` for strongly-consistent reads at ~2× RCU cost.
Rektifier's backend is a single PG instance — every read sees a
consistent MVCC snapshot regardless of the flag, which is at least as
strong as `ConsistentRead=true`. The flag is accepted (no error), has
no effect on the underlying read, and is recorded in the request
tracing span as `consistent_read=<bool>` for parallel-deployment
debugging.

**Affected ops:** GetItem (field not currently parsed — see
`crates/rekt-protocol/src/operations.rs`). Will extend to Query / Scan
when those land per `PLAN-4-query-scan.md` D7.

**Migration risk:** none from DDB → rektifier (strictly stronger).
Rektifier → DDB code that *relies on* eventual-consistency
characteristics (e.g. cache-friendly read latency) may behave
differently under load.

### SigV4 signatures: permissive verifier accepts everything

The `PermissiveVerifier` impl in `crates/rekt-sigv4/src/lib.rs` logs a
warning on first use and otherwise accepts every request. The
`StrictVerifier` (real SigV4 verification + credentials store) is not
yet implemented.

**Affected ops:** all.

**Migration risk:** code that doesn't actually sign requests
correctly will pass against rektifier and fail against DDB. Conversely
rektifier should not be exposed beyond a trusted network until the
strict verifier lands.

### Per-table size limits parsed but not enforced — Inert

`Limits` struct in `crates/rekt-config/src/lib.rs` parses
`max_item_size_bytes` (default 400 KB / 409_600 B per DDB),
`max_partition_key_bytes`, `max_sort_key_bytes`, etc. **None of these
are enforced anywhere** — items larger than the configured cap will
write successfully; oversized keys will write successfully (subject
only to PG's own row-size limits, which are far larger).

**Migration risk:** an application that exceeds DDB's 400 KB item
limit works against rektifier and silently truncates / errors against
DDB.

**Closure plan:** tracked in `PLAN-2-feature-completeness.md` under
"Per-table size-limit enforcement."

### Reserved-word strictness matches DDB — Parity

`is_reserved` in `crates/rekt-expressions/src/reserved_words.rs` carries
the canonical ~573-word DDB reserved-word list and rejects bare names
in UpdateExpression / ConditionExpression with
`ValidationException: Invalid <kind>Expression: Attribute name is a
reserved keyword; reserved keyword: <word>`. Aliased names
(`#x` → `"name"`) bypass the check. Diff-tested for parity.

**Convention** (also in `CLAUDE.md`): don't use bare reserved words
in main-path tests — pick safe alternatives so the test exercises
the feature, not the reservation rule.

### Parser error wording is not byte-identical to DDB — Observable, low-impact

DDB has highly specific parser error strings (e.g.
`"Invalid UpdateExpression: Syntax error; token: …"`). Rektifier
matches a subset (notably the reserved-word rejection wire shape)
but doesn't aim for byte-equivalence. The diff harness flags
divergence; we accept it for now.

**Specific known cases:**
- `KeyConditionExpression` parse errors will carry the
  ConditionExpression label (see `PLAN-4` D1). Lift via dedicated
  parser if/when fidelity becomes a hard requirement.

---

## GetItem

### `ConsistentRead` is silently ignored — Inert

`GetItemRequest` in `crates/rekt-protocol/src/operations.rs` does not
parse the field. See the cross-cutting `ConsistentRead` entry above.
**Closure plan:** add the field, log into tracing, no behavioral
change.

### `ProjectionExpression` / `AttributesToGet` are silently ignored — Inert

Neither field is on `GetItemRequest`. Callers requesting a subset of
attributes receive the full item. Returned shape is otherwise
correct; the divergence is bandwidth-only.

**Migration risk:** an application relying on projection to avoid
loading sensitive attributes will leak them through rektifier.

**Closure plan:** tracked in `PLAN-2` "GetItem" gaps.

---

## PutItem

### Duplicate set members are accepted — Parity-unverified / likely Lenient

DDB rejects `{"SS":["a","a"]}` / `{"NS":["1","1"]}` /
`{"BS":["AA==","AA=="]}` with `ValidationException`. Rektifier's
protocol layer deserializes any `Vec<String>` for set types
(`crates/rekt-protocol/src/attribute_value.rs`) and the translator
doesn't validate uniqueness — the duplicate round-trips through
PG and reappears in subsequent reads.

**Status:** parity not yet exercised in the diff harness.

### Empty string / empty binary values are accepted — Parity

DDB has allowed empty `S` and `B` non-key attribute values since
2020. Rektifier accepts them too; diff-tested.

### Empty string / empty binary as a *key* value is accepted — Parity-unverified / likely Lenient

DDB rejects an empty string / empty binary value for partition or
sort key attributes (`ValidationException`). Rektifier's
`extract_key` in `crates/rekt-translator/src/translate.rs` does not
check for emptiness.

**Status:** parity not yet exercised in the diff harness.

### Number precision: 38-digit DDB N round-trips losslessly through Put/Get — Parity

`N` is stored as the original DDB-JSON string (e.g. `"3.14159..."`)
inside the JSONB column; no numeric conversion happens on the
Put → Get path. Precision is only lost when arithmetic touches the
value (see UpdateItem § "ADD numeric" below).

---

## DeleteItem

No known divergences beyond the cross-cutting items above. ALL_OLD
return values, conditional delete, idempotent missing-row delete are
all parity-tested in `tests/diff/tests/diff.rs`.

---

## UpdateItem

### Nested paths in UpdateExpression / ConditionExpression are rejected — Strict

Paths must be top-level (`score`, not `meta.score` or `items[0]`).
`crates/rekt-translator/src/checks.rs::check_top_level` and
`check_condition_paths_top_level` reject deeper paths with
`UnsupportedNestedPath` / `UnsupportedConditionNestedPath`. DDB
accepts these.

**Migration risk:** DDB → rektifier code that uses dotted or
bracketed paths fails with a clear error at the translator boundary.

**Closure plan:** Phase 8 in `PLAN-2-feature-completeness.md`.

### `size(path)` operand in ConditionExpression is rejected — Strict

The parser deliberately omits the `size(...)` production
(`crates/rekt-expressions/src/parser.rs` — comment explicitly defers
it). Expressions like `size(items) > :n` fail at parse time. DDB
accepts them.

**Closure plan:** lift alongside the Phase 8 nested-path work — the
two share an `Operand` extension.

### ADD numeric uses f64 precision — Precision

`apply_add` in `crates/rekt-translator/src/eval/update.rs` parses
both operands as `f64` and formats the sum via `format_decimal`. DDB
internally uses 38-digit decimal arithmetic. For typical numeric
ranges (≤ ~15 significant digits) the result is identical; at the
edges, rektifier's result will diverge.

**Examples of divergence:**
- `ADD counter :1` where existing `counter = "9007199254740993"`
  (one past `2^53`) — DDB returns `"9007199254740994"`, rektifier
  returns `"9007199254740994"` *if it round-trips correctly* but the
  intermediate f64 can't represent the value exactly, so adjacent
  values may collapse.
- Adding very small decimals to very large decimals — the small
  addend may be silently truncated.

**Migration risk:** counter / accumulator workloads that exceed the
f64 safe-integer range diverge silently. Audit logs, billing
counters, high-precision financial values are the obvious red flags.

**Closure plan:** swap `f64` for an arbitrary-precision decimal
(`bigdecimal` or `rust_decimal`) when Phase 3d in `PLAN-2` lifts
counter-increment to a fast-path emitter; that phase already calls
out the precision concern.

### `begins_with` on B-typed paths returns `false` instead of matching — Observable

The slow-path evaluator
(`crates/rekt-translator/src/eval/condition.rs::eval_begins_with`)
explicitly comments "Skip for now — the slow path doesn't decode
binary." Any `begins_with(<B-attr>, :v)` evaluates to `false`
regardless of whether `:v` is in fact a prefix of the B value. DDB
performs a real binary prefix check.

**Migration risk:** conditional updates / future Query
KeyConditionExpressions that rely on binary prefix matching silently
miss every row against rektifier.

**Closure plan:** lift alongside `PLAN-4` D8 (which makes the same
restriction explicit for Query's KCE).

### `begins_with` on B-typed sort key in `KeyConditionExpression` is rejected — Strict

Per PLAN-4 D8, rektifier rejects `KeyConditionExpression` of the
form `pk = :v AND begins_with(<B-sk>, :pfx)` at translate time with
`ValidationException: begins_with operand must be S`. DDB-local
accepts the same KCE and performs a real binary-prefix match.

**Migration risk:** code that relies on B-SK prefix queries against
DDB will fail against rektifier. The diff harness pins this
divergence in `diff_query_binsorted_begins_with_b_sk_divergence` so
we notice if either side changes.

**Closure plan:** same as the slow-path-eval entry above — lift
together when the evaluator learns to decode B prefixes.

### Updating a key attribute via `SET` / `REMOVE` / `ADD` / `DELETE` is rejected — Parity

`check_not_key` in `crates/rekt-translator/src/checks.rs` rejects any
clause that touches the partition or sort-key attribute, surfacing
as `UpdateTouchesKey`. Matches DDB's `ValidationException`. Tested
in both translator unit tests and dispatch tests.

### Unused `ExpressionAttributeNames` / `Values` are silently accepted — Lenient

DDB rejects requests where `ExpressionAttributeNames` /
`ExpressionAttributeValues` carries an entry not referenced by the
expression (`ValidationException: Value provided in
ExpressionAttributeValues unused in expressions`). Rektifier's
`substitute_*` (`crates/rekt-expressions/src/substitute.rs`) processes
only the referenced placeholders and silently ignores unused entries.

**Migration risk:** dead-code placeholders that pass against
rektifier fail against DDB.

**Status:** parity not yet exercised in the diff harness.

### Reserved-word rejection — Parity

See cross-cutting entry. Tested in translator, dispatch, and diff
layers.

---

## Configuration

### Tables are runtime objects created via `CreateTable` — Parity

(Closed by PLAN-10 D8.) Pre-D8, tables were declared in
`rektifier.toml` under `[[tables]]` and resolved at process startup.
As of D8, the wire DDL API (`CreateTable` / `DeleteTable` /
`UpdateTable`) is the sole provisioning surface; the
`_rektifier_tables` PG catalog is the source of truth.

Closed divergences (kept for historical context):
- `CreateTable` returned `UnknownOperationException` — fixed in D6.
- `DescribeTable` / `ListTables` returned `UnknownOperationException`
  — fixed in D5.
- `DeleteTable` / `UpdateTable` returned `UnknownOperationException`
  — fixed in D7.

Standing divergences in this area are tracked separately:
"DescribeTable wire-status masks internal failure states" (Observable),
"PG table name is auto-derived" (Strict), "TableArn is synthesized
with empty region/account" (Observable + Strict), "Unserveable tables
surface as ResourceNotFoundException" (Strict cause, Observable
message).

### Unserveable tables surface as `ResourceNotFoundException` — Strict (cause), Observable (message)

When the reconciler (PLAN-10 D3) detects PG-schema drift on a
table, that table is flagged `serveable = false` in the in-memory
catalog. Every data-plane handler returns
`ResourceNotFoundException` for ops against an unserveable table.

The cause differs from DDB (DDB only emits RNF when the table truly
doesn't exist); the *wire shape* is identical, so SDK retry logic
that targets RNF behaves the same. The response `message` field is
`"Table not currently serveable: <name> (<unserveable_reason>)"`,
which lets operators tailing logs see what's actually wrong without
needing to inspect `_rektifier_tables` directly.

For batch / transact ops, any unserveable referenced table fails
the whole request with RNF (PLAN-10 KD5). DDB's actual behavior for
a batch referencing a missing or unhealthy table is
`Parity-unverified` — diff-harness exercise pending.

---

## BatchGetItem / BatchWriteItem

These entries describe known divergences planned for the in-progress
Batch ops implementation (`docs/plan/PLAN-6-batch-ops.md`). Until
B1 lands the wire surface, the ops themselves return
`UnknownOperationException`; these entries pre-document the chosen
behavior so the parity contract is locked when the SQL emission is
written.

### Multi-table batches are serial — Observable

Rektifier processes each table in a `BatchGetItem` / `BatchWriteItem`
request serially (one table's SQL completes before the next starts).
DDB itself fans out across partitions, so a multi-table batch
against DDB completes in roughly max-per-table time; against
rektifier it completes in roughly sum-per-table time.

**Affected ops:** BatchGetItem, BatchWriteItem.

**Practical impact:** in our experience, "batch" requests almost
always target a single table (bulk write to one table, multi-key
read from one table). Single-table batches pay no penalty. Real
multi-table fan-out is uncommon, and the divergence is in
wall-clock latency only — items and side effects are identical.

**Closure plan:** PLAN-6 phase B7 — `futures::join_all` across
tables in the dispatcher. The trait shape is already per-table, so
the change is dispatcher-local.

### `AttributesToGet` (legacy projection format) is rejected — Strict

DDB still accepts `AttributesToGet: [String]` on BatchGetItem as a
legacy alternative to `ProjectionExpression`. Rektifier rejects it
at translate time with `ValidationException: AttributesToGet not
supported; use ProjectionExpression` — consistent with how
GetItem, Query, and Scan handle the same field.

**Migration risk:** SDK code written against DDB that uses the
legacy field will fail fast against rektifier. SDKs themselves
default to `ProjectionExpression` in current versions, so this is
mostly a hazard for older / hand-rolled clients.

**Closure plan:** not planned. The two projection formats are
redundant; supporting one is the simpler product call.

### `UnprocessedKeys` / `UnprocessedItems` are always empty — Inert

DDB returns these to signal "we didn't get to these items, retry
them." Rektifier has no equivalent throttle / partial-failure
surface — errors are all-or-nothing per table; an SQL-level
failure on one table fails the whole request with a typed error
response.

The fields are always present in the response (`{}`), well-formed,
and never carry entries.

**Affected ops:** BatchGetItem (`UnprocessedKeys`), BatchWriteItem
(`UnprocessedItems`).

**Migration risk:** code that *handles* `UnprocessedKeys` /
`UnprocessedItems` correctly works identically against rektifier
(the retry path is just unreachable). Code that *forgets* to handle
them works against rektifier and may fail intermittently against
DDB.

**Closure plan:** PLAN-6 phase B7 — when rektifier grows real
per-request size budgeting or a throttle surface, populate the
fields accordingly.

### Quantity limits (100 keys / 25 writes) are configurable — Lenient when raised

DDB hard-caps `BatchGetItem` at 100 keys total and `BatchWriteItem`
at 25 requests total. Rektifier defaults to the same caps but
operators can override via `rektifier.toml`:

```toml
[limits]
batch_get_max_keys = 100      # DDB default; raise at your own risk
batch_write_max_requests = 25 # DDB default; raise at your own risk
```

**Migration risk:** operators who *raise* the cap have moved away
from DDB parity. Code written against a raised rektifier cap will
fail when moved to DDB with the same `ValidationException` DDB
emits at >100 / >25. Operators who lower the cap stay strictly
under DDB and pay no migration cost.

**Closure plan:** none — the operator-tunable limit is the
feature, not a bug. The default matches DDB.

---

## Investigation backlog (parity-unverified)

Items suspected to diverge but not yet confirmed via the diff harness.
Add a diff test, then promote to a real entry above (or remove if
parity-confirmed).

1. **Duplicate set members in PutItem / UpdateItem.**
2. **Empty string / empty binary as a key attribute value.**
3. **Unused `ExpressionAttributeNames` / `Values` entries.**
4. **38-digit ADD numeric near the f64 boundary** — add an explicit
   diff test exercising the divergence so we have a recorded
   reference for the closure plan.
5. **Items at or just over the 400 KB DDB limit** — currently we
   accept; confirm DDB rejects and add a diff test under
   `#[should_panic]` / equivalent.

---

## How to use this document

- **Before writing a feature** — search for any existing entry that
  the feature might lift; if you're closing a gap, remove the entry
  in the same PR.
- **Before debugging a parity failure** — check the relevant API
  section here first; the divergence may be known.
- **When the diff harness surfaces a new divergence** — add an entry
  in the right section, even if the closure plan is "later." A
  known divergence is much cheaper than an unknown one.

Severity-by-API quick index lives at the section headers above. Keep
entries terse; long-form discussion belongs in the relevant
`docs/plan/PLAN-*.md`.
