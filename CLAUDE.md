# Project conventions for Claude

## Project overview

**Rektifier** is a Rust-based DynamoDB-compatible API sidecar that
translates DDB JSON-1.0 wire requests into PostgreSQL operations.
Operators declare DDB tables in TOML; PG holds the data in `jsonb`
columns with `GENERATED ALWAYS AS ... STORED` columns derived for
key types. The goal is parity with DDB's wire surface so existing
SDK clients can point at rektifier instead of DDB without code
changes. Tracked divergences live in `COMPATIBILITY_NOTES.md`;
in-flight design lives in `docs/plan/` (gitignored).

## Stack

Rust 1.86, cargo workspace. axum + tower-http (HTTP server);
tokio-postgres + deadpool-postgres (PG client/pool); `winnow`
(UpdateExpression / ConditionExpression parsers); serde +
serde_json. Postgres 16+ as the backend. AWS CLI + DynamoDB-local
(docker compose) for diff-test parity.

## Attribute names in tests

DynamoDB has a ~573-word reserved-keyword list. Rektifier enforces this
via `rekt_expressions::is_reserved`: any bare attribute name appearing
in an UpdateExpression or ConditionExpression that matches a reserved
word is rejected with a `ValidationException`. The mechanism to use a
reserved name is the alias placeholder (`ExpressionAttributeNames`:
`{"#n":"name"}` → use `#n` in the expression).

**Do not use bare reserved words as attribute names in main-path tests.**
Pick non-reserved names so your test exercises the feature, not the
reservation rule.

### Known reserved (do NOT use bare in expression strings)

`name`, `status`, `counter`, `source`, `copy`, `size`, `count`, `total`,
`blob`, `items`, `value`, `drop`, `type`, `key`, `data`, `range`,
`action`, `filter`, ... — full list at
<https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html>
or in `crates/rekt-expressions/src/reserved_words.rs`.

When in doubt: `cargo test -p rekt-expressions reserved_words` will
confirm via `is_reserved()`.

### Safe defaults (use these in tests)

`id`, `label`, `flag`, `tally`, `origin`, `entries`, `replica`,
`score`, `version`, `tags`, `tier`, `email`, `title`, `balance`,
`amount`, `extent`, `goner`, `summed`, `binmark`, `val`.

(NOTE: `role` is actually a DDB reserved word — was incorrectly
listed here through Q4. Use `tier` instead.)

### Reserved-word rejection itself

Has dedicated edge-case tests in:
- `crates/rekt-translator/src/lib.rs` (`upd_reject_reserved_*`,
  `upd_aliased_reserved_name_is_accepted`,
  `upd_reserved_word_match_is_case_insensitive`)
- `crates/rekt-server/tests/dispatch.rs`
  (`update_rejects_reserved_*`, `update_accepts_reserved_name_when_aliased`)
- `tests/diff/tests/diff.rs` (`diff_reject_reserved_*`,
  `diff_aliased_reserved_name_is_accepted`)

Don't duplicate that concern in main-path tests.

## Working on a new feature

Anything that isn't a one-shot fix follows this flow:

1. **Write a plan first** in `docs/plan/PLAN-N-<title>.md` (gitignored
   — see next section). Plan structure: decisions lettered D1, D2,
   … with reasoning; data-model impact; module layout; phasing; open
   questions surfaced with recommended defaults. Pattern: see PLAN-2,
   PLAN-3, PLAN-4.
2. **Lock decisions in the plan doc** before writing code. Don't carry
   open questions into implementation; settle them, mark resolutions
   inline ("Decision: ..."), and capture deferred alternatives as
   "Follow-up option."
3. **Break work into phases.** Each phase is independently committable
   and testable. Track in the plan's status checklist (`- [ ]` /
   `- [x]`); mark each box when the phase lands.
4. **Use `TaskCreate` within a session** to track multi-step phase
   work. Mark tasks `in_progress` when started and `completed`
   immediately when done; don't batch.
5. **Commit after each phased milestone** — see "Commit messages"
   below.

## Plan documents

Implementation plans go in `docs/plan/` (gitignored). Never commit
them.

## Test surface for new features

Four test surfaces; new features cover all of them unless a layer is
genuinely irrelevant. Use safe attribute names (see top section).

- **Translator unit tests** in the crate's `mod tests` — happy paths
  + every documented rejection variant.
- **Dispatch tests** in `crates/rekt-server/tests/dispatch.rs` —
  end-to-end through the axum router with the in-memory `MockBackend`.
- **libpq integration tests** in `crates/rekt-storage-libpq/src/lib.rs`
  (`#[ignore]`-gated, against real PG) — confirms the SQL emitter
  behaves identically to the mock.
- **Diff tests** in `tests/diff/tests/diff.rs` (`#[ignore]`-gated,
  against DDB-local) — parity assertion vs. real DDB.

The `MockBackend` in dispatch tests delegates to
`rekt_translator::evaluate_condition` for filter / condition eval so
mock and real PG share semantics — keep that contract intact.

## Compatibility-notes discipline

`COMPATIBILITY_NOTES.md` at the project root tracks known divergences
from DDB on already-implemented APIs.

- **Log any newly-discovered divergence as soon as the diff harness
  surfaces it.** Severity labels: Lenient / Strict / Observable /
  Inert / Precision / Parity-unverified.
- **Don't ship a feature that knowingly diverges without an entry.**
  Unknown divergence is much more expensive than documented divergence.
- **When closing a divergence, remove the entry in the same commit.**
- **Investigation backlog** at the bottom of the doc: items we suspect
  diverge but haven't confirmed. Promote to a real entry (or remove)
  once the diff harness has spoken.

## Commit messages

- **No `Co-Authored-By: Claude ...` trailer.**
- **One commit per phased milestone** (per PLAN-N's phase
  checklist). Title summarizes the phase; body lists changes by
  area (translator / storage / dispatch / tests / docs).
- **No trailing summary text** in the commit body — the message
  describes the change, not a recap of "what I did in this session."

## Operating envelope

- **Don't restart user-controlled processes** (the running `rektifier`
  server, docker containers, etc.) without explicit confirmation.
- **No emoji** in code, comments, commit messages, or docs unless
  explicitly requested.
- **Prefer extending existing modules** over inventing new top-level
  abstractions. The module boundaries from PLAN-3 (R1/R2/R3) define
  the canonical layout — new code lands in the matching focused
  module, not a new sibling. Examples: SQL builders go in
  `crates/rekt-storage-libpq/src/<op>.rs`; translator pipeline
  pieces go in `crates/rekt-translator/src/{plan,translate,classify,
  materialize,checks,keys,key_condition}.rs`; evaluator branches go
  under `eval/`.
- **Don't add backwards-compat shims** for unreleased internal APIs.
  The trait surface widens cleanly per phase; that's the supported
  migration path.

## Useful one-liners

```
cargo test --workspace                          # unit + dispatch (fast)
cargo test -p rekt-storage-libpq -- --ignored   # PG-live (needs `just up` + `just bootstrap-pg`)
cargo test -p rekt-diff-tests -- --ignored      # DDB-local diff (needs rektifier running)
cargo clippy --workspace --tests -- -D warnings
just bench-conditional                          # run the CRUD-conditional bench suite
```
