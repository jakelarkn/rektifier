# Project conventions for Claude

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
`score`, `version`, `tags`, `role`, `email`, `title`, `balance`,
`amount`, `extent`, `goner`, `summed`, `binmark`, `val`.

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

## Plan documents

Implementation plans go in `docs/plan/` (gitignored). Never commit
them.

## Commit messages

No `Co-Authored-By: Claude ...` trailer.
