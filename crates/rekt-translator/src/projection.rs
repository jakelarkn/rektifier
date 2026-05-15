//! Parse + validate DDB `ProjectionExpression` strings.
//!
//! The grammar is a comma-separated list of paths. For Q6 v1, only
//! top-level paths (single identifier or `#alias` placeholder) are
//! supported — nested paths (`a.b`, `c[0]`) are deferred to Phase 8
//! in PLAN-2 (alongside the UpdateExpression / ConditionExpression
//! nested-path lift).
//!
//! Reserved-word handling mirrors UpdateExpression / ConditionExpression:
//! bare names are rejected if reserved; aliases bypass the check.

use crate::error::TranslateError;
use rekt_expressions::is_reserved;
use std::collections::{BTreeMap, BTreeSet};

/// Parse a `ProjectionExpression` string into the set of top-level
/// attribute names to retain in each returned item. Returns `None` for
/// an absent or blank input.
pub(crate) fn parse_projection(
    raw: Option<&str>,
    names: &BTreeMap<String, String>,
) -> Result<Option<BTreeSet<String>>, TranslateError> {
    let Some(input) = raw else { return Ok(None) };
    if input.trim().is_empty() {
        return Ok(None);
    }

    let mut out: BTreeSet<String> = BTreeSet::new();
    for raw_path in input.split(',') {
        let path = raw_path.trim();
        if path.is_empty() {
            return Err(TranslateError::InvalidProjectionExpression {
                reason: "empty path between commas".into(),
            });
        }
        // Disallow nested / indexed paths in v1.
        if path.contains('.') || path.contains('[') {
            return Err(TranslateError::ProjectionNestedPathUnsupported {
                path: path.to_string(),
            });
        }
        let resolved = if let Some(alias) = path.strip_prefix('#') {
            // Name placeholder; look up in EAN. The substitute layer
            // accepts EAN maps keyed either by `#alias` or by `alias`
            // — mirror that here so projection behaves the same way.
            names
                .get(&format!("#{alias}"))
                .or_else(|| names.get(alias))
                .ok_or(TranslateError::UnknownPlaceholder(format!("#{alias}")))?
                .clone()
        } else {
            // Bare name — must not match a DDB reserved word.
            if is_reserved(path) {
                return Err(TranslateError::ReservedWordInProjectionExpression {
                    word: path.to_string(),
                });
            }
            path.to_string()
        };
        out.insert(resolved);
    }
    Ok(Some(out))
}

/// Resolve `Select` + `ProjectionExpression` into the dispatcher's
/// two flags: `count_only` (drop items from response) and
/// `projection` (retain only listed attrs).
///
/// Combination rules (matching DDB):
/// - `Select=COUNT` + `ProjectionExpression` → `ValidationException`.
/// - `Select=SPECIFIC_ATTRIBUTES` requires `ProjectionExpression`.
/// - `Select=ALL_PROJECTED_ATTRIBUTES` is GSI-only — rejected until
///   GSI work lands.
/// - `Select=ALL_ATTRIBUTES` / unset → no special handling.
pub(crate) fn resolve_select_and_projection(
    select: Option<&str>,
    projection_expr: Option<&str>,
    names: &BTreeMap<String, String>,
) -> Result<(bool, Option<BTreeSet<String>>), TranslateError> {
    let count_only = match select {
        None | Some("ALL_ATTRIBUTES") => false,
        Some("COUNT") => true,
        Some("SPECIFIC_ATTRIBUTES") => {
            if projection_expr.is_none() || projection_expr.is_some_and(|s| s.trim().is_empty()) {
                return Err(TranslateError::InvalidSelectMode {
                    reason: "Select=SPECIFIC_ATTRIBUTES requires ProjectionExpression",
                });
            }
            false
        }
        Some("ALL_PROJECTED_ATTRIBUTES") => {
            return Err(TranslateError::InvalidSelectMode {
                reason:
                    "Select=ALL_PROJECTED_ATTRIBUTES requires IndexName (GSI/LSI scan), \
                     which is not yet supported",
            });
        }
        Some(other) => {
            return Err(TranslateError::UnsupportedSelect {
                got: other.to_string(),
            });
        }
    };
    let projection = parse_projection(projection_expr, names)?;
    if count_only && projection.is_some() {
        return Err(TranslateError::InvalidSelectMode {
            reason: "Select=COUNT is not valid with ProjectionExpression",
        });
    }
    Ok((count_only, projection))
}
