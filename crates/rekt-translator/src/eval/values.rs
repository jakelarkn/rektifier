//! Shared helpers for DDB-JSON value introspection.

use serde_json::{Map, Value};

/// Recognize the standard DDB-JSON shape `{"<TAG>": <inner>}` (e.g.
/// `{"N":"3"}`, `{"SS":["a"]}`). Returns `(tag, inner)` when the value
/// is a single-key map; `None` otherwise. The check is intentionally
/// shallow — it doesn't validate that `<TAG>` is a known DDB type or
/// that `<inner>`'s shape matches the tag.
pub(super) fn single_tagged(v: &Value) -> Option<(&str, &Value)> {
    let m: &Map<String, Value> = v.as_object()?;
    if m.len() != 1 {
        return None;
    }
    let (k, v) = m.iter().next()?;
    Some((k.as_str(), v))
}
