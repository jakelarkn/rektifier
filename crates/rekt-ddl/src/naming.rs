//! Deterministic mapping from a DDB table name to a PG identifier.
//!
//! PLAN-10 KD11. Operators don't supply PG names — rektifier derives
//! them. The mapping is:
//!
//! 1. lowercase
//! 2. non-alphanumeric → `_`
//! 3. strip leading `_`
//! 4. prefix `rekt_t_`
//! 5. if length > 63 bytes (PG identifier ceiling), truncate the
//!    sanitized body to 47 chars and append `_<8 hex chars of sha1(ddb_name)>`
//!
//! Collisions in the derived PG name are rejected at CreateTable
//! validation time (caller's responsibility — this module doesn't
//! reach PG).

use sha1::{Digest, Sha1};

/// PG identifier ceiling (NAMEDATALEN - 1).
const PG_IDENT_MAX: usize = 63;
const PREFIX: &str = "rekt_t_";

/// PLAN-9 D8. GSI index identifier. Mirrors `derive_lsi_index_name`
/// but uses the `_gsi_` infix so operators introspecting `pg_indexes`
/// can tell GSI- and LSI-backed indexes apart.
pub fn derive_gsi_index_name(pg_table: &str, gsi_name: &str) -> String {
    derive_index_name(pg_table, gsi_name, "_gsi_")
}

/// PLAN-11 D11. Derive a PG index identifier for an LSI on `pg_table`
/// named `lsi_name`. Pattern: `<pg_table>_lsi_<sanitized_lsi>_idx`.
/// Long composites are sha1-suffixed to fit inside the 63-byte PG
/// identifier ceiling.
pub fn derive_lsi_index_name(pg_table: &str, lsi_name: &str) -> String {
    derive_index_name(pg_table, lsi_name, "_lsi_")
}

/// Shared implementation for `derive_lsi_index_name` and
/// `derive_gsi_index_name`. `infix` is `_lsi_` or `_gsi_`.
fn derive_index_name(pg_table: &str, index_name: &str, infix: &str) -> String {
    let sanitized: String = index_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    let sanitized = sanitized.trim_start_matches('_');
    let candidate = format!("{pg_table}{infix}{sanitized}_idx");
    if candidate.len() <= PG_IDENT_MAX {
        return candidate;
    }
    let mut hasher = Sha1::new();
    hasher.update(pg_table.as_bytes());
    hasher.update(b"::");
    hasher.update(infix.as_bytes());
    hasher.update(index_name.as_bytes());
    let digest = hasher.finalize();
    let hex8: String = digest
        .iter()
        .take(4)
        .map(|b| format!("{b:02x}"))
        .collect();
    let fixed_len = pg_table.len() + infix.len() + "_".len() + hex8.len() + "_idx".len();
    let body_budget = PG_IDENT_MAX.saturating_sub(fixed_len);
    let body: String = sanitized.chars().take(body_budget).collect();
    format!("{pg_table}{infix}{body}_{hex8}_idx")
}

pub fn sanitize_pg_table_name(ddb_name: &str) -> String {
    let lowered: String = ddb_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    let stripped = lowered.trim_start_matches('_');
    let candidate = format!("{PREFIX}{stripped}");
    if candidate.len() <= PG_IDENT_MAX {
        return candidate;
    }
    // Truncate body to fit 8-char hash suffix + underscore + PREFIX
    // within PG_IDENT_MAX.
    //   PREFIX.len()           = 7
    //   8-char hash + '_'      = 9
    //   body budget            = 63 - 7 - 9 = 47
    let body_budget = PG_IDENT_MAX - PREFIX.len() - 9;
    let body: String = stripped.chars().take(body_budget).collect();
    let mut hasher = Sha1::new();
    hasher.update(ddb_name.as_bytes());
    let digest = hasher.finalize();
    let hex8: String = digest
        .iter()
        .take(4)
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{PREFIX}{body}_{hex8}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_lowercase_name_unchanged_modulo_prefix() {
        assert_eq!(sanitize_pg_table_name("users"), "rekt_t_users");
    }

    #[test]
    fn uppercase_is_lowered() {
        assert_eq!(sanitize_pg_table_name("Users"), "rekt_t_users");
    }

    /// DDB allows `.`, `-`, `_`; PG identifiers don't allow `.` or `-`.
    /// Sanitization collapses each disallowed char to `_`.
    #[test]
    fn dots_and_dashes_become_underscores() {
        assert_eq!(
            sanitize_pg_table_name("MyApp.Orders-Production"),
            "rekt_t_myapp_orders_production"
        );
    }

    /// Leading underscores after sanitization are stripped to avoid
    /// `rekt_t__foo` shapes (cosmetic).
    #[test]
    fn leading_garbage_is_stripped() {
        assert_eq!(sanitize_pg_table_name(".weird"), "rekt_t_weird");
        assert_eq!(sanitize_pg_table_name("__under"), "rekt_t_under");
    }

    /// Long names get truncated + sha1-suffixed. The result stays
    /// within the 63-byte PG identifier ceiling.
    #[test]
    fn long_names_hash_suffix_under_63_bytes() {
        let long = "aVeryLongTableNameThatExceedsThePostgresIdentifierLimitOf63Bytes";
        let pg = sanitize_pg_table_name(long);
        assert!(pg.len() <= 63, "got {pg} ({} bytes)", pg.len());
        assert!(pg.starts_with("rekt_t_"));
        // The suffix is deterministic for the same input.
        assert_eq!(pg, sanitize_pg_table_name(long));
    }

    /// Different inputs produce different hash suffixes — collision
    /// likelihood within a deployment is sha1's full 32-bit prefix space
    /// over a few thousand tables, which is fine for v1.
    #[test]
    fn long_names_with_same_prefix_get_different_suffixes() {
        let a = "aVeryLongTableNameThatExceedsTheLimitOfPostgresVersionOne";
        let b = "aVeryLongTableNameThatExceedsTheLimitOfPostgresVersionTwo";
        let pa = sanitize_pg_table_name(a);
        let pb = sanitize_pg_table_name(b);
        assert_ne!(pa, pb);
    }

    /// Digits are preserved (PG-safe alphanumeric).
    #[test]
    fn digits_preserved() {
        assert_eq!(sanitize_pg_table_name("orders2024"), "rekt_t_orders2024");
    }

    // ===== derive_lsi_index_name ============================================

    #[test]
    fn lsi_index_name_basic() {
        assert_eq!(
            derive_lsi_index_name("rekt_t_events", "by_status"),
            "rekt_t_events_lsi_by_status_idx"
        );
    }

    /// LSI names with dots/dashes from the DDB grammar collapse to
    /// underscores in the PG identifier.
    #[test]
    fn lsi_index_name_sanitizes_disallowed_chars() {
        assert_eq!(
            derive_lsi_index_name("rekt_t_events", "by-status.v2"),
            "rekt_t_events_lsi_by_status_v2_idx"
        );
    }

    /// Long composite (long pg_table + long lsi_name) collapses through
    /// the sha1-suffix path and stays under the 63-byte ceiling.
    #[test]
    fn lsi_index_name_long_collision_safe_under_63_bytes() {
        let long_table = "rekt_t_aVeryLongTableNameThatFillsTheBudget";
        let lsi = "by_some_really_long_index_attribute_name";
        let idx = derive_lsi_index_name(long_table, lsi);
        assert!(idx.len() <= 63, "got {idx} ({} bytes)", idx.len());
        assert!(idx.starts_with(long_table));
        assert!(idx.ends_with("_idx"));
        // Deterministic for the same input.
        assert_eq!(idx, derive_lsi_index_name(long_table, lsi));
    }

    /// Different LSI names on the same long table get distinct suffixes.
    #[test]
    fn lsi_index_name_distinct_on_collision_in_truncated_form() {
        let long_table = "rekt_t_aVeryLongTableNameThatFillsTheBudget";
        let a = "by_some_really_long_index_attribute_name_one";
        let b = "by_some_really_long_index_attribute_name_two";
        assert_ne!(
            derive_lsi_index_name(long_table, a),
            derive_lsi_index_name(long_table, b)
        );
    }
}
