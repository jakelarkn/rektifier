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
}
