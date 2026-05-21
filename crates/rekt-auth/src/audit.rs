//! Audit-log helpers (PLAN-13 A6).
//!
//! [`safe_claims_for_log`] filters an `Identity.claims` map through a
//! per-issuer allowlist. The verifier itself never emits audit lines
//! — the dispatch layer does — but the allowlist logic lives here so
//! the SigV4 / JWT / API-token verifiers can stamp their identity
//! with the safe-to-log subset directly.

use crate::Identity;

/// Built-in always-safe claim names — never PII, never deployment-
/// secret. Applies to every scheme so the dispatcher gets at least
/// these without per-issuer config.
const ALWAYS_SAFE: &[&str] = &["iss", "aud", "sub", "scheme", "token_kind", "aws_region", "aws_service"];

/// Project `identity.claims` through the union of `ALWAYS_SAFE` and
/// the caller-supplied `allowlist`. Anything else is dropped. The
/// returned map is JSON-encodable for a structured audit line.
pub fn safe_claims_for_log(
    identity: &Identity,
    issuer_allowlist: &[String],
) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    for (k, v) in identity.claims.iter() {
        let allowed = ALWAYS_SAFE.iter().any(|s| *s == k)
            || issuer_allowlist.iter().any(|s| s == k);
        if allowed {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthScheme, Identity};

    fn id_with_claims(pairs: &[(&str, &str)]) -> Identity {
        let mut claims = serde_json::Map::new();
        for (k, v) in pairs {
            claims.insert((*k).to_string(), serde_json::Value::String((*v).to_string()));
        }
        Identity {
            scheme: AuthScheme::Jwt,
            principal: "x".into(),
            issuer: None,
            claims,
        }
    }

    #[test]
    fn always_safe_claims_pass_through() {
        let id = id_with_claims(&[("iss", "x"), ("sub", "y"), ("aud", "z")]);
        let out = safe_claims_for_log(&id, &[]);
        assert_eq!(out.len(), 3);
        assert!(out.contains_key("iss"));
        assert!(out.contains_key("sub"));
        assert!(out.contains_key("aud"));
    }

    #[test]
    fn pii_claims_are_dropped() {
        let id = id_with_claims(&[
            ("email", "alice@example.com"),
            ("name", "Alice Example"),
            ("phone_number", "+15555550100"),
            ("sub", "u123"),
        ]);
        let out = safe_claims_for_log(&id, &[]);
        assert!(out.contains_key("sub"));
        assert!(!out.contains_key("email"));
        assert!(!out.contains_key("name"));
        assert!(!out.contains_key("phone_number"));
    }

    #[test]
    fn issuer_allowlist_extends_safe_set() {
        let id = id_with_claims(&[("oid", "abc"), ("tid", "tnt"), ("email", "x")]);
        let allow = vec!["oid".to_string(), "tid".to_string()];
        let out = safe_claims_for_log(&id, &allow);
        assert!(out.contains_key("oid"));
        assert!(out.contains_key("tid"));
        assert!(!out.contains_key("email"), "non-allowlisted PII must be dropped");
    }
}
