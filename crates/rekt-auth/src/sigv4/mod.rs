//! SigV4 strict verification (PLAN-13 D5 / A2).
//!
//! The verifier reconstructs the canonical SigV4 string-to-sign from
//! the inbound request, derives the signing key from the operator-
//! stored secret (decrypted via the AES-GCM master key, cached in
//! memory), recomputes the signature, and compares it constant-time
//! with the signature in the Authorization header.
//!
//! What is *not* in scope here: server-side IAM lookups, STS
//! federation (rejected — see PLAN-13 "Rejected alternatives"), or
//! key rotation tooling. Operators rotate by inserting a new row
//! and deleting the old; the cache TTL bounds the propagation
//! window.

pub mod cache;
pub mod canonical;
pub mod store;
pub mod verifier;

pub use cache::{CachedSecret, SecretCache};
pub use store::{
    ensure_credentials_table, fetch_credential_row, insert_credential_row, CredentialRow,
};
pub use verifier::Sigv4Verifier;

/// Parse the AKID out of an AWS4-HMAC-SHA256 `Authorization` header.
/// Returns `None` if the header isn't a SigV4 header or the `Credential`
/// component is missing/malformed.
pub fn parse_access_key_id(auth: &str) -> Option<String> {
    let rest = auth.strip_prefix("AWS4-HMAC-SHA256 ")?;
    let credential = rest
        .split(',')
        .find_map(|part| part.trim().strip_prefix("Credential="))?;
    let akid = credential.split('/').next()?;
    if akid.is_empty() {
        None
    } else {
        Some(akid.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn akid_parser_handles_minimal_header() {
        let auth = "AWS4-HMAC-SHA256 Credential=local/20260506/us-east-1/dynamodb/aws4_request";
        assert_eq!(parse_access_key_id(auth).as_deref(), Some("local"));
    }

    #[test]
    fn akid_parser_returns_none_for_non_sigv4() {
        assert_eq!(parse_access_key_id("Bearer foo"), None);
    }

    #[test]
    fn akid_parser_returns_none_for_missing_credential() {
        assert_eq!(parse_access_key_id("AWS4-HMAC-SHA256 SignedHeaders=host"), None);
    }
}
