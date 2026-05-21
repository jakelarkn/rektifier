//! SigV4 helpers shared by PermissiveVerifier (A1) and the strict
//! `Sigv4Verifier` (A2). The strict verifier itself lands in A2.

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
