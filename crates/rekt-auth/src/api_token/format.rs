//! Token-format scheme (PLAN-13 D6b).
//!
//! Tokens are operator-readable + secret-scanner-friendly:
//!
//! ```text
//! rekt_pat_<28 chars of random base62>     (Personal Access Token)
//! rekt_svc_<28 chars of random base62>     (Service / M2M token)
//! rekt_test_<28 chars of random base62>    (Test/dev token — never
//!                                            accepted in prod builds)
//! ```
//!
//! 28 chars of base62 = ~166 bits of entropy, well above the
//! 128-bit floor where hash-only storage is safe. The typed prefix
//! is what GitHub-style secret-scanning partners register on, and
//! also lets us reject obviously-malformed bearer tokens before
//! any HMAC or DB lookup.

use rand::RngCore;

/// Token kind. The wire-level prefix is `rekt_<kind>_`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// Personal access token: human-bound, long-lived (operator
    /// rotates manually).
    Pat,
    /// Service / M2M token: bound to a workload identity.
    Svc,
    /// Test/dev token. Never accepted in production builds —
    /// `ApiTokenVerifier::verify` reads a compile-time flag.
    Test,
}

impl TokenKind {
    pub fn prefix(&self) -> &'static str {
        match self {
            Self::Pat => "rekt_pat_",
            Self::Svc => "rekt_svc_",
            Self::Test => "rekt_test_",
        }
    }

    /// Parse the prefix off a raw bearer token. Returns `None` for
    /// tokens that don't start with any known prefix (cheap rejection
    /// before HMAC / DB lookup).
    pub fn detect(raw: &str) -> Option<(TokenKind, &str)> {
        for kind in [Self::Pat, Self::Svc, Self::Test] {
            if let Some(rest) = raw.strip_prefix(kind.prefix()) {
                return Some((kind, rest));
            }
        }
        None
    }
}

/// Length of the random body (after the prefix).
const BODY_LEN: usize = 28;

/// Mint a fresh token. Returns the wire-format string; the caller
/// hashes it via [`crate::api_token::cache::hmac_pepper`] before
/// storing in PG.
pub fn mint(kind: TokenKind) -> String {
    let body = random_base62(BODY_LEN);
    format!("{}{}", kind.prefix(), body)
}

fn random_base62(n: usize) -> String {
    const ALPHABET: &[u8] =
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut out = String::with_capacity(n);
    let mut rng = rand::thread_rng();
    let mut buf = vec![0u8; n];
    rng.fill_bytes(&mut buf);
    for b in buf {
        out.push(ALPHABET[(b as usize) % ALPHABET.len()] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_pat() {
        let (kind, body) = TokenKind::detect("rekt_pat_abc123").unwrap();
        assert_eq!(kind, TokenKind::Pat);
        assert_eq!(body, "abc123");
    }

    #[test]
    fn detect_svc() {
        let (kind, _) = TokenKind::detect("rekt_svc_xyz").unwrap();
        assert_eq!(kind, TokenKind::Svc);
    }

    #[test]
    fn detect_test() {
        let (kind, _) = TokenKind::detect("rekt_test_abc").unwrap();
        assert_eq!(kind, TokenKind::Test);
    }

    #[test]
    fn detect_rejects_unprefixed() {
        assert!(TokenKind::detect("randomstring").is_none());
        assert!(TokenKind::detect("rektnotaprefix").is_none());
        assert!(TokenKind::detect("").is_none());
    }

    #[test]
    fn mint_returns_prefixed_token() {
        let t = mint(TokenKind::Pat);
        assert!(t.starts_with("rekt_pat_"));
        assert_eq!(t.len(), "rekt_pat_".len() + 28);
    }

    #[test]
    fn mint_produces_distinct_tokens() {
        let a = mint(TokenKind::Pat);
        let b = mint(TokenKind::Pat);
        assert_ne!(a, b);
    }
}
