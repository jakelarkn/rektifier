//! AWS SigV4 verification for rektifier.
//!
//! For the MVP this crate exposes a [`Verifier`] trait and a
//! [`PermissiveVerifier`] that does not actually validate signatures — it just
//! best-effort-extracts the access key id and logs a loud warning.
//!
//! Strict verification (using the `aws-sigv4` crate) will land as a separate
//! `StrictVerifier` impl in a follow-up. The trait signature is what the server
//! commits to today; everything else can grow under it.

use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub struct VerifiedRequest {
    /// AKID extracted from the `Authorization` header, if present and parseable.
    /// `None` does not imply the request was unauthenticated under a strict
    /// verifier — only that we couldn't parse a `Credential=...` field.
    pub access_key_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SigV4Error {
    #[error("signature verification failed: {0}")]
    Failed(String),
}

pub trait Verifier: Send + Sync + 'static {
    fn verify(
        &self,
        parts: &http::request::Parts,
        body: &[u8],
    ) -> Result<VerifiedRequest, SigV4Error>;
}

/// Permissive verifier — accepts every request and logs a warning the first
/// time it's invoked. For dev / MVP use only. **Never enable in production.**
pub struct PermissiveVerifier;

impl Verifier for PermissiveVerifier {
    #[tracing::instrument(level = "debug", skip_all, name = "verifier.permissive")]
    fn verify(
        &self,
        parts: &http::request::Parts,
        _body: &[u8],
    ) -> Result<VerifiedRequest, SigV4Error> {
        warn_once();
        let access_key_id = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(parse_access_key_id);
        Ok(VerifiedRequest { access_key_id })
    }
}

fn warn_once() {
    static WARNED: OnceLock<()> = OnceLock::new();
    WARNED.get_or_init(|| {
        tracing::warn!(
            "PermissiveVerifier active: SigV4 signatures are NOT being validated. \
             Do not use in production."
        );
    });
}

/// Parse the AKID out of an AWS4-HMAC-SHA256 `Authorization` header.
/// Returns `None` if the header isn't a SigV4 header or the `Credential`
/// component is missing/malformed.
fn parse_access_key_id(auth: &str) -> Option<String> {
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

    fn parts_with(headers: &[(&str, &str)]) -> http::request::Parts {
        let mut builder = http::Request::builder().method("POST").uri("/");
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        builder.body(()).unwrap().into_parts().0
    }

    const SIGNED_AUTH: &str = "AWS4-HMAC-SHA256 \
        Credential=AKIAEXAMPLE/20260506/us-east-1/dynamodb/aws4_request, \
        SignedHeaders=content-type;host;x-amz-date;x-amz-target, \
        Signature=deadbeef";

    #[test]
    fn permissive_accepts_signed_request_and_extracts_akid() {
        let parts = parts_with(&[
            ("authorization", SIGNED_AUTH),
            ("x-amz-target", "DynamoDB_20120810.GetItem"),
            ("x-amz-date", "20260506T123456Z"),
            ("content-type", "application/x-amz-json-1.0"),
        ]);
        let r = PermissiveVerifier.verify(&parts, b"{}").unwrap();
        assert_eq!(r.access_key_id.as_deref(), Some("AKIAEXAMPLE"));
    }

    #[test]
    fn permissive_accepts_unsigned_request() {
        let parts = parts_with(&[]);
        let r = PermissiveVerifier.verify(&parts, b"{}").unwrap();
        assert!(r.access_key_id.is_none());
    }

    #[test]
    fn permissive_handles_garbage_auth_header() {
        let parts = parts_with(&[("authorization", "Bearer something-non-sigv4")]);
        let r = PermissiveVerifier.verify(&parts, b"{}").unwrap();
        assert!(r.access_key_id.is_none());
    }

    #[test]
    fn akid_parser_handles_minimal_header() {
        // No SignedHeaders, no Signature — still a valid Credential field.
        let auth = "AWS4-HMAC-SHA256 Credential=local/20260506/us-east-1/dynamodb/aws4_request";
        assert_eq!(parse_access_key_id(auth).as_deref(), Some("local"));
    }
}
