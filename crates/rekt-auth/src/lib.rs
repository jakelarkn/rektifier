//! Authentication for rektifier.
//!
//! See `docs/plan/PLAN-13-extensible-auth.md` for the full design.
//!
//! Three primitives — SigV4, JWT-with-JWKS, opaque API token — plus a
//! [`PermissiveVerifier`] for dev/test. The [`AuthChain`] composes them
//! and routes each request to the right primitive by `Authorization`
//! header prefix (D1).
//!
//! For phase A1 the only registered verifier is [`PermissiveVerifier`];
//! later phases add strict SigV4 (A2), the JWT engine (A3), and opaque
//! API tokens (A5).

use std::sync::OnceLock;

pub mod api_token;
pub mod audit;
pub mod crypto;
pub mod jwt;
pub mod master_key;
pub mod permissive;
pub mod sigv4;

pub use audit::safe_claims_for_log;

pub use api_token::{
    ensure_api_tokens_table, insert_token_hash, mint as mint_api_token, ApiTokenVerifier,
    TokenCache, TokenKind,
};
pub use jwt::{JwksCache, JwtIssuerConfig, JwtVerifier, PrincipalFormat};
pub use master_key::{MasterKeyError, MasterKeySource};
pub use permissive::PermissiveVerifier;
pub use sigv4::{
    ensure_credentials_table, fetch_credential_row, insert_credential_row, CredentialRow,
    SecretCache, Sigv4Verifier,
};

/// Successful verification produces an [`Identity`]. The dispatcher
/// records `scheme` + `principal` on the request span and (in A6) hands
/// it to the audit-log emitter.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Which primitive validated this request. Used for tracing,
    /// audit logging, and (future) authz routing.
    pub scheme: AuthScheme,

    /// A namespaced string the operator can map to an authz policy.
    /// Format: `<scheme>:<subtype>:<id>`. Examples:
    /// - `aws:akid:AKIAEXAMPLE`            (SigV4)
    /// - `gcp:sa:bench@p.iam.gserviceaccount.com` (GCP service account JWT)
    /// - `azure:sp:11111111-2222-…`        (Azure service principal)
    /// - `neon:key:rekt_pat_…`             (opaque API token)
    /// - `anonymous`                        (PermissiveVerifier)
    pub principal: String,

    /// Issuer string for JWT-shaped identities; `None` for SigV4 /
    /// opaque / permissive.
    pub issuer: Option<String>,

    /// All validated claims, for audit logging. **Never serialise this
    /// map wholesale to logs** — it contains PII (`email`, `name`,
    /// `phone_number`, custom claims). The audit emitter (A6) applies
    /// a per-issuer allowlist; everything else is dropped before the
    /// log line is constructed.
    pub claims: serde_json::Map<String, serde_json::Value>,
}

impl Identity {
    pub fn anonymous() -> Self {
        Self {
            scheme: AuthScheme::Permissive,
            principal: "anonymous".into(),
            issuer: None,
            claims: serde_json::Map::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthScheme {
    Sigv4,
    Jwt,
    ApiToken,
    Permissive,
}

impl AuthScheme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sigv4 => "sigv4",
            Self::Jwt => "jwt",
            Self::ApiToken => "api_token",
            Self::Permissive => "permissive",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing Authorization header (anonymous access disabled)")]
    MissingAuthorization,

    #[error("malformed Authorization header")]
    MalformedAuthorization,

    #[error("auth scheme not configured: {0}")]
    UnsupportedScheme(&'static str),

    #[error("signature verification failed: {0}")]
    SignatureFailed(String),

    #[error("token verification failed: {0}")]
    TokenFailed(String),

    #[error("identity not found")]
    IdentityNotFound,

    #[error("identity disabled")]
    IdentityDisabled,

    #[error("internal auth error: {0}")]
    Internal(String),
}

#[async_trait::async_trait]
pub trait Verifier: Send + Sync + 'static {
    async fn verify(
        &self,
        parts: &http::request::Parts,
        body: &[u8],
    ) -> Result<Identity, AuthError>;
}

/// What auth primitive an inbound `Authorization` header value selects.
/// Cheap, opaque to the verifier — the chain inspects the header once
/// and routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderRoute {
    Sigv4,
    JwtBearer,
    OpaqueBearer,
    Missing,
    Unknown,
}

fn classify_header(parts: &http::request::Parts) -> HeaderRoute {
    let Some(val) = parts.headers.get(http::header::AUTHORIZATION) else {
        return HeaderRoute::Missing;
    };
    let Ok(s) = val.to_str() else {
        return HeaderRoute::Unknown;
    };
    if s.starts_with("AWS4-HMAC-SHA256 ") {
        return HeaderRoute::Sigv4;
    }
    if let Some(rest) = s.strip_prefix("Bearer ") {
        return if looks_like_jwt(rest) {
            HeaderRoute::JwtBearer
        } else {
            HeaderRoute::OpaqueBearer
        };
    }
    HeaderRoute::Unknown
}

/// Cheap JWT vs opaque-token detector. JWTs have the shape
/// `header.payload.signature`, so we just check for at least two `.`
/// separators. The JWT validator does the real parsing later.
fn looks_like_jwt(s: &str) -> bool {
    let mut dots = 0;
    for c in s.chars() {
        if c == '.' {
            dots += 1;
            if dots >= 2 {
                return true;
            }
        }
    }
    false
}

/// Composable verifier chain. Holds zero or one registered verifier per
/// primitive; on each request it inspects the Authorization header
/// prefix and routes to the matching primitive (D1). Verifiers added
/// in later phases plug into the matching slot.
pub struct AuthChain {
    pub sigv4: Option<Box<dyn Verifier>>,
    pub jwt: Option<Box<dyn Verifier>>,
    pub api_token: Option<Box<dyn Verifier>>,
    pub permissive: Option<Box<dyn Verifier>>,
}

impl AuthChain {
    pub fn empty() -> Self {
        Self {
            sigv4: None,
            jwt: None,
            api_token: None,
            permissive: None,
        }
    }

    /// Dev-only convenience: a chain with only the PermissiveVerifier
    /// registered. Matches the pre-PLAN-13 behavior (every request
    /// accepted, no signatures validated, loud warning on first hit).
    pub fn permissive_only() -> Self {
        Self {
            sigv4: None,
            jwt: None,
            api_token: None,
            permissive: Some(Box::new(PermissiveVerifier)),
        }
    }

    pub fn with_sigv4(mut self, v: Box<dyn Verifier>) -> Self {
        self.sigv4 = Some(v);
        self
    }

    pub fn with_jwt(mut self, v: Box<dyn Verifier>) -> Self {
        self.jwt = Some(v);
        self
    }

    pub fn with_api_token(mut self, v: Box<dyn Verifier>) -> Self {
        self.api_token = Some(v);
        self
    }

    pub fn with_permissive(mut self, v: Box<dyn Verifier>) -> Self {
        self.permissive = Some(v);
        self
    }
}

#[async_trait::async_trait]
impl Verifier for AuthChain {
    #[tracing::instrument(level = "debug", skip_all, name = "auth.chain")]
    async fn verify(
        &self,
        parts: &http::request::Parts,
        body: &[u8],
    ) -> Result<Identity, AuthError> {
        let route = classify_header(parts);
        match route {
            HeaderRoute::Sigv4 => match &self.sigv4 {
                Some(v) => v.verify(parts, body).await,
                None => {
                    // Operator pointed an AWS SDK at a server with strict
                    // SigV4 disabled. If permissive is on, fall through;
                    // otherwise reject.
                    if let Some(p) = &self.permissive {
                        p.verify(parts, body).await
                    } else {
                        Err(AuthError::UnsupportedScheme("sigv4"))
                    }
                }
            },
            HeaderRoute::JwtBearer => match &self.jwt {
                Some(v) => v.verify(parts, body).await,
                None => Err(AuthError::UnsupportedScheme("jwt")),
            },
            HeaderRoute::OpaqueBearer => match &self.api_token {
                Some(v) => v.verify(parts, body).await,
                None => Err(AuthError::UnsupportedScheme("api_token")),
            },
            HeaderRoute::Missing => match &self.permissive {
                Some(v) => v.verify(parts, body).await,
                None => Err(AuthError::MissingAuthorization),
            },
            HeaderRoute::Unknown => Err(AuthError::MalformedAuthorization),
        }
    }
}

pub(crate) fn warn_permissive_once() {
    static WARNED: OnceLock<()> = OnceLock::new();
    WARNED.get_or_init(|| {
        tracing::warn!(
            "PermissiveVerifier active: SigV4 signatures are NOT being validated. \
             Do not use in production."
        );
    });
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
    fn jwt_detector_accepts_three_dot_form() {
        assert!(looks_like_jwt("eyJ.eyJ.sig"));
        assert!(looks_like_jwt("a.b.c.d"));
    }

    #[test]
    fn jwt_detector_rejects_one_dot_or_none() {
        assert!(!looks_like_jwt("abc"));
        assert!(!looks_like_jwt("opaque-token"));
        assert!(!looks_like_jwt("foo.bar"));
    }

    #[test]
    fn classify_routes_sigv4_header_to_sigv4() {
        let p = parts_with(&[("authorization", SIGNED_AUTH)]);
        assert_eq!(classify_header(&p), HeaderRoute::Sigv4);
    }

    #[test]
    fn classify_routes_bearer_jwt_to_jwt() {
        let p = parts_with(&[("authorization", "Bearer eyJhbGciOi.payload.sig")]);
        assert_eq!(classify_header(&p), HeaderRoute::JwtBearer);
    }

    #[test]
    fn classify_routes_bearer_opaque_to_api_token() {
        let p = parts_with(&[("authorization", "Bearer rekt_pat_opaque-token")]);
        assert_eq!(classify_header(&p), HeaderRoute::OpaqueBearer);
    }

    #[test]
    fn classify_missing_header() {
        let p = parts_with(&[]);
        assert_eq!(classify_header(&p), HeaderRoute::Missing);
    }

    #[test]
    fn classify_unknown_scheme() {
        let p = parts_with(&[("authorization", "Basic dXNlcjpwYXNz")]);
        assert_eq!(classify_header(&p), HeaderRoute::Unknown);
    }

    #[tokio::test]
    async fn permissive_only_chain_accepts_missing_header() {
        let chain = AuthChain::permissive_only();
        let p = parts_with(&[]);
        let id = chain.verify(&p, b"{}").await.unwrap();
        assert_eq!(id.scheme, AuthScheme::Permissive);
        assert_eq!(id.principal, "anonymous");
    }

    #[tokio::test]
    async fn permissive_only_chain_stashes_akid_in_claims_for_sigv4_header() {
        let chain = AuthChain::permissive_only();
        let p = parts_with(&[("authorization", SIGNED_AUTH)]);
        let id = chain.verify(&p, b"{}").await.unwrap();
        assert_eq!(id.scheme, AuthScheme::Permissive);
        assert_eq!(
            id.claims.get("aws_akid").and_then(|v| v.as_str()),
            Some("AKIAEXAMPLE")
        );
    }

    #[tokio::test]
    async fn empty_chain_rejects_missing_header() {
        let chain = AuthChain::empty();
        let p = parts_with(&[]);
        let err = chain.verify(&p, b"{}").await.unwrap_err();
        assert!(matches!(err, AuthError::MissingAuthorization));
    }

    #[tokio::test]
    async fn empty_chain_rejects_sigv4_when_unconfigured() {
        let chain = AuthChain::empty();
        let p = parts_with(&[("authorization", SIGNED_AUTH)]);
        let err = chain.verify(&p, b"{}").await.unwrap_err();
        assert!(matches!(err, AuthError::UnsupportedScheme("sigv4")));
    }

    #[tokio::test]
    async fn empty_chain_rejects_bearer_jwt_when_unconfigured() {
        let chain = AuthChain::empty();
        let p = parts_with(&[("authorization", "Bearer eyJ.payload.sig")]);
        let err = chain.verify(&p, b"{}").await.unwrap_err();
        assert!(matches!(err, AuthError::UnsupportedScheme("jwt")));
    }

    #[tokio::test]
    async fn empty_chain_rejects_bearer_opaque_when_unconfigured() {
        let chain = AuthChain::empty();
        let p = parts_with(&[("authorization", "Bearer rekt_pat_x")]);
        let err = chain.verify(&p, b"{}").await.unwrap_err();
        assert!(matches!(err, AuthError::UnsupportedScheme("api_token")));
    }

    #[tokio::test]
    async fn empty_chain_rejects_unknown_scheme() {
        let chain = AuthChain::empty();
        let p = parts_with(&[("authorization", "Basic dXNlcjpwYXNz")]);
        let err = chain.verify(&p, b"{}").await.unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuthorization));
    }
}
