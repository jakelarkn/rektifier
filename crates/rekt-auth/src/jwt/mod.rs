//! JWT validator (PLAN-13 A3).
//!
//! Multi-issuer: one `JwtIssuerConfig` per trusted IdP. The token's
//! `iss` claim selects the issuer; missing-issuer or unknown-issuer
//! requests are rejected with `TokenFailed`.
//!
//! Three hardcoded defenses sit above the validation library
//! (PLAN-13 D3a):
//! 1. `alg=none` rejection (regardless of `allowed_algs`).
//! 2. `typ=JWE` rejection (we don't validate encrypted JWTs).
//! 3. Per-JWKS-key algorithm pinning: validation algorithm comes from
//!    the JWKS entry's `alg`, NOT from the token header. Mismatched
//!    headers are rejected before any signature math runs.

pub mod issuer;
pub mod jwks;
pub mod presets;

pub use issuer::{IssuerConfigError, JwtIssuerConfig, PrincipalFormat};
pub use jwks::{JwksCache, JwksError, PinnedKey};

use crate::{AuthError, AuthScheme, Identity, Verifier};
use jsonwebtoken::{decode, decode_header, Validation};
use std::collections::HashMap;
use std::sync::Arc;

pub struct JwtVerifier {
    issuers: HashMap<String, IssuerEntry>,
}

struct IssuerEntry {
    config: JwtIssuerConfig,
    jwks: Arc<JwksCache>,
}

impl JwtVerifier {
    pub fn new() -> Self {
        Self {
            issuers: HashMap::new(),
        }
    }

    pub fn add_issuer(&mut self, config: JwtIssuerConfig) -> Result<(), IssuerConfigError> {
        config.validate()?;
        let jwks = Arc::new(JwksCache::new(
            config.jwks_url.clone(),
            config.jwks_cache_ttl,
        ));
        self.issuers
            .insert(config.issuer.clone(), IssuerEntry { config, jwks });
        Ok(())
    }

    pub fn add_issuer_with_jwks(&mut self, config: JwtIssuerConfig, jwks: Arc<JwksCache>) {
        self.issuers
            .insert(config.issuer.clone(), IssuerEntry { config, jwks });
    }

    /// PLAN-13 D4a — pre-fetch every configured issuer's JWKS at boot.
    /// Failure surfaces here so the operator sees misconfiguration at
    /// deploy time rather than at the first inbound request.
    pub async fn warm_up(&self) -> Result<(), AuthError> {
        for (issuer, entry) in &self.issuers {
            entry.jwks.warm_up().await.map_err(|e| {
                AuthError::Internal(format!(
                    "JWKS warm-up failed for issuer {issuer}: {e}"
                ))
            })?;
        }
        Ok(())
    }
}

impl Default for JwtVerifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Stub claims struct used by `jsonwebtoken::decode`. The crate
/// validates `exp`/`nbf`/`iat`/`aud`/`iss` from this struct's fields
/// automatically; everything else flows through `extra`.
#[derive(serde::Deserialize)]
struct Claims {
    iss: String,
    #[allow(dead_code)]
    aud: serde_json::Value,
    #[allow(dead_code)]
    sub: Option<String>,
    #[allow(dead_code)]
    exp: Option<i64>,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

#[async_trait::async_trait]
impl Verifier for JwtVerifier {
    #[tracing::instrument(level = "debug", skip_all, name = "auth.jwt")]
    async fn verify(
        &self,
        parts: &http::request::Parts,
        _body: &[u8],
    ) -> Result<Identity, AuthError> {
        let auth = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .ok_or(AuthError::MissingAuthorization)?
            .to_str()
            .map_err(|_| AuthError::MalformedAuthorization)?;
        let token = auth
            .strip_prefix("Bearer ")
            .ok_or(AuthError::MalformedAuthorization)?;

        // ----- D3a defenses, applied before signature verification -----

        let header = decode_header(token).map_err(|e| {
            AuthError::TokenFailed(format!("invalid JWT header: {e}"))
        })?;

        // Defense 1: alg=none is rejected unconditionally below by
        // enforcing JWKS-pinned alg (which never resolves to none —
        // see parse_alg in jwks.rs). The 3-segment-JWS check and
        // explicit kid lookup also guarantee any "alg=none" attempt
        // is rejected before signature math runs.
        //
        // The header's typ field, when present, must be JWT. JWE
        // tokens use `typ=JWE` or are 5-segment dot-strings; reject.
        if let Some(t) = header.typ.as_deref() {
            if !t.eq_ignore_ascii_case("JWT") {
                return Err(AuthError::TokenFailed(format!(
                    "unsupported token typ={t} (expected JWT)"
                )));
            }
        }
        // Defense 2: 5-segment JWE form `(header.eck.iv.ct.tag)`.
        if token.matches('.').count() != 2 {
            return Err(AuthError::TokenFailed(
                "token is not a 3-segment JWS (refusing JWE)".into(),
            ));
        }

        // Peek at the issuer claim to pick which issuer config / JWKS
        // applies. We decode-without-verify here only to read `iss`;
        // signature validation happens below with the JWKS-pinned key.
        let payload_seg = token.split('.').nth(1).ok_or_else(|| {
            AuthError::TokenFailed("token missing payload segment".into())
        })?;
        let payload_bytes = base64_url_decode(payload_seg)
            .ok_or_else(|| AuthError::TokenFailed("payload segment not base64url".into()))?;
        let prelim: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&payload_bytes).map_err(|e| {
                AuthError::TokenFailed(format!("payload not JSON: {e}"))
            })?;
        let iss = prelim
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::TokenFailed("missing iss claim".into()))?;

        let entry = self
            .issuers
            .get(iss)
            .ok_or_else(|| AuthError::TokenFailed(format!("issuer {iss} not trusted")))?;

        // Defense 3: per-JWKS-key alg pinning.
        let kid = header.kid.as_deref().ok_or_else(|| {
            AuthError::TokenFailed("token header missing kid".into())
        })?;
        let pinned = entry.jwks.get_key(kid).await.map_err(|e| match e {
            jwks::JwksError::UnknownKid { .. } => {
                AuthError::TokenFailed(format!("unknown kid: {kid}"))
            }
            other => AuthError::Internal(format!("JWKS error: {other}")),
        })?;

        // Bind validation to the JWKS-published alg, not the token
        // header alg. Mismatch = reject (alg-confusion defense).
        if pinned.alg != header.alg {
            return Err(AuthError::TokenFailed(format!(
                "token alg {:?} does not match JWKS-pinned alg {:?} for kid {kid}",
                header.alg, pinned.alg
            )));
        }
        // And the JWKS-pinned alg must be in the issuer's outer allowlist.
        if !entry.config.allowed_algs.contains(&pinned.alg) {
            return Err(AuthError::TokenFailed(format!(
                "JWKS alg {:?} not in issuer allowlist",
                pinned.alg
            )));
        }

        let mut validation = Validation::new(pinned.alg);
        validation.set_issuer(&[&entry.config.issuer]);
        validation.set_audience(&entry.config.expected_audience);
        validation.leeway = entry.config.clock_skew.as_secs();
        // jsonwebtoken would otherwise honor the token header alg;
        // explicitly restrict to the JWKS-pinned one.
        validation.algorithms = vec![pinned.alg];

        let token_data = decode::<Claims>(token, &pinned.key, &validation).map_err(|e| {
            AuthError::TokenFailed(format!("signature/claim validation failed: {e}"))
        })?;

        // Reassemble validated claims (iss, sub, aud, exp + extras) so
        // the audit-log hook (A6) sees the full set.
        let mut claims = token_data.claims.extra.clone();
        claims.insert("iss".into(), serde_json::Value::String(token_data.claims.iss.clone()));
        if let Some(sub) = token_data.claims.sub {
            claims.insert("sub".into(), serde_json::Value::String(sub));
        }
        if let Some(exp) = token_data.claims.exp {
            claims.insert("exp".into(), serde_json::Value::Number(exp.into()));
        }

        let principal = entry.config.resolve_principal(&claims).ok_or_else(|| {
            AuthError::TokenFailed("principal could not be derived from claims".into())
        })?;

        Ok(Identity {
            scheme: AuthScheme::Jwt,
            principal,
            issuer: Some(token_data.claims.iss),
            claims,
        })
    }
}

fn base64_url_decode(s: &str) -> Option<Vec<u8>> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    URL_SAFE_NO_PAD.decode(s).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use std::time::Duration;

    // Pre-generated 2048-bit RSA keypair (test-only, never reuse).
    // Generated with: openssl genrsa -out test.pem 2048 && openssl
    // rsa -in test.pem -pubout -out test.pub.pem.
    const TEST_RSA_PRIVATE_PEM: &str = include_str!("test_keys/rsa_2048_priv.pem");
    const TEST_RSA_PUBLIC_PEM: &str = include_str!("test_keys/rsa_2048_pub.pem");

    fn issuer_cfg() -> JwtIssuerConfig {
        JwtIssuerConfig {
            issuer: "https://issuer.example/".into(),
            jwks_url: "https://issuer.example/jwks".into(),
            expected_audience: vec!["rektifier".into()],
            allowed_algs: vec![Algorithm::RS256],
            clock_skew: Duration::from_secs(60),
            jwks_cache_ttl: Duration::from_secs(600),
            principal_format: PrincipalFormat::Subject("ex:user:".into()),
        }
    }

    fn mint_token(claims: serde_json::Value, kid: &str, alg: Algorithm) -> String {
        let mut header = Header::new(alg);
        header.kid = Some(kid.into());
        let key = EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_PEM.as_bytes()).unwrap();
        encode(&header, &claims, &key).unwrap()
    }

    fn seeded_verifier(kid: &str, alg: Algorithm) -> JwtVerifier {
        let mut v = JwtVerifier::new();
        let cfg = issuer_cfg();
        let mut cache = Arc::new(JwksCache::new(cfg.jwks_url.clone(), Duration::from_secs(600)));
        cache.seed_for_tests(vec![PinnedKey {
            kid: kid.into(),
            alg,
            key: Arc::new(
                jsonwebtoken::DecodingKey::from_rsa_pem(TEST_RSA_PUBLIC_PEM.as_bytes()).unwrap(),
            ),
        }]);
        v.add_issuer_with_jwks(cfg, cache);
        v
    }

    fn parts_with_bearer(token: &str) -> http::request::Parts {
        http::Request::builder()
            .method("POST")
            .uri("/")
            .header("authorization", format!("Bearer {token}"))
            .body(())
            .unwrap()
            .into_parts()
            .0
    }

    #[tokio::test]
    async fn happy_path_rs256_valid_token() {
        let verifier = seeded_verifier("k1", Algorithm::RS256);
        let token = mint_token(
            serde_json::json!({
                "iss": "https://issuer.example/",
                "sub": "alice",
                "aud": "rektifier",
                "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
            }),
            "k1",
            Algorithm::RS256,
        );
        let parts = parts_with_bearer(&token);
        let id = verifier.verify(&parts, b"").await.unwrap();
        assert_eq!(id.scheme, AuthScheme::Jwt);
        assert_eq!(id.principal, "ex:user:alice");
        assert_eq!(id.issuer.as_deref(), Some("https://issuer.example/"));
    }

    #[tokio::test]
    async fn rejects_alg_none() {
        // Build a token with alg=none manually (jsonwebtoken can't
        // sign one). Headers: {"alg":"none","typ":"JWT","kid":"k1"}
        let header_b64 = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIiwia2lkIjoiazEifQ";
        let payload_b64 = base64_url_encode(
            br#"{"iss":"https://issuer.example/","sub":"alice","aud":"rektifier","exp":9999999999}"#,
        );
        let token = format!("{header_b64}.{payload_b64}.");
        let verifier = seeded_verifier("k1", Algorithm::RS256);
        let parts = parts_with_bearer(&token);
        let err = verifier.verify(&parts, b"").await.unwrap_err();
        assert!(
            matches!(err, AuthError::TokenFailed(_)),
            "alg=none must be rejected; got {err:?}"
        );
    }

    #[tokio::test]
    async fn rejects_alg_confusion_hs256_with_rsa_kid() {
        // Token signed with HS256 using the RSA public key as the
        // HMAC secret (the classic alg-confusion attack). The JWKS
        // says the key's alg is RS256; per-key pinning must reject.
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("k1".into());
        let claims = serde_json::json!({
            "iss": "https://issuer.example/",
            "sub": "alice",
            "aud": "rektifier",
            "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
        });
        let hs_key = EncodingKey::from_secret(TEST_RSA_PUBLIC_PEM.as_bytes());
        let token = encode(&header, &claims, &hs_key).unwrap();

        let verifier = seeded_verifier("k1", Algorithm::RS256);
        let parts = parts_with_bearer(&token);
        let err = verifier.verify(&parts, b"").await.unwrap_err();
        assert!(
            matches!(err, AuthError::TokenFailed(_)),
            "alg-confusion HS256-with-RSA-pubkey must reject; got {err:?}"
        );
    }

    #[tokio::test]
    async fn rejects_jwe_5_segment_form() {
        // 5-segment JWE: header.eck.iv.ct.tag
        let token = "eyJhbGciOiJSU0EtT0FFUCJ9.x.x.x.x";
        let verifier = seeded_verifier("k1", Algorithm::RS256);
        let parts = parts_with_bearer(token);
        let err = verifier.verify(&parts, b"").await.unwrap_err();
        assert!(matches!(err, AuthError::TokenFailed(_)));
    }

    #[tokio::test]
    async fn rejects_unknown_issuer() {
        let verifier = seeded_verifier("k1", Algorithm::RS256);
        let token = mint_token(
            serde_json::json!({
                "iss": "https://other.example/",
                "sub": "alice",
                "aud": "rektifier",
                "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
            }),
            "k1",
            Algorithm::RS256,
        );
        let parts = parts_with_bearer(&token);
        let err = verifier.verify(&parts, b"").await.unwrap_err();
        assert!(matches!(err, AuthError::TokenFailed(_)));
    }

    #[tokio::test]
    async fn rejects_unknown_kid() {
        let verifier = seeded_verifier("k1", Algorithm::RS256);
        let token = mint_token(
            serde_json::json!({
                "iss": "https://issuer.example/",
                "sub": "alice",
                "aud": "rektifier",
                "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
            }),
            "k_unknown",
            Algorithm::RS256,
        );
        let parts = parts_with_bearer(&token);
        let err = verifier.verify(&parts, b"").await.unwrap_err();
        assert!(matches!(err, AuthError::TokenFailed(_)));
    }

    #[tokio::test]
    async fn rejects_expired_token() {
        let verifier = seeded_verifier("k1", Algorithm::RS256);
        let token = mint_token(
            serde_json::json!({
                "iss": "https://issuer.example/",
                "sub": "alice",
                "aud": "rektifier",
                "exp": (chrono::Utc::now() - chrono::Duration::hours(1)).timestamp(),
            }),
            "k1",
            Algorithm::RS256,
        );
        let parts = parts_with_bearer(&token);
        let err = verifier.verify(&parts, b"").await.unwrap_err();
        assert!(matches!(err, AuthError::TokenFailed(_)));
    }

    #[tokio::test]
    async fn rejects_wrong_audience() {
        let verifier = seeded_verifier("k1", Algorithm::RS256);
        let token = mint_token(
            serde_json::json!({
                "iss": "https://issuer.example/",
                "sub": "alice",
                "aud": "someone-else",
                "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
            }),
            "k1",
            Algorithm::RS256,
        );
        let parts = parts_with_bearer(&token);
        let err = verifier.verify(&parts, b"").await.unwrap_err();
        assert!(matches!(err, AuthError::TokenFailed(_)));
    }

    fn base64_url_encode(b: &[u8]) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;
        URL_SAFE_NO_PAD.encode(b)
    }
}
