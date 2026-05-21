//! Strict SigV4 verifier wired to the credential cache + PG store.

use crate::crypto;
use crate::sigv4::{
    cache::{CachedSecret, SecretCache},
    canonical::{canonical_request, compute_signature, hex_sha256, signing_key, string_to_sign},
    store::{fetch_credential_row, StoreError},
};
use crate::{AuthError, AuthScheme, Identity, Verifier};
use chrono::{NaiveDateTime, Utc};
use deadpool_postgres::Pool;
use std::sync::Arc;
use std::time::Instant;
use subtle::ConstantTimeEq;

/// Default SigV4 clock-skew tolerance. AWS docs cite 5–15 min; 15 is the
/// established ceiling and matches `aws-sigv4`'s default.
const DEFAULT_CLOCK_SKEW_SECS: i64 = 15 * 60;

/// HKDF purpose label so SigV4's AES-GCM data-encryption-key is
/// distinct from the A5 API-token HMAC pepper even when both derive
/// from the same operator-supplied master key.
pub const SIGV4_AES_PURPOSE: &[u8] = b"rektifier-sigv4-aes-v1";

pub struct Sigv4Verifier {
    pool: Pool,
    /// AES-GCM key derived once at boot via HKDF from the operator's
    /// master key. Held in process memory for the lifetime of the
    /// verifier; never serialised, never logged.
    aes_key: [u8; 32],
    cache: Arc<SecretCache>,
    clock_skew_secs: i64,
}

impl Sigv4Verifier {
    pub fn new(pool: Pool, master_key: &[u8; 32], cache: Arc<SecretCache>) -> Self {
        let aes_key = crypto::derive_subkey(master_key, SIGV4_AES_PURPOSE);
        Self {
            pool,
            aes_key,
            cache,
            clock_skew_secs: DEFAULT_CLOCK_SKEW_SECS,
        }
    }

    pub fn with_clock_skew_secs(mut self, secs: i64) -> Self {
        self.clock_skew_secs = secs;
        self
    }

    async fn fetch_secret(&self, akid: &str) -> Result<CachedSecret, AuthError> {
        if let Some(cached) = self.cache.get(akid) {
            return match cached {
                Ok(s) => Ok(s),
                Err(()) => Err(AuthError::IdentityNotFound),
            };
        }
        match fetch_credential_row(&self.pool, &self.aes_key, akid).await {
            Ok(Some(row)) if row.enabled => {
                let cached = CachedSecret {
                    secret: Arc::new(row.secret_access_key),
                    principal: row.principal,
                    fetched_at: Instant::now(),
                };
                self.cache.insert_positive(akid.to_string(), cached.clone());
                Ok(cached)
            }
            Ok(Some(_disabled)) => {
                self.cache.insert_negative(akid.to_string());
                Err(AuthError::IdentityDisabled)
            }
            Ok(None) => {
                self.cache.insert_negative(akid.to_string());
                Err(AuthError::IdentityNotFound)
            }
            Err(StoreError::Crypto(_)) => Err(AuthError::Internal(
                "credential decryption failed — master key mismatch".into(),
            )),
            Err(e) => Err(AuthError::Internal(format!("credential lookup failed: {e}"))),
        }
    }
}

/// Parsed components of the AWS4-HMAC-SHA256 Authorization header.
struct ParsedAuth {
    akid: String,
    date: String,    // YYYYMMDD
    region: String,
    service: String,
    signed_headers: String, // semicolon-joined
    signature: String,      // hex-encoded
}

fn parse_authorization(auth: &str) -> Result<ParsedAuth, AuthError> {
    let rest = auth
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or(AuthError::MalformedAuthorization)?;
    let mut credential: Option<String> = None;
    let mut signed_headers: Option<String> = None;
    let mut signature: Option<String> = None;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v.to_string());
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v.to_string());
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v.to_string());
        }
    }
    let credential = credential.ok_or(AuthError::MalformedAuthorization)?;
    let signed_headers = signed_headers.ok_or(AuthError::MalformedAuthorization)?;
    let signature = signature.ok_or(AuthError::MalformedAuthorization)?;
    let mut parts = credential.split('/');
    let akid = parts.next().ok_or(AuthError::MalformedAuthorization)?.to_string();
    let date = parts.next().ok_or(AuthError::MalformedAuthorization)?.to_string();
    let region = parts.next().ok_or(AuthError::MalformedAuthorization)?.to_string();
    let service = parts.next().ok_or(AuthError::MalformedAuthorization)?.to_string();
    let terminator = parts.next().ok_or(AuthError::MalformedAuthorization)?;
    if terminator != "aws4_request" {
        return Err(AuthError::MalformedAuthorization);
    }
    if akid.is_empty() {
        return Err(AuthError::MalformedAuthorization);
    }
    Ok(ParsedAuth {
        akid,
        date,
        region,
        service,
        signed_headers,
        signature,
    })
}

fn check_clock_skew(amz_date: &str, max_skew_secs: i64) -> Result<(), AuthError> {
    // X-Amz-Date is `YYYYMMDDTHHMMSSZ`.
    let parsed = NaiveDateTime::parse_from_str(amz_date, "%Y%m%dT%H%M%SZ")
        .map_err(|_| AuthError::SignatureFailed("invalid X-Amz-Date".into()))?;
    let signed_ts = parsed.and_utc().timestamp();
    let now_ts = Utc::now().timestamp();
    if (now_ts - signed_ts).abs() > max_skew_secs {
        return Err(AuthError::SignatureFailed(format!(
            "X-Amz-Date out of bounds (>{max_skew_secs}s skew)"
        )));
    }
    Ok(())
}

#[async_trait::async_trait]
impl Verifier for Sigv4Verifier {
    #[tracing::instrument(level = "debug", skip_all, name = "auth.sigv4")]
    async fn verify(
        &self,
        parts: &http::request::Parts,
        body: &[u8],
    ) -> Result<Identity, AuthError> {
        let auth_header = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .ok_or(AuthError::MissingAuthorization)?
            .to_str()
            .map_err(|_| AuthError::MalformedAuthorization)?;
        let parsed = parse_authorization(auth_header)?;

        // X-Amz-Date is required for SigV4.
        let amz_date = parts
            .headers
            .get("x-amz-date")
            .ok_or_else(|| AuthError::SignatureFailed("missing X-Amz-Date".into()))?
            .to_str()
            .map_err(|_| AuthError::MalformedAuthorization)?;
        check_clock_skew(amz_date, self.clock_skew_secs)?;

        let secret = self.fetch_secret(&parsed.akid).await?;

        // Reconstruct canonical request + string-to-sign.
        let payload_hash = hex_sha256(body);
        let method = parts.method.as_str();
        let path = parts.uri.path();
        let query = parts.uri.query().unwrap_or("");
        let canonical = canonical_request(
            method,
            path,
            query,
            &parts.headers,
            &parsed.signed_headers,
            &payload_hash,
        );
        let scope = format!(
            "{}/{}/{}/aws4_request",
            parsed.date, parsed.region, parsed.service
        );
        let sts = string_to_sign(amz_date, &scope, &canonical);
        let signing_k = signing_key(&secret.secret, &parsed.date, &parsed.region, &parsed.service);
        let computed = compute_signature(&signing_k, &sts);

        // Constant-time compare.
        if computed.as_bytes().ct_eq(parsed.signature.as_bytes()).unwrap_u8() != 1 {
            return Err(AuthError::SignatureFailed("signature mismatch".into()));
        }

        let mut claims = serde_json::Map::new();
        claims.insert(
            "aws_akid".into(),
            serde_json::Value::String(parsed.akid.clone()),
        );
        claims.insert("aws_region".into(), serde_json::Value::String(parsed.region));
        claims.insert(
            "aws_service".into(),
            serde_json::Value::String(parsed.service),
        );
        Ok(Identity {
            scheme: AuthScheme::Sigv4,
            principal: secret.principal.clone(),
            issuer: None,
            claims,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigv4::canonical::{compute_signature, signing_key, string_to_sign};

    #[test]
    fn parse_authorization_extracts_components() {
        let auth = "AWS4-HMAC-SHA256 \
            Credential=AKIA/20260101/us-east-1/dynamodb/aws4_request, \
            SignedHeaders=host;x-amz-date, \
            Signature=deadbeef";
        let p = parse_authorization(auth).unwrap();
        assert_eq!(p.akid, "AKIA");
        assert_eq!(p.date, "20260101");
        assert_eq!(p.region, "us-east-1");
        assert_eq!(p.service, "dynamodb");
        assert_eq!(p.signed_headers, "host;x-amz-date");
        assert_eq!(p.signature, "deadbeef");
    }

    #[test]
    fn parse_authorization_rejects_missing_credential() {
        let auth = "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=x";
        assert!(matches!(
            parse_authorization(auth),
            Err(AuthError::MalformedAuthorization)
        ));
    }

    #[test]
    fn parse_authorization_rejects_non_sigv4() {
        let auth = "Bearer xyz";
        assert!(matches!(
            parse_authorization(auth),
            Err(AuthError::MalformedAuthorization)
        ));
    }

    #[test]
    fn parse_authorization_rejects_wrong_terminator() {
        let auth = "AWS4-HMAC-SHA256 \
            Credential=AKIA/20260101/us-east-1/dynamodb/BAD, \
            SignedHeaders=host, Signature=x";
        assert!(matches!(
            parse_authorization(auth),
            Err(AuthError::MalformedAuthorization)
        ));
    }

    #[test]
    fn clock_skew_accepts_now() {
        let now = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        assert!(check_clock_skew(&now, 900).is_ok());
    }

    #[test]
    fn clock_skew_rejects_old_date() {
        assert!(check_clock_skew("20200101T000000Z", 900).is_err());
    }

    #[test]
    fn clock_skew_rejects_garbage() {
        assert!(check_clock_skew("not-a-date", 900).is_err());
    }

    /// End-to-end: sign a request with a known secret, parse it back,
    /// recompute, verify the signature matches. No PG / cache.
    #[test]
    fn self_signed_request_verifies() {
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let date = "20150830";
        let amz_date = "20150830T123600Z";
        let region = "us-east-1";
        let service = "service";

        let mut headers = http::HeaderMap::new();
        headers.insert("host", "example.amazonaws.com".parse().unwrap());
        headers.insert("x-amz-date", amz_date.parse().unwrap());

        let payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let signed_headers = "host;x-amz-date";
        let canonical =
            crate::sigv4::canonical::canonical_request("GET", "/", "", &headers, signed_headers, payload_hash);
        let scope = format!("{date}/{region}/{service}/aws4_request");
        let sts = string_to_sign(amz_date, &scope, &canonical);
        let sk = signing_key(secret, date, region, service);
        let sig = compute_signature(&sk, &sts);
        assert_eq!(
            sig,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }
}
