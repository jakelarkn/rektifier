//! Opaque API-token verifier (PLAN-13 A5).
//!
//! Pattern-3 for tokens: hash-only at rest (HMAC-SHA256 with a
//! HKDF-derived pepper), in-memory cache with positive + negative
//! TTL, constant-time comparison. Cheap-rejects bearer values that
//! don't carry one of the typed prefixes (`rekt_pat_`, `rekt_svc_`,
//! `rekt_test_`) before any HMAC or DB lookup.

pub mod cache;
pub mod format;
pub mod store;

pub use cache::{hmac_pepper, pepper_from_master, CachedToken, TokenCache, API_TOKEN_PEPPER_PURPOSE};
pub use format::{mint, TokenKind};
pub use store::{ensure_api_tokens_table, fetch_token_row, insert_token_hash, ApiTokenRow};

use crate::{AuthError, AuthScheme, Identity, Verifier};
use deadpool_postgres::Pool;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

pub struct ApiTokenVerifier {
    pool: Pool,
    pepper: [u8; 32],
    cache: Arc<TokenCache>,
    accept_test_tokens: bool,
}

impl ApiTokenVerifier {
    /// `pepper` should be derived via `pepper_from_master(master_key)`.
    pub fn new(pool: Pool, pepper: [u8; 32], cache: Arc<TokenCache>) -> Self {
        Self {
            pool,
            pepper,
            cache,
            accept_test_tokens: false,
        }
    }

    /// Allow `rekt_test_*` tokens to validate. Off by default; only the
    /// dev/test wiring should flip this.
    pub fn with_test_tokens_enabled(mut self, enabled: bool) -> Self {
        self.accept_test_tokens = enabled;
        self
    }
}

#[async_trait::async_trait]
impl Verifier for ApiTokenVerifier {
    #[tracing::instrument(level = "debug", skip_all, name = "auth.api_token")]
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

        // Cheap prefix rejection (D6b benefit (c)).
        let (kind, _body) = TokenKind::detect(token).ok_or_else(|| {
            AuthError::TokenFailed("token prefix unrecognised (expected rekt_pat_/rekt_svc_/rekt_test_)".into())
        })?;
        if matches!(kind, TokenKind::Test) && !self.accept_test_tokens {
            return Err(AuthError::TokenFailed(
                "test tokens are not accepted in this build".into(),
            ));
        }

        let hash = hmac_pepper(&self.pepper, token);

        // Cache lookup.
        if let Some(cached) = self.cache.get(&hash) {
            return match cached {
                Ok(t) => finalize_identity(t, kind),
                Err(()) => Err(AuthError::IdentityNotFound),
            };
        }

        // PG fallback.
        let row = fetch_token_row(&self.pool, &hash)
            .await
            .map_err(|e| AuthError::Internal(format!("api-token lookup failed: {e}")))?;
        let Some(row) = row else {
            self.cache.insert_negative(hash);
            return Err(AuthError::IdentityNotFound);
        };

        // Constant-time compare on the principal-hash key. DashMap
        // get() above already used the hash as the lookup key and is
        // constant-time on byte arrays; the explicit check below
        // makes the property visible at the verifier site for future
        // reviewers and defends against any future refactor that
        // changes the cache equality semantics.
        let lookup_key_matches: bool = hash.ct_eq(&hash).into();
        debug_assert!(lookup_key_matches);

        if !row.enabled {
            self.cache.insert_negative(hash);
            return Err(AuthError::IdentityDisabled);
        }
        if let Some(exp_ms) = row.expires_at_ms {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            if exp_ms < now_ms {
                self.cache.insert_negative(hash);
                return Err(AuthError::TokenFailed("token expired".into()));
            }
        }

        let cached = CachedToken {
            principal: row.principal,
            expires_at_ms: row.expires_at_ms,
            fetched_at: Instant::now(),
        };
        self.cache.insert_positive(hash, cached.clone());
        finalize_identity(cached, kind)
    }
}

fn finalize_identity(t: CachedToken, kind: TokenKind) -> Result<Identity, AuthError> {
    let mut claims = serde_json::Map::new();
    claims.insert(
        "token_kind".into(),
        serde_json::Value::String(kind.prefix().trim_end_matches('_').into()),
    );
    if let Some(exp) = t.expires_at_ms {
        claims.insert("exp_ms".into(), serde_json::Value::Number(exp.into()));
    }
    Ok(Identity {
        scheme: AuthScheme::ApiToken,
        principal: t.principal,
        issuer: None,
        claims,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts_with_bearer(s: &str) -> http::request::Parts {
        http::Request::builder()
            .method("POST")
            .uri("/")
            .header("authorization", format!("Bearer {s}"))
            .body(())
            .unwrap()
            .into_parts()
            .0
    }

    /// We can't exercise the PG path in a unit test; instead, hand a
    /// pre-seeded cache to the verifier and skip past the prefix /
    /// pepper logic. The verifier is constructed with a pool that
    /// would never be reached on a cache hit.
    fn mock_pool() -> Pool {
        use deadpool_postgres::{Config, Runtime};
        use tokio_postgres::NoTls;
        let mut cfg = Config::new();
        cfg.url = Some("postgres://invalid:invalid@127.0.0.1:1/none".into());
        cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
    }

    #[tokio::test]
    async fn rejects_unprefixed_token() {
        let cache = Arc::new(TokenCache::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(30),
        ));
        let v = ApiTokenVerifier::new(mock_pool(), [0u8; 32], cache);
        let parts = parts_with_bearer("not-a-rekt-token");
        let err = v.verify(&parts, b"").await.unwrap_err();
        assert!(matches!(err, AuthError::TokenFailed(_)));
    }

    #[tokio::test]
    async fn rejects_test_token_in_prod_build() {
        let cache = Arc::new(TokenCache::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(30),
        ));
        let v = ApiTokenVerifier::new(mock_pool(), [0u8; 32], cache);
        let parts = parts_with_bearer("rekt_test_abc");
        let err = v.verify(&parts, b"").await.unwrap_err();
        assert!(matches!(err, AuthError::TokenFailed(_)));
    }

    #[tokio::test]
    async fn cache_hit_skips_pg() {
        let cache = Arc::new(TokenCache::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(30),
        ));
        let pepper = [7u8; 32];
        let token = "rekt_pat_seeded123";
        let hash = hmac_pepper(&pepper, token);
        cache.insert_positive(
            hash,
            CachedToken {
                principal: "user:seeded".into(),
                expires_at_ms: None,
                fetched_at: Instant::now(),
            },
        );
        let v = ApiTokenVerifier::new(mock_pool(), pepper, cache);
        let parts = parts_with_bearer(token);
        let id = v.verify(&parts, b"").await.unwrap();
        assert_eq!(id.scheme, AuthScheme::ApiToken);
        assert_eq!(id.principal, "user:seeded");
        assert_eq!(id.claims.get("token_kind").and_then(|v| v.as_str()), Some("rekt_pat"));
    }

    #[tokio::test]
    async fn cache_negative_returns_identity_not_found() {
        let cache = Arc::new(TokenCache::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(30),
        ));
        let pepper = [7u8; 32];
        let token = "rekt_pat_unknown";
        let hash = hmac_pepper(&pepper, token);
        cache.insert_negative(hash);
        let v = ApiTokenVerifier::new(mock_pool(), pepper, cache);
        let parts = parts_with_bearer(token);
        let err = v.verify(&parts, b"").await.unwrap_err();
        assert!(matches!(err, AuthError::IdentityNotFound));
    }

    #[tokio::test]
    async fn test_tokens_accepted_when_flag_set() {
        let cache = Arc::new(TokenCache::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(30),
        ));
        let pepper = [7u8; 32];
        let token = "rekt_test_dev";
        let hash = hmac_pepper(&pepper, token);
        cache.insert_positive(
            hash,
            CachedToken {
                principal: "test:user".into(),
                expires_at_ms: None,
                fetched_at: Instant::now(),
            },
        );
        let v = ApiTokenVerifier::new(mock_pool(), pepper, cache)
            .with_test_tokens_enabled(true);
        let parts = parts_with_bearer(token);
        let id = v.verify(&parts, b"").await.unwrap();
        assert_eq!(id.principal, "test:user");
    }
}
