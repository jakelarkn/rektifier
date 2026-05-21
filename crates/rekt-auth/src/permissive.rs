//! Dev-only verifier that accepts every request. Loud-warns on first
//! invocation. Never enable in production. See `docs/plan/PLAN-13` D10.

use crate::{warn_permissive_once, AuthError, AuthScheme, Identity, Verifier};

pub struct PermissiveVerifier;

#[async_trait::async_trait]
impl Verifier for PermissiveVerifier {
    #[tracing::instrument(level = "debug", skip_all, name = "auth.permissive")]
    async fn verify(
        &self,
        parts: &http::request::Parts,
        _body: &[u8],
    ) -> Result<Identity, AuthError> {
        warn_permissive_once();
        let mut claims = serde_json::Map::new();
        if let Some(akid) = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(crate::sigv4::parse_access_key_id)
        {
            claims.insert("aws_akid".into(), serde_json::Value::String(akid));
        }
        Ok(Identity {
            scheme: AuthScheme::Permissive,
            principal: "anonymous".into(),
            issuer: None,
            claims,
        })
    }
}
