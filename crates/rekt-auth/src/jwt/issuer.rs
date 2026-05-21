//! Per-issuer JWT validation configuration. One `JwtIssuerConfig`
//! corresponds to one trusted IdP — Snowflake account, Azure tenant,
//! GCP project, etc. The A4 preset functions build these structs;
//! operators can also supply one by hand via `[[auth.jwt.issuer]]`
//! with `preset = "generic"`.

use jsonwebtoken::Algorithm;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum IssuerConfigError {
    #[error("jwks_url must use https://, got: {0}")]
    InsecureJwksUrl(String),
    #[error("jwks_url is not a valid URL: {0}")]
    InvalidJwksUrl(String),
    #[error("expected_audience must not be empty")]
    EmptyAudience,
    #[error("allowed_algs must not be empty")]
    EmptyAllowedAlgs,
    #[error("alg=none is rejected unconditionally")]
    NoneAlgForbidden,
}

#[derive(Debug, Clone)]
pub struct JwtIssuerConfig {
    /// Required `iss` claim value.
    pub issuer: String,
    /// HTTPS-only (D4a). Validated at construction.
    pub jwks_url: String,
    /// Any-of match on the token's `aud` claim. Must be non-empty.
    pub expected_audience: Vec<String>,
    /// Allowed algorithms. Operators typically configure `[RS256]`;
    /// the JWKS entry's alg is used at validation time (D3a), this
    /// list is the outer allowlist.
    pub allowed_algs: Vec<Algorithm>,
    /// +/- tolerance on `exp` / `nbf` / `iat`.
    pub clock_skew: Duration,
    /// How long a fetched JWKS document stays fresh in cache before
    /// background refresh.
    pub jwks_cache_ttl: Duration,
    /// How to map validated claims to `Identity.principal`.
    pub principal_format: PrincipalFormat,
    /// Claim names that may be safely emitted to audit logs for
    /// this issuer (PLAN-13 D2 / A6 PII allowlist). Anything not in
    /// this list is dropped before the audit line is constructed.
    /// Empty list = log nothing beyond `principal` + `scheme` +
    /// `issuer`.
    pub safe_to_log_claims: Vec<String>,
}

/// How to derive the namespaced `Identity.principal` string from the
/// validated claims. See PLAN-13 D3.
#[derive(Debug, Clone)]
pub enum PrincipalFormat {
    /// Use `sub` claim verbatim, prefixed with a static namespace
    /// (e.g. `"snowflake:user:{sub}"`).
    Subject(String),
    /// Use `email` claim, prefixed with a static namespace.
    Email(String),
    /// Use `oid` claim (Entra ID / Azure AD service principals).
    AzureObjectId(String),
    /// Operator-supplied template: `"databricks:user:{sub}"` or
    /// `"gcp:sa:{email}"`. Supported placeholders: any top-level
    /// claim name surrounded by `{}`.
    Custom(String),
}

impl JwtIssuerConfig {
    /// Validate a freshly-deserialised config. Rejects insecure JWKS
    /// URLs (D4a), empty audience, empty algs, and `alg=none`.
    pub fn validate(&self) -> Result<(), IssuerConfigError> {
        let parsed = url::Url::parse(&self.jwks_url)
            .map_err(|_| IssuerConfigError::InvalidJwksUrl(self.jwks_url.clone()))?;
        if parsed.scheme() != "https" {
            return Err(IssuerConfigError::InsecureJwksUrl(self.jwks_url.clone()));
        }
        if self.expected_audience.is_empty() {
            return Err(IssuerConfigError::EmptyAudience);
        }
        if self.allowed_algs.is_empty() {
            return Err(IssuerConfigError::EmptyAllowedAlgs);
        }
        if self.allowed_algs.iter().any(|a| matches!(a, Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512)) {
            // HS* with a public-key-style issuer is the alg-confusion
            // attack surface — reject it as a config-time error so a
            // misconfigured operator can't enable it.
            return Err(IssuerConfigError::EmptyAllowedAlgs);
        }
        Ok(())
    }

    /// Apply `principal_format` to a claim map, returning the resolved
    /// principal string. Returns `None` if a referenced claim is missing.
    pub fn resolve_principal(
        &self,
        claims: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<String> {
        match &self.principal_format {
            PrincipalFormat::Subject(prefix) => claims
                .get("sub")
                .and_then(|v| v.as_str())
                .map(|sub| format!("{prefix}{sub}")),
            PrincipalFormat::Email(prefix) => claims
                .get("email")
                .and_then(|v| v.as_str())
                .map(|email| format!("{prefix}{email}")),
            PrincipalFormat::AzureObjectId(prefix) => claims
                .get("oid")
                .and_then(|v| v.as_str())
                .map(|oid| format!("{prefix}{oid}")),
            PrincipalFormat::Custom(template) => render_template(template, claims),
        }
    }
}

fn render_template(
    template: &str,
    claims: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    // Replace `{claim_name}` with the corresponding string-valued
    // claim. Missing or non-string claims abort with None so the
    // caller surfaces a "principal could not be derived" error.
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            let mut name = String::new();
            for c2 in chars.by_ref() {
                if c2 == '}' {
                    break;
                }
                name.push(c2);
            }
            let v = claims.get(&name)?.as_str()?;
            out.push_str(v);
        } else {
            out.push(c);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good_cfg() -> JwtIssuerConfig {
        JwtIssuerConfig {
            issuer: "https://issuer.example/".into(),
            jwks_url: "https://issuer.example/.well-known/jwks.json".into(),
            expected_audience: vec!["rektifier".into()],
            allowed_algs: vec![Algorithm::RS256],
            clock_skew: Duration::from_secs(60),
            jwks_cache_ttl: Duration::from_secs(600),
            principal_format: PrincipalFormat::Subject("issuer:user:".into()),
            safe_to_log_claims: vec!["iss".into(), "sub".into(), "aud".into()],
        }
    }

    #[test]
    fn accepts_well_formed_config() {
        good_cfg().validate().unwrap();
    }

    #[test]
    fn rejects_http_jwks_url() {
        let mut c = good_cfg();
        c.jwks_url = "http://insecure.example/jwks".into();
        assert!(matches!(
            c.validate(),
            Err(IssuerConfigError::InsecureJwksUrl(_))
        ));
    }

    #[test]
    fn rejects_invalid_jwks_url() {
        let mut c = good_cfg();
        c.jwks_url = "not-a-url".into();
        assert!(matches!(
            c.validate(),
            Err(IssuerConfigError::InvalidJwksUrl(_))
        ));
    }

    #[test]
    fn rejects_empty_audience() {
        let mut c = good_cfg();
        c.expected_audience.clear();
        assert!(matches!(c.validate(), Err(IssuerConfigError::EmptyAudience)));
    }

    #[test]
    fn rejects_hmac_algs_at_config_time() {
        // HS* configured at the issuer level is the alg-confusion
        // attack surface; reject so the operator can't enable it.
        let mut c = good_cfg();
        c.allowed_algs = vec![Algorithm::HS256];
        assert!(c.validate().is_err());
    }

    #[test]
    fn resolves_subject_principal() {
        let mut claims = serde_json::Map::new();
        claims.insert("sub".into(), serde_json::Value::String("alice".into()));
        let cfg = good_cfg();
        assert_eq!(cfg.resolve_principal(&claims).as_deref(), Some("issuer:user:alice"));
    }

    #[test]
    fn resolves_email_principal() {
        let mut c = good_cfg();
        c.principal_format = PrincipalFormat::Email("gcp:sa:".into());
        let mut claims = serde_json::Map::new();
        claims.insert(
            "email".into(),
            serde_json::Value::String("bot@p.iam.gserviceaccount.com".into()),
        );
        assert_eq!(
            c.resolve_principal(&claims).as_deref(),
            Some("gcp:sa:bot@p.iam.gserviceaccount.com")
        );
    }

    #[test]
    fn resolves_custom_template() {
        let mut c = good_cfg();
        c.principal_format = PrincipalFormat::Custom("databricks:user:{sub}@{aud}".into());
        let mut claims = serde_json::Map::new();
        claims.insert("sub".into(), serde_json::Value::String("u123".into()));
        claims.insert("aud".into(), serde_json::Value::String("ws-prod".into()));
        assert_eq!(
            c.resolve_principal(&claims).as_deref(),
            Some("databricks:user:u123@ws-prod")
        );
    }

    #[test]
    fn custom_template_missing_claim_returns_none() {
        let mut c = good_cfg();
        c.principal_format = PrincipalFormat::Custom("x:{missing}".into());
        let claims = serde_json::Map::new();
        assert!(c.resolve_principal(&claims).is_none());
    }
}
