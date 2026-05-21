//! Ecosystem preset functions (PLAN-13 D9 / A4).
//!
//! One function per supported IdP. Each takes deployment-specific
//! parameters (tenant_id, account, workspace_url, …) plus the
//! expected audience and returns a `JwtIssuerConfig` with all the
//! ecosystem details (JWKS URL, allowed algs, principal_format)
//! baked in.
//!
//! These are pure code, version-bumped with rektifier. If an
//! ecosystem changes its JWKS endpoint, the upgrade is a code patch,
//! not an operator config change.

use crate::jwt::issuer::{JwtIssuerConfig, PrincipalFormat};
use jsonwebtoken::Algorithm;
use std::time::Duration;

const DEFAULT_CLOCK_SKEW: Duration = Duration::from_secs(60);
const DEFAULT_JWKS_TTL: Duration = Duration::from_secs(600);

/// Google Cloud (ID tokens from accounts.google.com).
///
/// Validates tokens minted by Google's OIDC endpoint — workforce
/// identity, service-account-impersonation ID tokens, and standard
/// user OAuth ID tokens. `principal` is mapped from the `email`
/// claim and prefixed `gcp:user:` (for human ID tokens) or
/// `gcp:sa:` (for service-account-domain emails). Callers that
/// need to distinguish can use `PrincipalFormat::Custom`.
pub fn gcp(audience: impl Into<String>) -> JwtIssuerConfig {
    JwtIssuerConfig {
        issuer: "https://accounts.google.com".into(),
        jwks_url: "https://www.googleapis.com/oauth2/v3/certs".into(),
        expected_audience: vec![audience.into()],
        allowed_algs: vec![Algorithm::RS256],
        clock_skew: DEFAULT_CLOCK_SKEW,
        jwks_cache_ttl: DEFAULT_JWKS_TTL,
        principal_format: PrincipalFormat::Email("gcp:user:".into()),
        safe_to_log_claims: vec!["iss".into(), "sub".into(), "aud".into(), "azp".into()],
    }
}

/// Microsoft Entra ID (formerly Azure AD), tenant-parameterised.
/// Validates app + service-principal + user tokens issued for a
/// specific tenant. Principal comes from the `oid` claim which is
/// the stable Entra object id (works for both users and SPs).
pub fn azure(tenant_id: &str, audience: impl Into<String>) -> JwtIssuerConfig {
    JwtIssuerConfig {
        issuer: format!("https://sts.windows.net/{tenant_id}/"),
        jwks_url: format!(
            "https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
        ),
        expected_audience: vec![audience.into()],
        allowed_algs: vec![Algorithm::RS256],
        clock_skew: DEFAULT_CLOCK_SKEW,
        jwks_cache_ttl: DEFAULT_JWKS_TTL,
        principal_format: PrincipalFormat::AzureObjectId("azure:sp:".into()),
        safe_to_log_claims: vec!["iss".into(), "sub".into(), "aud".into(), "oid".into(), "tid".into(), "appid".into()],
    }
}

/// Snowflake-issued OAuth tokens (account-parameterised). Snowflake
/// publishes the JWKS at the account's `oauth/jwks` endpoint.
///
/// NB: Snowflake's "key-pair JWT authentication" is a *different*
/// flow where the client self-signs with a registered keypair and
/// Snowflake validates — that path has no JWKS visible to us and
/// is not supported by this preset. See PLAN-13 D9 Snowflake notes.
pub fn snowflake(account: &str, audience: impl Into<String>) -> JwtIssuerConfig {
    JwtIssuerConfig {
        issuer: format!("https://{account}.snowflakecomputing.com"),
        jwks_url: format!("https://{account}.snowflakecomputing.com/oauth/jwks"),
        expected_audience: vec![audience.into()],
        allowed_algs: vec![Algorithm::RS256],
        clock_skew: DEFAULT_CLOCK_SKEW,
        jwks_cache_ttl: DEFAULT_JWKS_TTL,
        principal_format: PrincipalFormat::Subject("snowflake:user:".into()),
        safe_to_log_claims: vec!["iss".into(), "sub".into(), "aud".into()],
    }
}

/// Databricks workspace OIDC. M2M tokens for service principals and
/// user OAuth tokens both go through the workspace's `/oidc`
/// endpoint. `workspace_url` must include the scheme and host (e.g.
/// `https://my-shard.cloud.databricks.com`).
pub fn databricks(
    workspace_url: &str,
    audience: impl Into<String>,
) -> JwtIssuerConfig {
    let workspace = workspace_url.trim_end_matches('/');
    JwtIssuerConfig {
        issuer: format!("{workspace}/oidc"),
        jwks_url: format!("{workspace}/oidc/.well-known/jwks.json"),
        expected_audience: vec![audience.into()],
        allowed_algs: vec![Algorithm::RS256],
        clock_skew: DEFAULT_CLOCK_SKEW,
        jwks_cache_ttl: DEFAULT_JWKS_TTL,
        principal_format: PrincipalFormat::Subject("databricks:user:".into()),
        safe_to_log_claims: vec!["iss".into(), "sub".into(), "aud".into()],
    }
}

/// Neon project-scoped JWT validation (Neon Auth). Note: most
/// Neon API access uses opaque API keys — those go through the
/// A5 API-token verifier, not here.
pub fn neon(
    project_id: &str,
    region: &str,
    audience: impl Into<String>,
) -> JwtIssuerConfig {
    let host = format!("{project_id}.{region}.aws.neon.tech");
    JwtIssuerConfig {
        issuer: format!("https://{host}"),
        jwks_url: format!("https://{host}/.well-known/jwks.json"),
        expected_audience: vec![audience.into()],
        allowed_algs: vec![Algorithm::RS256],
        clock_skew: DEFAULT_CLOCK_SKEW,
        jwks_cache_ttl: DEFAULT_JWKS_TTL,
        principal_format: PrincipalFormat::Subject("neon:user:".into()),
        safe_to_log_claims: vec!["iss".into(), "sub".into(), "aud".into()],
    }
}

/// AWS Cognito user pool (common companion when running rektifier
/// behind a Cognito-fronted gateway). Parameterised by region and
/// user pool id.
pub fn aws_cognito(
    user_pool_id: &str,
    region: &str,
    audience: impl Into<String>,
) -> JwtIssuerConfig {
    JwtIssuerConfig {
        issuer: format!("https://cognito-idp.{region}.amazonaws.com/{user_pool_id}"),
        jwks_url: format!(
            "https://cognito-idp.{region}.amazonaws.com/{user_pool_id}/.well-known/jwks.json"
        ),
        expected_audience: vec![audience.into()],
        allowed_algs: vec![Algorithm::RS256],
        clock_skew: DEFAULT_CLOCK_SKEW,
        jwks_cache_ttl: DEFAULT_JWKS_TTL,
        principal_format: PrincipalFormat::Subject("cognito:user:".into()),
        safe_to_log_claims: vec!["iss".into(), "sub".into(), "aud".into(), "client_id".into()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gcp_preset_is_valid_and_canonical() {
        let c = gcp("rektifier-prod");
        c.validate().unwrap();
        assert_eq!(c.issuer, "https://accounts.google.com");
        assert_eq!(
            c.jwks_url,
            "https://www.googleapis.com/oauth2/v3/certs"
        );
        assert_eq!(c.expected_audience, vec!["rektifier-prod"]);
        assert_eq!(c.allowed_algs, vec![Algorithm::RS256]);
        assert!(matches!(c.principal_format, PrincipalFormat::Email(_)));
    }

    #[test]
    fn azure_preset_uses_tenant_id() {
        let tenant = "11111111-2222-3333-4444-555555555555";
        let c = azure(tenant, "api://rektifier");
        c.validate().unwrap();
        assert!(c.issuer.contains(tenant));
        assert!(c.jwks_url.contains(tenant));
        assert!(c.jwks_url.starts_with("https://login.microsoftonline.com/"));
        assert!(matches!(c.principal_format, PrincipalFormat::AzureObjectId(_)));
    }

    #[test]
    fn snowflake_preset_uses_account() {
        let c = snowflake("ab12345.us-east-1", "rektifier");
        c.validate().unwrap();
        assert_eq!(
            c.issuer,
            "https://ab12345.us-east-1.snowflakecomputing.com"
        );
        assert!(c.jwks_url.ends_with("/oauth/jwks"));
    }

    #[test]
    fn databricks_preset_trims_trailing_slash() {
        let c = databricks("https://my-shard.cloud.databricks.com/", "rektifier");
        c.validate().unwrap();
        assert_eq!(
            c.issuer,
            "https://my-shard.cloud.databricks.com/oidc"
        );
        assert_eq!(
            c.jwks_url,
            "https://my-shard.cloud.databricks.com/oidc/.well-known/jwks.json"
        );
    }

    #[test]
    fn neon_preset_uses_project_and_region() {
        let c = neon("proj-abc", "us-east-1", "rektifier");
        c.validate().unwrap();
        assert!(c.issuer.contains("proj-abc.us-east-1.aws.neon.tech"));
    }

    #[test]
    fn aws_cognito_preset_uses_region_and_pool_id() {
        let c = aws_cognito("us-east-1_abc123", "us-east-1", "rektifier");
        c.validate().unwrap();
        assert_eq!(
            c.issuer,
            "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123"
        );
        assert!(c.jwks_url.ends_with("/.well-known/jwks.json"));
    }

    #[test]
    fn all_presets_default_to_rs256_only() {
        for c in [
            gcp("aud"),
            azure("tenant", "aud"),
            snowflake("acct", "aud"),
            databricks("https://w", "aud"),
            neon("p", "r", "aud"),
            aws_cognito("pool", "r", "aud"),
        ] {
            assert_eq!(c.allowed_algs, vec![Algorithm::RS256]);
            // Per D3a + D4a — all preset JWKS URLs must be HTTPS.
            assert!(
                c.jwks_url.starts_with("https://"),
                "preset jwks_url is not https: {}",
                c.jwks_url
            );
        }
    }
}
