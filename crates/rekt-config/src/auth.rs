//! Auth-config types (PLAN-13 A8).
//!
//! The TOML surface and its resolved counterparts. The binary calls
//! [`Config::auth`] to get an [`AuthConfig`] and threads it into
//! `rekt-auth`'s verifier builders.

use serde::Deserialize;
use std::path::PathBuf;

use crate::ConfigError;

/// Fully-resolved auth configuration.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Master-key source shared by SigV4 + API tokens (HKDF subkeys).
    /// `None` when neither sigv4 nor api_token is enabled.
    pub master_key: Option<MasterKey>,
    pub permissive: PermissiveConfig,
    pub sigv4: Sigv4Config,
    pub api_token: ApiTokenConfig,
    pub jwt: JwtConfig,
    /// PLAN-13 A6: per-request `auth.audit` log line, off by default.
    pub audit_log_enabled: bool,
}

#[derive(Debug, Clone)]
pub enum MasterKey {
    Env(String),
    File(PathBuf),
    /// Deferred — A2a KMS provider.
    Kms(String),
}

#[derive(Debug, Clone, Default)]
pub struct PermissiveConfig {
    /// Dev-only: accept requests with no Authorization header.
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct Sigv4Config {
    pub enabled: bool,
    pub clock_skew_secs: i64,
    pub cache_ttl_secs: u64,
    pub negative_cache_ttl_secs: u64,
}

impl Default for Sigv4Config {
    fn default() -> Self {
        Self {
            enabled: false,
            clock_skew_secs: 900,
            cache_ttl_secs: 300,
            negative_cache_ttl_secs: 30,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApiTokenConfig {
    pub enabled: bool,
    pub accept_test_tokens: bool,
    pub cache_ttl_secs: u64,
    pub negative_cache_ttl_secs: u64,
}

impl Default for ApiTokenConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            accept_test_tokens: false,
            cache_ttl_secs: 300,
            negative_cache_ttl_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct JwtConfig {
    pub issuers: Vec<JwtIssuerEntry>,
}

#[derive(Debug, Clone)]
pub struct JwtIssuerEntry {
    /// Preset name as supplied. `"generic"` means the operator
    /// provided issuer + jwks_url + principal_format directly.
    pub preset: String,
    /// Per-preset parameters; the resolver checks the right
    /// combination for each preset name.
    pub tenant_id: Option<String>,
    pub account: Option<String>,
    pub workspace_url: Option<String>,
    pub project_id: Option<String>,
    pub region: Option<String>,
    pub user_pool_id: Option<String>,
    pub issuer: Option<String>,
    pub jwks_url: Option<String>,
    pub principal_format: Option<PrincipalFormatEntry>,
    pub expected_audience: Vec<String>,
    pub safe_to_log_claims: Option<Vec<String>>,
    pub clock_skew_secs: Option<u64>,
    pub jwks_cache_ttl_secs: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum PrincipalFormatEntry {
    Subject { prefix: String },
    Email { prefix: String },
    AzureObjectId { prefix: String },
    Custom { template: String },
}

// =========================================================================
// Raw (parse-only) types.
// =========================================================================

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawAuth {
    #[serde(default)]
    pub(crate) master_key_env: Option<String>,
    #[serde(default)]
    pub(crate) master_key_file: Option<PathBuf>,
    #[serde(default)]
    pub(crate) master_key_kms: Option<String>,
    #[serde(default)]
    pub(crate) audit_log_enabled: Option<bool>,
    #[serde(default)]
    pub(crate) permissive: Option<RawPermissive>,
    #[serde(default)]
    pub(crate) sigv4: Option<RawSigv4>,
    #[serde(default)]
    pub(crate) api_token: Option<RawApiToken>,
    #[serde(default)]
    pub(crate) jwt: Option<RawJwt>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPermissive {
    #[serde(default)]
    pub(crate) enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSigv4 {
    #[serde(default)]
    pub(crate) enabled: Option<bool>,
    #[serde(default)]
    pub(crate) clock_skew_secs: Option<i64>,
    #[serde(default)]
    pub(crate) cache_ttl_secs: Option<u64>,
    #[serde(default)]
    pub(crate) negative_cache_ttl_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawApiToken {
    #[serde(default)]
    pub(crate) enabled: Option<bool>,
    #[serde(default)]
    pub(crate) accept_test_tokens: Option<bool>,
    #[serde(default)]
    pub(crate) cache_ttl_secs: Option<u64>,
    #[serde(default)]
    pub(crate) negative_cache_ttl_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawJwt {
    #[serde(default)]
    pub(crate) issuer: Vec<RawJwtIssuer>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawJwtIssuer {
    pub(crate) preset: String,
    pub(crate) expected_audience: Vec<String>,
    #[serde(default)]
    pub(crate) tenant_id: Option<String>,
    #[serde(default)]
    pub(crate) account: Option<String>,
    #[serde(default)]
    pub(crate) workspace_url: Option<String>,
    #[serde(default)]
    pub(crate) project_id: Option<String>,
    #[serde(default)]
    pub(crate) region: Option<String>,
    #[serde(default)]
    pub(crate) user_pool_id: Option<String>,
    #[serde(default)]
    pub(crate) issuer: Option<String>,
    #[serde(default)]
    pub(crate) jwks_url: Option<String>,
    #[serde(default)]
    pub(crate) principal_format: Option<RawPrincipalFormat>,
    #[serde(default)]
    pub(crate) safe_to_log_claims: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) clock_skew_secs: Option<u64>,
    #[serde(default)]
    pub(crate) jwks_cache_ttl_secs: Option<u64>,
}

/// `principal_format` accepts shorthand strings ("subject", "email",
/// "azure_oid") OR a table `{ kind = "subject", prefix = "..." }`
/// OR `{ custom = "..." }`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum RawPrincipalFormat {
    Shorthand(String),
    Table(RawPrincipalFormatTable),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPrincipalFormatTable {
    #[serde(default)]
    pub(crate) kind: Option<String>,
    #[serde(default)]
    pub(crate) prefix: Option<String>,
    #[serde(default)]
    pub(crate) custom: Option<String>,
}

// =========================================================================
// Resolution.
// =========================================================================

pub(crate) fn resolve_auth(raw: Option<RawAuth>) -> Result<AuthConfig, ConfigError> {
    let raw = raw.unwrap_or_default();

    let master_key = match (raw.master_key_env, raw.master_key_file, raw.master_key_kms) {
        (None, None, None) => None,
        (Some(s), None, None) => Some(MasterKey::Env(s)),
        (None, Some(p), None) => Some(MasterKey::File(p)),
        (None, None, Some(s)) => Some(MasterKey::Kms(s)),
        _ => {
            return Err(ConfigError::validation(
                "auth: exactly one of master_key_env / master_key_file / master_key_kms may be set",
            ));
        }
    };

    let permissive = PermissiveConfig {
        enabled: raw.permissive.and_then(|p| p.enabled).unwrap_or(false),
    };

    let sigv4 = match raw.sigv4 {
        None => Sigv4Config::default(),
        Some(r) => {
            let mut s = Sigv4Config::default();
            if let Some(b) = r.enabled {
                s.enabled = b;
            }
            if let Some(n) = r.clock_skew_secs {
                s.clock_skew_secs = n;
            }
            if let Some(n) = r.cache_ttl_secs {
                s.cache_ttl_secs = n;
            }
            if let Some(n) = r.negative_cache_ttl_secs {
                s.negative_cache_ttl_secs = n;
            }
            s
        }
    };

    let api_token = match raw.api_token {
        None => ApiTokenConfig::default(),
        Some(r) => {
            let mut a = ApiTokenConfig::default();
            if let Some(b) = r.enabled {
                a.enabled = b;
            }
            if let Some(b) = r.accept_test_tokens {
                a.accept_test_tokens = b;
            }
            if let Some(n) = r.cache_ttl_secs {
                a.cache_ttl_secs = n;
            }
            if let Some(n) = r.negative_cache_ttl_secs {
                a.negative_cache_ttl_secs = n;
            }
            a
        }
    };

    let jwt = match raw.jwt {
        None => JwtConfig::default(),
        Some(r) => {
            let mut issuers = Vec::with_capacity(r.issuer.len());
            for ri in r.issuer {
                issuers.push(resolve_jwt_issuer(ri)?);
            }
            JwtConfig { issuers }
        }
    };

    // Cross-field validation: if SigV4 or API tokens are enabled, a
    // master_key source MUST be configured.
    if (sigv4.enabled || api_token.enabled) && master_key.is_none() {
        return Err(ConfigError::validation(
            "auth.sigv4 / auth.api_token are enabled but no auth.master_key_{env,file,kms} is set",
        ));
    }

    Ok(AuthConfig {
        master_key,
        permissive,
        sigv4,
        api_token,
        jwt,
        audit_log_enabled: raw.audit_log_enabled.unwrap_or(false),
    })
}

fn resolve_jwt_issuer(r: RawJwtIssuer) -> Result<JwtIssuerEntry, ConfigError> {
    let preset = r.preset.to_ascii_lowercase();
    let known_presets = [
        "gcp",
        "azure",
        "snowflake",
        "databricks",
        "neon",
        "aws-cognito",
        "aws_cognito",
        "generic",
    ];
    if !known_presets.contains(&preset.as_str()) {
        return Err(ConfigError::validation(format!(
            "auth.jwt.issuer.preset `{preset}` not recognised (valid: gcp / azure / snowflake / databricks / neon / aws-cognito / generic)"
        )));
    }
    if r.expected_audience.is_empty() {
        return Err(ConfigError::validation(format!(
            "auth.jwt.issuer (preset={preset}) expected_audience must not be empty"
        )));
    }

    // Per-preset required-fields check.
    match preset.as_str() {
        "azure" => {
            if r.tenant_id.is_none() {
                return Err(ConfigError::validation(
                    "auth.jwt.issuer (preset=azure) requires tenant_id",
                ));
            }
        }
        "snowflake" => {
            if r.account.is_none() {
                return Err(ConfigError::validation(
                    "auth.jwt.issuer (preset=snowflake) requires account",
                ));
            }
        }
        "databricks" => {
            if r.workspace_url.is_none() {
                return Err(ConfigError::validation(
                    "auth.jwt.issuer (preset=databricks) requires workspace_url",
                ));
            }
        }
        "neon" => {
            if r.project_id.is_none() || r.region.is_none() {
                return Err(ConfigError::validation(
                    "auth.jwt.issuer (preset=neon) requires project_id and region",
                ));
            }
        }
        "aws-cognito" | "aws_cognito" => {
            if r.user_pool_id.is_none() || r.region.is_none() {
                return Err(ConfigError::validation(
                    "auth.jwt.issuer (preset=aws-cognito) requires user_pool_id and region",
                ));
            }
        }
        "generic" => {
            if r.issuer.is_none() || r.jwks_url.is_none() {
                return Err(ConfigError::validation(
                    "auth.jwt.issuer (preset=generic) requires issuer and jwks_url",
                ));
            }
            if r.principal_format.is_none() {
                return Err(ConfigError::validation(
                    "auth.jwt.issuer (preset=generic) requires principal_format",
                ));
            }
        }
        _ => {}
    }

    let principal_format = match r.principal_format {
        None => None,
        Some(rpf) => Some(resolve_principal_format(rpf, &preset)?),
    };

    Ok(JwtIssuerEntry {
        preset,
        tenant_id: r.tenant_id,
        account: r.account,
        workspace_url: r.workspace_url,
        project_id: r.project_id,
        region: r.region,
        user_pool_id: r.user_pool_id,
        issuer: r.issuer,
        jwks_url: r.jwks_url,
        principal_format,
        expected_audience: r.expected_audience,
        safe_to_log_claims: r.safe_to_log_claims,
        clock_skew_secs: r.clock_skew_secs,
        jwks_cache_ttl_secs: r.jwks_cache_ttl_secs,
    })
}

fn resolve_principal_format(
    raw: RawPrincipalFormat,
    preset_name: &str,
) -> Result<PrincipalFormatEntry, ConfigError> {
    match raw {
        RawPrincipalFormat::Shorthand(s) => match s.as_str() {
            "subject" => Ok(PrincipalFormatEntry::Subject {
                prefix: String::new(),
            }),
            "email" => Ok(PrincipalFormatEntry::Email {
                prefix: String::new(),
            }),
            "azure_oid" => Ok(PrincipalFormatEntry::AzureObjectId {
                prefix: String::new(),
            }),
            other => Err(ConfigError::validation(format!(
                "auth.jwt.issuer (preset={preset_name}): principal_format shorthand `{other}` not recognised (valid: subject / email / azure_oid)"
            ))),
        },
        RawPrincipalFormat::Table(t) => {
            if let Some(custom) = t.custom {
                return Ok(PrincipalFormatEntry::Custom { template: custom });
            }
            let kind = t.kind.ok_or_else(|| {
                ConfigError::validation(format!(
                    "auth.jwt.issuer (preset={preset_name}): principal_format table missing `kind` or `custom`"
                ))
            })?;
            let prefix = t.prefix.unwrap_or_default();
            match kind.as_str() {
                "subject" => Ok(PrincipalFormatEntry::Subject { prefix }),
                "email" => Ok(PrincipalFormatEntry::Email { prefix }),
                "azure_oid" => Ok(PrincipalFormatEntry::AzureObjectId { prefix }),
                other => Err(ConfigError::validation(format!(
                    "auth.jwt.issuer (preset={preset_name}): principal_format kind `{other}` not recognised"
                ))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<AuthConfig, ConfigError> {
        let raw: RawAuth = toml::from_str(s).map_err(ConfigError::from)?;
        resolve_auth(Some(raw))
    }

    #[test]
    fn empty_block_defaults() {
        let a = resolve_auth(None).unwrap();
        assert!(a.master_key.is_none());
        assert!(!a.sigv4.enabled);
        assert!(!a.api_token.enabled);
        assert!(!a.permissive.enabled);
        assert!(a.jwt.issuers.is_empty());
        assert!(!a.audit_log_enabled);
    }

    #[test]
    fn sigv4_enabled_requires_master_key() {
        let toml_s = r#"
[sigv4]
enabled = true
"#;
        let err = parse(toml_s).unwrap_err();
        assert!(err.to_string().contains("master_key"), "got {err}");
    }

    #[test]
    fn sigv4_with_env_master_key_resolves() {
        let toml_s = r#"
master_key_env = "REKT_TEST_MK"
[sigv4]
enabled = true
"#;
        let a = parse(toml_s).unwrap();
        assert!(matches!(a.master_key, Some(MasterKey::Env(_))));
        assert!(a.sigv4.enabled);
    }

    #[test]
    fn rejects_multiple_master_key_sources() {
        let toml_s = r#"
master_key_env  = "X"
master_key_file = "/dev/null"
"#;
        let err = parse(toml_s).unwrap_err();
        assert!(
            err.to_string().contains("exactly one"),
            "got: {err}"
        );
    }

    #[test]
    fn jwt_gcp_preset_validates() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "gcp"
expected_audience = ["rektifier"]
"#;
        let a = parse(toml_s).unwrap();
        assert_eq!(a.jwt.issuers.len(), 1);
        assert_eq!(a.jwt.issuers[0].preset, "gcp");
    }

    #[test]
    fn jwt_azure_preset_requires_tenant_id() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "azure"
expected_audience = ["api://rektifier"]
"#;
        let err = parse(toml_s).unwrap_err();
        assert!(err.to_string().contains("tenant_id"), "got {err}");
    }

    #[test]
    fn jwt_snowflake_preset_requires_account() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "snowflake"
expected_audience = ["rektifier"]
"#;
        assert!(parse(toml_s).unwrap_err().to_string().contains("account"));
    }

    #[test]
    fn jwt_databricks_preset_requires_workspace_url() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "databricks"
expected_audience = ["rektifier"]
"#;
        assert!(parse(toml_s)
            .unwrap_err()
            .to_string()
            .contains("workspace_url"));
    }

    #[test]
    fn jwt_neon_preset_requires_project_and_region() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "neon"
expected_audience = ["rektifier"]
"#;
        assert!(parse(toml_s).unwrap_err().to_string().contains("project_id"));
    }

    #[test]
    fn jwt_cognito_preset_requires_pool_and_region() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "aws-cognito"
expected_audience = ["rektifier"]
"#;
        assert!(parse(toml_s).unwrap_err().to_string().contains("user_pool_id"));
    }

    #[test]
    fn jwt_generic_preset_requires_issuer_jwks_principal() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "generic"
expected_audience = ["x"]
"#;
        let err = parse(toml_s).unwrap_err();
        assert!(err.to_string().contains("issuer"), "got {err}");
    }

    #[test]
    fn jwt_generic_full_resolves() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "generic"
issuer = "https://auth.example.com/"
jwks_url = "https://auth.example.com/.well-known/jwks.json"
expected_audience = ["x"]
principal_format = { kind = "subject", prefix = "x:user:" }
"#;
        let a = parse(toml_s).unwrap();
        let p = &a.jwt.issuers[0];
        assert_eq!(p.issuer.as_deref(), Some("https://auth.example.com/"));
        assert!(matches!(
            p.principal_format,
            Some(PrincipalFormatEntry::Subject { ref prefix }) if prefix == "x:user:"
        ));
    }

    #[test]
    fn jwt_generic_custom_template() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "generic"
issuer = "https://x"
jwks_url = "https://x/jwks"
expected_audience = ["a"]
principal_format = { custom = "ext:{sub}" }
"#;
        let a = parse(toml_s).unwrap();
        assert!(matches!(
            a.jwt.issuers[0].principal_format,
            Some(PrincipalFormatEntry::Custom { ref template }) if template == "ext:{sub}"
        ));
    }

    #[test]
    fn jwt_unknown_preset_rejected() {
        let toml_s = r#"
[[jwt.issuer]]
preset = "wat"
expected_audience = ["x"]
"#;
        assert!(parse(toml_s).unwrap_err().to_string().contains("not recognised"));
    }

    #[test]
    fn audit_log_default_off() {
        let a = resolve_auth(None).unwrap();
        assert!(!a.audit_log_enabled);
    }

    #[test]
    fn audit_log_can_be_enabled() {
        let a = parse("audit_log_enabled = true").unwrap();
        assert!(a.audit_log_enabled);
    }
}
