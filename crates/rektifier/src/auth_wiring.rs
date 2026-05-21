//! Build the `AuthChain` from `rekt_config::AuthConfig`.
//!
//! Resolves the master key, ensures the credential + token tables
//! exist (PLAN-13 D14), constructs each enabled verifier, warms up
//! JWKS endpoints (D4a), wires everything into `AuthChain`. Also
//! enforces the D10 dev-mode guardrails: anonymous + non-loopback
//! is a startup error, and any unsafe mode requires
//! `REKTIFIER_ALLOW_UNSAFE_AUTH=I_UNDERSTAND_THIS_IS_UNSAFE`.

use anyhow::{anyhow, bail, Context, Result};
use deadpool_postgres::Pool;
use rekt_auth::api_token::cache::pepper_from_master;
use rekt_auth::jwt::issuer::PrincipalFormat as JwtPrincipalFormat;
use rekt_auth::jwt::presets as jwt_presets;
use rekt_auth::sigv4::verifier::SIGV4_AES_PURPOSE;
use rekt_auth::{
    ensure_api_tokens_table, ensure_credentials_table, ApiTokenVerifier, AuthChain,
    JwtIssuerConfig, JwtVerifier, MasterKeySource, PermissiveVerifier, SecretCache, Sigv4Verifier,
    TokenCache,
};
use rekt_config::{
    AuthConfig, JwtIssuerEntry, MasterKey, PrincipalFormatEntry, Sigv4Config,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub struct BuiltAuth {
    pub chain: Arc<AuthChain>,
    pub audit_enabled: bool,
}

pub async fn build_auth(
    pool: &Pool,
    listen_addr: SocketAddr,
    cfg: &AuthConfig,
) -> Result<BuiltAuth> {
    let any_strict_enabled = cfg.sigv4.enabled || cfg.api_token.enabled || !cfg.jwt.issuers.is_empty();
    let permissive_only = cfg.permissive.enabled && !any_strict_enabled;
    let unsafe_modes_active = cfg.permissive.enabled;

    // D10 guardrails — applied before we wire anything else, so a
    // misconfigured deployment fails startup with a loud message.
    enforce_dev_mode_guardrails(listen_addr, permissive_only, unsafe_modes_active)?;

    let master_key = match (&cfg.master_key, cfg.sigv4.enabled || cfg.api_token.enabled) {
        (Some(src), _) => Some(resolve_master_key(src)?),
        (None, false) => None,
        (None, true) => {
            // The config-level validator caught this already, but
            // keep the runtime check belt-and-suspenders.
            bail!("auth.sigv4 / auth.api_token enabled but no auth.master_key_* set");
        }
    };

    let mut chain = AuthChain::empty();

    if cfg.sigv4.enabled {
        let mk = master_key.as_ref().expect("master key present");
        let sig = build_sigv4(pool, mk, &cfg.sigv4).await?;
        chain = chain.with_sigv4(Box::new(sig));
    }

    if cfg.api_token.enabled {
        let mk = master_key.as_ref().expect("master key present");
        let api = build_api_token(pool, mk, &cfg.api_token).await?;
        chain = chain.with_api_token(Box::new(api));
    }

    if !cfg.jwt.issuers.is_empty() {
        let jwt = build_jwt(&cfg.jwt.issuers).await?;
        chain = chain.with_jwt(Box::new(jwt));
    }

    if cfg.permissive.enabled {
        chain = chain.with_permissive(Box::new(PermissiveVerifier));
        tracing::warn!(
            "auth.permissive.enabled = true — PermissiveVerifier registered. \
             Do NOT enable this in production."
        );
    }

    // Final sanity: at least one verifier must be registered.
    if chain.sigv4.is_none() && chain.jwt.is_none() && chain.api_token.is_none() && chain.permissive.is_none() {
        bail!(
            "auth: no verifier configured. Enable at least one of \
             [auth.sigv4] / [auth.api_token] / [[auth.jwt.issuer]] / [auth.permissive]"
        );
    }

    Ok(BuiltAuth {
        chain: Arc::new(chain),
        audit_enabled: cfg.audit_log_enabled,
    })
}

fn enforce_dev_mode_guardrails(
    listen_addr: SocketAddr,
    permissive_only: bool,
    unsafe_modes_active: bool,
) -> Result<()> {
    let is_loopback = listen_addr.ip().is_loopback();

    if permissive_only && !is_loopback {
        bail!(
            "auth: only [auth.permissive] is enabled but listen_addr {} is not loopback. \
             Permissive mode accepts every request without validating signatures — refusing to \
             bind to a non-loopback interface.",
            listen_addr
        );
    }

    if unsafe_modes_active {
        const SENTINEL: &str = "I_UNDERSTAND_THIS_IS_UNSAFE";
        match std::env::var("REKTIFIER_ALLOW_UNSAFE_AUTH") {
            Ok(v) if v == SENTINEL => {
                tracing::warn!(
                    "REKTIFIER_ALLOW_UNSAFE_AUTH override accepted. Unsafe auth modes are \
                     enabled; do NOT expose this rektifier instance to untrusted networks."
                );
            }
            Ok(other) => {
                bail!(
                    "auth: unsafe modes enabled but REKTIFIER_ALLOW_UNSAFE_AUTH={other:?} \
                     is not the expected sentinel `{SENTINEL}`."
                );
            }
            Err(_) => {
                bail!(
                    "auth: [auth.permissive] is enabled but REKTIFIER_ALLOW_UNSAFE_AUTH is \
                     not set. Set REKTIFIER_ALLOW_UNSAFE_AUTH=`{SENTINEL}` to acknowledge \
                     the risk, or disable [auth.permissive]."
                );
            }
        }
    }

    Ok(())
}

fn resolve_master_key(src: &MasterKey) -> Result<[u8; 32]> {
    let source = match src {
        MasterKey::Env(name) => MasterKeySource::Env(name.clone()),
        MasterKey::File(p) => MasterKeySource::File(p.clone()),
        MasterKey::Kms(uri) => {
            // A2a deferred — provider trait + per-cloud SDKs not yet
            // implemented. Bail loudly so the operator sees this is
            // not the working path.
            bail!(
                "auth.master_key_kms = {uri:?} requires the A2a KMS provider phase, which is \
                 not implemented yet. Use master_key_env or master_key_file."
            );
        }
    };
    source.load().context("loading master key")
}

async fn build_sigv4(
    pool: &Pool,
    master_key: &[u8; 32],
    cfg: &Sigv4Config,
) -> Result<Sigv4Verifier> {
    ensure_credentials_table(pool)
        .await
        .context("ensuring _rektifier_aws_credentials")?;
    let cache = Arc::new(SecretCache::new(
        Duration::from_secs(cfg.cache_ttl_secs),
        Duration::from_secs(cfg.negative_cache_ttl_secs),
    ));
    // The Sigv4Verifier derives the AES subkey itself; we hand it
    // the raw master key and a fresh cache.
    let v = Sigv4Verifier::new(pool.clone(), master_key, cache).with_clock_skew_secs(cfg.clock_skew_secs);
    let _aes_purpose_marker = SIGV4_AES_PURPOSE; // keep the link visible
    tracing::info!(
        clock_skew_secs = cfg.clock_skew_secs,
        cache_ttl_secs = cfg.cache_ttl_secs,
        "auth.sigv4 enabled"
    );
    Ok(v)
}

async fn build_api_token(
    pool: &Pool,
    master_key: &[u8; 32],
    cfg: &rekt_config::ApiTokenConfig,
) -> Result<ApiTokenVerifier> {
    ensure_api_tokens_table(pool)
        .await
        .context("ensuring _rektifier_api_tokens")?;
    let pepper = pepper_from_master(master_key);
    let cache = Arc::new(TokenCache::new(
        Duration::from_secs(cfg.cache_ttl_secs),
        Duration::from_secs(cfg.negative_cache_ttl_secs),
    ));
    let v = ApiTokenVerifier::new(pool.clone(), pepper, cache)
        .with_test_tokens_enabled(cfg.accept_test_tokens);
    tracing::info!(
        accept_test_tokens = cfg.accept_test_tokens,
        cache_ttl_secs = cfg.cache_ttl_secs,
        "auth.api_token enabled"
    );
    Ok(v)
}

async fn build_jwt(entries: &[JwtIssuerEntry]) -> Result<JwtVerifier> {
    let mut verifier = JwtVerifier::new();
    for entry in entries {
        let cfg = inflate_issuer(entry)?;
        verifier
            .add_issuer(cfg)
            .map_err(|e| anyhow!("auth.jwt.issuer (preset={}): {}", entry.preset, e))?;
        tracing::info!(
            preset = %entry.preset,
            audience = ?entry.expected_audience,
            "auth.jwt.issuer registered"
        );
    }
    // D4a boot-time warm-up — fails startup if any JWKS is
    // unreachable. The operator sees misconfiguration at deploy time.
    verifier
        .warm_up()
        .await
        .map_err(|e| anyhow!("auth.jwt boot warm-up: {}", e))?;
    Ok(verifier)
}

fn inflate_issuer(entry: &JwtIssuerEntry) -> Result<JwtIssuerConfig> {
    let audience = first_audience(entry)?;
    let mut cfg = match entry.preset.as_str() {
        "gcp" => jwt_presets::gcp(audience),
        "azure" => jwt_presets::azure(
            entry
                .tenant_id
                .as_deref()
                .expect("config validator enforces tenant_id"),
            audience,
        ),
        "snowflake" => jwt_presets::snowflake(
            entry.account.as_deref().expect("validator enforces account"),
            audience,
        ),
        "databricks" => jwt_presets::databricks(
            entry
                .workspace_url
                .as_deref()
                .expect("validator enforces workspace_url"),
            audience,
        ),
        "neon" => jwt_presets::neon(
            entry.project_id.as_deref().expect("validator"),
            entry.region.as_deref().expect("validator"),
            audience,
        ),
        "aws-cognito" | "aws_cognito" => jwt_presets::aws_cognito(
            entry.user_pool_id.as_deref().expect("validator"),
            entry.region.as_deref().expect("validator"),
            audience,
        ),
        "generic" => jwt_presets::generic(
            entry.issuer.clone().expect("validator"),
            entry.jwks_url.clone().expect("validator"),
            audience,
            convert_principal_format(
                entry
                    .principal_format
                    .as_ref()
                    .expect("validator enforces principal_format"),
            ),
        ),
        other => bail!("unknown jwt preset: {other}"),
    };

    // Apply overrides on top of preset defaults.
    cfg.expected_audience = entry.expected_audience.clone();
    if let Some(skew) = entry.clock_skew_secs {
        cfg.clock_skew = Duration::from_secs(skew);
    }
    if let Some(ttl) = entry.jwks_cache_ttl_secs {
        cfg.jwks_cache_ttl = Duration::from_secs(ttl);
    }
    if let Some(allow) = entry.safe_to_log_claims.clone() {
        cfg.safe_to_log_claims = allow;
    }
    if let Some(fmt) = entry.principal_format.as_ref() {
        // For non-generic presets, allow operator override of the
        // principal_format while keeping the preset's issuer/jwks.
        if entry.preset != "generic" {
            cfg.principal_format = convert_principal_format(fmt);
        }
    }
    Ok(cfg)
}

fn first_audience(entry: &JwtIssuerEntry) -> Result<String> {
    entry
        .expected_audience
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("expected_audience must not be empty"))
}

fn convert_principal_format(fmt: &PrincipalFormatEntry) -> JwtPrincipalFormat {
    match fmt {
        PrincipalFormatEntry::Subject { prefix } => JwtPrincipalFormat::Subject(prefix.clone()),
        PrincipalFormatEntry::Email { prefix } => JwtPrincipalFormat::Email(prefix.clone()),
        PrincipalFormatEntry::AzureObjectId { prefix } => {
            JwtPrincipalFormat::AzureObjectId(prefix.clone())
        }
        PrincipalFormatEntry::Custom { template } => JwtPrincipalFormat::Custom(template.clone()),
    }
}

