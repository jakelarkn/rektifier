//! PLAN-13 A5 PG-live verification. Run with:
//!   cargo test -p rekt-auth --test api_token_pg -- --ignored
//! after `just up`.

use deadpool_postgres::{Config, Runtime};
use rekt_auth::api_token::cache::{hmac_pepper, pepper_from_master};
use rekt_auth::{
    ensure_api_tokens_table, insert_token_hash, mint_api_token, ApiTokenVerifier, AuthScheme,
    TokenCache, TokenKind, Verifier,
};
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
}

async fn build_pool() -> deadpool_postgres::Pool {
    let mut cfg = Config::new();
    cfg.url = Some(database_url());
    cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool")
}

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

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn api_token_pattern3_end_to_end_via_pg() {
    let pool = build_pool().await;
    ensure_api_tokens_table(&pool).await.expect("ensure table");

    let master_key = [42u8; 32];
    let pepper = pepper_from_master(&master_key);

    // Mint a fresh token + insert its hash + principal.
    let token = mint_api_token(TokenKind::Svc);
    let hash = hmac_pepper(&pepper, &token);
    let principal = "neon:key:integration-test".to_string();

    // Cleanup any prior row.
    {
        let client = pool.get().await.expect("client");
        let hash_slice: &[u8] = &hash;
        client
            .execute(
                "DELETE FROM _rektifier_api_tokens WHERE token_hash = $1",
                &[&hash_slice],
            )
            .await
            .expect("cleanup");
    }

    insert_token_hash(&pool, &hash, &principal, None)
        .await
        .expect("insert");

    let cache = Arc::new(TokenCache::new(
        Duration::from_secs(300),
        Duration::from_secs(30),
    ));
    let verifier = ApiTokenVerifier::new(pool.clone(), pepper, cache.clone());

    // Cold path: cache miss → PG SELECT → populate.
    let parts = parts_with_bearer(&token);
    let id = verifier.verify(&parts, b"").await.expect("first verify");
    assert_eq!(id.scheme, AuthScheme::ApiToken);
    assert_eq!(id.principal, principal);
    let (pos, _neg) = cache.len();
    assert_eq!(pos, 1, "cache populated after first verify");

    // Hot path: cache hit, no PG roundtrip.
    let parts2 = parts_with_bearer(&token);
    let id2 = verifier.verify(&parts2, b"").await.expect("second verify");
    assert_eq!(id2.principal, principal);

    // Unknown token → negative cache + IdentityNotFound.
    let bogus = mint_api_token(TokenKind::Pat);
    let parts3 = parts_with_bearer(&bogus);
    let err = verifier.verify(&parts3, b"").await.unwrap_err();
    assert!(
        matches!(err, rekt_auth::AuthError::IdentityNotFound),
        "unknown token must IdentityNotFound; got {err:?}"
    );
    let (_pos, neg) = cache.len();
    assert!(neg >= 1, "negative cache populated for unknown token");

    // Cleanup.
    let client = pool.get().await.expect("client");
    let hash_slice: &[u8] = &hash;
    client
        .execute(
            "DELETE FROM _rektifier_api_tokens WHERE token_hash = $1",
            &[&hash_slice],
        )
        .await
        .expect("cleanup");
}
