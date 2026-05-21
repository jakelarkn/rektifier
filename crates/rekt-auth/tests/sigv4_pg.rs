//! PLAN-13 A2 PG-live verification. Run with:
//!   cargo test -p rekt-auth --test sigv4_pg -- --ignored
//! after `just up`.
//!
//! Verifies the full credential-lookup path: insert AES-GCM encrypted
//! row → Sigv4Verifier signs canonical request → cache miss decrypts
//! row → cache hit on second request → signature mismatch rejected.

use deadpool_postgres::{Config, Runtime};
use rekt_auth::crypto::derive_subkey;
use rekt_auth::sigv4::canonical::{compute_signature, hex_sha256, signing_key, string_to_sign};
use rekt_auth::sigv4::verifier::SIGV4_AES_PURPOSE;
use rekt_auth::sigv4::{
    canonical::canonical_request, cache::SecretCache, ensure_credentials_table,
    insert_credential_row, CredentialRow, Sigv4Verifier,
};
use rekt_auth::{AuthScheme, Verifier};
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

fn build_signed_request(
    secret: &str,
    akid: &str,
    region: &str,
    service: &str,
    body: &[u8],
) -> http::request::Parts {
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let signed_headers = "host;x-amz-date";
    let payload_hash = hex_sha256(body);
    let mut headers = http::HeaderMap::new();
    headers.insert("host", "rektifier.local".parse().unwrap());
    headers.insert("x-amz-date", amz_date.parse().unwrap());
    let canonical = canonical_request("POST", "/", "", &headers, signed_headers, &payload_hash);
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let sts = string_to_sign(&amz_date, &scope, &canonical);
    let sk = signing_key(secret, &date, region, service);
    let sig = compute_signature(&sk, &sts);
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={akid}/{date}/{region}/{service}/aws4_request, \
         SignedHeaders={signed_headers}, Signature={sig}"
    );
    let mut builder = http::Request::builder().method("POST").uri("/");
    builder = builder.header("host", "rektifier.local");
    builder = builder.header("x-amz-date", &amz_date);
    builder = builder.header("authorization", &auth);
    builder.body(()).unwrap().into_parts().0
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn strict_sigv4_verifies_real_signature_via_pg() {
    let pool = build_pool().await;
    ensure_credentials_table(&pool).await.expect("ensure table");

    let akid = "AKIATESTPLAN13";
    let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    let principal = "aws:akid:AKIATESTPLAN13".to_string();
    let master_key = [9u8; 32];

    // Clear any prior row from a previous run.
    {
        let client = pool.get().await.expect("client");
        client
            .execute(
                "DELETE FROM _rektifier_aws_credentials WHERE access_key_id = $1",
                &[&akid],
            )
            .await
            .expect("cleanup");
    }

    // Caller derives the SigV4 AES subkey from the master key; the
    // verifier does the same derivation in `new()`, so encryption and
    // decryption use the same key material without sharing it with
    // the A5 API-token pepper.
    let aes_key = derive_subkey(&master_key, SIGV4_AES_PURPOSE);
    insert_credential_row(
        &pool,
        &aes_key,
        &CredentialRow {
            access_key_id: akid.into(),
            secret_access_key: secret.into(),
            principal: principal.clone(),
            enabled: true,
        },
    )
    .await
    .expect("insert");

    let cache = Arc::new(SecretCache::new(
        Duration::from_secs(300),
        Duration::from_secs(30),
    ));
    let verifier = Sigv4Verifier::new(pool.clone(), &master_key, cache.clone());

    let body = b"{\"TableName\":\"t\"}";
    let parts = build_signed_request(secret, akid, "us-east-1", "dynamodb", body);

    // Cold path: cache miss → PG fetch → AES-GCM decrypt → HMAC verify.
    let id = verifier.verify(&parts, body).await.expect("first verify");
    assert_eq!(id.scheme, AuthScheme::Sigv4);
    assert_eq!(id.principal, principal);
    let (pos, neg) = cache.len();
    assert_eq!(pos, 1, "positive cache populated after first verify");
    assert_eq!(neg, 0);

    // Hot path: cache hit, no PG roundtrip.
    let parts2 = build_signed_request(secret, akid, "us-east-1", "dynamodb", body);
    let id2 = verifier.verify(&parts2, body).await.expect("second verify");
    assert_eq!(id2.principal, principal);

    // Mismatch: same AKID, wrong signature → SignatureFailed.
    let mut bad_parts = build_signed_request(secret, akid, "us-east-1", "dynamodb", body);
    let bad = bad_parts
        .headers
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .replace("Signature=", "Signature=0000");
    bad_parts.headers.insert("authorization", bad.parse().unwrap());
    let err = verifier.verify(&bad_parts, body).await.unwrap_err();
    assert!(
        matches!(err, rekt_auth::AuthError::SignatureFailed(_)),
        "wrong signature must SignatureFailed; got {err:?}"
    );

    // Unknown AKID → negative cache + IdentityNotFound.
    let unknown_parts = build_signed_request(secret, "AKIANEVERHEARDOF", "us-east-1", "dynamodb", body);
    let err = verifier.verify(&unknown_parts, body).await.unwrap_err();
    assert!(
        matches!(err, rekt_auth::AuthError::IdentityNotFound),
        "unknown AKID must IdentityNotFound; got {err:?}"
    );
    let (_pos, neg) = cache.len();
    assert!(neg >= 1, "negative cache populated for unknown AKID");

    // Cleanup.
    let client = pool.get().await.expect("client");
    client
        .execute(
            "DELETE FROM _rektifier_aws_credentials WHERE access_key_id = $1",
            &[&akid],
        )
        .await
        .expect("cleanup");
}
