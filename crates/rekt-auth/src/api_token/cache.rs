//! TokenCache + HMAC-pepper helpers (PLAN-13 D6a + D6c).
//!
//! Cache shape mirrors the SigV4 SecretCache: positive + negative TTL,
//! lazy, no background refresh. Key is the 32-byte HMAC hash; lookup
//! is therefore constant-time on the hash bytes by construction
//! (DashMap equality on fixed-size byte arrays).

use crate::crypto::derive_subkey;
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{Duration, Instant};

type HmacSha256 = Hmac<Sha256>;

/// HKDF purpose label so the API-token HMAC pepper is distinct from
/// the SigV4 AES key even when both derive from the same operator-
/// supplied master key.
pub const API_TOKEN_PEPPER_PURPOSE: &[u8] = b"rektifier-api-token-pepper-v1";

/// HMAC-SHA256 of `token` with the pepper subkey. The pepper turns a
/// stolen DB dump into useless ciphertext-on-its-own: without the
/// pepper, an attacker can't precompute hashes.
pub fn hmac_pepper(pepper: &[u8; 32], token: &str) -> [u8; 32] {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(pepper).expect("HMAC accepts any length");
    mac.update(token.as_bytes());
    let out = mac.finalize().into_bytes();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out);
    buf
}

/// Derive the pepper subkey from the operator's master key.
pub fn pepper_from_master(master_key: &[u8; 32]) -> [u8; 32] {
    derive_subkey(master_key, API_TOKEN_PEPPER_PURPOSE)
}

#[derive(Debug, Clone)]
pub struct CachedToken {
    pub principal: String,
    pub expires_at_ms: Option<i64>,
    pub fetched_at: Instant,
}

pub struct TokenCache {
    positive: DashMap<[u8; 32], (CachedToken, Instant)>,
    negative: DashMap<[u8; 32], Instant>,
    ttl: Duration,
    negative_ttl: Duration,
}

impl TokenCache {
    pub fn new(ttl: Duration, negative_ttl: Duration) -> Self {
        Self {
            positive: DashMap::new(),
            negative: DashMap::new(),
            ttl,
            negative_ttl,
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn get(&self, hash: &[u8; 32]) -> Option<Result<CachedToken, ()>> {
        let now = Instant::now();
        if let Some(entry) = self.positive.get(hash) {
            let (token, expires_at) = entry.value();
            if now < *expires_at {
                return Some(Ok(token.clone()));
            }
        }
        if let Some(entry) = self.negative.get(hash) {
            let expires_at = *entry.value();
            if now < expires_at {
                return Some(Err(()));
            }
        }
        None
    }

    pub fn insert_positive(&self, hash: [u8; 32], token: CachedToken) {
        let expires_at = token.fetched_at + self.ttl;
        self.negative.remove(&hash);
        self.positive.insert(hash, (token, expires_at));
    }

    pub fn insert_negative(&self, hash: [u8; 32]) {
        let expires_at = Instant::now() + self.negative_ttl;
        self.negative.insert(hash, expires_at);
    }

    pub fn invalidate(&self, hash: &[u8; 32]) {
        self.positive.remove(hash);
        self.negative.remove(hash);
    }

    pub fn len(&self) -> (usize, usize) {
        (self.positive.len(), self.negative.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pepper_is_deterministic() {
        let master = [3u8; 32];
        let p1 = pepper_from_master(&master);
        let p2 = pepper_from_master(&master);
        assert_eq!(p1, p2);
    }

    #[test]
    fn pepper_differs_from_sigv4_aes_key() {
        let master = [3u8; 32];
        let sigv4 = crate::crypto::derive_subkey(&master, b"rektifier-sigv4-aes-v1");
        let pepper = pepper_from_master(&master);
        assert_ne!(sigv4, pepper, "pepper and SigV4 AES key must be different");
    }

    #[test]
    fn hmac_pepper_changes_with_token() {
        let p = [9u8; 32];
        let h1 = hmac_pepper(&p, "rekt_pat_one");
        let h2 = hmac_pepper(&p, "rekt_pat_two");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hmac_pepper_changes_with_pepper() {
        let h1 = hmac_pepper(&[1u8; 32], "rekt_pat_x");
        let h2 = hmac_pepper(&[2u8; 32], "rekt_pat_x");
        assert_ne!(h1, h2);
    }

    fn cached(principal: &str) -> CachedToken {
        CachedToken {
            principal: principal.into(),
            expires_at_ms: None,
            fetched_at: Instant::now(),
        }
    }

    #[test]
    fn positive_hit_returns_token() {
        let c = TokenCache::new(Duration::from_secs(300), Duration::from_secs(30));
        let h = [9u8; 32];
        c.insert_positive(h, cached("p"));
        let got = c.get(&h).unwrap().unwrap();
        assert_eq!(got.principal, "p");
    }

    #[test]
    fn negative_hit_returns_err() {
        let c = TokenCache::new(Duration::from_secs(300), Duration::from_secs(30));
        let h = [9u8; 32];
        c.insert_negative(h);
        assert!(matches!(c.get(&h), Some(Err(()))));
    }

    #[test]
    fn positive_evicts_negative() {
        let c = TokenCache::new(Duration::from_secs(300), Duration::from_secs(30));
        let h = [9u8; 32];
        c.insert_negative(h);
        c.insert_positive(h, cached("p"));
        assert!(c.get(&h).unwrap().is_ok());
    }

    #[test]
    fn expired_entries_return_none() {
        let c = TokenCache::new(Duration::from_millis(1), Duration::from_millis(1));
        let h = [9u8; 32];
        c.insert_positive(h, cached("p"));
        std::thread::sleep(Duration::from_millis(5));
        assert!(c.get(&h).is_none());
    }
}
