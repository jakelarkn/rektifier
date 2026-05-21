//! In-memory cache for decrypted SigV4 secrets (PLAN-13 D5).
//!
//! Hit path: HMAC-SHA256 verify against the cached secret (~10 µs).
//! Miss path: PG SELECT + AES-GCM decrypt (~1 ms).
//! Negative cache: short TTL for "AKID not found / disabled" so a
//! token-scanning storm doesn't hammer PG.

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct CachedSecret {
    pub secret: Arc<String>,
    pub principal: String,
    pub fetched_at: Instant,
}

pub struct SecretCache {
    positive: DashMap<String, (CachedSecret, Instant)>,
    negative: DashMap<String, Instant>,
    ttl: Duration,
    negative_ttl: Duration,
}

impl SecretCache {
    pub fn new(ttl: Duration, negative_ttl: Duration) -> Self {
        Self {
            positive: DashMap::new(),
            negative: DashMap::new(),
            ttl,
            negative_ttl,
        }
    }

    /// Look up a cached entry. Returns:
    /// - `Some(Ok(secret))` for a fresh positive hit
    /// - `Some(Err(()))` for a fresh negative hit
    /// - `None` if no fresh entry exists (caller must look up + populate)
    #[allow(clippy::result_unit_err)]
    pub fn get(&self, akid: &str) -> Option<Result<CachedSecret, ()>> {
        let now = Instant::now();
        if let Some(entry) = self.positive.get(akid) {
            let (secret, expires_at) = entry.value();
            if now < *expires_at {
                return Some(Ok(secret.clone()));
            }
        }
        if let Some(entry) = self.negative.get(akid) {
            let expires_at = *entry.value();
            if now < expires_at {
                return Some(Err(()));
            }
        }
        None
    }

    pub fn insert_positive(&self, akid: String, secret: CachedSecret) {
        let expires_at = secret.fetched_at + self.ttl;
        // Drop any negative entry — positive must take precedence.
        self.negative.remove(&akid);
        self.positive.insert(akid, (secret, expires_at));
    }

    pub fn insert_negative(&self, akid: String) {
        let expires_at = Instant::now() + self.negative_ttl;
        self.negative.insert(akid, expires_at);
    }

    pub fn invalidate(&self, akid: &str) {
        self.positive.remove(akid);
        self.negative.remove(akid);
    }

    pub fn len(&self) -> (usize, usize) {
        (self.positive.len(), self.negative.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cached(secret: &str, principal: &str) -> CachedSecret {
        CachedSecret {
            secret: Arc::new(secret.to_string()),
            principal: principal.to_string(),
            fetched_at: Instant::now(),
        }
    }

    #[test]
    fn empty_cache_returns_none() {
        let c = SecretCache::new(Duration::from_secs(300), Duration::from_secs(30));
        assert!(c.get("AKIA").is_none());
    }

    #[test]
    fn positive_hit_returns_secret() {
        let c = SecretCache::new(Duration::from_secs(300), Duration::from_secs(30));
        c.insert_positive("AKIA".into(), cached("s3cret", "aws:akid:AKIA"));
        let got = c.get("AKIA").unwrap().unwrap();
        assert_eq!(&*got.secret, "s3cret");
        assert_eq!(got.principal, "aws:akid:AKIA");
    }

    #[test]
    fn negative_hit_returns_err() {
        let c = SecretCache::new(Duration::from_secs(300), Duration::from_secs(30));
        c.insert_negative("AKIANOTREAL".into());
        assert!(matches!(c.get("AKIANOTREAL"), Some(Err(()))));
    }

    #[test]
    fn positive_evicts_negative() {
        let c = SecretCache::new(Duration::from_secs(300), Duration::from_secs(30));
        c.insert_negative("AKIA".into());
        c.insert_positive("AKIA".into(), cached("s", "aws:akid:AKIA"));
        let got = c.get("AKIA").unwrap();
        assert!(got.is_ok(), "positive must take precedence over negative");
    }

    #[test]
    fn expired_positive_returns_none() {
        let c = SecretCache::new(Duration::from_millis(1), Duration::from_secs(30));
        c.insert_positive("AKIA".into(), cached("s", "p"));
        std::thread::sleep(Duration::from_millis(5));
        assert!(c.get("AKIA").is_none(), "expired positive must be a miss");
    }

    #[test]
    fn expired_negative_returns_none() {
        let c = SecretCache::new(Duration::from_secs(300), Duration::from_millis(1));
        c.insert_negative("AKIA".into());
        std::thread::sleep(Duration::from_millis(5));
        assert!(c.get("AKIA").is_none(), "expired negative must be a miss");
    }

    #[test]
    fn invalidate_drops_both_slots() {
        let c = SecretCache::new(Duration::from_secs(300), Duration::from_secs(30));
        c.insert_positive("AKIA".into(), cached("s", "p"));
        c.invalidate("AKIA");
        assert!(c.get("AKIA").is_none());
    }
}
