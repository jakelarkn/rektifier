//! AES-GCM encryption + HKDF key derivation for credential material.
//!
//! Two shared helpers used by the A2 SigV4 credential store and the A5
//! API-token store:
//!
//! - [`encrypt`] / [`decrypt`] wrap a 32-byte AES-GCM-256 key with a
//!   random 12-byte nonce. Output format: `nonce || ciphertext || tag`,
//!   stored split into `secret_aes_enc bytea` (ciphertext+tag) and
//!   `secret_nonce bytea` (nonce) columns so a future operator can
//!   inspect the layout without parsing.
//! - [`derive_subkey`] uses HKDF-SHA256 to split a single 32-byte master
//!   key into purpose-specific subkeys (e.g. "rektifier-sigv4-aes" vs
//!   "rektifier-api-token-pepper") so the SigV4 credential encryption
//!   and the API-token HMAC pepper never share key material.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use hkdf::Hkdf;
use rand::RngCore;
use sha2::Sha256;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("AES-GCM encryption failed")]
    EncryptFailed,
    #[error("AES-GCM decryption failed (ciphertext tampered or wrong key)")]
    DecryptFailed,
    #[error("invalid key length: expected 32 bytes, got {0}")]
    InvalidKeyLen(usize),
}

/// AES-GCM ciphertext components. `nonce` is 12 bytes; `ciphertext`
/// includes the 16-byte GCM tag appended by the AEAD.
#[derive(Debug, Clone)]
pub struct Ciphertext {
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
}

pub fn encrypt(key: &[u8; 32], plaintext: &[u8]) -> Result<Ciphertext, CryptoError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce);
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| CryptoError::EncryptFailed)?;
    Ok(Ciphertext {
        nonce,
        ciphertext: ct,
    })
}

pub fn decrypt(key: &[u8; 32], ct: &Ciphertext) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher
        .decrypt(Nonce::from_slice(&ct.nonce), ct.ciphertext.as_slice())
        .map_err(|_| CryptoError::DecryptFailed)
}

/// HKDF-SHA256 derive a 32-byte subkey for a named purpose. Sharing the
/// master key across purposes is safe only if each purpose pulls a
/// distinct subkey through HKDF.
pub fn derive_subkey(master: &[u8; 32], purpose: &[u8]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(None, master);
    let mut out = [0u8; 32];
    hk.expand(purpose, &mut out)
        .expect("HKDF expand 32 bytes — infallible for Sha256");
    out
}

/// Parse a 32-byte master key from a string. Accepts:
/// - 64 hex characters
/// - 44 chars base64 (standard or URL-safe, with or without `=` padding)
/// - exactly 32 raw bytes (treated as utf-8 unrecognized; reject unless
///   hex/base64 succeeds)
pub fn parse_master_key(s: &str) -> Result<[u8; 32], CryptoError> {
    let trimmed = s.trim();
    if trimmed.len() == 64 {
        if let Ok(decoded) = hex::decode(trimmed) {
            if decoded.len() == 32 {
                let mut out = [0u8; 32];
                out.copy_from_slice(&decoded);
                return Ok(out);
            }
        }
    }
    use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
    use base64::Engine;
    if let Ok(decoded) = STANDARD.decode(trimmed.as_bytes()) {
        if decoded.len() == 32 {
            let mut out = [0u8; 32];
            out.copy_from_slice(&decoded);
            return Ok(out);
        }
    }
    if let Ok(decoded) = URL_SAFE.decode(trimmed.as_bytes()) {
        if decoded.len() == 32 {
            let mut out = [0u8; 32];
            out.copy_from_slice(&decoded);
            return Ok(out);
        }
    }
    if let Ok(decoded) = URL_SAFE_NO_PAD.decode(trimmed.as_bytes()) {
        if decoded.len() == 32 {
            let mut out = [0u8; 32];
            out.copy_from_slice(&decoded);
            return Ok(out);
        }
    }
    Err(CryptoError::InvalidKeyLen(trimmed.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = [42u8; 32];
        let pt = b"hunter2";
        let ct = encrypt(&key, pt).unwrap();
        let out = decrypt(&key, &ct).unwrap();
        assert_eq!(out, pt);
    }

    #[test]
    fn decrypt_with_wrong_key_fails() {
        let key = [42u8; 32];
        let other = [43u8; 32];
        let ct = encrypt(&key, b"hunter2").unwrap();
        assert!(matches!(decrypt(&other, &ct), Err(CryptoError::DecryptFailed)));
    }

    #[test]
    fn decrypt_with_tampered_ciphertext_fails() {
        let key = [42u8; 32];
        let mut ct = encrypt(&key, b"hunter2").unwrap();
        ct.ciphertext[0] ^= 1;
        assert!(matches!(decrypt(&key, &ct), Err(CryptoError::DecryptFailed)));
    }

    #[test]
    fn nonce_is_random() {
        let key = [42u8; 32];
        let ct1 = encrypt(&key, b"x").unwrap();
        let ct2 = encrypt(&key, b"x").unwrap();
        // Probability of collision is 2^-96; treat as "always different."
        assert_ne!(ct1.nonce, ct2.nonce);
    }

    #[test]
    fn hkdf_subkey_is_deterministic_per_purpose() {
        let master = [1u8; 32];
        let a1 = derive_subkey(&master, b"purpose-a");
        let a2 = derive_subkey(&master, b"purpose-a");
        assert_eq!(a1, a2, "same purpose must yield same subkey");
    }

    #[test]
    fn hkdf_subkey_differs_across_purposes() {
        let master = [1u8; 32];
        let a = derive_subkey(&master, b"purpose-a");
        let b = derive_subkey(&master, b"purpose-b");
        assert_ne!(a, b, "different purposes must yield different subkeys");
    }

    #[test]
    fn parse_master_key_hex() {
        let hex_str = "0".repeat(64);
        let k = parse_master_key(&hex_str).unwrap();
        assert_eq!(k, [0u8; 32]);
    }

    #[test]
    fn parse_master_key_base64() {
        // 32 zero bytes -> base64 standard
        let b64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let k = parse_master_key(b64).unwrap();
        assert_eq!(k, [0u8; 32]);
    }

    #[test]
    fn parse_master_key_rejects_short() {
        assert!(parse_master_key("deadbeef").is_err());
    }
}
