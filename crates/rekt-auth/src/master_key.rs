//! Master-key loading for the AES-GCM credential encryption + HMAC
//! pepper. See PLAN-13 D5: three pluggable sources, exactly one must
//! be configured when SigV4 strict (A2) or opaque API tokens (A5) are
//! enabled.
//!
//! `master_key_env`  — read from a named environment variable (32 raw
//!                     bytes after hex/base64 decode).
//! `master_key_file` — read from a file path (32 raw bytes after
//!                     hex/base64 decode, or 32 raw bytes literal).
//! `master_key_kms`  — fetch a wrapped DEK from a KMS provider and
//!                     unwrap once at boot (D5a). Phase A2a.

use crate::crypto::{parse_master_key, CryptoError};

#[derive(Debug, thiserror::Error)]
pub enum MasterKeyError {
    #[error("environment variable {0} not set")]
    EnvMissing(String),
    #[error("environment variable {0} value invalid: {1}")]
    EnvInvalid(String, CryptoError),
    #[error("master_key_file {0} not readable: {1}")]
    FileRead(String, std::io::Error),
    #[error("master_key_file {0} contents invalid: {1}")]
    FileInvalid(String, CryptoError),
    #[error("master_key source not configured")]
    NotConfigured,
}

#[derive(Debug, Clone)]
pub enum MasterKeySource {
    Env(String),
    File(std::path::PathBuf),
    // Kms(...) lands in A2a.
}

impl MasterKeySource {
    pub fn load(&self) -> Result<[u8; 32], MasterKeyError> {
        match self {
            Self::Env(name) => {
                let raw = std::env::var(name).map_err(|_| MasterKeyError::EnvMissing(name.clone()))?;
                parse_master_key(&raw).map_err(|e| MasterKeyError::EnvInvalid(name.clone(), e))
            }
            Self::File(path) => {
                let contents = std::fs::read(path)
                    .map_err(|e| MasterKeyError::FileRead(path.display().to_string(), e))?;
                // Accept either an ASCII-encoded form (hex/base64, with
                // optional trailing whitespace from `cat secret.txt`) or
                // a raw 32-byte file.
                if contents.len() == 32 {
                    let mut out = [0u8; 32];
                    out.copy_from_slice(&contents);
                    return Ok(out);
                }
                let as_text = std::str::from_utf8(&contents).map_err(|_| {
                    MasterKeyError::FileInvalid(
                        path.display().to_string(),
                        CryptoError::InvalidKeyLen(contents.len()),
                    )
                })?;
                parse_master_key(as_text)
                    .map_err(|e| MasterKeyError::FileInvalid(path.display().to_string(), e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_source_decodes_hex() {
        let varname = "REKT_AUTH_TEST_MK_HEX";
        std::env::set_var(varname, "0".repeat(64));
        let k = MasterKeySource::Env(varname.into()).load().unwrap();
        assert_eq!(k, [0u8; 32]);
        std::env::remove_var(varname);
    }

    #[test]
    fn env_source_missing_returns_envmissing() {
        let varname = "REKT_AUTH_TEST_MK_MISSING";
        std::env::remove_var(varname);
        let err = MasterKeySource::Env(varname.into()).load().unwrap_err();
        assert!(matches!(err, MasterKeyError::EnvMissing(_)));
    }

    #[test]
    fn env_source_invalid_returns_envinvalid() {
        let varname = "REKT_AUTH_TEST_MK_INVALID";
        std::env::set_var(varname, "not-a-key");
        let err = MasterKeySource::Env(varname.into()).load().unwrap_err();
        assert!(matches!(err, MasterKeyError::EnvInvalid(_, _)));
        std::env::remove_var(varname);
    }

    #[test]
    fn file_source_raw_32_bytes() {
        let dir = tempdir();
        let path = dir.join("mk.bin");
        std::fs::write(&path, [7u8; 32]).unwrap();
        let k = MasterKeySource::File(path.clone()).load().unwrap();
        assert_eq!(k, [7u8; 32]);
    }

    #[test]
    fn file_source_hex_text() {
        let dir = tempdir();
        let path = dir.join("mk.hex");
        std::fs::write(&path, "ab".repeat(32)).unwrap();
        let k = MasterKeySource::File(path.clone()).load().unwrap();
        assert_eq!(k, [0xabu8; 32]);
    }

    fn tempdir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("rekt-auth-mk-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
