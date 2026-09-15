// SPDX-License-Identifier: GPL-2.0-only
//! Object storage for screenshots and audio segments.
//!
//! Layout: `{client_id}/{yyyy}/{mm}/{dd}/{token}.jpg` — matches the design's
//! filesystem backend; an S3-compatible backend can sit behind the same API.

use crate::crypto::{CryptoError, EnvelopeCipher};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// FILETIME (100ns ticks since 1601) → (year, month, day) for object paths —
/// the layout key shared by broker (writer) and API (reader).
pub fn ymd_from_filetime(ticks: i64) -> (i32, u32, u32) {
    const DELTA: i64 = 116_444_736_000_000_000;
    let days = (ticks - DELTA).div_euclid(86_400_000_000_000);
    // civil-from-days (Howard Hinnant's algorithm)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y } as i32, m as u32, d as u32)
}

#[derive(Debug, Error)]
pub enum ObjectError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("crypto: {0}")]
    Crypto(#[from] CryptoError),
    #[error("not found: {0}")]
    NotFound(String),
}

pub struct ObjectStore {
    root: PathBuf,
    cipher: Option<EnvelopeCipher>,
}

impl ObjectStore {
    /// Plaintext at rest.
    pub fn plain(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::open(root, None)
    }

    /// Encrypted at rest with a per-installation key.
    pub fn encrypted(root: impl Into<PathBuf>, key: [u8; 32]) -> std::io::Result<Self> {
        Self::open(root, Some(EnvelopeCipher::new(&key)))
    }

    fn open(root: impl Into<PathBuf>, cipher: Option<EnvelopeCipher>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, cipher })
    }

    /// `{client}/{y}/{m}/{d}/{token}.{ext}` — `date` is `(year, month, day)`.
    fn key_path(client_id: &str, date: (i32, u32, u32), token: &str, ext: &str) -> PathBuf {
        // Sanitize path components — tokens come off the wire.
        let clean = |s: &str| -> String {
            s.chars().filter(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.')).collect()
        };
        PathBuf::from(clean(client_id))
            .join(format!("{:04}", date.0))
            .join(format!("{:02}", date.1))
            .join(format!("{:02}", date.2))
            .join(format!("{}.{}", clean(token), clean(ext)))
    }

    fn abs(&self, rel: &Path) -> PathBuf {
        self.root.join(rel)
    }

    pub fn put_image(
        &self,
        client_id: &str,
        date: (i32, u32, u32),
        token: &str,
        jpeg: &[u8],
    ) -> Result<PathBuf, ObjectError> {
        self.put(client_id, date, token, "jpg", jpeg)
    }

    pub fn put_audio(
        &self,
        client_id: &str,
        date: (i32, u32, u32),
        token: &str,
        opus: &[u8],
    ) -> Result<PathBuf, ObjectError> {
        self.put(client_id, date, token, "opus", opus)
    }

    pub fn put(
        &self,
        client_id: &str,
        date: (i32, u32, u32),
        token: &str,
        ext: &str,
        bytes: &[u8],
    ) -> Result<PathBuf, ObjectError> {
        let rel = Self::key_path(client_id, date, token, ext);
        let abs = self.abs(&rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match &self.cipher {
            Some(c) => std::fs::write(&abs, c.seal(token.as_bytes(), bytes))?,
            None => std::fs::write(&abs, bytes)?,
        }
        Ok(rel)
    }

    /// Read an object back; `rel` is the path returned by `put*`.
    pub fn get(&self, client_id: &str, rel: &Path, token: &str) -> Result<Vec<u8>, ObjectError> {
        let abs = self.abs(rel);
        // Prevent path escape — resolved path must stay under root/client.
        let guard = self.root.join(client_id);
        if !abs.starts_with(&guard) {
            return Err(ObjectError::NotFound(rel.display().to_string()));
        }
        let raw = std::fs::read(&abs)?;
        match &self.cipher {
            Some(c) => Ok(c.open(token.as_bytes(), &raw)?),
            None => Ok(raw),
        }
    }

    pub fn exists(&self, rel: &Path) -> bool {
        self.abs(rel).is_file()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Absolute path for a relative object key — for serving via API.
    pub fn abs_path(&self, rel: &Path) -> PathBuf {
        self.abs(rel)
    }

    /// Wipe every object belonging to a client (panic button).
    pub fn purge_client(&self, client_id: &str) -> Result<(), ObjectError> {
        let dir = self.root.join(client_id);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }
}

/// Load (or generate) the object-encryption key under `data_root/.keys/`.
/// Both broker (writer) and API (reader) use this so ciphertext is readable
/// by exactly the processes that own the data root.
pub fn load_or_create_key(data_root: &Path) -> Result<[u8; 32], ObjectError> {
    let dir = data_root.join(".keys");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("objects.key");
    if path.exists() {
        let raw = std::fs::read(&path)?;
        if raw.len() == 32 {
            let mut k = [0u8; 32];
            k.copy_from_slice(&raw);
            return Ok(k);
        }
        let s = String::from_utf8_lossy(&raw);
        let s = s.trim();
        let decoded: Result<Vec<u8>, _> =
            (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16)).collect();
        let d = decoded.map_err(|e| {
            ObjectError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        let k: [u8; 32] = d
            .try_into()
            .map_err(|_| ObjectError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "objects.key must be 32 bytes")))?;
        Ok(k)
    } else {
        let k = EnvelopeCipher::generate_key();
        std::fs::write(&path, k)?;
        Ok(k)
    }
}
