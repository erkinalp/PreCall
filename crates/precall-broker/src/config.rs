// SPDX-License-Identifier: GPL-2.0-only

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Broker configuration — loadable from TOML, overridable by CLI flags.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    /// Listen address, e.g. `0.0.0.0:3390`.
    pub listen: SocketAddr,
    /// PEM certificate + key for TLS termination.
    pub tls_cert: PathBuf,
    pub tls_key: PathBuf,
    /// Per-client data root: `{data_root}/{client_id}/ukg.db` etc.
    pub data_root: PathBuf,
    /// Accepted pre-shared keys (dev/unmanaged deployments).
    #[serde(default)]
    pub psk_tokens: Vec<String>,
    /// SHA-256 fingerprints (hex, lowercase) of enrolled client certificates.
    #[serde(default)]
    pub enrolled_certs: Vec<String>,
    /// Accept NTLM auth blobs (forwarded to a verifier later; off by default).
    #[serde(default)]
    pub allow_ntlm: bool,
    /// Accept any client_id that authenticates (self-hosted default).
    #[serde(default = "default_true")]
    pub allow_any_client: bool,
    /// Require client payloads to be AEAD-sealed (client holds same key).
    #[serde(default)]
    pub require_sealed_payloads: bool,
    /// Heartbeat expectation (ms) written into ServerHello.
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_ms: u32,
    /// Max simultaneous client sessions.
    #[serde(default = "default_max_clients")]
    pub max_clients: usize,
    /// Encrypt objects at rest with a generated per-installation key.
    #[serde(default = "default_true")]
    pub encrypt_at_rest: bool,
    /// Embedding dimension for the si_* stores.
    #[serde(default = "default_dim")]
    pub embedding_dim: usize,
}

fn default_true() -> bool {
    true
}
fn default_heartbeat() -> u32 {
    30_000
}
fn default_max_clients() -> usize {
    100
}
fn default_dim() -> usize {
    384
}

impl BrokerConfig {
    pub fn load(path: Option<&std::path::Path>) -> Result<Self, crate::BrokerError> {
        let Some(path) = path else {
            return Err(crate::BrokerError::Config(
                "no config file provided".to_string(),
            ));
        };
        let text = std::fs::read_to_string(path)
            .map_err(|e| crate::BrokerError::Config(format!("{}: {e}", path.display())))?;
        toml::from_str(&text).map_err(|e| crate::BrokerError::Config(format!("{e}")))
    }

    pub fn store_dir(&self, client_id: &uuid::Uuid) -> PathBuf {
        self.data_root.join(client_id.to_string())
    }
}
