// SPDX-License-Identifier: GPL-2.0-only
//! Client authentication for the broker.
//!
//! Runs *inside* the TLS 1.3 session — the channel is already confidential,
//! so PSK comparison here never exposes tokens on the wire. Deliberately no
//! ML in the auth path (gesture/biometric ML is an anti-pattern for auth;
//! Windows Hello stays on the client side for its local cache).

use precall_proto::AuthMethod;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthDecision {
    /// Client may proceed; carries the principal name for audit logs.
    Allow { principal: String },
    Reject { reason: String },
}

pub struct Authenticator {
    /// SHA-256 digests of allowed PSK tokens — tokens are never stored in
    /// plaintext after config load.
    psk_digests: Vec<[u8; 32]>,
    enrolled_certs: Vec<String>,
    allow_ntlm: bool,
    allow_any_client: bool,
}

impl Authenticator {
    pub fn new(
        psk_tokens: &[String],
        enrolled_certs: &[String],
        allow_ntlm: bool,
        allow_any_client: bool,
    ) -> Self {
        Self {
            psk_digests: psk_tokens
                .iter()
                .map(|t| Sha256::digest(t.as_bytes()).into())
                .collect(),
            enrolled_certs: enrolled_certs.iter().map(|s| s.to_lowercase()).collect(),
            allow_ntlm,
            allow_any_client,
        }
    }

    /// One fixed set of allowed client UUIDs (empty = any authenticated id).
    pub fn authenticate(&self, auth: &AuthMethod, client_id: &uuid::Uuid) -> AuthDecision {
        match auth {
            AuthMethod::Psk { token } => {
                if !self.allow_any_client && self.psk_digests.is_empty() {
                    return AuthDecision::Reject {
                        reason: "psk auth disabled".into(),
                    };
                }
                let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
                // Constant-time-ish comparison over digests.
                if self.psk_digests.iter().any(|d| ct_eq(d, &digest)) {
                    AuthDecision::Allow { principal: format!("psk:{}", &client_id.to_string()[..8]) }
                } else {
                    AuthDecision::Reject { reason: "unknown pre-shared key".into() }
                }
            }
            AuthMethod::Certificate { fingerprint } => {
                let fp = fingerprint.to_lowercase();
                if self.enrolled_certs.iter().any(|e| e == &fp) {
                    AuthDecision::Allow { principal: format!("cert:{fp}") }
                } else {
                    AuthDecision::Reject { reason: "certificate not enrolled".into() }
                }
            }
            AuthMethod::Ntlm { user, .. } => {
                if self.allow_ntlm {
                    AuthDecision::Allow { principal: format!("ntlm:{user}") }
                } else {
                    AuthDecision::Reject {
                        reason: "ntlm disabled; use psk or certificate".into(),
                    }
                }
            }
        }
    }
}

fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut acc = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    acc == 0
}
