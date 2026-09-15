// SPDX-License-Identifier: GPL-2.0-only
//! TLS plumbing for the broker + self-signed certificate generation for
//! development and tests.

use crate::BrokerError;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

/// Load a rustls `ServerConfig` from PEM cert chain + key files.
pub fn server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>, BrokerError> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;
    Ok(Arc::new(
        ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(BrokerError::Tls)?,
    ))
}

/// `ServerConfig` from already-parsed material (tests/embeddings).
pub fn server_config_from(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<ServerConfig>, BrokerError> {
    // Both `ring` and `aws-lc-rs` may be enabled transitively (tokio-rustls
    // pulls the aws-lc default); pick one explicitly or rustls panics.
    let _ = rustls::crypto::ring::default_provider().install_default();
    Ok(Arc::new(
        ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(BrokerError::Tls)?,
    ))
}

pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, BrokerError> {
    let pem = std::fs::read(path)?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut pem.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|e| BrokerError::Config(format!("bad cert pem {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(BrokerError::Config(format!("no certificates in {}", path.display())));
    }
    Ok(certs)
}

pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, BrokerError> {
    let pem = std::fs::read(path)?;
    rustls_pemfile::private_key(&mut pem.as_slice())
        .map_err(|e| BrokerError::Config(format!("bad key pem {}: {e}", path.display())))?
        .ok_or_else(|| BrokerError::Config(format!("no private key in {}", path.display())))
}

/// A freshly generated self-signed server identity.
pub struct SelfSigned {
    pub cert_pem: String,
    pub key_pem: String,
    pub cert_der: Vec<u8>,
    /// SHA-256 fingerprint (lowercase hex) — what clients pin to.
    pub fingerprint: String,
}

/// Generate a self-signed cert for `hostname` (dev/test use; production
/// should use a real internal CA).
pub fn generate_self_signed(hostname: &str) -> Result<SelfSigned, BrokerError> {
    let mut params = rcgen::CertificateParams::new(vec![hostname.to_string()])
        .map_err(|e| BrokerError::CertGen(e.to_string()))?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, hostname.to_string());
    let key_pair = rcgen::KeyPair::generate().map_err(|e| BrokerError::CertGen(e.to_string()))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| BrokerError::CertGen(e.to_string()))?;
    let cert_der = cert.der().to_vec();
    let fp = hex_lower(&Sha256::digest(&cert_der));
    Ok(SelfSigned {
        cert_pem: cert.pem(),
        key_pem: key_pair.serialize_pem(),
        cert_der,
        fingerprint: fp,
    })
}

/// Write PEM files; returns fingerprint for pinning.
pub fn write_self_signed(
    hostname: &str,
    cert_path: &Path,
    key_path: &Path,
) -> Result<SelfSigned, BrokerError> {
    let s = generate_self_signed(hostname)?;
    std::fs::write(cert_path, &s.cert_pem)?;
    std::fs::write(key_path, &s.key_pem)?;
    Ok(s)
}

pub fn fingerprint_der(der: &[u8]) -> String {
    hex_lower(&Sha256::digest(der))
}

fn hex_lower(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Build a `PrivateKeyDer` directly from a PKCS#8 PEM string.
pub fn pkcs8_from_pem(pem: &str) -> Result<PrivateKeyDer<'static>, BrokerError> {
    let mut bytes = pem.as_bytes();
    rustls_pemfile::private_key(&mut bytes)
        .map_err(|e| BrokerError::Config(format!("pkcs8: {e}")))?
        .ok_or_else(|| BrokerError::Config("no private key in pem".into()))
}
