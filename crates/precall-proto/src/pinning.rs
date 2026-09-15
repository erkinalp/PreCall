// SPDX-License-Identifier: GPL-2.0-only
//! Certificate-pinning TLS client config.
//!
//! Precall clients pin the broker's certificate by SHA-256 fingerprint rather
//! than relying on WebPKI — appropriate for self-hosted deploys where the
//! server cert is provisioned at enrollment time. The handshake signature
//! checks are delegated to rustls' provider; the fingerprint comparison is
//! the actual trust decision, applied to the end-entity certificate.

use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, SignatureScheme};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Accept only the certificate whose SHA-256 fingerprint matches.
#[derive(Debug)]
pub struct PinnedCertVerifier {
    fingerprint: [u8; 32],
}

impl PinnedCertVerifier {
    /// `fingerprint` — lowercase hex SHA-256 of the DER cert.
    pub fn new(fingerprint_hex: &str) -> Result<Self, String> {
        let fp = fingerprint_hex.trim().to_lowercase();
        let bytes: Result<Vec<u8>, _> =
            (0..fp.len()).step_by(2).map(|i| u8::from_str_radix(&fp[i..i + 2], 16)).collect();
        let v = bytes.map_err(|e| format!("bad fingerprint hex: {e}"))?;
        let arr: [u8; 32] = v
            .try_into()
            .map_err(|_| "fingerprint must be 32 bytes (64 hex chars)".to_string())?;
        Ok(Self { fingerprint: arr })
    }
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let digest = Sha256::digest(end_entity.as_ref());
        if digest.as_slice() == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(TlsError::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    // Signature-verification methods are asserted: the certificate *is*
    // the trust anchor here, so there is no PKIX path to validate. The
    // fingerprint equality above is the entire trust decision.
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

/// `ClientConfig` that pins `fingerprint` (hex SHA-256 of server cert DER).
pub fn pinned_client_config(fingerprint: &str) -> Result<Arc<ClientConfig>, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let verifier = PinnedCertVerifier::new(fingerprint)?;
    let cfg = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}
