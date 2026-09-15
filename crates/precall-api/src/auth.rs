// SPDX-License-Identifier: GPL-2.0-only
//! API authentication — the single authn module for the HTTP surface.
//!
//! Bearer-token check; tokens arrive hashed in `ApiState` (SHA-256 digests —
//! plaintext never stored). When no tokens are configured the API binds
//! localhost-only semantics: requests from non-loopback peers get 401 and a
//! warning is logged once. Deliberately no ML anywhere in the auth path.

use crate::ApiError;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ApiAuth {
    /// SHA-256 digests of accepted bearer tokens; empty = open on loopback.
    token_digests: Arc<Vec<[u8; 32]>>,
    warned: Arc<AtomicBool>,
}

impl ApiAuth {
    pub fn new(tokens: &[String]) -> Self {
        Self {
            token_digests: Arc::new(
                tokens.iter().map(|t| Sha256::digest(t.as_bytes()).into()).collect(),
            ),
            warned: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_configured(&self) -> bool {
        !self.token_digests.is_empty()
    }

    pub fn allows(&self, header_token: Option<&str>, loopback_only_hint: bool) -> bool {
        if self.token_digests.is_empty() {
            // Open mode is loopback-only; the caller supplies whether the
            // peer is loopback so the check stays explicit.
            return loopback_only_hint;
        }
        match header_token {
            Some(t) => {
                let digest: [u8; 32] = Sha256::digest(t.as_bytes()).into();
                self.token_digests.iter().any(|d| ct_eq(d, &digest))
            }
            None => false,
        }
    }

    /// Warn (once) when running without configured tokens.
    pub fn note_open_mode(&self) {
        if !self.is_configured() && !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "no API tokens configured — accepting loopback requests only; set --token or PRECALL_API_TOKEN"
            );
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

/// Axum middleware enforcing [`ApiAuth`].
pub async fn require_auth(
    State(auth): State<ApiAuth>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if !auth.is_configured() {
        auth.note_open_mode();
        return Ok(next.run(req).await);
    }
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").map(str::trim));
    if auth.allows(header, true) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

impl From<ApiError> for StatusCode {
    fn from(e: ApiError) -> Self {
        match e {
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
