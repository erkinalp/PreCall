// SPDX-License-Identifier: GPL-2.0-only
//! Precall query API — the Recall-compatible REST + WebSocket surface.
//!
//! ```text
//! GET    /api/v1/timeline?client=&before=&limit=    paginated timeline
//! POST   /api/v1/search                              FTS5 + semantic hybrid
//! GET    /api/v1/snapshot/{id}?client=               screenshot (decrypted)
//! GET    /api/v1/regions/{id}?client=                OCR regions (Click-to-Do)
//! POST   /api/v1/relaunch                            {id} → activation_uri
//! GET    /api/v1/apps?client=                        app dwell analytics
//! GET    /api/v1/web?client=                         web domain analytics
//! GET    /api/v1/clients                             enrolled machines
//! GET    /api/v1/export?client=                      GDPR data export
//! DELETE /api/v1/client/{id}/data                    panic wipe
//! WS     /api/v1/live?client=                        new-capture stream
//! GET    /api/v1/health
//! ```

pub mod auth;
mod embed;
pub mod enrich;
mod routes;

pub use embed::{Embedder, EMBED_DIM};
pub use routes::{build_router, ApiState};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("db: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("store: {0}")]
    Store(#[from] precall_store::ukg::UkgError),
    #[error("objects: {0}")]
    Objects(#[from] precall_store::objects::ObjectError),
    #[error("semantic: {0}")]
    Semantic(#[from] precall_store::semantic::SemanticError),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("upstream: {0}")]
    Upstream(String),
}
