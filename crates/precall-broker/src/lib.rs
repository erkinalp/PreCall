// SPDX-License-Identifier: GPL-2.0-only
//! Precall connection broker — the server half of the reverse-capture pair.
//!
//! Listens on TLS 1.3, authenticates clients (PSK / machine certificate /
//! optional NTLM passthrough), then consumes multiplexed channels: `GFXRDP`
//! frames → object storage, `PCMETA` batches → `ukg.db`, `PCAUDIO` segments
//! → object storage, `PCCTRL` heartbeats/commands.

mod auth;
mod config;
mod ingest;
pub mod tls;

pub use auth::{AuthDecision, Authenticator};
pub use config::BrokerConfig;
pub use ingest::{filetime_now, filetime_to_ymd, Broker, ClientStore, LiveEvent};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS: {0}")]
    Tls(#[from] rustls::Error),
    #[error("protocol: {0}")]
    Proto(#[from] precall_proto::ProtoError),
    #[error("store: {0}")]
    Store(#[from] precall_store::ukg::UkgError),
    #[error("object store: {0}")]
    Objects(#[from] precall_store::objects::ObjectError),
    #[error("semantic index: {0}")]
    Semantic(#[from] precall_store::semantic::SemanticError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("auth rejected: {0}")]
    AuthRejected(String),
    #[error("bad config: {0}")]
    Config(String),
    #[error("certgen: {0}")]
    CertGen(String),
}
