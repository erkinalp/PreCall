// SPDX-License-Identifier: GPL-2.0-only
//! Precall capture client — the agent that runs on the workstation.
//!
//! * `dxgi` — Desktop Duplication API capture (what Terminal Services uses
//!   for its graphics pipeline on Win8+).
//! * `audio` — WASAPI loopback audio, PCM16 for now.
//! * `meta` — foreground-window metadata: title, bounds, process, AUMID,
//!   browser URL (UI Automation), Explorer paths (ShellWindows).
//! * `ocr` — on-device `Windows.Media.Ocr` regions (the same engine Recall
//!   uses for Click-to-Do).
//! * `privacy` — protected-window and user-exclusion filtering. A capture
//!   that fails any check is dropped *before* encoding — nothing sensitive
//!   ever reaches the wire.
//! * `session` — TLS + pinned-cert transport speaking `precall-proto`.
//! * `run` — capture loop with reconnect/backoff.
//! * `service` — Windows service host (SCM) wrapper.

pub mod audio;
pub mod config;
pub mod dxgi;
pub mod jpeg;
pub mod meta;
pub mod ocr;
pub mod privacy;
pub mod run;
pub mod service;
pub mod session;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("windows: {0}")]
    Windows(#[from] windows::core::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("proto: {0}")]
    Proto(#[from] precall_proto::ProtoError),
    #[error("tls: {0}")]
    Tls(#[from] rustls::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("image: {0}")]
    Image(#[from] image::ImageError),
    #[error("config: {0}")]
    Config(String),
}
