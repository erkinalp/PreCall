// SPDX-License-Identifier: GPL-2.0-only
//! Connection handshake.
//!
//! Stands in for the X.224/MCS/NLA negotiation phases of a real RDP
//! connection (see `docs/protocol.md` for the preservation/modification
//! split). Runs inside the TLS 1.3 session, so the exchange is already
//! confidential; authentication here identifies *which* client this is and
//! what it may do — it is deliberately not ML-assisted.

use crate::channel::ChannelId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Wire protocol version. Bump on any incompatible change.
pub const PROTOCOL_VERSION: u16 = 1;

/// How the client proves its identity inside the TLS session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthMethod {
    /// Pre-shared key for unmanaged deployments.
    Psk { token: String },
    /// Client machine certificate; `fingerprint` is SHA-256 of the DER form,
    /// matched against the server's enrolled-cert store.
    Certificate { fingerprint: String },
    /// Domain credentials for a future CredSSP/NLA path (currently rejected
    /// by the broker unless `allow_ntlm` is enabled).
    Ntlm { user: String, response_b64: String },
}

/// A connected display reported by the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayInfo {
    /// Surface identifier used in RDPGFX PDUs. 0 = primary desktop.
    pub surface_id: u16,
    pub width: u32,
    pub height: u32,
    /// Desktop offset for multi-monitor layouts.
    pub origin_x: i32,
    pub origin_y: i32,
}

/// What the client can send and wants to use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    pub displays: Vec<DisplayInfo>,
    /// Channels the client offers (must include GFXRDP + PCMETA).
    pub channels: Vec<ChannelId>,
    /// RDPGFX codec ids the client may produce.
    pub codec_ids: Vec<u16>,
    /// Client capture cadence in milliseconds.
    pub capture_interval_ms: u32,
    /// Client product string, e.g. `precall-client/0.1.0 windows-x86_64`.
    pub client_build: String,
}

/// Client → server, immediately after the TLS handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHello {
    pub protocol_version: u16,
    /// Stable machine identifier (registry- or config-generated UUID).
    pub client_id: Uuid,
    /// Human-readable machine name for dashboards.
    pub hostname: String,
    pub auth: AuthMethod,
    pub capabilities: Capabilities,
}

/// Server → client reply completing negotiation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHello {
    /// Session identifier for logs and resume tokens.
    pub session_id: Uuid,
    /// Subset of offered channels the server accepted.
    pub accepted_channels: Vec<ChannelId>,
    /// Server wall-clock in 100ns ticks since 1601-01-01 (FILETIME epoch).
    pub server_time_100ns: u64,
    /// Client must emit a PCCTRL heartbeat at least this often.
    pub heartbeat_interval_ms: u32,
    /// Server-side cap on one mux payload.
    pub max_frame_bytes: u32,
}

/// Rejection reply (sent instead of `ServerHello`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerReject {
    pub reason: String,
}
