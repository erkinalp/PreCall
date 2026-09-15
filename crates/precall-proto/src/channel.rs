// SPDX-License-Identifier: GPL-2.0-only
//! Virtual channel identifiers.
//!
//! In real RDP, `GFXRDP` rides a static virtual channel while the Precall
//! channels would be dynamic virtual channels (MS-RDPEDYC). We assign each a
//! fixed one-byte identifier used by [`crate::MuxFrame`] framing.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Virtual channel identifiers on the multiplexed TLS stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ChannelId {
    /// Desktop graphics: RDPGFX wire-format PDUs (client → server).
    GfxRdp = 0x01,
    /// Structured capture metadata: JSON `CaptureBatch` (client → server).
    PcMeta = 0x02,
    /// Opus audio segments (client → server).
    PcAudio = 0x03,
    /// Control plane: pause/resume/config/heartbeat (bidirectional).
    PcCtrl = 0x04,
    /// Server-pushed query results for local client UI (server → client).
    PcQuery = 0x05,
}

impl ChannelId {
    /// The RDP-style channel name used in handshake negotiation and logs.
    pub fn name(self) -> &'static str {
        match self {
            ChannelId::GfxRdp => "GFXRDP",
            ChannelId::PcMeta => "PCMETA",
            ChannelId::PcAudio => "PCAUDIO",
            ChannelId::PcCtrl => "PCCTRL",
            ChannelId::PcQuery => "PCQUERY",
        }
    }

    /// Parse from an RDP-style channel name (case-insensitive).
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "GFXRDP" => Some(ChannelId::GfxRdp),
            "PCMETA" => Some(ChannelId::PcMeta),
            "PCAUDIO" => Some(ChannelId::PcAudio),
            "PCCTRL" => Some(ChannelId::PcCtrl),
            "PCQUERY" => Some(ChannelId::PcQuery),
            _ => None,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0x01 => ChannelId::GfxRdp,
            0x02 => ChannelId::PcMeta,
            0x03 => ChannelId::PcAudio,
            0x04 => ChannelId::PcCtrl,
            0x05 => ChannelId::PcQuery,
            _ => return None,
        })
    }

    /// All channels a client may offer.
    pub fn all() -> &'static [ChannelId] {
        &[
            ChannelId::GfxRdp,
            ChannelId::PcMeta,
            ChannelId::PcAudio,
            ChannelId::PcCtrl,
            ChannelId::PcQuery,
        ]
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl From<ChannelId> for u8 {
    fn from(c: ChannelId) -> u8 {
        c as u8
    }
}
