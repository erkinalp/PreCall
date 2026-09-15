// SPDX-License-Identifier: GPL-2.0-only

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("truncated buffer: needed {needed} bytes, had {had}")]
    Truncated { needed: usize, had: usize },
    #[error("unknown RDPGFX command id: 0x{0:04x}")]
    UnknownCmdId(u16),
    #[error("unknown channel id: 0x{0:02x}")]
    UnknownChannel(u8),
    #[error("invalid pixel format: 0x{0:08x}")]
    InvalidPixelFormat(u32),
    #[error("malformed PDU: {0}")]
    Malformed(&'static str),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(usize),
}
