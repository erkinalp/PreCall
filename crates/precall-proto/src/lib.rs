// SPDX-License-Identifier: GPL-2.0-only
//! Precall reverse-capture wire protocol.
//!
//! The client (a Windows 11 workstation) initiates a connection *to* the
//! capture server and streams its own desktop — the inverse of a normal RDP
//! session. Graphics are transported inside RDPGFX-compatible PDUs
//! (MS-RDPEGFX wire format) so standard RDP tooling can decode the frames,
//! while metadata, audio, and control traffic ride on named virtual channels
//! multiplexed over a single TLS connection.
//!
//! Layout of a stream, after TLS establishment:
//!
//! ```text
//! Client                                 Server
//!   │── ClientHello (length-prefixed JSON) ─▶│   auth + capabilities
//!   │◀────────────── ServerHello ────────────│
//!   │═══ MuxFrame{GFXRDP, RDPGFX PDUs} ═════▶│   desktop frames
//!   │═══ MuxFrame{PCMETA, JSON batch} ══════▶│   ukg.db metadata
//!   │═══ MuxFrame{PCAUDIO, Opus} ══════════▶│   optional audio
//!   │◀═══ MuxFrame{PCCTRL, JSON} ═══════════│   pause/resume/config
//!   │◀═══ MuxFrame{PCQUERY, JSON} ═════════│   pushed query results
//! ```

mod channel;
mod codec;
mod error;
mod gfx;
mod handshake;
pub mod metadata;
pub mod pinning;
pub mod time;

pub use channel::ChannelId;
pub use time::{filetime_now, filetime_to_unix, FILETIME_UNIX_DELTA};
pub use codec::{read_limited_json, write_limited_json, MuxCodec, MuxFrame, MAX_MUX_PAYLOAD};
pub use error::ProtoError;
pub use gfx::{
    codec_name, GfxPdu, GfxPduIter, Rect, SurfaceInfo, RDPGFX_CMDID_END_FRAME,
    RDPGFX_CMDID_FRAMEACKNOWLEDGE, RDPGFX_CMDID_START_FRAME, RDPGFX_CMDID_WIRETOSURFACE_1,
    RDPGFX_CODECID_AVC420, RDPGFX_CODECID_AVC444, RDPGFX_CODECID_CAPROGRESSIVE_V2,
    RDPGFX_CODECID_JPEG, RDPGFX_CODECID_UNCOMPRESSED, RDPGFX_PIXEL_FORMAT_ARGB_8888,
    RDPGFX_PIXEL_FORMAT_XRGB_8888,
};
pub use handshake::{
    AuthMethod, Capabilities, ClientHello, DisplayInfo, ServerHello, ServerReject,
    PROTOCOL_VERSION,
};
pub use metadata::{
    AppRecord, CaptureBatch, ControlMessage, FileRecord, QueryPush, ScreenRegionRecord,
    WebRecord, WindowCaptureRecord,
};
