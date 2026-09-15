// SPDX-License-Identifier: GPL-2.0-only
//! RDPGFX-compatible graphics PDUs ([MS-RDPEGFX] §2.2).
//!
//! Frame data is transported on the `GFXRDP` channel using the exact wire
//! format from the RDP Graphics Pipeline Extension, so any conforming RDPGFX
//! decoder can reassemble Precall frames. Direction is reversed relative to
//! normal RDP usage: the client produces `START_FRAME`/`WIRE_TO_SURFACE`/
//! `END_FRAME`, the server consumes them and may reply with
//! `FRAME_ACKNOWLEDGE`.
//!
//! Vendor extension: `RDPGFX_CODECID_JPEG` (0xF1) carries a complete JPEG
//! snapshot — Precall's "snapshot mode" equivalent of Recall's `ImageToken`
//! stills. Codec ids 0x14-0xFF are unassigned by MS-RDPEGFX; see
//! `docs/protocol.md`.

use crate::error::ProtoError;
use bytes::{BufMut, Bytes, BytesMut};

// --- RDPGFX command ids (MS-RDPEGFX §2.2) -----------------------------------
pub const RDPGFX_CMDID_WIRETOSURFACE_1: u16 = 0x0001;
pub const RDPGFX_CMDID_START_FRAME: u16 = 0x000b;
pub const RDPGFX_CMDID_END_FRAME: u16 = 0x000c;
pub const RDPGFX_CMDID_FRAMEACKNOWLEDGE: u16 = 0x000d;

// --- Codec ids (MS-RDPEGFX §2.2.2.2.1 + Precall vendor extension) ------------
pub const RDPGFX_CODECID_UNCOMPRESSED: u16 = 0x0000;
pub const RDPGFX_CODECID_AVC420: u16 = 0x000e;
pub const RDPGFX_CODECID_AVC444: u16 = 0x000f;
pub const RDPGFX_CODECID_CAPROGRESSIVE_V2: u16 = 0x0013;
/// Precall vendor extension: a self-contained JPEG still image.
pub const RDPGFX_CODECID_JPEG: u16 = 0x00f1;

// --- Pixel formats (MS-RDPEGFX §2.2.2.2.1, 1 byte) ---------------------------
pub const RDPGFX_PIXEL_FORMAT_XRGB_8888: u8 = 0x20;
pub const RDPGFX_PIXEL_FORMAT_ARGB_8888: u8 = 0x21;

const HEADER_LEN: usize = 8;

/// A rectangle in desktop coordinates (`RECT16` in the spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rect {
    pub left: i16,
    pub top: i16,
    pub right: i16,
    pub bottom: i16,
}

impl Rect {
    pub fn new(left: i16, top: i16, right: i16, bottom: i16) -> Self {
        Self { left, top, right, bottom }
    }
    pub fn width(&self) -> i16 {
        self.right - self.left
    }
    pub fn height(&self) -> i16 {
        self.bottom - self.top
    }
}

/// Desktop surface geometry advertised by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SurfaceInfo {
    pub surface_id: u16,
    pub width: u32,
    pub height: u32,
}

/// A decoded RDPGFX PDU.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GfxPdu {
    /// `RDPGFX_START_FRAME_PDU` — begins frame `frame_id`.
    StartFrame { frame_id: u32, timestamp: u32 },
    /// `RDPGFX_END_FRAME_PDU` — completes frame `frame_id`.
    EndFrame { frame_id: u32 },
    /// `RDPGFX_WIRE_TO_SURFACE_PDU_1` — one encoded dirty region.
    WireToSurface1 {
        surface_id: u16,
        codec_id: u16,
        pixel_format: u8,
        dest_rect: Rect,
        bitmap_data: Bytes,
    },
    /// `RDPGFX_FRAME_ACKNOWLEDGE_PDU` — sent server → client.
    FrameAcknowledge {
        queue_depth: u32,
        frame_id: u32,
        total_frames_decoded: u32,
    },
}

impl GfxPdu {
    pub fn cmd_id(&self) -> u16 {
        match self {
            GfxPdu::StartFrame { .. } => RDPGFX_CMDID_START_FRAME,
            GfxPdu::EndFrame { .. } => RDPGFX_CMDID_END_FRAME,
            GfxPdu::WireToSurface1 { .. } => RDPGFX_CMDID_WIRETOSURFACE_1,
            GfxPdu::FrameAcknowledge { .. } => RDPGFX_CMDID_FRAMEACKNOWLEDGE,
        }
    }

    /// Serialize to the wire (8-byte RDPGFX header + body).
    pub fn encode(&self, dst: &mut BytesMut) {
        let mut body = BytesMut::new();
        match self {
            GfxPdu::StartFrame { frame_id, timestamp } => {
                body.put_u32_le(*frame_id);
                body.put_u32_le(*timestamp);
            }
            GfxPdu::EndFrame { frame_id } => {
                body.put_u32_le(*frame_id);
            }
            GfxPdu::WireToSurface1 {
                surface_id,
                codec_id,
                pixel_format,
                dest_rect,
                bitmap_data,
            } => {
                body.put_u16_le(*surface_id);
                body.put_u16_le(*codec_id);
                body.put_u8(*pixel_format);
                body.put_i16_le(dest_rect.left);
                body.put_i16_le(dest_rect.top);
                body.put_i16_le(dest_rect.right);
                body.put_i16_le(dest_rect.bottom);
                body.put_u32_le(bitmap_data.len() as u32);
                body.extend_from_slice(bitmap_data);
            }
            GfxPdu::FrameAcknowledge {
                queue_depth,
                frame_id,
                total_frames_decoded,
            } => {
                body.put_u32_le(*queue_depth);
                body.put_u32_le(*frame_id);
                body.put_u32_le(*total_frames_decoded);
            }
        }
        dst.put_u16_le(self.cmd_id());
        dst.put_u16_le(0); // flags
        dst.put_u32_le((HEADER_LEN + body.len()) as u32);
        dst.extend_from_slice(&body);
    }

    pub fn encoded_len(&self) -> usize {
        HEADER_LEN
            + match self {
                GfxPdu::StartFrame { .. } => 8,
                GfxPdu::EndFrame { .. } => 4,
                GfxPdu::WireToSurface1 { bitmap_data, .. } => 17 + bitmap_data.len(),
                GfxPdu::FrameAcknowledge { .. } => 12,
            }
    }

    /// Decode exactly one PDU from the front of `buf`. Returns the PDU and the
    /// number of bytes consumed (which may be less than `buf.len()`).
    pub fn decode(buf: &[u8]) -> Result<(GfxPdu, usize), ProtoError> {
        if buf.len() < HEADER_LEN {
            return Err(ProtoError::Truncated { needed: HEADER_LEN, had: buf.len() });
        }
        let cmd_id = u16::from_le_bytes([buf[0], buf[1]]);
        let pdu_len = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        if pdu_len < HEADER_LEN {
            return Err(ProtoError::Malformed("pduLength shorter than header"));
        }
        if buf.len() < pdu_len {
            return Err(ProtoError::Truncated { needed: pdu_len, had: buf.len() });
        }
        let body = &buf[HEADER_LEN..pdu_len];
        let pdu = match cmd_id {
            RDPGFX_CMDID_START_FRAME => {
                if body.len() < 8 {
                    return Err(ProtoError::Malformed("START_FRAME body too short"));
                }
                GfxPdu::StartFrame {
                    frame_id: u32::from_le_bytes(body[0..4].try_into().unwrap()),
                    timestamp: u32::from_le_bytes(body[4..8].try_into().unwrap()),
                }
            }
            RDPGFX_CMDID_END_FRAME => {
                if body.len() < 4 {
                    return Err(ProtoError::Malformed("END_FRAME body too short"));
                }
                GfxPdu::EndFrame { frame_id: u32::from_le_bytes(body[0..4].try_into().unwrap()) }
            }
            RDPGFX_CMDID_WIRETOSURFACE_1 => {
                if body.len() < 17 {
                    return Err(ProtoError::Malformed("WIRE_TO_SURFACE_1 body too short"));
                }
                let bitmap_len = u32::from_le_bytes(body[13..17].try_into().unwrap()) as usize;
                if body.len() < 17 + bitmap_len {
                    return Err(ProtoError::Malformed("WIRE_TO_SURFACE_1 bitmap truncated"));
                }
                GfxPdu::WireToSurface1 {
                    surface_id: u16::from_le_bytes(body[0..2].try_into().unwrap()),
                    codec_id: u16::from_le_bytes(body[2..4].try_into().unwrap()),
                    pixel_format: body[4],
                    dest_rect: Rect {
                        left: i16::from_le_bytes(body[5..7].try_into().unwrap()),
                        top: i16::from_le_bytes(body[7..9].try_into().unwrap()),
                        right: i16::from_le_bytes(body[9..11].try_into().unwrap()),
                        bottom: i16::from_le_bytes(body[11..13].try_into().unwrap()),
                    },
                    bitmap_data: Bytes::copy_from_slice(&body[17..17 + bitmap_len]),
                }
            }
            RDPGFX_CMDID_FRAMEACKNOWLEDGE => {
                if body.len() < 12 {
                    return Err(ProtoError::Malformed("FRAME_ACK body too short"));
                }
                GfxPdu::FrameAcknowledge {
                    queue_depth: u32::from_le_bytes(body[0..4].try_into().unwrap()),
                    frame_id: u32::from_le_bytes(body[4..8].try_into().unwrap()),
                    total_frames_decoded: u32::from_le_bytes(body[8..12].try_into().unwrap()),
                }
            }
            other => return Err(ProtoError::UnknownCmdId(other)),
        };
        Ok((pdu, pdu_len))
    }
}

/// Iterator over a buffer of concatenated RDPGFX PDUs (a single frame's
/// START/W2S*/END set is typically shipped in one mux payload).
pub struct GfxPduIter<'a> {
    rest: &'a [u8],
}

impl<'a> GfxPduIter<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { rest: buf }
    }
}

impl<'a> Iterator for GfxPduIter<'a> {
    type Item = Result<GfxPdu, ProtoError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        match GfxPdu::decode(self.rest) {
            Ok((pdu, consumed)) => {
                self.rest = &self.rest[consumed..];
                Some(Ok(pdu))
            }
            Err(e) => {
                self.rest = &[];
                Some(Err(e))
            }
        }
    }
}

/// Human-readable codec name for logs.
pub fn codec_name(codec_id: u16) -> &'static str {
    match codec_id {
        RDPGFX_CODECID_UNCOMPRESSED => "uncompressed",
        RDPGFX_CODECID_AVC420 => "avc420",
        RDPGFX_CODECID_AVC444 => "avc444",
        RDPGFX_CODECID_CAPROGRESSIVE_V2 => "caprogressive-v2",
        RDPGFX_CODECID_JPEG => "jpeg-precall",
        _ => "unknown",
    }
}
