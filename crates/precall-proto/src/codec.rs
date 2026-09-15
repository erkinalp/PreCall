// SPDX-License-Identifier: GPL-2.0-only
//! Multiplexed virtual-channel framing.
//!
//! Real RDP splits traffic across static/dynamic virtual channels inside the
//! MCS layer. Precall collapses the same channel names onto a single TLS
//! stream: every channel PDU is wrapped in a 5-byte mux header —
//! `[channel: u8][length: u32 LE][payload]`. The mapping from RDP channel
//! names to the byte identifier is documented in `docs/protocol.md`.

use crate::channel::ChannelId;
use crate::error::ProtoError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{Decoder, Encoder};

/// Cap on a single muxed channel payload (16 MiB — comfortably above one
/// 4K JPEG frame plus its metadata batch).
pub const MAX_MUX_PAYLOAD: usize = 16 * 1024 * 1024;
/// Cap on handshake JSON messages (1 MiB).
pub const MAX_HANDSHAKE_PAYLOAD: usize = 1024 * 1024;

const HEADER_LEN: usize = 5;

/// One channel PDU on the multiplexed stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MuxFrame {
    pub channel: ChannelId,
    pub payload: Bytes,
}

impl MuxFrame {
    pub fn new(channel: ChannelId, payload: impl Into<Bytes>) -> Self {
        Self { channel, payload: payload.into() }
    }
}

/// tokio-util codec for [`MuxFrame`].
#[derive(Debug, Default)]
pub struct MuxCodec;

impl Decoder for MuxCodec {
    type Item = MuxFrame;
    type Error = ProtoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<MuxFrame>, ProtoError> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }
        let channel_raw = src[0];
        let len = u32::from_le_bytes([src[1], src[2], src[3], src[4]]) as usize;
        if len > MAX_MUX_PAYLOAD {
            return Err(ProtoError::PayloadTooLarge(len));
        }
        if src.len() < HEADER_LEN + len {
            src.reserve(HEADER_LEN + len - src.len());
            return Ok(None);
        }
        let channel = ChannelId::from_u8(channel_raw)
            .ok_or(ProtoError::UnknownChannel(channel_raw))?;
        src.advance(HEADER_LEN);
        let payload = src.split_to(len).freeze();
        Ok(Some(MuxFrame { channel, payload }))
    }
}

impl Encoder<MuxFrame> for MuxCodec {
    type Error = ProtoError;

    fn encode(&mut self, item: MuxFrame, dst: &mut BytesMut) -> Result<(), ProtoError> {
        if item.payload.len() > MAX_MUX_PAYLOAD {
            return Err(ProtoError::PayloadTooLarge(item.payload.len()));
        }
        dst.reserve(HEADER_LEN + item.payload.len());
        dst.put_u8(item.channel.into());
        dst.put_u32_le(item.payload.len() as u32);
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}

/// Read one length-prefixed JSON value (`u32 LE` length + UTF-8 body).
/// Used for the `ClientHello`/`ServerHello` exchange that stands in for the
/// X.224/MCS negotiation phase.
pub async fn read_limited_json<R, T>(reader: &mut R) -> Result<T, ProtoError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = reader.read_u32_le().await? as usize;
    if len > MAX_HANDSHAKE_PAYLOAD {
        return Err(ProtoError::PayloadTooLarge(len));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(serde_json::from_slice(&buf)?)
}

/// Write one length-prefixed JSON value.
pub async fn write_limited_json<W, T>(writer: &mut W, value: &T) -> Result<(), ProtoError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(value)?;
    if body.len() > MAX_HANDSHAKE_PAYLOAD {
        return Err(ProtoError::PayloadTooLarge(body.len()));
    }
    writer.write_u32_le(body.len() as u32).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}
