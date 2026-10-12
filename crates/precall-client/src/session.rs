// SPDX-License-Identifier: GPL-2.0-only
//! Transport session: TLS → handshake → channel mux. Mirrors the broker's
//! wire format exactly (same `precall-proto` codec on both ends).

use crate::config::ClientConfig;
use crate::ClientError;
use bytes::BytesMut;
use futures_util::SinkExt;
use precall_proto::metadata::{CaptureBatch, ControlMessage, WindowCaptureRecord};
use precall_proto::{
    read_limited_json, write_limited_json, AuthMethod, Capabilities, ChannelId, ClientHello,
    DisplayInfo, MuxCodec, MuxFrame, ServerHello, ServerReject, PROTOCOL_VERSION,
};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use tokio_util::codec::Framed;
use uuid::Uuid;

type Wire = Framed<TlsStream<TcpStream>, MuxCodec>;

pub struct Connection {
    pub framed: Wire,
    pub hello: ServerHello,
    pub client_id: Uuid,
    seq: u64,
    frame_id: u32,
    surface_id: u16,
}

impl Connection {
    /// Connect + TLS + handshake. PSK is the primary auth method; cert-
    /// fingerprint pinning guards the TLS layer (WebPKI as fallback).
    pub async fn connect(cfg: &ClientConfig, display: (u32, u32)) -> Result<Self, ClientError> {
        let client_id = cfg.client_id.unwrap_or_else(Uuid::new_v4);
        let addr: SocketAddr = cfg
            .server
            .parse()
            .map_err(|_| ClientError::Config(format!("bad server address {:?}", cfg.server)))?;
        let tcp = TcpStream::connect(addr).await?;

        let tls_cfg = if let Some(fp) = &cfg.fingerprint {
            precall_proto::pinning::pinned_client_config(fp)
                .map_err(ClientError::Config)?
        } else {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
                .into()
        };
        let name = ServerName::try_from(cfg.server_name.clone())
            .map_err(|_| ClientError::Config("bad server_name".into()))?;
        let mut tls = TlsConnector::from(tls_cfg).connect(name, tcp).await?;

        let hello = ClientHello {
            protocol_version: PROTOCOL_VERSION,
            client_id,
            hostname: std::env::var("COMPUTERNAME")
                .or_else(|_| std::env::var("HOSTNAME"))
                .unwrap_or_else(|_| "precall-client".into()),
            auth: AuthMethod::Psk {
                token: cfg.psk.clone().unwrap_or_default(),
            },
            capabilities: Capabilities {
                displays: vec![DisplayInfo {
                    surface_id: 1,
                    width: display.0,
                    height: display.1,
                    origin_x: 0,
                    origin_y: 0,
                }],
                channels: vec![
                    ChannelId::GfxRdp,
                    ChannelId::PcMeta,
                    ChannelId::PcCtrl,
                    ChannelId::PcAudio,
                ],
                codec_ids: vec![RDPGFX_CODECID_JPEG],
                capture_interval_ms: cfg.capture_interval_ms as u32,
                client_build: env!("CARGO_PKG_VERSION").to_string(),
            },
        };
        write_limited_json(&mut tls, &hello).await?;
        let resp: serde_json::Value = read_limited_json(&mut tls).await?;
        let hello: ServerHello = match serde_json::from_value(resp.clone()) {
            Ok(h) => h,
            Err(_) => {
                let rej: ServerReject = serde_json::from_value(resp)
                    .unwrap_or_else(|_| ServerReject { reason: "unknown".into() });
                return Err(ClientError::Config(format!("rejected: {}", rej.reason)));
            }
        };
        let framed: Wire = Framed::new(tls, MuxCodec);

        Ok(Self { framed, hello, client_id, seq: 0, frame_id: 0, surface_id: 1 })
    }

    async fn send_mux(&mut self, channel: ChannelId, payload: BytesMut) -> Result<(), ClientError> {
        self.framed
            .send(MuxFrame { channel, payload: payload.freeze() })
            .await?;
        Ok(())
    }

    /// One RDPGFX frame on GFXRDP: START → W2S_1(JPEG) → END → ACK.
    /// Send one frame: `regions` is either the whole surface (a keyframe) or
    /// only the rectangles that changed.
    ///
    /// The payload is framed by `precall_proto::encode_capture_frame` — the very
    /// codec that parses it. Hand-rolling these bytes is how the client shipped a
    /// malformed START_FRAME envelope (flags/length/timestamp mis-sized, so
    /// `pdu_len` decoded as 0x000C0000) and 4-byte rectangle coordinates where
    /// the PDU has `i16`; no client frame had ever been decodable, and only
    /// `precall-mock`, which uses the codec, was covered end-to-end.
    pub async fn send_frame_regions(
        &mut self,
        regions: &[(precall_frame::Rect, Vec<u8>)],
        timestamp_100ns: u64,
    ) -> Result<(), ClientError> {
        self.frame_id += 1;
        let framed: Vec<(precall_proto::Rect, bytes::Bytes)> = regions
            .iter()
            .map(|(r, jpeg)| {
                (
                    precall_proto::Rect::new(r.left, r.top, r.right, r.bottom),
                    bytes::Bytes::copy_from_slice(jpeg),
                )
            })
            .collect();
        let out = precall_proto::encode_capture_frame(
            self.frame_id,
            timestamp_100ns as u32,
            self.surface_id,
            &framed,
        );
        self.send_mux(ChannelId::GfxRdp, out).await
    }

    /// Send a whole-desktop keyframe.
    pub async fn send_frame(
        &mut self,
        jpeg: &[u8],
        width: u32,
        height: u32,
        timestamp_100ns: u64,
    ) -> Result<(), ClientError> {
        let region = [(
            precall_frame::Rect::new(0, 0, width as i16, height as i16),
            jpeg.to_vec(),
        )];
        self.send_frame_regions(&region, timestamp_100ns).await
    }


    pub async fn send_meta(&mut self, cap: WindowCaptureRecord) -> Result<(), ClientError> {
        self.seq += 1;
        let batch = CaptureBatch {
            client_id: self.client_id,
            seq: self.seq,
            captures: vec![cap],
        };
        let payload = serde_json::to_vec(&batch)?;
        self.send_mux(ChannelId::PcMeta, BytesMut::from(&payload[..])).await
    }

    /// PCAUDIO chunk: `[u16 header-len][json header][pcm16 payload]`.
    pub async fn send_audio(
        &mut self,
        pcm: &[i16],
        channels: u16,
        rate: u32,
    ) -> Result<(), ClientError> {
        let head = serde_json::json!({
            "codec": "pcm16",
            "channels": channels,
            "sample_rate": rate,
        });
        let hb = serde_json::to_vec(&head)?;
        let mut out = BytesMut::with_capacity(2 + hb.len() + pcm.len() * 2);
        out.extend_from_slice(&(hb.len() as u16).to_le_bytes());
        out.extend_from_slice(&hb);
        for s in pcm {
            out.extend_from_slice(&s.to_le_bytes());
        }
        self.send_mux(ChannelId::PcAudio, out).await
    }

    pub async fn send_ctrl(&mut self, msg: &ControlMessage) -> Result<(), ClientError> {
        let payload = serde_json::to_vec(msg)?;
        self.send_mux(ChannelId::PcCtrl, BytesMut::from(&payload[..])).await
    }
}
