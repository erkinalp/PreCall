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
    RDPGFX_CMDID_END_FRAME, RDPGFX_CMDID_FRAMEACKNOWLEDGE, RDPGFX_CMDID_START_FRAME,
    RDPGFX_CMDID_WIRETOSURFACE_1, RDPGFX_CODECID_JPEG, RDPGFX_PIXEL_FORMAT_XRGB_8888,
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
    pub async fn connect(cfg: &ClientConfig) -> Result<Self, ClientError> {
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
                    width: 0,
                    height: 0,
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
    /// Layout identical to MS-RDPEGFX; the client assigns the frame id.
    pub async fn send_frame(
        &mut self,
        jpeg: &[u8],
        width: u32,
        height: u32,
        timestamp_100ns: u64,
    ) -> Result<(), ClientError> {
        self.frame_id += 1;
        let fid = self.frame_id;
        let mut out = BytesMut::new();
        out.extend_from_slice(&RDPGFX_CMDID_START_FRAME.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&12u32.to_le_bytes());
        out.extend_from_slice(&fid.to_le_bytes());
        out.extend_from_slice(&timestamp_100ns.to_le_bytes());
        let bl = jpeg.len() as u32;
        let pdu_len = 8 + 2 + 2 + 1 + 8 + 4 + bl;
        out.extend_from_slice(&RDPGFX_CMDID_WIRETOSURFACE_1.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&pdu_len.to_le_bytes());
        out.extend_from_slice(&(self.surface_id as u32).to_le_bytes()[..2]);
        out.extend_from_slice(&RDPGFX_CODECID_JPEG.to_le_bytes());
        out.extend_from_slice(&[RDPGFX_PIXEL_FORMAT_XRGB_8888]);
        out.extend_from_slice(&0i32.to_le_bytes());
        out.extend_from_slice(&0i32.to_le_bytes());
        out.extend_from_slice(&(width as i32).to_le_bytes());
        out.extend_from_slice(&(height as i32).to_le_bytes());
        out.extend_from_slice(&bl.to_le_bytes());
        out.extend_from_slice(jpeg);
        out.extend_from_slice(&RDPGFX_CMDID_END_FRAME.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&8u32.to_le_bytes());
        out.extend_from_slice(&fid.to_le_bytes());
        out.extend_from_slice(&RDPGFX_CMDID_FRAMEACKNOWLEDGE.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&20u32.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&fid.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes());
        self.send_mux(ChannelId::GfxRdp, out).await
    }

    /// PCMETA batch — one WindowCaptureRecord + app/web/file/region rows.
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
