// SPDX-License-Identifier: GPL-2.0-only
//! Headless Precall client used for end-to-end tests and demos.
//!
//! Generates synthetic JPEG "screenshots", drives the exact same wire
//! protocol as the real client (handshake → GFXRDP frames → PCMETA
//! batches → PCCTRL heartbeat), and supports certificate pinning so the test
//! path exercises production TLS semantics.


use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use precall_proto::*;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::TlsConnector;
use tokio_util::codec::Framed;
use uuid::Uuid;

pub use precall_proto::pinning::{pinned_client_config, PinnedCertVerifier};

/// One synthesized desktop frame.
pub struct SyntheticFrame {
    pub jpeg: Vec<u8>,
    pub meta: WindowCaptureRecord,
}

/// Render a deterministic JPEG "screenshot": a colour gradient derived from
/// `seed` (so consecutive frames differ but stay cheap to encode).
pub fn synthesize_jpeg(width: u32, height: u32, seed: u64) -> Vec<u8> {
    let mut img = image::RgbImage::new(width, height);
    for (x, y, px) in img.enumerate_pixels_mut() {
        px[0] = ((x as u64 * 255 / width.max(1) as u64) ^ seed) as u8;
        px[1] = ((y as u64 * 255 / height.max(1) as u64) ^ (seed >> 8)) as u8;
        px[2] = (seed as u8).wrapping_mul(31) ^ (x as u8) ^ (y as u8);
    }
    let mut out = std::io::Cursor::new(Vec::new());
    let enc = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 75);
    image::ImageEncoder::write_image(enc, img.as_raw(), width, height, image::ExtendedColorType::Rgb8)
        .expect("jpeg encode");
    out.into_inner()
}

pub fn sample_meta(client_frame: u64, image_token: &str) -> WindowCaptureRecord {
    let base = 133_000_000_000_000_000u64 + client_frame * 5 * 10_000_000;
    WindowCaptureRecord {
        name: format!("Precall test page {client_frame}"),
        image_token: image_token.to_string(),
        is_foreground: true,
        window_id: 0x1000 + client_frame as i64,
        window_bounds: "0,0,1280,720".into(),
        window_title: format!("Kerberos ticket {client_frame} - Precall mock"),
        properties: None,
        timestamp_100ns: base,
        activation_uri: Some(format!("precall://relaunch/{client_frame}")),
        activity_id: None,
        fallback_uri: None,
        apps: vec![AppRecord {
            windows_app_id: None,
            icon_uri: None,
            name: "precall-mock.exe".into(),
            path: Some("C:\\Precall\\precall-mock.exe".into()),
            properties: None,
        }],
        files: vec![FileRecord {
            path: format!("C:\\docs\\spec-{client_frame}.txt"),
            name: format!("spec-{client_frame}.txt"),
            extension: Some("txt".into()),
            kind: Some("document".into()),
            r#type: None,
            properties: None,
            object_id: None,
            volume_id: None,
        }],
        webs: vec![WebRecord {
            domain: "example.test".into(),
            uri: format!("https://example.test/page/{client_frame}"),
            icon_uri: None,
            properties: None,
        }],
        regions: vec![ScreenRegionRecord {
            region_kind: "text_block".into(),
            ocr_text: Some(format!(
                "The Kerberos V5 protocol page {client_frame} mentions tickets and realms"
            )),
            bounds: "0,0,1280,720".into(),
        }],
    }
}

/// An established session: handshake done, channels negotiated.
pub struct Session {
    framed: Framed<tokio_rustls::client::TlsStream<TcpStream>, MuxCodec>,
    pub hello: ServerHello,
    pub client_id: Uuid,
    seq: u64,
    frame_id: u32,
}

impl Session {
    /// Connect, handshake, return ready session.
    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        tls: std::sync::Arc<ClientConfig>,
        auth: AuthMethod,
        client_id: Uuid,
        hostname: &str,
    ) -> Result<Self, anyhow::Error> {
        let stream = TcpStream::connect(addr).await?;
        let connector = TlsConnector::from(tls);
        let name = ServerName::try_from(server_name.to_string())?;
        let mut tls_stream = connector.connect(name, stream).await?;

        let hello = ClientHello {
            protocol_version: PROTOCOL_VERSION,
            client_id,
            hostname: hostname.into(),
            auth,
            capabilities: Capabilities {
                displays: vec![DisplayInfo {
                    surface_id: 0,
                    width: 1280,
                    height: 720,
                    origin_x: 0,
                    origin_y: 0,
                }],
                channels: ChannelId::all().to_vec(),
                codec_ids: vec![RDPGFX_CODECID_JPEG],
                capture_interval_ms: 5000,
                client_build: concat!("precall-mock/", env!("CARGO_PKG_VERSION")).into(),
            },
        };
        write_limited_json(&mut tls_stream, &hello).await?;
        let resp: ServerHello = read_limited_json(&mut tls_stream).await?;
        Ok(Self {
            framed: Framed::new(tls_stream, MuxCodec),
            hello: resp,
            client_id,
            seq: 0,
            frame_id: 0,
        })
    }

    /// Send one capture: GFXRDP frame (START + W2S + END) then a PCMETA batch.
    pub async fn send_capture(&mut self, f: &SyntheticFrame) -> Result<(), anyhow::Error> {
        self.frame_id += 1;
        let mut gfx = BytesMut::new();
        GfxPdu::StartFrame {
            frame_id: self.frame_id,
            timestamp: (f.meta.timestamp_100ns & 0xFFFF_FFFF) as u32,
        }
        .encode(&mut gfx);
        GfxPdu::WireToSurface1 {
            surface_id: 0,
            codec_id: RDPGFX_CODECID_JPEG,
            pixel_format: RDPGFX_PIXEL_FORMAT_XRGB_8888,
            dest_rect: Rect::new(0, 0, 1280, 720),
            bitmap_data: bytes::Bytes::copy_from_slice(&f.jpeg),
        }
        .encode(&mut gfx);
        GfxPdu::EndFrame { frame_id: self.frame_id }.encode(&mut gfx);
        self.framed
            .send(MuxFrame::new(ChannelId::GfxRdp, gfx.freeze()))
            .await?;

        self.seq += 1;
        let batch = CaptureBatch {
            client_id: self.client_id,
            seq: self.seq,
            captures: vec![f.meta.clone()],
        };
        self.framed
            .send(MuxFrame::new(ChannelId::PcMeta, serde_json::to_vec(&batch)?))
            .await?;
        Ok(())
    }

    pub async fn heartbeat(&mut self, sent: u64, queue: u32) -> Result<(), anyhow::Error> {
        let msg = ControlMessage::Heartbeat {
            frames_sent: sent,
            frames_skipped_unchanged: 0,
            queue_depth: queue,
        };
        self.framed
            .send(MuxFrame::new(ChannelId::PcCtrl, serde_json::to_vec(&msg)?))
            .await?;
        Ok(())
    }

    /// Read one inbound mux frame (PCCTRL/PCQUERY), with timeout.
    pub async fn read(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Option<MuxFrame>, anyhow::Error> {
        match tokio::time::timeout(timeout, self.framed.next()).await {
            Ok(Some(Ok(f))) => Ok(Some(f)),
            Ok(Some(Err(e))) => Err(e.into()),
            Ok(None) => Ok(None),
            Err(_) => Ok(None),
        }
    }

    pub async fn close(mut self) -> Result<(), anyhow::Error> {
        let bye = ControlMessage::Goodbye { reason: "done".into() };
        self.framed
            .send(MuxFrame::new(ChannelId::PcCtrl, serde_json::to_vec(&bye)?))
            .await?;
        Ok(())
    }
}
