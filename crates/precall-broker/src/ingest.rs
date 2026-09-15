// SPDX-License-Identifier: GPL-2.0-only
//! Per-connection ingestion: handshake → auth → channel dispatch → storage.

use crate::auth::{AuthDecision, Authenticator};
use crate::config::BrokerConfig;
use crate::BrokerError;
use futures_util::{SinkExt, StreamExt};
use precall_proto::*;
use precall_store::{ObjectStore, SemanticIndex, Ukg};
use rustls::ServerConfig;
use serde::Serialize;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

/// Emitted on the in-process bus for every stored capture (the API process
/// polls `ukg.db` for the same data when running standalone — see
/// `docs/protocol.md`).
#[derive(Debug, Clone, Serialize)]
pub struct LiveEvent {
    pub client_id: uuid::Uuid,
    pub window_capture_id: i64,
    pub image_token: Option<String>,
    pub window_title: Option<String>,
    pub timestamp_100ns: i64,
}

/// All per-client storage handles in one place.
pub struct ClientStore {
    pub ukg: Ukg,
    pub objects: ObjectStore,
    pub text_index: SemanticIndex,
    pub image_index: SemanticIndex,
}

pub struct Broker {
    config: BrokerConfig,
    auth: Authenticator,
    tls: Arc<ServerConfig>,
    /// Encryption key for objects at rest; generated under `data_root` on
    /// first boot so ciphertext can't wander off with the key.
    object_key: [u8; 32],
    /// Global registry DB (`{data_root}/server.db`) — `PrecallClient` rows
    /// let the API enumerate tenants without scanning per-client DBs.
    server_db: std::sync::Mutex<Ukg>,
    live_tx: broadcast::Sender<LiveEvent>,
    sessions: std::sync::atomic::AtomicUsize,
}

impl Broker {
    pub fn new(config: BrokerConfig, tls: Arc<ServerConfig>) -> Result<Self, BrokerError> {
        std::fs::create_dir_all(&config.data_root)?;
        let object_key = precall_store::objects::load_or_create_key(&config.data_root)?;
        let (live_tx, _) = broadcast::channel(1024);
        let server_db = std::sync::Mutex::new(Ukg::open(&config.data_root.join("server.db"))?);
        Ok(Self {
            auth: Authenticator::new(
                &config.psk_tokens,
                &config.enrolled_certs,
                config.allow_ntlm,
                config.allow_any_client,
            ),
            server_db,
            config,
            tls,
            object_key,
            live_tx,
            sessions: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Subscribe to live ingest events (same-process consumers only).
    pub fn subscribe(&self) -> broadcast::Receiver<LiveEvent> {
        self.live_tx.subscribe()
    }

    pub async fn run(self: Arc<Self>) -> Result<(), BrokerError> {
        let listener = TcpListener::bind(self.config.listen).await?;
        info!(addr = %self.config.listen, "precall-broker listening (TLS 1.3)");
        self.run_on(listener).await
    }

    /// Serve on an already-bound listener (tests bind port 0 to get an
    /// ephemeral port back via `listener.local_addr()`).
    pub async fn run_on(self: Arc<Self>, listener: TcpListener) -> Result<(), BrokerError> {
        let acceptor = TlsAcceptor::from(self.tls.clone());
        loop {
            let (stream, addr) = listener.accept().await?;
            let this = Arc::clone(&self);
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Err(e) = this.handle_conn(stream, addr, acceptor).await {
                    debug!(%addr, error = %e, "session ended with error");
                }
            });
        }
    }

    /// Open the per-client storage bundle. Each session holds its own
    /// handles — SQLite WAL mode makes concurrent connections to the same
    /// `ukg.db` safe, and `rusqlite::Connection` isn't `Sync` anyway.
    fn open_store(&self, client_id: uuid::Uuid) -> Result<ClientStore, BrokerError> {
        let dir = self.config.store_dir(&client_id);
        std::fs::create_dir_all(&dir)?;
        let ukg = Ukg::open(&dir.join("ukg.db"))?;
        let objects = if self.config.encrypt_at_rest {
            ObjectStore::encrypted(dir.join("objects"), self.object_key)?
        } else {
            ObjectStore::plain(dir.join("objects"))?
        };
        let text_index =
            SemanticIndex::open(&dir.join("SemanticTextStore.db"), self.config.embedding_dim, "text")?;
        let image_index =
            SemanticIndex::open(&dir.join("SemanticImageStore.db"), self.config.embedding_dim, "image")?;
        info!(%client_id, "opened client store at {}", dir.display());
        Ok(ClientStore { ukg, objects, text_index, image_index })
    }

    async fn handle_conn(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        acceptor: TlsAcceptor,
    ) -> Result<(), BrokerError> {
        if self.sessions.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= self.config.max_clients
        {
            self.sessions.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return Err(BrokerError::Config("max_clients reached".into()));
        }
        let _guard = SessionGuard(&self.sessions);

        stream.set_nodelay(true).ok();
        let mut tls = acceptor.accept(stream).await?;

        // --- Hello ----------------------------------------------------------
        let hello: ClientHello = precall_proto::read_limited_json(&mut tls).await?;
        if hello.protocol_version != PROTOCOL_VERSION {
            write_reject(&mut tls, "protocol version mismatch").await.ok();
            return Err(BrokerError::AuthRejected(format!(
                "version {} != {}",
                hello.protocol_version, PROTOCOL_VERSION
            )));
        }
        match self.auth.authenticate(&hello.auth, &hello.client_id) {
            AuthDecision::Allow { principal } => {
                info!(%addr, client = %hello.client_id, %principal, host = %hello.hostname, "authenticated");
            }
            AuthDecision::Reject { reason } => {
                write_reject(&mut tls, &reason).await.ok();
                return Err(BrokerError::AuthRejected(reason));
            }
        }
        let store = self.open_store(hello.client_id)?;
        let cid = hello.client_id.to_string();
        let now = filetime_now() as i64;
        store.ukg.touch_client(&cid, &hello.hostname, now)?;
        if let Ok(server_db) = self.server_db.lock() {
            server_db.touch_client(&cid, &hello.hostname, now)?;
        }
        let accepted: Vec<ChannelId> = hello
            .capabilities
            .channels
            .iter()
            .copied()
            .filter(|c| [ChannelId::GfxRdp, ChannelId::PcMeta, ChannelId::PcCtrl, ChannelId::PcAudio].contains(c))
            .collect();
        let resp = ServerHello {
            session_id: uuid::Uuid::new_v4(),
            accepted_channels: accepted,
            server_time_100ns: filetime_now(),
            heartbeat_interval_ms: self.config.heartbeat_interval_ms,
            max_frame_bytes: precall_proto::MAX_MUX_PAYLOAD as u32,
        };
        precall_proto::write_limited_json(&mut tls, &resp).await?;

        // --- Session loop ----------------------------------------------------
        let (mut sink, mut source) = Framed::new(tls, MuxCodec).split();
        let (_ctrl_tx, mut ctrl_rx) = mpsc::channel::<ControlMessage>(32);
        let mut frames = FrameAssembler::new();
        let mut pending: VecDeque<PendingFrame> = VecDeque::new();
        let mut hb_interval =
            tokio::time::interval(std::time::Duration::from_millis(self.config.heartbeat_interval_ms as u64 * 2));
        hb_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_seen = std::time::Instant::now();

        loop {
            tokio::select! {
                item = source.next() => {
                    let Some(item) = item else { break };
                    let frame = item?;
                    last_seen = std::time::Instant::now();
                    match frame.channel {
                        ChannelId::GfxRdp => self.handle_gfx(&hello.client_id, &frame.payload, &mut frames, &mut pending)?,
                        ChannelId::PcMeta => self.handle_meta(&hello.client_id, &frame.payload, &store, &mut pending)?,
                        ChannelId::PcAudio => self.handle_audio(&hello.client_id, &frame.payload, &store)?,
                        ChannelId::PcCtrl => self.handle_ctrl(&frame.payload)?,
                        ChannelId::PcQuery => warn!("client must not send on PCQUERY; dropping"),
                    }
                }
                Some(cmd) = ctrl_rx.recv() => {
                    match cmd {
                        ControlMessage::Goodbye { .. } => {
                            let _ = sink.send(MuxFrame::new(ChannelId::PcCtrl, serde_json::to_vec(&cmd)?)).await;
                            break;
                        }
                        other => {
                            sink.send(MuxFrame::new(ChannelId::PcCtrl, serde_json::to_vec(&other)?)).await?;
                        }
                    }
                }
                _ = hb_interval.tick() => {
                    if last_seen.elapsed() > std::time::Duration::from_millis(self.config.heartbeat_interval_ms as u64 * 4) {
                        warn!(client = %hello.client_id, "heartbeat timeout; dropping session");
                        break;
                    }
                }
            }
        }
        info!(client = %hello.client_id, "session closed");
        Ok(())
    }

    /// Parse the RDPGFX stream; completed frames join the pending queue to be
    /// named by the capture metadata that follows them on PCMETA.
    fn handle_gfx(
        &self,
        client_id: &uuid::Uuid,
        payload: &[u8],
        frames: &mut FrameAssembler,
        pending: &mut VecDeque<PendingFrame>,
    ) -> Result<(), BrokerError> {
        for pdu in GfxPduIter::new(payload) {
            let pdu = pdu?;
            match pdu {
                GfxPdu::StartFrame { frame_id, timestamp } => frames.begin(frame_id, timestamp),
                GfxPdu::WireToSurface1 { codec_id, dest_rect, bitmap_data, .. } => {
                    frames.region(codec_id, dest_rect, bitmap_data);
                }
                GfxPdu::EndFrame { frame_id } => {
                    if let Some(done) = frames.finish(frame_id) {
                        if pending.len() >= 64 {
                            pending.pop_front();
                        }
                        pending.push_back(done);
                    }
                }
                GfxPdu::FrameAcknowledge { .. } => {}
            }
        }
        let _ = client_id;
        Ok(())
    }

    fn handle_meta(
        &self,
        client_id: &uuid::Uuid,
        payload: &[u8],
        store: &ClientStore,
        pending: &mut VecDeque<PendingFrame>,
    ) -> Result<(), BrokerError> {
        let batch: CaptureBatch = serde_json::from_slice(payload)?;
        if &batch.client_id != client_id {
            warn!(%client_id, "batch client_id mismatch; dropping");
            return Ok(());
        }
        for capture in &batch.captures {
            // Pair with the oldest pending frame (FIFO — frames and metadata
            // are produced in the same order).
            let frame = pending.pop_front();
            if let Some(f) = frame {
                let ext = if f.codec_id == RDPGFX_CODECID_JPEG { "jpg" } else { "raw" };
                let ts = capture.timestamp_100ns as i64;
                let date = filetime_to_ymd(ts);
                store.objects.put(
                    &client_id.to_string(),
                    date,
                    &capture.image_token,
                    ext,
                    &f.data,
                )?;
            }
            let wc_id = store.ukg.insert_capture(capture)?;
            let _ = self.live_tx.send(LiveEvent {
                client_id: *client_id,
                window_capture_id: wc_id,
                image_token: Some(capture.image_token.clone()),
                window_title: Some(capture.window_title.clone()),
                timestamp_100ns: capture.timestamp_100ns as i64,
            });
        }
        Ok(())
    }

    /// PCAUDIO payload: `[u16 header_len][json {token}][opus bytes]`.
    fn handle_audio(
        &self,
        client_id: &uuid::Uuid,
        payload: &[u8],
        store: &ClientStore,
    ) -> Result<(), BrokerError> {
        if payload.len() < 2 {
            return Ok(());
        }
        let hlen = u16::from_le_bytes([payload[0], payload[1]]) as usize;
        if payload.len() < 2 + hlen {
            return Ok(());
        }
        #[derive(serde::Deserialize)]
        struct AudioHead {
            token: String,
            #[serde(default)]
            timestamp_100ns: Option<i64>,
        }
        let head: AudioHead = serde_json::from_slice(&payload[2..2 + hlen])?;
        let data = &payload[2 + hlen..];
        if data.is_empty() {
            return Ok(());
        }
        let date = filetime_to_ymd(head.timestamp_100ns.unwrap_or_else(|| filetime_now() as i64));
        store.objects.put_audio(&client_id.to_string(), date, &head.token, data)?;
        Ok(())
    }

    fn handle_ctrl(
        &self,
        payload: &[u8],
    ) -> Result<(), BrokerError> {
        match serde_json::from_slice::<ControlMessage>(payload) {
            Ok(ControlMessage::Heartbeat { frames_sent, queue_depth, .. }) => {
                debug!(frames_sent, queue_depth, "heartbeat");
            }
            Ok(ControlMessage::PanicWipe) | Ok(ControlMessage::Goodbye { .. }) => {}
            Ok(_) => {}
            Err(e) => warn!("bad control message: {e}"),
        }
        Ok(())
    }
}

/// One finished frame awaiting its metadata record.
struct PendingFrame {
    codec_id: u16,
    data: bytes::Bytes,
}

/// Accumulates START/W2S*/END state.
struct FrameAssembler {
    current_id: Option<u32>,
    timestamp: u32,
    regions: Vec<(u16, Rect, bytes::Bytes)>,
}

impl FrameAssembler {
    fn new() -> Self {
        Self { current_id: None, timestamp: 0, regions: Vec::new() }
    }
    fn begin(&mut self, frame_id: u32, timestamp: u32) {
        self.current_id = Some(frame_id);
        self.timestamp = timestamp;
        self.regions.clear();
    }
    fn region(&mut self, codec_id: u16, rect: Rect, data: bytes::Bytes) {
        if self.current_id.is_some() {
            self.regions.push((codec_id, rect, data));
        }
    }
    /// Single-JPEG-region frames (snapshot mode) yield the JPEG verbatim;
    /// multi-region frames yield the concatenated region payloads — decoding
    /// non-JPEG codecs is a later concern.
    fn finish(&mut self, frame_id: u32) -> Option<PendingFrame> {
        if self.current_id != Some(frame_id) {
            return None;
        }
        self.current_id = None;
        let (codec_id, data) = if self.regions.len() == 1 {
            let (c, _, d) = self.regions.pop().unwrap();
            (c, d)
        } else {
            let mut blob = bytes::BytesMut::new();
            let mut codec = RDPGFX_CODECID_UNCOMPRESSED;
            for (c, r, d) in self.regions.drain(..) {
                codec = c;
                blob.extend_from_slice(&r.left.to_le_bytes());
                blob.extend_from_slice(&r.top.to_le_bytes());
                blob.extend_from_slice(&r.right.to_le_bytes());
                blob.extend_from_slice(&r.bottom.to_le_bytes());
                blob.extend_from_slice(&(d.len() as u32).to_le_bytes());
                blob.extend_from_slice(&d);
            }
            (codec, blob.freeze())
        };
        Some(PendingFrame { codec_id, data })
    }
}

async fn write_reject<W: tokio::io::AsyncWrite + Unpin>(w: &mut W, reason: &str) -> std::io::Result<()> {
    #[derive(Serialize)]
    struct Reject {
        error: String,
    }
    let body = serde_json::to_vec(&Reject { error: reason.into() }).unwrap();
    tokio::io::AsyncWriteExt::write_u32_le(w, body.len() as u32).await?;
    tokio::io::AsyncWriteExt::write_all(w, &body).await?;
    tokio::io::AsyncWriteExt::flush(w).await
}

struct SessionGuard<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for SessionGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// 100ns ticks since 1601 (FILETIME) — shared wire timestamp lives in proto.
pub fn filetime_now() -> u64 {
    precall_proto::filetime_now()
}

/// FILETIME → (year, month, day) for object-store paths — delegates to the
/// shared layout helper so broker and API always agree.
pub fn filetime_to_ymd(ticks: i64) -> (i32, u32, u32) {
    precall_store::objects::ymd_from_filetime(ticks)
}


