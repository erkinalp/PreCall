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

/// Recording of composition threads, **off unless a test turns it on**.
///
/// Runtime-placement is asserted on this rather than on wall-clock tick gaps,
/// which depend on host load. It must cost nothing when disabled: a production
/// broker would otherwise grow this vector forever (one entry per composed
/// frame) and take a global lock on every composition.
#[doc(hidden)]
pub static COMPOSE_RECORDING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[doc(hidden)]
pub static COMPOSE_THREADS: std::sync::Mutex<Vec<std::thread::ThreadId>> =
    std::sync::Mutex::new(Vec::new());

/// How many thread ids to retain even while recording: enough for a test, and
/// a hard bound in case one forgets to turn recording off.
const COMPOSE_THREADS_MAX: usize = 64;

fn record_compose_thread() {
    if !COMPOSE_RECORDING.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let mut seen = COMPOSE_THREADS.lock().unwrap();
    if seen.len() < COMPOSE_THREADS_MAX {
        seen.push(std::thread::current().id());
    }
}

/// All per-client storage handles in one place.
pub struct ClientStore {
    pub ukg: Ukg,
    pub objects: ObjectStore,
    pub text_index: SemanticIndex,
    pub image_index: SemanticIndex,
    /// The client's desktop surface, maintained only once a client proves it
    /// sends dirty rectangles. A full-frame-only client never pays a decode:
    /// its frames are archived verbatim, exactly as before.
    pub canvas: std::sync::Arc<std::sync::Mutex<Option<crate::canvas::Canvas>>>,
    /// The surface the client advertised in its hello. A one-rectangle frame is
    /// only a keyframe if that rectangle covers this surface.
    pub display: Display,
    /// Set when a delta could not be applied, or a keyframe could not be
    /// decoded: the client's later deltas are relative to that lost frame, so
    /// composing them onto an older surface would leave the areas it changed
    /// permanently stale. Only a keyframe that decodes clears it.
    pub surface_invalid: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// `DisplayInfo::surface_id` from the hello: the surface the canvas models.
    ///
    /// One canvas is kept per client, so a frame from a *different* surface
    /// cannot be composed onto it — multi-surface clients are not modelled yet,
    /// and such a frame is stored as an honest region blob rather than
    /// corrupting the archive.
    pub primary_surface: u16,
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
        Ok(ClientStore {
            ukg,
            objects,
            text_index,
            image_index,
            canvas: std::sync::Arc::new(std::sync::Mutex::new(None)),
            display: Display::Unknown,
            primary_surface: 0,
            surface_invalid: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
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
        let mut store = self.open_store(hello.client_id)?;
        if let Some(display) = hello.capabilities.displays.first() {
            store.primary_surface = display.surface_id;
            store.display = if display.width == 0 || display.height == 0 {
                Display::Unknown
            } else {
                Display::Surface {
                    width: display.width,
                    height: display.height,
                }
            };
        }
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
                        ChannelId::PcMeta => self
                            .handle_meta(&hello.client_id, &frame.payload, &mut store, &mut pending)
                            .await?,
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
                GfxPdu::WireToSurface1 {
                    surface_id,
                    codec_id,
                    dest_rect,
                    bitmap_data,
                    ..
                } => {
                    frames.region(surface_id, codec_id, dest_rect, bitmap_data);
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

    async fn handle_meta(
        &self,
        client_id: &uuid::Uuid,
        payload: &[u8],
        // `&mut` rather than `&`: `ClientStore` holds a rusqlite connection
        // (`!Sync`), so a *shared* borrow held across an await would make the
        // session future `!Send`. `&mut T` is `Send` whenever `T` is, which lets
        // the offset composition run on the blocking pool.
        store: &mut ClientStore,
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
                let ts = capture.timestamp_100ns as i64;
                let date = filetime_to_ymd(ts);
                let cid = client_id.to_string();

                // What gets archived for this capture. Dirty rectangles are
                // applied to the client's surface and the *composed* desktop is
                // archived as an ordinary JPEG, so every reader — snapshot
                // route, image embedder, timeline, export — keeps seeing one
                // full-frame image per capture, exactly as Recall's own store
                // does. (RDPEGFX composes into a surface the same way.)
                // Exactly the advertised surface composes. `None` means the
                // frame's regions span several surfaces, which cannot go into one
                // canvas either — comparing the Option rather than the inner
                // value keeps both cases out.
                let composes = f.surface_id == Some(store.primary_surface);
                if !composes {
                    // A refused frame may contain primary-surface updates that are
                    // now lost, so the surface is no longer a valid base.
                    store
                        .surface_invalid
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    *store.canvas.lock().unwrap() = None;
                    warn!(
                        client = %client_id,
                        surface = ?f.surface_id,
                        primary = store.primary_surface,
                        "frame is not wholly from the advertised surface; storing the region blob"
                    );
                }
                let payload: Option<(&str, bytes::Bytes)> = if !composes {
                    Some(("raw", f.data.clone()))
                } else if is_full_surface(&f, store.display) {
                    // A keyframe: archive it verbatim, and keep the surface
                    // current from it when this client also sends rectangles.
                    // Decode when the client has a surface to refresh *or* when
                    // one was invalidated: a client that never sends deltas pays
                    // no decode, but a surface that was lost can only be recovered
                    // by a keyframe.
                    let needs_surface = store.canvas.lock().unwrap().is_some()
                        || store
                            .surface_invalid
                            .load(std::sync::atomic::Ordering::Relaxed);
                    if needs_surface {
                        // Decoding a full frame is image work: it goes to the
                        // blocking pool with the rest, never on the runtime
                        // worker that serves this session's reads and timers.
                        let canvas = std::sync::Arc::clone(&store.canvas);
                        let invalid = std::sync::Arc::clone(&store.surface_invalid);
                        let data = f.data.clone();
                        let cid_log = *client_id;
                        let expected = store.display;
                        let _ = tokio::task::spawn_blocking(move || {
                            record_compose_thread();
                            match crate::canvas::Canvas::from_jpeg(&data) {
                                Ok(c) if dimensions_match(expected, c.width(), c.height()) => {
                                    *canvas.lock().unwrap() = Some(c);
                                    invalid.store(false, std::sync::atomic::Ordering::Relaxed);
                                }
                                Ok(c) => {
                                    // A frame whose rectangle claims the surface
                                    // but whose image is a different size would
                                    // make every later delta clip into the wrong
                                    // canvas.
                                    *canvas.lock().unwrap() = None;
                                    invalid.store(true, std::sync::atomic::Ordering::Relaxed);
                                    warn!(
                                        client = %cid_log,
                                        image = ?(c.width(), c.height()),
                                        "keyframe size does not match the advertised surface, surface invalidated"
                                    );
                                }
                                Err(e) => {
                                    // Do not keep the old surface: the client's
                                    // later deltas are relative to *this* frame,
                                    // so composing them onto an older one would
                                    // leave every area it changed permanently
                                    // stale. Without a surface they are stored as
                                    // blobs until a keyframe that decodes.
                                    *canvas.lock().unwrap() = None;
                                    invalid.store(true, std::sync::atomic::Ordering::Relaxed);
                                    warn!(
                                        client = %cid_log,
                                        "keyframe not decodable, surface invalidated: {e}"
                                    );
                                }
                            }
                        })
                        .await;
                    }
                    Some(("jpg", f.data.clone()))
                } else {
                    // Every other frame is a delta, whatever its region count —
                    // one rectangle is a perfectly ordinary small edit. The
                    // compositor answers `None` when there is no surface to
                    // seed or the codec cannot be composed, and then the region
                    // blob is stored honestly rather than as a corrupt image.
                    //
                    // Seeding reads the previous capture (cheap, one row and one
                    // object), then the decode/blit/encode runs on the blocking
                    // pool: that encode is ~45 ms for a 1080p desktop, and doing
                    // it here would block this connection's runtime worker, so
                    // enough dirty-rectangle clients would starve socket reads
                    // and heartbeats for healthy sessions too.
                    // The object read is I/O and stays here; the *decode* of the
                    // base image is image work and goes to the blocking pool
                    // with the rectangles and the encode.
                    let invalid = store.surface_invalid.load(std::sync::atomic::Ordering::Relaxed);
                    let base = if !invalid && store.canvas.lock().unwrap().is_none() {
                        Self::last_capture_bytes(&*store, &cid)
                    } else {
                        None
                    };
                    let canvas = std::sync::Arc::clone(&store.canvas);
                    let surface_invalid = std::sync::Arc::clone(&store.surface_invalid);
                    let rects: Vec<(Rect, bytes::Bytes)> = f.rects.clone();
                    let codec_id = f.codec_id;
                    // A surface known to be invalid (a lost keyframe or a refused
                    // frame) is not seeded from an older image: the deltas are
                    // relative to the lost state, so that would publish a stale
                    // composition. Wait for a keyframe.
                    let composed = if invalid {
                        None
                    } else {
                        let expected = store.display;
                        match tokio::task::spawn_blocking(move || {
                            compose(canvas, codec_id, &rects, base, expected)
                        })
                        .await
                        {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("composition task failed: {e}");
                                None
                            }
                        }
                    };
                    match composed {
                        Some(jpeg) => Some(("jpg", bytes::Bytes::from(jpeg))),
                        None => {
                            // Cannot apply this delta: whatever the cause, the
                            // surface must not be reused, because the next delta is
                            // relative to *this* frame.
                            surface_invalid.store(true, std::sync::atomic::Ordering::Relaxed);
                            *store.canvas.lock().unwrap() = None;
                            Some(("raw", f.data.clone()))
                        }
                    }
                };

                if let Some((ext, bytes)) = payload {
                    store.objects.put(&cid, date, &capture.image_token, ext, &bytes)?;
                }
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

    /// The archived bytes of the desktop as of the previous capture, for seeding
    /// a surface that does not exist yet.
    ///
    /// Reading it here (rather than decoding every full frame up front) means a
    /// client that only ever sends full frames never pays a decode at all: the
    /// surface is built the moment it is needed.
    fn last_capture_bytes(store: &ClientStore, client_id: &str) -> Option<bytes::Bytes> {
        // Walk back to the newest capture that actually has an archived *image*:
        // a capture stored as a region blob (composition failed, or the client
        // was paused) has no JPEG, and stopping at it would leave the client
        // permanently unable to seed a surface — every later delta degraded to a
        // blob. Bounded, so a long run of blobs cannot turn this into a scan.
        const LOOKBACK: i64 = 64;
        for entry in store.ukg.timeline(None, LOOKBACK).ok()? {
            let Some(token) = store.ukg.image_token(entry.id).ok().flatten() else {
                continue;
            };
            let rel = ObjectStore::key_path(
                client_id,
                filetime_to_ymd(entry.timestamp_100ns),
                &token,
                "jpg",
            );
            if !store.objects.exists(&rel) {
                continue;
            }
            match store.objects.get(client_id, &rel, &token) {
                Ok(bytes) => {
                    debug!(
                        client_id,
                        capture = entry.id,
                        "surface seeded from an archived frame"
                    );
                    return Some(bytes.into());
                }
                Err(e) => {
                    debug!(client_id, capture = entry.id, "cannot read seed frame: {e}");
                    continue;
                }
            }
        }
        None
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

/// Is this frame the whole desktop (a keyframe), rather than a delta?
///
/// Counting regions is *not* enough: a small edit is commonly a single
/// rectangle, and mistaking it for a keyframe archived the patch as the capture
/// and replaced the surface with a patch-sized image, corrupting every later
/// composition. The test is geometric — one rectangle that starts at or before
/// the origin and reaches the advertised display size.
///
/// A client that advertises no display size (the field is optional in practice)
/// keeps the old behaviour: one JPEG region is treated as a full frame, with a
/// warning, because guessing "delta" there would compose a patch onto a surface
/// that was never seeded.
fn is_full_surface(frame: &PendingFrame, display: Display) -> bool {
    if frame.codec_id != RDPGFX_CODECID_JPEG || frame.rects.len() != 1 {
        return false;
    }
    let rect = frame.rects[0].0;
    match display {
        Display::Unknown => true,
        Display::Surface { width, height } => {
            // Signed arithmetic on purpose: casting a negative coordinate to
            // `u32` wraps it to a huge value that satisfies any `>=` test.
            rect.left <= 0
                && rect.top <= 0
                && rect.right as i32 >= width as i32
                && rect.bottom as i32 >= height as i32
        }
    }
}

/// The surface a client told us it captures.
///
/// Rectangles on the wire are **output-local** (the client sends
/// `(0, 0, width, height)` for a whole frame, with the size from the same
/// output), so the display's desktop *origin* is not applied to them. If a
/// client ever starts sending virtual-desktop coordinates, keyframe detection
/// and composition have to become origin-aware together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Display {
    /// A client that did not say (it may not know yet): a single region is
    /// treated as a keyframe, with a warning, because guessing "delta" would
    /// compose a patch onto a surface that was never seeded.
    Unknown,
    Surface { width: u32, height: u32 },
}

/// Does a decoded image match the surface the client advertised?
///
/// A client that advertised nothing (`Display::Unknown`) cannot be checked, and
/// is accepted as before.
fn dimensions_match(display: Display, width: u32, height: u32) -> bool {
    match display {
        Display::Unknown => true,
        Display::Surface { width: w, height: h } => width == w && height == h,
    }
}

/// Quality of the composed archive frame.
const COMPOSED_JPEG_QUALITY: u8 = 85;

/// Apply rectangles to the client's surface and encode the result, on the
/// blocking pool.
///
/// `None` means "cannot compose this frame" — no surface (nothing to seed from),
/// a codec that cannot be composed, or a rectangle that does not match its
/// payload — and the caller then stores the region blob rather than pretending
/// it is an image.
fn compose(
    canvas: std::sync::Arc<std::sync::Mutex<Option<crate::canvas::Canvas>>>,
    codec_id: u16,
    rects: &[(Rect, bytes::Bytes)],
    base: Option<bytes::Bytes>,
    expected: Display,
) -> Option<Vec<u8>> {
    record_compose_thread();
    if codec_id != RDPGFX_CODECID_JPEG {
        warn!(
            codec = codec_name(codec_id),
            "region codec cannot be composed; storing the blob"
        );
        return None;
    }
    // Validate every region *before* touching the surface, so a bad rectangle
    // cannot leave a half-applied canvas behind for the next archive frame.
    let mut decoded = Vec::with_capacity(rects.len());
    for (rect, data) in rects {
        match crate::canvas::Canvas::decode_region(rect.left, rect.top, rect.right, rect.bottom, data)
        {
            Ok(img) => decoded.push((*rect, img)),
            Err(e) => {
                warn!("region refused: {e}");
                return None;
            }
        }
    }
    let mut guard = canvas.lock().unwrap();
    if guard.is_none() {
        // No surface yet: build it from the previous capture's archive frame.
        let base = match base {
            Some(base) => base,
            None => {
                debug!("dirty rects before any full frame; storing the region blob");
                return None;
            }
        };
        match crate::canvas::Canvas::from_jpeg(&base) {
            Ok(seeded) if dimensions_match(expected, seeded.width(), seeded.height()) => {
                *guard = Some(seeded)
            }
            Ok(seeded) => {
                warn!(
                    image = ?(seeded.width(), seeded.height()),
                    "archived frame does not match the advertised surface; not seeding from it"
                );
                return None;
            }
            Err(e) => {
                warn!("cannot seed the surface from the previous capture: {e}");
                return None;
            }
        }
    }
    let canvas = guard.as_mut()?;
    for (rect, img) in &decoded {
        canvas.blit(rect.left, rect.top, img);
    }
    match canvas.encode_jpeg(COMPOSED_JPEG_QUALITY) {
        Ok(jpeg) => Some(jpeg),
        Err(e) => {
            warn!("compose encode failed: {e}");
            None
        }
    }
}

/// One finished frame awaiting its metadata record.
struct PendingFrame {
    codec_id: u16,
    /// The wire payload: the codec's own stream for a single region, or the
    /// `[l,t,r,b,len][data]…` concatenation for several. Which one it is is
    /// decided by [`is_full_surface`], not by the region count.
    data: bytes::Bytes,
    /// The rectangles and their payloads, when this frame is a dirty-rect
    /// update — what the compositor needs.
    rects: Vec<(Rect, bytes::Bytes)>,
    /// Surface the rectangles came from. A frame whose rectangles span several
    /// surfaces cannot be composed into the single canvas.
    surface_id: Option<u16>,
}

/// Accumulates START/W2S*/END state.
struct FrameAssembler {
    current_id: Option<u32>,
    timestamp: u32,
    regions: Vec<(u16, u16, Rect, bytes::Bytes)>,
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
    fn region(&mut self, surface_id: u16, codec_id: u16, rect: Rect, data: bytes::Bytes) {
        if self.current_id.is_some() {
            self.regions.push((surface_id, codec_id, rect, data));
        }
    }
    /// Single-JPEG-region frames (snapshot mode) yield the JPEG verbatim;
    /// multi-region frames yield their rectangles separately (plus the legacy
    /// concatenation, for callers that cannot compose).
    ///
    /// The region count travels with the payload because it decides how the
    /// object may be labelled: the concatenation is **not** a JPEG, whatever
    /// codec the regions used.
    fn finish(&mut self, frame_id: u32) -> Option<PendingFrame> {
        if self.current_id != Some(frame_id) {
            return None;
        }
        self.current_id = None;
        let rects: Vec<(Rect, bytes::Bytes)> = self
            .regions
            .iter()
            .map(|(_, _, r, d)| (*r, d.clone()))
            .collect();
        // A frame whose regions come from more than one surface cannot be
        // composed into the single canvas, so it is not "a surface" at all.
        let surfaces: Vec<u16> = self.regions.iter().map(|(s, _, _, _)| *s).collect();
        let surface_id = if surfaces.iter().all(|s| *s == surfaces[0]) {
            surfaces.first().copied()
        } else {
            None
        };
        let (codec_id, data) = if self.regions.len() == 1 {
            let (_, c, _, d) = self.regions.pop().unwrap();
            (c, d)
        } else {
            let mut blob = bytes::BytesMut::new();
            let mut codec = RDPGFX_CODECID_UNCOMPRESSED;
            for (_, c, r, d) in self.regions.drain(..) {
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
        Some(PendingFrame {
            codec_id,
            data,
            rects,
            surface_id,
        })
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


