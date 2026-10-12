// SPDX-License-Identifier: GPL-2.0-only
//! The capture loop. Owns the reconnect/backoff policy and ties together
//! dxgi → privacy → jpeg → ocr → meta → session.

use crate::audio::spawn_loopback;
use crate::config::ClientConfig;
use crate::dxgi::spawn_capture;
use crate::jpeg::encode_jpeg;
use crate::meta::{browser_url, explorer_paths, foreground_meta, is_protected_window};
use crate::ocr::ocr_bgra;
use crate::privacy::{check, set_paused, DropReason, Exclusions};
use crate::session::Connection;
use crate::ClientError;
use futures_util::StreamExt;
use precall_proto::metadata::{ControlMessage, WindowCaptureRecord};
use precall_proto::{filetime_now, ChannelId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

/// Frames/skipped counters for PCCTRL Heartbeat.
#[derive(Default)]
pub struct Counters {
    pub frames_sent: u64,
    pub frames_skipped_unchanged: u64,
    pub frames_skipped_privacy: u64,
    /// Frames the capture thread produced but the pipeline never carried —
    /// the honest measure of "this link/core cannot hold the cadence".
    pub frames_dropped: u64,
}

/// Result of one capture's offloaded preparation.
enum Prepared {
    /// Privacy gate: no foreground window, so there is nothing to capture.
    NoWindow,
    /// Privacy gate: an excluded window, site, or protected surface.
    Dropped,
    Frame(Box<PreparedFrame>),
}

/// What this capture puts on the wire.
enum Payload {
    /// The whole frame — a keyframe, or a change too large to be worth deltas.
    Full {
        jpeg: Vec<u8>,
        width: u32,
        height: u32,
    },
    /// Only the rectangles that changed.
    Regions(Vec<(precall_frame::Rect, Vec<u8>)>),
}

/// Everything one capture needs, produced off the runtime thread.
struct PreparedFrame {
    payload: Payload,
    meta: crate::meta::WindowMeta,
    regions: Vec<precall_proto::metadata::ScreenRegionRecord>,
    urls: Vec<precall_proto::metadata::WebRecord>,
    files: Vec<precall_proto::metadata::FileRecord>,
}

/// Main loop: runs until `stop` flips. Reconnects forever with bounded
/// backoff — a capture agent must outlive network blips.
pub async fn run(cfg: ClientConfig, mut stop: watch::Receiver<bool>) -> Result<(), ClientError> {
    let exclusions = Exclusions::new(&cfg.excluded_processes, &cfg.excluded_domains);
    let audio_stop = Arc::new(AtomicBool::new(false));

    // Capture + audio threads live for the whole run. Frames do **not** buffer:
    // the capture thread overwrites a single slot, so a slow pipeline drops
    // frames instead of accumulating a backlog (and a desktop from minutes ago).
    let mut frames = spawn_capture(0, |e| tracing::warn!("capture thread: {e}"));
    let audio_rx = if cfg.enable_audio {
        Some(spawn_loopback(audio_stop.clone()))
    } else {
        None
    };

    // Bridge the blocking slot into the runtime. Capacity 2 bounds memory at
    // slot(1) + channel(2) frames however fast the display updates.
    let mut frame_chan = frame_bridge(&frames);
    let (audio_tx, mut audio_chan) =
        tokio::sync::mpsc::channel::<crate::audio::AudioChunk>(8);
    let audio_enabled = audio_rx.is_some();
    let audio_bridge = audio_rx.map(|rx| {
        tokio::task::spawn_blocking(move || loop {
            match rx.recv() {
                Ok(Ok(c)) => {
                    if audio_tx.blocking_send(c).is_err() {
                        return;
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!("audio thread: {e}");
                    return;
                }
                Err(_) => return,
            }
        })
    });

    let mut backoff = std::time::Duration::from_millis(500);
    let mut counters = Counters::default();
    let mut last_hash = 0u64;

    loop {
        if *stop.borrow() {
            break;
        }
        // A capture worker that exited (duplication lost, and the one recreate
        // attempt failed) closes its slot; without this the client would keep
        // reconnecting and heartbeating while capturing nothing at all.
        if frames.is_closed() {
            tracing::warn!("capture worker gone; restarting it");
            frames = spawn_capture(0, |e| tracing::warn!("capture thread: {e}"));
            frame_chan = frame_bridge(&frames);
        }
        let conn = match Connection::connect(&cfg).await {
            Ok(c) => {
                backoff = std::time::Duration::from_millis(500);
                tracing::info!("connected; session {}", c.hello.session_id);
                Some(c)
            }
            Err(e) => {
                tracing::warn!("connect failed: {e}; retrying in {:?}", backoff);
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = stop.changed() => { break; }
                }
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                None
            }
        };
        let Some(mut conn) = conn else { continue };

        // One policy for both operating points: 16 ms streaming, 3000 ms for
        // Recall's collection cadence. The old floor here was 200 ms, i.e. a
        // hard 5 captures/s ceiling however the client was configured.
        let mut capture_tick = tokio::time::interval(std::time::Duration::from_millis(
            precall_frame::capture_interval_ms(cfg.capture_interval_ms),
        ));
        let mut hb_tick =
            tokio::time::interval(std::time::Duration::from_millis(conn.hello.heartbeat_interval_ms.max(5000) as u64));
        let mut latest_frame: Option<crate::dxgi::CaptureFrame> = None;
        // A keyframe first: the server has no surface for this connection, and
        // a reconnected client's surface died with its session.
        let mut force_keyframe = true;
        let mut pending: std::collections::VecDeque<u64> = std::collections::VecDeque::new();

        loop {
            tokio::select! {
                _ = stop.changed() => break,
                f = frame_chan.recv() => {
                    match f {
                        Some(fr) => latest_frame = Some(fr),
                        None => { tracing::error!("capture thread died"); break; }
                    }
                }
                a = audio_chan.recv(), if audio_enabled => {
                    if let Some(chunk) = a {
                        if let Err(e) = conn.send_audio(&chunk.pcm16, chunk.channels, chunk.sample_rate).await {
                            tracing::warn!("audio send: {e}");
                            break;
                        }
                    }
                }
                _ = capture_tick.tick() => {
                    let Some(frame) = latest_frame.take() else { continue };
                    // When DXGI gives us dirty rectangles they are the
                    // authoritative change signal (and an empty list means the
                    // desktop did not change), so the 1.18 ms fingerprint is
                    // skipped. It still guards the fallback path, where
                    // metadata is unavailable: DXGI redelivers an identical
                    // desktop around mode changes, and a moved pointer changes
                    // metadata but no pixels.
                    if frame.dirty.is_none() {
                        let hash = precall_frame::frame_fingerprint(&frame.bgra);
                        if hash == last_hash {
                            counters.frames_skipped_unchanged += 1;
                            continue;
                        }
                        last_hash = hash;
                    }

                    // Everything below is per-capture CPU — 58 ms for a
                    // desktop-like 1080p frame and 117 ms for photographic
                    // content (measured, `precall-frame/examples/client_cost`).
                    // It runs on the blocking pool: on the reactor it would
                    // stall this loop, and the whole point of the cadence is
                    // that the *next* capture is decided on time.
                    // What to send for this frame. The policy is pure and
                    // tested (`precall_frame::plan_capture`); unreadable
                    // metadata and scrolled frames force a keyframe, a fresh
                    // connection always starts with one (the server has no
                    // surface for this session yet).
                    let plan = match (&frame.dirty, frame.moved) {
                        (Some(dirty), false) if !force_keyframe => precall_frame::plan_capture(
                            dirty,
                            false,
                            (frame.width as i64) * (frame.height as i64),
                            false,
                        ),
                        _ => precall_frame::CapturePlan::Keyframe,
                    };
                    if plan == precall_frame::CapturePlan::Skip {
                        counters.frames_skipped_unchanged += 1;
                        continue;
                    }

                    let prepare = {
                        let exclusions = exclusions.clone();
                        let enable_ocr = cfg.enable_ocr;
                        let region_plan = plan.clone();
                        let (bgra, width, height) = (frame.bgra, frame.width, frame.height);
                        tokio::task::spawn_blocking(move || -> Result<Prepared, crate::ClientError> {
                            let meta = foreground_meta();
                            let protected = is_protected_window(
                                windows::Win32::Foundation::HWND(meta.hwnd as *mut _)
                            );
                            match check(Some(&meta), &exclusions, protected) {
                                None => {}
                                Some(DropReason::NoWindow) => return Ok(Prepared::NoWindow),
                                Some(_) => return Ok(Prepared::Dropped),
                            }
                            // The CPU win: a delta encodes a few small
                            // rectangles instead of 8 MiB of pixels.
                            let payload = match &region_plan {
                                precall_frame::CapturePlan::Regions(rects) => {
                                    let mut out = Vec::with_capacity(rects.len());
                                    for r in rects {
                                        if let Some((region, rw, rh)) = precall_frame::extract_bgra_region(
                                            &bgra, width, height, r,
                                        ) {
                                            if let Ok(jpeg) = encode_jpeg(&region, rw, rh, 80) {
                                                out.push((*r, jpeg));
                                            }
                                        }
                                    }
                                    if out.is_empty() {
                                        Payload::Full {
                                            jpeg: encode_jpeg(&bgra, width, height, 80)?,
                                            width,
                                            height,
                                        }
                                    } else {
                                        Payload::Regions(out)
                                    }
                                }
                                _ => Payload::Full {
                                    jpeg: encode_jpeg(&bgra, width, height, 80)?,
                                    width,
                                    height,
                                },
                            };
                            let regions = if enable_ocr {
                                ocr_bgra(&bgra, width, height).unwrap_or_default()
                            } else {
                                Vec::new()
                            };
                            let urls = if meta.pid != 0 {
                                browser_url(windows::Win32::Foundation::HWND(meta.hwnd as *mut _))
                            } else {
                                Vec::new()
                            };
                            let files = explorer_paths();
                            Ok(Prepared::Frame(Box::new(PreparedFrame {
                                payload,
                                meta,
                                regions,
                                urls,
                                files,
                            })))
                        })
                        .await
                    };
                    let prepared = match prepare {
                        Ok(Ok(Prepared::Frame(f))) => *f,
                        Ok(Ok(Prepared::Dropped)) => {
                            counters.frames_skipped_privacy += 1;
                            continue;
                        }
                        // No foreground window is the ordinary case, not a drop.
                        Ok(Ok(Prepared::NoWindow)) => continue,
                        Ok(Err(e)) => {
                            tracing::warn!("jpeg: {e}");
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!("capture preparation panicked: {e}");
                            continue;
                        }
                    };
                    let PreparedFrame { payload, meta, regions, urls, files } = prepared;
                    let token = uuid::Uuid::new_v4().to_string();
                    let now = filetime_now();
                    pending.push_back(now);
                    if pending.len() > 64 {
                        pending.pop_front();
                    }
                    let rec = WindowCaptureRecord {
                        name: meta.capture_name(),
                        image_token: token,
                        is_foreground: true,
                        window_id: meta.hwnd as i64,
                        window_bounds: format!("{},{},{},{}", meta.bounds.left, meta.bounds.top, meta.bounds.right, meta.bounds.bottom),
                        window_title: meta.title.clone(),
                        properties: None,
                        timestamp_100ns: now,
                        activation_uri: meta.activation_uri(),
                        activity_id: meta.aumid.clone(),
                        fallback_uri: meta.fallback_uri(),
                        apps: meta.app_record().into_iter().collect(),
                        files,
                        webs: urls,
                        regions,
                    };
                    let sent = match &payload {
                        Payload::Full { jpeg, width, height } => {
                            conn.send_frame(jpeg, *width, *height, now as u64).await
                        }
                        Payload::Regions(rs) => conn.send_regions(rs, now as u64).await,
                    };
                    if let Err(e) = sent {
                        tracing::warn!("frame send: {e}");
                        break;
                    }
                    if let Err(e) = conn.send_meta(rec).await {
                        tracing::warn!("meta send: {e}");
                        break;
                    }
                    counters.frames_sent += 1;
                    // From here the server holds a surface for this session.
                    force_keyframe = false;
                }
                _ = hb_tick.tick() => {
                    counters.frames_dropped = frames.dropped();
                    if let Err(e) = conn.send_ctrl(&ControlMessage::Heartbeat {
                        frames_sent: counters.frames_sent,
                        frames_skipped_unchanged: counters.frames_skipped_unchanged,
                        queue_depth: pending.len() as u32,
                        frames_dropped: counters.frames_dropped,
                    }).await {
                        tracing::warn!("heartbeat: {e}");
                        break;
                    }
                }
                // Inbound control: pause/resume/wipe.
                msg = next_ctrl(&mut conn) => {
                    match msg {
                        Ok(Some(ControlMessage::Pause)) => set_paused(true),
                        Ok(Some(ControlMessage::Resume{ .. })) => set_paused(false),
                        Ok(Some(ControlMessage::PanicWipe)) => {
                            // The client holds no history — acknowledge by
                            // flushing the pending queue and continuing.
                            pending.clear();
                            set_paused(true);
                        }
                        Ok(Some(ControlMessage::Goodbye{..})) => break,
                        Ok(Some(_)) | Ok(None) => {}
                        Err(_) => break,
                    }
                }
            }
        }
        // Connection dropped or stopped — loop back to reconnect unless
        // the stop signal fired.
    }

    audio_stop.store(true, Ordering::Relaxed);
    if let Some(b) = audio_bridge {
        b.abort();
    }
    Ok(())
}

/// Pump the blocking latest-frame slot into the async send loop.
///
/// The returned receiver yields `None` exactly when the capture worker closes
/// the slot, which is the signal that the loop must abandon this session.
fn frame_bridge(
    frames: &std::sync::Arc<precall_frame::LatestSlot<crate::dxgi::CaptureFrame>>,
) -> tokio::sync::mpsc::Receiver<crate::dxgi::CaptureFrame> {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let slot = std::sync::Arc::clone(frames);
    tokio::task::spawn_blocking(move || loop {
        match slot.recv() {
            Some(f) => {
                if tx.blocking_send(f).is_err() {
                    return;
                }
            }
            // Producer gone: dropping the sender wakes the select arm.
            None => return,
        }
    });
    rx
}

/// Non-blocking read of the next control message on the wire.
async fn next_ctrl(conn: &mut Connection) -> Result<Option<ControlMessage>, ClientError> {
    use tokio::time::{timeout, Duration};
    match timeout(Duration::from_millis(50), conn.framed.next()).await {
        Ok(Some(Ok(f))) if f.channel == ChannelId::PcCtrl => {
            Ok(Some(serde_json::from_slice(&f.payload)?))
        }
        Ok(Some(Ok(_))) => Ok(None),
        Ok(Some(Err(e))) => Err(ClientError::Proto(e)),
        Ok(None) => Err(ClientError::Config("server closed connection".into())),
        Err(_) => Ok(None),
    }
}
