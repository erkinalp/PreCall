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
}

/// Main loop: runs until `stop` flips. Reconnects forever with bounded
/// backoff — a capture agent must outlive network blips.
pub async fn run(cfg: ClientConfig, mut stop: watch::Receiver<bool>) -> Result<(), ClientError> {
    let exclusions = Exclusions::new(&cfg.excluded_processes, &cfg.excluded_domains);
    let audio_stop = Arc::new(AtomicBool::new(false));

    // Capture + audio threads live for the whole run; frames buffer in
    // their channels while we're reconnecting.
    let frame_rx = spawn_capture(0);
    let audio_rx = if cfg.enable_audio {
        Some(spawn_loopback(audio_stop.clone()))
    } else {
        None
    };

    // Bridge std::mpsc → tokio without blocking the runtime.
    let (frame_tx, mut frame_chan) = tokio::sync::mpsc::channel::<crate::dxgi::CaptureFrame>(4);
    let (audio_tx, mut audio_chan) =
        tokio::sync::mpsc::channel::<crate::audio::AudioChunk>(8);
    let frame_bridge = tokio::task::spawn_blocking(move || loop {
        match frame_rx.recv() {
            Ok(Ok(f)) => {
                if frame_tx.blocking_send(f).is_err() {
                    return;
                }
            }
            Ok(Err(e)) => {
                tracing::warn!("capture thread: {e}");
                return;
            }
            Err(_) => return,
        }
    });
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

        let mut capture_tick =
            tokio::time::interval(std::time::Duration::from_millis(cfg.capture_interval_ms.max(200)));
        let mut hb_tick =
            tokio::time::interval(std::time::Duration::from_millis(conn.hello.heartbeat_interval_ms.max(5000) as u64));
        let mut latest_frame: Option<crate::dxgi::CaptureFrame> = None;
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
                    // Dedup by cheap hash — DXGI can still deliver identical
                    // frames around mode changes.
                    let hash = cheap_hash(&frame.bgra);
                    if hash == last_hash && frame.dirty_regions == 0 {
                        counters.frames_skipped_unchanged += 1;
                        continue;
                    }
                    last_hash = hash;

                    let meta = foreground_meta();
                    let protected = is_protected_window(
                        windows::Win32::Foundation::HWND(meta.hwnd as *mut _)
                    );
                    if let Some(reason) = check(Some(&meta), &exclusions, protected) {
                        if reason != DropReason::NoWindow {
                            counters.frames_skipped_privacy += 1;
                        }
                        continue;
                    }

                    // Encode first — if JPEG fails we skip cleanly.
                    let jpeg = match encode_jpeg(&frame.bgra, frame.width, frame.height, 80) {
                        Ok(j) => j,
                        Err(e) => {
                            tracing::warn!("jpeg: {e}");
                            continue;
                        }
                    };
                    let regions = if cfg.enable_ocr {
                        ocr_bgra(&frame.bgra, frame.width, frame.height).unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    let urls = if meta.pid != 0 {
                        browser_url(windows::Win32::Foundation::HWND(meta.hwnd as *mut _))
                    } else {
                        Vec::new()
                    };
                    let files = explorer_paths();
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
                    if let Err(e) = conn.send_frame(&jpeg, frame.width, frame.height, now as u64).await {
                        tracing::warn!("frame send: {e}");
                        break;
                    }
                    if let Err(e) = conn.send_meta(rec).await {
                        tracing::warn!("meta send: {e}");
                        break;
                    }
                    counters.frames_sent += 1;
                }
                _ = hb_tick.tick() => {
                    if let Err(e) = conn.send_ctrl(&ControlMessage::Heartbeat {
                        frames_sent: counters.frames_sent,
                        frames_skipped_unchanged: counters.frames_skipped_unchanged,
                        queue_depth: pending.len() as u32,
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
    frame_bridge.abort();
    if let Some(b) = audio_bridge {
        b.abort();
    }
    Ok(())
}

/// Cheap frame-dedup hash over sampled bytes.
fn cheap_hash(bgra: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for chunk in bgra.chunks(4096) {
        for &b in chunk.iter().take(16) {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
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
