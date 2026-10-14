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
    let (mut frames, mut geometry) = spawn_capture(0, |e| tracing::warn!("capture thread: {e}"));
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
    // How much of the slot's cumulative drop counter has already been reported.
    // It must outlive connections: the slot does, and resetting it would re-add
    // every historical overwrite to the next heartbeat.
    let mut slot_drops_reported = 0u64;
    loop {
        if *stop.borrow() {
            break;
        }
        // A capture worker that exited (duplication lost, and the one recreate
        // attempt failed) closes its slot; without this the client would keep
        // reconnecting and heartbeating while capturing nothing at all.
        if frames.is_closed() {
            tracing::warn!("capture worker gone; restarting it");
            let (new_frames, new_geometry) = spawn_capture(0, |e| tracing::warn!("capture thread: {e}"));
            frames = new_frames;
            geometry = new_geometry;
            frame_chan = frame_bridge(&frames);
            // The new slot's drop counter starts at zero: keeping the old
            // baseline would subtract an unrelated count and hide this worker's
            // early drops (hundreds during a reconnect backoff).
            slot_drops_reported = 0;
        }
        // The broker composes dirty rectangles against this size, so it must be
        // the real desktop, not zero (which used to make every single-rectangle
        // delta look like a keyframe). Prefer what the capture thread actually
        // selected: it scans adapters, so it can find a display that a
        // first-adapter query would miss.
        let capture_geometry = *geometry.lock().unwrap();
        let display = if capture_geometry.width > 0 {
            (capture_geometry.width, capture_geometry.height)
        } else {
            crate::dxgi::primary_output_size().unwrap_or((0, 0))
        };
        // The geometry the broker is told about for this connection. A display
        // mode change re-creates the duplicator, so the broker's surface must be
        // rebuilt at the new size.
        let advertised = display;
        let conn = match Connection::connect(&cfg, display).await {
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
        let mut latest_frame: Option<BridgedFrame> = None;
        // The snapshot above was taken *before* the handshake: the capture thread
        // may have created or re-created the duplicator while it was in flight,
        // in which case the current generation is the newer one and the frames we
        // just received belong to it. Re-read before deciding what to keep — and
        // declare the tracker *here*, where its value is authoritative, rather
        // than before the await only to overwrite it (which is a dead store).
        let capture_geometry = *geometry.lock().unwrap();
        let mut last_generation = capture_geometry.generation;
        retain_current_generation(&mut latest_frame, &mut frame_chan, last_generation);
        // Fingerprint of the last frame *successfully transmitted* through the
        // metadata-less path, and only for *this* session: the broker's new
        // session has no surface, so a previous session's hash must never
        // suppress the keyframe this one needs. `None` means "unknown" — what a
        // metadata-backed send leaves behind, since no fingerprint was computed
        // for the frame the broker now holds.
        let mut last_hash: Option<u64> = None;
        // Changes reported since the last capture we sent. DXGI reports them
        // relative to the previous *acquired* frame, so dropping any would apply
        // only the newest change to the server's surface and archive stale
        // screenshots. `None` means some frame's metadata was unreadable, which
        // forces a keyframe rather than quietly sending a partial delta.
        let mut pending_dirty: Option<Vec<precall_frame::Rect>> = Some(Vec::new());
        // A scroll seen since the last send: the wire has no surface-copy
        // operation, so the next frame goes whole.
        let mut pending_moved = false;
        // A keyframe first: the server has no surface for this connection, and
        // a reconnected client's surface died with its session.
        let mut force_keyframe = true;
        let mut pending: std::collections::VecDeque<u64> = std::collections::VecDeque::new();

        loop {
            // The desktop can be re-created under us (a display mode change
            // destroys DXGI duplication). A new generation means the previous
            // surface is not a valid base for a delta, so the next frame goes
            // whole; a size change means the broker's canvas is the wrong shape,
            // which only a fresh handshake can fix.
            {
                let current = *geometry.lock().unwrap();
                if current.generation != last_generation {
                    last_generation = current.generation;
                    // The desktop was re-created: the surface is not a valid
                    // base, and the forced keyframe is never deduplicated, so
                    // the fingerprint needs no reset here (a send clears it).
                    force_keyframe = true;
                    pending_dirty = Some(Vec::new());
                    pending_moved = false;
                    // A resident or queued frame from the old duplicator must not
                    // be sent as the new generation's keyframe.
                    retain_current_generation(&mut latest_frame, &mut frame_chan, last_generation);
                }
                if current.width > 0 && (current.width, current.height) != advertised {
                    tracing::info!(
                        "display is now {}x{} (was {}x{}); re-handshaking so the broker rebuilds its surface",
                        current.width, current.height, advertised.0, advertised.1
                    );
                    break;
                }
            }
            tokio::select! {
                _ = stop.changed() => break,
                f = frame_chan.recv() => {
                    match f {
                        Some(fr) => {
                            // Filtering here, not only at the transition: the
                            // bridge may already have been blocked in
                            // `blocking_send` with a frame from the previous
                            // duplicator, which lands *after* the drain. Sending
                            // it as the new generation's keyframe would leave every
                            // later delta composed onto an obsolete desktop.
                            if fr.0.generation != last_generation {
                                continue;
                            }
                            if let Some(prev) = latest_frame.replace(fr) {
                                absorb(&mut pending_dirty, &mut pending_moved, prev);
                                counters.frames_dropped += 1;
                            }
                            // Newest-only: anything already queued is older.
                            while let Ok(fr) = frame_chan.try_recv() {
                                if fr.0.generation != last_generation {
                                    continue;
                                }
                                if let Some(prev) = latest_frame.replace(fr) {
                                    absorb(&mut pending_dirty, &mut pending_moved, prev);
                                    counters.frames_dropped += 1;
                                }
                            }
                        }
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
                    let Some((frame, frame_dirty, frame_moved)) = latest_frame.take() else {
                        continue;
                    };
                    absorb_meta(&mut pending_dirty, &mut pending_moved, frame_dirty, frame_moved);
                    // When DXGI gives us dirty rectangles they are the
                    // authoritative change signal (and an empty list means the
                    // desktop did not change), so the 1.18 ms fingerprint is
                    // skipped. It still guards the fallback path, where
                    // metadata is unavailable: DXGI redelivers an identical
                    // desktop around mode changes, and a moved pointer changes
                    // metadata but no pixels.
                    // Computed only on the metadata-less path; recorded only
                    // after the frame is actually sent (below), so a dropped
                    // preparation cannot leave a hash for a frame the broker
                    // never received.
                    let fallback_hash = if pending_dirty.is_none() && !force_keyframe {
                        let hash = precall_frame::frame_fingerprint(&frame.bgra);
                        if last_hash == Some(hash) {
                            counters.frames_skipped_unchanged += 1;
                            continue;
                        }
                        Some(hash)
                    } else {
                        None
                    };

                    // Everything below is per-capture CPU — 58 ms for a
                    // desktop-like 1080p frame and 117 ms for photographic
                    // content (measured, `precall-frame/examples/client_cost`).
                    // It runs on the blocking pool: on the reactor it would
                    // stall this loop, and the whole point of the cadence is
                    // that the *next* capture is decided on time.
                    // Everything that changed since the last *sent* capture.
                    // The policy is pure and tested
                    // (`precall_frame::plan_capture`): unreadable metadata,
                    // accumulated scrolls and a fresh connection all force a
                    // keyframe; past half a screen's worth of change a keyframe
                    // is cheaper than the per-rectangle overhead.
                    let plan = match (&pending_dirty, pending_moved) {
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
                                    // Every planned rectangle must make it: the
                                    // send path clears the accumulated dirty set,
                                    // so a silently omitted region would never be
                                    // applied to the broker's surface. Any
                                    // failure falls back to the whole frame.
                                    let mut out = Vec::with_capacity(rects.len());
                                    let mut complete = true;
                                    for r in rects {
                                        match precall_frame::extract_bgra_region(&bgra, width, height, r)
                                        {
                                            Some((region, rw, rh)) => match encode_jpeg(&region, rw, rh, 80) {
                                                Ok(jpeg) => out.push((*r, jpeg)),
                                                Err(e) => {
                                                    tracing::warn!("region encode failed: {e}");
                                                    complete = false;
                                                    break;
                                                }
                                            },
                                            None => {
                                                tracing::warn!("region extraction failed");
                                                complete = false;
                                                break;
                                            }
                                        }
                                    }
                                    if complete && !out.is_empty() {
                                        Payload::Regions(out)
                                    } else {
                                        Payload::Full {
                                            jpeg: encode_jpeg(&bgra, width, height, 80)?,
                                            width,
                                            height,
                                        }
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
                            // Keep accumulating: these pixels were never sent,
                            // so the surface still needs them. Bounding is what
                            // stops a long pause or an excluded app from growing
                            // the set without limit; over budget the next frame
                            // is a keyframe instead.
                            bound_pending(&mut pending_dirty);
                            continue;
                        }
                        // No foreground window is the ordinary case, not a drop.
                        Ok(Ok(Prepared::NoWindow)) => {
                            bound_pending(&mut pending_dirty);
                            continue;
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("jpeg: {e}");
                            bound_pending(&mut pending_dirty);
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!("capture preparation panicked: {e}");
                            bound_pending(&mut pending_dirty);
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
                        Payload::Regions(rs) => conn.send_frame_regions(rs, now as u64).await,
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
                    // The broker now displays this frame: track its fingerprint
                    // if we computed one, otherwise mark the value unknown.
                    last_hash = fallback_hash;
                    // The surface is current as of this capture; from here the
                    // server holds a surface for this session.
                    pending_dirty = Some(Vec::new());
                    pending_moved = false;
                    force_keyframe = false;
                }
                _ = hb_tick.tick() => {
                    // Slot overwrites (the producer outran us) plus frames this
                    // loop discarded after taking them out of the channel.
                    counters.frames_dropped += frames.dropped().saturating_sub(slot_drops_reported);
                    slot_drops_reported = frames.dropped();
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
/// Fold a discarded frame's change metadata into the pending set.
fn absorb(
    pending: &mut Option<Vec<precall_frame::Rect>>,
    moved: &mut bool,
    (_, dirty, frame_moved): BridgedFrame,
) {
    absorb_meta(pending, moved, dirty, frame_moved);
}

fn absorb_meta(
    pending: &mut Option<Vec<precall_frame::Rect>>,
    moved: &mut bool,
    dirty: Option<Vec<precall_frame::Rect>>,
    frame_moved: bool,
) {
    match (pending.as_mut(), dirty) {
        // Unreadable metadata on any frame makes the whole union unknown.
        (_, None) => *pending = None,
        (Some(acc), Some(rects)) => {
            acc.extend(rects);
            *acc = precall_frame::merge_rects(acc);
        }
        (None, Some(_)) => {}
    }
    *moved |= frame_moved;
    // A busy desktop can report many disjoint rectangles between ticks; at a
    // long cadence the set would otherwise grow far past the per-capture budget
    // and every absorb would re-merge a growing vector. Past budget the plan is
    // necessarily a keyframe anyway.
    bound_pending(pending);
}

/// Keep an accumulating delta bounded: past the per-capture rectangle budget the
/// next frame is a keyframe, which is both correct and what a long pause wants.
fn bound_pending(pending: &mut Option<Vec<precall_frame::Rect>>) {
    if let Some(rects) = pending.as_mut() {
        *rects = precall_frame::merge_rects(rects);
        if rects.len() > precall_frame::MAX_DIRTY_RECTS {
            *pending = None;
        }
    }
}

/// Keep the newest frame that belongs to `generation`, dropping the rest.
///
/// Frames from an older duplicator describe a different desktop (a mode change
/// re-creates it), so they must not be sent as a keyframe for the new one — but
/// DXGI need not publish another frame while the desktop is static, so the
/// newest *matching* frame is retained rather than the queue being emptied.
fn retain_current_generation(
    latest: &mut Option<BridgedFrame>,
    chan: &mut tokio::sync::mpsc::Receiver<BridgedFrame>,
    generation: u64,
) {
    if latest.as_ref().is_some_and(|(f, _, _)| f.generation != generation) {
        *latest = None;
    }
    let mut newest: Option<BridgedFrame> = None;
    while let Ok(frame) = chan.try_recv() {
        if frame.0.generation == generation {
            newest = Some(frame);
        }
    }
    if newest.is_some() {
        *latest = newest;
    }
}

/// One captured frame plus every change reported since the last one handed out.
///
/// The handoff itself ([`precall_frame::ChangeSlot`]) folds in the metadata of
/// frames it overwrote; the bridge adds nothing, it just carries it, so the
/// send loop's own accumulation only has to cover what *it* discards.
type BridgedFrame = (
    crate::dxgi::CaptureFrame,
    Option<Vec<precall_frame::Rect>>,
    bool,
);

fn frame_bridge(
    frames: &std::sync::Arc<precall_frame::ChangeSlot<crate::dxgi::CaptureFrame>>,
) -> tokio::sync::mpsc::Receiver<BridgedFrame> {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let slot = std::sync::Arc::clone(frames);
    tokio::task::spawn_blocking(move || loop {
        match slot.recv() {
            Some(f) => {
                if tx.blocking_send(f).is_err() {
                    // The send loop is gone (it stopped, or the process is
                    // shutting down): close the slot so the capture thread
                    // notices and exits instead of acquiring and copying
                    // desktop frames for the rest of the process.
                    slot.close();
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
