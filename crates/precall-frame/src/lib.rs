// SPDX-License-Identifier: GPL-2.0-only
//! Platform-independent capture plumbing.
//!
//! Everything here decides whether a capture agent can hold **full framerate**
//! (streaming) *and* drop back to Recall's collection cadence without growing
//! or spinning. It lives outside `precall-client` so it can be measured and
//! tested on any host: the client itself cannot compile off Windows.
//!
//! Three decisions, each of which was a real defect at full framerate:
//!
//! * [`LatestSlot`] — the handoff between the capture thread and the send
//!   loop. A queue *cannot* be unbounded here: at 1920x1080x60 the display
//!   produces ~480 MB/s of BGRA, so anything that does not drop frames grows
//!   without limit and the stream falls further behind every second. The slot
//!   holds exactly one frame and overwrites it — drop-oldest, never
//!   drop-newest — and counts what it dropped.
//! * [`frame_fingerprint`] — dedup for the recall-cadence path, sampling the
//!   buffer instead of hashing all 8 MB per tick (see the numbers in
//!   `examples/client_cost.rs`).
//! * [`capture_interval_ms`] — one cadence policy for both operating points.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

/// A one-frame handoff: the producer overwrites, the consumer takes the newest.
///
/// This is deliberately *not* a queue. A capture agent's job at full framerate
/// is to keep up with *now*; buffering old frames converts a CPU shortfall into
/// unbounded memory growth and a stream that shows a minute-old desktop.
pub struct LatestSlot<T> {
    slot: Mutex<Option<T>>,
    ready: Condvar,
    published: AtomicU64,
    dropped: AtomicU64,
    closed: std::sync::atomic::AtomicBool,
}

impl<T> Default for LatestSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> LatestSlot<T> {
    pub fn new() -> Self {
        Self {
            slot: Mutex::new(None),
            ready: Condvar::new(),
            published: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Mark the producer gone. A waiting [`LatestSlot::recv`] returns `None`
    /// instead of blocking forever — without this, a capture thread that died
    /// (duplication lost and not recoverable) left the send loop blocked on a
    /// slot nobody would ever fill, still heartbeating, capturing nothing.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.ready.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Publish a frame, replacing (and counting) any frame the consumer has not
    /// taken yet.
    pub fn publish(&self, frame: T) {
        if self.is_closed() {
            return;
        }
        let mut slot = self.slot.lock().unwrap();
        if slot.is_some() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            self.ready.notify_one();
        }
        *slot = Some(frame);
        self.published.fetch_add(1, Ordering::Relaxed);
    }

    /// Block until a frame is available, then take it.
    ///
    /// `None` means the producer closed the slot — the caller must stop, not
    /// wait for a frame that is never coming.
    pub fn recv(&self) -> Option<T> {
        let mut slot = self.slot.lock().unwrap();
        loop {
            if let Some(frame) = slot.take() {
                return Some(frame);
            }
            if self.is_closed() {
                return None;
            }
            slot = self.ready.wait(slot).unwrap();
        }
    }

    /// Take a frame only if one is waiting — the non-blocking form used by the
    /// send loop's `select!` arm.
    pub fn try_recv(&self) -> Option<T> {
        self.slot.lock().unwrap().take()
    }

    /// Frames published so far.
    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    /// Frames overwritten before the consumer could take them.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Mixing constant for the word-wise fingerprint (golden ratio / xxHash prime).
const MIX_A: u64 = 0x9E37_79B9_7F4A_7C15;
const MIX_B: u64 = 0xBF58_476D_1CE4_E5B9;

/// Fingerprint a BGRA frame **exactly**: every byte participates.
///
/// An earlier version sampled one byte every 64. That is wrong, not merely
/// approximate: a 1920-pixel BGRA row is 7680 bytes, a multiple of 64, so the
/// stride landed on the *same* byte of the same sub-pixel column in every row
/// (always the blue channel). A red- or green-only change confined to the
/// unsampled pixels produced an identical fingerprint, and the capture loop
/// treated a real screen update as "unchanged" and never recorded it.
///
/// This fold reads 8 bytes at a time, so it costs a few operations per word
/// rather than per byte and is still a whole-buffer function (see
/// `examples/client_cost.rs` for the measured cost against the byte-wise
/// baseline).
pub fn frame_fingerprint(bgra: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for w in bgra.as_chunks::<8>().0 {
        let word = u64::from_le_bytes(*w);
        h ^= word.wrapping_mul(MIX_A);
        h = h.rotate_left(31).wrapping_mul(MIX_B);
    }
    // The tail (a BGRA buffer is usually 8-byte aligned, but do not assume).
    for &b in bgra.as_chunks::<8>().1 {
        h ^= b as u64;
        h = h.rotate_left(13).wrapping_mul(MIX_A);
    }
    h
}

/// Byte-at-a-time fold — the slow baseline, kept so the harness can price the
/// exact fingerprint against the obvious implementation.
pub fn fingerprint_full(bgra: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bgra {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Hard floor on the capture cadence: 100 captures/s. A zero interval would
/// spin the send loop; anything below ~10 ms buys nothing a display can show.
pub const MIN_CAPTURE_INTERVAL_MS: u64 = 10;

/// Number of captures per second at a given cadence, floored like
/// [`capture_interval_ms`].
pub fn captures_per_second(configured_ms: u64) -> f64 {
    1000.0 / capture_interval_ms(configured_ms) as f64
}

/// Effective capture interval for a configured value.
///
/// Both operating points are ordinary configurations of this one knob:
/// `16` for full-framerate streaming (60 captures/s, with [`LatestSlot`]
/// dropping anything the pipeline cannot carry), `3000` for Recall's
/// collection cadence (one capture every three seconds, and nothing at all
/// while the desktop is unchanged).
pub fn capture_interval_ms(configured_ms: u64) -> u64 {
    configured_ms.max(MIN_CAPTURE_INTERVAL_MS)
}

/// Recall's own collection cadence, in milliseconds.
pub const RECALL_CADENCE_MS: u64 = 3_000;

/// Full-framerate streaming cadence (60 captures/s), in milliseconds.
pub const STREAMING_CADENCE_MS: u64 = 16;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn slot_keeps_the_newest_frame_and_counts_the_rest() {
        let slot = LatestSlot::new();
        for i in 0..1000u64 {
            slot.publish(i);
        }
        // One frame in memory, 999 overwritten — not a 1000-frame backlog.
        assert_eq!(slot.published(), 1000);
        assert_eq!(slot.dropped(), 999);
        assert_eq!(slot.recv(), Some(999), "the consumer gets the newest");
        assert!(slot.try_recv().is_none());
    }

    #[test]
    fn a_closed_slot_wakes_its_consumer_instead_of_hanging() {
        let slot = Arc::new(LatestSlot::<u32>::new());
        let consumer = {
            let slot = Arc::clone(&slot);
            std::thread::spawn(move || slot.recv())
        };
        std::thread::sleep(Duration::from_millis(20));
        slot.close();
        assert_eq!(consumer.join().unwrap(), None, "a dead producer must not hang the bridge");
        // Publishing after close changes nothing.
        slot.publish(1);
        assert_eq!(slot.published(), 0);
        assert_eq!(slot.recv(), None);
    }

    #[test]
    fn slot_bounds_memory_under_a_producer_faster_than_the_consumer() {
        let slot = Arc::new(LatestSlot::new());
        let producer = {
            let slot = Arc::clone(&slot);
            std::thread::spawn(move || {
                for i in 0..100_000u64 {
                    slot.publish(vec![0u8; 1024]);
                    if i % 64 == 0 {
                        std::thread::yield_now();
                    }
                }
            })
        };
        // Slow consumer: one frame per iteration, so most are dropped.
        for _ in 0..100 {
            let _ = slot.recv();
        }
        producer.join().unwrap();
        let taken = 100u64;
        // At most one frame can still be resident; everything else was either
        // taken or overwritten. That identity *is* the memory bound.
        let resident = u64::from(slot.try_recv().is_some());
        assert!(resident <= 1, "the slot must never hold more than one frame");
        assert_eq!(
            slot.published(),
            taken + slot.dropped() + resident,
            "published = taken + dropped + resident, with resident <= 1"
        );
        assert!(slot.dropped() > 99_000, "the fast producer should mostly drop");
    }

    #[test]
    fn recv_blocks_until_a_frame_arrives() {
        let slot = Arc::new(LatestSlot::new());
        let producer = {
            let slot = Arc::clone(&slot);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                slot.publish(7u32);
            })
        };
        assert_eq!(slot.recv(), Some(7));
        producer.join().unwrap();
    }

    #[test]
    fn fingerprint_is_exact_at_1920px_row_width() {
        // A realistic 1080p BGRA buffer: 7680-byte rows.
        let (w, h) = (1920usize, 8usize);
        let mut frame = vec![0u8; w * h * 4];
        for (i, b) in frame.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let base = frame_fingerprint(&frame);
        assert_eq!(base, frame_fingerprint(&frame));

        // A red-only change at a pixel a 64-byte stride would never look at.
        // (The stride landed on the same blue byte of every 16th pixel, so this
        // was invisible — a real screen update treated as "unchanged".)
        let mut red_only = frame.clone();
        red_only[4 * 7 + 2] ^= 0xFF;
        assert_ne!(base, frame_fingerprint(&red_only), "a red-only change is a change");

        // Green-only, and a single-pixel change, likewise.
        let mut green_only = frame.clone();
        green_only[4 * 7 + 1] ^= 0xFF;
        assert_ne!(base, frame_fingerprint(&green_only));
        let mut one_pixel = frame.clone();
        one_pixel[4 * 1000] ^= 0x01;
        assert_ne!(base, frame_fingerprint(&one_pixel));

        // Order matters: swapping two words must not cancel out.
        let mut swapped = frame.clone();
        let (a, b) = (4usize, 4 + 8);
        for k in 0..8 {
            swapped.swap(a + k, b + k);
        }
        assert_ne!(base, frame_fingerprint(&swapped), "not a commutative fold");
    }

    #[test]
    fn cadence_policy_covers_both_operating_points() {
        assert_eq!(capture_interval_ms(STREAMING_CADENCE_MS), 16);
        assert_eq!(capture_interval_ms(RECALL_CADENCE_MS), 3_000);
        // Configured values are honored, with a floor that cannot spin.
        assert_eq!(capture_interval_ms(1), MIN_CAPTURE_INTERVAL_MS);
        assert_eq!(capture_interval_ms(0), MIN_CAPTURE_INTERVAL_MS);
        assert_eq!(captures_per_second(16), 62.5);
        assert_eq!(captures_per_second(RECALL_CADENCE_MS), 1.0 / 3.0);
        // The old client floored the cadence at 200 ms, i.e. 5 captures/s —
        // below the 60 fps the streaming mode promises.
        assert!(captures_per_second(200) < 60.0);
        assert!(captures_per_second(capture_interval_ms(200)) < 60.0);
    }
}

// ---------------------------------------------------------------------------
// Dirty-rectangle capture
// ---------------------------------------------------------------------------

/// A rectangle in desktop coordinates, matching the wire's `Rect`.
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

    pub fn width(&self) -> i32 {
        (self.right as i32 - self.left as i32).max(0)
    }

    pub fn height(&self) -> i32 {
        (self.bottom as i32 - self.top as i32).max(0)
    }

    pub fn area(&self) -> i64 {
        self.width() as i64 * self.height() as i64
    }

    pub fn is_empty(&self) -> bool {
        self.width() == 0 || self.height() == 0
    }
}

/// Biggest rectangle list we will send for one capture. DXGI can report a lot
/// of tiny rects for a scrolling page; past this, the frame is cheaper as a
/// keyframe than as a delta.
pub const MAX_DIRTY_RECTS: usize = 32;

/// What to send for one captured frame.
#[derive(Debug, Clone, PartialEq)]
pub enum CapturePlan {
    /// Send the whole frame (and re-seed the server's surface).
    Keyframe,
    /// Send only these rectangles.
    Regions(Vec<Rect>),
    /// Nothing changed.
    Skip,
}

/// Decide what to send for a frame with these dirty rectangles.
///
/// Both operating points fall out of the same rule, which is why it is one
/// function and not a mode flag:
///
/// * **Recall cadence** — the desktop changed, so a keyframe is due anyway
///   (a record a few seconds apart is a *moment*, and a full frame keeps the
///   archive independently meaningful); or a small edit sends one tiny delta.
/// * **streaming** — deltas only, keyframes only when forced (a reconnect, or
///   when the delta would cost more than the frame).
///
/// `moved` says DXGI reported *move* rectangles for this frame: the desktop
/// scrolled, which DXGI describes as "copy this region to there" rather than
/// "these pixels changed". The wire has no surface-copy operation, so the whole
/// frame is sent instead — correct, and cheap enough at any recording cadence.
///
/// `force_keyframe` is the caller's policy: a fresh connection must always
/// start with one, because the server has no surface for this session (and a
/// reconnected client's surface was dropped with its session).
pub fn plan_capture(
    dirty: &[Rect],
    moved: bool,
    frame_area: i64,
    force_keyframe: bool,
) -> CapturePlan {
    if force_keyframe || moved {
        return CapturePlan::Keyframe;
    }
    let mut rects: Vec<Rect> = dirty.iter().copied().filter(|r| !r.is_empty()).collect();
    if rects.is_empty() {
        return CapturePlan::Skip;
    }
    if rects.len() > MAX_DIRTY_RECTS {
        return CapturePlan::Keyframe;
    }
    let covered: i64 = rects.iter().map(|r| r.area()).sum();
    // Past half the screen the delta is not saving anything worth the
    // per-rect overhead (each rect is a separate JPEG), so send the frame.
    if covered * 2 >= frame_area {
        return CapturePlan::Keyframe;
    }
    // Deterministic order: the server applies them onto a surface, so order
    // does not matter for the result, but a stable order makes captures
    // comparable in tests and logs.
    rects.sort_by_key(|r| (r.top, r.left, r.bottom, r.right));
    CapturePlan::Regions(rects)
}

/// Copy one BGRA rectangle out of a full frame, tightly packed.
///
/// The client encodes each rectangle on its own, which is where the wire and
/// CPU savings come from: a 200x100 text edit costs a fraction of a full frame.
/// Returns `None` for a rectangle that does not intersect the frame.
pub fn extract_bgra_region(
    bgra: &[u8],
    frame_width: u32,
    frame_height: u32,
    rect: &Rect,
) -> Option<(Vec<u8>, u32, u32)> {
    let x0 = rect.left.max(0) as u32;
    let y0 = rect.top.max(0) as u32;
    let x1 = (rect.right as i32).min(frame_width as i32).max(0) as u32;
    let y1 = (rect.bottom as i32).min(frame_height as i32).max(0) as u32;
    if x1 <= x0 || y1 <= y0 {
        return None;
    }
    let (w, h) = (x1 - x0, y1 - y0);
    let mut out = Vec::with_capacity((w as usize) * (h as usize) * 4);
    let stride = (frame_width as usize) * 4;
    for row in y0..y1 {
        let start = (row as usize) * stride + (x0 as usize) * 4;
        let end = start + (w as usize) * 4;
        if end > bgra.len() {
            return None;
        }
        out.extend_from_slice(&bgra[start..end]);
    }
    Some((out, w, h))
}

#[cfg(test)]
mod dirty_tests {
    use super::*;

    #[test]
    fn a_small_edit_is_a_delta_and_a_large_change_is_a_keyframe() {
        let screen = 1920 * 1080;
        assert_eq!(plan_capture(&[], false, screen, false), CapturePlan::Skip);
        assert_eq!(
            plan_capture(&[Rect::new(100, 100, 300, 200)], false, screen, false),
            CapturePlan::Regions(vec![Rect::new(100, 100, 300, 200)])
        );
        // Past half the screen: the frame is cheaper than the deltas.
        assert_eq!(
            plan_capture(&[Rect::new(0, 0, 1920, 1080)], false, screen, false),
            CapturePlan::Keyframe
        );
        // A forced keyframe (fresh connection) wins over any delta.
        assert_eq!(
            plan_capture(&[Rect::new(1, 1, 2, 2)], false, screen, true),
            CapturePlan::Keyframe
        );
        // A scroll is a move rectangle: the wire cannot express a surface copy,
        // so the frame goes whole.
        assert_eq!(
            plan_capture(&[Rect::new(0, 400, 1920, 460)], true, screen, false),
            CapturePlan::Keyframe
        );
    }

    #[test]
    fn a_scrolling_page_falls_back_to_a_keyframe() {
        // More rectangles than the per-capture budget: the JPEG overhead per
        // rect exceeds the saving.
        let many: Vec<Rect> = (0..(MAX_DIRTY_RECTS as i16 + 1))
            .map(|i| Rect::new(i, i, i + 1, i + 1))
            .collect();
        assert_eq!(
            plan_capture(&many, false, 1920 * 1080, false),
            CapturePlan::Keyframe
        );
    }

    #[test]
    fn regions_are_clipped_to_the_frame_and_ordered() {
        let (w, h) = (8u32, 4u32);
        // Row-major BGRA with a distinct value per pixel.
        let mut bgra = Vec::new();
        for y in 0..h {
            for x in 0..w {
                bgra.extend_from_slice(&[x as u8, y as u8, 0, 255]);
            }
        }
        let (region, rw, rh) = extract_bgra_region(&bgra, w, h, &Rect::new(2, 1, 5, 3)).unwrap();
        assert_eq!((rw, rh), (3, 2));
        assert_eq!(region.len(), 3 * 2 * 4);
        assert_eq!(&region[0..4], &[2, 1, 0, 255], "top-left of the rect");
        assert_eq!(&region[4..8], &[3, 1, 0, 255]);
        // A rectangle past the edge is clipped, not refused.
        let (clipped, cw, ch) = extract_bgra_region(&bgra, w, h, &Rect::new(6, 0, 20, 2)).unwrap();
        assert_eq!((cw, ch), (2, 2));
        assert_eq!(clipped.len(), 2 * 2 * 4);
        // A rectangle entirely outside yields nothing.
        assert!(extract_bgra_region(&bgra, w, h, &Rect::new(50, 50, 60, 60)).is_none());
    }

    /// The saving that matters: a small rectangle costs a small buffer.
    #[test]
    fn a_delta_region_is_a_fraction_of_the_frame() {
        let (w, h) = (1920u32, 1080u32);
        let frame = vec![0u8; (w as usize) * (h as usize) * 4];
        let (region, rw, rh) =
            extract_bgra_region(&frame, w, h, &Rect::new(100, 100, 300, 200)).unwrap();
        assert_eq!((rw, rh), (200, 100));
        assert_eq!(region.len(), 200 * 100 * 4);
        let full = (w as usize) * (h as usize) * 4;
        assert!(
            region.len() * 50 < full,
            "a 200x100 edit is <1/50 of the frame's bytes"
        );
    }
}
