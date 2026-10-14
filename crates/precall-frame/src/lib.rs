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
    ///
    /// The flag is set **while holding the slot mutex**: a waiter that checked
    /// the flag and is about to `wait` must not miss the notification, or it
    /// sleeps forever and the dead-producer symptom comes back.
    pub fn close(&self) {
        let guard = self.slot.lock().unwrap();
        self.closed.store(true, Ordering::SeqCst);
        self.ready.notify_all();
        drop(guard);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Publish a frame, replacing (and counting) any frame the consumer has not
    /// taken yet.
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

/// The change metadata a captured frame carries, taken out of it on the way to
/// the sender.
///
/// `None` means the platform could not report changes for that frame: the
/// caller must treat it as "send the whole frame", never as "nothing changed".
pub trait ChangeMeta {
    fn take_changes(&mut self) -> (Option<Vec<Rect>>, bool);
}

/// A one-frame handoff that does **not** lose change metadata.
///
/// [`LatestSlot`] keeps one frame: while the sender is encoding or the link is
/// slow — routine at a 16 ms cadence — the producer overwrites the resident
/// frame and it is gone. Every dirty list DXGI reports is relative to the
/// previous *acquired* frame, so a frame lost this way leaves the broker's
/// desktop surface stale exactly where it changed.
///
/// `ChangeSlot` keeps the frame **and** the accumulated changes under one lock
/// and hands them out together. That single critical section is what makes the
/// pairing sound: a displaced frame's rectangles can neither be recorded after a
/// consumer has taken its replacement (they would then wait for a frame that may
/// never come — the desktop's last change, then silence) nor be drained by a
/// consumer handing out an *older* frame (they would be painted from that
/// frame's pixels, which do not contain the change).
pub struct ChangeSlot<T: ChangeMeta> {
    state: Mutex<SlotState<T>>,
    ready: Condvar,
    published: AtomicU64,
    dropped: AtomicU64,
}

struct SlotState<T> {
    frame: Option<T>,
    /// Changes reported since the last handoff. `None` means some frame's
    /// metadata was unreadable or past budget: the next send must be a keyframe.
    pending: Option<Vec<Rect>>,
    /// A scroll seen since the last handoff: the wire cannot express a surface
    /// copy, so the next send must be a keyframe.
    moved: bool,
    closed: bool,
}

impl<T: ChangeMeta> Default for ChangeSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// `Some` only if both sides have usable metadata.
fn combine(a: Option<Vec<Rect>>, b: Option<Vec<Rect>>) -> Option<Vec<Rect>> {
    match (a, b) {
        (Some(mut a), Some(b)) => {
            a.extend(b);
            Some(a)
        }
        _ => None,
    }
}

/// Merge overlaps and collapse to "unknown" past the per-capture budget, which
/// is what keeps an arbitrarily long outage (nobody draining) bounded.
fn bound(pending: &mut Option<Vec<Rect>>) {
    if let Some(rects) = pending.as_mut() {
        *rects = merge_rects(rects);
        if rects.len() > MAX_DIRTY_RECTS {
            *pending = None;
        }
    }
}

impl<T: ChangeMeta> SlotState<T> {
    /// Fold one frame's changes into the accumulated set. A method (rather than
    /// free fields) because the state sits behind a mutex guard: borrowing two
    /// fields of a `DerefMut` target at once is not allowed.
    fn accumulate(&mut self, frame: &mut T) {
        let (dirty, frame_moved) = frame.take_changes();
        self.pending = combine(self.pending.take(), dirty);
        self.moved |= frame_moved;
        bound(&mut self.pending);
    }
}

impl<T: ChangeMeta> ChangeSlot<T> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SlotState {
                frame: None,
                pending: Some(Vec::new()),
                moved: false,
                closed: false,
            }),
            ready: Condvar::new(),
            published: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    /// Publish a frame, retaining the change metadata of any frame it displaces.
    pub fn publish(&self, frame: T) {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return;
        }
        let displaced = state.frame.replace(frame);
        match displaced {
            Some(mut displaced) => {
                // Recorded under the same lock that holds the replacement: no
                // consumer can see the new frame before this runs.
                state.accumulate(&mut displaced);
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            None => self.ready.notify_one(),
        }
        self.published.fetch_add(1, Ordering::Relaxed);
    }

    /// Block until a frame is available, then take it together with every change
    /// reported since the last handoff.
    pub fn recv(&self) -> Option<(T, Option<Vec<Rect>>, bool)> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(taken) = Self::take(&mut state) {
                return Some(taken);
            }
            if state.closed {
                return None;
            }
            state = self.ready.wait(state).unwrap();
        }
    }

    pub fn try_recv(&self) -> Option<(T, Option<Vec<Rect>>, bool)> {
        let mut state = self.state.lock().unwrap();
        Self::take(&mut state)
    }

    /// The frame and its accumulated metadata leave in one critical section, so
    /// metadata always describes the frame it is handed out with.
    fn take(state: &mut SlotState<T>) -> Option<(T, Option<Vec<Rect>>, bool)> {
        let mut frame = state.frame.take()?;
        let (dirty, frame_moved) = frame.take_changes();
        let pending = combine(state.pending.replace(Vec::new()), dirty);
        let moved = frame_moved || std::mem::replace(&mut state.moved, false);
        Some((frame, pending.map(|rects| merge_rects(&rects)), moved))
    }

    /// Mark the producer gone: waiting receivers return `None`.
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.ready.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
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
        // Deterministic: publish everything *before* the consumer runs, so no
        // assertion depends on which thread wins a race (an earlier version
        // blocked a `recv()` that the producer might never satisfy).
        let slot = LatestSlot::new();
        for i in 0..100_000u64 {
            slot.publish(vec![i as u8; 1024]);
        }
        assert!(!slot.is_closed());
        // One frame is resident, 99_999 were overwritten; nothing was queued.
        let mut taken = 0u64;
        while slot.try_recv().is_some() {
            taken += 1;
        }
        assert_eq!(taken, 1, "exactly one frame survives");
        assert_eq!(slot.published(), 100_000);
        assert_eq!(slot.dropped(), 99_999);
        assert_eq!(slot.published(), taken + slot.dropped());
    }

    #[test]
    fn closes_never_lose_their_wakeup() {
        // Stress the check-then-wait race: the consumer must never sleep past a
        // close, whatever the interleaving.
        for _ in 0..500 {
            let slot = Arc::new(LatestSlot::<u32>::new());
            let consumer = {
                let slot = Arc::clone(&slot);
                let (tx, rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let _ = tx.send(slot.recv());
                });
                rx
            };
            // Interleave close with the consumer's entry into recv().
            if slot.published().is_multiple_of(2) {
                std::thread::yield_now();
            }
            slot.close();
            let outcome = consumer
                .recv_timeout(Duration::from_secs(2))
                .expect("a closed slot must wake its consumer (lost wakeup)");
            assert_eq!(outcome, None);
        }
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

/// Merge rectangles that overlap (or touch) into their bounding box.
///
/// The client accumulates the dirty lists of every frame it discards between
/// captures — each list describes changes since the *previous acquired* frame,
/// so dropping them would apply only the newest change to the server's surface
/// and silently archive stale screenshots. Merging keeps that union inside
/// [`MAX_DIRTY_RECTS`], which is what makes the accumulated delta still a delta
/// instead of a keyframe.
pub fn merge_rects(rects: &[Rect]) -> Vec<Rect> {
    let mut out: Vec<Rect> = Vec::with_capacity(rects.len());
    for &r in rects.iter().filter(|r| !r.is_empty()) {
        let mut merged = r;
        let mut i = 0;
        while i < out.len() {
            if touches(&merged, &out[i]) {
                let other = out.swap_remove(i);
                merged = Rect::new(
                    merged.left.min(other.left),
                    merged.top.min(other.top),
                    merged.right.max(other.right),
                    merged.bottom.max(other.bottom),
                );
                // The new box may now touch rectangles already merged in.
                i = 0;
            } else {
                i += 1;
            }
        }
        out.push(merged);
    }
    out.sort_by_key(|r| (r.top, r.left, r.bottom, r.right));
    out
}

/// Do two rectangles overlap or sit edge-to-edge (within a pixel)?
fn touches(a: &Rect, b: &Rect) -> bool {
    a.left - 1 <= b.right && b.left - 1 <= a.right && a.top - 1 <= b.bottom && b.top - 1 <= a.bottom
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

#[cfg(test)]
mod merge_tests {
    use super::*;

    #[test]
    fn overlapping_and_adjacent_rectangles_merge_transitively() {
        // A chain: (0..10) touches (11..20) touches (21..30) — one box.
        let merged = merge_rects(&[
            Rect::new(0, 0, 10, 10),
            Rect::new(11, 0, 20, 10),
            Rect::new(21, 0, 30, 10),
        ]);
        assert_eq!(merged, vec![Rect::new(0, 0, 30, 10)]);

        // Overlapping boxes become their bounding box.
        let merged = merge_rects(&[Rect::new(0, 0, 10, 10), Rect::new(5, 5, 20, 20)]);
        assert_eq!(merged, vec![Rect::new(0, 0, 20, 20)]);

        // Distant boxes stay separate, in a deterministic order.
        let merged = merge_rects(&[Rect::new(100, 100, 110, 110), Rect::new(0, 0, 10, 10)]);
        assert_eq!(
            merged,
            vec![Rect::new(0, 0, 10, 10), Rect::new(100, 100, 110, 110)]
        );

        // Empty rectangles are dropped, not merged into anything.
        assert!(merge_rects(&[Rect::new(5, 5, 5, 5)]).is_empty());
    }

    /// Accumulating the dirty lists of discarded frames must not turn a small
    /// change into something past the per-capture budget.
    #[test]
    fn accumulation_stays_a_delta() {
        // A caret blinking inside one text box, reported by 40 discarded frames
        // at slightly different positions.
        let dirty: Vec<Rect> = (0..40)
            .map(|i| Rect::new(200 + i, 100, 202 + i, 120))
            .collect();
        let merged = merge_rects(&dirty);
        assert_eq!(merged.len(), 1, "all of it is one text box: {merged:?}");
        let plan = plan_capture(&merged, false, 1920 * 1080, false);
        assert!(
            matches!(plan, CapturePlan::Regions(_)),
            "still sendable as a delta: {plan:?}"
        );
    }
}

#[cfg(test)]
mod change_slot_tests {
    use super::*;

    #[derive(Debug)]
    struct TestFrame {
        id: u32,
        dirty: Option<Vec<Rect>>,
        moved: bool,
    }

    impl ChangeMeta for TestFrame {
        fn take_changes(&mut self) -> (Option<Vec<Rect>>, bool) {
            (self.dirty.take(), std::mem::replace(&mut self.moved, false))
        }
    }

    fn frame(id: u32, dirty: &[Rect]) -> TestFrame {
        TestFrame { id, dirty: Some(dirty.to_vec()), moved: false }
    }

    /// The bug this exists for: a frame overwritten in the slot loses its
    /// rectangles, and every DXGI list is relative to the previous acquired
    /// frame, so those pixels never reach the broker's surface.
    #[test]
    fn a_displaced_frame_still_contributes_its_rectangles() {
        let slot = ChangeSlot::new();
        let far = Rect::new(1700, 900, 1800, 1000);
        slot.publish(frame(1, &[far]));
        // Overwritten before anyone took it.
        for i in 2..50 {
            slot.publish(frame(i, &[Rect::new(0, 0, 4, 4)]));
        }
        let (newest, dirty, moved) = slot.recv().unwrap();
        assert_eq!(newest.id, 49, "the newest frame is handed out");
        let dirty = dirty.expect("metadata was readable");
        assert!(
            dirty.iter().any(|r| r.left <= far.left && r.right >= far.right
                && r.top <= far.top && r.bottom >= far.bottom),
            "the displaced frame's rectangle survives: {dirty:?}"
        );
        assert!(!moved);
        // Bounded: 49 frames of changes collapsed to a couple of rectangles.
        assert!(dirty.len() <= 2, "accumulation stays a delta: {dirty:?}");
    }

    #[test]
    fn a_displaced_scroll_forces_a_keyframe() {
        let slot = ChangeSlot::new();
        let mut scrolled = frame(1, &[Rect::new(0, 400, 1920, 460)]);
        scrolled.moved = true;
        slot.publish(scrolled);
        slot.publish(frame(2, &[Rect::new(10, 10, 20, 20)]));
        let (_, dirty, moved) = slot.recv().unwrap();
        assert!(moved, "the scroll flag survives the overwrite");
        assert!(dirty.is_some());
    }

    #[test]
    fn unreadable_metadata_dominates_the_union() {
        let slot = ChangeSlot::new();
        slot.publish(TestFrame { id: 1, dirty: None, moved: false });
        slot.publish(frame(2, &[Rect::new(1, 1, 2, 2)]));
        let (_, dirty, _) = slot.recv().unwrap();
        assert!(
            dirty.is_none(),
            "one unreadable frame means the caller must send a whole frame"
        );
    }

    #[test]
    fn a_taken_frame_leaves_nothing_pending() {
        let slot = ChangeSlot::new();
        slot.publish(frame(1, &[Rect::new(0, 0, 8, 8)]));
        let (_, first, _) = slot.recv().unwrap();
        assert_eq!(first.unwrap().len(), 1);
        // Nothing published since: the next recv blocks, so use try_recv.
        assert!(slot.try_recv().is_none());
        slot.publish(frame(2, &[]));
        let (_, second, moved) = slot.recv().unwrap();
        assert!(second.unwrap().is_empty(), "no stale rectangles linger");
        assert!(!moved);
        assert_eq!(slot.published(), 2);
        assert_eq!(slot.dropped(), 0);
    }
}

#[cfg(test)]
mod change_slot_bound_tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Debug)]
    struct F {
        dirty: Option<Vec<Rect>>,
    }

    impl ChangeMeta for F {
        fn take_changes(&mut self) -> (Option<Vec<Rect>>, bool) {
            (self.dirty.take(), false)
        }
    }

    fn f(dirty: &[Rect]) -> F {
        F { dirty: Some(dirty.to_vec()) }
    }

    /// While the consumer is blocked (a server outage, a slow link) every
    /// overwrite lands in the accumulator — it must not grow for the whole
    /// outage.
    #[test]
    fn metadata_stays_bounded_while_the_consumer_is_blocked() {
        let slot = ChangeSlot::new();
        for i in 0..20_000i16 {
            // A distinct rectangle each time, so merging cannot cheat.
            slot.publish(f(&[Rect::new(i % 1000, i / 1000, i % 1000 + 3, i / 1000 + 3)]));
        }
        let (_, dirty, _) = slot.recv().unwrap();
        match dirty {
            // Over budget the accumulator says "unknown", which means keyframe.
            None => {}
            Some(rects) => assert!(
                rects.len() <= MAX_DIRTY_RECTS,
                "accumulator must stay inside the per-capture budget: {}",
                rects.len()
            ),
        }
    }

    /// The desktop's last change, then silence: the displaced frame's rectangles
    /// must ride with the very next take, not wait for a frame that never comes.
    #[test]
    fn a_displaced_frame_is_paired_with_the_next_take() {
        let slot = ChangeSlot::new();
        let last = Rect::new(900, 700, 950, 750);
        slot.publish(f(&[last]));
        slot.publish(f(&[]));
        let (_, dirty, _) = slot.recv().unwrap();
        let rects = dirty.expect("metadata readable");
        assert!(
            rects.iter().any(|r| r.left <= last.left && r.right >= last.right),
            "the displaced change came out with the replacement: {rects:?}"
        );
        assert!(slot.try_recv().is_none());
    }

    /// Same guarantee under contention: whatever the interleaving, the first
    /// frame's change is handed out with a take, never stranded.
    #[test]
    fn displaced_metadata_is_never_stranded_under_contention() {
        for _ in 0..200 {
            let slot = Arc::new(ChangeSlot::new());
            let first = Rect::new(10, 10, 20, 20);
            let producer = {
                let slot = Arc::clone(&slot);
                std::thread::spawn(move || {
                    slot.publish(f(&[first]));
                    for i in 0..8i16 {
                        slot.publish(f(&[Rect::new(i, i, i + 2, i + 2)]));
                    }
                    slot.close();
                })
            };
            let mut seen_first = false;
            while let Some((_, dirty, _)) = slot.recv() {
                if let Some(rects) = dirty {
                    if rects.iter().any(|r| r.left <= first.left && r.right >= first.right) {
                        seen_first = true;
                    }
                } else {
                    // An unreadable/over-budget accumulator implies a keyframe.
                    seen_first = true;
                }
            }
            producer.join().unwrap();
            assert!(seen_first, "the first frame's change must not be stranded");
        }
    }
}
