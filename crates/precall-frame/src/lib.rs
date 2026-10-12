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
        one_pixel[4 * 1000 + 0] ^= 0x01;
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
