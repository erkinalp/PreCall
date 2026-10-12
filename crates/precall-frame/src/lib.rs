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
        }
    }

    /// Publish a frame, replacing (and counting) any frame the consumer has not
    /// taken yet.
    pub fn publish(&self, frame: T) {
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
    pub fn recv(&self) -> T {
        let mut slot = self.slot.lock().unwrap();
        loop {
            if let Some(frame) = slot.take() {
                return frame;
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

/// Bytes of BGRA to sample per fingerprint. 64 gives one byte per 256
/// pixels — 32 KiB of a 1080p frame instead of 8 MiB.
pub const FRAME_HASH_STRIDE: usize = 64;

/// Fingerprint a BGRA frame by sampling it on a fixed stride.
///
/// Used only to skip frames that are byte-identical to the previous capture
/// (DXGI can redeliver the same desktop around mode changes). Sampling cannot
/// miss a *real* change in practice — a screenful of pixels that matches on
/// every 64th byte would have to differ in fewer than 1/64 of its bytes — and
/// it keeps the per-tick cost flat instead of proportional to resolution.
pub fn frame_fingerprint(bgra: &[u8]) -> u64 {
    fingerprint_strided(bgra, FRAME_HASH_STRIDE)
}

/// [`frame_fingerprint`] with an explicit stride, for measurement.
pub fn fingerprint_strided(bgra: &[u8], stride: usize) -> u64 {
    let stride = stride.max(1);
    // FNV-1a over the sampled bytes.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0usize;
    while i < bgra.len() {
        h ^= bgra[i] as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
        i += stride;
    }
    h
}

/// The fingerprint of a whole buffer — the cost baseline this replaces.
pub fn fingerprint_full(bgra: &[u8]) -> u64 {
    fingerprint_strided(bgra, 1)
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
        assert_eq!(slot.recv(), 999, "the consumer gets the newest, not the oldest");
        assert!(slot.try_recv().is_none());
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
        assert_eq!(slot.recv(), 7);
        producer.join().unwrap();
    }

    #[test]
    fn fingerprint_is_stable_and_stride_sensitive() {
        // 1 MiB of BGRA is enough to exercise the stride pattern.
        let frame: Vec<u8> = (0..(1 << 20)).map(|i| (i % 251) as u8).collect();
        assert_eq!(frame_fingerprint(&frame), frame_fingerprint(&frame));
        // A one-byte change off-stride is invisible to the sampled form and
        // visible to the full one — documented, not accidental.
        let mut off_stride = frame.clone();
        off_stride[1] ^= 0xFF;
        assert_eq!(frame_fingerprint(&frame), frame_fingerprint(&off_stride));
        assert_ne!(fingerprint_full(&frame), fingerprint_full(&off_stride));
        // A change *on* the stride is always seen.
        let mut on_stride = frame.clone();
        on_stride[0] ^= 0xFF;
        assert_ne!(frame_fingerprint(&frame), frame_fingerprint(&on_stride));
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
