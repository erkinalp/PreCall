// SPDX-License-Identifier: GPL-2.0-only
//! Client-side per-frame cost, measured on synthetic 1080p frames.
//!
//! The Windows capture path itself (DXGI, WinRT OCR, UI Automation) cannot run
//! here, so this measures the parts of the client's per-frame work that are
//! platform-independent and are on the critical path *every* capture:
//!
//! * BGRA→JPEG encoding at the client's quality (80) — the dominant CPU cost;
//! * frame fingerprinting, full-buffer versus sampled.
//!
//! These are the numbers that decide whether a configured 16 ms cadence is
//! achievable, and what the sampled fingerprint saves per tick.
//!
//! Run: `cargo run --release -p precall-frame --example client_cost`

use precall_frame::{capture_interval_ms, fingerprint_full, frame_fingerprint};
use std::io::Cursor;
use std::time::{Duration, Instant};

fn time<T>(reps: usize, mut f: impl FnMut() -> T) -> Duration {
    // One untimed call to warm caches/allocator.
    let _ = f();
    let t0 = Instant::now();
    for _ in 0..reps {
        std::hint::black_box(f());
    }
    t0.elapsed() / reps as u32
}

fn bgra_gradient(width: u32, height: u32) -> Vec<u8> {
    // An 8-bit gradient, i.e. a plausible desktop: smooth areas plus edges.
    let mut v = vec![0u8; (width * height * 4) as usize];
    for y in 0..height {
        for x in 0..width {
            let i = ((y * width + x) * 4) as usize;
            v[i] = (x % 256) as u8;
            v[i + 1] = (y % 256) as u8;
            v[i + 2] = ((x ^ y) % 256) as u8;
            v[i + 3] = 0xFF;
        }
    }
    v
}

fn bgra_noise(width: u32, height: u32) -> Vec<u8> {
    // Worst case: photographic content, which is what makes JPEG large.
    let mut state = 0x1234_5678u64;
    let mut v = vec![0u8; (width * height * 4) as usize];
    for b in v.chunks_mut(4) {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        b[0] = (state >> 33) as u8;
        b[1] = (state >> 41) as u8;
        b[2] = (state >> 49) as u8;
        b[3] = 0xFF;
    }
    v
}

fn jpeg_size_estimate(bgra: &[u8], width: u32, height: u32) -> usize {
    let rgb: Vec<u8> = bgra
        .as_chunks::<4>()
        .0
        .iter()
        .flat_map(|p| [p[2], p[1], p[0]])
        .collect();
    let mut out = Cursor::new(Vec::new());
    let enc = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 80);
    image::ImageEncoder::write_image(
        enc,
        &rgb,
        width,
        height,
        image::ExtendedColorType::Rgb8,
    )
    .unwrap();
    out.into_inner().len()
}

fn main() {
    let (w, h) = (1920u32, 1080u32);
    let bgra = bgra_gradient(w, h);
    let noise = bgra_noise(w, h);
    let frame_bytes = bgra.len() as f64 / (1024.0 * 1024.0);

    println!("PreCall client-side per-frame cost at {w}x{h} ({frame_bytes:.1} MiB BGRA)\n");

    // --- fingerprinting -----------------------------------------------------
    // `frame_fingerprint` must be *exact* (a sampled stride aliased against the
    // 1920-px row width and deduped real changes away), so it is compared
    // against the byte-at-a-time baseline rather than against a sampled one.
    let bytewise = time(20, || fingerprint_full(&bgra));
    let exact = time(200, || frame_fingerprint(&bgra));
    let budget_at_60 = Duration::from_millis(16);
    println!(
        "fingerprint   exact, word-wise {:>8.3} ms/frame ({:>7.0} MB/s, {:>4.1}% of a 16 ms budget)",
        exact.as_secs_f64() * 1000.0,
        frame_bytes / exact.as_secs_f64(),
        exact.as_secs_f64() / budget_at_60.as_secs_f64() * 100.0,
    );
    println!(
        "              byte-at-a-time  {:>8.3} ms/frame ({:>7.1}x slower; every byte participates in both)",
        bytewise.as_secs_f64() * 1000.0,
        bytewise.as_secs_f64() / exact.as_secs_f64(),
    );

    // --- JPEG encode (what the client sends) --------------------------------
    println!("\nBGRA -> RGB conversion + JPEG q80:");
    for (label, buf) in [("desktop-like", &bgra), ("photographic", &noise)] {
        let t = time(3, || jpeg_size_estimate(buf, w, h));
        let bytes = jpeg_size_estimate(buf, w, h);
        println!(
            "  {label:<14} {:>7.2} ms/frame -> {:>7.1} KiB/frame; one core sustains {:>6.1} fps ({:>6.1} MB/s wire)",
            t.as_secs_f64() * 1000.0,
            bytes as f64 / 1024.0,
            1.0 / t.as_secs_f64(),
            (bytes as f64 / (1024.0 * 1024.0)) / t.as_secs_f64(),
        );
    }

    println!(
        "\noperating points: streaming {} ms (60 captures/s) vs Recall cadence {} ms (1 capture / 3 s)",
        capture_interval_ms(16),
        capture_interval_ms(3_000),
    );
    println!(
        "note: OCR (WinRT) and URL/file extraction (UI Automation) are Windows-only and are NOT measured here;\n\
         they are per-capture costs on the same path and must move off the runtime thread (see run.rs)."
    );
}
