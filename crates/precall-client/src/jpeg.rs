// SPDX-License-Identifier: GPL-2.0-only
//! BGRA → JPEG encoding for the wire (`RDPGFX_CODECID_JPEG`).

use crate::ClientError;
use image::codecs::jpeg::JpegEncoder;
use image::ImageEncoder;

/// Encode a tightly-packed BGRA buffer to JPEG (quality ~80 — Recall-grade
/// compression ratio without the latency of a real codec like AVC444).
pub fn encode_jpeg(bgra: &[u8], width: u32, height: u32, quality: u8) -> Result<Vec<u8>, ClientError> {
    let mut rgb = Vec::with_capacity((width * height * 3) as usize);
    for px in bgra.chunks_exact(4) {
        rgb.extend_from_slice(&[px[2], px[1], px[0]]);
    }
    let mut out = Vec::new();
    JpegEncoder::new_with_quality(&mut out, quality).write_image(
        &rgb,
        width,
        height,
        image::ExtendedColorType::Rgb8,
    )?;
    Ok(out)
}
