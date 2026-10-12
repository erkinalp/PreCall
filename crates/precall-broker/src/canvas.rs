// SPDX-License-Identifier: GPL-2.0-only
//! Per-client desktop surface.
//!
//! A dirty-rect capture agent sends only the rectangles that changed, in the
//! `RDPGFX_WIRE_TO_SURFACE_PDU_1` shape the protocol already carries. Storing
//! those rectangles as-is would make every reader — the snapshot route, the
//! image embedder, the timeline, the export — a compositor, and would stop
//! stored objects from being Recall-shaped (`ukg.db` + one full-frame JPEG per
//! capture). So the broker keeps the desktop surface and applies each region to
//! it, exactly as the RDP graphics pipeline does, and archives the composed
//! surface as an ordinary JPEG.
//!
//! Cost model: applying a region is a memcpy; the JPEG encode is the expensive
//! step and happens once per *stored capture*, so the client's cadence — not the
//! display rate — governs it. (Measured: ~58 ms for a desktop-like 1080p frame
//! with the software encoder; a hardware encoder such as Media Foundation is
//! the production answer, which is what Microsoft's own pipeline uses.)

use image::codecs::jpeg::JpegEncoder;
use image::{ExtendedColorType, ImageEncoder};

/// What went wrong composing a region into the surface.
#[derive(Debug, thiserror::Error)]
pub enum CanvasError {
    #[error("region is not a decodable image: {0}")]
    Decode(String),
    #[error("surface is {width}x{height}, region is {rw}x{rh}")]
    Size {
        width: u32,
        height: u32,
        rw: u32,
        rh: u32,
    },
    #[error("encode: {0}")]
    Encode(String),
}

/// A client's desktop, RGB8, `width * height * 3` bytes.
#[derive(Debug, Clone)]
pub struct Canvas {
    width: u32,
    height: u32,
    rgb: Vec<u8>,
}

impl Canvas {
    pub fn new(width: u32, height: u32) -> Self {
        Self {
            width,
            height,
            rgb: vec![0u8; (width as usize) * (height as usize) * 3],
        }
    }

    /// Start from an encoded full frame.
    pub fn from_jpeg(bytes: &[u8]) -> Result<Self, CanvasError> {
        let img = image::load_from_memory(bytes)
            .map_err(|e| CanvasError::Decode(e.to_string()))?
            .to_rgb8();
        let (width, height) = (img.width(), img.height());
        Ok(Self {
            width,
            height,
            rgb: img.into_raw(),
        })
    }

    pub fn width(&self) -> u32 {
        self.width
    }

    pub fn height(&self) -> u32 {
        self.height
    }

    pub fn rgb(&self) -> &[u8] {
        &self.rgb
    }

    /// Apply one dirty rectangle: decode it and blit it at `(left, top)`.
    ///
    /// A region whose decoded size does not match the rectangle it claims to
    /// cover is refused rather than silently stretched — the coordinates are
    /// what the timeline and Click-to-Do overlays are indexed against.
    pub fn apply_region(
        &mut self,
        left: i16,
        top: i16,
        right: i16,
        bottom: i16,
        encoded: &[u8],
    ) -> Result<(), CanvasError> {
        let img = image::load_from_memory(encoded)
            .map_err(|e| CanvasError::Decode(e.to_string()))?
            .to_rgb8();
        let rw = (right as i32 - left as i32).max(0);
        let rh = (bottom as i32 - top as i32).max(0);
        if rw == 0 || rh == 0 || img.width() != rw as u32 || img.height() != rh as u32 {
            return Err(CanvasError::Size {
                width: img.width(),
                height: img.height(),
                rw: rw as u32,
                rh: rh as u32,
            });
        }
        // Clip against the surface: RDPGFX rectangles are virtual-desktop
        // coordinates, so an origin may be negative or extend past the surface.
        let (l, t) = (left as i32, top as i32);
        for row in 0..img.height() as i32 {
            let dst_y = t + row;
            if dst_y < 0 || dst_y >= self.height as i32 {
                continue;
            }
            let first = (-l).max(0);
            let last = ((self.width as i32 - l).min(img.width() as i32)).max(first);
            if last <= first {
                continue;
            }
            let span = ((last - first) as usize) * 3;
            let src = ((row as usize) * (img.width() as usize) + first as usize) * 3;
            let dst = ((dst_y as usize) * (self.width as usize) + (l + first) as usize) * 3;
            self.rgb[dst..dst + span].copy_from_slice(&img.as_raw()[src..src + span]);
        }
        Ok(())
    }

    /// Encode the composed surface — the one expensive step, once per capture.
    pub fn encode_jpeg(&self, quality: u8) -> Result<Vec<u8>, CanvasError> {
        let mut out = Vec::new();
        JpegEncoder::new_with_quality(&mut out, quality)
            .write_image(
                &self.rgb,
                self.width,
                self.height,
                ExtendedColorType::Rgb8,
            )
            .map_err(|e| CanvasError::Encode(e.to_string()))?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patch(w: u32, h: u32, rgb: [u8; 3]) -> Vec<u8> {
        let img = image::RgbImage::from_pixel(w, h, image::Rgb(rgb));
        let mut out = Vec::new();
        JpegEncoder::new_with_quality(&mut out, 90)
            .write_image(img.as_raw(), w, h, ExtendedColorType::Rgb8)
            .unwrap();
        out
    }

    fn pixel_at(canvas: &Canvas, x: u32, y: u32) -> [u8; 3] {
        let i = ((y as usize) * (canvas.width() as usize) + x as usize) * 3;
        let r = canvas.rgb();
        [r[i], r[i + 1], r[i + 2]]
    }

    /// Regions arrive JPEG-encoded, so a composed pixel is approximate: JPEG
    /// shifts a solid colour by a unit or two even at high quality.
    fn assert_near(actual: [u8; 3], expected: [u8; 3], slack: u8) {
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert!(
                a.abs_diff(*e) <= slack,
                "expected {expected:?} ±{slack}, got {actual:?}"
            );
        }
    }

    #[test]
    fn regions_compose_into_the_surface() {
        let mut canvas = Canvas::new(64, 48);
        canvas
            .apply_region(0, 0, 32, 24, &patch(32, 24, [255, 0, 0]))
            .unwrap();
        canvas
            .apply_region(32, 24, 64, 48, &patch(32, 24, [0, 0, 255]))
            .unwrap();
        // Both patches landed where their rectangles said.
        assert_near(pixel_at(&canvas, 5, 5), [255, 0, 0], 4);
        assert_near(pixel_at(&canvas, 40, 40), [0, 0, 255], 4);
        // ...and nowhere else (the untouched corner stays black).
        assert_near(pixel_at(&canvas, 40, 5), [0, 0, 0], 2);
        assert_near(pixel_at(&canvas, 5, 40), [0, 0, 0], 2);
    }

    #[test]
    fn the_composed_surface_round_trips_through_the_archive_format() {
        let mut canvas = Canvas::new(64, 48);
        canvas
            .apply_region(16, 8, 48, 40, &patch(32, 32, [0, 200, 0]))
            .unwrap();
        let jpeg = canvas.encode_jpeg(90).unwrap();
        assert_eq!(&jpeg[..2], &[0xFF, 0xD8], "a real JPEG, not a blob");
        let back = Canvas::from_jpeg(&jpeg).unwrap();
        assert_eq!(back.width(), 64);
        assert_eq!(back.height(), 48);
        let px = pixel_at(&back, 32, 24);
        assert!(px[1] > 180 && px[0] < 60 && px[2] < 60, "green patch survived: {px:?}");
    }

    #[test]
    fn a_mismatched_region_is_refused_not_stretched() {
        let mut canvas = Canvas::new(64, 48);
        let err = canvas
            .apply_region(0, 0, 32, 24, &patch(16, 16, [10, 10, 10]))
            .unwrap_err();
        assert!(matches!(err, CanvasError::Size { .. }), "{err}");
        // Nothing was written.
        assert_near(pixel_at(&canvas, 1, 1), [0, 0, 0], 1);
    }

    #[test]
    fn regions_are_clipped_to_the_surface() {
        let mut canvas = Canvas::new(32, 32);
        // A region extending past the right/bottom edges must not panic or
        // corrupt the buffer (virtual desktops really do have such rects).
        canvas
            .apply_region(16, 16, 48, 48, &patch(32, 32, [7, 7, 7]))
            .unwrap();
        assert_near(pixel_at(&canvas, 31, 31), [7, 7, 7], 4);
        assert_eq!(canvas.rgb().len(), 32 * 32 * 3);
    }

    #[test]
    fn garbage_is_a_decode_error_not_a_panic() {
        let mut canvas = Canvas::new(8, 8);
        let err = canvas.apply_region(0, 0, 8, 8, b"not an image").unwrap_err();
        assert!(matches!(err, CanvasError::Decode(_)), "{err}");
    }
}
