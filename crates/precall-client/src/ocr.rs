// SPDX-License-Identifier: GPL-2.0-only
//! On-device OCR via `Windows.Media.Ocr` — the same WinRT engine Recall
//! uses for Click-to-Do regions. Produces word-level `ScreenRegion` rows
//! whose text lands in the FTS5 index server-side.

use crate::ClientError;
use precall_proto::metadata::ScreenRegionRecord;
use std::sync::OnceLock;
use windows::Graphics::Imaging::{BitmapPixelFormat, SoftwareBitmap};
use windows::Media::Ocr::OcrEngine;
use windows::Storage::Streams::DataWriter;

fn engine() -> Option<&'static OcrEngine> {
    static ENGINE: OnceLock<Option<OcrEngine>> = OnceLock::new();
    ENGINE
        .get_or_init(|| OcrEngine::TryCreateFromUserProfileLanguages().ok())
        .as_ref()
}

/// Available on this machine (Win10+ with an OCR language pack).
pub fn available() -> bool {
    engine().is_some()
}

/// OCR a BGRA frame → word regions with pixel-space bounds.
/// Failures (no language pack, oversized image) yield an empty list.
pub fn ocr_bgra(bgra: &[u8], width: u32, height: u32) -> Result<Vec<ScreenRegionRecord>, ClientError> {
    let Some(eng) = engine() else {
        return Ok(Vec::new());
    };
    let bitmap = SoftwareBitmap::Create(BitmapPixelFormat::Bgra8, width as i32, height as i32)?;
    {
        let writer = DataWriter::new()?;
        writer.WriteBytes(bgra)?;
        let buf = writer.DetachBuffer()?;
        bitmap.CopyFromBuffer(&buf)?;
    }
    let result = eng.RecognizeAsync(&bitmap)?.join()?;
    let mut regions = Vec::new();
    for line in result.Lines()? {
        for word in line.Words()? {
            let r = word.BoundingRect()?;
            regions.push(ScreenRegionRecord {
                region_kind: "text".into(),
                ocr_text: Some(word.Text()?.to_string()),
                bounds: format!(
                    "{},{},{},{}",
                    r.X as i32,
                    r.Y as i32,
                    (r.X + r.Width) as i32,
                    (r.Y + r.Height) as i32
                ),
            });
        }
    }
    Ok(regions)
}
