// SPDX-License-Identifier: GPL-2.0-only
//! Background enrichment: pull `IsProcessed=0` captures, embed their OCR text
//! into `SemanticTextStore.db`, embed their stored screenshot into
//! `SemanticImageStore.db`, extract topics (when the AI service is attached),
//! and mark them processed.
//!
//! **Both** `si_*` spaces are populated here. The image space is what makes
//! "show me the screenshot where X" answerable without OCR text; it is fed
//! from the JPEG object the broker wrote (`{client}/{y}/{m}/{d}/{token}.jpg`),
//! read back through the same at-rest cipher the `/snapshot` route uses.
//!
//! Runs in the API process so the broker stays a lean ingester. Polls each
//! client's store on a cadence — cheap for workstation-scale data.

use crate::embed::Embedder;
use precall_store::objects::{ymd_from_filetime, ObjectStore};
use precall_store::{SemanticIndex, Ukg};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, warn};

/// What one enrichment pass accomplished. Tests assert on it; the polling
/// loop logs it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EnrichStats {
    pub text_embeddings: usize,
    pub image_embeddings: usize,
    /// Captures that could not be image-embedded: no image token, no
    /// timestamp, or no JPEG object on disk (e.g. the frame arrived with a
    /// non-JPEG codec).
    pub images_skipped: usize,
}

/// Poll `data_root` for unprocessed captures every `interval`.
///
/// `object_key` must be the at-rest key the broker encrypted screenshots with
/// — the same `[u8; 32]` handed to [`ObjectStore::encrypted`].
pub fn spawn_enricher(
    data_root: PathBuf,
    embedder: Arc<Embedder>,
    dim: usize,
    interval: std::time::Duration,
    object_key: [u8; 32],
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match enrich_once(&data_root, &embedder, dim, object_key).await {
                Ok(s) if s != EnrichStats::default() => debug!(?s, "enrichment pass complete"),
                Ok(_) => debug!("enrichment pass: nothing to do"),
                Err(e) => warn!("enrichment pass failed: {e}"),
            }
            tokio::time::sleep(interval).await;
        }
    })
}

/// One pass over every client store under `data_root`.
///
/// Public so tests can drive a pass deterministically instead of racing the
/// poller.
pub async fn enrich_once(
    data_root: &Path,
    embedder: &Embedder,
    dim: usize,
    object_key: [u8; 32],
) -> anyhow::Result<EnrichStats> {
    let mut stats = EnrichStats::default();

    let mut dirs = Vec::new();
    let mut rd = std::fs::read_dir(data_root)?;
    while let Some(ent) = rd.next().transpose()? {
        let p = ent.path();
        if p.is_dir() && p.join("ukg.db").exists() {
            dirs.push(p);
        }
    }

    for dir in dirs {
        // The client id is the directory name, exactly as the broker wrote it.
        let client = dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();

        let ukg = Ukg::open(&dir.join("ukg.db"))?;
        let pending: Vec<(i64, Option<i64>, Option<String>)> = {
            let mut stmt = ukg.conn().prepare(
                "SELECT \"Id\",\"TimeStamp\",\"ImageToken\" FROM \"WindowCapture\"
                 WHERE \"IsProcessed\" = 0 LIMIT 64",
            )?;
            let rows = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        if pending.is_empty() {
            continue;
        }

        let text_index = SemanticIndex::open(&dir.join("SemanticTextStore.db"), dim, "text")?;
        let image_index = SemanticIndex::open(&dir.join("SemanticImageStore.db"), dim, "image")?;
        let objects = ObjectStore::encrypted(dir.join("objects"), object_key)?;

        for (wc_id, ts, image_token) in pending {
            let item = SemanticIndex::item_id_for_capture(wc_id);

            // --- text leg: OCR regions → SemanticTextStore -----------------
            let regions = ukg.regions(wc_id)?;
            let text: String = regions
                .iter()
                .filter_map(|(_, _, ocr, _)| ocr.clone())
                .collect::<Vec<_>>()
                .join(" ");
            if !text.is_empty() && !text_index.has_item(&item)? {
                if let Some(emb) = embedder.embed_text(&text).await {
                    text_index.insert(&item, None, None, &emb)?;
                    stats.text_embeddings += 1;
                }
                for (label, score) in embedder.topics(&text).await {
                    ukg.attach_topic(wc_id, &label, score)?;
                }
            }

            // --- image leg: stored JPEG → SemanticImageStore ---------------
            match (image_token, ts) {
                (Some(token), Some(ts)) => {
                    // The guard makes the pass restart-safe: an insert that
                    // succeeded before a crash must not be duplicated.
                    if !image_index.has_item(&item)? {
                        let rel = ObjectStore::key_path(
                            &client,
                            ymd_from_filetime(ts),
                            &token,
                            "jpg",
                        );
                        if objects.exists(&rel) {
                            let jpeg = objects.get(&client, &rel, &token)?;
                            match embedder.embed_image(&jpeg).await {
                                Some(emb) => {
                                    image_index.insert(&item, None, None, &emb)?;
                                    stats.image_embeddings += 1;
                                }
                                None => warn!(
                                    client, wc_id,
                                    "image embedder unavailable — processed without an image embedding"
                                ),
                            }
                        } else {
                            stats.images_skipped += 1;
                            debug!(client, wc_id, "no jpeg object for capture");
                        }
                    }
                }
                _ => stats.images_skipped += 1,
            }

            ukg.mark_processed(wc_id)?;
        }
    }

    Ok(stats)
}
