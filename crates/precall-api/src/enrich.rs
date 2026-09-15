// SPDX-License-Identifier: GPL-2.0-only
//! Background enrichment: pull `IsProcessed=0` captures, embed their OCR text
//! into the `si_*` stores, extract topics (when the AI service is attached),
//! and mark them processed.
//!
//! Runs in the API process so the broker stays a lean ingester. Polls each
//! client's store on a cadence — cheap for workstation-scale data.

use crate::embed::Embedder;
use precall_store::{SemanticIndex, Ukg};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, warn};

/// Poll `data_root` for unprocessed captures every `interval`.
pub fn spawn_enricher(data_root: PathBuf, embedder: Arc<Embedder>, dim: usize, interval: std::time::Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(e) = enrich_once(&data_root, &embedder, dim).await {
                warn!("enrichment pass failed: {e}");
            }
            tokio::time::sleep(interval).await;
        }
    })
}

async fn enrich_once(
    data_root: &std::path::Path,
    embedder: &Embedder,
    dim: usize,
) -> anyhow::Result<()> {
    let mut dirs = Vec::new();
    let mut rd = std::fs::read_dir(data_root)?;
    while let Some(ent) = rd.next().transpose()? {
        let p = ent.path();
        if p.is_dir() && p.join("ukg.db").exists() {
            dirs.push(p);
        }
    }
    for dir in dirs {
        let ukg = Ukg::open(&dir.join("ukg.db"))?;
        let pending: Vec<(i64, Option<i64>)> = {
            let mut stmt = ukg.conn().prepare(
                "SELECT \"Id\",\"TimeStamp\" FROM \"WindowCapture\" WHERE \"IsProcessed\" = 0 LIMIT 64",
            )?;
            let rows = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        if pending.is_empty() {
            continue;
        }
        let text_index = SemanticIndex::open(&dir.join("SemanticTextStore.db"), dim, "text")?;
        for (wc_id, _ts) in pending {
            let regions = ukg.regions(wc_id)?;
            let text: String = regions
                .iter()
                .filter_map(|(_, _, ocr, _)| ocr.clone())
                .collect::<Vec<_>>()
                .join(" ");
            if !text.is_empty() {
                if let Some(emb) = embedder.embed_text(&text).await {
                    let item = SemanticIndex::item_id_for_capture(wc_id);
                    text_index.insert(&item, None, None, &emb)?;
                }
                for (label, score) in embedder.topics(&text).await {
                    ukg.attach_topic(wc_id, &label, score)?;
                }
            }
            ukg.mark_processed(wc_id)?;
        }
    }
    debug!("enrichment pass complete");
    Ok(())
}
