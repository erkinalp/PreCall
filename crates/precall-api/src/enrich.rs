// SPDX-License-Identifier: GPL-2.0-only
//! Background enrichment: pull captures out of the metadata database, embed
//! their OCR text into `SemanticTextStore.db` and their stored screenshot into
//! `SemanticImageStore.db`, extract topics (when the AI service is attached),
//! and mark them processed.
//!
//! **Both** `si_*` spaces are populated here. The image space is what makes
//! "show me the screenshot where X" answerable without OCR text; it is fed from
//! the JPEG object the broker wrote (`{client}/{y}/{m}/{d}/{token}.jpg`), read
//! back through the same at-rest cipher the `/snapshot` route uses.
//!
//! ## Two queues, two predicates
//!
//! The text leg can key off `WindowCapture.IsProcessed`, but that flag cannot
//! express the image leg's two awkward cases, so image state lives in
//! `PrecallImageEmbedding` (`pending` | `done` | `skipped`) and the pass runs:
//!
//! 1. **backfill / retry** — captures already processed (or already attempted)
//!    whose image embedding is missing or `pending`. This is what gives an
//!    upgraded installation its historical screenshots back, and what retries
//!    a capture after an AI-service outage.
//! 2. **fresh** — `IsProcessed = 0`, text + image legs inline.
//!
//! A transient failure (transport error, timeout, 5xx) leaves the capture
//! retryable: the image state stays `pending` and a failed text leg leaves
//! `IsProcessed = 0`. A *refused* input (4xx — an undecodable frame) is
//! terminal: `skipped`, with the error recorded, because retrying identical
//! bytes can never succeed.
//!
//! Runs in the API process so the broker stays a lean ingester. Polls each
//! client's store on a cadence — cheap for workstation-scale data.

use crate::embed::{EmbedOutcome, Embedder};
use precall_store::objects::{ymd_from_filetime, ObjectStore};
use precall_store::{SemanticIndex, Ukg};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, warn};

/// What one enrichment pass accomplished. Tests assert on it; the polling loop
/// logs it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EnrichStats {
    pub text_embeddings: usize,
    pub image_embeddings: usize,
    /// Images embedded for captures that were already processed — the
    /// backfill path (upgraded installs, recovered outages).
    pub backfilled: usize,
    /// Captures whose image embedding is terminally unavailable: no image
    /// token, no timestamp, a non-JPEG frame, or an input the embedder
    /// refused.
    pub images_skipped: usize,
    /// Captures whose image embedding failed transiently and will be retried.
    pub images_pending: usize,
    /// Captures left unprocessed because their text embedding failed
    /// transiently and will be retried.
    pub texts_pending: usize,
}

/// How one capture's image leg ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImageLeg {
    /// Embedded now, or already present.
    Done,
    /// Nothing to do, ever (no JPEG, refused input, embeddings disabled by
    /// configuration).
    Terminal,
    /// Transient — the caller must not mark the capture finished.
    Retry,
    /// The embedder itself is switched off; stop draining this queue.
    Unavailable,
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

    // Asked once per pass, not once per client: this is an HTTP round-trip for
    // the AI-backed embedder, and every client shares the answer.
    let spaces = embedder.spaces().await;

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
        let text_index = SemanticIndex::open(
            &dir.join("SemanticTextStore.db"),
            dim,
            precall_store::SPACE_UNKNOWN,
        )?;
        let image_index = SemanticIndex::open(
            &dir.join("SemanticImageStore.db"),
            dim,
            precall_store::SPACE_UNKNOWN,
        )?;
        let objects = ObjectStore::encrypted(dir.join("objects"), object_key)?;
        let now = precall_proto::time::filetime_now() as i64;

        // A vector store is only meaningful against the embedder that filled
        // it. Enabling the AI service (or switching models) after a run with
        // the hash embedder leaves rows that are incomparable with the new
        // queries, so the affected store is rebuilt instead of being ranked
        // against them. `spaces()` answers `None` when the service cannot say,
        // and then nothing is invalidated.
        if let Some((text_space, image_space)) = spaces.as_ref() {
            if let Some(stored) = text_index.space_id()? {
                if stored != *text_space && stored != precall_store::SPACE_UNKNOWN {
                    warn!(client, stored, active = %text_space,
                          "text embedding space changed; rebuilding SemanticTextStore");
                    text_index.clear()?;
                    ukg.mark_all_unprocessed()?;
                }
            }
            text_index.set_space_id(text_space)?;

            if let Some(stored) = image_index.space_id()? {
                if stored != *image_space && stored != precall_store::SPACE_UNKNOWN {
                    warn!(client, stored, active = %image_space,
                          "image embedding space changed; rebuilding SemanticImageStore");
                    image_index.clear()?;
                    ukg.reset_image_embedding_states()?;
                }
            }
            image_index.set_space_id(image_space)?;
        }

        // Queue 1: already-processed captures with no image embedding (or a
        // pending one whose backoff has elapsed). Empty on a fresh install's
        // first pass.
        let backfill = ukg.captures_needing_image_embedding(BATCH, now, RETRY_BACKOFF_TICKS)?;

        // Queue 2: fresh captures — both legs inline.
        let fresh: Vec<(i64, Option<i64>, Option<String>)> = {
            let mut stmt = ukg.conn().prepare(
                "SELECT \"Id\",\"TimeStamp\",\"ImageToken\" FROM \"WindowCapture\"
                 WHERE \"IsProcessed\" = 0 ORDER BY \"Id\" LIMIT ?",
            )?;
            let rows = stmt
                .query_map([BATCH], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        if backfill.is_empty() && fresh.is_empty() {
            continue;
        }

        // --- Queue 1: backfill / retry (image leg only) ---------------------
        //
        // The await sits between `image_step` and `image_record` rather than
        // inside a helper that takes `&Ukg`/`&SemanticIndex`: both wrap a
        // `rusqlite::Connection` (Send but *not* Sync), so a reference to one
        // held across an await makes this future non-Send and `tokio::spawn` in
        // `spawn_enricher` stops compiling.
        let mut stop_images = false;
        for (wc_id, ts, token) in backfill {
            let step = image_step(
                &ukg, &image_index, &objects, &client, wc_id, ts, &token, &mut stats,
            )?;
            let leg = match step {
                ImageStep::Embed { jpeg, attempts } => {
                    let outcome = embedder.embed_image(&jpeg).await;
                    image_record(&ukg, &image_index, wc_id, attempts, outcome, &mut stats)?
                }
                ImageStep::Settled(leg) => leg,
            };
            match leg {
                ImageLeg::Done => stats.backfilled += 1,
                ImageLeg::Unavailable => {
                    stop_images = true;
                    break;
                }
                ImageLeg::Terminal | ImageLeg::Retry => {}
            }
        }

        // --- Queue 2: fresh captures (text + image legs) --------------------
        for (wc_id, ts, image_token) in fresh {
            let item = SemanticIndex::item_id_for_capture(wc_id);

            // --- text leg: OCR regions → SemanticTextStore -----------------
            let regions = ukg.regions(wc_id)?;
            let text: String = regions
                .iter()
                .filter_map(|(_, _, ocr, _)| ocr.clone())
                .collect::<Vec<_>>()
                .join(" ");
            let mut text_retry = false;
            if !text.is_empty() && !text_index.has_item(&item)? {
                match embedder.embed_text(&text).await {
                    EmbedOutcome::Ok { embedding: emb, space } => {
                        text_index.insert(&item, None, None, &emb)?;
                        text_index.set_space_id(&space)?;
                        stats.text_embeddings += 1;
                    }
                    EmbedOutcome::Disabled => {}
                    EmbedOutcome::Rejected => {
                        warn!(client, wc_id, "text embedder refused the input; marking processed")
                    }
                    EmbedOutcome::Retry => {
                        text_retry = true;
                        stats.texts_pending += 1;
                    }
                }
                // The `has_item` guard above means topics are attached exactly
                // once, even when a later leg forces a retry of this capture.
                if !text_retry {
                    for (label, score) in embedder.topics(&text).await {
                        ukg.attach_topic(wc_id, &label, score)?;
                    }
                }
            }

            // --- image leg: stored JPEG → SemanticImageStore ---------------
            let image_leg = if stop_images {
                ImageLeg::Unavailable
            } else if let Some(token) = image_token {
                let step = image_step(
                    &ukg, &image_index, &objects, &client, wc_id, ts, &token, &mut stats,
                )?;
                match step {
                    ImageStep::Embed { jpeg, attempts } => {
                        let outcome = embedder.embed_image(&jpeg).await;
                        image_record(&ukg, &image_index, wc_id, attempts, outcome, &mut stats)?
                    }
                    ImageStep::Settled(leg) => leg,
                }
            } else {
                stats.images_skipped += 1;
                ImageLeg::Terminal
            };
            if image_leg == ImageLeg::Unavailable {
                stop_images = true;
            }

            // Only a capture with nothing left to retry may leave the queue.
            if !text_retry && image_leg != ImageLeg::Retry {
                ukg.mark_processed(wc_id)?;
            }
        }
    }

    Ok(stats)
}

/// What to do with one capture's image leg, decided without any `await`.
///
/// Split from [`image_record`] so the embedder call happens in the caller's
/// scope: a `&Ukg`/`&SemanticIndex` held across an await would make the pass
/// non-`Send`.
enum ImageStep {
    /// Hand `jpeg` to the embedder, then call [`image_record`] with the
    /// attempt count.
    Embed { jpeg: Vec<u8>, attempts: i64 },
    /// Nothing to embed — already settled.
    Settled(ImageLeg),
}

/// Everything the image leg needs before the embedder call: read the object,
/// check the recorded state, and settle the cases that need no embedding.
#[allow(clippy::too_many_arguments)]
fn image_step(
    ukg: &Ukg,
    image_index: &SemanticIndex,
    objects: &ObjectStore,
    client: &str,
    wc_id: i64,
    ts: Option<i64>,
    token: &str,
    stats: &mut EnrichStats,
) -> anyhow::Result<ImageStep> {
    let item = SemanticIndex::item_id_for_capture(wc_id);

    // Restart safety: an insert that succeeded before a crash must not be
    // repeated, whatever the state row says.
    if image_index.has_item(&item)? {
        return Ok(ImageStep::Settled(ImageLeg::Done));
    }

    let prior = ukg.image_embedding_state(wc_id)?;
    let attempts = prior.as_ref().map(|(_, a, _)| *a).unwrap_or(0);
    if let Some((state, _, err)) = &prior {
        if state == "skipped" {
            debug!(
                client, wc_id, attempts,
                err = err.as_deref(),
                "image embedding previously refused; not retrying"
            );
            stats.images_skipped += 1;
            return Ok(ImageStep::Settled(ImageLeg::Terminal));
        }
    }

    let now = precall_proto::time::filetime_now() as i64;
    let Some(ts) = ts else {
        // Without a timestamp the object key cannot be rebuilt. Terminal, with
        // the reason recorded so an operator can see why.
        ukg.set_image_embedding_state(wc_id, "skipped", attempts + 1, Some("no timestamp"), now)?;
        stats.images_skipped += 1;
        return Ok(ImageStep::Settled(ImageLeg::Terminal));
    };

    let rel = ObjectStore::key_path(client, ymd_from_filetime(ts), token, "jpg");
    if !objects.exists(&rel) {
        let raw = ObjectStore::key_path(client, ymd_from_filetime(ts), token, "raw");
        if objects.exists(&raw) {
            // Non-JPEG frames have no decode path here: the embedders take
            // encoded images. Retrying would never change that.
            ukg.set_image_embedding_state(
                wc_id,
                "skipped",
                attempts + 1,
                Some("non-JPEG frame"),
                now,
            )?;
            stats.images_skipped += 1;
            return Ok(ImageStep::Settled(ImageLeg::Terminal));
        }
        // The metadata arrived but the object is not there (or not yet).
        ukg.set_image_embedding_state(
            wc_id,
            "pending",
            attempts + 1,
            Some("object missing"),
            now,
        )?;
        stats.images_pending += 1;
        return Ok(ImageStep::Settled(ImageLeg::Retry));
    }

    let jpeg = objects.get(client, &rel, token)?;
    Ok(ImageStep::Embed { jpeg, attempts })
}

/// Record the embedder's verdict in `PrecallImageEmbedding` so the next pass
/// knows whether this capture may be forgotten or must be retried.
fn image_record(
    ukg: &Ukg,
    image_index: &SemanticIndex,
    wc_id: i64,
    attempts: i64,
    outcome: EmbedOutcome,
    stats: &mut EnrichStats,
) -> anyhow::Result<ImageLeg> {
    let now = precall_proto::time::filetime_now() as i64;
    match outcome {
        EmbedOutcome::Ok { embedding, space } => {
            let item = SemanticIndex::item_id_for_capture(wc_id);
            image_index.insert(&item, None, None, &embedding)?;
            image_index.set_space_id(&space)?;
            ukg.set_image_embedding_state(wc_id, "done", attempts + 1, None, now)?;
            stats.image_embeddings += 1;
            Ok(ImageLeg::Done)
        }
        EmbedOutcome::Disabled => {
            stats.images_skipped += 1;
            Ok(ImageLeg::Unavailable)
        }
        EmbedOutcome::Rejected => {
            // The service refused these bytes; retrying cannot help.
            ukg.set_image_embedding_state(
                wc_id,
                "skipped",
                attempts + 1,
                Some("embedder refused input"),
                now,
            )?;
            stats.images_skipped += 1;
            Ok(ImageLeg::Terminal)
        }
        EmbedOutcome::Retry => {
            ukg.set_image_embedding_state(
                wc_id,
                "pending",
                attempts + 1,
                Some("embedder unavailable"),
                now,
            )?;
            stats.images_pending += 1;
            Ok(ImageLeg::Retry)
        }
    }
}

/// Captures handled per queue per pass.
const BATCH: i64 = 64;

/// How long a failed image attempt waits before it is retried. Without this a
/// batch of stuck captures is re-attempted every poll interval and, because the
/// queue is `LIMIT`ed, starves every later capture.
const RETRY_BACKOFF_TICKS: i64 = 60 * 10_000_000; // 60s
