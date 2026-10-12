// SPDX-License-Identifier: GPL-2.0-only
//! Text/image embedding providers.
//!
//! * `Http` — calls the Python AI pipeline (`precall-ai`), which wraps the
//!   real models (sentence-transformers / CLIP).
//! * `Local` — deterministic hashing embedder. Not a real model: it exists so
//!   dev/CI runs and the semantic-search plumbing is exercised end-to-end
//!   without a GPU or a 400MB model download. Scores are meaningful only as
//!   lexical-similarity proxies.
//! * `Disabled` — FTS5-only search.

use sha2::{Digest, Sha256};

pub const EMBED_DIM: usize = 384;

/// Identity of the deterministic in-process embedder's vector spaces.
///
/// The Python fallback hashes differently (XOR vs. sum of digests), so its
/// space id differs — switching between the two *must* invalidate the stores
/// instead of silently comparing incomparable vectors.
pub const LOCAL_TEXT_SPACE: &str = "hash:rust-text@384";
pub const LOCAL_IMAGE_SPACE: &str = "hash:rust-image@384";

/// Why an embedding call ended the way it did.
///
/// `Option<Vec<f32>>` cannot express this, and the enrichment loop needs the
/// distinction: a capture whose embedding failed because the AI service was
/// down must be retried, while one whose *input* the service refused will never
/// succeed and must stop consuming attempts.
#[derive(Debug, Clone, PartialEq)]
pub enum EmbedOutcome {
    /// The vector, plus the space it belongs to. Vectors from different models
    /// are incomparable even at equal dimension, so the space travels with the
    /// value rather than being assumed.
    Ok { embedding: Vec<f32>, space: String },
    /// No embedder is configured — nothing to retry.
    Disabled,
    /// Upstream refused this input (4xx, e.g. an undecodable frame).
    Rejected,
    /// Transport error, timeout or 5xx — retry later.
    Retry,
}

impl EmbedOutcome {
    /// The embedding, if this call produced one.
    pub fn embedding(self) -> Option<Vec<f32>> {
        match self {
            EmbedOutcome::Ok { embedding, .. } => Some(embedding),
            _ => None,
        }
    }
}

/// Is this status the service refusing *this input* (so retrying the same bytes
/// is futile), or a service-wide condition that may clear on its own?
///
/// Only genuinely input-specific statuses are terminal. 408/429 are transient,
/// 5xx is an outage, and 401/404 are configuration or rollout states that
/// disappear once the deployment is fixed — treating those as refusals used to
/// mark screenshots `skipped` permanently.
fn is_input_refusal(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 400 | 413 | 415 | 422)
}

#[derive(Debug, Clone)]
pub enum Embedder {
    /// `http://host:port` of the AI service.
    Http { base: String, client: reqwest::Client },
    Local,
    Disabled,
}

impl Embedder {
    pub fn from_env() -> Self {
        match std::env::var("PRECALL_AI_URL") {
            Ok(url) if !url.is_empty() => Embedder::Http {
                base: url.trim_end_matches('/').to_string(),
                client: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new()),
            },
            _ => Embedder::Local,
        }
    }

    /// Which text/image vector spaces this embedder produces right now.
    ///
    /// For the in-process embedder these are constants. For the AI service the
    /// spaces are whatever *it* reports at `/health`, because the same URL can
    /// be backed by a hash fallback today and CLIP tomorrow: `None` means the
    /// service did not answer, so the caller must not conclude that anything
    /// changed.
    pub async fn spaces(&self) -> Option<(String, String)> {
        match self {
            Embedder::Local => Some((LOCAL_TEXT_SPACE.to_string(), LOCAL_IMAGE_SPACE.to_string())),
            Embedder::Disabled => None,
            Embedder::Http { base, client } => {
                #[derive(serde::Deserialize)]
                struct Health {
                    text_space_id: Option<String>,
                    image_space_id: Option<String>,
                }
                let r = client.get(format!("{base}/health")).send().await.ok()?;
                if !r.status().is_success() {
                    return None;
                }
                let h: Health = r.json().await.ok()?;
                match (h.text_space_id, h.image_space_id) {
                    (Some(t), Some(i)) => Some((t, i)),
                    _ => None,
                }
            }
        }
    }

    /// Embed text → `dim` f32s. `Disabled` when no embedder is configured.
    pub async fn embed_text(&self, text: &str) -> EmbedOutcome {
        match self {
            Embedder::Http { base, client } => {
                #[derive(serde::Serialize)]
                struct Req<'a> {
                    text: &'a str,
                }
                #[derive(serde::Deserialize)]
                struct Resp {
                    embedding: Vec<f32>,
                    #[serde(default)]
                    space: Option<String>,
                }
                let r = match client
                    .post(format!("{base}/embed/text"))
                    .json(&Req { text })
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::debug!("text embed request failed: {e}");
                        return EmbedOutcome::Retry;
                    }
                };
                let status = r.status();
                if is_input_refusal(status) {
                    return EmbedOutcome::Rejected;
                }
                if !status.is_success() {
                    if status == reqwest::StatusCode::NOT_FOUND
                        || status == reqwest::StatusCode::NOT_IMPLEMENTED
                    {
                        // The route itself is absent (wrong URL, older build).
                        return EmbedOutcome::Disabled;
                    }
                    tracing::debug!("text embedder unavailable: {status}");
                    return EmbedOutcome::Retry;
                }
                match r.json::<Resp>().await {
                    Ok(x) => EmbedOutcome::Ok {
                        embedding: x.embedding,
                        space: x.space.unwrap_or_else(|| format!("http:{base}")),
                    },
                    Err(e) => {
                        tracing::debug!("text embed response unreadable: {e}");
                        EmbedOutcome::Retry
                    }
                }
            }
            Embedder::Local => EmbedOutcome::Ok {
                embedding: hash_embedding(text, EMBED_DIM),
                space: LOCAL_TEXT_SPACE.to_string(),
            },
            Embedder::Disabled => EmbedOutcome::Disabled,
        }
    }

    /// Image embedding — same plumbing for the `si_image` space.
    ///
    /// Returns an [`EmbedOutcome`] rather than an `Option` because the caller
    /// (enrichment) must treat "retry later" and "this input is unacceptable"
    /// differently; see [`EmbedOutcome`].
    pub async fn embed_image(&self, jpeg: &[u8]) -> EmbedOutcome {
        match self {
            Embedder::Http { base, client } => {
                #[derive(serde::Deserialize)]
                struct Resp {
                    embedding: Vec<f32>,
                    #[serde(default)]
                    space: Option<String>,
                }
                let r = match client
                    .post(format!("{base}/embed/image"))
                    .body(jpeg.to_vec())
                    .header(reqwest::header::CONTENT_TYPE, "image/jpeg")
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::debug!("image embed request failed: {e}");
                        return EmbedOutcome::Retry;
                    }
                };
                let status = r.status();
                if is_input_refusal(status) {
                    // The service understood the request and refused this
                    // input (bad/oversized/undecodable frame) — retrying the
                    // same bytes cannot succeed.
                    tracing::debug!("image embedder rejected input: {status}");
                    return EmbedOutcome::Rejected;
                }
                if !status.is_success() {
                    if status == reqwest::StatusCode::NOT_FOUND
                        || status == reqwest::StatusCode::NOT_IMPLEMENTED
                    {
                        return EmbedOutcome::Disabled;
                    }
                    tracing::debug!("image embedder unavailable: {status}");
                    return EmbedOutcome::Retry;
                }
                match r.json::<Resp>().await {
                    Ok(x) => EmbedOutcome::Ok {
                        embedding: x.embedding,
                        space: x.space.unwrap_or_else(|| format!("http:{base}")),
                    },
                    Err(e) => {
                        tracing::debug!("image embed response unreadable: {e}");
                        EmbedOutcome::Retry
                    }
                }
            }
            Embedder::Local => {
                // Content-hash embedding for image bytes. Must be `EMBED_DIM`
                // wide and L2-normalised, because it is written straight into
                // a `dim`-wide `si_*` store; the Python fallback
                // (`hash_embedding_bytes`) is the reference implementation.
                let mut acc = [0u8; 32];
                for chunk in jpeg.chunks(32) {
                    let h = Sha256::digest(chunk);
                    for (a, b) in acc.iter_mut().zip(h.iter()) {
                        *a = a.wrapping_add(*b);
                    }
                }
                let mut v: Vec<f32> = (0..EMBED_DIM)
                    .map(|i| (acc[i % acc.len()] as f32 / 255.0) - 0.5)
                    .collect();
                let n = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
                for x in v.iter_mut() {
                    *x /= n;
                }
                EmbedOutcome::Ok {
                    embedding: v,
                    space: LOCAL_IMAGE_SPACE.to_string(),
                }
            }
            Embedder::Disabled => EmbedOutcome::Disabled,
        }
    }

    /// Text → **image space** embedding.
    ///
    /// Only the AI service can produce this: it needs the same CLIP stack that
    /// encoded the stored screenshots. The service answers `501` when its image
    /// backend is the deterministic hash fallback, because hashed text and
    /// hashed pixels live in different spaces — searching one with the other
    /// returns noise, not "no matches". `Disabled` therefore means *this
    /// deployment cannot answer text queries against the image store* and
    /// callers must report the leg as skipped; `Retry` means the service is
    /// reachable in principle but not answering right now.
    pub async fn embed_image_text(&self, text: &str) -> EmbedOutcome {
        let Embedder::Http { base, client } = self else {
            return EmbedOutcome::Disabled;
        };
        #[derive(serde::Serialize)]
        struct Req<'a> {
            text: &'a str,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            embedding: Vec<f32>,
            #[serde(default)]
            space: Option<String>,
        }
        let r = match client
            .post(format!("{base}/embed/image-text"))
            .json(&Req { text })
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!("image-text embed request failed: {e}");
                return EmbedOutcome::Retry;
            }
        };
        let status = r.status();
        // 501 (and 404 from an older service) states, permanently, that there
        // is no CLIP text encoder here — a missing capability, not an outage.
        if status == reqwest::StatusCode::NOT_IMPLEMENTED
            || status == reqwest::StatusCode::NOT_FOUND
        {
            return EmbedOutcome::Disabled;
        }
        if is_input_refusal(status) {
            return EmbedOutcome::Rejected;
        }
        if !status.is_success() {
            return EmbedOutcome::Retry;
        }
        match r.json::<Resp>().await {
            Ok(x) => EmbedOutcome::Ok {
                embedding: x.embedding,
                space: x.space.unwrap_or_else(|| format!("http:{base}")),
            },
            Err(e) => {
                tracing::debug!("image-text embed response unreadable: {e}");
                EmbedOutcome::Retry
            }
        }
    }

    /// Topic extraction via AI service (returns [] when unavailable).
    pub async fn topics(&self, text: &str) -> Vec<(String, f64)> {
        let Embedder::Http { base, client } = self else {
            return Vec::new();
        };
        #[derive(serde::Serialize)]
        struct Req<'a> {
            text: &'a str,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            topics: Vec<TopicOut>,
        }
        #[derive(serde::Deserialize)]
        struct TopicOut {
            label: String,
            score: f64,
        }
        let resp = client
            .post(format!("{base}/topics"))
            .json(&Req { text })
            .send()
            .await;
        match resp {
            Ok(r) if r.status().is_success() => r
                .json::<Resp>()
                .await
                .map(|x| x.topics.into_iter().map(|t| (t.label, t.score)).collect())
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }
}

/// Deterministic word-hash embedding — bag-of-words hashed into `dim`
/// buckets with per-word position weighting. Good enough for plumbing tests.
fn hash_embedding(text: &str, dim: usize) -> Vec<f32> {
    let mut v = vec![0f32; dim];
    for (pos, word) in text.split_whitespace().enumerate() {
        let w = word.to_lowercase();
        let h = Sha256::digest(w.as_bytes());
        let idx = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize % dim;
        let sign = if h[4] & 1 == 1 { 1.0 } else { -1.0 };
        v[idx] += sign * (1.0 / (1.0 + pos as f32 * 0.05));
    }
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    for x in v.iter_mut() {
        *x /= n;
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_embedding_is_deterministic_and_normalized() {
        let e = Embedder::Local;
        let EmbedOutcome::Ok { embedding: a, .. } = e.embed_text("kerberos tickets").await else {
            panic!("local embedder must succeed");
        };
        let EmbedOutcome::Ok { embedding: b, .. } = e.embed_text("kerberos tickets").await else {
            panic!("local embedder must succeed");
        };
        assert_eq!(a, b);
        let n: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n - 1.0).abs() < 1e-3);
    }

    /// The image embedding is written straight into a `dim`-wide `si_*` store,
    /// so it must be `EMBED_DIM` long and normalised — and distinct images must
    /// not collide.
    #[tokio::test]
    async fn local_image_embedding_fits_the_store() {
        let e = Embedder::Local;
        let EmbedOutcome::Ok { embedding: a, .. } = e.embed_image(b"\xff\xd8scene-a\xff\xd9").await else {
            panic!("local embedder must succeed");
        };
        let EmbedOutcome::Ok { embedding: b, .. } = e.embed_image(b"\xff\xd8scene-b\xff\xd9").await else {
            panic!("local embedder must succeed");
        };
        assert_eq!(a.len(), EMBED_DIM);
        let n: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n - 1.0).abs() < 1e-3, "not normalised: {n}");
        assert_ne!(a, b);
    }

    /// Only input-specific statuses are terminal; transient and
    /// deployment-level ones must stay retryable.
    #[test]
    fn only_input_specific_statuses_are_refusals() {
        for code in [400u16, 413, 415, 422] {
            assert!(
                is_input_refusal(reqwest::StatusCode::from_u16(code).unwrap()),
                "{code} should be a refusal"
            );
        }
        for code in [401u16, 403, 404, 408, 429, 500, 502, 503] {
            assert!(
                !is_input_refusal(reqwest::StatusCode::from_u16(code).unwrap()),
                "{code} must stay retryable"
            );
        }
    }

    /// A 4xx refusal is terminal; a 5xx/transport failure is an outage. The
    /// enricher depends on the difference.
    #[tokio::test]
    async fn image_embed_outcome_separates_rejection_from_outage() {
        use axum::routing::post;
        use std::net::SocketAddr;

        async fn refused() -> (axum::http::StatusCode, &'static str) {
            (axum::http::StatusCode::BAD_REQUEST, "undecodable frame")
        }
        async fn broken() -> (axum::http::StatusCode, &'static str) {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "gpu on fire")
        }

        async fn serve(route: axum::Router) -> SocketAddr {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, route).await });
            addr
        }

        let refused_addr = serve(axum::Router::new().route("/embed/image", post(refused))).await;
        let broken_addr = serve(axum::Router::new().route("/embed/image", post(broken))).await;
        let dead_addr = {
            // Bind and drop, so nothing is listening on that port.
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let a = l.local_addr().unwrap();
            drop(l);
            a
        };

        let embedder = |addr: SocketAddr| Embedder::Http {
            base: format!("http://{addr}"),
            client: reqwest::Client::new(),
        };

        assert_eq!(embedder(refused_addr).embed_image(b"x").await, EmbedOutcome::Rejected);
        assert_eq!(embedder(broken_addr).embed_image(b"x").await, EmbedOutcome::Retry);
        assert_eq!(embedder(dead_addr).embed_image(b"x").await, EmbedOutcome::Retry);
        assert_eq!(Embedder::Disabled.embed_image(b"x").await, EmbedOutcome::Disabled);
    }
}
