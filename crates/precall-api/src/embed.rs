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

    /// Embed text → `dim` f32s. `None` when disabled.
    pub async fn embed_text(&self, text: &str) -> Option<Vec<f32>> {
        match self {
            Embedder::Http { base, client } => {
                #[derive(serde::Serialize)]
                struct Req<'a> {
                    text: &'a str,
                }
                #[derive(serde::Deserialize)]
                struct Resp {
                    embedding: Vec<f32>,
                }
                let r = client
                    .post(format!("{base}/embed/text"))
                    .json(&Req { text })
                    .send()
                    .await
                    .ok()?;
                if !r.status().is_success() {
                    return None;
                }
                let resp: Resp = r.json().await.ok()?;
                Some(resp.embedding)
            }
            Embedder::Local => Some(hash_embedding(text, EMBED_DIM)),
            Embedder::Disabled => None,
        }
    }

    /// Image embedding — same plumbing for the `si_image` space.
    pub async fn embed_image(&self, jpeg: &[u8]) -> Option<Vec<f32>> {
        match self {
            Embedder::Http { base, client } => {
                #[derive(serde::Deserialize)]
                struct Resp {
                    embedding: Vec<f32>,
                }
                let r = client
                    .post(format!("{base}/embed/image"))
                    .body(jpeg.to_vec())
                    .header(reqwest::header::CONTENT_TYPE, "image/jpeg")
                    .send()
                    .await
                    .ok()?;
                if !r.status().is_success() {
                    return None;
                }
                r.json::<Resp>().await.ok().map(|r| r.embedding)
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
                Some(v)
            }
            Embedder::Disabled => None,
        }
    }

    /// Text → **image space** embedding.
    ///
    /// Only the AI service can produce this: it needs the same CLIP stack that
    /// encoded the stored screenshots. The service answers `501` when its image
    /// backend is the deterministic hash fallback, because hashed text and
    /// hashed pixels live in different spaces — searching one with the other
    /// returns noise, not "no matches". `None` therefore means *this
    /// deployment cannot answer text queries against the image store* and
    /// callers must report the leg as skipped.
    pub async fn embed_image_text(&self, text: &str) -> Option<Vec<f32>> {
        let Embedder::Http { base, client } = self else {
            return None;
        };
        #[derive(serde::Serialize)]
        struct Req<'a> {
            text: &'a str,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            embedding: Vec<f32>,
        }
        let r = client
            .post(format!("{base}/embed/image-text"))
            .json(&Req { text })
            .send()
            .await
            .ok()?;
        if !r.status().is_success() {
            return None;
        }
        r.json::<Resp>().await.ok().map(|x| x.embedding)
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
        let a = e.embed_text("kerberos tickets").await.unwrap();
        let b = e.embed_text("kerberos tickets").await.unwrap();
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
        let a = e.embed_image(b"\xff\xd8scene-a\xff\xd9").await.unwrap();
        let b = e.embed_image(b"\xff\xd8scene-b\xff\xd9").await.unwrap();
        assert_eq!(a.len(), EMBED_DIM);
        let n: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n - 1.0).abs() < 1e-3, "not normalised: {n}");
        assert_ne!(a, b);
    }
}
