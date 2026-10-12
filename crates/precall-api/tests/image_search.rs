// SPDX-License-Identifier: GPL-2.0-only
//! Image-space tests: the `SemanticImageStore.db` leg of the pipeline.
//!
//! Before this test existed the image space was provisioned (`si_*` schema,
//! a `ClientStore::image_index` handle, `Embedder::embed_image`) but nothing
//! ever wrote to it or queried it, so "find the screenshot that looks like
//! this" silently returned nothing. These tests pin the two halves:
//!
//! 1. enrichment embeds each capture's stored JPEG into the image space,
//!    idempotently (a restart must not double-insert);
//! 2. `/search` reaches that space — by image, and (only when a CLIP-backed
//!    AI service is attached) by text query.
//!
//! The embedding vectors here are deterministic byte/word hashes, not real
//! models: they prove the plumbing, exactly as `providers.py` documents its
//! hash fallback.

use axum::extract::State;
use axum::routing::post;
use base64::Engine as _;
use precall_api::{build_router, ApiState, Embedder};
use precall_proto::metadata::{ScreenRegionRecord, WindowCaptureRecord};
use precall_store::objects::ymd_from_filetime;
use precall_store::{ObjectStore, SemanticIndex, Ukg};
use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

/// Two distinguishable "JPEG" payloads. The embedders exercised here hash the
/// bytes (Local) or are stubbed, so a real JPEG is not required; the real
/// decode path is covered by `ai/tests/test_server.py`.
const JPEG_A: &[u8] = b"\xff\xd8\xff\xe0JFIF-scene-a\x00\xff\xd9";
const JPEG_B: &[u8] = b"\xff\xd8\xff\xe0JFIF-scene-b\x00\xff\xd9";

const TS_A: u64 = 133_000_000_000_000_000;
const TS_B: u64 = 133_000_000_600_000_000;

struct Fixture {
    _dir: TempDir,
    data_root: std::path::PathBuf,
    client: String,
    key: [u8; 32],
    /// `WindowCapture.Id` of the capture holding `JPEG_A` / `JPEG_B`.
    id_a: i64,
    id_b: i64,
}

fn capture(name: &str, token: &str, ts: u64, ocr: &str) -> WindowCaptureRecord {
    WindowCaptureRecord {
        name: name.into(),
        image_token: token.to_string(),
        is_foreground: true,
        window_id: 1,
        window_bounds: "0,0,1920,1080".into(),
        window_title: name.into(),
        properties: None,
        timestamp_100ns: ts,
        activation_uri: None,
        activity_id: None,
        fallback_uri: None,
        apps: vec![],
        files: vec![],
        webs: vec![],
        regions: vec![ScreenRegionRecord {
            region_kind: "text".into(),
            ocr_text: Some(ocr.to_string()),
            bounds: "0,0,10,10".into(),
        }],
    }
}

/// A client store with two captures and two distinct screenshots on disk.
fn seed() -> Fixture {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();
    let client_id = Uuid::new_v4();
    let client_dir = data_root.join(client_id.to_string());
    std::fs::create_dir_all(&client_dir).unwrap();

    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let id_a = ukg
        .insert_capture(&capture("scene-a.jpg", "tok-a", TS_A, "kerberos tickets"))
        .unwrap();
    let id_b = ukg
        .insert_capture(&capture("scene-b.jpg", "tok-b", TS_B, "quarterly invoice"))
        .unwrap();

    let key = precall_store::objects::load_or_create_key(&data_root).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    let client = client_id.to_string();
    objects
        .put(&client, ymd_from_filetime(TS_A as i64), "tok-a", "jpg", JPEG_A)
        .unwrap();
    objects
        .put(&client, ymd_from_filetime(TS_B as i64), "tok-b", "jpg", JPEG_B)
        .unwrap();

    Fixture { _dir: dir, data_root, client, key, id_a, id_b }
}

fn image_index(fx: &Fixture) -> SemanticIndex {
    SemanticIndex::open(
        &fx.data_root.join(&fx.client).join("SemanticImageStore.db"),
        384,
        "image",
    )
    .unwrap()
}

async fn serve(fx: &Fixture, embedder: Embedder) -> SocketAddr {
    let state = ApiState {
        data_root: fx.data_root.clone(),
        embedder: Arc::new(embedder),
        object_key: fx.key,
        auth: precall_api::auth::ApiAuth::new(&[]),
        embedding_dim: 384,
    };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[tokio::test]
async fn enrichment_populates_the_image_space_idempotently() {
    let fx = seed();
    let stats = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(stats.image_embeddings, 2, "both screenshots embedded: {stats:?}");
    assert_eq!(stats.text_embeddings, 2, "OCR text still embedded: {stats:?}");
    assert_eq!(stats.images_skipped, 0, "{stats:?}");

    let idx = image_index(&fx);
    assert_eq!(idx.count().unwrap(), 2);
    assert!(idx.has_item(&SemanticIndex::item_id_for_capture(fx.id_a)).unwrap());
    assert!(idx.has_item(&SemanticIndex::item_id_for_capture(fx.id_b)).unwrap());

    // Simulate the crash-restart window: captures unmarked (IsProcessed=0)
    // although their embeddings are already present.
    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    ukg.conn()
        .execute("UPDATE \"WindowCapture\" SET \"IsProcessed\" = 0", [])
        .unwrap();
    drop(ukg);

    let again = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(again.image_embeddings, 0, "guard must suppress duplicates");
    assert_eq!(again.text_embeddings, 0, "guard must suppress duplicates");
    assert_eq!(image_index(&fx).count().unwrap(), 2);
}

#[tokio::test]
async fn search_by_image_reaches_the_image_space() {
    let fx = seed();
    precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    let addr = serve(&fx, Embedder::Local).await;
    let http = reqwest::Client::new();
    let base = format!("http://{addr}");

    let r = http
        .post(format!("{base}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client,
            "mode": "image",
            "image": b64(JPEG_A),
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "search: {}", r.status());
    let body: serde_json::Value = r.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty(), "image search returned nothing: {body}");
    // The exact bytes of A rank first — its stored embedding is identical.
    assert_eq!(results[0]["window_capture_id"], serde_json::json!(fx.id_a));
    assert!(
        body["legs"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("image-semantic")),
        "legs: {}",
        body["legs"]
    );

    // A data URL is accepted too (what a browser FileReader hands over).
    let r = http
        .post(format!("{base}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client,
            "mode": "image",
            "image": format!("data:image/jpeg;base64,{}", b64(JPEG_B)),
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["results"][0]["window_capture_id"], serde_json::json!(fx.id_b));

    // mode=image without an image is a client error, not an empty result set.
    let r = http
        .post(format!("{base}/api/v1/search"))
        .json(&serde_json::json!({"client": fx.client, "mode": "image"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn text_to_image_leg_is_reported_skipped_without_clip() {
    let fx = seed();
    precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    let addr = serve(&fx, Embedder::Local).await;
    let http = reqwest::Client::new();

    let r = http
        .post(format!("http://{addr}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client,
            "query": "kerberos",
            "mode": "hybrid",
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    let body: serde_json::Value = r.json().await.unwrap();
    let legs: Vec<String> = body["legs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    // No CLIP stack in-process → the cross-modal leg must be absent AND named
    // in `notes`; treating it as "no matches" would hide a missing capability.
    assert!(!legs.contains(&"image-text".to_string()), "legs: {legs:?}");
    let notes = body["notes"].as_array().expect("notes must name the skipped leg");
    assert!(
        notes.iter().any(|n| n.as_str().unwrap().contains("image-text")),
        "notes: {notes:?}"
    );
}

#[derive(Clone)]
struct StubAi {
    image_text: Arc<Vec<f32>>,
}

async fn stub_image_text(State(s): State<StubAi>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "embedding": *s.image_text, "dim": s.image_text.len() }))
}

#[tokio::test]
async fn text_query_reaches_the_image_space_when_the_ai_backend_is_clip() {
    let fx = seed();

    // A CLIP-backed deployment would have produced this vector for both the
    // stored screenshot and the text query. Seed the space directly and stub
    // the text encoder to return the same vector.
    let vector: Vec<f32> = (0..384).map(|i| (i as f32 * 0.01).sin()).collect();
    let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    let vector: Vec<f32> = vector.into_iter().map(|x| x / norm).collect();
    let idx = image_index(&fx);
    for id in [fx.id_a, fx.id_b] {
        let mut other = vec![0.0f32; 384];
        other[0] = 1.0;
        let emb = if id == fx.id_a { vector.clone() } else { other };
        idx.insert(&SemanticIndex::item_id_for_capture(id), None, None, &emb)
            .unwrap();
    }
    drop(idx);
    // The processed flags gate nothing for the search path; mark them done so
    // the poller (if it were running) stays quiet.
    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    for id in [fx.id_a, fx.id_b] {
        ukg.mark_processed(id).unwrap();
    }
    drop(ukg);

    let app = axum::Router::new()
        .route("/embed/image-text", post(stub_image_text))
        .with_state(StubAi { image_text: Arc::new(vector.clone()) });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ai_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });

    let addr = serve(
        &fx,
        Embedder::Http {
            base: format!("http://{ai_addr}"),
            client: reqwest::Client::new(),
        },
    )
    .await;

    let r = reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client,
            "query": "a red bicycle screenshot",
            "mode": "semantic",
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "search: {}", r.status());
    let body: serde_json::Value = r.json().await.unwrap();
    let legs = body["legs"].as_array().unwrap();
    assert!(
        legs.contains(&serde_json::json!("image-text")),
        "cross-modal leg must run when the service provides it: {body}"
    );
    let results = body["results"].as_array().unwrap();
    assert!(
        results
            .iter()
            .any(|r| r["window_capture_id"] == serde_json::json!(fx.id_a)),
        "the capture whose image embedding matches the query must be returned: {body}"
    );
}

/// Guard against a regression where the object layout is rebuilt by hand in a
/// reader and drifts from what the broker wrote.
#[test]
fn snapshot_key_layout_is_shared() {
    assert_eq!(
        ObjectStore::key_path("c", (2026, 9, 16), "tok", "jpg"),
        std::path::PathBuf::from("c/2026/09/16/tok.jpg")
    );
}
