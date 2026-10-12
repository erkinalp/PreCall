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
use precall_api::{build_router, ApiState, EmbedOutcome, Embedder};
use precall_proto::metadata::{AppRecord, ScreenRegionRecord, WindowCaptureRecord};
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

fn capture(
    name: &str,
    token: &str,
    ts: u64,
    ocr: &str,
    app: Option<(&str, &str)>,
) -> WindowCaptureRecord {
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
        apps: app
            .map(|(name, path)| {
                vec![AppRecord {
                    windows_app_id: None,
                    icon_uri: None,
                    name: name.to_string(),
                    path: Some(path.to_string()),
                    properties: None,
                }]
            })
            .unwrap_or_default(),
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
        .insert_capture(&capture(
            "scene-a.jpg",
            "tok-a",
            TS_A,
            "kerberos tickets",
            Some(("msedge.exe", r"C:\msedge.exe")),
        ))
        .unwrap();
    let id_b = ukg
        .insert_capture(&capture(
            "scene-b.jpg",
            "tok-b",
            TS_B,
            "quarterly invoice",
            Some(("notepad.exe", r"C:\notepad.exe")),
        ))
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

/// The image store, stamped with the space of the embedder the test will query
/// with — a store built by one model cannot be ranked against another's query.
fn image_index_in_space(fx: &Fixture, space: &str) -> SemanticIndex {
    SemanticIndex::open(
        &fx.data_root.join(&fx.client).join("SemanticImageStore.db"),
        384,
        space,
    )
    .unwrap()
}

fn image_index(fx: &Fixture) -> SemanticIndex {
    image_index_in_space(fx, precall_api::LOCAL_IMAGE_SPACE)
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

/// Space id the CLIP-shaped stubs advertise (and the tests stamp their stores
/// with) — deliberately not the local hash space, so a test that forgot to
/// align them fails loudly instead of passing by accident.
const STUB_CLIP_SPACE: &str = "clip:stub-vit@384";

#[derive(Clone)]
struct StubAi {
    image_text: Arc<Vec<f32>>,
}

async fn stub_image_text(State(s): State<StubAi>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "embedding": *s.image_text,
        "dim": s.image_text.len(),
        "space": STUB_CLIP_SPACE,
    }))
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
    let idx = image_index_in_space(&fx, STUB_CLIP_SPACE);
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

// ---------------------------------------------------------------------------
// Regressions from the Codex review of the first revision (all four findings
// were real: backfill, retry, filter enforcement, score-scale fusion).
// ---------------------------------------------------------------------------

/// A stub AI service that answers every path with one status code.
async fn serve_status(code: axum::http::StatusCode) -> SocketAddr {
    #[derive(Clone)]
    struct S(axum::http::StatusCode);
    async fn handler(State(s): State<S>) -> (axum::http::StatusCode, &'static str) {
        (s.0, "stub")
    }
    let app = axum::Router::new().fallback(handler).with_state(S(code));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

fn http_embedder(addr: SocketAddr) -> Embedder {
    Embedder::Http {
        base: format!("http://{addr}"),
        client: reqwest::Client::new(),
    }
}

/// Move every recorded attempt a couple of minutes into the past, i.e. "the
/// service came back after the retry backoff elapsed".
fn expire_retry_backoff(fx: &Fixture) {
    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    ukg.conn()
        .execute(
            "UPDATE \"PrecallImageEmbedding\" SET \"UpdatedAt\" = \"UpdatedAt\" - ?",
            [120 * 10_000_000i64],
        )
        .unwrap();
}

fn processed_count(fx: &Fixture) -> i64 {
    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    ukg.conn()
        .query_row(
            "SELECT COUNT(*) FROM \"WindowCapture\" WHERE \"IsProcessed\" = 0",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// Finding 1: an upgraded installation has every capture already processed by
/// the text-only build, so gating the image leg on `IsProcessed = 0` would
/// leave its whole history unsearchable.
#[tokio::test]
async fn upgraded_installations_get_their_history_backfilled() {
    let fx = seed();
    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    ukg.conn()
        .execute("UPDATE \"WindowCapture\" SET \"IsProcessed\" = 1", [])
        .unwrap();
    drop(ukg);
    assert_eq!(image_index(&fx).count().unwrap(), 0, "nothing embedded yet");

    let stats =
        precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
            .await
            .unwrap();
    assert_eq!(stats.image_embeddings, 2, "{stats:?}");
    assert_eq!(stats.backfilled, 2, "these came from the backfill queue: {stats:?}");
    assert_eq!(stats.text_embeddings, 0, "text was already done: {stats:?}");
    assert_eq!(image_index(&fx).count().unwrap(), 2);
}

/// Finding 2a: a transient failure (5xx / no answer) must not consume the
/// capture — it has to come back when the service does.
#[tokio::test]
async fn transient_image_embedding_failures_are_retried() {
    let fx = seed();
    let down = http_embedder(serve_status(axum::http::StatusCode::INTERNAL_SERVER_ERROR).await);

    let first = precall_api::enrich::enrich_once(&fx.data_root, &down, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(first.image_embeddings, 0);
    assert_eq!(first.images_pending, 2, "{first:?}");
    assert_eq!(image_index(&fx).count().unwrap(), 0);
    // Both legs failed transiently, so neither capture may leave the queue.
    assert_eq!(processed_count(&fx), 2, "a failed capture must stay pending");
    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    assert_eq!(
        ukg.image_embedding_state(fx.id_a).unwrap().unwrap().0,
        "pending"
    );
    drop(ukg);

    // A pass moments later must not hammer the service again: the retry is
    // inside its backoff window.
    let immediate = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(immediate.image_embeddings, 0, "backoff must pace retries: {immediate:?}");
    assert_eq!(immediate.images_pending, 0, "{immediate:?}");

    // Service recovers, the backoff elapses: the same captures embed, once.
    expire_retry_backoff(&fx);
    let second = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(second.image_embeddings, 2, "{second:?}");
    assert_eq!(image_index(&fx).count().unwrap(), 2);
    assert_eq!(processed_count(&fx), 0);
}

/// Finding 2b: an input the embedder *refuses* is terminal — retrying identical
/// bytes forever would be a busy loop.
#[tokio::test]
async fn refused_images_are_not_retried_forever() {
    let fx = seed();
    let refusing = http_embedder(serve_status(axum::http::StatusCode::BAD_REQUEST).await);
    let first = precall_api::enrich::enrich_once(&fx.data_root, &refusing, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(first.images_skipped, 2, "{first:?}");
    assert_eq!(first.images_pending, 0, "{first:?}");
    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    assert_eq!(
        ukg.image_embedding_state(fx.id_a).unwrap().unwrap().0,
        "skipped"
    );
    drop(ukg);

    // A healthy service must not resurrect a refused input.
    let second = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(second.image_embeddings, 0, "{second:?}");
    assert_eq!(image_index(&fx).count().unwrap(), 0);
}

/// Finding 3: in `mode=image` there is no FTS leg, so if the semantic merge
/// ignores the filters the API happily returns captures outside the requested
/// window or application.
#[tokio::test]
async fn image_search_enforces_time_range_and_app_filter() {
    let fx = seed();
    precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    let addr = serve(&fx, Embedder::Local).await;
    let http = reqwest::Client::new();
    let url = format!("http://{addr}/api/v1/search");

    // Unfiltered baseline finds A.
    let r = http
        .post(&url)
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["results"][0]["window_capture_id"], serde_json::json!(fx.id_a));

    // A window that starts after A's timestamp excludes A — and, crucially,
    // must not put it back with a zero score.
    let r = http
        .post(&url)
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
            "time_range": { "start": TS_A as i64 + 1 },
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(
        !body["results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["window_capture_id"] == serde_json::json!(fx.id_a)),
        "time_range must filter semantic candidates: {body}"
    );
    assert!(body["legs"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("image-semantic")));

    // A window that starts after *both* captures leaves nothing at all.
    let r = http
        .post(&url)
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
            "time_range": { "start": TS_B as i64 + 1 },
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(
        body["results"].as_array().unwrap().is_empty(),
        "no capture is inside that window: {body}"
    );

    // An app_filter naming the other capture's app excludes A (the filter used
    // to be applied to the FTS leg only; in mode=image there is no FTS leg).
    let r = http
        .post(&url)
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
            "app_filter": ["notepad.exe"],
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(
        !body["results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["window_capture_id"] == serde_json::json!(fx.id_a)),
        "app_filter must filter semantic candidates: {body}"
    );

    // Matching by path (the other half of the FTS predicate) keeps A.
    let r = http
        .post(&url)
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
            "app_filter": [r"C:\msedge.exe"],
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["results"][0]["window_capture_id"], serde_json::json!(fx.id_a));
}

/// Finding 4 (the other half): the flat cosine scan returns *every* stored
/// item, so a candidate with no positive similarity is not evidence and must
/// not be ranked against real matches.
#[tokio::test]
async fn image_search_drops_non_positive_similarity() {
    let fx = seed();
    let EmbedOutcome::Ok { embedding: emb, .. } = Embedder::Local.embed_image(JPEG_A).await else {
        panic!("local embedder must succeed");
    };
    let inverted: Vec<f32> = emb.iter().map(|x| -x).collect();
    let idx = image_index(&fx);
    idx.insert(
        &SemanticIndex::item_id_for_capture(fx.id_a),
        None,
        None,
        &inverted,
    )
    .unwrap();
    drop(idx);

    let addr = serve(&fx, Embedder::Local).await;
    let r = reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(
        body["results"].as_array().unwrap().is_empty(),
        "cosine = -1 is anti-evidence, not a weak match: {body}"
    );
    assert!(body["legs"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("image-semantic")));
}

/// An AI outage during image search is a 503 (the same request may work later),
/// while a refused image is the caller's 400.
#[tokio::test]
async fn image_search_distinguishes_outage_from_bad_input() {
    let fx = seed();
    let url_for = |addr: SocketAddr| format!("http://{addr}/api/v1/search");

    let down = serve(&fx, http_embedder(serve_status(axum::http::StatusCode::INTERNAL_SERVER_ERROR).await)).await;
    let r = reqwest::Client::new()
        .post(url_for(down))
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503, "an outage is retryable, not a bad request");

    let refusing = serve(&fx, http_embedder(serve_status(axum::http::StatusCode::BAD_REQUEST).await)).await;
    let r = reqwest::Client::new()
        .post(url_for(refusing))
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

/// Codex finding 4, sharpened: reciprocal rank fusion cannot separate two
/// rank-1 hits from different legs, and the vector leg ranks *every* stored
/// item — so an exact lexical match must win that tie rather than lose a
/// coin-flip to recency.
#[tokio::test]
async fn an_exact_lexical_match_wins_a_tie_against_a_vector_hit() {
    let fx = seed(); // A holds the text "kerberos tickets"; B is newer.
    let vector: Vec<f32> = (0..384).map(|i| (i as f32 * 0.017).cos()).collect();
    let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    let vector: Vec<f32> = vector.into_iter().map(|x| x / norm).collect();
    let idx = image_index_in_space(&fx, STUB_CLIP_SPACE);
    idx.insert(
        &SemanticIndex::item_id_for_capture(fx.id_b),
        None,
        None,
        &vector,
    )
    .unwrap();
    drop(idx);

    // The AI service can put the *text* query into the image space, but its
    // text embedder is not answering — so A can only come from the FTS leg and
    // B only from the image leg: a perfect rank-1 tie.
    let app = axum::Router::new()
        .route("/embed/image-text", post(stub_image_text))
        .with_state(StubAi {
            image_text: Arc::new(vector.clone()),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ai_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });

    let addr = serve(&fx, http_embedder(ai_addr)).await;
    let r = reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client, "query": "kerberos", "mode": "hybrid",
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    let body: serde_json::Value = r.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    let ids: Vec<i64> = results
        .iter()
        .map(|r| r["window_capture_id"].as_i64().unwrap())
        .collect();
    assert!(ids.contains(&fx.id_a) && ids.contains(&fx.id_b), "{body}");
    assert_eq!(
        ids[0], fx.id_a,
        "the exact lexical match must win the tie, not the newer vector hit: {body}"
    );
}

/// An AI-shaped stub that reports its spaces, so the enrichment pass can
/// invalidate stores built in a different one.
#[derive(Clone)]
struct SpaceStub {
    space: Arc<String>,
    image: Arc<Vec<f32>>,
    text: Arc<Vec<f32>>,
}

async fn stub_health(State(s): State<SpaceStub>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ok",
        "text_space_id": *s.space,
        "image_space_id": *s.space,
        "dim": 384,
    }))
}

async fn stub_embed_image(State(s): State<SpaceStub>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "embedding": *s.image, "space": *s.space }))
}

async fn stub_embed_text(State(s): State<SpaceStub>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "embedding": *s.text, "space": *s.space }))
}

async fn serve_ai(space: &str) -> SocketAddr {
    let vector: Vec<f32> = {
        let mut v = vec![0.0f32; 384];
        v[0] = 1.0;
        v
    };
    let app = axum::Router::new()
        .route("/health", axum::routing::get(stub_health))
        .route("/embed/image", post(stub_embed_image))
        .route("/embed/text", post(stub_embed_text))
        .with_state(SpaceStub {
            space: Arc::new(space.to_string()),
            image: Arc::new(vector.clone()),
            text: Arc::new(vector),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

/// Finding (pool truncation): filtering the top `2 * limit` candidates *after*
/// truncation drops eligible matches that rank below the cut.
#[tokio::test]
async fn filters_apply_before_the_candidate_pool_is_truncated() {
    let fx = seed();
    // Three stored vectors, ranked against the query (1,0,0…): B(0.98) > C(0.9)
    // > A(0.2). With limit = 1 the pool is 2, so A is below the cut.
    let mut v_b = vec![0.0f32; 384];
    v_b[0] = 0.98;
    v_b[1] = 0.199;
    let mut v_c = vec![0.0f32; 384];
    v_c[0] = 0.9;
    v_c[2] = 0.436;
    let mut v_a = vec![0.0f32; 384];
    v_a[0] = 0.2;
    v_a[3] = 0.98;

    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    let id_c = ukg
        .insert_capture(&capture("scene-c.jpg", "tok-c", TS_B + 1, "third", None))
        .unwrap();
    // Stamped with the space the stub reports, so only the pool truncation is
    // under test here.
    let idx = image_index_in_space(&fx, "clip:stub-vit@384");
    idx.insert(&SemanticIndex::item_id_for_capture(fx.id_b), None, None, &v_b).unwrap();
    idx.insert(&SemanticIndex::item_id_for_capture(id_c), None, None, &v_c).unwrap();
    idx.insert(&SemanticIndex::item_id_for_capture(fx.id_a), None, None, &v_a).unwrap();
    drop(idx);

    // Allow only A (via its app), with limit = 1: the pool of 2 holds B and C,
    // so only a scan-time filter can still see A.
    let addr = serve(&fx, http_embedder(serve_ai("clip:stub-vit@384").await)).await;
    let r = reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
            "limit": 1, "app_filter": ["msedge.exe"],
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body["results"].as_array().unwrap().len(),
        1,
        "the eligible match ranks below the top-2 cut and must still be found: {body}"
    );
    assert_eq!(body["results"][0]["window_capture_id"], serde_json::json!(fx.id_a));
}

/// Finding (space invalidation): a store filled by the hash embedder must not
/// keep answering once the deployment switches to CLIP.
#[tokio::test]
async fn switching_the_vector_backend_rebuilds_the_store() {
    let fx = seed();
    // Pass 1 in space "clip:stub-a@384" — note /embed/image returns the same
    // vector for everything, so which space stamped the rows is what matters.
    let ai_a = serve_ai("clip:stub-a@384").await;
    let stats = precall_api::enrich::enrich_once(&fx.data_root, &http_embedder(ai_a), 384, fx.key)
        .await
        .unwrap();
    assert_eq!(stats.image_embeddings, 2, "{stats:?}");
    assert_eq!(
        image_index(&fx).space_id().unwrap().as_deref(),
        Some("clip:stub-a@384")
    );

    // A row from a hypothetical earlier backend survives until the switch.
    {
        let idx = image_index(&fx);
        idx.insert(&SemanticIndex::item_id_for_capture(999), None, None, &vec![0.0f32; 384])
            .unwrap();
        assert_eq!(idx.count().unwrap(), 3);
    }

    // Pass 2 reports a different backend: the store must be rebuilt, not kept.
    let ai_b = serve_ai("clip:stub-b@384").await;
    let stats = precall_api::enrich::enrich_once(&fx.data_root, &http_embedder(ai_b), 384, fx.key)
        .await
        .unwrap();
    assert_eq!(stats.image_embeddings, 2, "the backlog must be re-embedded: {stats:?}");
    let idx = image_index(&fx);
    assert_eq!(idx.count().unwrap(), 2, "stale rows from the old space are gone");
    assert_eq!(
        idx.space_id().unwrap().as_deref(),
        Some("clip:stub-b@384")
    );
}

/// The same switch seen from a query: until the rebuild happens, the leg is
/// reported as skipped, never ranked against incomparable vectors.
#[tokio::test]
async fn queries_refuse_to_rank_against_a_different_space() {
    let fx = seed();
    let idx = image_index_in_space(&fx, "clip:stub-a@384");
    idx.insert(
        &SemanticIndex::item_id_for_capture(fx.id_a),
        None,
        None,
        &{
            let mut v = vec![0.0f32; 384];
            v[0] = 1.0;
            v
        },
    )
    .unwrap();
    drop(idx);

    // The API's embedder now reports a different space.
    let addr = serve(&fx, http_embedder(serve_ai("clip:stub-b@384").await)).await;
    let r = reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client, "mode": "image", "image": b64(JPEG_A),
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(body["results"].as_array().unwrap().is_empty(), "{body}");
    let notes = body["notes"].as_array().expect("a skipped leg must be named");
    assert!(
        notes.iter().any(|n| n.as_str().unwrap().contains("clip:stub-a@384")),
        "the note must name the store's space: {notes:?}"
    );
}

// ---------------------------------------------------------------------------
// Regressions from the Codex re-review of 310f1be.
// ---------------------------------------------------------------------------

/// A single unreadable object must be isolated, not abort the pass: aborting
/// re-selects the same capture forever and stalls every client after it.
#[tokio::test]
async fn an_unreadable_object_is_isolated_not_fatal() {
    let fx = seed();
    // Corrupt A's stored ciphertext in place (bad authentication tag).
    let rel = ObjectStore::key_path(&fx.client, ymd_from_filetime(TS_A as i64), "tok-a", "jpg");
    let path = fx
        .data_root
        .join(&fx.client)
        .join("objects")
        .join(&rel);
    let mut bytes = std::fs::read(&path).unwrap();
    let n = bytes.len();
    bytes[n - 1] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let stats = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .expect("the pass must survive one bad object");
    assert_eq!(stats.images_skipped, 1, "the corrupt object is terminal: {stats:?}");
    assert_eq!(stats.image_embeddings, 1, "B is still embedded: {stats:?}");
    assert_eq!(image_index(&fx).count().unwrap(), 1);

    // Terminal means exactly that: later passes do not keep retrying it.
    let again = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(again.image_embeddings, 0, "{again:?}");
    assert_eq!(again.images_pending, 0, "{again:?}");
}

/// `/health` can fail while `/embed/image` succeeds (service recovered, or was
/// rolled forward). Inserting that vector and relabelling the store would leave
/// a permanently mixed index.
#[tokio::test]
async fn a_successful_embed_in_a_new_space_rebuilds_instead_of_mixing() {
    let fx = seed();
    // Store stamped with an earlier space, holding one stray row.
    let idx = image_index_in_space(&fx, "clip:old@384");
    idx.insert(
        &SemanticIndex::item_id_for_capture(999),
        None,
        None,
        &vec![0.0f32; 384],
    )
    .unwrap();
    drop(idx);

    // /health is down, /embed/image answers in a new space.
    #[derive(Clone)]
    struct S;
    async fn health_down() -> (axum::http::StatusCode, &'static str) {
        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "health down")
    }
    async fn embed_image_new_space() -> axum::Json<serde_json::Value> {
        let mut v = vec![0.0f32; 384];
        v[0] = 1.0;
        axum::Json(serde_json::json!({ "embedding": v, "space": "clip:new@384" }))
    }
    let app = axum::Router::new()
        .route("/health", axum::routing::get(health_down))
        .route("/embed/image", post(embed_image_new_space))
        .with_state(S);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });

    let stats = precall_api::enrich::enrich_once(&fx.data_root, &http_embedder(addr), 384, fx.key)
        .await
        .unwrap();
    assert_eq!(stats.image_embeddings, 2, "{stats:?}");
    let idx = image_index(&fx);
    assert_eq!(idx.space_id().unwrap().as_deref(), Some("clip:new@384"));
    assert_eq!(
        idx.count().unwrap(),
        2,
        "the old space's rows must be gone, not ranked alongside new ones"
    );
}

/// 408/429/401/404 are deployment or transient states, not per-input refusals:
/// marking them `skipped` would exclude screenshots forever.
#[tokio::test]
async fn service_wide_client_errors_stay_retryable() {
    for code in [
        axum::http::StatusCode::TOO_MANY_REQUESTS,
        axum::http::StatusCode::REQUEST_TIMEOUT,
        axum::http::StatusCode::UNAUTHORIZED,
    ] {
        let fx = seed();
        let address = serve_status(code).await;
        let stats = precall_api::enrich::enrich_once(&fx.data_root, &http_embedder(address), 384, fx.key)
            .await
            .unwrap();
        assert_eq!(stats.images_pending, 2, "{code} must be retryable: {stats:?}");
        assert_eq!(stats.images_skipped, 0, "{code} must not be terminal: {stats:?}");
        let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
        assert_eq!(
            ukg.image_embedding_state(fx.id_a).unwrap().unwrap().0,
            "pending",
            "{code}"
        );
    }
}

/// A capture that cannot complete stays queued but is paced, so 64 of them
/// cannot exclude every later capture from enrichment.
#[tokio::test]
async fn fresh_captures_are_paced_by_the_same_backoff() {
    let fx = seed();
    // No object on disk for either capture → the image leg stays retryable and
    // the captures stay unprocessed.
    std::fs::remove_dir_all(fx.data_root.join(&fx.client).join("objects")).unwrap();
    let first = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(first.images_pending, 2, "{first:?}");
    assert_eq!(processed_count(&fx), 2, "they stay queued");

    // Immediately afterwards the queue is quiet (backoff), rather than
    // re-attempting the same rows every poll.
    let second = precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();
    assert_eq!(second.images_pending, 0, "paced: {second:?}");
    assert_eq!(second.text_embeddings, 0, "and the text leg is not re-run either: {second:?}");
}

/// The index commit and the `PrecallImageEmbedding` write are separate
/// databases; a crash between them must not leave a capture selected as
/// state-less work forever.
#[tokio::test]
async fn an_existing_embedding_gets_its_state_recorded() {
    let fx = seed();
    let idx = image_index(&fx);
    idx.insert(
        &SemanticIndex::item_id_for_capture(fx.id_a),
        None,
        None,
        &vec![0.0f32; 384],
    )
    .unwrap();
    drop(idx);
    // A's text leg is already done, so it is backfill-queue work with no state
    // row — exactly the crash window.
    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    ukg.mark_processed(fx.id_a).unwrap();
    assert!(ukg.image_embedding_state(fx.id_a).unwrap().is_none());
    drop(ukg);

    precall_api::enrich::enrich_once(&fx.data_root, &Embedder::Local, 384, fx.key)
        .await
        .unwrap();

    let ukg = Ukg::open(&fx.data_root.join(&fx.client).join("ukg.db")).unwrap();
    let (state, _, err) = ukg
        .image_embedding_state(fx.id_a)
        .unwrap()
        .expect("the guard must record the state it verified");
    assert_eq!(state, "done");
    assert!(err.is_none());
}
