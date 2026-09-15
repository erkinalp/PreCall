// SPDX-License-Identifier: GPL-2.0-only
//! HTTP surface test: seed a client store → serve the router in-proc →
//! exercise timeline/search/snapshot/regions/apps/clients + auth rejection.

use precall_api::{build_router, ApiState, Embedder};
use precall_proto::metadata::{ScreenRegionRecord, WindowCaptureRecord};
use precall_store::objects::ymd_from_filetime;
use precall_store::{ObjectStore, Ukg};
use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

const FAKE_JPEG: &[u8] = b"\xff\xd8\xff\xe0JFIF-test\x00\xff\xd9";

struct Fixture {
    _dir: TempDir,
    client: String,
    addr: SocketAddr,
    object_key: [u8; 32],
}

async fn serve_fixture() -> Fixture {
    let dir = TempDir::new().unwrap();
    let data_root = dir.path().to_path_buf();
    let client_id = Uuid::new_v4();
    let client_dir = data_root.join(client_id.to_string());
    std::fs::create_dir_all(&client_dir).unwrap();

    // Register the client in server.db.
    let server_db = Ukg::open(&data_root.join("server.db")).unwrap();
    server_db
        .touch_client(&client_id.to_string(), "testhost", 133_000_000_000_000_000)
        .unwrap();

    // Seed ukg.db + a screenshot object.
    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let token = "e2e-token-1".to_string();
    let ts = 133_000_000_000_000_000u64; // 2022-06-01-ish
    let rec = WindowCaptureRecord {
        name: "msedge.exe (1234)".into(),
        image_token: token.clone(),
        is_foreground: true,
        window_id: 42,
        window_bounds: "0,0,1920,1080".into(),
        window_title: "Kerberos tickets - Microsoft Learn".into(),
        properties: None,
        timestamp_100ns: ts,
        activation_uri: Some("shell:AppsFolder\\Microsoft.MicrosoftEdge".into()),
        activity_id: None,
        fallback_uri: Some("file:///C:/msedge.exe".into()),
        apps: vec![],
        files: vec![],
        webs: vec![],
        regions: vec![ScreenRegionRecord {
            region_kind: "text".into(),
            ocr_text: Some("Kerberos tickets renewals policy".into()),
            bounds: "10,10,400,40".into(),
        }],
    };
    let wc_id = ukg.insert_capture(&rec).unwrap();
    assert!(wc_id > 0);

    let key = precall_store::objects::load_or_create_key(&data_root).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    let date = ymd_from_filetime(ts as i64);
    objects
        .put(&client_id.to_string(), date, &token, "jpg", FAKE_JPEG)
        .unwrap();

    let state = ApiState {
        data_root: data_root.clone(),
        embedder: Arc::new(Embedder::Local),
        object_key: key,
        auth: precall_api::auth::ApiAuth::new(&[]),
        embedding_dim: 384,
    };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });

    Fixture {
        _dir: dir,
        client: client_id.to_string(),
        addr,
        object_key: key,
    }
}

#[tokio::test]
async fn api_surface_smoke() {
    let fx = serve_fixture().await;
    let http = reqwest::Client::new();
    let base = format!("http://{}", fx.addr);

    // health
    let r = http.get(format!("{base}/health")).send().await.unwrap();
    assert!(r.status().is_success());

    // timeline
    let r = http
        .get(format!("{base}/api/v1/timeline?client={}", fx.client))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "timeline: {}", r.status());
    let body: serde_json::Value = r.json().await.unwrap();
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["window_title"], "Kerberos tickets - Microsoft Learn");

    // search (FTS5 hits the OCR text)
    let r = http
        .post(format!("{base}/api/v1/search"))
        .json(&serde_json::json!({
            "client": fx.client,
            "query": "kerberos",
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "search: {}", r.status());
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["results"].as_array().unwrap().len(), 1);

    // snapshot (decrypted object read-through)
    let r = http
        .get(format!(
            "{base}/api/v1/snapshot/1?client={}",
            fx.client
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "snapshot: {}", r.status());
    let bytes = r.bytes().await.unwrap();
    assert_eq!(&bytes[..2], &[0xff, 0xd8]);

    // regions
    let r = http
        .get(format!("{base}/api/v1/regions/1?client={}", fx.client))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body["regions"][0]["text"],
        serde_json::json!("Kerberos tickets renewals policy")
    );

    // relaunch returns the stored URI
    let r = http
        .post(format!("{base}/api/v1/relaunch"))
        .json(&serde_json::json!({
            "client": fx.client,
            "window_capture_id": 1,
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body["activation_uri"],
        serde_json::json!("shell:AppsFolder\\Microsoft.MicrosoftEdge")
    );

    // clients list
    let r = http
        .get(format!("{base}/api/v1/clients"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["clients"].as_array().unwrap().len(), 1);
    assert_eq!(body["clients"][0]["hostname"], "testhost");

    // analytics
    let r = http
        .get(format!("{base}/api/v1/apps?client={}", fx.client))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    let r = http
        .get(format!("{base}/api/v1/web?client={}", fx.client))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());

    // export
    let r = http
        .get(format!("{base}/api/v1/export?client={}", fx.client))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["captures"].as_array().unwrap().len(), 1);

    // unknown client → 404
    let r = http
        .get(format!(
            "{base}/api/v1/timeline?client={}",
            Uuid::new_v4()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    let _ = fx.object_key; // fixture keeps TempDir alive
}

#[tokio::test]
async fn api_requires_bearer_when_tokens_configured() {
    let dir = TempDir::new().unwrap();
    let state = ApiState {
        data_root: dir.path().to_path_buf(),
        embedder: Arc::new(Embedder::Local),
        object_key: [7u8; 32],
        auth: precall_api::auth::ApiAuth::new(&["sekrit".to_string()]),
        embedding_dim: 384,
    };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });

    let http = reqwest::Client::new();
    let base = format!("http://{addr}");

    // health stays open
    assert!(http
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    // api without token → 401
    let r = http
        .get(format!("{base}/api/v1/clients"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // wrong token → 401
    let r = http
        .get(format!("{base}/api/v1/clients"))
        .bearer_auth("wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // right token → passes auth (404 because no clients, not 401)
    let r = http
        .get(format!("{base}/api/v1/clients"))
        .bearer_auth("sekrit")
        .send()
        .await
        .unwrap();
    assert_ne!(r.status(), 401);
}
