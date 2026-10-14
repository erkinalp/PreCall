// SPDX-License-Identifier: GPL-2.0-only
//! End-to-end: real TLS socket, real handshake, real ingestion, real sqlite.

use precall_broker::{tls, Broker, BrokerConfig};
use precall_mock::{pinned_client_config, sample_meta, synthesize_jpeg, Session, SyntheticFrame};
use precall_proto::{AuthMethod, Rect};
use precall_store::objects::load_or_create_key;
use precall_store::{ObjectStore, Ukg};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use uuid::Uuid;

const PSK: &str = "test-token-not-secret";

fn test_cert() -> (Vec<rustls::pki_types::CertificateDer<'static>>, rustls::pki_types::PrivateKeyDer<'static>, String) {
    let s = tls::generate_self_signed("localhost").unwrap();
    let certs: Vec<_> = rustls_pemfile::certs(&mut s.cert_pem.as_bytes())
        .collect::<Result<_, _>>()
        .unwrap();
    let key = tls::pkcs8_from_pem(&s.key_pem).unwrap();
    (certs, key, s.fingerprint)
}

fn config(data_root: &std::path::Path) -> BrokerConfig {
    BrokerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls_cert: data_root.join("cert.pem"),
        tls_key: data_root.join("key.pem"),
        data_root: data_root.to_path_buf(),
        psk_tokens: vec![PSK.to_string()],
        enrolled_certs: vec![],
        allow_ntlm: false,
        allow_any_client: true,
        require_sealed_payloads: false,
        heartbeat_interval_ms: 1000,
        max_clients: 8,
        encrypt_at_rest: true,
        embedding_dim: 384,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_to_end_capture_and_store() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let cfg = config(dir.path());
    let broker = Arc::new(Broker::new(cfg, tls_cfg).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "TESTBOX",
    )
    .await
    .expect("handshake");
    assert!(session.hello.accepted_channels.contains(&precall_proto::ChannelId::GfxRdp));

    for i in 0..3u64 {
        let token = format!("e2e-{i}");
        let frame = SyntheticFrame {
            jpeg: synthesize_jpeg(640, 360, i),
            meta: sample_meta(i, &token),
        };
        session.send_capture(&frame).await.unwrap();
    }
    session.heartbeat(3, 0).await.unwrap();
    // Give the broker a beat to commit.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    session.close().await.unwrap();
    handle.abort();

    // --- Verify the landing zone -------------------------------------------
    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    assert_eq!(timeline.len(), 3);

    let hits = ukg
        .search_fts(&precall_store::SearchOptions {
            query: precall_store::ukg::fts_escape("Kerberos"),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 3);

    // Image written under encrypted object store.
    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    let ts = timeline[0].timestamp_100ns;
    let (y, m, d) = precall_broker::filetime_to_ymd(ts);
    let rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"))
        .join(format!("{token}.jpg"));
    let bytes = objects.get(&client_id.to_string(), &rel, &token).unwrap();
    assert_eq!(&bytes[..2], &[0xFF, 0xD8]); // JPEG SOI
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_psk_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let tls = pinned_client_config(&fingerprint).unwrap();
    let result = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: "wrong-token".into() },
        Uuid::new_v4(),
        "BADBOX",
    )
    .await;
    handle.abort();
    assert!(result.is_err(), "bad PSK must not complete handshake");
}

/// A dirty-rect frame (several WIRE_TO_SURFACE regions in one START/END pair)
/// is composed onto the client's desktop surface and archived as an ordinary
/// full-frame JPEG — the reader-facing contract (`ukg.db` + one image per
/// capture) is unchanged, which is the whole reason the broker composites
/// rather than storing deltas.
#[tokio::test]
async fn dirty_rect_frames_are_composed_into_a_full_frame_archive() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "DIRTYBOX",
    )
    .await
    .expect("handshake");

    // Keyframe: a full white desktop at the size the mock advertises. The
    // surface is only adopted if the image matches the advertised dimensions.
    let base = SyntheticFrame {
        jpeg: solid_jpeg(1280, 720, [255, 255, 255]),
        meta: sample_meta(1, "base-1"),
    };
    session.send_capture(&base).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Then one dirty rectangle: a red patch at (16,16)-(48,40), plus a second
    // patch so the multi-region path is the one under test.
    let regions = vec![
        (Rect::new(16, 16, 48, 40), solid_jpeg(32, 24, [255, 0, 0])),
        (Rect::new(0, 48, 64, 64), solid_jpeg(64, 16, [0, 0, 255])),
    ];
    let dirty = SyntheticFrame {
        jpeg: vec![],
        meta: sample_meta(2, "dirty-2"),
    };
    session.send_dirty_regions(&regions, &dirty.meta).await.unwrap();
    let client_dir = dir.path().join(client_id.to_string());
    let landed = wait_for_captures(&client_dir.join("ukg.db"), 2).await;
    session.close().await.unwrap();
    handle.abort();
    assert_eq!(landed, 2, "both captures landed");

    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();

    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    assert_eq!(token, "dirty-2", "timeline is newest first");
    let ts = timeline[0].timestamp_100ns;
    let (y, m, d) = precall_broker::filetime_to_ymd(ts);
    let rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"))
        .join(format!("{token}.jpg"));
    assert!(objects.exists(&rel), "the dirty frame is archived as a .jpg");

    // And it is a real, decodable, *composed* image: the keyframe's white
    // desktop with both patches applied in their rectangles.
    let bytes = objects.get(&client_id.to_string(), &rel, &token).unwrap();
    let img = image::load_from_memory(&bytes).unwrap().to_rgb8();
    assert_eq!((img.width(), img.height()), (1280, 720));
    let px = |x: u32, y: u32| img.get_pixel(x, y).0;
    let near = |p: [u8; 3], want: [u8; 3]| p.iter().zip(want.iter()).all(|(a, b)| a.abs_diff(*b) <= 6);
    assert!(near(px(32, 28), [255, 0, 0]), "red patch composed: {:?}", px(32, 28));
    assert!(near(px(32, 55), [0, 0, 255]), "blue patch composed: {:?}", px(32, 55));
    assert!(
        near(px(60, 4), [255, 255, 255]),
        "untouched area keeps the keyframe: {:?}",
        px(60, 4)
    );
}

/// A dirty-rect frame that arrives before any keyframe cannot be composed; it
/// is stored as a region blob (never as a corrupt `.jpg`) and reported.
#[tokio::test]
async fn dirty_rects_without_a_keyframe_stay_an_honest_blob() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "BLOBBOX",
    )
    .await
    .expect("handshake");

    let regions = vec![
        (Rect::new(0, 0, 32, 32), solid_jpeg(32, 32, [1, 2, 3])),
        (Rect::new(32, 32, 64, 64), solid_jpeg(32, 32, [4, 5, 6])),
    ];
    let frame = SyntheticFrame {
        jpeg: vec![],
        meta: sample_meta(1, "orphan-1"),
    };
    session.send_dirty_regions(&regions, &frame.meta).await.unwrap();
    let client_dir = dir.path().join(client_id.to_string());
    let landed = wait_for_captures(&client_dir.join("ukg.db"), 1).await;
    session.close().await.unwrap();
    handle.abort();
    assert_eq!(landed, 1);

    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    let ts = timeline[0].timestamp_100ns;
    let (y, m, d) = precall_broker::filetime_to_ymd(ts);
    let base = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"));

    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    assert!(
        objects.exists(&base.join(format!("{token}.raw"))),
        "no surface and no keyframe: the region blob is stored, labelled honestly"
    );
    assert!(
        !objects.exists(&base.join(format!("{token}.jpg"))),
        "a concatenation must never be labelled .jpg"
    );
}

/// Wait for `count` captures to land in a client's database.
///
/// Polling beats a fixed sleep: composition now runs on the blocking pool, and a
/// fixed 300 ms was already marginal on a slower runner — the failure was a
/// flake in the test, not in the broker.
async fn wait_for_captures(db: &std::path::Path, count: usize) -> usize {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut landed = 0usize;
    while std::time::Instant::now() < deadline {
        landed = Ukg::open(db)
            .ok()
            .and_then(|u| u.timeline(None, 64).ok())
            .map(|t| t.len())
            .unwrap_or(0);
        if landed >= count {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    landed
}

/// Deterministic solid-colour JPEG, for asserting composed pixels.
fn solid_jpeg(w: u32, h: u32, rgb: [u8; 3]) -> Vec<u8> {
    let img = image::RgbImage::from_pixel(w, h, image::Rgb(rgb));
    let mut out = Vec::new();
    let enc = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 95);
    image::ImageEncoder::write_image(enc, img.as_raw(), w, h, image::ExtendedColorType::Rgb8)
        .unwrap();
    out
}

/// A small edit is often a *single* rectangle. Treating one JPEG region as a
/// keyframe archived the patch itself as the capture and replaced the desktop
/// surface with a patch-sized image, corrupting every later composition — so the
/// keyframe test is geometric, not a region count.
#[tokio::test]
async fn a_single_rectangle_delta_composes_onto_the_surface() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "ONERECT",
    )
    .await
    .expect("handshake");

    // Keyframe: the mock advertises 1280x720 and sends a full-surface rect.
    let base = SyntheticFrame {
        jpeg: solid_jpeg(1280, 720, [255, 255, 255]),
        meta: sample_meta(1, "base-1"),
    };
    session.send_capture(&base).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // One rectangle. It must compose, not become the capture.
    let regions = vec![(Rect::new(100, 100, 200, 150), solid_jpeg(100, 50, [255, 0, 0]))];
    let dirty = SyntheticFrame {
        jpeg: vec![],
        meta: sample_meta(2, "one-rect-2"),
    };
    session.send_dirty_regions(&regions, &dirty.meta).await.unwrap();
    let client_dir = dir.path().join(client_id.to_string());
    let landed = wait_for_captures(&client_dir.join("ukg.db"), 2).await;
    session.close().await.unwrap();
    handle.abort();
    assert_eq!(landed, 2, "the keyframe and the delta both landed");

    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    assert_eq!(token, "one-rect-2");
    let ts = timeline[0].timestamp_100ns;
    let (y, m, d) = precall_broker::filetime_to_ymd(ts);
    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    let rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"))
        .join(format!("{token}.jpg"));
    assert!(objects.exists(&rel), "the delta is archived as a composed .jpg");

    let bytes = objects.get(&client_id.to_string(), &rel, &token).unwrap();
    let img = image::load_from_memory(&bytes).unwrap().to_rgb8();
    assert_eq!(
        (img.width(), img.height()),
        (1280, 720),
        "the archive is the desktop, not the patch"
    );
    let near = |p: [u8; 3], want: [u8; 3]| p.iter().zip(want.iter()).all(|(a, b)| a.abs_diff(*b) <= 6);
    assert!(near(img.get_pixel(150, 125).0, [255, 0, 0]), "patch applied");
    assert!(
        near(img.get_pixel(1000, 600).0, [255, 255, 255]),
        "the keyframe's desktop survives"
    );
    assert!(
        near(img.get_pixel(50, 50).0, [255, 255, 255]),
        "and outside the patch too"
    );
}

/// Composing a dirty frame costs a full-desktop JPEG encode (~45 ms at 1080p).
/// If that ran on the session's own runtime worker, a handful of dirty-rectangle
/// clients would starve socket reads and heartbeats for *healthy* sessions too.
///
/// This pins the offload by **placement** rather than by timing: on a
/// single-worker runtime, image work must never be recorded on the worker thread
/// that runs the session. (A tick-gap threshold would depend on host load — a
/// busy runner can deschedule a worker for longer than an encode even when
/// composition never ran on it.)
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn composing_a_delta_does_not_block_the_runtime_worker() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "LIVENESS",
    )
    .await
    .expect("handshake");

    // A keyframe matching the advertised surface, so the compositor has a
    // surface to work on at all.
    let base = SyntheticFrame {
        jpeg: solid_jpeg(1280, 720, [255, 255, 255]),
        meta: sample_meta(1, "base-1"),
    };
    session.send_capture(&base).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // The runtime worker this test (and every task of it) runs on, with
    // `worker_threads = 1`. Composition must not appear here.
    let worker_thread = std::thread::current().id();
    precall_broker::COMPOSE_THREADS.lock().unwrap().clear();
    precall_broker::COMPOSE_RECORDING.store(true, std::sync::atomic::Ordering::Relaxed);

    // One delta: this is what triggers the 1080p compose + encode.
    let regions = vec![(Rect::new(100, 100, 300, 200), solid_jpeg(200, 100, [255, 0, 0]))];
    let dirty = SyntheticFrame {
        jpeg: vec![],
        meta: sample_meta(2, "delta-2"),
    };
    session.send_dirty_regions(&regions, &dirty.meta).await.unwrap();
    // Wait for the compose to land rather than guessing a duration: on one
    // worker everything (the 1080p encode, the test's own awaits) shares a
    // thread, so a fixed sleep would be the flakiest part of this test.
    let landed = wait_for_captures(&dir.path().join(client_id.to_string()).join("ukg.db"), 2).await;
    assert_eq!(landed, 2, "the delta reached the store");

    // Confirm the delta was actually composed (otherwise this proves nothing).
    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    assert_eq!(timeline.len(), 2, "the delta reached the store");
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    let ts = timeline[0].timestamp_100ns;
    let (y, m, d) = precall_broker::filetime_to_ymd(ts);
    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    let rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"))
        .join(format!("{token}.jpg"));
    let bytes = objects.get(&client_id.to_string(), &rel, &token).unwrap();
    let img = image::load_from_memory(&bytes).unwrap();
    assert_eq!((img.width(), img.height()), (1280, 720));
    handle.abort();

    // Placement, not timing: on one worker a synchronous compose would run on
    // `worker_thread`. Asserting on the recorded threads is robust to host load,
    // which is what a tick-gap threshold is not.
    precall_broker::COMPOSE_RECORDING.store(false, std::sync::atomic::Ordering::Relaxed);
    let compose_threads = precall_broker::COMPOSE_THREADS.lock().unwrap().clone();
    assert!(
        !compose_threads.is_empty(),
        "the delta was composed (otherwise this proves nothing)"
    );
    assert!(
        compose_threads.iter().all(|t| *t != worker_thread),
        "composition ran on the runtime worker: {compose_threads:?} contains {worker_thread:?}"
    );
}

/// A delta whose second region is undecodable must not leave the *first* region
/// applied to the surface: composition validates every region before touching
/// the canvas, and a failed frame is stored as a blob.
#[tokio::test]
async fn a_failed_delta_leaves_the_surface_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "ATOMIC",
    )
    .await
    .expect("handshake");

    let base = SyntheticFrame {
        jpeg: solid_jpeg(1280, 720, [255, 255, 255]),
        meta: sample_meta(1, "base-1"),
    };
    session.send_capture(&base).await.unwrap();
    let db = dir.path().join(client_id.to_string()).join("ukg.db");
    assert_eq!(wait_for_captures(&db, 1).await, 1);

    // Region 1 is fine (red), region 2 claims 100x100 but carries a 20x20 image.
    let bad = vec![
        (Rect::new(10, 10, 110, 110), solid_jpeg(100, 100, [255, 0, 0])),
        (Rect::new(200, 200, 300, 300), solid_jpeg(20, 20, [0, 255, 0])),
    ];
    let frame = SyntheticFrame { jpeg: vec![], meta: sample_meta(2, "bad-2") };
    session.send_dirty_regions(&bad, &frame.meta).await.unwrap();
    assert_eq!(wait_for_captures(&db, 2).await, 2);

    // A delta after the failure must NOT be composed: it is relative to the
    // frame that could not be applied, so the surface stays invalid.
    let after = vec![(Rect::new(800, 400, 900, 500), solid_jpeg(100, 100, [0, 0, 255]))];
    let frame = SyntheticFrame { jpeg: vec![], meta: sample_meta(3, "after-3") };
    session.send_dirty_regions(&after, &frame.meta).await.unwrap();
    assert_eq!(wait_for_captures(&db, 3).await, 3);

    // A decodable keyframe re-seeds the surface, and then deltas compose again.
    let again = SyntheticFrame {
        jpeg: solid_jpeg(1280, 720, [255, 255, 255]),
        meta: sample_meta(4, "again-4"),
    };
    session.send_capture(&again).await.unwrap();
    assert_eq!(wait_for_captures(&db, 4).await, 4);
    let good = vec![(Rect::new(800, 400, 900, 500), solid_jpeg(100, 100, [0, 0, 255]))];
    let frame = SyntheticFrame { jpeg: vec![], meta: sample_meta(5, "good-5") };
    session.send_dirty_regions(&good, &frame.meta).await.unwrap();
    assert_eq!(wait_for_captures(&db, 5).await, 5);
    session.close().await.unwrap();
    handle.abort();

    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&db).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    // Object keys are relative to the object root, so they carry exactly one
    // client-id segment (`{client}/{YYYY}/{MM}/{DD}/{token}.{ext}`).
    let obj_for = |token: &str, ts: i64, ext: &str| {
        let (y, m, d) = precall_broker::filetime_to_ymd(ts);
        std::path::PathBuf::from(client_id.to_string())
            .join(format!("{y:04}"))
            .join(format!("{m:02}"))
            .join(format!("{d:02}"))
            .join(format!("{token}.{ext}"))
    };

    // Look captures up by token: the timeline is newest-first and this test now
    // performs several captures, so indices are not stable.
    let entry_for = |token: &str| {
        timeline
            .iter()
            .find(|e| ukg.image_token(e.id).unwrap().as_deref() == Some(token))
            .unwrap_or_else(|| panic!("capture {token} is missing"))
            .clone()
    };

    // The failed frame is an honest blob, and the surface never saw its first
    // region.
    let bad = entry_for("bad-2");
    assert!(
        objects.exists(&obj_for(
            bad.image_token.as_deref().unwrap(),
            bad.timestamp_100ns,
            "raw"
        )),
        "a frame that cannot be composed whole is stored as a blob"
    );

    // The delta right after the failure is a blob even though it is valid.
    let after = entry_for("after-3");
    assert!(
        objects.exists(&obj_for(
            after.image_token.as_deref().unwrap(),
            after.timestamp_100ns,
            "raw"
        )),
        "a delta relative to a lost frame is stored as a blob"
    );

    let good_token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    let rel = obj_for(&good_token, timeline[0].timestamp_100ns, "jpg");
    let bytes = objects
        .get(&client_id.to_string(), &rel, &good_token)
        .unwrap();
    let img = image::load_from_memory(&bytes).unwrap().to_rgb8();
    let near = |p: [u8; 3], want: [u8; 3]| p.iter().zip(want.iter()).all(|(a, b)| a.abs_diff(*b) <= 6);
    assert!(near(img.get_pixel(850, 450).0, [0, 0, 255]), "the good delta composed");
    assert!(
        near(img.get_pixel(60, 60).0, [255, 255, 255]),
        "the failed frame's valid region must not be on the surface: {:?}",
        img.get_pixel(60, 60).0
    );
}

/// A rectangle with negative coordinates must be compared as signed: casting
/// `-3000` to `u32` wraps to a huge value that satisfies any "reaches the
/// display" test, which would archive a patch to the left of the desktop as a
/// full screenshot.
#[tokio::test]
async fn a_negative_rectangle_is_not_a_keyframe() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "NEGATIVE",
    )
    .await
    .expect("handshake");

    let base = SyntheticFrame {
        jpeg: solid_jpeg(1280, 720, [255, 255, 255]),
        meta: sample_meta(1, "base-1"),
    };
    session.send_capture(&base).await.unwrap();
    let db = dir.path().join(client_id.to_string()).join("ukg.db");
    assert_eq!(wait_for_captures(&db, 1).await, 1);

    // Entirely left of and above the origin.
    let off = vec![(Rect::new(-4000, -4000, -3000, -3000), solid_jpeg(1000, 1000, [255, 0, 0]))];
    let frame = SyntheticFrame { jpeg: vec![], meta: sample_meta(2, "off-2") };
    session.send_dirty_regions(&off, &frame.meta).await.unwrap();
    assert_eq!(wait_for_captures(&db, 2).await, 2);
    session.close().await.unwrap();
    handle.abort();

    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&db).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    let (y, m, d) = precall_broker::filetime_to_ymd(timeline[0].timestamp_100ns);
    let rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"))
        .join(format!("{token}.jpg"));
    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    assert!(
        objects.exists(&rel),
        "a negative rectangle is a delta: composed against the surface"
    );
    let bytes = objects.get(&client_id.to_string(), &rel, &token).unwrap();
    let img = image::load_from_memory(&bytes).unwrap();
    assert_eq!(
        (img.width(), img.height()),
        (1280, 720),
        "the archive stays the desktop, not the patch"
    );
}

/// A frame whose regions span several surfaces has no single surface, so it
/// cannot go into the one canvas the broker keeps for a client — and the `None`
/// that marks it must not be mistaken for "the primary surface".
#[tokio::test]
async fn a_frame_from_two_surfaces_is_not_composed() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "MIXED",
    )
    .await
    .expect("handshake");

    let base = SyntheticFrame {
        jpeg: solid_jpeg(1280, 720, [255, 255, 255]),
        meta: sample_meta(1, "base-1"),
    };
    session.send_capture(&base).await.unwrap();
    let db = dir.path().join(client_id.to_string()).join("ukg.db");
    assert_eq!(wait_for_captures(&db, 1).await, 1);

    // Region on the advertised surface (0, in the mock's hello) and one on
    // another surface, in the same frame.
    let mixed = vec![
        (0u16, Rect::new(0, 0, 100, 100), solid_jpeg(100, 100, [255, 0, 0])),
        (7u16, Rect::new(200, 0, 300, 100), solid_jpeg(100, 100, [0, 255, 0])),
    ];
    let frame = SyntheticFrame { jpeg: vec![], meta: sample_meta(2, "mixed-2") };
    session.send_regions_mixed(&mixed, &frame.meta).await.unwrap();
    assert_eq!(wait_for_captures(&db, 2).await, 2);
    session.close().await.unwrap();
    handle.abort();

    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&db).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    let (y, m, d) = precall_broker::filetime_to_ymd(timeline[0].timestamp_100ns);
    let base_rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"));
    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    assert!(
        objects.exists(&base_rel.join(format!("{token}.raw"))),
        "a mixed-surface frame is stored as a blob, never composed"
    );
    assert!(!objects.exists(&base_rel.join(format!("{token}.jpg"))));
}

/// A keyframe the decoder rejects must invalidate the surface: the client's later
/// deltas are relative to *that* frame, so composing them onto an older surface
/// would leave every area it changed permanently stale.
#[tokio::test]
async fn a_rejected_keyframe_invalidates_the_surface() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "REJECT",
    )
    .await
    .expect("handshake");

    let base = SyntheticFrame {
        jpeg: solid_jpeg(1280, 720, [255, 255, 255]),
        meta: sample_meta(1, "base-1"),
    };
    session.send_capture(&base).await.unwrap();
    let db = dir.path().join(client_id.to_string()).join("ukg.db");
    assert_eq!(wait_for_captures(&db, 1).await, 1);

    // A delta establishes the surface from the keyframe.
    let one = vec![(Rect::new(0, 0, 50, 50), solid_jpeg(50, 50, [1, 2, 3]))];
    let frame = SyntheticFrame { jpeg: vec![], meta: sample_meta(2, "delta-2") };
    session.send_dirty_regions(&one, &frame.meta).await.unwrap();
    assert_eq!(wait_for_captures(&db, 2).await, 2);

    // A full-surface keyframe whose bytes are not an image.
    let broken = SyntheticFrame {
        jpeg: b"this is not a JPEG".to_vec(),
        meta: sample_meta(3, "broken-3"),
    };
    session.send_capture(&broken).await.unwrap();
    assert_eq!(wait_for_captures(&db, 3).await, 3);

    // The next delta must not be composed against a stale surface.
    let two = vec![(Rect::new(600, 600, 650, 650), solid_jpeg(50, 50, [9, 9, 9]))];
    let frame = SyntheticFrame { jpeg: vec![], meta: sample_meta(4, "delta-4") };
    session.send_dirty_regions(&two, &frame.meta).await.unwrap();
    assert_eq!(wait_for_captures(&db, 4).await, 4);
    session.close().await.unwrap();
    handle.abort();

    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&db).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    assert_eq!(token, "delta-4", "newest first");
    let (y, m, d) = precall_broker::filetime_to_ymd(timeline[0].timestamp_100ns);
    let rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"));
    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    assert!(
        objects.exists(&rel.join(format!("{token}.raw"))),
        "the delta after a rejected keyframe is a blob, not a composition onto a stale surface"
    );
}

/// A keyframe whose rectangle claims the whole surface but whose image is a
/// different size must not become the surface: every later delta would clip into
/// the wrong canvas and the archive would be the wrong size.
#[tokio::test]
async fn a_wrong_sized_keyframe_does_not_become_the_surface() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir.path()), tls_cfg).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move { b.run_on(listener).await });

    let client_id = Uuid::new_v4();
    let tls = pinned_client_config(&fingerprint).unwrap();
    let mut session = Session::connect(
        addr,
        "localhost",
        tls,
        AuthMethod::Psk { token: PSK.into() },
        client_id,
        "WRONGSIZE",
    )
    .await
    .expect("handshake");

    // 640x360 bytes on a full-surface (1280x720) rectangle.
    let wrong = SyntheticFrame {
        jpeg: solid_jpeg(640, 360, [255, 255, 255]),
        meta: sample_meta(1, "wrong-1"),
    };
    session.send_capture(&wrong).await.unwrap();
    let db = dir.path().join(client_id.to_string()).join("ukg.db");
    assert_eq!(wait_for_captures(&db, 1).await, 1);

    // A delta after it must not be composed into a 640x360 canvas.
    let one = vec![(Rect::new(0, 0, 50, 50), solid_jpeg(50, 50, [1, 2, 3]))];
    let frame = SyntheticFrame { jpeg: vec![], meta: sample_meta(2, "delta-2") };
    session.send_dirty_regions(&one, &frame.meta).await.unwrap();
    assert_eq!(wait_for_captures(&db, 2).await, 2);
    session.close().await.unwrap();
    handle.abort();

    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&db).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    let token = ukg.image_token(timeline[0].id).unwrap().unwrap();
    assert_eq!(token, "delta-2");
    let (y, m, d) = precall_broker::filetime_to_ymd(timeline[0].timestamp_100ns);
    let rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{y:04}"))
        .join(format!("{m:02}"))
        .join(format!("{d:02}"));

    // The keyframe was still archived (the client labelled it as an image), but
    // it never became the surface.
    let wrong_token = ukg.image_token(timeline[1].id).unwrap().unwrap();
    assert_eq!(wrong_token, "wrong-1");
    let (wy, wm, wd) = precall_broker::filetime_to_ymd(timeline[1].timestamp_100ns);
    let wrong_rel = std::path::PathBuf::from(client_id.to_string())
        .join(format!("{wy:04}"))
        .join(format!("{wm:02}"))
        .join(format!("{wd:02}"))
        .join(format!("{wrong_token}.jpg"));

    let key = load_or_create_key(dir.path()).unwrap();
    let objects = ObjectStore::encrypted(client_dir.join("objects"), key).unwrap();
    assert!(objects.exists(&wrong_rel), "the keyframe is archived as sent");
    assert!(
        objects.exists(&rel.join(format!("{token}.raw"))),
        "the delta is a blob: the wrong-sized keyframe was not adopted as the surface"
    );
}
