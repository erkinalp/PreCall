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

    // Keyframe: a full 64x64 white desktop.
    let base = SyntheticFrame {
        jpeg: solid_jpeg(64, 64, [255, 255, 255]),
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
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    session.close().await.unwrap();
    handle.abort();

    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    assert_eq!(timeline.len(), 2, "both captures landed");

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
    assert_eq!((img.width(), img.height()), (64, 64));
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
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    session.close().await.unwrap();
    handle.abort();

    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    assert_eq!(timeline.len(), 1);
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
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    session.close().await.unwrap();
    handle.abort();

    let client_dir = dir.path().join(client_id.to_string());
    let ukg = Ukg::open(&client_dir.join("ukg.db")).unwrap();
    let timeline = ukg.timeline(None, 10).unwrap();
    assert_eq!(timeline.len(), 2);
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
