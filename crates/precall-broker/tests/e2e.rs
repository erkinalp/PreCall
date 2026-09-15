// SPDX-License-Identifier: GPL-2.0-only
//! End-to-end: real TLS socket, real handshake, real ingestion, real sqlite.

use precall_broker::{tls, Broker, BrokerConfig};
use precall_mock::{pinned_client_config, sample_meta, synthesize_jpeg, Session, SyntheticFrame};
use precall_proto::AuthMethod;
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
