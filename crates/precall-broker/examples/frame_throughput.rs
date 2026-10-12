// SPDX-License-Identifier: GPL-2.0-only
//! Frame-rate audit: where the capture pipeline actually spends its time.
//!
//! Two operating points matter (see README "Performance envelope"):
//!
//! * **full framerate** — a streaming session, 30-60 captures/s sustained;
//! * **Recall cadence** — one capture every few seconds, and *nothing* when the
//!   desktop has not changed.
//!
//! This harness measures both sides of the wire that can be measured off
//! Windows:
//!
//! 1. the store primitives the broker calls once per capture (`put` +
//!    `insert_capture`), which bound ingest from below;
//! 2. a real end-to-end run — real TLS, real handshake, real RDPGFX framing,
//!    real ingestion — driven by `precall-mock` at full framerate, reporting
//!    sustained frames/s and MB/s.
//!
//! Run: `cargo run --release -p precall-broker --example frame_throughput`
//! (`--` `[frames] [width] [height]`).

use precall_broker::{tls, Broker, BrokerConfig};
use precall_mock::{pinned_client_config, sample_meta, synthesize_jpeg, Session, SyntheticFrame};
use precall_proto::AuthMethod;
use precall_store::objects::load_or_create_key;
use precall_store::{ObjectStore, Ukg};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use uuid::Uuid;

const PSK: &str = "throughput-token-not-secret";

fn test_cert() -> (
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
    String,
) {
    let s = tls::generate_self_signed("localhost").unwrap();
    let certs: Vec<_> = rustls_pemfile::certs(&mut s.cert_pem.as_bytes())
        .collect::<Result<_, _>>()
        .unwrap();
    let key = tls::pkcs8_from_pem(&s.key_pem).unwrap();
    (certs, key, s.fingerprint)
}

fn config(data_root: &std::path::Path, encrypt: bool) -> BrokerConfig {
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
        // The ingest loop's liveness guard is 4x this; a throughput run must not
        // be killed by a missing client heartbeat mid-benchmark.
        heartbeat_interval_ms: 60_000,
        max_clients: 64,
        encrypt_at_rest: encrypt,
        embedding_dim: 384,
    }
}

/// `put` + `insert_capture`, the two calls the broker makes per capture.
fn store_microbench(dir: &std::path::Path, frames: usize, jpeg: &[u8]) {
    let client = "11111111-1111-1111-1111-111111111111";
    let key = load_or_create_key(dir).unwrap();
    let objects = ObjectStore::encrypted(dir.join(client).join("objects"), key).unwrap();
    std::fs::create_dir_all(dir.join(client)).unwrap();
    let ukg = Ukg::open(&dir.join(client).join("ukg.db")).unwrap();

    // Derive the date from the timestamp exactly as the ingest path does.
    let base_ts = 133_000_000_000_000_000u64;

    let t_put = Instant::now();
    for i in 0..frames {
        let ts = base_ts + (i as u64) * 10_000_000;
        let date = precall_broker::filetime_to_ymd(ts as i64);
        objects
            .put(client, date, &format!("bench-{i}"), "jpg", jpeg)
            .unwrap();
    }
    let put = t_put.elapsed();

    let t_ins = Instant::now();
    for i in 0..frames {
        let ts = base_ts + (i as u64) * 10_000_000;
        let mut rec = sample_meta(i as u64, &format!("bench-{i}"));
        rec.timestamp_100ns = ts;
        ukg.insert_capture(&rec).unwrap();
    }
    let ins = t_ins.elapsed();

    let mb = (jpeg.len() * frames) as f64 / (1024.0 * 1024.0);
    println!(
        "  store micro   {frames} frames x {:.1} KiB: put {:>8.2} ms total ({:>6.0} fps, {:>6.0} MB/s) | insert_capture {:>8.2} ms ({:>6.0} fps) | combined {:>6.0} fps",
        jpeg.len() as f64 / 1024.0,
        put.as_secs_f64() * 1000.0,
        frames as f64 / put.as_secs_f64(),
        mb / put.as_secs_f64(),
        ins.as_secs_f64() * 1000.0,
        frames as f64 / ins.as_secs_f64(),
        frames as f64 / (put + ins).as_secs_f64(),
    );
}

/// End-to-end: mock client → TLS → broker → objects + ukg.db.
async fn end_to_end(dir: &std::path::Path, frames: usize, width: u32, height: u32, encrypt: bool) -> f64 {
    let (certs, key, fingerprint) = test_cert();
    let tls_cfg = tls::server_config_from(certs, key).unwrap();
    let broker = Arc::new(Broker::new(config(dir, encrypt), tls_cfg).unwrap());
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
        AuthMethod::Psk {
            token: PSK.to_string(),
        },
        client_id,
        "BENCHBOX",
    )
    .await
    .expect("handshake");

    // Pre-encode the frames so the client side of the bench measures transport
    // and ingest, not JPEG compression (which the client does locally).
    let encoded: Vec<SyntheticFrame> = (0..frames)
        .map(|i| SyntheticFrame {
            jpeg: synthesize_jpeg(width, height, i as u64),
            meta: sample_meta(i as u64, &format!("bench-{i}")),
        })
        .collect();
    let bytes: usize = encoded.iter().map(|f| f.jpeg.len()).sum();

    // Warm-up: one frame through the whole path.
    session.send_capture(&encoded[0]).await.unwrap();

    let t0 = Instant::now();
    for f in &encoded {
        session.send_capture(f).await.unwrap();
    }
    let send = t0.elapsed();

    // Wait for the last capture to land in the metadata DB.
    let ukg_path = dir.join(client_id.to_string()).join("ukg.db");
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut landed = 0usize;
    while Instant::now() < deadline {
        if let Ok(ukg) = Ukg::open(&ukg_path) {
            landed = ukg.timeline(None, (frames + 8) as i64).map(|t| t.len()).unwrap_or(0);
            if landed >= frames {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let total = t0.elapsed();

    handle.abort();
    let mb = bytes as f64 / (1024.0 * 1024.0);
    println!(
        "  end-to-end    {frames} frames {}x{} ({:.1} KiB/frame, {:.0} MB payload{}): send {:>7.2} ms ({:>6.0} fps) | sent+committed {:>7.2} ms ({:>6.0} fps, {:>6.0} MB/s) | landed {landed}",
        width,
        height,
        bytes as f64 / frames as f64 / 1024.0,
        mb,
        if encrypt { ", encrypted" } else { ", plaintext" },
        send.as_secs_f64() * 1000.0,
        frames as f64 / send.as_secs_f64(),
        total.as_secs_f64() * 1000.0,
        frames as f64 / total.as_secs_f64(),
        mb / total.as_secs_f64(),
    );
    frames as f64 / total.as_secs_f64()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();
    let args: Vec<String> = std::env::args().collect();
    let frames: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(600);
    let width: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1920);
    let height: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1080);

    println!("PreCall frame-rate audit: {frames} frames at {width}x{height}");
    println!("(full framerate target = 60 fps; Recall cadence = 1 capture / 3 s on change)\n");

    let dir = tempfile::tempdir().unwrap();
    let jpeg = synthesize_jpeg(width, height, 7);
    println!("reference frame: {} KiB", jpeg.len() / 1024);
    store_microbench(dir.path(), frames, &jpeg);

    let dir2 = tempfile::tempdir().unwrap();
    let encrypted = end_to_end(dir2.path(), frames, width, height, true).await;

    let dir3 = tempfile::tempdir().unwrap();
    let plain = end_to_end(dir3.path(), frames, width, height, false).await;

    println!(
        "\nsummary: encrypted {encrypted:.0} fps vs plaintext {plain:.0} fps \
         (at-rest encryption overhead {:.1}%)",
        (plain - encrypted) / plain * 100.0
    );
    if encrypted >= 60.0 {
        println!("verdict: sustained full framerate (>= 60 fps) at {width}x{height}");
    } else {
        println!(
            "verdict: {encrypted:.0} fps at {width}x{height} — below the 60 fps full-framerate target"
        );
    }
}
