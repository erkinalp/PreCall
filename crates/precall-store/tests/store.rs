// SPDX-License-Identifier: GPL-2.0-only
use precall_proto::*;
use precall_store::*;

fn sample_capture(n: u64) -> WindowCaptureRecord {
    WindowCaptureRecord {
        name: format!("Doc {n} - Editor"),
        image_token: format!("tok-{n}"),
        is_foreground: true,
        window_id: 0xBEEF,
        window_bounds: "0,0,1280,800".into(),
        window_title: format!("Kerberos spec page {n} - Notepad"),
        properties: None,
        timestamp_100ns: 133_000_000_000_000_000 + n * 10_000_000,
        activation_uri: Some("notepad://open?file=kerb.txt".into()),
        activity_id: None,
        fallback_uri: None,
        apps: vec![AppRecord {
            windows_app_id: None,
            icon_uri: None,
            name: "Notepad".into(),
            path: Some("C:\\Windows\\System32\\notepad.exe".into()),
            properties: None,
        }],
        files: vec![FileRecord {
            path: "C:\\docs\\kerb.txt".into(),
            name: "kerb.txt".into(),
            extension: Some("txt".into()),
            kind: None,
            r#type: None,
            properties: None,
            object_id: None,
            volume_id: None,
        }],
        webs: vec![WebRecord {
            domain: "learn.microsoft.com".into(),
            uri: "https://learn.microsoft.com/kerberos".into(),
            icon_uri: None,
            properties: None,
        }],
        regions: vec![ScreenRegionRecord {
            region_kind: "text_block".into(),
            ocr_text: Some("The Kerberos V5 protocol uses tickets".into()),
            bounds: "10,10,600,400".into(),
        }],
    }
}

#[test]
fn schema_has_recall_surface() {
    let db = Ukg::open_in_memory().unwrap();
    let tables = db.check_schema().unwrap();
    for required in [
        "WindowCapture",
        "App",
        "WindowCaptureAppRelation",
        "File",
        "WindowCaptureFileRelation",
        "Web",
        "WindowCaptureWebRelation",
        "ScreenRegion",
        "Topic",
        "WindowCaptureTopicRelation",
        "AppDwellTime",
        "WebDomainDwellTime",
        "WindowCaptureTextIndex",
        "PrecallSyncState",
        "PrecallConfig",
        "PrecallClient",
    ] {
        assert!(tables.iter().any(|t| t == required), "missing table {required}");
    }
}

#[test]
fn capture_insert_search_timeline() {
    let db = Ukg::open_in_memory().unwrap();
    for n in 0..5 {
        db.insert_capture(&sample_capture(n)).unwrap();
    }
    // Idempotent on image_token
    let again = db.insert_capture(&sample_capture(0)).unwrap();
    assert_eq!(again, 1);

    let tl = db.timeline(None, 10).unwrap();
    assert_eq!(tl.len(), 5);
    assert_eq!(tl[0].image_token.as_deref(), Some("tok-4")); // newest first
    assert_eq!(tl[0].apps, vec!["Notepad".to_string()]);

    let hits = db
        .search_fts(&SearchOptions {
            query: precall_store::ukg::fts_escape("Kerberos tickets"),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 5);
    assert!(hits[0].ocr_preview.as_deref().unwrap_or("").contains("Kerberos"));

    // App-filtered search
    let filtered = db
        .search_fts(&SearchOptions {
            query: precall_store::ukg::fts_escape("Kerberos"),
            app_filter: vec!["chrome.exe".into()],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(filtered.len(), 0);

    // Regions
    let regions = db.regions(1).unwrap();
    assert_eq!(regions.len(), 1);
    assert_eq!(regions[0].1, "text_block");
}

#[test]
fn dwell_time_buckets() {
    let db = Ukg::open_in_memory().unwrap();
    let ts = 133_000_000_000_000_000i64;
    db.add_app_dwell("notepad.exe", ts, 1500).unwrap();
    db.add_app_dwell("notepad.exe", ts, 2500).unwrap();
    db.add_web_dwell("learn.microsoft.com", ts, 900).unwrap();
    let apps = db.app_dwell().unwrap();
    assert_eq!(apps.len(), 1);
    assert_eq!(apps[0].dwell_time_ms, 4000);
    let web = db.web_dwell().unwrap();
    assert_eq!(web[0].dwell_time_ms, 900);
}

#[test]
fn semantic_index_round_trip() {
    let idx = SemanticIndex::open_in_memory(4, "text").unwrap();
    let item_a = SemanticIndex::item_id_for_capture(42);
    let item_b = SemanticIndex::item_id_for_capture(43);
    idx.insert(&item_a, Some("r1"), None, &[1.0, 0.0, 0.0, 0.0]).unwrap();
    idx.insert(&item_b, Some("r2"), None, &[0.0, 1.0, 0.0, 0.0]).unwrap();
    let hits = idx.search(&[0.9, 0.1, 0.0, 0.0], 1).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(SemanticIndex::capture_id_for_item(&hits[0].item_id), Some(42));
}

#[test]
fn object_store_encrypted_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectStore::encrypted(dir.path(), EnvelopeCipher::generate_key()).unwrap();
    let rel = store
        .put_image("client-1", (2026, 9, 15), "tok-9", b"\xFF\xD8jpeg")
        .unwrap();
    // On-disk bytes are ciphertext (not raw jpeg).
    let on_disk = std::fs::read(store.abs_path(&rel)).unwrap();
    assert_ne!(on_disk, b"\xFF\xD8jpeg");
    let back = store.get("client-1", &rel, "tok-9").unwrap();
    assert_eq!(back, b"\xFF\xD8jpeg");
    store.purge_client("client-1").unwrap();
    assert!(!store.exists(&rel));
}

#[test]
fn envelope_cipher_tamper_detection() {
    let key = EnvelopeCipher::generate_key();
    let c = EnvelopeCipher::new(&key);
    let mut sealed = c.seal(b"t", b"payload");
    let n = sealed.len();
    sealed[n - 1] ^= 1;
    assert!(c.open(b"t", &sealed).is_err());
}
