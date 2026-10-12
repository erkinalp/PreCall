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
    let hits = idx.search(&[0.9, 0.1, 0.0, 0.0], 1, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(SemanticIndex::capture_id_for_item(&hits[0].item_id), Some(42));
    // An allow-list is applied during the scan, not to a truncated top-k: a
    // rank-2 match survives when only it is allowed.
    let only_b = std::collections::HashSet::from([43]);
    let filtered = idx.search(&[0.9, 0.1, 0.0, 0.0], 1, Some(&only_b)).unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(SemanticIndex::capture_id_for_item(&filtered[0].item_id), Some(43));
}

#[test]
fn semantic_has_item_is_the_idempotency_guard() {
    let idx = SemanticIndex::open_in_memory(4, "image").unwrap();
    let item = SemanticIndex::item_id_for_capture(7);
    assert!(!idx.has_item(&item).unwrap());
    idx.insert(&item, None, None, &[0.5, 0.5, 0.5, 0.5]).unwrap();
    assert!(idx.has_item(&item).unwrap());
    // A restart that re-runs the pass sees the item and skips the insert.
    assert_eq!(idx.count().unwrap(), 1);
    // Length is validated the same way insert() validates it.
    assert!(idx.has_item(&[0u8; 8]).is_err());
}

#[test]
fn object_store_encrypted_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectStore::encrypted(dir.path(), EnvelopeCipher::generate_key()).unwrap();
    let rel = store
        .put_image("client-1", (2026, 9, 15), "tok-9", b"\xFF\xD8jpeg")
        .unwrap();
    // Readers (snapshot route, enrichment) rebuild the key with key_path;
    // it must match what put() wrote, byte for byte.
    assert_eq!(rel, ObjectStore::key_path("client-1", (2026, 9, 15), "tok-9", "jpg"));
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

#[test]
fn a_torn_insert_is_not_treated_as_done() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("SemanticImageStore.db");
    let idx = SemanticIndex::open(&path, 4, "hash:test@4").unwrap();
    let item = SemanticIndex::item_id_for_capture(9);
    // Simulate a crash after the first of the three inserts.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO \"si_items\" (\"id\") VALUES (?)",
            rusqlite::params![item.clone()],
        )
        .unwrap();
    }
    assert!(
        !idx.has_item(&item).unwrap(),
        "an item row without a graph row is not a searchable embedding"
    );
    // The repair path can then complete the insert.
    idx.insert(&item, None, None, &[1.0, 0.0, 0.0, 0.0]).unwrap();
    assert!(idx.has_item(&item).unwrap());
}

#[test]
fn space_id_round_trips_and_clear_empties_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("SemanticTextStore.db");
    let idx = SemanticIndex::open(&path, 4, "hash:first@4").unwrap();
    assert_eq!(idx.space_id().unwrap().as_deref(), Some("hash:first@4"));
    idx.insert(
        &SemanticIndex::item_id_for_capture(1),
        None,
        None,
        &[1.0, 0.0, 0.0, 0.0],
    )
    .unwrap();
    idx.set_space_id("clip:second@4").unwrap();
    assert_eq!(idx.space_id().unwrap().as_deref(), Some("clip:second@4"));
    idx.clear().unwrap();
    assert_eq!(idx.count().unwrap(), 0);
    assert!(!idx.has_item(&SemanticIndex::item_id_for_capture(1)).unwrap());
}

/// The retry queue must not let a batch of stuck captures monopolize it: rows
/// attempted recently are skipped, never-attempted rows come first, and the
/// rest rotate by how long ago they were tried.
#[test]
fn image_backfill_queue_rotates_and_backs_off() {
    let db = Ukg::open_in_memory().unwrap();
    let mut ids = Vec::new();
    for n in 1..=3u64 {
        let mut rec = sample_capture(n);
        rec.image_token = format!("tok-{n}");
        let id = db.insert_capture(&rec).unwrap();
        db.mark_processed(id).unwrap();
        ids.push(id);
    }
    let now = 133_000_000_000_000_000i64;
    let backoff = 60 * 10_000_000i64;

    // #1 failed just now; #3 failed long ago; #2 has never been attempted.
    db.set_image_embedding_state(ids[0], "pending", 1, Some("stub"), now)
        .unwrap();
    db.set_image_embedding_state(ids[2], "pending", 1, Some("stub"), now - 3 * backoff)
        .unwrap();
    // A settled capture is never selected.
    db.set_image_embedding_state(ids[1], "skipped", 1, Some("non-JPEG frame"), now)
        .unwrap();

    // The fresh queue paces on the same clock, and the clock is touched by the
    // *attempt*, not by the image state (a text-only retry must advance it).
    db.touch_enrich_attempt(ids[1], now).unwrap();
    let fresh = db.captures_pending_enrichment(64, now, backoff).unwrap();
    assert!(
        !fresh.iter().any(|(id, _, _)| *id == ids[1]),
        "#2 is inside its attempt backoff"
    );
    let fresh = db.captures_pending_enrichment(64, now - 2 * backoff, backoff).unwrap();
    assert!(
        fresh.iter().any(|(id, _, _)| *id == ids[1]),
        "#2 returns once its attempt backoff elapses: {fresh:?}"
    );

    // #1 is inside its backoff window: the other two still get served.
    let queued: Vec<i64> = db
        .captures_needing_image_embedding(64, now, backoff)
        .unwrap()
        .into_iter()
        .map(|(id, _, _)| id)
        .collect();
    assert_eq!(queued, vec![ids[2]], "only the elapsed retry is due: {queued:?}");

    // Once its backoff elapses, #1 returns — it is not starved forever.
    let later = now + backoff + 1;
    let queued: Vec<i64> = db
        .captures_needing_image_embedding(64, later, backoff)
        .unwrap()
        .into_iter()
        .map(|(id, _, _)| id)
        .collect();
    // #3 was attempted longest ago, so it comes first; #1's slot is now due
    // rather than excluded.
    assert_eq!(queued, vec![ids[2], ids[0]], "oldest attempt first: {queued:?}");
}
