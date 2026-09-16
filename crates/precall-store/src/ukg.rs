// SPDX-License-Identifier: GPL-2.0-only
//! `ukg.db` — the Recall-compatible metadata database.
//!
//! Table and column names match Windows Recall exactly; Precall-only state
//! lives in `Precall*` tables. One database per client on the server; the same
//! file format is used for the client's offline cache, which is what makes
//! migration a file copy.

use crate::schema::UKG_SCHEMA;
use precall_proto::{AppRecord, FileRecord, WebRecord, WindowCaptureRecord};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UkgError {
    #[error("sqlite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// (Id, RegionKind, OcrText, Bounds) — a `ScreenRegion` row.
pub type RegionRow = (i64, String, Option<String>, String);

/// One row of the timeline endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineEntry {
    pub id: i64,
    pub name: Option<String>,
    pub image_token: Option<String>,
    pub window_title: Option<String>,
    pub window_bounds: Option<String>,
    /// FILETIME ticks.
    pub timestamp_100ns: i64,
    pub activation_uri: Option<String>,
    pub fallback_uri: Option<String>,
    pub apps: Vec<String>,
}

/// One search hit from FTS5 and/or semantic search.
#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub window_capture_id: i64,
    pub timestamp_100ns: i64,
    pub window_title: Option<String>,
    pub app_name: Option<String>,
    pub image_token: Option<String>,
    pub ocr_preview: Option<String>,
    /// bm25 (negative, lower=better) mapped to a positive score, or cosine.
    pub score: f64,
    pub activation_uri: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SearchOptions {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub start_100ns: Option<i64>,
    #[serde(default)]
    pub end_100ns: Option<i64>,
    #[serde(default)]
    pub app_filter: Vec<String>,
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AppDwellEntry {
    pub windows_app_id: String,
    pub hour_of_day: u32,
    pub day_of_week: u32,
    pub hour_start_100ns: i64,
    pub dwell_time_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WebDwellEntry {
    pub domain: String,
    pub hour_of_day: u32,
    pub day_of_week: u32,
    pub hour_start_100ns: i64,
    pub dwell_time_ms: i64,
}

/// A per-client (or local-cache) `ukg.db`.
pub struct Ukg {
    conn: Connection,
}

impl Ukg {
    pub fn open(path: &Path) -> Result<Self, UkgError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(path)?;
        let db = Self { conn };
        db.init()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self, UkgError> {
        let conn = Connection::open_in_memory()?;
        let db = Self { conn };
        db.init()?;
        Ok(db)
    }

    fn init(&self) -> Result<(), UkgError> {
        self.conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;",
        )?;
        self.conn.execute_batch(UKG_SCHEMA)?;
        Ok(())
    }

    /// Verify the database carries the Recall table surface — used by the
    /// schema-compat test.
    pub fn check_schema(&self) -> Result<Vec<String>, UkgError> {
        let mut stmt = self
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type IN ('table','view') ORDER BY name")?;
        let names = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(names)
    }

    // -- ingestion -----------------------------------------------------------

    /// Insert one capture record + all its relations. Idempotent on
    /// `image_token` (returns existing id on duplicate).
    pub fn insert_capture(&self, rec: &WindowCaptureRecord) -> Result<i64, UkgError> {
        let tx = self.conn.unchecked_transaction()?;
        if let Some(id) = tx
            .query_row(
                "SELECT \"Id\" FROM \"WindowCapture\" WHERE \"ImageToken\" = ?",
                params![rec.image_token],
                |r| r.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }
        tx.execute(
            "INSERT INTO \"WindowCapture\"
             (\"Name\",\"ImageToken\",\"IsForeground\",\"WindowId\",\"WindowBounds\",
              \"WindowTitle\",\"Properties\",\"TimeStamp\",\"IsProcessed\",
              \"ActivationUri\",\"ActivityId\",\"FallbackUri\")
             VALUES (?,?,?,?,?,?,?,?,0,?,?,?)",
            params![
                rec.name,
                rec.image_token,
                rec.is_foreground as i64,
                rec.window_id,
                rec.window_bounds,
                rec.window_title,
                rec.properties,
                rec.timestamp_100ns as i64,
                rec.activation_uri,
                rec.activity_id,
                rec.fallback_uri,
            ],
        )?;
        let wc_id = tx.last_insert_rowid();

        for app in &rec.apps {
            let app_id = upsert_app(&tx, app)?;
            tx.execute(
                "INSERT OR IGNORE INTO \"WindowCaptureAppRelation\" (\"WindowCaptureId\",\"AppId\") VALUES (?,?)",
                params![wc_id, app_id],
            )?;
        }
        for f in &rec.files {
            let file_id = upsert_file(&tx, f)?;
            tx.execute(
                "INSERT OR IGNORE INTO \"WindowCaptureFileRelation\" (\"WindowCaptureId\",\"FileId\") VALUES (?,?)",
                params![wc_id, file_id],
            )?;
        }
        for w in &rec.webs {
            let web_id = upsert_web(&tx, w)?;
            tx.execute(
                "INSERT OR IGNORE INTO \"WindowCaptureWebRelation\" (\"WindowCaptureId\",\"WebId\") VALUES (?,?)",
                params![wc_id, web_id],
            )?;
        }
        let mut all_ocr = String::new();
        for r in &rec.regions {
            tx.execute(
                "INSERT INTO \"ScreenRegion\" (\"WindowCaptureId\",\"RegionKind\",\"OcrText\",\"Bounds\")
                 VALUES (?,?,?,?)",
                params![wc_id, r.region_kind, r.ocr_text, r.bounds],
            )?;
            if let Some(t) = &r.ocr_text {
                all_ocr.push_str(t);
                all_ocr.push(' ');
            }
        }
        tx.execute(
            "INSERT INTO \"WindowCaptureTextIndex\" (\"rowid\",\"Name\",\"WindowTitle\",\"OcrText\")
             VALUES (?,?,?,?)",
            params![wc_id, rec.name, rec.window_title, all_ocr.trim()],
        )?;
        tx.commit()?;
        Ok(wc_id)
    }

    /// Record (or refresh) a client in `PrecallClient`.
    pub fn touch_client(&self, client_id: &str, hostname: &str, now_100ns: i64) -> Result<(), UkgError> {
        self.conn.execute(
            "INSERT INTO \"PrecallClient\" (\"ClientId\",\"Hostname\",\"FirstSeen\",\"LastSeen\")
             VALUES (?,?,?,?)
             ON CONFLICT(\"ClientId\") DO UPDATE SET \"Hostname\"=excluded.\"Hostname\", \"LastSeen\"=excluded.\"LastSeen\"",
            params![client_id, hostname, now_100ns, now_100ns],
        )?;
        Ok(())
    }

    /// Attach a topic to a capture (server-side AI enrichment).
    pub fn attach_topic(&self, wc_id: i64, title: &str, score: f64) -> Result<(), UkgError> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO \"Topic\" (\"Title\") VALUES (?)",
            params![title],
        )?;
        let topic_id: i64 = tx.query_row(
            "SELECT \"Id\" FROM \"Topic\" WHERE \"Title\" = ?",
            params![title],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO \"WindowCaptureTopicRelation\" (\"WindowCaptureId\",\"TopicId\",\"Score\")
             VALUES (?,?,?)",
            params![wc_id, topic_id, score],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Accumulate dwell time for an app in the hour bucket of `ts_100ns`.
    pub fn add_app_dwell(
        &self,
        windows_app_id: &str,
        ts_100ns: i64,
        dwell_ms: i64,
    ) -> Result<(), UkgError> {
        let (hour, dow, bucket) = hour_bucket(ts_100ns);
        self.conn.execute(
            "INSERT INTO \"AppDwellTime\" (\"WindowsAppId\",\"HourOfDay\",\"DayOfWeek\",\"HourStartTimestamp\",\"DwellTime\")
             VALUES (?,?,?,?,?)
             ON CONFLICT(\"WindowsAppId\",\"HourStartTimestamp\") DO UPDATE SET \"DwellTime\"=\"DwellTime\"+excluded.\"DwellTime\"",
            params![windows_app_id, hour, dow, bucket, dwell_ms],
        )?;
        Ok(())
    }

    pub fn add_web_dwell(
        &self,
        domain: &str,
        ts_100ns: i64,
        dwell_ms: i64,
    ) -> Result<(), UkgError> {
        let (hour, dow, bucket) = hour_bucket(ts_100ns);
        self.conn.execute(
            "INSERT INTO \"WebDomainDwellTime\" (\"Domain\",\"HourOfDay\",\"DayOfWeek\",\"HourStartTimestamp\",\"DwellTime\")
             VALUES (?,?,?,?,?)
             ON CONFLICT(\"Domain\",\"HourStartTimestamp\") DO UPDATE SET \"DwellTime\"=\"DwellTime\"+excluded.\"DwellTime\"",
            params![domain, hour, dow, bucket, dwell_ms],
        )?;
        Ok(())
    }

    /// Mark a capture's OCR/embedding enrichment complete.
    pub fn mark_processed(&self, wc_id: i64) -> Result<(), UkgError> {
        self.conn.execute(
            "UPDATE \"WindowCapture\" SET \"IsProcessed\" = 1 WHERE \"Id\" = ?",
            params![wc_id],
        )?;
        Ok(())
    }

    // -- queries -------------------------------------------------------------

    /// Paginated timeline, newest first, `before` for cursor pagination.
    pub fn timeline(
        &self,
        before_100ns: Option<i64>,
        limit: i64,
    ) -> Result<Vec<TimelineEntry>, UkgError> {
        let before = before_100ns.unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            "SELECT \"Id\",\"Name\",\"ImageToken\",\"WindowTitle\",\"WindowBounds\",
                    \"TimeStamp\",\"ActivationUri\",\"FallbackUri\"
             FROM \"WindowCapture\" WHERE \"TimeStamp\" < ?
             ORDER BY \"TimeStamp\" DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![before, limit], |r| {
            Ok(TimelineEntry {
                id: r.get(0)?,
                name: r.get(1)?,
                image_token: r.get(2)?,
                window_title: r.get(3)?,
                window_bounds: r.get(4)?,
                timestamp_100ns: r.get(5)?,
                activation_uri: r.get(6)?,
                fallback_uri: r.get(7)?,
                apps: Vec::new(),
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            let mut e = row?;
            e.apps = self.capture_app_names(e.id)?;
            out.push(e);
        }
        Ok(out)
    }

    /// Full-text search over the FTS5 index. `opts.query` is FTS5 syntax —
    /// callers pass a sanitized version (see [`fts_escape`]).
    pub fn search_fts(&self, opts: &SearchOptions) -> Result<Vec<SearchHit>, UkgError> {
        let limit = opts.limit.unwrap_or(50).clamp(1, 500);
        let mut sql = String::from(
            "SELECT w.\"Id\", w.\"TimeStamp\", w.\"WindowTitle\", w.\"ImageToken\",
                    w.\"ActivationUri\", snippet(\"WindowCaptureTextIndex\", 2, '<b>', '</b>', '…', 16) AS snip,
                    bm25(\"WindowCaptureTextIndex\") AS rank
             FROM \"WindowCaptureTextIndex\" f
             JOIN \"WindowCapture\" w ON w.\"Id\" = f.rowid
             WHERE \"WindowCaptureTextIndex\" MATCH ?",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        params_vec.push(Box::new(opts.query.clone()));
        if let Some(s) = opts.start_100ns {
            sql.push_str(" AND w.\"TimeStamp\" >= ?");
            params_vec.push(Box::new(s));
        }
        if let Some(end) = opts.end_100ns {
            sql.push_str(" AND w.\"TimeStamp\" <= ?");
            params_vec.push(Box::new(end));
        }
        if !opts.app_filter.is_empty() {
            let marks = vec!["?"; opts.app_filter.len()].join(",");
            sql.push_str(&format!(
                " AND w.\"Id\" IN (SELECT \"WindowCaptureId\" FROM \"WindowCaptureAppRelation\" r
                 JOIN \"App\" a ON a.\"Id\" = r.\"AppId\" WHERE a.\"Path\" IN ({marks}) OR a.\"Name\" IN ({marks}))"
            ));
            for app in &opts.app_filter {
                params_vec.push(Box::new(app.clone()));
                params_vec.push(Box::new(app.clone()));
            }
        }
        sql.push_str(" ORDER BY rank LIMIT ?");
        params_vec.push(Box::new(limit));
        let mut stmt = self.conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(&params_ref[..], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, f64>(6)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, ts, title, token, act, snip, rank) = row?;
            out.push(SearchHit {
                window_capture_id: id,
                timestamp_100ns: ts,
                window_title: title,
                app_name: self.capture_app_names(id)?.into_iter().next(),
                image_token: token,
                ocr_preview: snip,
                score: -rank,
                activation_uri: act,
            });
        }
        Ok(out)
    }

    /// Re-rank a set of capture ids by semantic score (called by the API
    /// after the `si_*` index produces candidates).
    pub fn captures_by_ids(&self, ids: &[i64]) -> Result<HashMap<i64, SearchHit>, UkgError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let marks = vec!["?"; ids.len()].join(",");
        let mut stmt = self.conn.prepare(&format!(
            "SELECT \"Id\",\"TimeStamp\",\"WindowTitle\",\"ImageToken\",\"ActivationUri\",\"Name\"
             FROM \"WindowCapture\" WHERE \"Id\" IN ({marks})"
        ))?;
        let params: Vec<i64> = ids.to_vec();
        let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| {
            Ok((
                r.get::<_, i64>(0)?,
                SearchHit {
                    window_capture_id: r.get(0)?,
                    timestamp_100ns: r.get(1)?,
                    window_title: r.get(2)?,
                    image_token: r.get(3)?,
                    activation_uri: r.get(4)?,
                    app_name: None,
                    ocr_preview: r.get(5)?,
                    score: 0.0,
                },
            ))
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let (id, hit) = row?;
            map.insert(id, hit);
        }
        Ok(map)
    }

    pub fn app_dwell(&self) -> Result<Vec<AppDwellEntry>, UkgError> {
        let mut stmt = self.conn.prepare(
            "SELECT \"WindowsAppId\",\"HourOfDay\",\"DayOfWeek\",\"HourStartTimestamp\",\"DwellTime\"
             FROM \"AppDwellTime\" ORDER BY \"HourStartTimestamp\" DESC LIMIT 10000",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(AppDwellEntry {
                windows_app_id: r.get(0)?,
                hour_of_day: r.get::<_, i64>(1)? as u32,
                day_of_week: r.get::<_, i64>(2)? as u32,
                hour_start_100ns: r.get(3)?,
                dwell_time_ms: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn web_dwell(&self) -> Result<Vec<WebDwellEntry>, UkgError> {
        let mut stmt = self.conn.prepare(
            "SELECT \"Domain\",\"HourOfDay\",\"DayOfWeek\",\"HourStartTimestamp\",\"DwellTime\"
             FROM \"WebDomainDwellTime\" ORDER BY \"HourStartTimestamp\" DESC LIMIT 10000",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(WebDwellEntry {
                domain: r.get(0)?,
                hour_of_day: r.get::<_, i64>(1)? as u32,
                day_of_week: r.get::<_, i64>(2)? as u32,
                hour_start_100ns: r.get(3)?,
                dwell_time_ms: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Distinct apps seen — for analytics page + filters.
    pub fn apps(&self) -> Result<Vec<(i64, String, Option<String>)>, UkgError> {
        let mut stmt = self
            .conn
            .prepare("SELECT \"Id\",\"Name\",\"Path\" FROM \"App\" ORDER BY \"Name\"")?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn image_token(&self, wc_id: i64) -> Result<Option<String>, UkgError> {
        Ok(self
            .conn
            .query_row(
                "SELECT \"ImageToken\" FROM \"WindowCapture\" WHERE \"Id\" = ?",
                params![wc_id],
                |r| r.get(0),
            )
            .optional()?)
    }

    pub fn capture_timestamp(&self, wc_id: i64) -> Result<Option<i64>, UkgError> {
        Ok(self
            .conn
            .query_row(
                "SELECT \"TimeStamp\" FROM \"WindowCapture\" WHERE \"Id\" = ?",
                params![wc_id],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Screen regions for one capture (Click-to-Do overlays).
    pub fn regions(
        &self,
        wc_id: i64,
    ) -> Result<Vec<RegionRow>, UkgError> {
        let mut stmt = self.conn.prepare(
            "SELECT \"Id\",\"RegionKind\",\"OcrText\",\"Bounds\"
             FROM \"ScreenRegion\" WHERE \"WindowCaptureId\" = ? ORDER BY \"Id\"",
        )?;
        let rows = stmt.query_map(params![wc_id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Set a `PrecallConfig` value.
    pub fn set_config(&self, key: &str, value: &str) -> Result<(), UkgError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO \"PrecallConfig\" (\"Key\",\"Value\") VALUES (?,?)",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_config(&self, key: &str) -> Result<Option<String>, UkgError> {
        Ok(self
            .conn
            .query_row(
                "SELECT \"Value\" FROM \"PrecallConfig\" WHERE \"Key\" = ?",
                params![key],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Delete every trace of a client (panic button).
    pub fn purge_all(&self) -> Result<(), UkgError> {
        self.conn.execute_batch(
            "DELETE FROM \"WindowCaptureTopicRelation\";
             DELETE FROM \"WindowCaptureAppRelation\";
             DELETE FROM \"WindowCaptureFileRelation\";
             DELETE FROM \"WindowCaptureWebRelation\";
             DELETE FROM \"ScreenRegion\";
             DELETE FROM \"WindowCaptureTextIndex\";
             DELETE FROM \"WindowCapture\";
             DELETE FROM \"AppDwellTime\";
             DELETE FROM \"WebDomainDwellTime\";
             DELETE FROM \"PrecallSyncState\";",
        )?;
        Ok(())
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    fn capture_app_names(&self, wc_id: i64) -> Result<Vec<String>, UkgError> {
        let mut stmt = self.conn.prepare(
            "SELECT a.\"Name\" FROM \"WindowCaptureAppRelation\" r
             JOIN \"App\" a ON a.\"Id\" = r.\"AppId\" WHERE r.\"WindowCaptureId\" = ?",
        )?;
        let rows = stmt.query_map(params![wc_id], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
}

fn upsert_app(tx: &Connection, a: &AppRecord) -> Result<i64, UkgError> {
    tx.execute(
        "INSERT OR IGNORE INTO \"App\" (\"WindowsAppId\",\"IconUri\",\"Name\",\"Path\",\"Properties\")
         VALUES (?,?,?,?,?)",
        params![a.windows_app_id, a.icon_uri, a.name, a.path, a.properties],
    )?;
    Ok(tx.query_row(
        "SELECT \"Id\" FROM \"App\" WHERE \"Name\" = ? AND IFNULL(\"Path\",'') = IFNULL(?, '')",
        params![a.name, a.path],
        |r| r.get(0),
    )?)
}

fn upsert_file(tx: &Connection, f: &FileRecord) -> Result<i64, UkgError> {
    tx.execute(
        "INSERT OR IGNORE INTO \"File\" (\"Path\",\"Name\",\"Extension\",\"Kind\",\"Type\",\"Properties\",\"ObjectId\",\"VolumeId\")
         VALUES (?,?,?,?,?,?,?,?)",
        params![f.path, f.name, f.extension, f.kind, f.r#type, f.properties, f.object_id, f.volume_id],
    )?;
    Ok(tx.query_row(
        "SELECT \"Id\" FROM \"File\" WHERE \"Path\" = ?",
        params![f.path],
        |r| r.get(0),
    )?)
}

fn upsert_web(tx: &Connection, w: &WebRecord) -> Result<i64, UkgError> {
    tx.execute(
        "INSERT OR IGNORE INTO \"Web\" (\"Domain\",\"Uri\",\"IconUri\",\"Properties\")
         VALUES (?,?,?,?)",
        params![w.domain, w.uri, w.icon_uri, w.properties],
    )?;
    Ok(tx.query_row(
        "SELECT \"Id\" FROM \"Web\" WHERE \"Uri\" = ?",
        params![w.uri],
        |r| r.get(0),
    )?)
}

/// Escape a free-text query for FTS5: quote each term so user punctuation
/// can't inject MATCH syntax.
pub fn fts_escape(query: &str) -> String {
    query
        .split_whitespace()
        .filter(|t| !t.is_empty())
        .map(|t| format!("\"{}\"", t.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Hour bucket for dwell-time tables. Returns (hour_of_day, day_of_week,
/// hour_start_100ns) — day_of_week 0=Sunday like Recall.
fn hour_bucket(ts_100ns: i64) -> (i64, i64, i64) {
    const TICKS_PER_SEC: i64 = 10_000_000;
    const TICKS_PER_HOUR: i64 = 3600 * TICKS_PER_SEC;
    // FILETIME epoch 1601-01-01 was a Monday; unix epoch offset:
    const FILETIME_UNIX_DELTA: i64 = 116_444_736_000_000_000;
    let unix_secs = (ts_100ns - FILETIME_UNIX_DELTA) / TICKS_PER_SEC;
    let days = unix_secs.div_euclid(86_400);
    let secs_of_day = unix_secs.rem_euclid(86_400);
    let hour = secs_of_day / 3600;
    // 1970-01-01 was Thursday (dow 4).
    let dow = (days + 4).rem_euclid(7);
    let hour_start = FILETIME_UNIX_DELTA + (days * 24 + hour) * TICKS_PER_HOUR;
    (hour, dow, hour_start)
}
