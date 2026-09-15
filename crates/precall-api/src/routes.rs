// SPDX-License-Identifier: GPL-2.0-only
//! Route handlers. All DB access is per-request (`rusqlite::Connection` is
//! `Send` but not `Sync`; WAL makes concurrent readers cheap).

use crate::auth::{require_auth, ApiAuth};
use crate::embed::Embedder;
use crate::ApiError;
use axum::extract::{Path as AxPath, Query, State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use precall_store::objects::ymd_from_filetime;
use precall_store::ukg::fts_escape;
use precall_store::{ObjectStore, SemanticIndex, Ukg};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct ApiState {
    pub data_root: PathBuf,
    pub embedder: Arc<Embedder>,
    pub object_key: [u8; 32],
    pub auth: ApiAuth,
    pub embedding_dim: usize,
}

/// Router with auth middleware applied.
pub fn build_router(state: ApiState) -> Router {
    let api = Router::new()
        .route("/timeline", get(timeline))
        .route("/search", post(search))
        .route("/snapshot/{id}", get(snapshot))
        .route("/regions/{id}", get(regions))
        .route("/relaunch", post(relaunch))
        .route("/apps", get(apps))
        .route("/web", get(web))
        .route("/clients", get(clients))
        .route("/export", get(export))
        .route("/client/{id}/data", axum::routing::delete(wipe_client))
        .route("/live", get(live_ws))
        .route_layer(axum::middleware::from_fn_with_state(state.auth.clone(), require_auth));

    Router::new()
        .route("/health", get(health))
        .nest("/api/v1", api)
        .with_state(state)
}

// -- helpers ------------------------------------------------------------------

fn client_db(state: &ApiState, client: &str) -> Result<Ukg, ApiError> {
    let id = Uuid::parse_str(client)
        .map_err(|_| ApiError::BadRequest("client must be a uuid".into()))?;
    let path = state.data_root.join(id.to_string()).join("ukg.db");
    if !path.exists() {
        return Err(ApiError::NotFound(format!("unknown client {client}")));
    }
    Ok(Ukg::open(&path)?)
}

fn client_dir(state: &ApiState, client: &str) -> Result<PathBuf, ApiError> {
    let id = Uuid::parse_str(client)
        .map_err(|_| ApiError::BadRequest("client must be a uuid".into()))?;
    let dir = state.data_root.join(id.to_string());
    if !dir.exists() {
        return Err(ApiError::NotFound(format!("unknown client {client}")));
    }
    Ok(dir)
}

#[derive(Deserialize)]
struct TimelineParams {
    client: String,
    before: Option<i64>,
    limit: Option<i64>,
}

async fn health() -> &'static str {
    "ok"
}

async fn timeline(
    State(st): State<ApiState>,
    Query(p): Query<TimelineParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let db = client_db(&st, &p.client)?;
    let entries = db.timeline(p.before, p.limit.unwrap_or(50).clamp(1, 500))?;
    Ok(Json(serde_json::json!({ "entries": entries })))
}

#[derive(Deserialize)]
struct SearchBody {
    client: String,
    query: String,
    #[serde(default)]
    time_range: Option<TimeRange>,
    #[serde(default)]
    app_filter: Vec<String>,
    #[serde(default)]
    limit: Option<i64>,
    /// "hybrid" (default) | "fts" | "semantic"
    #[serde(default)]
    mode: Option<String>,
}

#[derive(Deserialize)]
struct TimeRange {
    start: Option<i64>,
    end: Option<i64>,
}

async fn search(
    State(st): State<ApiState>,
    Json(body): Json<SearchBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if body.query.trim().is_empty() {
        return Err(ApiError::BadRequest("query must not be empty".into()));
    }
    let db = client_db(&st, &body.client)?;
    let mode = body.mode.as_deref().unwrap_or("hybrid");
    let limit = body.limit.unwrap_or(20).clamp(1, 200);

    let (start, end) = body
        .time_range
        .as_ref()
        .map(|t| (t.start, t.end))
        .unwrap_or((None, None));

    let opts = precall_store::SearchOptions {
        query: fts_escape(&body.query),
        start_100ns: start,
        end_100ns: end,
        app_filter: body.app_filter.clone(),
        limit: Some(limit),
    };

    // FTS5 leg.
    let mut hits: HashMap<i64, precall_store::SearchHit> = HashMap::new();
    if mode != "semantic" {
        for h in db.search_fts(&opts)? {
            hits.insert(h.window_capture_id, h);
        }
    }

    // Semantic leg: embed the query, scan the text si_* store, merge.
    if mode != "fts" {
        if let Some(qemb) = st.embedder.embed_text(&body.query).await {
            let dir = client_dir(&st, &body.client)?;
            let sidx = SemanticIndex::open(
                &dir.join("SemanticTextStore.db"),
                st.embedding_dim,
                "text",
            )?;
            let semantic = sidx.search(&qemb, (limit * 2) as usize)?;
            let mut related = db.captures_by_ids(
                &semantic
                    .iter()
                    .filter_map(|h| SemanticIndex::capture_id_for_item(&h.item_id))
                    .collect::<Vec<_>>(),
            )?;
            for (rank, sh) in semantic.iter().enumerate() {
                if let Some(cid) = SemanticIndex::capture_id_for_item(&sh.item_id) {
                    if let Some(entry) = related.get_mut(&cid) {
                        entry.score = sh.score as f64 + (limit as f64 - rank as f64) * 1e-4;
                        hits.entry(cid).or_insert_with(|| entry.clone());
                    }
                }
            }
        }
    }

    let mut results: Vec<_> = hits.into_values().collect();
    results.sort_by(|a, b| {
        b.timestamp_100ns
            .cmp(&a.timestamp_100ns)
            .then(b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal))
    });
    results.truncate(limit as usize);

    let out: Vec<serde_json::Value> = results
        .iter()
        .map(|h| {
            serde_json::json!({
                "window_capture_id": h.window_capture_id,
                "timestamp_100ns": h.timestamp_100ns,
                "window_title": h.window_title,
                "app": {"name": h.app_name},
                "screenshot_url": format!("/api/v1/snapshot/{}?client={}", h.window_capture_id, body.client),
                "ocr_text_preview": h.ocr_preview,
                "relevance_score": h.score,
                "activation_uri": h.activation_uri,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "results": out })))
}

#[derive(Deserialize)]
struct SnapshotParams {
    client: String,
}

async fn snapshot(
    State(st): State<ApiState>,
    AxPath(id): AxPath<i64>,
    Query(p): Query<SnapshotParams>,
) -> Result<Response, ApiError> {
    let db = client_db(&st, &p.client)?;
    let token = db
        .image_token(id)?
        .ok_or_else(|| ApiError::NotFound("no such capture".into()))?;
    let ts = db
        .capture_timestamp(id)?
        .ok_or_else(|| ApiError::NotFound("no such capture".into()))?;
    let dir = client_dir(&st, &p.client)?;
    let objects = ObjectStore::encrypted(dir.join("objects"), st.object_key)?;
    let (y, m, d) = ymd_from_filetime(ts);
    for ext in ["jpg", "raw"] {
        let rel = PathBuf::from(&p.client)
            .join(format!("{y:04}"))
            .join(format!("{m:02}"))
            .join(format!("{d:02}"))
            .join(format!("{token}.{ext}"));
        if objects.exists(&rel) {
            let bytes = objects.get(&p.client, &rel, &token)?;
            let mime = if ext == "jpg" { "image/jpeg" } else { "application/octet-stream" };
            return Ok(([(axum::http::header::CONTENT_TYPE, mime)], bytes).into_response());
        }
    }
    Err(ApiError::NotFound("image object missing".into()))
}

async fn regions(
    State(st): State<ApiState>,
    AxPath(id): AxPath<i64>,
    Query(p): Query<SnapshotParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let db = client_db(&st, &p.client)?;
    let regs = db.regions(id)?;
    let out: Vec<serde_json::Value> = regs
        .iter()
        .map(|(rid, kind, ocr, bounds)| {
            serde_json::json!({
                "id": rid, "kind": kind, "text": ocr, "bounds": bounds,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "regions": out })))
}

#[derive(Deserialize)]
struct RelaunchBody {
    client: String,
    window_capture_id: i64,
}

/// Returns the activation/fallback URI recorded for the capture — the caller
/// (web UI or a connected client polling PCQUERY) performs the actual
/// `IApplicationActivationManager` launch.
async fn relaunch(
    State(st): State<ApiState>,
    Json(body): Json<RelaunchBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let db = client_db(&st, &body.client)?;
    let mut stmt = db.conn().prepare(
        "SELECT \"ActivationUri\",\"FallbackUri\" FROM \"WindowCapture\" WHERE \"Id\" = ?",
    )?;
    let uris: Option<(Option<String>, Option<String>)> = stmt
        .query_map([body.window_capture_id], |r| Ok((r.get(0)?, r.get(1)?)))?
        .next()
        .transpose()?;
    match uris {
        Some((act, fb)) if act.is_some() || fb.is_some() => Ok(Json(serde_json::json!({
            "window_capture_id": body.window_capture_id,
            "activation_uri": act,
            "fallback_uri": fb,
        }))),
        Some(_) => Err(ApiError::NotFound("capture has no launch URI".into())),
        None => Err(ApiError::NotFound("no such capture".into())),
    }
}

async fn apps(
    State(st): State<ApiState>,
    Query(p): Query<SnapshotParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let db = client_db(&st, &p.client)?;
    Ok(Json(serde_json::json!({
        "apps": db.apps()?,
        "dwell": db.app_dwell()?,
    })))
}

async fn web(
    State(st): State<ApiState>,
    Query(p): Query<SnapshotParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let db = client_db(&st, &p.client)?;
    Ok(Json(serde_json::json!({ "dwell": db.web_dwell()? })))
}

async fn clients(State(st): State<ApiState>) -> Result<Json<serde_json::Value>, ApiError> {
    let db = Ukg::open(&st.data_root.join("server.db"))?;
    let mut stmt = db.conn().prepare(
        "SELECT \"ClientId\",\"Hostname\",\"FirstSeen\",\"LastSeen\" FROM \"PrecallClient\" ORDER BY \"LastSeen\" DESC",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(serde_json::json!({
                "client_id": r.get::<_, String>(0)?,
                "hostname": r.get::<_, String>(1)?,
                "first_seen_100ns": r.get::<_, i64>(2)?,
                "last_seen_100ns": r.get::<_, i64>(3)?,
            }))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(serde_json::json!({ "clients": rows })))
}

/// Full GDPR-style export for one client.
async fn export(
    State(st): State<ApiState>,
    Query(p): Query<SnapshotParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let db = client_db(&st, &p.client)?;
    let mut captures = Vec::new();
    let mut before = None;
    loop {
        let page = db.timeline(before, 500)?;
        if page.is_empty() {
            break;
        }
        before = Some(page.last().unwrap().timestamp_100ns);
        for e in page {
            let regs = db.regions(e.id)?;
            captures.push(serde_json::json!({
                "id": e.id,
                "name": e.name,
                "image_token": e.image_token,
                "window_title": e.window_title,
                "timestamp_100ns": e.timestamp_100ns,
                "activation_uri": e.activation_uri,
                "fallback_uri": e.fallback_uri,
                "apps": e.apps,
                "regions": regs.iter().map(|(rid,k,t,b)| serde_json::json!({
                    "id": rid, "kind": k, "text": t, "bounds": b
                })).collect::<Vec<_>>(),
            }));
        }
    }
    Ok(Json(serde_json::json!({
        "client": p.client,
        "captures": captures,
        "app_dwell": db.app_dwell()?,
        "web_dwell": db.web_dwell()?,
    })))
}

/// Panic button — delete all stored data for a client.
async fn wipe_client(
    State(st): State<ApiState>,
    AxPath(client): AxPath<String>,
) -> Result<StatusCode, ApiError> {
    let dir = client_dir(&st, &client)?;
    let objects = ObjectStore::encrypted(dir.join("objects"), st.object_key)?;
    objects.purge_client(&client)?;
    let db = client_db(&st, &client)?;
    db.purge_all()?;
    Ok(StatusCode::NO_CONTENT)
}

/// WS live stream: polls for new captures every 1.5s (cross-process safe —
/// the broker's in-proc broadcast doesn't cross the process boundary).
async fn live_ws(
    State(st): State<ApiState>,
    Query(p): Query<SnapshotParams>,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let _db = client_db(&st, &p.client)?; // validate client up front
    Ok(ws.on_upgrade(move |socket| live_loop(socket, st.data_root.clone(), p.client)))
}

async fn live_loop(mut socket: axum::extract::ws::WebSocket, data_root: PathBuf, client: String) {
    let db_path = data_root.join(&client).join("ukg.db");
    let mut last_id: i64 = 0;
    // Start at the newest id so we only stream *new* captures.
    if let Ok(db) = Ukg::open(&db_path) {
        if let Ok(rows) = db.timeline(None, 1) {
            last_id = rows.first().map(|e| e.id).unwrap_or(0);
        }
    }
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(1500));
    loop {
        interval.tick().await;
        let Ok(db) = Ukg::open(&db_path) else {
            continue;
        };
        let new_rows: Vec<serde_json::Value> = (|| -> Result<Vec<serde_json::Value>, ApiError> {
            let mut stmt = db.conn().prepare(
                "SELECT \"Id\",\"Name\",\"WindowTitle\",\"TimeStamp\",\"ImageToken\"
                 FROM \"WindowCapture\" WHERE \"Id\" > ? ORDER BY \"Id\" LIMIT 50",
            )?;
            let rows = stmt
                .query_map([last_id], |r| {
                    Ok(serde_json::json!({
                        "kind": "capture",
                        "window_capture_id": r.get::<_, i64>(0)?,
                        "name": r.get::<_, String>(1)?,
                        "window_title": r.get::<_, String>(2)?,
                        "timestamp_100ns": r.get::<_, i64>(3)?,
                        "image_token": r.get::<_, Option<String>>(4)?,
                    }))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })()
        .unwrap_or_default();
        for row in new_rows {
            if let Some(id) = row.get("window_capture_id").and_then(|v| v.as_i64()) {
                last_id = last_id.max(id);
            }
            if socket
                .send(axum::extract::ws::Message::Text(row.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let message = self.to_string();
        let status = StatusCode::from(self);
        (status, message).into_response()
    }
}
