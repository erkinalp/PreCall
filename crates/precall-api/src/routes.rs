// SPDX-License-Identifier: GPL-2.0-only
//! Route handlers. All DB access is per-request (`rusqlite::Connection` is
//! `Send` but not `Sync`; WAL makes concurrent readers cheap).

use crate::auth::{require_auth, ApiAuth};
use crate::embed::{EmbedOutcome, Embedder};
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
    /// Natural-language query. May be empty only when `image` is supplied.
    #[serde(default)]
    query: String,
    #[serde(default)]
    time_range: Option<TimeRange>,
    #[serde(default)]
    app_filter: Vec<String>,
    #[serde(default)]
    limit: Option<i64>,
    /// "hybrid" (default) | "fts" | "semantic" | "image"
    #[serde(default)]
    mode: Option<String>,
    /// Base64 JPEG — or a `data:image/jpeg;base64,…` URL — to search the
    /// **image** space with: "captures that look like this".
    #[serde(default)]
    image: Option<String>,
}

#[derive(Deserialize)]
struct TimeRange {
    start: Option<i64>,
    end: Option<i64>,
}

/// Decode an image query: plain base64 or a data URL.
fn decode_image_query(s: &str) -> Result<Vec<u8>, ApiError> {
    use base64::Engine as _;
    let s = s.trim();
    let b64 = if let Some(rest) = s.strip_prefix("data:") {
        rest.split_once("base64,")
            .map(|(_, b)| b)
            .ok_or_else(|| ApiError::BadRequest("data URL must be base64-encoded".into()))?
    } else {
        s
    };
    base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| ApiError::BadRequest(format!("image is not valid base64: {e}")))
}

/// Reciprocal-rank-fusion constant (Cormack et al., 2009).
///
/// Legs are fused by **rank**, never by raw score: SQLite `bm25` values and
/// cosine similarities are different scales, and mixing them made an unrelated
/// semantic candidate outrank an exact lexical match in small corpora where the
/// BM25 magnitude collapses to ~1e-6.
const RRF_K: f64 = 60.0;

/// Whether a store's vectors were produced by the embedder now being queried.
enum SpaceVerdict {
    Ok,
    /// The store was built in a different space (previous label carried).
    Mismatch(String),
}

/// Compare the store's stamped space against the query embedding's space.
///
/// Same dimension is not enough: a hash embedding and a CLIP embedding are
/// incomparable, so a store built before the AI service was configured must be
/// reported, not silently ranked against CLIP queries.
fn index_space_verdict(
    idx: &SemanticIndex,
    query_space: &str,
) -> Result<SpaceVerdict, ApiError> {
    match idx.space_id()? {
        Some(stored)
            if stored != query_space && stored != precall_store::SPACE_UNKNOWN =>
        {
            Ok(SpaceVerdict::Mismatch(stored))
        }
        _ => Ok(SpaceVerdict::Ok),
    }
}

/// How much deeper each leg's candidate window is than the final result count.
///
/// Rank fusion needs the union of the legs' windows: a capture ranked just
/// outside `limit` by one leg but inside it by another must still contribute
/// both ranks, and with `limit = 20` an FTS rank of 21 combined with a semantic
/// rank of 21 legitimately beats a rank-1 single-leg hit. Truncating each leg to
/// `limit` before fusion discards that.
const FUSION_POOL_FACTOR: i64 = 4;

/// Candidate window per leg before fusion (the final truncation still applies).
fn fusion_pool(limit: i64) -> i64 {
    (limit * FUSION_POOL_FACTOR).clamp(limit, 500)
}

/// A semantic candidate with no positive similarity is not evidence of
/// similarity: the flat cosine scan returns *every* stored item, so the
/// zero/negative tail is the whole store, not a match.
const MIN_SEMANTIC_SCORE: f32 = 0.0;

/// Resolve a semantic leg's hits into ranked capture ids plus their metadata.
///
/// The candidate list is already filtered (the scan takes an allow-list), so
/// this only drops non-evidence: the flat cosine scan returns *every* stored
/// item, and a zero/negative similarity is the absence of a match rather than a
/// weak one.
fn semantic_candidates(
    db: &Ukg,
    semantic: &[precall_store::EmbeddingHit],
) -> Result<(Vec<i64>, HashMap<i64, precall_store::SearchHit>), ApiError> {
    let mut ids: Vec<i64> = Vec::new();
    for h in semantic {
        if h.score <= MIN_SEMANTIC_SCORE {
            continue;
        }
        if let Some(id) = SemanticIndex::capture_id_for_item(&h.item_id) {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
    }
    let meta = db.captures_by_ids(&ids)?;
    Ok((ids, meta))
}

/// Merge a semantic leg's survivors into the result set and record the leg.
fn absorb_semantic_leg(
    hits: &mut HashMap<i64, precall_store::SearchHit>,
    leg_ranks: &mut Vec<Vec<i64>>,
    legs: &mut Vec<&'static str>,
    leg: &'static str,
    db: &Ukg,
    found: &[precall_store::EmbeddingHit],
) -> Result<(), ApiError> {
    let (ids, meta) = semantic_candidates(db, found)?;
    // Only survivors may enter the result set: a candidate the filters
    // excluded must not come back with a zero score.
    for id in &ids {
        if let Some(h) = meta.get(id) {
            hits.entry(*id).or_insert_with(|| h.clone());
        }
    }
    leg_ranks.push(ids);
    legs.push(leg);
    Ok(())
}

/// Reciprocal rank fusion over one id list per leg that ran.
///
/// Ranks are the only input, so a leg whose scores are on a wildly different
/// scale cannot swamp another: a capture found by two legs accumulates both
/// contributions and outranks one found by a single leg at the same rank.
fn reciprocal_rank_fusion(leg_ranks: &[Vec<i64>]) -> HashMap<i64, f64> {
    let mut fused: HashMap<i64, f64> = HashMap::new();
    for ids in leg_ranks {
        for (rank, id) in ids.iter().enumerate() {
            *fused.entry(*id).or_insert(0.0) += 1.0 / (RRF_K + rank as f64 + 1.0);
        }
    }
    fused
}

async fn search(
    State(st): State<ApiState>,
    Json(body): Json<SearchBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mode = body.mode.as_deref().unwrap_or("hybrid");
    let has_query = !body.query.trim().is_empty();
    let image_b64 = body
        .image
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if !has_query && image_b64.is_none() {
        return Err(ApiError::BadRequest(
            "query must not be empty (or supply an image)".into(),
        ));
    }
    match mode {
        "hybrid" | "fts" | "semantic" | "image" => {}
        other => return Err(ApiError::BadRequest(format!("unknown mode {other:?}"))),
    }
    if mode == "image" && image_b64.is_none() {
        return Err(ApiError::BadRequest("mode=image requires an image query".into()));
    }
    if mode == "fts" && image_b64.is_some() {
        return Err(ApiError::BadRequest(
            "an image query needs mode hybrid, semantic or image".into(),
        ));
    }

    let db = client_db(&st, &body.client)?;
    let limit = body.limit.unwrap_or(20).clamp(1, 200);
    let pool = fusion_pool(limit);

    let (start, end) = body
        .time_range
        .as_ref()
        .map(|t| (t.start, t.end))
        .unwrap_or((None, None));

    let mut hits: HashMap<i64, precall_store::SearchHit> = HashMap::new();
    // Each leg's capture ids in rank order. Fusion uses only these ranks.
    let mut leg_ranks: Vec<Vec<i64>> = Vec::new();
    // Captures the lexical leg found — used to break fused-score ties.
    let mut fts_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut legs: Vec<&'static str> = Vec::new();
    let mut notes: Vec<String> = Vec::new();

    // FTS5 leg.
    if (mode == "hybrid" || mode == "fts") && has_query {
        let opts = precall_store::SearchOptions {
            query: fts_escape(&body.query),
            start_100ns: start,
            end_100ns: end,
            app_filter: body.app_filter.clone(),
            limit: Some(pool),
        };
        let mut ids = Vec::new();
        for h in db.search_fts(&opts)? {
            ids.push(h.window_capture_id);
            fts_ids.insert(h.window_capture_id);
            hits.insert(h.window_capture_id, h);
        }
        leg_ranks.push(ids);
        legs.push("fts");
    }

    let dir = client_dir(&st, &body.client)?;

    // Text query → text space.
    if (mode == "hybrid" || mode == "semantic") && has_query {
        match st.embedder.embed_text(&body.query).await {
            EmbedOutcome::Ok { embedding: qemb, space } => {
                let idx = SemanticIndex::open(
                    &dir.join("SemanticTextStore.db"),
                    st.embedding_dim,
                    precall_store::SPACE_UNKNOWN,
                )?;
                match index_space_verdict(&idx, &space)? {
                    SpaceVerdict::Ok => {
                        let allowed = db.capture_ids_matching_filters(start, end, &body.app_filter)?;
                        if qemb.len() != st.embedding_dim {
                            notes.push(format!(
                                "text-semantic leg skipped: the embedder returned {} values, the store holds {}",
                                qemb.len(), st.embedding_dim
                            ));
                        } else {
                            let found = idx.search(&qemb, pool as usize, allowed.as_ref())?;
                            absorb_semantic_leg(
                                &mut hits, &mut leg_ranks, &mut legs, "text-semantic", &db, &found,
                            )?;
                        }
                    }
                    SpaceVerdict::Mismatch(stored) => notes.push(format!(
                        "text-semantic leg skipped: SemanticTextStore.db was built with {stored}, \
                         not {space}; the enricher will rebuild it"
                    )),
                }
            }
            EmbedOutcome::Disabled => {
                notes.push("text-semantic leg skipped: no embedder configured".into())
            }
            EmbedOutcome::Rejected | EmbedOutcome::Retry => notes.push(
                "text-semantic leg skipped: the AI service did not answer (FTS results only)".into(),
            ),
        }
    }

    // Supplied image → image space.
    if let Some(b64) = image_b64 {
        let raw = decode_image_query(b64)?;
        if raw.is_empty() {
            return Err(ApiError::BadRequest("image query is empty".into()));
        }
        match st.embedder.embed_image(&raw).await {
            EmbedOutcome::Ok { embedding: emb, space } => {
                let idx = SemanticIndex::open(
                    &dir.join("SemanticImageStore.db"),
                    st.embedding_dim,
                    precall_store::SPACE_UNKNOWN,
                )?;
                match index_space_verdict(&idx, &space)? {
                    SpaceVerdict::Ok => {
                        if emb.len() != st.embedding_dim {
                            return Err(ApiError::Unavailable(format!(
                                "the AI service returned {} values for this image, expected {}",
                                emb.len(),
                                st.embedding_dim
                            )));
                        }
                        let allowed = db.capture_ids_matching_filters(start, end, &body.app_filter)?;
                        let found = idx.search(&emb, pool as usize, allowed.as_ref())?;
                        absorb_semantic_leg(
                            &mut hits, &mut leg_ranks, &mut legs, "image-semantic", &db, &found,
                        )?;
                    }
                    SpaceVerdict::Mismatch(stored) => notes.push(format!(
                        "image-semantic leg skipped: SemanticImageStore.db was built with {stored}, \
                         not {space}; the enricher will rebuild it"
                    )),
                }
            }
            EmbedOutcome::Disabled => {
                return Err(ApiError::BadRequest(
                    "image embedding unavailable (no embedder configured)".into(),
                ))
            }
            EmbedOutcome::Rejected => {
                return Err(ApiError::BadRequest(
                    "the AI service refused this image (undecodable frame?)".into(),
                ))
            }
            EmbedOutcome::Retry => {
                return Err(ApiError::Unavailable(
                    "the AI service is not answering; image search needs it".into(),
                ))
            }
        }
    }

    // Text query → image space (CLIP text encoder). Refused by the AI service
    // when its image backend is the hash fallback, because hashed text and
    // hashed pixels share no space — that leg is reported as skipped, never as
    // "no matches".
    if (mode == "hybrid" || mode == "semantic") && has_query {
        match st.embedder.embed_image_text(&body.query).await {
            EmbedOutcome::Ok { embedding: emb, space } => {
                let idx = SemanticIndex::open(
                    &dir.join("SemanticImageStore.db"),
                    st.embedding_dim,
                    precall_store::SPACE_UNKNOWN,
                )?;
                match index_space_verdict(&idx, &space)? {
                    SpaceVerdict::Ok => {
                        if emb.len() != st.embedding_dim {
                            notes.push(format!(
                                "text→image leg skipped: the service returned {} values, the store holds {}",
                                emb.len(), st.embedding_dim
                            ));
                        } else {
                            let allowed =
                                db.capture_ids_matching_filters(start, end, &body.app_filter)?;
                            let found = idx.search(&emb, pool as usize, allowed.as_ref())?;
                            absorb_semantic_leg(
                                &mut hits, &mut leg_ranks, &mut legs, "image-text", &db, &found,
                            )?;
                        }
                    }
                    SpaceVerdict::Mismatch(stored) => notes.push(format!(
                        "text→image leg skipped: SemanticImageStore.db was built with {stored}, \
                         not {space}; the enricher will rebuild it"
                    )),
                }
            }
            EmbedOutcome::Disabled => notes.push(
                "text→image leg skipped: needs a CLIP-backed AI service (/embed/image-text)".into(),
            ),
            EmbedOutcome::Rejected | EmbedOutcome::Retry => notes.push(
                "text→image leg skipped: the AI service did not answer (/embed/image-text)".into(),
            ),
        }
    }

    // Reciprocal rank fusion across the legs that ran.
    let fused = reciprocal_rank_fusion(&leg_ranks);
    for (id, h) in hits.iter_mut() {
        h.score = fused.get(id).copied().unwrap_or(0.0);
    }

    // Anything no leg ranked contributes nothing; it must not appear as a
    // zero-score "match".
    hits.retain(|id, _| fused.contains_key(id));

    let mut results: Vec<_> = hits.into_values().collect();
    // Relevance first, recency only as a tie-break. The old route sorted by
    // recency first, which buried an exact match under whatever capture
    // happened to be newest and left `relevance_score` decorative.
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            // Rank fusion cannot separate two rank-1 hits from different legs,
            // and a vector hit is ranked against *every* stored item, so an
            // exact lexical match must not lose that tie to a recent
            // similarity score. Lexical first, then recency.
            .then_with(|| {
                match (
                    fts_ids.contains(&b.window_capture_id),
                    fts_ids.contains(&a.window_capture_id),
                ) {
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    _ => std::cmp::Ordering::Equal,
                }
            })
            .then(b.timestamp_100ns.cmp(&a.timestamp_100ns))
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

    let mut body_out = serde_json::json!({ "results": out, "legs": legs });
    if !notes.is_empty() {
        body_out["notes"] = serde_json::json!(notes);
    }
    Ok(Json(body_out))
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
    for ext in ["jpg", "raw"] {
        let rel = ObjectStore::key_path(&p.client, ymd_from_filetime(ts), &token, ext);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A capture both legs found must outrank one only a single leg found,
    /// whatever the legs' raw scores looked like — that is the whole point of
    /// fusing ranks instead of scores (SQLite `bm25` vs. cosine).
    #[test]
    fn rrf_rewards_agreement_between_legs() {
        let both = reciprocal_rank_fusion(&[vec![1], vec![1]]);
        let one = reciprocal_rank_fusion(&[vec![1]]);
        assert!(both[&1] > one[&1]);

        let fused = reciprocal_rank_fusion(&[vec![1, 2], vec![2, 3]]);
        assert!(fused[&2] > fused[&1], "agreement must win");
        assert!(fused[&2] > fused[&3], "agreement must win");
    }

    /// Each leg must offer more than the final result count, or fusion cannot
    /// see the candidates it is supposed to combine.
    #[test]
    fn each_leg_offers_a_deeper_window_than_the_final_count() {
        assert!(fusion_pool(1) > 1);
        assert_eq!(fusion_pool(20), 80);
        assert_eq!(fusion_pool(400), 500, "clamped like the FTS limit");
    }

    #[test]
    fn rrf_preserves_single_leg_order() {
        let fused = reciprocal_rank_fusion(&[vec![7, 8, 9]]);
        assert!(fused[&7] > fused[&8] && fused[&8] > fused[&9]);
    }

    /// The fusion function takes only ranks, so an exact lexical match cannot
    /// be pushed down by a semantic leg whose magnitudes happen to be larger:
    /// rank 1 of any leg carries identical weight.
    #[test]
    fn rrf_is_scale_free() {
        let fused = reciprocal_rank_fusion(&[vec![1], vec![2, 3, 4, 5]]);
        assert_eq!(fused[&1], fused[&2]);
        assert!(fused[&2] > fused[&3] && fused[&3] > fused[&4] && fused[&4] > fused[&5]);
    }
}
