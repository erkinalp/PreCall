// SPDX-License-Identifier: GPL-2.0-only
//! Metadata records mirroring Windows Recall's `ukg.db` schema.
//!
//! Serialized as JSON on the `PCMETA` channel. Field names and semantics match
//! the Recall column names so `precall-store` can persist them without a
//! translation layer.

use serde::{Deserialize, Serialize};

/// Row shape of `WindowCapture` — one captured desktop moment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowCaptureRecord {
    /// Display name (usually foreground window title).
    pub name: String,
    /// Token the server uses as the object-store key for the screenshot
    /// (matches Recall's `ImageToken`; client-generated, unique per image).
    pub image_token: String,
    pub is_foreground: bool,
    /// HWND-equivalent opaque id.
    pub window_id: i64,
    /// "left,top,right,bottom" in desktop coordinates.
    pub window_bounds: String,
    pub window_title: String,
    /// Recall's opaque `Properties` bag — preserved verbatim when migrating.
    #[serde(default)]
    pub properties: Option<String>,
    /// Capture time, 100ns ticks since 1601-01-01 (FILETIME).
    pub timestamp_100ns: u64,
    /// Deep link that re-opens the captured app state (UserActivity URI).
    #[serde(default)]
    pub activation_uri: Option<String>,
    #[serde(default)]
    pub activity_id: Option<String>,
    #[serde(default)]
    pub fallback_uri: Option<String>,
    /// Per-frame associations (0..n of each).
    #[serde(default)]
    pub apps: Vec<AppRecord>,
    #[serde(default)]
    pub files: Vec<FileRecord>,
    #[serde(default)]
    pub webs: Vec<WebRecord>,
    #[serde(default)]
    pub regions: Vec<ScreenRegionRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRecord {
    #[serde(default)]
    pub windows_app_id: Option<String>,
    #[serde(default)]
    pub icon_uri: Option<String>,
    pub name: String,
    /// Executable path or package identity.
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub properties: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecord {
    pub path: String,
    pub name: String,
    #[serde(default)]
    pub extension: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub properties: Option<String>,
    #[serde(default)]
    pub object_id: Option<String>,
    #[serde(default)]
    pub volume_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebRecord {
    pub domain: String,
    pub uri: String,
    #[serde(default)]
    pub icon_uri: Option<String>,
    #[serde(default)]
    pub properties: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScreenRegionRecord {
    /// e.g. "text_block", "image", "interactive".
    pub region_kind: String,
    #[serde(default)]
    pub ocr_text: Option<String>,
    /// "left,top,right,bottom".
    pub bounds: String,
}

/// One PCMETA payload: an ordered batch of captures from one client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureBatch {
    /// Client identity (checked against the authenticated session).
    pub client_id: uuid::Uuid,
    /// Monotonic per-connection sequence for gap detection.
    pub seq: u64,
    pub captures: Vec<WindowCaptureRecord>,
}

/// PCCTRL control messages (bidirectional).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ControlMessage {
    /// Server → client: stop streaming until resumed.
    Pause,
    /// Server → client: resume, optionally with a new cadence.
    Resume { capture_interval_ms: Option<u32> },
    /// Server → client: replace the exclusion list.
    SetExclusions { apps: Vec<String>, domains: Vec<String> },
    /// Client → server: liveness + capture stats.
    Heartbeat {
        frames_sent: u64,
        frames_skipped_unchanged: u64,
        queue_depth: u32,
    },
    /// Server → client: wipe all server-side data for this client and
    /// confirm destruction.
    PanicWipe,
    /// Either side: graceful close.
    Goodbye { reason: String },
}

/// PCQUERY push (server → client): a query result for local UI display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPush {
    pub query: String,
    /// WindowCapture ids ordered by relevance.
    pub window_capture_ids: Vec<i64>,
}
