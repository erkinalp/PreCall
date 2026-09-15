// SPDX-License-Identifier: GPL-2.0-only
//! `ukg.db` schema — identical table/column names to Windows Recall, plus
//! Precall's own sync/config/tenancy tables (all `Precall*`-prefixed so the
//! Recall-compatible surface stays pristine).

/// DDL executed at open time (idempotent — all IF NOT EXISTS).
pub const UKG_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS "WindowCapture" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Name" TEXT,
    "ImageToken" TEXT,
    "IsForeground" INTEGER NOT NULL DEFAULT 0,
    "WindowId" INTEGER NOT NULL DEFAULT 0,
    "WindowBounds" TEXT,
    "WindowTitle" TEXT,
    "Properties" TEXT,
    "TimeStamp" INTEGER NOT NULL,
    "IsProcessed" INTEGER NOT NULL DEFAULT 0,
    "ActivationUri" TEXT,
    "ActivityId" TEXT,
    "FallbackUri" TEXT
);
CREATE INDEX IF NOT EXISTS "IX_WindowCapture_TimeStamp" ON "WindowCapture"("TimeStamp");
CREATE UNIQUE INDEX IF NOT EXISTS "UX_WindowCapture_ImageToken" ON "WindowCapture"("ImageToken");

CREATE TABLE IF NOT EXISTS "App" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "WindowsAppId" TEXT,
    "IconUri" TEXT,
    "Name" TEXT,
    "Path" TEXT,
    "Properties" TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS "UX_App_Name_Path" ON "App"("Name", IFNULL("Path", ''));

CREATE TABLE IF NOT EXISTS "WindowCaptureAppRelation" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "WindowCaptureId" INTEGER NOT NULL,
    "AppId" INTEGER NOT NULL,
    FOREIGN KEY("WindowCaptureId") REFERENCES "WindowCapture"("Id") ON DELETE CASCADE,
    FOREIGN KEY("AppId") REFERENCES "App"("Id") ON DELETE CASCADE,
    UNIQUE("WindowCaptureId", "AppId")
);

CREATE TABLE IF NOT EXISTS "File" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Path" TEXT,
    "Name" TEXT,
    "Extension" TEXT,
    "Kind" TEXT,
    "Type" TEXT,
    "Properties" TEXT,
    "ObjectId" TEXT,
    "VolumeId" TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS "UX_File_Path" ON "File"("Path");

CREATE TABLE IF NOT EXISTS "WindowCaptureFileRelation" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "WindowCaptureId" INTEGER NOT NULL,
    "FileId" INTEGER NOT NULL,
    FOREIGN KEY("WindowCaptureId") REFERENCES "WindowCapture"("Id") ON DELETE CASCADE,
    FOREIGN KEY("FileId") REFERENCES "File"("Id") ON DELETE CASCADE,
    UNIQUE("WindowCaptureId", "FileId")
);

CREATE TABLE IF NOT EXISTS "Web" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Domain" TEXT,
    "Uri" TEXT,
    "IconUri" TEXT,
    "Properties" TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS "UX_Web_Uri" ON "Web"("Uri");

CREATE TABLE IF NOT EXISTS "WindowCaptureWebRelation" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "WindowCaptureId" INTEGER NOT NULL,
    "WebId" INTEGER NOT NULL,
    FOREIGN KEY("WindowCaptureId") REFERENCES "WindowCapture"("Id") ON DELETE CASCADE,
    FOREIGN KEY("WebId") REFERENCES "Web"("Id") ON DELETE CASCADE,
    UNIQUE("WindowCaptureId", "WebId")
);

CREATE TABLE IF NOT EXISTS "ScreenRegion" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "WindowCaptureId" INTEGER NOT NULL,
    "RegionKind" TEXT,
    "OcrText" TEXT,
    "Bounds" TEXT,
    FOREIGN KEY("WindowCaptureId") REFERENCES "WindowCapture"("Id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "IX_ScreenRegion_WindowCaptureId" ON "ScreenRegion"("WindowCaptureId");

CREATE TABLE IF NOT EXISTS "Topic" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Title" TEXT,
    "Properties" TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS "UX_Topic_Title" ON "Topic"("Title");

CREATE TABLE IF NOT EXISTS "WindowCaptureTopicRelation" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "WindowCaptureId" INTEGER NOT NULL,
    "TopicId" INTEGER NOT NULL,
    "Score" REAL NOT NULL DEFAULT 0,
    FOREIGN KEY("WindowCaptureId") REFERENCES "WindowCapture"("Id") ON DELETE CASCADE,
    FOREIGN KEY("TopicId") REFERENCES "Topic"("Id") ON DELETE CASCADE,
    UNIQUE("WindowCaptureId", "TopicId")
);

CREATE TABLE IF NOT EXISTS "AppDwellTime" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "WindowsAppId" TEXT,
    "HourOfDay" INTEGER NOT NULL,
    "DayOfWeek" INTEGER NOT NULL,
    "HourStartTimestamp" INTEGER NOT NULL,
    "DwellTime" INTEGER NOT NULL DEFAULT 0,
    UNIQUE("WindowsAppId", "HourStartTimestamp")
);

CREATE TABLE IF NOT EXISTS "WebDomainDwellTime" (
    "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Domain" TEXT,
    "HourOfDay" INTEGER NOT NULL,
    "DayOfWeek" INTEGER NOT NULL,
    "HourStartTimestamp" INTEGER NOT NULL,
    "DwellTime" INTEGER NOT NULL DEFAULT 0,
    UNIQUE("Domain", "HourStartTimestamp")
);

-- Full-text index across the human-readable capture fields (Recall ships the
-- same FTS5 surface). Contentless: we manage rows explicitly.
CREATE VIRTUAL TABLE IF NOT EXISTS "WindowCaptureTextIndex" USING fts5(
    "Name", "WindowTitle", "OcrText",
    tokenize = 'unicode61'
);

-- ---------------------------------------------------------------------------
-- Precall extensions (never present in real Recall databases)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "PrecallSyncState" (
    "WindowCaptureId" INTEGER PRIMARY KEY,
    "SyncedToServer" INTEGER NOT NULL DEFAULT 0,
    "SyncTimestamp" INTEGER,
    "ServerAckId" TEXT,
    FOREIGN KEY("WindowCaptureId") REFERENCES "WindowCapture"("Id") ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "PrecallConfig" (
    "Key" TEXT PRIMARY KEY,
    "Value" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "PrecallClient" (
    "ClientId" TEXT PRIMARY KEY,
    "Hostname" TEXT NOT NULL DEFAULT '',
    "FirstSeen" INTEGER NOT NULL,
    "LastSeen" INTEGER NOT NULL
);
"#;

/// `si_*` schema shared by `SemanticTextStore.db` and `SemanticImageStore.db`.
pub const SI_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS "si_items" (
    "id" BLOB(16) PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS "si_diskann_graph" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "embedding" BLOB NOT NULL,
    "outbound_ids" BLOB NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS "si_diskann_info" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "graph_table_name" TEXT NOT NULL,
    "dimension" INTEGER NOT NULL,
    "vector_space_id" TEXT
);
CREATE TABLE IF NOT EXISTS "si_embedding_metadata" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "embedding_id" INTEGER NOT NULL,
    "item_id" BLOB NOT NULL,
    "region_id" TEXT,
    "metadata_json" TEXT,
    FOREIGN KEY("embedding_id") REFERENCES "si_diskann_graph"("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "IX_si_embedding_metadata_item" ON "si_embedding_metadata"("item_id");
CREATE TABLE IF NOT EXISTS "si_diskann_config" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "graph_table_name" TEXT NOT NULL,
    "max_degree" INTEGER NOT NULL DEFAULT 32,
    "alpha" REAL NOT NULL DEFAULT 1.2
);
"#;
