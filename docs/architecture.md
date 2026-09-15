# Precall: Architecture Design Document

## 1. Executive Summary

**Precall** is a Windows Recall clone for Windows 11 architected as a client-server system. Unlike the real Windows Recall (which stores and processes everything locally on the user's PC), Precall implements the architecture that privacy-concerned users *suspected* Recall used: a **reverse RDP connection** from the client workstation to a configurable remote server. The client captures desktop activity and streams it to the server, which performs AI-powered indexing, semantic search, and timeline replay.

The design maximizes reuse of existing Windows Terminal Services infrastructure and achieves drop-in compatibility with Windows Recall's frontend (database schema, API surface, and UI integration points).

### Classification

**MultiDevin / Scott-Walden Class** — This is a multi-component system requiring deep integration with Windows internals (DXGI, RDP virtual channels, VBS enclaves) and novel protocol design (reverse RDP capture).

---

## 2. Background & Prior Art

### 2.1 Windows Recall (Production)

Windows Recall is a Copilot+ PC feature that periodically captures screenshots, performs on-device OCR and screen segmentation via the NPU, and stores results in three SQLite databases:

| Database | Purpose |
|---|---|
| `ukg.db` | Metadata: WindowCapture, App, File, Web, ScreenRegion, Topic tables + FTS5 full-text index |
| `SemanticTextStore.db` | DiskANN graph-based vector index for text embeddings |
| `SemanticImageStore.db` | DiskANN graph-based vector index for image embeddings |

Screenshots are stored as JPEG files referenced by `ImageToken` in `WindowCapture`. All data is encrypted at rest using keys sealed in a VBS (Virtualization-Based Security) enclave and unlocked via Windows Hello biometric proof-of-presence.

**Key APIs used by Recall:**
- `Windows.Graphics.Capture` / DXGI Desktop Duplication — screen capture
- `SetWindowDisplayAffinity(WDA_MONITOR)` — DRM/privacy exclusion
- `UserActivity` API — deep-link relaunching of captured content
- `Microsoft.Windows.AI.Recall` — internal WinRT namespace for query/timeline
- FTS5 + DiskANN — full-text and semantic vector search

**References:**
- Subramanya, S. J., et al. "DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single Node." *NeurIPS 2019*. (The vector search engine used in Recall's SemanticTextStore/SemanticImageStore.)
- Singh, A., et al. "FreshDiskANN: A Fast and Accurate Graph-Based ANN Index for Streaming Similarity Search." *arXiv:2105.09613, 2021*.
- Krishnaswamy, R., Manohar, M., Simhadri, H. "The DiskANN Library: Graph-Based Indices for Fast, Fresh and Filtered Vector Search." *IEEE Data Eng. Bull.* 48, pp. 20-42, Dec 2024.

### 2.2 Terminal Services / RDP Architecture

The Remote Desktop Protocol (MS-RDPBCGR) provides the transport and rendering infrastructure we build upon:

| Component | Specification | Role in Precall |
|---|---|---|
| Connection Sequence | MS-RDPBCGR §1.3.1.1 | Reverse connection initiation (client→server) |
| Static Virtual Channels | MS-RDPBCGR §1.3.3 | Structured data transport (up to 31 channels) |
| Dynamic Virtual Channels | MS-RDPEDYC | On-demand channels for capture metadata |
| Graphics Pipeline | MS-RDPEGFX | Efficient frame encoding (RFX Progressive, AVC) |
| Shadow Sessions | MS-RDPBCGR §1.3.9 | Conceptual model for "observing" a desktop |
| Server Redirection | MS-RDPBCGR §1.3.8 | Load balancing across capture servers |

**Key insight:** In standard RDP, the *server* hosts the desktop session and the *client* renders it remotely. In Precall, we **reverse** this: the *client* hosts the desktop (the user's workstation) and initiates a connection to the *server* to stream its display — conceptually a "reverse shadow session."

**References:**
- Microsoft. "[MS-RDPBCGR]: Remote Desktop Protocol: Basic Connectivity and Graphics Remoting." Open Specifications, rev. 75.0, Apr 2025.
- Microsoft. "[MS-RDPEGFX]: Remote Desktop Protocol: Graphics Pipeline Extension." Open Specifications, rev. 18.1, Aug 2025.
- Microsoft. "[MS-RDPEDYC]: Remote Desktop Protocol: Dynamic Channel Virtual Channel Extension." Open Specifications.

### 2.3 systemd-recalld (Linux Predecessor)

The user's existing [systemd-recalld](https://github.com/erkinalp/recalld) project implements this "reverse capture" concept for Linux using:
- Reverse RTP for audio streaming
- Reverse VNC for desktop video streaming
- HTTPS/RCLD for bulk data transfer
- SQLite + AES-256-GCM for local encrypted storage
- Server-side AI query (Whisper, CLIP, BERT, Flamingo)

Precall is the Windows counterpart, replacing Linux-specific capture (X11/Wayland, ALSA) with Windows equivalents (DXGI, WASAPI) and replacing VNC/RTP with RDP-based transport.

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    CLIENT (Windows 11 Workstation)                  │
│                                                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────────┐  │
│  │ Screen Capture   │  │ Audio Capture    │  │ Metadata Extractor │  │
│  │ (DXGI Desktop    │  │ (WASAPI          │  │ (Foreground window │  │
│  │  Duplication)    │  │  Loopback)       │  │  title, app path,  │  │
│  └────────┬────────┘  └────────┬─────────┘  │  URL, file path)   │  │
│           │                    │             └─────────┬──────────┘  │
│           │                    │                       │             │
│           └────────────┬───────┴───────────────────────┘             │
│                        ▼                                             │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │                   Capture Pipeline                           │    │
│  │                                                              │    │
│  │  Frame Diff → JPEG/H.264 Encode → Privacy Filter →          │    │
│  │  Local OCR (Windows.AI.MachineLearning / Foundry) →          │    │
│  │  Metadata Assembly → Encryption (DPAPI-NG / VBS Enclave)     │    │
│  └──────────────────────┬───────────────────────────────────────┘    │
│                         │                                            │
│                         ▼                                            │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │           Reverse RDP Transport Layer                        │    │
│  │                                                              │    │
│  │  ┌─────────────┐ ┌──────────────┐ ┌───────────────────┐     │    │
│  │  │ RDPGFX      │ │ Precall      │ │ Precall           │     │    │
│  │  │ Channel     │ │ Metadata     │ │ Control           │     │    │
│  │  │ (Frames)    │ │ Channel      │ │ Channel           │     │    │
│  │  │ SVC: GFXRDP │ │ DVC: PCMETA  │ │ DVC: PCCTRL       │     │    │
│  │  └─────────────┘ └──────────────┘ └───────────────────┘     │    │
│  └──────────────────────┬───────────────────────────────────────┘    │
│                         │ TLS 1.3                                    │
│  ┌──────────────────────┴───────────────────────────────────────┐    │
│  │ Local Cache (ukg.db compatible)                              │    │
│  │ Offline buffer when server unreachable                       │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ Precall Service (NT Service)         precallctl.exe (CLI)    │    │
│  │ precall-svc.exe                      Precall Settings (GUI)  │    │
│  └──────────────────────────────────────────────────────────────┘    │
└─────────────────────────────┬────────────────────────────────────────┘
                              │ Reverse RDP (Client-initiated,
                              │ TLS 1.3 + NLA/CredSSP)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SERVER (Configurable Remote Host)                 │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ Connection Broker                                            │   │
│  │ (Reverse RDP Listener + TLS Termination + Auth)              │   │
│  └──────────────────────┬───────────────────────────────────────┘   │
│                         │                                           │
│           ┌─────────────┼──────────────┐                            │
│           ▼             ▼              ▼                             │
│  ┌──────────────┐ ┌──────────┐ ┌────────────────┐                  │
│  │ Frame Store  │ │ Metadata │ │ Audio Store     │                  │
│  │ (JPEG/H.264  │ │ Ingester │ │ (Opus segments  │                  │
│  │  → Object    │ │ → ukg.db │ │  → Object       │                  │
│  │  Storage)    │ │          │ │  Storage)        │                  │
│  └──────┬───────┘ └────┬─────┘ └────────┬────────┘                  │
│         │              │                │                            │
│         └──────────────┼────────────────┘                            │
│                        ▼                                             │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                  AI Processing Pipeline                      │   │
│  │                                                              │   │
│  │  OCR Verification → Screen Segmentation → Embedding Gen →    │   │
│  │  DiskANN Index (SemanticTextStore + SemanticImageStore) →    │   │
│  │  Topic Extraction → Activity Correlation                     │   │
│  └──────────────────────┬───────────────────────────────────────┘   │
│                         │                                           │
│  ┌──────────────────────┴───────────────────────────────────────┐   │
│  │ Query API (REST + gRPC)                                      │   │
│  │                                                              │   │
│  │ GET  /api/v1/timeline       → Paginated timeline             │   │
│  │ POST /api/v1/search         → Semantic + FTS5 search         │   │
│  │ GET  /api/v1/snapshot/{id}  → Screenshot + metadata          │   │
│  │ POST /api/v1/relaunch       → UserActivity deep link         │   │
│  │ GET  /api/v1/apps           → App dwell time analytics       │   │
│  │ GET  /api/v1/web            → Web domain analytics           │   │
│  │ WS   /api/v1/live           → Real-time capture stream       │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ Recall-Compatible Frontend (Web UI)                          │   │
│  │ Timeline view, semantic search, Click-to-Do actions          │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Design

### 4.1 Client: Screen Capture Engine

**Technology:** DXGI Desktop Duplication API (`IDXGIOutputDuplication`)

This is the same underlying API that Remote Desktop Services uses for screen capture. It provides:
- GPU-accelerated frame acquisition (BGRA8 surfaces)
- Dirty region tracking (`GetFrameDirtyRects`) — only changed regions are captured
- Move region tracking (`GetFrameMoveRects`) — efficient handling of window moves/scrolls
- Pointer shape updates (`GetFramePointerShape`)
- Multi-monitor support via per-output duplication

**Capture cadence:** Configurable, default every 5 seconds (matching Recall's default). Adaptive: if no dirty regions, skip the frame.

**Frame encoding:**
1. **Snapshot mode** (default): JPEG compression (quality 85) for individual screenshots — compatible with Recall's `ImageToken` storage
2. **Stream mode** (optional): H.264/HEVC via Media Foundation for continuous video — compatible with RDPGFX AVC encoding

**Privacy filtering:** Before capture, enumerate windows via `EnumWindows` and check:
- `SetWindowDisplayAffinity(WDA_MONITOR)` — skip DRM-protected windows
- Application exclusion list (configurable, stored in registry)
- InPrivate/Incognito browser detection via accessibility APIs

```
Capture Loop (5s default):
  1. AcquireNextFrame() → DXGI surface
  2. Check dirty regions → skip if unchanged
  3. Apply privacy filter (mask excluded windows)
  4. Encode frame (JPEG snapshot or H.264 keyframe)
  5. Extract metadata (foreground window, title, app, URL)
  6. Run local OCR if NPU available (Windows.AI.MachineLearning)
  7. Encrypt payload (DPAPI-NG or VBS enclave key)
  8. Queue for transmission + local cache write
```

### 4.2 Client: Audio Capture Engine

**Technology:** WASAPI Loopback Capture

Captures system audio output (what the user hears) plus optional microphone input:
- Opus encoding at 48kHz for efficient streaming
- Voice Activity Detection (VAD) to skip silence
- Configurable: system audio only, mic only, or both

### 4.3 Client: Metadata Extractor

Extracts structured metadata matching Windows Recall's `ukg.db` schema:

| Recall Table | Precall Source | Method |
|---|---|---|
| `WindowCapture` | Foreground window | `GetForegroundWindow()` + `GetWindowText()` |
| `App` | Running process | `QueryFullProcessImageName()`, `SHGetFileInfo()` |
| `File` | Active document | Shell `IFileIsInUse`, recent file MRU |
| `Web` | Browser URL | UI Automation (`IUIAutomationElement`) on browser address bar |
| `ScreenRegion` | OCR results | Windows.AI.MachineLearning OCR or Tesseract fallback |
| `Topic` | Content classification | Server-side AI (BERT/GPT topic extraction) |
| `AppDwellTime` | Focus tracking | `SetWinEventHook(EVENT_SYSTEM_FOREGROUND)` |
| `WebDomainDwellTime` | URL tracking | Accumulated from `Web` + focus events |

### 4.4 Transport: Reverse RDP Protocol

The core innovation: we reuse the RDP protocol stack but in reverse — the client (workstation) initiates a connection to the server and streams its own desktop, rather than the server hosting a session.

#### 4.4.1 Connection Sequence (Modified MS-RDPBCGR)

```
Client (Workstation)                    Server (Capture Server)
        │                                        │
        │──── X.224 Connection Request ──────────>│  Phase 1: Initiation
        │<─── X.224 Connection Confirm ───────────│  (Client connects TO server)
        │                                        │
        │──── MCS Connect Initial ───────────────>│  Phase 2: Basic Settings
        │     (Client capabilities:               │  (Client advertises its
        │      screen resolution,                 │   display properties)
        │      color depth,                       │
        │      capture channels)                  │
        │<─── MCS Connect Response ──────────────│
        │                                        │
        │──── NLA/CredSSP Authentication ────────>│  Phase 3: Auth
        │<─── Authentication Result ──────────────│  (Kerberos, NTLM, or
        │                                        │   certificate-based)
        │                                        │
        │──── Channel Join (GFXRDP, PCMETA, ─────>│  Phase 4: Channel Setup
        │     PCCTRL, PCAUDIO)                    │
        │<─── Channel Join Confirm ───────────────│
        │                                        │
        │════ Frame Data (RDPGFX PDUs) ══════════>│  Phase 5: Streaming
        │════ Metadata (PCMETA DVC) ═════════════>│  (Continuous)
        │════ Audio (PCAUDIO DVC) ════════════════>│
        │<════ Control Commands (PCCTRL) ═════════│
        │                                        │
```

**Key difference from standard RDP:** In standard RDP, graphics flow server→client. In Precall, graphics flow client→server (the client is the "display source").

#### 4.4.2 Virtual Channels

| Channel | Type | Direction | Purpose |
|---|---|---|---|
| `GFXRDP` | Static (SVC) | Client→Server | Frame data using RDPGFX wire format |
| `PCMETA` | Dynamic (DVC) | Client→Server | Structured metadata (JSON-serialized WindowCapture records) |
| `PCAUDIO` | Dynamic (DVC) | Client→Server | Opus audio stream |
| `PCCTRL` | Dynamic (DVC) | Bidirectional | Control: pause/resume, config changes, heartbeat |
| `PCQUERY` | Dynamic (DVC) | Server→Client | Query results pushed to client for local UI |

#### 4.4.3 Frame Encoding (RDPGFX-compatible)

Reuse the RDPGFX wire format (MS-RDPEGFX §2.2) for frame transport:

```
RDPGFX_START_FRAME_PDU
  ├── frameId: monotonic counter
  └── timestamp: capture time (100ns ticks)

RDPGFX_WIRE_TO_SURFACE_PDU_1  (for each dirty region)
  ├── surfaceId: 0 (primary desktop)
  ├── codecId: RDPGFX_CODECID_AVC420 or RDPGFX_CODECID_CAPROGRESSIVE
  ├── pixelFormat: PIXEL_FORMAT_XRGB_8888
  ├── destRect: dirty region bounds
  └── bitmapData: compressed frame data

RDPGFX_END_FRAME_PDU
  └── frameId: matching start frame
```

This means the server can decode frames using standard RDPGFX decoders — and potentially, a standard RDP client could be used to "shadow" the session in real-time.

### 4.5 Client: Local Cache & Offline Buffer

When the server is unreachable, the client buffers captures locally using a **Recall-compatible SQLite database** (`ukg.db` schema). This serves dual purposes:
1. Offline resilience — captures are never lost
2. Local query capability — the client can operate standalone if needed

The local cache uses the exact Windows Recall schema (§2.1) so that:
- The native Recall UI could theoretically read Precall's local cache
- Migration between Recall and Precall is bidirectional

**Encryption:** DPAPI-NG with Windows Hello proof-of-presence (matching Recall's security model). On systems with VBS, use VBS enclaves for key sealing.

### 4.6 Client: NT Service (`precall-svc.exe`)

Runs as a Windows service under `LOCAL SYSTEM` (for DXGI access) with:
- Automatic startup on user login (Session 0 isolation handled via `WTSQueryUserToken`)
- Session change notifications (`SERVICE_CONTROL_SESSIONCHANGE`)
- Service recovery actions (restart on failure)
- ETW (Event Tracing for Windows) logging

**Service registration:**
```
sc create PrecallCapture binPath= "C:\Program Files\Precall\precall-svc.exe" \
   start= auto DisplayName= "Precall Capture Service" \
   depend= "RpcSs/Winmgmt"
```

### 4.7 Server: Connection Broker

Listens for incoming reverse RDP connections from clients:

```
precall-server.exe --listen 0.0.0.0:3390 \
    --tls-cert server.pem --tls-key server.key \
    --auth kerberos|ntlm|certificate \
    --storage-path /var/lib/precall \
    --max-clients 100
```

**Authentication methods:**
1. **Kerberos/NTLM** via CredSSP (reuses standard RDP NLA)
2. **Certificate-based** — client presents a machine certificate
3. **Pre-shared key** — for non-domain environments

**Multi-tenancy:** Each authenticated client gets an isolated storage namespace.

### 4.8 Server: Storage Layer

#### 4.8.1 Metadata Database (`ukg.db` — Recall-compatible)

Per-client SQLite database with the exact Windows Recall schema:

```sql
-- Core tables (matching Recall exactly)
WindowCapture (Id, Name, ImageToken, IsForeground, WindowId,
               WindowBounds, WindowTitle, Properties, TimeStamp,
               IsProcessed, ActivationUri, ActivityId, FallbackUri)
App (Id, WindowsAppId, IconUri, Name, Path, Properties)
WindowCaptureAppRelation (WindowCaptureId, AppId)
File (Id, Path, Name, Extension, Kind, Type, Properties, ObjectId, VolumeId)
WindowCaptureFileRelation (WindowCaptureId, FileId)
Web (Id, Domain, Uri, IconUri, Properties)
WindowCaptureWebRelation (WindowCaptureId, WebId)
ScreenRegion (Id, WindowCaptureId, RegionKind, OcrText, Bounds)
Topic (Id, Title, Properties)
WindowCaptureTopicRelation (WindowCaptureId, TopicId, Score)
AppDwellTime (Id, WindowsAppId, HourOfDay, DayOfWeek, HourStartTimestamp, DwellTime)
WebDomainDwellTime (Id, Domain, HourOfDay, DayOfWeek, HourStartTimestamp, DwellTime)

-- FTS5 full-text index (matching Recall)
WindowCaptureTextIndex USING fts5(Name, WindowTitle, OcrText)
```

#### 4.8.2 Semantic Index (DiskANN-compatible)

Per-client vector databases using the same DiskANN schema as Recall:

```sql
-- SemanticTextStore.db
si_items (id BLOB(16) PRIMARY KEY)
si_diskann_graph (id INTEGER PRIMARY KEY, embedding BLOB, outbound_ids BLOB)
si_diskann_info (graph_table_name, dimension, vector_space_id, ...)
si_embedding_metadata (embedding_id, item_id, region_id, metadata_json)
si_diskann_config (graph_table_name, max_degree, alpha, ...)

-- SemanticImageStore.db (identical schema)
```

**Embedding generation:** Server-side using:
- **Text:** Sentence-BERT or E5-large for OCR text embeddings
- **Image:** CLIP ViT-L/14 for screenshot region embeddings
- **Multimodal:** Florence-2 or LLaVA for combined understanding

#### 4.8.3 Object Storage

Screenshots and audio segments stored in configurable backend:
- Local filesystem (default)
- S3-compatible object storage (MinIO, AWS S3)
- Azure Blob Storage

File naming: `{client_id}/{year}/{month}/{day}/{image_token}.jpg`

### 4.9 Server: AI Processing Pipeline

```
Incoming Frame
     │
     ├──► OCR Enhancement (if client OCR unavailable)
     │    └── Windows.AI.MachineLearning / Tesseract / PaddleOCR
     │
     ├──► Screen Segmentation
     │    └── Identify UI regions, text blocks, images
     │    └── Populate ScreenRegion table
     │
     ├──► Text Embedding Generation
     │    └── Sentence-BERT / E5-large
     │    └── Insert into SemanticTextStore DiskANN graph
     │
     ├──► Image Embedding Generation
     │    └── CLIP ViT-L/14
     │    └── Insert into SemanticImageStore DiskANN graph
     │
     ├──► Topic Extraction
     │    └── Zero-shot classification (BART-large-mnli)
     │    └── Populate Topic + WindowCaptureTopicRelation
     │
     └──► Audio Transcription (if audio captured)
          └── Whisper large-v3
          └── Link transcription to timeline
```

### 4.10 Server: Query API

REST + gRPC API for searching and browsing captured history:

```
POST /api/v1/search
{
  "query": "the PDF about Kerberos I saw yesterday",
  "time_range": {"start": "2026-04-27T00:00:00Z", "end": "2026-04-28T00:00:00Z"},
  "app_filter": ["msedge.exe", "chrome.exe"],
  "limit": 20
}

Response:
{
  "results": [
    {
      "window_capture_id": 12345,
      "timestamp": "2026-04-27T14:32:15Z",
      "window_title": "Kerberos_Protocol_Specification.pdf - Adobe Acrobat",
      "app": {"name": "Adobe Acrobat", "icon_uri": "..."},
      "screenshot_url": "/api/v1/snapshot/12345",
      "ocr_text_preview": "Kerberos V5 protocol specification...",
      "relevance_score": 0.94,
      "activation_uri": "acrobat://open?file=C:\\Users\\...\\Kerberos_Protocol_Specification.pdf",
      "screen_regions": [
        {"kind": "text_block", "bounds": "100,200,800,600", "text": "..."}
      ]
    }
  ]
}
```

### 4.11 Frontend: Recall-Compatible UI

The web-based frontend aims for drop-in compatibility with Windows Recall's UX:

#### 4.11.1 Timeline View
- Horizontal timeline scrollbar (matching Recall's timeline strip)
- Thumbnail preview of screenshots at each time point
- Filterable by app, website, date range
- Dwell time heatmap showing app/web usage patterns

#### 4.11.2 Semantic Search
- Natural language search bar (top of UI)
- Results displayed as screenshot cards with highlighted OCR text regions
- "Click to Do" actions on recognized content:
  - Copy text from screenshot region
  - Open URL detected in screenshot
  - Relaunch app at captured state (via `ActivationUri`)
  - Search web for selected text

#### 4.11.3 Native Windows Integration (Optional)
For full drop-in compatibility, a COM server can expose the Precall data through interfaces that the Recall UI expects:
- `IRecallTimeline` — timeline browsing
- `IRecallSearch` — semantic search
- `IRecallSnapshot` — screenshot access
- Registered as a Recall provider via registry keys

---

## 5. Security Architecture

### 5.1 Transport Security
- TLS 1.3 mandatory for all client-server communication
- CredSSP/NLA for authentication (reuses RDP's auth infrastructure)
- Perfect forward secrecy via ephemeral ECDHE key exchange
- Certificate pinning supported for enterprise deployments

### 5.2 Data Encryption
| Layer | Method | Key Management |
|---|---|---|
| In-transit | TLS 1.3 (AES-256-GCM) | Ephemeral keys via ECDHE |
| At-rest (client cache) | DPAPI-NG + VBS enclave | Windows Hello proof-of-presence |
| At-rest (server) | AES-256-GCM | Per-client keys, sealed in server HSM or TPM |
| Database (ukg.db) | SQLite Encryption Extension (SEE) or SQLCipher | Derived from client's master key |

### 5.3 Privacy Controls
- Per-application capture exclusion list
- Per-website exclusion (browser extension integration)
- InPrivate/Incognito auto-detection and exclusion
- DRM-protected content exclusion (`WDA_MONITOR`)
- Configurable retention period with automatic purging
- "Panic button" — instant wipe of all server-side data for a client
- GDPR/privacy: data export and deletion APIs

### 5.4 Authentication & Authorization
- Multi-factor: Windows Hello + device certificate
- Role-based access: admin (full access), user (own data only), auditor (read-only)
- Audit logging for all data access events

---

## 6. Technology Stack

### Client (Windows 11, C++ / C# hybrid)
| Component | Technology |
|---|---|
| Service host | C++ Win32 NT Service |
| Screen capture | DXGI Desktop Duplication (C++) |
| Audio capture | WASAPI Loopback (C++) |
| Metadata extraction | C# with P/Invoke + UI Automation |
| Local OCR | Windows.AI.MachineLearning (WinRT) |
| Local database | SQLite3 (C) |
| Encryption | DPAPI-NG / BCrypt / VBS Enclave SDK (C++) |
| RDP transport | Custom implementation based on FreeRDP core (C) |
| CLI tool | C# (.NET 8) console app |
| Settings GUI | WinUI 3 / XAML (C#) |
| Build system | CMake + MSBuild |

### Server (Cross-platform, Rust + Python)
| Component | Technology |
|---|---|
| Connection broker | Rust (tokio + rustls) |
| RDP protocol handler | Rust (custom, based on IronRDP) |
| Storage layer | SQLite3 + S3 SDK |
| AI pipeline | Python (PyTorch, transformers, DiskANN) |
| Query API | Rust (axum) + Python (FastAPI for AI endpoints) |
| Web frontend | TypeScript, React, TailwindCSS |
| Vector search | DiskANN (Rust bindings via diskannpy) |
| Build system | Cargo (Rust) + uv (Python) + Vite (Frontend) |

### Key Dependencies
| Dependency | Purpose | License |
|---|---|---|
| FreeRDP | RDP protocol reference (client transport) | Apache 2.0 |
| IronRDP | Rust RDP implementation (server-side) | MIT/Apache 2.0 |
| DiskANN | Vector search (Recall-compatible) | MIT |
| SQLite | Metadata storage | Public Domain |
| Tesseract | OCR fallback (non-NPU systems) | Apache 2.0 |
| sentence-transformers | Text embeddings | Apache 2.0 |
| openai/clip | Image embeddings | MIT |
| whisper | Audio transcription | MIT |

---

## 7. Database Schema Compatibility

Precall uses the **exact same database schema** as Windows Recall for `ukg.db`, `SemanticTextStore.db`, and `SemanticImageStore.db`. This means:

1. **Windows Recall's native UI** can read Precall's local cache database
2. **Forensic tools** (TotalRecall, etc.) work unmodified on Precall data
3. **Migration** between Recall and Precall is a file copy
4. **Third-party Recall integrations** work with Precall's data

The only additions Precall makes to the schema:

```sql
-- Precall extensions (in ukg.db)
CREATE TABLE IF NOT EXISTS "PrecallSyncState" (
    "WindowCaptureId" INTEGER PRIMARY KEY,
    "SyncedToServer" INTEGER NOT NULL DEFAULT 0,
    "SyncTimestamp" INTEGER,
    "ServerAckId" TEXT,
    FOREIGN KEY(WindowCaptureId) REFERENCES WindowCapture(Id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "PrecallConfig" (
    "Key" TEXT PRIMARY KEY,
    "Value" TEXT NOT NULL
);
-- Keys: server_url, client_id, capture_interval_ms, audio_enabled, etc.
```

---

## 8. Deployment & Configuration

### 8.1 Client Installation

```powershell
# MSI installer
msiexec /i Precall-Client-1.0.0.msi /qn \
  SERVER_URL=precall.example.com:3390 \
  AUTH_MODE=certificate \
  CAPTURE_INTERVAL=5000

# Or via winget
winget install Precall.Client --override "/SERVER_URL=precall.example.com:3390"
```

### 8.2 Server Installation

```bash
# Docker Compose (recommended)
docker compose -f docker-compose.yml up -d

# Components:
#   precall-broker:  Connection broker (Rust)
#   precall-api:     Query API (Rust + Python)
#   precall-ai:      AI pipeline workers (Python + GPU)
#   precall-web:     Frontend (Nginx + React SPA)
#   minio:           Object storage
```

### 8.3 Configuration

Client configuration via registry + Group Policy:
```
HKLM\SOFTWARE\Precall\
  ServerUrl         REG_SZ    "precall.example.com:3390"
  CaptureIntervalMs REG_DWORD 5000
  AudioEnabled      REG_DWORD 1
  ExcludedApps      REG_MULTI_SZ ["mstsc.exe", "KeePass.exe"]
  ExcludedDomains   REG_MULTI_SZ ["bank.example.com"]
  RetentionDays     REG_DWORD 90
  LocalCacheEnabled REG_DWORD 1
  EncryptionMode    REG_SZ    "dpapi-ng"  // or "vbs-enclave"
```

Group Policy ADMX templates provided for enterprise deployment.

---

## 9. Relationship to Existing Projects

| Project | Relationship | What We Reuse |
|---|---|---|
| Windows Recall | API/schema compatibility target | Database schema, API surface, UI concepts |
| Terminal Services / RDP | Transport protocol foundation | RDPGFX wire format, virtual channels, NLA auth |
| systemd-recalld | Linux predecessor by same author | Client-server architecture concept, AI query pipeline design |
| FreeRDP | Client-side RDP library | Protocol implementation reference |
| IronRDP | Server-side RDP library | Rust RDP protocol handling |
| DiskANN | Vector search engine | Same engine used by Recall for semantic search |
| OpenRecall | OSS Recall alternative | Reference for cross-platform capture techniques |

---

## 10. Testing Strategy

### 10.1 Unit Tests
- Frame capture pipeline (mock DXGI surface)
- Metadata extraction (mock window state)
- RDP channel serialization/deserialization
- Database operations (SQLite schema compliance)
- Encryption round-trip

### 10.2 Integration Tests
- Client→Server connection establishment
- Frame transmission and storage verification
- Search query end-to-end (capture → index → query → result)
- Offline buffer and sync recovery
- Multi-client concurrent connections

### 10.3 Compatibility Tests
- Verify `ukg.db` readable by TotalRecall tool
- Verify DiskANN indices readable by diskannpy
- Verify RDPGFX frames decodable by standard RDP client
- Test with Windows Recall UI (if available on test system)

### 10.4 CI Pipeline
```yaml
# GitHub Actions
- build-client:     MSBuild on windows-latest
- build-server:     cargo build + pytest on ubuntu-latest
- test-unit:        Per-component unit tests
- test-integration: Docker Compose with mock client
- test-schema:      Schema compatibility verification
- lint:             clippy (Rust), ruff (Python), eslint (TS)
```

---

## 11. Open Questions for Review

1. **Audio capture scope:** Should audio capture be included in v1, or deferred? (It adds significant complexity and storage requirements.)

2. **RDPGFX vs. custom encoding:** Should we use the actual RDPGFX wire format (maximum RDP compatibility) or a simpler custom format (faster to implement, less protocol overhead)?

3. **Server language:** The design proposes Rust for the server core. Alternatively, the entire server could be C# (.NET 8) for better Windows ecosystem integration, or Go for simpler deployment.

4. **Local-only mode:** Should Precall support a "local-only" mode (no server, just local capture + local AI) as a fallback? This would make it a direct Recall replacement rather than strictly client-server.

5. **VBS Enclave SDK:** Full VBS enclave integration for client-side key sealing requires Virtualization-Based Security enabled. Should we require this, or gracefully degrade to DPAPI-NG?

6. **Drop-in Recall UI compatibility:** Full COM server registration to intercept Recall's UI is technically feasible but fragile across Windows updates. Should we prioritize the web UI instead?

---

## 12. References

1. Subramanya, S. J., et al. "DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single Node." *Advances in Neural Information Processing Systems (NeurIPS)*, 2019.
2. Singh, A., et al. "FreshDiskANN: A Fast and Accurate Graph-Based ANN Index for Streaming Similarity Search." *arXiv:2105.09613*, 2021.
3. Krishnaswamy, R., Manohar, M., Simhadri, H. "The DiskANN Library: Graph-Based Indices for Fast, Fresh and Filtered Vector Search." *IEEE Data Eng. Bull.* 48, pp. 20-42, Dec 2024.
4. Microsoft. "[MS-RDPBCGR]: Remote Desktop Protocol: Basic Connectivity and Graphics Remoting." Open Specifications, rev. 75.0, Apr 2025.
5. Microsoft. "[MS-RDPEGFX]: Remote Desktop Protocol: Graphics Pipeline Extension." Open Specifications, rev. 18.1, Aug 2025.
6. Microsoft. "[MS-RDPEDYC]: Remote Desktop Protocol: Dynamic Channel Virtual Channel Extension." Open Specifications.
7. Microsoft. "Desktop Duplication API." Windows Dev Center, Win32 Documentation.
8. Microsoft. "Update on Recall Security and Privacy Architecture." Windows Experience Blog, Sep 2024.
9. Radford, A., et al. "Learning Transferable Visual Models From Natural Language Supervision." *ICML*, 2021. (CLIP — used for image embeddings.)
10. Reimers, N. and Gurevych, I. "Sentence-BERT: Sentence Embeddings using Siamese BERT-Networks." *EMNLP*, 2019. (Text embedding model.)
11. Radford, A., et al. "Robust Speech Recognition via Large-Scale Weak Supervision." *arXiv:2212.04356*, 2022. (Whisper — audio transcription.)
12. Feldman, D. "Database Schema for Microsoft's Copilot+ Recall Feature." GitHub Gist, Jun 2024. (Reverse-engineered Recall schema reference.)
