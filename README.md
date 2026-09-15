<!-- SPDX-License-Identifier: GPL-2.0-only -->
# PreCall

**Windows Recall the way people think it works.**

The real Windows Recall captures, OCRs, embeds, and indexes your desktop
*entirely locally* — which is precisely why it needed a Copilot+ NPU. PreCall
implements the counterfactual: the client workstation streams its desktop to a
configurable server over a reverse-RDP-style channel, and **all inference,
indexing, and storage happen on the central server** — exactly the architecture
critics assumed, and exactly the one that would have justified a datacenter
uplink upgrade.

## Components

| Piece | Language | Role |
|---|---|---|
| `crates/precall-proto` | Rust | Wire protocol: mux framing, handshake, RDPGFX PDUs, FILETIME, TLS pinning |
| `crates/precall-store` | Rust | Recall-identical `ukg.db` + `si_*` semantic stores + AES-256-GCM object store |
| `crates/precall-broker` | Rust | TLS 1.3 listener, PSK/cert/NTLM auth, frame+metadata ingestion |
| `crates/precall-api` | Rust | REST + WebSocket query surface (timeline, hybrid search, relaunch, analytics, export, wipe) |
| `crates/precall-client` | Rust | Windows capture service: DXGI frames, WASAPI loopback, WinRT OCR, UIA URL/file extraction, privacy gates |
| `crates/precall-mock` | Rust | Headless client for end-to-end tests |
| `ai/` | Python | Central inference: embeddings, topics, whisper (`/embed/*`, `/topics`, `/transcribe`) |
| `web/` | React/TS | Recall-style UI: Timeline, Search, Analytics, live captures, Click-to-Do overlays |

> **Implementation note:** the architecture document specified the client in
> C++/C#. It is implemented in Rust against the `windows` crate (0.62) —
> identical Win32/WinRT surface, memory-safe by construction.

## Quickstart — server tier (Docker)

```bash
PRECALL_PSK=hunter2 PRECALL_API_TOKEN=tok docker compose up --build
# web UI → http://localhost/  • API → :8080  • broker TLS → :3390  • ai → :3000
```

Self-signed TLS is generated for development (`--self-signed`); mount
`PRECALL_TLS_CERT`/`PRECALL_TLS_KEY` for production.

## Quickstart — Windows 11 client

```powershell
cargo build --release -p precall-client
precall-client enroll --server YOUR_HOST:3390 --psk hunter2   # persists to HKCU\Software\Precall
precall-client install-service                              # registers the "Precall" service
precall-client run                                          # or run interactively
```

The endpoint is fully configurable (`--server`, env `PRECALL_SERVER`) and
defaults to `127.0.0.1:3390`. For self-signed servers, pin the cert fingerprint
(`--fingerprint`) once at enrollment; afterwards TLS is validated against the
pin, not the CA store.

Privacy gates run **on the client before upload**: process/domain exclusion
lists, protected-window detection (display affinity), pause/resume via the
server control channel.

## API surface (`/api/v1`)

```
GET  /timeline?client&before&limit     Recall-style paged timeline
POST /search                           {query, mode: fts|semantic|hybrid, app_filter, time_range}
GET  /snapshot/{id}?client             decrypted JPEG snapshot
GET  /regions/{id}?client              OCR regions (Click-to-Do data)
POST /relaunch                         {window_capture_id} → activation_uri / fallback_uri
GET  /apps  /web  ?client              dwell analytics
GET  /clients                          enrolled clients
GET  /export?client                    GDPR-style per-client dump
DELETE /client/{id}/data               right-to-erasure wipe
WS   /live?client                      live capture feed
GET  /health
```

## Repo layout

```
crates/        Rust workspace (proto → store → broker/api/client/mock)
ai/            Python inference service (uv; extras: ai-cpu, ai-cuda, ai-rocm, whisper)
web/           React + Vite frontend
deploy/        Dockerfiles for the server tier
docs/          architecture.md (design doc) + protocol.md (wire spec)
docker-compose.yml
```

## Build & test

```bash
cargo check --workspace          # msvc toolchain on Windows runners
cargo test  --workspace
cd ai  && uv sync --extra dev && uv run pytest
cd web && npm ci && npm run build
```

CI: `.github/workflows/ci.yml` — Rust on `windows-latest` (the client is
Win32-only), Python and web on Ubuntu; server Docker images build only the
Linux-compatible crates.

## License

GPL-2.0-only. See `LICENSE`.
