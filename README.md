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
POST /search                           {query, mode: fts|semantic|hybrid|image, image, app_filter, time_range}
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

Search answers with `{results, legs}` and, when a leg could not run, a
`notes` array naming it — a missing capability is reported, never silently
returned as "no matches". The legs are:

| leg | runs when | needs |
|---|---|---|
| `fts` | `mode` is `hybrid`/`fts` | nothing |
| `text-semantic` | `mode` is `hybrid`/`semantic` | `ukg.db` OCR text + either embedder |
| `image-semantic` | `image` (base64 JPEG or data URL) is supplied | screenshots embedded into `SemanticImageStore.db` |
| `image-text` | `mode` is `hybrid`/`semantic` and the AI service has CLIP | `ai-cpu`/`ai-cuda` extra (`/embed/image-text`) |

Results are ranked by **relevance**, with recency only as a tie-break.
`relevance_score` is a reciprocal-rank-fusion score (higher is better), not a
cosine similarity: legs are fused by `rank` because SQLite `bm25` and cosine
are different scales, and mixing them let an unrelated vector hit outrank an
exact lexical match. Within a leg, a semantic candidate with no positive
similarity is dropped — the flat scan returns every stored item, so the
zero/negative tail is the whole store, not a match.

`time_range` and `app_filter` apply to **every** leg — and they are applied
*during* each leg's scan, not to an already-truncated top-k, so an eligible
capture that ranks below the cut is still found. Each leg offers a window four
times deeper than the final result count before fusion (capped at the FTS
limit), because rank fusion needs the union of the legs' candidate windows: a
capture ranked just outside `limit` by one leg and inside it by another must
still combine both ranks.

A search that needs the AI service reports its state honestly: `notes` names a
leg that could not run, and an image query returns **503** while the service is
down (retryable) rather than an empty result set. `/embed/image-text`
deliberately answers **501** when no CLIP stack is installed: the deterministic
hash fallback has no shared text/image space, so answering with a hash
embedding would turn text→image search into an arbitrary ranker.

Image embeddings survive the two cases a `IsProcessed` flag cannot express:
captures processed by an older, text-only build are **backfilled** from the
stored screenshots, and a capture whose embedding failed while the AI service
was down is **retried** rather than lost. Per-capture state lives in
`PrecallImageEmbedding` (`pending`/`done`/`skipped`); `skipped` records why
(no timestamp, non-JPEG frame, refused input) and is never retried. **Both**
queues back off for a minute and are ordered least-recently-attempted first, so
a handful of stuck captures cannot starve the rest of the backlog. The clock
they pace against is `PrecallEnrichAttempt` — touched by *every* attempt,
whichever leg, because a capture whose image leg is done while its text leg
keeps failing would otherwise never move off the front of the queue. Only
input-specific statuses (400/413/415/422) are terminal: 408/429, 5xx, transport
failures and even 401/404 are retried, because those clear when the deployment
is fixed. A read that fails is isolated to its own capture — one corrupt
screenshot cannot stall a client's whole backlog.

Embeddings carry the **vector space** that produced them (`space` on every
`/embed/*` response, `*_space_id` on `/health`), and that id is stamped into
each `si_*` store. Vectors from different encoders are incomparable even at the
same dimension, so when the space changes — enabling the AI service after
running with the built-in hash embedder, or switching models — the enricher
clears the affected store and rebuilds it, and a query that arrives before the
rebuild reports the mismatch in `notes` instead of ranking incomparable
vectors. (The Rust and Python hash fallbacks are different functions and carry
different ids.)

## Performance envelope (measured, not estimated)

Two operating points matter: **full framerate** (streaming) and **Recall's
collection cadence** (a capture every few seconds, nothing while the desktop is
unchanged). Both are reproduced by two harnesses in this repo:

```bash
cargo run --release -p precall-broker --example frame_throughput   # server + wire
cargo run --release -p precall-frame  --example client_cost        # client per-frame CPU
```

Measured on a workstation (1920x1080, ~130-220 KiB/frame JPEG q80, TLS 1.3,
real handshake, real RDPGFX framing, AES-GCM at rest):

| stage | measured | note |
|---|---|---|
| broker ingest, encrypted | **1494 captures/s** (201 MB/s) | end-to-end, sent *and* committed |
| broker ingest, plaintext objects | 3072 captures/s | at-rest encryption costs ~51% of throughput |
| `put` + `insert_capture` | ~1980 captures/s combined | store alone never the ceiling |
| client JPEG q80, desktop-like | 57 ms/frame → **17 fps per core** | photographic: 116 ms → 8.6 fps, 2.8 MiB/frame |
| client dedup fingerprint | 1.18 ms/frame exact (7.4% of a 16 ms budget) | byte-at-a-time would be 7.5 ms; a *sampled* stride was cheaper but wrong (see below) |

So at 1080p the **server is not the constraint** (60 fps needs ~4% of it), and
neither is the number of clients. The constraint is per client and it is the
link, then the encoder:

| operating point | wire per client | client CPU (one core) |
|---|---|---|
| streaming, 16 ms cadence | 3.7 MB/s desktop-like, **23.8 MB/s photographic** (≈30-190 Mbps up) | 1.05 cores to 6.8 cores |
| Recall cadence, 3000 ms | 37 KB/s to 950 KB/s | ~3% of a core |

That is the honest reason Recall collects a capture every few seconds instead of
streaming: a client-server design at full framerate is bounded by the *uplink*
(and by the encoder's CPU), not by the datacenter.

### Dirty rectangles, and why the server composes them

The lever is to send only what changed. The client takes DXGI's dirty
rectangles, plans a delta (`precall_frame::plan_capture` — scrolling and
oversized changes fall back to a keyframe, since the wire has no surface-copy
operation), encodes each rectangle on its own and sends them as
`WIRE_TO_SURFACE` PDUs inside one frame. The broker applies them to a per-client
**desktop surface** and archives the composed result as an ordinary full-frame
JPEG (`precall-broker/src/canvas.rs`).

That choice is deliberate: it keeps the reader-facing contract — `ukg.db` plus
one image per capture, Recall's own layout — so the snapshot route, Click-to-Do,
the image embedder, the export and any migration to or from Recall keep working
without a compositor. It is also what the RDP graphics pipeline does
(`WIRE_TO_SURFACE` applies bitmaps to a *surface* the server owns). Storing
deltas would have been cheaper to ingest and worse everywhere else.

Measured, same harness, two rectangles per capture (5.1 KiB on the wire instead
of 253 KiB — **50x less**):

| path | wire per capture | server per capture | client per capture |
|---|---|---|---|
| full frames | 253 KiB | ~0.7 ms (stored verbatim) | 58 ms encode |
| dirty rects | 5.1 KiB | **50 ms** (canvas compose + encode, 1080p) | ~2 ms encode |

So dirty rectangles move the encode from the client to the server. At Recall's
cadence that is 50 ms per three seconds — **1.7% of one core per client** — while
the client drops from 58 ms to a couple of milliseconds and the uplink from
253 KiB to 5 KiB. At a 60 fps *stream* the server pays ~3 cores per client,
which is the case for a hardware encoder (Media Foundation on Windows; the
measured figure is the software JPEG path). The knob that decides it is the
capture cadence: `16` ms is streaming, `3000` ms is Recall, and the server's
cost per capture does not change.

Two invariants keep a client honest at *both* operating points, and both are
enforced by `precall-frame` and tested:

* **bounded memory, drop-oldest.** The capture thread publishes into a
  one-frame slot, not a queue: `published = taken + dropped + resident`, with
  `resident <= 1`. A queue would turn a slow link into unbounded growth and a
  stream showing a desktop from minutes ago. The heartbeat carries
  `frames_dropped`, so link pressure is visible server-side instead of living in
  client logs.
* **one cadence policy.** `capture_interval_ms()` floors at 10 ms (100
  captures/s) and passes anything else through, so `16` is streaming and `3000`
  is Recall. (The client used to floor at 200 ms — a hard 5 captures/s ceiling
  whatever was configured.)
* **fingerprinting is exact, not sampled.** An earlier revision sampled one
  byte per 64; because a 1920-pixel BGRA row is 7680 bytes (a multiple of 64)
  the stride landed on the same blue byte of the same columns in every row, so a
  red- or green-only update could fingerprint identically and be deduped away —
  a real screen change never recorded. The word-wise fold reads every byte for
  1.18 ms/frame.
* **a dead capture worker is visible.** `LatestSlot::close()` wakes the bridge,
  drops the sender and ends the session, so a client whose duplication was lost
  cannot keep heartbeating while capturing nothing (the run loop restarts the
  worker on its next pass).

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
