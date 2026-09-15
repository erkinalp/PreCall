<!-- SPDX-License-Identifier: GPL-2.0-only -->
# Precall Wire Protocol

The capture channel is a **reverse RDP-style connection**: the client workstation
connects outbound to the broker (default `host:3390`), performs a TLS 1.3
handshake plus a JSON hello, then streams multiplexed channels back to the
server — the same inversion Terminal Services Gateway performs when a session
is hosted by the client rather than the terminal.

## 1. Framing

All post-handshake traffic uses `MuxFrame`:

```
+----------+----------------+==========================+
| channel  | length (u32le) | payload                  |
|  u8      |                | <= 16 MiB                |
+----------+----------------+==========================+
```

| ChannelId | Value | Direction | Contents |
|---|---|---|---|
| `GfxRdp` | `0x01` | C→S | RDPGFX-comand PDUs (screenshot stream) |
| `PcMeta` | `0x02` | C→S | `CaptureBatch` JSON (Recall-compatible metadata) |
| `PcAudio` | `0x03` | C→S | `[u16 json_len][json {codec,channels,sample_rate}][pcm16]` |
| `PcCtrl` | `0x04` | S→C | `CtrlMessage` JSON (Pause/Resume/PanicWipe/Goodbye) + C→S `Heartbeat` |
| `PcQuery` | `0x05` | both | reserved for on-client query bridge |

## 2. Handshake

1. TCP connect, then **TLS 1.3** (rustls; `ring` provider).
   - PSK token **or** pinned client-cert fingerprint **or** NTLM (flagged).
   - Self-signed deployments: client pins the server cert's SHA-256 fingerprint
     (`--fingerprint`); CA deployments use webpki-roots.
2. C→S: length-prefixed (u32le, ≤1 MiB) JSON `ClientHello`:
   ```json
   { "protocol_version": 1, "client_id": "<uuid or null>", "hostname": "...",
     "auth": { "psk": "..." } | { "cert_fingerprint": "<hex>" },
     "capabilities": { "displays": [...], "channels": [1,2,3,4],
                      "codec_ids": [61681], "capture_interval_ms": 3000,
                      "client_build": "0.1.0" } }
   ```
3. S→C: `ServerHello { server_name, assigned_client_id }` **or**
   `ServerReject { reason }` then close.

## 3. Graphics channel (GfxRdp)

Real MS-RDPEGGFX command IDs, verbatim — the identifier space is preserved so
the stream can ride an existing RDP stack's virtual-channel plumbing:

| Command | CmdId | Notes |
|---|---|---|
| `RDPGFX_WIRE_TO_SURFACE_PDU_1` | `0x0001` | header + surface frame |
| `RDPGFX_CMDID_START_FRAME` | `0x000b` | frame boundary (frame_id u32) |
| `RDPGFX_CMDID_END_FRAME` | `0x000c` | frame boundary |
| `RDPGFX_CMDID_FRAME_ACK` | `0x000d` | sent by client after each frame (S→C ack is optional) |

**Vendor codec extension:** `codec_id = 0x00F1` ("JPEG") inside the W2S1
surface-frame `bitmapData`, pixel format `XRGB_8888` (the outer PDU shape is
unchanged). MS assigns `0x0000`–`0x00FF`; `0x00F1` is Precall's vendor slot —
`H264 = 0x01`, `AVC420/444` remain free for a future hardware path.

Surface payloads: `{surface_id u16, frame_id u32, timestamp_100ns u64,
rects n×(i16 x,y,w,h), jpeg bytes}`.

## 4. Metadata channel (PcMeta)

`CaptureBatch { client_id, seq, captures: [WindowCaptureRecord] }` — one batch
per captured frame, ordered FIFO with the GfxRdp channel (broker pairs batch
`seq` against frames by arrival; each batch references its frame's
`image_token`).

`WindowCaptureRecord` mirrors Recall's `WindowCapture` row shape:
`name` ("proc.exe (pid)"), `image_token`, `window_title`,
`window_bounds` ("l,t,r,b"), `is_foreground`, `window_id`,
`activation_uri` / `fallback_uri` (Recall's relaunch contract),
`apps[]`, `files[]`, `webs[]`, `regions[]` (OCR text + bounds),
`timestamp_100ns` (FILETIME: 100-ns ticks since 1601-01-01).

## 5. Timestamps

All clocks are **FILETIME** (`u64`, 100 ns since 1601-01-01 UTC) end-to-end —
the same epoch Recall's databases use, so exports can diff against a real
`ukg.db` without epoch math. `precall_proto::time` holds the conversion
helpers; `FILETIME_UNIX_DELTA = 116_444_736_000_000_000`.

## 6. Storage layout (server side)

```
{data_root}/server.db                    client registry (PrecallClient table)
{data_root}/.keys/objects.key            AES-256-GCM object key
{data_root}/{client_uuid}/
  ukg.db                                 Recall-identical schema + FTS5
  SemanticTextStore.db                   si_* tables (vectors, DIM=384)
  SemanticImageStore.db                  si_* tables
  objects/{yyyy}/{mm}/{dd}/{token}.{jpg,opus}
```

`objects/` files are AES-256-GCM sealed with the token as AAD — same threat
model as Recall's encrypted object store, minus the VBS enclave (the counterfactual
runs on a server that has no TPM per client).

## 7. Control plane (PcCtrl)

S→C `CtrlMessage`: `Pause`, `Resume`, `PanicWipe` (server asks the client to
purge local state), `Goodbye`.
C→S `Heartbeat { uptime_secs, frames_sent, captures_sent, last_error }` every
30 s; broker closes clients silent >3 intervals.

## 8. Versioning

`PROTOCOL_VERSION = 1`. Mismatched majors → `ServerReject`. Field additions
must be JSON-optional on the wire (serde defaults) so mixed-version fleets
keep capturing.
