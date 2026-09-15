// SPDX-License-Identifier: GPL-2.0-only
// API client for the Precall query service.

export interface TimelineEntry {
  id: number;
  name: string;
  image_token: string | null;
  window_title: string;
  timestamp_100ns: number;
  is_foreground: boolean;
  activation_uri: string | null;
  fallback_uri: string | null;
}

export interface SearchResult {
  window_capture_id: number;
  timestamp_100ns: number;
  window_title: string;
  app: { name: string };
  screenshot_url: string;
  ocr_text_preview: string | null;
  relevance_score: number;
  activation_uri: string | null;
}

export interface Region {
  id: number;
  kind: string;
  text: string | null;
  bounds: string;
}

export interface ClientInfo {
  client_id: string;
  hostname: string;
  first_seen_100ns: number;
  last_seen_100ns: number;
}

export interface AppDwell {
  name: string;
  path: string | null;
  dwell_ms: number;
  hour_bucket: number;
}

export interface WebDwell {
  domain: string;
  dwell_ms: number;
  hour_bucket: number;
}

export interface LiveEvent {
  kind: string;
  window_capture_id: number;
  name: string;
  window_title: string;
  timestamp_100ns: number;
  image_token: string | null;
}

const API = "/api/v1";

function authHeaders(token?: string): HeadersInit {
  return token ? { Authorization: `Bearer ${token}` } : {};
}

export async function listClients(token?: string): Promise<ClientInfo[]> {
  const r = await fetch(`${API}/clients`, { headers: authHeaders(token) });
  if (!r.ok) throw new Error(`clients: ${r.status}`);
  return (await r.json()).clients;
}

export async function timeline(
  client: string,
  before?: number,
  limit = 50,
  token?: string,
): Promise<TimelineEntry[]> {
  const p = new URLSearchParams({ client, limit: String(limit) });
  if (before) p.set("before", String(before));
  const r = await fetch(`${API}/timeline?${p}`, { headers: authHeaders(token) });
  if (!r.ok) throw new Error(`timeline: ${r.status}`);
  return (await r.json()).entries;
}

export async function search(
  client: string,
  query: string,
  opts: { app?: string; start?: number; end?: number; limit?: number; mode?: string } = {},
  token?: string,
): Promise<SearchResult[]> {
  const body: Record<string, unknown> = { client, query };
  if (opts.app) body.app_filter = [opts.app];
  if (opts.start || opts.end) body.time_range = { start: opts.start, end: opts.end };
  if (opts.limit) body.limit = opts.limit;
  if (opts.mode) body.mode = opts.mode;
  const r = await fetch(`${API}/search`, {
    method: "POST",
    headers: { "content-type": "application/json", ...authHeaders(token) },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`search: ${r.status}`);
  return (await r.json()).results;
}

export async function regions(
  client: string,
  captureId: number,
  token?: string,
): Promise<Region[]> {
  const r = await fetch(`${API}/regions/${captureId}?client=${client}`, {
    headers: authHeaders(token),
  });
  if (!r.ok) throw new Error(`regions: ${r.status}`);
  return (await r.json()).regions;
}

export function snapshotUrl(client: string, captureId: number): string {
  return `${API}/snapshot/${captureId}?client=${client}`;
}

export async function relaunch(
  client: string,
  captureId: number,
  token?: string,
): Promise<{ activation_uri: string | null; fallback_uri: string | null }> {
  const r = await fetch(`${API}/relaunch`, {
    method: "POST",
    headers: { "content-type": "application/json", ...authHeaders(token) },
    body: JSON.stringify({ client, window_capture_id: captureId }),
  });
  if (!r.ok) throw new Error(`relaunch: ${r.status}`);
  return r.json();
}

export async function appAnalytics(
  client: string,
  token?: string,
): Promise<{ apps: { name: string; path: string | null }[]; dwell: AppDwell[] }> {
  const r = await fetch(`${API}/apps?client=${client}`, { headers: authHeaders(token) });
  if (!r.ok) throw new Error(`apps: ${r.status}`);
  return r.json();
}

export async function webAnalytics(client: string, token?: string): Promise<WebDwell[]> {
  const r = await fetch(`${API}/web?client=${client}`, { headers: authHeaders(token) });
  if (!r.ok) throw new Error(`web: ${r.status}`);
  return (await r.json()).dwell;
}

/** Subscribe to live captures; returns an unsubscribe fn. */
export function subscribeLive(
  client: string,
  onEvent: (e: LiveEvent) => void,
  onClose?: () => void,
): () => void {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}${API}/live?client=${client}`);
  ws.onmessage = (ev) => {
    try {
      onEvent(JSON.parse(ev.data));
    } catch {
      /* non-JSON frame */
    }
  };
  ws.onclose = () => onClose?.();
  return () => ws.close();
}

/** FILETIME (100ns since 1601) → Date */
export function filetimeToDate(ticks: number): Date {
  return new Date((ticks - 116444736000000000) / 10000);
}
