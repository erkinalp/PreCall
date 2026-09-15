// SPDX-License-Identifier: GPL-2.0-only
import { useState } from "react";
import {
  filetimeToDate,
  relaunch,
  search,
  SearchResult,
  snapshotUrl,
} from "../api";

export default function Search({ client }: { client: string }) {
  const [query, setQuery] = useState("");
  const [appFilter, setAppFilter] = useState("");
  const [mode, setMode] = useState("hybrid");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const run = () => {
    if (!query.trim()) return;
    setBusy(true);
    setError("");
    search(client, query, {
      app: appFilter || undefined,
      mode,
      limit: 40,
    })
      .then(setResults)
      .catch((e) => setError(String(e)))
      .finally(() => setBusy(false));
  };

  return (
    <div className="search">
      <div className="search-bar">
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && run()}
          placeholder="Search captures… (FTS5 + semantic hybrid)"
        />
        <input
          value={appFilter}
          onChange={(e) => setAppFilter(e.target.value)}
          placeholder="app filter (optional)"
          className="app-filter"
        />
        <select value={mode} onChange={(e) => setMode(e.target.value)}>
          <option value="hybrid">hybrid</option>
          <option value="fts">full-text</option>
          <option value="semantic">semantic</option>
        </select>
        <button onClick={run} disabled={busy}>
          {busy ? "…" : "Search"}
        </button>
      </div>
      {error && <div className="error">{error}</div>}
      <div className="results">
        {results.map((r) => (
          <div key={r.window_capture_id} className="result">
            <img
              src={snapshotUrl(client, r.window_capture_id)}
              alt=""
              loading="lazy"
              onError={(ev) =>
                ((ev.target as HTMLImageElement).style.visibility = "hidden")
              }
            />
            <div className="result-meta">
              <div className="result-title">{r.window_title || r.app.name}</div>
              <div className="result-sub">
                {r.app.name} · {filetimeToDate(r.timestamp_100ns).toLocaleString()} ·
                score {r.relevance_score.toFixed(3)}
              </div>
              {r.ocr_text_preview && (
                <div className="result-preview">{r.ocr_text_preview}</div>
              )}
              {r.activation_uri && (
                <button
                  className="relaunch-btn"
                  onClick={() =>
                    relaunch(client, r.window_capture_id).then((x) => {
                      const u = x.activation_uri ?? x.fallback_uri;
                      if (u) window.open(u, "_blank");
                    })
                  }
                >
                  Relaunch
                </button>
              )}
            </div>
          </div>
        ))}
        {!busy && !results.length && query && (
          <p className="empty">No results.</p>
        )}
      </div>
    </div>
  );
}
