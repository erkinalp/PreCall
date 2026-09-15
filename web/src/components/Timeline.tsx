// SPDX-License-Identifier: GPL-2.0-only
import { useEffect, useRef, useState } from "react";
import {
  filetimeToDate,
  subscribeLive,
  timeline,
  TimelineEntry,
} from "../api";
import CaptureCard from "./CaptureCard";

export default function Timeline({ client }: { client: string }) {
  const [entries, setEntries] = useState<TimelineEntry[]>([]);
  const [selected, setSelected] = useState<TimelineEntry | null>(null);
  const [loading, setLoading] = useState(false);
  const [live, setLive] = useState(true);
  const bottomRef = useRef<HTMLDivElement>(null);

  const load = () => {
    setLoading(true);
    timeline(client, undefined, 60)
      .then(setEntries)
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    setEntries([]);
    setSelected(null);
    load();
  }, [client]);

  useEffect(() => {
    if (!live || !client) return;
    return subscribeLive(client, (ev) => {
      if (ev.kind === "capture") load();
    });
  }, [client, live]);

  const loadMore = () => {
    const last = entries[entries.length - 1];
    if (!last) return;
    timeline(client, last.timestamp_100ns, 60).then((more) =>
      setEntries((e) => [...e, ...more]),
    );
  };

  // Group into sessions: >10min gap = new session (Recall's "Work session").
  const sessions: TimelineEntry[][] = [];
  for (const e of entries) {
    const prev = sessions[sessions.length - 1]?.[0];
    if (prev && prev.timestamp_100ns - e.timestamp_100ns > 600 * 10_000_000) {
      sessions.push([e]);
    } else {
      (sessions[sessions.length - 1] ??= []).push(e);
    }
  }

  return (
    <div className="timeline">
      <div className="timeline-toolbar">
        <label>
          <input
            type="checkbox"
            checked={live}
            onChange={(e) => setLive(e.target.checked)}
          />{" "}
          Live
        </label>
        <button onClick={load} disabled={loading}>
          {loading ? "…" : "Refresh"}
        </button>
      </div>
      <div className="timeline-body">
        <div className="strip">
          {sessions.map((s, i) => (
            <div key={i} className="session">
              <div className="session-label">
                {filetimeToDate(s[0].timestamp_100ns).toLocaleString()}
              </div>
              <div className="cards">
                {s.map((e) => (
                  <button
                    key={e.id}
                    className={`thumb ${selected?.id === e.id ? "sel" : ""}`}
                    onClick={() => setSelected(e)}
                    title={e.window_title}
                  >
                    <img
                      src={`/api/v1/snapshot/${e.id}?client=${client}`}
                      alt={e.window_title}
                      loading="lazy"
                      onError={(ev) => {
                        (ev.target as HTMLImageElement).style.visibility = "hidden";
                      }}
                    />
                    <span className="thumb-title">{e.name}</span>
                  </button>
                ))}
              </div>
            </div>
          ))}
        </div>
        <div className="detail">
          {selected ? (
            <CaptureCard client={client} entry={selected} />
          ) : (
            <p className="empty">Select a capture.</p>
          )}
        </div>
      </div>
      <div ref={bottomRef}>
        <button onClick={loadMore}>Load older</button>
      </div>
    </div>
  );
}
