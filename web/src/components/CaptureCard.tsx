// SPDX-License-Identifier: GPL-2.0-only
import { useEffect, useState } from "react";
import {
  filetimeToDate,
  regions as fetchRegions,
  relaunch,
  snapshotUrl,
  Region,
  TimelineEntry,
} from "../api";

export default function CaptureCard({
  client,
  entry,
}: {
  client: string;
  entry: TimelineEntry;
}) {
  const [regions, setRegions] = useState<Region[]>([]);
  const [uri, setUri] = useState<string | null>(null);

  useEffect(() => {
    setUri(null);
    fetchRegions(client, entry.id).then(setRegions).catch(() => setRegions([]));
    relaunch(client, entry.id)
      .then((r) => setUri(r.activation_uri ?? r.fallback_uri))
      .catch(() => setUri(null));
  }, [client, entry.id]);

  const textRegions = regions.filter((r) => r.kind === "text" && r.text);

  return (
    <div className="capture-card">
      <div className="capture-head">
        <h3>{entry.window_title || entry.name}</h3>
        <span>{filetimeToDate(entry.timestamp_100ns).toLocaleString()}</span>
      </div>
      <div className="shot-wrap">
        <img
          className="shot"
          src={snapshotUrl(client, entry.id)}
          alt="screenshot"
        />
        {/* Click-to-Do style text overlays, positioned by OCR bounds. */}
        {textRegions.slice(0, 200).map((r) => {
          const [l, t, rr, b] = r.bounds.split(",").map(Number);
          return (
            <span
              key={r.id}
              className="ocr-box"
              style={{
                left: `${(l / 1920) * 100}%`,
                top: `${(t / 1080) * 100}%`,
                width: `${((rr - l) / 1920) * 100}%`,
                height: `${((b - t) / 1080) * 100}%`,
              }}
              title={r.text ?? ""}
            />
          );
        })}
      </div>
      {uri && (
        <div className="relaunch">
          <a href={uri}>Relaunch: {uri}</a>
        </div>
      )}
      {textRegions.length > 0 && (
        <details>
          <summary>OCR text ({textRegions.length} regions)</summary>
          <pre>{textRegions.map((r) => r.text).join(" ")}</pre>
        </details>
      )}
    </div>
  );
}
