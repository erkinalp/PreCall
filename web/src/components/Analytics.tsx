// SPDX-License-Identifier: GPL-2.0-only
import { useEffect, useState } from "react";
import { appAnalytics, webAnalytics, AppDwell, WebDwell } from "../api";

function Bar({ label, ms, max }: { label: string; ms: number; max: number }) {
  const pct = max ? Math.max(2, (ms / max) * 100) : 0;
  const mins = Math.round(ms / 60000);
  return (
    <div className="bar-row">
      <span className="bar-label">{label}</span>
      <div className="bar-track">
        <div className="bar-fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="bar-value">{mins}m</span>
    </div>
  );
}

export default function Analytics({ client }: { client: string }) {
  const [apps, setApps] = useState<AppDwell[]>([]);
  const [web, setWeb] = useState<WebDwell[]>([]);

  useEffect(() => {
    appAnalytics(client)
      .then((a) => setApps(a.dwell))
      .catch(() => setApps([]));
    webAnalytics(client)
      .then(setWeb)
      .catch(() => setWeb([]));
  }, [client]);

  const appAgg = new Map<string, number>();
  for (const d of apps) appAgg.set(d.name, (appAgg.get(d.name) ?? 0) + d.dwell_ms);
  const appTop = [...appAgg.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12);
  const appMax = appTop[0]?.[1] ?? 0;

  const webAgg = new Map<string, number>();
  for (const d of web) webAgg.set(d.domain, (webAgg.get(d.domain) ?? 0) + d.dwell_ms);
  const webTop = [...webAgg.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12);
  const webMax = webTop[0]?.[1] ?? 0;

  return (
    <div className="analytics">
      <section>
        <h2>App dwell time</h2>
        {appTop.length === 0 && <p className="empty">No app data yet.</p>}
        {appTop.map(([name, ms]) => (
          <Bar key={name} label={name} ms={ms} max={appMax} />
        ))}
      </section>
      <section>
        <h2>Web dwell time</h2>
        {webTop.length === 0 && <p className="empty">No web data yet.</p>}
        {webTop.map(([domain, ms]) => (
          <Bar key={domain} label={domain} ms={ms} max={webMax} />
        ))}
      </section>
    </div>
  );
}
