// SPDX-License-Identifier: GPL-2.0-only
import { useEffect, useState } from "react";
import { listClients, ClientInfo } from "./api";
import Timeline from "./components/Timeline";
import Search from "./components/Search";
import Analytics from "./components/Analytics";

type Tab = "timeline" | "search" | "analytics";

export default function App() {
  const [clients, setClients] = useState<ClientInfo[]>([]);
  const [client, setClient] = useState<string>("");
  const [tab, setTab] = useState<Tab>("timeline");
  const [error, setError] = useState<string>("");

  useEffect(() => {
    listClients()
      .then((cs) => {
        setClients(cs);
        if (cs.length && !client) setClient(cs[0].client_id);
      })
      .catch((e) => setError(String(e)));
    const t = setInterval(() => {
      listClients().then(setClients).catch(() => {});
    }, 15000);
    return () => clearInterval(t);
  }, []);

  return (
    <div className="app">
      <header>
        <h1>Precall</h1>
        <nav>
          {(["timeline", "search", "analytics"] as Tab[]).map((t) => (
            <button
              key={t}
              className={tab === t ? "active" : ""}
              onClick={() => setTab(t)}
            >
              {t[0].toUpperCase() + t.slice(1)}
            </button>
          ))}
        </nav>
        <select value={client} onChange={(e) => setClient(e.target.value)}>
          {clients.length === 0 && <option value="">no clients enrolled</option>}
          {clients.map((c) => (
            <option key={c.client_id} value={c.client_id}>
              {c.hostname} ({c.client_id.slice(0, 8)})
            </option>
          ))}
        </select>
      </header>
      {error && <div className="error">{error}</div>}
      <main>
        {!client ? (
          <p className="empty">
            Enroll a client to see captures. On a workstation run
            <code> precall-client enroll --server HOST:PORT --psk … </code>
          </p>
        ) : tab === "timeline" ? (
          <Timeline client={client} />
        ) : tab === "search" ? (
          <Search client={client} />
        ) : (
          <Analytics client={client} />
        )}
      </main>
    </div>
  );
}
