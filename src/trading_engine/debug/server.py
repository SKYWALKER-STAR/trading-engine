from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from os import getenv
from typing import Any
from urllib.parse import parse_qs, urlparse

from trading_engine.debug.dashboard import PositionDebugStore


HTML_PAGE = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Position Debug Dashboard</title>
    <style>
      :root {
        --bg: #f4f6f8;
        --panel: #ffffff;
        --text: #1f2937;
        --muted: #6b7280;
        --line: #e5e7eb;
        --primary: #0f766e;
        --primary-soft: #dff5f3;
        --warn: #a16207;
        --warn-soft: #fef3c7;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        padding: 20px;
        background: linear-gradient(180deg, #f7f9fb 0%, #eef2f7 100%);
        color: var(--text);
        font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
        overflow-x: hidden;
      }
      .dashboard {
        max-width: 1280px;
        margin: 0 auto;
      }
      .layout {
        display: grid;
        grid-template-columns: 300px 1fr;
        gap: 14px;
      }
      .layout > * { min-width: 0; }
      .panel {
        min-width: 0;
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 6px 16px rgba(15, 23, 42, 0.06);
      }
      body.refreshing .panel,
      body.refreshing .state-card,
      body.refreshing .node {
        animation: refreshPulse 380ms ease;
      }
      .page-header {
        grid-column: 1 / -1;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
      }
      .eyebrow {
        font-size: 11px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: var(--primary);
        margin-bottom: 4px;
      }
      h1, h2, h3 {
        margin: 0;
        font-weight: 600;
      }
      h3 {
        margin-top: 10px;
        margin-bottom: 8px;
      }
      .header-actions {
        display: flex;
        align-items: center;
        gap: 10px;
      }
      .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 7px 11px;
        border-radius: 999px;
        border: 1px solid #8fd5cd;
        background: var(--primary-soft);
        color: #0f4f4a;
        font-size: 12px;
        font-weight: 600;
      }
      .status-dot {
        width: 9px;
        height: 9px;
        border-radius: 50%;
        background: var(--primary);
        box-shadow: 0 0 0 0 rgba(15, 118, 110, 0.5);
        animation: liveDot 1.6s infinite;
      }
      body.paused .status-pill {
        border-color: #eab308;
        background: var(--warn-soft);
        color: #854d0e;
      }
      body.paused .status-dot {
        background: var(--warn);
        box-shadow: none;
        animation: none;
      }
      .last-updated {
        font-size: 12px;
        color: var(--muted);
      }
      .controls {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 8px;
      }
      input,
      button {
        height: 38px;
        border-radius: 8px;
        border: 1px solid #d1d5db;
        background: #ffffff;
        color: var(--text);
        padding: 0 12px;
        transition: border-color 0.2s ease, box-shadow 0.2s ease;
      }
      input {
        flex: 1 1 140px;
        min-width: 0;
      }
      input:focus,
      button:focus {
        outline: none;
        border-color: #0ea5a4;
        box-shadow: 0 0 0 3px rgba(14, 165, 164, 0.15);
      }
      button {
        cursor: pointer;
        font-weight: 600;
      }
      button.primary {
        background: #0f766e;
        border-color: #0f766e;
        color: #ffffff;
      }
      button.ghost {
        background: #ffffff;
      }
      .muted { color: var(--muted); }
      .badge {
        display: inline-block;
        margin-top: 8px;
        padding: 5px 9px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 600;
        color: #0f4f4a;
        background: var(--primary-soft);
        border: 1px solid #8fd5cd;
      }
      .badge.lifecycle-flat {
        background: #eef2ff;
        border-color: #c7d2fe;
        color: #3730a3;
      }
      .badge.lifecycle-long,
      .badge.lifecycle-open_long,
      .badge.lifecycle-opening_long,
      .badge.lifecycle-close_long,
      .badge.lifecycle-closing_long {
        background: #ecfdf5;
        border-color: #a7f3d0;
        color: #065f46;
      }
      .badge.lifecycle-short,
      .badge.lifecycle-open_short,
      .badge.lifecycle-opening_short,
      .badge.lifecycle-close_short,
      .badge.lifecycle-closing_short {
        background: #fff7ed;
        border-color: #fed7aa;
        color: #9a4d00;
      }
      .badge.lifecycle-opening_long,
      .badge.lifecycle-closing_long,
      .badge.lifecycle-opening_short,
      .badge.lifecycle-closing_short {
        background: #fffbeb;
        border-color: #fcd34d;
        color: #92400e;
      }
      .state-card {
        background: #f8fafc;
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 14px;
        margin-bottom: 12px;
        min-width: 0;
      }
      .chips {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }
      .chip {
        background: #e6fffb;
        border: 1px solid #99f6e4;
        color: #134e4a;
        border-radius: 999px;
        padding: 4px 9px;
        font-size: 12px;
        font-weight: 600;
      }
      .info-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
        margin-top: 14px;
      }
      .kpi {
        background: #ffffff;
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 10px;
      }
      .kpi .label {
        font-size: 11px;
        color: var(--muted);
        text-transform: uppercase;
      }
      .kpi .value {
        margin-top: 6px;
        font-size: 14px;
        font-weight: 600;
        overflow-wrap: anywhere;
      }
      .graph {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
        gap: 8px;
        margin: 12px 0;
      }
      .node {
        border: 1px solid var(--line);
        border-radius: 8px;
        background: #ffffff;
        color: #374151;
        padding: 9px 10px;
        font-size: 12px;
        font-weight: 600;
        text-align: center;
      }
      .node.active {
        border-color: #0f766e;
        background: #ecfdf5;
        color: #065f46;
      }
      .node.secondary {
        border-color: #facc15;
        background: #fffbeb;
        color: #854d0e;
      }
      .graph-meta {
        grid-column: 1 / -1;
      }
      .table-wrap {
        width: 100%;
        overflow-x: auto;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        min-width: 540px;
      }
      th,
      td {
        padding: 8px 4px;
        border-bottom: 1px solid var(--line);
        text-align: left;
        font-size: 13px;
        vertical-align: top;
      }
      tbody tr.recent td {
        background: rgba(15, 118, 110, 0.04);
      }
      .keys-table {
        min-width: 460px;
      }
      .keys-table td:nth-child(2) {
        word-break: break-all;
      }
      @keyframes liveDot {
        0% { box-shadow: 0 0 0 0 rgba(15, 118, 110, 0.45); }
        70% { box-shadow: 0 0 0 9px rgba(15, 118, 110, 0); }
        100% { box-shadow: 0 0 0 0 rgba(15, 118, 110, 0); }
      }
      @keyframes refreshPulse {
        0% { transform: translateY(0); }
        35% { transform: translateY(-1px); }
        100% { transform: translateY(0); }
      }
      @media (max-width: 980px) {
        .layout {
          grid-template-columns: 1fr;
        }
        .page-header {
          flex-direction: column;
          align-items: flex-start;
        }
        .header-actions {
          width: 100%;
          justify-content: space-between;
          flex-wrap: wrap;
        }
      }
      @media (max-width: 640px) {
        body {
          padding: 12px;
        }
        table {
          min-width: 460px;
        }
        .info-grid {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <div class="dashboard">
      <div class="layout">
        <header class="panel page-header">
          <div>
            <div class="eyebrow">Realtime monitor</div>
            <h1>Position Debug</h1>
          </div>
          <div class="header-actions">
            <div class="status-pill">
              <span class="status-dot"></span>
              <span id="status-text">Live updating</span>
            </div>
            <div class="last-updated" id="last-updated">Waiting for first update...</div>
          </div>
        </header>

        <aside class="panel">
          <div>
            <label class="muted" for="symbol">Symbol</label>
            <div class="controls">
              <input id="symbol" value="BTCUSDT" />
              <button id="refresh" class="primary">Refresh</button>
              <button id="toggle-live" class="ghost">Pause</button>
            </div>
          </div>
          <div style="margin-top: 18px;">
            <div class="muted">Current lifecycle</div>
            <div id="lifecycle" class="badge">flat</div>
          </div>
        </aside>

        <main class="panel">
          <div id="state" class="state-card"></div>

          <h3>Redis keys</h3>
          <div id="keys" class="state-card"></div>

          <h3>State machine view</h3>
          <div id="graph" class="graph"></div>

          <h3>Transition history</h3>
          <div class="table-wrap">
            <table class="history-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Reason</th>
                  <th>From</th>
                  <th>To</th>
                </tr>
              </thead>
              <tbody id="history"></tbody>
            </table>
          </div>
        </main>
      </div>
    </div>

    <script>
      const symbolInput = document.getElementById('symbol');
      const refreshButton = document.getElementById('refresh');
      const toggleLiveButton = document.getElementById('toggle-live');
      const statusText = document.getElementById('status-text');
      const lastUpdatedHost = document.getElementById('last-updated');
      const stateHost = document.getElementById('state');
      const historyHost = document.getElementById('history');
      const lifecycleHost = document.getElementById('lifecycle');
      const graphHost = document.getElementById('graph');
      const keysHost = document.getElementById('keys');

      const stateSequence = [
        'flat', 'open_long', 'opening_long', 'long', 'close_long', 'closing_long',
        'open_short', 'opening_short', 'short', 'close_short', 'closing_short'
      ];

      let autoRefreshEnabled = true;
      let autoRefreshTimer = null;

      function setLiveStatus(label, isLive) {
        statusText.textContent = label;
        document.body.classList.toggle('paused', !isLive);
      }

      function flashRefresh() {
        document.body.classList.remove('refreshing');
        void document.body.offsetWidth;
        document.body.classList.add('refreshing');
        window.setTimeout(() => document.body.classList.remove('refreshing'), 380);
      }

      function updateLastUpdated() {
        const timestamp = new Date().toLocaleTimeString([], {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        });
        lastUpdatedHost.textContent = `Last updated at ${timestamp}`;
      }

      function startAutoRefresh() {
        if (autoRefreshTimer) {
          clearInterval(autoRefreshTimer);
        }
        autoRefreshTimer = setInterval(() => {
          if (autoRefreshEnabled) {
            load();
          }
        }, 2000);
      }

      function lifecycleClassName(lifecycle) {
        const normalized = String(lifecycle || 'flat').trim().toLowerCase();
        if (!normalized) return 'lifecycle-flat';
        if (normalized.includes('long')) return normalized.includes('opening') || normalized.includes('closing') ? 'lifecycle-opening_long' : 'lifecycle-long';
        if (normalized.includes('short')) return normalized.includes('opening') || normalized.includes('closing') ? 'lifecycle-opening_short' : 'lifecycle-short';
        return 'lifecycle-flat';
      }

      function renderGraph(currentLifecycle, previousLifecycle, reason) {
        const nodes = stateSequence.map((name) => {
          const active = name === (currentLifecycle || 'flat');
          const previousMatch = previousLifecycle && name === previousLifecycle;
          return `
            <div class="node ${active ? 'active' : ''} ${previousMatch ? 'secondary' : ''}">
              ${name}
            </div>
          `;
        }).join('');

        graphHost.innerHTML = nodes + `
          <div class="kpi graph-meta">
            <div class="label">last reason</div>
            <div class="value">${reason || 'n/a'}</div>
          </div>
        `;
      }

      async function fetchJson(path) {
        const res = await fetch(path);
        if (!res.ok) {
          throw new Error(`Request failed: ${res.status}`);
        }
        return res.json();
      }

      function renderState(state) {
        if (!state) {
          stateHost.innerHTML = '<div class="muted">No state for this symbol yet.</div>';
          lifecycleHost.textContent = 'flat';
          lifecycleHost.className = 'badge lifecycle-flat';
          return;
        }

        const lifecycle = state.current_lifecycle || state.lifecycle || 'flat';
        lifecycleHost.textContent = lifecycle;
        lifecycleHost.className = `badge ${lifecycleClassName(lifecycle)}`;
        stateHost.innerHTML = `
          <div class="chips">
            <span class="chip">symbol: ${state.symbol}</span>
            <span class="chip">direction: ${state.direction || 'flat'}</span>
            <span class="chip">lifecycle: ${lifecycle}</span>
            <span class="chip">quantity: ${state.quantity ?? 0}</span>
          </div>
          <div class="info-grid">
            <div class="kpi">
              <div class="label">reason</div>
              <div class="value">${state.reason || 'n/a'}</div>
            </div>
            <div class="kpi">
              <div class="label">updated_at</div>
              <div class="value">${state.occurred_at || state.updated_at || 'n/a'}</div>
            </div>
            <div class="kpi">
              <div class="label">active order</div>
              <div class="value">${state.active_order_id || 'n/a'}</div>
            </div>
            <div class="kpi">
              <div class="label">direction change</div>
              <div class="value">${state.previous_direction || 'n/a'} -> ${state.direction || 'n/a'}</div>
            </div>
          </div>
        `;
      }

      function renderKeys(keysPayload, stateSourceKey) {
        if (!keysPayload || !keysPayload.keys) {
          keysHost.innerHTML = '<div class="muted">No key diagnostics available.</div>';
          return;
        }

        const keys = keysPayload.keys;
        const exists = keysPayload.exists || {};
        const entries = Object.entries(keys).map(([name, key]) => {
          const present = exists[name] ? 'yes' : 'no';
          const selected = stateSourceKey === key ? ' (active source)' : '';
          return `
            <tr>
              <td>${name}</td>
              <td>${key}${selected}</td>
              <td>${present}</td>
            </tr>
          `;
        }).join('');

        keysHost.innerHTML = `
          <div class="table-wrap">
            <table class="keys-table">
              <thead>
                <tr>
                  <th>name</th>
                  <th>key</th>
                  <th>exists</th>
                </tr>
              </thead>
              <tbody>${entries}</tbody>
            </table>
          </div>
        `;
      }

      function renderHistory(entries) {
        if (!entries || entries.length === 0) {
          historyHost.innerHTML = '<tr><td colspan="4" class="muted">No transitions recorded yet.</td></tr>';
          return;
        }

        historyHost.innerHTML = entries.map((entry, index) => `
          <tr class="${index === 0 ? 'recent' : ''}">
            <td>${entry.occurred_at}</td>
            <td>${entry.reason}</td>
            <td>${entry.previous_lifecycle}</td>
            <td>${entry.current_lifecycle}</td>
          </tr>
        `).join('');
      }

      async function load() {
        const symbol = symbolInput.value.trim() || 'BTCUSDT';
        try {
          const state = await fetchJson(`/api/state?symbol=${encodeURIComponent(symbol)}`);
          const history = await fetchJson(`/api/history?symbol=${encodeURIComponent(symbol)}`);
          const keys = await fetchJson(`/api/keys?symbol=${encodeURIComponent(symbol)}`);

          const currentState = state.state || state;
          const historyEntries = history.history || history;

          renderState(currentState);
          renderKeys(keys, state.state_source_key || null);
          renderGraph(
            currentState && (currentState.current_lifecycle || currentState.lifecycle || 'flat'),
            currentState && currentState.previous_lifecycle,
            currentState && currentState.reason,
          );
          renderHistory(historyEntries);

          flashRefresh();
          updateLastUpdated();
          setLiveStatus(autoRefreshEnabled ? 'Live updating' : 'Paused', autoRefreshEnabled);
        } catch (error) {
          stateHost.innerHTML = `<div class="muted">${error.message}</div>`;
          historyHost.innerHTML = '<tr><td colspan="4" class="muted">Unable to load state.</td></tr>';
          graphHost.innerHTML = '<div class="muted">No graph available.</div>';
          keysHost.innerHTML = '<div class="muted">Unable to load key diagnostics.</div>';
          setLiveStatus('Refresh failed', false);
        }
      }

      function toggleLiveUpdates() {
        autoRefreshEnabled = !autoRefreshEnabled;
        toggleLiveButton.textContent = autoRefreshEnabled ? 'Pause' : 'Resume';
        setLiveStatus(autoRefreshEnabled ? 'Live updating' : 'Paused', autoRefreshEnabled);

        if (autoRefreshEnabled) {
          load();
        }
      }

      refreshButton.addEventListener('click', load);
      toggleLiveButton.addEventListener('click', toggleLiveUpdates);
      symbolInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          load();
        }
      });

      setLiveStatus('Live updating', true);
      load();
      startAutoRefresh();
    </script>
  </body>
</html>
"""


class _PositionDebugHandler(BaseHTTPRequestHandler):
    server_version = "PositionDebug/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self._send_json_or_html(HTML_PAGE, content_type="text/html; charset=utf-8")
            return

        if parsed.path == "/api/state":
            symbol = parse_qs(parsed.query).get("symbol", ["BTCUSDT"])[0]
            state, state_source_key = self.server.store.get_state_with_source(symbol)
            payload = {"symbol": symbol, "state": state, "state_source_key": state_source_key}
            self._send_json(payload)
            return

        if parsed.path == "/api/history":
            symbol = parse_qs(parsed.query).get("symbol", ["BTCUSDT"])[0]
            payload = {"symbol": symbol, "history": self.server.store.get_history(symbol)}
            self._send_json(payload)
            return

        if parsed.path == "/api/keys":
            symbol = parse_qs(parsed.query).get("symbol", ["BTCUSDT"])[0]
            payload = self.server.store.get_key_info(symbol)
            self._send_json(payload)
            return

        self._send_json({"error": "not_found"}, status=404)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, payload: dict[str, Any], *, status: int = 200) -> None:
        encoded = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_json_or_html(self, content: str, *, content_type: str) -> None:
        encoded = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def run(argv: list[str] | None = None) -> None:
    host = getenv("POSITION_DEBUG_HOST", "127.0.0.1")
    port = int(getenv("POSITION_DEBUG_PORT", "8001"))
    redis_url = getenv("POSITION_REDIS_URL", "redis://127.0.0.1:6379/0")
    key_prefix = getenv(
        "POSITION_VIEW_KEY_PREFIX",
        getenv("POSITION_REDIS_KEY_PREFIX", "binance:position:usdt_futures"),
    )

    store = PositionDebugStore(redis_url=redis_url, key_prefix=key_prefix)
    server = ThreadingHTTPServer((host, port), _PositionDebugHandler)
    server.store = store
    print(f"Position debug dashboard running at http://{host}:{port}/")
    server.serve_forever()


if __name__ == "__main__":
    run()
