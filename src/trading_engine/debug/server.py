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
        --bg: #08111f;
        --panel: #111d2e;
        --muted: #9cadc5;
        --primary: #38bdf8;
        --success: #34d399;
        --warn: #fbbf24;
        --danger: #f87171;
        --border: #22314a;
        --node: #0f172a;
      }
      * { box-sizing: border-box; }
      body {
        font-family: Arial, sans-serif; margin: 20px; background: linear-gradient(180deg, #08111f 0%, #0d1728 100%);
        color: #e2e8f0;
      }
      body.refreshing .panel, body.refreshing .state-card, body.refreshing .node {
        animation: refreshPulse 0.6s ease;
      }
      .layout { display: grid; grid-template-columns: 320px 1fr; gap: 20px; }
      .panel {
        background: rgba(17, 29, 46, 0.95); border: 1px solid var(--border); border-radius: 14px; padding: 16px;
        box-shadow: 0 16px 35px rgba(15, 23, 42, 0.25);
      }
      .page-header {
        grid-column: 1 / -1; display: flex; justify-content: space-between; align-items: center; gap: 16px;
      }
      .eyebrow {
        font-size: 11px; letter-spacing: 0.12em; text-transform: uppercase; color: var(--primary); margin-bottom: 6px;
      }
      h1, h2, h3 { margin-top: 0; }
      .header-actions { display: flex; align-items: center; gap: 12px; }
      .status-pill {
        display: inline-flex; align-items: center; gap: 8px; padding: 8px 12px; border-radius: 999px;
        background: rgba(52, 211, 153, 0.12); border: 1px solid rgba(52, 211, 153, 0.35); color: #d1fae5;
        font-size: 12px; font-weight: bold;
      }
      .status-dot {
        width: 10px; height: 10px; border-radius: 50%; background: var(--success); box-shadow: 0 0 0 0 rgba(52, 211, 153, 0.7);
        animation: liveDot 1.8s infinite;
      }
      body.paused .status-pill {
        background: rgba(251, 191, 36, 0.1); border-color: rgba(251, 191, 36, 0.35); color: #fef3c7;
      }
      body.paused .status-dot {
        background: var(--warn); box-shadow: none; animation: none;
      }
      .last-updated { font-size: 12px; color: var(--muted); }
      input, button {
        padding: 10px 12px; border-radius: 8px; border: 1px solid #475569; background: #0b1220; color: white;
        transition: all 0.2s ease;
      }
      button {
        margin-left: 8px; cursor: pointer; min-width: 102px;
      }
      button:hover { border-color: var(--primary); filter: brightness(1.08); }
      .ghost {
        background: rgba(56, 189, 248, 0.08); border-color: rgba(56, 189, 248, 0.35); color: #bae6fd;
      }
      .state-card { background: #162338; border-radius: 12px; padding: 16px; margin-bottom: 12px; }
      .chips { display: flex; gap: 8px; flex-wrap: wrap; }
      .chip { background: var(--primary); color: #082f49; border-radius: 999px; padding: 6px 10px; font-size: 12px; font-weight: bold; }
      .badge { padding: 6px 8px; border-radius: 999px; background: #14532d; color: #dcfce7; font-size: 11px; font-weight: bold; }
      .muted { color: var(--muted); }
      table { width: 100%; border-collapse: collapse; margin-top: 12px; }
      th, td { text-align: left; border-bottom: 1px solid var(--border); padding: 8px 0; }
      .graph { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; margin: 18px 0; }
      .node {
        min-width: 120px; padding: 12px 14px; border-radius: 12px; border: 1px solid var(--border);
        background: var(--node); text-align: center; font-weight: bold; color: #e2e8f0;
      }
      .node.active { border-color: var(--success); box-shadow: 0 0 0 2px rgba(52,211,153,0.35); }
      .node.secondary { border-color: var(--warn); }
      .arrow { color: var(--primary); font-size: 22px; }
      .info-grid { display: grid; grid-template-columns: repeat(2, minmax(150px, 1fr)); gap: 10px; }
      .kpi { background: #0f172a; border-radius: 10px; padding: 10px; border: 1px solid var(--border); }
      .kpi .label { font-size: 11px; color: var(--muted); text-transform: uppercase; }
      .kpi .value { font-size: 18px; font-weight: bold; margin-top: 6px; }
      @keyframes liveDot {
        0% { box-shadow: 0 0 0 0 rgba(52, 211, 153, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(52, 211, 153, 0); }
        100% { box-shadow: 0 0 0 0 rgba(52, 211, 153, 0); }
      }
      @keyframes refreshPulse {
        0% { transform: translateY(0); box-shadow: 0 0 0 rgba(56,189,248,0); }
        30% { transform: translateY(-2px); box-shadow: 0 0 20px rgba(56,189,248,0.35); }
        100% { transform: translateY(0); box-shadow: 0 0 0 rgba(56,189,248,0); }
      }
      @media (max-width: 900px) {
        .layout { grid-template-columns: 1fr; }
        .page-header { flex-direction: column; align-items: flex-start; }
      }
    </style>
  </head>
  <body>
    <div class="layout">
      <header class="panel page-header">
        <div>
          <div class="eyebrow">Market observability</div>
          <h1>Position Debug</h1>
        </div>
        <div class="header-actions">
          <div class="status-pill" id="status-pill">
            <span class="status-dot"></span>
            <span id="status-text">Live updating</span>
          </div>
          <div class="last-updated" id="last-updated">Waiting for first update…</div>
        </div>
      </header>

      <aside class="panel">
        <div>
          <label class="muted" for="symbol">Symbol</label>
          <div style="display:flex; margin-top: 8px; align-items: center;">
            <input id="symbol" value="BTCUSDT" />
            <button id="refresh">Refresh</button>
            <button id="toggle-live" class="ghost">Pause</button>
          </div>
        </div>
        <div style="margin-top: 20px;">
          <div class="muted">Current lifecycle</div>
          <div id="lifecycle" class="badge" style="display:inline-block; margin-top:8px;">flat</div>
        </div>
      </aside>
      <main class="panel">
        <div id="state" class="state-card"></div>

        <h3>Redis keys</h3>
        <div id="keys" class="state-card"></div>

        <h3>State machine view</h3>
        <div id="graph" class="graph"></div>

        <h3>Transition history</h3>
        <table>
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
      </main>
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
        window.setTimeout(() => document.body.classList.remove('refreshing'), 500);
      }

      function updateLastUpdated() {
        const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        lastUpdatedHost.textContent = `Last updated at ${timestamp}`;
      }

      function startAutoRefresh() {
        if (autoRefreshTimer) {
          clearInterval(autoRefreshTimer);
        }
        autoRefreshTimer = setInterval(() => {
          if (autoRefreshEnabled) {
            load({ silent: true });
          }
        }, 2000);
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
        }).join('<div class="arrow">→</div>');

        graphHost.innerHTML = nodes + `
          <div class="kpi" style="min-width: 180px; margin-left: 8px;">
            <div class="label">last reason</div>
            <div class="value" style="font-size: 14px;">${reason || 'n/a'}</div>
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
          return;
        }

        const lifecycle = state.current_lifecycle || state.lifecycle || 'flat';
        lifecycleHost.textContent = lifecycle;
        stateHost.innerHTML = `
          <div class="chips">
            <span class="chip">symbol: ${state.symbol}</span>
            <span class="chip">direction: ${state.direction || 'flat'}</span>
            <span class="chip">lifecycle: ${lifecycle}</span>
            <span class="chip">quantity: ${state.quantity ?? 0}</span>
          </div>
          <div class="info-grid" style="margin-top: 16px;">
            <div class="kpi">
              <div class="label">reason</div>
              <div class="value" style="font-size: 14px;">${state.reason || 'n/a'}</div>
            </div>
            <div class="kpi">
              <div class="label">updated_at</div>
              <div class="value" style="font-size: 14px;">${state.occurred_at || state.updated_at || 'n/a'}</div>
            </div>
            <div class="kpi">
              <div class="label">active order</div>
              <div class="value" style="font-size: 14px;">${state.active_order_id || 'n/a'}</div>
            </div>
            <div class="kpi">
              <div class="label">direction change</div>
              <div class="value" style="font-size: 14px;">${state.previous_direction || 'n/a'} → ${state.direction || 'n/a'}</div>
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
          <table>
            <thead>
              <tr>
                <th>name</th>
                <th>key</th>
                <th>exists</th>
              </tr>
            </thead>
            <tbody>${entries}</tbody>
          </table>
        `;
      }

      function renderHistory(entries) {
        if (!entries || entries.length === 0) {
          historyHost.innerHTML = '<tr><td colspan="4" class="muted">No transitions recorded yet.</td></tr>';
          return;
        }

        historyHost.innerHTML = entries.map((entry) => `
          <tr>
            <td>${entry.occurred_at}</td>
            <td>${entry.reason}</td>
            <td>${entry.previous_lifecycle}</td>
            <td>${entry.current_lifecycle}</td>
          </tr>
        `).join('');
      }

      async function load({ silent = false } = {}) {
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

          if (!silent) {
            flashRefresh();
          }
          updateLastUpdated();
          setLiveStatus('Live updating', true);
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
          load({ silent: true });
        }
      }

      refreshButton.addEventListener('click', () => load({ silent: false }));
      toggleLiveButton.addEventListener('click', toggleLiveUpdates);
      symbolInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          load({ silent: false });
        }
      });

      setLiveStatus('Live updating', true);
      load({ silent: true });
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
