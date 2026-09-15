from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from os import getenv
from typing import Any
from urllib.parse import parse_qs, urlparse

from trading_engine.debug.dashboard import PositionDebugStore


HTML_PAGE = """<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Position Debug Dashboard</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; background: #0f172a; color: #e2e8f0; }
      .layout { display: grid; grid-template-columns: 280px 1fr; gap: 20px; }
      .panel { background: #111827; border: 1px solid #334155; border-radius: 12px; padding: 16px; }
      h1 { margin-top: 0; }
      input, button { padding: 10px; border-radius: 8px; border: 1px solid #475569; background: #0b1220; color: white; }
      button { margin-left: 8px; cursor: pointer; }
      .state-card { background: #1e293b; border-radius: 12px; padding: 16px; margin-bottom: 12px; }
      .chips { display: flex; gap: 8px; flex-wrap: wrap; }
      .chip { background: #0ea5e9; color: #082f49; border-radius: 999px; padding: 6px 10px; font-size: 12px; font-weight: bold; }
      table { width: 100%; border-collapse: collapse; margin-top: 12px; }
      th, td { text-align: left; border-bottom: 1px solid #334155; padding: 8px 0; }
      .badge { padding: 6px 8px; border-radius: 999px; background: #14532d; color: #dcfce7; font-size: 11px; }
      .muted { color: #94a3b8; }
      ul { padding-left: 18px; }
    </style>
  </head>
  <body>
    <div class=\"layout\">
      <aside class=\"panel\">
        <h1>Position Debug</h1>
        <div>
          <label class=\"muted\" for=\"symbol\">Symbol</label>
          <div style=\"display:flex; margin-top: 8px;\">
            <input id=\"symbol\" value=\"BTCUSDT\" />
            <button id=\"refresh\">Refresh</button>
          </div>
        </div>
        <div style=\"margin-top: 20px;\">
          <div class=\"muted\">Live lifecycle</div>
          <div id=\"lifecycle\" class=\"badge\" style=\"display:inline-block; margin-top:8px;\">flat</div>
        </div>
      </aside>
      <main class=\"panel\">
        <div id=\"state\" class=\"state-card\"></div>
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
          <tbody id=\"history\"></tbody>
        </table>
      </main>
    </div>

    <script>
      const symbolInput = document.getElementById('symbol');
      const refreshButton = document.getElementById('refresh');
      const stateHost = document.getElementById('state');
      const historyHost = document.getElementById('history');
      const lifecycleHost = document.getElementById('lifecycle');

      async function fetchJson(path) {
        const res = await fetch(path);
        if (!res.ok) {
          throw new Error(`Request failed: ${res.status}`);
        }
        return res.json();
      }

      function renderState(state) {
        if (!state) {
          stateHost.innerHTML = '<div class=\"muted\">No state for this symbol yet.</div>';
          lifecycleHost.textContent = 'flat';
          return;
        }

        lifecycleHost.textContent = state.current_lifecycle || state.lifecycle || 'flat';
        stateHost.innerHTML = `
          <div class=\"chips\">
            <span class=\"chip\">symbol: ${state.symbol}</span>
            <span class=\"chip\">direction: ${state.direction || 'flat'}</span>
            <span class=\"chip\">lifecycle: ${state.current_lifecycle || state.lifecycle || 'flat'}</span>
            <span class=\"chip\">quantity: ${state.quantity ?? 0}</span>
          </div>
          <div style=\"margin-top: 12px;\">\
            <div class=\"muted\">reason</div>\
            <div><strong>${state.reason || 'n/a'}</strong></div>\
          </div>
          <div style=\"margin-top: 12px;\">\
            <div class=\"muted\">updated_at</div>\
            <div>${state.occurred_at || state.updated_at || 'n/a'}</div>\
          </div>
        `;
      }

      function renderHistory(entries) {
        if (!entries || entries.length === 0) {
          historyHost.innerHTML = '<tr><td colspan=\"4\" class=\"muted\">No transitions recorded yet.</td></tr>';
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

      async function load() {
        const symbol = symbolInput.value.trim() || 'BTCUSDT';
        try {
          const state = await fetchJson(`/api/state?symbol=${encodeURIComponent(symbol)}`);
          const history = await fetchJson(`/api/history?symbol=${encodeURIComponent(symbol)}`);
          renderState(state.state || state);
          renderHistory(history.history || history);
        } catch (error) {
          stateHost.innerHTML = `<div class=\"muted\">${error.message}</div>`;
          historyHost.innerHTML = '<tr><td colspan=\"4\" class=\"muted\">Unable to load state.</td></tr>';
        }
      }

      refreshButton.addEventListener('click', load);
      symbolInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          load();
        }
      });
      load();
      setInterval(load, 2000);
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
            state = self.server.store.get_state(symbol)
            payload = {"symbol": symbol, "state": state}
            self._send_json(payload)
            return

        if parsed.path == "/api/history":
            symbol = parse_qs(parsed.query).get("symbol", ["BTCUSDT"])[0]
            payload = {"symbol": symbol, "history": self.server.store.get_history(symbol)}
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
    key_prefix = getenv("POSITION_REDIS_KEY_PREFIX", "position")

    store = PositionDebugStore(redis_url=redis_url, key_prefix=key_prefix)
    server = ThreadingHTTPServer((host, port), _PositionDebugHandler)
    server.store = store
    print(f"Position debug dashboard running at http://{host}:{port}/")
    server.serve_forever()


if __name__ == "__main__":
    run()
