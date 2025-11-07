#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Dashboard v0.1 — Read-only glass cockpit (HTMX refresh)
- Summary (equity, DD, breaker)
- Positions (main + subs via Relay if configured)
- Orders (reduce-only TP ladders)
- Health (bot heartbeats / stale markers)
- Alerts stream placeholder (reads recent notifier console echoes later)

Safe by design: no write endpoints.

.env (add if missing)
  DASH_HOST=127.0.0.1
  DASH_PORT=5000
  REFRESH_MS=3000
  RELAY_URL=https://<your-ngrok>
  RELAY_TOKEN=...
  SUB_UIDS=260417078,302355261,152304954,65986659,65986592,152499802
  TZ=America/Phoenix
"""
from __future__ import annotations
import os
from pathlib import Path
from flask import Flask, jsonify, render_template
from dotenv import load_dotenv

# Load layered env from project root
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

from dashboard.adapters.pnl import get_summary_payload
from dashboard.adapters.breaker_adapter import get_breaker_payload
from dashboard.adapters.positions import get_positions_payload
from dashboard.adapters.orders import get_orders_payload
from dashboard.adapters.health import get_health_payload

app = Flask(__name__, template_folder="templates", static_folder="static")

@app.route("/")
def index():
    refresh_ms = int(os.getenv("REFRESH_MS", "3000") or "3000")
    return render_template("index.html", refresh_ms=refresh_ms)

@app.route("/api/summary")
def api_summary():
    return jsonify(get_summary_payload())

@app.route("/api/breaker")
def api_breaker():
    return jsonify(get_breaker_payload())

@app.route("/api/positions")
def api_positions():
    return jsonify(get_positions_payload())

@app.route("/api/orders")
def api_orders():
    return jsonify(get_orders_payload())

@app.route("/api/health")
def api_health():
    return jsonify(get_health_payload())

def main():
    host = os.getenv("DASH_HOST", "127.0.0.1")
    port = int(os.getenv("DASH_PORT", "5000"))
    app.run(host=host, port=port, debug=False, threaded=True)

if __name__ == "__main__":
    main()
