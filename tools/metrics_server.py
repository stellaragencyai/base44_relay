#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools.metrics_server — minimalist Prometheus /metrics exporter.

Exports:
  base44_outcomes_total{setup_tag,won} counter
  base44_setup_prior{setup_tag} gauge (current alpha/(alpha+beta))

Run:
  python tools/metrics_server.py --port 9108
"""

from __future__ import annotations
import argparse, time, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import json, os

ROOT = Path(os.getcwd())
STATE_DIR = ROOT / "state"
OUTCOME_PATH = Path(os.getenv("OUTCOME_PATH", STATE_DIR / "outcomes.jsonl"))
MODEL_PATH = Path(os.getenv("MODEL_STATE_PATH", STATE_DIR / "model_state.json"))

_counts = {}   # (setup, won)->int
_lock = threading.RLock()

def _tail_outcomes():
    # very dumb tailer; good enough for now
    pos = 0
    while True:
        try:
            with OUTCOME_PATH.open("r", encoding="utf-8") as fh:
                fh.seek(pos, 0)
                for line in fh:
                    pos = fh.tell()
                    try:
                        obj = json.loads(line)
                        setup = str(obj.get("setup_tag") or "Unknown")
                        won = "true" if bool(obj.get("won")) else "false"
                        with _lock:
                            _counts[(setup, won)] = _counts.get((setup, won), 0) + 1
                    except Exception:
                        pass
        except FileNotFoundError:
            pass
        time.sleep(1.0)

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404); self.end_headers(); return
        payload = []
        with _lock:
            for (setup, won), v in _counts.items():
                payload.append(f'base44_outcomes_total{{setup_tag="{setup}",won="{won}"}} {v}')
        try:
            ms = json.loads(MODEL_PATH.read_text(encoding="utf-8"))
        except Exception:
            ms = {}
        for setup, d in (ms or {}).items():
            try:
                a, b = float(d.get("alpha", 0)), float(d.get("beta", 0))
                tot = max(1e-9, a+b)
                p = a/tot
                payload.append(f'base44_setup_prior{{setup_tag="{setup}"}} {p}')
            except Exception:
                continue
        body = "\n".join(payload) + "\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9108)
    args = ap.parse_args()
    t = threading.Thread(target=_tail_outcomes, daemon=True)
    t.start()
    srv = HTTPServer(("0.0.0.0", args.port), H)
    print(f"metrics on :{args.port}")
    srv.serve_forever()

if __name__ == "__main__":
    main()
