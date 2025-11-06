#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/decision_log.py
Structured decision/event logging for Base44.

Writes:
  - JSON Lines to {DECISION_LOG_DIR}/YYYY-MM-DD/decisions.jsonl
  - Optional Parquet to {DECISION_LOG_DIR}/YYYY-MM-DD/decisions.parquet (if pyarrow/fastparquet installed)

Env:
  DECISION_LOG_DIR=./logs/decisions
  DECISION_LOG_FORMATS=jsonl,parquet
  DECISION_LOG_FLUSH_SEC=2
  DECISION_LOG_ROTATE_DAYS=7
"""

import os, time, json, threading, queue, hashlib, datetime as dt, shutil
from typing import Dict, Any, Optional, List

try:
    import pandas as pd   # type: ignore
    _HAVE_PANDAS = True
except Exception:
    _HAVE_PANDAS = False

try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore
    _HAVE_PARQUET = True
except Exception:
    try:
        import fastparquet  # type: ignore
        _HAVE_PARQUET = True
    except Exception:
        _HAVE_PARQUET = False

_DIR = os.getenv("DECISION_LOG_DIR","./logs/decisions")
_FORMATS = [x.strip().lower() for x in (os.getenv("DECISION_LOG_FORMATS","jsonl").split(","))]
_FLUSH_SEC = max(1, int(os.getenv("DECISION_LOG_FLUSH_SEC","2")))
_ROTATE_DAYS = max(1, int(os.getenv("DECISION_LOG_ROTATE_DAYS","7")))

_q: "queue.Queue[Dict[str,Any]]" = queue.Queue(maxsize=10000)
_stop = threading.Event()
_worker_started = False
_lock = threading.RLock()

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def _day_path(day: dt.date) -> str:
    return os.path.join(_DIR, day.strftime("%Y-%m-%d"))

def _jsonl_path(day: dt.date) -> str:
    return os.path.join(_day_path(day), "decisions.jsonl")

def _parquet_path(day: dt.date) -> str:
    return os.path.join(_day_path(day), "decisions.parquet")

def _rotate():
    try:
        for name in os.listdir(_DIR):
            dpath = os.path.join(_DIR, name)
            if not os.path.isdir(dpath): 
                continue
            try:
                d = dt.datetime.strptime(name, "%Y-%m-%d").date()
            except Exception:
                continue
            if (dt.date.today() - d).days > _ROTATE_DAYS:
                shutil.rmtree(dpath, ignore_errors=True)
    except FileNotFoundError:
        pass

def _hash_event(e: Dict[str,Any]) -> str:
    s = json.dumps(e, sort_keys=True, default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def start_worker():
    global _worker_started
    with _lock:
        if _worker_started:
            return
        _worker_started = True
        t = threading.Thread(target=_run, name="decision-log-writer", daemon=True)
        t.start()

def stop_worker():
    _stop.set()

def _run():
    buf: List[Dict[str,Any]] = []
    last_flush = time.time()
    while not _stop.is_set():
        try:
            try:
                item = _q.get(timeout=0.5)
                buf.append(item)
            except queue.Empty:
                pass

            now = time.time()
            if buf and (now - last_flush >= _FLUSH_SEC):
                _flush(buf)
                buf.clear()
                last_flush = now
        except Exception as e:
            # Swallow log errors; never crash trading
            buf.clear()
            last_flush = time.time()

def _flush(batch: List[Dict[str,Any]]):
    day = dt.date.today()
    base = _day_path(day)
    _ensure_dir(base)

    if "jsonl" in _FORMATS:
        jp = _jsonl_path(day)
        with open(jp, "a", encoding="utf-8") as f:
            for e in batch:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")

    if "parquet" in _FORMATS and _HAVE_PANDAS and _HAVE_PARQUET:
        try:
            # append mode parquet: write partitioned by day, simple concat for now
            pp = _parquet_path(day)
            df = pd.DataFrame(batch)
            if os.path.exists(pp):
                # naive append
                old = pd.read_parquet(pp)
                df = pd.concat([old, df], ignore_index=True)
            df.to_parquet(pp, index=False)
        except Exception:
            # ignore parquet errors; jsonl is primary
            pass

    _rotate()

def log_event(component: str,
              event: str,
              symbol: str,
              account_uid: str,
              payload: Optional[Dict[str,Any]] = None,
              trade_id: Optional[str] = None,
              level: str = "info") -> None:
    """
    component: 'signal' | 'executor' | 'reconciler' | 'guard' | 'tp_manager' | 'relay' | 'human'
    event:     short string e.g. 'signal_ok', 'order_create', 'reprice', 'blocked', 'sl_placed'
    payload:   dict with fields specific to event (prices, qty, reasons, thresholds, etc.)
    """
    start_worker()
    ts = dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    e = {
        "ts": ts,
        "level": level,
        "component": component,
        "event": event,
        "symbol": symbol.upper() if symbol else "",
        "account_uid": str(account_uid),
        "trade_id": trade_id or "",
        "payload": payload or {}
    }
    e["id"] = _hash_event(e)
    try:
        _q.put_nowait(e)
    except queue.Full:
        # drop oldest behavior could be added; for now, drop this event
        pass
