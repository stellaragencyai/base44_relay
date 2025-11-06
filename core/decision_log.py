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
  DECISION_LOG_QUEUE_MAX=10000           # max queued events before backpressure policy kicks in
  DECISION_LOG_DROP_OLDEST=true          # when queue is full, drop oldest (true) or newest (false)
  RUN_ID=<any string>                    # stamped on every event for session correlation
"""

from __future__ import annotations
import os, time, json, threading, queue, hashlib, datetime as dt, shutil, socket, atexit, traceback
from typing import Dict, Any, Optional, List

# Optional deps
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

# ----------------- env/config -----------------
_DIR = os.getenv("DECISION_LOG_DIR","./logs/decisions")
_FORMATS = [x.strip().lower() for x in (os.getenv("DECISION_LOG_FORMATS","jsonl").split(","))]
_FLUSH_SEC = max(1, int(os.getenv("DECISION_LOG_FLUSH_SEC","2")))
_ROTATE_DAYS = max(1, int(os.getenv("DECISION_LOG_ROTATE_DAYS","7")))
_QUEUE_MAX = max(1000, int(os.getenv("DECISION_LOG_QUEUE_MAX", "10000")))
_DROP_OLDEST = (os.getenv("DECISION_LOG_DROP_OLDEST","true").strip().lower() in {"1","true","yes","on"})
_RUN_ID = os.getenv("RUN_ID", "") or ""

_HOSTNAME = socket.gethostname()
_PID = os.getpid()

_q: "queue.Queue[Dict[str,Any]]" = queue.Queue(maxsize=_QUEUE_MAX)
_stop = threading.Event()
_worker_started = False
_lock = threading.RLock()
_context_lock = threading.RLock()

# Optional extra context that can be attached to every event
_extra_ctx: Dict[str, Any] = {}

# ----------------- paths/helpers -----------------
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
        os.makedirs(_DIR, exist_ok=True)
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
    except Exception:
        # rotation failure is non-fatal
        pass

def _hash_event(e: Dict[str,Any]) -> str:
    s = json.dumps(e, sort_keys=True, default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

# ----------------- lifecycle -----------------
def start_worker():
    global _worker_started
    with _lock:
        if _worker_started:
            return
        _worker_started = True
        t = threading.Thread(target=_run, name="decision-log-writer", daemon=True)
        t.start()
        # Ensure clean shutdown even on abrupt exit
        atexit.register(_atexit_flush)

def stop_worker():
    """Signal the writer to stop and flush any remaining events."""
    _stop.set()
    # Wake the thread if it's idle on .get(timeout=0.5)
    try:
        _q.put_nowait({"__ping__": True})
    except Exception:
        pass
    # Give it a moment to flush synchronously
    _final_drain_and_flush(timeout_sec=2.5)

def _atexit_flush():
    # Best-effort flush at process exit
    try:
        stop_worker()
    except Exception:
        pass

# ----------------- writer thread -----------------
def _run():
    buf: List[Dict[str,Any]] = []
    last_flush = time.time()
    while not _stop.is_set():
        try:
            try:
                item = _q.get(timeout=0.5)
                # Ignore internal ping
                if isinstance(item, dict) and item.get("__ping__"):
                    pass
                else:
                    buf.append(item)
            except queue.Empty:
                pass

            now = time.time()
            if buf and (now - last_flush >= _FLUSH_SEC):
                _flush(buf)
                buf.clear()
                last_flush = now
        except Exception:
            # Swallow log errors; never crash trading loop
            buf.clear()
            last_flush = time.time()

    # Final flush on stop
    try:
        if buf:
            _flush(buf)
    except Exception:
        pass

def _final_drain_and_flush(timeout_sec: float = 2.5):
    """Drain queue for a short window and flush synchronously."""
    t0 = time.time()
    batch: List[Dict[str,Any]] = []
    while (time.time() - t0) < timeout_sec:
        try:
            item = _q.get_nowait()
            if isinstance(item, dict) and item.get("__ping__"):
                continue
            batch.append(item)
        except queue.Empty:
            break
    if batch:
        try:
            _flush(batch)
        except Exception:
            pass

# ----------------- flush backends -----------------
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
            pp = _parquet_path(day)
            df = pd.DataFrame(batch)
            if not df.empty:
                if os.path.exists(pp):
                    # naive append: read + concat; small daily files keep this acceptable
                    old = pd.read_parquet(pp)
                    df = pd.concat([old, df], ignore_index=True)
                df.to_parquet(pp, index=False)
        except Exception:
            # ignore parquet errors; jsonl is primary
            pass

    _rotate()

# ----------------- public API -----------------
def set_context(**kwargs):
    """
    Attach extra static context to every subsequent event, e.g.:
      set_context(strategy='TrendPullback', version='1.4.2', sub_label='SUB7')
    """
    with _context_lock:
        _extra_ctx.update({k: v for k, v in kwargs.items() if v is not None})

def clear_context(keys: Optional[List[str]] = None):
    with _context_lock:
        if keys:
            for k in keys:
                _extra_ctx.pop(k, None)
        else:
            _extra_ctx.clear()

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
    with _context_lock:
        ctx = dict(_extra_ctx)  # shallow copy
    e = {
        "ts": ts,
        "level": level,
        "component": component,
        "event": event,
        "symbol": symbol.upper() if symbol else "",
        "account_uid": str(account_uid),
        "trade_id": trade_id or "",
        "payload": payload or {},
        "run_id": _RUN_ID,
        "host": _HOSTNAME,
        "pid": _PID,
    }
    # merge context under top-level and inside payload for convenience
    if ctx:
        e.update({k: v for k, v in ctx.items() if k not in e})
        # also surface inside payload under _ctx to avoid key collisions
        if isinstance(e["payload"], dict):
            e["payload"] = {**e["payload"], "_ctx": ctx}
    e["id"] = _hash_event(e)

    try:
        _q.put_nowait(e)
    except queue.Full:
        # Choose drop policy
        if _DROP_OLDEST:
            try:
                _ = _q.get_nowait()  # drop one oldest
                _q.put_nowait(e)
            except Exception:
                # if still full, drop newest to avoid blocking trade path
                pass
        # else drop newest silently

def log_event_sync(component: str,
                   event: str,
                   symbol: str,
                   account_uid: str,
                   payload: Optional[Dict[str,Any]] = None,
                   trade_id: Optional[str] = None,
                   level: str = "info") -> None:
    """
    Synchronous write for critical events. Avoid spamming this; it blocks the caller.
    """
    ts = dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    with _context_lock:
        ctx = dict(_extra_ctx)
    e = {
        "ts": ts,
        "level": level,
        "component": component,
        "event": event,
        "symbol": symbol.upper() if symbol else "",
        "account_uid": str(account_uid),
        "trade_id": trade_id or "",
        "payload": (payload or {}) | {"_ctx": ctx} if ctx else (payload or {}),
        "run_id": _RUN_ID,
        "host": _HOSTNAME,
        "pid": _PID,
    }
    e["id"] = _hash_event(e)
    # write immediately
    try:
        _flush([e])
    except Exception:
        # last-ditch: try jsonl-only direct append
        try:
            day = dt.date.today()
            base = _day_path(day)
            _ensure_dir(base)
            jp = _jsonl_path(day)
            with open(jp, "a", encoding="utf-8") as f:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        except Exception:
            # swallow; logging must never crash trading
            pass

def log_kv(component: str, event: str, **kv):
    """Convenience: log arbitrary key/value payload."""
    log_event(component, event, symbol=str(kv.pop("symbol", "") or ""),
              account_uid=str(kv.pop("account_uid", "")),
              payload=kv)

# Backwards-compat aliases
log = log_event
