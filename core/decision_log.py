#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/decision_log.py — low-latency structured decision/event logging (Base44)

Writes:
  • JSONL shards at {DECISION_LOG_DIR}/jsonl/YYYY/MM/DD.jsonl
  • Optional Parquet shards at {DECISION_LOG_DIR}/parquet/y=YYYY/m=MM/d=DD/part-*.parquet
    (requires pyarrow or fastparquet; no read-modify-write)

Keeps your favorites:
  • Async queue with backpressure (drop-oldest or drop-newest)
  • Extra static context via set_context(...)
  • RUN_ID, host, pid, stable event id hash
  • Synchronous API for critical events

Env:
  DECISION_LOG_DIR=./logs/decisions
  DECISION_LOG_FORMATS=jsonl,parquet
  DECISION_LOG_FLUSH_SEC=2
  DECISION_LOG_ROTATE_DAYS=7
  DECISION_LOG_QUEUE_MAX=10000
  DECISION_LOG_DROP_OLDEST=true
  RUN_ID=<string>

Public API:
  - set_context(**kv), clear_context(keys=None)
  - log_event(component, event, symbol, account_uid, payload=None, trade_id=None, level="info")
  - log_event_sync(...)
  - flush_now(), close()
"""

from __future__ import annotations
import os, io, json, time, atexit, socket, hashlib, threading, queue
import datetime as dt
from typing import Dict, Any, Optional, List

# Settings-aware paths and timezone
try:
    from core.config import settings
except Exception:
    settings = None  # type: ignore

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore

# ---------------- Env ----------------
def _env_str(k: str, d: str) -> str:
    v = os.getenv(k)
    return d if v is None else str(v)

def _env_int(k: str, d: int) -> int:
    v = os.getenv(k)
    try:
        return int(v) if v is not None else d
    except Exception:
        return d

# Base directory defaults to settings.DIR_LOGS/decisions if available
_default_dir = None
if settings and getattr(settings, "DIR_LOGS", None):
    _default_dir = os.path.join(str(settings.DIR_LOGS), "decisions")

LOG_DIR = _env_str("DECISION_LOG_DIR", _default_dir or "./logs/decisions")
FORMATS = [s.strip().lower() for s in _env_str("DECISION_LOG_FORMATS", "jsonl,parquet").split(",") if s.strip()]
if not FORMATS:
    FORMATS = ["jsonl"]
FLUSH_SEC = max(1, _env_int("DECISION_LOG_FLUSH_SEC", 2))
ROTATE_DAYS = max(1, _env_int("DECISION_LOG_ROTATE_DAYS", 7))
QUEUE_MAX = max(1000, _env_int("DECISION_LOG_QUEUE_MAX", 10000))
DROP_OLDEST = _env_str("DECISION_LOG_DROP_OLDEST", "true").lower() in {"1","true","yes","on"}
RUN_ID = _env_str("RUN_ID", "")

HOSTNAME = socket.gethostname()
PID = os.getpid()

# Optional parquet engines (no pandas requirement if pyarrow is present)
_parquet_ok = False
_parquet_engine = None
try:
    import pyarrow as _pa               # type: ignore
    import pyarrow.parquet as _papq     # type: ignore
    _parquet_ok = True
    _parquet_engine = "pyarrow"
except Exception:
    try:
        import fastparquet as _fp       # type: ignore
        import pandas as _pd            # fastparquet needs pandas
        _parquet_ok = True
        _parquet_engine = "fastparquet"
    except Exception:
        _parquet_ok = False

# ---------------- Internals ----------------
_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)
_stop = threading.Event()
_worker_started = False
_lock = threading.RLock()
_ctx_lock = threading.RLock()
_extra_ctx: Dict[str, Any] = {}

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

# ensure base log dir exists early (tiny hardening)
_ensure_dir(LOG_DIR)

def _now_tz(ts: Optional[float] = None) -> dt.datetime:
    val = ts if ts is not None else time.time()
    if settings and getattr(settings, "TZ", None) and ZoneInfo:
        try:
            return dt.datetime.fromtimestamp(val, ZoneInfo(settings.TZ))
        except Exception:
            pass
    # fallback localtime
    return dt.datetime.fromtimestamp(val)

def _today_parts(ts: Optional[float] = None):
    d = _now_tz(ts)
    return d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")

def _jsonl_path(ts: Optional[float] = None) -> str:
    y, m, d = _today_parts(ts)
    base = os.path.join(LOG_DIR, "jsonl", y, m)
    _ensure_dir(base)
    return os.path.join(base, f"{d}.jsonl")

def _parquet_dir(ts: Optional[float] = None) -> str:
    y, m, d = _today_parts(ts)
    base = os.path.join(LOG_DIR, "parquet", f"y={y}", f"m={m}", f"d={d}")
    _ensure_dir(base)
    return base

def _event_hash(e: Dict[str, Any]) -> str:
    # stable hash for dedupe/correlation; payload order-agnostic
    try:
        s = json.dumps(e, sort_keys=True, default=str, ensure_ascii=False)
    except Exception:
        # desperate fallback: drop payload
        safe = dict(e)
        safe["payload"] = "<unserializable>"
        s = json.dumps(safe, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def _prune_old_jsonl():
    try:
        cutoff = dt.datetime.utcnow() - dt.timedelta(days=ROTATE_DAYS)
        y_cut, m_cut, d_cut = int(cutoff.strftime("%Y")), int(cutoff.strftime("%m")), int(cutoff.strftime("%d"))
        base = os.path.join(LOG_DIR, "jsonl")
        if not os.path.isdir(base):
            return
        for ydir in list(os.scandir(base)):
            if not ydir.is_dir():
                continue
            try:
                y = int(ydir.name)
            except Exception:
                continue
            for mdir in list(os.scandir(ydir.path)):
                if not mdir.is_dir():
                    continue
                try:
                    m = int(mdir.name)
                except Exception:
                    continue
                for f in list(os.scandir(mdir.path)):
                    if not f.is_file() or not f.name.endswith(".jsonl"):
                        continue
                    try:
                        d = int(f.name.replace(".jsonl", ""))
                        if dt.datetime(y, m, d) < cutoff:
                            os.remove(f.path)
                    except Exception:
                        continue
                # clean empty month dirs
                try:
                    if not any(os.scandir(mdir.path)):
                        os.rmdir(mdir.path)
                except Exception:
                    pass
            # clean empty year dirs
            try:
                if not any(os.scandir(ydir.path)):
                    os.rmdir(ydir.path)
            except Exception:
                pass
    except Exception:
        pass

def _prune_old_parquet():
    if not _parquet_ok:
        return
    try:
        cutoff = dt.datetime.utcnow() - dt.timedelta(days=ROTATE_DAYS)
        base = os.path.join(LOG_DIR, "parquet")
        if not os.path.isdir(base):
            return
        for ydir in list(os.scandir(base)):
            if not (ydir.is_dir() and ydir.name.startswith("y=")): continue
            try:
                y = int(ydir.name.split("=",1)[-1])
            except Exception:
                continue
            for mdir in list(os.scandir(ydir.path)):
                if not (mdir.is_dir() and mdir.name.startswith("m=")): continue
                try:
                    m = int(mdir.name.split("=",1)[-1])
                except Exception:
                    continue
                for ddir in list(os.scandir(mdir.path)):
                    if not (ddir.is_dir() and ddir.name.startswith("d=")): continue
                    try:
                        d = int(ddir.name.split("=",1)[-1])
                        if dt.datetime(y, m, d) < cutoff:
                            for f in list(os.scandir(ddir.path)):
                                try: os.remove(f.path)
                                except Exception: pass
                            try: os.rmdir(ddir.path)
                            except Exception: pass
                    except Exception:
                        continue
                try:
                    if not any(os.scandir(mdir.path)):
                        os.rmdir(mdir.path)
                except Exception:
                    pass
            try:
                if not any(os.scandir(ydir.path)):
                    os.rmdir(ydir.path)
            except Exception:
                pass
    except Exception:
        pass  # best-effort

# ---------------- Flush backends ----------------
def _flush_jsonl(rows: List[Dict[str, Any]], ts: Optional[float] = None):
    if not rows:
        return
    path = _jsonl_path(ts)
    try:
        with open(path, "a", encoding="utf-8") as f:
            for r in rows:
                # ensure serializable
                try:
                    line = json.dumps(r, ensure_ascii=False, default=str)
                except Exception:
                    r2 = dict(r)
                    r2["payload"] = "<unserializable>"
                    line = json.dumps(r2, ensure_ascii=False)
                f.write(line)
                f.write("\n")
    except Exception:
        # last-ditch fallback
        try:
            fb = os.path.join(LOG_DIR, "jsonl_fallback.log")
            _ensure_dir(os.path.dirname(fb))
            with open(fb, "a", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False, default=str))
                    f.write("\n")
        except Exception:
            pass

def _flush_parquet(rows: List[Dict[str, Any]], ts: Optional[float] = None):
    if not (_parquet_ok and rows):
        return
    d = _parquet_dir(ts)
    fname = f"part-{int(time.time()*1e6)}.parquet"
    path = os.path.join(d, fname)
    try:
        if _parquet_engine == "pyarrow":
            import pyarrow as pa
            import pyarrow.parquet as pq
            tbl = pa.Table.from_pylist(rows)        # schema inferred
            pq.write_table(tbl, path)
        else:
            # use the already-imported pandas alias to avoid redundant import
            df = _pd.DataFrame(rows)  # type: ignore
            _fp.write(path, df)       # type: ignore
    except Exception:
        # parquet is optional; ignore
        pass

def _flush_batch(rows: List[Dict[str, Any]], force_ts: Optional[float] = None):
    if not rows:
        return
    ts = force_ts or time.time()
    if "jsonl" in FORMATS or not FORMATS:
        _flush_jsonl(rows, ts)
    if "parquet" in FORMATS:
        _flush_parquet(rows, ts)
    # hourly-ish pruning
    if int(ts) % 3600 < FLUSH_SEC:
        _prune_old_jsonl()
        _prune_old_parquet()

# ---------------- Worker ----------------
def _writer():
    buf: List[Dict[str, Any]] = []
    last = time.time()
    while not _stop.is_set():
        try:
            try:
                item = _q.get(timeout=0.5)
                if isinstance(item, dict) and item.get("__ping__"):
                    pass
                else:
                    buf.append(item)
            except queue.Empty:
                pass

            now = time.time()
            if buf and (now - last >= FLUSH_SEC):
                _flush_batch(buf, now)
                buf.clear()
                last = now
        except Exception:
            # Never crash the writer; drop the batch and keep going
            buf.clear()
            last = time.time()

    # final flush
    try:
        if buf:
            _flush_batch(buf, time.time())
    except Exception:
        pass

def start_worker():
    global _worker_started
    with _lock:
        if _worker_started:
            return
        _worker_started = True
        t = threading.Thread(target=_writer, name="decision-log-writer", daemon=True)
        t.start()
        atexit.register(close)

def close():
    _stop.set()
    # nudge the queue
    try:
        _q.put_nowait({"__ping__": True})
    except Exception:
        pass
    # quick drain and flush
    t0 = time.time()
    batch: List[Dict[str, Any]] = []
    while time.time() - t0 < 2.5:
        try:
            item = _q.get_nowait()
            if isinstance(item, dict) and item.get("__ping__"):
                continue
            batch.append(item)
        except queue.Empty:
            break
    if batch:
        try:
            _flush_batch(batch, time.time())
        except Exception:
            pass

def flush_now():
    # opportunistic flush: drain queue quickly and flush
    t0 = time.time()
    drained: List[Dict[str, Any]] = []
    while time.time() - t0 < 0.5:
        try:
            item = _q.get_nowait()
            if isinstance(item, dict) and item.get("__ping__"):
                continue
            drained.append(item)
        except queue.Empty:
            break
    if drained:
        try:
            _flush_batch(drained, time.time())
        except Exception:
            pass

# ---------------- Context ----------------
def set_context(**kwargs):
    """Attach static context to all future events (e.g., strategy, version, sub_label)."""
    with _ctx_lock:
        for k, v in kwargs.items():
            if v is not None:
                _extra_ctx[k] = v

def clear_context(keys: Optional[List[str]] = None):
    with _ctx_lock:
        if keys:
            for k in keys:
                _extra_ctx.pop(k, None)
        else:
            _extra_ctx.clear()

# ---------------- Public logging ----------------
def _safe_payload(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not payload:
        return {}
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        try:
            json.dumps(v, default=str)
            out[k] = v
        except Exception:
            out[k] = repr(v)
    return out

def _base_event(component: str,
                event: str,
                symbol: str,
                account_uid: str,
                payload: Optional[Dict[str, Any]],
                trade_id: Optional[str],
                level: str) -> Dict[str, Any]:
    # ISO with ms + Z
    ts = dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    with _ctx_lock:
        ctx = dict(_extra_ctx)
    e = {
        "ts": ts,
        "level": (level or "info").lower(),
        "component": component or "",
        "event": event or "",
        "symbol": (symbol or "").upper(),
        "account_uid": str(account_uid or ""),
        "trade_id": trade_id or "",
        "payload": _safe_payload(payload),
        "run_id": RUN_ID,
        "host": HOSTNAME,
        "pid": PID,
    }
    if ctx:
        # surface context both top-level (non-colliding) and inside payload._ctx
        for k, v in ctx.items():
            if k not in e:
                e[k] = v
        try:
            if isinstance(e["payload"], dict):
                e["payload"] = {**e["payload"], "_ctx": ctx}
        except Exception:
            pass
    e["id"] = _event_hash(e)
    return e

def log_event(component: str,
              event: str,
              symbol: str,
              account_uid: str,
              payload: Optional[Dict[str, Any]] = None,
              trade_id: Optional[str] = None,
              level: str = "info") -> None:
    start_worker()
    e = _base_event(component, event, symbol, account_uid, payload, trade_id, level)
    try:
        _q.put_nowait(e)
    except queue.Full:
        if DROP_OLDEST:
            try:
                _ = _q.get_nowait()  # evict one
                _q.put_nowait(e)
            except Exception:
                pass  # still full: drop newest to avoid blocking
        # else: drop newest silently

def log_event_sync(component: str,
                   event: str,
                   symbol: str,
                   account_uid: str,
                   payload: Optional[Dict[str, Any]] = None,
                   trade_id: Optional[str] = None,
                   level: str = "info") -> None:
    e = _base_event(component, event, symbol, account_uid, payload, trade_id, level)
    try:
        _flush_batch([e], time.time())
    except Exception:
        # desperate direct JSONL append
        try:
            p = _jsonl_path()
            _ensure_dir(os.path.dirname(p))
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(e, ensure_ascii=False, default=str))
                f.write("\n")
        except Exception:
            pass

def log_kv(component: str, event: str, **kv):
    """Convenience to log arbitrary key/values."""
    symbol = str(kv.pop("symbol", "") or "")
    account_uid = str(kv.pop("account_uid", "") or "")
    log_event(component, event, symbol, account_uid, payload=kv)

# Back-compat alias
log = log_event

# ---------------- CLI self-test ----------------
if __name__ == "__main__":
    set_context(strategy="TrendPullback", version="1.5.0", sub_label="SUB7")
    log_event("executor", "entry_ok", "BTCUSDT", "MAIN",
              {"side": "Buy", "qty": 0.0123, "spr_bps": 3.1, "link": "B44-BTC-..."},
              level="info")
    log_event("tpsl", "ladder_sync", "ETHUSDT", "MAIN",
              {"rungs": 5, "stop": 3350.12, "targets": [3400.1, 3420.5]}, level="info")
    flush_now()
    print("decision_log wrote to:", LOG_DIR)
