#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Core DB (SQLite)
Purpose:
- Durable storage for order lifecycle and executions.
- Simple API used by bots: insert_order, set_order_state, insert_execution.
- Minimal, battle-hardened defaults; safe on Windows and Linux.

Tables:
  orders(
      link_id TEXT PRIMARY KEY,
      symbol TEXT NOT NULL,
      side   TEXT NOT NULL,         -- 'Buy' | 'Sell'
      qty    REAL NOT NULL,
      price  REAL,                  -- entry price if known at placement
      tag    TEXT,                  -- 'B44' etc.
      state  TEXT NOT NULL,         -- NEW|SENT|ACKED|FILLED|REJECTED|CANCELLED|ERROR
      exchange_id TEXT,             -- Bybit orderId once known
      err_code TEXT,
      err_msg  TEXT,
      created_ts INTEGER NOT NULL,  -- epoch ms
      updated_ts INTEGER NOT NULL
  )

  executions(
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      link_id   TEXT NOT NULL,
      qty       REAL NOT NULL,
      price     REAL NOT NULL,
      fee       REAL DEFAULT 0.0,
      ts_ms     INTEGER NOT NULL,
      FOREIGN KEY(link_id) REFERENCES orders(link_id) ON DELETE CASCADE
  )

Notes:
- We trust link_id uniqueness at the strategy layer (<=36 chars for Bybit).
- All timestamps are epoch milliseconds.
"""

from __future__ import annotations
import os
import sqlite3
import time
from pathlib import Path
from typing import Optional, Tuple, Any, Dict

# Settings import with tolerant casing
try:
    from core.config import settings
except Exception:
    # Fallback if project casing differs somewhere
    from Core.config import settings  # type: ignore

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(exist_ok=True, parents=True)

# Use settings.DB_PATH if present, else default to state/base44.db
DB_PATH = Path(getattr(settings, "DB_PATH", STATE_DIR / "base44.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------- connection helpers ----------

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

_CONN: Optional[sqlite3.Connection] = None

def _get_conn() -> sqlite3.Connection:
    global _CONN
    if _CONN is None:
        _CONN = _connect()
    return _CONN

# ---------- migrations ----------

SCHEMA_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
  link_id      TEXT PRIMARY KEY,
  symbol       TEXT NOT NULL,
  side         TEXT NOT NULL,
  qty          REAL NOT NULL,
  price        REAL,
  tag          TEXT,
  state        TEXT NOT NULL,
  exchange_id  TEXT,
  err_code     TEXT,
  err_msg      TEXT,
  created_ts   INTEGER NOT NULL,
  updated_ts   INTEGER NOT NULL
);
"""

SCHEMA_EXECUTIONS = """
CREATE TABLE IF NOT EXISTS executions (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  link_id   TEXT NOT NULL,
  qty       REAL NOT NULL,
  price     REAL NOT NULL,
  fee       REAL DEFAULT 0.0,
  ts_ms     INTEGER NOT NULL,
  FOREIGN KEY(link_id) REFERENCES orders(link_id) ON DELETE CASCADE
);
"""

SCHEMA_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_orders_state  ON orders(state);",
    "CREATE INDEX IF NOT EXISTS idx_orders_exid   ON orders(exchange_id);",
    "CREATE INDEX IF NOT EXISTS idx_exec_link     ON executions(link_id);",
    "CREATE INDEX IF NOT EXISTS idx_exec_ts       ON executions(ts_ms);",
]

def migrate() -> None:
    conn = _get_conn()
    with conn:
        conn.execute(SCHEMA_ORDERS)
        conn.execute(SCHEMA_EXECUTIONS)
        for ddl in SCHEMA_INDEXES:
            conn.execute(ddl)

# ---------- time helper ----------

def _now_ms() -> int:
    return int(time.time() * 1000)

# ---------- public API (used by bots) ----------

def insert_order(
    link_id: str,
    symbol: str,
    side: str,
    qty: float,
    price: Optional[float],
    tag: str,
    state: str = "NEW",
) -> None:
    """
    Insert a new order row if absent; if present, do not clobber historical state.
    Safe to call repeatedly for idempotency.
    """
    ts = _now_ms()
    conn = _get_conn()
    with conn:
        # insert or ignore
        conn.execute(
            """
            INSERT OR IGNORE INTO orders(link_id, symbol, side, qty, price, tag, state, exchange_id, err_code, err_msg, created_ts, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
            """,
            (link_id, symbol, side, float(qty), float(price) if price is not None else None, tag, state, ts, ts),
        )
        # if row existed, only update updated_ts if you really want; we skip to preserve first state
        conn.execute("UPDATE orders SET updated_ts=? WHERE link_id=?", (ts, link_id))

def set_order_state(
    link_id: str,
    state: str,
    *,
    exchange_id: Optional[str] = None,
    err_code: Optional[str] = None,
    err_msg: Optional[str] = None,
) -> None:
    """
    Update state for an existing order. If the row doesn't exist yet, create a stub row.
    """
    ts = _now_ms()
    conn = _get_conn()
    with conn:
        # ensure presence
        conn.execute(
            """
            INSERT OR IGNORE INTO orders(link_id, symbol, side, qty, price, tag, state, exchange_id, err_code, err_msg, created_ts, updated_ts)
            VALUES (?, 'UNKNOWN', 'UNKNOWN', 0.0, NULL, NULL, 'NEW', NULL, NULL, NULL, ?, ?)
            """,
            (link_id, ts, ts),
        )
        # update state
        conn.execute(
            """
            UPDATE orders
               SET state=?,
                   exchange_id=COALESCE(?, exchange_id),
                   err_code=?,
                   err_msg=?,
                   updated_ts=?
             WHERE link_id=?
            """,
            (state, exchange_id, err_code, err_msg, ts, link_id),
        )

def insert_execution(
    link_id: str,
    qty: float,
    price: float,
    *,
    fee: float = 0.0,
    ts_ms: Optional[int] = None,
) -> None:
    """
    Record an execution fill tied to the order link_id.
    Does NOT change order.state; your reconciler can mark FILLED based on totals.
    """
    ts = ts_ms if ts_ms is not None else _now_ms()
    conn = _get_conn()
    with conn:
        # ensure parent order exists
        conn.execute(
            """
            INSERT OR IGNORE INTO orders(link_id, symbol, side, qty, price, tag, state, exchange_id, err_code, err_msg, created_ts, updated_ts)
            VALUES (?, 'UNKNOWN', 'UNKNOWN', 0.0, NULL, NULL, 'NEW', NULL, NULL, NULL, ?, ?)
            """,
            (link_id, ts, ts),
        )
        conn.execute(
            "INSERT INTO executions(link_id, qty, price, fee, ts_ms) VALUES (?, ?, ?, ?, ?)",
            (link_id, float(qty), float(price), float(fee), ts),
        )

# ---------- convenience queries (optional) ----------

def get_order(link_id: str) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.execute("SELECT * FROM orders WHERE link_id=?", (link_id,))
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return {k: row[i] for i, k in enumerate(cols)}

def list_orders(state: Optional[str] = None, limit: int = 100) -> list[Dict[str, Any]]:
    conn = _get_conn()
    if state:
        cur = conn.execute(
            "SELECT * FROM orders WHERE state=? ORDER BY updated_ts DESC LIMIT ?",
            (state, int(limit)),
        )
    else:
        cur = conn.execute("SELECT * FROM orders ORDER BY updated_ts DESC LIMIT ?", (int(limit),))
    cols = [d[0] for d in cur.description]
    return [{k: row[i] for i, k in enumerate(cols)} for row in cur.fetchall()]

def counts() -> Dict[str, int]:
    conn = _get_conn()
    cur = conn.execute("SELECT COUNT(*) FROM orders"); c_orders = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(*) FROM executions"); c_execs = cur.fetchone()[0]
    return {"orders": int(c_orders), "executions": int(c_execs)}

# ---------- CLI ----------

def _cmd_migrate() -> None:
    migrate()
    c = counts()
    print(f"[DB] migrated at {DB_PATH} • {c['orders']} orders • {c['executions']} executions")

def _cmd_inspect() -> None:
    migrate()
    c = counts()
    print(f"[DB] {DB_PATH} • counts={c}")
    for row in list_orders(limit=10):
        print(" -", row.get("link_id"), row.get("symbol"), row.get("state"), row.get("exchange_id"))

if __name__ == "__main__":
    import sys
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "migrate").lower()
    if cmd in ("migrate", "init"):
        _cmd_migrate()
    elif cmd in ("inspect", "ls"):
        _cmd_inspect()
    else:
        print("Usage: python -m core.db [migrate|inspect]")
