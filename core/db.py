#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Core DB (SQLite)

Now with:
- orders / executions (as before)
- positions (latest snapshot per sub_uid+symbol)
- guard_state (daily session anchors and running PnL)

API used by bots:
- insert_order, set_order_state, insert_execution
- upsert_position, get_positions
- guard_load(), guard_update_pnl(delta_usd), guard_reset_day()

All timestamps are epoch milliseconds. File path defaults to state/base44.db
unless settings.DB_PATH is set.
"""

from __future__ import annotations
import sqlite3
import time
from pathlib import Path
from typing import Optional, Tuple, Any, Dict, List

# Settings import with tolerant casing
try:
    from core.config import settings
except Exception:
    from Core.config import settings  # type: ignore

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(exist_ok=True, parents=True)

DB_PATH = Path(getattr(settings, "DB_PATH", STATE_DIR / "base44.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------- connection ----------

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

# ---------- schema ----------

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

SCHEMA_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
  sub_uid    TEXT NOT NULL,
  symbol     TEXT NOT NULL,
  qty        REAL NOT NULL,
  avg_price  REAL NOT NULL,
  side       TEXT NOT NULL,        -- Long|Short|Flat
  updated_ts INTEGER NOT NULL,
  PRIMARY KEY(sub_uid, symbol)
);
"""

SCHEMA_GUARD = """
CREATE TABLE IF NOT EXISTS guard_state (
  id            INTEGER PRIMARY KEY CHECK (id = 1),
  session_start_ms INTEGER NOT NULL,
  start_equity_usd REAL NOT NULL DEFAULT 0.0,
  realized_pnl_usd REAL NOT NULL DEFAULT 0.0,
  breach         INTEGER NOT NULL DEFAULT 0  -- 0/1
);
"""

SCHEMA_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_orders_state  ON orders(state);",
    "CREATE INDEX IF NOT EXISTS idx_orders_exid   ON orders(exchange_id);",
    "CREATE INDEX IF NOT EXISTS idx_exec_link     ON executions(link_id);",
    "CREATE INDEX IF NOT EXISTS idx_exec_ts       ON executions(ts_ms);",
    "CREATE INDEX IF NOT EXISTS idx_pos_sym       ON positions(symbol);",
]

def migrate() -> None:
    c = _get_conn()
    with c:
        c.execute(SCHEMA_ORDERS)
        c.execute(SCHEMA_EXECUTIONS)
        c.execute(SCHEMA_POSITIONS)
        c.execute(SCHEMA_GUARD)
        for ddl in SCHEMA_INDEXES:
            c.execute(ddl)
        # seed guard row if absent
        cur = c.execute("SELECT COUNT(*) FROM guard_state WHERE id=1")
        if (cur.fetchone() or [0])[0] == 0:
            now = _now_ms()
            c.execute(
                "INSERT INTO guard_state(id, session_start_ms, start_equity_usd, realized_pnl_usd, breach) VALUES (1, ?, 0.0, 0.0, 0)",
                (now,),
            )

# ---------- time ----------

def _now_ms() -> int:
    return int(time.time() * 1000)

# ---------- orders / executions ----------

def insert_order(link_id: str, symbol: str, side: str, qty: float, price: Optional[float], tag: str, state: str = "NEW") -> None:
    ts = _now_ms()
    c = _get_conn()
    with c:
        c.execute(
            """
            INSERT OR IGNORE INTO orders(link_id, symbol, side, qty, price, tag, state, exchange_id, err_code, err_msg, created_ts, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
            """,
            (link_id, symbol, side, float(qty), float(price) if price is not None else None, tag, state, ts, ts),
        )
        c.execute("UPDATE orders SET updated_ts=? WHERE link_id=?", (ts, link_id))

def set_order_state(link_id: str, state: str, *, exchange_id: Optional[str] = None, err_code: Optional[str] = None, err_msg: Optional[str] = None) -> None:
    ts = _now_ms()
    c = _get_conn()
    with c:
        c.execute(
            """
            INSERT OR IGNORE INTO orders(link_id, symbol, side, qty, price, tag, state, exchange_id, err_code, err_msg, created_ts, updated_ts)
            VALUES (?, 'UNKNOWN', 'UNKNOWN', 0.0, NULL, NULL, 'NEW', NULL, NULL, NULL, ?, ?)
            """,
            (link_id, ts, ts),
        )
        c.execute(
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

def insert_execution(link_id: str, qty: float, price: float, *, fee: float = 0.0, ts_ms: Optional[int] = None) -> None:
    ts = ts_ms if ts_ms is not None else _now_ms()
    c = _get_conn()
    with c:
        c.execute(
            """
            INSERT OR IGNORE INTO orders(link_id, symbol, side, qty, price, tag, state, exchange_id, err_code, err_msg, created_ts, updated_ts)
            VALUES (?, 'UNKNOWN', 'UNKNOWN', 0.0, NULL, NULL, 'NEW', NULL, NULL, NULL, ?, ?)
            """,
            (link_id, ts, ts),
        )
        c.execute(
            "INSERT INTO executions(link_id, qty, price, fee, ts_ms) VALUES (?, ?, ?, ?, ?)",
            (link_id, float(qty), float(price), float(fee), ts),
        )

def list_orders(state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    c = _get_conn()
    if state:
        cur = c.execute("SELECT * FROM orders WHERE state=? ORDER BY updated_ts DESC LIMIT ?", (state, int(limit)))
    else:
        cur = c.execute("SELECT * FROM orders ORDER BY updated_ts DESC LIMIT ?", (int(limit),))
    cols = [d[0] for d in cur.description]
    return [{k: row[i] for i, k in enumerate(cols)} for row in cur.fetchall()]

def counts() -> Dict[str, int]:
    c = _get_conn()
    co = c.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    ce = c.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
    cp = c.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    return {"orders": int(co), "executions": int(ce), "positions": int(cp)}

# ---------- positions ----------

def upsert_position(symbol: str, sub_uid: str, qty: float, avg_price: float, side: str) -> None:
    ts = _now_ms()
    c = _get_conn()
    with c:
        c.execute(
            """
            INSERT INTO positions(sub_uid, symbol, qty, avg_price, side, updated_ts)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(sub_uid, symbol) DO UPDATE SET
                qty=excluded.qty,
                avg_price=excluded.avg_price,
                side=excluded.side,
                updated_ts=excluded.updated_ts
            """,
            (str(sub_uid), symbol, float(qty), float(avg_price), side, ts),
        )

def get_positions(sub_uid: Optional[str] = None) -> List[Dict[str, Any]]:
    c = _get_conn()
    if sub_uid:
        cur = c.execute("SELECT * FROM positions WHERE sub_uid=? ORDER BY updated_ts DESC", (str(sub_uid),))
    else:
        cur = c.execute("SELECT * FROM positions ORDER BY updated_ts DESC")
    cols = [d[0] for d in cur.description]
    return [{k: row[i] for i, k in enumerate(cols)} for row in cur.fetchall()]

# ---------- guard state ----------

def guard_load() -> Dict[str, Any]:
    c = _get_conn()
    cur = c.execute("SELECT session_start_ms, start_equity_usd, realized_pnl_usd, breach FROM guard_state WHERE id=1")
    row = cur.fetchone()
    if not row:
        migrate()
        return guard_load()
    return {
        "session_start_ms": row[0],
        "start_equity_usd": float(row[1]),
        "realized_pnl_usd": float(row[2]),
        "breach": bool(row[3]),
    }

def guard_update_pnl(delta_usd: float) -> None:
    c = _get_conn()
    with c:
        c.execute(
            "UPDATE guard_state SET realized_pnl_usd = realized_pnl_usd + ?, updated_ts=? WHERE id=1",
            (float(delta_usd), _now_ms()),
        )

def guard_reset_day(start_equity_usd: float = 0.0) -> None:
    c = _get_conn()
    with c:
        c.execute(
            "UPDATE guard_state SET session_start_ms=?, start_equity_usd=?, realized_pnl_usd=0.0, breach=0",
            (_now_ms(), float(start_equity_usd)),
        )
