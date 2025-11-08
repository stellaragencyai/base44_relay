#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core.db — SQLite backbone for Base44
Purpose:
- Be the *single source of truth* for automation state so restarts are safe.
- Persist:
  • jobs (durable queue; visibility timeouts; idempotency)
  • orders (client lifecycle NEW→SENT→ACKED→PARTIAL→FILLED|CANCELED|REJECTED|ERROR)
  • executions (fills)
  • positions (canonical per symbol|sub_uid)
  • approvals (security daemon audit)
  • metrics (equity, drawdown, notes)

No external deps. Works on Windows. Safe to import before tables exist.

Tables created on first run via migrate().
"""

from __future__ import annotations
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable, Optional

# ------------------------
# Logger shim (Core vs core)
# ------------------------
try:
    from Core.logger import get_logger  # in case someone used capitalized package
except Exception:  # pragma: no cover
    try:
        from core.logger import get_logger
    except Exception:
        def get_logger(name: str):  # ultra-minimal fallback
            class _L:
                def info(self, *a, **k):  pass
                def warning(self, *a, **k):  pass
                def error(self, *a, **k):  pass
            return _L()

log = get_logger("core.db")

# ------------------------
# Config
# ------------------------
DB_PATH = os.getenv("DB_PATH", "./state/base44.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Single process, multi-thread safe connection
_lock = threading.RLock()

@contextmanager
def conn_rw():
    """Read/write connection with sane pragmas and Row factory."""
    with _lock:
        con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
            con.execute("PRAGMA foreign_keys=ON;")
            yield con
        finally:
            con.close()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ------------------------
# Migration
# ------------------------
def migrate() -> None:
    """Create tables and indexes if they don't exist."""
    ddl: Iterable[str] = (
        # Durable jobs queue
        """
        CREATE TABLE IF NOT EXISTS jobs (
          id            TEXT PRIMARY KEY,
          job_key       TEXT UNIQUE,               -- idempotency key
          type          TEXT NOT NULL,             -- e.g., 'order.create'
          payload       TEXT NOT NULL,             -- JSON string
          status        TEXT NOT NULL DEFAULT 'queued', -- queued|claimed|done|failed
          visible_at    REAL NOT NULL,             -- unix epoch when claimable
          attempts      INTEGER NOT NULL DEFAULT 0,
          max_attempts  INTEGER NOT NULL DEFAULT 5,
          vt_seconds    INTEGER NOT NULL DEFAULT 30,
          created_at    TEXT NOT NULL,
          updated_at    TEXT NOT NULL
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_jobs_visible ON jobs(status, visible_at);",
        "CREATE INDEX IF NOT EXISTS idx_jobs_key ON jobs(job_key);",

        # Orders lifecycle
        """
        CREATE TABLE IF NOT EXISTS orders (
          id              TEXT PRIMARY KEY,        -- our client order id (orderLinkId)
          symbol          TEXT NOT NULL,
          side            TEXT NOT NULL,           -- Buy|Sell
          qty             REAL NOT NULL,
          price           REAL,
          tag             TEXT,                    -- AUTO_*, B44, MANUAL, etc.
          state           TEXT NOT NULL,           -- NEW|SENT|ACKED|PARTIAL|FILLED|CANCELED|REJECTED|ERROR
          exchange_id     TEXT,                    -- Bybit orderId
          error_code      TEXT,
          error_msg       TEXT,
          created_at      TEXT NOT NULL,
          updated_at      TEXT NOT NULL
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_orders_state ON orders(state);",
        "CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);",

        # Executions (fills)
        """
        CREATE TABLE IF NOT EXISTS executions (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          order_id      TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
          fill_qty      REAL NOT NULL,
          fill_price    REAL NOT NULL,
          fee           REAL DEFAULT 0,
          ts            TEXT NOT NULL
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_exec_order ON executions(order_id);",

        # Positions (canonical snapshot)
        """
        CREATE TABLE IF NOT EXISTS positions (
          key           TEXT PRIMARY KEY,          -- symbol|sub_uid
          symbol        TEXT NOT NULL,
          sub_uid       TEXT NOT NULL,
          qty           REAL NOT NULL,
          avg_price     REAL NOT NULL,
          side          TEXT NOT NULL,             -- Long|Short|Flat
          updated_at    TEXT NOT NULL
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_pos_symbol ON positions(symbol);",

        # Security approvals audit
        """
        CREATE TABLE IF NOT EXISTS approvals (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          action        TEXT NOT NULL,             -- withdraw|transfer|breaker_clear
          amount        REAL,
          sub_uid       TEXT,
          ok            INTEGER NOT NULL,          -- 0/1
          token         TEXT,
          ts            TEXT NOT NULL
        );
        """,

        # Metrics (equity, drawdown, notes)
        """
        CREATE TABLE IF NOT EXISTS metrics (
          ts            TEXT PRIMARY KEY,
          equity_usd    REAL,
          green_days    INTEGER,
          dd_pct        REAL,
          notes         TEXT
        );
        """
    )
    with conn_rw() as con:
        for sql in ddl:
            con.execute(sql)
    log.info("DB migrated at %s", DB_PATH)

# ------------------------
# Orders API (used by auto_executor)
# ------------------------
def insert_order(order_id: str, symbol: str, side: str, qty: float, price: Optional[float],
                 tag: Optional[str], state: str = "NEW") -> None:
    """Create order row with initial state."""
    with conn_rw() as con:
        con.execute("""
            INSERT OR REPLACE INTO orders(id, symbol, side, qty, price, tag, state, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?, ?, ?)
        """, (order_id, symbol, side, float(qty), price, tag, state, _now_iso(), _now_iso()))

def set_order_state(order_id: str, state: str, exchange_id: Optional[str] = None,
                    err_code: Optional[str] = None, err_msg: Optional[str] = None) -> None:
    """Move order to a new state, optionally annotate with exchange id or error."""
    with conn_rw() as con:
        con.execute("""
            UPDATE orders
               SET state=?,
                   exchange_id=COALESCE(?, exchange_id),
                   error_code=?,
                   error_msg=?,
                   updated_at=?
             WHERE id=?
        """, (state, exchange_id, err_code, err_msg, _now_iso(), order_id))

def insert_execution(order_id: str, fill_qty: float, fill_price: float, fee: float = 0.0) -> None:
    """Append a fill to executions."""
    with conn_rw() as con:
        con.execute("""
            INSERT INTO executions(order_id, fill_qty, fill_price, fee, ts)
            VALUES(?,?,?,?,?)
        """, (order_id, float(fill_qty), float(fill_price), float(fee), _now_iso()))

# ------------------------
# Positions API (for reconciler)
# ------------------------
def upsert_position(symbol: str, sub_uid: str, qty: float, avg_price: float, side: str) -> None:
    """Create or update a canonical position snapshot."""
    key = f"{symbol}|{sub_uid}"
    with conn_rw() as con:
        con.execute("""
            INSERT INTO positions(key, symbol, sub_uid, qty, avg_price, side, updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                qty=excluded.qty,
                avg_price=excluded.avg_price,
                side=excluded.side,
                updated_at=excluded.updated_at
        """, (key, symbol, str(sub_uid), float(qty), float(avg_price), side, _now_iso()))

# ------------------------
# Approvals & Metrics (security + reporting)
# ------------------------
def add_approval(action: str, amount: float | None, sub_uid: str | None, ok: bool, token: str | None) -> None:
    with conn_rw() as con:
        con.execute("""
            INSERT INTO approvals(action, amount, sub_uid, ok, token, ts)
            VALUES(?,?,?,?,?,?)
        """, (action, amount, sub_uid, 1 if ok else 0, token, _now_iso()))

def put_metric(ts_iso: str, equity_usd: float | None = None, green_days: int | None = None,
               dd_pct: float | None = None, notes: str | None = None) -> None:
    """Upsert a metrics row keyed by timestamp (ISO)."""
    with conn_rw() as con:
        con.execute("""
            INSERT INTO metrics(ts, equity_usd, green_days, dd_pct, notes)
            VALUES(?,?,?,?,?)
            ON CONFLICT(ts) DO UPDATE SET
              equity_usd=COALESCE(excluded.equity_usd, metrics.equity_usd),
              green_days=COALESCE(excluded.green_days, metrics.green_days),
              dd_pct=COALESCE(excluded.dd_pct, metrics.dd_pct),
              notes=COALESCE(excluded.notes, metrics.notes)
        """, (ts_iso, equity_usd, green_days, dd_pct, notes))

# ------------------------
# Lightweight selectors (handy for dashboard/reconciler)
# ------------------------
def get_open_orders(symbol: Optional[str] = None) -> list[sqlite3.Row]:
    q = "SELECT * FROM orders WHERE state IN ('NEW','SENT','ACKED','PARTIAL')"
    args: list = []
    if symbol:
        q += " AND symbol=?"
        args.append(symbol)
    with conn_rw() as con:
        return con.execute(q + " ORDER BY created_at ASC", args).fetchall()

def get_orders_by_tag(tag_prefix: str) -> list[sqlite3.Row]:
    with conn_rw() as con:
        return con.execute("SELECT * FROM orders WHERE tag LIKE ? ORDER BY created_at DESC", (f"{tag_prefix}%",)).fetchall()

def get_last_executions(limit: int = 50) -> list[sqlite3.Row]:
    with conn_rw() as con:
        return con.execute("""
            SELECT e.*, o.symbol, o.side, o.tag
            FROM executions e
            JOIN orders o ON o.id=e.order_id
            ORDER BY e.id DESC LIMIT ?
        """, (limit,)).fetchall()
