#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/db_migrate.py — Create/patch Base44 SQLite schema (idempotent)

Creates (IF NOT EXISTS):
  - orders(id TEXT PK, symbol, side, qty, price, tag, state, exchange_id, err_code, err_msg, ts)
  - executions(id INTEGER PK AUTOINCREMENT, order_link_id, qty, price, fee, ts)
  - positions(symbol, sub_uid, qty, avg_price, side, ts, PRIMARY KEY(symbol, sub_uid))
  - guard_state(key TEXT PK, val TEXT, ts)

Env:
  DB_PATH=./state/base44.db   (default)
"""
from __future__ import annotations
import os, sqlite3, time, pathlib, sys

DB_PATH = os.getenv("DB_PATH", "./state/base44.db")
DB_PATH = os.path.normpath(DB_PATH)

def _ensure_dir(p: str):
    d = os.path.dirname(os.path.abspath(p))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

SCHEMA = [
    # Core order lifecycle, used by executor/reconciler
    """
    CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY,            -- our orderLinkId
        symbol TEXT NOT NULL,
        side TEXT CHECK(side IN ('Buy','Sell')) NOT NULL,
        qty REAL NOT NULL,
        price REAL,                     -- may be NULL for market
        tag TEXT,
        state TEXT,                     -- NEW|SENT|ACKED|PARTIAL|FILLED|CANCELED|REJECTED|ERROR
        exchange_id TEXT,
        err_code TEXT,
        err_msg TEXT,
        ts INTEGER DEFAULT (strftime('%s','now'))
    );
    """,
    # Fills (multiple per order)
    """
    CREATE TABLE IF NOT EXISTS executions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_link_id TEXT NOT NULL,
        qty REAL NOT NULL,
        price REAL NOT NULL,
        fee REAL DEFAULT 0.0,
        ts INTEGER DEFAULT (strftime('%s','now')),
        FOREIGN KEY(order_link_id) REFERENCES orders(id) ON DELETE CASCADE
    );
    """,
    # Live position snapshot per symbol+sub
    """
    CREATE TABLE IF NOT EXISTS positions (
        symbol TEXT NOT NULL,
        sub_uid TEXT NOT NULL,
        qty REAL NOT NULL,
        avg_price REAL NOT NULL,
        side TEXT CHECK(side IN ('Long','Short','Flat')) NOT NULL,
        ts INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY(symbol, sub_uid)
    );
    """,
    # Lightweight key/value state used by PortfolioGuard (and friends)
    """
    CREATE TABLE IF NOT EXISTS guard_state (
        key TEXT PRIMARY KEY,
        val TEXT,
        ts INTEGER DEFAULT (strftime('%s','now'))
    );
    """,
]

# Helpful indices (safe to re-run)
INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_symbol_ts ON orders(symbol, ts);",
    "CREATE INDEX IF NOT EXISTS idx_execs_link_ts ON executions(order_link_id, ts);",
    "CREATE INDEX IF NOT EXISTS idx_positions_side ON positions(side);",
]

# Seed a couple of keys in guard_state if missing
SEEDS = {
    "session_start_equity": "0",
    "halted": "false"
}

def migrate(db_path: str) -> None:
    _ensure_dir(db_path)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        for stmt in SCHEMA:
            cur.execute(stmt)
        for idx in INDEXES:
            cur.execute(idx)
        # seed guard_state keys if absent
        for k, v in SEEDS.items():
            cur.execute("INSERT OR IGNORE INTO guard_state(key, val, ts) VALUES (?, ?, strftime('%s','now'))", (k, v))
        conn.commit()
    finally:
        conn.close()

def main():
    print(f"[db_migrate] using DB_PATH={DB_PATH}")
    migrate(DB_PATH)
    # quick sanity
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        for t in ("orders","executions","positions","guard_state"):
            cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{t}'")
            row = cur.fetchone()
            print(f"[OK] table {t} present" if row else f"[MISS] table {t} missing")
    finally:
        conn.close()
    print("[DONE] migration complete")

if __name__ == "__main__":
    main()
