#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core.queue — Durable jobs with visibility timeout + idempotency.

Tables used (created by core.db.migrate):
  - jobs(id, job_key, type, payload, status, visible_at, attempts, max_attempts, vt_seconds, created_at, updated_at)

API:
  enqueue(type, payload, job_key=None, vt_seconds=None, max_attempts=5) -> id
  claim(worker_id, limit=1) -> [ {id,type,payload,attempts,vt_seconds} ... ]
  ack(job_id)
  fail(job_id, retry_delay_sec=None)
  extend(job_id, add_seconds)

Idempotency: if job_key is provided and exists, returns the existing job id.
"""

from __future__ import annotations
import json, os, time, uuid, sqlite3
from datetime import datetime, timezone

# logger shim
try:
    from Core.logger import get_logger
except Exception:
    try:
        from core.logger import get_logger
    except Exception:
        def get_logger(_):
            class _L:
                def info(self,*a,**k): pass
                def warning(self,*a,**k): pass
                def error(self,*a,**k): pass
            return _L()
log = get_logger("core.queue")

# DB conn
try:
    from Core.db import conn_rw
except Exception:
    from core.db import conn_rw  # type: ignore

VISIBILITY_DEFAULT = int(os.getenv("QUEUE_VISIBILITY_TIMEOUT", "30"))

def _now_epoch() -> float:
    return time.time()

def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def enqueue(job_type: str, payload: dict, job_key: str | None = None,
            vt_seconds: int | None = None, max_attempts: int = 5) -> str | None:
    jid = str(uuid.uuid4())
    if vt_seconds is None:
        vt_seconds = VISIBILITY_DEFAULT
    visible_at = _now_epoch()
    with conn_rw() as con:
        try:
            con.execute("""
            INSERT INTO jobs(id, job_key, type, payload, status, visible_at, attempts,
                             max_attempts, vt_seconds, created_at, updated_at)
            VALUES(?,?,?,?, 'queued', ?, 0, ?, ?, ?, ?)
            """, (jid, job_key, job_type, json.dumps(payload, separators=(",",":")),
                  visible_at, max_attempts, vt_seconds, _utc(), _utc()))
            log.info("enqueued %s type=%s key=%s", jid, job_type, job_key)
            return jid
        except sqlite3.IntegrityError:
            if job_key:
                row = con.execute("SELECT id FROM jobs WHERE job_key=?", (job_key,)).fetchone()
                if row:
                    log.info("dedup key=%s -> %s", job_key, row["id"])
                    return row["id"]
            log.error("enqueue failed: integrity error and no dedup row for key=%s", job_key)
            return None

def claim(worker_id: str, limit: int = 1):
    now = _now_epoch()
    claimed = []
    with conn_rw() as con:
        for _ in range(limit):
            row = con.execute("""
            SELECT id, type, payload, attempts, vt_seconds
            FROM jobs
            WHERE status IN ('queued','claimed') AND visible_at <= ?
            ORDER BY created_at ASC
            LIMIT 1
            """, (now,)).fetchone()
            if not row:
                break
            new_visible = _now_epoch() + row["vt_seconds"]
            con.execute("""
            UPDATE jobs SET status='claimed', attempts=attempts+1, visible_at=?, updated_at=?
            WHERE id=?""", (new_visible, _utc(), row["id"]))
            claimed.append({
                "id": row["id"],
                "type": row["type"],
                "payload": json.loads(row["payload"]),
                "attempts": row["attempts"] + 1,
                "vt_seconds": row["vt_seconds"]
            })
    return claimed

def ack(job_id: str) -> None:
    with conn_rw() as con:
        con.execute("UPDATE jobs SET status='done', updated_at=? WHERE id=?", (_utc(), job_id))

def fail(job_id: str, retry_delay_sec: int | None = None) -> None:
    with conn_rw() as con:
        row = con.execute("SELECT attempts, max_attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            return
        attempts, max_attempts = row["attempts"], row["max_attempts"]
        if attempts >= max_attempts:
            con.execute("UPDATE jobs SET status='failed', updated_at=? WHERE id=?", (_utc(), job_id))
        else:
            delay = retry_delay_sec if retry_delay_sec is not None else 5
            con.execute("UPDATE jobs SET status='queued', visible_at=?, updated_at=? WHERE id=?",
                        (_now_epoch() + delay, _utc(), job_id))

def extend(job_id: str, add_seconds: int) -> None:
    with conn_rw() as con:
        row = con.execute("SELECT visible_at FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            return
        con.execute("UPDATE jobs SET visible_at=?, updated_at=? WHERE id=?",
                    (row["visible_at"] + add_seconds, _utc(), job_id))
