#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Order Tag Utilities
Consistent tag/metadata for ownership across all bots.
"""

from __future__ import annotations
import os
import time
from typing import Optional, Dict

B44_TAG_PREFIX = "B44"

def session_id() -> str:
    # yyyyMMddTHHmmss in UTC, stable per process
    return os.environ.get("B44_SESSION_ID") or time.strftime("%Y%m%dT%H%M%S", time.gmtime())

def build_tag(sub_uid: str, strategy: str, sess: Optional[str] = None) -> str:
    return f"{B44_TAG_PREFIX}:{sub_uid}:{strategy}:{sess or session_id()}"

def parse_tag(tag: str) -> Optional[Dict[str, str]]:
    try:
        pfx, sub_uid, strat, sess = tag.split(":", 3)
        if pfx != B44_TAG_PREFIX:
            return None
        return {"sub_uid": sub_uid, "strategy": strat, "session": sess}
    except Exception:
        return None

def is_b44_tag(tag: Optional[str]) -> bool:
    return bool(tag and parse_tag(tag))

def attach_to_client_order_id(base: str, tag: str) -> str:
    # Maintain 36–64 char limits safely
    base = (base or "B44").replace(" ", "")[:24]
    tail = tag.replace(":", "-")  # safer for venues that dislike colons
    cid = f"{base}|{tail}"
    return cid[:64]
