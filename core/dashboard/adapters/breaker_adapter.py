from __future__ import annotations
from typing import Dict, Any
from core import breaker

def get_breaker_payload() -> Dict[str, Any]:
    st = breaker.status()
    return {
        "active": bool(st.get("breach")),
        "reason": st.get("reason") or "",
        "ttl": int(st.get("ttl") or 0),
        "ts": st.get("ts"),
        "version": st.get("version", 1),
    }
