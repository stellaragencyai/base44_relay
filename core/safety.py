# core/safety.py
from __future__ import annotations
import os, time
from dataclasses import dataclass

def env_bool(name: str, default: bool=False) -> bool:
    v = (os.getenv(name, str(int(default))) or "").strip().lower()
    return v in {"1","true","yes","on"}

def env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)).strip())
    except: return default

@dataclass
class SafetyConfig:
    adopt_existing: bool = env_bool("TP_ADOPT_EXISTING", True)
    cancel_non_b44: bool = env_bool("TP_CANCEL_NON_B44", False)
    dry_run: bool = env_bool("TP_DRY_RUN", True)
    grace_sec: int = env_int("TP_STARTUP_GRACE_SEC", 20)
    managed_tag: str = os.getenv("TP_MANAGED_TAG", "B44").strip() or "B44"

class GuardRail:
    def __init__(self, cfg: SafetyConfig | None = None):
        self.cfg = cfg or SafetyConfig()
        self.t0 = time.monotonic()

    def in_grace(self) -> bool:
        return (time.monotonic() - self.t0) < max(0, self.cfg.grace_sec)

    # Placement rules
    def allow_place_tp(self, reduce_only: bool, link_id: str) -> tuple[bool,str]:
        if not reduce_only:
            return False, "TP placement rejected: not reduce-only"
        if self.cfg.dry_run:
            return False, "TP placement blocked: DRY_RUN"
        if self.in_grace():
            # We still allow placement in grace, but only reduce-only with our tag
            if not link_id or self.cfg.managed_tag not in link_id:
                return False, "TP placement blocked in GRACE without managed tag"
        return True, "ok"

    def allow_place_sl(self, reduce_only: bool, link_id: str) -> tuple[bool,str]:
        if not reduce_only:
            return False, "SL placement rejected: not reduce-only"
        if self.cfg.dry_run:
            return False, "SL placement blocked: DRY_RUN"
        if self.in_grace() and (not link_id or self.cfg.managed_tag not in link_id):
            return False, "SL placement blocked in GRACE without managed tag"
        return True, "ok"

    # Cancel rules
    def allow_cancel(self, link_id: str | None) -> tuple[bool,str]:
        if self.in_grace():
            return False, "Cancel blocked: startup GRACE"
        if not link_id:
            # Never cancel anonymous orders unless explicitly allowed
            return False, "Cancel blocked: missing orderLinkId"
        if (self.cfg.managed_tag not in link_id) and (not self.cfg.cancel_non_b44):
            return False, "Cancel blocked: non-Base44 order"
        return True, "ok"

    # Market-close rule (hard nope inside manager)
    def allow_market_close(self) -> tuple[bool,str]:
        return False, "Market close blocked: manager forbidden"

# Helper to ensure RO + tag
def ensure_reduce_only(params: dict, tag: str) -> dict:
    p = dict(params)
    p["reduceOnly"] = True
    link = str(p.get("orderLinkId") or "")
    if tag and tag not in link:
        p["orderLinkId"] = f"{tag}-{link or 'auto'}"
    return p
