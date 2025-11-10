#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — LaunchGate (Phase-4 Canary)
Decides per-signal: ALLOW_LIVE, SHADOW_ONLY, or BLOCK.
Reads config/canary.yaml, checks breaker, time windows, regime, correlation,
risk caps, and account overrides. Returns a decision and size_factor.
"""
from __future__ import annotations
import time, datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    import yaml  # pip install pyyaml
except Exception as e:
    raise RuntimeError("pyyaml is required: pip install pyyaml") from e

@dataclass
class Decision:
    ALLOW_LIVE = "ALLOW_LIVE"
    SHADOW_ONLY = "SHADOW_ONLY"
    BLOCK = "BLOCK"
    action: str
    reason: str
    size_factor: float = 1.0

class LaunchGate:
    def __init__(self, cfg_path: Optional[Path] = None):
        try:
            from core.config import settings
            root = Path(getattr(settings, "ROOT", Path.cwd()))
        except Exception:
            root = Path.cwd()
        self.cfg_path = cfg_path or (root / "config" / "canary.yaml")
        self.cfg = self._load_cfg(self.cfg_path)

    def _load_cfg(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"Missing canary policy: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def reload(self) -> None:
        self.cfg = self._load_cfg(self.cfg_path)

    @staticmethod
    def _utc_tuple() -> Tuple[int, str]:
        now = dt.datetime.utcnow()
        return now.weekday(), now.strftime("%H:%M")

    def _is_within_live_window(self) -> bool:
        wins = self.cfg.get("live_windows", [])
        wd, hm = self._utc_tuple()
        for w in wins:
            if wd in (w.get("days") or []):
                if w["start"] <= hm <= w["end"]:
                    return True
        return False

    def _account_rule(self, sub_uid: str) -> Dict[str, Any]:
        return (self.cfg.get("accounts") or {}).get(str(sub_uid), {})

    def _breaker_clear(self) -> bool:
        try:
            from core.breaker import state as breaker_state
            return bool(breaker_state())
        except Exception:
            return False  # when unsure, be conservative

    def _guard_snapshot(self) -> Dict[str, Any]:
        try:
            from core.db import guard_load
            return guard_load() or {}
        except Exception:
            return {}

    def _passes_caps(self, symbol: str) -> Tuple[bool, str]:
        try:
            from core.db import get_positions
            positions = get_positions()
        except Exception:
            positions = []
        cap_total = float(self.cfg.get("max_concurrent_initial_risk_pct", 6.0))
        cap_symbol = float(self.cfg.get("max_symbol_concentration_pct", 50.0))
        cur_total = sum(float(p.get("initial_risk_pct", 0)) for p in positions)
        cur_symbol = sum(float(p.get("initial_risk_pct", 0)) for p in positions if p.get("symbol") == symbol)
        if cur_total >= cap_total:
            return False, f"risk_cap_total {cur_total:.2f}%>={cap_total:.2f}%"
        if cur_symbol >= cap_symbol:
            return False, f"risk_cap_symbol {cur_symbol:.2f}%>={cap_symbol:.2f}%"
        return True, "ok"

    def _corr_ok(self, sub_uid: str, symbol: str) -> bool:
        try:
            from core.corr_gate import allow as corr_allow
            return bool(corr_allow(symbol, sub_uid))
        except Exception:
            return True

    def _regime_ok(self, meta: Dict[str, Any]) -> Tuple[bool, str]:
        adx = float(meta.get("bias_adx", 0))
        atrp = float(meta.get("atr_pct", 0))
        vz = float(meta.get("vol_zscore", 0))
        if adx < float(self.cfg.get("min_adx_bias", 0)): return False, f"adx {adx:.1f} < min"
        if atrp < float(self.cfg.get("min_atr_pct_intraday", 0)): return False, f"atr% {atrp:.2f} < min"
        if vz < float(self.cfg.get("min_vol_zscore", -9)): return False, f"vol_z {vz:.2f} < min"
        return True, "ok"

    def decide(self, sub_uid: str, symbol: str, side: str,
               calc_qty: float, meta: Dict[str, Any]) -> Decision:
        if not self.cfg.get("enabled", True):
            return Decision(Decision.ALLOW_LIVE, "disabled", 1.0)

        if self.cfg.get("breaker_required_clear", True) and not self._breaker_clear():
            return Decision(Decision.SHADOW_ONLY, "breaker_on", 1.0)

        if not self._is_within_live_window():
            return Decision(Decision.SHADOW_ONLY, "out_of_window", 1.0)

        wl = set(self.cfg.get("symbol_whitelist", []) or [])
        bl = set(self.cfg.get("symbol_blacklist", []) or [])
        if wl and symbol not in wl: return Decision(Decision.BLOCK, f"not_whitelisted:{symbol}", 0)
        if symbol in bl:            return Decision(Decision.BLOCK, f"blacklisted:{symbol}", 0)

        ok, why = self._regime_ok(meta)
        if not ok: return Decision(Decision.BLOCK, f"regime:{why}", 0)
        if not self._corr_ok(sub_uid, symbol): return Decision(Decision.BLOCK, "corr_block", 0)
        caps_ok, caps_msg = self._passes_caps(symbol)
        if not caps_ok: return Decision(Decision.BLOCK, caps_msg, 0)

        gs = self._guard_snapshot()
        attempts = int(gs.get("attempts", 0))
        max_attempts = int(self.cfg.get("max_daily_attempts", 20))
        if attempts >= max_attempts:
            return Decision(Decision.SHADOW_ONLY, "attempt_budget_exhausted", 1.0)

        last_loss_ts = int(gs.get("last_loss_ts", 0))
        cooldown = int(self.cfg.get("cooldown_minutes_on_loss", 0)) * 60
        if cooldown and last_loss_ts and time.time() - last_loss_ts < cooldown:
            return Decision(Decision.SHADOW_ONLY, "cooldown_active", 1.0)

        # account overrides
        ar = self._account_rule(sub_uid)
        mode = (ar.get("mode") or self.cfg.get("mode") or "canary").lower()
        size_factor = float(ar.get("size_factor", self.cfg.get("size_factor", 0.25)))
        if mode == "off":    return Decision(Decision.SHADOW_ONLY, "account_off", 1.0)
        if mode == "shadow": return Decision(Decision.SHADOW_ONLY, "account_shadow", 1.0)
        return Decision(Decision.ALLOW_LIVE, "canary_ok", size_factor)
