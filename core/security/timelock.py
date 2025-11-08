# core/security/timelock.py
import json, os
from datetime import datetime, timezone
from core.logger import get_logger
from .config import load

cfg = load()
log = get_logger("timelock")

STATE_PATH = os.path.join(os.path.dirname(__file__), "state", "timelock.json")

def _load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"green_days": 0, "last_update": None, "equity_usd": 0.0}

def _save_state(st):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, indent=2)

def update_metrics(green_days: int = None, equity_usd: float = None):
    st = _load_state()
    if green_days is not None: st["green_days"] = int(green_days)
    if equity_usd is not None: st["equity_usd"] = float(equity_usd)
    st["last_update"] = datetime.now(timezone.utc).isoformat()
    _save_state(st)

def check() -> (bool, str):
    if not cfg.timelock_enabled:
        return True, "timelock disabled"
    # Date gate
    try:
        allow_dt = datetime.fromisoformat(cfg.timelock_allow_after.replace("Z","+00:00"))
    except Exception:
        allow_dt = datetime(1970,1,1,tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    if now < allow_dt:
        return False, f"blocked by date until {allow_dt.isoformat()}"

    st = _load_state()
    if st.get("equity_usd", 0.0) < cfg.timelock_min_equity:
        return False, f"equity {st.get('equity_usd')} < min {cfg.timelock_min_equity}"
    if cfg.timelock_min_green_days and st.get("green_days", 0) < cfg.timelock_min_green_days:
        return False, f"green_days {st.get('green_days')} < min {cfg.timelock_min_green_days}"
    return True, "timelock ok"
