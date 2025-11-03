# bots/funding_fees_tracker.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Funding & Fees Tracker (monitor-only)
- Pulls trading fees from /v5/execution/list
- Pulls funding from /v5/position/funding (fallback: /v5/account/transaction-log)
- Writes CSVs under logs/pnl/: fees_trades.csv and funding.csv
- Deduplicates via state (.state/funding_fees_<scope>.json)

ENV (optional):
  FF_POLL_SEC=60
  RISK_CATEGORY=linear
  PNL_SETTLE_COIN=USDT
  FF_MEMBER_ID=           # if set, track that sub only; otherwise master account
  STATE_DIR=.state
  TELEGRAM_SILENT=0       # set "1" to mute Telegram notifications
"""

import os, sys, time, json
from pathlib import Path
from datetime import datetime, timezone

# ── robust import path (project root) then pull client helpers
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# tolerate either "proxy" or "bybit_proxy" in base44_client
try:
    from core.base44_client import proxy, tg_send  # type: ignore
except Exception:
    from core.base44_client import bybit_proxy as proxy, tg_send  # type: ignore

FF_POLL_SEC  = int(os.getenv("FF_POLL_SEC", "60"))
CATEGORY     = os.getenv("RISK_CATEGORY", "linear")
SETTLE_COIN  = os.getenv("PNL_SETTLE_COIN", "USDT")
MEMBER_ID    = (os.getenv("FF_MEMBER_ID") or "").strip()
STATE_DIR    = Path(os.getenv("STATE_DIR", ".state"))
TELEGRAM_SILENT = os.getenv("TELEGRAM_SILENT", "0") == "1"

SCOPE = f"sub_{MEMBER_ID}" if MEMBER_ID else "master"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / f"funding_fees_state_{SCOPE}.json"

LOG_DIR = PROJECT_ROOT / "logs" / "pnl"
LOG_DIR.mkdir(parents=True, exist_ok=True)
FEES_CSV = LOG_DIR / "fees_trades.csv"
FUND_CSV = LOG_DIR / "funding.csv"

# ──────────────────────────────────────────────────────────────────────────────
# Utils
# ──────────────────────────────────────────────────────────────────────────────
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def tg(msg: str):
    if TELEGRAM_SILENT:
        print(f"[ff→tg muted] {msg}")
        return
    try:
        tg_send(msg)
    except Exception as e:
        print(f"[ff] telegram send failed: {e}\n{msg}")

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    # start 24h back to catch anything recent on first run
    start_ms = int((time.time() - 24 * 3600) * 1000)
    return {
        "last_exec_ms": start_ms,
        "seen_exec_ids": [],
        "last_funding_ms": start_ms,
        "seen_funding_keys": []
    }

def save_state(st: dict):
    # shrink dedupe sets
    st["seen_exec_ids"] = st.get("seen_exec_ids", [])[-5000:]
    st["seen_funding_keys"] = st.get("seen_funding_keys", [])[-2000:]
    STATE_PATH.write_text(json.dumps(st, indent=2), encoding="utf-8")

def ensure_csv_headers(path: Path, headers: list[str]):
    if not path.exists():
        path.write_text(",".join(headers) + "\n", encoding="utf-8")

def append_csv(path: Path, row: dict, headers: list[str]):
    ensure_csv_headers(path, headers)
    # simple CSV writer without commas in values
    vals = []
    for h in headers:
        v = row.get(h, "")
        s = f"{v}"
        if "," in s:
            s = s.replace(",", " ")
        vals.append(s)
    with path.open("a", encoding="utf-8") as f:
        f.write(",".join(vals) + "\n")

# ──────────────────────────────────────────────────────────────────────────────
# Pullers
# ──────────────────────────────────────────────────────────────────────────────
def pull_executions_since(start_ms: int) -> list[dict]:
    """
    /v5/execution/list  params: category, startTime, memberId?, limit, cursor...
    Returns normalized list with execId, execTime, execFee, symbol, side, orderId, etc.
    """
    params = {"category": CATEGORY, "startTime": start_ms, "limit": 200}
    if MEMBER_ID:
        params["memberId"] = MEMBER_ID
    out = []
    cursor = None
    while True:
        if cursor:
            params["cursor"] = cursor
        body = proxy("GET", "/v5/execution/list", params=params)
        if (body or {}).get("retCode") != 0:
            # stop on API error (common if no records)
            break
        result = (body.get("result") or {})
        rows = result.get("list") or []
        out.extend(rows)
        cursor = result.get("nextPageCursor") or None
        if not cursor or not rows:
            break
    return out

def pull_funding_since(start_ms: int) -> list[dict]:
    """
    Try /v5/position/funding first; if not available, fallback to /v5/account/transaction-log.
    Normalize to fields: timeMs, symbol, fundingFee, fundingRate, currency
    """
    params = {"category": CATEGORY, "startTime": start_ms, "limit": 200}
    if MEMBER_ID:
        params["memberId"] = MEMBER_ID

    body = proxy("GET", "/v5/position/funding", params=params)
    items = []
    if (body or {}).get("retCode") == 0:
        rows = (body.get("result") or {}).get("list") or []
        for r in rows:
            items.append({
                "timeMs": int(r.get("execTime") or r.get("fundingTime") or 0),
                "symbol": r.get("symbol") or "",
                "fundingFee": float(r.get("fundingFee") or r.get("execFee") or 0),
                "fundingRate": float(r.get("fundingRate") or 0),
                "currency": r.get("feeCurrency") or r.get("currency") or "USDT",
                "raw": r
            })
        return items

    # Fallback: transaction log (filter funding-like records)
    p2 = {"accountType": "UNIFIED", "startTime": start_ms, "limit": 200}
    if MEMBER_ID:
        p2["memberId"] = MEMBER_ID
    body2 = proxy("GET", "/v5/account/transaction-log", params=p2)
    if (body2 or {}).get("retCode") != 0:
        return []
    rows = (body2.get("result") or {}).get("list") or []
    for r in rows:
        # Heuristic: look for types that imply funding
        typ = (r.get("type") or r.get("bizType") or "").upper()
        if "FUNDING" in typ:
            items.append({
                "timeMs": int(r.get("tradeTimeMs") or r.get("execTime") or r.get("createdTime") or 0),
                "symbol": r.get("symbol") or "",
                "fundingFee": float(r.get("change") or r.get("fee") or 0),
                "fundingRate": float(r.get("fundingRate") or 0),
                "currency": r.get("coin") or r.get("currency") or "USDT",
                "raw": r
            })
    return items

# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────
def main():
    print(f"Funding & Fees Tracker running • scope={SCOPE} • poll={FF_POLL_SEC}s • category={CATEGORY}")
    st = load_state()

    fees_headers = [
        "timestamp_iso","member_id","symbol","side","orderId","execId",
        "exec_price","exec_qty","exec_value","exec_fee","fee_currency"
    ]
    funding_headers = [
        "timestamp_iso","member_id","symbol","funding_fee","funding_rate","currency"
    ]

    while True:
        try:
            # ── 1) Executions → fees
            execs = pull_executions_since(st.get("last_exec_ms", 0))
            new_exec_count = 0
            for r in execs:
                exec_id = r.get("execId") or r.get("id") or ""
                if not exec_id:
                    continue
                if exec_id in st.get("seen_exec_ids", []):
                    continue

                t_ms = int(r.get("execTime") or r.get("tradeTimeMs") or 0)
                if t_ms and t_ms > st.get("last_exec_ms", 0):
                    st["last_exec_ms"] = t_ms

                row = {
                    "timestamp_iso": datetime.fromtimestamp((t_ms or 0)/1000, tz=timezone.utc).isoformat(),
                    "member_id": MEMBER_ID or "MASTER",
                    "symbol": r.get("symbol") or "",
                    "side": r.get("side") or "",
                    "orderId": r.get("orderId") or "",
                    "execId": exec_id,
                    "exec_price": r.get("execPrice") or r.get("price") or "",
                    "exec_qty": r.get("execQty") or r.get("qty") or "",
                    "exec_value": r.get("execValue") or r.get("value") or "",
                    "exec_fee": r.get("execFee") or r.get("commission") or "",
                    "fee_currency": r.get("feeCurrency") or r.get("currency") or "USDT",
                }
                append_csv(FEES_CSV, row, fees_headers)
                st.setdefault("seen_exec_ids", []).append(exec_id)
                new_exec_count += 1

            if new_exec_count:
                print(f"[fees] logged {new_exec_count} executions")

            # ── 2) Funding
            fund = pull_funding_since(st.get("last_funding_ms", 0))
            new_fund_count = 0
            for r in fund:
                key = f"{r.get('timeMs')}-{r.get('symbol')}-{r.get('fundingFee')}"
                if key in st.get("seen_funding_keys", []):
                    continue
                t_ms = int(r.get("timeMs") or 0)
                if t_ms and t_ms > st.get("last_funding_ms", 0):
                    st["last_funding_ms"] = t_ms

                row = {
                    "timestamp_iso": datetime.fromtimestamp((t_ms or 0)/1000, tz=timezone.utc).isoformat(),
                    "member_id": MEMBER_ID or "MASTER",
                    "symbol": r.get("symbol") or "",
                    "funding_fee": r.get("fundingFee") or "",
                    "funding_rate": r.get("fundingRate") or "",
                    "currency": r.get("currency") or "USDT",
                }
                append_csv(FUND_CSV, row, funding_headers)
                st.setdefault("seen_funding_keys", []).append(key)
                new_fund_count += 1

            if new_fund_count:
                print(f"[funding] logged {new_fund_count} records")

            save_state(st)
            time.sleep(FF_POLL_SEC)

        except KeyboardInterrupt:
            print("Stopped by user.")
            break
        except Exception as e:
            print(f"[ff] error: {e}")
            time.sleep(FF_POLL_SEC)

if __name__ == "__main__":
    main()
