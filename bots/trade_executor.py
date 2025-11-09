#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trade_executor.py — risk-based entries via relay (DRY by default), maker-first, window-gated, cooldowned,
with symbol whitelist/blacklist guards, guard-aware, idempotent links, and DB journaling.

Enhancements added:
- Global breaker integration via core.guard.guard_blocking_reason() to block new entries while allowing other systems to manage exits elsewhere.
- Deterministic orderLinkId (hash-based) for idempotent replays/restarts.
- DB journaling into core.db (orders/state) on submit/ack/fail for audit and metrics.
- Keeps existing account policy, portfolio guard (soft), decision log, and relay client flow.
"""

from __future__ import annotations
import os, json, time, logging, pathlib, math, datetime, hashlib
from collections import defaultdict
from dotenv import load_dotenv

# Project modules
import core.relay_client as rc
from core.instruments import load_or_fetch, round_price, round_qty

# Optional: account policy + decision log + guard (all soft imports)
try:
    from core.account_policy import may_open, risk_multiplier, symbol_allowed, get_account
except Exception:
    def may_open(_uid:str)->bool: return True
    def risk_multiplier(_uid:str)->float: return 1.0
    def symbol_allowed(_uid:str,_sym:str)->bool: return True
    def get_account(_uid:str)->dict: return {"uid":_uid, "mode":"auto"}

try:
    from core.decision_log import log_event
except Exception:
    def log_event(component, event, symbol, account_uid, payload=None, trade_id=None, level="info"):
        # Fallback: keep stdout noisy if decision logger isn't present
        print(f"[DECLOG/{component}/{event}] {symbol} @{account_uid} {payload or {}}")

# Portfolio guard remains soft
try:
    from core.portfolio_guard import guard as _guard
except Exception:
    _guard = None

# Global breaker / guard (HARD gate for entries)
try:
    from core.guard import guard_blocking_reason
except Exception:
    def guard_blocking_reason():
        return (False, "")

# Optional Telegram notifier
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str = "info", **_):
        print(f"[notify/{priority}] {msg}")

# Optional DB journaling
try:
    from core.db import insert_order, set_order_state
except Exception:
    def insert_order(link_id, symbol, side, qty, price, tag, state="NEW"):  # type: ignore
        pass
    def set_order_state(link_id, state, *, exchange_id=None, err_code=None, err_msg=None):  # type: ignore
        pass

# ------------- env helpers -------------
load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("trade_executor")

LIVE = (os.getenv("LIVE") or "false").strip().lower() == "true"
ROOT = pathlib.Path(__file__).resolve().parents[1]
SIGDIR = ROOT / (os.getenv("SIGNAL_DIR") or "signals")
QUEUE = SIGDIR / "observed.jsonl"

def _env_bool(k: str, default: bool) -> bool:
    v = (os.getenv(k, str(int(default))) or "").strip().lower()
    return v in {"1","true","yes","on"}

def _env_int(k: str, default: int) -> int:
    try: return int((os.getenv(k, str(default)) or "").strip())
    except Exception: return default

def _env_float(k: str, default: float) -> float:
    try: return float((os.getenv(k, str(default)) or "").strip())
    except Exception: return default

def _env_set(k: str) -> set[str]:
    v = (os.getenv(k, "") or "").strip()
    if not v: return set()
    return {x.strip().upper() for x in v.split(",") if x.strip()}

EXEC_RISK_PCT      = _env_float("EXEC_RISK_PCT", 0.15)      # percent of equity risk per trade
EXEC_MAX_LEVERAGE  = _env_int("EXEC_MAX_LEVERAGE", 50)
EXEC_MAKER_TICKS   = _env_int("EXEC_MAKER_TICKS", 2)
EXEC_COOLDOWN_SEC  = _env_int("EXEC_COOLDOWN_SEC", 300)
EXEC_ALLOW_SHORTS  = _env_bool("EXEC_ALLOW_SHORTS", True)
EQUITY_COIN        = os.getenv("EQUITY_COIN", "USDT") or "USDT"
TZ                 = os.getenv("TZ", "America/Phoenix") or "America/Phoenix"
TW_START           = os.getenv("EXEC_TRADING_START", "00:00")
TW_END             = os.getenv("EXEC_TRADING_END", "23:59")
ACCOUNT_UID        = (os.getenv("EXEC_ACCOUNT_UID") or "MAIN").strip()
ORDER_TAG_PREFIX   = (os.getenv("ORDER_TAG_PREFIX") or "B44").strip()

WL                 = _env_set("EXEC_SYMBOL_WHITELIST")
BL                 = _env_set("EXEC_SYMBOL_BLACKLIST")

_LAST_TRADE_TS = defaultdict(float)

# ------------- helpers -------------
def bps(a, b):
    if not a or not b: return 999999.0
    try: return abs((a - b) / ((a + b) / 2.0)) * 10000.0
    except ZeroDivisionError: return 999999.0

def _now_local():
    try:
        from zoneinfo import ZoneInfo
        return datetime.datetime.now(ZoneInfo(TZ))
    except Exception:
        return datetime.datetime.now()

def _in_trading_window() -> bool:
    now = _now_local()
    try:
        s_hour, s_min = [int(x) for x in TW_START.split(":")]
        e_hour, e_min = [int(x) for x in TW_END.split(":")]
    except Exception:
        return True
    start = now.replace(hour=s_hour, minute=s_min, second=0, microsecond=0)
    end   = now.replace(hour=e_hour, minute=e_min, second=59, microsecond=0)
    if end >= start: return start <= now <= end
    return now >= start or now <= end

def _atr14_pct_fallback(symbol: str, last: float) -> float:
    if last <= 0: return 0.005
    if symbol.upper().startswith(("BTC","ETH")): return 0.008
    return 0.012

def _compute_stop_dist(symbol: str, last: float, params: dict) -> float:
    try:
        sd = float(params.get("stop_dist"))
        if sd > 0: return sd
    except Exception:
        pass
    return _atr14_pct_fallback(symbol, last)

def _shade_limit_price(side: str, ref_px: float, tick: float) -> float:
    delta = EXEC_MAKER_TICKS * tick
    px = ref_px - delta if side.upper() == "BUY" else ref_px + delta
    return round_price(px, tick)

def _leverage_ok(qty: float, price: float, equity: float, max_lev: int) -> bool:
    if equity <= 0: return False
    notional = qty * price
    return notional <= equity * max_lev

def _risk_cash(equity: float, risk_pct: float) -> float:
    return equity * (risk_pct / 100.0)

def _link_id(sym: str, side: str, qty: float, price: float|None, tag: str, ts_ms: int) -> str:
    """
    Deterministic orderLinkId for idempotency on restarts.
    Buckets price/qty to avoid trivial drift causing duplicates.
    """
    bucket_px = 0 if price is None else int(round(float(price), 6) * 1e6)
    bucket_q  = int(round(float(qty), 6) * 1e6)
    payload = f"{sym}|{side}|{bucket_q}|{bucket_px}|{tag}|{ts_ms//30000}"  # 30s bucket to dedupe bursts
    h = hashlib.blake2s(payload.encode("utf-8"), digest_size=10).hexdigest()
    base = (tag or ORDER_TAG_PREFIX)[:12]
    return f"{base}-exe-{h}"[:36]

def place_entry(symbol, side, qty, price=None, order_tag="B44", link_id: str|None=None):
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side.upper(),
        "orderType": "Limit" if price else "Market",
        "qty": str(qty),
        **({"price": f"{price:.10f}".rstrip("0").rstrip(".")} if price else {}),
        "timeInForce": "PostOnly" if price else "IOC",
        "orderLinkId": link_id or f"{order_tag}-{int(time.time()*1000)}"
    }
    if not LIVE:
        log.info(f"[DRY] /v5/order/create {json.dumps(body, separators=(',',':'))}")
        tg_send(f"🧪 EXEC DRY • {symbol} {side} {qty} @ {body.get('price','MKT')} • {body['orderLinkId']}", priority="info")
        return {"ok": True, "dry": True, "orderLinkId": body["orderLinkId"]}
    return rc.proxy("POST", "/v5/order/create", body=body)

# ------------- main -------------
def run():
    # trading window gate
    if not _in_trading_window():
        log.info(f"outside trading window {TW_START}-{TW_END} local; skipping this pass")
        log_event("executor","window_block","",ACCOUNT_UID,{"start":TW_START,"end":TW_END})
        return

    if not may_open(ACCOUNT_UID):
        acct = get_account(ACCOUNT_UID)
        log.info(f"account policy blocks opens for {ACCOUNT_UID} mode={acct.get('mode')}")
        log_event("executor","account_block_all","",ACCOUNT_UID,{"mode":acct.get("mode")})
        return

    if not QUEUE.exists():
        log.info(f"signal queue not found: {QUEUE}")
        log_event("executor","no_signal_file","",ACCOUNT_UID,{"path":str(QUEUE)})
        return

    syms, sigs = set(), []
    for line in QUEUE.read_text(encoding="utf-8").splitlines():
        if not line.strip(): continue
        try:
            s = json.loads(line)
        except Exception:
            continue
        sigs.append(s)
        if s.get("symbol"): syms.add(s["symbol"])

    if not sigs:
        log.info("no signals to process")
        log_event("executor","no_signals","",ACCOUNT_UID)
        return

    # WL/BL gates
    if WL:
        blocked = {x for x in syms if x.upper() not in WL}
        for b in sorted(blocked):
            log.info(f"{b}: blocked by whitelist")
            log_event("executor","wl_block",b,ACCOUNT_UID)
        syms = {x for x in syms if x.upper() in WL}
        sigs = [s for s in sigs if s.get("symbol","").upper() in WL]

    if BL:
        for b in sorted({x for x in syms if x.upper() in BL}):
            log.info(f"{b}: blocked by blacklist")
            log_event("executor","bl_block",b,ACCOUNT_UID)
        syms = {s for s in syms if s.upper() not in BL}
        sigs = [s for s in sigs if s.get("symbol","").upper() not in BL]

    if not sigs:
        log.info("no signals left after WL/BL filters")
        log_event("executor","no_signals_after_filters","",ACCOUNT_UID)
        return

    # HARD guard gate: block new entries if breaker ON
    blocked, why = guard_blocking_reason()
    if blocked:
        log.info(f"guard breaker active: block new entries • {why}")
        log_event("executor","guard_block_all","",ACCOUNT_UID,{"reason":why})
        tg_send(f"⛔ Executor block: breaker ON • {why}", priority="warn")
        return

    # instruments + equity
    instr = load_or_fetch(sorted(list(syms)))
    try:
        equity = rc.equity_unified(coin=EQUITY_COIN) if hasattr(rc, "equity_unified") else rc.equity_unified()
    except Exception:
        equity = 0.0

    if equity <= 0:
        log.warning(f"equity=0 or fetch failed; using 10 {EQUITY_COIN} as dummy base")
        equity = 10.0

    # per-account risk multiplier
    rmul = max(0.0, float(risk_multiplier(ACCOUNT_UID)))
    eff_risk_pct = max(0.0, EXEC_RISK_PCT * rmul)

    processed = 0

    for s in sigs:
        sym = s.get("symbol","").upper()
        typ = str(s.get("signal","")).upper()
        params = s.get("params") or {}
        maker_only = bool(params.get("maker_only", True))
        spread_max_bps = float(params.get("spread_max_bps", 8))
        tag = str(params.get("tag") or ORDER_TAG_PREFIX)

        if not sym:
            continue

        # account-level symbol allow-list
        if not symbol_allowed(ACCOUNT_UID, sym):
            log_event("executor","account_symbol_block",sym,ACCOUNT_UID)
            continue

        # side filter
        if typ.startswith("SHORT") and not EXEC_ALLOW_SHORTS:
            log.info(f"{sym}: short signals disabled; skipping")
            log_event("executor","shorts_disabled",sym,ACCOUNT_UID)
            continue

        want_buy = typ in ("LONG_TEST","LONG_BREAKOUT","LONG")
        want_sell = typ.startswith("SHORT")
        if not (want_buy or want_sell):
            continue

        # portfolio guard (soft concurrency/drawdown gate)
        if _guard is not None and not _guard.allow_new_trade(sym):
            hb = _guard.heartbeat()
            log.info(f"{sym}: blocked by portfolio guard (halted={hb.get('halted')}, open={len(hb.get('open_trades',{}))})")
            log_event("executor","guard_block_soft",sym,ACCOUNT_UID,{"halted":hb.get("halted"),"open":len(hb.get("open_trades",{}))})
            continue

        meta = instr.get(sym)
        if not meta:
            log.warning(f"{sym}: no instrument meta; skipping")
            log_event("executor","no_instrument_meta",sym,ACCOUNT_UID)
            continue
        tick = meta["tickSize"]; step = meta["lotStep"] if "lotStep" in meta else meta.get("qtyStep", meta.get("stepSize", 0))
        minq = meta.get("minQty", 0)

        tk = rc.ticker(sym)
        if not tk:
            log.warning(f"{sym}: no ticker")
            log_event("executor","no_ticker",sym,ACCOUNT_UID)
            continue
        try:
            bid = float(tk.get("bid1Price") or 0)
            ask = float(tk.get("ask1Price") or 0)
            last = float(tk.get("lastPrice") or 0)
        except Exception:
            log.warning(f"{sym}: bad ticker fields")
            log_event("executor","bad_ticker",sym,ACCOUNT_UID,{"raw":tk})
            continue

        spr_bps = bps(ask, bid)
        if maker_only and spr_bps > spread_max_bps:
            log.info(f"{sym}: spread {spr_bps:.1f}bps > {spread_max_bps}bps; skip")
            log_event("executor","spread_block",sym,ACCOUNT_UID,{"spread_bps":spr_bps,"max_bps":spread_max_bps})
            continue

        now = time.time()
        if now - _LAST_TRADE_TS[sym] < EXEC_COOLDOWN_SEC:
            log.info(f"{sym}: cooldown active; skipping")
            log_event("executor","cooldown_block",sym,ACCOUNT_UID,{"cooldown_sec":EXEC_COOLDOWN_SEC})
            continue

        stop_dist_frac = _compute_stop_dist(sym, last, params)
        stop_dist = max(stop_dist_frac, 1e-6) * max(last, 1e-9)

        risk_cash_val = _risk_cash(equity, eff_risk_pct)
        if risk_cash_val <= 0:
            log.info(f"{sym}: risk_cash <= 0; skip")
            log_event("executor","zero_risk_cash",sym,ACCOUNT_UID,{"equity":equity,"risk_pct":eff_risk_pct})
            continue

        raw_qty = risk_cash_val / stop_dist
        # leverage cap
        notional = raw_qty * last
        max_notional = equity * EXEC_MAX_LEVERAGE
        if notional > max_notional:
            raw_qty = max_notional / max(last, 1e-9)

        qty = round_qty(raw_qty, tick_step := step, min_qty := minq)
        if qty <= 0:
            log.info(f"{sym}: qty {raw_qty:.8f} -> {qty} after rounding; skip")
            log_event("executor","qty_round_block",sym,ACCOUNT_UID,{"raw_qty":raw_qty,"step":step,"minQty":minq})
            continue

        if want_buy:
            side = "Buy"
            ref_px = bid if maker_only else last
        else:
            side = "Sell"
            ref_px = ask if maker_only else last

        price = _shade_limit_price(side, ref_px, tick) if maker_only else None

        if not _leverage_ok(qty, (price or last), equity, EXEC_MAX_LEVERAGE):
            log.info(f"{sym}: leverage cap would be exceeded; skip")
            log_event("executor","leverage_block",sym,ACCOUNT_UID,{"qty":qty,"px":price or last,"equity":equity,"maxLev":EXEC_MAX_LEVERAGE})
            continue

        # deterministic link id
        ts_ms = int(s.get("ts") or int(now*1000))
        link_id = _link_id(sym, side, qty, price, tag, ts_ms)

        # DB: record intent
        try:
            insert_order(link_id, sym, side, float(qty), float(price) if price else None, tag, state="NEW")
        except Exception:
            pass

        # submit
        log_event("executor","order_submit",sym,ACCOUNT_UID,{
            "side": side, "qty": qty, "price": price or "MKT",
            "risk_pct": eff_risk_pct, "spr_bps": spr_bps,
            "maker_only": maker_only, "tag": tag, "link": link_id
        })

        res = place_entry(sym, side, qty, price=price if maker_only else None, order_tag=tag, link_id=link_id)
        _LAST_TRADE_TS[sym] = now

        mode = "LIVE" if LIVE else "DRY"
        if isinstance(res, dict) and res.get("ok") and res.get("dry"):
            try:
                set_order_state(link_id, "OPEN", exchange_id=None)
            except Exception:
                pass
            msg = (
                f"✅ EXEC {mode} • {sym} {side.upper()} {qty} @ {price if price else 'MKT'} "
                f"• risk {eff_risk_pct:.3f}% of {EQUITY_COIN}≈{risk_cash_val:.4f} "
                f"• spr {spr_bps:.1f}bps • lev≤{EXEC_MAX_LEVERAGE}x"
            )
            tg_send(msg, priority="info")
            log_event("executor","order_ack_dry",sym,ACCOUNT_UID,{"resp":"ok","link":link_id})
        elif isinstance(res, dict) and res.get("ok"):
            exid = (res.get("result") or {}).get("orderId") if isinstance(res.get("result"), dict) else None
            try:
                set_order_state(link_id, "OPEN", exchange_id=exid)
            except Exception:
                pass
            tg_send(
                f"🟢 EXEC LIVE • {sym} {side.upper()} {qty} @ {price if price else 'MKT'} • link={link_id}",
                priority="success"
            )
            log_event("executor","order_ack_live",sym,ACCOUNT_UID,{"resp":str(res)[:200],"link":link_id})
        else:
            errtxt = str(res)[:200]
            try:
                set_order_state(link_id, "REJECTED", err_code="API", err_msg=errtxt)
            except Exception:
                pass
            tg_send(f"❌ EXEC {mode} • {sym} place failed • {errtxt}", priority="error")
            log_event("executor","order_fail",sym,ACCOUNT_UID,{"resp":errtxt,"link":link_id}, level="error")

        log.info(f"sym={sym} type={typ} maker={maker_only} qty={qty} bid={bid} ask={ask} spr={spr_bps:.1f}bps mode={'LIVE' if LIVE else 'DRY'}")
        processed += 1

    log.info(f"done processed={processed}, LIVE={LIVE}")
    log_event("executor","batch_done","",ACCOUNT_UID,{"processed":processed,"live":LIVE})

if __name__ == "__main__":
    base = os.getenv('EXECUTOR_RELAY_BASE') or os.getenv('RELAY_BASE') or 'http://127.0.0.1:5000'
    acct = get_account(ACCOUNT_UID)
    log.info(
        f"executor LIVE={LIVE} relay={base} window={TW_START}-{TW_END} "
        f"risk={EXEC_RISK_PCT:.3f}% rmul={risk_multiplier(ACCOUNT_UID):.3f} "
        f"lev≤{EXEC_MAX_LEVERAGE}x WL={','.join(sorted(WL)) or '-'} BL={','.join(sorted(BL)) or '-'} "
        f"acct={ACCOUNT_UID}:{acct.get('mode','auto')}"
    )
    log_event("executor","startup","",ACCOUNT_UID,{
        "live": LIVE, "relay": base, "window":[TW_START,TW_END],
        "risk_pct_base": EXEC_RISK_PCT, "risk_mul": risk_multiplier(ACCOUNT_UID),
        "max_lev": EXEC_MAX_LEVERAGE, "wl": list(sorted(WL)), "bl": list(sorted(BL))
    })
    run()
