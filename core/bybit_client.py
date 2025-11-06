#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Bybit v5 Client (zero external deps)

Goals
- One tiny, dependable wrapper for Bybit v5.
- Works with mainnet/testnet via core.config.settings.
- Signed private requests, robust timestamp handling with drift correction.
- Simple retry policy for flaky network and timestamp errors.
- Proxy support via settings.PROXY_URL.
- Helpful logging that doesn't drown you.

Usage
    from core.bybit_client import Bybit
    from core.logger import get_logger

    log = get_logger("core.bybit_client.demo")
    by = Bybit()

    ok, data, err = by.get_wallet_balance(coin="USDT")
    if not ok:
        log.error("balance fail: %s", err)
    else:
        log.info("balance: %s", data)

Design notes
- No third-party HTTP libs (uses urllib).
- Returns (ok, data, err) everywhere:
    ok=True  -> data=dict (server JSON 'result' or full data)
    ok=False -> err=str   (human-readable), data may hold raw response
- Does not throw unless you asked for it; your bots shouldn't crash on a 10010.

Caveats
- You’re responsible for providing valid API keys and IP permissions on Bybit.
- This file doesn’t place position-level TP via set_trading_stop because you told me not to earlier. Add it if you want.

"""

from __future__ import annotations
import datetime as _dt
import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, Optional, Tuple

from core.config import settings
from core.logger import get_logger

log = get_logger("core.bybit_client")

# ---------- helpers ----------

def _now_ms_utc() -> int:
    return int(time.time() * 1000)

def _json_dumps(obj: Any) -> str:
    # Compact and stable
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

def _build_opener(proxy_url: Optional[str]):
    if proxy_url:
        proxy = { "http": proxy_url, "https": proxy_url }
        return urllib.request.build_opener(urllib.request.ProxyHandler(proxy))
    return urllib.request.build_opener()

# ---------- client ----------

class Bybit:
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
        recv_window_ms: int = 5000,
        max_retries: int = 3,
        proxy_url: Optional[str] = None,
    ):
        self.api_key = (api_key or settings.BYBIT_API_KEY or "").strip()
        self.api_secret = (api_secret or settings.BYBIT_API_SECRET or "").strip()
        self.base_url = (base_url or settings.BYBIT_BASE_URL).rstrip("/")
        self.recv_window_ms = recv_window_ms
        self.max_retries = max_retries
        self.opener = _build_opener(proxy_url or settings.PROXY_URL)
        self._time_delta_ms = 0  # server_time - local_time

        if not self.api_key or not self.api_secret:
            log.warning("[bybit] API keys missing. Private endpoints will fail.")

    # ----- time sync -----

    def sync_time(self) -> bool:
        """
        Sync local time delta using /v5/market/time
        """
        ok, data, err = self._request_public("/v5/market/time", params=None)
        if not ok:
            log.warning("[bybit] time sync failed: %s", err)
            return False
        try:
            # Bybit returns {"retCode":0,"retMsg":"OK","result":{"timeSecond":"...","timeNano":"..."}}
            server_ms = int(data["result"]["timeSecond"]) * 1000
            local_ms = _now_ms_utc()
            self._time_delta_ms = server_ms - local_ms
            log.info("[bybit] time synced: delta=%dms", self._time_delta_ms)
            return True
        except Exception as e:
            log.warning("[bybit] time sync parse error: %s", e)
            return False

    def _ts_ms(self) -> int:
        # Local now adjusted by last known delta
        return _now_ms_utc() + self._time_delta_ms

    # ----- low-level requestors -----

    def _sign(self, ts_ms: int, query_or_body: str) -> str:
        """
        Per Bybit v5: sign = HMAC_SHA256( timestamp + apiKey + recvWindow + (querystring|body) )
        """
        msg = f"{ts_ms}{self.api_key}{self.recv_window_ms}{query_or_body}".encode("utf-8")
        key = self.api_secret.encode("utf-8")
        return hmac.new(key, msg, hashlib.sha256).hexdigest()

    def _headers_private(self, ts_ms: int, sign: str) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": sign,
            "X-BAPI-TIMESTAMP": str(ts_ms),
            "X-BAPI-RECV-WINDOW": str(self.recv_window_ms),
        }

    def _request_public(
        self, path: str, params: Optional[Dict[str, Any]], method: str = "GET"
    ) -> Tuple[bool, Dict[str, Any], str]:
        url = f"{self.base_url}{path}"
        if params:
            q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{url}?{q}"

        req = urllib.request.Request(url=url, method=method)
        try:
            with self.opener.open(req, timeout=settings.HTTP_TIMEOUT_S) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
            return False, {}, f"HTTP {e.code} {raw[:300]}"
        except Exception as e:
            return False, {}, f"network error: {e}"

        try:
            data = json.loads(raw)
        except Exception:
            return False, {}, f"bad json: {raw[:300]}"

        if data.get("retCode") == 0 or data.get("ok") is True:
            return True, data, ""
        return False, data, f"retCode={data.get('retCode')} retMsg={data.get('retMsg')}"

    def _request_private_json(
        self, path: str, body: Optional[Dict[str, Any]] = None, method: str = "POST"
    ) -> Tuple[bool, Dict[str, Any], str]:
        """
        Private request with JSON body (for POST endpoints). Retries on timestamp issues.
        """
        if not self.api_key or not self.api_secret:
            return False, {}, "missing API credentials"

        attempt = 0
        last_err = ""
        while attempt < self.max_retries:
            attempt += 1
            ts = self._ts_ms()
            payload_str = _json_dumps(body or {})
            sign = self._sign(ts, payload_str)
            headers = self._headers_private(ts, sign)

            url = f"{self.base_url}{path}"
            req = urllib.request.Request(url=url, data=payload_str.encode("utf-8"), headers=headers, method=method)

            try:
                with self.opener.open(req, timeout=settings.HTTP_TIMEOUT_S) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
            except urllib.error.HTTPError as e:
                raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
                code = e.code
                # Retry 400/401 only if timestamp related
                if self._should_resync(raw):
                    self.sync_time()
                    last_err = f"HTTP {code} {raw[:300]}"
                    continue
                return False, {}, f"HTTP {code} {raw[:300]}"
            except Exception as e:
                last_err = f"network error: {e}"
                time.sleep(0.4 * attempt)
                continue

            # Parse & check Bybit envelope
            try:
                data = json.loads(raw)
            except Exception:
                return False, {}, f"bad json: {raw[:300]}"

            rc = data.get("retCode")
            if rc == 0:
                return True, data, ""
            # Common timestamp/expiry issues -> resync and retry
            if self._should_resync(json.dumps(data)):
                self.sync_time()
                last_err = f"retCode={rc} retMsg={data.get('retMsg')}"
                continue

            return False, data, f"retCode={rc} retMsg={data.get('retMsg')}"

        return False, {}, last_err or "max retries exceeded"

    def _request_private_query(
        self, path: str, params: Optional[Dict[str, Any]] = None, method: str = "GET"
    ) -> Tuple[bool, Dict[str, Any], str]:
        """
        Private request with querystring signing (for GET endpoints).
        """
        if not self.api_key or not self.api_secret:
            return False, {}, "missing API credentials"

        attempt = 0
        last_err = ""
        params = params or {}
        # Bybit requires consistent encoding ordering
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})

        while attempt < self.max_retries:
            attempt += 1
            ts = self._ts_ms()
            sign = self._sign(ts, query)
            headers = self._headers_private(ts, sign)

            url = f"{self.base_url}{path}"
            if query:
                url = f"{url}?{query}"

            req = urllib.request.Request(url=url, headers=headers, method=method)

            try:
                with self.opener.open(req, timeout=settings.HTTP_TIMEOUT_S) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
            except urllib.error.HTTPError as e:
                raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
                code = e.code
                if self._should_resync(raw):
                    self.sync_time()
                    last_err = f"HTTP {code} {raw[:300]}"
                    continue
                return False, {}, f"HTTP {code} {raw[:300]}"
            except Exception as e:
                last_err = f"network error: {e}"
                time.sleep(0.4 * attempt)
                continue

            try:
                data = json.loads(raw)
            except Exception:
                return False, {}, f"bad json: {raw[:300]}"

            rc = data.get("retCode")
            if rc == 0:
                return True, data, ""
            if self._should_resync(json.dumps(data)):
                self.sync_time()
                last_err = f"retCode={rc} retMsg={data.get('retMsg')}"
                continue

            return False, data, f"retCode={rc} retMsg={data.get('retMsg')}"

        return False, {}, last_err or "max retries exceeded"

    @staticmethod
    def _should_resync(raw: str) -> bool:
        s = raw.lower()
        # Bybit will often say invalid timestamp or expired recv window
        return ("timestamp" in s and ("invalid" in s or "expired" in s)) or "recvwindow" in s

    # ----- convenience wrappers (you’ll actually use these) -----

    # Public
    def get_tickers(self, category: str = "linear", symbol: Optional[str] = None) -> Tuple[bool, Dict[str, Any], str]:
        params = {"category": category}
        if symbol:
            params["symbol"] = symbol
        return self._request_public("/v5/market/tickers", params=params)

    # Private
    def get_wallet_balance(self, accountType: str = "UNIFIED", coin: Optional[str] = None) -> Tuple[bool, Dict[str, Any], str]:
        params = {"accountType": accountType}
        if coin:
            params["coin"] = coin
        return self._request_private_query("/v5/account/wallet-balance", params=params)

    def get_positions(self, category: str = "linear", symbol: Optional[str] = None) -> Tuple[bool, Dict[str, Any], str]:
        params = {"category": category}
        if symbol:
            params["symbol"] = symbol
        return self._request_private_query("/v5/position/list", params=params)

    def place_order(
        self,
        category: str,
        symbol: str,
        side: str,
        orderType: str,
        qty: str,
        price: Optional[str] = None,
        timeInForce: str = "GoodTillCancel",
        reduceOnly: Optional[bool] = None,
        positionIdx: Optional[int] = None,
        triggerDirection: Optional[int] = None,
        orderLinkId: Optional[str] = None,
        takeProfit: Optional[str] = None,
        stopLoss: Optional[str] = None,
        tpslMode: Optional[str] = None,  # "Full" | "Partial"
        marketUnit: Optional[str] = None,  # "baseCoin" | "quoteCoin"
        # add other fields as needed
    ) -> Tuple[bool, Dict[str, Any], str]:
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": orderType,
            "qty": qty,
            "timeInForce": timeInForce,
        }
        if price is not None:
            body["price"] = price
        if reduceOnly is not None:
            body["reduceOnly"] = reduceOnly
        if positionIdx is not None:
            body["positionIdx"] = positionIdx
        if triggerDirection is not None:
            body["triggerDirection"] = triggerDirection
        if orderLinkId:
            body["orderLinkId"] = orderLinkId
        if takeProfit is not None:
            body["takeProfit"] = takeProfit
        if stopLoss is not None:
            body["stopLoss"] = stopLoss
        if tpslMode is not None:
            body["tpslMode"] = tpslMode
        if marketUnit is not None:
            body["marketUnit"] = marketUnit

        return self._request_private_json("/v5/order/create", body=body, method="POST")

    def cancel_order(
        self,
        category: str,
        symbol: str,
        orderId: Optional[str] = None,
        orderLinkId: Optional[str] = None,
    ) -> Tuple[bool, Dict[str, Any], str]:
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
        }
        if orderId:
            body["orderId"] = orderId
        if orderLinkId:
            body["orderLinkId"] = orderLinkId

        return self._request_private_json("/v5/order/cancel", body=body, method="POST")

    def cancel_all(
        self,
        category: str,
        symbol: Optional[str] = None,
        baseCoin: Optional[str] = None,
        settleCoin: Optional[str] = None,
    ) -> Tuple[bool, Dict[str, Any], str]:
        body: Dict[str, Any] = {"category": category}
        if symbol:
            body["symbol"] = symbol
        if baseCoin:
            body["baseCoin"] = baseCoin
        if settleCoin:
            body["settleCoin"] = settleCoin

        return self._request_private_json("/v5/order/cancel-all", body=body, method="POST")

    def get_open_orders(
        self, category: str, symbol: Optional[str] = None, openOnly: bool = True
    ) -> Tuple[bool, Dict[str, Any], str]:
        params: Dict[str, Any] = {"category": category}
        if symbol:
            params["symbol"] = symbol
        if openOnly is not None:
            params["openOnly"] = int(bool(openOnly))
        return self._request_private_query("/v5/order/realtime", params=params)

    # Example of position TP/SL adjust if you re-enable it later:
    # def set_trading_stop(...): return self._request_private_json("/v5/position/trading-stop", body=payload)

# ---------- self-test ----------

if __name__ == "__main__":
    """
    Quick CLI checks:
        python -m core.bybit_client
    """
    by = Bybit()
    by.sync_time()  # no harm if it fails

    ok, data, err = by.get_tickers(category="linear", symbol="BTCUSDT")
    if ok:
        print("Tickers OK:", _json_dumps(data.get("result", {}) )[:300])
    else:
        print("Tickers ERR:", err)

    ok, data, err = by.get_wallet_balance(coin="USDT")
    if ok:
        print("Balance OK:", _json_dumps(data.get("result", {}) )[:300])
    else:
        print("Balance ERR:", err)
