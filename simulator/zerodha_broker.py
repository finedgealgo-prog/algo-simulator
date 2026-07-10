"""
zerodha_broker.py
-----------------
Zerodha Kite Connect integration for live option chain data.
"""

import json
import math
import os
import datetime
import logging
import time
import threading

from kiteconnect import KiteConnect

logger = logging.getLogger(__name__)

# ── Caches ──────────────────────────────────────────────────────────────────
# instruments list: heavy download, valid for 1 hour (doesn't change intraday)
_instruments_cache: dict = {}       # exchange → (fetched_at, data)
_instruments_ttl   = 3600           # seconds

# full option chain response: valid for 30 s (UI may reload quickly)
_chain_cache: dict = {}             # (symbol, near) → (fetched_at, data)
_chain_ttl   = 30                   # seconds

_cache_lock = threading.Lock()
# ────────────────────────────────────────────────────────────────────────────

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "zerodha_config.json")
TOKEN_FILE  = os.path.join(os.path.dirname(__file__), "..", "zerodha_token.json")

SYMBOL_MAP = {
    "nifty":       "NIFTY",
    "banknifty":   "BANKNIFTY",
    "finnifty":    "FINNIFTY",
    "midcpnifty":  "MIDCPNIFTY",
    "sensex":      "SENSEX",
    "bankex":      "BANKEX",
}

EXCHANGE_MAP = {
    "NIFTY":      "NFO",
    "BANKNIFTY":  "NFO",
    "FINNIFTY":   "NFO",
    "MIDCPNIFTY": "NFO",
    "SENSEX":     "BFO",
    "BANKEX":     "BFO",
}

SPOT_SYMBOL_MAP = {
    "NIFTY":      "NSE:NIFTY 50",
    "BANKNIFTY":  "NSE:NIFTY BANK",
    "FINNIFTY":   "NSE:NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
    "SENSEX":     "BSE:SENSEX",
    "BANKEX":     "BSE:BANKEX",
}


class ZerodhaBroker:
    def __init__(self):
        self.config = self._load_config()
        api_key = self.config.get("api_key", "")
        self.kite = KiteConnect(api_key=api_key) if api_key else None
        self._load_token()

    # ── config ────────────────────────────────────────────────────────────
    def _load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE) as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def save_config(self, api_key: str, api_secret: str):
        self.config = {"api_key": api_key.strip(), "api_secret": api_secret.strip()}
        with open(CONFIG_FILE, "w") as f:
            json.dump(self.config, f)
        self.kite = KiteConnect(api_key=self.config["api_key"])
        logger.info("Zerodha config saved.")

    def has_config(self):
        return bool(self.config.get("api_key") and self.config.get("api_secret"))

    # ── token ─────────────────────────────────────────────────────────────
    def _load_token(self):
        try:
            if self.kite and os.path.exists(TOKEN_FILE):
                with open(TOKEN_FILE) as f:
                    data = json.load(f)
                token = data.get("access_token")
                if token:
                    self.kite.set_access_token(token)
        except Exception:
            pass

    def _save_token(self, data: dict):
        serialisable = {k: str(v) if isinstance(v, datetime.date) else v
                        for k, v in data.items()}
        with open(TOKEN_FILE, "w") as f:
            json.dump(serialisable, f)

    # ── auth ──────────────────────────────────────────────────────────────
    def get_login_url(self):
        if not self.kite:
            raise RuntimeError("API key not configured.")
        return self.kite.login_url()

    def generate_session(self, request_token: str):
        if not self.kite:
            raise RuntimeError("API key not configured.")
        data = self.kite.generate_session(
            request_token,
            api_secret=self.config["api_secret"],
        )
        self.kite.set_access_token(data["access_token"])
        self._save_token(data)
        logger.info(f"Zerodha session generated for user: {data.get('user_name')}")
        return data

    # ── status ────────────────────────────────────────────────────────────
    def is_connected(self):
        if not self.kite:
            return False, None
        try:
            profile = self.kite.profile()
            return True, profile
        except Exception:
            return False, None

    # ── live option chain ─────────────────────────────────────────────────
    def get_live_option_chain(self, symbol: str = "nifty", near_expiry_only: bool = False, extra_expiries: list = None):
        if not self.kite:
            raise RuntimeError("Zerodha not connected.")

        sym      = SYMBOL_MAP.get(symbol.lower(), symbol.upper())
        exchange = EXCHANGE_MAP.get(sym, "NFO")
        spot_sym = SPOT_SYMBOL_MAP.get(sym, f"NSE:{sym}")
        extra_expiries = sorted(set(extra_expiries or []))
        cache_key = (sym, near_expiry_only, tuple(extra_expiries))
        now_ts    = time.monotonic()

        # ── Layer 1: full response cache (30 s) ─────────────────────────────
        with _cache_lock:
            cached = _chain_cache.get(cache_key)
            if cached and (now_ts - cached[0]) < _chain_ttl:
                logger.info(f"[cache] Returning cached option chain for {sym} (age {now_ts - cached[0]:.1f}s)")
                return cached[1]

        # 1. Spot quote (OHLC for day_open / prev_close + spot price)
        spot_data  = self.kite.quote([spot_sym])
        sq         = spot_data.get(spot_sym, {})
        spot_price = float(sq.get("last_price", 0))
        spot_ohlc  = sq.get("ohlc", {})
        day_open   = float(spot_ohlc.get("open", 0))
        prev_close = float(spot_ohlc.get("close", 0))  # Kite "close" = previous day close

        # ── Layer 2: instruments cache (1 h) ────────────────────────────────
        with _cache_lock:
            cached_inst = _instruments_cache.get(exchange)
        if cached_inst and (now_ts - cached_inst[0]) < _instruments_ttl:
            instruments = cached_inst[1]
            logger.info(f"[cache] Using cached instruments for {exchange}")
        else:
            instruments = self.kite.instruments(exchange)
            with _cache_lock:
                _instruments_cache[exchange] = (now_ts, instruments)
            logger.info(f"[fetch] Downloaded {len(instruments)} instruments for {exchange}")

        # 3. Filter: target symbol + options only
        now_date = datetime.date.today()
        options = [
            i for i in instruments
            if i["name"] == sym
            and i["instrument_type"] in ("CE", "PE")
            and i["expiry"] >= now_date
        ]

        if not options:
            return []

        # 4. Expiry filter — current month + any extra_expiries requested
        all_expiries = sorted(set(str(i["expiry"])[:10] for i in options))
        if near_expiry_only:
            current_month = now_date.strftime("%Y-%m")
            near_expiries = [e for e in all_expiries if e.startswith(current_month)]
            if not near_expiries:
                near_expiries = all_expiries[:1]
            # also include expiries explicitly requested (e.g. open-position expiries)
            for exp in extra_expiries:
                if exp in all_expiries and exp not in near_expiries:
                    near_expiries.append(exp)
            near_expiries = sorted(near_expiries)
            options  = [i for i in options if str(i["expiry"])[:10] in near_expiries]
            expiries = near_expiries
        else:
            expiries = all_expiries
        logger.info(f"Expiries for {sym} (near={near_expiry_only}): {expiries}")

        # ── Layer 3: throttled batch quotes (0.35 s between batches) ────────
        trading_symbols = [f"{exchange}:{i['tradingsymbol']}" for i in options]
        total       = len(trading_symbols)
        all_quotes: dict = {}
        batch_size  = 450
        total_batches = (total + batch_size - 1) // batch_size
        logger.info(f"{sym}: {total} symbols across {total_batches} quote batches")

        for batch_num, idx in enumerate(range(0, total, batch_size), 1):
            batch = trading_symbols[idx: idx + batch_size]
            try:
                q = self.kite.quote(batch)
                all_quotes.update(q)
                logger.info(f"Batch {batch_num}/{total_batches}: got {len(q)} quotes")
            except Exception as e:
                logger.warning(f"Batch {batch_num}/{total_batches} error: {e}")
            if batch_num < total_batches:
                time.sleep(0.35)   # throttle — stay within Kite rate limits

        # 6. Build result rows (same schema as MongoDB option_chain collection)
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%dT%H:%M:00")

        rows = []
        for inst in options:
            key = f"{exchange}:{inst['tradingsymbol']}"
            if key not in all_quotes:
                continue
            q = all_quotes[key]
            ohlc  = q.get("ohlc", {})
            ltp   = float(q.get("last_price", 0))
            # IV proxy: annualised decimal (e.g. 0.15 = 15%).
            # Brenner-Subrahmanyam ATM approx: sigma ≈ C / (S * sqrt(T / 2π))
            # Using DTE from instrument expiry for accurate T per leg.
            dte = max((inst["expiry"] - now_date).days, 1)
            t_years = dte / 365.0
            if ltp > 0:
                bs_proxy = ltp / max(spot_price, 1) * math.sqrt(2 * math.pi / t_years)
                iv_proxy = round(min(max(bs_proxy, 0.05), 1.0), 6)
            else:
                iv_proxy = 0.15
            rows.append({
                "timestamp":   timestamp,
                "underlying":  sym,
                "strike":      float(inst["strike"]),
                "type":        inst["instrument_type"],   # CE / PE
                "expiry":      str(inst["expiry"])[:10],
                "spot_price":  spot_price,
                "open":        float(ohlc.get("open", 0)),
                "high":        float(ohlc.get("high", 0)),
                "low":         float(ohlc.get("low", 0)),
                "close":       ltp,
                "volume":      int(q.get("volume", 0)),
                "oi":          int(q.get("oi", 0)),
                # Greeks placeholders (not available from Kite quote API)
                "iv":          iv_proxy,
                "delta":       0,
                "gamma":       0,
                "theta":       0,
                "vega":        0,
                "rho":         0,
            })

        logger.info(f"Live option chain: {sym} | {len(rows)} rows | expiries: {expiries}")

        # Nearest futures price
        fut_price  = None
        fut_expiry = None
        try:
            fut_instruments = sorted(
                [i for i in instruments if i["name"] == sym and i["instrument_type"] == "FUT" and i["expiry"] >= now_date],
                key=lambda i: i["expiry"]
            )
            if fut_instruments:
                near_fut   = fut_instruments[0]
                fut_sym    = f"{exchange}:{near_fut['tradingsymbol']}"
                fq         = self.kite.ltp([fut_sym])
                fut_price  = float(fq.get(fut_sym, {}).get("last_price", 0))
                fut_expiry = str(near_fut["expiry"])[:10]
        except Exception as e:
            logger.warning(f"Futures fetch error in option chain: {e}")

        market_stats = {
            "symbol":     sym,
            "spot":       spot_price,
            "day_open":   day_open,
            "prev_close": prev_close,
            "futures":    fut_price,
            "fut_expiry": fut_expiry,
        }

        result = {"rows": rows, "market_stats": market_stats}

        # Store in response cache
        with _cache_lock:
            _chain_cache[cache_key] = (time.monotonic(), result)

        return result

    def get_market_stats(self, symbol: str = "nifty"):
        """Return day_open, prev_close, spot, nearest futures price + expiry."""
        if not self.kite:
            raise RuntimeError("Zerodha not connected.")

        sym      = SYMBOL_MAP.get(symbol.lower(), symbol.upper())
        exchange = EXCHANGE_MAP.get(sym, "NFO")
        spot_sym = SPOT_SYMBOL_MAP.get(sym, f"NSE:{sym}")

        # Spot OHLC (day_open + prev_close)
        spot_quote = self.kite.quote([spot_sym])
        sq         = spot_quote.get(spot_sym, {})
        spot       = float(sq.get("last_price", 0))
        ohlc       = sq.get("ohlc", {})
        day_open   = float(ohlc.get("open", 0))
        prev_close = float(ohlc.get("close", 0))   # Kite "close" = previous day close

        # Nearest futures contract
        now_date = datetime.date.today()
        with _cache_lock:
            cached_inst = _instruments_cache.get(exchange)
        if cached_inst and (time.monotonic() - cached_inst[0]) < _instruments_ttl:
            instruments = cached_inst[1]
        else:
            instruments = self.kite.instruments(exchange)
            with _cache_lock:
                _instruments_cache[exchange] = (time.monotonic(), instruments)

        fut_instruments = sorted(
            [i for i in instruments if i["name"] == sym and i["instrument_type"] == "FUT" and i["expiry"] >= now_date],
            key=lambda i: i["expiry"]
        )

        fut_price  = None
        fut_expiry = None
        if fut_instruments:
            near_fut = fut_instruments[0]
            fut_sym  = f"{exchange}:{near_fut['tradingsymbol']}"
            try:
                fq = self.kite.ltp([fut_sym])
                fut_price  = float(fq.get(fut_sym, {}).get("last_price", 0))
                fut_expiry = str(near_fut["expiry"])[:10]
            except Exception as e:
                logger.warning(f"Futures price fetch error: {e}")

        return {
            "symbol":     sym,
            "spot":       spot,
            "day_open":   day_open,
            "prev_close": prev_close,
            "futures":    fut_price,
            "fut_expiry": fut_expiry,
        }
