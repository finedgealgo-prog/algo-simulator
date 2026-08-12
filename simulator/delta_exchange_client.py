"""
delta_exchange_client.py
─────────────────────────
Standalone Delta Exchange (crypto derivatives) REST client — deliberately NOT
routed through features/broker_gateway.py. That gateway's "one active broker"
model (kite vs dhan, switched via kite_market_config.enabled) is for NSE
equity/index brokers that are interchangeable with each other; Delta is a
different asset class (BTC/ETH crypto options) running alongside Dhan/Kite,
not instead of it, so it gets its own credentials read, its own REST calls,
and (see delta_exchange_ws.py) its own websocket — nothing here is shared
with or wired into the existing broker plumbing.

Credentials live in the same `kite_market_config` collection as every other
broker (doc with broker="deltaExchange"), read fresh from Mongo on each call
— no in-process caching of the secret.

Market-data endpoints used here (products, tickers) are PUBLIC on Delta's API
(no signature required). `_signed_headers()` below is prepared for future
authenticated calls (order placement, balances, positions) but nothing here
invokes it yet — this module currently only reads chain data.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import socket
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
import urllib3.util.connection as _urllib3_connection

log = logging.getLogger(__name__)

# api.india.delta.exchange resolves dual-stack (A + AAAA records) behind a CDN. On any
# host with working outbound IPv6 (most cloud/home connections today), Python's default
# getaddrinfo() puts the IPv6 addresses first and urllib3/requests connects via whichever
# resolves first — so every call here was silently going out over this machine's IPv6
# address, not its IPv4 one. Delta's API-key IP whitelist only had the IPv4 address on it
# (see delta_exchange_router.py's broker-status endpoint / kite_market_config), so every
# signed request was rejected with ip_not_whitelisted_for_api_key even after the IPv4 was
# correctly whitelisted — confirmed by comparing `getaddrinfo(..., AF_INET)` (IPv4 only)
# against the default AF_UNSPEC result, which came back IPv6-first. Forcing IPv4 here is
# the standard urllib3 recipe (patching allowed_gai_family, which urllib3.connection's
# create_connection() consults process-wide) — safe globally in this process since nothing
# else this backend talks to (Mongo, Dhan/Kite/FlatTrade REST) is IPv6-only.
_urllib3_connection.allowed_gai_family = lambda: socket.AF_INET

DELTA_REST_BASE = "https://api.india.delta.exchange"
_HTTP_TIMEOUT = 6.0
IST = timezone(timedelta(hours=5, minutes=30))

# Delta quotes option/move-option mark_price (and best_bid/best_ask) *per 1 unit of the
# underlying* — e.g. what an option on a full 1 BTC would cost — not the actual USD debited
# for one contract, which only ever represents contract_value of that (0.001 BTC for BTC,
# 0.01 ETH for ETH; see /v2/products' own "contract_value" field, stable across every live
# strike/expiry we checked). Verified independently: Black-Scholes on live spot/strike/IV for
# a same-day ATM BTC straddle gives ~$818 raw, matching the unscaled mark_price sum almost
# exactly — and $818 * 0.001 = $0.82, matching a third-party platform's (AlgoTest) displayed
# max-profit for the identical position almost exactly. Every ltp/bid/ask below gets this
# scaling applied once, right here, so every consumer (chain display, order pad, margin,
# net-premium, MTM) sees the real per-contract price without redoing this conversion itself.
# Perpetual futures are NOT included — fetch_perpetual_ticker's mark_price already IS the
# real spot-equivalent USD price; contract_value there only affects margin/notional sizing,
# not the quote itself. Greeks (delta/gamma/theta/vega) are left in Delta's native per-1-BTC
# convention too — display-only fields here, nothing in this codebase computes $ P&L from
# them directly (everything re-derives via its own calibrated Black-Scholes instead).
DELTA_CONTRACT_VALUE: dict[str, float] = {"BTC": 0.001, "ETH": 0.01}


def get_delta_credentials(db) -> dict | None:
    """Reads the deltaExchange broker doc from kite_market_config. Returns
    None if not configured/enabled — callers should treat that as
    "credentials not set up yet", not raise."""
    doc = db["kite_market_config"].find_one({"broker": "deltaExchange", "enabled": True})
    if not doc:
        return None
    return {
        "api_key": doc.get("api_key", ""),
        "api_secret": doc.get("api_secret", ""),
        "user_id": doc.get("user_id", ""),
    }


def _signed_headers(method: str, path: str, query: str, payload: str, api_key: str, api_secret: str) -> dict:
    """Delta's documented signing scheme: HMAC-SHA256 over
    method + timestamp + path + query_string + body, hex digest, sent as the
    'signature' header alongside 'api-key' and 'timestamp'. Not used by any
    endpoint in this module yet (market data below is all public) — kept
    ready for the order-placement/account endpoints that will need it."""
    timestamp = str(int(time.time()))
    message = method + timestamp + path + query + payload
    signature = hmac.new(api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    return {
        "api-key": api_key,
        "signature": signature,
        "timestamp": timestamp,
        "Content-Type": "application/json",
    }


def _get(path: str, params: dict | None = None) -> dict:
    resp = requests.get(f"{DELTA_REST_BASE}{path}", params=params or {}, timeout=_HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# Delta's own position API (fetch_delta_open_positions) reports entry_price/mark_price/
# margin/liquidation_price all in USD (settling_asset/quoting_asset are USD even for the
# India entity) — Delta's own UI converts to INR purely for display, and that conversion is
# NOT a live market rate: back-computing it from Delta's own displayed Notional against the
# live spot price at the same instant gave 84.997 — a live third-party FX rate at that same
# moment was ~95.3 (two independent sources agreed). Delta pegs to a fixed ~85 internally
# rather than updating with the market, so matching their own displayed figures means
# matching that fixed peg, not fetching a "more correct" live rate that would only diverge
# further from what their UI actually shows.
#
# Stored in Mongo (crypto_usd_inr_rate_history) instead of hardcoded, so it (a) survives as
# an actual historical record if Delta ever re-pegs and someone updates it, and (b) can be
# corrected without a code deploy. Every write is an append (set_usd_inr_rate), never an
# update-in-place — get_usd_inr_rate always reads back whichever row is most recently
# effective, and every prior value stays queryable via get_usd_inr_rate_history.
USD_INR_RATE_DEFAULT = 85.0
_USD_INR_RATE_COLLECTION = "crypto_usd_inr_rate_history"
# Short TTL (not the hour+ a slow-moving FX rate would justify elsewhere in this codebase) —
# an admin correcting a wrong rate should take effect on every open crypto page within a
# few minutes, not up to an hour later.
_USD_INR_RATE_CACHE_TTL = 300.0
_usd_inr_rate_cache: dict[str, float] = {}


def get_usd_inr_rate(db) -> float:
    cached = _usd_inr_rate_cache
    if cached and (time.time() - cached.get("fetched_at", 0)) < _USD_INR_RATE_CACHE_TTL:
        return cached["rate"]

    doc = db[_USD_INR_RATE_COLLECTION].find_one(sort=[("effective_at", -1)])
    if doc:
        rate = float(doc.get("rate") or USD_INR_RATE_DEFAULT)
    else:
        # First-ever read with nothing stored yet — seed one real history row instead of
        # silently falling back forever with nothing for get_usd_inr_rate_history to show.
        rate = USD_INR_RATE_DEFAULT
        now = datetime.now(timezone.utc).isoformat()
        db[_USD_INR_RATE_COLLECTION].insert_one({
            "rate": rate,
            "effective_at": now,
            "created_at": now,
            "note": "seeded default — matches Delta's own fixed internal USD->INR peg",
        })

    _usd_inr_rate_cache["rate"] = rate
    _usd_inr_rate_cache["fetched_at"] = time.time()
    return rate


def set_usd_inr_rate(db, rate: float, note: str = "") -> dict:
    """Admin-only (see delta_exchange_router.py's endpoint) — appends a new historical row
    rather than overwriting the last one, and drops the in-process cache so the very next
    get_usd_inr_rate() call (on any process/request) picks it up within the TTL, not up to
    5 minutes stale."""
    now = datetime.now(timezone.utc).isoformat()
    doc = {"rate": float(rate), "effective_at": now, "created_at": now, "note": note}
    result = db[_USD_INR_RATE_COLLECTION].insert_one(doc)
    _usd_inr_rate_cache.clear()
    return {"_id": str(result.inserted_id), **doc}


def get_usd_inr_rate_history(db, limit: int = 50) -> list[dict]:
    docs = list(db[_USD_INR_RATE_COLLECTION].find(sort=[("effective_at", -1)], limit=limit))
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs


def verify_delta_credentials(db) -> dict:
    """Delta has no TOTP/session-login flow to replay daily like Dhan's — its API key +
    secret are permanent, every request signed fresh per-call (see _signed_headers). So
    "auto-login" here means "confirm the stored credentials actually authenticate", not
    refresh an expiring token: a real signed GET against an account-scoped endpoint
    (wallet balances), first use of _signed_headers in this module. On success, stamps
    kite_market_config (broker=deltaExchange) with a login_time/expiry_time pair purely
    for the Monitors page's display parity with the Dhan section — Delta itself doesn't
    expire anything."""
    creds = get_delta_credentials(db)
    if not creds or not creds.get("api_key") or not creds.get("api_secret"):
        message = "kite_market_config (broker=deltaExchange) is missing api_key/api_secret, or not enabled"
        return {"ok": False, "message": message}

    path = "/v2/wallet/balances"
    headers = _signed_headers("GET", path, "", "", creds["api_key"], creds["api_secret"])
    try:
        resp = requests.get(f"{DELTA_REST_BASE}{path}", headers=headers, timeout=_HTTP_TIMEOUT)
        data = resp.json()
    except Exception as exc:
        message = f"Delta credential check request failed: {exc}"
        log.warning("[delta verify] %s", message)
        return {"ok": False, "message": message}

    if not data.get("success"):
        error = (data.get("error") or {}).get("code") or data.get("error") or "unknown error"
        message = f"Delta rejected the stored credentials: {error}"
        log.warning("[delta verify] %s", message)
        return {"ok": False, "message": message}

    login_time = time.strftime("%Y-%m-%dT%H:%M:%S")
    db["kite_market_config"].update_one(
        {"broker": "deltaExchange"},
        {"$set": {"login_time": login_time, "verified": True}},
    )
    balances = data.get("result") or []
    message = f"Delta API credentials verified ({len(balances)} wallet balance(s) readable)."
    log.info("[delta verify] %s", message)
    return {"ok": True, "message": message, "login_time": login_time}


def list_products(underlying: str) -> list[dict]:
    """Live call/put option products for the given underlying (e.g. 'BTC')."""
    data = _get("/v2/products", {
        "contract_types": "call_options,put_options",
        "underlying_asset_symbols": underlying,
        "states": "live",
    })
    return data.get("result", []) if data.get("success") else []


def list_expiries(underlying: str) -> list[str]:
    """Sorted expiry dates (DD-MM-YYYY, Delta's own query-param format) derived
    from the live product list's symbol suffix (DDMMYY)."""
    products = list_products(underlying)
    seen: dict[str, str] = {}
    for p in products:
        symbol = p.get("symbol", "")
        suffix = symbol.rsplit("-", 1)[-1]  # DDMMYY
        if len(suffix) != 6 or not suffix.isdigit():
            continue
        dd, mm, yy = suffix[0:2], suffix[2:4], suffix[4:6]
        seen[suffix] = f"{dd}-{mm}-20{yy}"
    return [seen[k] for k in sorted(seen.keys())]


def _leg_row(ticker: dict, contract_value: float) -> dict:
    quotes = ticker.get("quotes") or {}
    greeks = ticker.get("greeks") or {}
    return {
        "symbol": ticker.get("symbol", ""),
        "product_id": ticker.get("product_id"),
        "strike": float(ticker.get("strike_price") or 0),
        # ltp/bid/ask: real per-contract USD (see DELTA_CONTRACT_VALUE's comment) — what the
        # payoff/margin/P&L math actually needs, verified against Delta's own shown figures.
        "ltp": float(ticker.get("mark_price") or 0) * contract_value,
        "bid": (float(quotes.get("best_bid") or 0) if quotes.get("best_bid") is not None else 0.0) * contract_value,
        "ask": (float(quotes.get("best_ask") or 0) if quotes.get("best_ask") is not None else 0.0) * contract_value,
        # mark_raw: the UNSCALED quote (per 1 unit of underlying), exactly what Delta's own
        # order book and every third-party tool (AlgoTest included) shows in an option chain's
        # price column — a genuinely different, both-correct number from ltp above. Display-
        # only: the chain table's price columns read this; nothing else should.
        "mark_raw": float(ticker.get("mark_price") or 0),
        "iv": float(quotes.get("mark_iv") or 0) if quotes.get("mark_iv") is not None else 0.0,
        "delta": float(greeks.get("delta") or 0) if greeks.get("delta") is not None else 0.0,
        "gamma": float(greeks.get("gamma") or 0) if greeks.get("gamma") is not None else 0.0,
        "theta": float(greeks.get("theta") or 0) if greeks.get("theta") is not None else 0.0,
        "vega": float(greeks.get("vega") or 0) if greeks.get("vega") is not None else 0.0,
        "oi": float(ticker.get("oi") or 0),
        "volume": float(ticker.get("volume") or 0),
    }


def build_chain_from_tickers(underlying: str, expiry: str, tickers: list[dict]) -> dict:
    """Shared shaping logic used by both the REST fetch below and the
    websocket cache (delta_exchange_ws.py) — so a chain built from a live WS
    snapshot and one built from a cold REST call look identical to callers."""
    contract_value = DELTA_CONTRACT_VALUE.get(underlying, 1.0)
    ce_rows, pe_rows = [], []
    spot_price = 0.0
    for t in tickers:
        if t.get("underlying_asset_symbol") != underlying:
            continue
        if t.get("contract_type") == "call_options":
            ce_rows.append(_leg_row(t, contract_value))
        elif t.get("contract_type") == "put_options":
            pe_rows.append(_leg_row(t, contract_value))
        greeks = t.get("greeks") or {}
        if greeks.get("spot"):
            spot_price = float(greeks["spot"])
        elif t.get("spot_price"):
            spot_price = float(t["spot_price"])

    ce_rows.sort(key=lambda r: r["strike"])
    pe_rows.sort(key=lambda r: r["strike"])

    strikes = sorted({r["strike"] for r in ce_rows + pe_rows})
    atm_strike = min(strikes, key=lambda s: abs(s - spot_price)) if strikes and spot_price else 0.0
    strike_interval = min(
        (b - a for a, b in zip(strikes, strikes[1:])), default=0.0
    ) if len(strikes) > 1 else 0.0

    return {
        "underlying": underlying,
        "expiry": expiry,
        "spot_price": spot_price,
        "atm_strike": atm_strike,
        "strike_interval": strike_interval,
        "chain": {"CE": ce_rows, "PE": pe_rows},
    }


def fetch_perpetual_ticker(underlying: str) -> dict:
    """The single perpetual future Delta lists per underlying — BTCUSD/ETHUSD, no
    expiry/strike (unlike NSE's monthly F&O futures list, which this deliberately
    doesn't try to imitate — Delta genuinely only has the one contract)."""
    data = _get(f"/v2/tickers/{underlying}USD")
    result = data.get("result") if data.get("success") else None
    if not result:
        return {"symbol": f"{underlying}USD", "ltp": 0.0}
    return {
        "symbol": result.get("symbol", f"{underlying}USD"),
        "ltp": float(result.get("mark_price") or 0),
        "open": float(result.get("open") or 0),
    }


# Our UI's resolution codes (Chart.tsx's SUPPORTED_RESOLUTIONS: plain-minute strings
# plus "1D") mapped to Delta's /v2/history/candles resolution codes. Delta has no
# 8h(480)/12h(720) code, so those two fall back to the nearest smaller supported
# resolution — the frontend's own getBarsForResolution-style aggregation (already
# used for 3D/1W/1M off 1D) can re-bucket further client-side if a wider bucket is
# ever needed; this endpoint just returns the finest Delta actually has for them.
DELTA_CANDLE_RESOLUTIONS: dict[str, str] = {
    "1": "1m", "3": "3m", "5": "5m", "15": "15m", "30": "30m",
    "60": "1h", "120": "2h", "240": "4h", "360": "6h",
    "480": "4h", "720": "6h",
    "1D": "1d",
}

# Delta's spot/index price symbols — NOT listed in /v2/products (that only lists
# tradable contracts: perpetuals/options/etc, "spot_index" isn't even a valid
# contract_types filter value there), only directly queryable by symbol on
# /v2/history/candles and on Delta's own chart CDN (cdn.india.deltaex.org/v2/
# chart/symbols?symbol=.DEXBTUSD). No derivable naming pattern between
# underlyings (BTC drops the "H", ETH keeps it but drops the "X") — confirmed
# individually, hardcoded rather than guessed. This is the price CryptoTradeNew.
# tsx's own option-chain "spot_price" field (and Delta's own site) shows as
# "the BTC/ETH price" — genuinely different from the perpetual's mark_price
# (small but real funding-basis gap) which fetch_perpetual_ticker/DELTA_
# CANDLE_RESOLUTIONS' old `{underlying}USD` symbol used to source candles from.
DELTA_SPOT_INDEX_SYMBOL: dict[str, str] = {"BTC": ".DEXBTUSD", "ETH": ".DEETHUSD"}


def fetch_candles(underlying: str, resolution: str, start: int, end: int) -> list[dict]:
    """GET /v2/history/candles for the underlying's spot/index price (DELTA_
    SPOT_INDEX_SYMBOL) — public endpoint, no signing. `start`/`end` are unix
    seconds (Delta's own convention). Returns bars as {"time" (ms), "open",
    "high", "low", "close", "volume"}, ms so this matches the OhlcBar
    convention fetchBars()/Chart.tsx already expects everywhere else."""
    delta_resolution = DELTA_CANDLE_RESOLUTIONS.get(str(resolution))
    if not delta_resolution:
        raise ValueError(f"Unsupported resolution for Delta candles: {resolution}")
    spot_symbol = DELTA_SPOT_INDEX_SYMBOL.get(underlying)
    if not spot_symbol:
        raise ValueError(f"No spot-index symbol known for underlying: {underlying}")
    data = _get("/v2/history/candles", {
        "symbol": spot_symbol,
        "resolution": delta_resolution,
        "start": start,
        "end": end,
    })
    if not data.get("success"):
        return []
    rows = data.get("result", [])
    bars = [
        {
            "time": int(row["time"]) * 1000,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row.get("volume") or 0),
        }
        for row in rows
        if row.get("time") is not None
    ]
    bars.sort(key=lambda b: b["time"])
    return bars


def list_move_options(underlying: str) -> list[dict]:
    """Delta's own native 'Move' product — NOT synthesized from a call+put combo,
    a single listed instrument per (underlying, strike, settlement). Mathematically
    it settles at |spot - strike|, i.e. exactly a straddle's combined payoff, which
    is why the frontend can price it with the same bsCall+bsPut machinery used for
    regular options (see CryptoTradeNew.tsx's "MV" optType handling)."""
    data = _get("/v2/products", {
        "contract_types": "move_options",
        "underlying_asset_symbols": underlying,
        "states": "live",
    })
    return data.get("result", []) if data.get("success") else []


def fetch_move_options_rest(underlying: str) -> list[dict]:
    contract_value = DELTA_CONTRACT_VALUE.get(underlying, 1.0)
    products = list_move_options(underlying)
    if not products:
        return []
    data = _get("/v2/tickers", {"contract_types": "move_options", "underlying_asset_symbols": underlying})
    tickers_by_symbol = {t["symbol"]: t for t in (data.get("result", []) if data.get("success") else [])}
    rows = []
    for p in products:
        symbol = p["symbol"]
        t = tickers_by_symbol.get(symbol, {})
        greeks = t.get("greeks") or {}
        quotes = t.get("quotes") or {}
        rows.append({
            "symbol": symbol,
            "strike": float(p.get("strike_price") or 0),
            "settlement_time": p.get("settlement_time", ""),
            "ltp": float(t.get("mark_price") or 0) * contract_value,  # same per-1-underlying-unit quoting as options
            "iv": float(quotes.get("mark_iv") or 0),
            "delta": float(greeks.get("delta") or 0),
            "gamma": float(greeks.get("gamma") or 0),
            "theta": float(greeks.get("theta") or 0),
            "vega": float(greeks.get("vega") or 0),
            "oi": float(t.get("oi") or 0),
        })
    rows.sort(key=lambda r: (r["settlement_time"], r["strike"]))
    return rows


def _parse_option_symbol(symbol: str) -> dict:
    """Splits a Delta option product_symbol ('C-BTC-45000-300824') into its parts.
    Prefix is 'C'/'P' (call/put), middle is the underlying, then strike, then a
    DDMMYY expiry suffix — same suffix convention list_expiries already parses.
    Returns {} for anything that doesn't match (e.g. a perpetual future's plain
    'BTCUSD' symbol), so callers can skip non-option legs instead of crashing."""
    parts = symbol.split("-")
    if len(parts) != 4 or parts[0] not in ("C", "P"):
        return {}
    _, underlying, strike, suffix = parts
    if len(suffix) != 6 or not suffix.isdigit():
        return {}
    dd, mm, yy = suffix[0:2], suffix[2:4], suffix[4:6]
    try:
        strike_val = float(strike)
    except ValueError:
        return {}
    return {
        "option_type": "call" if parts[0] == "C" else "put",
        "underlying": underlying,
        "strike": strike_val,
        "expiry": f"20{yy}-{mm}-{dd}",
    }


def _fetch_delta_fills_since(api_key: str, api_secret: str, start_time_us: int) -> list[dict]:
    """All fills from start_time_us (epoch microseconds) to now, paginated via the
    'after' cursor /v2/fills returns in its `meta` block. 100/page keeps this to one
    call for a normal day's fill count; a genuinely high-frequency day pages through."""
    fills: list[dict] = []
    after: str | None = None
    for _ in range(20):  # hard cap — never loop forever against a runaway cursor
        params: dict[str, Any] = {"start_time": start_time_us, "page_size": 100}
        if after:
            params["after"] = after
        query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        headers = _signed_headers("GET", "/v2/fills", query, "", api_key, api_secret)
        resp = requests.get(f"{DELTA_REST_BASE}/v2/fills{query}", headers=headers, timeout=_HTTP_TIMEOUT)
        data = resp.json()
        if not data.get("success"):
            break
        page = data.get("result") or []
        fills.extend(page)
        after = (data.get("meta") or {}).get("after")
        if not after or not page:
            break
    return fills


def fetch_delta_closed_positions_today(db) -> list[dict]:
    """Reconstructs today's (IST calendar day) fully-closed option legs from Delta's own
    fill history — /v2/positions/margined (fetch_delta_open_positions) only ever lists
    currently-OPEN positions, so a leg that was opened AND fully closed today has already
    dropped out of that endpoint entirely by the time anyone asks; this is the only way to
    still show it as an "exited today" row, the same way NSE's Positions.tsx shows a
    same-day-closed leg (Dhan reports both open and closed in one call; Delta doesn't).

    Approach: walk each product_symbol's fills in chronological order. Whenever a fill's
    own meta_data.new_position.size lands back on exactly 0, that fill closed the episode —
    Delta already computed the realized_pnl for it right there, no need to re-derive P&L
    from entry/exit ourselves. entry_price is the price of whichever fill most recently
    took that symbol from flat to non-flat; for a position built from a single fill (the
    overwhelmingly common case) this is exact — a position built from several partial
    fills on the way in gets its first fill's price, not a true weighted average, which is
    a known simplification here, not a currently-solved case.
    """
    creds = get_delta_credentials(db)
    if not creds or not creds.get("api_key") or not creds.get("api_secret"):
        return []

    today_start_ist = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
    start_time_us = int(today_start_ist.astimezone(timezone.utc).timestamp() * 1_000_000)
    try:
        fills = _fetch_delta_fills_since(creds["api_key"], creds["api_secret"], start_time_us)
    except Exception as exc:
        log.warning("[delta closed positions] fills fetch failed: %s", exc)
        return []

    # Oldest first — the API returns newest first, and this reconstruction depends on
    # processing each symbol's fills in the order they actually happened.
    fills.sort(key=lambda f: str(f.get("created_at") or ""))

    open_episode_price: dict[str, float] = {}
    closed_legs: list[dict] = []
    for fill in fills:
        symbol = str(fill.get("product_symbol") or "")
        parsed = _parse_option_symbol(symbol)
        if not parsed:
            continue
        new_position = (fill.get("meta_data") or {}).get("new_position") or {}
        fill_price = float(fill.get("price") or 0)
        fill_side = str(fill.get("side") or "").strip().lower()
        is_closing = new_position.get("size") == 0

        if not is_closing:
            # Building/adding to the position, not closing it — record the price only for
            # a genuinely fresh episode (first fill seen for this symbol since it was last
            # flat), so a later closing fill can tell "I have this episode's real entry" apart
            # from "this symbol's episode started before today's fetch window even began".
            if symbol not in open_episode_price:
                open_episode_price[symbol] = fill_price
            continue

        # position_side is the ORIGINAL position's direction, not this closing fill's own
        # side — closing a short means buying it back, so a "buy" fill here means the
        # leg itself was short (sell), and vice versa. Using the fill's own side directly
        # (as an earlier version of this did) showed every closed short leg with a "Buy"
        # badge, backwards from what was actually held.
        position_side = "sell" if fill_side == "buy" else "buy"
        quantity = abs(float(fill.get("size") or 0))
        realized_pnl = new_position.get("realized_pnl")

        if symbol in open_episode_price:
            entry_price = open_episode_price.pop(symbol)
        elif realized_pnl is not None and quantity > 0:
            # This symbol's opening fill happened before today's fetch window (position
            # carried over from an earlier day) — there's no real entry price available
            # here to reconstruct. Back-solve one instead: the frontend's own calcLegPnl
            # always recomputes P&L from (entry, exit) itself (no "trust this pnl" escape
            # hatch), so this picks whichever entry_price makes that recomputation land
            # exactly on Delta's own realized_pnl — not the true historical entry, but the
            # displayed P&L is correct either way, which is what actually matters here.
            # realized_pnl is already real $ (see fetch_delta_open_positions' margin/
            # cashflow comment) while fill_price/entry_price here are raw per-1-BTC quotes
            # (contract_value scaling happens later, in the router's _flatten_delta_
            # strategies) — dividing by contract_value first converts the $ delta back to
            # this same raw scale before adding it to fill_price, otherwise it's off by
            # 1/contract_value (1000x for BTC), which is exactly what happened here first.
            contract_value = DELTA_CONTRACT_VALUE.get(parsed["underlying"], 1.0)
            realized_raw = float(realized_pnl) / contract_value
            entry_price = fill_price + realized_raw / quantity if position_side == "sell" else fill_price - realized_raw / quantity
        else:
            entry_price = fill_price

        closed_legs.append({
            "_id": f"{symbol}-closed-{fill.get('id')}",
            "type": position_side,
            "option_type": parsed["option_type"],
            "strike": parsed["strike"],
            "expiry": parsed["expiry"],
            "token": symbol,
            "entry_price": entry_price,
            "current_ltp": fill_price,
            "lot_size": 1,
            "quantity": quantity,
            "exited": True,
            "pnl": float(realized_pnl) if realized_pnl is not None else 0.0,
            "margin": 0.0,
            "cashflow": float(new_position.get("realized_cashflow") or 0),
        })
    return closed_legs


def fetch_delta_position_tp_sl(creds: dict) -> dict[str, dict]:
    """Delta's own native per-position TP/SL (the "TP/SL: +Add" button on their own
    Positions table) isn't a field on the position object at all — it's a pair of
    reduce-only "bracket" stop orders sitting in the pending order book, tagged
    meta_data.order_source == "positions_TP_SL_order" (confirmed live: setting one on
    Delta's own UI creates exactly this). One /v2/orders?states=open,pending call gets
    every pending order account-wide (cheap — no per-product_id fan-out needed), filtered
    down here to just the TP/SL-bracket ones and grouped by product_symbol.

    This is a genuinely different thing from crypto_simulator_triggers (our own app's
    "Add Alert" — percent/points relative to entry, evaluated by simulator_risk_monitor)
    — Delta's is an absolute trigger price already sitting as a real order on their own
    book, independent of whether our app is even running.
    """
    headers = _signed_headers("GET", "/v2/orders", "?states=open,pending&page_size=100", "", creds["api_key"], creds["api_secret"])
    try:
        resp = requests.get(f"{DELTA_REST_BASE}/v2/orders?states=open,pending&page_size=100", headers=headers, timeout=_HTTP_TIMEOUT)
        data = resp.json()
    except Exception as exc:
        log.warning("[delta tp/sl] orders fetch failed: %s", exc)
        return {}
    if not data.get("success"):
        return {}

    by_symbol: dict[str, dict] = {}
    for order in data.get("result") or []:
        meta = order.get("meta_data") or {}
        if meta.get("order_source") != "positions_TP_SL_order":
            continue
        symbol = str(order.get("product_symbol") or "")
        stop_order_type = str(order.get("stop_order_type") or "")
        stop_price = order.get("stop_price")
        if not symbol or stop_price is None:
            continue
        # stop_price is the trigger — once the mark crosses it, Delta fires the order at
        # limit_price (the actual price it'll try to fill at), a genuinely different number
        # from the trigger itself. Delta's own Positions page shows both, so this does too.
        limit_price = order.get("limit_price")
        entry = by_symbol.setdefault(symbol, {})
        if stop_order_type == "take_profit_order":
            entry["tp_price"] = float(stop_price)
            entry["tp_limit_price"] = float(limit_price) if limit_price is not None else None
        elif stop_order_type == "stop_loss_order":
            entry["sl_price"] = float(stop_price)
            entry["sl_limit_price"] = float(limit_price) if limit_price is not None else None
    return by_symbol


def fetch_delta_open_positions(db) -> dict:
    """Fetches every open BTC/ETH option position on the account and reshapes it into
    the StrategyItem/StrategyPosition JSON shape components/simulator/StrategyCard.tsx
    already knows how to render (same shape PortfolioNew/CryptoPortfolioNew's own
    strategies list uses), grouped one StrategyItem per underlying so all of an
    underlying's open legs land on one card — same grouping PositionsPage does for Dhan.

    Endpoint: GET /v2/positions/margined with no query params — confirmed live: lists
    every open margined (derivatives) position on the account in one call, not just one
    product. Each position's own entry_price/mark_price/margin/liquidation_price come
    back in USD (settling_asset/quoting_asset are USD even for the India entity) — Delta's
    own UI converts these to INR purely for display, which is why usd_inr_rate rides
    alongside `strategies` in the return value below.

    entry_price/current_ltp are read straight off each position (not re-derived from a
    separate ticker fetch) — same raw per-1-underlying-unit convention as everywhere else
    in this module (see DELTA_CONTRACT_VALUE's comment); StrategyCard.tsx's
    getContractValue scaling depends on that being true.
    """
    creds = get_delta_credentials(db)
    if not creds or not creds.get("api_key") or not creds.get("api_secret"):
        return {"ok": False, "detail": "Delta Exchange isn't configured (kite_market_config: broker=deltaExchange missing/disabled).", "strategies": []}

    path = "/v2/positions/margined"
    headers = _signed_headers("GET", path, "", "", creds["api_key"], creds["api_secret"])
    try:
        resp = requests.get(f"{DELTA_REST_BASE}{path}", headers=headers, timeout=_HTTP_TIMEOUT)
        data = resp.json()
    except Exception as exc:
        message = f"Delta positions request failed: {exc}"
        log.warning("[delta positions] %s", message)
        return {"ok": False, "detail": message, "strategies": []}

    if not data.get("success"):
        error = (data.get("error") or {}).get("code") or data.get("error") or "unknown error"
        message = f"Delta rejected the positions request: {error}"
        log.warning("[delta positions] %s", message)
        return {"ok": False, "detail": message, "strategies": []}

    raw_positions = [p for p in (data.get("result") or []) if float(p.get("size") or 0) != 0]

    legs_by_underlying: dict[str, list[dict]] = {}
    for position in raw_positions:
        symbol = str(position.get("product_symbol") or "")
        parsed = _parse_option_symbol(symbol)
        if not parsed:
            continue  # not an option leg (e.g. a perpetual future) — skip for now
        size = float(position.get("size") or 0)
        leg = {
            "_id": symbol,
            "type": "buy" if size > 0 else "sell",
            "option_type": parsed["option_type"],
            "strike": parsed["strike"],
            "expiry": parsed["expiry"],
            "token": symbol,
            "entry_price": float(position.get("entry_price") or 0),
            # Delta's own position object already carries its own mark_price — this is
            # the exact figure their own UI/margin/liquidation-price math is computed
            # from, so reading it straight off here (instead of a second /v2/tickers
            # call keyed by symbol) is both simpler AND guaranteed consistent with
            # whatever Delta itself is showing for this same position, not a
            # possibly-microseconds-apart independent snapshot from a separate call.
            "current_ltp": float(position.get("mark_price") or 0) or None,
            "lot_size": 1,
            "quantity": abs(size),
            "exited": False,
            # Both already real per-position USD amounts (not raw-per-1-BTC quotes, so no
            # contract_value scaling needed) — margin*rate and realized_cashflow*rate matched
            # Delta's own India-facing UI's Margin/Cashflows columns to the cent when checked
            # against a live account. Delta's UI "Cashflows" column is realized_cashflow only
            # (not combined with unrealized_cashflow, which is really just -margin's live half).
            "margin": float(position.get("margin") or 0),
            "cashflow": float(position.get("realized_cashflow") or 0),
        }
        legs_by_underlying.setdefault(parsed["underlying"], []).append(leg)

    # Delta's own native per-position TP/SL (set directly on their platform, not via our
    # "Add Alert") — one call for every open leg at once, see fetch_delta_position_tp_sl's
    # docstring for why this is a separate concept from crypto_simulator_triggers.
    if legs_by_underlying:
        tp_sl_by_symbol = fetch_delta_position_tp_sl(creds)
        for legs in legs_by_underlying.values():
            for leg in legs:
                tp_sl = tp_sl_by_symbol.get(leg["token"])
                if tp_sl:
                    leg["broker_sl_price"] = tp_sl.get("sl_price")
                    leg["broker_tp_price"] = tp_sl.get("tp_price")
                    leg["broker_sl_limit_price"] = tp_sl.get("sl_limit_price")
                    leg["broker_tp_limit_price"] = tp_sl.get("tp_limit_price")

    # Same-day closed legs — /v2/positions/margined above only ever lists what's still
    # open, so a leg opened AND fully closed today needs this separate fill-history
    # reconstruction to show up at all (see fetch_delta_closed_positions_today's docstring).
    for closed_leg in fetch_delta_closed_positions_today(db):
        closed_parsed = _parse_option_symbol(str(closed_leg.get("token") or ""))
        if not closed_parsed:
            continue
        legs_by_underlying.setdefault(closed_parsed["underlying"], []).append(closed_leg)

    if not legs_by_underlying:
        return {"ok": True, "detail": "", "strategies": [], "usd_inr_rate": get_usd_inr_rate(db)}

    # Still one REST tickers call per underlying (not per-leg) — but only for spot_price
    # now (the position object has no spot field of its own), same batching style
    # _get_dhan_token_maps uses to avoid an N-call fan-out.
    strategies: list[dict] = []
    for underlying, legs in legs_by_underlying.items():
        spot_price = 0.0
        try:
            ticker_data = _get("/v2/tickers", {
                "contract_types": "call_options,put_options",
                "underlying_asset_symbols": underlying,
            })
            for t in (ticker_data.get("result", []) if ticker_data.get("success") else []):
                greeks = t.get("greeks") or {}
                if greeks.get("spot"):
                    spot_price = float(greeks["spot"])
                    break
        except Exception as exc:
            log.warning("[delta positions] spot ticker fetch failed for %s: %s", underlying, exc)

        strategies.append({
            "_id": f"delta-live-{underlying}",
            "strategy_name": f"{underlying} Live Position",
            "instrument": underlying,
            "spot_price": spot_price or None,
            "positions": legs,
            "execution_mode": "regular",
        })

    return {"ok": True, "detail": "", "strategies": strategies, "usd_inr_rate": get_usd_inr_rate(db)}


def fetch_option_chain_rest(underlying: str, expiry: str = "") -> dict:
    """Cold REST fetch — resolves the nearest expiry if none given, then
    pulls the full call+put ticker set for it in one call."""
    expiries = list_expiries(underlying)
    if not expiries:
        return {
            "underlying": underlying, "expiry": "", "expiries": [],
            "spot_price": 0.0, "atm_strike": 0.0, "strike_interval": 0.0,
            "chain": {"CE": [], "PE": []}, "source": "rest",
        }
    resolved_expiry = expiry or expiries[0]

    data = _get("/v2/tickers", {
        "contract_types": "call_options,put_options",
        "underlying_asset_symbols": underlying,
        "expiry_date": resolved_expiry,
    })
    tickers = data.get("result", []) if data.get("success") else []
    payload = build_chain_from_tickers(underlying, resolved_expiry, tickers)
    payload["expiries"] = expiries
    payload["source"] = "rest"
    return payload
