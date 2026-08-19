"""
delta_exchange_router.py
──────────────────────────
REST surface for the Crypto (Delta Exchange) page in algo-admin — separate
router, separate prefix, mounted standalone in api.py. Does not touch or
extend the existing /simulator/zerodha/* or /live-greeks-chain routes.

GET /simulator/crypto/option-chain?underlying=BTC[&expiry=DD-MM-YYYY]
  Tries the live websocket cache first (delta_exchange_ws.py) — genuinely
  push-updated, sub-second-fresh data once warm. Falls back to a cold REST
  call (delta_exchange_client.py) on the very first request for an
  (underlying, expiry) pair, or if the websocket connection is down/stale.
  Every request also calls ensure_subscribed(), so the very first request
  both answers immediately (via REST) AND arms the websocket for every
  request after it.

GET /simulator/crypto/rest-option-chain/{instrument}[?expiry=DD-MM-YYYY]
  Same data, reshaped to exactly match algo.scanner's /rest-option-chain/
  {instrument} response shape (StrategyPayload in the frontend: "instrument"
  not "underlying", plus india_vix/lot_size filled with crypto-sensible
  defaults) — this is the one CryptoTradeNew.tsx (the paper-trade-new clone)
  actually calls, via useLiveChainSocket's restApiBase/restPath params.
  Instrument is a path segment here (not a query param) to match that hook's
  URL construction (`${restApiBase}/${restPath}/${instrument}`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from features import auth as app_auth
from features.order_brokers.brokers.delta_exchange import _is_delta_doc
from simulator.delta_exchange_client import (
    DELTA_CONTRACT_VALUE,
    fetch_candles,
    fetch_delta_open_positions,
    fetch_move_options_rest,
    fetch_option_chain_rest,
    fetch_perpetual_ticker,
    get_delta_credentials,
    get_usd_inr_rate,
    get_usd_inr_rate_history,
    set_usd_inr_rate,
    verify_delta_credentials,
)
from simulator.delta_exchange_ws import delta_ticker_manager

log = logging.getLogger(__name__)

delta_exchange_router = APIRouter(prefix="/simulator/crypto")

SUPPORTED_UNDERLYINGS = ("BTC", "ETH")


@delta_exchange_router.get("/option-chain")
async def get_delta_option_chain(
    underlying: str = Query(default="BTC"),
    expiry: str = Query(default=""),
) -> dict:
    underlying = underlying.strip().upper()
    if underlying not in SUPPORTED_UNDERLYINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported underlying '{underlying}'. Use one of {SUPPORTED_UNDERLYINGS}.")

    resolved_expiry = delta_ticker_manager.ensure_subscribed(underlying, expiry)
    if not resolved_expiry:
        raise HTTPException(status_code=502, detail=f"No live option expiries found for {underlying} on Delta Exchange.")

    snapshot = delta_ticker_manager.get_chain_snapshot(underlying, resolved_expiry)
    if snapshot is not None:
        snapshot["expiries"] = delta_ticker_manager.get_expiries(underlying)
        return snapshot

    try:
        return fetch_option_chain_rest(underlying, resolved_expiry)
    except Exception as exc:
        log.warning("[delta_exchange_router] REST fallback failed for %s: %s", underlying, exc)
        raise HTTPException(status_code=502, detail=f"Delta Exchange option chain fetch failed: {exc}")


def _leg_to_strategy_leg(leg: dict) -> dict:
    return {**leg, "token": leg.get("symbol", "")}


def _ddmmyyyy_to_iso(value: str) -> str:
    """Delta's own query-param format (DD-MM-YYYY) -> ISO (YYYY-MM-DD). Required, not
    cosmetic: CryptoTradeNew.tsx's getLegExpiryMs does `new Date(leg.expiry)`, and JS
    silently returns Invalid Date for a non-ISO "DD-MM-YYYY" string whenever DD > 12
    (confirmed: new Date("21-08-2026") -> Invalid Date, while new Date("07-08-2026")
    happens to "work" by being misread as MM-DD) — every expiry beyond the first
    couple of weekly ones would have quietly NaN'd out the whole payoff/margin engine
    downstream. ISO is the one format `new Date()` is spec-guaranteed to parse."""
    parts = value.split("-")
    if len(parts) != 3:
        return value
    dd, mm, yyyy = parts
    return f"{yyyy}-{mm}-{dd}"


def _iso_to_ddmmyyyy(value: str) -> str:
    """Inverse of the above — the frontend sends back whatever `expiry` it was given
    (now ISO) as the ?expiry= query param when the user picks a different expiry;
    Delta's own API (list_expiries/fetch_option_chain_rest) only understands its
    native DD-MM-YYYY, so this converts right back before any Delta call."""
    parts = value.split("-")
    if len(parts) != 3:
        return value
    yyyy, mm, dd = parts
    return f"{dd}-{mm}-{yyyy}"


# underlying -> (utc_day it was resolved for, previous close). Delta's /rest-
# option-chain/{instrument} is polled on every chain refresh (sub-second cadence
# once a picker/chart is open), but a closed daily candle never changes once its
# UTC day is over — no reason to hit Delta's candle API more than once per
# underlying per day. Mirrors execution_socket.py's _STABLE_PREV_CLOSE for NSE.
_CRYPTO_PREV_CLOSE_CACHE: dict[str, tuple[str, float]] = {}


def _resolve_crypto_previous_close(underlying: str) -> float:
    """Previous UTC calendar day's close for a crypto underlying.

    Crypto trades 24/7 — there's no NSE-style 15:30 closing-auction print to
    anchor "previous close" on, so this can't reuse _previous_session_close's
    approach of scanning our own recorded ticks for one. Delta's own public
    daily candle (fetch_candles, /v2/history/candles on the spot-index symbol)
    IS the authoritative close for a UTC day, and needs no auth — used the same
    on-demand way _dhan_daily_close uses Dhan's historical-candle API as the
    ground truth for NSE's previous close, rather than depending on any of our
    own collections having a tick recorded at exactly the right moment.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cached = _CRYPTO_PREV_CLOSE_CACHE.get(underlying)
    if cached is not None and cached[0] == today and cached[1] > 0:
        return cached[1]
    try:
        now = datetime.now(timezone.utc)
        bars = fetch_candles(underlying, "1D", int((now - timedelta(days=4)).timestamp()), int(now.timestamp()))
    except Exception as exc:
        log.warning("[delta_exchange_router] previous-close candle fetch failed for %s: %s", underlying, exc)
        return cached[1] if cached else 0.0
    # bars are ascending by time and can include today's still-forming candle —
    # walk back from the end and take the first bar dated strictly before today.
    prev_close = 0.0
    for bar in reversed(bars):
        bar_day = datetime.fromtimestamp(bar["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        if bar_day < today:
            prev_close = float(bar["close"])
            break
    if prev_close > 0:
        _CRYPTO_PREV_CLOSE_CACHE[underlying] = (today, prev_close)
    return prev_close


def _to_strategy_payload(underlying: str, snapshot: dict, expiries: list[str]) -> dict:
    """Reshapes our internal chain dict (see delta_exchange_client.build_chain_from_tickers)
    into the exact StrategyPayload shape CryptoTradeNew.tsx/useLiveChainSocket expect —
    "instrument" not "underlying", plus the couple of fields that type requires but our
    internal shape doesn't carry (india_vix, lot_size). Delta options are already quoted
    per-contract (no lot multiplier), so lot_size is fixed at 1; india_vix has no crypto
    equivalent — 0 rather than fabricating a number. expiry/expiries go out as ISO — see
    _ddmmyyyy_to_iso.

    previous_close/change_pct/change_points added here (previously missing entirely from
    this payload) — CryptoTradeNew.tsx's mainSpotChange/overlaySpotChange read change_pct
    straight off this chain (mainChain?.change_pct / overlayChain?.change_pct), which was
    always undefined without this, so the instrument bar's %change always showed 0.00%
    (or silently mismatched a stale/unrelated value from a different chain source)."""
    spot_price = snapshot["spot_price"]
    previous_close = _resolve_crypto_previous_close(underlying)
    change_pct = round((spot_price - previous_close) / previous_close * 100, 2) if previous_close > 0 else 0.0
    change_points = round(spot_price - previous_close, 2) if previous_close > 0 else 0.0
    return {
        "instrument": underlying,
        "expiry": _ddmmyyyy_to_iso(snapshot["expiry"]),
        "expiries": [_ddmmyyyy_to_iso(e) for e in expiries],
        "spot_price": spot_price,
        "previous_close": round(previous_close, 2),
        "change_pct": change_pct,
        "change_points": change_points,
        "atm_strike": snapshot["atm_strike"],
        "strike_interval": snapshot["strike_interval"],
        "india_vix": 0.0,
        "lot_size": 1,
        "chain": {
            "CE": [_leg_to_strategy_leg(leg) for leg in snapshot["chain"]["CE"]],
            "PE": [_leg_to_strategy_leg(leg) for leg in snapshot["chain"]["PE"]],
        },
        "type": "chain",
    }


@delta_exchange_router.get("/rest-option-chain/{instrument}")
async def get_delta_rest_option_chain(instrument: str, expiry: str = Query(default="")) -> dict:
    underlying = instrument.strip().upper()
    if underlying not in SUPPORTED_UNDERLYINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported underlying '{underlying}'. Use one of {SUPPORTED_UNDERLYINGS}.")

    # The frontend only ever holds the ISO expiry this endpoint itself handed out
    # (see _to_strategy_payload) — convert back to Delta's DD-MM-YYYY before any
    # Delta-facing call below.
    delta_expiry = _iso_to_ddmmyyyy(expiry) if expiry else ""
    resolved_expiry = delta_ticker_manager.ensure_subscribed(underlying, delta_expiry)
    if not resolved_expiry:
        raise HTTPException(status_code=502, detail=f"No live option expiries found for {underlying} on Delta Exchange.")

    snapshot = delta_ticker_manager.get_chain_snapshot(underlying, resolved_expiry)
    expiries = delta_ticker_manager.get_expiries(underlying)
    if snapshot is None:
        try:
            snapshot = fetch_option_chain_rest(underlying, resolved_expiry)
            expiries = snapshot.get("expiries") or expiries
        except Exception as exc:
            log.warning("[delta_exchange_router] rest-option-chain fallback failed for %s: %s", underlying, exc)
            raise HTTPException(status_code=502, detail=f"Delta Exchange option chain fetch failed: {exc}")

    return _to_strategy_payload(underlying, snapshot, expiries)


@delta_exchange_router.get("/futures/{instrument}")
async def get_delta_futures(instrument: str) -> dict:
    """Matches /simulator/paper-trade/futures-chain's {status, futures, synthetic_futures}
    response shape (what CryptoTradeNew.tsx's "Fut" tab already expects) — but Delta only
    ever lists ONE contract per underlying (the perpetual, BTCUSD/ETHUSD), not a monthly
    expiry ladder, so `futures` is always a single row and `synthetic_futures` is always
    empty (that concept — combining options into a synthetic future — has no Delta
    equivalent worth fabricating here)."""
    underlying = instrument.strip().upper()
    if underlying not in SUPPORTED_UNDERLYINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported underlying '{underlying}'. Use one of {SUPPORTED_UNDERLYINGS}.")
    ticker = fetch_perpetual_ticker(underlying)
    return {
        "status": "ok",
        "futures": [{
            "expiry": "Perpetual",
            "symbol": ticker["symbol"],
            "lot_size": 1,
            "token": ticker["symbol"],
            "ltp": ticker["ltp"],
        }],
        "synthetic_futures": [],
    }


@delta_exchange_router.get("/historical_chart")
async def get_delta_historical_chart(
    underlying: str = Query(default="BTC"),
    resolution: str = Query(default="5"),
    from_: int = Query(..., alias="from"),
    to: int = Query(...),
) -> dict:
    """OHLC candle history for the crypto full-chart page (CryptoFullChart.tsx) —
    the Delta-backed analog of chart_api.py's /v1/symbol_historical_chart, kept
    entirely separate from that NSE endpoint/pipeline rather than extended in
    place. Gated on kite_market_config's deltaExchange doc (broker_global_type=2)
    being enabled, same gate every other Delta endpoint here already uses."""
    underlying = underlying.strip().upper()
    if underlying not in SUPPORTED_UNDERLYINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported underlying '{underlying}'. Use one of {SUPPORTED_UNDERLYINGS}.")

    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        if not get_delta_credentials(db._db):
            raise HTTPException(status_code=503, detail="Delta Exchange isn't configured (kite_market_config: broker=deltaExchange missing/disabled).")
    finally:
        db.close()

    try:
        bars = fetch_candles(underlying, resolution, from_, to)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.warning("[delta_exchange_router] historical_chart fetch failed for %s: %s", underlying, exc)
        raise HTTPException(status_code=502, detail=f"Delta Exchange candle fetch failed: {exc}")

    return {"bars": bars}


@delta_exchange_router.get("/broker-status")
async def get_delta_broker_status() -> dict:
    """Mirrors /broker/dhan/status's shape for the Monitors page — "has_token" here
    means "api_key/api_secret are present", not an expiring session (Delta has none,
    see verify_delta_credentials's docstring); "login_time" reflects the last
    successful /verify-credentials call, not an actual re-auth."""
    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        cfg = db._db["kite_market_config"].find_one(
            {"broker": "deltaExchange"},
            {"user_id": 1, "api_key": 1, "login_time": 1, "enabled": 1, "api_secret": 1, "verified": 1},
        ) or {}
    finally:
        db.close()
    has_credentials = bool(cfg.get("api_key")) and bool(cfg.get("api_secret"))
    return {
        "ok": True,
        "enabled": bool(cfg.get("enabled")),
        "user_id": str(cfg.get("user_id") or ""),
        "has_token": has_credentials and bool(cfg.get("verified")),
        "login_time": str(cfg.get("login_time") or ""),
        "expiry_time": "Permanent (Delta has no session expiry)" if has_credentials else "",
    }


@delta_exchange_router.get("/verify-login")
async def post_delta_verify_login() -> dict:
    """The Monitors page's "Relogin"-equivalent button for Delta — see
    verify_delta_credentials's docstring for why this verifies rather than refreshes."""
    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        return verify_delta_credentials(db._db)
    finally:
        db.close()


class DeltaBrokerPositionsRequest(BaseModel):
    # No default — the frontend always sends whatever real broker_id
    # get_delta_positions_broker_status handed it (see that endpoint's own
    # comment for why a real Mongo _id, not the literal "deltaExchange").
    broker_id: str


# ── Live positions (Crypto "Live Positions" page) ──────────────────────────
# Deliberately shaped to match /simulator/positions/broker-status and
# /simulator/positions/by-broker's own {status, broker_status/strategies}
# contracts (see algo.simulator/api.py + Positions.tsx) so CryptoLivePositions.tsx
# can reuse the exact same broker-select logic — just against a single Delta
# account instead of Positions.tsx's multi-account broker_configuration list.
# Delta has no OAuth login flow (api_key/api_secret are entered once, not
# refreshed via popup) — is_logged_in here means "credentials present and
# last verified", not "session currently valid".
def _find_delta_broker_config_doc(raw_db) -> dict | None:
    """The `broker_configuration` doc order placement actually resolves against
    (crypto_order_router.py's _resolve_delta_adapter, algo.order/8004) — NOT
    kite_market_config, which is a second, separate place Delta credentials also
    live (used internally for reading positions/chain data, see get_delta_credentials).
    Both happen to hold the same api_key/api_secret for the one Delta account that
    exists today, but they're different Mongo documents with different _ids.
    positions/broker-status below used to hand out kite_market_config's own _id as
    "broker_id" — CryptoTradeNew.tsx/CryptoLivePositions.tsx just forward whatever
    that returns straight into place-order's own broker_id, so an order placed from
    the /live/:underlying/:brokerId URL always failed with "Broker not resolved."
    (_resolve_delta_adapter only ever looks in broker_configuration). Resolving here
    against broker_configuration instead — same collection/id every other broker
    (Dhan included) already uses uniformly for both viewing and trading — makes one
    id work end to end, matching _is_delta_doc's own docstring: "kept for the day
    Delta's credentials move to a per-account store like the other brokers" — that
    day already happened, this endpoint just hadn't caught up to it.
    """
    for doc in raw_db["broker_configuration"].find({"broker_type": "live"}):
        if _is_delta_doc(doc):
            return doc
    return None


@delta_exchange_router.get("/positions/broker-status")
async def get_delta_positions_broker_status() -> dict:
    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        cfg = _find_delta_broker_config_doc(db._db) or {}
    finally:
        db.close()
    # broker_configuration (unlike kite_market_config) has no enabled/verified flags —
    # same "credential presence is the only signal we have" rule Dhan/FlatTrade/Kite
    # entries in this same collection already use (see execution_socket.py's
    # _list_dhan_docs/_list_broker_docs comment).
    is_logged_in = bool(cfg.get("api_key")) and bool(cfg.get("api_secret"))
    broker_id = str(cfg.get("_id") or "")
    return {
        "status": "success",
        "broker_status": [{
            "broker_id": broker_id,
            "broker_name": "Delta Exchange",
            "is_logged_in": is_logged_in,
            # No popup login URL — Delta's credentials are configured directly in
            # Broker Settings, not via an OAuth redirect like Dhan/Kite/FlatTrade.
            "login_url": "",
            "message": "" if is_logged_in else "Delta Exchange isn't connected. Configure api_key/api_secret in Broker Settings.",
        }],
    }


# Delta's daily BTC/ETH options settle at 12:00 UTC (5:30pm IST) — same convention already
# confirmed against live product data on the frontend (CryptoTradeNew.tsx's
# formatTimeToExpiry). A leg whose expiry has already passed that moment (open OR closed —
# Delta itself has settled/removed it from the live product list by then) must not reach the
# live Positions page at all once the next expiry has rolled in, rather than relying on the
# frontend to hide it.
def _is_expiry_settled(expiry_iso: str) -> bool:
    if not expiry_iso:
        return False  # perpetual futures/legs with no expiry are never "settled"
    try:
        settlement = datetime.strptime(expiry_iso[:10], "%Y-%m-%d").replace(hour=12, tzinfo=timezone.utc)
    except ValueError:
        return False
    return datetime.now(timezone.utc) >= settlement


def _flatten_delta_strategies(strategies: list[dict], db=None) -> list[dict]:
    """Reshapes the grouped {instrument, positions:[...]}[] payload (what
    CryptoLivePositions.tsx's StrategyCard components read) into the flat, one-leg-
    per-row shape CryptoTradeNew.tsx's fetchLivePositions expects — same flat
    Dhan-style field names (position/expiry_date/option/entry_price/ltp/broker_id)
    _fetch_dhan_broker_option_positions already returns for NSE, so the "Open in
    Builder" deep-link (/simulator/crypto/live/:underlying/:brokerId) can reuse that
    exact same fetch/map code path instead of needing its own crypto-specific parser.
    """
    flat: list[dict] = []
    for strategy in strategies:
        underlying = strategy.get("instrument") or ""
        # CryptoTradeNew.tsx's own leg.entry/leg.ltp convention is the real per-contract $
        # price (raw-per-1-BTC-or-ETH quote * contract value — see DELTA_CONTRACT_VALUE's
        # docstring), NOT the raw quote itself. That's different from `strategies` above,
        # whose entry_price/current_ltp StrategyCard.tsx deliberately keeps raw and scales
        # at render time (see its getContractValue comment) — this flat leg list is the one
        # and only consumer that needs the multiplication done here instead.
        contract_value = DELTA_CONTRACT_VALUE.get(underlying, 1.0)
        for leg in strategy.get("positions") or []:
            if _is_expiry_settled(leg.get("expiry") or ""):
                continue
            raw_entry = leg.get("entry_price") or 0
            # CRYPTO BUGFIX: this used to fall back to `raw_entry` whenever Delta's own
            # mark_price came back 0/missing (fetch_delta_open_positions already turns that
            # into current_ltp=None) — silently reporting "ltp == entry" to the frontend,
            # which reads as a flat 0 P&L. That's indistinguishable from a real "price hasn't
            # moved" position, so a leg exited (see CryptoTradeNew.tsx's exitLegAtLtp) while
            # its mark_price happened to be stale/0 got its real, nonzero P&L permanently
            # zeroed out — the exited leg freezes at whatever ltp it last had, and this was
            # feeding it a fabricated one. Sending None here instead lets the frontend's own
            # `existing?.ltp` fallback (fetchLivePositions) keep the last known-good live
            # price instead of clobbering it with entry_price.
            raw_ltp = leg.get("current_ltp")
            flat.append({
                "underlying": underlying,
                "position": "SELL" if leg.get("type") == "sell" else "BUY",
                "expiry_date": leg.get("expiry") or "",
                "strike": leg.get("strike") or 0,
                "option": "PE" if leg.get("option_type") == "put" else "CE",
                "lot_size": leg.get("lot_size") or 1,
                "quantity": leg.get("quantity") or 0,
                "entry_price": raw_entry * contract_value,
                "ltp": raw_ltp * contract_value if raw_ltp is not None else None,
                # fetchLivePositions (CryptoTradeNew.tsx) reads exit_price specifically for an
                # exited leg's displayed LTP — current_ltp on a closed leg (see
                # fetch_delta_closed_positions_today) already holds the real closing fill
                # price, just needs the same contract_value scaling as entry_price/ltp above.
                "exit_price": raw_ltp * contract_value if leg.get("exited") and raw_ltp is not None else None,
                # Delta's own authoritative realized P&L for this closed leg (fees/slippage
                # included, see fetch_delta_closed_positions_today's own comment — already
                # real $, same convention entry_price/ltp above land on after *contract_value,
                # so no additional scaling here). CryptoTradeNew.tsx's calcLegPnl/
                # calcRealizedLegPnl prefer this over re-deriving P&L from (entry - exit_price)
                # * qty whenever it's available — that derivation is blind to fees, so a leg
                # that happened to close at the exact same raw price it opened at (a real,
                # if coincidental, outcome — not missing data) always priced out to exactly $0
                # even when Delta's own ledger shows a small nonzero loss/gain from fees alone.
                "exit_pnl": leg.get("pnl") if leg.get("exited") and leg.get("pnl") is not None else None,
                "exited": bool(leg.get("exited")),
                "token": leg.get("token") or "",
                "leg_id": leg.get("token") or "",
                "broker_id": "deltaExchange",
                # Already real per-position USD (see fetch_delta_open_positions' own comment) —
                # no contract_value multiplication needed here, unlike entry_price/ltp above.
                "margin": leg.get("margin") or 0,
                "cashflow": leg.get("cashflow") or 0,
                # Delta's own native TP/SL (set on their platform, see
                # fetch_delta_position_tp_sl) — raw per-1-BTC quotes same as entry_price, so
                # scaled by contract_value the same way. None when nothing's set on Delta's side.
                "broker_sl_price": (leg["broker_sl_price"] * contract_value) if leg.get("broker_sl_price") is not None else None,
                "broker_tp_price": (leg["broker_tp_price"] * contract_value) if leg.get("broker_tp_price") is not None else None,
                "broker_sl_limit_price": (leg["broker_sl_limit_price"] * contract_value) if leg.get("broker_sl_limit_price") is not None else None,
                "broker_tp_limit_price": (leg["broker_tp_limit_price"] * contract_value) if leg.get("broker_tp_limit_price") is not None else None,
            })

    if db is not None:
        _attach_delta_leg_risk(flat, db)
        _attach_delta_portfolio_risk(flat, db)
    return flat


def _attach_delta_leg_risk(flat_legs: list[dict], db) -> None:
    """Re-attaches each leg's saved SL/Target ("Add Alert") — mirrors
    _fetch_dhan_broker_option_positions' equivalent NSE block (execution_socket.py) exactly,
    just reading crypto_simulator_triggers (algo.order's crypto_paper_trade_triggers.py
    writes here, keyed the same way: broker_id+leg_id) instead of simulator_triggers. Without
    this, a saved crypto alert never reads back — CryptoTradeNew.tsx's fetchLivePositions
    only ever populates riskControls from `p.risk` on each flat leg, which nothing here was
    ever setting.

    Same drift-check as NSE: a trigger only re-attaches if entry_price/quantity/exited still
    match what they were the moment "Add Alert" was last saved — otherwise the saved SL/TP
    was computed off numbers that no longer describe this leg (position added-to, or this
    leg_id slot closed and reopened as a genuinely different trade) and must not be silently
    reapplied. A mismatch flips the trigger to status="stale" (kept, not deleted) instead of
    attaching `risk`.
    """
    from datetime import datetime, timezone

    leg_ids = list({leg["leg_id"] for leg in flat_legs if leg.get("leg_id")})
    if not leg_ids:
        return
    triggers_col = db["crypto_simulator_triggers"]
    trig_by_leg_id: dict[str, dict] = {
        str(trig.get("leg_id") or "").strip(): trig
        for trig in triggers_col.find({"leg_id": {"$in": leg_ids}, "broker_id": "deltaExchange", "status": "active"})
    }
    stale_ids = []
    for leg in flat_legs:
        trig = trig_by_leg_id.get(leg.get("leg_id") or "")
        if not trig:
            continue
        entry_matches = abs(float(trig.get("entry_price_at_set") or 0) - float(leg.get("entry_price") or 0)) < 0.01
        qty_matches = int(trig.get("quantity_at_set") or 0) == int(leg.get("quantity") or 0)
        was_exited = bool(trig.get("exited_at_set"))
        exited_matches = was_exited == bool(leg.get("exited"))
        if entry_matches and qty_matches and exited_matches and not was_exited:
            leg["risk"] = {
                "sl_mode": trig.get("sl_mode"), "sl_value": trig.get("sl_value"),
                "tp_mode": trig.get("tp_mode"), "tp_value": trig.get("tp_value"),
                # Present only in Auto trading mode (see handleAlertsToggle) — a
                # real Delta stop order already sitting on the exchange for this
                # leg. None in Alert Only mode, or once cleared/cancelled.
                "broker_order_id": trig.get("broker_order_id"),
            }
        else:
            stale_ids.append(trig["_id"])
    if stale_ids:
        triggers_col.update_many(
            {"_id": {"$in": stale_ids}},
            {"$set": {"status": "stale", "updated_at": datetime.now(timezone.utc).isoformat()}},
        )


def _attach_delta_portfolio_risk(flat_legs: list[dict], db) -> None:
    """Re-attaches the payoff chart's saved upper/lower stoploss marker
    (crypto_simulator_portfolio_triggers, one doc per broker_id+underlying — see
    crypto_paper_trade_triggers.py's /portfolio-triggers save endpoint) — mirrors
    _fetch_dhan_broker_option_positions' equivalent NSE block (execution_socket.py,
    its own "Attach the payoff chart's saved upper/lower stoploss marker" comment)
    exactly, just reading crypto_simulator_portfolio_triggers instead of
    simulator_portfolio_triggers. Without this, a saved payoff-chart alert never read
    back at all — CryptoTradeNew.tsx's fetchLivePositions only ever populates
    slSavedUpper/slSavedLower from `p.portfolio_risk` on a flat leg, which nothing
    here was ever setting, so the marker genuinely was in Mongo (the save call
    itself works fine) but silently never reappeared on the next page load/poll.

    Same drift-check as _attach_delta_leg_risk above, just basket-level: only
    re-attaches while the set of open legs (and their quantities) for that
    underlying still exactly matches what it was when the marker was saved —
    otherwise the payoff curve it was set against no longer reflects reality. A
    mismatch flips the trigger to status="stale" (kept, not deleted) instead of
    reapplying it.
    """
    from datetime import datetime, timezone

    underlyings = list({leg["underlying"] for leg in flat_legs if leg.get("underlying")})
    if not underlyings:
        return
    portfolio_col = db["crypto_simulator_portfolio_triggers"]
    trig_by_underlying: dict[str, dict] = {
        str(trig.get("underlying") or "").strip(): trig
        for trig in portfolio_col.find({"broker_id": "deltaExchange", "underlying": {"$in": underlyings}, "status": "active"})
    }
    stale_ids = []
    for underlying in underlyings:
        trig = trig_by_underlying.get(underlying)
        if not trig:
            continue
        current_legs = {
            (str(leg.get("leg_id") or "").strip(), int(leg.get("quantity") or 0))
            for leg in flat_legs
            if leg.get("underlying") == underlying and not leg.get("exited") and int(leg.get("quantity") or 0) > 0
        }
        snapshot_legs = {
            (str(s.get("leg_id") or "").strip(), int(s.get("quantity") or 0))
            for s in (trig.get("legs_snapshot") or [])
        }
        if current_legs == snapshot_legs:
            for leg in flat_legs:
                if leg.get("underlying") == underlying:
                    leg["portfolio_risk"] = {"sl_upper": trig.get("sl_upper"), "sl_lower": trig.get("sl_lower")}
        else:
            stale_ids.append(trig["_id"])
    if stale_ids:
        portfolio_col.update_many(
            {"_id": {"$in": stale_ids}},
            {"$set": {"status": "stale", "updated_at": datetime.now(timezone.utc).isoformat()}},
        )


@delta_exchange_router.get("/positions/all")
async def get_delta_positions_all() -> dict:
    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        payload = fetch_delta_open_positions(db._db)
    finally:
        db.close()
    strategies = payload.get("strategies") or []
    # Drop legs whose expiry has already settled (see _is_expiry_settled) before either
    # `strategies` (CryptoLivePositions.tsx's StrategyCard grouping) or the flattened
    # `positions` list (CryptoTradeNew.tsx) below sees them — once the next expiry has
    # rolled in at 5:30pm IST, neither an open nor a same-day-closed leg from the now-
    # settled expiry belongs on the live Positions page anymore. A strategy left with zero
    # legs after filtering is dropped entirely rather than rendering an empty card.
    strategies = [
        {**strategy, "positions": [leg for leg in (strategy.get("positions") or []) if not _is_expiry_settled(leg.get("expiry") or "")]}
        for strategy in strategies
    ]
    strategies = [strategy for strategy in strategies if strategy.get("positions")]
    return {
        "status": "success" if payload.get("ok") else "error",
        "broker_id": "deltaExchange",
        "strategies": strategies,
        # Flat leg list — see _flatten_delta_strategies's docstring for why this rides
        # alongside `strategies` instead of replacing it.
        "positions": _flatten_delta_strategies(strategies, db._db),
        # USD->INR — every entry_price/current_ltp above is in USD (Delta's own
        # settling/quoting asset even for the India entity); the frontend applies this
        # at display time so the Total/Booked/Unbooked P&L shown matches what Delta's
        # own India-facing UI shows in ₹, not the raw USD figure. See get_usd_inr_rate's
        # docstring for the source/fallback.
        "usd_inr_rate": payload.get("usd_inr_rate") or get_usd_inr_rate(db._db),
        "detail": payload.get("detail") or "",
    }


@delta_exchange_router.post("/positions/by-broker")
async def get_delta_positions_by_broker(body: DeltaBrokerPositionsRequest) -> dict:
    # Only one Delta account exists today — this mirrors /simulator/positions/by-broker's
    # POST-with-broker_id shape for parity with Positions.tsx's broker-select flow.
    # broker_id IS checked (previously accepted and silently ignored any value):
    # get_delta_positions_broker_status hands out the connected broker_configuration
    # doc's own _id (see _find_delta_broker_config_doc's docstring for why that
    # collection, not kite_market_config) — a request carrying anything else (a stale
    # id, a guess, someone editing the URL by hand) must not silently fall through to
    # "today's Delta positions" regardless of what was actually asked for.
    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        cfg = _find_delta_broker_config_doc(db._db)
    finally:
        db.close()
    real_broker_id = str(cfg["_id"]) if cfg else ""
    if not real_broker_id or body.broker_id != real_broker_id:
        return {
            "status": "error",
            "broker_id": real_broker_id,
            "strategies": [],
            "positions": [],
            "usd_inr_rate": 0.0,
            "detail": "Unknown broker_id — it doesn't match the connected Delta Exchange account.",
        }
    return await get_delta_positions_all()


@delta_exchange_router.get("/usd-inr-rate")
async def get_delta_usd_inr_rate() -> dict:
    """Standalone (not bundled into a specific positions/strategies payload) so every
    crypto page — live positions, paper-trade portfolio/webhook-strategies (which read
    from crypto_paper_trade_router.py, a different module entirely), and the builder —
    can fetch this once via one shared frontend hook, regardless of which endpoint each
    page otherwise uses for its own data. See get_usd_inr_rate's docstring for source/
    fallback/caching."""
    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        return {"status": "success", "rate": get_usd_inr_rate(db._db)}
    finally:
        db.close()


class SetUsdInrRateRequest(BaseModel):
    rate: float
    note: str = ""


@delta_exchange_router.post("/usd-inr-rate")
async def post_delta_usd_inr_rate(
    body: SetUsdInrRateRequest,
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    """Admin-only — appends a new historical row (crypto_usd_inr_rate_history), never
    overwrites a past one, so every rate this app ever used for a P&L display stays
    auditable. See set_usd_inr_rate's docstring."""
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    if body.rate <= 0:
        raise HTTPException(status_code=400, detail="rate must be positive")
    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        entry = set_usd_inr_rate(db._db, body.rate, body.note)
        return {"status": "success", "entry": entry}
    finally:
        db.close()


@delta_exchange_router.get("/usd-inr-rate/history")
async def get_delta_usd_inr_rate_history(
    limit: int = Query(default=50, ge=1, le=200),
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    from features.mongo_data import MongoData  # type: ignore
    db = MongoData()
    try:
        return {"status": "success", "history": get_usd_inr_rate_history(db._db, limit)}
    finally:
        db.close()


@delta_exchange_router.get("/move-options/{instrument}")
async def get_delta_move_options(instrument: str) -> dict:
    """Delta's native 'Move' product for the crypto page's Straddles tab — see
    fetch_move_options_rest's docstring for why this is priced as a real straddle
    (bsCall+bsPut) on the frontend instead of a placeholder."""
    underlying = instrument.strip().upper()
    if underlying not in SUPPORTED_UNDERLYINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported underlying '{underlying}'. Use one of {SUPPORTED_UNDERLYINGS}.")
    try:
        rows = fetch_move_options_rest(underlying)
    except Exception as exc:
        log.warning("[delta_exchange_router] move-options fetch failed for %s: %s", underlying, exc)
        raise HTTPException(status_code=502, detail=f"Delta Exchange move-options fetch failed: {exc}")
    return {"status": "ok", "move_options": rows}


# ── BTC/ETH chart alert checker — manual override ──────────────────────────
# Both loops are already auto-started unconditionally at process boot
# (api.py's _auto_start_delta_alert_checker) and never auto-stopped for
# market hours — these exist purely as a manual override, same parity NSE's
# /v1/alert-checker/* and /v1/indicator-alert-monitor/* endpoints give that
# checker (shared/chart_api.py).
@delta_exchange_router.api_route("/alert-checker/start", methods=["GET", "POST"])
async def delta_alert_checker_start(current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    from simulator.delta_alert_checker import start_delta_alert_checker_monitor
    return start_delta_alert_checker_monitor()


@delta_exchange_router.api_route("/alert-checker/stop", methods=["GET", "POST"])
async def delta_alert_checker_stop(current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    from simulator.delta_alert_checker import stop_delta_alert_checker_monitor
    return await stop_delta_alert_checker_monitor()


@delta_exchange_router.get("/alert-checker/status")
async def delta_alert_checker_status(current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    from simulator.delta_alert_checker import is_delta_alert_checker_running
    return {"status": "success", "running": is_delta_alert_checker_running()}


@delta_exchange_router.post("/indicator-alert-monitor/start")
async def delta_indicator_alert_monitor_start(current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    from simulator.delta_alert_checker import start_delta_indicator_alert_monitor
    return start_delta_indicator_alert_monitor()


@delta_exchange_router.post("/indicator-alert-monitor/stop")
async def delta_indicator_alert_monitor_stop(current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    from simulator.delta_alert_checker import stop_delta_indicator_alert_monitor
    return await stop_delta_indicator_alert_monitor()


@delta_exchange_router.get("/indicator-alert-monitor/status")
async def delta_indicator_alert_monitor_status(current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    from simulator.delta_alert_checker import is_delta_indicator_alert_monitor_running
    return {"status": "success", "running": is_delta_indicator_alert_monitor_running()}


# ── Delta Exchange upstream WS connection ───────────────────────────────────
# The actual socket everything crypto-side depends on (chart live ticks,
# delta_alert_checker.py's price polling, the option chain) — all read
# through delta_ticker_manager's one in-process cache, so this connection
# being down silently stales every one of them at once. Lazily started on
# first ensure_subscribed() call (see delta_exchange_ws.py) and self-healing
# (auto-reconnects on drop, delta_exchange_ws.py's _run_forever) — there's no
# clean "stop" to offer here, only "confirm it's up" and "force a (re)connect
# + resubscribe BTC/ETH", i.e. the same restartOnly pattern the admin
# Monitors page already uses for the NSE Websocket Ticker row.
@delta_exchange_router.get("/ws-status")
async def get_delta_ws_status() -> dict:
    return delta_ticker_manager.get_status()


@delta_exchange_router.api_route("/ws-restart", methods=["GET", "POST"])
async def delta_ws_restart(current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    delta_ticker_manager.start()
    for underlying in SUPPORTED_UNDERLYINGS:
        try:
            delta_ticker_manager.ensure_subscribed(underlying, "")
        except Exception as exc:
            log.warning("[delta_exchange_router] ws-restart ensure_subscribed failed for %s: %s", underlying, exc)
    return {"status": "success", "message": "Delta WS (re)connect + BTC/ETH resubscribe requested."}
