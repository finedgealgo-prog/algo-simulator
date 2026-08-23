"""
Crypto paper-trade strategy/portfolio persistence — fully isolated from the
NSE paper-trade endpoints in api.py.

Background: CryptoTradeNew.tsx (Delta Exchange BTC/ETH options paper
trading) used to call the exact same /simulator/paper-trade/strategies,
/simulator/paper-trade/portfolios etc. endpoints the NSE PaperTradeNew.tsx
page uses, reading/writing the same `simulator_strategy` / `simulator_portfolio`
Mongo collections. That meant a crypto strategy and an NSE strategy could
show up side by side in the same list, and — worse — counted against the
same plan limits (active_strategy_limit, advanced_slots).

This router gives crypto its own persistence: same request/response shapes,
same plan-limit checks, same portfolio-auto-create-by-name, same status
normalization as the NSE endpoints in api.py, just pointed at
`crypto_simulator_strategy` / `crypto_simulator_portfolio` instead — so
crypto and NSE strategies never mix and never share plan-limit counts.

Every handler below is a line-for-line mirror of its NSE counterpart in
api.py (see the docstring/comment on each for the exact line it mirrors),
with only the collection name swapped. The handful of names imported from
`api` below are deliberate, not laziness — see the "Reused from api.py"
comment further down for why.

Mounted at /simulator/crypto-paper-trade by simulator_main.py, the same way
fast_option_chain_api.router and chart_api.router are mounted there.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from bson import ObjectId
from fastapi import APIRouter, Depends, Query

from features.mongo_data import MongoData
from features import auth as app_auth

log = logging.getLogger(__name__)

# ── Reused from api.py ──────────────────────────────────────────────────────
# Everything below is either (a) a Pydantic request-body model — imported
# so the wire contract with the frontend can never silently drift between
# the NSE and crypto endpoints, or (b) pure, platform-level subscription/plan
# logic that has nothing to do with NSE vs crypto — a user has exactly ONE
# subscription plan, shared across both. _sim_find_plan/_seed_...
# additionally depend on api.py's ~120-line _SIM_DEFAULT_PLANS seed data,
# which would be a bad thing to fork/duplicate here (guaranteed drift).
# Everything that IS strategy/portfolio persistence (collections, counts,
# ownership checks, index management) is reimplemented locally below,
# pointed at the crypto_* collections — that's the actual isolation this
# router exists for.
#
# This import is safe / not circular: simulator_main.py (the only thing that
# imports this module) always does `from api import app` first, so api.py
# is already fully loaded — and api.py never imports this module. Reading
# these names does not modify api.py or touch any of its routes/state.
from api import (  # noqa: E402
    app,
    PTStrategyIn,
    PTPortfolioIn,
    PTExecutionModeIn,
    PTAdjustmentIn,
    PTAdjustmentPatchIn,
    PTWebhookIn,
    PTNewStrategyWebhookIn,
    PTUpdateStrategyWebhookIn,
    PTTriggerIn,
    PTAlertConfigIn,
    _resolve_sim_user_id,
    _enrich_pt_strategy_positions,
    _sim_user_or_filter,
    _sim_sub_effective_status,
    _sim_find_plan,
    _seed_sim_subscription_plans_if_empty,
    SIM_SUB_STATUS_ACTIVE,
    _disable_tv_alerts_for_webhook,
    _normalize_pt_option_type,
    _net_pt_positions,
    _SIM_DEFAULT_USER_ID,
    # Product-scoping helpers (added alongside the crypto `product` field on
    # sim_subscription_plans/sim_user_subscriptions) — see api.py's own
    # "product scoping" section for the full rationale. Reused here rather
    # than duplicated so "simulator" vs "crypto" filtering semantics (missing
    # product == simulator, never == crypto) can't drift between the two
    # files the way the plan-resolution logic itself once did (see this
    # file's module docstring).
    SIM_PRODUCT_CRYPTO,
    _sim_product_query_filter,
    _sim_plans_list_response,
    _sim_my_plan_response,
)

router = APIRouter(prefix="/simulator/crypto-paper-trade")

# Own MongoData instance, same instantiation pattern api.py (and its sibling
# simulator/api_server.py) use: `_shared_mongo = MongoData()` at module scope,
# accessed everywhere via `_shared_mongo._db[...]`.
_shared_mongo = MongoData()
IST = timezone(timedelta(hours=5, minutes=30))

CRYPTO_STRATEGY_COLLECTION = "crypto_simulator_strategy"
CRYPTO_PORTFOLIO_COLLECTION = "crypto_simulator_portfolio"
# Webhook Strategies (pending/today's-triggered TradingView-webhook positions not yet
# mapped to a saved strategy) — isolated the same way crypto_simulator_strategy is
# isolated from simulator_strategy. Written to by crypto_pt_create_new_strategy_webhook
# below (the crypto mirror of api.py's simulator_pt_create_new_strategy_webhook,
# api.py:5033) and updated by the crypto webhook trigger route once a webhook actually
# fires — see the "Webhooks" section further down for both.
CRYPTO_NEW_POSITIONS_COLLECTION = "crypto_simulator_new_positions"
# Basket-level conditional rules ("if underlying moves X%, roll the strategy")
# attached to a saved crypto strategy — isolated the same way crypto_simulator_strategy
# is isolated from simulator_strategy. NOTE: SimulatorRiskMonitor (simulator_risk_monitor.py)
# is what actually EVALUATES and FIRES adjustments on live positions, and it only reads/writes
# the NSE `simulator_adjustments` collection (see e.g. _fetch_adjustment_docs_broker/
# _fetch_adjustment_docs_paper at simulator_risk_monitor.py:653-677) — it has not been taught
# to scan crypto_simulator_adjustments. So rows saved here are plain CRUD/history for now;
# they will not be auto-evaluated/rolled until the risk monitor is separately updated to also
# scan this collection. That's out of scope for this pass (see this file's module docstring).
CRYPTO_ADJUSTMENTS_COLLECTION = "crypto_simulator_adjustments"
# TradingView-webhook URLs (the "Generate Webhook URL" flow) — isolated the same way
# crypto_simulator_strategy is isolated from simulator_strategy. See the "Webhooks"
# section near the bottom of this file for the endpoints that read/write this and the
# force_fire_adjustment caveat (SimulatorRiskMonitor is NSE-only, see that section).
CRYPTO_WEBHOOKS_COLLECTION = "crypto_simulator_webhooks"
# Per-leg SL/Target ("Add Alert") and basket-level Position Configuration
# (Stoploss/Target/Trail SL/Hedge) storage — isolated the same way
# crypto_simulator_adjustments is isolated from simulator_adjustments.
# Same caveat as CRYPTO_ADJUSTMENTS_COLLECTION above: SimulatorRiskMonitor
# only scans the NSE simulator_triggers/simulator_portfolio_triggers
# collections, so these are plain CRUD/record-keeping for now — nothing
# auto-fires off a saved crypto trigger yet. Indexes for both already exist
# (see MongoData.ensure_core_indexes, uniq_crypto_trigger_by_broker_leg /
# uniq_crypto_portfolio_trigger_by_broker_underlying).
CRYPTO_TRIGGERS_COLLECTION = "crypto_simulator_triggers"
CRYPTO_PORTFOLIO_TRIGGERS_COLLECTION = "crypto_simulator_portfolio_triggers"

# Same default portfolio buckets api.py seeds for NSE (_DEFAULT_PAPER_TRADE_PORTFOLIOS),
# duplicated here (not imported) since it's a two-item literal, not worth
# coupling to api.py's private constant for.
_DEFAULT_CRYPTO_PAPER_TRADE_PORTFOLIOS = ["Running Trades", "Week On Nct Mnth"]


def _ensure_default_crypto_simulator_portfolios() -> None:
    """Mirrors api.py's _ensure_default_simulator_portfolios (api.py:3161), crypto_ collection."""
    col = _shared_mongo._db[CRYPTO_PORTFOLIO_COLLECTION]
    for portfolio_name in _DEFAULT_CRYPTO_PAPER_TRADE_PORTFOLIOS:
        if not col.find_one({"name": portfolio_name}, {"_id": 1}):
            col.insert_one({
                "name": portfolio_name,
                "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            })


_CRYPTO_SIMULATOR_STRATEGY_INDEX_ENSURED = False


def _ensure_crypto_simulator_strategy_index() -> None:
    """Mirrors api.py's _ensure_simulator_strategy_index (api.py:7951), crypto_ collection."""
    global _CRYPTO_SIMULATOR_STRATEGY_INDEX_ENSURED
    if _CRYPTO_SIMULATOR_STRATEGY_INDEX_ENSURED:
        return
    try:
        _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].create_index(
            [("user_id", 1), ("all_exited", 1)],
            name="idx_crypto_simulator_strategy_user_v1",
        )
    except Exception:
        pass
    try:
        _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].create_index(
            [("portfolio_id", 1), ("user_id", 1)],
            name="idx_crypto_simulator_strategy_portfolio_v1",
        )
        _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].create_index(
            [("portfolio_name", 1), ("user_id", 1)],
            name="idx_crypto_simulator_strategy_portfolio_name_v1",
        )
    except Exception:
        pass
    try:
        # One-time backfill for docs saved before the `status` field existed,
        # same rule api.py's version applies (1 = active, 2 = closed, 0 = inactive).
        strategy_col = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION]
        strategy_col.update_many(
            {"status": {"$exists": False}, "all_exited": True},
            {"$set": {"status": 2}},
        )
        strategy_col.update_many(
            {"status": {"$exists": False}},
            {"$set": {"status": 1}},
        )
    except Exception:
        pass
    _CRYPTO_SIMULATOR_STRATEGY_INDEX_ENSURED = True


def _crypto_sim_resolve_plan_and_advanced_slots(user_id: Any) -> tuple[dict, int]:
    """
    Mirrors api.py's _sim_resolve_plan_and_advanced_slots (api.py:6543) —
    EXCEPT scoped to product="crypto", not "simulator". This used to be a
    straight copy of the NSE query with no product distinction at all, which
    meant this resolved whichever row (NSE or crypto) was most recently
    granted/most recent by _id — i.e. a crypto webhook-mode/advanced-slot
    check could actually be reading the user's *NSE* plan's fields. Now that
    sim_user_subscriptions carries a `product` field, this is scoped to the
    crypto rows only (see _sim_product_query_filter's docstring in api.py for
    why a *missing* product still never counts as "crypto").
    """
    _seed_sim_subscription_plans_if_empty()
    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    sub_doc = subs_col.find_one(
        {**_sim_user_or_filter(user_id), **_sim_product_query_filter(SIM_PRODUCT_CRYPTO)},
        sort=[("_id", -1)],
    ) if user_id is not None else None
    is_active_sub = _sim_sub_effective_status(sub_doc) == SIM_SUB_STATUS_ACTIVE
    plan_id = sub_doc["plan_id"] if (sub_doc and is_active_sub) else "crypto_free"
    plan = _sim_find_plan(plan_id) or _sim_find_plan("crypto_free") or {}
    advanced_slots_purchased = int((sub_doc or {}).get("advanced_slots_purchased") or 0) if is_active_sub else 0
    advanced_slots_total = int(plan.get("advanced_slots") or 0) + advanced_slots_purchased
    return plan, advanced_slots_total


def _crypto_sim_active_strategy_limit_error(user_id: Any) -> Optional[str]:
    """Mirrors api.py's _sim_active_strategy_limit_error (api.py:6516), crypto_ collection
    count — scoped to product="crypto" for the same reason as
    _crypto_sim_resolve_plan_and_advanced_slots above."""
    _seed_sim_subscription_plans_if_empty()
    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    sub_doc = subs_col.find_one(
        {**_sim_user_or_filter(user_id), **_sim_product_query_filter(SIM_PRODUCT_CRYPTO)},
        sort=[("_id", -1)],
    ) if user_id is not None else None
    is_active_sub = _sim_sub_effective_status(sub_doc) == SIM_SUB_STATUS_ACTIVE
    plan_id = sub_doc["plan_id"] if (sub_doc and is_active_sub) else "crypto_free"
    plan = _sim_find_plan(plan_id) or _sim_find_plan("crypto_free") or {}
    plan_name = plan.get("plan_name") or "current"
    limit = (plan or {}).get("active_strategy_limit", -1)
    if limit is None or limit == -1:
        return None
    _ensure_crypto_simulator_strategy_index()
    count = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].count_documents(
        {"$or": [{"user_id": user_id}, {"user_id": {"$exists": False}}]}
    )
    if count >= limit:
        return f"Strategy limit reached ({count}/{limit}) on your {plan_name} plan. Upgrade your plan to create more strategies."
    return None


# ── Subscription (crypto product) ───────────────────────────────────────────
# Crypto mirrors of api.py's GET /simulator/subscription/plans and
# GET /simulator/subscription/my-plan (api.py:7130/7148-ish) — the two didn't
# exist here before crypto plans did, so the crypto pages had nothing to call
# except the NSE endpoints, which is exactly the bug this whole change fixes
# (see this module's docstring). Both just force product="crypto" through the
# same shared response builders api.py's own endpoints use
# (_sim_plans_list_response / _sim_my_plan_response), so field shape/behavior
# can't drift between the NSE and crypto versions the way the plan-resolution
# helpers above once did.
@router.get("/subscription/plans")
def crypto_subscription_plans() -> list[dict[str, Any]]:
    """Public — no auth. Crypto plan catalogue only, never user-specific data."""
    return _sim_plans_list_response(SIM_PRODUCT_CRYPTO)


@router.get("/subscription/my-plan")
def crypto_subscription_my_plan(current_user: dict = Depends(app_auth.require_current_user)) -> dict[str, Any]:
    user_id = _resolve_sim_user_id(current_user)
    return _sim_my_plan_response(user_id, SIM_PRODUCT_CRYPTO)


def _crypto_sim_advanced_strategies(user_id: Any) -> list[dict]:
    """Mirrors api.py's _sim_advanced_strategies (api.py:6633), crypto_ collection."""
    _ensure_crypto_simulator_strategy_index()
    docs = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].find(
        {
            "$or": [{"user_id": user_id}, {"user_id": {"$exists": False}}],
            "execution_mode": "advanced",
            "all_exited": {"$ne": True},
            "status": {"$ne": 2},
        },
        {"strategy_name": 1, "portfolio_name": 1},
    )
    return [{"id": str(d["_id"]), "strategy_name": d.get("strategy_name") or "",
              "portfolio_name": d.get("portfolio_name") or ""} for d in docs]


def _crypto_sim_advanced_slot_limit_error(user_id: Any) -> Optional[str]:
    """Mirrors api.py's _sim_advanced_slot_limit_error (api.py:6655)."""
    plan, advanced_slots_total = _crypto_sim_resolve_plan_and_advanced_slots(user_id)
    plan_name = plan.get("plan_name") or "current"
    if advanced_slots_total <= 0:
        return f"Your {plan_name} plan has no Advanced strategy slots. Upgrade your plan or buy extra slots."
    used = len(_crypto_sim_advanced_strategies(user_id))
    if used >= advanced_slots_total:
        return f"Advanced slot limit reached ({used}/{advanced_slots_total}) on your {plan_name} plan. Switch an existing Advanced strategy to Normal to free up a slot."
    return None


def _insert_crypto_simulator_strategy(
    portfolio_name: str,
    strategy_name: str,
    instrument: Optional[str],
    spot_price: Optional[float],
    config: Optional[dict[str, Any]],
    positions: list[dict],
    mode: Optional[str],
    extra_fields: Optional[dict[str, Any]] = None,
    user_id: Optional[Any] = None,
) -> str:
    """Mirrors api.py's _insert_simulator_strategy (api.py:6673), crypto_ collections."""
    portfolio_col = _shared_mongo._db[CRYPTO_PORTFOLIO_COLLECTION]
    strategy_col = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION]
    portfolio = portfolio_col.find_one(
        {"name": portfolio_name, "$or": [{"user_id": user_id}, {"user_id": {"$exists": False}}]},
        {"_id": 1},
    )
    if not portfolio:
        inserted = portfolio_col.insert_one({
            "name": portfolio_name,
            "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            "user_id": user_id,
        })
        portfolio_id = inserted.inserted_id
    else:
        portfolio_id = portfolio["_id"]
    now_iso = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    initial_pos_history = [{
        "action": "INITIAL_SAVE",
        "time": now_iso,
        "strike": p.get("strike"),
        "option_type": p.get("option_type") or p.get("type"),
        "expiry": str(p.get("expiry", ""))[:10],
        "entry_price": p.get("entry_price"),
        "lots": p.get("lots"),
        "lot_size": p.get("lot_size"),
    } for p in positions if not p.get("exited")]
    doc = {
        "portfolio_id": str(portfolio_id),
        "portfolio_name": portfolio_name,
        "strategy_name": strategy_name,
        "instrument": instrument or "nifty",
        "spot_price": spot_price,
        "config": config or {},
        "positions": positions,
        "saved_at": now_iso,
        "position_history": initial_pos_history,
        "mode": mode or "live",
        "user_id": user_id,
        # 1 = active, 2 = closed (all legs exited), 0 = inactive — always 1 on
        # creation, same convention api.py's simulator_strategy docs use.
        "status": 1,
        **(extra_fields or {}),
    }
    result = strategy_col.insert_one(doc)
    return str(result.inserted_id)


def _crypto_group_expiry_to_iso(expiry_date: str) -> str:
    """algo_trade_positions_history's expiry_date is 'DD-MM-YYYY HH:MM:SS'
    (execution_socket.py's own save format) — crypto_pt_get_strategy's positions
    use plain ISO 'YYYY-MM-DD' (see CryptoTradeNew.tsx's fetchTradeStrategy:
    `String(p.expiry || "").slice(0, 10)`), so convert here rather than pushing
    two different expiry formats onto one frontend Leg-mapping function."""
    raw = expiry_date.strip()
    if not raw:
        return ""
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw[:19], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _find_owned_crypto_strategy(strategy_id: str, current_user: dict) -> Optional[dict]:
    """Mirrors api.py's _find_owned_strategy (api.py:5861), crypto_ collection."""
    doc = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].find_one({"_id": ObjectId(strategy_id)})
    if not doc:
        return None
    doc_user_id = doc.get("user_id")
    current_user_id = current_user.get("_id")
    if doc_user_id is not None and current_user_id is not None and str(doc_user_id) != str(current_user_id):
        return None
    return doc


def _str_id(doc: dict | None) -> dict | None:
    """Mirrors api.py's _str_id (api.py:3171)."""
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


# ── Strategies ───────────────────────────────────────────────────────────────

@router.post("/strategies")
async def crypto_pt_save_strategy(body: PTStrategyIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_save_strategy (api.py:6740)."""
    try:
        user_id = _resolve_sim_user_id(current_user)
        limit_error = _crypto_sim_active_strategy_limit_error(user_id)
        if limit_error:
            return {"status": "error", "message": limit_error}
        execution_mode = "advanced" if str(body.execution_mode or "regular").lower() == "advanced" else "regular"
        if execution_mode == "advanced":
            slot_error = _crypto_sim_advanced_slot_limit_error(user_id)
            if slot_error:
                return {"status": "error", "message": slot_error}
        positions = []
        for position in (body.positions or []):
            pos = position.dict()
            if pos.get("quantity") is None:
                pos["quantity"] = (pos.get("lots") or 1) * (pos.get("lot_size") or 1)
            positions.append(pos)
        strategy_id = _insert_crypto_simulator_strategy(
            body.portfolio_name, body.strategy_name, body.instrument, body.spot_price,
            body.config, positions, body.mode,
            extra_fields={"execution_mode": execution_mode},
            user_id=user_id,
        )
        return {"status": "success", "id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/strategies")
async def crypto_pt_list_strategies(
    portfolio_id: Optional[str] = None,
    portfolio_name: Optional[str] = None,
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """Mirrors api.py's simulator_pt_list_strategies (api.py:5781)."""
    try:
        _ensure_crypto_simulator_strategy_index()
        filt: dict[str, Any] = {}
        normalized_portfolio_id = str(portfolio_id or "").strip()
        normalized_portfolio_name = str(portfolio_name or "").strip()
        if normalized_portfolio_id:
            filt["portfolio_id"] = normalized_portfolio_id
        elif normalized_portfolio_name:
            filt["portfolio_name"] = normalized_portfolio_name
        current_user_id = _resolve_sim_user_id(current_user)
        filt["$or"] = [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]
        docs = list(_shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].find(filt).sort("saved_at", -1))
        result = []
        for doc in docs:
            doc = _enrich_pt_strategy_positions(doc, allow_rest_fallback=False)
            doc["_id"] = str(doc["_id"])
            positions = doc.get("positions", [])
            doc["position_count"] = len(positions)
            doc["all_exited"] = all(p.get("exited", False) for p in positions) if positions else False
            realized = 0.0
            open_positions = []
            for p in positions:
                qty = p.get("quantity") or ((p.get("lots") or 1) * (p.get("lot_size") or 1))
                is_sell = str(p.get("type", "")).lower() == "sell"
                if p.get("exited"):
                    if p.get("pnl") is not None:
                        realized += p["pnl"]
                    elif p.get("exit_price") is not None and p.get("entry_price") is not None:
                        realized += (p["entry_price"] - p["exit_price"]) * qty if is_sell else (p["exit_price"] - p["entry_price"]) * qty
                else:
                    open_positions.append({
                        "type": p.get("type", ""),
                        "option_type": p.get("option_type", ""),
                        "strike": p.get("strike", 0),
                        "expiry": p.get("expiry", ""),
                        "token": p.get("token", ""),
                        "entry_price": p.get("entry_price", 0),
                        "quantity": qty,
                    })
            doc["realized_pnl"] = round(realized, 2)
            doc["open_positions"] = open_positions
            result.append(doc)
        return {"status": "success", "strategies": result}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# Registered before /strategies/{strategy_id} so FastAPI's first-match routing
# doesn't swallow "count" as a strategy_id path param — same ordering reason
# api.py's own simulator_pt_count_strategies comment gives (api.py:5839).
@router.get("/strategies/count")
async def crypto_pt_count_strategies(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_count_strategies (api.py:5841)."""
    try:
        _ensure_crypto_simulator_strategy_index()
        current_user_id = _resolve_sim_user_id(current_user)
        filt = {"$or": [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]}
        count = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].count_documents(filt)
        return {"status": "success", "count": count}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/strategies/{strategy_id}")
async def crypto_pt_get_strategy(strategy_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_get_strategy (api.py:5882)."""
    try:
        doc = _find_owned_crypto_strategy(strategy_id, current_user)
        if not doc:
            return {"status": "error", "message": "Not found"}
        return {"status": "success", "strategy": _str_id(_enrich_pt_strategy_positions(doc))}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/executed-group/{group_id}")
async def crypto_pt_get_executed_group(group_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Crypto-specific replacement for CryptoTradeAnalyse.tsx's old path into the
    algo.trade-side, NSE-shared /strategy-trade-history/group/{group_id} endpoint
    (api.py:algo.trade's _aggregate_group_trade_history_payload) — that endpoint
    reshapes algo_trades/algo_trade_positions_history legs into a generic
    "legs.open/closed" shape built for NSE, which needed a growing pile of
    frontend-side reinterpretation (loadContractValue multiplies, isExternalEntityView
    exceptions) to make crypto's raw-points convention line up with it, and was still
    unreliable (its status/activation_mode guess didn't account for "fast-forward"
    mode at all, silently returning zero legs for a fast-forward group).

    This instead reads algo_trades (filtered by strategy_group_id, no activation_mode
    guess needed — a group_id is unique regardless of mode) + algo_trade_positions_history
    directly — same collections/fields compute_strategy_mtm already reads correctly for
    the Overall SL/Target trigger — and returns legs in the EXACT field shape
    crypto_pt_get_strategy's own `positions` array uses (type/expiry/strike/option_type/
    entry_price/exit_price/current_ltp/exited/token/entry_time/exit_time/lots/lot_size/
    quantity). The frontend's fetchTradeStrategy leg-mapper — proven correct for
    /trade/:id — is reused completely unchanged against this response; only the fetch
    URL and the "no update/save for a live executed group" affordances differ.

    lot_size is always returned as 1 with lots/quantity set to the real traded quantity
    (never DB's own `lot_size` field) — that field is confirmed spurious NSE-carryover
    on a crypto leg (see strategyLegPnl.ts's legQty() comment), never the real Delta
    contract count; forcing 1 here means resolveBrokerSizing's existing
    lots = quantity/lot_size math naturally recovers the right size with zero
    crypto-specific branching needed on the frontend.
    """
    try:
        from features.trading_core import safe_float, is_sell, parse_timestamp, COL_POSITIONS_HIST
        from features.delta_event import normalize_crypto_underlying

        current_user_id = _resolve_sim_user_id(current_user)
        trades_col = _shared_mongo._db["algo_trades"]
        query: dict[str, Any] = {"strategy_group_id": group_id}
        if current_user_id:
            query["user_id"] = current_user_id
        trades = list(trades_col.find(query))
        if not trades:
            return {"status": "error", "message": "Not found"}
        trade_ids = [str(t["_id"]) for t in trades]

        hist_col = _shared_mongo._db[COL_POSITIONS_HIST]
        # entry_trade/exit_trade.traded_timestamp are IST civil time (execution_socket.py
        # writes them via datetime.now(IST), same convention this file's own IST constant
        # exists for) — naive, no tzinfo. Comparing that against a naive UTC "now" silently
        # misjudged a leg as still-open for up to 5.5 hours after it had actually already
        # exited (verified against a real leg: exit_trade present with a real fill price,
        # but exit_dt in the future relative to UTC "now" made exited come back False).
        # parse_timestamp strips tzinfo (see its docstring), so match its naive domain by
        # dropping IST's own offset here too, rather than leaving it UTC.
        now_dt = datetime.now(IST).replace(tzinfo=None)
        positions: list[dict] = []
        for doc in hist_col.find({"trade_id": {"$in": trade_ids}}):
            entry_trade = doc.get("entry_trade") if isinstance(doc.get("entry_trade"), dict) else {}
            if not entry_trade:
                continue  # pending leg — not yet entered, nothing to show
            entry_price = safe_float(entry_trade.get("price") or entry_trade.get("trigger_price"))
            if entry_price <= 0:
                continue
            exit_trade = doc.get("exit_trade") if isinstance(doc.get("exit_trade"), dict) else None
            exit_ts = str((exit_trade or {}).get("traded_timestamp") or (exit_trade or {}).get("trigger_timestamp") or "").strip()
            exit_dt = parse_timestamp(exit_ts)
            exited = bool(exit_trade) and (not exit_dt or not now_dt or exit_dt <= now_dt)
            quantity = safe_float(doc.get("quantity") or entry_trade.get("quantity"))
            if quantity <= 0:
                continue
            positions.append({
                "type": "sell" if is_sell(str(doc.get("position") or "")) else "buy",
                "expiry": _crypto_group_expiry_to_iso(str(doc.get("expiry_date") or "")),
                "strike": safe_float(doc.get("strike")),
                "option_type": "put" if str(doc.get("option") or "").strip().upper() == "PE" else "call",
                "entry_price": entry_price,
                "exit_price": (safe_float(exit_trade.get("price") or exit_trade.get("trigger_price")) if exit_trade else None),
                # Always null, matching crypto_pt_get_strategy's own crypto_simulator_strategy
                # source exactly — CryptoTradeAnalyse.tsx's fetchTradeStrategy leg-mapper has a
                # `Number(p.current_ltp) > 0 ? ... : existing?.ltp` branch that ONLY takes the
                # `existing?.ltp` path (preserving the live WS-ticked price across every 30s
                # poll) when current_ltp is falsy — see that mapper's own comment. That branch
                # is unreachable for a crypto_simulator_strategy leg since the backend never
                # populates current_ltp for Delta symbols there. Populating it here from
                # last_saw_price (a periodically-persisted DB snapshot, staler than the live WS
                # tick stream by design) took the OTHER branch instead — silently snapping this
                # endpoint's legs' ltp backward to that stale value every 30s poll, a visible
                # "wrong number" a live WS-ticked NSE-sourced leg never has. Leaving this null
                # lets the exact same fallback protect the live tick here too.
                "current_ltp": None,
                "exited": exited,
                "token": (str(doc.get("token") or doc.get("symbol") or "") or None),
                "entry_time": (str(entry_trade.get("traded_timestamp") or "") or None),
                "exit_time": (str((exit_trade or {}).get("traded_timestamp") or "") if exited and exit_trade else None),
                "lot_size": 1,
                "lots": quantity,
                "quantity": quantity,
                "leg_id": str(doc.get("_id") or ""),
            })

        tickers = {normalize_crypto_underlying(str(t.get("ticker") or "")) for t in trades}
        tickers.discard("")
        instrument = next(iter(tickers), "")
        group_names = sorted({str(t.get("name") or "").strip() for t in trades if t.get("name")})
        strategy_name = (
            f"{group_names[0]} ({len(trades)})" if len(trades) > 1 and group_names
            else (group_names[0] if group_names else f"Group {group_id}")
        )
        return {
            "status": "success",
            "strategy": {
                "_id": group_id,
                "strategy_name": strategy_name,
                "instrument": instrument,
                "positions": positions,
                "execution_mode": "regular",
            },
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/strategies/{strategy_id}")
async def crypto_pt_update_strategy(strategy_id: str, body: PTStrategyIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_update_strategy (api.py:6381)."""
    try:
        if not _find_owned_crypto_strategy(strategy_id, current_user):
            return {"status": "error", "message": "Strategy not found"}
        current_user_id = _resolve_sim_user_id(current_user)
        portfolio_col = _shared_mongo._db[CRYPTO_PORTFOLIO_COLLECTION]
        strategy_col = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION]
        portfolio = portfolio_col.find_one(
            {"name": body.portfolio_name, "$or": [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]},
            {"_id": 1},
        )
        if not portfolio:
            result = portfolio_col.insert_one({
                "name": body.portfolio_name,
                "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
                "user_id": current_user_id,
            })
            portfolio_id = result.inserted_id
        else:
            portfolio_id = portfolio["_id"]
        positions = []
        for position in (body.positions or []):
            pos = position.dict()
            if pos.get("quantity") is None:
                pos["quantity"] = (pos.get("lots") or 1) * (pos.get("lot_size") or 1)
            positions.append(pos)
        result = strategy_col.update_one(
            {"_id": ObjectId(strategy_id)},
            {"$set": {
                "portfolio_id": str(portfolio_id),
                "portfolio_name": body.portfolio_name,
                "strategy_name": body.strategy_name,
                "instrument": body.instrument or "nifty",
                "spot_price": body.spot_price,
                "config": body.config or {},
                "positions": positions,
                "updated_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            }},
        )
        if result.matched_count == 0:
            return {"status": "error", "message": "Strategy not found"}
        return {"status": "success", "id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.delete("/strategies/{strategy_id}")
async def crypto_pt_delete_strategy(strategy_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Mirrors api.py's simulator_pt_delete_strategy (api.py:6428-6456), including
    the cross-collection webhook cleanup that version does — now that
    crypto_simulator_webhooks exists (see the "Webhooks" section below) this is
    no longer reaching into a collection this router has no business touching;
    that was only true before crypto webhooks got their own collection. tv_alerts
    is still the *shared* collection on purpose (see the "New Positions" section
    comment above).
    """
    try:
        if not _find_owned_crypto_strategy(strategy_id, current_user):
            return {"status": "error", "message": "Strategy not found"}
        result = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].delete_one({"_id": ObjectId(strategy_id)})
        if result.deleted_count == 0:
            return {"status": "error", "message": "Strategy not found"}

        webhook_docs = list(_shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].find({"strategy_id": strategy_id}))
        if webhook_docs:
            _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].delete_many({"strategy_id": strategy_id})
            for webhook_doc in webhook_docs:
                _disable_tv_alerts_for_webhook(str(webhook_doc["_id"]), webhook_doc.get("user_id"))
        _shared_mongo._db["tv_alerts"].delete_many(
            {"webhook_strategy_id": strategy_id, "webhookEnabled": True},
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/strategies/{strategy_id}/execution-mode")
async def crypto_pt_set_execution_mode(strategy_id: str, body: PTExecutionModeIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_set_execution_mode (api.py:6844)."""
    try:
        doc = _find_owned_crypto_strategy(strategy_id, current_user)
        if not doc:
            return {"status": "error", "message": "Strategy not found"}
        mode = "advanced" if str(body.execution_mode or "").lower() == "advanced" else "regular"
        if mode == "advanced" and doc.get("execution_mode") != "advanced":
            user_id = _resolve_sim_user_id(current_user)
            slot_error = _crypto_sim_advanced_slot_limit_error(user_id)
            if slot_error:
                return {"status": "error", "message": slot_error}
        _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].update_one(
            {"_id": ObjectId(strategy_id)}, {"$set": {"execution_mode": mode}},
        )
        return {"status": "success", "execution_mode": mode}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ── Portfolios ───────────────────────────────────────────────────────────────

@router.get("/portfolios")
async def crypto_pt_list_portfolios(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_list_portfolios (api.py:4637)."""
    try:
        _ensure_default_crypto_simulator_portfolios()
        current_user_id = _resolve_sim_user_id(current_user)
        filt: dict[str, Any] = {"$or": [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]}
        docs = list(_shared_mongo._db[CRYPTO_PORTFOLIO_COLLECTION].find(filt, {"_id": 1, "name": 1}))
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return {"status": "success", "portfolios": docs}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/portfolios")
async def crypto_pt_create_portfolio(body: PTPortfolioIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_create_portfolio (api.py:4656)."""
    try:
        col = _shared_mongo._db[CRYPTO_PORTFOLIO_COLLECTION]
        current_user_id = _resolve_sim_user_id(current_user)
        existing = col.find_one(
            {"name": body.name, "$or": [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]},
            {"_id": 1},
        )
        if existing:
            return {"status": "success", "id": str(existing["_id"]), "created": False}
        result = col.insert_one({
            "name": body.name,
            "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            "user_id": current_user_id,
        })
        return {"status": "success", "id": str(result.inserted_id), "created": True}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ── Advanced-slot usage ─────────────────────────────────────────────────────

@router.get("/advanced-slot-usage")
async def crypto_pt_advanced_slot_usage(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_advanced_slot_usage (api.py:6769), crypto_ collection."""
    try:
        user_id = _resolve_sim_user_id(current_user)
        _, total = _crypto_sim_resolve_plan_and_advanced_slots(user_id)
        strategies = _crypto_sim_advanced_strategies(user_id)
        return {"status": "success", "used": len(strategies), "total": total, "strategies": strategies}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ── New Positions (Webhook Strategies) ──────────────────────────────────────
# webhook_id on each row now points into crypto_simulator_webhooks (see the
# "Webhooks" section below) — fully isolated from the NSE simulator_webhooks
# collection, same as everything else in this file. _disable_tv_alerts_for_webhook
# still touches the *shared* tv_alerts collection on purpose: a TradingView alert
# doc isn't instrument-family-specific storage, it's a single cross-cutting
# "who owns this webhook id" cleanup shared by both NSE and crypto (see that
# function's own docstring in api.py).

@router.get("/new-positions")
async def crypto_pt_list_new_positions(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_list_new_positions (api.py:5207-5292), crypto_ collections."""
    try:
        current_user_id = current_user.get("_id")
        conditions: list[dict[str, Any]] = []
        if current_user_id is not None:
            conditions.append({"$or": [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]})
        now_ist = datetime.now(IST)
        today_start = now_ist.strftime("%Y-%m-%dT00:00:00")
        tomorrow_start = (now_ist + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        conditions.append({"$or": [
            {"status": {"$ne": 2}},
            {"triggered_at": {"$gte": today_start, "$lt": tomorrow_start}},
        ]})
        filt: dict[str, Any] = {"$and": conditions} if conditions else {}
        docs = list(_shared_mongo._db[CRYPTO_NEW_POSITIONS_COLLECTION].find(filt).sort("created_at", -1))

        referenced_ids = [d["webhook_id"] for d in docs if d.get("webhook_id")]
        existing_webhooks: dict[str, dict] = {}
        if referenced_ids:
            existing_webhooks = {
                str(w["_id"]): w
                for w in _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].find(
                    {"_id": {"$in": [ObjectId(wid) for wid in referenced_ids]}},
                )
            }
        orphaned_ids = [
            d["_id"] for d in docs
            if d.get("webhook_id") and str(d["webhook_id"]) not in existing_webhooks
        ]
        if orphaned_ids:
            _shared_mongo._db[CRYPTO_NEW_POSITIONS_COLLECTION].delete_many({"_id": {"$in": orphaned_ids}})
            docs = [d for d in docs if d["_id"] not in orphaned_ids]

        stale_webhooks = {wid: w for wid, w in existing_webhooks.items() if w.get("status") == 2}
        result = []
        for doc in docs:
            item = {
                "id": str(doc["_id"]),
                "webhook_id": doc.get("webhook_id"),
                "portfolio_name": doc.get("portfolio_name"),
                "strategy_name": doc.get("strategy_name"),
                "instrument": doc.get("instrument"),
                "positions": doc.get("positions") or [],
                "trade_status": doc.get("trade_status"),
                "broker_id": doc.get("broker_id"),
                "status": doc.get("status", 1),
                "resulting_strategy_id": doc.get("resulting_strategy_id"),
                "created_at": doc.get("created_at"),
                "triggered_at": doc.get("triggered_at"),
            }
            stale = stale_webhooks.get(str(doc.get("webhook_id")))
            if item["status"] != 2 and stale:
                strategy_doc = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].find_one(
                    {"portfolio_name": doc.get("portfolio_name"), "strategy_name": doc.get("strategy_name")},
                    sort=[("saved_at", -1)],
                )
                resulting_strategy_id = str(strategy_doc["_id"]) if strategy_doc else None
                item["status"] = 2
                item["resulting_strategy_id"] = resulting_strategy_id
                item["triggered_at"] = item["triggered_at"] or datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
            result.append(item)
        return {"status": "success", "new_positions": result}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.delete("/new-positions/{position_id}")
async def crypto_pt_delete_new_position(position_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_delete_new_position (api.py:5295-5330), crypto_ collection."""
    try:
        try:
            doc_id = ObjectId(position_id)
        except Exception:
            return {"status": "error", "message": "Invalid position id"}
        current_user_id = current_user.get("_id")
        filt: dict[str, Any] = {"_id": doc_id}
        if current_user_id is not None:
            filt["$or"] = [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]
        doc = _shared_mongo._db[CRYPTO_NEW_POSITIONS_COLLECTION].find_one(filt)
        if not doc:
            return {"status": "error", "message": "Webhook not found"}
        if doc.get("status") == 2:
            return {"status": "error", "message": "This webhook already fired — delete its strategy instead."}
        webhook_id = doc.get("webhook_id")
        if webhook_id:
            try:
                _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].delete_one({"_id": ObjectId(webhook_id)})
            except Exception:
                pass
            _disable_tv_alerts_for_webhook(webhook_id, doc.get("user_id"))
        _shared_mongo._db[CRYPTO_NEW_POSITIONS_COLLECTION].delete_one({"_id": doc_id})
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ── Adjustments ──────────────────────────────────────────────────────────────
# Line-for-line mirrors of the NSE handlers in api.py, pointed at
# crypto_simulator_adjustments instead of simulator_adjustments. See the
# CRYPTO_ADJUSTMENTS_COLLECTION comment above for the risk-monitor caveat:
# these rows are not yet evaluated/fired by anything — plain CRUD only.

@router.get("/adjustments")
async def crypto_pt_list_adjustments(
    broker_id: Optional[str] = Query(default=None),
    underlying: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """Mirrors api.py's simulator_pt_list_adjustments (api.py:4814-4839), crypto_ collection."""
    try:
        query = {"strategy_id": strategy_id} if strategy_id else {"broker_id": broker_id, "underlying": underlying}
        # Only the live, armed config — a fired/disabled doc is history, not something
        # to restore into the "🔔 Alert" editor (see PTAdjustmentIn.status).
        query["status"] = {"$ne": False}
        docs = list(_shared_mongo._db[CRYPTO_ADJUSTMENTS_COLLECTION].find(query).sort("updated_at", -1))
        for d in docs:
            d["_id"] = str(d["_id"])
        return {"status": "success", "adjustments": docs}
    except Exception as exc:
        return {"status": "error", "message": str(exc), "adjustments": []}


@router.post("/adjustments")
async def crypto_pt_create_adjustment(body: PTAdjustmentIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_create_adjustment (api.py:4842-4852), crypto_ collection."""
    try:
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        doc = body.model_dump()
        doc["created_at"] = now_str
        doc["updated_at"] = now_str
        result = _shared_mongo._db[CRYPTO_ADJUSTMENTS_COLLECTION].insert_one(doc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.patch("/adjustments/{adjustment_id}")
async def crypto_pt_update_adjustment(adjustment_id: str, body: PTAdjustmentPatchIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_update_adjustment (api.py:4855-4875), crypto_ collection."""
    try:
        update: dict = {"updated_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")}
        update["positions"] = [p.model_dump() for p in body.positions]
        # Editing and re-saving re-arms it — same record gets updated in place rather
        # than a new one created (see crypto_pt_create_adjustment/PTAdjustmentIn.status).
        update["status"] = True
        # Clears a stale failure from a previous webhook fire — otherwise re-saving after
        # fixing whatever caused it would still show the old error forever, since only the
        # next actual fire attempt would ever overwrite this field again.
        update["webhook_error"] = None
        if body.trigger_price is not None:
            update["trigger_price"] = body.trigger_price
        if body.trigger_condition is not None:
            update["trigger_condition"] = body.trigger_condition
        _shared_mongo._db[CRYPTO_ADJUSTMENTS_COLLECTION].update_one({"_id": ObjectId(adjustment_id)}, {"$set": update})
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.delete("/adjustments")
async def crypto_pt_delete_adjustment(
    trigger_condition: str = Query(...),
    broker_id: Optional[str] = Query(default=None),
    underlying: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """Mirrors api.py's simulator_pt_delete_adjustment (api.py:4915-4938), crypto_ collection."""
    try:
        query: dict = {"trigger_condition": trigger_condition}
        query.update({"strategy_id": strategy_id} if strategy_id else {"broker_id": broker_id, "underlying": underlying})
        result = _shared_mongo._db[CRYPTO_ADJUSTMENTS_COLLECTION].delete_many(query)
        return {"status": "success", "deleted": result.deleted_count}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ── Triggers / Alert config (Position Configuration panel) ──────────────────
@router.post("/triggers")
async def crypto_pt_save_trigger(body: PTTriggerIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_save_trigger (api.py:4696-4727), crypto_ collection."""
    try:
        col = _shared_mongo._db[CRYPTO_TRIGGERS_COLLECTION]
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        col.update_one(
            {"broker_id": body.broker_id, "leg_id": body.leg_id},
            {
                "$set": {
                    "underlying": body.underlying, "expiry": body.expiry, "strike": body.strike,
                    "option_type": body.option_type, "side": body.side,
                    "sl_mode": body.sl_mode, "sl_value": body.sl_value,
                    "tp_mode": body.tp_mode, "tp_value": body.tp_value,
                    "entry_price_at_set": body.entry_price, "quantity_at_set": body.quantity,
                    "exited_at_set": body.exited,
                    "status": "active", "updated_at": now_str,
                },
                "$setOnInsert": {"created_at": now_str},
            },
            upsert=True,
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/alert-config")
async def crypto_pt_get_alert_config(broker_id: str = Query(...), underlying: str = Query(...), current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_get_alert_config (api.py:4765-4788), crypto_ collection."""
    try:
        doc = _shared_mongo._db[CRYPTO_PORTFOLIO_TRIGGERS_COLLECTION].find_one(
            {"broker_id": broker_id, "underlying": underlying},
        ) or {}
        return {
            "status": "success",
            "trading_mode": doc.get("alert_trading_mode") or "auto",
            "stoploss": doc.get("alert_stoploss") or {},
            "target": doc.get("alert_target") or {},
            "trailing_stop": doc.get("alert_trailing_stop") or {},
            "hedge_strike_type": doc.get("alert_hedge_strike_type") or {},
            "hedge_time_control": doc.get("alert_hedge_time_control") or {},
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/alert-config")
async def crypto_pt_save_alert_config(body: PTAlertConfigIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_save_alert_config (api.py:4791+), crypto_ collection."""
    try:
        col = _shared_mongo._db[CRYPTO_PORTFOLIO_TRIGGERS_COLLECTION]
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        snapshot = sorted(
            ({"leg_id": s.leg_id, "quantity": s.quantity, "entry_price": s.entry_price, "side": s.side} for s in body.legs_snapshot),
            key=lambda s: s["leg_id"],
        )
        col.update_one(
            {"broker_id": body.broker_id, "underlying": body.underlying},
            {
                "$set": {
                    "alert_trading_mode": body.trading_mode,
                    "alert_stoploss": body.stoploss.model_dump(),
                    "alert_target": body.target.model_dump(),
                    "alert_trailing_stop": body.trailing_stop.model_dump(),
                    "alert_hedge_strike_type": body.hedge_strike_type.model_dump(),
                    "alert_hedge_time_control": body.hedge_time_control.model_dump(),
                    "legs_snapshot": snapshot,
                    "status": "active", "updated_at": now_str,
                },
                "$setOnInsert": {"created_at": now_str},
            },
            upsert=True,
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ── Webhooks ─────────────────────────────────────────────────────────────────
# Crypto mirrors of api.py's TradingView-webhook handlers (simulator_pt_create_
# new_strategy_webhook, simulator_pt_create_update_strategy_webhook,
# simulator_pt_list_strategy_webhooks, simulator_pt_delete_webhook,
# simulator_pt_webhook_usage, simulator_pt_create_strategy_webhook_usage,
# _simulator_pt_webhook_trigger — api.py:4978-5778 / 6560-6837), pointed at
# crypto_simulator_webhooks/crypto_simulator_strategy/crypto_simulator_new_positions
# instead of their NSE counterparts. NOT mirrored: api.py's plain
# simulator_pt_create_webhook (POST /simulator/paper-trade/webhooks, the STOPLOSS
# chip's per-adjustment webhook for a *live-broker-view* adjustment keyed by
# broker_id/underlying) — that's out of scope for this pass; nothing here ever
# creates a crypto_simulator_webhooks doc with adjustment_id set and strategy_id
# unset, so the trigger route below only needs to actually handle the two shapes
# the endpoints in this section do produce (see _crypto_simulator_pt_webhook_trigger).


def _crypto_sim_active_webhook_strategy_count(user_id: Any) -> int:
    """Mirrors api.py's _sim_active_webhook_strategy_count (api.py:6590), crypto_ collection."""
    _ensure_crypto_simulator_strategy_index()
    return _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].count_documents({
        "$or": [{"user_id": user_id}, {"user_id": {"$exists": False}}],
        "execute_status": "webhook",
        "all_exited": {"$ne": True},
        "status": {"$ne": 2},
    })


def _crypto_sim_create_strategy_webhook_limit_error(user_id: Any) -> Optional[str]:
    """Mirrors api.py's _sim_create_strategy_webhook_limit_error (api.py:6609)."""
    plan, _ = _crypto_sim_resolve_plan_and_advanced_slots(user_id)
    if plan.get("create_strategy_webhook_mode") != "enabled":
        return f"Creating strategies via webhook isn't available on your {plan.get('plan_name') or 'current'} plan. Upgrade to unlock this feature."
    limit = int(plan.get("create_strategy_webhook_limit") or 0)
    if limit == -1:
        return None
    used = _crypto_sim_active_webhook_strategy_count(user_id)
    if used >= limit:
        plan_name = plan.get("plan_name") or "current"
        noun = "strategy" if limit == 1 else "strategies"
        return f"Your {plan_name} plan allows up to {limit} active {noun} created via webhook ({used}/{limit} used). Close an existing one or upgrade for more."
    return None


def _crypto_sim_webhook_url_limit_error(current_user: dict, strategy_id: Optional[str]) -> Optional[str]:
    """Mirrors api.py's _sim_webhook_url_limit_error (api.py:6560), crypto_ collection."""
    user_id = _resolve_sim_user_id(current_user)
    plan, _ = _crypto_sim_resolve_plan_and_advanced_slots(user_id)
    limit = int(plan.get("webhook_url_limit") or 0)
    if limit == -1:
        return None
    webhooks_col = _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION]
    if strategy_id:
        used = webhooks_col.count_documents({"strategy_id": strategy_id, "adjustment_id": None, "status": 1})
    else:
        used = webhooks_col.count_documents({"strategy_id": None, "user_id": current_user.get("_id"), "status": 1})
    if used >= limit:
        plan_name = plan.get("plan_name") or "current"
        noun = "webhook URL" if limit == 1 else "webhook URLs"
        return f"Your {plan_name} plan allows up to {limit} active {noun} per strategy ({used}/{limit} used). Remove an existing one or upgrade for more."
    return None


@router.post("/webhooks")
async def crypto_pt_create_webhook(body: PTWebhookIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Mirrors api.py's simulator_pt_create_webhook (api.py:4960-4994) — the payoff graph's
    per-side "🔔 Alert" webhook icon (CryptoTradeNew.tsx's generateWebhook), for BOTH a
    saved strategy's adjustment AND a live-broker-view one (strategy_id None, adjustment
    keyed by broker_id/underlying instead — see PTAdjustmentIn). This endpoint itself was
    the one deliberate gap left when the rest of the crypto webhook system was built (see
    this section's earlier "NOT mirrored" comment) — CryptoTradeNew.tsx's generateWebhook
    was calling NSE's own /simulator/paper-trade/webhooks instead, which saved a
    simulator_webhooks doc that NSE's own trigger route would then try to fire against
    simulator_adjustments/simulator_strategy — the wrong collections entirely for a
    crypto adjustment id, so it silently never worked. Idempotent, same as NSE: re-clicking
    the icon for the same (strategy_id, adjustment_id) pair returns the existing webhook id
    instead of creating a duplicate row.
    """
    try:
        existing = _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].find_one(
            {"strategy_id": body.strategy_id, "adjustment_id": body.adjustment_id},
        )
        if existing:
            return {"status": "success", "id": str(existing["_id"])}
        limit_error = _crypto_sim_webhook_url_limit_error(current_user, body.strategy_id)
        if limit_error:
            return {"status": "error", "message": limit_error}
        doc = {
            "strategy_id": body.strategy_id,
            "adjustment_id": body.adjustment_id,
            # Needed at trigger time for the live-adjustment path's plan check — the
            # saved-strategy path instead reads user_id off the strategy doc itself, same
            # split NSE's simulator_pt_create_webhook uses.
            "user_id": current_user.get("_id"),
            "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            "status": 1,
        }
        result = _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].insert_one(doc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/webhooks/new-strategy")
async def crypto_pt_create_new_strategy_webhook(body: PTNewStrategyWebhookIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_create_new_strategy_webhook (api.py:4978-5051)."""
    trade_status = (body.trade_status or "").strip().lower()
    if trade_status not in ("paper", "live"):
        return {"status": "error", "message": "trade_status must be 'paper' or 'live'."}
    if trade_status == "live" and not str(body.broker_id or "").strip():
        return {"status": "error", "message": "broker_id is required for a live webhook."}
    if not (body.positions or []):
        return {"status": "error", "message": "No positions to generate a webhook for."}
    create_limit_error = _crypto_sim_create_strategy_webhook_limit_error(_resolve_sim_user_id(current_user))
    if create_limit_error:
        return {"status": "error", "message": create_limit_error}
    try:
        doc = {
            "strategy_id": None,
            "adjustment_id": None,
            "trade_status": trade_status,
            "broker_id": str(body.broker_id).strip() if trade_status == "live" else None,
            "user_id": current_user.get("_id"),
            "portfolio_name": body.portfolio_name,
            "strategy_name": body.strategy_name,
            "instrument": body.instrument or "nifty",
            "spot_price": body.spot_price,
            "config": body.config or {},
            "positions": [p.dict() for p in body.positions],
            "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            "status": 1,
        }
        result = _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].insert_one(doc)
        try:
            # Mirror row for the "Webhook Strategies" / New Positions page — see
            # CRYPTO_NEW_POSITIONS_COLLECTION comment near the top of this file.
            _shared_mongo._db[CRYPTO_NEW_POSITIONS_COLLECTION].insert_one({
                "webhook_id": str(result.inserted_id),
                "portfolio_name": doc["portfolio_name"],
                "strategy_name": doc["strategy_name"],
                "instrument": doc["instrument"],
                "positions": doc["positions"],
                "trade_status": doc["trade_status"],
                "broker_id": doc["broker_id"],
                "user_id": doc["user_id"],
                "created_at": doc["created_at"],
                "status": 1,
                "resulting_strategy_id": None,
                "triggered_at": None,
            })
        except Exception as mirror_exc:
            log.error("crypto_simulator_new_positions mirror insert failed for webhook %s: %s", result.inserted_id, mirror_exc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/webhooks/update-strategy/{strategy_id}")
async def crypto_pt_create_update_strategy_webhook(
    strategy_id: str,
    body: PTUpdateStrategyWebhookIn,
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """Mirrors api.py's simulator_pt_create_update_strategy_webhook (api.py:5054-5093)."""
    trade_status = (body.trade_status or "paper").strip().lower()
    if trade_status not in ("paper", "live"):
        return {"status": "error", "message": "trade_status must be 'paper' or 'live'."}
    if not str(strategy_id or "").strip():
        return {"status": "error", "message": "strategy_id is required."}
    if not (body.positions or []):
        return {"status": "error", "message": "No positions to generate a webhook for."}
    try:
        if not _find_owned_crypto_strategy(strategy_id, current_user):
            return {"status": "error", "message": "Strategy not found."}
        limit_error = _crypto_sim_webhook_url_limit_error(current_user, strategy_id)
        if limit_error:
            return {"status": "error", "message": limit_error}
        doc = {
            "strategy_id":  strategy_id,
            "adjustment_id": None,
            "trade_status": trade_status,
            "broker_id":    str(body.broker_id).strip() if trade_status == "live" and body.broker_id else None,
            "user_id":      current_user.get("_id"),
            "positions":    [p.dict() for p in body.positions],
            "created_at":   datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            "status":       1,
        }
        result = _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].insert_one(doc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/webhooks")
async def crypto_pt_list_strategy_webhooks(
    strategy_id: Optional[str] = None,
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """Mirrors api.py's simulator_pt_list_strategy_webhooks (api.py:5096-5136)."""
    try:
        if strategy_id:
            filt: dict[str, Any] = {"strategy_id": strategy_id, "adjustment_id": None}
        else:
            filt = {"strategy_id": None, "adjustment_id": None}
            current_user_id = current_user.get("_id")
            if current_user_id is not None:
                filt["$or"] = [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]
        docs = list(
            _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION]
            .find(filt)
            .sort("created_at", 1)
        )
        webhooks = [
            {"id": str(d["_id"]), "positions": d.get("positions") or [], "status": d.get("status", 1)}
            for d in docs
        ]
        return {"status": "success", "webhooks": webhooks}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.delete("/webhooks/{webhook_id}")
async def crypto_pt_delete_webhook(webhook_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_delete_webhook (api.py:5176-5203)."""
    try:
        try:
            doc_id = ObjectId(webhook_id)
        except Exception:
            return {"status": "error", "message": "Invalid webhook id"}
        current_user_id = current_user.get("_id")
        filt: dict[str, Any] = {"_id": doc_id}
        if current_user_id is not None:
            filt["$or"] = [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]
        doc = _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].find_one(filt)
        if not doc:
            return {"status": "error", "message": "Webhook not found"}
        _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].delete_one({"_id": doc_id})
        _shared_mongo._db[CRYPTO_NEW_POSITIONS_COLLECTION].delete_one({"webhook_id": webhook_id})
        _disable_tv_alerts_for_webhook(webhook_id, doc.get("user_id"))
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/webhook-usage")
async def crypto_pt_webhook_usage(strategy_id: Optional[str] = None, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_webhook_usage (api.py:6786-6817), crypto_ collection."""
    try:
        user_id = _resolve_sim_user_id(current_user)
        plan, _ = _crypto_sim_resolve_plan_and_advanced_slots(user_id)
        limit = int(plan.get("webhook_url_limit") or 0)
        webhooks_col = _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION]
        if strategy_id:
            used = webhooks_col.count_documents({"strategy_id": strategy_id, "adjustment_id": None, "status": 1})
        else:
            used = webhooks_col.count_documents({
                "strategy_id": None,
                "user_id": current_user.get("_id"),
                "status": 1,
            })
        return {"status": "success", "used": used, "total": limit}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/create-strategy-webhook-usage")
async def crypto_pt_create_strategy_webhook_usage(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """Mirrors api.py's simulator_pt_create_strategy_webhook_usage (api.py:6820-6837)."""
    try:
        user_id = _resolve_sim_user_id(current_user)
        plan, _ = _crypto_sim_resolve_plan_and_advanced_slots(user_id)
        mode = plan.get("create_strategy_webhook_mode", "disabled")
        limit = int(plan.get("create_strategy_webhook_limit") or 0)
        used = _crypto_sim_active_webhook_strategy_count(user_id)
        return {"status": "success", "mode": mode, "used": used, "total": limit}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _crypto_webhook_create_strategy(webhook_doc: dict) -> dict:
    """
    Mirrors api.py's _simulator_pt_webhook_create_strategy (api.py:5333-5496) — what
    hitting a "new strategy" crypto webhook URL actually does — with one deliberate
    omission: the paper branch there re-quotes every leg's entry_price via
    _prefetch_dhan_quotes_for_legs/_resolve_mpp_price/_resolve_ltp_price right before
    saving, so a webhook that sits unfired for hours doesn't save a stale snapshot
    price. Those three helpers resolve a Dhan securityId off the NSE F&O contract
    master (see _resolve_dhan_security in api.py) — there is no Delta Exchange/crypto
    equivalent wired up, so calling them here would just fail to resolve every leg and
    silently no-op (harmless, but pointless — and not "mirroring", it'd be dead code
    imported from api.py for no benefit). So: the paper branch below saves each leg's
    entry_price/entry_time exactly as captured when the webhook URL was generated,
    same as every other "no live requote" webhook doc field.

    The live branch below calls _place_crypto_manual_order_via_order_service (the
    crypto-specific gateway), NOT api.py's NSE _place_manual_order_via_order_service —
    this used to reuse that NSE proxy on the theory that it was generic, but it only
    recognizes Dhan/FlatTrade/Kite broker_configuration docs and rejects everything
    else, so every live crypto webhook strategy created through this branch was
    silently failing to place its real order at that exact last step. See
    _place_crypto_manual_order_via_order_service's own docstring, and this function's
    order-construction comment below for why order_type is always MARKET here.
    """
    positions = webhook_doc.get("positions") or []
    trade_status = str(webhook_doc.get("trade_status") or "paper")
    instrument = webhook_doc.get("instrument") or "nifty"
    for p in positions:
        p["order_type"] = "mpp" if str(p.get("order_type") or "").strip().upper() == "MARKET" else "ltp"
    extra_fields: dict[str, Any] = {"trade_status": trade_status, "execute_status": "webhook", "execution_mode": "advanced"}

    user_id = str(webhook_doc["user_id"]) if webhook_doc.get("user_id") else _SIM_DEFAULT_USER_ID
    create_limit_error = _crypto_sim_create_strategy_webhook_limit_error(user_id)
    if create_limit_error:
        return {"status": "error", "message": create_limit_error}
    slot_error = _crypto_sim_advanced_slot_limit_error(user_id)
    if slot_error:
        return {"status": "error", "message": slot_error}
    limit_error = _crypto_sim_active_strategy_limit_error(user_id)
    if limit_error:
        return {"status": "error", "message": limit_error}

    if trade_status == "live":
        broker_id = str(webhook_doc.get("broker_id") or "")
        if not broker_id:
            return {"status": "error", "message": "Webhook has no broker configured."}
        open_positions = [p for p in positions if not p.get("exited")]
        # Plain dicts shaped like CryptoOrderLeg (algo.order/crypto_order_router.py), NOT
        # ManualOrderLeg — the NSE Pydantic model this used to build. _place_manual_order_
        # via_order_service (the NSE gateway) only recognizes Dhan/FlatTrade/Kite
        # broker_configuration docs and rejects everything else, so every live crypto
        # webhook strategy created via this branch was silently failing to place its real
        # order at this exact step — see _place_crypto_manual_order_via_order_service's
        # own docstring for why the crypto-specific gateway had to be built, and
        # _crypto_webhook_fire_live_adjustment below (already correct) for the pattern
        # this now mirrors.
        #
        # order_type="MARKET" unconditionally, not the mpp/ltp choice the webhook payload
        # carries — same reasoning _crypto_webhook_fire_live_adjustment's own order-
        # construction comment gives: DeltaExchangeAdapter's _ORDER_TYPE_TO_DELTA only
        # recognizes LIMIT/MARKET/SL/SL-M, so an unrecognized "MPP"/"LTP" value silently
        # fell back to a LIMIT order at price=0.0 (there's no live requote step here to
        # safely price a LIMIT order, unlike NSE's MPP/LTP resolution) — the exact
        # badly-priced-live-order failure mode that comment warns about. MARKET is the
        # safe, real-fill choice until a genuine Delta depth/LTP requote path exists.
        orders = [
            {
                "underlying": instrument,
                "expiry": str(p.get("expiry") or ""),
                "strike": float(p.get("strike") or 0),
                "option_type": _normalize_pt_option_type(str(p.get("option_type") or p.get("type") or "")),
                "side": "SELL" if str(p.get("type") or "").strip().lower().startswith("s") else "BUY",
                "quantity": int((p.get("lots") or 1) * (p.get("lot_size") or 1)),
                "order_type": "MARKET",
                "price": 0.0,
                "trigger_price": 0.0,
                "leg_id": str(p.get("leg_id") or p.get("token") or ""),
            }
            for p in open_positions
        ]
        if not orders:
            return {"status": "error", "message": "No open legs to trade."}

        order_result = await _place_crypto_manual_order_via_order_service(broker_id, orders)
        if order_result.get("status") not in ("success", "partial"):
            return {"status": "error", "message": order_result.get("message") or "Order placement failed.", "results": order_result.get("results")}
        extra_fields["broker_id"] = broker_id
        extra_fields["order_results"] = order_result.get("results")
        fire_time = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        for leg_result, p in zip(order_result.get("results") or [], open_positions):
            if leg_result.get("status") == "success" and leg_result.get("price"):
                p["entry_price"] = round(float(leg_result["price"]), 2)
                p["entry_time"] = fire_time
    # else: paper — see this function's docstring for why there's no requote step here.

    try:
        strategy_name = webhook_doc.get("strategy_name") or "Webhook Strategy"
        strategy_id = _insert_crypto_simulator_strategy(
            webhook_doc.get("portfolio_name") or "Running Trades",
            strategy_name,
            instrument,
            webhook_doc.get("spot_price"),
            webhook_doc.get("config"),
            positions,
            "live",
            extra_fields,
            user_id=user_id,
        )
        open_count = len([p for p in positions if not p.get("exited")])
        from features.telegram_notifier import notify_user_for
        notify_user_for(
            webhook_doc.get("user_id"),
            "WEBHOOK_STRATEGY_EXECUTED",
            f'"{strategy_name}" went {trade_status} via crypto webhook — {open_count} leg(s) on {instrument.upper()}.',
            {"strategy_id": strategy_id, "trade_status": trade_status},
        )
        try:
            _shared_mongo._db[CRYPTO_NEW_POSITIONS_COLLECTION].update_one(
                {"webhook_id": str(webhook_doc["_id"])},
                {"$set": {
                    "status": 2,
                    "resulting_strategy_id": strategy_id,
                    "triggered_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
                }},
            )
        except Exception as mirror_exc:
            log.error("crypto_simulator_new_positions status update failed for webhook %s: %s", webhook_doc["_id"], mirror_exc)
        return {"status": "success", "strategy_id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _crypto_webhook_update_strategy(webhook_doc: dict) -> dict:
    """Mirrors api.py's _simulator_pt_webhook_update_strategy (api.py:5565-5612), crypto_ collection."""
    try:
        strategy_id = str(webhook_doc.get("strategy_id") or "")
        if not strategy_id:
            return {"status": "error", "message": "Webhook has no strategy_id."}

        raw_db = _shared_mongo._db
        doc = raw_db[CRYPTO_STRATEGY_COLLECTION].find_one({"_id": ObjectId(strategy_id)})
        if not doc:
            return {"status": "error", "message": "Strategy not found."}

        existing_positions = list(doc.get("positions") or [])
        new_positions      = [p if isinstance(p, dict) else p.dict() for p in (webhook_doc.get("positions") or [])]
        if not new_positions:
            return {"status": "error", "message": "No positions in webhook doc."}
        for p in new_positions:
            p["order_type"] = "mpp" if str(p.get("order_type") or "").strip().upper() == "MARKET" else "ltp"

        merged = _net_pt_positions(existing_positions, new_positions)
        raw_db[CRYPTO_STRATEGY_COLLECTION].update_one(
            {"_id": ObjectId(strategy_id)},
            {"$set": {
                "positions":  merged,
                "updated_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            }},
        )
        strategy_name = str(doc.get("strategy_name") or "Strategy")
        instrument    = str(doc.get("instrument") or "nifty").upper()
        open_count    = len([p for p in merged if not p.get("exited")])
        from features.telegram_notifier import notify_user_for
        notify_user_for(
            webhook_doc.get("user_id"),
            "WEBHOOK_STRATEGY_UPDATED",
            f'"{strategy_name}" updated via crypto webhook — {open_count} open leg(s) on {instrument}.',
            {"strategy_id": strategy_id},
        )
        return {"status": "success", "strategy_id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# Reuses the SAME LIVE_ORDER_PLACEMENT/LIVE_ORDER_BASE_URL toggle crypto_order_router.py's
# own /place-order already uses (see that file's module docstring) instead of a separate
# env var — false (this dev box's actual .env value) means algo.order is reachable
# directly on localhost, so route straight to it; true means only the real whitelisted
# box (LIVE_ORDER_BASE_URL, defaults to production) can actually reach Delta, so route
# there instead. Read once at import time like every other module-level env constant in
# this codebase — uvicorn --reload restarts the whole process (not just re-executes this
# one module) on any .py save, so this always reflects whatever's currently in .env by
# the time a request actually comes in, no separate cache-invalidation needed.
_CRYPTO_LIVE_ORDER_PLACEMENT = os.getenv("LIVE_ORDER_PLACEMENT", "false").strip().lower() == "true"
_CRYPTO_ORDER_SERVICE_URL = (
    os.getenv("LIVE_ORDER_BASE_URL", "https://order.finedgealgo.com/order")
    if _CRYPTO_LIVE_ORDER_PLACEMENT
    else "http://localhost:8004/order"
)
_CRYPTO_INTERNAL_HEADERS = {"X-Internal-Token": os.getenv("INTERNAL_SERVICE_TOKEN", "")}


async def _place_crypto_manual_order_via_order_service(broker_id: str, orders: list[dict]) -> dict:
    """
    Crypto twin of api.py's _place_manual_order_via_order_service (api.py:4405-4436) —
    calls algo.order's crypto-specific internal gateway (crypto_order_router.py's
    /internal/place-order, mounted under this same router's own /order/trade/crypto
    prefix — see that endpoint's own docstring for why it had to be built from scratch:
    the NSE proxy this mirrors only recognizes Dhan/FlatTrade/Kite broker_configuration
    docs and rejects everything else, so every "live" crypto webhook branch that reused
    it was silently failing at this exact last step). `orders` are plain dicts shaped
    like CryptoOrderLeg (underlying/expiry/strike/option_type/side/quantity/order_type/
    price/trigger_price/leg_id) — passed straight through as JSON, no Pydantic model
    imported across the algo.simulator/algo.order process boundary.
    """
    try:
        resp = await asyncio.to_thread(
            requests.post,
            f"{_CRYPTO_ORDER_SERVICE_URL}/trade/crypto/internal/place-order",
            json={"broker_id": broker_id, "orders": orders},
            headers=_CRYPTO_INTERNAL_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        log.error("[CRYPTO PLACE_ORDER] internal gateway call failed: %s", exc)
        return {"status": "error", "message": f"Order service call failed: {exc}", "results": []}


async def _crypto_webhook_fire_live_adjustment(webhook_doc: dict) -> dict:
    """
    Mirrors api.py's _simulator_pt_webhook_fire_live_adjustment (api.py:5634-5729),
    crypto_simulator_adjustments collection — fires a live-broker-view adjustment
    (PTAdjustmentIn keyed by broker_id/underlying, no strategy_id — see openAlertModal/
    savePortfolioTrigger in CryptoTradeNew.tsx) generated from the payoff graph's SL/
    Target "🔔 Alert" reverse-exit preview. Places a REAL opposite-side order on the
    broker the adjustment was created against, via the same generic
    _place_manual_order_via_order_service proxy _crypto_webhook_create_strategy's own
    live branch already uses — no NSE-specific logic in that call, so it needs no
    changes to work for a Delta Exchange broker_id.

    On any failure the raw error is written onto the adjustment doc as `webhook_error`
    instead of only ever reaching a webhook caller (TradingView/curl) with no way to
    surface it back to the user — the saved-SL marker on the payoff graph can read this
    to show what went wrong, same as NSE's equivalent.
    """
    adjustment_id = str(webhook_doc.get("adjustment_id") or "")
    adjustments_col = _shared_mongo._db[CRYPTO_ADJUSTMENTS_COLLECTION]
    try:
        adj_doc = adjustments_col.find_one({"_id": ObjectId(adjustment_id)})
    except Exception:
        return {"status": "error", "message": "Invalid adjustment id"}
    if not adj_doc:
        return {"status": "error", "message": "Adjustment not found"}
    if adj_doc.get("status") is False:
        return {"status": "error", "message": "Adjustment already fired or inactive"}

    if not str(adj_doc.get("broker_id") or ""):
        return {"status": "error", "message": "Adjustment has no broker configured"}
    # adj_doc.get("broker_id") is the internal literal "deltaExchange" (see
    # legToPositionPayload/openExistingPositionLegs[0].brokerId in CryptoTradeNew.tsx —
    # deliberately left as that stable internal token when the URL-facing broker_id was
    # switched to a real broker_configuration ObjectId, see delta_exchange_router.py's
    # _find_delta_broker_config_doc), not a real Mongo _id — resolve it to the actual
    # broker_configuration doc's own _id here, same as that function does, since that's
    # what _place_crypto_manual_order_via_order_service's _resolve_delta_adapter call
    # actually needs to find a real, credentialed account.
    from simulator.delta_exchange_router import _find_delta_broker_config_doc
    broker_cfg = _find_delta_broker_config_doc(_shared_mongo._db)
    if not broker_cfg:
        message = "Delta Exchange isn't connected — configure it in Broker Settings."
        adjustments_col.update_one({"_id": ObjectId(adjustment_id)}, {"$set": {"webhook_error": message}})
        return {"status": "error", "message": message}
    broker_id = str(broker_cfg["_id"])

    webhook_user_id = webhook_doc.get("user_id")
    plan, _ = _crypto_sim_resolve_plan_and_advanced_slots(str(webhook_user_id) if webhook_user_id else _SIM_DEFAULT_USER_ID)
    if plan.get("trade_generate_webhook_mode") != "enabled":
        message = f"Webhook Trading isn't available on your {plan.get('plan_name') or 'current'} plan."
        adjustments_col.update_one({"_id": ObjectId(adjustment_id)}, {"$set": {"webhook_error": message}})
        return {"status": "error", "message": message}

    open_positions = [p for p in (adj_doc.get("positions") or []) if not p.get("exited")]
    if not open_positions:
        message = "No open legs on this adjustment."
        adjustments_col.update_one({"_id": ObjectId(adjustment_id)}, {"$set": {"webhook_error": message}})
        return {"status": "error", "message": message}

    underlying = str(adj_doc.get("underlying") or "BTC")
    orders = [
        {
            "underlying": underlying,
            "expiry": str(p.get("expiry") or ""),
            "strike": float(p.get("strike") or 0),
            "option_type": _normalize_pt_option_type(str(p.get("option_type") or "")),
            "side": "SELL" if str(p.get("side") or "").strip().upper() == "S" else "BUY",
            "quantity": int(p.get("qty") or p.get("lots") or 1),
            # MARKET, always — NOT the "MPP"-unless-explicit-LTP/LIMIT/SL convention
            # _crypto_webhook_create_strategy's own live branch uses for NSE. Checked
            # DeltaExchangeAdapter.place_order directly: its _ORDER_TYPE_TO_DELTA map only
            # recognizes LIMIT/MARKET/SL/SL-M — an unrecognized value like "MPP" or "LTP"
            # silently falls back to "limit_order" (delta_exchange.py:265,
            # `_ORDER_TYPE_TO_DELTA.get(order_type, "limit_order")`), and this dict always
            # sends price=0.0 (there's no LTP-resolution step here the way NSE's MPP/LTP
            # branches have) — that combination would have placed a real LIMIT order at
            # price 0, the exact "badly-priced live order" NSE's own place-order docstring
            # warns MPP/LTP-with-price:0 produces. A stoploss-triggered exit firing via
            # webhook wants "fill me now" semantics anyway (same reasoning the Order Pad's
            # per-leg exit "X" icon defaults to Market for) — MARKET is both the safe choice
            # and the semantically correct one here, not a placeholder.
            "order_type": "MARKET",
            "price": 0.0,
            "trigger_price": 0.0,
            "leg_id": str(p.get("token") or p.get("leg_id") or ""),
        }
        for p in open_positions
    ]
    order_result = await _place_crypto_manual_order_via_order_service(broker_id, orders)
    now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    if order_result.get("status") not in ("success", "partial"):
        message = order_result.get("message") or "Order placement failed."
        adjustments_col.update_one(
            {"_id": ObjectId(adjustment_id)},
            {"$set": {"webhook_error": message, "webhook_error_results": order_result.get("results"), "webhook_error_at": now_str}},
        )
        return {"status": "error", "message": message, "results": order_result.get("results")}

    # Fired — same "flip to False, never delete" convention NSE's simulator_adjustments
    # uses, so a re-hit of the same URL correctly no-ops above instead of double-firing.
    adjustments_col.update_one(
        {"_id": ObjectId(adjustment_id)},
        {"$set": {
            "status": False,
            "webhook_error": None,
            "fired_at": now_str,
            "order_results": order_result.get("results"),
        }},
    )
    try:
        from features.telegram_notifier import notify_user_for
        notify_user_for(
            webhook_user_id,
            "WEBHOOK_ADJUSTMENT_FIRED",
            f'Live crypto webhook fired {len(orders)} leg(s) on {underlying.upper()}.',
            {"adjustment_id": adjustment_id, "broker_id": broker_id},
        )
    except Exception:
        pass
    return {"status": "success", "results": order_result.get("results")}


async def _crypto_simulator_pt_webhook_trigger(webhook_id: str) -> dict:
    """
    Crypto mirror of api.py's _simulator_pt_webhook_trigger (api.py:5713-5761), pointed
    at crypto_simulator_webhooks/crypto_simulator_strategy instead of their NSE
    counterparts. One branch is intentionally NOT mirrored — see the inline comment
    below at the strategy_id+adjustment_id case.
    """
    try:
        webhook_doc = _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].find_one({"_id": ObjectId(webhook_id)})
    except Exception:
        return {"status": "error", "message": "Invalid webhook id"}
    if not webhook_doc:
        return {"status": "error", "message": "Webhook not found"}
    current_status = webhook_doc.get("status")
    if current_status == 2:
        return {"status": "noop", "message": "Already triggered"}
    if current_status == 0:
        return {"status": "noop", "message": "Webhook is inactive"}

    if not webhook_doc.get("strategy_id") and not webhook_doc.get("adjustment_id"):
        result = await _crypto_webhook_create_strategy(webhook_doc)
    elif webhook_doc.get("strategy_id") and not webhook_doc.get("adjustment_id"):
        strategy_doc = _shared_mongo._db[CRYPTO_STRATEGY_COLLECTION].find_one(
            {"_id": ObjectId(webhook_doc["strategy_id"])}, {"user_id": 1, "execution_mode": 1},
        )
        strategy_user_id = (strategy_doc or {}).get("user_id") or webhook_doc.get("user_id")
        plan, _ = _crypto_sim_resolve_plan_and_advanced_slots(strategy_user_id)
        is_advanced = str((strategy_doc or {}).get("execution_mode") or "").lower() == "advanced"
        if plan.get("trade_generate_webhook_mode") != "enabled" or not is_advanced:
            return {
                "status": "error",
                "message": f"Webhook is available only for Advanced Strategies on a plan with Webhook Trading enabled — your current plan is {plan.get('plan_name') or 'unknown'}. Buy Additional Credit or upgrade your plan to continue.",
            }
        result = await _crypto_webhook_update_strategy(webhook_doc)
    elif webhook_doc.get("strategy_id") and webhook_doc.get("adjustment_id"):
        # NSE's equivalent of this branch forwards to SimulatorRiskMonitor.
        # force_fire_adjustment (simulator_risk_monitor.py:1525), which force-fires the
        # strategy's simulator_adjustments basket outside the normal price-band check.
        # That function is not a thin collection-name-swap away from working for crypto:
        # it hardcodes raw_db['simulator_adjustments']/raw_db['simulator_strategy']
        # (simulator_risk_monitor.py:1539/1548) AND reads self.registry.
        # paper_baskets_by_strategy, an in-memory registry the risk monitor's own
        # continuous tick loop populates only by scanning simulator_strategy — it has
        # never been taught to also watch crypto_simulator_strategy (see the
        # CRYPTO_ADJUSTMENTS_COLLECTION comment near the top of this file, which
        # documents the exact same gap for the non-webhook adjustment-firing path).
        # Reimplementing _fire_paper_adjustment's leg-matching/LTP-resolution logic
        # (simulator_risk_monitor.py, ~100 more lines, itself dependent on that same
        # registry) here would mean duplicating a stateful chunk of the shared risk
        # monitor rather than "extending a router" — exactly the kind of separately-
        # tracked, larger background-engine work this pass was told not to take on.
        # So: no crypto strategies ever get an execute_status where this branch would
        # be hit yet regardless (this router has no endpoint that attaches
        # adjustment_id to a strategy_id-bearing crypto webhook — see this section's
        # top-of-section comment), but if one ever existed, fail loud and clearly
        # instead of silently no-op'ing or crashing on a KeyError.
        return {
            "status": "error",
            "message": "Firing a strategy adjustment basket via webhook isn't supported for crypto strategies yet — it requires SimulatorRiskMonitor support for crypto_simulator_strategy/crypto_simulator_adjustments, which is separate background-engine work.",
        }
    else:
        # adjustment_id set, strategy_id unset — the live-broker-view adjustment shape
        # (PTWebhookIn, now created by crypto_pt_create_webhook above — see its own
        # docstring for the gap this closes).
        result = await _crypto_webhook_fire_live_adjustment(webhook_doc)

    if result.get("status") == "success":
        _shared_mongo._db[CRYPTO_WEBHOOKS_COLLECTION].update_one({"_id": webhook_doc["_id"]}, {"$set": {"status": 2}})
    return result


@router.get("/webhook/tv/alert/{webhook_id}")
@router.post("/webhook/tv/alert/{webhook_id}")
async def crypto_pt_webhook_trigger(webhook_id: str) -> dict:
    """
    Public, unauthenticated on purpose — same security model as api.py's
    simulator_pt_webhook_trigger (api.py:5764-5778): the unguessable Mongo id in the
    path IS the credential, since this is the URL a TradingView alert (or curl) hits
    directly and can't carry our app's JWT. Deliberately a SEPARATE route from the NSE
    one (not touched by this change) — registered here under this router's own
    "/simulator/crypto-paper-trade" prefix, so the full path is
    /simulator/crypto-paper-trade/webhook/tv/alert/{webhook_id}, distinct from NSE's
    /webhook/tv/alert/{webhook_id}.

    Unlike api.py's docstring claim that its version is unconditionally "paper-only...
    never calls a broker" — that's only true for its strategy_id+adjustment_id branch
    (force_fire_adjustment, which IS paper-only); its "new strategy" and "live-broker-
    view adjustment" branches both place real orders when trade_status=="live"/the
    adjustment has a broker_id. Same is true here: a trade_status=="live" new-strategy
    webhook places a real order via _place_manual_order_via_order_service (see
    _crypto_webhook_create_strategy). The one branch that's genuinely unreachable here
    is the basket-adjustment force-fire — see _crypto_simulator_pt_webhook_trigger's
    comment on that branch for why.
    """
    return await _crypto_simulator_pt_webhook_trigger(webhook_id)


# Shorter public-facing alias — /simulator/crypto/webhook/tv/alert/{id} instead of
# /simulator/crypto-paper-trade/webhook/tv/alert/{id} above. Registered directly on
# `app` (not through this file's own `router`, whose prefix is fixed for every route on
# it) and not through delta_exchange_router.py's /simulator/crypto-prefixed router
# either, even though that would read more naturally — that file is imported BY api.py
# (api.py:7949 `from simulator.delta_exchange_router import delta_exchange_router`), so
# it importing this file back (which itself does `from api import ...` above) would be a
# circular import. `app` is already fully built by the time this module loads (see
# simulator_main.py: `from api import app` runs before `from simulator.
# crypto_paper_trade_router import router`), so registering straight onto it here is
# import-safe. Same handler either way — this is just a shorter URL for whatever fires
# it (TradingView, curl); the /simulator/crypto-paper-trade/... path above keeps working
# too, so an already-configured TradingView alert using the longer path isn't broken.
@app.get("/simulator/crypto/webhook/tv/alert/{webhook_id}")
@app.post("/simulator/crypto/webhook/tv/alert/{webhook_id}")
async def crypto_webhook_trigger_short_alias(webhook_id: str) -> dict:
    return await _crypto_simulator_pt_webhook_trigger(webhook_id)
