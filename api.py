"""
Local Backtest API
──────────────────
Run:
    uvicorn api:app --reload --port 8001

Endpoints:
    GET  /health                    → health check
    POST /backtest                  → run backtest (blocking, waits for result)
    POST /backtest/file             → run backtest using current_backtest_request.json
    POST /backtest/start            → start backtest in background, returns job_id
    GET  /backtest/status/{job_id}  → poll progress: completed_days / total_days
    GET  /backtest/result/{job_id}  → get final result when status=done
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import multiprocessing
import os
import re
import threading

import pathlib as _pathlib
from dotenv import load_dotenv
load_dotenv(_pathlib.Path(__file__).resolve().parent / ".env")

log = logging.getLogger(__name__)
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional

from bson import ObjectId
from fastapi import FastAPI, HTTPException, APIRouter, Query, Request, UploadFile, File, Depends
from fastapi.routing import APIRoute
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.routing import WebSocketRoute

from features.backtest_engine import run_backtest
from features.portfolio_worker import strategy_worker
from features.mongo_data     import MongoData
from features.expiry_config  import seed_expiry_config
from features import auth as app_auth
from features.broker_gateway import (
    broker_get_login_url            as get_login_url,
    broker_generate_session         as generate_session,
    get_broker_rest_client_with_token as get_kite_instance,
    save_broker_session             as save_kite_session,
    get_stored_broker_access_token  as get_stored_access_token,
    broker_ticker_manager           as ticker_manager,
)
from features.mock_ticker import mock_ticker_manager
from simulator_risk_monitor import simulator_risk_monitor
from simulator.api_server import router as simulator_router
from simulator.models import MiniStrangleRequest
from simulator.monitor_service import get_simulator_monitor_service
from simulator.monitor_ui import build_monitor_toggle_page
from simulator.strategy_monitor_bridge import (
    reentry_status as simulator_bridge_reentry_status,
    start as simulator_bridge_start,
    status as simulator_bridge_status,
    stop as simulator_bridge_stop,
)
from simulator.strategy_engine import StrategyEngine
from simulator.streaming_controller import StreamingController
from simulator.zerodha_broker import ZerodhaBroker as SimulatorZerodhaBroker
from features.broker_gateway import (
    load_broker_instruments         as _load_kite_instruments,
    BROKER_INDEX_TOKENS             as KITE_INDEX_TOKENS,
    get_broker_expiries             as get_kite_expiries,
    list_broker_option_contracts    as list_kite_option_contracts,
    get_broker_credentials          as get_common_credentials,
    get_broker_ltp_map              as get_ltp_map,
    broker_is_configured            as is_configured,
    load_broker_credentials_from_db as load_credentials_from_db,
)
from features.spot_atm_utils import get_cached_spot_doc
from features.execution_socket import (
    broadcast_backtest_simulation_step,
    emit_broker_settings_for_user,
    queue_execute_order_group_start,
    run_backtest_simulation_step,
    socket_router,
    _fetch_dhan_broker_option_positions,
    _build_message,
    _extract_broker_configuration_label,
)
from features.live_fast_monitor import live_fast_monitor_supervisor
from features.live_monitor_socket import live_monitor_loop
from features import live_entry_monitor
from features.broker_accounts import (
    validate_broker_configuration_session as _validate_broker_configuration_session,
    DEFAULT_APP_USER_ID,
    get_broker_accounts_for_user,
)
from features.mock_kite_socket import mock_kite_socket_router
from features.live_quote_socket import live_quote_socket_router

# ─── Config ───────────────────────────────────────────────────────────────────

REQUEST_JSON_PATH = Path(__file__).parent / "current_backtest_request.json"
SAMPLE_RESULT_PATH = Path(__file__).parent / "sample_backtest_result" / "new_portfolio_result.json"
JOB_STATE_DIR = Path("/tmp/option_algo_backtest_jobs")
CACHE_DIR = Path("/tmp/option_algo_backtest_cache")
API_ROUTE_GROUP_PREFIXES = ("/algo", "/simulator", "/scanner")
API_VERSION_PREFIXES = tuple(
    f"/{segment}"
    for segment in [
        str(value).strip().strip("/")
        for value in os.getenv("API_ROUTE_VERSIONS", "v1,v2").split(",")
    ]
    if segment
)

JOB_TTL_SECONDS = 3600       # auto-delete completed jobs older than 1 hour
MAX_JOBS        = 10         # max jobs kept in memory at once

# ─── Job store (in-memory) ────────────────────────────────────────────────────
# job_id → { status, completed, total, percent, current_day, result, error, created_at }

_jobs: dict = {}
_jobs_lock = multiprocessing.Lock()
_LIST_CACHE_TTL_SECONDS = 30.0
_list_cache: dict[str, dict] = {}
_list_cache_lock = threading.Lock()

_ACTIVE_OPTION_CHAIN_CACHE: dict[str, dict[str, Any]] = {}
_ACTIVE_OPTION_CHAIN_CACHE_LOCK = threading.Lock()
_simulator_broker = SimulatorZerodhaBroker()
_simulator_sessions: dict[str, StrategyEngine] = {}
_shared_mongo = MongoData()
IST = timezone(timedelta(hours=5, minutes=30))
ALGO_TRADE_PORTFOLIO_COLLECTION = "algo_trade_portfolio"


# Fallback identity for simulator per-user data (simulator_strategy,
# simulator_portfolio, simulator_new_positions, sim_user_subscriptions, etc.)
# when current_user["_id"] resolves to None — i.e. the request reached us as
# the anonymous stub (get_current_user with AUTH_ENFORCEMENT_ENABLED off, or a
# process that hasn't picked up the env change yet without a restart). Keeps
# every "current user's data" query/write consistent under one real user
# instead of silently splitting across None vs real ids. Remove once every
# simulator-facing service reliably resolves a real logged-in user.
# Stored as a plain string on simulator_strategy (not ObjectId) — per request,
# this collection's user_id is a string field, not a Mongo reference type.
_SIM_DEFAULT_USER_ID = "6a3917c7e0e323bfd2a398e7"


def _resolve_sim_user_id(current_user: dict) -> str:
    raw = current_user.get("_id")
    return str(raw) if raw else _SIM_DEFAULT_USER_ID


def _resolve_app_user_id(value: str | None = None) -> str:
    normalized_value = str(value or "").strip()
    if normalized_value:
        return normalized_value
    return DEFAULT_APP_USER_ID


def _normalize_runtime_activation_mode(value: str | None = None) -> str:
    return str(value or "").strip().lower() or "algo-backtest"


def _default_runtime_trade_date(value: str | None = None, date_hint: str | None = None) -> str:
    normalized_date = str(date_hint or "").strip()
    if normalized_date:
        return normalized_date
    normalized_mode = _normalize_runtime_activation_mode(value)
    if normalized_mode in {"live", "fast-forward", "forward-test"}:
        return datetime.now(IST).strftime("%Y-%m-%d")
    return ""


def _list_cache_get(key: str):
    now = time.time()
    with _list_cache_lock:
        item = _list_cache.get(key)
        if not item:
            return None
        if now - item.get("ts", 0) > _LIST_CACHE_TTL_SECONDS:
            _list_cache.pop(key, None)
            return None
        return deepcopy(item["value"])


def _list_cache_set(key: str, value) -> None:
    with _list_cache_lock:
        _list_cache[key] = {"ts": time.time(), "value": deepcopy(value)}


def _invalidate_list_cache(*keys: str) -> None:
    with _list_cache_lock:
        if not keys:
            _list_cache.clear()
            return
        for key in keys:
            _list_cache.pop(key, None)


def _should_register_version_alias(path: str) -> bool:
    normalized_path = str(path or "").strip()
    if not normalized_path:
        return False
    return any(normalized_path.startswith(prefix) for prefix in API_ROUTE_GROUP_PREFIXES)


def _register_versioned_route_aliases(app_instance: FastAPI) -> None:
    if not API_VERSION_PREFIXES:
        return

    existing_paths = {getattr(route, "path", "") for route in app_instance.routes}
    routes_snapshot = list(app_instance.routes)

    for route in routes_snapshot:
        path = getattr(route, "path", "")
        if not _should_register_version_alias(path):
            continue

        for version_prefix in API_VERSION_PREFIXES:
            alias_path = f"{version_prefix}{path}"
            if alias_path in existing_paths:
                continue

            if isinstance(route, APIRoute):
                app_instance.add_api_route(
                    alias_path,
                    route.endpoint,
                    methods=list(route.methods or []),
                    name=f"{route.name}{version_prefix}",
                    include_in_schema=False,
                    response_model=route.response_model,
                    status_code=route.status_code,
                    tags=list(route.tags),
                    dependencies=list(route.dependencies),
                    summary=route.summary,
                    description=route.description,
                    response_description=route.response_description,
                    responses=dict(route.responses),
                    deprecated=route.deprecated,
                    operation_id=None,
                    response_model_include=route.response_model_include,
                    response_model_exclude=route.response_model_exclude,
                    response_model_by_alias=route.response_model_by_alias,
                    response_model_exclude_unset=route.response_model_exclude_unset,
                    response_model_exclude_defaults=route.response_model_exclude_defaults,
                    response_model_exclude_none=route.response_model_exclude_none,
                    response_class=route.response_class,
                    openapi_extra=route.openapi_extra,
                    generate_unique_id_function=route.generate_unique_id_function,
                )
                existing_paths.add(alias_path)
                continue

            if isinstance(route, WebSocketRoute):
                app_instance.add_api_websocket_route(
                    alias_path,
                    route.endpoint,
                    name=f"{route.name}{version_prefix}",
                )
                existing_paths.add(alias_path)


def _load_active_option_chain_cache() -> dict[str, dict[str, Any]]:
    db = MongoData()
    try:
        contracts = list(
            db._db["active_option_tokens"].find(
                {},
                {
                    "_id": 0,
                    "instrument": 1,
                    "option_type": 1,
                    "expiry": 1,
                    "strike": 1,
                    "exchange": 1,
                    "symbol": 1,
                    "token": 1,
                    "tokens": 1,
                    "created_at": 1,
                    "updated_at": 1,
                },
            ).sort([("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)])
        )
    finally:
        db.close()

    cache: dict[str, dict[str, Any]] = {}
    for contract in contracts:
        instrument = str(contract.get("instrument") or "").strip().upper()
        expiry = str(contract.get("expiry") or "").strip()[:10]
        option_type = str(contract.get("option_type") or "").strip().upper()
        token = str(contract.get("token") or contract.get("tokens") or "").strip()
        if not instrument or not expiry:
            continue

        instrument_bucket = cache.setdefault(
            instrument,
            {
                "instrument": instrument,
                "expiries": [],
                "expiry_count": 0,
                "total_contracts": 0,
                "source": "active_option_tokens",
                "option_chain": [],
                "grouped_option_chain": {},
            },
        )
        if expiry not in instrument_bucket["expiries"]:
            instrument_bucket["expiries"].append(expiry)

        grouped_bucket = instrument_bucket["grouped_option_chain"].setdefault(
            expiry,
            {"CE": [], "PE": []},
        )

        strike_raw = contract.get("strike")
        try:
            strike_value = float(strike_raw)
        except (TypeError, ValueError):
            strike_value = 0.0
        strike = int(strike_value) if strike_value.is_integer() else strike_value

        row = {
            "instrument": instrument,
            "expiry": expiry,
            "strike": strike,
            "option_type": option_type,
            "token": token,
            "tokens": token,
            "symbol": str(contract.get("symbol") or "").strip(),
            "exchange": str(contract.get("exchange") or "").strip(),
            "ltp": 0.0,
            "created_at": str(contract.get("created_at") or "").strip(),
            "updated_at": str(contract.get("updated_at") or "").strip(),
        }
        instrument_bucket["option_chain"].append(row)
        if option_type in {"CE", "PE"}:
            grouped_bucket[option_type].append(row)

    for instrument_bucket in cache.values():
        instrument_bucket["expiries"].sort()
        instrument_bucket["expiry_count"] = len(instrument_bucket["expiries"])
        instrument_bucket["total_contracts"] = len(instrument_bucket["option_chain"])
        for expiry_bucket in instrument_bucket["grouped_option_chain"].values():
            expiry_bucket["CE"].sort(key=lambda item: float(item.get("strike") or 0.0))
            expiry_bucket["PE"].sort(key=lambda item: float(item.get("strike") or 0.0))

    return cache


def _refresh_active_option_chain_cache() -> dict[str, dict[str, Any]]:
    cache = _load_active_option_chain_cache()
    with _ACTIVE_OPTION_CHAIN_CACHE_LOCK:
        _ACTIVE_OPTION_CHAIN_CACHE.clear()
        _ACTIVE_OPTION_CHAIN_CACHE.update(cache)
    return cache


def _get_active_option_chain_cache(instrument: str) -> dict[str, Any] | None:
    normalized_instrument = str(instrument or "").strip().upper()
    with _ACTIVE_OPTION_CHAIN_CACHE_LOCK:
        cached = _ACTIVE_OPTION_CHAIN_CACHE.get(normalized_instrument)
        if cached is not None:
            return cached

    cache = _refresh_active_option_chain_cache()
    return cache.get(normalized_instrument)


def _request_fingerprint(request: dict) -> str:
    payload = json.dumps(request, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _estimate_total_steps(request: dict) -> int:
    start_date = request.get("start_date")
    end_date = request.get("end_date")
    if not start_date or not end_date:
        return 0
    try:
        db = MongoData()
        holidays = db.get_holidays()
        cur = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        total_days = 0
        while cur <= end_dt:
            if cur.weekday() < 5 and cur.strftime("%Y-%m-%d") not in holidays:
                total_days += 1
            cur += timedelta(days=1)
        db.close()
        return total_days + 1 if total_days > 0 else 0
    except Exception:
        return 0


def _job_state_path(job_id: str) -> Path:
    JOB_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return JOB_STATE_DIR / f"{job_id}.json"


def _cache_path(fingerprint: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{fingerprint}.json"


def _write_job_state(job_id: str, payload: dict) -> None:
    path = _job_state_path(job_id)
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(payload, f)
    os.replace(tmp_path, path)


def _read_job_state(job_id: str) -> dict | None:
    path = _job_state_path(job_id)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _read_cached_result(fingerprint: str) -> dict | None:
    path = _cache_path(fingerprint)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _write_cached_result(fingerprint: str, result: dict) -> None:
    path = _cache_path(fingerprint)
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(result, f)
    os.replace(tmp_path, path)


def _cleanup_old_jobs():
    """Remove finished jobs older than JOB_TTL_SECONDS and enforce MAX_JOBS limit."""
    # Sync in-memory "running" jobs from file — child process only writes files,
    # so _jobs in the parent can be stale (still "running" after the child finishes).
    for jid, job in list(_jobs.items()):
        if job["status"] == "running":
            file_state = _read_job_state(jid)
            if file_state and file_state.get("status") != "running":
                _jobs[jid].update(file_state)

    now = time.time()
    expired = [jid for jid, j in _jobs.items()
               if j["status"] != "running"
               and now - j.get("created_at", now) > JOB_TTL_SECONDS]
    for jid in expired:
        state_path = _job_state_path(jid)
        if state_path.exists():
            state_path.unlink()
        del _jobs[jid]

    # if still over limit, remove oldest completed jobs first
    if len(_jobs) >= MAX_JOBS:
        done = sorted(
            [(jid, j) for jid, j in _jobs.items() if j["status"] != "running"],
            key=lambda x: x[1].get("created_at", 0),
        )
        for jid, _ in done[:len(_jobs) - MAX_JOBS + 1]:
            del _jobs[jid]




def _run_job(job_id: str, request: dict):
    try:
        os.nice(15)
    except Exception:
        pass

    state = _read_job_state(job_id) or {}

    def on_progress(completed: int, total: int, day: str):
        state.update({
            "job_id": job_id,
            "status": "running",
            "completed": completed,
            "total": total,
            "percent": round(completed / total * 100, 1) if total else 0,
            "current_day": day,
            "error": None,
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)

    try:
        result = run_backtest(request, on_progress=on_progress)
        fingerprint = state.get("fingerprint")
        if fingerprint:
            _write_cached_result(fingerprint, result)
        total = state.get("total", 0)
        state.update({
            "job_id": job_id,
            "status": "done",
            "completed": total,
            "percent": 100.0 if total else 0.0,
            "current_day": "Completed",
            "result": result,
            "error": None,
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)
    except Exception as e:
        state.update({
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)


def strategy_worker(args: dict):
    strategy_id_str = str((args or {}).get("strategy_id_str") or "")
    backtest_req = dict((args or {}).get("backtest_req") or {})
    job_id = str((args or {}).get("job_id") or "")

    # Write per-strategy progress to a temp file — avoids Manager IPC complexity
    prog_path = JOB_STATE_DIR / f"{job_id}_{strategy_id_str}.prog" if job_id else None

    def on_progress(completed: int, total: int, day: str):
        if not prog_path:
            return
        try:
            tmp = prog_path.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump({"completed": completed, "total": total, "day": day}, f)
            os.replace(tmp, prog_path)
        except Exception:
            pass

    try:
        result = run_backtest(backtest_req, on_progress=on_progress)
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "completed",
            "error": None,
            "results": result,
        }
    except Exception as exc:
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "error",
            "error": str(exc),
            "results": None,
        }
    finally:
        # Clean up progress file on completion/error
        if prog_path and prog_path.exists():
            try:
                prog_path.unlink()
            except Exception:
                pass


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_datetime_string(value: Any) -> datetime | None:
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return None
    normalized_value = normalized_value.replace("T", " ")
    for pattern in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(normalized_value, pattern)
        except ValueError:
            continue
    return None


def _shift_datetime_string_by_minutes(value: Any, minutes: int) -> Any:
    if not minutes:
        return value
    parsed_value = _parse_datetime_string(value)
    if parsed_value is None:
        return value
    shifted_value = parsed_value - timedelta(minutes=minutes)
    if "." in str(value or ""):
        return shifted_value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return shifted_value.strftime("%Y-%m-%d %H:%M:%S")


def _load_strategy_time_difference_minutes(db: MongoData, activation_mode: str) -> int:
    normalized_mode = str(activation_mode or "").strip()
    if not normalized_mode:
        return 0

    query_candidates = [
        {"activation_mode": normalized_mode, "status": 1},
        {"activation_mode": normalized_mode, "is_active": True},
        {"activation_mode": normalized_mode, "active": True},
        {"activation_mode": normalized_mode},
    ]

    for query in query_candidates:
        try:
            config_doc = db._db["strategy_entry_time_difference"].find_one(
                query,
                {"difference_time_interval": 1},
                sort=[("_id", -1)],
            )
        except Exception:
            config_doc = None
        if config_doc:
            return max(0, _safe_int(config_doc.get("difference_time_interval"), 0))
    return 0


def _load_activation_portfolio_doc(db: MongoData, portfolio_id: str):
    normalized_portfolio_id = str(portfolio_id or "").strip()
    if not normalized_portfolio_id:
        raise HTTPException(status_code=400, detail="portfolio_id is required")
    try:
        portfolio_oid = ObjectId(normalized_portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio_id")

    source_doc = db._db["saved_portfolios"].find_one({"_id": portfolio_oid}, {"_id": 1, "name": 1})
    if source_doc:
        return "source", portfolio_oid, source_doc

    daily_doc = db._db[ALGO_TRADE_PORTFOLIO_COLLECTION].find_one(
        {"_id": portfolio_oid},
        {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1},
    )
    if daily_doc:
        return "daily", portfolio_oid, daily_doc

    raise HTTPException(status_code=404, detail="Portfolio not found")


def _get_source_portfolio_id_from_doc(portfolio_kind: str, portfolio_oid, portfolio_doc: dict) -> str:
    if portfolio_kind == "daily":
        resolved = str((portfolio_doc or {}).get("source_portfolio_id") or "").strip()
        if resolved:
            return resolved
    return str(portfolio_oid)


def _load_source_portfolio_root(db: MongoData, portfolio_kind: str, portfolio_oid, portfolio_doc: dict):
    if portfolio_kind == "source":
        return portfolio_oid, portfolio_doc or {}

    source_portfolio_id = str((portfolio_doc or {}).get("source_portfolio_id") or "").strip()
    if source_portfolio_id:
        try:
            source_oid = ObjectId(source_portfolio_id)
            source_doc = db._db["saved_portfolios"].find_one({"_id": source_oid}, {"_id": 1, "name": 1}) or {}
            return source_oid, source_doc
        except Exception:
            pass
    return portfolio_oid, {"_id": portfolio_oid, "name": str((portfolio_doc or {}).get("source_portfolio_name") or (portfolio_doc or {}).get("name") or "").strip()}


def _normalize_trade_index(value: Any) -> str:
    return str(value or "").strip().upper()


def _extract_trade_index(*candidates: Any) -> str:
    for candidate in candidates:
        if isinstance(candidate, dict):
            nested_value = _extract_trade_index(
                candidate.get("trade_index"),
                candidate.get("ticker"),
                candidate.get("underlying"),
                ((candidate.get("config") or {}) if isinstance(candidate.get("config"), dict) else {}).get("Ticker"),
                ((candidate.get("strategy_detail") or {}) if isinstance(candidate.get("strategy_detail"), dict) else {}).get("underlying"),
                ((candidate.get("strategy") or {}) if isinstance(candidate.get("strategy"), dict) else {}).get("Ticker"),
            )
            if nested_value:
                return nested_value
            continue
        normalized = _normalize_trade_index(candidate)
        if normalized:
            return normalized
    return "NIFTY"


def _resolve_daily_portfolio(
    db: MongoData,
    source_portfolio_oid,
    source_portfolio_doc: dict,
    activation_mode: str = "",
    trade_date_hint: str = "",
    trade_index: str = "",
):
    """Find or create a daily runtime portfolio in algo_trade_portfolio.

    Runtime portfolio identity is scoped by:
      trade_date + activation_mode + trade_index

    Returns (portfolio_id_str, portfolio_doc_dict).
    """
    normalized_mode = _normalize_runtime_activation_mode(activation_mode)
    trade_date = _default_runtime_trade_date(normalized_mode, str(trade_date_hint or "").strip()[:10])
    if not trade_date:
        trade_date = datetime.now(IST).strftime("%Y-%m-%d")
    normalized_trade_index = _extract_trade_index(trade_index)

    collection = db._db[ALGO_TRADE_PORTFOLIO_COLLECTION]
    query = {
        "trade_date": trade_date,
        "activation_mode": normalized_mode,
        "trade_index": normalized_trade_index,
    }
    existing = collection.find_one(
        query,
        {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1, "created_at": 1, "updated_at": 1},
    )
    if existing:
        return str(existing["_id"]), existing

    new_oid = ObjectId()
    now_iso = datetime.utcnow().isoformat()
    sibling_doc = collection.find_one(
        {
            "trade_date": trade_date,
            "activation_mode": normalized_mode,
            "trade_group_portfolio": {"$exists": True, "$ne": ""},
        },
        {"trade_group_portfolio": 1},
    )
    trade_group_portfolio = str((sibling_doc or {}).get("trade_group_portfolio") or "").strip() or str(ObjectId())
    new_doc = {
        "_id": new_oid,
        "trade_portfolio": str(new_oid),
        "trade_group_portfolio": trade_group_portfolio,
        "trade_index": normalized_trade_index,
        "trade_date": trade_date,
        "activation_mode": normalized_mode,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    try:
        result = collection.insert_one(new_doc)
        return str(result.inserted_id), {
            "_id": result.inserted_id,
            "trade_portfolio": str(new_doc["trade_portfolio"]),
            "trade_group_portfolio": trade_group_portfolio,
            "trade_index": normalized_trade_index,
            "trade_date": trade_date,
            "activation_mode": normalized_mode,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    except Exception:
        fallback = collection.find_one(
            query,
            {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1, "created_at": 1, "updated_at": 1},
        )
        if fallback:
            return str(fallback["_id"]), fallback
        return str(source_portfolio_oid), source_portfolio_doc


def _apply_strategy_time_difference_to_trade(trade_doc: dict, difference_minutes: int) -> dict:
    if difference_minutes <= 0 or not isinstance(trade_doc, dict):
        return trade_doc

    adjusted_doc = dict(trade_doc)
    for field_name in ("entry_time", "exit_time", "check_after_ts"):
        if field_name in adjusted_doc:
            adjusted_doc[field_name] = _shift_datetime_string_by_minutes(
                adjusted_doc.get(field_name),
                difference_minutes,
            )
    return adjusted_doc


def _calc_leg_pnl(leg: dict) -> dict:
    entry_trade = leg.get("entry_trade") if isinstance(leg.get("entry_trade"), dict) else {}
    exit_trade = leg.get("exit_trade") if isinstance(leg.get("exit_trade"), dict) else {}
    entry_price = _safe_float(entry_trade.get("price"))
    quantity = _safe_int(leg.get("quantity") or entry_trade.get("quantity"))
    lot_size = _safe_int(leg.get("lot_size"), 1)
    effective_quantity = max(0, quantity) * max(1, lot_size)
    is_sell = "sell" in str(leg.get("position") or "").lower()

    if exit_trade:
        mark_price = _safe_float(exit_trade.get("price"))
        pnl_price_source = "exit_trade"
    else:
        mark_price = _safe_float(leg.get("last_saw_price"))
        pnl_price_source = "last_saw_price"

    if entry_price <= 0 or effective_quantity <= 0:
        pnl_value = 0.0
    else:
        pnl_value = ((entry_price - mark_price) if is_sell else (mark_price - entry_price)) * effective_quantity

    leg_payload = dict(leg)
    leg_payload["entry_price"] = entry_price
    leg_payload["mark_price"] = round(mark_price, 2)
    leg_payload["effective_quantity"] = effective_quantity
    leg_payload["pnl_price_source"] = pnl_price_source
    leg_payload["pnl"] = round(pnl_value, 2)
    return leg_payload


def _populate_history_legs(db_instance, records: list) -> list:
    """
    Batch-fetch all algo_trade_positions_history docs for the given trade records
    by querying trade_id. Groups docs per trade and attaches them as legs[].
    Status counts are derived from history docs:
      status=1 → open_legs_count
      status=2 → closed_legs_count
      status=0 → pending_legs_count
    """
    if not records:
        return records

    trade_ids = [str(rec.get("_id") or "") for rec in records if rec.get("_id")]
    if not trade_ids:
        return records

    # Single batch query: all history docs for all trades at once
    history_by_trade: dict[str, list] = {tid: [] for tid in trade_ids}
    try:
        history_col = db_instance["algo_trade_positions_history"]
        for doc in history_col.find({"trade_id": {"$in": trade_ids}}):
            doc["_id"] = str(doc.get("_id") or "")
            tid = str(doc.get("trade_id") or "")
            if tid in history_by_trade:
                history_by_trade[tid].append(doc)
    except Exception:
        pass

    populated = []
    for rec in records:
        trade_id = str(rec.get("_id") or "")
        history_legs = history_by_trade.get(trade_id) or []
        new_rec = dict(rec)
        new_rec["legs"] = history_legs
        new_rec["open_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 1)
        new_rec["closed_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 2)
        new_rec["pending_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 0)
        populated.append(new_rec)
    return populated


def _format_feature_status_timestamp(value) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    return raw_value.replace("T", " ")


def _format_feature_status_price(value) -> str:
    numeric = _safe_float(value)
    if numeric <= 0:
        return "-"
    return f"₹{numeric:.2f}"


def _describe_feature_status_row(row: dict) -> str:
    if not isinstance(row, dict):
        return ""

    description = str(row.get("trigger_description") or "").strip()
    if description:
        return description

    feature_key = str(row.get("feature") or "").strip()
    if feature_key in {"overall_sl", "overall_target"}:
        label = "Overall SL" if feature_key == "overall_sl" else "Overall Target"
        cycle_number = int(row.get("cycle_number") or 1)
        trigger_value = _format_feature_status_price(row.get("trigger_value"))
        next_value = _format_feature_status_price(row.get("next_trigger_value"))
        reentry_type = str(row.get("reentry_type") or "None")
        reentry_count = int(row.get("reentry_count") or 0)
        reentry_done = int(row.get("reentry_done") or 0)
        return (
            f"{label} active for cycle {cycle_number}. "
            f"Current threshold {trigger_value}. "
            f"Re-entry {reentry_type} used {reentry_done}/{reentry_count}. "
            f"Next cycle threshold {next_value}."
        )
    if feature_key == "pending_entry":
        option = str(row.get("option") or "").strip().upper() or "-"
        position = str(row.get("position") or "").split(".")[-1].strip() or "Position"
        strike = str(row.get("strike") or "").strip() or "-"
        queued_at = _format_feature_status_timestamp(row.get("queued_at"))
        triggered_at = _format_feature_status_timestamp(row.get("triggered_at"))
        status = str(row.get("status") or "").strip().lower()

        if status == "triggered":
            return (
                f"Pending entry triggered for {strike} {option} {position} leg at {triggered_at or '-'}."
            )

        return (
            f"Pending entry active for {strike} {option} {position} leg since {queued_at or '-'}. "
            f"Waiting for next entry cycle."
        )

    if feature_key != "momentum_pending":
        return ""

    status = str(row.get("status") or "").strip().lower()
    option = str(row.get("option") or "").strip().upper() or "-"
    position = str(row.get("position") or "").split(".")[-1].strip() or "Position"
    strike = str(row.get("strike") or "").strip() or "-"
    momentum_type = str(row.get("momentum_type") or "").split(".")[-1].strip() or "Momentum"
    momentum_value = _safe_float(row.get("momentum_value"))
    base_price = _format_feature_status_price(row.get("momentum_base_price"))
    target_price = _format_feature_status_price(row.get("momentum_target_price"))
    queued_at = _format_feature_status_timestamp(row.get("queued_at"))
    armed_at = _format_feature_status_timestamp(row.get("armed_at"))

    if status == "triggered":
        triggered_at = _format_feature_status_timestamp(row.get("triggered_at"))
        return (
            f"Momentum triggered for {strike} {option} {position} leg at {triggered_at or '-'}."
        )

    if _safe_float(row.get("momentum_base_price")) > 0 and _safe_float(row.get("momentum_target_price")) > 0:
        return (
            f"Momentum waiting for {strike} {option} {position} leg. "
            f"{momentum_type} {momentum_value:g} armed at {armed_at or queued_at or '-'} "
            f"with base {base_price} and target {target_price}."
        )

    return (
        f"Momentum queue active for {strike} {option} {position} leg since {queued_at or '-'}. "
        f"Waiting to arm {momentum_type} {momentum_value:g}."
    )


def _build_pending_feature_leg(row: dict) -> dict:
    row_copy = dict(row)
    description = _describe_feature_status_row(row_copy)
    if description:
        row_copy["trigger_description"] = description

    feature_map = {}
    feature_key = str(row_copy.get("feature") or "").strip()
    if feature_key:
        feature_map[feature_key] = row_copy

    return {
        "id": str(row_copy.get("leg_id") or ""),
        "leg_id": str(row_copy.get("leg_id") or ""),
        "status": 0,
        "position": row_copy.get("position"),
        "option": row_copy.get("option"),
        "strike": row_copy.get("strike"),
        "expiry_date": row_copy.get("expiry_date"),
        "token": row_copy.get("token"),
        "symbol": row_copy.get("symbol"),
        "quantity": 0,
        "lot_config_value": int(row_copy.get("lot_config_value") or 1),
        "entry_trade": None,
        "exit_trade": None,
        "last_saw_price": row_copy.get("momentum_base_price"),
        "is_lazy": True,
        "is_pending_feature_leg": True,
        "queued_at": row_copy.get("queued_at"),
        "armed_at": row_copy.get("armed_at"),
        "triggered_at": row_copy.get("triggered_at"),
        "leg_type": row_copy.get("leg_type"),
        "momentum_base_price": row_copy.get("momentum_base_price"),
        "momentum_target_price": row_copy.get("momentum_target_price"),
        "feature_status_rows": [row_copy],
        "feature_status_map": feature_map,
        "active_trigger_descriptions": [description] if description else [],
    }


def _attach_leg_feature_statuses(db_instance, records: list) -> list:
    if not records:
        return records

    trade_ids = [str(rec.get("_id") or "") for rec in records if rec.get("_id")]
    if not trade_ids:
        return records

    feature_rows_by_key: dict[tuple[str, str], list] = {}
    try:
        feature_col = db_instance["algo_leg_feature_status"]
        for doc in feature_col.find(
            {
                "trade_id": {"$in": trade_ids},
                "enabled": True,
            }
        ):
            trade_id = str(doc.get("trade_id") or "")
            leg_id = str(doc.get("leg_id") or "")
            if not trade_id or not leg_id:
                continue
            doc["_id"] = str(doc.get("_id") or "")
            feature_rows_by_key.setdefault((trade_id, leg_id), []).append(doc)
    except Exception:
        return records

    enriched_records = []
    for rec in records:
        trade_id = str(rec.get("_id") or "")
        legs = rec.get("legs") if isinstance(rec.get("legs"), list) else []
        existing_leg_ids = set()
        enriched_legs = []
        for leg in legs:
            if not isinstance(leg, dict):
                enriched_legs.append(leg)
                continue
            leg_id = str(leg.get("_id") or leg.get("leg_id") or leg.get("id") or "")
            if leg_id:
                existing_leg_ids.add(leg_id)
            feature_rows = feature_rows_by_key.get((trade_id, leg_id), [])
            leg_copy = dict(leg)
            leg_copy["feature_status_rows"] = feature_rows
            feature_map = {}
            active_descriptions = []
            for row in feature_rows:
                feature_key = str(row.get("feature") or "").strip()
                if not feature_key:
                    continue
                row_copy = dict(row)
                description = _describe_feature_status_row(row_copy)
                if description:
                    row_copy["trigger_description"] = description
                feature_map[feature_key] = row_copy
                if description:
                    active_descriptions.append(description)
            leg_copy["feature_status_map"] = feature_map
            leg_copy["feature_status_rows"] = list(feature_map.values()) if feature_map else feature_rows
            leg_copy["active_trigger_descriptions"] = active_descriptions
            enriched_legs.append(leg_copy)

        pending_feature_legs = []
        strategy_feature_rows = []
        for (feature_trade_id, feature_leg_id), feature_rows in feature_rows_by_key.items():
            if feature_trade_id != trade_id or not feature_leg_id or feature_leg_id in existing_leg_ids:
                continue
            if feature_leg_id == "__overall__":
                for row in feature_rows:
                    row_copy = dict(row)
                    description = _describe_feature_status_row(row_copy)
                    if description:
                        row_copy["trigger_description"] = description
                    strategy_feature_rows.append(row_copy)
                continue
            for row in feature_rows:
                if str(row.get("feature") or "").strip() not in {"momentum_pending", "pending_entry"}:
                    continue
                if str(row.get("status") or "").strip().lower() != "active":
                    continue
                pending_feature_legs.append(_build_pending_feature_leg(row))

        new_rec = dict(rec)
        new_rec["legs"] = enriched_legs
        new_rec["pending_feature_legs"] = pending_feature_legs
        new_rec["strategy_feature_status_rows"] = strategy_feature_rows
        enriched_records.append(new_rec)
    return enriched_records


def _extract_broker_configuration_label(document: dict, fallback_broker_id: str = "") -> str:
    if not isinstance(document, dict):
        return fallback_broker_id
    for key in (
        "broker_name",
        "display_name",
        "name",
        "title",
        "broker",
        "broker_type",
        "provider",
        "vendor",
    ):
        value = str(document.get(key) or "").strip()
        if value:
            return value
    return str(fallback_broker_id or "").strip()


def _attach_broker_configuration_details(db_instance, records: list) -> list:
    if not records:
        return records

    broker_ids = []
    broker_object_ids = []
    for record in records:
        broker_id = str((record or {}).get("broker") or "").strip()
        if not broker_id:
            continue
        broker_ids.append(broker_id)
        try:
            broker_object_ids.append(ObjectId(broker_id))
        except Exception:
            continue

    if not broker_ids:
        return records

    broker_docs_by_id = {}
    try:
        cursor = db_instance["broker_configuration"].find(
            {"_id": {"$in": broker_object_ids}},
            {
                "_id": 1,
                "broker_name": 1,
                "display_name": 1,
                "name": 1,
                "title": 1,
                "broker": 1,
                "broker_icon": 1,
                "broker_type": 1,
                "provider": 1,
                "vendor": 1,
            },
        )
        for item in cursor:
            if not item:
                continue
            item_id = str(item.get("_id") or "").strip()
            if item_id:
                broker_docs_by_id[item_id] = item
    except Exception:
        return records

    if not broker_docs_by_id:
        return records

    enriched_records = []
    for record in records:
        new_record = dict(record)
        broker_id = str(new_record.get("broker") or "").strip()
        broker_doc = broker_docs_by_id.get(broker_id)
        if broker_doc:
            broker_details = dict(broker_doc)
            broker_details["_id"] = str(broker_doc.get("_id") or broker_id)
            new_record["broker_details"] = broker_details
            new_record["broker_label"] = _extract_broker_configuration_label(broker_doc, broker_id)
        enriched_records.append(new_record)
    return enriched_records


def _enrich_execution_record_with_pnl(record: dict) -> dict:
    legs = record.get("legs") if isinstance(record.get("legs"), list) else []
    enriched_legs = [_calc_leg_pnl(leg) for leg in legs if isinstance(leg, dict)]
    enriched_record = dict(record)
    enriched_record["legs"] = enriched_legs
    return enriched_record


def _run_portfolio_job(job_id: str, request: dict):
    """
    Subprocess worker for portfolio backtest.
    Runs all strategies in parallel using ProcessPoolExecutor.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed, wait
    import multiprocessing

    try:
        os.nice(10)
    except Exception:
        pass

    state = _read_job_state(job_id) or {}
    portfolio_id = request.get("portfolio")
    start_date   = request.get("start_date")
    end_date     = request.get("end_date")

    try:
        db = MongoData()
        portfolio = db._db["saved_portfolios"].find_one({"_id": ObjectId(portfolio_id)})
        if not portfolio:
            state.update({"job_id": job_id, "status": "error",
                          "error": f"Portfolio {portfolio_id} not found",
                          "updated_at": time.time()})
            _write_job_state(job_id, state)
            db.close()
            return

        strategy_ids = portfolio.get("strategy_ids", [])
        if not strategy_ids:
            state.update({"job_id": job_id, "status": "error",
                          "error": "Portfolio has no strategies",
                          "updated_at": time.time()})
            _write_job_state(job_id, state)
            db.close()
            return

        strategy_docs = list(db._db["saved_strategies"].find(
            {"_id": {"$in": strategy_ids}},
            {"_id": 1, "name": 1, "full_config": 1},
        ))
        db.close()

        strategy_map     = {str(d["_id"]): d for d in strategy_docs}
        total_strategies = len(strategy_ids)
        name_map         = {}

        # Build per-strategy worker args
        worker_args = []
        error_results = []
        for strategy_id_obj in strategy_ids:
            strategy_id_str = str(strategy_id_obj)
            strategy_doc    = strategy_map.get(strategy_id_str)
            strategy_name   = (strategy_doc or {}).get("name") or strategy_id_str
            name_map[strategy_id_str] = strategy_name

            if not strategy_doc:
                error_results.append({
                    "_id":     strategy_id_str,
                    "item_id": strategy_id_str,
                    "status":  "error",
                    "error":   "Strategy not found",
                    "results": None,
                })
                continue

            full_config  = strategy_doc.get("full_config") or {}
            backtest_req = dict(full_config)
            backtest_req["start_date"] = start_date
            backtest_req["end_date"]   = end_date
            if "weekly_old_regime" in request:
                backtest_req["weekly_old_regime"] = request["weekly_old_regime"]

            worker_args.append({
                "strategy_id_str": strategy_id_str,
                "backtest_req":    backtest_req,
                "job_id":          job_id,
            })

        # Initial progress state
        state.update({
            "job_id":         job_id,
            "status":         "running",
            "strategy_count": total_strategies,
            "completed":      0,
            "total":          total_strategies,
            "percent":        0.0,
            "current_day":    f"Running {total_strategies} strategies in parallel…",
            "error":          None,
            "updated_at":     time.time(),
        })
        _write_job_state(job_id, state)

        results_by_id = {}
        for r in error_results:
            results_by_id[r["item_id"]] = r

        # Run in parallel — use min(strategies, cpu_count, 8) workers
        max_workers  = max(1, min(len(worker_args), os.cpu_count() or 4, 8))
        done_count   = len(error_results)

        def _read_prog_files() -> dict:
            """Read all per-strategy progress files for this job."""
            result = {}
            try:
                for p in JOB_STATE_DIR.glob(f"{job_id}_*.prog"):
                    try:
                        with open(p) as f:
                            data = json.load(f)
                        sid = p.stem[len(job_id) + 1:]
                        result[sid] = data
                    except Exception:
                        pass
            except Exception:
                pass
            return result

        if worker_args:
            with ProcessPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(strategy_worker, args): args["strategy_id_str"]
                    for args in worker_args
                }
                while futures:
                    done, not_done = wait(futures, timeout=1.0)

                    # Read per-strategy progress from temp files
                    prog_files = _read_prog_files()
                    total_pct  = done_count * 100.0
                    active_day = f"Completed {done_count}/{total_strategies}"
                    for sid, info in prog_files.items():
                        if info.get("total"):
                            worker_pct = (info["completed"] / info["total"]) * 100.0
                            worker_pct = max(worker_pct, 2.0)
                            total_pct += worker_pct
                            if info.get("day"):
                                active_day = info["day"]

                    overall_pct = round(total_pct / total_strategies, 1) if total_strategies else 0.0
                    state.update({
                        "job_id":      job_id,
                        "status":      "running",
                        "completed":   done_count,
                        "percent":     overall_pct,
                        "current_day": active_day,
                        "error":       None,
                        "updated_at":  time.time(),
                    })
                    _write_job_state(job_id, state)

                    for future in done:
                        result_item = future.result()
                        sid         = result_item["item_id"]
                        results_by_id[sid] = result_item
                        done_count += 1
                        del futures[future]

                        # Write immediately after each strategy completes
                        pct = round(done_count / total_strategies * 100, 1) if total_strategies else 0.0
                        state.update({
                            "completed":   done_count,
                            "percent":     pct,
                            "current_day": f"Completed {done_count}/{total_strategies} strategies",
                            "updated_at":  time.time(),
                        })
                        _write_job_state(job_id, state)

        # Preserve original strategy order
        results = [results_by_id[str(sid)] for sid in strategy_ids if str(sid) in results_by_id]

        final_result = {
            "status":   "completed",
            "progress": 100,
            "results":  results,
        }

        state.update({
            "job_id":      job_id,
            "status":      "done",
            "completed":   total_strategies,
            "total":       total_strategies,
            "percent":     100.0,
            "current_day": "Completed",
            "result":      final_result,
            "error":       None,
            "updated_at":  time.time(),
        })
        _write_job_state(job_id, state)

    except Exception as e:
        import traceback
        state.update({
            "job_id":     job_id,
            "status":     "error",
            "error":      traceback.format_exc(),
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)


# ─── App ──────────────────────────────────────────────────────────────────────

app    = FastAPI(title="Local Backtest API", version="2.0.0")
sim_router = APIRouter()   # simulator routes live at /simulator/... (no extra prefix)

# fno-stocks and historical-data are common/shared concerns — code lives in
# shared/features/ but is served ONLY from algo.websocket (8003), not mounted
# here too. algo.simulator's own api only contains simulator-specific routes.

# Exception: index/MCX-commodity token sync (GET /algo/sync-tokens/start/{instrument},
# /status, /stop) is admin/data-sync tooling the user wants triggerable from
# algo.simulator directly too, without needing algo.trade running — see
# shared/features/dhan_token_sync.py (same code algo.trade's own
# /algo/sync-tokens/* endpoints use). Equity F&O stock sync isn't covered here
# (that stays algo.trade-only); commodities/indices is everything this needs.
from features.dhan_token_sync import router as dhan_token_sync_router  # noqa: E402
app.include_router(dhan_token_sync_router)


class PTPortfolioIn(BaseModel):
    name: str


class ZerodhaConfigRequest(BaseModel):
    api_key: str
    api_secret: str


class PTPositionIn(BaseModel):
    type: str
    option_type: str
    strike: float = 0.0  # 0.0 for a futures leg (option_type "FUT") — no strike on a future
    expiry: str
    token: Optional[str] = None
    entry_price: float
    entry_time: Optional[str] = None
    lots: Optional[int] = 1
    lot_size: Optional[int] = 75
    quantity: Optional[float] = None
    exited: Optional[bool] = False
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    # "MARKET" / "LIMIT" / "SL" as picked in the Generate-Webhook-URL modal's legs table
    # (PaperTradeNew.tsx) — only consulted for a live webhook's order placement (see
    # _simulator_pt_webhook_create_strategy), where it's mapped to "MPP"/"LTP" respectively.
    # None for anything saved via the normal Save button, which never sends it.
    order_type: Optional[str] = None


class PTStrategyIn(BaseModel):
    portfolio_name: str
    strategy_name: str
    instrument: Optional[str] = "nifty"
    spot_price: Optional[float] = None
    config: Optional[dict[str, Any]] = None
    positions: Optional[list[PTPositionIn]] = []
    # "backtest" for strategies saved from the historical-data builder
    # (PaperTradeBacktest.tsx), "live" for everything saved from the
    # live-broker/positions views — pure bookkeeping, doesn't gate the risk
    # monitor (that's alert_status, set separately by the "Add Alert" toggle).
    mode: Optional[str] = "live"
    # "advanced" (tick-by-tick MTM, see live_quote_socket.py) or "regular" (30s
    # batch) — distinct from `mode` above, which is backtest/live bookkeeping.
    # Gated by the plan's advanced_slots, see _sim_advanced_slot_limit_error.
    execution_mode: Optional[str] = "regular"


class PTWebhookIn(BaseModel):
    # None for a live-broker-view adjustment (PTAdjustmentIn keyed by broker_id/underlying
    # instead — see openAlertModal in PaperTradeNew.tsx) — _simulator_pt_webhook_trigger
    # routes on this field's presence to tell that case apart from a saved strategy's
    # adjustment webhook.
    strategy_id: Optional[str] = None
    adjustment_id: str


class PTNewStrategyWebhookIn(BaseModel):
    """
    "Generate Webhook URL" (PaperTradeNew.tsx) — for a strategy that hasn't been saved
    yet, unlike PTWebhookIn above (always an existing strategy_id/adjustment_id pair).
    Carries everything simulator_pt_create_strategy would otherwise need, so hitting the
    generated URL later (_simulator_pt_webhook_trigger) can create the strategy from this
    snapshot with no further input.
    """
    trade_status: str  # "paper" | "live"
    broker_id: Optional[str] = None  # required when trade_status == "live"
    portfolio_name: str
    strategy_name: str
    instrument: Optional[str] = "nifty"
    spot_price: Optional[float] = None
    config: Optional[dict[str, Any]] = None
    positions: list[PTPositionIn] = []


class PTUpdateStrategyWebhookIn(BaseModel):
    """
    "Generate Webhook URL" on a *saved* strategy (PaperTradeNew.tsx /trade/:id view).
    Unlike PTNewStrategyWebhookIn (no strategy yet), this carries `strategy_id` so
    hitting the URL merges the snapshot positions into the existing strategy via the same
    netting logic the frontend "Update" button uses.
    """
    trade_status: str = "paper"
    broker_id: Optional[str] = None
    positions: list[PTPositionIn] = []


class PTTriggerIn(BaseModel):
    broker_id: str
    leg_id: str
    underlying: Optional[str] = None
    expiry: Optional[str] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None
    side: Optional[str] = None
    sl_mode: str
    sl_value: float
    tp_mode: str
    tp_value: float
    entry_price: float
    quantity: int
    exited: Optional[bool] = False


class PortfolioLegSnapshot(BaseModel):
    leg_id: str
    quantity: int


class PTPortfolioTriggerIn(BaseModel):
    broker_id: str
    underlying: str
    sl_upper: Optional[float] = None
    sl_lower: Optional[float] = None
    legs_snapshot: list[PortfolioLegSnapshot] = []


class PTAlertConfigLegSnapshot(BaseModel):
    leg_id: str
    quantity: int
    entry_price: float
    side: str


class PTAlertConfigToggle(BaseModel):
    enabled: bool = False
    unit: str = "points"
    value: float = 0.0


class PTAlertConfigTrailingStop(BaseModel):
    enabled: bool = False
    unit: str = "points"
    x: float = 0.0
    y: float = 0.0


class PTAlertConfigHedgeStrikeType(BaseModel):
    enabled: bool = False
    mode: str = "delta"
    value: float = 0.0
    strike: str = "ATM"


class PTAlertConfigHedgeTimeControl(BaseModel):
    enabled: bool = False
    entry_time: str = "09:15"
    exit_time: str = "15:30"


class PTAlertConfigIn(BaseModel):
    broker_id: str
    underlying: str
    # "alert_only" -> a leg/basket SL-TP hit is logged + Telegrammed to the
    # user (see simulator_risk_monitor.py's notify_user calls) but no real
    # order is placed. "auto" -> today's existing behavior (fires for real,
    # gated only by the global AUTO_FIRE_ENABLED kill-switch).
    trading_mode: str = "auto"
    stoploss: PTAlertConfigToggle
    target: PTAlertConfigToggle
    trailing_stop: PTAlertConfigTrailingStop
    hedge_strike_type: PTAlertConfigHedgeStrikeType
    hedge_time_control: PTAlertConfigHedgeTimeControl
    legs_snapshot: list[PTAlertConfigLegSnapshot] = []


class AdjustmentPositionIn(BaseModel):
    side: str
    lots: int
    qty: int
    strike: float
    option_type: str
    expiry: str
    entry_price: float
    tag: str  # "EXIT" | "NEW"


class PTAdjustmentIn(BaseModel):
    # Live-broker view keys by (broker_id, underlying); a saved/virtual
    # strategy (no broker_id/leg_id) keys by strategy_id instead — exactly
    # one of the two pairs is ever sent by the frontend depending on which
    # view (PaperTradeNew.tsx's isSavedStrategyView) is open.
    broker_id: Optional[str] = None
    underlying: Optional[str] = None
    strategy_id: Optional[str] = None
    trigger_condition: Optional[str] = None
    trigger_price: Optional[float] = None
    positions: list[AdjustmentPositionIn] = []
    # True while this is the live, armed config the risk monitor will act on; the
    # monitor flips it to False (never deletes) once fired, so simulator_adjustments
    # keeps a history of past adjustments instead of losing them.
    status: bool = True


class PTAdjustmentPatchIn(BaseModel):
    positions: list[AdjustmentPositionIn] = []
    trigger_price: Optional[float] = None
    trigger_condition: Optional[str] = None


class SimulatorBrokerPositionsRequest(BaseModel):
    broker_id: Optional[str] = None


class ManualOrderLeg(BaseModel):
    underlying: str
    expiry: str            # "YYYY-MM-DD"
    strike: float = 0.0    # 0.0 for a futures leg (option_type "FUT")
    option_type: str       # "CE" / "PE" / "FUT"
    side: str               # "BUY" / "SELL"
    quantity: int
    order_type: str         # "MARKET" / "LIMIT" / "SL"
    product: str             # "NRML" / "MIS"
    price: float = 0.0
    trigger_price: float = 0.0


class ManualOrderRequest(BaseModel):
    broker_id: str
    orders: list[ManualOrderLeg]


def _normalize_pt_option_type(option_type: str) -> str:
    normalized = str(option_type or "").strip().upper()
    if normalized in {"CALL", "CE"}:
        return "CE"
    if normalized in {"PUT", "PE"}:
        return "PE"
    return normalized


def _resolve_pt_position_token(position: dict, instrument: str = "") -> str:
    direct_token = str(position.get("token") or position.get("tokens") or "").strip()
    if direct_token:
        return direct_token

    normalized_instrument = str(instrument or position.get("instrument") or "").strip().upper()
    normalized_expiry = str(position.get("expiry") or "").strip()[:10]
    normalized_option_type = _normalize_pt_option_type(str(position.get("option_type") or ""))
    is_future = normalized_option_type == "FUT"
    try:
        strike_value = float(position.get("strike") or 0)
    except (TypeError, ValueError):
        strike_value = 0.0

    if not normalized_instrument or not normalized_expiry or not normalized_option_type:
        return ""
    if not is_future and strike_value <= 0:
        return ""

    # _enrich_pt_strategy_positions' own "cross_tokens" fallback (below) already
    # does this exact broker-aware active_option_tokens lookup correctly — but
    # only for positions that already have *some* stored token needing
    # cross-broker resolution. A position with no stored token at all (this
    # function's whole reason to exist) used to only ever try
    # _load_kite_instruments(), a Kite-only instrument master — with Dhan as
    # the active broker that's always empty/wrong, so current_ltp/MTM never
    # got computed and the frontend never even subscribed the leg's token for
    # live updates. Try the active broker's own token collection first.
    try:
        from features.broker_gateway import _active_broker
        if _active_broker() == "dhan":
            # A futures contract has no strike (always stored as 0.0 — see
            # _sync_dhan_index_future_tokens), so the query must omit it entirely
            # rather than matching strike: 0.0 literally against whatever this
            # position happens to carry.
            query = {
                "instrument": normalized_instrument,
                "expiry": {"$regex": f"^{normalized_expiry}"},
                "option_type": normalized_option_type,
                "broker": "dhan",
            }
            if not is_future:
                query["strike"] = strike_value
            doc = _shared_mongo._db["active_option_tokens"].find_one(
                query,
                {"token": 1, "tokens": 1, "_id": 0},
            )
            if doc:
                return str(doc.get("token") or doc.get("tokens") or "").strip()
            return ""
    except Exception:
        pass

    try:
        instrument_doc = (_load_kite_instruments() or {}).get(
            (normalized_instrument, normalized_expiry, strike_value, normalized_option_type)
        ) or {}
        return str(instrument_doc.get("token") or instrument_doc.get("tokens") or "").strip()
    except Exception:
        return ""


def _enrich_pt_strategy_positions(strategy_doc: dict) -> dict:
    enriched = dict(strategy_doc or {})
    instrument = str(enriched.get("instrument") or "").strip().upper()

    # Step 1: resolve tokens
    positions = []
    for raw_position in (enriched.get("positions") or []):
        if not isinstance(raw_position, dict):
            positions.append(raw_position)
            continue
        position = dict(raw_position)
        resolved_token = _resolve_pt_position_token(position, instrument)
        if resolved_token:
            position["token"] = resolved_token
        positions.append(position)

    # Step 2: fetch current LTP for all position tokens
    try:
        from features.broker_gateway import get_broker_ltp_map, get_broker_rest_quotes, _active_broker  # type: ignore
        ws_ltp = get_broker_ltp_map() or {}
        active_broker = _active_broker()

        # Build broker-native token map: stored token → active broker's token
        # Needed when positions have Kite tokens but Dhan is active (or vice-versa)
        broker_token_for: dict[str, str] = {}  # stored_token → broker_token
        ws_seg_for: dict[str, str] = {}         # broker_token → ws_segment

        stored_tokens = [str(p.get("token") or "") for p in positions if isinstance(p, dict) and p.get("token")]
        if stored_tokens:
            db_docs = list(_shared_mongo._db["active_option_tokens"].find(
                {"token": {"$in": stored_tokens}, "broker": active_broker},
                {"_id": 0, "token": 1, "ws_segment": 1},
            ))
            found_broker_tokens = {str(d["token"]) for d in db_docs}
            for d in db_docs:
                t = str(d["token"])
                broker_token_for[t] = t   # already a broker token
                ws_seg_for[t] = str(d.get("ws_segment") or "NSE_FNO")

            # Positions with non-broker tokens → resolve by strike/expiry/option_type
            cross_tokens = [t for t in stored_tokens if t not in found_broker_tokens]
            if cross_tokens:
                # Batch-fetch the position details we need for cross-resolution
                pos_by_token = {str(p.get("token") or ""): p for p in positions if isinstance(p, dict) and p.get("token")}
                for stored_tok in cross_tokens:
                    pos = pos_by_token.get(stored_tok) or {}
                    instr = str(pos.get("instrument") or instrument or "").upper()
                    expiry = str(pos.get("expiry") or "")[:10]
                    strike = pos.get("strike")
                    ot = _normalize_pt_option_type(str(pos.get("option_type") or ""))
                    is_future = ot == "FUT"
                    # A futures position's strike is always 0.0 (falsy) — only CE/PE
                    # positions need a real strike to resolve, see PTPositionIn.
                    if not (instr and expiry and ot) or (not is_future and not strike):
                        continue
                    try:
                        cross_query = {"instrument": instr, "expiry": {"$regex": f"^{expiry}"},
                                        "option_type": ot, "broker": active_broker}
                        if not is_future:
                            cross_query["strike"] = float(strike)
                        dhan_doc = _shared_mongo._db["active_option_tokens"].find_one(
                            cross_query,
                            {"token": 1, "ws_segment": 1, "_id": 0},
                        )
                        if dhan_doc:
                            bt = str(dhan_doc["token"])
                            broker_token_for[stored_tok] = bt
                            ws_seg_for[bt] = str(dhan_doc.get("ws_segment") or "NSE_FNO")
                    except Exception:
                        pass

        # Collect all broker tokens for REST fallback
        all_broker_tokens = list({bt for bt in broker_token_for.values() if bt})
        missing_ltp = [t for t in all_broker_tokens if not ws_ltp.get(t)]
        rest_quotes: dict = {}
        if missing_ltp:
            try:
                rest_quotes = get_broker_rest_quotes(missing_ltp, _shared_mongo._db, ws_seg_for)
            except Exception:
                pass

        for position in positions:
            if not isinstance(position, dict):
                continue
            stored_tok = str(position.get("token") or "")
            if not stored_tok:
                continue
            bt = broker_token_for.get(stored_tok, stored_tok)
            # If cross-resolution mapped stored_tok → a different broker-native token,
            # update the position's token field so the frontend subscribes the correct
            # token to /ws/live-quotes (otherwise a Kite token in Dhan mode would be
            # subscribed, Dhan WS wouldn't recognise it, and no LTP tick would arrive).
            if bt and bt != stored_tok:
                position["token"] = bt
            ltp = float(ws_ltp.get(bt) or 0)
            if ltp == 0:
                ltp = float((rest_quotes.get(bt) or {}).get("ltp") or 0)
            if ltp > 0:
                position["current_ltp"] = round(ltp, 2)
    except Exception:
        pass

    enriched["positions"] = positions
    return enriched


_DEFAULT_PAPER_TRADE_SPOT_BROKER_ID = "69e18416c3d234dc8c90e6ca"


def _serialize_instrument_spot_token(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id") or "").strip(),
        "broker_id": str(doc.get("broker_id") or "").strip(),
        "instrument": str(doc.get("instrument") or "").strip().upper(),
        "code": str(doc.get("code") or "").strip().upper(),
        "token": str(doc.get("token") or "").strip(),
    }


def _get_instrument_spot_token_docs(broker_id: str = "") -> list[dict]:
    resolved_broker_id = str(broker_id or _DEFAULT_PAPER_TRADE_SPOT_BROKER_ID).strip()
    query = {"broker_id": resolved_broker_id} if resolved_broker_id else {}
    docs = list(
        _shared_mongo._db["instrument_spot_token"].find(
            query,
            {"broker_id": 1, "instrument": 1, "code": 1, "token": 1},
        ).sort("instrument", 1)
    )
    return [_serialize_instrument_spot_token(doc) for doc in docs]


def _get_simulator_default_quote_tokens(broker_id: str = "") -> list[str]:
    return [
        str(item.get("token") or "").strip()
        for item in _get_instrument_spot_token_docs(broker_id)
        if str(item.get("token") or "").strip()
    ]


_DEFAULT_PAPER_TRADE_PORTFOLIOS = [
    "Running Trades",  "Week On Nct Mnth"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _setup_logging():
    from features.app_logger import setup_logging
    setup_logging()
    try:
        MongoData().ensure_core_indexes()
    except Exception:
        log.exception("Failed to ensure MongoDB indexes at startup")
    try:
        _refresh_active_option_chain_cache()
    except Exception:
        log.exception("Failed to preload active option chain cache at startup")


@app.on_event("startup")
async def _auto_start_ticker():
    """Auto-start the broker WebSocket ticker on server startup (for live spot price / VIX)."""
    import asyncio, threading
    async def _bg():
        await asyncio.sleep(5)  # wait for server to fully initialise
        try:
            if ticker_manager.status not in ("running", "connecting"):
                threading.Thread(target=_start_ticker_bg, daemon=True).start()
                log.info("[STARTUP] Broker ticker auto-started.")
        except Exception:
            log.exception("[STARTUP] Broker ticker auto-start failed.")
    asyncio.create_task(_bg())




@app.on_event("startup")
async def _auto_start_alert_checker():
    """Continuously evaluate chart price/trendline alerts (tv_chart_state)
    against live spot price (option_chain_index_spot) and fire their
    webhooks — runs for the life of this process regardless of whether any
    browser tab with the chart open is still around. See
    features/alert_checker.py for the actual crossing logic, ported from
    algo-admin's Chart.tsx so server-side and client-side evaluation agree."""
    import asyncio
    from features.alert_checker import start_alert_checker_loop
    asyncio.create_task(start_alert_checker_loop())


@app.on_event("startup")
async def _auto_expiry_squareoff_catchup():
    """
    On server restart, exit any paper-strategy positions whose expiry has already
    passed (or whose expiry is today and 15:29 IST has already passed) at entry
    price, storing exit_reason='due to expiry alert' on each position. Runs once,
    independently of whether the risk monitor is manually started — no real broker
    call involved (paper strategies only).
    """
    import asyncio
    async def _run():
        await asyncio.sleep(5)  # let DB/ticker startup complete first
        try:
            await simulator_risk_monitor.run_startup_expiry_catchup()
        except Exception:
            log.exception('[EXPIRY SQUAREOFF] startup catch-up failed')
    asyncio.create_task(_run())



# Indicator-condition alerts (Supertrend/MACD/MA Cross/RSI/Stochastic) are
# deliberately NOT auto-started here, unlike the price/trendline loop above
# — they're controlled on demand via the monitor page/endpoints at
# /signal/indicator-alert-monitor/{start,stop,status} (signal_builder/
# router.py), the same manual start/stop pattern simulator/api_server.py's
# /monitor/{start,stop,status} already uses for the Simulator Monitor. See
# features/alert_checker.py's start_indicator_alert_monitor/
# stop_indicator_alert_monitor.


@app.on_event("startup")
async def _span_params_startup():
    """Seed SPAN defaults to DB (if empty) and load into memory cache."""
    import asyncio
    async def _bg():
        await asyncio.sleep(3)
        try:
            from features.span_file import save_defaults_to_db, fetch_span_file
            await asyncio.to_thread(save_defaults_to_db)   # seed DB if empty
            await asyncio.to_thread(fetch_span_file)       # load DB + any local files
        except Exception:
            log.exception("SPAN params startup failed — hardcoded defaults will be used")
    asyncio.create_task(_bg())


@app.on_event("startup")
async def _redis_prewarm():
    """
    On server startup: if REDIS_MEMORY=True, push all cached pkl5 files to Redis
    in a background thread so the first backtest hits Redis instead of disk.
    Only runs if Redis is reachable and pkl5 cache exists.
    """
    from features.backtest_engine import REDIS_MEMORY, _cache_dir, _pkl5_path, _get_redis, DataIndex
    if not REDIS_MEMORY:
        return
    import threading, pickle, pathlib

    def _warm():
        try:
            r = _get_redis()
        except Exception as e:
            print(f"[prewarm] Redis not available: {e}")
            return

        loaded = 0
        skipped = 0
        base = pathlib.Path.home() / ".backtest_cache"
        for underlying_dir in base.iterdir():
            if not underlying_dir.is_dir():
                continue
            underlying = underlying_dir.name
            for pkl5 in sorted(underlying_dir.glob("*.pkl5")):
                date = pkl5.stem
                key  = f"di:{underlying}:{date}"
                if r.exists(key):
                    skipped += 1
                    continue
                try:
                    with open(pkl5, 'rb') as f:
                        data = pickle.load(f)
                    r.set(key, pickle.dumps(data, protocol=5))
                    loaded += 1
                except Exception:
                    pass

        total = loaded + skipped
        print(f"[prewarm] Redis ready: {total} days ({loaded} loaded, {skipped} already cached)")

    threading.Thread(target=_warm, daemon=True).start()


# ─── Endpoints ────────────────────────────────────────────────────────────────



# ─── App user auth (mobile + password, JWT) ──────────────────────────────────











# ── Blocking endpoints (existing behaviour) ───────────────────────────────────





# ── Background job endpoints (with progress) ──────────────────────────────────





















def _extract_indicator_minutes(node):
    if isinstance(node, list):
        for item in node:
            minutes = _extract_indicator_minutes(item)
            if minutes is not None:
                return minutes
        return None
    if not isinstance(node, dict):
        return None
    value = node.get("Value")
    if isinstance(value, dict) and value.get("IndicatorName") == "IndicatorType.TimeIndicator":
        params = value.get("Parameters") or {}
        try:
            hour = int(params.get("Hour", 0))
            minute = int(params.get("Minute", 0))
            return hour * 60 + minute
        except Exception:
            return None
    if isinstance(value, list):
        nested = _extract_indicator_minutes(value)
        if nested is not None:
            return nested
    children = node.get("children") or node.get("Children")
    if isinstance(children, list):
        return _extract_indicator_minutes(children)
    return None


def _normalize_leg_instrument(option_value, instrument_kind):
    option = str(option_value or "").strip()
    if option.startswith("LegType."):
        return option
    if option in {"CE", "PE", "FUT"}:
        return f"LegType.{option}"
    instrument = str(instrument_kind or "").strip()
    if instrument.startswith("LegType."):
        return instrument
    if instrument in {"CE", "PE", "FUT"}:
        return f"LegType.{instrument}"
    return "LegType.CE"


def _normalize_weekdays_map(values):
    normalized = {
        "monday": False,
        "tuesday": False,
        "wednesday": False,
        "thursday": False,
        "friday": False,
        "saturday": False,
        "sunday": False,
    }
    mapping = {
        "m": "monday",
        "monday": "monday",
        "t": "tuesday",
        "tuesday": "tuesday",
        "w": "wednesday",
        "wednesday": "wednesday",
        "th": "thursday",
        "thu": "thursday",
        "thursday": "thursday",
        "f": "friday",
        "friday": "friday",
        "sat": "saturday",
        "saturday": "saturday",
        "sun": "sunday",
        "sunday": "sunday",
    }
    for value in values if isinstance(values, list) else []:
        key = mapping.get(str(value or "").strip().lower())
        if key:
            normalized[key] = True
    return normalized


def _default_leg_execution_config():
    return {
        "ProductType": "ProductType.NRML",
        "ExitOrder": {
            "Type": "OrderType.Limit",
            "Value": {
                "Buffer": {
                    "Type": "BufferType.Points",
                    "Value": {"TriggerBuffer": 0, "LimitBuffer": 3},
                },
                "Modification": {
                    "ModificationFrequency": 5,
                    "ContinuousMonitoring": "True",
                    "MarketOrderAfter": 1,
                },
            },
        },
        "EntryOrder": {
            "Type": "OrderType.Limit",
            "Value": {
                "Buffer": {
                    "Type": "BufferType.Points",
                    "Value": {"TriggerBuffer": 0, "LimitBuffer": 3},
                },
                "Modification": {"MarketOrderAfter": 40},
            },
        },
        "ReferenceForTgtSL": "PriceReferenceType.Trigger",
        "EntryDelay": 0,
    }


def _build_execution_cache(strategy_detail: dict, strategy_state: dict):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []
    ticker = detail.get("underlying") or strategy.get("Ticker") or strategy_state.get("ticker") or "NIFTY"

    lot_config = []
    expiries = []
    instruments = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        lot = leg.get("LotConfig") if isinstance(leg.get("LotConfig"), dict) else {}
        contract = leg.get("ContractType") if isinstance(leg.get("ContractType"), dict) else {}
        lot_config.append({
            "type": lot.get("Type") or "LotType.Quantity",
            "value": int(lot.get("Value", 1) or 1),
        })
        expiries.append(contract.get("Expiry") or "ExpiryType.Weekly")
        instruments.append(_normalize_leg_instrument(contract.get("Option"), contract.get("InstrumentKind")))

    return {
        "execution_version": "v2",
        "entry_time": _extract_indicator_minutes(strategy.get("EntryIndicators")),
        "exit_time": _extract_indicator_minutes(strategy.get("ExitIndicators")),
        "num_original_legs": len(legs),
        "lot_config": lot_config,
        "expiries": expiries,
        "instruments": instruments,
        "ticker": ticker,
        "strategy_type": strategy.get("StrategyType") or "StrategyType.IntradaySameDay",
        "reentry_restriction": strategy.get("ReentryTimeRestriction"),
    }


def _build_strategy_execution_config(strategy_detail: dict, strategy_state: dict, activation_mode: str):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []

    execution_config_base = detail.get("execution_config_base") if isinstance(detail.get("execution_config_base"), dict) else {}
    if not execution_config_base:
        execution_config_base = {
            "Multiplier": int(strategy_state.get("qty_multiplier") or 1),
            "LikeBacktester": activation_mode != "live",
            "MarginAutoSquareOff": True,
            "TimeDelta": 0,
        }
    else:
        execution_config_base = dict(execution_config_base)
        execution_config_base.setdefault("Multiplier", int(strategy_state.get("qty_multiplier") or 1))
        execution_config_base.setdefault("LikeBacktester", activation_mode != "live")
        execution_config_base.setdefault("MarginAutoSquareOff", True)
        execution_config_base.setdefault("TimeDelta", 0)

    execution_config_extra = detail.get("execution_config_extra") if isinstance(detail.get("execution_config_extra"), dict) else {}
    if not execution_config_extra or not isinstance(execution_config_extra.get("ListOfLegExecutionConfig"), list):
        execution_config_extra = {
            "ListOfLegExecutionConfig": [_default_leg_execution_config() for _ in legs]
        }
    else:
        execution_config_extra = dict(execution_config_extra)

    return {
        "execution_config_base": execution_config_base,
        "execution_config_extra": execution_config_extra,
        "is_weekdays": bool(strategy_state.get("is_weekdays", True)),
        "dte": strategy_state.get("dte") if isinstance(strategy_state.get("dte"), list) else [],
        "weekdays": _normalize_weekdays_map(strategy_state.get("weekdays") if isinstance(strategy_state.get("weekdays"), list) else []),
        "view_config": detail.get("view_config") if isinstance(detail.get("view_config"), dict) else {"advanced_exec_config_modal": True},
    }


def _normalize_execution_settings_payload(source_detail: dict, payload: dict, activation_mode: str):
    detail = _clone_json_value(source_detail) if isinstance(source_detail, dict) else {}
    incoming = payload if isinstance(payload, dict) else {}

    if isinstance(incoming.get("execution_config_base"), dict):
        detail["execution_config_base"] = _clone_json_value(incoming.get("execution_config_base"))
    if isinstance(incoming.get("execution_config_extra"), dict):
        detail["execution_config_extra"] = _clone_json_value(incoming.get("execution_config_extra"))
    if isinstance(incoming.get("view_config"), dict):
        detail["view_config"] = _clone_json_value(incoming.get("view_config"))

    normalized = _build_strategy_execution_config(
        detail,
        {
            "qty_multiplier": ((incoming.get("execution_config_base") or {}).get("Multiplier") if isinstance(incoming.get("execution_config_base"), dict) else 1) or 1,
            "is_weekdays": incoming.get("is_weekdays", True),
            "dte": incoming.get("dte") if isinstance(incoming.get("dte"), list) else [],
            "weekdays": list((incoming.get("weekdays") or {}).keys()) if isinstance(incoming.get("weekdays"), dict) else [],
        },
        activation_mode,
    )

    if isinstance(incoming.get("weekdays"), dict):
        normalized["weekdays"] = {
            "friday": bool(incoming["weekdays"].get("friday")),
            "monday": bool(incoming["weekdays"].get("monday")),
            "saturday": bool(incoming["weekdays"].get("saturday")),
            "sunday": bool(incoming["weekdays"].get("sunday")),
            "thursday": bool(incoming["weekdays"].get("thursday")),
            "tuesday": bool(incoming["weekdays"].get("tuesday")),
            "wednesday": bool(incoming["weekdays"].get("wednesday")),
        }
    normalized["is_weekdays"] = bool(incoming.get("is_weekdays", normalized.get("is_weekdays", True)))
    normalized["dte"] = incoming.get("dte") if isinstance(incoming.get("dte"), list) else normalized.get("dte", [])
    normalized["view_config"] = incoming.get("view_config") if isinstance(incoming.get("view_config"), dict) else normalized.get("view_config", {"advanced_exec_config_modal": True})
    return normalized


def _clone_json_value(value):
    return deepcopy(value)


def _normalize_optional_config(config):
    if not isinstance(config, dict):
        return None
    normalized = _clone_json_value(config)
    config_type = str(normalized.get("Type") or "").strip()
    if not config_type or config_type == "None":
        return None
    return normalized


def _normalize_reentry_value(config):
    if not isinstance(config, dict):
        return None
    reentry_type = str(config.get("Type") or "").strip()
    if not reentry_type or reentry_type == "None":
        return None

    raw_value = config.get("Value")
    normalized_value = raw_value
    if isinstance(raw_value, dict):
        if "NextLegRef" in raw_value:
            normalized_value = raw_value.get("NextLegRef")
        elif "ReentryCount" in raw_value:
            normalized_value = raw_value.get("ReentryCount")
        elif len(raw_value) == 1:
            normalized_value = next(iter(raw_value.values()))

    return {
        "Type": reentry_type,
        "Value": normalized_value,
    }


def _normalize_option_kind(instrument_kind: str):
    value = str(instrument_kind or "").upper()
    if "PE" in value:
        return "PE"
    return "CE"


def _normalize_contract_strike(value):
    if isinstance(value, (int, float)):
        return value
    raw_value = str(value or "").strip()
    if not raw_value:
        return 0
    if raw_value == "StrikeType.ATM":
        return 0
    numeric_match = re.fullmatch(r"-?\d+(?:\.\d+)?", raw_value)
    if numeric_match:
        parsed = float(raw_value)
        return int(parsed) if parsed.is_integer() else parsed
    return raw_value


def _build_algo_leg_config_entry(leg_config: dict):
    leg = leg_config if isinstance(leg_config, dict) else {}
    stop_loss = _normalize_optional_config(leg.get("LegStopLoss"))
    target = _normalize_optional_config(leg.get("LegTarget"))
    trail = _normalize_optional_config(leg.get("LegTrailSL"))
    momentum = _normalize_optional_config(leg.get("LegMomentum"))
    stop_reentry = _normalize_reentry_value(leg.get("LegReentrySL"))
    target_reentry = _normalize_reentry_value(leg.get("LegReentryTP"))

    if stop_loss and stop_reentry:
        stop_loss["Reentry"] = stop_reentry
    if stop_loss and trail:
        stop_loss["Trail"] = trail
    if target and target_reentry:
        target["Reentry"] = target_reentry

    return {
        "PositionType": leg.get("PositionType") or "PositionType.Sell",
        "ContractType": {
            "Option": _normalize_option_kind(leg.get("InstrumentKind")),
            "Expiry": leg.get("ExpiryKind") or "ExpiryType.Weekly",
            "InstrumentKind": "OPT",
            "StrikeParameter": _normalize_contract_strike(leg.get("StrikeParameter")),
            "EntryKind": leg.get("EntryType") or "EntryType.EntryByStrikeType",
        },
        "LotConfig": _clone_json_value(leg.get("LotConfig")) if isinstance(leg.get("LotConfig"), dict) else {
            "Type": "LotType.Quantity",
            "Value": 1,
        },
        "LegMomentum": momentum,
        "LegTarget": target,
        "LegStopLoss": stop_loss,
    }


def _build_algo_execution_leg_entry(leg_execution_config: dict):
    config = leg_execution_config if isinstance(leg_execution_config, dict) else {}
    entry_order = config.get("EntryOrder") if isinstance(config.get("EntryOrder"), dict) else {}
    exit_order = config.get("ExitOrder") if isinstance(config.get("ExitOrder"), dict) else {}

    entry_order_config = _clone_json_value(entry_order.get("Config")) if isinstance(entry_order.get("Config"), dict) else _clone_json_value(entry_order)
    exit_order_config = _clone_json_value(exit_order.get("Config")) if isinstance(exit_order.get("Config"), dict) else _clone_json_value(exit_order)
    if not entry_order_config:
        entry_order_config = {"Type": "OrderType.Market"}
    if not exit_order_config:
        exit_order_config = {"Type": "OrderType.Market"}

    return {
        "Product": config.get("Product") or config.get("ProductType") or "ProductType.NRML",
        "Reference": config.get("Reference") or config.get("ReferenceForTgtSL") or "PriceReferenceType.Trigger",
        "EntryOrder": {
            "Config": entry_order_config,
            "Delay": int(config.get("EntryDelay") or entry_order.get("Delay") or 0),
        },
        "ExitOrder": {
            "Config": exit_order_config,
            "Delay": int(config.get("ExitDelay") or exit_order.get("Delay") or 0),
        },
    }


def _build_algo_trade_config(strategy_detail: dict, strategy_state: dict, activation_mode: str):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    if not strategy:
        return None

    parent_legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []
    idle_legs = strategy.get("IdleLegConfigs") if isinstance(strategy.get("IdleLegConfigs"), dict) else {}
    execution_config = _build_strategy_execution_config(detail, strategy_state, activation_mode)
    execution_base = execution_config.get("execution_config_base") if isinstance(execution_config.get("execution_config_base"), dict) else {}
    execution_extra = execution_config.get("execution_config_extra") if isinstance(execution_config.get("execution_config_extra"), dict) else {}
    execution_leg_configs = execution_extra.get("ListOfLegExecutionConfig") if isinstance(execution_extra.get("ListOfLegExecutionConfig"), list) else []

    keyed_leg_configs = {}
    keyed_execution_legs = {}
    for index, leg in enumerate(parent_legs, start=1):
        leg_key = f"og_leg_{index}"
        keyed_leg_configs[leg_key] = _build_algo_leg_config_entry(leg)
        keyed_execution_legs[leg_key] = _build_algo_execution_leg_entry(
            execution_leg_configs[index - 1] if index - 1 < len(execution_leg_configs) else {}
        )

    normalized_idle_legs = {}
    for idle_key, idle_leg in idle_legs.items():
        normalized_idle_legs[str(idle_key)] = _build_algo_leg_config_entry(idle_leg)

    return {
        "ExecutionConfig": {
            "LikeBacktester": bool(execution_base.get("LikeBacktester", activation_mode != "live")),
            "MarginAutoSquareOff": bool(execution_base.get("MarginAutoSquareOff", True)),
            "LotMultiplier": int(execution_base.get("Multiplier") or strategy_state.get("qty_multiplier") or 1),
            "LegsConfig": keyed_execution_legs,
        },
        "Ticker": strategy.get("Ticker") or detail.get("underlying") or strategy_state.get("ticker") or "NIFTY",
        "TakeUnderlyingFromCash": str(strategy.get("TakeUnderlyingFromCashOrNot") or "True").lower() == "true",
        "TrailSLtoBreakeven": _normalize_optional_config(strategy.get("TrailSLtoBreakeven")),
        "SquareOffAllLegs": str(strategy.get("SquareOffAllLegs") or "False").lower() == "true",
        "LegConfigs": keyed_leg_configs,
        "IdleLegConfigs": normalized_idle_legs,
        "OverallSL": _normalize_optional_config(strategy.get("OverallSL")),
        "OverallTgt": _normalize_optional_config(strategy.get("OverallTgt")),
        "LockAndTrail": _normalize_optional_config(strategy.get("LockAndTrail")),
        "OverallTrailSL": _normalize_optional_config(strategy.get("OverallTrailSL")),
        "OverallReentrySL": _normalize_optional_config(strategy.get("OverallReentrySL")),
        "OverallReentryTgt": _normalize_optional_config(strategy.get("OverallReentryTgt")),
        "OverallMomentum": _normalize_optional_config(strategy.get("OverallMomentum")),
    }










def _calculate_margin_sync(body: dict) -> dict:
    """Run all blocking DB + CPU work in a thread — keeps the async event loop free."""
    from features.span_margin import calculate_margin, SpanPosition

    legs_raw = body.get("legs", [])
    positions = []
    resolved_legs: list[dict[str, Any]] = []
    broker_margin: dict[str, Any] | None = None
    db = MongoData()
    try:
        try:
            load_credentials_from_db(db)
        except Exception:
            log.exception("Failed to load Kite credentials for margin calculation")

        for leg in legs_raw:
            underlying = str(leg.get("underlying", "NIFTY")).upper().strip()
            instrument_type = str(leg.get("instrument_type", "CE")).upper().strip()
            expiry = str(leg.get("expiry", "")).strip()
            strike = float(leg.get("strike", 0) or 0)
            transaction_type = str(leg.get("transaction_type", "SELL")).upper().strip()
            quantity = int(leg.get("quantity", 1))
            lot_size = int(leg.get("lot_size", 1))
            ltp = float(leg.get("ltp", 0) or 0)
            spot = float(leg.get("spot", 0) or 0)

            if spot <= 0:
                spot_doc = get_cached_spot_doc(db._db, underlying)
                spot = float(
                    (spot_doc or {}).get("spot_price")
                    or (spot_doc or {}).get("ltp")
                    or (spot_doc or {}).get("close")
                    or 0.0
                )

            if instrument_type in {"CE", "PE"} and ltp <= 0:
                ltp = _resolve_single_option_ltp(
                    db._db, underlying, expiry, strike, instrument_type,
                )
            elif instrument_type == "FUT" and ltp <= 0:
                ltp = spot

            positions.append(SpanPosition(
                underlying=underlying, instrument_type=instrument_type,
                expiry=expiry, strike=strike, transaction_type=transaction_type,
                quantity=quantity, lot_size=lot_size, ltp=ltp, spot=spot,
            ))
            resolved_legs.append({
                "underlying": underlying, "instrument_type": instrument_type,
                "expiry": expiry, "strike": strike, "transaction_type": transaction_type,
                "quantity": quantity, "lot_size": lot_size, "ltp": ltp, "spot": spot,
            })

        use_broker_api = body.get("use_broker_api", True)
        if resolved_legs and use_broker_api:
            broker_margin = _calculate_kite_basket_margin(db._db, resolved_legs)
    finally:
        db.close()

    if not positions:
        return {"span_margin": 0, "exposure_margin": 0, "total_margin": 0, "premium_received": 0, "net_margin": 0, "legs": []}

    product = str(body.get("product", "NRML")).upper()
    broker  = str(body.get("broker",  "kite")).lower()
    result  = calculate_margin(positions, product=product, broker=broker)
    broker_final = (broker_margin or {}).get("final") or {}
    if isinstance(broker_final, dict) and broker_final:
        premium_received_display = 0.0
        for leg in resolved_legs:
            it = str(leg.get("instrument_type") or "").upper()
            if it not in {"CE", "PE"}:
                continue
            leg_premium_value = float(leg.get("ltp") or 0.0) * int(leg.get("quantity") or 0) * int(leg.get("lot_size") or 0)
            if str(leg.get("transaction_type") or "").upper() == "SELL":
                premium_received_display += leg_premium_value
            else:
                premium_received_display -= leg_premium_value
        return {
            "span_margin": float(broker_final.get("span") or 0.0),
            "exposure_margin": float(broker_final.get("exposure") or 0.0),
            "total_margin": float(broker_final.get("total") or 0.0),
            "premium_received": round(premium_received_display, 2),
            "net_margin": float(broker_final.get("total") or 0.0),
            "source": "kite_basket_order_margins",
            "broker_margin": broker_margin,
            "legs": [
                {"underlying": l.underlying, "instrument_type": l.instrument_type,
                 "expiry": l.expiry, "strike": l.strike, "transaction_type": l.transaction_type,
                 "quantity": l.quantity, "lot_size": l.lot_size, "ltp": l.ltp,
                 "span_contribution": l.span_contribution, "exposure_margin": l.exposure_margin,
                 "total_margin": l.total_margin, "implied_vol": l.implied_vol}
                for l in result.legs
            ],
        }
    return {
        "span_margin": result.span_margin, "exposure_margin": result.exposure_margin,
        "total_margin": result.total_margin, "premium_received": result.premium_received,
        "net_margin": result.net_margin, "source": "local_span_engine",
        "legs": [
            {"underlying": l.underlying, "instrument_type": l.instrument_type,
             "expiry": l.expiry, "strike": l.strike, "transaction_type": l.transaction_type,
             "quantity": l.quantity, "lot_size": l.lot_size, "ltp": l.ltp,
             "span_contribution": l.span_contribution, "exposure_margin": l.exposure_margin,
             "total_margin": l.total_margin, "implied_vol": l.implied_vol}
            for l in result.legs
        ],
    }
















def _build_trade_history_payload(db_obj, raw_trade: dict, normalized_status: str):
    normalized_strategy_id = str(raw_trade.get("_id") or "").strip()
    trade_record = {
        "_id": normalized_strategy_id,
        "strategy_id": str(raw_trade.get("strategy_id") or ""),
        "source_strategy_id": str(raw_trade.get("source_strategy_id") or ""),
        "name": raw_trade.get("name") or "",
        "status": raw_trade.get("status") or "",
        "trade_status": raw_trade.get("trade_status"),
        "active_on_server": bool(raw_trade.get("active_on_server")),
        "activation_mode": raw_trade.get("activation_mode") or normalized_status,
        "trade_date": raw_trade.get("trade_date") or "",
        "broker": raw_trade.get("broker") or "",
        "user_id": raw_trade.get("user_id") or "",
        "ticker": raw_trade.get("ticker") or "",
        "creation_ts": raw_trade.get("creation_ts") or "",
        "last_activation_ts": raw_trade.get("last_activation_ts") or "",
        "entry_time": raw_trade.get("entry_time") or "",
        "exit_time": raw_trade.get("exit_time") or "",
        "portfolio": raw_trade.get("portfolio") if isinstance(raw_trade.get("portfolio"), dict) else {},
        "strategy": raw_trade.get("strategy") if isinstance(raw_trade.get("strategy"), dict) else {},
        "execution_config_base": raw_trade.get("execution_config_base") if isinstance(raw_trade.get("execution_config_base"), dict) else {},
        "execution_config_extra": raw_trade.get("execution_config_extra") if isinstance(raw_trade.get("execution_config_extra"), dict) else {},
    }

    populated_records = _populate_history_legs(db_obj, [trade_record])
    populated_records = _attach_leg_feature_statuses(db_obj, populated_records)
    populated_records = _attach_broker_configuration_details(db_obj, populated_records)
    detailed_trade = _enrich_execution_record_with_pnl((populated_records or [trade_record])[0])

    legs = detailed_trade.get("legs") if isinstance(detailed_trade.get("legs"), list) else []
    pending_feature_legs = detailed_trade.get("pending_feature_legs") if isinstance(detailed_trade.get("pending_feature_legs"), list) else []

    trade_mtm = round(sum(float((leg or {}).get("pnl") or 0) for leg in legs if isinstance(leg, dict)), 2)
    open_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 1]
    closed_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 2]
    pending_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 0]

    orders = []
    for doc in (
        db_obj["broker_orders"]
        .find({"trade_id": normalized_strategy_id})
        .sort("placed_at", -1)
        .limit(1000)
    ):
        doc["_id"] = str(doc.get("_id") or "")
        orders.append(doc)

    notifications = []
    feature_filters = [{"trade_id": normalized_strategy_id}]
    related_strategy_ids = {
        str(detailed_trade.get("strategy_id") or "").strip(),
        str(detailed_trade.get("source_strategy_id") or "").strip(),
    }
    related_strategy_ids.discard("")
    for related_id in related_strategy_ids:
        feature_filters.append({"strategy_id": related_id})

    for doc in (
        db_obj["algo_leg_feature_status"]
        .find({"$or": feature_filters})
        .sort("created_at", -1)
        .limit(1000)
    ):
        normalized_doc = dict(doc)
        normalized_doc["_id"] = str(doc.get("_id") or "")
        normalized_doc["type"] = str(doc.get("feature") or "").strip() or "feature_status"
        normalized_doc["event_type"] = normalized_doc["type"]
        normalized_doc["timestamp"] = (
            doc.get("triggered_at")
            or doc.get("updated_at")
            or doc.get("created_at")
            or ""
        )
        notifications.append(normalized_doc)

    notification_status = {}
    for item in notifications:
        event_type = str(item.get("event_type") or item.get("type") or "unknown").strip() or "unknown"
        notification_status[event_type] = notification_status.get(event_type, 0) + 1

    trade_notifications = []
    for doc in (
        db_obj["algo_trade_notification"]
        .find({"$or": feature_filters})
        .sort("timestamp", -1)
        .limit(1000)
    ):
        normalized_doc = dict(doc)
        normalized_doc["_id"] = str(doc.get("_id") or "")
        trade_notifications.append(normalized_doc)

    return {
        "success": True,
        "view_type": "strategy",
        "strategy_id": normalized_strategy_id,
        "group_id": str(((detailed_trade.get("portfolio") or {}).get("group_id")) or "").strip(),
        "activation_mode": str(detailed_trade.get("activation_mode") or normalized_status),
        "trade": detailed_trade,
        "summary": {
            "mtm": trade_mtm,
            "open_positions": len(open_legs),
            "closed_positions": len(closed_legs),
            "pending_positions": len(pending_legs),
            "broker_orders_count": len(orders),
            "notifications_count": len(notifications),
        },
        "legs": {
            "all": legs,
            "open": open_legs,
            "closed": closed_legs,
            "pending": pending_legs,
            "pending_feature_legs": pending_feature_legs,
        },
        "broker_orders": orders,
        "open_orders": [
            order for order in orders
            if str(order.get("status") or "").strip().upper() in {"OPEN", "PENDING", "TRIGGER PENDING"}
        ],
        "notifications": notifications,
        "notification_status": notification_status,
        "trade_notifications": trade_notifications,
        "execution_config_base": raw_trade.get("execution_config_base") if isinstance(raw_trade.get("execution_config_base"), dict) else {},
        "execution_config_extra": raw_trade.get("execution_config_extra") if isinstance(raw_trade.get("execution_config_extra"), dict) else {},
    }


def _aggregate_group_trade_history_payload(group_id: str, normalized_status: str, payloads: list[dict]):
    valid_payloads = [payload for payload in payloads if isinstance(payload, dict)]
    if not valid_payloads:
        raise HTTPException(status_code=404, detail="Strategy trade history not found for this group_id")

    primary_payload = valid_payloads[0]
    primary_trade = primary_payload.get("trade") if isinstance(primary_payload.get("trade"), dict) else {}
    group_name = str(((primary_trade.get("portfolio") or {}).get("group_name")) or "").strip() or f"Group {group_id}"
    strategy_names = []
    tickers = set()
    broker_labels = set()
    user_id = ""
    all_legs = []
    open_legs = []
    closed_legs = []
    pending_legs = []
    pending_feature_legs = []
    broker_orders = []
    notifications = []
    trade_notifications = []
    strategy_execution_configs = []
    notification_status = {}
    total_mtm = 0.0

    for payload in valid_payloads:
        trade = payload.get("trade") if isinstance(payload.get("trade"), dict) else {}
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        legs = payload.get("legs") if isinstance(payload.get("legs"), dict) else {}
        trade_name = str(trade.get("name") or "").strip()
        if trade_name:
            strategy_names.append(trade_name)
        ticker = str(trade.get("ticker") or trade.get("underlying") or "").strip()
        if ticker:
            tickers.add(ticker)
        broker_label = str(
            ((trade.get("broker_details") or {}).get("broker_name"))
            or ((trade.get("broker_details") or {}).get("display_name"))
            or trade.get("broker_label")
            or trade.get("broker")
            or ""
        ).strip()
        if broker_label:
            broker_labels.add(broker_label)
        if not user_id:
            user_id = str(trade.get("user_id") or "").strip()

        total_mtm += float(summary.get("mtm") or 0)
        all_legs.extend(legs.get("all") if isinstance(legs.get("all"), list) else [])
        open_legs.extend(legs.get("open") if isinstance(legs.get("open"), list) else [])
        closed_legs.extend(legs.get("closed") if isinstance(legs.get("closed"), list) else [])
        pending_legs.extend(legs.get("pending") if isinstance(legs.get("pending"), list) else [])
        pending_feature_legs.extend(legs.get("pending_feature_legs") if isinstance(legs.get("pending_feature_legs"), list) else [])
        broker_orders.extend(payload.get("broker_orders") if isinstance(payload.get("broker_orders"), list) else [])
        notifications.extend(payload.get("notifications") if isinstance(payload.get("notifications"), list) else [])
        trade_notifications.extend(payload.get("trade_notifications") if isinstance(payload.get("trade_notifications"), list) else [])

        for key, value in (payload.get("notification_status") or {}).items():
            normalized_key = str(key or "").strip() or "unknown"
            notification_status[normalized_key] = notification_status.get(normalized_key, 0) + int(value or 0)

        strategy_execution_configs.append({
            "strategy_id": str(trade.get("_id") or payload.get("strategy_id") or "").strip(),
            "name": str(trade.get("name") or "").strip(),
            "execution_config_base": payload.get("execution_config_base") if isinstance(payload.get("execution_config_base"), dict) else {},
            "execution_config_extra": payload.get("execution_config_extra") if isinstance(payload.get("execution_config_extra"), dict) else {},
        })

    broker_orders.sort(key=lambda item: str((item or {}).get("placed_at") or ""), reverse=True)
    notifications.sort(
        key=lambda item: str(
            (item or {}).get("timestamp")
            or (item or {}).get("triggered_at")
            or (item or {}).get("updated_at")
            or (item or {}).get("created_at")
            or ""
        ),
        reverse=True,
    )
    trade_notifications.sort(key=lambda item: str((item or {}).get("timestamp") or ""), reverse=True)

    strategy_count = len(valid_payloads)
    tickers_label = ", ".join(sorted(tickers)) if tickers else "Multiple"
    broker_label_text = ", ".join(sorted(broker_labels)) if broker_labels else (primary_trade.get("broker") or "-")
    trade = deepcopy(primary_trade)
    trade["_id"] = group_id
    trade["name"] = f"{group_name} ({strategy_count})"
    trade["ticker"] = tickers_label
    trade["user_id"] = user_id or str(trade.get("user_id") or "")
    trade["activation_mode"] = normalized_status
    trade["broker_label"] = broker_label_text
    portfolio_meta = trade.get("portfolio") if isinstance(trade.get("portfolio"), dict) else {}
    portfolio_meta["group_id"] = group_id
    portfolio_meta["group_name"] = group_name
    portfolio_meta["strategy_count"] = strategy_count
    trade["portfolio"] = portfolio_meta
    trade["strategy_names"] = strategy_names
    trade["status"] = trade.get("status") or "Group"

    return {
        "success": True,
        "view_type": "group",
        "group_id": group_id,
        "strategy_id": "",
        "activation_mode": normalized_status,
        "trade": trade,
        "summary": {
            "mtm": round(total_mtm, 2),
            "open_positions": len(open_legs),
            "closed_positions": len(closed_legs),
            "pending_positions": len(pending_legs),
            "broker_orders_count": len(broker_orders),
            "notifications_count": len(notifications),
            "strategy_count": strategy_count,
        },
        "legs": {
            "all": all_legs,
            "open": open_legs,
            "closed": closed_legs,
            "pending": pending_legs,
            "pending_feature_legs": pending_feature_legs,
        },
        "broker_orders": broker_orders[:1000],
        "open_orders": [
            order for order in broker_orders
            if str(order.get("status") or "").strip().upper() in {"OPEN", "PENDING", "TRIGGER PENDING"}
        ][:1000],
        "notifications": notifications[:1000],
        "notification_status": notification_status,
        "trade_notifications": trade_notifications[:1000],
        "execution_config_base": (strategy_execution_configs[0].get("execution_config_base") if strategy_execution_configs else {}),
        "execution_config_extra": (strategy_execution_configs[0].get("execution_config_extra") if strategy_execution_configs else {}),
        "strategy_execution_configs": strategy_execution_configs,
    }


def _aggregate_portfolio_trade_history_payload(portfolio_id: str, normalized_status: str, payloads: list[dict]):
    valid_payloads = [payload for payload in payloads if isinstance(payload, dict)]
    if not valid_payloads:
        raise HTTPException(status_code=404, detail="Strategy trade history not found for this portfolio")

    # Group individual strategy payloads by group_id
    groups_map: dict[str, list[dict]] = {}
    for payload in valid_payloads:
        gid = str(payload.get("group_id") or ((payload.get("trade") or {}).get("portfolio") or {}).get("group_id") or "").strip()
        if not gid:
            gid = "__no_group__"
        groups_map.setdefault(gid, []).append(payload)

    # Build per-group aggregations
    groups = []
    for gid, group_payloads in groups_map.items():
        group_agg = _aggregate_group_trade_history_payload(gid, normalized_status, group_payloads)
        group_agg["strategies"] = group_payloads
        groups.append(group_agg)

    # Sort groups by group_id for stable ordering
    groups.sort(key=lambda g: str(g.get("group_id") or ""))

    # Portfolio-level aggregation (sum of all strategies)
    portfolio_agg = _aggregate_group_trade_history_payload(portfolio_id, normalized_status, valid_payloads)
    trade = portfolio_agg.get("trade") if isinstance(portfolio_agg.get("trade"), dict) else {}
    portfolio_meta = trade.get("portfolio") if isinstance(trade.get("portfolio"), dict) else {}
    portfolio_name = str(portfolio_meta.get("group_name") or trade.get("name") or "").strip() or f"Portfolio {portfolio_id}"
    strategy_count = len(valid_payloads)
    group_count = len(groups)

    trade["_id"] = portfolio_id
    trade["name"] = f"{portfolio_name} ({strategy_count})"
    portfolio_meta["portfolio"] = portfolio_id
    trade["portfolio"] = portfolio_meta

    portfolio_agg["view_type"] = "portfolio"
    portfolio_agg["portfolio_id"] = portfolio_id
    portfolio_agg["group_id"] = str(portfolio_meta.get("group_id") or "").strip()
    portfolio_agg["strategy_id"] = ""
    portfolio_agg["trade"] = trade
    portfolio_agg["summary"]["group_count"] = group_count
    portfolio_agg["groups"] = groups
    portfolio_agg["strategies"] = valid_payloads
    return portfolio_agg
















# ─── Notification history ──────────────────────────────────────────────────────

















def _ensure_default_simulator_portfolios() -> None:
    col = _shared_mongo._db["simulator_portfolio"]
    for portfolio_name in _DEFAULT_PAPER_TRADE_PORTFOLIOS:
        if not col.find_one({"name": portfolio_name}, {"_id": 1}):
            col.insert_one({
                "name": portfolio_name,
                "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            })


def _str_id(doc: dict | None) -> dict | None:
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


def _sync_simulator_broker_with_market_session() -> bool:
    try:
        cfg = _shared_mongo._db["kite_market_config"].find_one(
            {"enabled": True},
            {"access_token": 1, "api_key": 1, "user_name": 1, "user_id": 1},
        ) or {}
        access_token = str(cfg.get("access_token") or "").strip()
        api_key = str(cfg.get("api_key") or "").strip()

        if access_token:
            # Reuse the same Kite app/session used by /live/kite-callback.
            _simulator_broker.kite = get_kite_instance(access_token)
            _simulator_broker.config["api_key"] = api_key or str(_simulator_broker.config.get("api_key") or "").strip() or "market-session"
            _simulator_broker.config["api_secret"] = str(_simulator_broker.config.get("api_secret") or "").strip() or "market-session"
            return True

        loaded = load_credentials_from_db(_shared_mongo)
        if not loaded:
            return False
        api_key, access_token = get_common_credentials()
        api_key = str(api_key or "").strip()
        access_token = str(access_token or "").strip()
        if not api_key or not access_token:
            return False
        _simulator_broker.kite = get_kite_instance(access_token)
        _simulator_broker.config["api_key"] = api_key
        _simulator_broker.config["api_secret"] = str(_simulator_broker.config.get("api_secret") or "").strip() or "market-session"
        return True
    except Exception:
        return False


@sim_router.post("/simulator/mini-strangle/start")
async def simulator_start_mini_strangle(request: MiniStrangleRequest) -> StreamingResponse:
    session_id = str(uuid.uuid4())
    stream = StreamingController(position_start_time=request.position_start_time)
    engine = StrategyEngine(request, stream)
    _simulator_sessions[session_id] = engine
    asyncio.create_task(_run_simulator_session(session_id, engine))
    return StreamingResponse(
        stream.stream(),
        media_type="text/event-stream",
        headers={
            "X-Session-ID": session_id,
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@sim_router.post("/simulator/mini-strangle/stop/{session_id}")
async def simulator_stop_mini_strangle(session_id: str) -> dict:
    engine = _simulator_sessions.get(session_id)
    if not engine:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    engine.stop()
    _simulator_sessions.pop(session_id, None)
    return {"status": "stopped", "session_id": session_id}


@sim_router.get("/simulator/mini-strangle/sessions")
async def simulator_list_sessions() -> dict:
    return {"active_sessions": list(_simulator_sessions.keys()), "count": len(_simulator_sessions)}


@sim_router.get("/simulator/monitor/start")
async def simulator_monitor_start(
    strategy_id: str = Query(default=""),
    portfolio_name: str = Query(default=""),
) -> HTMLResponse:
    try:
        market_ready = _sync_simulator_broker_with_market_session()
        if not market_ready or getattr(_simulator_broker, "kite", None) is None:
            return HTMLResponse(content=build_monitor_toggle_page(
                running=False,
                title="Simulator Monitor",
                status_text="Zerodha market session is not ready.",
                detail_text="Configure Zerodha market session first, then open this start page again.",
                start_href="./start",
                stop_href="./stop",
                status_href="./status",
            ))
        payload = await simulator_bridge_start(
            _simulator_broker.kite,
            _shared_mongo._db["simulator_strategy"],
            _shared_mongo._db,
        )
        detail_parts = []
        if strategy_id:
            detail_parts.append(f"strategy_id={strategy_id}")
        if portfolio_name:
            detail_parts.append(f"portfolio_name={portfolio_name}")
        if payload.get("subscribed_tokens") is not None:
            detail_parts.append(f"subscribed_tokens={payload.get('subscribed_tokens')}")
        return HTMLResponse(content=build_monitor_toggle_page(
            running=True,
            title="Simulator Monitor",
            status_text=str(payload.get("message") or payload.get("status") or "Monitor started"),
            detail_text=" | ".join(detail_parts) or "Monitor is running. Click Stop to stop the background monitor.",
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ))
    except Exception as exc:
        return HTMLResponse(content=build_monitor_toggle_page(
            running=False,
            title="Simulator Monitor",
            status_text="Failed to start monitor.",
            detail_text=str(exc),
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ), status_code=500)


@sim_router.post("/simulator/monitor/start")
async def simulator_monitor_start_post(
    strategy_id: str = Query(default=""),
    portfolio_name: str = Query(default=""),
) -> dict:
    try:
        market_ready = _sync_simulator_broker_with_market_session()
        if not market_ready or getattr(_simulator_broker, "kite", None) is None:
            return {
                "status": "error",
                "message": "Simulator market session not ready. Configure Zerodha market session first.",
            }
        payload = await simulator_bridge_start(
            _simulator_broker.kite,
            _shared_mongo._db["simulator_strategy"],
            _shared_mongo._db,
        )
        if strategy_id or portfolio_name:
            payload["requested_strategy_id"] = strategy_id
            payload["requested_portfolio_name"] = portfolio_name
        return payload
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/monitor/stop")
async def simulator_monitor_stop() -> HTMLResponse:
    try:
        _sync_simulator_broker_with_market_session()
        payload = await simulator_bridge_stop(
            getattr(_simulator_broker, "kite", None),
            _shared_mongo._db["simulator_strategy"],
            _shared_mongo._db,
        )
        return HTMLResponse(content=build_monitor_toggle_page(
            running=False,
            title="Simulator Monitor",
            status_text=str(payload.get("message") or payload.get("status") or "Monitor stopped"),
            detail_text="Monitor is stopped. Click Start to start it again.",
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ))
    except Exception as exc:
        return HTMLResponse(content=build_monitor_toggle_page(
            running=False,
            title="Simulator Monitor",
            status_text="Failed to stop monitor.",
            detail_text=str(exc),
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ), status_code=500)


@sim_router.post("/simulator/monitor/stop")
async def simulator_monitor_stop_post() -> dict:
    try:
        _sync_simulator_broker_with_market_session()
        return await simulator_bridge_stop(
            getattr(_simulator_broker, "kite", None),
            _shared_mongo._db["simulator_strategy"],
            _shared_mongo._db,
        )
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/monitor/status")
async def simulator_monitor_status() -> dict:
    _sync_simulator_broker_with_market_session()
    return await simulator_bridge_status(
        getattr(_simulator_broker, "kite", None),
        _shared_mongo._db["simulator_strategy"],
        _shared_mongo._db,
    )


@sim_router.get("/simulator/monitor/reentry-status")
async def simulator_monitor_reentry_status() -> dict:
    _sync_simulator_broker_with_market_session()
    return await simulator_bridge_reentry_status(
        getattr(_simulator_broker, "kite", None),
        _shared_mongo._db["simulator_strategy"],
        _shared_mongo._db,
    )


# ── Simulator risk monitor (SL/Target/hedge auto-exit on simulator_triggers
# / simulator_portfolio_triggers) — separate engine from the monitor above,
# see features/simulator_risk_monitor.py. Same start/stop/status page pattern
# as /simulator/monitor/* but its own toggle, since starting that one must
# never implicitly arm this one (real broker exit orders) or vice versa.

@sim_router.get("/simulator/risk-monitor/start")
async def simulator_risk_monitor_start_page() -> HTMLResponse:
    status = simulator_risk_monitor.start()
    return HTMLResponse(content=build_monitor_toggle_page(
        running=status["running"],
        title="Simulator Risk Monitor",
        status_text=f"Watching {status['legs_watched']} leg(s), {status['baskets_watched']} basket(s).",
        detail_text="Auto-fires real broker exit orders on saved SL/Target hits. Click Stop to disarm.",
        start_href="./start",
        stop_href="./stop",
        status_href="./status",
    ))


@sim_router.post("/simulator/risk-monitor/start")
async def simulator_risk_monitor_start_post() -> dict:
    return simulator_risk_monitor.start()


@sim_router.get("/simulator/risk-monitor/stop")
async def simulator_risk_monitor_stop_page() -> HTMLResponse:
    status = simulator_risk_monitor.stop()
    return HTMLResponse(content=build_monitor_toggle_page(
        running=status["running"],
        title="Simulator Risk Monitor",
        status_text="Stopped — no further SL/Target checks or auto-exits until restarted.",
        detail_text="Click Start to re-arm.",
        start_href="./start",
        stop_href="./stop",
        status_href="./status",
    ))


@sim_router.post("/simulator/risk-monitor/stop")
async def simulator_risk_monitor_stop_post() -> dict:
    return simulator_risk_monitor.stop()


@sim_router.get("/simulator/risk-monitor/status")
async def simulator_risk_monitor_status() -> dict:
    return simulator_risk_monitor.get_status()


@sim_router.get("/simulator/health")
async def simulator_health() -> dict:
    return {"status": "ok"}


@sim_router.get("/simulator/zerodha/status")
async def simulator_zerodha_status() -> dict:
    cfg = _shared_mongo._db["kite_market_config"].find_one(
        {"enabled": True},
        {"access_token": 1, "api_key": 1, "user_name": 1, "user_id": 1},
    ) or {}
    market_ready = _sync_simulator_broker_with_market_session()
    connected, profile = _simulator_broker.is_connected() if market_ready else (False, None)
    stored_user_name = str(cfg.get("user_name") or "").strip() or None
    stored_user_id = str(cfg.get("user_id") or "").strip() or None
    return {
        "connected": bool(connected or market_ready),
        "has_config": bool(_simulator_broker.has_config() or market_ready),
        "user_name": profile.get("user_name") if profile else stored_user_name,
        "user_id": profile.get("user_id") if profile else stored_user_id,
    }


@sim_router.post("/simulator/zerodha/config")
async def simulator_zerodha_save_config(req: ZerodhaConfigRequest, current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    try:
        _simulator_broker.save_config(req.api_key, req.api_secret)
        return {"status": "ok", "message": "Config saved"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/zerodha/has-config")
async def simulator_zerodha_has_config() -> dict:
    market_ready = _sync_simulator_broker_with_market_session()
    return {"has_config": bool(_simulator_broker.has_config() or market_ready)}


@sim_router.get("/simulator/zerodha/config")
async def simulator_zerodha_get_config(current_user: dict = Depends(app_auth.require_current_user)) -> dict:
    # require_current_user (not get_current_user) — this must stay auth-gated
    # even if AUTH_ENFORCEMENT_ENABLED is ever flipped off elsewhere, since it
    # exposes broker app credentials. Admin-only: this is the Zerodha app's
    # own api_key/secret, not anything scoped to a single user.
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    return {
        "status": "ok",
        "api_key": _simulator_broker.config.get("api_key", ""),
        # Never echo the raw secret back over the wire once it's saved —
        # nothing in the frontend reads this field, it only ever needed to
        # confirm a value is configured.
        "api_secret_configured": bool(_simulator_broker.config.get("api_secret")),
    }


@sim_router.get("/simulator/zerodha/login-url")
async def simulator_zerodha_login_url() -> dict:
    try:
        _sync_simulator_broker_with_market_session()
        url = _simulator_broker.get_login_url()
        return {"status": "ok", "login_url": url}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/zerodha/callback", response_class=HTMLResponse)
async def simulator_zerodha_callback(request_token: str = "", action: str = "", status: str = ""):
    if status == "success" and request_token:
        try:
            data = _simulator_broker.generate_session(request_token)
            user = data.get("user_name", "User")
            return HTMLResponse(f"""<!DOCTYPE html>
<html><head><title>Zerodha Login</title></head>
<body style="font-family:sans-serif;text-align:center;padding:40px;background:#f0f9f4;">
  <h2 style="color:#1a7a3c;">&#10003; Login Successful!</h2>
  <p>Welcome, <strong>{user}</strong></p>
  <p>Fetching live option chain...</p>
  <script>
    if(window.opener) {{
      window.opener.postMessage({{type:'zerodha_auth',success:true,user:'{user}'}}, '*');
    }}
    setTimeout(function(){{ window.close(); }}, 1500);
  </script>
</body></html>""")
        except Exception as exc:
            err_msg = str(exc)
            hint = ""
            if "invalid" in err_msg.lower() or "expired" in err_msg.lower():
                hint = "<p style='font-size:12px;color:#888;'>Request token expires in ~2 minutes. Please login again fresh.</p>"
            err_safe = err_msg.replace("'", "\\'")
            return HTMLResponse(f"""<!DOCTYPE html>
<html><head><title>Zerodha Login Failed</title></head>
<body style="font-family:sans-serif;text-align:center;padding:40px;background:#fff4f4;">
  <h2 style="color:#c0392b;">&#10007; Login Failed</h2>
  <p><strong>{err_msg}</strong></p>
  {hint}
  <p style="margin-top:20px;"><a href="/algo/simulator/zerodha/login-url-redirect" style="color:#1a7a3c;font-weight:bold;">&#8594; Try Login Again</a></p>
  <script>
    if(window.opener) {{
      window.opener.postMessage({{type:'zerodha_auth',success:false,error:'{err_safe}'}}, '*');
    }}
    setTimeout(function(){{ window.close(); }}, 5000);
  </script>
</body></html>""")
    return HTMLResponse("""<!DOCTYPE html>
<html><head><title>Zerodha</title></head>
<body style="font-family:sans-serif;text-align:center;padding:40px;">
  <p>Invalid callback. Please try again.</p>
</body></html>""")


@sim_router.get("/simulator/zerodha/login-url-redirect")
async def simulator_zerodha_login_url_redirect():
    try:
        url = _simulator_broker.get_login_url()
        return RedirectResponse(url)
    except Exception as exc:
        return HTMLResponse(f"<p>Error: {exc}</p>")


@sim_router.get("/simulator/zerodha/market-stats")
async def simulator_zerodha_market_stats(symbol: str = "nifty") -> dict:
    try:
        _sync_simulator_broker_with_market_session()
        data = _simulator_broker.get_market_stats(symbol)
        return {"status": "success", "data": data}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/zerodha/live-option-chain")
async def simulator_zerodha_live_option_chain(symbol: str = "nifty", near: bool = False, expiries: str = "") -> dict:
    try:
        _sync_simulator_broker_with_market_session()
        extra = [e.strip() for e in expiries.split(",") if e.strip()] if expiries else []
        result = _simulator_broker.get_live_option_chain(symbol, near_expiry_only=near, extra_expiries=extra)
        rows = result.get("rows", result) if isinstance(result, dict) else result
        stats = result.get("market_stats") if isinstance(result, dict) else None
        return {"status": "success", "count": len(rows), "data": rows, "market_stats": stats}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/get-market-holidays")
async def simulator_get_market_holidays() -> dict:
    try:
        dates = [
            doc["date"]
            for doc in _shared_mongo._db["market_holidays"].find({}, {"_id": 0, "date": 1})
            if "date" in doc
        ]
        return {"status": "success", "holidays": sorted(dates)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/get-option-chain")
async def simulator_get_option_chain(timestamp: str = Query(...)) -> dict:
    try:
        data = list(_shared_mongo._db["option_chain_historical_data"].find({"timestamp": timestamp}, {"_id": 0}))
        return {"status": "success", "timestamp": timestamp, "count": len(data), "data": data}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/lot-size")
async def simulator_get_lot_size(instrument: str = "nifty") -> dict:
    try:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        symbol = str(instrument or "nifty").upper()
        doc = _shared_mongo._db["lot_sizes"].find_one(
            {
                "underlying": symbol,
                "from_date": {"$lte": today},
                "$or": [
                    {"to_date": None},
                    {"to_date": {"$exists": False}},
                    {"to_date": {"$gte": today}},
                ],
            },
            sort=[("from_date", -1)],
        )
        if doc:
            return {"instrument": symbol, "lot_size": int(doc["lot_size"])}
        defaults = {"NIFTY": 75, "BANKNIFTY": 15, "FINNIFTY": 40, "MIDCPNIFTY": 120, "SENSEX": 10}
        return {"instrument": symbol, "lot_size": defaults.get(symbol, 75)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/positions/all")
async def simulator_all_positions(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Generic simulator positions endpoint.
    Broker name is intentionally not exposed in the URL so the frontend can keep
    using the same route even if the active broker changes later.
    """
    try:
        from features.broker_gateway import _active_broker as _get_active_broker_name

        active_broker = str(_get_active_broker_name() or "").strip().lower()
        if active_broker == "dhan":
            payload = await asyncio.to_thread(_fetch_dhan_broker_option_positions, _shared_mongo, include_broker_status=False)
            return {
                "status": "success" if payload.get("ok") else "error",
                "broker": active_broker or "unknown",
                "positions": payload.get("open_positions") or [],
                "underlyings": payload.get("underlyings") or {},
                "token_market_data": payload.get("token_market_data") or [],
                "detail": payload.get("detail") or "",
            }

        return {
            "status": "error",
            "broker": active_broker or "unknown",
            "detail": f"Simulator positions endpoint is not implemented for broker '{active_broker or 'unknown'}' yet.",
            "positions": [],
            "token_market_data": [],
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc), "positions": [], "token_market_data": []}


@sim_router.get("/simulator/positions/broker-status")
async def simulator_positions_broker_status(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        payload = await asyncio.to_thread(_fetch_dhan_broker_option_positions, _shared_mongo, include_broker_status=True)
        return {
            "status": "success" if payload.get("ok") else "error",
            "broker_status": payload.get("broker_status") or [],
            "detail": payload.get("detail") or "",
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc), "broker_status": []}


@sim_router.post("/simulator/positions/by-broker")
async def simulator_positions_by_broker(body: SimulatorBrokerPositionsRequest, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        broker_id = str(body.broker_id or "").strip()
        payload = await asyncio.to_thread(
            _fetch_dhan_broker_option_positions,
            _shared_mongo,
            selected_broker_id=broker_id or None,
            include_broker_status=False,
        )
        return {
            "status": "success" if payload.get("ok") else "error",
            "broker_id": broker_id,
            "positions": payload.get("open_positions") or [],
            "underlyings": payload.get("underlyings") or {},
            "token_market_data": payload.get("token_market_data") or [],
            "detail": payload.get("detail") or "",
        }
    except Exception as exc:
        return {
            "status": "error",
            "broker_id": str(body.broker_id or "").strip(),
            "message": str(exc),
            "positions": [],
            "token_market_data": [],
        }


_manual_order_kite_cache: dict[tuple, dict] = {}
_manual_order_kite_cache_date: str = ""


def _fetch_manual_order_kite_cache(raw_db, kite_doc: dict | None) -> dict[tuple, dict]:
    """
    Same shape/keying as spot_atm_utils._load_kite_instruments(), fetched directly with a
    specific Kite account's own credentials instead of going through that shared helper —
    which silently skips fetching (returns its empty cache) whenever Dhan is the active
    market-data feed broker, a global/unrelated setting that has nothing to do with whether
    a real Kite account is configured for placing this order.
    """
    global _manual_order_kite_cache, _manual_order_kite_cache_date
    today = datetime.now().strftime("%Y-%m-%d")
    if _manual_order_kite_cache_date == today and _manual_order_kite_cache:
        return _manual_order_kite_cache

    doc = kite_doc
    if doc is None:
        for candidate in raw_db["broker_configuration"].find({"broker_type": "live"}):
            name = str(candidate.get("broker_name") or candidate.get("name") or "").lower()
            if ("kite" in name or "zerodha" in name) and candidate.get("api_key") and candidate.get("access_token"):
                doc = candidate
                break
    if not doc:
        return {}

    try:
        from kiteconnect import KiteConnect  # type: ignore

        kite = KiteConnect(api_key=str(doc.get("api_key") or "").strip())
        kite.set_access_token(str(doc.get("access_token") or "").strip())
        new_cache: dict[tuple, dict] = {}
        for segment in ("NFO", "BFO"):
            for inst in kite.instruments(segment):
                name = str(inst.get("name") or "").strip().upper()
                inst_type = str(inst.get("instrument_type") or "").strip().upper()
                exp = inst.get("expiry")
                stk = inst.get("strike")
                sym = str(inst.get("tradingsymbol") or "").strip()
                if not (name and inst_type in ("CE", "PE") and exp and stk is not None and sym):
                    continue
                try:
                    exp_str = exp.strftime("%Y-%m-%d")
                except AttributeError:
                    exp_str = str(exp)[:10]
                new_cache[(name, exp_str, float(stk), inst_type)] = {
                    "symbol": sym,
                    "exchange": str(inst.get("exchange") or segment),
                }
        _manual_order_kite_cache = new_cache
        _manual_order_kite_cache_date = today
        return new_cache
    except Exception as exc:
        log.debug("manual order kite instrument fetch error: %s", exc)
        return {}


def _resolve_manual_order_symbol(leg: "ManualOrderLeg", raw_db, kite_doc: dict | None = None) -> tuple[str, str] | None:
    """
    Kite-native (underlying, expiry, strike, option_type) → (tradingsymbol, exchange).
    Same instrument metadata _to_flattrade_symbol() already uses for the FlatTrade
    conversion — account-agnostic, so it's safe to resolve this way regardless of
    which broker_id is actually placing the order.
    """
    from features.spot_atm_utils import _load_kite_instruments

    cache = _load_kite_instruments()
    if not cache:
        cache = _fetch_manual_order_kite_cache(raw_db, kite_doc)

    key = (
        leg.underlying.strip().upper(),
        leg.expiry.strip()[:10],
        float(leg.strike),
        leg.option_type.strip().upper(),
    )
    inst = cache.get(key)
    if not inst:
        return None
    return str(inst["symbol"]), str(inst["exchange"])


def _resolve_dhan_security(leg: "ManualOrderLeg", raw_db) -> dict | None:
    """
    (underlying, expiry, strike, option_type) → Dhan's own securityId/symbol/exchangeSegment,
    from the same active_option_tokens collection execution_socket.py already keys positions off
    of. Dhan identifies instruments by numeric securityId, not a tradingsymbol string, so this
    doesn't reuse _resolve_manual_order_symbol (that one resolves the Kite-style symbol).
    """
    doc = raw_db["active_option_tokens"].find_one({
        "broker": "dhan",
        "instrument": leg.underlying.strip().upper(),
        "expiry": leg.expiry.strip()[:10],
        "strike": float(leg.strike),
        "option_type": leg.option_type.strip().upper(),
    })
    if not doc:
        return None
    security_id = str(doc.get("token") or "").strip()
    if not security_id:
        return None
    return {
        "security_id": security_id,
        "symbol": str(doc.get("symbol") or "").strip(),
        "exchange_segment": str(doc.get("ws_segment") or "").strip().upper() or "NSE_FNO",
    }


async def _prefetch_dhan_quotes_for_legs(legs: list["ManualOrderLeg"], raw_db) -> dict[str, dict]:
    """
    Batches every leg's Dhan quote into as few REST calls as possible — one per distinct
    exchange segment (almost always just one call total for a whole multi-leg webhook/
    basket, since every leg is normally NSE_FNO), instead of _fetch_dhan_quote_for_leg's
    default one-call-per-leg. Dhan's /marketfeed/quote accepts up to 1000 instruments per
    request across segments at 1 req/sec (docs.dhanhq.co/api/v2/market-quote/get-quote) —
    firing one call per leg was burning through that budget on every multi-leg fire, which
    is what was causing "429 Too many requests... user may be blocked".

    Returns the same {security_id: {"ltp","oi","bid","ask","prev_close","source"}} shape
    _fetch_dhan_market_data returns — pass straight through as the `prefetched` argument to
    _fetch_dhan_quote_for_leg/_resolve_mpp_price/_resolve_ltp_price for every leg in this
    same batch; each still resolves its own security_id via _resolve_dhan_security (a plain
    Mongo find_one, no rate limit) and looks it up here instead of fetching individually.
    """
    resolved_list = await asyncio.gather(*(asyncio.to_thread(_resolve_dhan_security, leg, raw_db) for leg in legs))
    by_segment: dict[str, list[int]] = {}
    for resolved in resolved_list:
        if not resolved:
            continue
        by_segment.setdefault(resolved["exchange_segment"], []).append(int(resolved["security_id"]))
    quotes: dict[str, dict] = {}
    for segment, sec_ids in by_segment.items():
        data = await asyncio.to_thread(_fetch_dhan_market_data, segment, sec_ids, _shared_mongo)
        quotes.update(data)
    return quotes


async def _fetch_dhan_quote_for_leg(leg: "ManualOrderLeg", raw_db, prefetched: dict[str, dict] | None = None) -> dict | None:
    """
    Resolves this leg's Dhan security_id and returns its live quote {"symbol","ltp","bid","ask"}.
    Returns None if Dhan has no contract match for this leg at all.

    Shared by _resolve_mpp_price and _resolve_ltp_price — every order's price, regardless of
    which broker (FlatTrade/Kite/Dhan) actually executes it, is read from this one feed. Dhan
    already streams/queries the full F&O chain, whereas Kite's own feed isn't even running
    unless Kite is the active market-data broker (kite_market_config) — and the broker that
    places the order has nothing to do with which one is the best price source.

    prefetched: pass the result of _prefetch_dhan_quotes_for_legs (called once for every leg
    in the same basket) to look the quote up there instead of making this leg's own separate
    REST call — see that function's docstring for why. None (default) preserves the old
    one-call-per-leg behavior for a lone/standalone caller.
    """
    resolved = await asyncio.to_thread(_resolve_dhan_security, leg, raw_db)
    if not resolved:
        return None
    if prefetched is not None:
        quote = prefetched.get(resolved["security_id"], {})
    else:
        quote = (await asyncio.to_thread(
            _fetch_dhan_market_data, resolved["exchange_segment"], [int(resolved["security_id"])], _shared_mongo,
        )).get(resolved["security_id"], {})
    return {
        "symbol": resolved["symbol"],
        "ltp": float(quote.get("ltp") or 0),
        "bid": float(quote.get("bid") or 0),
        "ask": float(quote.get("ask") or 0),
        # "ws" (in-memory, ~instant) / "rest" (Dhan API round trip, rate-gated) / "cache"
        # (last-good fallback) / absent if no quote at all — see _fetch_dhan_market_data.
        "source": quote.get("source"),
    }


def _notify_mpp_ltp_price_unresolved(kind: str, message: str) -> None:
    """
    Shared by _resolve_mpp_price/_resolve_ltp_price — every failure to resolve a real,
    fresh price pages admin via Telegram instead of failing silently, since the only other
    signal is a 0.0 return the caller must already be checking for.
    """
    print(f"[{kind} PRICE] {message}", flush=True)
    try:
        from features.telegram_notifier import notify_admin
        notify_admin(f"{kind.lower()}_price_unresolved", message)
    except Exception as exc:
        log.warning("[%s PRICE] notify_admin failed: %s", kind, exc)


async def _resolve_mpp_price(leg: "ManualOrderLeg", raw_db, prefetched: dict[str, dict] | None = None) -> float:
    """
    MPP's bid + protection% / ask - protection% formula, priced off Dhan's feed regardless of
    the execution broker (see _fetch_dhan_quote_for_leg). The order itself still goes out
    through whichever broker/symbol the caller resolved separately.

    Returns 0.0 — NEVER leg.price or ltp as a stand-in for a missing bid/ask — when Dhan has
    no contract match or no live depth on the side this leg needs. Every caller (see
    _simulator_place_manual_order_core) already treats a <= 0 return as "unresolved" and
    aborts the order instead of placing it; substituting ltp here would silently hand back a
    fabricated "protected" price with no real depth behind it — exactly the risk that made
    this whole function worth having in the first place.

    prefetched: see _fetch_dhan_quote_for_leg — pass _prefetch_dhan_quotes_for_legs' result
    when pricing every leg of the same basket, so they share one REST call instead of one each.
    """
    from features.live_order_manager import _mpp_protection_pct, _clamp_limit_price

    quote = await _fetch_dhan_quote_for_leg(leg, raw_db, prefetched)
    if not quote:
        _notify_mpp_ltp_price_unresolved(
            "MPP", f"No Dhan contract match for {leg.option_type} {leg.strike} exp={leg.expiry} — order NOT placed.",
        )
        return 0.0

    ltp = quote["ltp"]
    bid = quote["bid"]
    ask = quote["ask"]
    is_buy = leg.side == "BUY"
    # Only the side this order actually needs (bid for BUY, ask for SELL) has to be live —
    # but never substitute ltp for it if it's missing.
    if (is_buy and bid <= 0) or (not is_buy and ask <= 0):
        _notify_mpp_ltp_price_unresolved(
            "MPP",
            f"No live depth for {quote.get('symbol')} (bid={bid}, ask={ask}) — order NOT placed.",
        )
        return 0.0

    # NSE's MPP protection band is sized differently for options vs futures (tighter for
    # futures — see _mpp_protection_pct's docstring) — a futures leg must not get priced
    # with the wider option band.
    pct = _mpp_protection_pct(ltp, is_option=leg.option_type.strip().upper() != "FUT")
    base_price = bid if is_buy else ask
    raw_price = base_price * (1 + pct / 100) if is_buy else base_price * (1 - pct / 100)
    price = _clamp_limit_price(raw_price, is_buy)
    print(
        f"[MPP PRICE][dhan-feed] symbol={quote['symbol']} source={quote.get('source')} ltp={ltp} bid={bid} ask={ask} "
        f"pct={pct}% price={price} is_buy={is_buy}",
        flush=True,
    )
    return price


async def _resolve_ltp_price(leg: "ManualOrderLeg", raw_db, prefetched: dict[str, dict] | None = None) -> float:
    """
    "Execute At LTP" price source — same Dhan-feed-regardless-of-execution-broker principle as
    _resolve_mpp_price, just without the protection-band markup: submits a plain LIMIT order at
    Dhan's current ltp instead of trusting the order pad row's possibly-seconds-stale client-side
    ltp.

    Returns 0.0 — never leg.price — if Dhan has no match/quote yet; see _resolve_mpp_price's
    docstring for why no fallback price is used here.

    prefetched: see _fetch_dhan_quote_for_leg/_resolve_mpp_price.
    """
    quote = await _fetch_dhan_quote_for_leg(leg, raw_db, prefetched)
    if not quote or quote["ltp"] <= 0:
        _notify_mpp_ltp_price_unresolved(
            "LTP", f"No Dhan quote for {leg.option_type} {leg.strike} exp={leg.expiry} — order NOT placed.",
        )
        return 0.0
    print(f"[LTP PRICE][dhan-feed] symbol={quote['symbol']} source={quote.get('source')} ltp={quote['ltp']}", flush=True)
    return quote["ltp"]


async def _simulator_place_manual_order_core(body: ManualOrderRequest) -> dict:
    """
    Places real orders with the broker — this is live money, not a simulation.
    FlatTrade/Kite use their own place_order() already proven elsewhere in this
    codebase. Dhan goes straight to https://api.dhan.co/v2/orders (same direct-
    REST pattern already used for Dhan positions/quotes) — UNVERIFIED against a
    live order, unlike the other two: dhanhq SDK isn't installed, and this is
    adapted from an untested reference in the sibling option-algo repo. Test
    with one small/throwaway order before relying on it for size.
    """
    broker_id = str(body.broker_id or "").strip()
    print(f"[PLACE_ORDER] request broker_id={broker_id} legs={len(body.orders)} orders={[o.model_dump() for o in body.orders]}", flush=True)
    try:
        raw_db = _shared_mongo._db

        dhan_cfg = raw_db["kite_market_config"].find_one({"broker": "dhan"}) or {}
        if broker_id and broker_id == str(dhan_cfg.get("_id") or "").strip():
            dhan_client_id = str(dhan_cfg.get("user_id") or dhan_cfg.get("dhan_client_id") or "").strip()
            dhan_access_token = str(dhan_cfg.get("access_token") or "").strip()
            if not dhan_access_token or not dhan_client_id:
                print("[PLACE_ORDER][dhan] credentials not configured", flush=True)
                return {"status": "error", "message": "Dhan credentials not configured.", "results": []}

            from features.dhan_broker import get_dhan_instance
            from features.order_execution import place_broker_order

            dhan_order_type_map = {"LIMIT": "LIMIT", "MARKET": "MARKET", "SL": "SL"}
            dhan_adapter = get_dhan_instance(_shared_mongo, dhan_client_id, dhan_access_token)
            # One batched quote fetch for the whole basket instead of one REST call per leg —
            # see _prefetch_dhan_quotes_for_legs. This is what was tripping Dhan's 429 rate
            # limit on every multi-leg MPP/LTP fire.
            prefetched_quotes = await _prefetch_dhan_quotes_for_legs(body.orders, raw_db)

            async def _place_one_dhan_leg(leg: "ManualOrderLeg") -> dict:
                resolved = await asyncio.to_thread(_resolve_dhan_security, leg, raw_db)
                if not resolved:
                    print(f"[PLACE_ORDER][dhan] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}

                price = leg.price
                requested_type = leg.order_type
                if requested_type == "MPP":
                    price = await _resolve_mpp_price(leg, raw_db, prefetched_quotes)
                    if price <= 0:
                        print(f"[PLACE_ORDER][dhan] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    requested_type = "LIMIT"
                elif requested_type == "LTP":
                    price = await _resolve_ltp_price(leg, raw_db, prefetched_quotes)
                    if price <= 0:
                        print(f"[PLACE_ORDER][dhan] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    requested_type = "LIMIT"

                dhan_order_type = dhan_order_type_map.get(requested_type, "LIMIT")
                result = await asyncio.to_thread(
                    place_broker_order,
                    dhan_adapter,
                    tradingsymbol=resolved["symbol"],
                    exchange="NFO",
                    transaction_type="BUY" if leg.side == "BUY" else "SELL",
                    quantity=leg.quantity,
                    order_type=dhan_order_type,
                    product=leg.product,
                    price=price,
                    trigger_price=leg.trigger_price or 0.0,
                    context={"purpose": "manual_order_pad", "broker": "dhan", "symbol": resolved["symbol"]},
                )
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                # price here is whatever was actually submitted — the MPP/LTP-resolved fresh
                # quote, not the leg's own (possibly stale/zero) price — callers that persist a
                # position snapshot (e.g. _simulator_pt_webhook_create_strategy) use this as the
                # real entry_price instead of whatever was showing when the webhook was generated.
                return {"leg": leg.model_dump(), "status": "success", "order_id": result["order_id"], "price": price}

            # Every leg in the basket fires at once instead of waiting on the previous leg's
            # broker round-trip — for a multi-leg strategy that's the difference between the
            # whole basket landing together vs. legs getting staggered fills at drifting prices.
            dhan_results: list[dict] = await asyncio.gather(*(_place_one_dhan_leg(leg) for leg in body.orders))

            any_ok = any(r["status"] == "success" for r in dhan_results)
            all_ok = bool(dhan_results) and all(r["status"] == "success" for r in dhan_results)
            overall_status = "success" if all_ok else ("partial" if any_ok else "error")
            print(f"[PLACE_ORDER] done status={overall_status} results={dhan_results}", flush=True)
            return {"status": overall_status, "results": dhan_results}

        try:
            doc = raw_db["broker_configuration"].find_one({"_id": ObjectId(broker_id)})
        except Exception:
            doc = None
        if not doc:
            print(f"[PLACE_ORDER] broker account not found for broker_id={broker_id}", flush=True)
            return {"status": "error", "message": "Broker account not found.", "results": []}

        broker_name = str(doc.get("broker_name") or doc.get("name") or "").strip().lower()
        is_flattrade = "flattrade" in broker_name
        is_kite = "zerodha" in broker_name or "kite" in broker_name
        print(f"[PLACE_ORDER] resolved broker_name={broker_name} is_flattrade={is_flattrade} is_kite={is_kite}", flush=True)
        if not is_flattrade and not is_kite:
            print(f"[PLACE_ORDER] rejected — order placement not supported for broker_name={broker_name}", flush=True)
            return {"status": "error", "message": "Order placement isn't available for this broker yet.", "results": []}

        results: list[dict] = []

        if is_flattrade:
            from features.flattrade_broker import get_flattrade_instance

            adapter = get_flattrade_instance(str(doc.get("user_id") or ""), str(doc.get("access_token") or ""))
            if adapter is None:
                print("[PLACE_ORDER][flattrade] session not available", flush=True)
                return {"status": "error", "message": "FlatTrade session not available.", "results": []}
            # One batched quote fetch for the whole basket instead of one REST call per leg —
            # see _prefetch_dhan_quotes_for_legs (price source is always Dhan's feed regardless
            # of FlatTrade being the execution broker here).
            prefetched_quotes = await _prefetch_dhan_quotes_for_legs(body.orders, raw_db)

            async def _place_one_flattrade_leg(leg: "ManualOrderLeg") -> dict:
                resolved = await asyncio.to_thread(_resolve_manual_order_symbol, leg, raw_db)
                if not resolved:
                    print(f"[PLACE_ORDER][flattrade] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}
                symbol, exchange = resolved

                price = leg.price
                order_type = leg.order_type
                if order_type == "MPP":
                    # FlatTrade has no native MPP order type — "MPP" would silently fall back to
                    # a plain LIMIT at price=0 (rejected by the exchange) if sent through as-is.
                    # Price source is always Dhan's feed (see _resolve_mpp_price), independent of
                    # FlatTrade being the execution broker here.
                    price = await _resolve_mpp_price(leg, raw_db, prefetched_quotes)
                    if price <= 0:
                        print(f"[PLACE_ORDER][flattrade] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"
                elif order_type == "LTP":
                    # Same Dhan-feed-regardless-of-execution-broker principle — submit at Dhan's
                    # current ltp instead of trusting a possibly-stale client-side price.
                    price = await _resolve_ltp_price(leg, raw_db, prefetched_quotes)
                    if price <= 0:
                        print(f"[PLACE_ORDER][flattrade] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"

                print(
                    f"[PLACE_ORDER][flattrade] placing tradingsymbol={symbol} exchange={exchange} "
                    f"transaction_type={leg.side} quantity={leg.quantity} order_type={order_type} "
                    f"product={leg.product} price={price} trigger_price={leg.trigger_price}",
                    flush=True,
                )
                from features.order_execution import place_broker_order
                result = await asyncio.to_thread(
                    place_broker_order,
                    adapter,
                    tradingsymbol=symbol,
                    exchange=exchange,
                    transaction_type=leg.side,
                    quantity=leg.quantity,
                    order_type=order_type,
                    product=leg.product,
                    price=price,
                    trigger_price=leg.trigger_price,
                    context={"purpose": "manual_order_pad", "broker": "flattrade", "symbol": symbol},
                )
                print(f"[PLACE_ORDER][flattrade] response={result}", flush=True)
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                # See the Dhan branch's identical comment — price is the MPP/LTP-resolved fresh
                # quote actually submitted, not leg.price.
                return {"leg": leg.model_dump(), "status": "success", "order_id": result["order_id"], "price": price}

            # Whole basket fires together instead of one leg waiting on the previous leg's
            # broker round-trip — same reasoning as the Dhan branch above.
            results = await asyncio.gather(*(_place_one_flattrade_leg(leg) for leg in body.orders))
        else:
            from kiteconnect import KiteConnect  # type: ignore

            api_key = str(doc.get("api_key") or "").strip()
            access_token = str(doc.get("access_token") or "").strip()
            if not api_key or not access_token:
                print("[PLACE_ORDER][kite] session not available", flush=True)
                return {"status": "error", "message": "Kite session not available.", "results": []}
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            # One batched quote fetch for the whole basket instead of one REST call per leg —
            # see _prefetch_dhan_quotes_for_legs (price source is always Dhan's feed regardless
            # of Kite being the execution broker here).
            prefetched_quotes = await _prefetch_dhan_quotes_for_legs(body.orders, raw_db)

            async def _place_one_kite_leg(leg: "ManualOrderLeg") -> dict:
                # Resolve with this exact account's own token — instrument metadata fetched via
                # Dhan's feed wouldn't reflect this Kite account's session, and the shared cache
                # is empty whenever Dhan (not Kite) is the active market-data broker anyway.
                resolved = await asyncio.to_thread(_resolve_manual_order_symbol, leg, raw_db, doc)
                if not resolved:
                    print(f"[PLACE_ORDER][kite] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}
                symbol, exchange = resolved

                price = leg.price
                order_type = leg.order_type
                if order_type == "MPP":
                    # Kite has no native MPP order type either — price source is always Dhan's
                    # feed (see _resolve_mpp_price), independent of Kite being the execution
                    # broker here.
                    price = await _resolve_mpp_price(leg, raw_db, prefetched_quotes)
                    if price <= 0:
                        print(f"[PLACE_ORDER][kite] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"
                elif order_type == "LTP":
                    # Same Dhan-feed-regardless-of-execution-broker principle — submit at Dhan's
                    # current ltp instead of trusting a possibly-stale client-side price.
                    price = await _resolve_ltp_price(leg, raw_db, prefetched_quotes)
                    if price <= 0:
                        print(f"[PLACE_ORDER][kite] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"

                print(
                    f"[PLACE_ORDER][kite] placing tradingsymbol={symbol} exchange={exchange} "
                    f"transaction_type={leg.side} quantity={leg.quantity} order_type={order_type} "
                    f"product={leg.product} price={price} trigger_price={leg.trigger_price}",
                    flush=True,
                )
                from features.order_execution import place_broker_order
                result = await asyncio.to_thread(
                    place_broker_order,
                    kite,
                    tradingsymbol=symbol,
                    exchange=exchange,
                    transaction_type=leg.side,
                    quantity=leg.quantity,
                    order_type=order_type,
                    product=leg.product,
                    price=price or 0.0,
                    trigger_price=leg.trigger_price or 0.0,
                    variety=kite.VARIETY_REGULAR,
                    context={"purpose": "manual_order_pad", "broker": "kite", "symbol": symbol},
                )
                print(f"[PLACE_ORDER][kite] response={result}", flush=True)
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                # See the Dhan branch's identical comment — price is the MPP/LTP-resolved fresh
                # quote actually submitted, not leg.price.
                return {"leg": leg.model_dump(), "status": "success", "order_id": result["order_id"], "price": price}

            # Whole basket fires together instead of one leg waiting on the previous leg's
            # broker round-trip — same reasoning as the Dhan branch above.
            results = await asyncio.gather(*(_place_one_kite_leg(leg) for leg in body.orders))

        any_ok = any(r["status"] == "success" for r in results)
        all_ok = bool(results) and all(r["status"] == "success" for r in results)
        overall_status = "success" if all_ok else ("partial" if any_ok else "error")
        print(f"[PLACE_ORDER] done status={overall_status} results={results}", flush=True)
        return {
            "status": overall_status,
            "results": results,
        }
    except Exception as exc:
        print(f"[PLACE_ORDER] unhandled error={exc}", flush=True)
        return {"status": "error", "message": str(exc), "results": []}


@sim_router.post("/trade/positions/place-order")
async def simulator_place_manual_order(body: ManualOrderRequest, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Same route + wrapper as algo.trade's (both call the identical
    _simulator_place_manual_order_core defined above) — this service's own
    Order Pad "Trade" button posts to POSITIONS_API_BASE (this port) at this
    exact path, which had no route registered here before, only in algo.trade.
    """
    result = await _simulator_place_manual_order_core(body)
    try:
        from features.telegram_notifier import notify_user

        status = str(result.get("status") or "")
        leg_summary = ", ".join(
            f"{o.side} {o.underlying} {o.strike}{o.option_type} x{o.quantity}" for o in body.orders
        )
        if status == "success":
            notify_user("PT_ORDER_PLACED", f"Order placed — {leg_summary}", {"broker": body.broker_id})
        elif status in ("error", "partial"):
            notify_user(
                "PT_ORDER_FAILED" if status == "error" else "PT_ORDER_PARTIAL",
                f"Order {status} — {leg_summary} — {result.get('message', '')}",
                {"broker": body.broker_id},
            )
    except Exception as exc:
        print(f"[PLACE_ORDER] telegram notify error={exc}", flush=True)
    return result


@sim_router.get("/simulator/positions/debug/kite")
async def simulator_debug_kite_positions(broker_id: str = Query(default=""), current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Debug helper to inspect raw Kite/Zerodha positions and symbol parsing.
    Use this when simulator positions page shows "No positions found" for Kite.
    """
    try:
        from features.kite_broker_ws import parse_kite_tradingsymbol

        requested_broker_id = str(broker_id or "").strip()
        query: dict[str, Any] = {"broker_type": "live"}
        if requested_broker_id:
            try:
                query["_id"] = ObjectId(requested_broker_id)
            except Exception:
                return {
                    "status": "error",
                    "requested_broker_id": requested_broker_id,
                    "message": "Invalid broker_id",
                    "accounts": [],
                }

        accounts: list[dict[str, Any]] = []
        for doc in _shared_mongo._db["broker_configuration"].find(query):
            broker_name = str(doc.get("broker_name") or doc.get("name") or "").strip()
            if not any(name in broker_name.lower() for name in ("kite", "zerodha")):
                continue
            broker_doc_id = str(doc.get("_id") or "").strip()
            raw_access_token = str(doc.get("access_token") or "").strip()
            masked_access_token = ""
            if raw_access_token:
                if len(raw_access_token) <= 8:
                    masked_access_token = raw_access_token
                else:
                    masked_access_token = f"{raw_access_token[:4]}...{raw_access_token[-4:]}"
            print(
                "[KITE DEBUG DOC]",
                {
                    "broker_doc_id": broker_doc_id,
                    "broker_name": broker_name,
                    "has_access_token": bool(raw_access_token),
                    "access_token_len": len(raw_access_token),
                    "has_api_key": bool(str(doc.get("api_key") or "").strip()),
                },
                flush=True,
            )
            is_logged_in, _expired, session_message = _validate_broker_configuration_session(doc, _shared_mongo._db)
            accounts.append({
                "_id": broker_doc_id,
                "broker_name": broker_name,
                "account_id": str(doc.get("user_id") or "").strip(),
                "api_key": str(doc.get("api_key") or "").strip(),
                "access_token": str(doc.get("access_token") or "").strip(),
                "is_logged_in": is_logged_in,
                "login_url": "" if is_logged_in else f"/broker/kite/login?broker_doc_id={broker_doc_id}",
                "message": session_message,
                "debug_has_access_token": bool(raw_access_token),
                "debug_access_token_len": len(raw_access_token),
                "debug_has_api_key": bool(str(doc.get("api_key") or "").strip()),
                "debug_raw_doc_preview": {
                    "_id": broker_doc_id,
                    "name": str(doc.get("name") or "").strip(),
                    "broker_name": broker_name,
                    "broker_type": str(doc.get("broker_type") or "").strip(),
                    "user_id": str(doc.get("user_id") or "").strip(),
                    "user_name": str(doc.get("user_name") or "").strip(),
                    "broker_user_id": str(doc.get("broker_user_id") or "").strip(),
                    "has_access_token": bool(raw_access_token),
                    "access_token_masked": masked_access_token,
                    "access_token_len": len(raw_access_token),
                    "has_api_key": bool(str(doc.get("api_key") or "").strip()),
                    "api_key": str(doc.get("api_key") or "").strip(),
                    "login_time": str(doc.get("login_time") or "").strip(),
                    "updated_at": str(doc.get("updated_at") or "").strip(),
                    "redirect_url": str(doc.get("redirect_url") or "").strip(),
                    "postback_url": str(doc.get("postback_url") or "").strip(),
                },
            })

        debug_accounts: list[dict[str, Any]] = []
        for account in accounts:
            account_id = str(account.get("_id") or "").strip()
            broker_name = str(account.get("broker_name") or "").strip()
            account_user_id = str(account.get("account_id") or "").strip()
            access_token = str(account.get("access_token") or "").strip()
            is_logged_in = bool(account.get("is_logged_in"))

            account_debug: dict[str, Any] = {
                "broker_id": account_id,
                "broker_name": broker_name,
                "account_id": account_user_id,
                "api_key": str(account.get("api_key") or "").strip(),
                "is_logged_in": is_logged_in,
                "login_url": str(account.get("login_url") or "").strip(),
                "message": str(account.get("message") or "").strip(),
                "raw_count": 0,
                "raw_positions": [],
                "parsed_positions": [],
                "unresolved_symbols": [],
            }

            if not is_logged_in or not access_token:
                debug_accounts.append(account_debug)
                continue

            try:
                from kiteconnect import KiteConnect  # type: ignore

                api_key = str(account.get("api_key") or "").strip()
                if not api_key or not access_token:
                    account_debug["message"] = "Missing api_key or access_token in broker_configuration"
                    debug_accounts.append(account_debug)
                    continue
                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                raw_payload = kite.positions() if kite is not None else {}
                raw_rows = raw_payload.get("net") if isinstance(raw_payload, dict) else []
                raw_rows = raw_rows if isinstance(raw_rows, list) else []

                parsed_positions: list[dict[str, Any]] = []
                unresolved_symbols: list[dict[str, Any]] = []

                for row in raw_rows:
                    if not isinstance(row, dict):
                        continue
                    quantity = int(row.get("quantity") or 0)
                    tradingsymbol = str(row.get("tradingsymbol") or "").strip()
                    parsed = parse_kite_tradingsymbol(tradingsymbol)
                    row_debug = {
                        "tradingsymbol": tradingsymbol,
                        "quantity": quantity,
                        "product": str(row.get("product") or "").strip(),
                        "average_price": row.get("average_price"),
                        "last_price": row.get("last_price"),
                        "pnl": row.get("pnl"),
                    }
                    if parsed:
                        parsed_positions.append({**row_debug, **parsed})
                    else:
                        unresolved_symbols.append(row_debug)

                account_debug["raw_count"] = len(raw_rows)
                account_debug["raw_positions"] = raw_rows
                account_debug["parsed_positions"] = parsed_positions
                account_debug["unresolved_symbols"] = unresolved_symbols
            except Exception as exc:
                account_debug["message"] = str(exc)

            debug_accounts.append(account_debug)

        return {
            "status": "success",
            "requested_broker_id": requested_broker_id,
            "account_count": len(debug_accounts),
            "accounts": debug_accounts,
        }
    except Exception as exc:
        return {
            "status": "error",
            "requested_broker_id": str(broker_id or "").strip(),
            "message": str(exc),
            "accounts": [],
        }


@sim_router.get("/simulator/paper-trade/portfolios")
async def simulator_pt_list_portfolios(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        _ensure_default_simulator_portfolios()
        # Must match the string type _insert_simulator_strategy's auto-create
        # (and simulator_pt_update_strategy's) already write here — comparing
        # a raw ObjectId against a stored string never matches in Mongo, which
        # was silently hiding every portfolio auto-created alongside a saved
        # strategy from this list.
        current_user_id = _resolve_sim_user_id(current_user)
        filt: dict[str, Any] = {"$or": [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]}
        docs = list(_shared_mongo._db["simulator_portfolio"].find(filt, {"_id": 1, "name": 1}))
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return {"status": "success", "portfolios": docs}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.post("/simulator/paper-trade/portfolios")
async def simulator_pt_create_portfolio(body: PTPortfolioIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        col = _shared_mongo._db["simulator_portfolio"]
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


@sim_router.post("/simulator/paper-trade/triggers")
async def simulator_pt_save_trigger(body: PTTriggerIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Upserts the SL/Target a leg's "Add Alert"/"Update Alert" toggle was set to, keyed by
    (broker_id, leg_id) — always overwrites rather than no-op'ing on an existing doc, since
    re-saving must always take the latest values. entry_price/quantity are stored as the
    snapshot _fetch_dhan_broker_option_positions() compares future polls against to decide
    whether this trigger is still valid (see its drift-check block) — this endpoint itself
    does no validation, it just records "these were the values when the user last confirmed."
    """
    try:
        col = _shared_mongo._db["simulator_triggers"]
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


@sim_router.post("/simulator/paper-trade/portfolio-triggers")
async def simulator_pt_save_portfolio_trigger(body: PTPortfolioTriggerIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Upserts the payoff chart's upper/lower stoploss marker for a whole basket, keyed by
    (broker_id, underlying) — basket-level, not per-leg, since the payoff curve it's drawn
    against is the sum of every open leg for that underlying. legs_snapshot records which
    legs (and at what quantity) made up that basket when this was saved; the drift-check in
    _fetch_dhan_broker_option_positions() invalidates it the moment that set stops matching
    (a leg added/removed/resized), since the saved price no longer means what it did when
    the user looked at the chart and set it.
    """
    try:
        col = _shared_mongo._db["simulator_portfolio_triggers"]
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        snapshot = sorted(
            ({"leg_id": s.leg_id, "quantity": s.quantity} for s in body.legs_snapshot if s.quantity > 0),
            key=lambda s: s["leg_id"],
        )
        col.update_one(
            {"broker_id": body.broker_id, "underlying": body.underlying},
            {
                "$set": {
                    "sl_upper": body.sl_upper, "sl_lower": body.sl_lower,
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


@sim_router.get("/simulator/paper-trade/alert-config")
async def simulator_pt_get_alert_config(broker_id: str = Query(...), underlying: str = Query(...), current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Reads back what the POST endpoint above saves — without this, the
    Position Configuration panel has no way to show what was last saved when
    re-entering alert mode (or reloading the page); it always renders its
    React defaults, looking exactly like the save silently failed even
    though it didn't.
    """
    try:
        doc = _shared_mongo._db["simulator_portfolio_triggers"].find_one(
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


@sim_router.post("/simulator/paper-trade/alert-config")
async def simulator_pt_save_alert_config(body: PTAlertConfigIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Upserts the "Position Configuration" panel's basket-level Stoploss/Target/
    Trail SL/Hedge settings into the SAME doc as the payoff-chart sl_upper/
    sl_lower marker (simulator_portfolio_triggers, keyed by broker_id+
    underlying) — but under separately-namespaced alert_* fields, written by
    this endpoint only, so this save and that one's $set never clobber each
    other's fields. alert_peak_mtm resets to 0 on every save (new baseline for
    Trail SL's "highest MTM seen since this was configured") — the live
    ratcheting itself happens in features/simulator_risk_monitor.py, not here.
    No drift-check against alert_legs_snapshot in this round, unlike
    sl_upper/lower's legs_snapshot — not requested, kept out to match scope.
    """
    try:
        col = _shared_mongo._db["simulator_portfolio_triggers"]
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        snapshot = [s.model_dump() for s in body.legs_snapshot if s.quantity > 0]
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
                    "alert_legs_snapshot": snapshot,
                    "alert_peak_mtm": 0.0,
                    "alert_status": "active",
                    "alert_updated_at": now_str,
                },
                "$setOnInsert": {"broker_id": body.broker_id, "underlying": body.underlying, "created_at": now_str},
            },
            upsert=True,
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/paper-trade/adjustments")
async def simulator_pt_list_adjustments(
    broker_id: Optional[str] = Query(default=None),
    underlying: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """
    The "🔔 Alert" bottom-sheet's saved reverse-order preview — plain CRUD, no drift-check
    (unlike the two trigger collections above): this is a record of "what to do if the alert
    fires," not a value that gets auto-applied anywhere, so the same staleness risk doesn't
    apply the same way. Keyed by (broker_id, underlying) for the live-broker view, or by
    strategy_id for a saved/virtual strategy (no broker_id/leg_id there) — exactly one pair is
    ever sent (see PTAdjustmentIn).
    """
    try:
        query = {"strategy_id": strategy_id} if strategy_id else {"broker_id": broker_id, "underlying": underlying}
        # Only the live, armed config — a fired/disabled doc is history, not something
        # to restore into the "🔔 Alert" editor (see PTAdjustmentIn.status).
        query["status"] = {"$ne": False}
        docs = list(_shared_mongo._db["simulator_adjustments"].find(query).sort("updated_at", -1))
        for d in docs:
            d["_id"] = str(d["_id"])
        return {"status": "success", "adjustments": docs}
    except Exception as exc:
        return {"status": "error", "message": str(exc), "adjustments": []}


@sim_router.post("/simulator/paper-trade/adjustments")
async def simulator_pt_create_adjustment(body: PTAdjustmentIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        doc = body.model_dump()
        doc["created_at"] = now_str
        doc["updated_at"] = now_str
        result = _shared_mongo._db["simulator_adjustments"].insert_one(doc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.patch("/simulator/paper-trade/adjustments/{adjustment_id}")
async def simulator_pt_update_adjustment(adjustment_id: str, body: PTAdjustmentPatchIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        update: dict = {"updated_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")}
        update["positions"] = [p.model_dump() for p in body.positions]
        # Editing and re-saving re-arms it — same record gets updated in place rather
        # than a new one created (see simulator_pt_create_adjustment/PTAdjustmentIn.status).
        update["status"] = True
        # Clears a stale failure from a previous webhook fire (see
        # _simulator_pt_webhook_fire_live_adjustment) — otherwise re-saving after fixing
        # whatever caused it (e.g. a broker relogin) would still show the old error forever,
        # since only the next actual fire attempt would ever overwrite this field again.
        update["webhook_error"] = None
        if body.trigger_price is not None:
            update["trigger_price"] = body.trigger_price
        if body.trigger_condition is not None:
            update["trigger_condition"] = body.trigger_condition
        _shared_mongo._db["simulator_adjustments"].update_one({"_id": ObjectId(adjustment_id)}, {"$set": update})
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/risk-monitor/adjustment-orders")
async def simulator_get_adjustment_orders(
    broker_id: Optional[str] = Query(default=None),
    underlying: Optional[str] = Query(default=None),
    adjustment_doc_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """
    Read simulator_adjustment_orders — order tracking for upper/lower adjustment
    fires. Completely separate from broker_orders (algo_trades only).
    Query by adjustment_doc_id for a specific fire, or by broker_id+underlying
    for all orders for a basket.
    """
    try:
        query: dict = {}
        if adjustment_doc_id:
            query['adjustment_doc_id'] = adjustment_doc_id
        elif broker_id:
            query['broker_id'] = broker_id
            if underlying:
                query['underlying'] = underlying
        if status:
            query['status'] = status.upper()
        docs = list(
            _shared_mongo._db['simulator_adjustment_orders']
            .find(query)
            .sort('placed_at', -1)
            .limit(500)
        )
        for d in docs:
            d['_id'] = str(d['_id'])
        return {'status': 'success', 'orders': docs, 'count': len(docs)}
    except Exception as exc:
        return {'status': 'error', 'message': str(exc), 'orders': []}


@sim_router.delete("/simulator/paper-trade/adjustments")
async def simulator_pt_delete_adjustment(
    trigger_condition: str = Query(...),
    broker_id: Optional[str] = Query(default=None),
    underlying: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """
    Removing the payoff chart's Upper/Lower SL marker (removeStoploss, PaperTradeNew.tsx) must
    also drop that side's saved "🔔 Alert" reverse-order basket — otherwise the marker's price
    is gone but its adjustment positions silently linger in simulator_adjustments, and the next
    time that side is set up again, opening the Alert panel restores the STALE basket (matched
    by the same trigger_condition, see simulator_pt_list_adjustments's docstring) instead of
    starting fresh. Keyed the same way the GET above is: strategy_id for a saved/virtual
    strategy, or (broker_id, underlying) for the live-broker view.
    """
    try:
        query: dict = {"trigger_condition": trigger_condition}
        query.update({"strategy_id": strategy_id} if strategy_id else {"broker_id": broker_id, "underlying": underlying})
        result = _shared_mongo._db["simulator_adjustments"].delete_many(query)
        return {"status": "success", "deleted": result.deleted_count}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.post("/simulator/paper-trade/webhooks")
async def simulator_pt_create_webhook(body: PTWebhookIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    The STOPLOSS chip's webhook icon (PaperTradeNew.tsx) — one webhook per
    (strategy_id, adjustment_id) pair. strategy_id is None for a live-broker-view
    adjustment (see PTAdjustmentIn/PTWebhookIn) — _simulator_pt_webhook_trigger then routes
    to _simulator_pt_webhook_fire_live_adjustment, which places a real order, instead of the
    paper-only force_fire_adjustment used for a saved strategy's adjustment. Idempotent:
    re-clicking the icon for the same saved adjustment returns the same webhook id instead
    of creating a duplicate row. See simulator_pt_webhook_trigger for what hitting the URL
    does.
    """
    try:
        existing = _shared_mongo._db["simulator_webhooks"].find_one(
            {"strategy_id": body.strategy_id, "adjustment_id": body.adjustment_id},
        )
        if existing:
            return {"status": "success", "id": str(existing["_id"])}
        limit_error = _sim_webhook_url_limit_error(current_user, body.strategy_id)
        if limit_error:
            return {"status": "error", "message": limit_error}
        doc = {
            "strategy_id": body.strategy_id,
            "adjustment_id": body.adjustment_id,
            # Needed at trigger time for the live-adjustment path's plan check + Telegram
            # notify (see _simulator_pt_webhook_fire_live_adjustment) — the saved-strategy
            # path instead reads user_id off the strategy doc itself.
            "user_id": current_user.get("_id"),
            "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            "status": 1,
        }
        result = _shared_mongo._db["simulator_webhooks"].insert_one(doc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.post("/simulator/paper-trade/webhooks/new-strategy")
async def simulator_pt_create_new_strategy_webhook(body: PTNewStrategyWebhookIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    "Generate Webhook URL" (PaperTradeNew.tsx, replaces the basket-name input for a
    not-yet-saved strategy). Unlike simulator_pt_create_webhook above, there's no
    strategy_id/adjustment_id yet — this doc carries the full position snapshot instead,
    and strategy_id/adjustment_id are left unset on purpose: that absence is exactly what
    _simulator_pt_webhook_trigger uses to tell "fire an existing adjustment" apart from
    "create a brand-new strategy from this snapshot" (see _simulator_pt_webhook_create_strategy).
    Not idempotent like the other one — every click is a fresh draft, there's no
    (strategy_id, adjustment_id) pair yet to de-dupe against.
    """
    trade_status = (body.trade_status or "").strip().lower()
    if trade_status not in ("paper", "live"):
        return {"status": "error", "message": "trade_status must be 'paper' or 'live'."}
    if trade_status == "live" and not str(body.broker_id or "").strip():
        return {"status": "error", "message": "broker_id is required for a live webhook."}
    if not (body.positions or []):
        return {"status": "error", "message": "No positions to generate a webhook for."}
    # Gated by create_strategy_webhook_mode/limit only — NOT webhook_url_limit
    # (that field is scoped to webhooks on an *already-existing* strategy: the
    # per-side SL webhook and the "update strategy" webhook. This endpoint only
    # ever creates a brand-new strategy, so it's governed solely by the
    # separate create_strategy_webhook_* config — see
    # _sim_create_strategy_webhook_limit_error).
    create_limit_error = _sim_create_strategy_webhook_limit_error(_resolve_sim_user_id(current_user))
    if create_limit_error:
        return {"status": "error", "message": create_limit_error}
    try:
        doc = {
            "strategy_id": None,
            "adjustment_id": None,
            "trade_status": trade_status,
            "broker_id": str(body.broker_id).strip() if trade_status == "live" else None,
            # Whoever generated this link — _simulator_pt_webhook_create_strategy uses this
            # to notify them personally (telegram_notifier.notify_user_for) once the URL is
            # actually hit. None when auth is off (current_user is the anonymous stub) — the
            # notify falls back to the shared TELEGRAM_USER_CHAT_ID in that case.
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
        result = _shared_mongo._db["simulator_webhooks"].insert_one(doc)
        try:
            # Dedicated history table for "new position, no strategy mapped yet" webhooks —
            # kept separate from simulator_webhooks (which also holds update-strategy and
            # per-side adjustment webhooks) so the New Positions page can list/track just
            # these without filtering out unrelated webhook types. Correlated by webhook_id,
            # not a shared _id, so the two collections' primary keys stay independent.
            _shared_mongo._db["simulator_new_positions"].insert_one({
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
            log.error("simulator_new_positions mirror insert failed for webhook %s: %s", result.inserted_id, mirror_exc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.post("/simulator/paper-trade/webhooks/update-strategy/{strategy_id}")
async def simulator_pt_create_update_strategy_webhook(
    strategy_id: str,
    body: PTUpdateStrategyWebhookIn,
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """
    "Generate Webhook URL" on a *saved* strategy page (/trade/:id).  Unlike
    simulator_pt_create_new_strategy_webhook (which creates a brand-new strategy),
    this stores strategy_id so _simulator_pt_webhook_trigger knows to call
    _simulator_pt_webhook_update_strategy (merge incoming positions into the
    existing strategy with netting) instead of creating fresh.
    """
    trade_status = (body.trade_status or "paper").strip().lower()
    if trade_status not in ("paper", "live"):
        return {"status": "error", "message": "trade_status must be 'paper' or 'live'."}
    if not str(strategy_id or "").strip():
        return {"status": "error", "message": "strategy_id is required."}
    if not (body.positions or []):
        return {"status": "error", "message": "No positions to generate a webhook for."}
    try:
        if not _find_owned_strategy(strategy_id, current_user):
            return {"status": "error", "message": "Strategy not found."}
        limit_error = _sim_webhook_url_limit_error(current_user, strategy_id)
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
        result = _shared_mongo._db["simulator_webhooks"].insert_one(doc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/paper-trade/webhooks")
async def simulator_pt_list_strategy_webhooks(
    strategy_id: Optional[str] = None,
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """
    Webhook Alerts bell dropdown (PaperTradeNew.tsx) — every "Generate Webhook URL" webhook
    generated for this saved strategy, oldest first, so a page reload repopulates the list
    instead of losing it (the dropdown itself is otherwise session-only client state).
    adjustment_id: None filters out the STOPLOSS chips' own per-side webhooks (see
    simulator_pt_create_webhook) — those belong to the SL strip, not this dropdown.

    strategy_id omitted: same dropdown, but for a not-yet-saved ("new strategy")
    basket — those webhooks are created with strategy_id left None (see
    simulator_pt_create_new_strategy_webhook), so there's no strategy to key off
    of; scope by user_id instead so a page refresh still repopulates the list.
    """
    try:
        if strategy_id:
            filt: dict[str, Any] = {"strategy_id": strategy_id, "adjustment_id": None}
        else:
            filt = {"strategy_id": None, "adjustment_id": None}
            current_user_id = current_user.get("_id")
            if current_user_id is not None:
                filt["$or"] = [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]
        docs = list(
            _shared_mongo._db["simulator_webhooks"]
            .find(filt)
            .sort("created_at", 1)
        )
        webhooks = [
            # status: 1 = active/pending, 2 = already triggered (see
            # _simulator_pt_webhook_trigger) — exposed so the frontend can
            # count only active ones against webhook_url_limit instead of
            # every webhook ever generated for this strategy.
            {"id": str(d["_id"]), "positions": d.get("positions") or [], "status": d.get("status", 1)}
            for d in docs
        ]
        return {"status": "success", "webhooks": webhooks}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def _disable_tv_alerts_for_webhook(webhook_id: str, owner_id: Any) -> None:
    """
    A deleted simulator webhook may still be pasted into a TradingView alert's
    Webhook URL field (tv_alerts, algo.chart's domain — same "stock_data" Mongo
    DB, read directly here rather than calling out to that service). Left
    alone, that alert would keep firing at a webhook id that no longer
    resolves to anything, so the matching alert doc is deleted outright (not
    just flagged webhookEnabled: False — the dead webhook is gone for good,
    so there's nothing left for a disabled alert to point back at). Scoped to
    the webhook owner's alerts only, matching webhookUrl by the webhook id it
    embeds (see chart_api.py's _WEBHOOK_URL_ID_RE) rather than full string
    equality, so it still matches regardless of which host the URL was
    copied against (localhost vs prod).
    """
    if not webhook_id:
        return
    owner_id_str = str(owner_id) if owner_id is not None else None
    if not owner_id_str:
        return
    # webhook_owner_id is normally stored as a string (chart_api.py's
    # _resolve_webhook_scope stringifies it before persisting), but older
    # alerts saved before that fix still carry a raw ObjectId — match both
    # so this cascade doesn't silently miss those.
    owner_candidates: list[Any] = [owner_id_str]
    try:
        owner_candidates.append(ObjectId(owner_id_str))
    except Exception:
        pass
    _shared_mongo._db["tv_alerts"].delete_many(
        {
            "webhook_owner_id": {"$in": owner_candidates},
            "webhookEnabled": True,
            "webhookUrl": {"$regex": re.escape(f"/webhook/tv/alert/{webhook_id}") + "$"},
        },
    )


@sim_router.delete("/simulator/paper-trade/webhooks/{webhook_id}")
async def simulator_pt_delete_webhook(webhook_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    "Remove" (x icon, Webhook Alerts bell dropdown) — actually deletes the
    simulator_webhooks doc instead of just dropping it from local React state,
    so it stops counting against webhook_url_limit (see
    simulator_pt_webhook_usage) and doesn't reappear once the dropdown
    re-hydrates from the server on the next page load. Also drops the
    simulator_new_positions mirror row if this was a not-yet-saved-strategy
    draft webhook (no-op delete_one if it wasn't).
    """
    try:
        try:
            doc_id = ObjectId(webhook_id)
        except Exception:
            return {"status": "error", "message": "Invalid webhook id"}
        current_user_id = current_user.get("_id")
        filt: dict[str, Any] = {"_id": doc_id}
        if current_user_id is not None:
            filt["$or"] = [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]
        doc = _shared_mongo._db["simulator_webhooks"].find_one(filt)
        if not doc:
            return {"status": "error", "message": "Webhook not found"}
        _shared_mongo._db["simulator_webhooks"].delete_one({"_id": doc_id})
        _shared_mongo._db["simulator_new_positions"].delete_one({"webhook_id": webhook_id})
        _disable_tv_alerts_for_webhook(webhook_id, doc.get("user_id"))
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/paper-trade/new-positions")
async def simulator_pt_list_new_positions(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    New Positions tracking page — every "Generate Webhook URL" hit from the blank Builder
    (simulator_pt_create_new_strategy_webhook), newest first. These are positions that were
    never mapped to an existing strategy; status flips from 1 (pending) to 2 (triggered) in
    _simulator_pt_webhook_create_strategy once the webhook URL is actually hit.

    Self-heals docs stuck at status 1 whose underlying simulator_webhooks doc already shows
    triggered (status 2) — covers the mirror-update in _simulator_pt_webhook_create_strategy
    failing, or a create-strategy webhook predating this table. Also drops (cascade-deletes)
    any row whose simulator_webhooks doc no longer exists at all.
    """
    try:
        current_user_id = current_user.get("_id")
        conditions: list[dict[str, Any]] = []
        if current_user_id is not None:
            conditions.append({"$or": [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]})
        # Pending/disabled webhooks stay on the page indefinitely (no date bound) since
        # they haven't fired yet — only webhooks that already triggered get narrowed down
        # to today (IST), so the page doesn't keep piling up every trigger ever fired.
        # triggered_at is stored as a "%Y-%m-%dT%H:%M:%S" string, so plain lexicographic
        # $gte/$lt bounds work fine.
        now_ist = datetime.now(IST)
        today_start = now_ist.strftime("%Y-%m-%dT00:00:00")
        tomorrow_start = (now_ist + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        conditions.append({"$or": [
            {"status": {"$ne": 2}},
            {"triggered_at": {"$gte": today_start, "$lt": tomorrow_start}},
        ]})
        filt: dict[str, Any] = {"$and": conditions} if conditions else {}
        docs = list(_shared_mongo._db["simulator_new_positions"].find(filt).sort("created_at", -1))

        # Each doc mirrors a specific simulator_webhooks doc (see webhook_id) —
        # if that webhook was deleted (e.g. someone cleared simulator_webhooks
        # directly), this mirror row is orphaned and stale forever (it caches
        # its own status/resulting_strategy_id, so it never self-corrects on
        # its own). Drop those here instead of leaving ghost rows on the page.
        referenced_ids = [d["webhook_id"] for d in docs if d.get("webhook_id")]
        existing_webhooks: dict[str, dict] = {}
        if referenced_ids:
            existing_webhooks = {
                str(w["_id"]): w
                for w in _shared_mongo._db["simulator_webhooks"].find(
                    {"_id": {"$in": [ObjectId(wid) for wid in referenced_ids]}},
                )
            }
        orphaned_ids = [
            d["_id"] for d in docs
            if d.get("webhook_id") and str(d["webhook_id"]) not in existing_webhooks
        ]
        if orphaned_ids:
            _shared_mongo._db["simulator_new_positions"].delete_many({"_id": {"$in": orphaned_ids}})
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
                strategy_doc = _shared_mongo._db["simulator_strategy"].find_one(
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


@sim_router.delete("/simulator/paper-trade/new-positions/{position_id}")
async def simulator_pt_delete_new_position(position_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Remove (trash icon, Webhook Strategies page) a webhook that hasn't fired yet —
    deletes both this simulator_new_positions mirror row and its underlying
    simulator_webhooks doc, freeing the slot it was holding against
    webhook_url_limit (see simulator_pt_webhook_usage). Already-triggered
    webhooks are real strategies now — those go through
    DELETE /simulator/paper-trade/strategies/{id} instead, same as any other
    strategy, so this endpoint refuses to touch a triggered row.
    """
    try:
        try:
            doc_id = ObjectId(position_id)
        except Exception:
            return {"status": "error", "message": "Invalid position id"}
        current_user_id = current_user.get("_id")
        filt: dict[str, Any] = {"_id": doc_id}
        if current_user_id is not None:
            filt["$or"] = [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]
        doc = _shared_mongo._db["simulator_new_positions"].find_one(filt)
        if not doc:
            return {"status": "error", "message": "Webhook not found"}
        if doc.get("status") == 2:
            return {"status": "error", "message": "This webhook already fired — delete its strategy instead."}
        webhook_id = doc.get("webhook_id")
        if webhook_id:
            try:
                _shared_mongo._db["simulator_webhooks"].delete_one({"_id": ObjectId(webhook_id)})
            except Exception:
                pass
            _disable_tv_alerts_for_webhook(webhook_id, doc.get("user_id"))
        _shared_mongo._db["simulator_new_positions"].delete_one({"_id": doc_id})
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _simulator_pt_webhook_create_strategy(webhook_doc: dict) -> dict:
    """
    What hitting a "new strategy" webhook URL (see simulator_pt_create_new_strategy_webhook)
    actually does. Paper just saves the snapshot like the normal Save button
    (_insert_simulator_strategy — same helper, so the two never drift on what a freshly
    created strategy doc looks like). Live also places real orders first, through the
    broker picked when the webhook was generated — same order-placement path the Order
    Pad's Execute button uses (_simulator_place_manual_order_core) — and only proceeds to
    save the strategy doc if at least one leg actually got placed.
    """
    positions = webhook_doc.get("positions") or []
    trade_status = str(webhook_doc.get("trade_status") or "paper")
    instrument = webhook_doc.get("instrument") or "nifty"
    # Normalize every position's order_type to what it actually means to execute — "mpp" for
    # the modal's Market radio, "ltp" for anything else (Limit/LTP/legacy SL/unset) — instead
    # of leaking the frontend radio's raw MARKET/LIMIT/LTP value into the saved strategy doc.
    # Same on a paper webhook too (nothing to execute, but the label should still be honest).
    for p in positions:
        p["order_type"] = "mpp" if str(p.get("order_type") or "").strip().upper() == "MARKET" else "ltp"
    # Webhook-created strategies are always Advanced (webhook automation is an
    # Advanced-only feature — see create_strategy_webhook_mode check below) —
    # distinguishes this doc from a manual Save, which is absent entirely on
    # strategies created via the normal Save button.
    extra_fields: dict[str, Any] = {"trade_status": trade_status, "execute_status": "webhook", "execution_mode": "advanced"}

    user_id = str(webhook_doc["user_id"]) if webhook_doc.get("user_id") else _SIM_DEFAULT_USER_ID
    # Checked before placing any live order below — no point sending real broker
    # orders for a strategy that's about to be rejected anyway.
    # create_strategy_webhook_mode/limit, not trade_generate_webhook_mode — this
    # path only ever creates a brand-new strategy (see the routing in
    # _simulator_pt_webhook_trigger), which is exactly what the separate
    # "Create Strategy via Webhook" plan config gates.
    create_limit_error = _sim_create_strategy_webhook_limit_error(user_id)
    if create_limit_error:
        return {"status": "error", "message": create_limit_error}
    slot_error = _sim_advanced_slot_limit_error(user_id)
    if slot_error:
        return {"status": "error", "message": slot_error}
    limit_error = _sim_active_strategy_limit_error(user_id)
    if limit_error:
        return {"status": "error", "message": limit_error}

    if trade_status == "live":
        broker_id = str(webhook_doc.get("broker_id") or "")
        if not broker_id:
            return {"status": "error", "message": "Webhook has no broker configured."}
        open_positions = [p for p in positions if not p.get("exited")]
        # Never a literal "MARKET" order — that gets rejected by the broker's API for F&O
        # (see _simulator_place_manual_order_core's Dhan branch). Instead, each leg's
        # normalized order_type (see above) maps to one of the two protected order types
        # _simulator_place_manual_order_core already resolves a fresh quote for at submit
        # time: "mpp" -> "MPP" (bid/ask-protected limit, same as the Order Pad's "Execute At
        # MPP Order"), anything else -> "LTP" (plain limit at the freshest LTP, same as
        # "Execute At LTP") — never the frontend's possibly stale/long-gone price captured
        # when the webhook was generated.
        orders = [
            ManualOrderLeg(
                underlying=instrument,
                expiry=str(p.get("expiry") or ""),
                strike=float(p.get("strike") or 0),
                option_type=_normalize_pt_option_type(str(p.get("option_type") or p.get("type") or "")),
                side="SELL" if str(p.get("type") or "").strip().lower().startswith("s") else "BUY",
                quantity=int((p.get("lots") or 1) * (p.get("lot_size") or 1)),
                order_type="MPP" if p.get("order_type") == "mpp" else "LTP",
                product="NRML",
            )
            for p in open_positions
        ]
        if not orders:
            return {"status": "error", "message": "No open legs to trade."}

        order_result = await _simulator_place_manual_order_core(ManualOrderRequest(broker_id=broker_id, orders=orders))
        if order_result.get("status") not in ("success", "partial"):
            return {"status": "error", "message": order_result.get("message") or "Order placement failed.", "results": order_result.get("results")}
        extra_fields["broker_id"] = broker_id
        extra_fields["order_results"] = order_result.get("results")
        # Replace each successfully-placed leg's entry_price/entry_time — still the stale
        # snapshot from whenever the webhook URL was generated — with what it actually
        # executed at (order_result's per-leg "price", the MPP/LTP quote
        # _simulator_place_manual_order_core resolved fresh at submit time) and when
        # (now, the actual fire time). `orders` was built by iterating open_positions in
        # order and asyncio.gather preserves that order, so results[i] pairs with
        # open_positions[i]; open_positions holds the same dict objects as positions, so
        # mutating here is reflected in what _insert_simulator_strategy saves below. A
        # failed leg (result["status"] != "success", no "price") is left untouched.
        fire_time = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        for leg_result, p in zip(order_result.get("results") or [], open_positions):
            if leg_result.get("status") == "success" and leg_result.get("price"):
                p["entry_price"] = round(float(leg_result["price"]), 2)
                p["entry_time"] = fire_time
    else:
        # Paper — no real broker order, but the same staleness bug applies: without this,
        # entry_price stays frozen at whatever was showing when the webhook URL was
        # generated, which could be hours/days before the alert actually fires. Simulate a
        # realistic fill the same way a live order would have resolved it (same
        # _resolve_mpp_price/_resolve_ltp_price, same Dhan feed), just without ever calling
        # a broker.
        open_positions = [p for p in positions if not p.get("exited")]
        raw_db = _shared_mongo._db
        fire_time = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        paper_legs = [
            ManualOrderLeg(
                underlying=instrument,
                expiry=str(p.get("expiry") or ""),
                strike=float(p.get("strike") or 0),
                option_type=_normalize_pt_option_type(str(p.get("option_type") or p.get("type") or "")),
                side="SELL" if str(p.get("type") or "").strip().lower().startswith("s") else "BUY",
                quantity=int((p.get("lots") or 1) * (p.get("lot_size") or 1)),
                order_type="MPP" if p.get("order_type") == "mpp" else "LTP",
                product="NRML",
            )
            for p in open_positions
        ]
        # One batched quote fetch for the whole basket instead of one REST call per leg — see
        # _prefetch_dhan_quotes_for_legs (this is what was tripping Dhan's 429 rate limit).
        prefetched_quotes = await _prefetch_dhan_quotes_for_legs(paper_legs, raw_db)
        for p, leg in zip(open_positions, paper_legs):
            resolved_price = await (
                _resolve_mpp_price(leg, raw_db, prefetched_quotes) if leg.order_type == "MPP"
                else _resolve_ltp_price(leg, raw_db, prefetched_quotes)
            )
            if resolved_price and resolved_price > 0:
                p["entry_price"] = round(float(resolved_price), 2)
                p["entry_time"] = fire_time

    try:
        strategy_name = webhook_doc.get("strategy_name") or "Webhook Strategy"
        strategy_id = _insert_simulator_strategy(
            webhook_doc.get("portfolio_name") or "Running Trades",
            strategy_name,
            instrument,
            webhook_doc.get("spot_price"),
            webhook_doc.get("config"),
            positions,
            "live",
            extra_fields,
            # Already stringified + defaulted above (see user_id, used for the
            # limit check) — webhook_doc's own "user_id" field stays untouched
            # (still the raw ObjectId/None) since that field also drives the
            # personal-vs-shared Telegram notify fallback a few lines below.
            user_id=user_id,
        )
        open_count = len([p for p in positions if not p.get("exited")])
        from features.telegram_notifier import notify_user_for
        notify_user_for(
            webhook_doc.get("user_id"),
            "WEBHOOK_STRATEGY_EXECUTED",
            f'"{strategy_name}" went {trade_status} via webhook — {open_count} leg(s) on {instrument.upper()}.',
            {"strategy_id": strategy_id, "trade_status": trade_status},
        )
        try:
            _shared_mongo._db["simulator_new_positions"].update_one(
                {"webhook_id": str(webhook_doc["_id"])},
                {"$set": {
                    "status": 2,
                    "resulting_strategy_id": strategy_id,
                    "triggered_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
                }},
            )
        except Exception as mirror_exc:
            log.error("simulator_new_positions status update failed for webhook %s: %s", webhook_doc["_id"], mirror_exc)
        return {"status": "success", "strategy_id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def _normalize_pt_option_type_str(raw: str) -> str:
    """'Call'/'call'/'CE'/'ce' → 'CE'; 'Put'/'put'/'PE'/'pe' → 'PE'; else 'FUT'."""
    v = raw.strip().lower()
    if v in ("call", "ce"):
        return "CE"
    if v in ("put", "pe"):
        return "PE"
    return "FUT"


def _net_pt_positions(existing: list[dict], incoming: list[dict]) -> list[dict]:
    """
    Python port of the frontend's mergeNewPositionsIntoStrategy: nets each incoming
    open position against existing open positions at the same expiry/strike/option_type
    (ignoring side), closing matched lots. Leftover incoming lots become new open
    positions appended at the end.  Exited positions on either side are left untouched.
    """
    from datetime import datetime as _dt
    result = [dict(p) for p in existing]
    now_iso = _dt.now(IST).strftime("%Y-%m-%dT%H:%M:%S")

    for new_pos in incoming:
        if new_pos.get("exited"):
            result.append(dict(new_pos))
            continue
        new_type  = str(new_pos.get("type") or "").strip().lower()   # "buy"/"sell"
        new_opt   = _normalize_pt_option_type_str(str(new_pos.get("option_type") or ""))
        new_exp   = str(new_pos.get("expiry") or "")[:10]
        new_stk   = float(new_pos.get("strike") or 0)
        new_lots  = int(new_pos.get("lots") or 1)
        entry_px  = float(new_pos.get("entry_price") or 0)

        remaining = new_lots
        for ex in result:
            if remaining <= 0:
                break
            if ex.get("exited"):
                continue
            ex_type = str(ex.get("type") or "").strip().lower()
            if ex_type == new_type:           # same side → no net
                continue
            ex_opt = _normalize_pt_option_type_str(str(ex.get("option_type") or ""))
            ex_exp = str(ex.get("expiry") or "")[:10]
            ex_stk = float(ex.get("strike") or 0)
            if ex_opt != new_opt or ex_exp != new_exp or ex_stk != new_stk:
                continue
            matched = min(remaining, int(ex.get("lots") or 1))
            if matched <= 0:
                continue
            if matched < int(ex.get("lots") or 1):
                ex["lots"] = int(ex.get("lots") or 1) - matched
            else:
                ex["exited"]     = True
                ex["exit_price"] = round(entry_px, 2)
                ex["exit_time"]  = now_iso
                pnl_sign = -1.0 if ex_type.startswith("b") else 1.0
                lot_size = int(ex.get("lot_size") or ex.get("lotSize") or 1)
                ex["pnl"] = round(pnl_sign * (entry_px - float(ex.get("entry_price") or 0)) * matched * lot_size, 2)
            remaining -= matched

        if remaining > 0:
            result.append({**new_pos, "lots": remaining})

    return result


async def _simulator_pt_webhook_update_strategy(webhook_doc: dict) -> dict:
    """
    Merges the positions stored in an "update-strategy" webhook doc into the target
    strategy, using the same netting logic the frontend's "Update" button applies.
    """
    try:
        from bson import ObjectId
        strategy_id = str(webhook_doc.get("strategy_id") or "")
        if not strategy_id:
            return {"status": "error", "message": "Webhook has no strategy_id."}

        raw_db = _shared_mongo._db
        doc = raw_db["simulator_strategy"].find_one({"_id": ObjectId(strategy_id)})
        if not doc:
            return {"status": "error", "message": "Strategy not found."}

        existing_positions = list(doc.get("positions") or [])
        new_positions      = [p if isinstance(p, dict) else p.dict() for p in (webhook_doc.get("positions") or [])]
        if not new_positions:
            return {"status": "error", "message": "No positions in webhook doc."}
        # Same order_type normalization as _simulator_pt_webhook_create_strategy — keeps the
        # field's stored meaning ("mpp"/"ltp") consistent regardless of which webhook flow
        # created the leg. This flow doesn't place a live order itself (see its docstring),
        # so there's no fill price to backfill entry_price with yet.
        for p in new_positions:
            p["order_type"] = "mpp" if str(p.get("order_type") or "").strip().upper() == "MARKET" else "ltp"

        merged = _net_pt_positions(existing_positions, new_positions)
        raw_db["simulator_strategy"].update_one(
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
            f'"{strategy_name}" updated via webhook — {open_count} open leg(s) on {instrument}.',
            {"strategy_id": strategy_id},
        )
        return {"status": "success", "strategy_id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _simulator_pt_webhook_fire_live_adjustment(webhook_doc: dict) -> dict:
    """
    Fires a live-broker-view adjustment (PTAdjustmentIn keyed by broker_id/underlying, no
    strategy_id — see openAlertModal/saveAlertOrdersToDb in PaperTradeNew.tsx) generated
    from the payoff graph's SL/Target "🔔 Alert" reverse-exit preview. Unlike
    force_fire_adjustment (the saved-strategy sibling below, explicitly paper-only), this
    places a REAL opposite-side order on the broker the adjustment was created against —
    same order-placement path as the Order Pad's Execute button and
    _simulator_pt_webhook_create_strategy's live branch.

    On any failure (broker rejection, missing quote, credentials, etc.) the raw error is
    written onto the adjustment doc as `webhook_error` instead of only ever reaching a
    webhook caller (TradingView/curl) that has no way to surface it back to the user — the
    saved-SL chip on the payoff graph reads this to show what went wrong.
    """
    adjustment_id = str(webhook_doc.get("adjustment_id") or "")
    adjustments_col = _shared_mongo._db["simulator_adjustments"]
    try:
        adj_doc = adjustments_col.find_one({"_id": ObjectId(adjustment_id)})
    except Exception:
        return {"status": "error", "message": "Invalid adjustment id"}
    if not adj_doc:
        return {"status": "error", "message": "Adjustment not found"}
    if adj_doc.get("status") is False:
        return {"status": "error", "message": "Adjustment already fired or inactive"}

    broker_id = str(adj_doc.get("broker_id") or "")
    if not broker_id:
        return {"status": "error", "message": "Adjustment has no broker configured"}

    webhook_user_id = webhook_doc.get("user_id")
    plan, _ = _sim_resolve_plan_and_advanced_slots(str(webhook_user_id) if webhook_user_id else _SIM_DEFAULT_USER_ID)
    if plan.get("trade_generate_webhook_mode") != "enabled":
        message = f"Webhook Trading isn't available on your {plan.get('plan_name') or 'current'} plan."
        adjustments_col.update_one({"_id": ObjectId(adjustment_id)}, {"$set": {"webhook_error": message}})
        return {"status": "error", "message": message}

    open_positions = [p for p in (adj_doc.get("positions") or []) if not p.get("exited")]
    if not open_positions:
        message = "No open legs on this adjustment."
        adjustments_col.update_one({"_id": ObjectId(adjustment_id)}, {"$set": {"webhook_error": message}})
        return {"status": "error", "message": message}

    orders = [
        ManualOrderLeg(
            underlying=str(adj_doc.get("underlying") or "nifty").lower(),
            expiry=str(p.get("expiry") or ""),
            strike=float(p.get("strike") or 0),
            option_type=_normalize_pt_option_type(str(p.get("option_type") or "")),
            side="SELL" if str(p.get("side") or "").strip().upper() == "S" else "BUY",
            quantity=int(p.get("qty") or p.get("lots") or 1),
            order_type="LTP",
            product="NRML",
        )
        for p in open_positions
    ]
    order_result = await _simulator_place_manual_order_core(ManualOrderRequest(broker_id=broker_id, orders=orders))
    now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    if order_result.get("status") not in ("success", "partial"):
        message = order_result.get("message") or "Order placement failed."
        adjustments_col.update_one(
            {"_id": ObjectId(adjustment_id)},
            {"$set": {"webhook_error": message, "webhook_error_results": order_result.get("results"), "webhook_error_at": now_str}},
        )
        return {"status": "error", "message": message, "results": order_result.get("results")}

    # Fired — same "flip to False, never delete" convention as the risk monitor uses
    # elsewhere in simulator_adjustments, so a re-hit of the same URL correctly no-ops
    # above instead of placing the order twice.
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
            f'Live webhook fired {len(orders)} leg(s) on {str(adj_doc.get("underlying") or "").upper()}.',
            {"adjustment_id": adjustment_id, "broker_id": broker_id},
        )
    except Exception:
        pass
    return {"status": "success", "results": order_result.get("results")}


async def _simulator_pt_webhook_trigger(webhook_id: str) -> dict:
    try:
        webhook_doc = _shared_mongo._db["simulator_webhooks"].find_one({"_id": ObjectId(webhook_id)})
    except Exception:
        return {"status": "error", "message": "Invalid webhook id"}
    if not webhook_doc:
        return {"status": "error", "message": "Webhook not found"}
    current_status = webhook_doc.get("status")
    if current_status == 2:
        return {"status": "noop", "message": "Already triggered"}
    if current_status == 0:
        return {"status": "noop", "message": "Webhook is inactive"}
    # Route by webhook_type / presence of strategy_id / adjustment_id:
    # • No strategy_id + no adjustment_id  → "new strategy" webhook, create fresh.
    # • strategy_id + no adjustment_id     → "update strategy" webhook, merge positions.
    # • strategy_id + adjustment_id        → fire an existing (paper) strategy's adjustment basket.
    # • no strategy_id, + adjustment_id    → fire a live-broker-view adjustment for real (see
    #                                         _simulator_pt_webhook_fire_live_adjustment above).
    if not webhook_doc.get("strategy_id") and not webhook_doc.get("adjustment_id"):
        result = await _simulator_pt_webhook_create_strategy(webhook_doc)
    elif webhook_doc.get("strategy_id"):
        # Both "update strategy" and "fire adjustment" act on an existing
        # strategy — webhook automation only works for Advanced strategies on a
        # plan with trade_generate_webhook_mode "enabled", regardless of which
        # of the two this is.
        strategy_doc = _shared_mongo._db["simulator_strategy"].find_one(
            {"_id": ObjectId(webhook_doc["strategy_id"])}, {"user_id": 1, "execution_mode": 1},
        )
        strategy_user_id = (strategy_doc or {}).get("user_id") or webhook_doc.get("user_id")
        plan, _ = _sim_resolve_plan_and_advanced_slots(strategy_user_id)
        is_advanced = str((strategy_doc or {}).get("execution_mode") or "").lower() == "advanced"
        if plan.get("trade_generate_webhook_mode") != "enabled" or not is_advanced:
            return {
                "status": "error",
                "message": f"Webhook is available only for Advanced Strategies on a plan with Webhook Trading enabled — your current plan is {plan.get('plan_name') or 'unknown'}. Buy Additional Credit or upgrade your plan to continue.",
            }
        if not webhook_doc.get("adjustment_id"):
            result = await _simulator_pt_webhook_update_strategy(webhook_doc)
        else:
            result = await simulator_risk_monitor.force_fire_adjustment(
                MongoData(), str(webhook_doc["strategy_id"]), str(webhook_doc["adjustment_id"]),
            )
    elif webhook_doc.get("adjustment_id"):
        result = await _simulator_pt_webhook_fire_live_adjustment(webhook_doc)
    else:
        return {"status": "error", "message": "Webhook has no strategy_id."}
    if result.get("status") == "success":
        _shared_mongo._db["simulator_webhooks"].update_one({"_id": webhook_doc["_id"]}, {"$set": {"status": 2}})
    return result


@sim_router.get("/webhook/tv/alert/{webhook_id}")
@sim_router.post("/webhook/tv/alert/{webhook_id}")
async def simulator_pt_webhook_trigger(webhook_id: str) -> dict:
    """
    Public, unauthenticated on purpose — this is the URL a TradingView alert (or
    curl, for manual testing) hits directly; it can't carry our app's JWT. The
    unguessable Mongo id in the path is the credential, same as every other
    "TradingView webhook URL" feature works. Force-fires the linked
    simulator_adjustments basket via SimulatorRiskMonitor.force_fire_adjustment
    regardless of current price — the alert itself is the trigger signal, not a
    price band this still needs to re-check. Paper-only: see the
    PAPER_AUTO_FIRE_ENABLED comment in simulator_risk_monitor.py — never calls a
    broker, only writes to simulator_strategy/simulator_adjustments.
    """
    return await _simulator_pt_webhook_trigger(webhook_id)


@sim_router.get("/simulator/paper-trade/strategies")
async def simulator_pt_list_strategies(
    portfolio_id: Optional[str] = None,
    portfolio_name: Optional[str] = None,
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    try:
        _ensure_simulator_strategy_index()
        filt: dict[str, Any] = {}
        normalized_portfolio_id = str(portfolio_id or "").strip()
        normalized_portfolio_name = str(portfolio_name or "").strip()
        if normalized_portfolio_id:
            filt["portfolio_id"] = normalized_portfolio_id
        elif normalized_portfolio_name:
            filt["portfolio_name"] = normalized_portfolio_name
        # Scope to the caller's own strategies (falling back to the shared default
        # identity when current_user is unresolved — see _resolve_sim_user_id —
        # so strategies created under that same fallback are actually listable).
        # Docs saved before user_id existed have no such field — keep those
        # visible to everyone rather than orphaning them.
        current_user_id = _resolve_sim_user_id(current_user)
        filt["$or"] = [{"user_id": current_user_id}, {"user_id": {"$exists": False}}]
        docs = list(_shared_mongo._db["simulator_strategy"].find(filt).sort("saved_at", -1))
        result = []
        for doc in docs:
            doc = _enrich_pt_strategy_positions(doc)
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


def _find_owned_strategy(strategy_id: str, current_user: dict) -> Optional[dict]:
    """
    Looks up a simulator_strategy doc by id and enforces ownership — returns
    None (treated as "Not found", never a distinct 403) if the doc doesn't
    exist OR belongs to a different user than the caller. Docs saved before
    user_id existed have no such field and stay visible/editable by anyone,
    same backward-compat rule the list/portfolio routes use.
    """
    doc = _shared_mongo._db["simulator_strategy"].find_one({"_id": ObjectId(strategy_id)})
    if not doc:
        return None
    doc_user_id = doc.get("user_id")
    current_user_id = current_user.get("_id")
    # Compare as strings — user_id is stored as a plain string on this
    # collection now, but current_user["_id"] is always a real ObjectId, so a
    # raw != would never match a legitimate owner.
    if doc_user_id is not None and current_user_id is not None and str(doc_user_id) != str(current_user_id):
        return None
    return doc


@sim_router.get("/simulator/paper-trade/strategies/{strategy_id}")
async def simulator_pt_get_strategy(strategy_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        doc = _find_owned_strategy(strategy_id, current_user)
        if not doc:
            return {"status": "error", "message": "Not found"}
        return {"status": "success", "strategy": _str_id(_enrich_pt_strategy_positions(doc))}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/paper-trade/spot-tokens")
async def simulator_pt_spot_tokens(broker_id: str = Query(default=""), current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        resolved_broker_id = str(broker_id or _DEFAULT_PAPER_TRADE_SPOT_BROKER_ID).strip()
        items = _get_instrument_spot_token_docs(resolved_broker_id)
        return {
            "status": "success",
            "broker_id": resolved_broker_id,
            "items": items,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/paper-trade/underlying-quotes")
async def simulator_pt_underlying_quotes(instruments: str = "") -> dict:
    """
    Live {spot_price, change_pct, change_points, ...} per underlying, for
    whatever instruments the *paper-trade strategies* hold — deliberately
    independent of /simulator/positions/all's `underlyings` map, which is
    derived from the real broker account's actual open positions and so
    only ever covers underlyings that happen to exist there. A paper
    strategy on a stock the real account never traded (e.g. BSE, TCS) would
    never get a change_pct from that map no matter what, since it's simply
    not a key in it — this endpoint asks for exactly the instruments the
    caller names instead.
    """
    from features.execution_socket import _fetch_dhan_index_quotes
    names = {n.strip().upper() for n in str(instruments or "").split(",") if n.strip()}
    if not names:
        return {"status": "success", "underlyings": {}}
    db = MongoData()
    try:
        quotes = await asyncio.to_thread(_fetch_dhan_index_quotes, db, names)
    except Exception as exc:
        log.warning("paper trade underlying quote error instruments=%s: %s", ",".join(names), exc)
        quotes = {}
    finally:
        db.close()
    return {"status": "success", "underlyings": quotes}


@sim_router.get("/simulator/paper-trade/futures-chain")
async def simulator_pt_futures_chain(instrument: str = "", current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Real NSE/BSE index futures (FUTIDX — current/next/far month) PLUS per-weekly-
    expiry "Synthetic Futures" (ATM strike + call_ltp - put_ltp) for the paper-trade
    builder's "Fut" tab — both in one call.

    Earlier version of this endpoint only returned `futures`; the frontend then made
    one *additional* full /live-greeks-chain fetch per weekly expiry to derive the
    synthetic rows — each of those builds and prices the entire chain (every strike,
    Black-Scholes IV/Greeks, OI) just to read one ATM row back out, ~5 HTTP round
    trips and ~5 separate Dhan-bound IO chains for what's fundamentally 2 sec_ids per
    weekly expiry. This instead resolves each expiry's ATM CE/PE token directly from
    active_option_tokens (already populated by the existing option-chain sync — see
    _sync_dhan_index_option_tokens) and prices everything in one go.

    Pricing uses dhan_quote_post_blocking (waits for the shared Dhan rate-gate slot)
    rather than get_broker_rest_quotes/dhan_quote_post — that pair's non-blocking call
    silently returns nothing whenever some other caller in this process (the Opt tab's
    own chain pricing, the live-quote socket's background refresh, ...) used the
    ~1-per-1.05s shared gate within the same window, which on a page with several
    concurrent Dhan callers is often, not rare. For a hot polling loop that's the
    right trade (skip beats blocking the loop), but for this one-shot, user-triggered
    fetch a skip just means stale data masquerading as live — confirmed live: repeated
    calls a few seconds apart kept replaying an identical, minutes-stale snapshot
    because every live attempt in between had been silently skipped. Blocking trades
    a bounded (~1-2s) wait for an actual fresh attempt every call. _LAST_GOOD_FUTURES_TOKEN_QUOTE
    (this endpoint's own, separate from get_broker_rest_quotes' _LAST_GOOD_QUOTE) is
    still the fallback for genuine Dhan-side gaps (502s, empty pre-market bodies).
    """
    normalized = str(instrument or "").strip().upper()
    if not normalized:
        return {"status": "success", "instrument": "", "futures": [], "synthetic_futures": []}

    try:
        future_master = await asyncio.to_thread(_get_dhan_index_future_master)
    except Exception as exc:
        log.warning("futures master fetch error instrument=%s: %s", normalized, exc)
        future_master = {}
    future_contracts = future_master.get(normalized, [])
    futures = [
        {"expiry": c["expiry"], "symbol": c["symbol"], "lot_size": c["lot_size"], "token": str(c["sec_id"]), "ltp": 0.0}
        for c in future_contracts
    ]

    db = MongoData()
    try:
        raw_db = db._db
        from features.execution_socket import _fetch_dhan_index_quotes
        spot_price = 0.0
        try:
            spot_quotes = await asyncio.to_thread(_fetch_dhan_index_quotes, db, {normalized})
            spot_price = _safe_float((spot_quotes.get(normalized) or {}).get("spot_price"))
        except Exception as exc:
            log.warning("futures-chain spot fetch error instrument=%s: %s", normalized, exc)

        option_col = raw_db['active_option_tokens']
        monthly_expiries = {c["expiry"] for c in future_contracts}
        today_str = datetime.now().strftime("%Y-%m-%d")
        weekly_expiries = sorted(
            e for e in option_col.distinct("expiry", {"instrument": normalized, "broker": "dhan"})
            if e >= today_str and e not in monthly_expiries
        )[:4]

        synthetic_futures: list[dict] = []
        atm_legs: dict[str, dict] = {}  # expiry -> {atm_strike, ce_token, pe_token, segment}
        if spot_price > 0:
            for expiry in weekly_expiries:
                strikes = option_col.distinct("strike", {"instrument": normalized, "broker": "dhan", "expiry": expiry})
                if not strikes:
                    continue
                atm_strike = min(strikes, key=lambda s: abs(s - spot_price))
                rows = list(option_col.find(
                    {"instrument": normalized, "broker": "dhan", "expiry": expiry, "strike": atm_strike, "option_type": {"$in": ["CE", "PE"]}},
                    {"_id": 0, "option_type": 1, "token": 1, "tokens": 1, "ws_segment": 1, "lot_size": 1},
                ))
                ce_token = next((r.get("token") or r.get("tokens") for r in rows if r.get("option_type") == "CE"), None)
                pe_token = next((r.get("token") or r.get("tokens") for r in rows if r.get("option_type") == "PE"), None)
                segment = next((str(r.get("ws_segment") or "NSE_FNO").upper() for r in rows), "NSE_FNO")
                lot_size = next((r.get("lot_size") for r in rows if r.get("lot_size")), None)
                if ce_token and pe_token:
                    atm_legs[expiry] = {"atm_strike": atm_strike, "ce_token": str(ce_token), "pe_token": str(pe_token), "segment": segment}
                # ce_token/pe_token/lot_size let the frontend build the equivalent 2-leg
                # option combo (buy ATM call + sell ATM put, or reverse) when the user clicks
                # B/S on a Synthetic Futures row — a synthetic future isn't a tradable
                # instrument on its own, so "trading" one means adding both real option legs.
                synthetic_futures.append({
                    "expiry": expiry,
                    "atm_strike": atm_strike,
                    "ltp": 0.0,
                    "ce_token": str(ce_token) if ce_token else None,
                    "pe_token": str(pe_token) if pe_token else None,
                    "call_ltp": 0.0,
                    "put_ltp": 0.0,
                    "lot_size": lot_size,
                })

        token_ids: list[str] = []
        ws_segments: dict[str, str] = {}
        for c in future_contracts:
            sec_id = str(c["sec_id"]).strip()
            if not sec_id:
                continue
            token_ids.append(sec_id)
            ws_segments[sec_id] = "BSE_FNO" if c["exchange"] == "BSE" else "NSE_FNO"
        for leg in atm_legs.values():
            for token in (leg["ce_token"], leg["pe_token"]):
                token_ids.append(token)
                ws_segments[token] = leg["segment"]

        ltp_by_token: dict[str, float] = {}
        if token_ids:
            cfg = raw_db['kite_market_config'].find_one({'broker': 'dhan', 'enabled': True}) or {}
            access_token = str(cfg.get('access_token') or '').strip()
            client_id = str(cfg.get('user_id') or cfg.get('dhan_client_id') or '').strip()
            if access_token and client_id:
                by_segment: dict[str, list[int]] = {}
                for token in token_ids:
                    try:
                        by_segment.setdefault(ws_segments[token], []).append(int(token))
                    except (TypeError, ValueError):
                        continue
                try:
                    from features.broker_gateway import dhan_quote_post_blocking
                    response = await asyncio.to_thread(dhan_quote_post_blocking, by_segment, access_token, client_id, 5.0)
                    payload = response.json() if response is not None and response.ok else {}
                    data = payload.get('data') or payload or {}
                    for segment in by_segment:
                        segment_data = data.get(segment) or {}
                        if isinstance(segment_data, dict):
                            for sec_id, info in segment_data.items():
                                if not isinstance(info, dict):
                                    continue
                                ltp = _safe_float(info.get('last_price') or info.get('ltp'))
                                if ltp > 0:
                                    ltp_by_token[str(sec_id)] = ltp
                                    _LAST_GOOD_FUTURES_TOKEN_QUOTE[str(sec_id)] = ltp
                except Exception as exc:
                    log.warning("futures/synthetic quote fetch error instrument=%s: %s", normalized, exc)
            # Dhan's /marketfeed/quote can come back empty for a never-before-priced
            # token (just-subscribed, pre-market, transient 429/502) — reuse the last
            # real price this token ever showed instead of a misleading 0.
            for token in token_ids:
                if token not in ltp_by_token and token in _LAST_GOOD_FUTURES_TOKEN_QUOTE:
                    ltp_by_token[token] = _LAST_GOOD_FUTURES_TOKEN_QUOTE[token]

        for item, contract in zip(futures, future_contracts):
            item["ltp"] = ltp_by_token.get(str(contract["sec_id"]), 0.0)
        for item in synthetic_futures:
            leg = atm_legs.get(item["expiry"])
            if not leg:
                continue
            call_ltp = ltp_by_token.get(leg["ce_token"], 0.0)
            put_ltp = ltp_by_token.get(leg["pe_token"], 0.0)
            item["call_ltp"] = call_ltp
            item["put_ltp"] = put_ltp
            if call_ltp > 0 and put_ltp > 0:
                item["ltp"] = item["atm_strike"] + call_ltp - put_ltp
    finally:
        db.close()

    return {"status": "success", "instrument": normalized, "futures": futures, "synthetic_futures": synthetic_futures}


@sim_router.get("/simulator/paper-trade/quotes")
async def simulator_pt_quotes(
    tokens: str = "", broker_id: str = Query(default=""), include_index_defaults: bool = True,
) -> dict:
    """
    Reuses the same canonical sources every other simulator page already
    gets correct numbers from (see features/simulator_risk_monitor.py and
    /live-greeks-chain for the same consolidation):
      - index/spot tokens  -> Dhan's own index ids, same Dhan REST/WS path
                              as the FNO legs below (features.broker_gateway.
                              get_broker_rest_quotes, segment "IDX_I")
      - FNO option tokens  -> features.broker_gateway.get_broker_rest_quotes
    This endpoint used to exclude index tokens (assuming callers never sent
    them) and queried option tokens via its own Dhan REST call. Both were
    bugs: (1) the exclusion check compared against BROKER_INDEX_TOKENS, a
    lazy dict that resolves to whichever broker is *currently* active (Dhan
    IDs: NIFTY=13, SENSEX=51, ...) — a caller sending Kite-style index
    tokens (NIFTY=256265, SENSEX=265, ...), as PaperTradeNew.tsx does, never
    matched, so every index token fell through to the FNO path and always
    came back "unavailable"; (2) the FNO path called Dhan's
    /v2/marketfeed/ltp with ad-hoc segment handling instead of the existing,
    already-correct get_broker_rest_quotes (WS-first, REST fallback, proper
    NSE_FNO/BSE_FNO routing, 429 backoff) used elsewhere.
    Index quotes used to go through features.execution_socket._fetch_dhan_index_quotes
    instead, which is correct but pays a hardcoded 1.1s sleep + its own separate
    Dhan REST call to dodge a 429 against the FNO call right next to it — that
    alone pushed every load of this endpoint past 1s. Folding both into one
    get_broker_rest_quotes() call (one Dhan REST round trip covering every
    segment, or zero once WS has ticked) removes the sleep and the second
    request entirely for the dhan-active case this app actually runs.
    """
    from features.broker_gateway import (
        _KITE_INDEX_TOKENS, _DHAN_INDEX_TOKENS,
        _active_broker as _get_active_broker_name,
        get_broker_rest_quotes,
    )
    from features.execution_socket import _fetch_dhan_index_quotes

    # Recognize an index/spot token regardless of which broker's ID space it
    # was sent in — a caller's token scheme doesn't necessarily match
    # whichever broker happens to be active right now. Both dicts share the
    # same underlying-name KEYS (NIFTY, SENSEX, ...) with different token
    # VALUES, so merging the dicts themselves (`{**a, **b}`) would just have
    # one broker's token silently overwrite the other's for every entry —
    # has to be two separate token->underlying passes instead.
    index_underlying_by_token: dict[str, str] = {}
    for underlying, tok in _KITE_INDEX_TOKENS.items():
        index_underlying_by_token[str(tok)] = underlying
    for underlying, tok in _DHAN_INDEX_TOKENS.items():
        index_underlying_by_token[str(tok)] = underlying

    requested_tokens = [str(token).strip() for token in str(tokens or "").split(",") if str(token).strip()]
    # The scanner's equity-holdings caller (see scanner/router.py::scanner_quotes)
    # opts out of this — it filters these defaults back out of the response
    # anyway (it only ever wants the tokens it asked for), so fetching them
    # was a pure-waste Dhan REST call competing for the same ~1 req/sec
    # account-wide budget every other quote on this same request needs.
    default_tokens = _get_simulator_default_quote_tokens(str(broker_id or "").strip()) if include_index_defaults else []
    unique_tokens = list(dict.fromkeys(requested_tokens + [token for token in default_tokens if token]))
    if not unique_tokens:
        return {"status": "success", "quotes": {}}

    quotes: dict[str, dict[str, float | str]] = {}
    active_broker = _get_active_broker_name()
    db = MongoData()
    try:
        index_tokens = [t for t in unique_tokens if t in index_underlying_by_token]
        fno_tokens = [t for t in unique_tokens if t not in index_underlying_by_token]

        if index_tokens and active_broker == "dhan":
            # Translate each caller-facing index token (Kite or Dhan id space)
            # to Dhan's own index id and fold it into the same batch as the
            # FNO legs below — see get_broker_rest_quotes(combined_tokens, ...)
            # a few lines down.
            dhan_id_by_frontend_token: dict[str, str] = {}
            for token in index_tokens:
                underlying = index_underlying_by_token[token]
                dhan_id = str(_DHAN_INDEX_TOKENS.get(underlying) or "").strip()
                if dhan_id:
                    dhan_id_by_frontend_token[token] = dhan_id
            unresolved_index_tokens = [t for t in index_tokens if t not in dhan_id_by_frontend_token]
        else:
            dhan_id_by_frontend_token = {}
            unresolved_index_tokens = list(index_tokens)

        if unresolved_index_tokens:
            # Broker isn't dhan, or an underlying has no known Dhan index id —
            # fall back to the original (slower but broker-agnostic) path.
            underlyings = {index_underlying_by_token[t] for t in unresolved_index_tokens}
            try:
                index_quotes = await asyncio.to_thread(_fetch_dhan_index_quotes, db, underlyings)
            except Exception as exc:
                log.warning("paper trade index quote error underlyings=%s: %s", underlyings, exc)
                index_quotes = {}
            for token in unresolved_index_tokens:
                underlying = index_underlying_by_token[token]
                spot = float((index_quotes.get(underlying) or {}).get("spot_price") or 0.0)
                if spot > 0:
                    quotes[token] = {"token": token, "ltp": round(spot, 2), "source": "index_quote"}

        # Scanner portfolio holdings send plain NSE equity tokens (Kite-space
        # kite_token, same id scanner/service.py's historical sync already
        # resolves a dhan_security_id for) mixed in with whatever real FNO
        # option tokens this same endpoint also serves for the simulator.
        # Without this carve-out every equity token fell through to the FNO
        # lookup below (never matches active_option_tokens, defaults to
        # segment NSE_FNO) and always came back "unavailable".
        dhan_equity_id_by_frontend_token = (
            _resolve_dhan_equity_ids_by_kite_tokens(fno_tokens, db._db) if fno_tokens and active_broker == "dhan" else {}
        )
        pure_fno_tokens = [t for t in fno_tokens if t not in dhan_equity_id_by_frontend_token]

        # A handful of NSE equities' Dhan NSE_EQ security id happens to equal
        # one of Dhan's 7 reserved index ids (e.g. ADANIENT's id 25 doubles as
        # BANKNIFTY's IDX_I id — both broker token schemes derive straight
        # from NSE's own per-segment token registry, so a collision is just
        # an id reused across segments). dhan_ticker.py's live ltp_map is one
        # flat {security_id: ltp} dict with no segment key — get_broker_rest_quotes'
        # WS-first shortcut below would silently hand back BANKNIFTY's price
        # for "25" instead of ADANIENT's. Route only these few colliding ids
        # around that shortcut via their own forced REST call further down;
        # every other equity id (no collision) still takes the normal,
        # cheaper WS-or-REST batch path.
        _dhan_reserved_index_ids = {str(v) for v in _DHAN_INDEX_TOKENS.values()}
        colliding_equity_id_by_frontend_token = {
            frontend_token: dhan_id
            for frontend_token, dhan_id in dhan_equity_id_by_frontend_token.items()
            if dhan_id in _dhan_reserved_index_ids
        }
        safe_equity_id_by_frontend_token = {
            frontend_token: dhan_id
            for frontend_token, dhan_id in dhan_equity_id_by_frontend_token.items()
            if frontend_token not in colliding_equity_id_by_frontend_token
        }

        if (pure_fno_tokens or dhan_id_by_frontend_token or safe_equity_id_by_frontend_token) and active_broker == "dhan":
            segment_by_token = {
                str(row.get("token") or row.get("tokens") or "").strip(): str(row.get("ws_segment") or "NSE_FNO").strip().upper()
                for row in db._db["active_option_tokens"].find(
                    {"broker": "dhan", "token": {"$in": pure_fno_tokens}},
                    {"_id": 0, "token": 1, "tokens": 1, "ws_segment": 1},
                )
            } if pure_fno_tokens else {}
            for dhan_id in dhan_id_by_frontend_token.values():
                segment_by_token[dhan_id] = "IDX_I"
            for dhan_id in safe_equity_id_by_frontend_token.values():
                segment_by_token[dhan_id] = "NSE_EQ"
            combined_tokens = list(dict.fromkeys(
                pure_fno_tokens + list(dhan_id_by_frontend_token.values()) + list(safe_equity_id_by_frontend_token.values())
            ))
            try:
                # get_broker_rest_quotes wants the raw pymongo Database (db._db),
                # same as every other call site (execution_socket.py,
                # live_option_chain.py, api.py:1637) — passing the MongoData
                # wrapper itself made `db["kite_market_config"]` inside it raise
                # (MongoData isn't subscriptable), silently caught by that
                # function's own try/except, so REST fallback always returned
                # nothing — only the WS path (when a token happened to already
                # have a fresh tick) ever worked. That's the "sometimes 0"
                # intermittency: not random, just REST always failing.
                rest_quotes = await asyncio.to_thread(get_broker_rest_quotes, combined_tokens, db._db, segment_by_token)
            except Exception as exc:
                log.warning("paper trade quote error tokens=%s: %s", ",".join(combined_tokens), exc)
                rest_quotes = {}
            for token, info in rest_quotes.items():
                ltp = float((info or {}).get("ltp") or 0)
                if ltp > 0 and token in unique_tokens:
                    quotes[token] = {"token": token, "ltp": round(ltp, 2), "source": "ws_or_rest"}
            # rest_quotes is keyed by Dhan's own index id (e.g. "13"), not the
            # caller-facing token (e.g. "256265") — copy each resolved index
            # quote back onto the token the frontend actually sent instead of
            # leaking Dhan's internal id as an extra key in the response.
            for frontend_token, dhan_id in dhan_id_by_frontend_token.items():
                info = rest_quotes.get(dhan_id)
                ltp = float((info or {}).get("ltp") or 0)
                if ltp > 0:
                    quotes[frontend_token] = {"token": frontend_token, "ltp": round(ltp, 2), "source": "index_quote"}
            # Same copy-back as above, for the scanner equity tokens carved out
            # of fno_tokens — rest_quotes is keyed by Dhan's security id, not
            # the kite_token the scanner frontend actually sent.
            for frontend_token, dhan_id in safe_equity_id_by_frontend_token.items():
                info = rest_quotes.get(dhan_id)
                ltp = float((info or {}).get("ltp") or 0)
                if ltp > 0:
                    quotes[frontend_token] = {"token": frontend_token, "ltp": round(ltp, 2), "source": "equity_quote"}

        if colliding_equity_id_by_frontend_token and active_broker == "dhan":
            # Bypasses get_broker_rest_quotes entirely — its WS-first check
            # would read these ids straight out of the shared ltp_map, which
            # is exactly the segment-blind shortcut that returns the wrong
            # (index) price for them. A direct, explicitly NSE_EQ-scoped
            # /marketfeed/quote call has no such ambiguity.
            # Uses the *blocking* rate-gate wait (dhan_quote_post_blocking),
            # not skip-on-miss dhan_quote_post — the get_broker_rest_quotes
            # call for the other (non-colliding) equities a few lines above
            # already claims the shared 1-req/sec Dhan slot microseconds
            # earlier in this same request, so a skip-on-miss call here always
            # lost that race and these tokens came back ltp=0 on every single
            # poll, not just under real rate-limit contention (same failure
            # mode dhan_quote_post_blocking's own docstring describes).
            try:
                cfg = db._db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
                access_token = str(cfg.get("access_token") or "").strip()
                client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
                if access_token and client_id:
                    from features.broker_gateway import dhan_quote_post_blocking
                    sec_ids = [int(dhan_id) for dhan_id in colliding_equity_id_by_frontend_token.values()]
                    response = await asyncio.to_thread(
                        dhan_quote_post_blocking, {"NSE_EQ": sec_ids}, access_token, client_id, 5.0,
                    )
                    payload = response.json() if response is not None and response.ok else {}
                    eq_data = (payload.get("data") or {}).get("NSE_EQ") or payload.get("NSE_EQ") or {}
                    for frontend_token, dhan_id in colliding_equity_id_by_frontend_token.items():
                        info = eq_data.get(str(dhan_id)) if isinstance(eq_data, dict) else None
                        ltp = float((info or {}).get("last_price") or 0) if isinstance(info, dict) else 0.0
                        if ltp > 0:
                            _LAST_GOOD_EQUITY_COLLISION_QUOTE[frontend_token] = ltp
                            quotes[frontend_token] = {"token": frontend_token, "ltp": round(ltp, 2), "source": "equity_quote"}
            except Exception as exc:
                log.warning(
                    "scanner equity index-id-collision quote error tokens=%s: %s",
                    list(colliding_equity_id_by_frontend_token), exc,
                )
            # Dhan's REST quote is rate-limited to ~1 req/sec account-wide and
            # this app has several independent periodic callers (live-quote
            # hub's 3s refresh, this same endpoint's own poll, ...) — a 429
            # here is common, not exceptional. Falling back to the last real
            # price seen for these few tokens beats surfacing "unavailable"
            # on every transient rate-limit hit.
            for frontend_token in colliding_equity_id_by_frontend_token:
                if frontend_token not in quotes or not quotes[frontend_token].get("ltp"):
                    cached_ltp = _LAST_GOOD_EQUITY_COLLISION_QUOTE.get(frontend_token)
                    if cached_ltp:
                        quotes[frontend_token] = {"token": frontend_token, "ltp": round(cached_ltp, 2), "source": "equity_quote"}
        if fno_tokens and active_broker != "dhan":
            try:
                if is_configured():
                    api_key, access_token = get_common_credentials()
                    if api_key and access_token:
                        def _kite_quote_call() -> dict:
                            try:
                                kite = get_kite_instance(access_token)
                                return kite.quote([int(token) for token in fno_tokens]) or {}
                            except Exception:
                                return {}
                        quote_docs = await asyncio.to_thread(_kite_quote_call)
                        for quote_key, quote_doc in quote_docs.items():
                            resolved_token = str(
                                quote_doc.get("instrument_token")
                                or quote_key.split(":")[-1]
                                or ""
                            ).strip()
                            if not resolved_token:
                                continue
                            quote_ltp = float(
                                quote_doc.get("last_price")
                                or (quote_doc.get("ohlc") or {}).get("close")
                                or 0.0
                            )
                            if quote_ltp > 0:
                                quotes[resolved_token] = {
                                    "token": resolved_token,
                                    "ltp": round(quote_ltp, 2),
                                    "source": "quote",
                                }
            except Exception as exc:
                log.warning("paper trade quote batch error tokens=%s: %s", ",".join(fno_tokens), exc)
    finally:
        db.close()

    for token in unique_tokens:
        quotes.setdefault(token, {"token": token, "ltp": 0.0, "source": "unavailable"})

    return {"status": "success", "quotes": quotes}








@sim_router.put("/simulator/paper-trade/strategies/{strategy_id}")
async def simulator_pt_update_strategy(strategy_id: str, body: PTStrategyIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        if not _find_owned_strategy(strategy_id, current_user):
            return {"status": "error", "message": "Strategy not found"}
        current_user_id = _resolve_sim_user_id(current_user)
        portfolio_col = _shared_mongo._db["simulator_portfolio"]
        strategy_col = _shared_mongo._db["simulator_strategy"]
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


@sim_router.delete("/simulator/paper-trade/strategies/{strategy_id}")
async def simulator_pt_delete_strategy(strategy_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        if not _find_owned_strategy(strategy_id, current_user):
            return {"status": "error", "message": "Strategy not found"}
        result = _shared_mongo._db["simulator_strategy"].delete_one({"_id": ObjectId(strategy_id)})
        if result.deleted_count == 0:
            return {"status": "error", "message": "Strategy not found"}

        # A triggered webhook's simulator_webhooks doc gets its strategy_id filled in
        # once it fires (see _simulator_pt_webhook_create_strategy) — deleting the
        # strategy from here (Portfolio page, not the Webhook Strategies "new-positions"
        # trash icon) skipped cleaning that up entirely until now, same gap
        # simulator_pt_delete_new_position/simulator_pt_delete_webhook already close for
        # their own delete paths.
        webhook_docs = list(_shared_mongo._db["simulator_webhooks"].find({"strategy_id": strategy_id}))
        if webhook_docs:
            _shared_mongo._db["simulator_webhooks"].delete_many({"strategy_id": strategy_id})
            for webhook_doc in webhook_docs:
                _disable_tv_alerts_for_webhook(str(webhook_doc["_id"]), webhook_doc.get("user_id"))
        # Belt-and-braces: tv_alerts also caches webhook_strategy_id directly at save
        # time (see chart_api.py's save_chart_alert), so catch any alert pointed at
        # this strategy even if its webhookUrl doesn't match a webhook doc above.
        _shared_mongo._db["tv_alerts"].delete_many(
            {"webhook_strategy_id": strategy_id, "webhookEnabled": True},
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def _sim_user_or_filter(user_id: Any) -> dict:
    """
    Matches user_id whether it's stored as the plain string simulator_strategy/
    simulator_portfolio use (see _resolve_sim_user_id) or as a raw ObjectId, which is
    still how the older sim_user_subscriptions collection stores it.
    """
    ids: list[Any] = [user_id]
    try:
        ids.append(ObjectId(user_id))
    except Exception:
        pass
    return {"user_id": {"$in": ids}}


# ── sim_user_subscriptions.status codes ─────────────────────────────────────
# Stored as an int (0/1/2/3), not the old "active"/"expired"/"cancelled"
# strings — every place that reads/writes this field goes through the
# constants/helpers below so the numeric code and its human label never drift
# apart. API responses still return the string label (SIM_SUB_STATUS_LABELS)
# so nothing on the frontend needs to change.
SIM_SUB_STATUS_INACTIVE  = 0
SIM_SUB_STATUS_ACTIVE    = 1
SIM_SUB_STATUS_EXPIRED   = 2
SIM_SUB_STATUS_CANCELLED = 3

SIM_SUB_STATUS_LABELS: dict[int, str] = {
    SIM_SUB_STATUS_INACTIVE: "inactive",
    SIM_SUB_STATUS_ACTIVE: "active",
    SIM_SUB_STATUS_EXPIRED: "expired",
    SIM_SUB_STATUS_CANCELLED: "cancelled",
}


def _sim_sub_effective_status(sub_doc: Optional[dict]) -> int:
    """
    Resolves the CURRENT status code for a sim_user_subscriptions doc.
    Cancelled always wins — once cancelled, it stays cancelled regardless of
    expires_at — otherwise falls back to comparing expires_at against now.

    Fixes a real bug: every caller of this used to compute
    "expired if expires_at has passed else active" straight from expires_at,
    completely ignoring a stored "cancelled" status — so a user whose plan an
    admin had just cancelled (Remove button) would keep showing as "Active"
    right up until natural expiry, both in the UI and in the strategy-limit
    checks that gate creating new strategies.
    """
    if not sub_doc:
        return SIM_SUB_STATUS_INACTIVE
    if sub_doc.get("status") == SIM_SUB_STATUS_CANCELLED:
        return SIM_SUB_STATUS_CANCELLED
    expires_at = sub_doc.get("expires_at")
    now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    if expires_at and expires_at < now_str:
        return SIM_SUB_STATUS_EXPIRED
    return SIM_SUB_STATUS_ACTIVE


def _sim_active_strategy_limit_error(user_id: Any) -> Optional[str]:
    """
    Server-side backstop behind the frontend's StrategyLimitModal gate (which only
    blocks the UI's own Save/Generate Webhook/Trade buttons) — re-checked here so a
    request that bypasses those buttons entirely (devtools-edited DOM, a raw API call)
    still can't create a strategy past the plan's active_strategy_limit. Returns an
    error message when the limit is already hit, else None.
    """
    _seed_sim_subscription_plans_if_empty()
    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    sub_doc = subs_col.find_one(_sim_user_or_filter(user_id), sort=[("_id", -1)]) if user_id is not None else None
    is_active_sub = _sim_sub_effective_status(sub_doc) == SIM_SUB_STATUS_ACTIVE
    plan_id = sub_doc["plan_id"] if (sub_doc and is_active_sub) else "free"
    plan = _sim_find_plan(plan_id) or {}
    plan_name = plan.get("plan_name") or "current"
    limit = (plan or {}).get("active_strategy_limit", -1)
    if limit is None or limit == -1:
        return None
    _ensure_simulator_strategy_index()
    count = _shared_mongo._db["simulator_strategy"].count_documents(
        {"$or": [{"user_id": user_id}, {"user_id": {"$exists": False}}]}
    )
    if count >= limit:
        return f"Strategy limit reached ({count}/{limit}) on your {plan_name} plan. Upgrade your plan to create more strategies."
    return None


def _sim_resolve_plan_and_advanced_slots(user_id: Any) -> tuple[dict, int]:
    """
    Same plan/expiry resolution simulator_subscription_my_plan does inline —
    factored out here so the advanced-slot check below and that endpoint can't
    drift on what counts as the user's active plan.
    """
    _seed_sim_subscription_plans_if_empty()
    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    sub_doc = subs_col.find_one(_sim_user_or_filter(user_id), sort=[("_id", -1)]) if user_id is not None else None
    is_active_sub = _sim_sub_effective_status(sub_doc) == SIM_SUB_STATUS_ACTIVE
    plan_id = sub_doc["plan_id"] if (sub_doc and is_active_sub) else "free"
    plan = _sim_find_plan(plan_id) or {}
    advanced_slots_purchased = int((sub_doc or {}).get("advanced_slots_purchased") or 0) if is_active_sub else 0
    advanced_slots_total = int(plan.get("advanced_slots") or 0) + advanced_slots_purchased
    return plan, advanced_slots_total


def _sim_webhook_url_limit_error(current_user: dict, strategy_id: Optional[str]) -> Optional[str]:
    """
    Server-side backstop behind the frontend's webhookGateBlocked (which only
    gates the UI's own "Generate Webhook URL" buttons) — re-checked here so a
    request that bypasses those buttons entirely (devtools-edited DOM, a raw
    API call) still can't create more webhooks than the plan's
    webhook_url_limit allows. Mirrors simulator_pt_webhook_usage's own
    counting exactly: scoped per strategy_id (None counts this user's still-
    pending "new strategy" drafts instead), and only counts still-active
    (status 1) webhooks — one that's already fired (status 2) has done its
    job and shouldn't keep occupying the slot. Returns an error message when
    the limit is already hit, else None.
    """
    user_id = _resolve_sim_user_id(current_user)
    plan, _ = _sim_resolve_plan_and_advanced_slots(user_id)
    limit = int(plan.get("webhook_url_limit") or 0)
    if limit == -1:
        return None
    webhooks_col = _shared_mongo._db["simulator_webhooks"]
    if strategy_id:
        used = webhooks_col.count_documents({"strategy_id": strategy_id, "adjustment_id": None, "status": 1})
    else:
        used = webhooks_col.count_documents({"strategy_id": None, "user_id": current_user.get("_id"), "status": 1})
    if used >= limit:
        plan_name = plan.get("plan_name") or "current"
        noun = "webhook URL" if limit == 1 else "webhook URLs"
        return f"Your {plan_name} plan allows up to {limit} active {noun} per strategy ({used}/{limit} used). Remove an existing one or upgrade for more."
    return None


def _sim_active_webhook_strategy_count(user_id: Any) -> int:
    """
    Count of currently open (not exited/closed) strategies that were created
    via the "Generate Webhook URL" new-strategy flow (execute_status ==
    "webhook", set in _simulator_pt_webhook_create_strategy) — what
    create_strategy_webhook_limit caps. Strategies created any other way
    (manual Save, or a webhook on an already-existing strategy) never carry
    that flag, so this limit never applies retroactively to strategies that
    already existed — only to new ones born from a "new strategy" webhook hit.
    """
    _ensure_simulator_strategy_index()
    return _shared_mongo._db["simulator_strategy"].count_documents({
        "$or": [{"user_id": user_id}, {"user_id": {"$exists": False}}],
        "execute_status": "webhook",
        "all_exited": {"$ne": True},
        "status": {"$ne": 2},
    })


def _sim_create_strategy_webhook_limit_error(user_id: Any) -> Optional[str]:
    """
    Gate for "Create Strategy via Webhook" (create_strategy_webhook_mode /
    create_strategy_webhook_limit) — deliberately separate from
    trade_generate_webhook_mode/webhook_url_limit above, which stay scoped to
    webhooks on an *existing* strategy (per-side SL webhook, "update
    strategy" webhook). Returns an error message when the feature is off on
    this plan, or the cap on currently-active webhook-created strategies is
    already hit, else None.
    """
    plan, _ = _sim_resolve_plan_and_advanced_slots(user_id)
    if plan.get("create_strategy_webhook_mode") != "enabled":
        return f"Creating strategies via webhook isn't available on your {plan.get('plan_name') or 'current'} plan. Upgrade to unlock this feature."
    limit = int(plan.get("create_strategy_webhook_limit") or 0)
    if limit == -1:
        return None
    used = _sim_active_webhook_strategy_count(user_id)
    if used >= limit:
        plan_name = plan.get("plan_name") or "current"
        noun = "strategy" if limit == 1 else "strategies"
        return f"Your {plan_name} plan allows up to {limit} active {noun} created via webhook ({used}/{limit} used). Close an existing one or upgrade for more."
    return None


def _sim_advanced_strategies(user_id: Any) -> list[dict]:
    """
    Open (not closed) strategies currently occupying an advanced slot. Excludes
    both all_exited and status==2 (closed) — the two are kept in sync by every
    exit path (see simulator_risk_monitor.py), but checking both here means a
    closed strategy stops counting against the slot limit even if a future exit
    path only ever remembers to set one of the two.
    """
    _ensure_simulator_strategy_index()
    docs = _shared_mongo._db["simulator_strategy"].find(
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


def _sim_advanced_slot_limit_error(user_id: Any) -> Optional[str]:
    """
    Mirrors _sim_active_strategy_limit_error above, but against the plan's
    advanced_slots_total instead of active_strategy_limit — the frontend's
    own pre-check (advanced-slot-usage) should normally catch this first and
    offer the downgrade-one-to-free-a-slot flow, so hitting this server-side
    means that check was bypassed (stale UI state, a raw API call, etc).
    """
    plan, advanced_slots_total = _sim_resolve_plan_and_advanced_slots(user_id)
    plan_name = plan.get("plan_name") or "current"
    if advanced_slots_total <= 0:
        return f"Your {plan_name} plan has no Advanced strategy slots. Upgrade your plan or buy extra slots."
    used = len(_sim_advanced_strategies(user_id))
    if used >= advanced_slots_total:
        return f"Advanced slot limit reached ({used}/{advanced_slots_total}) on your {plan_name} plan. Switch an existing Advanced strategy to Normal to free up a slot."
    return None


def _insert_simulator_strategy(
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
    """
    Shared by simulator_pt_save_strategy (the normal "Save" button) and
    _simulator_pt_webhook_create_strategy (a "Generate Webhook URL" hit creating the
    strategy fresh) — same portfolio-lookup-or-create + position_history seeding either
    way, so the two never drift on what a freshly-created strategy doc looks like.
    """
    portfolio_col = _shared_mongo._db["simulator_portfolio"]
    strategy_col = _shared_mongo._db["simulator_strategy"]
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
        # 1 = active, 2 = closed (all legs exited — kept in sync by the risk
        # monitor's exit-fire/expiry-squareoff paths, see simulator_risk_monitor.py),
        # 0 = inactive. Always 1 on creation, whether from the normal Save button
        # (simulator_pt_save_strategy) or a webhook-created strategy
        # (_simulator_pt_webhook_create_strategy) — both funnel through here.
        "status": 1,
        **(extra_fields or {}),
    }
    result = strategy_col.insert_one(doc)
    return str(result.inserted_id)


@sim_router.post("/simulator/paper-trade/strategies")
async def simulator_pt_save_strategy(body: PTStrategyIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        user_id = _resolve_sim_user_id(current_user)
        limit_error = _sim_active_strategy_limit_error(user_id)
        if limit_error:
            return {"status": "error", "message": limit_error}
        execution_mode = "advanced" if str(body.execution_mode or "regular").lower() == "advanced" else "regular"
        if execution_mode == "advanced":
            slot_error = _sim_advanced_slot_limit_error(user_id)
            if slot_error:
                return {"status": "error", "message": slot_error}
        positions = []
        for position in (body.positions or []):
            pos = position.dict()
            if pos.get("quantity") is None:
                pos["quantity"] = (pos.get("lots") or 1) * (pos.get("lot_size") or 1)
            positions.append(pos)
        strategy_id = _insert_simulator_strategy(
            body.portfolio_name, body.strategy_name, body.instrument, body.spot_price,
            body.config, positions, body.mode,
            extra_fields={"execution_mode": execution_mode},
            user_id=user_id,
        )
        return {"status": "success", "id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/paper-trade/advanced-slot-usage")
async def simulator_pt_advanced_slot_usage(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Lets the frontend check Advanced-slot headroom *before* it even lets the
    user pick "Advanced" on the create form — if used >= total, the create
    modal offers switching one of the returned `strategies` to Normal instead
    of letting the save round-trip and bounce off _sim_advanced_slot_limit_error.
    """
    try:
        user_id = _resolve_sim_user_id(current_user)
        _, total = _sim_resolve_plan_and_advanced_slots(user_id)
        strategies = _sim_advanced_strategies(user_id)
        return {"status": "success", "used": len(strategies), "total": total, "strategies": strategies}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@sim_router.get("/simulator/paper-trade/webhook-usage")
async def simulator_pt_webhook_usage(strategy_id: Optional[str] = None, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Real-time, server-verified webhook_url_limit check — every "Generate
    Webhook URL" click re-checks here instead of trusting the frontend's own
    webhookAlerts list, which is session state that can't survive a page
    refresh for a not-yet-saved strategy (its simulator_webhooks docs are
    stored with strategy_id=None, so there's nothing to re-hydrate against).

    strategy_id supplied (saved-strategy / update-webhook flow): counts
    still-active (status 1) webhooks tied to that exact strategy.
    strategy_id omitted (not-yet-saved "new-strategy" webhook flow): counts
    this user's still-pending draft webhooks (strategy_id still None) instead
    — the same rows the Webhook Strategies page (simulator_new_positions)
    tracks, so the two stay consistent with each other.
    """
    try:
        user_id = _resolve_sim_user_id(current_user)
        plan, _ = _sim_resolve_plan_and_advanced_slots(user_id)
        limit = int(plan.get("webhook_url_limit") or 0)
        webhooks_col = _shared_mongo._db["simulator_webhooks"]
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


@sim_router.get("/simulator/paper-trade/create-strategy-webhook-usage")
async def simulator_pt_create_strategy_webhook_usage(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Real-time, server-verified create_strategy_webhook_limit check — the "Generate
    Webhook URL" button on the blank/not-yet-saved builder re-checks here before
    opening its modal (see _sim_create_strategy_webhook_limit_error). Counts
    currently-active strategies already created via this same flow, never
    strategies created any other way — see _sim_active_webhook_strategy_count.
    """
    try:
        user_id = _resolve_sim_user_id(current_user)
        plan, _ = _sim_resolve_plan_and_advanced_slots(user_id)
        mode = plan.get("create_strategy_webhook_mode", "disabled")
        limit = int(plan.get("create_strategy_webhook_limit") or 0)
        used = _sim_active_webhook_strategy_count(user_id)
        return {"status": "success", "mode": mode, "used": used, "total": limit}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


class PTExecutionModeIn(BaseModel):
    execution_mode: str  # "advanced" | "regular"


@sim_router.put("/simulator/paper-trade/strategies/{strategy_id}/execution-mode")
async def simulator_pt_set_execution_mode(strategy_id: str, body: PTExecutionModeIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        doc = _find_owned_strategy(strategy_id, current_user)
        if not doc:
            return {"status": "error", "message": "Strategy not found"}
        mode = "advanced" if str(body.execution_mode or "").lower() == "advanced" else "regular"
        if mode == "advanced" and doc.get("execution_mode") != "advanced":
            user_id = _resolve_sim_user_id(current_user)
            slot_error = _sim_advanced_slot_limit_error(user_id)
            if slot_error:
                return {"status": "error", "message": slot_error}
        _shared_mongo._db["simulator_strategy"].update_one(
            {"_id": ObjectId(strategy_id)}, {"$set": {"execution_mode": mode}},
        )
        return {"status": "success", "execution_mode": mode}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _run_simulator_session(session_id: str, engine: StrategyEngine) -> None:
    try:
        await engine.run()
    finally:
        _simulator_sessions.pop(session_id, None)


# ── Subscription plans ────────────────────────────────────────────────────────
# Mirrors the frontend's DEFAULT_PLANS (src/context/SubscriptionContext.tsx) so
# the UI renders identically whether it's reading its own fallback or this API.
# sim_subscription_plans is seeded once on first read; after that, admin edits
# happen directly in Mongo (no admin UI yet) and are picked up on next request —
# nothing here re-seeds over an existing collection.
_SIM_DEFAULT_PLANS: list[dict[str, Any]] = [
    {
        "slug": "free", "plan_name": "Free", "most_popular": False, "is_active": True,
        "has_billing_toggle": False, "sort_order": 1,
        "price_oneday": None, "price_monthly": 0, "price_yearly": 0, "original_yearly": None,
        "per_day_monthly": None, "per_day_yearly": None,
        "active_strategy_limit": 2, "advanced_slots": 0,
        "paper_trading": True, "live_trading": False, "live_mtm": True, "stop_loss_target": True,
        "upper_lower_adjustment": True, "telegram_email_alerts": True, "buy_extra_slots": False,
        "tradingview_integration": False, "priority_queue": False, "custom_strategies": False,
        "reentry": True, "premium_support": False, "extra_slot_price": None,
        "execution_log_days": 7, "api_priority": "none",
        "card_features": ["Paper Trading", "2 Default Strategies", "30s Execution Engine", "Live MTM", "Telegram / Email Alerts"],
        "card_inherit_label": None,
        "plan_description": "Get started with paper trading and up to 2 strategies, free forever.",
        "billing_type": "monthly",
        "sl_target_individual_leg_sl": True, "sl_target_individual_leg_target": True,
        "sl_target_overall_sl": False, "sl_target_overall_target": False,
        "webhook_url_limit": 0,
        "create_strategy_webhook_mode": "locked", "create_strategy_webhook_limit": 0,
        "tv_bar_replay": False, "tv_alerts_enabled": False, "tv_price_alerts": False, "tv_price_alerts_count": 0,
        "tv_trendline_alerts": False, "tv_trendline_alerts_count": 0,
        "tv_indicator_alerts": False, "tv_max_alerts_per_strategy": 0, "tv_max_indicator_conditions": 0,
        "manual_position_closing": True, "auto_position_management": False,
        "position_telegram_notifications": True,
        "trade_button_mode": "enabled", "trade_generate_webhook_mode": "locked",
        "trade_add_alert_mode": "locked", "trade_payoff_graph_sl_mode": "locked",
        "upgrade_popup_enabled": True, "credit_purchase_enabled": False, "credit_purchase_amount": None,
    },
    {
        "slug": "oneday", "plan_name": "1 Day Plan", "most_popular": False, "is_active": True,
        "has_billing_toggle": False, "sort_order": 2,
        "price_oneday": 149, "price_monthly": None, "price_yearly": None, "original_yearly": None,
        "per_day_monthly": None, "per_day_yearly": None,
        "active_strategy_limit": 50, "advanced_slots": 0,
        "paper_trading": True, "live_trading": True, "live_mtm": True, "stop_loss_target": True,
        "upper_lower_adjustment": True, "telegram_email_alerts": True, "buy_extra_slots": False,
        "tradingview_integration": False, "priority_queue": False, "custom_strategies": True,
        "reentry": True, "premium_support": False, "extra_slot_price": None,
        "execution_log_days": 1, "api_priority": "normal",
        "card_features": ["Live Trading", "50 Active Strategies", "Stop Loss / Target", "Upper / Lower Adjustment", "Live MTM"],
        "card_inherit_label": None,
        "plan_description": "Full live trading access for a single day — try everything before you commit.",
        "billing_type": "oneday",
        "sl_target_individual_leg_sl": True, "sl_target_individual_leg_target": True,
        "sl_target_overall_sl": True, "sl_target_overall_target": True,
        "webhook_url_limit": 0,
        "create_strategy_webhook_mode": "locked", "create_strategy_webhook_limit": 0,
        "tv_bar_replay": False, "tv_alerts_enabled": False, "tv_price_alerts": False, "tv_price_alerts_count": 0,
        "tv_trendline_alerts": False, "tv_trendline_alerts_count": 0,
        "tv_indicator_alerts": False, "tv_max_alerts_per_strategy": 0, "tv_max_indicator_conditions": 0,
        "manual_position_closing": True, "auto_position_management": True,
        "position_telegram_notifications": True,
        "trade_button_mode": "enabled", "trade_generate_webhook_mode": "locked",
        "trade_add_alert_mode": "locked", "trade_payoff_graph_sl_mode": "locked",
        "upgrade_popup_enabled": True, "credit_purchase_enabled": False, "credit_purchase_amount": None,
    },
    {
        "slug": "standard", "plan_name": "Standard", "most_popular": True, "is_active": True,
        "has_billing_toggle": True, "sort_order": 3,
        "price_oneday": None, "price_monthly": 999, "price_yearly": 7999, "original_yearly": 11988,
        "per_day_monthly": 33, "per_day_yearly": 22,
        "active_strategy_limit": 50, "advanced_slots": 3,
        "paper_trading": True, "live_trading": True, "live_mtm": True, "stop_loss_target": True,
        "upper_lower_adjustment": True, "telegram_email_alerts": True, "buy_extra_slots": True,
        "tradingview_integration": True, "priority_queue": False, "custom_strategies": True,
        "reentry": True, "premium_support": False, "extra_slot_price": 75,
        "execution_log_days": 30, "api_priority": "normal",
        "card_features": ["3 Advanced Slots", "TradingView Integration", "Buy Extra Slots", "30 Day Execution Logs"],
        "card_inherit_label": "1 Day Plan +",
        "plan_description": "Unlimited regular strategies plus 3 Advanced slots, TradingView, and Webhooks.",
        "billing_type": "monthly",
        "sl_target_individual_leg_sl": True, "sl_target_individual_leg_target": True,
        "sl_target_overall_sl": True, "sl_target_overall_target": True,
        "webhook_url_limit": 1,
        "create_strategy_webhook_mode": "enabled", "create_strategy_webhook_limit": 1,
        "tv_bar_replay": True, "tv_alerts_enabled": True, "tv_price_alerts": True, "tv_price_alerts_count": 2,
        "tv_trendline_alerts": True, "tv_trendline_alerts_count": 2,
        "tv_indicator_alerts": True, "tv_max_alerts_per_strategy": 2, "tv_max_indicator_conditions": 2,
        "manual_position_closing": True, "auto_position_management": True,
        "position_telegram_notifications": True,
        "trade_button_mode": "enabled", "trade_generate_webhook_mode": "enabled",
        "trade_add_alert_mode": "enabled", "trade_payoff_graph_sl_mode": "enabled",
        "upgrade_popup_enabled": True, "credit_purchase_enabled": True, "credit_purchase_amount": 75,
    },
    {
        "slug": "pro", "plan_name": "Pro", "most_popular": False, "is_active": False,
        "has_billing_toggle": True, "sort_order": 4,
        "price_oneday": None, "price_monthly": 2499, "price_yearly": 17999, "original_yearly": 29988,
        "per_day_monthly": 83, "per_day_yearly": 49,
        "active_strategy_limit": -1, "advanced_slots": 20,
        "paper_trading": True, "live_trading": True, "live_mtm": True, "stop_loss_target": True,
        "upper_lower_adjustment": True, "telegram_email_alerts": True, "buy_extra_slots": True,
        "tradingview_integration": True, "priority_queue": True, "custom_strategies": True,
        "reentry": True, "premium_support": True, "extra_slot_price": 75,
        "execution_log_days": -1, "api_priority": "high",
        "card_features": ["20 Advanced Slots", "Priority Queue", "Unlimited Strategies", "Unlimited Logs", "High API Priority"],
        "card_inherit_label": "Standard Plan +",
        "plan_description": "Everything unlocked — unlimited strategies, 20 Advanced slots, unlimited Webhooks and alerts.",
        "billing_type": "monthly",
        "sl_target_individual_leg_sl": True, "sl_target_individual_leg_target": True,
        "sl_target_overall_sl": True, "sl_target_overall_target": True,
        "webhook_url_limit": -1,
        "create_strategy_webhook_mode": "enabled", "create_strategy_webhook_limit": -1,
        "tv_bar_replay": True, "tv_alerts_enabled": True, "tv_price_alerts": True, "tv_price_alerts_count": -1,
        "tv_trendline_alerts": True, "tv_trendline_alerts_count": -1,
        "tv_indicator_alerts": True, "tv_max_alerts_per_strategy": -1, "tv_max_indicator_conditions": 10,
        "manual_position_closing": True, "auto_position_management": True,
        "position_telegram_notifications": True,
        "trade_button_mode": "enabled", "trade_generate_webhook_mode": "enabled",
        "trade_add_alert_mode": "enabled", "trade_payoff_graph_sl_mode": "enabled",
        "upgrade_popup_enabled": False, "credit_purchase_enabled": False, "credit_purchase_amount": None,
    },
]

# Fields renamed since this collection was first seeded — old key -> new key.
# The old bool (True/False) becomes the equivalent string mode; anything not
# listed here defaults to the new field's seeded value from _SIM_DEFAULT_PLANS.
_SIM_PLAN_FIELD_RENAMES: dict[str, tuple[str, dict[bool, str]]] = {
    "trade_button_enabled":         ("trade_button_mode",          {True: "enabled", False: "disabled"}),
    "trade_generate_webhook_enabled": ("trade_generate_webhook_mode", {True: "enabled", False: "disabled"}),
    "trade_add_alert_enabled":      ("trade_add_alert_mode",       {True: "enabled", False: "disabled"}),
    "trade_payoff_graph_sl_enabled": ("trade_payoff_graph_sl_mode", {True: "enabled", False: "disabled"}),
}


def _looks_like_object_id(value: Any) -> bool:
    try:
        ObjectId(str(value))
        return True
    except Exception:
        return False


def _sim_plan_public(doc: dict) -> dict:
    """
    Serializes a sim_subscription_plans Mongo doc for API responses.
    plan_id is the plan's real Mongo _id (as a string) — replacing the old
    scheme where plan_id was a hand-picked human-readable slug. `slug` (e.g.
    "free") is still exposed where present, purely for readability/debugging;
    nothing outside _sim_find_plan's fallback-to-free path should match on it.
    """
    out = {k: v for k, v in doc.items() if k != "_id"}
    out["plan_id"] = str(doc["_id"])
    out.setdefault("slug", None)
    return out


def _sim_find_plan(value: Optional[str]) -> Optional[dict]:
    """
    Resolves a plan reference to its raw Mongo doc — accepts either the
    plan's real _id (as a string, the normal case for anything read back out
    of sim_user_subscriptions.plan_id) or a slug like "free" (the literal
    fallback string used throughout this file when a user has no active
    subscription). Tries _id first since that's the common path.
    """
    if not value:
        return None
    plans_col = _shared_mongo._db["sim_subscription_plans"]
    if _looks_like_object_id(value):
        doc = plans_col.find_one({"_id": ObjectId(value)})
        if doc:
            return doc
    return plans_col.find_one({"slug": value})


def _seed_sim_subscription_plans_if_empty() -> None:
    col = _shared_mongo._db["sim_subscription_plans"]
    if col.count_documents({}) == 0:
        col.insert_many([dict(p) for p in _SIM_DEFAULT_PLANS])
        return

    # ── One-time migration: plan_id (string slug) -> real Mongo _id ────────
    # This collection used to store its own hand-picked "plan_id" field
    # ("free"/"oneday"/...) as the primary reference. Every plan doc still
    # carrying that old field gets it renamed to `slug` (the _id was always
    # there, just unused as a reference) — self-healing, runs on every
    # request via this function, so it fixes itself in any environment.
    for doc in col.find({"plan_id": {"$exists": True}}):
        col.update_one(
            {"_id": doc["_id"]},
            {"$set": {"slug": doc.get("slug") or doc["plan_id"]}, "$unset": {"plan_id": ""}},
        )

    # sim_user_subscriptions.plan_id used to store that same old slug — any
    # doc still holding a non-ObjectId value there gets remapped to the real
    # plan _id (looked up by slug, now that the rename above has run), so an
    # already-granted subscription doesn't get silently orphaned by the
    # reference-scheme change.
    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    for sub_doc in subs_col.find({"plan_id": {"$exists": True, "$ne": None}}):
        stored = sub_doc.get("plan_id")
        if _looks_like_object_id(stored):
            continue
        plan = col.find_one({"slug": stored})
        if plan:
            subs_col.update_one({"_id": sub_doc["_id"]}, {"$set": {"plan_id": str(plan["_id"])}})

    # sim_user_subscriptions.status used to store "active"/"expired"/
    # "cancelled" strings — migrate any doc still holding a string to the
    # matching numeric code (SIM_SUB_STATUS_*).
    _status_str_to_code = {v: k for k, v in SIM_SUB_STATUS_LABELS.items()}
    for sub_doc in subs_col.find({"status": {"$type": "string"}}):
        code = _status_str_to_code.get(sub_doc["status"])
        if code is not None:
            subs_col.update_one({"_id": sub_doc["_id"]}, {"$set": {"status": code}})

    # Backfill fields added to _SIM_DEFAULT_PLANS after this collection was
    # first seeded (e.g. the Subscription Plan Configuration fields) — only
    # fills keys that are missing entirely, so it never clobbers values an
    # admin has already edited via the Subscription Plans screen. Matched by
    # slug now (plan_id is never stored, always derived from _id).
    for default in _SIM_DEFAULT_PLANS:
        existing = col.find_one({"slug": default["slug"]})
        if existing is None:
            continue
        missing = {k: v for k, v in default.items() if k not in existing}
        # Migrate renamed fields: carry the old bool value over as the new
        # mode string (rather than the schema's default) so an admin's prior
        # enabled/disabled choice survives the rename, then drop the old key.
        unset = {}
        for old_key, (new_key, value_map) in _SIM_PLAN_FIELD_RENAMES.items():
            if old_key in existing:
                if new_key not in existing:
                    missing[new_key] = value_map.get(existing[old_key], default.get(new_key))
                unset[old_key] = ""
        update: dict[str, dict] = {}
        if missing:
            update["$set"] = missing
        if unset:
            update["$unset"] = unset
        if update:
            col.update_one({"_id": existing["_id"]}, update)


@sim_router.get("/simulator/subscription/plans")
async def simulator_subscription_plans() -> list[dict[str, Any]]:
    """Public — no auth. Plan catalogue only, never user-specific data."""
    _seed_sim_subscription_plans_if_empty()
    col = _shared_mongo._db["sim_subscription_plans"]
    return [_sim_plan_public(d) for d in col.find({}).sort("sort_order", 1)]


@sim_router.get("/simulator/subscription/my-plan")
async def simulator_subscription_my_plan(current_user: dict = Depends(app_auth.require_current_user)) -> dict[str, Any]:
    _seed_sim_subscription_plans_if_empty()
    subs_col = _shared_mongo._db["sim_user_subscriptions"]

    user_id = _resolve_sim_user_id(current_user)
    sub_doc = subs_col.find_one(_sim_user_or_filter(user_id), sort=[("_id", -1)]) if user_id else None

    sub_status = _sim_sub_effective_status(sub_doc)
    is_active_sub = sub_status == SIM_SUB_STATUS_ACTIVE

    plan_id = sub_doc["plan_id"] if (sub_doc and is_active_sub) else "free"
    plan = _sim_find_plan(plan_id) or {}

    # Once a paid sub_doc expires/cancels, plan_id (and so plan/limits) falls
    # back to Free above — expires_at/starts_at must follow that same fallback,
    # else the response reports "Free" but still carries the old paid plan's
    # dates (e.g. "Free — expires tomorrow"), which is just the previous
    # plan's stale expiry, not anything to do with the Free tier itself.
    expires_at = sub_doc.get("expires_at") if (sub_doc and is_active_sub) else None
    starts_at  = sub_doc.get("starts_at")  if (sub_doc and is_active_sub) else None

    advanced_slots_purchased = int((sub_doc or {}).get("advanced_slots_purchased") or 0) if is_active_sub else 0
    # "free"/"expired"/"active" here describes which tier is in effect (used
    # by the frontend's isFreePlan checks), not the raw subscription-row
    # status — a cancelled sub_doc already resolves plan_id back to "free"
    # above, so it naturally lands in the "free" branch here too. Checked via
    # plan["slug"] rather than plan_id == "free" directly — a persisted free
    # sub_doc (see the admin user-sim-plan backfill) has its plan_id
    # self-healing-migrated from the "free" slug to the plan's real _id by
    # _seed_sim_subscription_plans_if_empty, so the literal string won't
    # always match even though the plan itself is still Free.
    status = "free" if plan.get("slug") == "free" else ("expired" if sub_status == SIM_SUB_STATUS_EXPIRED else "active")

    return {
        "plan_id": str(plan.get("_id", "")),
        "plan_name": plan["plan_name"],
        "active_strategy_limit": plan["active_strategy_limit"],
        "advanced_slots_included": plan["advanced_slots"],
        "advanced_slots_purchased": advanced_slots_purchased,
        "advanced_slots_total": plan["advanced_slots"] + advanced_slots_purchased,
        "paper_trading": plan["paper_trading"],
        "live_trading": plan["live_trading"],
        "live_mtm": plan["live_mtm"],
        "stop_loss_target": plan["stop_loss_target"],
        "upper_lower_adjustment": plan["upper_lower_adjustment"],
        "telegram_email_alerts": plan["telegram_email_alerts"],
        "buy_extra_slots": plan["buy_extra_slots"],
        "tradingview_integration": plan["tradingview_integration"],
        "priority_queue": plan["priority_queue"],
        "execution_log_days": plan["execution_log_days"],
        "api_priority": plan["api_priority"],
        "status": status,
        "expires_at": expires_at,
        "starts_at": starts_at,
        # Granular fields added for the Subscription Plan Configuration
        # screen — previously only exposed on the admin-facing plan catalog
        # (GET /simulator/admin/subscription-plans), never on this
        # resolved-per-user endpoint, so no Simulator page could actually
        # gate anything on them.
        "sl_target_individual_leg_sl": plan["sl_target_individual_leg_sl"],
        "sl_target_individual_leg_target": plan["sl_target_individual_leg_target"],
        "sl_target_overall_sl": plan["sl_target_overall_sl"],
        "sl_target_overall_target": plan["sl_target_overall_target"],
        "webhook_url_limit": plan["webhook_url_limit"],
        # New, so any custom plan saved before this change won't have them yet
        # — same .get()-with-default pattern as tv_price_alerts below.
        "create_strategy_webhook_mode": plan.get("create_strategy_webhook_mode", "disabled"),
        "create_strategy_webhook_limit": plan.get("create_strategy_webhook_limit", 0),
        "tv_bar_replay": plan["tv_bar_replay"],
        "tv_alerts_enabled": plan["tv_alerts_enabled"],
        # .get() with a default here, unlike the plain [] access on the older
        # fields above — these two are new, so any custom (non-built-in-slug)
        # plan saved before this change won't have them yet, and only the
        # built-in free/oneday/standard/pro plans get auto-backfilled (see
        # _seed_sim_subscription_plans_if_empty's slug-matched backfill).
        "tv_price_alerts": plan.get("tv_price_alerts", False),
        "tv_price_alerts_count": plan.get("tv_price_alerts_count", 0),
        "tv_trendline_alerts": plan["tv_trendline_alerts"],
        "tv_trendline_alerts_count": plan.get("tv_trendline_alerts_count", 0),
        "tv_indicator_alerts": plan["tv_indicator_alerts"],
        "tv_max_alerts_per_strategy": plan["tv_max_alerts_per_strategy"],
        "tv_max_indicator_conditions": plan["tv_max_indicator_conditions"],
        "manual_position_closing": plan["manual_position_closing"],
        "auto_position_management": plan["auto_position_management"],
        "position_telegram_notifications": plan["position_telegram_notifications"],
        "trade_button_mode": plan["trade_button_mode"],
        "trade_generate_webhook_mode": plan["trade_generate_webhook_mode"],
        "trade_add_alert_mode": plan["trade_add_alert_mode"],
        "trade_payoff_graph_sl_mode": plan["trade_payoff_graph_sl_mode"],
        "upgrade_popup_enabled": plan["upgrade_popup_enabled"],
    }


@sim_router.get("/simulator/admin/user-sim-plan/{user_id}")
async def simulator_admin_get_user_sim_plan(user_id: str) -> dict:
    """Admin: get the current sim plan for a given user_id. A user with no
    subscription history at all has never had a row in sim_user_subscriptions
    — the Free tier was purely synthesized on the fly and so had nothing for
    the admin Subscriptions table to show. Backfill one real "free" row here
    (plan_id stays the literal "free" slug, same sentinel every other
    resolver — my-plan, resolve_user_plan, etc. — already falls back to when
    there's no sub_doc, so this changes nothing about how those resolve;
    it just makes the Free tier a persisted, admin-visible record too)."""
    _seed_sim_subscription_plans_if_empty()
    subs_col  = _shared_mongo._db["sim_user_subscriptions"]

    sub_doc = subs_col.find_one(_sim_user_or_filter(user_id), sort=[("_id", -1)])
    if not sub_doc:
        free_plan = _sim_find_plan("free")
        free_name = free_plan.get("plan_name", "Free") if free_plan else "Free"
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        try:
            user_ref: Any = ObjectId(user_id)
        except Exception:
            user_ref = user_id
        subs_col.insert_one({
            "user_id":      user_ref,
            "plan_id":      "free",
            "plan_name":    free_name,
            "status":       SIM_SUB_STATUS_ACTIVE,
            "starts_at":    now_str,
            "expires_at":   None,
            "billing":      None,
            "reference_by": "system_free",
            "advanced_slots_purchased": 0,
            "updated_at":   now_str,
        })
        return {"plan_id": "free", "plan_name": free_name, "status": "free", "expires_at": None, "reference_by": "system_free"}

    expires_at = sub_doc.get("expires_at")
    status     = SIM_SUB_STATUS_LABELS[_sim_sub_effective_status(sub_doc)]
    plan_id    = sub_doc.get("plan_id", "free")
    plan_doc   = _sim_find_plan(plan_id) or {}
    # Checked via plan_doc["slug"] rather than plan_id == "free" — see the
    # matching note in simulator_subscription_my_plan above.
    if plan_doc.get("slug") == "free" and status == "active":
        status = "free"

    return {
        "plan_id":      plan_id,
        "plan_name":    plan_doc.get("plan_name") or sub_doc.get("plan_name") or plan_id,
        "status":       status,
        "expires_at":   expires_at,
        "starts_at":    sub_doc.get("starts_at"),
        "billing":      sub_doc.get("billing"),
        "reference_by": sub_doc.get("reference_by"),
    }


@sim_router.delete("/simulator/admin/user-sim-plan/{user_id}")
async def simulator_admin_cancel_user_sim_plan(user_id: str) -> dict:
    """Admin: cancel the given user's current active sim plan. Targets only
    the currently-active row (by _id) — sim_user_subscriptions is an
    append-only history now, so a blind update-by-user_id could otherwise
    hit an arbitrary past (already-cancelled/expired) row instead of the
    live one."""
    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    current = subs_col.find_one(
        {**_sim_user_or_filter(user_id), "status": SIM_SUB_STATUS_ACTIVE},
        sort=[("_id", -1)],
    )
    if not current:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="No active sim plan found for this user")
    subs_col.update_one(
        {"_id": current["_id"]},
        {"$set": {"status": SIM_SUB_STATUS_CANCELLED, "updated_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")}},
    )
    return {"ok": True}


@sim_router.post("/simulator/admin/grant-sim-plan")
async def simulator_admin_grant_sim_plan(payload: dict) -> dict:
    """
    Admin-only: instantly activate a sim plan for any user without payment.
    Body: { user_id, plan_id, plan_name, validity_days, billing }
    """
    user_id_raw   = str(payload.get("user_id") or "").strip()
    plan_id_input = str(payload.get("plan_id") or "").strip()
    validity_days = int(payload.get("validity_days") or 30)
    billing       = str(payload.get("billing") or "monthly")

    if not user_id_raw or not plan_id_input:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="user_id and plan_id are required")

    _seed_sim_subscription_plans_if_empty()
    # Accepts either the plan's real _id or a slug (e.g. "free") — always
    # normalized to the real _id below before it's stored, so
    # sim_user_subscriptions.plan_id is consistently an _id regardless of
    # which form the caller passed.
    plan_doc = _sim_find_plan(plan_id_input)
    if not plan_doc:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Plan '{plan_id_input}' not found in sim_subscription_plans")
    plan_id   = str(plan_doc["_id"])
    plan_name = str(payload.get("plan_name") or plan_doc.get("plan_name") or plan_id)

    now = datetime.now(IST)
    now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
    expires_at = (now + timedelta(days=validity_days)).strftime("%Y-%m-%dT%H:%M:%S")

    # Support both ObjectId and string storage — try ObjectId first
    try:
        user_oid = ObjectId(user_id_raw)
    except Exception:
        user_oid = None

    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    # Always insert a fresh row — sim_user_subscriptions is an append-only
    # history of every grant/cancel, not one row per user that gets
    # overwritten in place. Any row still marked active for this user is
    # superseded (cancelled) first, mirroring the same
    # cancel-then-insert pattern payment.py's admin_grant_subscription/
    # verify_payment already use for the generic subscriptions collection.
    subs_col.update_many(
        {**_sim_user_or_filter(user_id_raw), "status": SIM_SUB_STATUS_ACTIVE},
        {"$set": {"status": SIM_SUB_STATUS_CANCELLED, "updated_at": now_str}},
    )
    subs_col.insert_one({
        "user_id":     user_oid if user_oid else user_id_raw,
        "plan_id":     plan_id,
        "plan_name":   plan_name,
        "status":      SIM_SUB_STATUS_ACTIVE,
        "starts_at":   now_str,
        "expires_at":  expires_at,
        "billing":     billing,
        "payment_id":  "admin_grant",
        "reference_by": "admin",
        "advanced_slots_purchased": 0,
        "updated_at":  now_str,
    })

    return {
        "ok": True,
        "user_id": user_id_raw,
        "plan_id": plan_id,
        "plan_name": plan_name,
        "validity_days": validity_days,
        "expires_at": expires_at,
    }


# ── Subscription plan admin CRUD ──────────────────────────────────────────────
# The Subscription Plan Configuration screen (Admin > Subscription Plans) reads
# and writes sim_subscription_plans through these — previously admin edits only
# happened directly in Mongo (see comment at _SIM_DEFAULT_PLANS above).

class AdminPlanIn(BaseModel):
    # plan_id is NOT settable here — it's always the plan's real Mongo _id
    # (auto-generated on create, immutable after). slug is an optional,
    # purely internal/cosmetic tag (only "free" is actually load-bearing —
    # see _sim_find_plan's fallback-to-free-plan resolution).
    slug: Optional[str] = None
    plan_name: str
    plan_description: str = ""
    billing_type: str = "monthly"  # monthly | yearly | lifetime | oneday
    most_popular: bool = False
    is_active: bool = True
    has_billing_toggle: bool = False
    sort_order: int = 0
    # Pricing
    price_oneday: Optional[float] = None
    price_monthly: Optional[float] = None
    price_yearly: Optional[float] = None
    original_yearly: Optional[float] = None
    per_day_monthly: Optional[float] = None
    per_day_yearly: Optional[float] = None
    # Strategy config
    active_strategy_limit: int = -1  # -1 = unlimited
    advanced_slots: int = 0
    extra_slot_price: Optional[float] = None
    buy_extra_slots: bool = False
    # Stop Loss & Target
    stop_loss_target: bool = False
    sl_target_individual_leg_sl: bool = False
    sl_target_individual_leg_target: bool = False
    sl_target_overall_sl: bool = False
    sl_target_overall_target: bool = False
    # Webhook — trade_generate_webhook_mode is the sole on/off/locked control now
    # (webhook_enabled used to duplicate it as a separate boolean; removed since
    # the two always moved together and having both was confusing to configure).
    # Gates webhooks on an *existing* strategy only (per-side SL webhook,
    # "update strategy" webhook) — see create_strategy_webhook_mode below for
    # the separate "create a brand-new strategy via webhook" feature.
    webhook_url_limit: int = 0  # -1 = unlimited
    # Distinct from trade_generate_webhook_mode/webhook_url_limit above: gates
    # only the "Generate Webhook URL" flow that creates a brand-new strategy
    # (blank/not-yet-saved builder) once the webhook fires. Never applies to
    # webhooks on an existing strategy. create_strategy_webhook_limit caps how
    # many currently-active (not exited/closed) strategies created this way a
    # user can have at once — strategies created any other way never count
    # against it.
    create_strategy_webhook_mode: Literal["enabled", "disabled", "locked"] = "disabled"
    create_strategy_webhook_limit: int = 0  # -1 = unlimited
    # TradingView
    tradingview_integration: bool = False
    tv_bar_replay: bool = False
    tv_alerts_enabled: bool = False
    tv_price_alerts: bool = False
    tv_price_alerts_count: int = 0  # -1 = unlimited
    tv_trendline_alerts: bool = False
    tv_trendline_alerts_count: int = 0  # -1 = unlimited
    tv_indicator_alerts: bool = False
    tv_max_alerts_per_strategy: int = 0  # -1 = unlimited
    tv_max_indicator_conditions: int = 0
    # Position management
    manual_position_closing: bool = True
    auto_position_management: bool = False
    position_telegram_notifications: bool = False
    # Trade features — 3-state per button: "enabled" (works normally), "disabled"
    # (hidden), "locked" (visible but shows a plan-comparison prompt on click —
    # actual click-time wiring into the live Simulator pages is a follow-up;
    # this only stores the admin's chosen mode for now).
    trade_button_mode: Literal["enabled", "disabled", "locked"] = "enabled"
    trade_generate_webhook_mode: Literal["enabled", "disabled", "locked"] = "disabled"
    trade_add_alert_mode: Literal["enabled", "disabled", "locked"] = "disabled"
    trade_payoff_graph_sl_mode: Literal["enabled", "disabled", "locked"] = "disabled"
    # Upgrade configuration
    upgrade_popup_enabled: bool = True
    credit_purchase_enabled: bool = False
    credit_purchase_amount: Optional[float] = None
    # Misc existing flags
    paper_trading: bool = True
    live_trading: bool = False
    live_mtm: bool = True
    upper_lower_adjustment: bool = False
    telegram_email_alerts: bool = False
    priority_queue: bool = False
    custom_strategies: bool = False
    reentry: bool = False
    premium_support: bool = False
    execution_log_days: int = 7
    api_priority: str = "none"
    card_features: list[str] = []
    card_inherit_label: Optional[str] = None


def _normalize_plan_cascade(doc: dict) -> dict:
    """
    Server-side backstop mirroring the admin UI's parent/child greying-out —
    zeroes out child permissions whenever their parent is disabled, so a direct
    API call can't save an inconsistent hierarchy (GitHub-style permissions:
    disabling a parent disables everything under it).
    """
    if not doc.get("stop_loss_target"):
        doc["sl_target_individual_leg_sl"] = False
        doc["sl_target_individual_leg_target"] = False
        doc["sl_target_overall_sl"] = False
        doc["sl_target_overall_target"] = False
    if doc.get("advanced_slots", 0) <= 0:
        doc["advanced_slots"] = 0
    if not doc.get("tradingview_integration"):
        doc["tv_bar_replay"] = False
        doc["tv_alerts_enabled"] = False
        doc["tv_price_alerts"] = False
        doc["tv_price_alerts_count"] = 0
        doc["tv_trendline_alerts"] = False
        doc["tv_trendline_alerts_count"] = 0
        doc["tv_indicator_alerts"] = False
        doc["tv_max_alerts_per_strategy"] = 0
        doc["tv_max_indicator_conditions"] = 0
    if not doc.get("tv_alerts_enabled"):
        doc["tv_price_alerts"] = False
        doc["tv_price_alerts_count"] = 0
        doc["tv_trendline_alerts"] = False
        doc["tv_trendline_alerts_count"] = 0
        doc["tv_indicator_alerts"] = False
        doc["tv_max_alerts_per_strategy"] = 0
    if not doc.get("tv_price_alerts"):
        doc["tv_price_alerts_count"] = 0
    if not doc.get("tv_trendline_alerts"):
        doc["tv_trendline_alerts_count"] = 0
    if not doc.get("tv_indicator_alerts"):
        doc["tv_max_indicator_conditions"] = 0
    if doc.get("trade_generate_webhook_mode") == "disabled":
        doc["webhook_url_limit"] = 0
    if doc.get("create_strategy_webhook_mode") == "disabled":
        doc["create_strategy_webhook_limit"] = 0
    if not doc.get("credit_purchase_enabled"):
        doc["credit_purchase_amount"] = None
    return doc


@sim_router.get("/simulator/admin/subscription-plans")
async def simulator_admin_list_plans() -> list[dict[str, Any]]:
    """Admin: full plan catalogue including inactive plans, all fields — plus
    active_subscribers, a live count from sim_user_subscriptions (the only
    thing tying a user to a plan; there's no real DB foreign key between the
    two collections) so the admin list can show "N users on this plan"."""
    _seed_sim_subscription_plans_if_empty()
    col = _shared_mongo._db["sim_subscription_plans"]
    plans = [_sim_plan_public(d) for d in col.find({}).sort("sort_order", 1)]

    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    counts_by_plan: dict[str, int] = {}
    for row in subs_col.aggregate([
        {"$match": {"status": SIM_SUB_STATUS_ACTIVE}},
        {"$group": {"_id": "$plan_id", "count": {"$sum": 1}}},
    ]):
        counts_by_plan[row["_id"]] = row["count"]

    for plan in plans:
        plan["active_subscribers"] = counts_by_plan.get(plan["plan_id"], 0)
    return plans


@sim_router.get("/simulator/admin/subscriptions")
async def simulator_admin_list_subscriptions(plan_id: Optional[str] = None) -> list[dict[str, Any]]:
    """
    Admin: every sim_user_subscriptions row, joined with the subscriber's
    name/email (from user_details) and the plan's current name (from
    sim_subscription_plans) — this is the actual "which user has which plan"
    view, since sim_user_subscriptions.plan_id is only a matching string, not
    a real foreign key into sim_subscription_plans. Optional ?plan_id= filters
    to one plan's subscribers (used by the "N active users" badge on the
    Subscription Plans list).
    """
    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    query: dict[str, Any] = {"plan_id": plan_id} if plan_id else {}
    docs = list(subs_col.find(query).sort("expires_at", -1))

    users_col = _shared_mongo._db[app_auth.USERS_COLLECTION]
    plans_col = _shared_mongo._db["sim_subscription_plans"]
    plan_names = {str(p["_id"]): p["plan_name"] for p in plans_col.find({}, {"plan_name": 1})}

    rows = []
    for doc in docs:
        user_id_raw = doc.get("user_id")
        user_doc = None
        try:
            user_doc = users_col.find_one({"_id": ObjectId(str(user_id_raw))}, {"name": 1, "email": 1, "mobile": 1})
        except Exception:
            pass
        rows.append({
            "user_id": str(user_id_raw),
            "user_name": (user_doc or {}).get("name"),
            "user_email": (user_doc or {}).get("email"),
            "user_mobile": (user_doc or {}).get("mobile"),
            "plan_id": doc.get("plan_id"),
            "plan_name": plan_names.get(doc.get("plan_id")) or doc.get("plan_name"),
            "status": SIM_SUB_STATUS_LABELS[_sim_sub_effective_status(doc)],
            "billing": doc.get("billing"),
            "starts_at": doc.get("starts_at"),
            "expires_at": doc.get("expires_at"),
            "reference_by": doc.get("reference_by"),
        })
    return rows


@sim_router.post("/simulator/admin/subscription-plans")
async def simulator_admin_create_plan(body: AdminPlanIn) -> dict:
    """Admin: create a new subscription plan. plan_id is never accepted from
    the client — Mongo assigns the real _id on insert, and that's the only
    plan_id this (or any) plan will ever have."""
    col = _shared_mongo._db["sim_subscription_plans"]
    doc = body.dict()
    doc = _normalize_plan_cascade(doc)
    result = col.insert_one(doc)
    return {"ok": True, "plan_id": str(result.inserted_id)}


@sim_router.put("/simulator/admin/subscription-plans/{plan_id}")
async def simulator_admin_update_plan(plan_id: str, body: AdminPlanIn) -> dict:
    """Admin: update an existing subscription plan (full replace of editable fields)."""
    try:
        oid = ObjectId(plan_id)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid plan_id '{plan_id}'")
    col = _shared_mongo._db["sim_subscription_plans"]
    doc = body.dict()
    doc = _normalize_plan_cascade(doc)
    result = col.update_one({"_id": oid}, {"$set": doc}, upsert=False)
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail=f"Plan '{plan_id}' not found")
    return {"ok": True}


@sim_router.delete("/simulator/admin/subscription-plans/{plan_id}")
async def simulator_admin_delete_plan(plan_id: str) -> dict:
    """Admin: delete a subscription plan. The plan whose slug is "free" is
    protected — it's the hardcoded fallback every plan-resolution helper
    falls back to (see _sim_resolve_plan_and_advanced_slots etc.), so
    removing it would break expired/no-subscription users everywhere.

    Also blocked if any sim_user_subscriptions doc still actively references
    this plan_id (its real _id) — without this check a delete here would
    silently orphan those users' subscriptions."""
    try:
        oid = ObjectId(plan_id)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid plan_id '{plan_id}'")
    col = _shared_mongo._db["sim_subscription_plans"]
    target = col.find_one({"_id": oid})
    if target and target.get("slug") == "free":
        raise HTTPException(status_code=400, detail="Cannot delete the Free plan — it's the system fallback for expired/no subscriptions.")
    subs_col = _shared_mongo._db["sim_user_subscriptions"]
    active_count = subs_col.count_documents({"plan_id": plan_id, "status": SIM_SUB_STATUS_ACTIVE})
    if active_count > 0:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete '{plan_id}' — {active_count} user(s) are currently active on this plan. Move them to another plan first.",
        )
    result = col.delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"Plan '{plan_id}' not found")
    return {"ok": True}


app.include_router(sim_router)
# simulator_router (simulator/api_server.py) self-prefixes with "/simulator"
# already (router = APIRouter(prefix="/simulator")) — mount with no extra
# prefix here. It has a handful of endpoints sim_router doesn't (chart-state,
# alerts, paper-trade/strategies/{id}/{alert-config,manual-check,sl-marker});
# everything else it defines duplicates a sim_router path and is shadowed
# (sim_router was included first, so its version wins — harmless).
app.include_router(simulator_router)
app.include_router(live_quote_socket_router)
# Same payment_router (hard-linked to algo.trade/features/payment.py, see
# shared/features/payment.py) mounted under this service's own "/simulator"
# prefix instead of algo.trade's "/algo" — identical handler code, reachable
# at http://localhost:8001/simulator/auth/subscriptions here.
from features.payment import payment_router  # noqa: E402
app.include_router(payment_router, prefix="/simulator")


# ─── Kite Broker Endpoints ────────────────────────────────────────────────────

# Temporary in-memory store: session_id → broker_doc_id
# Cleared after use (one-time use per login)
_kite_pending: dict = {}






# ── Dhan OAuth endpoints ──────────────────────────────────────────────────────
_dhan_pending: dict[str, str] = {}


def _dhan_popup_result_html(success: bool, message: str) -> str:
    import json as _json
    payload_js = _json.dumps({"type": "DHAN_LOGIN", "success": success, "message": message})
    icon  = "✓" if success else "✗"
    color = "#22c55e" if success else "#ef4444"
    title = "Login Successful" if success else "Login Failed"
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Dhan Login</title>
<style>
  body{{font-family:-apple-system,sans-serif;display:flex;align-items:center;
       justify-content:center;min-height:100vh;margin:0;background:#0f172a;color:#f1f5f9;}}
  .card{{text-align:center;padding:2rem;background:#1e293b;border-radius:12px;border:1px solid #334155;}}
  .icon{{font-size:3rem;color:{color};}}
  p{{color:#94a3b8;}}
</style></head>
<body>
  <div class="card">
    <div class="icon">{icon}</div>
    <h2>{title}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) window.opener.postMessage(payload, "*");
    setTimeout(() => window.close(), 1500);
  </script>
</body></html>"""












_FNO_STOCKS_CACHE: dict = {}        # {"data": [...], "fetched_at": float}
_FNO_MASTER_CACHE: dict = {}        # {"rows": {symbol: [contracts]}, "fetched_at": float}
_FNO_CACHE_TTL = 3600               # refresh once per hour

_DHAN_SCRIP_MASTER_CACHE: dict = {}  # {"rows": [csv_row_dict, ...], "date": "YYYY-MM-DD"}


def _get_dhan_scrip_master_rows() -> list[dict]:
    """
    Raw Dhan scrip master CSV rows (~30MB file), downloaded once per calendar day
    and shared by every Dhan contract sync — stocks, indices, anything else —
    so the file is fetched at most once a day no matter how many instruments sync.
    """
    import io as _io, csv as _csv, requests as _req
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_SCRIP_MASTER_CACHE.get("rows") and _DHAN_SCRIP_MASTER_CACHE.get("date") == today_str:
        return _DHAN_SCRIP_MASTER_CACHE["rows"]

    resp = _req.get("https://images.dhan.co/api-data/api-scrip-master.csv", timeout=30)
    resp.raise_for_status()
    rows = list(_csv.DictReader(_io.StringIO(resp.text)))
    _DHAN_SCRIP_MASTER_CACHE["rows"] = rows
    _DHAN_SCRIP_MASTER_CACHE["date"] = today_str
    return rows


def _get_dhan_fno_master() -> dict[str, list[dict]]:
    """
    Returns {symbol: [{sec_id, strike, opt_type, expiry, exchange}]} from
    Dhan security master CSV.  Cached for 1 hour.
    Also populates _FNO_MASTER_CACHE["equity_ids"] = {symbol: sec_id} for spot lookup.
    """
    import time as _t
    if _FNO_MASTER_CACHE.get("rows") and (_t.time() - _FNO_MASTER_CACHE.get("fetched_at", 0)) < _FNO_CACHE_TTL:
        return _FNO_MASTER_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    equity_ids: dict[str, str] = {}
    for row in reader:
        inst = row.get("SEM_INSTRUMENT_NAME", "").strip()
        exch = row.get("SEM_EXM_EXCH_ID", "").strip()
        sec_id = row.get("SEM_SMST_SECURITY_ID", "").strip()
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()

        # Capture NSE equity security IDs for spot price lookup
        # Dhan CSV may use EQUITY, ES, EQ or similar for cash equity
        _deriv_types = {"OPTSTK", "OPTIDX", "FUTSTK", "FUTIDX", "FUTCUR", "OPTCUR", "FUTCOM", "OPTFUT"}
        if exch == "NSE" and inst not in _deriv_types and ts and sec_id:
            sym = ts.split("-")[0].strip()
            if sym and sym not in equity_ids:
                equity_ids[sym] = sec_id

        if inst != "OPTSTK":
            continue
        symbol = ts.split("-")[0].strip() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        entry = {
            "sec_id":   sec_id,
            "strike":   float(row.get("SEM_STRIKE_PRICE") or 0),
            "opt_type": row.get("SEM_OPTION_TYPE", "").strip().upper(),
            "expiry":   expiry,
            "exchange": exch,
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        }
        master.setdefault(symbol, []).append(entry)

    _FNO_MASTER_CACHE["rows"] = master
    _FNO_MASTER_CACHE["equity_ids"] = equity_ids
    _FNO_MASTER_CACHE["fetched_at"] = _t.time()
    return master


_LAST_GOOD_EQUITY_COLLISION_QUOTE: dict[str, float] = {}  # frontend (kite) token -> last real ltp


def _get_dhan_equity_sec_id(symbol: str) -> str:
    """Return the NSE equity security ID for a stock symbol from Dhan CSV cache."""
    _get_dhan_fno_master()  # ensure cache is populated
    return str(_FNO_MASTER_CACHE.get("equity_ids", {}).get(symbol.strip().upper()) or "")


def _resolve_dhan_equity_ids_by_kite_tokens(kite_tokens: list[str], db) -> dict[str, str]:
    """
    kite_token -> dhan_security_id for scanner equity holdings, via scanner_stocks_list
    (same dhan_security_id field scanner/service.py's historical-data sync already
    resolves per-row via _resolve_stock_dhan_security_id — this just batches that lookup
    by token for the live quote endpoint). Lets a caller tell a scanner stock's Kite-space
    token apart from a simulator FNO/option token before deciding which Dhan segment to
    query — scanner holdings were always falling into the FNO-only lookup otherwise.
    """
    if not kite_tokens:
        return {}
    docs = db["scanner_stocks_list"].find(
        {"kite_token": {"$in": kite_tokens}},
        {"_id": 0, "kite_token": 1, "dhan_security_id": 1},
    )
    return {
        str(doc["kite_token"]): str(doc["dhan_security_id"])
        for doc in docs
        if doc.get("kite_token") and doc.get("dhan_security_id")
    }


_DHAN_INDEX_OPTION_CACHE: dict = {}  # {"rows": {instrument: [contract, ...]}, "date": "YYYY-MM-DD"}


def _get_dhan_index_option_master() -> dict[str, list[dict]]:
    """
    Returns {instrument: [{sec_id, symbol, strike, opt_type, expiry, exchange, lot_size}]}
    for index (OPTIDX) contracts — NIFTY, SENSEX, BANKNIFTY, etc. — straight from Dhan's
    scrip master CSV. The CSV is ~30MB, so it's downloaded once per calendar day and
    reused for every call that day, same caching shape as _get_dhan_fno_master() above.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_INDEX_OPTION_CACHE.get("rows") and _DHAN_INDEX_OPTION_CACHE.get("date") == today_str:
        return _DHAN_INDEX_OPTION_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in reader:
        if row.get("SEM_INSTRUMENT_NAME", "").strip() != "OPTIDX":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "strike":   float(row.get("SEM_STRIKE_PRICE") or 0),
            "opt_type": row.get("SEM_OPTION_TYPE", "").strip().upper(),
            "expiry":   expiry,
            "exchange": row.get("SEM_EXM_EXCH_ID", "").strip(),
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    _DHAN_INDEX_OPTION_CACHE["rows"] = master
    _DHAN_INDEX_OPTION_CACHE["date"] = today_str
    return master


_DHAN_INDEX_FUTURE_CACHE: dict = {}  # {"rows": {instrument: [contract, ...]}, "date": "YYYY-MM-DD"}

# token -> last real nonzero LTP ever seen for it via /simulator/paper-trade/futures-chain.
# Never evicted, same "a slightly stale real quote beats showing 0" reasoning as
# execution_socket.py's _LAST_GOOD_UNDERLYING_QUOTE — futures/ATM-option tokens are
# priced via dhan_quote_post_blocking (see simulator_pt_futures_chain), not the
# shared get_broker_rest_quotes/_LAST_GOOD_QUOTE path, so this is this endpoint's own.
_LAST_GOOD_FUTURES_TOKEN_QUOTE: dict[str, float] = {}


def _get_dhan_index_future_master() -> dict[str, list[dict]]:
    """
    Returns {instrument: [{sec_id, symbol, expiry, exchange, lot_size}]} for index
    (FUTIDX) futures contracts — NIFTY, SENSEX, BANKNIFTY, etc. — straight from Dhan's
    scrip master CSV, same caching shape as _get_dhan_index_option_master() above.

    These were never synced into active_option_tokens: _get_dhan_fno_master() and
    _get_dhan_index_option_master() both explicitly skip every FUT* instrument type
    (they only ever kept OPTSTK/OPTIDX), so there's no Mongo collection to query —
    this reads the CSV directly instead, same as the option masters do.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_INDEX_FUTURE_CACHE.get("rows") and _DHAN_INDEX_FUTURE_CACHE.get("date") == today_str:
        return _DHAN_INDEX_FUTURE_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in reader:
        if row.get("SEM_INSTRUMENT_NAME", "").strip() != "FUTIDX":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "expiry":   expiry,
            "exchange": row.get("SEM_EXM_EXCH_ID", "").strip(),
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    for contracts in master.values():
        contracts.sort(key=lambda c: c["expiry"])

    _DHAN_INDEX_FUTURE_CACHE["rows"] = master
    _DHAN_INDEX_FUTURE_CACHE["date"] = today_str
    return master


_SIMULATOR_STRATEGY_INDEX_ENSURED = False


def _ensure_simulator_strategy_index() -> None:
    """
    Every per-user strategy read (list, the active-strategy-limit count, the
    live_quote_socket MTM broadcast's per-session cache refresh) filters by
    user_id — without this index each of those is a full collection scan, which
    is fine at a few thousand docs and genuinely dangerous once this collection
    is in the crores (see the "10 crore total strategies" scale discussion).
    """
    global _SIMULATOR_STRATEGY_INDEX_ENSURED
    if _SIMULATOR_STRATEGY_INDEX_ENSURED:
        return
    try:
        _shared_mongo._db["simulator_strategy"].create_index(
            [("user_id", 1), ("all_exited", 1)],
            name="idx_simulator_strategy_user_v1",
        )
    except Exception:
        pass
    try:
        # One-time backfill for docs saved before the `status` field existed
        # (1 = active, 2 = closed, 0 = inactive — see _insert_simulator_strategy).
        # all_exited already tracked "closed" pre-status, so mirror it rather
        # than defaulting every pre-existing doc to active regardless of state.
        strategy_col = _shared_mongo._db["simulator_strategy"]
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
    _SIMULATOR_STRATEGY_INDEX_ENSURED = True


_ACTIVE_OPTION_TOKENS_INDEX_ENSURED = False


def _ensure_active_option_tokens_index(col) -> None:
    """
    Create the compound index every Dhan contract upsert matches on, once per process.
    Without it, each upsert inside a bulk_write does a full collection scan to check for
    an existing match — that alone turned a multi-thousand-contract sync from under a
    second into ~10s per instrument (measured: NIFTY's 4080 contracts 9.8s -> 0.28s).
    """
    global _ACTIVE_OPTION_TOKENS_INDEX_ENSURED
    if _ACTIVE_OPTION_TOKENS_INDEX_ENSURED:
        return
    try:
        col.create_index(
            [("broker", 1), ("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
            name="idx_active_option_contract_v2",
        )
    except Exception:
        pass
    _ACTIVE_OPTION_TOKENS_INDEX_ENSURED = True


def _sync_dhan_index_option_tokens(instrument: str) -> dict:
    """
    Refresh active_option_tokens for one index instrument from Dhan's scrip master
    (see _get_dhan_index_option_master). Replaces the Kite-instrument-cache path for
    indices when Dhan is the active broker — that path is skipped entirely for Dhan
    and was only ever serving a stale, narrow strike range from whatever was already
    in the DB.
    """
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_index_option_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan index option contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            opt_type = c["opt_type"]
            if opt_type not in {"CE", "PE"}:
                continue
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            exch = c.get("exchange") or ("BSE" if normalized in {"SENSEX", "BANKEX"} else "NSE")
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": c["strike"],
                "option_type": opt_type,
            }
            update_payload = {
                **key,
                "instrument_type": "index",
                "exchange": exch,
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-{c['strike']}-{opt_type}",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "BSE_FNO" if exch == "BSE" else "NSE_FNO",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens sync completed from Dhan scrip master",
        }
    finally:
        db.close()


def _sync_dhan_index_future_tokens(instrument: str) -> dict:
    """
    Refresh active_option_tokens for one index's FUTIDX contracts (see
    _get_dhan_index_future_master). Same `option_type: "FUT", strike: 0.0` shape
    _sync_dhan_commodity_tokens already uses for MCX futures (FUTCOM) — that's
    proof this collection's compound index and every downstream reader already
    tolerate a strike-less contract; this just does the same thing for index
    futures, which were never synced anywhere before (every other index-token
    sync explicitly skips FUT* instrument types).
    """
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_index_future_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan index future contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            exch = c.get("exchange") or ("BSE" if normalized in {"SENSEX", "BANKEX"} else "NSE")
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": 0.0,
                "option_type": "FUT",
            }
            update_payload = {
                **key,
                "instrument_type": "future",
                "exchange": exch,
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-FUT",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "BSE_FNO" if exch == "BSE" else "NSE_FNO",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens FUT sync completed from Dhan scrip master",
        }
    finally:
        db.close()


_DHAN_COMMODITY_MASTER_CACHE: dict = {}  # {"rows": {underlying: [contract, ...]}, "date": "YYYY-MM-DD"}


def _get_dhan_commodity_master() -> dict[str, list[dict]]:
    """
    Returns {underlying: [{sec_id, symbol, strike, opt_type, expiry, exchange, lot_size}]}
    for every MCX commodity — gold, silver, crude oil, copper, and everything else Dhan
    lists on MCX — covering both futures (FUTCOM, opt_type "FUT", strike 0) and options
    on futures (OPTFUT, opt_type CE/PE). Underlyings aren't a fixed list like the indices;
    they're discovered straight from whatever Dhan's scrip master actually carries.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_COMMODITY_MASTER_CACHE.get("rows") and _DHAN_COMMODITY_MASTER_CACHE.get("date") == today_str:
        return _DHAN_COMMODITY_MASTER_CACHE["rows"]

    rows = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID", "").strip() != "MCX":
            continue
        inst = row.get("SEM_INSTRUMENT_NAME", "").strip()
        if inst not in ("FUTCOM", "OPTFUT"):
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        if inst == "FUTCOM":
            opt_type, strike = "FUT", 0.0
        else:
            opt_type = row.get("SEM_OPTION_TYPE", "").strip().upper()
            strike = float(row.get("SEM_STRIKE_PRICE") or 0)
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "strike":   strike,
            "opt_type": opt_type,
            "expiry":   expiry,
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    _DHAN_COMMODITY_MASTER_CACHE["rows"] = master
    _DHAN_COMMODITY_MASTER_CACHE["date"] = today_str
    return master


def _sync_dhan_commodity_tokens(instrument: str) -> dict:
    """Refresh active_option_tokens for one MCX commodity (futures + options) from Dhan's scrip master."""
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_commodity_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan commodity contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            opt_type = c["opt_type"]
            if opt_type not in {"CE", "PE", "FUT"}:
                continue
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": c["strike"],
                "option_type": opt_type,
            }
            update_payload = {
                **key,
                "instrument_type": "commodity",
                "exchange": "MCX",
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-{c['strike']}-{opt_type}",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "MCX_COMM",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens sync completed from Dhan scrip master (commodity)",
        }
    finally:
        db.close()



















def _kite_popup_html(
    success: bool,
    message: str,
    access_token: str = "",
    user_id: str = "",
    user_name: str = "",
    broker_doc_id: str = "",
) -> str:
    payload = {
        "type":          "KITE_LOGIN",
        "success":       success,
        "message":       message,
        "access_token":  access_token,
        "user_id":       user_id,
        "user_name":     user_name,
        "broker_doc_id": broker_doc_id,
    }
    import json as _json
    payload_js = _json.dumps(payload)
    status_color = "#22c55e" if success else "#ef4444"
    status_icon  = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Kite Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">You can close this window after checking the URL.</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
  </script>
</body>
</html>"""




# ─── FlatTrade postback (order status push) ──────────────────────────────────

async def _parse_flattrade_postback_payload(request: Request) -> dict:
    data: dict = {}
    try:
        query_params = dict(request.query_params or {})
    except Exception:
        query_params = {}

    try:
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8", errors="replace")
    except Exception:
        body_str = ""

    try:
        if body_str.startswith("jData="):
            import urllib.parse
            parsed = urllib.parse.parse_qs(body_str)
            jdata_str = (parsed.get("jData") or ["{}"])[0]
            data = json.loads(jdata_str)
        elif body_str.strip():
            data = json.loads(body_str)
    except Exception as exc:
        log.warning("[FLATTRADE POSTBACK] body parse error: %s", exc)
        data = {}

    if not data and query_params:
        if "jData" in query_params:
            try:
                data = json.loads(str(query_params.get("jData") or "{}"))
            except Exception as exc:
                log.warning("[FLATTRADE POSTBACK] query jData parse error: %s", exc)
                data = {}
        else:
            data = query_params
    return data if isinstance(data, dict) else {}


def _process_flattrade_postback_payload(
    *,
    data: dict,
    broker_doc_id: str = "",
    source_tag: str = "FLATTRADE POSTBACK",
) -> None:
    from features.live_order_manager import process_broker_order_update

    order_id = str(data.get("norenordno") or data.get("order_id") or "").strip()
    status_raw = str(data.get("status") or "").upper().strip()
    fill_price = float(data.get("avgprc") or data.get("flprc") or data.get("prc") or 0)
    fill_qty = int(data.get("fillshares") or data.get("filledshares") or data.get("qty") or 0)
    rej_reason = str(data.get("rejreason") or data.get("emsg") or "").lower()
    uid = str(data.get("uid") or data.get("actid") or "").strip()

    log.info(
        "[%s] broker=%s uid=%s order_id=%s status=%s fill=%.2f qty=%d payload=%s",
        source_tag,
        broker_doc_id or "-",
        uid or "-",
        order_id,
        status_raw,
        fill_price,
        fill_qty,
        data,
    )

    if not order_id:
        return

    _status_map = {
        "COMPLETE": "COMPLETE",
        "COMPLETED": "COMPLETE",
        "REJECTED": "REJECTED",
        "CANCELLED": "CANCELLED",
        "CANCELED": "CANCELLED",
        "OPEN": "OPEN",
        "TRIGGER_PENDING": "TRIGGER_PENDING",
    }
    status = _status_map.get(status_raw, status_raw)
    if status not in ("COMPLETE", "REJECTED", "CANCELLED"):
        return

    local_db = MongoData()
    try:
        if broker_doc_id:
            broker_order = local_db._db["broker_orders"].find_one(
                {"order_id": order_id},
                {"trade_id": 1},
            )
            if broker_order:
                trade_id = str(broker_order.get("trade_id") or "").strip()
                trade = local_db._db["algo_trades"].find_one(
                    {"_id": trade_id},
                    {"broker": 1},
                )
                trade_broker = str((trade or {}).get("broker") or "").strip()
                if trade_broker and trade_broker != broker_doc_id:
                    log.warning(
                        "[%s] broker=%s order_id=%s belongs_to=%s - skipping",
                        source_tag, broker_doc_id, order_id, trade_broker,
                    )
                    return

        updated = process_broker_order_update(
            local_db,
            order_id=order_id,
            status=status,
            fill_price=fill_price,
            fill_qty=fill_qty,
            rejection_reason=rej_reason,
            source="postback",
        )
        if not updated and status == "COMPLETE":
            exit_doc = local_db._db["broker_orders"].find_one(
                {"order_id": order_id},
                {"order_side": 1, "status": 1, "trade_id": 1, "leg_id": 1, "exit_reason": 1},
            ) or {}
            if str(exit_doc.get("order_side") or "").strip() == "exit":
                from features.live_order_manager import _sync_live_exit_fill
                trade_id = str(exit_doc.get("trade_id") or "").strip()
                leg_id = str(exit_doc.get("leg_id") or "").strip()
                exit_reason = str(exit_doc.get("exit_reason") or "stoploss").strip() or "stoploss"
                if trade_id and leg_id and fill_price > 0:
                    local_db._db["broker_orders"].update_one(
                        {"order_id": order_id},
                        {"$set": {
                            "status": "COMPLETE",
                            "fill_price": float(fill_price or 0),
                            "fill_qty": int(fill_qty or 0),
                            "filled_at": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                            "updated_at": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                        }},
                    )
                    _sync_live_exit_fill(local_db, trade_id, leg_id, exit_reason, fill_price)
                    updated = True
                    log.info(
                        "[%s] forced exit sync broker=%s order_id=%s trade=%s leg=%s reason=%s fill=%.2f",
                        source_tag, broker_doc_id or "-", order_id, trade_id, leg_id, exit_reason, fill_price,
                    )
        log.info("[%s] broker=%s order_id=%s updated=%s", source_tag, broker_doc_id or "-", order_id, updated)
    except Exception as exc:
        log.error("[%s] processing error broker=%s order_id=%s: %s", source_tag, broker_doc_id or "-", order_id, exc)
    finally:
        try:
            local_db.close()
        except Exception:
            pass










# ─── FlatTrade broker login ───────────────────────────────────────────────────

_flattrade_pending: dict = {}
















def _broker_popup_html(
    broker: str,
    success: bool,
    message: str,
    access_token: str = "",
    user_id: str = "",
    user_name: str = "",
    broker_doc_id: str = "",
) -> str:
    import json as _json
    payload = {
        "type":          f"{broker.upper()}_LOGIN",
        "success":       success,
        "message":       message,
        "access_token":  access_token,
        "user_id":       user_id,
        "user_name":     user_name,
        "broker_doc_id": broker_doc_id,
    }
    payload_js   = _json.dumps(payload)
    status_color = "#22c55e" if success else "#ef4444"
    status_icon  = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{broker} Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
    setTimeout(() => window.close(), 1500);
  </script>
</body>
</html>"""


# ─── Live Market Data (KiteTicker) ───────────────────────────────────────────

_LIVE_CONTROL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Live Trade Control</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center;
    }
    .card {
      background: #1e293b; border: 1px solid #334155;
      border-radius: 16px; padding: 2.5rem 3rem;
      width: 420px; text-align: center;
    }
    .title {
      font-size: 1.25rem; font-weight: 600; color: #94a3b8;
      margin-bottom: 2rem; letter-spacing: 0.05em; text-transform: uppercase;
    }
    .status-row {
      display: flex; align-items: center; justify-content: center;
      gap: 0.6rem; margin-bottom: 2rem;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #475569; transition: background 0.3s;
    }
    .dot.running    { background: #22c55e; box-shadow: 0 0 8px #22c55e; animation: pulse 1.5s infinite; }
    .dot.stopped    { background: #ef4444; }
    .dot.connecting { background: #f59e0b; animation: pulse 0.8s infinite; }
    .dot.error      { background: #ef4444; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .status-text { font-size: 1rem; font-weight: 500; color: #cbd5e1; text-transform: capitalize; }
    .btn {
      width: 100%; padding: 1rem; border: none; border-radius: 10px;
      font-size: 1.1rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.2s, transform 0.1s; letter-spacing: 0.03em;
    }
    .btn:active { transform: scale(0.98); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-start { background: #22c55e; color: #fff; }
    .btn-start:hover:not(:disabled) { opacity: 0.9; }
    .btn-stop  { background: #ef4444; color: #fff; }
    .btn-stop:hover:not(:disabled)  { opacity: 0.9; }
    .stats {
      display: grid; grid-template-columns: 1fr 1fr;
      gap: 0.75rem; margin-top: 1.75rem;
    }
    .stat-box {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.75rem;
    }
    .stat-label { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1.1rem; font-weight: 700; color: #e2e8f0; }
    .spot-section { margin-top: 1.5rem; text-align: left; }
    .spot-title { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .spot-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .spot-item {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.5rem 0.75rem;
      display: flex; justify-content: space-between; align-items: center;
    }
    .spot-name  { font-size: 0.75rem; color: #94a3b8; font-weight: 600; }
    .spot-price { font-size: 0.85rem; color: #22c55e; font-weight: 700; }
    .spot-price.na { color: #475569; }
    .error-msg {
      margin-top: 1rem; font-size: 0.8rem; color: #f87171;
      background: #1a0a0a; border-radius: 6px; padding: 0.5rem 0.75rem; display: none;
    }
    .started-at { margin-top: 1rem; font-size: 0.72rem; color: #475569; }
  </style>
</head>
<body>
<div class="card">
  <div class="title">Live Trade Control</div>
  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span class="status-text" id="statusText">Loading...</span>
  </div>
  <button class="btn" id="actionBtn" disabled onclick="handleAction()">...</button>
  <div class="stats">
    <div class="stat-box">
      <div class="stat-label">Ticks Received</div>
      <div class="stat-value" id="tickCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">LTP Tokens</div>
      <div class="stat-value" id="ltpCount">—</div>
    </div>
  </div>
  <div class="spot-section">
    <div class="spot-title">Spot Prices</div>
    <div class="spot-grid">
      <div class="spot-item"><span class="spot-name">NIFTY</span><span class="spot-price na" id="spot-NIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">BANKNIFTY</span><span class="spot-price na" id="spot-BANKNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">FINNIFTY</span><span class="spot-price na" id="spot-FINNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">SENSEX</span><span class="spot-price na" id="spot-SENSEX">—</span></div>
    </div>
  </div>
  <div class="error-msg" id="errorMsg"></div>
  <div class="started-at" id="startedAt"></div>
</div>
<script>
  const API = '';

  async function fetchStatus() {
    try {
      const res  = await fetch(API + '/live/status');
      const data = await res.json();
      renderStatus(data);
    } catch(e) {
      renderStatus({ status: 'error', error: 'Cannot reach server' });
    }
  }

  function renderStatus(data) {
    const status = data.status || 'stopped';
    document.getElementById('dot').className       = 'dot ' + status;
    document.getElementById('statusText').textContent = status;

    const btn = document.getElementById('actionBtn');
    btn.disabled = false;
    if (status === 'running') {
      btn.textContent = 'Stop Live Trading';
      btn.className   = 'btn btn-stop';
    } else if (status === 'connecting') {
      btn.textContent = 'Connecting...';
      btn.className   = 'btn btn-start';
      btn.disabled    = true;
    } else {
      btn.textContent = 'Start Live Trading';
      btn.className   = 'btn btn-start';
    }

    document.getElementById('tickCount').textContent =
      data.tick_count !== undefined ? data.tick_count.toLocaleString() : '—';
    document.getElementById('ltpCount').textContent =
      data.ltp_count !== undefined ? data.ltp_count.toLocaleString() : '—';

    const spotMap = data.spot_map || {};
    ['NIFTY','BANKNIFTY','FINNIFTY','SENSEX'].forEach(sym => {
      const el = document.getElementById('spot-' + sym);
      const v  = spotMap[sym];
      if (!el) return;
      if (v) {
        el.textContent = '\\u20B9' + Number(v).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        el.className = 'spot-price';
      } else {
        el.textContent = '—';
        el.className = 'spot-price na';
      }
    });

    const errEl = document.getElementById('errorMsg');
    if (data.error) { errEl.textContent = data.error; errEl.style.display = 'block'; }
    else            { errEl.style.display = 'none'; }

    const startEl = document.getElementById('startedAt');
    startEl.textContent = data.started_at
      ? 'Started: ' + data.started_at.replace('T',' ').slice(0,19)
      : '';
  }

  async function handleAction() {
    const btn    = document.getElementById('actionBtn');
    const status = document.getElementById('statusText').textContent;
    btn.disabled    = true;
    btn.textContent = 'Please wait...';
    try {
      const url = status === 'running' ? '/live/stop' : '/live/start';
      await fetch(API + url + '?ui=1');
    } catch(e) { console.error(e); }
    setTimeout(fetchStatus, 800);
    setTimeout(fetchStatus, 2000);
    setTimeout(fetchStatus, 4000);
  }

  fetchStatus();
  setInterval(fetchStatus, 3000);
</script>
</body>
</html>"""


def _start_ticker_bg():
    """Run in background thread — loads tokens from DB and starts KiteTicker."""
    _db = MongoData()
    try:
        print(
            f'[MONITOR TICKER START] '
            f'current_status={ticker_manager.status} '
            f'tick_count={int(ticker_manager.tick_count or 0)}'
        )
        if ticker_manager.status == "running":
            ticker_manager.restart(_db._db)
        else:
            ticker_manager.start(_db._db)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("ticker start error: %s", exc)
    finally:
        try:
            _db.close()
        except Exception:
            pass


def _build_monitor_control_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Live + Fast-Forward Monitor</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(34, 197, 94, 0.14), transparent 34%),
        linear-gradient(160deg, #07111f 0%, #0f172a 55%, #111827 100%);
      color: #e5eefb;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(560px, calc(100vw - 32px));
      background: rgba(10, 19, 34, 0.94);
      border: 1px solid rgba(148, 163, 184, 0.18);
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 22px 70px rgba(0, 0, 0, 0.35);
    }}
    .eyebrow {{
      color: #7dd3fc;
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    .title {{
      font-size: 30px;
      font-weight: 700;
      margin-bottom: 10px;
    }}
    .subtitle {{
      color: #94a3b8;
      font-size: 14px;
      line-height: 1.6;
      margin-bottom: 20px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 16px;
      align-items: center;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(125, 211, 252, 0.12);
      border-radius: 18px;
      padding: 18px 20px;
      margin-bottom: 18px;
    }}
    .status-row {{
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 18px;
      font-weight: 600;
    }}
    .dot {{
      width: 12px;
      height: 12px;
      border-radius: 999px;
      background: #64748b;
      box-shadow: 0 0 0 transparent;
    }}
    .dot.running {{ background: #22c55e; box-shadow: 0 0 12px rgba(34, 197, 94, 0.8); }}
    .dot.connecting {{ background: #f59e0b; box-shadow: 0 0 12px rgba(245, 158, 11, 0.8); }}
    .dot.stopped {{ background: #ef4444; box-shadow: 0 0 12px rgba(239, 68, 68, 0.45); }}
    .clock-box {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .clock-label {{
      color: #64748b;
      font-size: 11px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}
    .clock-value {{
      margin-top: 6px;
      font-size: 18px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .stat {{
      background: rgba(15, 23, 42, 0.85);
      border: 1px solid rgba(148, 163, 184, 0.12);
      border-radius: 16px;
      padding: 14px 16px;
    }}
    .stat-label {{
      font-size: 11px;
      color: #64748b;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }}
    .stat-value {{
      font-size: 19px;
      font-weight: 700;
      line-height: 1.35;
      word-break: break-word;
    }}
    .actions {{
      display: flex;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .btn {{
      flex: 1;
      border: none;
      border-radius: 14px;
      padding: 14px 16px;
      font-size: 15px;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.12s ease, opacity 0.2s ease;
    }}
    .btn:active {{ transform: scale(0.985); }}
    .btn-primary {{ background: linear-gradient(135deg, #22c55e, #16a34a); color: #04110a; }}
    .btn-danger {{ background: linear-gradient(135deg, #f97316, #ef4444); color: #fff7ed; }}
    .btn-secondary {{ background: #1e293b; color: #cbd5e1; border: 1px solid rgba(148, 163, 184, 0.18); }}
    .btn:disabled {{ opacity: 0.55; cursor: not-allowed; }}
    .panel {{
      background: rgba(15, 23, 42, 0.88);
      border: 1px solid rgba(148, 163, 184, 0.12);
      border-radius: 18px;
      padding: 16px;
    }}
    .panel-title {{
      color: #cbd5e1;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    .strategies {{
      display: flex;
      flex-direction: column;
      gap: 10px;
      max-height: 220px;
      overflow: auto;
    }}
    .strategy-item {{
      border-radius: 12px;
      padding: 12px 14px;
      background: rgba(8, 15, 28, 0.9);
      border: 1px solid rgba(148, 163, 184, 0.1);
    }}
    .strategy-name {{
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .strategy-meta {{
      color: #94a3b8;
      font-size: 12px;
      line-height: 1.5;
    }}
    .empty {{
      color: #94a3b8;
      font-size: 13px;
      line-height: 1.6;
      padding: 10px 4px 2px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="eyebrow">Auto Monitor</div>
    <div class="title">Live + Fast-Forward Monitor</div>
    <div class="subtitle">
      Single control page for both <b>live</b> and <b>fast-forward</b>. The backend supervisor starts automatically,
      refreshes active strategies every second, and keeps the live execution path highest priority.
    </div>

    <div class="hero">
      <div class="status-row">
        <span class="dot stopped" id="statusDot"></span>
        <span id="statusText">Loading...</span>
      </div>
      <div class="clock-box">
        <div class="clock-label">Server Time</div>
        <div class="clock-value" id="serverTime">--</div>
      </div>
    </div>

    <div class="grid">
      <div class="stat">
        <div class="stat-label">Trade Date</div>
        <div class="stat-value" id="tradeDateValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Live Count</div>
        <div class="stat-value" id="liveCountValue">0</div>
      </div>
      <div class="stat">
        <div class="stat-label">Started At</div>
        <div class="stat-value" id="startedAtValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Fast-Forward Count</div>
        <div class="stat-value" id="ffCountValue">0</div>
      </div>
      <div class="stat">
        <div class="stat-label">Last Tick</div>
        <div class="stat-value" id="lastTickValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Ticker Ticks</div>
        <div class="stat-value" id="tickCountValue">0</div>
      </div>
    </div>

    <div class="actions">
      <button class="btn btn-primary" id="toggleBtn" onclick="toggleMonitor()" disabled>Loading...</button>
      <button class="btn btn-secondary" onclick="refreshStatus()">Refresh</button>
    </div>

    <div class="panel">
      <div class="panel-title">Live Strategies</div>
      <div class="strategies" id="strategiesBox">
        <div class="empty">Checking active live strategies...</div>
      </div>
    </div>

    <div class="panel" style="margin-top: 14px;">
      <div class="panel-title">Fast-Forward Strategies</div>
      <div class="strategies" id="ffStrategiesBox">
        <div class="empty">Checking active fast-forward strategies...</div>
      </div>
    </div>
  </div>

  <script>
    function formatDateTime(value) {{
      if (!value) return '--';
      return String(value).replace('T', ' ').slice(0, 19);
    }}

    function escapeHtml(value) {{
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    async function startMonitorSilently() {{
      try {{
        await fetch('/monitor/start');
      }} catch (err) {{
        console.error(err);
      }}
    }}

    async function refreshStatus() {{
      try {{
        const res = await fetch('/monitor/status');
        const data = await res.json();
        renderStatus(data);
      }} catch (err) {{
        console.error(err);
      }}
    }}

    function renderStatus(data) {{
      const running = !!data.running;
      const status = data.monitor_status || (running ? 'running' : 'stopped');
      const button = document.getElementById('toggleBtn');
      const statusDot = document.getElementById('statusDot');
      const statusText = document.getElementById('statusText');
      const serverTime = document.getElementById('serverTime');
      const tradeDate = document.getElementById('tradeDateValue');
      const startedAt = document.getElementById('startedAtValue');
      const lastTick = document.getElementById('lastTickValue');
      const liveCountValue = document.getElementById('liveCountValue');
      const ffCountValue = document.getElementById('ffCountValue');
      const tickCountValue = document.getElementById('tickCountValue');
      const strategiesBox = document.getElementById('strategiesBox');
      const ffStrategiesBox = document.getElementById('ffStrategiesBox');

      statusDot.className = 'dot ' + (running ? 'running' : 'stopped');
      statusText.textContent = running ? 'Listening' : 'Stopped';
      serverTime.textContent = formatDateTime(data.server_time);
      tradeDate.textContent = data.trade_date || '--';
      startedAt.textContent = formatDateTime(data.started_at);
      lastTick.textContent = formatDateTime(data.last_tick_at);
      liveCountValue.textContent = String(((data.counts || {}).live) || 0);
      ffCountValue.textContent = String(((data.counts || {})['fast-forward']) || 0);
      tickCountValue.textContent = String(data.tick_count || 0);

      button.disabled = false;
      button.textContent = running ? 'Stop Listening' : 'Start Listening';
      button.className = 'btn ' + (running ? 'btn-danger' : 'btn-primary');
      button.dataset.running = running ? '1' : '0';

      const recordsByMode = data.records_by_mode || {{}};
      const liveRecords = Array.isArray(recordsByMode.live) ? recordsByMode.live : [];
      const ffRecords = Array.isArray(recordsByMode['fast-forward']) ? recordsByMode['fast-forward'] : [];

      function renderRecords(records, emptyText) {{
        if (!records.length) {{
          return '<div class="empty">' + emptyText + '</div>';
        }}
        return records.map(function(record) {{
          return (
            '<div class="strategy-item">' +
              '<div class="strategy-name">' + escapeHtml(record.name || '-') + '</div>' +
              '<div class="strategy-meta">' +
                'Group: ' + escapeHtml(record.group_name || '-') + '<br>' +
                'Ticker: ' + escapeHtml(record.ticker || '-') + '<br>' +
                'Mode: ' + escapeHtml(record.activation_mode || '-') + '<br>' +
                'Entry: ' + escapeHtml(record.entry_time || '-') + ' | Exit: ' + escapeHtml(record.exit_time || '-') + '<br>' +
                'Open Legs: ' + escapeHtml(record.open_legs || 0) + '/' + escapeHtml(record.total_legs || 0) +
              '</div>' +
            '</div>'
          );
        }}).join('');
      }}

      strategiesBox.innerHTML = renderRecords(
        liveRecords,
        'No active live strategies right now. Supervisor still keeps checking every second.'
      );
      ffStrategiesBox.innerHTML = renderRecords(
        ffRecords,
        'No active fast-forward strategies right now. Supervisor still keeps checking every second.'
      );
    }}

    async function toggleMonitor() {{
      const button = document.getElementById('toggleBtn');
      const running = button.dataset.running === '1';
      button.disabled = true;
      button.textContent = 'Please wait...';
      try {{
        const path = running ? '/monitor/stop' : '/monitor/start';
        await fetch(path);
      }} catch (err) {{
        console.error(err);
      }}
      setTimeout(refreshStatus, 400);
      setTimeout(refreshStatus, 1200);
    }}

    startMonitorSilently().then(function() {{
      refreshStatus();
      setInterval(refreshStatus, 1000);
    }});
  </script>
</body>
</html>"""


def _start_monitor_services(trade_date: str = '') -> dict:
    import threading
    import asyncio

    normalized_trade_date = str(trade_date or '').strip() or datetime.now().strftime('%Y-%m-%d')
    print(
        f'[MONITOR START REQUEST] '
        f'trade_date={normalized_trade_date} '
        f'ticker_status={ticker_manager.status} '
        f'tick_count={int(ticker_manager.tick_count or 0)}'
    )
    if ticker_manager.status not in ('running', 'connecting'):
        threading.Thread(target=_start_ticker_bg, daemon=True).start()
    live_fast_monitor_supervisor.start(trade_date=normalized_trade_date)
    try:
        live_entry_monitor.start(asyncio.get_running_loop())
    except RuntimeError:
        pass
    return {
        'ok': True,
        'message': 'Global monitor started',
        'trade_date': live_fast_monitor_supervisor.trade_date,
    }


def _build_monitor_status_payload() -> dict:
    supervisor_status = live_fast_monitor_supervisor.get_status()
    ticker_status = ticker_manager.get_status()
    return {
        'server_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'running': bool(supervisor_status.get('running')),
        'monitor_status': 'running' if bool(supervisor_status.get('running')) else 'stopped',
        'trade_date': str(supervisor_status.get('trade_date') or datetime.now().strftime('%Y-%m-%d')),
        'started_at': str(supervisor_status.get('started_at') or ''),
        'last_tick_at': str(supervisor_status.get('last_tick_at') or ''),
        'last_refresh_at': str(supervisor_status.get('last_refresh_at') or ''),
        'counts': supervisor_status.get('counts') or {},
        'records_by_mode': supervisor_status.get('records_by_mode') or {},
        'ticker_status': str(ticker_status.get('status') or ''),
        'tick_count': ticker_status.get('tick_count'),
        'ltp_count': ticker_status.get('ltp_count'),
        'spot_map': ticker_status.get('spot_map') or {},
        'ticker_error': str(ticker_status.get('error') or ''),
    }


def _build_live_ltp_payload(active_contracts: list[dict], now_ts: str) -> list[dict]:
    payload: list[dict] = []
    for contract in (active_contracts or []):
        token = str(contract.get("token") or "").strip()
        option_type = str(contract.get("option") or "").strip()
        if option_type == "SPOT":
            underlying = str(contract.get("underlying") or "").strip().upper()
            spot_price = float(ticker_manager.get_spot(underlying) or 0.0)
            if spot_price <= 0:
                continue
            payload.append({
                "token": token,
                "timestamp": now_ts,
                "ltp": spot_price,
                "bb_qty": 0,
                "bb_price": 0.0,
                "ba_qty": 0,
                "ba_price": 0.0,
                "vol_in_day": 0,
                "underlying": underlying,
                "option_type": "SPOT",
            })
            continue

        live_ltp = float(ticker_manager.get_ltp(token) or 0.0)
        if live_ltp <= 0:
            continue
        payload.append({
            "token": token,
            "timestamp": now_ts,
            "ltp": live_ltp,
            "bb_qty": 0,
            "bb_price": 0.0,
            "ba_qty": 0,
            "ba_price": 0.0,
            "vol_in_day": 0,
            "expiry": str(contract.get("expiry_date") or ""),
            "strike": contract.get("strike"),
            "option_type": option_type,
        })
    return payload


def _save_market_kite_session(session: dict) -> None:
    api_key = session.get("api_key") or str(getattr(get_kite_instance(), "api_key", "") or "").strip()
    access_token = session.get("access_token")
    login_time = datetime.now().isoformat()
    update_fields = {
        "broker": "kite",
        "api_key": api_key,
        "access_token": access_token,
        "login_time": login_time,
        "user_id": session.get("user_id"),
        "user_name": session.get("user_name"),
        "app_user_id": _resolve_app_user_id(),
    }
    local_db = MongoData()
    try:
        # Match by broker, not by whichever doc currently has enabled:True —
        # that used to match Dhan's doc whenever Dhan was the active broker,
        # overwriting its credentials with this Kite session. Each broker's
        # own login should never be able to touch another broker's doc.
        existing = local_db._db["kite_market_config"].find_one({"broker": "kite"}, {"api_secret": 1}) or {}
        api_secret = str(existing.get("api_secret") or "").strip()
        local_db._db["kite_market_config"].update_one(
            {"broker": "kite"},
            {"$set": update_fields},
            upsert=True,
        )
        from features.kite_broker import sync_kite_access_token_by_credentials
        sync_kite_access_token_by_credentials(
            local_db._db, api_key, api_secret, access_token, login_time,
            skip_collection="kite_market_config",
        )
    finally:
        local_db.close()


def _clear_market_kite_session() -> None:
    local_db = MongoData()
    try:
        local_db._db["kite_market_config"].update_one(
            {"enabled": True},
            {"$set": {"access_token": "", "login_time": datetime.now().isoformat()}},
            upsert=True,
        )
    finally:
        local_db.close()


def _get_kite_market_session_status() -> tuple[bool, str]:
    local_db = MongoData()
    try:
        cfg = local_db._db["kite_market_config"].find_one(
            {"enabled": True},
            {"access_token": 1, "api_key": 1, "user_id": 1, "broker": 1, "login_time": 1},
        ) or {}
        broker = str(cfg.get("broker") or "kite").strip().lower()
        access_token = str(cfg.get("access_token") or "").strip()
        login_time = str(cfg.get("login_time") or "").strip()

        if broker == "dhan":
            user_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
            if not user_id:
                return False, "Dhan config missing user_id in kite_market_config"
            if not access_token:
                return False, "Dhan access_token not found in kite_market_config"
            # Load into dhan_broker_ws cache so the WS can start
            try:
                from features.dhan_broker_ws import set_common_credentials  # type: ignore
                set_common_credentials(user_id, access_token)
            except Exception:
                pass
            # Validate via Dhan profile API
            try:
                import requests as _req  # type: ignore
                resp = _req.get(
                    "https://api.dhan.co/v2/profile",
                    headers={"access-token": access_token, "Content-Type": "application/json"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    return True, "Dhan access token valid"
                return False, f"Dhan token invalid (HTTP {resp.status_code})"
            except Exception as exc:
                return False, f"Dhan token validation error: {exc}"

        # ── Kite path ──
        api_key = str(cfg.get("api_key") or "").strip()
        if not api_key:
            return False, (
                "Kite market config missing api_key"
                + (f" (login_time: {login_time})" if login_time else "")
            )
        if not access_token:
            return False, "Access token not found"
    finally:
        local_db.close()

    try:
        kite = get_kite_instance(access_token)
        kite.profile()
        return True, "Access token valid"
    except Exception as exc:
        try:
            _clear_market_kite_session()
        except Exception:
            pass
        return False, f"Access token invalid or expired: {exc}"


def _has_ready_kite_market_session() -> bool:
    is_ready, _ = _get_kite_market_session_status()
    return is_ready


def _build_monitor_dhan_token_page(trade_date: str = '', reason: str = '', retry_url: str = '') -> str:
    reason_text = str(reason or "Dhan access token not configured").strip()
    if not retry_url:
        retry_url = '/monitor/start' + (f'?trade_date={trade_date}' if trade_date else '')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dhan Login Required</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; min-height: 100vh; display: flex; align-items: center;
      justify-content: center;
      background: radial-gradient(circle at top, rgba(249,115,22,0.12), transparent 34%),
                  linear-gradient(155deg, #07111f 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0; font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(480px, calc(100vw - 32px));
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(249,115,22,0.22);
      border-radius: 28px; padding: 36px 28px;
      box-shadow: 0 28px 80px rgba(0,0,0,0.38); text-align: center;
    }}
    .badge {{
      display: inline-block; padding: 7px 16px; border-radius: 999px;
      background: rgba(249,115,22,0.12); border: 1px solid rgba(249,115,22,0.28);
      color: #fb923c; letter-spacing: 0.12em; font-size: 11px; text-transform: uppercase;
    }}
    h1 {{ margin: 18px 0 10px; font-size: 26px; line-height: 1.15; color: #f8fafc; }}
    p {{ margin: 0 auto 0; max-width: 400px; color: #94a3b8; line-height: 1.7; font-size: 15px; }}
    .reason {{ margin-top: 10px; color: #f87171; font-size: 13px; }}
    .actions {{ margin-top: 26px; display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }}
    .btn {{
      display: inline-flex; align-items: center; justify-content: center;
      min-width: 180px; padding: 14px 22px; border-radius: 14px;
      text-decoration: none; border: none; cursor: pointer;
      font-size: 15px; font-weight: 700; transition: opacity .15s;
    }}
    .btn:active {{ opacity: .8; }}
    .btn-dhan {{ background: linear-gradient(135deg, #f97316, #ea580c); color: #fff;
                 box-shadow: 0 8px 24px rgba(249,115,22,0.3); }}
    .btn-retry {{ background: #1e293b; color: #cbd5e1; border: 1px solid rgba(148,163,184,0.18); }}
    .hint {{ margin-top: 18px; color: #64748b; font-size: 13px; min-height: 1.4rem; }}
    .divider {{ margin: 24px 0 16px; border: none; border-top: 1px solid rgba(148,163,184,0.1); }}
    .manual-label {{
      font-size: 12px; color: #475569; cursor: pointer; text-decoration: underline;
      display: block; margin-bottom: 12px;
    }}
    .form-row {{ display: flex; flex-direction: column; gap: 10px; text-align: left; }}
    label {{ font-size: 13px; color: #94a3b8; margin-bottom: 2px; }}
    input {{
      width: 100%; padding: 11px 14px; border-radius: 10px;
      border: 1px solid rgba(148,163,184,0.22); background: rgba(30,41,59,0.8);
      color: #f1f5f9; font-size: 14px;
    }}
    input:focus {{ outline: none; border-color: #f97316; }}
    .btn-save {{
      width: 100%; margin-top: 12px; padding: 12px; border-radius: 10px;
      background: linear-gradient(135deg,#f97316,#ea580c); border: none;
      color: #fff; font-size: 15px; font-weight: 700; cursor: pointer;
    }}
    .err {{ color: #f87171; font-size: 13px; margin-top: 8px; display: none; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">Dhan Login Required</div>
    <h1>Connect Dhan Account</h1>
    <p>Login with Dhan to start the live monitor. Your credentials are already saved — just click the button below.</p>
    <p class="reason">Reason: {reason_text}</p>

    <div class="actions">
      <button class="btn btn-dhan" onclick="openDhanLogin()">Login with Dhan →</button>
      <a class="btn btn-retry" href="{retry_url}">Retry</a>
    </div>
    <div class="hint" id="hintText">A Dhan login window will open. Complete login there and return here.</div>

    <hr class="divider">
    <span class="manual-label" onclick="document.getElementById('manualForm').style.display='block';this.style.display='none'">
      Or enter access token manually
    </span>
    <div id="manualForm" style="display:none">
      <div class="form-row">
        <div>
          <label>Dhan Client ID</label>
          <input id="clientId" type="text" placeholder="e.g. HA9835" autocomplete="off" />
        </div>
        <div>
          <label>Access Token</label>
          <input id="accessToken" type="password" placeholder="Paste Dhan access token" autocomplete="off" />
        </div>
      </div>
      <div class="err" id="errMsg"></div>
      <button class="btn-save" onclick="saveToken()">Save &amp; Start Monitor</button>
    </div>
  </div>
  <script>
    let _popup = null;

    function openDhanLogin() {{
      const hint = document.getElementById('hintText');
      hint.textContent = 'Opening Dhan login window...';
      _popup = window.open('/broker/dhan/login', 'DhanLogin',
        'width=420,height=560,resizable=yes,scrollbars=yes');
      if (!_popup) {{
        hint.textContent = 'Popup blocked — allow popups and try again, or use the link below.';
        return;
      }}
      hint.textContent = 'Complete login in the Dhan window. This page will refresh automatically.';
    }}

    window.addEventListener('message', function(e) {{
      if (!e.data || e.data.type !== 'DHAN_LOGIN') return;
      const hint = document.getElementById('hintText');
      if (e.data.success) {{
        hint.textContent = 'Login successful! Redirecting to monitor...';
        hint.style.color = '#22c55e';
        setTimeout(() => {{ window.location.href = {json.dumps(retry_url)}; }}, 1000);
      }} else {{
        hint.textContent = 'Login failed: ' + (e.data.message || 'Unknown error');
        hint.style.color = '#f87171';
      }}
    }});

    async function saveToken() {{
      const clientId    = document.getElementById('clientId').value.trim();
      const accessToken = document.getElementById('accessToken').value.trim();
      const err  = document.getElementById('errMsg');
      err.style.display = 'none';
      if (!clientId || !accessToken) {{
        err.textContent = 'Both Client ID and Access Token are required.';
        err.style.display = 'block';
        return;
      }}
      const hint = document.getElementById('hintText');
      hint.textContent = 'Saving credentials...';
      try {{
        const res = await fetch('/broker/dhan/config', {{
          method: 'POST',
          headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{ client_id: clientId, access_token: accessToken }}),
        }});
        const data = await res.json();
        if (!res.ok || data.status === 'error') {{
          err.textContent = data.message || 'Failed to save credentials.';
          err.style.display = 'block';
          hint.textContent = 'Save failed.';
          return;
        }}
        hint.textContent = 'Saved! Starting monitor...';
        window.location.href = {json.dumps(retry_url)};
      }} catch (e) {{
        err.textContent = 'Network error: ' + e.message;
        err.style.display = 'block';
      }}
    }}
  </script>
</body>
</html>"""


def _build_monitor_kite_login_page(trade_date: str = '', reason: str = '') -> str:
    normalized_trade_date = str(trade_date or '').strip()
    retry_url = "/monitor/start"
    if normalized_trade_date:
        retry_url += f"?trade_date={normalized_trade_date}"
    reason_text = str(reason or "No broker session found").strip()

    # Detect active broker and show appropriate page
    try:
        _local_db = MongoData()
        _cfg = _local_db._db["kite_market_config"].find_one({"enabled": True}, {"broker": 1}) or {}
        _local_db.close()
        if str(_cfg.get("broker") or "kite").strip().lower() == "dhan":
            return _build_monitor_dhan_token_page(
                trade_date=trade_date, reason=reason_text, retry_url=retry_url,
            )
    except Exception:
        pass
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Kite Login Required</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(59, 130, 246, 0.16), transparent 34%),
        linear-gradient(155deg, #07111f 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(520px, calc(100vw - 32px));
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.16);
      border-radius: 28px;
      padding: 34px 28px;
      box-shadow: 0 28px 80px rgba(0, 0, 0, 0.38);
      text-align: center;
    }}
    .badge {{
      display: inline-block;
      padding: 9px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.92);
      border: 1px solid rgba(148, 163, 184, 0.14);
      color: #7dd3fc;
      letter-spacing: 0.14em;
      font-size: 12px;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 18px 0 12px;
      font-size: 32px;
      line-height: 1.15;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 420px;
      color: #94a3b8;
      line-height: 1.7;
      font-size: 15px;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 220px;
      padding: 16px 22px;
      border-radius: 18px;
      text-decoration: none;
      border: none;
      cursor: pointer;
      font-size: 17px;
      font-weight: 700;
    }}
    .btn.primary {{
      background: linear-gradient(135deg, #38bdf8, #2563eb);
      color: #eff6ff;
      box-shadow: 0 16px 32px rgba(37, 99, 235, 0.24);
    }}
    .btn.secondary {{
      background: #1e293b;
      color: #cbd5e1;
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}
    .hint {{
      margin-top: 18px;
      color: #7dd3fc;
      font-size: 13px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">Kite Required</div>
    <h1>Connect Kite API First</h1>
    <p>
      Monitor start needs a valid Kite access token. Login popup will open, save the access token,
      and then this page will automatically start the server listener.
    </p>
    <p style="margin-top:14px;color:#7dd3fc;font-size:13px;">Reason: {reason_text}</p>
    <div class="actions">
      <button class="btn primary" onclick="openKiteLogin()">Connect Kite API</button>
      <a class="btn secondary" href="/monitor/stop">Open Stop Page</a>
    </div>
    <div class="hint" id="hintText">Waiting for Kite login...</div>
  </div>

  <script>
    let kitePopup = null;

    function openKiteLogin() {{
      kitePopup = window.open('/broker/kite/login', 'kiteLogin', 'width=540,height=720');
      if (!kitePopup) {{
        document.getElementById('hintText').textContent = 'Popup blocked. Please allow popups and click again.';
        return;
      }}
      document.getElementById('hintText').textContent = 'Kite login popup opened. Complete login to continue.';
    }}

    window.addEventListener('message', function(event) {{
      const data = event.data || {{}};
      if (data.type !== 'KITE_LOGIN') return;
      if (!data.success) {{
        document.getElementById('hintText').textContent = data.message || 'Kite login failed.';
        return;
      }}
      document.getElementById('hintText').textContent = 'Kite login successful. Starting monitor...';
      window.location.href = {json.dumps(retry_url)};
    }});

    setTimeout(openKiteLogin, 250);
  </script>
</body>
</html>"""


def _build_monitor_action_page(*, running: bool, trade_date: str = '') -> str:
    title = 'Monitor Running' if running else 'Monitor Stopped'
    status_text = 'Listening is active' if running else 'Listening is stopped'
    button_label = 'Stop Listening' if running else 'Start Listening'
    button_href = '/monitor/stop' if running else '/monitor/start'
    button_class = 'danger' if running else 'success'
    trade_date_text = str(trade_date or '').strip() or datetime.now().strftime('%Y-%m-%d')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(56, 189, 248, 0.18), transparent 32%),
        linear-gradient(155deg, #06101d 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .shell {{
      width: min(540px, calc(100vw - 32px));
      padding: 18px;
    }}
    .card {{
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.16);
      border-radius: 28px;
      padding: 34px 28px;
      box-shadow: 0 28px 80px rgba(0, 0, 0, 0.38);
      text-align: center;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 9px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.14);
      font-size: 13px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: #cbd5e1;
    }}
    .dot {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: {('#22c55e' if running else '#ef4444')};
      box-shadow: 0 0 14px {('rgba(34, 197, 94, 0.85)' if running else 'rgba(239, 68, 68, 0.7)')};
    }}
    h1 {{
      margin: 20px 0 12px;
      font-size: 34px;
      line-height: 1.15;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 420px;
      font-size: 15px;
      line-height: 1.7;
      color: #94a3b8;
    }}
    .meta {{
      margin-top: 18px;
      font-size: 13px;
      color: #7dd3fc;
      letter-spacing: 0.06em;
      font-variant-numeric: tabular-nums;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 240px;
      padding: 16px 24px;
      border-radius: 18px;
      text-decoration: none;
      font-size: 18px;
      font-weight: 700;
      transition: transform 0.12s ease, opacity 0.2s ease;
    }}
    .btn:active {{ transform: scale(0.985); }}
    .btn.success {{
      background: linear-gradient(135deg, #22c55e, #16a34a);
      color: #04110a;
      box-shadow: 0 16px 32px rgba(22, 163, 74, 0.28);
    }}
    .btn.danger {{
      background: linear-gradient(135deg, #fb7185, #ef4444);
      color: #fff7ed;
      box-shadow: 0 16px 32px rgba(239, 68, 68, 0.24);
    }}
    .link-row {{
      margin-top: 18px;
      font-size: 14px;
    }}
    .link-row a {{
      color: #7dd3fc;
      text-decoration: none;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="card">
      <div class="pill"><span class="dot"></span>{status_text}</div>
      <h1>{title}</h1>
      <p>
        Single monitor service for live and fast-forward is currently
        {'running and checking active strategies every second.' if running else 'stopped. Click below to start listening again.'}
      </p>
      <div class="meta">Trade Date: {trade_date_text}</div>
      <div class="actions">
        <a class="btn {button_class}" href="{button_href}">{button_label}</a>
      </div>
      <div class="link-row"><a href="/monitor">Open Full Monitor</a></div>
    </div>
  </div>
</body>
</html>"""


















# ─── Mock Ticker ──────────────────────────────────────────────────────────────

_MOCK_CONTROL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Mock Ticker Control</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center;
    }
    .card {
      background: #1e293b; border: 1px solid #334155;
      border-radius: 16px; padding: 2.5rem 3rem;
      width: 460px; text-align: center;
    }
    .title {
      font-size: 1.25rem; font-weight: 600; color: #a78bfa;
      margin-bottom: 0.5rem; letter-spacing: 0.05em; text-transform: uppercase;
    }
    .subtitle {
      font-size: 0.75rem; color: #475569;
      margin-bottom: 2rem;
    }
    .status-row {
      display: flex; align-items: center; justify-content: center;
      gap: 0.6rem; margin-bottom: 1.5rem;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #475569; transition: background 0.3s;
    }
    .dot.running    { background: #a78bfa; box-shadow: 0 0 8px #a78bfa; animation: pulse 1.5s infinite; }
    .dot.connecting { background: #f59e0b; animation: pulse 0.8s infinite; }
    .dot.stopped    { background: #ef4444; }
    .dot.error      { background: #ef4444; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .status-text { font-size: 1rem; font-weight: 500; color: #cbd5e1; text-transform: capitalize; }
    .mock-time-badge {
      font-size: 0.78rem; color: #a78bfa; margin-bottom: 1.25rem;
      font-variant-numeric: tabular-nums; min-height: 1.2em;
    }
    /* Time picker row — only shown when stopped */
    .time-row {
      display: flex; gap: 0.5rem; margin-bottom: 1.25rem;
    }
    .time-input {
      flex: 1; padding: 0.65rem 0.75rem;
      background: #0f172a; border: 1px solid #334155;
      border-radius: 8px; color: #e2e8f0; font-size: 0.875rem; outline: none;
      color-scheme: dark;
    }
    .time-input:focus { border-color: #7c3aed; }
    .btn {
      width: 100%; padding: 1rem; border: none; border-radius: 10px;
      font-size: 1.1rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.2s, transform 0.1s; letter-spacing: 0.03em;
    }
    .btn:active { transform: scale(0.98); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-start { background: #7c3aed; color: #fff; }
    .btn-start:hover:not(:disabled) { opacity: 0.9; }
    .btn-stop  { background: #ef4444; color: #fff; }
    .btn-stop:hover:not(:disabled)  { opacity: 0.9; }
    .stats {
      display: grid; grid-template-columns: 1fr 1fr 1fr;
      gap: 0.75rem; margin-top: 1.75rem;
    }
    .stat-box {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.75rem;
    }
    .stat-label { font-size: 0.65rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1rem; font-weight: 700; color: #e2e8f0; }
    .spot-section { margin-top: 1.5rem; text-align: left; }
    .spot-title { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .spot-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .spot-item { background: #0f172a; border: 1px solid #1e293b; border-radius: 8px; padding: 0.5rem 0.75rem; display: flex; justify-content: space-between; align-items: center; }
    .spot-name  { font-size: 0.75rem; color: #94a3b8; font-weight: 600; }
    .spot-price { font-size: 0.85rem; color: #a78bfa; font-weight: 700; }
    .spot-price.na { color: #475569; }
    .error-msg { margin-top: 1rem; font-size: 0.8rem; color: #f87171; background: #1a0a0a; border-radius: 6px; padding: 0.5rem 0.75rem; display: none; }
    .started-at { margin-top: 1rem; font-size: 0.72rem; color: #475569; }
  </style>
</head>
<body>
<div class="card">
  <div class="title">Mock Ticker</div>
  <div class="subtitle">Simulates Kite WebSocket using historical DB data</div>

  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span class="status-text" id="statusText">Loading...</span>
  </div>

  <div class="mock-time-badge" id="mockTimeBadge"></div>

  <!-- Time picker — hidden when running -->
  <div class="time-row" id="timeRow">
    <input class="time-input" type="datetime-local" id="mockTimeInput" step="60" />
  </div>

  <button class="btn btn-start" id="actionBtn" disabled onclick="handleAction()">...</button>

  <div class="stats">
    <div class="stat-box">
      <div class="stat-label">Ticks</div>
      <div class="stat-value" id="tickCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">LTP Tokens</div>
      <div class="stat-value" id="ltpCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">Subscribed</div>
      <div class="stat-value" id="subCount">—</div>
    </div>
  </div>

  <div class="spot-section">
    <div class="spot-title">Mock Spot Prices</div>
    <div class="spot-grid">
      <div class="spot-item"><span class="spot-name">NIFTY</span><span class="spot-price na" id="spot-NIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">BANKNIFTY</span><span class="spot-price na" id="spot-BANKNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">FINNIFTY</span><span class="spot-price na" id="spot-FINNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">SENSEX</span><span class="spot-price na" id="spot-SENSEX">—</span></div>
    </div>
  </div>

  <div class="error-msg" id="errorMsg"></div>
  <div class="started-at" id="startedAt"></div>
</div>

<script>
  const API = '';   // same origin

  async function fetchStatus() {
    try {
      const res  = await fetch(API + '/mock/status');
      const data = await res.json();
      renderStatus(data);
    } catch(e) {
      renderStatus({ status: 'error', error: 'Cannot reach server' });
    }
  }

  function renderStatus(data) {
    const status = data.status || 'stopped';
    document.getElementById('dot').className       = 'dot ' + status;
    document.getElementById('statusText').textContent = status;

    const btn      = document.getElementById('actionBtn');
    const timeRow  = document.getElementById('timeRow');
    const badgeEl  = document.getElementById('mockTimeBadge');

    btn.disabled = false;

    if (status === 'running' || status === 'connecting') {
      btn.textContent = 'Stop Mock Server';
      btn.className   = 'btn btn-stop';
      if (status === 'connecting') btn.disabled = true;
      timeRow.style.display = 'none';
      badgeEl.textContent   = data.mock_time
        ? '\\u25B6 Simulating: ' + data.mock_time.replace('T', ' ')
        : '';
    } else {
      btn.textContent       = 'Start Listening';
      btn.className         = 'btn btn-start';
      timeRow.style.display = 'flex';
      const inputEl = document.getElementById('mockTimeInput');
      if (inputEl && data.mock_time) {
        inputEl.value = data.mock_time.slice(0, 16);
      }
      badgeEl.textContent   = data.mock_time
        ? 'Last stopped at: ' + data.mock_time.replace('T', ' ')
        : 'Set simulation start time above';
    }

    document.getElementById('tickCount').textContent =
      data.tick_count !== undefined ? data.tick_count.toLocaleString() : '—';
    document.getElementById('ltpCount').textContent =
      data.ltp_count !== undefined ? data.ltp_count.toLocaleString() : '—';
    document.getElementById('subCount').textContent =
      data.subscribed_tokens !== undefined ? data.subscribed_tokens.toLocaleString() : '—';

    const spotMap = data.spot_map || {};
    ['NIFTY','BANKNIFTY','FINNIFTY','SENSEX'].forEach(sym => {
      const el  = document.getElementById('spot-' + sym);
      const val = spotMap[sym];
      if (!el) return;
      if (val) {
        el.textContent = '\\u20B9' + Number(val).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        el.className = 'spot-price';
      } else {
        el.textContent = '—';
        el.className = 'spot-price na';
      }
    });

    const errEl = document.getElementById('errorMsg');
    if (data.error) { errEl.textContent = data.error; errEl.style.display = 'block'; }
    else            { errEl.style.display = 'none'; }

    const startEl = document.getElementById('startedAt');
    startEl.textContent = data.started_at
      ? 'Started: ' + data.started_at.replace('T',' ').slice(0,19)
      : '';
  }

  async function handleAction() {
    const btn    = document.getElementById('actionBtn');
    const status = document.getElementById('statusText').textContent;
    btn.disabled    = true;
    btn.textContent = 'Please wait...';

    try {
      if (status === 'running') {
        await fetch(API + '/mock/stop');
      } else {
        const raw = document.getElementById('mockTimeInput').value;
        if (!raw) {
          await fetch(API + '/mock/start');
        } else {
          const timeStr = raw.length === 16 ? raw + ':00' : raw;
          await fetch(API + '/mock/start?time=' + encodeURIComponent(timeStr));
        }
      }
    } catch(e) { console.error(e); }

    setTimeout(fetchStatus, 600);
    setTimeout(fetchStatus, 1800);
    setTimeout(fetchStatus, 4000);
  }

  fetchStatus();
  setInterval(fetchStatus, 2000);
</script>
</body>
</html>"""


def _start_mock_bg(time_str: str) -> None:
    """Run in a daemon thread — sets mock time then starts MockTicker."""
    result = mock_ticker_manager.set_mock_time(time_str)
    if not result.get("ok"):
        import logging
        logging.getLogger(__name__).error("mock set_mock_time failed: %s", result)
        return
    _db = MongoData()
    try:
        mock_ticker_manager.start(_db)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("mock start error: %s", exc)
    finally:
        try:
            _db.close()
        except Exception:
            pass


def _upsert_contracts_into_col(
    active_tokens_col,
    contracts: list[dict],
    stock_name: str,
    now_ts: str,
    broker: str = "",
) -> tuple[int, int]:
    if not contracts:
        return 0, 0

    from pymongo import UpdateOne

    _idx_set = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
    instrument_type = "index" if stock_name.upper() in _idx_set else "stock"
    ops = []
    for contract in contracts:
        expiry_val = str(contract.get("expiry") or "").strip()[:10]
        opt_type_val = str(contract.get("option_type") or contract.get("opt_type") or "").strip().upper()
        strike_val = contract.get("strike")
        query: dict = {
            "instrument": stock_name,
            "expiry": expiry_val,
            "strike": strike_val,
            "option_type": opt_type_val,
        }
        if broker:
            query["broker"] = broker
        update_payload: dict = {
            "instrument": stock_name,
            "instrument_type": instrument_type,
            "expiry": expiry_val,
            "strike": strike_val,
            "option_type": opt_type_val,
            "token": str(contract.get("token") or "").strip(),
            "tokens": str(contract.get("tokens") or contract.get("token") or "").strip(),
            "symbol": str(contract.get("symbol") or "").strip(),
            "exchange": str(contract.get("exchange") or "").strip(),
            "updated_at": now_ts,
        }
        if broker:
            update_payload["broker"] = broker
        ops.append(UpdateOne(
            query,
            {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}},
            upsert=True,
        ))

    # Batched in one round-trip per call (ordered=False so one bad op can't stall the rest)
    # instead of one update_one() per contract — this is what made syncing thousands of
    # contracts take tens of seconds.
    result = active_tokens_col.bulk_write(ops, ordered=False)
    created = result.upserted_count
    updated = result.matched_count
    return created, updated


def _sync_active_option_tokens(instrument: str) -> dict:
    normalized_instrument = str(instrument or "").strip().upper()
    if not normalized_instrument:
        raise HTTPException(status_code=400, detail="Instrument is required")

    today_str = datetime.now().strftime("%Y-%m-%d")
    db = MongoData()
    try:
        credentials_loaded = load_credentials_from_db(db)
        active_tokens_col = db._db["active_option_tokens"]
        try:
            active_tokens_col.create_index(
                [("broker", 1), ("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
                name="idx_active_option_contract_v2",
            )
        except Exception:
            pass

        from features.broker_gateway import _active_broker as _sync_get_broker  # type: ignore
        active_broker = _sync_get_broker()

        _INDEX_SET = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}

        # Special case: iterate ALL non-index FNO stock underlyings
        if normalized_instrument == "FNO-STOCKS":
            now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            created_count = 0
            updated_count = 0
            contracts_processed = 0
            all_expiries: set[str] = set()

            if active_broker == "dhan":
                # Clear existing FNO stock contracts (expired + stale) before re-inserting
                deleted = active_tokens_col.delete_many({
                    "broker": "dhan",
                    "instrument_type": "stock",
                })
                if deleted.deleted_count == 0:
                    # First run — no instrument_type field yet, clear by excluding known indices
                    active_tokens_col.delete_many({
                        "broker": "dhan",
                        "instrument": {"$nin": list(_INDEX_SET)},
                    })

                # Dhan: use CSV master directly (avoids circular DB read)
                master = _get_dhan_fno_master()
                for symbol, all_contracts in master.items():
                    for c in all_contracts:
                        exp = str(c.get("expiry") or "").strip()[:10]
                        if not exp or exp < today_str:
                            continue
                        all_expiries.add(exp)
                        contracts_processed += 1
                        opt_type = str(c.get("opt_type") or "").strip().upper()
                        query = {
                            "broker": "dhan",
                            "instrument": symbol,
                            "expiry": exp,
                            "strike": c.get("strike"),
                            "option_type": opt_type,
                        }
                        payload = {
                            "broker": "dhan",
                            "instrument": symbol,
                            "instrument_type": "stock",
                            "expiry": exp,
                            "strike": c.get("strike"),
                            "option_type": opt_type,
                            "token": str(c.get("sec_id") or "").strip(),
                            "tokens": str(c.get("sec_id") or "").strip(),
                            "symbol": f"{symbol}{int(c['strike']) if float(c['strike']).is_integer() else c['strike']}{opt_type}",
                            "exchange": str(c.get("exchange") or "NSE").strip(),
                            "lot_size": c.get("lot_size"),
                            "updated_at": now_ts,
                        }
                        res = active_tokens_col.update_one(
                            query,
                            {"$set": payload, "$setOnInsert": {"created_at": now_ts}},
                            upsert=True,
                        )
                        if res.upserted_id is not None:
                            created_count += 1
                        elif res.matched_count:
                            updated_count += 1
            else:
                # Kite: load from Kite REST instruments API
                from features.spot_atm_utils import (  # type: ignore
                    _load_kite_instruments as _kite_inst_load,
                    list_kite_option_contracts as _kite_list_contracts,
                )
                known_indices = set(KITE_INDEX_TOKENS.keys())
                cache = _kite_inst_load(force=True)
                if not cache:
                    return {
                        "instrument": "FNO-STOCKS",
                        "expiries": [],
                        "contracts_processed": 0,
                        "created": 0,
                        "updated": 0,
                        "message": "No active option contracts found",
                        "credentials_loaded": credentials_loaded,
                        "hint": "Check kite_market_config access_token/login if this instrument should have live contracts",
                    }

                underlyings: dict[str, set[str]] = {}
                for (name, exp, _strike, _type) in cache:
                    if name not in known_indices and exp >= today_str:
                        underlyings.setdefault(name, set()).add(exp)

                for stock_name, expiry_set in underlyings.items():
                    for expiry in sorted(expiry_set):
                        contracts = _kite_list_contracts(stock_name, expiry)
                        all_expiries.update(expiry_set)
                        contracts_processed += len(contracts)
                        c, u = _upsert_contracts_into_col(
                            active_tokens_col, contracts, stock_name, now_ts, broker="kite"
                        )
                        created_count += c
                        updated_count += u

            return {
                "instrument": "FNO-STOCKS",
                "underlyings_count": contracts_processed,
                "expiries": sorted(all_expiries),
                "contracts_processed": contracts_processed,
                "created": created_count,
                "updated": updated_count,
                "credentials_loaded": credentials_loaded,
                "message": "active_option_tokens sync completed" if contracts_processed else "No active option contracts found",
            }

        expiries = get_kite_expiries(normalized_instrument, today_str, force_refresh=True)
        if not expiries:
            return {
                "instrument": normalized_instrument,
                "expiries": [],
                "contracts_processed": 0,
                "created": 0,
                "updated": 0,
                "message": "No active option contracts found",
                "credentials_loaded": credentials_loaded,
                "hint": (
                    "Check kite_market_config access_token/login if this instrument should have live contracts"
                ),
            }

        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        created_count = 0
        updated_count = 0
        contracts_processed = 0

        for expiry_index, expiry in enumerate(expiries):
            contracts = list_kite_option_contracts(
                normalized_instrument,
                expiry,
                force_refresh=(expiry_index == 0),
            )
            contracts_processed += len(contracts)
            c, u = _upsert_contracts_into_col(
                active_tokens_col, contracts, normalized_instrument, now_ts, broker=active_broker
            )
            created_count += c
            updated_count += u

        return {
            "instrument": normalized_instrument,
            "expiries": expiries,
            "contracts_processed": contracts_processed,
            "created": created_count,
            "updated": updated_count,
            "credentials_loaded": credentials_loaded,
            "message": "active_option_tokens sync completed",
        }
    finally:
        db.close()


def _get_live_index_spot_price(normalized_instrument: str) -> float:
    index_token = KITE_INDEX_TOKENS.get(normalized_instrument)
    if not index_token:
        return 0.0
    try:
        from features.broker_gateway import get_broker_ltp_map  # type: ignore

        ltp_value = (get_broker_ltp_map() or {}).get(str(index_token), 0.0)
        return float(ltp_value or 0.0)
    except Exception:
        return 0.0


def _resolve_single_option_ltp(
    db,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
) -> float:
    normalized_underlying = str(underlying or "").strip().upper()
    normalized_expiry = str(expiry or "").strip()[:10]
    normalized_option_type = str(option_type or "").strip().upper()

    contract = {}
    try:
        contract = db["active_option_tokens"].find_one(
            {
                "instrument": normalized_underlying,
                "expiry": normalized_expiry,
                "strike": strike,
                "option_type": normalized_option_type,
            },
            {
                "_id": 0,
                "token": 1,
                "tokens": 1,
                "symbol": 1,
            },
        ) or {}
    except Exception:
        contract = {}

    token = str(contract.get("token") or contract.get("tokens") or "").strip()
    symbol = str(contract.get("symbol") or "").strip()
    if not token:
        try:
            inst = (_load_kite_instruments() or {}).get(
                (normalized_underlying, normalized_expiry, float(strike), normalized_option_type)
            ) or {}
            token = str(inst.get("token") or "").strip()
            symbol = str(inst.get("symbol") or "").strip()
        except Exception:
            token = ""

    if not token:
        log.warning(
            "margin quote token not found underlying=%s expiry=%s strike=%s option_type=%s",
            normalized_underlying,
            normalized_expiry,
            strike,
            normalized_option_type,
        )
        return 0.0

    try:
        live_ltp = float((get_ltp_map() or {}).get(token, 0.0) or 0.0)
        if live_ltp > 0:
            return live_ltp
    except Exception:
        pass

    try:
        if not is_configured():
            return 0.0
        api_key, access_token = get_common_credentials()
        if not api_key or not access_token:
            return 0.0
        kite = get_kite_instance(access_token)
        quotes = kite.quote([int(token)]) or {}
        for _quote_key, quote_doc in quotes.items():
            quote_ltp = float(
                quote_doc.get("last_price")
                or (quote_doc.get("ohlc") or {}).get("close")
                or 0.0
            )
            if quote_ltp > 0:
                print(
                    f"[MARGIN SINGLE QUOTE] underlying={normalized_underlying} "
                    f"expiry={normalized_expiry} strike={strike} type={normalized_option_type} "
                    f"token={token} symbol={symbol or '-'} ltp={quote_ltp}",
                    flush=True,
                )
                return quote_ltp
    except Exception as exc:
        log.warning(
            "margin single quote error underlying=%s expiry=%s strike=%s option_type=%s token=%s: %s",
            normalized_underlying,
            normalized_expiry,
            strike,
            normalized_option_type,
            token,
            exc,
        )

    return 0.0


def _resolve_margin_order_contract(
    db,
    underlying: str,
    instrument_type: str,
    expiry: str,
    strike: float,
) -> dict[str, Any]:
    normalized_underlying = str(underlying or "").strip().upper()
    normalized_instrument_type = str(instrument_type or "").strip().upper()
    normalized_expiry = str(expiry or "").strip()[:10]

    if normalized_instrument_type in {"CE", "PE"}:
        contract = db["active_option_tokens"].find_one(
            {
                "instrument": normalized_underlying,
                "expiry": normalized_expiry,
                "strike": strike,
                "option_type": normalized_instrument_type,
            },
            {
                "_id": 0,
                "symbol": 1,
                "exchange": 1,
            },
        ) or {}
        symbol = str(contract.get("symbol") or "").strip()
        exchange = str(contract.get("exchange") or "").strip() or ("BFO" if normalized_underlying in {"SENSEX", "BANKEX"} else "NFO")
        if symbol:
            return {"tradingsymbol": symbol, "exchange": exchange}
    return {}


def _calculate_kite_basket_margin(db, legs: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not legs or not is_configured():
        return None

    api_key, access_token = get_common_credentials()
    if not api_key or not access_token:
        return None

    orders: list[dict[str, Any]] = []
    for leg in legs:
        contract = _resolve_margin_order_contract(
            db,
            leg.get("underlying"),
            leg.get("instrument_type"),
            leg.get("expiry"),
            float(leg.get("strike") or 0.0),
        )
        tradingsymbol = str(contract.get("tradingsymbol") or "").strip()
        exchange = str(contract.get("exchange") or "").strip()
        quantity = int(leg.get("quantity") or 0) * int(leg.get("lot_size") or 0)
        if not tradingsymbol or not exchange or quantity <= 0:
            return None
        orders.append(
            {
                "exchange": exchange,
                "tradingsymbol": tradingsymbol,
                "transaction_type": str(leg.get("transaction_type") or "SELL").upper(),
                "variety": "regular",
                "product": "NRML",
                "order_type": "MARKET",
                "quantity": quantity,
                "price": 0,
                "trigger_price": 0,
            }
        )

    try:
        kite = get_kite_instance(access_token)
        return kite.basket_order_margins(orders, consider_positions=False) or {}
    except Exception as exc:
        log.warning("kite basket margin error: %s", exc)
        return None


def _build_full_option_chain_response(instrument: str) -> dict[str, Any]:
    normalized_instrument = str(instrument or "").strip().upper()
    if not normalized_instrument:
        raise HTTPException(status_code=400, detail="Instrument is required")

    allowed_instruments = {"NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY"}
    if normalized_instrument not in allowed_instruments:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported instrument '{normalized_instrument}'. "
                "Use one of: NIFTY, BANKNIFTY, SENSEX, FINNIFTY, MIDCPNIFTY"
            ),
        )

    cached_base = _get_active_option_chain_cache(normalized_instrument)
    if not cached_base:
        raise HTTPException(
            status_code=404,
            detail=f"No option chain rows found in active_option_tokens for instrument {normalized_instrument}",
        )

    response = deepcopy(cached_base)
    return {
        **response,
        "spot_price": _get_live_index_spot_price(normalized_instrument),
    }


















_INDEX_KITE_SYMBOLS: dict[str, str] = {
    "NIFTY":      "NSE:NIFTY 50",
    "BANKNIFTY":  "NSE:NIFTY BANK",
    "FINNIFTY":   "NSE:NIFTY FIN SERVICE",
    "SENSEX":     "BSE:SENSEX",
    "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
}


def _get_kite_rest_client():
    """Return a configured broker REST client using DB credentials, or None."""
    try:
        from features.broker_gateway import get_broker_rest_client  # type: ignore
        return get_broker_rest_client()
    except Exception:
        return None


# NSE option chain in-process cache — keyed by "SYMBOL:YYYY-MM-DD"
_nse_chain_cache: dict[str, tuple[float, dict]] = {}
_nse_chain_cache_lock = threading.Lock()
_NSE_CHAIN_CACHE_TTL = 60.0  # seconds

# India VIX NSE-API fallback cache — see get_live_greeks_chain's VIX section.
_india_vix_cache: dict[str, tuple[float, float]] = {}
_INDIA_VIX_CACHE_TTL = 60.0  # seconds


def _resolve_chain_reference_spot(
    rows_by_side: dict[str, dict[float, dict]],
    spot_price: float,
    T: float,
    r: float,
    q: float,
) -> float:
    """
    Convert the ATM synthetic future into a spot-equivalent reference price.

    When spot_price is 0 (equity spot fetch failed), estimates spot from
    put-call parity by finding the strike where |CE_ltp - PE_ltp| is minimum.
    """
    ce_by_strike = rows_by_side.get("CE") or {}
    pe_by_strike = rows_by_side.get("PE") or {}
    common_strikes = [
        strike
        for strike in set(ce_by_strike) & set(pe_by_strike)
        if float((ce_by_strike.get(strike) or {}).get("ltp") or 0) > 0
        and float((pe_by_strike.get(strike) or {}).get("ltp") or 0) > 0
    ]
    if not common_strikes:
        return spot_price

    if spot_price > 0:
        atm_strike = min(common_strikes, key=lambda strike: abs(strike - spot_price))
    else:
        # Estimate ATM via put-call parity: strike where |CE - PE| is minimized
        atm_strike = min(
            common_strikes,
            key=lambda strike: abs(
                float((ce_by_strike.get(strike) or {}).get("ltp") or 0)
                - float((pe_by_strike.get(strike) or {}).get("ltp") or 0)
            ),
        )

    ce_ltp = float((ce_by_strike.get(atm_strike) or {}).get("ltp") or 0)
    pe_ltp = float((pe_by_strike.get(atm_strike) or {}).get("ltp") or 0)
    synthetic_future = atm_strike + ce_ltp - pe_ltp
    if synthetic_future <= 0:
        return spot_price

    # Convert forward/synthetic reference back to a BSM-compatible spot input.
    return synthetic_future * math.exp(-(r - q) * max(T, 0.0))


def _fetch_nse_chain_data(symbol: str, expiry_iso: str) -> dict:
    """
    Fetch LTP + OI + spot from NSE option chain for a symbol + expiry.
    Returns {"spot": float, "chain": {"24500_CE": {"ltp": 22.3, "oi": 131000}, ...}}
    Results are cached for 60 seconds to avoid repeated slow HTTP calls.
    """
    import requests as _req
    from datetime import datetime as _dt

    cache_key = f"{symbol.upper()}:{expiry_iso[:10]}"
    _now = time.monotonic()
    with _nse_chain_cache_lock:
        _hit = _nse_chain_cache.get(cache_key)
        if _hit and (_now - _hit[0]) < _NSE_CHAIN_CACHE_TTL:
            return _hit[1]

    try:
        expiry_dt = _dt.strptime(expiry_iso[:10], "%Y-%m-%d")
        _day = expiry_dt.strftime("%d").lstrip("0")
        _mon = expiry_dt.strftime("%b")
        _yr  = expiry_dt.strftime("%Y")
        expiry_nse_dash  = f"{_day}-{_mon}-{_yr}"   # "23-Jun-2026"
        expiry_nse_space = f"{_day} {_mon} {_yr}"   # "23 Jun 2026"
    except Exception:
        expiry_nse_dash = expiry_nse_space = ""

    _INDICES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
    is_index = symbol.upper() in _INDICES
    url = (
        f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol.upper()}"
        if is_index
        else f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol.upper()}"
    )

    empty: dict = {"spot": 0.0, "chain": {}}
    try:
        sess = _req.Session()
        sess.get("https://www.nseindia.com", timeout=5,
                 headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
        r = sess.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.nseindia.com",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        })
        if r.status_code != 200:
            log.warning("[NSE CHAIN] %s HTTP %s", symbol, r.status_code)
            return empty
        records = r.json().get("records") or {}
        data_rows = records.get("data") or []
        spot = float(records.get("underlyingValue") or 0)
        chain: dict[str, dict] = {}
        for row in data_rows:
            _row_expiry = str(row.get("expiryDate") or "").strip()
            if expiry_nse_dash and _row_expiry not in (expiry_nse_dash, expiry_nse_space):
                continue
            strike = row.get("strikePrice")
            if strike is None:
                continue
            strike_int = int(float(strike))
            if not spot:
                spot = float(row.get("CE", {}).get("underlyingValue") or row.get("PE", {}).get("underlyingValue") or 0)
            for opt_type in ("CE", "PE"):
                opt_data = row.get(opt_type) or {}
                chain[f"{strike_int}_{opt_type}"] = {
                    "ltp": float(opt_data.get("lastPrice") or 0),
                    "oi":  int(opt_data.get("openInterest") or 0),
                }
        result = {"spot": spot, "chain": chain}
        if chain:
            with _nse_chain_cache_lock:
                _nse_chain_cache[cache_key] = (time.monotonic(), result)
        return result
    except Exception as _e:
        log.warning("[NSE CHAIN] %s error: %s", symbol, _e)
        return empty


def _fetch_nse_oi_map(symbol: str, expiry_iso: str) -> dict[str, int]:
    """Backward-compat wrapper — returns only OI map."""
    return {k: v["oi"] for k, v in _fetch_nse_chain_data(symbol, expiry_iso).get("chain", {}).items()}


# f"{segment}:{sec_id}" → last-seen-good market-data dict. Never evicted —
# see the resilience note in _fetch_dhan_market_data()'s docstring below.
_DHAN_MARKET_DATA_LAST_GOOD: dict[str, dict] = {}


def _fetch_dhan_market_data(segment: str, sec_ids: list[int], db) -> dict[str, dict]:
    """
    Fetch LTP + OI + best bid/ask from Dhan /marketfeed/quote for a list of security IDs.
    Returns {str(sec_id): {"ltp": float, "oi": int, "bid": float, "ask": float, "prev_close": float}}.
    Dhan /quote supports up to 1000 per segment — send as few requests as possible.

    WS-first + last-good fallback, same resilience as
    features.broker_gateway.get_broker_rest_quotes: Dhan's REST quote
    endpoint rate-limits to ~1 req/sec per account, and this function used
    to retry a 429 with a blocking time.sleep(1s/2s/3s) per batch — on
    /live-greeks-chain, which calls this 2+ times sequentially (equity
    spot, then the whole NSE_FNO/BSE_FNO chain), that alone could add
    several seconds to one page load. A WS ltp_map hit resolves a sec_id
    with zero REST round trip; a 429/failed REST attempt now falls straight
    back to the last real value seen for that sec_id instead of blocking
    to retry.
    """
    if not sec_ids:
        return {}
    raw_db = db._db if hasattr(db, "_db") else db
    cfg = raw_db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
    access_token = str(cfg.get("access_token") or "").strip()
    client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    if not access_token or not client_id:
        return {}

    result: dict[str, dict] = {}

    # WS ltp_map/oi_map are keyed by bare numeric security id regardless of
    # segment (index/equity/FNO ticks all land there — see dhan_ticker.py's
    # binary parser), so a hit here is an in-memory read, no REST call at all.
    # `ticker_manager` (broker_ticker_manager, imported at module top) — NOT a direct
    # dhan_ticker_manager import. In central-tick mode (this process — see
    # simulator_main.py's set_central_client), the real ticks live in CentralTickClient,
    # not this process's own (never-started, empty) dhan_ticker_manager; a direct import
    # here meant every WS lookup missed and every single call fell through to the REST
    # fallback below, exactly the bug broker_gateway.get_broker_rest_quotes's own comment
    # already warns about (and already avoids, via this same proxy).
    try:
        _dtm = ticker_manager
        for sid in sec_ids:
            sid_str = str(sid)
            ws_ltp = float(_dtm.ltp_map.get(sid_str) or 0)
            if ws_ltp > 0:
                cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}") or {}
                # bid_map/ask_map: live, from Full-packet depth (see dhan_ticker.py) — only
                # ever populated for F&O legs on the main connection, direct or relayed
                # through CentralTickClient. Falls back to the last REST-cached value when
                # a token was never subscribed in Full mode (e.g. chain-warming-only ticks).
                ws_bid = float(_dtm.bid_map.get(sid_str) or 0)
                ws_ask = float(_dtm.ask_map.get(sid_str) or 0)
                result[sid_str] = {
                    "ltp": ws_ltp,
                    "oi": int(_dtm.oi_map.get(sid_str) or cached.get("oi", 0)),
                    "bid": ws_bid or cached.get("bid", 0.0),
                    "ask": ws_ask or cached.get("ask", 0.0),
                    "prev_close": cached.get("prev_close", 0.0),
                    # Lets callers (see _fetch_dhan_quote_for_leg) log/print which path actually
                    # priced this leg — "ws" (in-memory, ~instant) vs "rest" vs "cache" below.
                    "source": "ws",
                }
    except Exception:
        pass

    missing = [sid for sid in sec_ids if str(sid) not in result]
    if missing:
        from features.broker_gateway import dhan_quote_post_blocking

        _BATCH = 500  # Dhan /quote supports up to 1000 per segment
        batches = [missing[i: i + _BATCH] for i in range(0, len(missing), _BATCH)]

        for batch in batches:
            # Up to 3 tries: a single transient 429/5xx from Dhan (real
            # per-account rate limit, not just our internal gate — momentary
            # under genuinely heavy concurrent demand from other features
            # sharing this same gate) used to surface as a flat ltp=0 for the
            # whole chain with no second chance. wait_for_dhan_slot() inside
            # dhan_quote_post_blocking() already spaces retries >=1.05s apart.
            for _attempt in range(3):
                try:
                    # Blocking, not skip-on-busy: this is usually called right
                    # after a spot-price quote on the same rate gate (e.g.
                    # get_live_greeks_chain fetches index spot, then the whole
                    # chain, microseconds apart) — skip-on-busy made the second
                    # call lose that race almost every time, rendering the
                    # whole chain as ltp=0. See dhan_quote_post_blocking's docstring.
                    r = dhan_quote_post_blocking({segment: batch}, access_token, client_id, timeout=15.0)
                    if r is None:
                        continue
                    if r.status_code == 200:
                        raw = r.json()
                        data = (raw.get("data") or raw).get(segment) or {}
                        for sid, info in data.items():
                            if not isinstance(info, dict):
                                continue
                            depth = info.get("depth") or {}
                            buy_levels = depth.get("buy") or []
                            sell_levels = depth.get("sell") or []
                            entry = {
                                "ltp": float(info.get("last_price") or 0),
                                "oi":  int(info.get("oi") or 0),
                                # Best bid/ask (level 0) — 0 if that side of the book is empty.
                                "bid": float((buy_levels[0] or {}).get("price") or 0) if buy_levels else 0.0,
                                "ask": float((sell_levels[0] or {}).get("price") or 0) if sell_levels else 0.0,
                                # Previous trading day's close — Dhan's own quote response
                                # already carries this in ohlc.close, additive field so
                                # nothing keying off just ['ltp']/['oi'] etc. is affected.
                                "prev_close": float((info.get("ohlc") or {}).get("close") or 0),
                                "source": "rest",
                            }
                            result[str(sid)] = entry
                            if entry["ltp"] > 0:
                                _DHAN_MARKET_DATA_LAST_GOOD[f"{segment}:{sid}"] = entry
                        break
                    else:
                        # Most commonly a 429 — retry a couple times (spaced
                        # by the shared gate) before giving up to the
                        # last-good backfill below.
                        log.warning("[DHAN QUOTE] segment=%s status=%d attempt=%d body=%s",
                                    segment, r.status_code, _attempt, r.text[:200])
                except Exception as _e:
                    log.warning("[DHAN QUOTE] error=%s attempt=%d", _e, _attempt)

    for sid in sec_ids:
        sid_str = str(sid)
        if sid_str not in result or not result[sid_str].get("ltp"):
            cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}")
            if cached:
                result[sid_str] = {**cached, "source": "cache"}

    return result


def _fetch_dhan_ltp(segment: str, sec_ids: list[int], db) -> dict[str, float]:
    """Convenience wrapper — returns {str(sec_id): ltp}."""
    return {k: v["ltp"] for k, v in _fetch_dhan_market_data(segment, sec_ids, db).items()}










# ── Background sync state ─────────────────────────────────────────────────────
_bg_sync_state: dict = {
    "running": False,
    "instrument": "",
    "started_at": "",
    "finished_at": "",
    "result": None,
    "error": "",
}
_bg_sync_thread: threading.Thread | None = None


def _run_bg_sync(instrument: str) -> None:
    global _bg_sync_state
    _bg_sync_state["running"] = True
    _bg_sync_state["instrument"] = instrument
    _bg_sync_state["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _bg_sync_state["finished_at"] = ""
    _bg_sync_state["result"] = None
    _bg_sync_state["error"] = ""
    try:
        result = _sync_active_option_tokens(instrument)
        _bg_sync_state["result"] = result
    except Exception as exc:
        _bg_sync_state["error"] = str(exc)
    finally:
        _bg_sync_state["running"] = False
        _bg_sync_state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")














# ─── MTM Historical Data ──────────────────────────────────────────────────────







_TRADE_DATA_DIR = Path(__file__).resolve().parent.parent / "algoreq" / "trade-data"


def _read_trade_static_json(filename: str):
    path = _TRADE_DATA_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{filename} not found")
    import json as _json_mod
    return _json_mod.loads(path.read_text(encoding="utf-8"))












# ─── Data Migration ───────────────────────────────────────────────────────────



_register_versioned_route_aliases(app)
