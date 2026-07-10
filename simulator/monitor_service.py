from __future__ import annotations

import asyncio
import threading
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Optional

from bson import ObjectId
from pydantic import ValidationError

from features.mongo_data import MongoData

from .models import MiniStrangleRequest
from .strategy_engine import StrategyEngine
from .streaming_controller import StreamingController

_LOT_SIZE_BY_INSTRUMENT = {
    "NIFTY": 75,
    "BANKNIFTY": 30,
    "FINNIFTY": 65,
    "SENSEX": 10,
    "MIDCPNIFTY": 120,
}


def _now_ist() -> datetime:
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _map_trade_mode(value: Any) -> int:
    normalized = str(value or "").strip().lower()
    mapping = {
        "manual": 1,
        "semi_auto": 2,
        "semi_automatic": 2,
        "auto": 3,
        "automatic": 3,
    }
    if normalized in mapping:
        return mapping[normalized]
    return _safe_int(value, 1) if str(value or "").strip() else 1


def _map_expiry_type(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    mapping = {
        "CURRENT_WEEK": "current_week",
        "NEXT_WEEK": "next_week",
        "MONTHLY_EXPIRY": "monthly_expiry",
        "MONTHLY": "monthly_expiry",
        "CURRENT_MONTH": "monthly_expiry",
        "CUSTOM": "current_week",
    }
    if normalized in mapping:
        return mapping[normalized]
    lowered = str(value or "").strip().lower()
    if lowered in {"current_week", "next_week", "monthly_expiry"}:
        return lowered
    return "current_week"


def _map_unit_to_type(value: Any) -> Optional[int]:
    normalized = str(value or "").strip().lower()
    if normalized == "points":
        return 1
    if normalized in {"pct", "percentage", "%"}:
        return 2
    return None


def _map_hedge_type(value: Any) -> Optional[int]:
    normalized = str(value or "").strip().lower()
    mapping = {"delta": 1, "closest_premium": 2, "strike": 3}
    if normalized in mapping:
        return mapping[normalized]
    return _safe_int(value, 0) or None


def _map_strike_shift(value: Any) -> float:
    normalized = str(value or "").strip().upper()
    if normalized == "ATM":
        return 0.0
    if normalized.startswith("ITM"):
        return float(_safe_int(normalized[3:], 0))
    if normalized.startswith("OTM"):
        return float(-_safe_int(normalized[3:], 0))
    return 0.0


def _extract_strategy_date(positions: list[dict]) -> str:
    for position in positions or []:
        raw_entry_time = str(position.get("entry_time") or "").strip()
        if len(raw_entry_time) >= 10:
            return raw_entry_time[:10]
    return _now_ist().strftime("%Y-%m-%d")


def _normalize_saved_config(
    config: dict[str, Any],
    *,
    instrument: str,
    positions: list[dict],
) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {}

    # If the config is already in engine request shape, preserve it.
    required_keys = {
        "lot",
        "timeframe",
        "expiry_type",
        "stoploss_status",
        "target_status",
        "trailing_sl_status",
        "trading_mode",
        "headging_status",
        "backtest_start_date",
        "backtest_end_date",
        "position_start_time",
        "position_end_time",
        "position_exit_time_on_expiry",
    }
    if required_keys.issubset(set(config.keys())):
        return deepcopy(config)

    stop_loss = config.get("stopLoss") if isinstance(config.get("stopLoss"), dict) else {}
    target = config.get("target") if isinstance(config.get("target"), dict) else {}
    trailing = config.get("trailingStop") if isinstance(config.get("trailingStop"), dict) else {}
    time_control = config.get("timeControl") if isinstance(config.get("timeControl"), dict) else {}
    strike_type = config.get("strikeType") if isinstance(config.get("strikeType"), dict) else {}

    trade_date = _extract_strategy_date(positions)
    lot_size = _LOT_SIZE_BY_INSTRUMENT.get(str(instrument or "").upper(), 75)
    hedge_enabled = bool(strike_type.get("enabled"))
    hedge_type = _map_hedge_type(strike_type.get("mode")) if hedge_enabled else None
    hedge_value = (
        _map_strike_shift(strike_type.get("strike"))
        if hedge_type == 3
        else _safe_float(strike_type.get("value"), 0.0)
    )
    entry_time = str(time_control.get("entryTime") or "09:15").strip() or "09:15"
    exit_time = str(time_control.get("exitTime") or "15:30").strip() or "15:30"

    return {
        "lot": max(1, _safe_int(config.get("lots"), 1)),
        "lot_size": lot_size,
        "timeframe": str(config.get("timeframe") or "5m").strip() or "5m",
        "strategy_type": str(config.get("strategy_type") or "mini_strangle").strip() or "mini_strangle",
        "expiry_type": _map_expiry_type(config.get("expiryType")),
        "stoploss_status": 1 if stop_loss.get("enabled") else 0,
        "stoploss_type": _map_unit_to_type(stop_loss.get("unit")),
        "stoploss_value": _safe_float(stop_loss.get("value"), 0.0),
        "target_status": 1 if target.get("enabled") else 0,
        "target_type": _map_unit_to_type(target.get("unit")),
        "target_value": _safe_float(target.get("value"), 0.0),
        "trailing_sl_status": 1 if trailing.get("enabled") else 0,
        "trailing_sl_x": _safe_float(trailing.get("x"), 0.0),
        "trailing_sl_y": _safe_float(trailing.get("y"), 0.0),
        "trading_mode": _map_trade_mode(config.get("trading_mode")),
        "headging_status": 1 if hedge_enabled else 0,
        "headging_type": hedge_type,
        "headging_value": hedge_value if hedge_enabled else None,
        "headging_entry_time": entry_time if hedge_enabled and time_control.get("enabled") else None,
        "headging_exit_time": exit_time if hedge_enabled and time_control.get("enabled") else None,
        "backtest_start_date": str(config.get("backtest_start_date") or trade_date),
        "backtest_end_date": str(config.get("backtest_end_date") or trade_date),
        "position_start_time": str(config.get("position_start_time") or entry_time),
        "position_end_time": str(config.get("position_end_time") or exit_time),
        "position_exit_time_on_expiry": str(
            config.get("position_exit_time_on_expiry") or exit_time
        ),
        "adjustment_triggered_waiting": _safe_int(
            config.get("adjustment_triggered_waiting"),
            0,
        ),
        "reentry_delay": max(0, _safe_int(config.get("reentry_delay", config.get("reentry_interval")), 0)),
        "event_hit_position_status": _safe_int(config.get("event_hit_position_status"), 0),
        "event_hit_entry_condition": (
            _safe_int(config.get("event_hit_entry_condition"), 0)
            if config.get("event_hit_entry_condition") is not None
            else None
        ),
    }


class MonitorStreamingController(StreamingController):
    def __init__(self, on_event, position_start_time: str = "09:15") -> None:
        super().__init__(position_start_time=position_start_time)
        self._on_event = on_event

    async def send(self, event_type: str, data: dict) -> None:
        await super().send(event_type, data)
        self._on_event(event_type, data)


class SimulatorMonitorService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._engine: Optional[StrategyEngine] = None
        self._controller: Optional[MonitorStreamingController] = None
        self._run_task: Optional[asyncio.Task] = None
        self._drain_task: Optional[asyncio.Task] = None
        self._session_id: str = ""
        self._started_at: str = ""
        self._stopped_at: str = ""
        self._last_event: str = ""
        self._last_event_at: str = ""
        self._last_error: str = ""
        self._last_report_path: str = ""
        self._latest_monitor: dict[str, Any] = {}
        self._last_event_data: dict[str, Any] = {}
        self._event_counts: dict[str, int] = {}
        self._strategy_meta: dict[str, Any] = {}

    def _is_running(self) -> bool:
        return bool(self._run_task and not self._run_task.done())

    def _load_request(
        self,
        *,
        strategy_id: str = "",
        portfolio_name: str = "",
    ) -> tuple[MiniStrangleRequest, dict[str, Any]]:
        db = MongoData()
        strategy_col = db._db["simulator_strategy"]

        try:
            if strategy_id:
                try:
                    filt: dict[str, Any] = {"_id": ObjectId(strategy_id)}
                except Exception as exc:
                    raise ValueError(f"Invalid strategy_id: {strategy_id}") from exc
                doc = strategy_col.find_one(filt)
            else:
                filt = {}
                if portfolio_name:
                    filt["portfolio_name"] = portfolio_name
                doc = strategy_col.find_one(filt, sort=[("saved_at", -1), ("_id", -1)])

            if not doc:
                if strategy_id:
                    raise ValueError(f"Simulator strategy not found: {strategy_id}")
                if portfolio_name:
                    raise ValueError(
                        f"No simulator strategy found for portfolio '{portfolio_name}'"
                    )
                raise ValueError(
                    "No simulator strategy found. Save a paper-trade strategy first."
                )

            positions = doc.get("positions") if isinstance(doc.get("positions"), list) else []
            config = _normalize_saved_config(
                deepcopy(doc.get("config") or {}),
                instrument=str(doc.get("instrument") or ""),
                positions=positions,
            )
            if not config:
                raise ValueError(
                    f"Strategy '{doc.get('strategy_name') or doc.get('_id')}' has no config"
                )

            try:
                request = MiniStrangleRequest(**config)
            except ValidationError as exc:
                raise ValueError(
                    f"Saved simulator config is incomplete: {exc.errors()}"
                ) from exc

            meta = {
                "strategy_id": str(doc.get("_id") or ""),
                "strategy_name": str(doc.get("strategy_name") or ""),
                "portfolio_name": str(doc.get("portfolio_name") or ""),
                "saved_at": str(doc.get("saved_at") or ""),
                "config_keys": sorted(config.keys()),
            }
            return request, meta
        finally:
            try:
                db.close()
            except Exception:
                pass

    def _on_event(self, event_type: str, data: dict) -> None:
        with self._lock:
            self._last_event = str(event_type or "")
            self._last_event_at = str(data.get("timestamp") or data.get("ts") or "")
            self._last_event_data = deepcopy(data or {})
            self._event_counts[event_type] = int(self._event_counts.get(event_type, 0) or 0) + 1
            if event_type == "monitor":
                self._latest_monitor = deepcopy(data or {})
            if event_type == "error":
                self._last_error = str(data.get("message") or "Unknown simulator monitor error")
            if event_type == "stopped":
                self._stopped_at = self._last_event_at

    async def _drain_stream(self, controller: MonitorStreamingController) -> None:
        async for _ in controller.stream():
            pass

    async def _run_engine(self, session_id: str, engine: StrategyEngine) -> None:
        try:
            await engine.run()
        finally:
            with self._lock:
                if self._session_id == session_id:
                    self._engine = None
                    self._controller = None
                    self._run_task = None
                    self._drain_task = None

    async def start(
        self,
        *,
        strategy_id: str = "",
        portfolio_name: str = "",
    ) -> dict[str, Any]:
        with self._lock:
            if self._is_running():
                payload = self.get_status()
                payload["already_running"] = True
                return payload

        request, meta = self._load_request(
            strategy_id=str(strategy_id or "").strip(),
            portfolio_name=str(portfolio_name or "").strip(),
        )

        controller = MonitorStreamingController(
            self._on_event,
            position_start_time=request.position_start_time,
        )
        engine = StrategyEngine(request, controller)
        loop = asyncio.get_running_loop()
        session_id = str(uuid.uuid4())

        with self._lock:
            self._engine = engine
            self._controller = controller
            self._session_id = session_id
            self._started_at = datetime.now().isoformat(timespec="seconds")
            self._stopped_at = ""
            self._last_event = "starting"
            self._last_event_at = ""
            self._last_error = ""
            self._latest_monitor = {}
            self._last_event_data = {}
            self._event_counts = {}
            self._strategy_meta = meta
            self._last_report_path = getattr(controller, "_csv_path", "") or ""
            self._run_task = loop.create_task(
                self._run_engine(session_id, engine),
                name=f"simulator-monitor-run-{session_id}",
            )
            self._drain_task = loop.create_task(
                self._drain_stream(controller),
                name=f"simulator-monitor-drain-{session_id}",
            )

        return self.get_status()

    async def stop(self) -> dict[str, Any]:
        with self._lock:
            engine = self._engine
            run_task = self._run_task
            drain_task = self._drain_task

        if not engine:
            payload = self.get_status()
            payload["already_stopped"] = True
            return payload

        engine.stop()

        if run_task:
            try:
                await asyncio.wait_for(asyncio.shield(run_task), timeout=10)
            except asyncio.TimeoutError:
                pass

        if drain_task:
            try:
                await asyncio.wait_for(asyncio.shield(drain_task), timeout=2)
            except asyncio.TimeoutError:
                pass

        with self._lock:
            self._engine = None
            self._controller = None
            self._run_task = None
            self._drain_task = None
            if not self._stopped_at:
                self._stopped_at = self._last_event_at

        return self.get_status()

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            running = self._is_running()
            return {
                "running": running,
                "monitor_status": "running" if running else "stopped",
                "session_id": self._session_id,
                "started_at": self._started_at,
                "stopped_at": self._stopped_at,
                "last_event": self._last_event,
                "last_event_at": self._last_event_at,
                "last_error": self._last_error,
                "strategy": deepcopy(self._strategy_meta),
                "event_counts": deepcopy(self._event_counts),
                "latest_monitor": deepcopy(self._latest_monitor),
                "last_event_data": deepcopy(self._last_event_data),
                "report_path": self._last_report_path,
            }


_monitor_service: Optional[SimulatorMonitorService] = None


def get_simulator_monitor_service() -> SimulatorMonitorService:
    global _monitor_service
    if _monitor_service is None:
        _monitor_service = SimulatorMonitorService()
    return _monitor_service
