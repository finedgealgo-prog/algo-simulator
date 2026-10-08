"""
delta_alert_checker.py
────────────────────────
Background loop that evaluates BTC/ETH chart price/trendline/indicator
alerts (the same `tv_alerts` documents CryptoFullChartWorkspace.tsx creates
via the shared /v1/alerts endpoints) against Delta Exchange's live spot/index
price — the crypto analog of shared/features/alert_checker.py.

Deliberately a SEPARATE module/loop/websocket from that NSE checker, not a
branch bolted onto it, for the same reason delta_exchange_client.py's own
docstring gives for not routing through features/broker_gateway.py: Delta is
a different, always-open market running alongside NSE, not instead of it.
Concretely, three things the NSE checker can't be reused for as-is:
  1. Price source — NSE reads `option_chain_index_spot` (Kite/Dhan ticks);
     this reads delta_ticker_manager's live cache (same source
     delta_live_quote_socket.py's live chart ticks come from), keyed by
     spot_price rather than mark_price (see delta_live_quote_socket.py's
     module docstring for why those two numbers genuinely differ).
  2. Bar fetch — NSE calls get_index_historical_chart_bars (Kite/Dhan);
     this calls delta_exchange_client.fetch_candles (Delta's public
     /v2/history/candles on the spot-index symbol).
  3. Timing — NSE's crossing/trendline/indicator-scheduler math assumes a
     09:15–15:30 IST Mon–Fri session (see alert_checker.py's own
     _trading_minutes_between and indicator_alerts.py's seconds_until_next_
     bar_close); Delta trades 24/7, so every one of those needed a plain
     wall-clock/epoch equivalent instead — see the local helpers below.
Everything else — the crossing/trigger-mode/trendline-value math itself,
webhook delivery, Telegram notification — has no NSE dependency and is
reused directly from features.alert_checker/features.indicator_alerts/
features.telegram_notifier.

Started unconditionally at process boot (see api.py) and NEVER registered
with features/market_hours_scheduler.py's NSE auto-stop — a 24/7 market has
no "after hours" to pause for.
"""

from __future__ import annotations

import asyncio
import logging
import queue
import threading
import time
from typing import Any

from features.alert_checker import _deliver_webhook, _resolve_message_placeholders
from features.indicator_alerts import evaluate_indicator_condition, evaluate_price_condition
from features.mongo_data import MongoData
from features.telegram_notifier import notify_user_for

from simulator.delta_alert_events_socket import mark_delta_alert_fired
from features.delta_exchange_client import fetch_candles
from features.delta_exchange_ws import delta_ticker_manager

logger = logging.getLogger(__name__)

# POLL_INTERVAL_SECONDS is now only the slow reconcile/safety-net cadence
# (catches a missed WS hook call, a fresh alert not yet in _ALERTS_CACHE,
# or a reconnect gap) — the real hot path is on_underlying_tick() below,
# called directly from delta_exchange_ws.py's WS thread per underlying, so
# a BTC tick never re-evaluates ETH alerts (or vice versa). See module
# docstring update + algo_signal_alert_engine_low_cpu_architecture doc.
POLL_INTERVAL_SECONDS = 5.0
ONCE_PER_MINUTE_COOLDOWN_MS = 60_000
INDICATOR_SCHEDULER_MAX_SLEEP_SECONDS = 60.0
_ALERTS_CACHE_TTL_SECONDS = 5.0

# Trigger execution is decoupled from the tick hot path: on_underlying_tick
# runs on delta_exchange_ws.py's WS receive thread, so it must never block
# on a webhook HTTP call or Telegram send. The atomic claim (_try_claim)
# still happens inline — it's a single indexed Mongo update, fast — but the
# actual delivery work is hop-off'd onto a small fixed worker pool behind a
# BOUNDED queue (never grows unbounded; a full queue drops-and-logs rather
# than blocking the feed or piling up memory indefinitely).
_TRIGGER_QUEUE_MAXSIZE = 500
_TRIGGER_WORKER_COUNT = 4
_trigger_queue: "queue.Queue[tuple[Any, ...]]" = queue.Queue(maxsize=_TRIGGER_QUEUE_MAXSIZE)

_TRENDLINE_BARS_TTL_SECONDS = 60.0
_TRENDLINE_BARS_RESOLUTION = "1"
_BAR_CLOSE_BUFFER_SECONDS = 5.0

ALERTS_COLLECTION = "tv_alerts"
CRYPTO_UNDERLYINGS = ("BTC", "ETH")

# Mirrors alert_checker.py's _UNIMPLEMENTED_DIRECTIONS (Chart.tsx's
# isUnimplementedDirection) — selectable in the UI, never evaluated.
_UNIMPLEMENTED_DIRECTIONS = {
    "enter_channel", "exit_channel", "inside_channel", "outside_channel",
    "moving_up", "moving_down", "moving_up_percent", "moving_down_percent",
    "rising_to_falling", "falling_to_rising",
}


# ── 24/7 timing helpers — see module docstring point 3 ─────────────────────

def _get_crypto_indicator_lookback_seconds(resolution: str) -> int:
    """24/7 equivalent of indicator_alerts.py's get_indicator_lookback_seconds
    — every calendar day is a full trading day here, so no 6.25h/day
    trading-time conversion is needed, just a flat day count."""
    day_seconds = 86400
    if resolution == "1D":
        return day_seconds * 200
    if resolution == "3D":
        return day_seconds * 200 * 3
    if resolution == "1W":
        return day_seconds * 365 * 4
    if resolution == "1M":
        return day_seconds * 365 * 10
    try:
        minutes = float(resolution)
    except (TypeError, ValueError):
        return day_seconds * 30
    if minutes <= 0:
        return day_seconds * 30
    bars_per_day = max(1, int(1440 // minutes))
    days_needed = -(-150 // bars_per_day) + 3  # ceil(150 / bars_per_day) + buffer
    return day_seconds * days_needed


def _seconds_until_next_crypto_bar_close(resolution: str, now_ts: float) -> float:
    """24/7 equivalent of indicator_alerts.py's seconds_until_next_bar_close
    — bar boundaries are plain epoch-ms-aligned buckets (see the frontend's
    own getCurrentCryptoBarStartMs in CryptoFullChartWorkspace.tsx, same
    convention), not anchored to an NSE session open."""
    day_seconds = 86400.0
    if resolution in ("1D", "3D", "1W", "1M"):
        interval_seconds = day_seconds
    else:
        try:
            interval = float(resolution)
        except (TypeError, ValueError):
            interval = 5.0
        if interval <= 0:
            interval = 5.0
        interval_seconds = interval * 60.0
    next_boundary = (int(now_ts // interval_seconds) + 1) * interval_seconds
    return (next_boundary - now_ts) + _BAR_CLOSE_BUFFER_SECONDS


def _get_trendline_price_at_time(points: list[dict] | None, t: float) -> float | None:
    """24/7 equivalent of alert_checker.py's _get_trendline_price_at_time —
    plain ms-based linear interpolation, no trading-session-minutes
    conversion needed (every ms is "trading time" in a 24/7 market, so this
    is exact rather than an approximation the way the NSE calendar-minutes
    version is — no bar-index refinement pass needed either)."""
    if not points or len(points) < 2:
        return None
    start, end = points[0], points[-1]
    start_time, end_time = start.get("time"), end.get("time")
    start_price, end_price = start.get("price"), end.get("price")
    if start_time is None or end_time is None or start_time == end_time:
        return None
    if start_price is None or end_price is None:
        return None
    slope_per_ms = (end_price - start_price) / (end_time - start_time)
    return start_price + slope_per_ms * (t - start_time)


def _is_time_inside_alert_line(points: list[dict] | None, line_mode: str | None, t: float) -> bool:
    if not points or len(points) < 2:
        return False
    start_time = points[0].get("time")
    if start_time is None:
        return False
    if line_mode in ("extended", "ray_left"):
        return True
    return t >= start_time


def _chain_needs_bar_close_engine(alert: dict) -> bool:
    if alert.get("sourceType") == "indicator":
        return True
    return bool(alert.get("additionalConditions"))


def _get_alert_price_at_time(alert: dict, t: float) -> float | None:
    if alert.get("sourceType") == "trendline":
        points = alert.get("linePoints")
        if not _is_time_inside_alert_line(points, alert.get("lineMode"), t):
            return None
        return _get_trendline_price_at_time(points, t)
    price = alert.get("price")
    return float(price) if isinstance(price, (int, float)) else None


def _did_cross_level(prev_price: float, curr_price: float, alert: dict, curr_t: float) -> str | None:
    direction = alert.get("direction")
    if direction in _UNIMPLEMENTED_DIRECTIONS:
        return None
    curr_level = _get_alert_price_at_time(alert, curr_t)
    if curr_level is None:
        return None
    if direction == "crosses_above":
        return "up" if curr_price >= curr_level else None
    if direction == "crosses_below":
        return "down" if curr_price <= curr_level else None
    if direction == "greater_than":
        return "up" if curr_price > curr_level else None
    if direction == "less_than":
        return "down" if curr_price < curr_level else None
    if prev_price < curr_level <= curr_price:
        return "up"
    if prev_price > curr_level >= curr_price:
        return "down"
    return None


def _is_level_touched(price: float, t: float, alert: dict) -> str | None:
    direction = alert.get("direction")
    if direction in _UNIMPLEMENTED_DIRECTIONS:
        return None
    level = _get_alert_price_at_time(alert, t)
    if level is None:
        return None
    if direction in ("crosses_below", "less_than"):
        return "down" if price <= level else None
    if direction in ("crosses_above", "greater_than"):
        return "up" if price >= level else None
    if price >= level:
        return "up"
    if price <= level:
        return "down"
    return None


def evaluate_trendline_condition(
    direction: str, line_points: list[dict] | None, line_mode: str | None, bars: list[dict]
) -> tuple[bool, float] | None:
    n = len(bars)
    if n < 2:
        return None
    last, prev = n - 1, n - 2
    bar_time = float(bars[last]["time"])
    prev_close, curr_close = bars[prev].get("close"), bars[last].get("close")
    if not isinstance(prev_close, (int, float)) or not isinstance(curr_close, (int, float)):
        return None
    pseudo_alert = {"sourceType": "trendline", "direction": direction, "linePoints": line_points, "lineMode": line_mode}
    crossed = _did_cross_level(float(prev_close), float(curr_close), pseudo_alert, bar_time)
    return crossed is not None, bar_time


class _DeltaAlertChecker:
    """Crypto analog of alert_checker.py's _AlertChecker — holds only each
    underlying's last-seen price/time across polls; everything else is read
    straight from the stored tv_alerts document each cycle."""

    def __init__(self) -> None:
        self._previous_sample: dict[str, tuple[float, float]] = {}
        self._trendline_bars_cache: dict[str, tuple[float, list[dict]]] = {}
        self._ensured_underlyings: set[str] = set()
        # Per-underlying in-memory alert cache — the doc's "no Mongo read in
        # the tick hot path" rule. on_underlying_tick() reads this instead of
        # querying alerts_col on every tick; TTL is the staleness ceiling,
        # invalidate_alert_cache() drops an entry immediately on create/
        # update/delete so edits don't wait out the TTL.
        self._alerts_cache: dict[str, tuple[float, list[dict]]] = {}
        self._alerts_cache_lock = threading.Lock()

    def _get_trendline_bars(self, symbol: str, trendline_alerts: list[dict]) -> list[dict]:
        now = time.time()
        cached = self._trendline_bars_cache.get(symbol)
        if cached and now - cached[0] < _TRENDLINE_BARS_TTL_SECONDS:
            return cached[1]

        anchor_times = [
            pt.get("time")
            for alert in trendline_alerts
            for pt in (alert.get("linePoints") or [])
            if isinstance(pt.get("time"), (int, float))
        ]
        if not anchor_times:
            return cached[1] if cached else []

        from_ts = int(min(anchor_times) / 1000) - 3600
        to_ts = int(now)
        try:
            bars = fetch_candles(symbol, _TRENDLINE_BARS_RESOLUTION, from_ts, to_ts)
        except Exception:
            logger.exception("[delta_alert_checker] failed to fetch %s trendline bars", symbol)
            bars = cached[1] if cached else []
        self._trendline_bars_cache[symbol] = (now, bars)
        return bars

    def run_cycle(self) -> None:
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]

        alerts = list(alerts_col.find({"active": True, "symbol": {"$in": list(CRYPTO_UNDERLYINGS)}}))
        if not alerts:
            return

        now_ms = time.time() * 1000
        needed_underlyings = {str(a.get("symbol") or "").upper() for a in alerts}

        new_underlyings = needed_underlyings - self._ensured_underlyings
        if new_underlyings:
            for underlying in new_underlyings:
                try:
                    delta_ticker_manager.ensure_subscribed(underlying, "")
                except Exception:
                    logger.exception("[delta_alert_checker] ensure_subscribed error for %s", underlying)
            self._ensured_underlyings.update(new_underlyings)

        current_price_by_underlying: dict[str, float] = {}
        for underlying in needed_underlyings:
            ticker = delta_ticker_manager.get_ticker(f"{underlying}USD")
            if not ticker:
                continue
            spot = float(ticker.get("spot_price") or 0)
            if spot > 0:
                current_price_by_underlying[underlying] = spot

        alerts_by_symbol: dict[str, list[dict]] = {}
        for alert in alerts:
            alerts_by_symbol.setdefault(str(alert.get("symbol") or ""), []).append(alert)

        for symbol, symbol_alerts in alerts_by_symbol.items():
            underlying = symbol.upper()
            curr_price = current_price_by_underlying.get(underlying)
            if curr_price is None:
                continue

            prev_price, prev_t = self._previous_sample.get(underlying, (None, None))
            self._previous_sample[underlying] = (curr_price, now_ms)
            if prev_price is None:
                continue

            trendline_alerts = [a for a in symbol_alerts if a.get("sourceType") == "trendline"]
            bars = self._get_trendline_bars(symbol, trendline_alerts) if trendline_alerts else []

            self._check_alerts(symbol_alerts, prev_price, curr_price, now_ms, bars)

    def _get_symbol_alerts(self, underlying: str, *, force: bool = False) -> list[dict]:
        now = time.time()
        with self._alerts_cache_lock:
            cached = self._alerts_cache.get(underlying)
            if not force and cached and now - cached[0] < _ALERTS_CACHE_TTL_SECONDS:
                return cached[1]
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        alerts = list(alerts_col.find({"active": True, "symbol": underlying}))
        with self._alerts_cache_lock:
            self._alerts_cache[underlying] = (now, alerts)
        return alerts

    def invalidate_symbol_cache(self, underlying: str) -> None:
        with self._alerts_cache_lock:
            self._alerts_cache.pop(underlying, None)

    def on_underlying_tick(self, underlying: str, curr_price: float, now_ms: float) -> None:
        """Token-routed hot path — call directly (not via the poll loop) for
        the SPECIFIC underlying that just ticked, e.g. from delta_exchange_
        ws.py's WS message handler. A BTC tick only ever evaluates BTC
        alerts, never ETH's, unlike the old design where any Delta tick woke
        run_cycle() into rescanning both symbols' full alert sets.

        Runs on the caller's thread (typically the WS receive thread) — must
        stay cheap: in-memory cache read + the same crossing math run_cycle
        already used, no Mongo read, no blocking webhook/Telegram call (see
        _try_claim/_enqueue_trigger below for how firing is decoupled)."""
        if not underlying or curr_price is None or curr_price <= 0:
            return
        try:
            curr_price = float(curr_price)
        except (TypeError, ValueError):
            return

        prev_price, _ = self._previous_sample.get(underlying, (None, None))
        self._previous_sample[underlying] = (curr_price, now_ms)
        if prev_price is None:
            return

        symbol_alerts = self._get_symbol_alerts(underlying)
        if not symbol_alerts:
            return

        trendline_alerts = [a for a in symbol_alerts if a.get("sourceType") == "trendline"]
        bars = self._get_trendline_bars(underlying, trendline_alerts) if trendline_alerts else []
        self._check_alerts(symbol_alerts, prev_price, curr_price, now_ms, bars)

    def get_active_indicator_resolutions(self) -> set[str]:
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        values = alerts_col.distinct("indicatorResolution", {
            "active": True,
            "indicatorResolution": {"$ne": None},
            "symbol": {"$in": list(CRYPTO_UNDERLYINGS)},
        })
        return {value for value in values if value}

    def _fetch_indicator_bars(self, symbol: str, resolution: str) -> list[dict]:
        try:
            lookback = _get_crypto_indicator_lookback_seconds(resolution)
            to_ts = int(time.time())
            return fetch_candles(symbol, resolution, to_ts - lookback, to_ts)
        except Exception:
            logger.exception("[delta_alert_checker] failed to fetch %s/%s bars for indicator alert", symbol, resolution)
            return []

    @staticmethod
    def _evaluate_chain_entry(
        entry: dict, symbol: str, resolution: str, bars: list[dict], evaluation_by_tuple: dict
    ) -> tuple[bool, float] | None:
        kind = entry.get("kind")
        if kind == "indicator":
            indicator_name = entry.get("indicatorName")
            condition = entry.get("indicatorCondition")
            if not (indicator_name and condition):
                return None
            raw_value = entry.get("value")
            threshold = None
            if raw_value not in (None, ""):
                try:
                    threshold = float(raw_value)
                except (TypeError, ValueError):
                    threshold = None
            key = (symbol, resolution, "indicator", indicator_name, condition, threshold)
            if key not in evaluation_by_tuple:
                evaluation_by_tuple[key] = evaluate_indicator_condition(indicator_name, condition, bars, threshold)
            return evaluation_by_tuple[key]
        if kind == "price":
            direction = entry.get("direction")
            value = entry.get("value")
            if direction is None or value is None:
                return None
            try:
                value = float(value)
            except (TypeError, ValueError):
                return None
            key = (symbol, resolution, "price", direction, value)
            if key not in evaluation_by_tuple:
                evaluation_by_tuple[key] = evaluate_price_condition(direction, value, bars)
            return evaluation_by_tuple[key]
        if kind == "trendline":
            direction = entry.get("direction")
            if direction is None:
                return None
            return evaluate_trendline_condition(direction, entry.get("linePoints"), entry.get("lineMode"), bars)
        return None

    @staticmethod
    def _effective_entry_resolution(entry: dict, base_resolution: str) -> str:
        if entry.get("kind") == "indicator":
            return entry.get("resolution") or base_resolution
        return base_resolution

    def check_indicator_alerts(self) -> None:
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        alerts = list(alerts_col.find({
            "active": True,
            "indicatorResolution": {"$ne": None},
            "symbol": {"$in": list(CRYPTO_UNDERLYINGS)},
        }))
        if not alerts:
            return

        bars_by_key: dict[tuple[str, str], list[dict]] = {}
        evaluation_by_tuple: dict[tuple, tuple[bool, float] | None] = {}

        for alert in alerts:
            if alert.get("isReplayTest"):
                continue

            symbol = str(alert.get("symbol") or "")
            alert_id = alert.get("id")
            base_resolution = alert.get("indicatorResolution")
            if not (alert_id and base_resolution):
                continue

            source_type = alert.get("sourceType")
            if source_type == "indicator":
                primary_entry = {
                    "kind": "indicator",
                    "indicatorName": alert.get("indicatorName"),
                    "indicatorCondition": alert.get("indicatorCondition"),
                    "value": alert.get("indicatorValue"),
                    "resolution": base_resolution,
                }
            elif source_type == "trendline":
                primary_entry = {
                    "kind": "trendline",
                    "direction": alert.get("direction"),
                    "linePoints": alert.get("linePoints"),
                    "lineMode": alert.get("lineMode"),
                }
            else:
                primary_entry = {"kind": "price", "direction": alert.get("direction"), "value": alert.get("price")}
            chain = [primary_entry, *(alert.get("additionalConditions") or [])]
            resolved_chain = [
                (entry, self._effective_entry_resolution(entry, base_resolution)) for entry in chain
            ]

            evaluations: list[tuple[bool, float] | None] = []
            bars_missing = False
            for entry, resolution in resolved_chain:
                bar_key = (symbol, resolution)
                if bar_key not in bars_by_key:
                    bars_by_key[bar_key] = self._fetch_indicator_bars(symbol, resolution)
                bars = bars_by_key[bar_key]
                if len(bars) < 2:
                    bars_missing = True
                    break
                evaluations.append(self._evaluate_chain_entry(entry, symbol, resolution, bars, evaluation_by_tuple))
            if bars_missing:
                continue
            if any(evaluation is None or not evaluation[0] for evaluation in evaluations):
                continue

            base_bars = bars_by_key.get((symbol, base_resolution))
            if not base_bars:
                continue
            bar_time = base_bars[-1].get("time")
            if not isinstance(bar_time, (int, float)):
                continue

            last_signal = alert.get("lastIndicatorSignalBarTime")
            if isinstance(last_signal, (int, float)) and bar_time <= last_signal:
                continue

            trigger_price = base_bars[-1].get("close")
            if not isinstance(trigger_price, (int, float)):
                trigger_price = alert.get("price") or 0.0

            field_updates = {"lastIndicatorSignalBarTime": bar_time}
            if alert.get("triggerMode") == "once_only":
                field_updates["active"] = False

            claim_filter = {
                "$or": [
                    {"lastIndicatorSignalBarTime": {"$exists": False}},
                    {"lastIndicatorSignalBarTime": {"$lt": bar_time}},
                ]
            }
            claimed = self._try_claim(alert_id, claim_filter, field_updates)
            if claimed is None:
                continue
            _enqueue_trigger(self, claimed, "indicator", float(trigger_price), field_updates)

    def _check_alerts(
        self,
        alerts: list[dict],
        prev_price: float,
        curr_price: float,
        now_ms: float,
        bars: list[dict] | None = None,
    ) -> None:
        for alert in alerts:
            alert_id = alert.get("id")
            if not alert_id or not alert.get("active"):
                continue
            if alert.get("isReplayTest"):
                continue
            if _chain_needs_bar_close_engine(alert):
                continue
            armed_from = alert.get("armedFromBarTime") or 0
            if now_ms <= armed_from:
                continue

            trigger_mode = alert.get("triggerMode")
            direction: str | None = None

            if trigger_mode == "once_per_minute":
                touched = _is_level_touched(curr_price, now_ms, alert)
                if touched:
                    last_fired = alert.get("lastTriggeredAt") or 0
                    if now_ms - last_fired >= ONCE_PER_MINUTE_COOLDOWN_MS:
                        direction = touched
            else:
                raw_direction = _did_cross_level(prev_price, curr_price, alert, now_ms)
                alert_direction = alert.get("direction")
                if alert_direction in ("crosses_above", "crosses_below"):
                    cross_primed = bool(alert.get("crossPrimed"))
                    if raw_direction:
                        if cross_primed:
                            direction = raw_direction
                    elif not cross_primed:
                        self._persist_update(alert_id, {"crossPrimed": True})
                else:
                    direction = raw_direction

            if not direction:
                continue

            trigger_price = _get_alert_price_at_time(alert, now_ms)
            if trigger_price is None:
                trigger_price = alert.get("price")

            field_updates = {"lastTriggeredAt": now_ms}
            if alert.get("direction") in ("crosses_above", "crosses_below"):
                field_updates["crossPrimed"] = False
            if trigger_mode == "once_only":
                field_updates["active"] = False

            # Atomic trigger claim (ARMED -> TRIGGER_CLAIMED): guard the fire
            # against a concurrent evaluation of the same crossing/cooldown/
            # once-only edge — old code fired then persisted, which could
            # double-fire under overlapping cycles. crossPrimed/once_per_
            # minute alerts re-validate their arm/cooldown state against
            # Mongo's CURRENT value, not the (possibly briefly stale,
            # cached) `alert` dict.
            claim_filter: dict[str, Any] = {}
            if "crossPrimed" in field_updates:
                claim_filter["crossPrimed"] = True
            if trigger_mode == "once_per_minute":
                cooldown_cutoff = now_ms - ONCE_PER_MINUTE_COOLDOWN_MS
                claim_filter["$or"] = [
                    {"lastTriggeredAt": {"$exists": False}},
                    {"lastTriggeredAt": {"$lte": cooldown_cutoff}},
                ]

            claimed = self._try_claim(alert_id, claim_filter, field_updates)
            if claimed is None:
                continue
            _enqueue_trigger(self, claimed, direction, trigger_price, field_updates)

    def _fire_alert(self, alert: dict, direction: str, trigger_price: float, field_updates: dict) -> None:
        alert_name = alert.get("name") or "Alert"
        price_str = f"{float(trigger_price):.2f}" if trigger_price is not None else ""
        arrow = "↑" if direction == "up" else "↓" if direction == "down" else "→"
        source_type = alert.get("sourceType") or "price"
        condition_label = {
            "crosses_above": "Crossing Up",
            "crosses_below": "Crossing Down",
            "crosses_either": "Crossing",
            "greater_than": "Greater Than",
            "less_than": "Less Than",
        }.get(alert.get("direction") or "", alert.get("direction") or "")
        symbol_label = str(alert.get("symbol") or "").upper()
        line_kind = "Trendline" if source_type == "trendline" else "Price"

        mark_delta_alert_fired(str(alert.get("user_id") or ""), {
            "alert_id": alert.get("id"),
            "name": alert_name,
            "symbol": symbol_label,
            "sourceType": source_type,
            "direction": alert.get("direction"),
            "triggerMode": alert.get("triggerMode"),
            "action": alert.get("action"),
            "message": alert.get("message"),
            "triggerPrice": float(trigger_price) if trigger_price is not None else None,
            "deactivated": field_updates.get("active") is False,
        })

        if alert.get("notifyInApp", True):
            user_id = str(alert.get("user_id") or "").strip()
            tg_message = (
                f"{arrow} {alert_name}\n"
                f"{symbol_label} · {line_kind} · {condition_label}\n"
                f"Price: {price_str}"
            )
            notify_user_for(
                user_id or None,
                "CRYPTO CHART ALERT",
                tg_message,
                context={"symbol": symbol_label, "price": price_str},
                category="chart",
            )

        webhook_enabled = bool(alert.get("webhookEnabled"))
        webhook_url = alert.get("webhookUrl") or ""
        if not webhook_enabled or not webhook_url:
            logger.info("[delta_alert_checker] %s triggered (%s) — no webhook configured", alert_name, direction)
            return

        message = alert.get("message") or f"{alert_name} triggered"
        body = _resolve_message_placeholders(message, float(trigger_price) if trigger_price is not None else 0.0)
        result = _deliver_webhook(webhook_url, body)

        # Informational-only fields (never guard a trigger decision), so a
        # plain best-effort persist after delivery is fine here — unlike the
        # ARMED/cooldown/once-only fields, which are already committed
        # atomically by _try_claim() before this method ever runs.
        self._persist_update(alert.get("id"), {
            "lastWebhookOk": result["ok"],
            "lastWebhookStatus": result["status"],
            "lastWebhookResponse": (result["responseText"] or "")[:2000],
        })

        if result["ok"]:
            logger.info("[delta_alert_checker] %s triggered (%s) — webhook delivered to %s", alert_name, direction, webhook_url)
        else:
            logger.error(
                "[delta_alert_checker] %s triggered (%s) — webhook FAILED (%s) to %s: %s",
                alert_name, direction, result["status"], webhook_url, result["responseText"],
            )

    def _try_claim(self, alert_id: str, extra_filter: dict, field_updates: dict) -> dict | None:
        """Atomic compare-and-set trigger claim (doc: ARMED -> TRIGGER_
        CLAIMED). `extra_filter` re-validates the arm/cooldown condition
        against Mongo's CURRENT document, not the cached `alert` dict this
        evaluation started from — only the caller whose extra_filter still
        matches wins the update and may fire; everyone else (a concurrent
        tick, a concurrent scheduler run) gets None and must not fire.
        Returns the PRE-update document (pymongo's ReturnDocument.BEFORE
        default) so the caller still has the alert's config fields to build
        the notification/webhook from."""
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        claim_filter = {"id": alert_id, "active": True, **extra_filter}
        try:
            return alerts_col.find_one_and_update(claim_filter, {"$set": field_updates})
        except Exception:
            logger.exception("[delta_alert_checker] atomic trigger claim failed for alert %s", alert_id)
            return None

    def _persist_update(self, alert_id: str, field_updates: dict) -> None:
        db = MongoData()._db
        alerts_col = db[ALERTS_COLLECTION]
        try:
            alerts_col.update_one({"id": alert_id}, {"$set": field_updates})
        except Exception:
            logger.exception("[delta_alert_checker] failed to persist trigger state for alert %s", alert_id)


_checker = _DeltaAlertChecker()


# ── Decoupled trigger execution — bounded queue + fixed worker pool ────────
# See module docstring / _TRIGGER_QUEUE_MAXSIZE above: on_underlying_tick()
# runs on the Delta WS receive thread and must never block on a webhook HTTP
# call, so the actual _fire_alert() delivery work happens here instead, off
# that thread.

def _enqueue_trigger(checker: "_DeltaAlertChecker", alert: dict, direction: str, trigger_price: float, field_updates: dict) -> None:
    try:
        _trigger_queue.put_nowait((checker, alert, direction, trigger_price, field_updates))
    except queue.Full:
        logger.error(
            "[delta_alert_checker] trigger queue full (%d) — dropping fire for alert %s "
            "(symbol=%s); webhook/Telegram delivery is falling behind",
            _TRIGGER_QUEUE_MAXSIZE, alert.get("id"), alert.get("symbol"),
        )


def _trigger_worker_loop() -> None:
    while True:
        checker, alert, direction, trigger_price, field_updates = _trigger_queue.get()
        try:
            checker._fire_alert(alert, direction, trigger_price, field_updates)
        except Exception:
            logger.exception("[delta_alert_checker] trigger delivery failed for alert %s", alert.get("id"))
        finally:
            _trigger_queue.task_done()


_trigger_workers_started = False
_trigger_workers_lock = threading.Lock()


def _ensure_trigger_workers_started() -> None:
    global _trigger_workers_started
    with _trigger_workers_lock:
        if _trigger_workers_started:
            return
        for i in range(_TRIGGER_WORKER_COUNT):
            threading.Thread(
                target=_trigger_worker_loop,
                name=f"delta-alert-trigger-{i}",
                daemon=True,
            ).start()
        _trigger_workers_started = True


# ── Public entry points for delta_exchange_ws.py / chart_api.py ────────────

def on_underlying_tick(underlying: str, curr_price: float, now_ms: float) -> None:
    """Call directly from the Delta WS tick handler for the underlying that
    just ticked (see delta_exchange_ws.py's _dispatch_to_alert_engine). Lazy-
    starts the trigger worker pool on first use so importing this module
    alone never spins up threads."""
    _ensure_trigger_workers_started()
    _checker.on_underlying_tick(underlying, curr_price, now_ms)


def invalidate_alert_cache(symbol: str | None) -> None:
    """Best-effort cache-bust after an alert create/update/delete (see
    shared/chart_api.py's save_chart_alert/delete_chart_alert) so an edit is
    visible to on_underlying_tick() immediately instead of waiting out
    _ALERTS_CACHE_TTL_SECONDS. Safe to call for a non-crypto symbol too —
    it's just a dict.pop on an underlying that was never cached."""
    if not symbol:
        return
    _checker.invalidate_symbol_cache(str(symbol).upper())


async def start_delta_alert_checker_loop() -> None:
    """Call once from a FastAPI startup hook — runs forever for the life of
    the process. Unlike alert_checker.py's NSE loop, never registered with
    market_hours_scheduler: Delta has no after-hours to auto-stop for.

    NO LONGER the real trigger path — on_underlying_tick() (called directly
    from delta_exchange_ws.py's WS thread, token-routed per underlying) is.
    This is now a slow reconcile/safety-net full scan every
    POLL_INTERVAL_SECONDS, covering: a fresh alert not yet in the per-
    underlying cache, a missed/failed WS-thread dispatch, and the gap right
    after a WS reconnect before ticks resume. Deliberately no longer woken
    by delta_ticker_manager.tick_event — waking (and rescanning BOTH BTC and
    ETH) on every single Delta tick was exactly the O(total_alerts)-per-tick
    cost the token-routed hot path replaces."""
    _ensure_trigger_workers_started()
    while True:
        try:
            await asyncio.to_thread(_checker.run_cycle)
        except Exception:
            logger.exception("[delta_alert_checker] check cycle failed")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def start_delta_indicator_alert_scheduler_loop() -> None:
    """Call once from a FastAPI startup hook, alongside start_delta_alert_
    checker_loop — sleeps until the next real bar-close across whichever
    resolutions currently have an active crypto indicator alert."""
    while True:
        sleep_seconds = INDICATOR_SCHEDULER_MAX_SLEEP_SECONDS
        try:
            resolutions = await asyncio.to_thread(_checker.get_active_indicator_resolutions)
            if resolutions:
                now = time.time()
                sleep_seconds = min(
                    min(_seconds_until_next_crypto_bar_close(r, now) for r in resolutions),
                    INDICATOR_SCHEDULER_MAX_SLEEP_SECONDS,
                )
        except Exception:
            logger.exception("[delta_indicator_alert_scheduler] failed to compute next wake time")

        await asyncio.sleep(max(sleep_seconds, 1.0))

        try:
            await asyncio.to_thread(_checker.check_indicator_alerts)
        except Exception:
            logger.exception("[delta_indicator_alert_scheduler] check cycle failed")


# Manual on/off control (parity with alert_checker.py's endpoints) — both
# loops are also auto-started unconditionally at process boot (see api.py),
# unlike NSE's indicator scheduler which is manual-start-only there; a 24/7
# market has no reason to default either crypto loop to off.
_alert_checker_task: asyncio.Task | None = None
_indicator_monitor_task: asyncio.Task | None = None


def is_delta_alert_checker_running() -> bool:
    return _alert_checker_task is not None and not _alert_checker_task.done()


def start_delta_alert_checker_monitor() -> dict[str, Any]:
    global _alert_checker_task
    if is_delta_alert_checker_running():
        return {"status": "success", "running": True, "message": "Crypto alert checker is already running."}
    _alert_checker_task = asyncio.create_task(start_delta_alert_checker_loop())
    logger.info("[delta_alert_checker] monitor started")
    return {"status": "success", "running": True, "message": "Crypto alert checker started."}


async def stop_delta_alert_checker_monitor() -> dict[str, Any]:
    global _alert_checker_task
    task = _alert_checker_task
    if task is None or task.done():
        _alert_checker_task = None
        return {"status": "success", "running": False, "message": "Crypto alert checker is already stopped."}
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _alert_checker_task = None
    logger.info("[delta_alert_checker] monitor stopped")
    return {"status": "success", "running": False, "message": "Crypto alert checker stopped."}


def is_delta_indicator_alert_monitor_running() -> bool:
    return _indicator_monitor_task is not None and not _indicator_monitor_task.done()


def start_delta_indicator_alert_monitor() -> dict[str, Any]:
    global _indicator_monitor_task
    if is_delta_indicator_alert_monitor_running():
        return {"status": "success", "running": True, "message": "Crypto indicator alert monitor is already running."}
    _indicator_monitor_task = asyncio.create_task(start_delta_indicator_alert_scheduler_loop())
    logger.info("[delta_indicator_alert_scheduler] monitor started")
    return {"status": "success", "running": True, "message": "Crypto indicator alert monitor started."}


async def stop_delta_indicator_alert_monitor() -> dict[str, Any]:
    global _indicator_monitor_task
    task = _indicator_monitor_task
    if task is None or task.done():
        _indicator_monitor_task = None
        return {"status": "success", "running": False, "message": "Crypto indicator alert monitor is already stopped."}
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _indicator_monitor_task = None
    logger.info("[delta_indicator_alert_scheduler] monitor stopped")
    return {"status": "success", "running": False, "message": "Crypto indicator alert monitor stopped."}
