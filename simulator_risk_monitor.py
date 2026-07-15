"""
simulator_risk_monitor.py
──────────────────────────
Standalone SL / Target / hedge auto-exit monitor for the PaperTradeNew /
Positions simulator's saved triggers (`simulator_triggers` +
`simulator_portfolio_triggers`).

Deliberately separate from features/live_fast_monitor.py + trading_core.py
(the algo_trades strategy-builder's live + fast-forward engine) — different
collections, different UI, no shared task, no shared state. The only
cross-module calls are read-only reuse of
features.execution_socket._fetch_dhan_broker_option_positions() (already the
canonical "what's open + is the saved trigger still valid" lookup, used by
three existing endpoints) and a call into api.py's
_place_manual_order_via_order_service() to fire a real exit order — this box
isn't the one whitelisted with the broker for live order placement, so that
call goes out over HTTP to algo.order's internal gateway (the box that is)
rather than placing the order in-process here.

Architecture — hot loop vs warm refresh (see plan doc for the full rationale):
  - Hot tick, every HOT_TICK_SECONDS: pure in-memory dict/arithmetic against
    dhan_ticker_manager.ltp_map (already kept hot by the existing WS ticker).
    No Mongo, no broker call. O(active legs/baskets), not O(I/O).
  - Warm refresh, every WARM_REFRESH_SECONDS: the only place touching
    Mongo/broker. One _fetch_dhan_broker_option_positions() call per distinct
    broker_id that currently has an active trigger.
  - Fire: rare event. Places the real exit order, flips the trigger to
    status="fired" (never deleted — same audit-trail convention the existing
    drift-check uses for status="stale"), evicts from the hot cache.

Not auto-started at import or app-startup — controlled exclusively via the
/simulator/risk-monitor/start|stop endpoints (mirrors the existing
/simulator/monitor/start|stop toggle-page pattern), so a server restart
never silently re-arms automatic real-money exits.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from features.hedge_strike_resolver import resolve_hedge_strike

from features.mongo_data import MongoData
from features.sim_plans import normalize_execution_mode, resolve_user_plan
from features.telegram_notifier import notify_admin, notify_user

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

HOT_TICK_SECONDS = 1
WARM_REFRESH_TICKS = 5  # one warm refresh every 5 hot ticks (~5s)

# Regular-mode (execution_mode != "advanced") paper legs/baskets are only
# checked once every this many hot ticks (~30s at HOT_TICK_SECONDS=1) instead
# of every tick — Advanced-mode strategies stay on the full hot-tick cadence.
REGULAR_MODE_CHECK_TICKS = 30

# Auto square-off time on expiry day (IST "HH:MM") — positions whose leg expiry
# matches today are exited at this time via the risk monitor's warm refresh loop.
# On server startup, any legs whose expiry has ALREADY passed (missed 15:29 window)
# are also caught up and exited at entry price — see run_startup_expiry_catchup().
EXPIRY_SQUAREOFF_TIME = "15:29"

# Kill switch for the REAL BROKER call only — the monitor still starts/stops
# via the /simulator/risk-monitor/start|stop endpoints and still checks every
# leg/basket every second either way. While False, a hit is logged loudly
# (human-readable, every tick it stays hit) but no real order is placed and
# no trigger status changes — flip to True only after watching that log
# agree with what you expect, broker IP whitelisting included (see the Kite
# "IP not allowed" error this was added in response to).
AUTO_FIRE_ENABLED = False

# Paper strategies never call a broker — _fire_paper_adjustment/_fire_paper_exit
# only write to the simulator_strategy/simulator_adjustments Mongo collections,
# so they carry none of the real-money risk AUTO_FIRE_ENABLED above guards
# against. Deliberately a separate switch so paper auto-adjust can stay on
# while the live-broker kill switch stays off.
PAPER_AUTO_FIRE_ENABLED = True


def _now_iso() -> str:
    return datetime.now(IST).strftime('%Y-%m-%dT%H:%M:%S')


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_side(value: Any) -> str:
    """'B' -> 'BUY', 'S' -> 'SELL' — the alert-builder UI's Leg.side is the single-letter
    form; simulator_strategy.positions and the broker order leg model both expect the
    full word."""
    s = str(value or '').strip().upper()
    if s in ('B', 'BUY'):
        return 'BUY'
    if s in ('S', 'SELL'):
        return 'SELL'
    return s


def _normalize_option_type(value: Any) -> str:
    """'CE'/'Call'/'C' -> 'CALL', 'PE'/'Put'/'P' -> 'PUT' — simulator_strategy.positions
    stores the full word ('Call'/'Put') while simulator_adjustments.positions stores the
    option-chain abbreviation ('CE'/'PE'); adjustment-fire matching needs both forms equal."""
    s = str(value or '').strip().upper()
    if s in ('C', 'CE', 'CALL'):
        return 'CALL'
    if s in ('P', 'PE', 'PUT'):
        return 'PUT'
    return s


def _leg_risk_price(entry: float, side: str, mode: str, value: float, kind: str) -> float:
    """
    Mirrors PaperTradeNew.tsx::calcLegRiskPrice exactly, so the price that
    fires server-side always matches what the user saw on screen.
    side: 'BUY' / 'SELL'.  kind: 'sl' / 'tp'.
    """
    delta = (entry * value) / 100.0 if mode == 'percent' else value
    is_stoploss = kind == 'sl'
    above_entry = (side != 'BUY') if is_stoploss else (side == 'BUY')
    return entry + delta if above_entry else entry - delta


def _leg_signed_qty(leg: dict) -> int:
    qty = abs(_safe_int(leg.get('quantity')))
    return qty if str(leg.get('position') or '').upper() == 'BUY' else -qty


_MONTH_ABBR = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def _ordinal_suffix(day: int) -> str:
    if day % 10 == 1 and day % 100 != 11:
        return 'st'
    if day % 10 == 2 and day % 100 != 12:
        return 'nd'
    if day % 10 == 3 and day % 100 != 13:
        return 'rd'
    return 'th'


def _format_leg_label(leg: dict) -> str:
    """'B - 23rd Jun 25000 CE' — mirrors Positions.tsx's buildPositionLabel
    (ordinal day + short month + strike + option type), prefixed with the
    B/S side letter so a leg reads the same in the log as it does on screen."""
    side = str(leg.get('position') or '').upper()
    side_letter = 'B' if side == 'BUY' else 'S' if side == 'SELL' else '?'

    expiry_part = ''
    expiry_raw = str(leg.get('expiry_date') or '').strip()[:10]
    try:
        dt = datetime.strptime(expiry_raw, '%Y-%m-%d')
        expiry_part = f'{dt.day}{_ordinal_suffix(dt.day)} {_MONTH_ABBR[dt.month - 1]} '
    except ValueError:
        pass

    strike_part = str(leg.get('strike') or '')
    try:
        strike_val = float(leg.get('strike'))
        strike_part = str(int(strike_val)) if strike_val == int(strike_val) else f'{strike_val:.2f}'
    except (TypeError, ValueError):
        pass

    option_type = str(leg.get('option') or '')
    return f'{side_letter} - {expiry_part}{strike_part} {option_type}'.strip()


def _resolve_live_value(live_map: dict, key: str, cached_value: Any) -> tuple[float, bool]:
    """
    (value, is_live). Prefers the live WS tick; falls back to the value
    _fetch_dhan_broker_option_positions() already resolved at the last warm
    refresh (REST-quote/previous-close fallback baked in there — see its
    'ltp'/'spot_price' fields) — the same number Positions.tsx is showing.
    A thin/illiquid strike with no recent WS tick used to read as fully
    "missing" here even though the Positions page had a perfectly good
    (just up-to-~5s-old) value for it; this is why.
    """
    live_value = _safe_float(live_map.get(key))
    if live_value > 0:
        return live_value, True
    return _safe_float(cached_value), False


def _resolve_broker_label(raw_db, broker_id: str) -> str:
    """'<broker name> (<account holder>)' for log readability — same Dhan/
    FlatTrade/Kite collections the existing /simulator/positions/broker-status
    endpoint already reads (kite_market_config for Dhan, broker_configuration
    for FlatTrade/Kite), just resolved for one broker_id instead of listing all."""
    if not broker_id:
        return ''
    dhan_cfg = raw_db['kite_market_config'].find_one({'broker': 'dhan'}) or {}
    if broker_id == str(dhan_cfg.get('_id') or '').strip():
        user_name = str(dhan_cfg.get('user_name') or '').strip()
        return f'Dhan ({user_name})' if user_name else 'Dhan'
    try:
        from bson import ObjectId
        doc = raw_db['broker_configuration'].find_one({'_id': ObjectId(broker_id)})
    except Exception:
        doc = None
    if doc:
        name = str(doc.get('broker_name') or doc.get('name') or 'Broker').strip()
        user_name = str(doc.get('user_name') or '').strip()
        return f'{name} ({user_name})' if user_name else name
    return broker_id


class _RiskMonitorRegistry:
    """In-memory cache the hot loop reads; only the warm refresh writes to it."""

    def __init__(self) -> None:
        self.leg_by_token: dict[str, dict] = {}
        self.baskets_by_key: dict[tuple[str, str], list[dict]] = {}
        # Tokens with a fire already dispatched but not yet confirmed —
        # excluded from hot-tick evaluation so a broker round trip that
        # takes longer than one HOT_TICK_SECONDS cycle can't get the same
        # leg fired twice. Cleared (success or failure) when that fire
        # settles; see SimulatorRiskMonitor._fire_exit's finally block.
        self.in_flight: set[str] = set()
        # (broker_id, underlying) pairs with a Hedge Time Control entry/exit
        # order already dispatched but not yet confirmed — same "don't kick
        # off a second attempt mid-flight" guard as `in_flight` above, just
        # keyed per-basket since a hedge entry/exit is two orders, not one
        # token. Cleared (success or failure) in _check_hedge_time_control's
        # finally block.
        self.hedge_in_flight: set[tuple[str, str]] = set()
        self.broker_labels: dict[str, str] = {}
        # "Position Configuration" panel's basket-level Stoploss/Target/Trail
        # SL — keyed the same as baskets_by_key, refreshed alongside it.
        # Separate dict (not folded into baskets_by_key's legs) because it's
        # one doc per (broker_id, underlying), not per leg.
        self.alert_configs: dict[tuple[str, str], dict] = {}
        # Saved/virtual strategies (simulator_strategy, no broker_id/leg_id
        # at all) — kept in entirely separate dicts from the broker-position
        # ones above rather than merged in, since a paper leg's "fire" action
        # (mark exited in the strategy doc) is fundamentally different from a
        # broker leg's (place a real order) and must never be reachable by
        # accident through the broker path. Leg dicts are still shaped with
        # the exact same field names (position/option/expiry_date/strike/
        # quantity/entry_price/token/ltp) so every existing per-leg helper
        # (_check_leg_risk, _format_leg_label, _leg_signed_qty,
        # _resolve_live_value) works on them completely unchanged.
        self.paper_leg_by_token: dict[str, dict] = {}
        self.paper_baskets_by_strategy: dict[str, list[dict]] = {}
        self.paper_alert_configs: dict[str, dict] = {}
        # Adjustment docs from simulator_adjustments, keyed by
        # (broker_id, underlying, "<=" | ">=") for broker positions and
        # (strategy_id, "", "<=" | ">=") for paper strategies.
        # Loaded every warm refresh alongside baskets/alert_configs.
        self.adjustments: dict[tuple[str, str, str], dict] = {}
        # Adjustment fires in-flight — keyed (broker_id_or_sid, underlying_or_empty, side)
        # so a second tick can't re-fire the same side while orders are still settling.
        self.adjustment_in_flight: set[tuple[str, str, str]] = set()
        self.legs_watched = 0
        self.baskets_watched = 0

    def label_for(self, broker_id: str) -> str:
        return self.broker_labels.get(broker_id) or broker_id

    def replace_paper_strategy_slice(self, strategy_id: str, legs_with_risk: list[dict], basket_legs: list[dict] | None) -> None:
        for token, leg in list(self.paper_leg_by_token.items()):
            if str(leg.get('strategy_id') or '') == strategy_id:
                del self.paper_leg_by_token[token]
        for leg in legs_with_risk:
            token = str(leg.get('token') or '').strip()
            if token:
                self.paper_leg_by_token[token] = leg
        if basket_legs:
            self.paper_baskets_by_strategy[strategy_id] = basket_legs
        else:
            self.paper_baskets_by_strategy.pop(strategy_id, None)

    def replace_broker_slice(self, broker_id: str, legs_with_risk: list[dict], baskets: dict[tuple[str, str], list[dict]]) -> None:
        # Drop this broker's previous entries before re-adding the fresh ones,
        # so a leg that closed/lost its trigger since the last refresh
        # doesn't linger in the hot cache.
        for token, leg in list(self.leg_by_token.items()):
            if str(leg.get('broker_id') or '') == broker_id:
                del self.leg_by_token[token]
        for key in [k for k in self.baskets_by_key if k[0] == broker_id]:
            del self.baskets_by_key[key]

        for leg in legs_with_risk:
            token = str(leg.get('token') or '').strip()
            if token:
                self.leg_by_token[token] = leg
        for key, legs in baskets.items():
            if legs:
                self.baskets_by_key[key] = legs

        self.legs_watched = len(self.leg_by_token)
        self.baskets_watched = len(self.baskets_by_key)


class SimulatorRiskMonitor:
    """Start/stop controller — same shape as live_fast_monitor's supervisor."""

    def __init__(self) -> None:
        self._running = False
        self._task: asyncio.Task | None = None
        self._tick = 0
        self.started_at = ''
        self.stopped_at = ''
        self.last_tick_at = ''
        self.last_fire: dict[str, Any] = {}
        self.last_error = ''
        self.registry = _RiskMonitorRegistry()

    def is_running(self) -> bool:
        return bool(self._task and not self._task.done())

    def start(self) -> dict[str, Any]:
        if self.is_running():
            return self.get_status()
        self._running = True
        self.started_at = _now_iso()
        self.stopped_at = ''
        self.last_error = ''
        self._tick = 0
        self._task = asyncio.create_task(self._run())
        print(f'[SIMULATOR RISK MONITOR] started at {self.started_at}', flush=True)
        return self.get_status()

    def stop(self) -> dict[str, Any]:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None
        self.stopped_at = _now_iso()
        print(f'[SIMULATOR RISK MONITOR] stopped at {self.stopped_at}', flush=True)
        return self.get_status()

    def get_status(self) -> dict[str, Any]:
        return {
            'running': self.is_running(),
            'auto_fire_enabled': AUTO_FIRE_ENABLED,
            'paper_auto_fire_enabled': PAPER_AUTO_FIRE_ENABLED,
            'started_at': self.started_at,
            'stopped_at': self.stopped_at,
            'last_tick_at': self.last_tick_at,
            'legs_watched': self.registry.legs_watched,
            'baskets_watched': self.registry.baskets_watched,
            'paper_legs_watched': len(self.registry.paper_leg_by_token),
            'paper_baskets_watched': len(self.registry.paper_baskets_by_strategy),
            'last_fire': self.last_fire,
            'last_error': self.last_error,
        }

    async def _run(self) -> None:
        db = MongoData()
        try:
            while self._running:
                try:
                    if self._tick % WARM_REFRESH_TICKS == 0:
                        await self._warm_refresh(db)
                    self._hot_tick(db)
                except Exception as exc:
                    self.last_error = str(exc)
                    log.error('[SIMULATOR RISK MONITOR] cycle error: %s', exc)
                    # telegram_notifier's own 45s dedup window keeps a recurring
                    # cycle error from spamming admin every tick (this loop runs
                    # every HOT_TICK_SECONDS=1s).
                    notify_admin('RISK_MONITOR_CYCLE_ERROR', str(exc))
                self.last_tick_at = _now_iso()
                self._tick += 1
                await asyncio.sleep(HOT_TICK_SECONDS)
        except asyncio.CancelledError:
            pass
        finally:
            try:
                db.close()
            except Exception:
                pass

    # ── Warm refresh: the only place touching Mongo/broker ──────────────────

    def _active_broker_ids(self, db: MongoData) -> set[str]:
        raw_db = db._db
        ids: set[str] = set()
        for doc in raw_db['simulator_triggers'].find({'status': 'active'}, {'broker_id': 1}):
            bid = str(doc.get('broker_id') or '').strip()
            if bid:
                ids.add(bid)
        for doc in raw_db['simulator_portfolio_triggers'].find({'status': 'active'}, {'broker_id': 1}):
            bid = str(doc.get('broker_id') or '').strip()
            if bid:
                ids.add(bid)
        return ids

    async def _warm_refresh(self, db: MongoData) -> None:
        from features.execution_socket import _fetch_dhan_broker_option_positions

        broker_ids = self._active_broker_ids(db)
        for broker_id in broker_ids:
            try:
                self.registry.broker_labels[broker_id] = await asyncio.to_thread(
                    _resolve_broker_label, db._db, broker_id,
                )
            except Exception as exc:
                log.debug('[SIMULATOR RISK MONITOR] broker label lookup error broker=%s: %s', broker_id, exc)

            try:
                payload = await asyncio.to_thread(
                    _fetch_dhan_broker_option_positions,
                    db,
                    selected_broker_id=broker_id,
                    include_broker_status=False,
                )
            except Exception as exc:
                log.warning('[SIMULATOR RISK MONITOR] warm refresh error broker=%s: %s', broker_id, exc)
                continue

            open_positions = payload.get('open_positions') or []
            legs_with_risk = [p for p in open_positions if not p.get('exited') and p.get('risk')]

            open_underlyings = {
                str(p.get('underlying') or '').strip()
                for p in open_positions if not p.get('exited')
            }
            alert_config_by_underlying = await asyncio.to_thread(
                self._fetch_alert_configs, db, broker_id, open_underlyings,
            )

            baskets: dict[tuple[str, str], list[dict]] = {}
            for p in open_positions:
                if p.get('exited'):
                    continue
                underlying = str(p.get('underlying') or '').strip()
                # Tracked if EITHER the spot-price-band marker (portfolio_risk,
                # attached by _fetch_dhan_broker_option_positions itself) or
                # this new MTM-based alert config is active — a basket can
                # have just one of the two configured.
                if not p.get('portfolio_risk') and underlying not in alert_config_by_underlying:
                    continue
                key = (broker_id, underlying)
                baskets.setdefault(key, []).append(p)

            self.registry.replace_broker_slice(broker_id, legs_with_risk, baskets)
            for key in [k for k in self.registry.alert_configs if k[0] == broker_id]:
                del self.registry.alert_configs[key]
            for underlying, config in alert_config_by_underlying.items():
                self.registry.alert_configs[(broker_id, underlying)] = config

            # Load adjustment docs for all open underlyings
            for key in [k for k in self.registry.adjustments if k[0] == broker_id]:
                del self.registry.adjustments[key]
            try:
                adj_docs = await asyncio.to_thread(
                    self._fetch_adjustment_docs_broker, db, broker_id, open_underlyings,
                )
                for (underlying, condition), doc in adj_docs.items():
                    self.registry.adjustments[(broker_id, underlying, condition)] = doc
            except Exception as exc:
                log.debug('[SIMULATOR RISK MONITOR] adjustment load error broker=%s: %s', broker_id, exc)

        # Brokers that no longer have any active trigger at all must still be
        # evicted even though the loop above never touches them. (Parens
        # matter: set `-` binds tighter than `|` in Python, unlike `and`/`or`.)
        cached_brokers = {str(leg.get('broker_id') or '') for leg in self.registry.leg_by_token.values()}
        cached_brokers |= {k[0] for k in self.registry.baskets_by_key}
        for broker_id in (cached_brokers - broker_ids):
            self.registry.replace_broker_slice(broker_id, [], {})
            for key in [k for k in self.registry.alert_configs if k[0] == broker_id]:
                del self.registry.alert_configs[key]
            for key in [k for k in self.registry.adjustments if k[0] == broker_id]:
                del self.registry.adjustments[key]

        await self._warm_refresh_paper_strategies(db)
        await self._poll_executing_adjustments(db)
        await self._auto_squareoff_expired_legs(db)

    async def _warm_refresh_paper_strategies(self, db: MongoData) -> None:
        """
        Saved/virtual strategies (simulator_strategy) with a per-leg SL/TP
        set on any position, or an active basket-level alert_* config —
        candidates fetched with one query (never one per strategy). Each
        candidate's positions get the exact same token + current_ltp
        resolution Positions.tsx/the /trade/:strategyId page already see
        (lazy-imported _enrich_pt_strategy_positions, api.py — same function
        the duplicate-endpoint bug fix earlier this session was about), so
        this monitor never disagrees with what's on screen.
        """
        raw_db = db._db
        cursor = raw_db['simulator_strategy'].find({
            '$or': [
                {'alert_status': 'active'},
                {'positions.sl_value': {'$gt': 0}},
                {'positions.tp_value': {'$gt': 0}},
                {'sl_marker_status': 'active'},
            ],
        })
        candidate_ids: set[str] = set()
        for doc in cursor:
            strategy_id = str(doc.get('_id') or '')
            if not strategy_id:
                continue
            plan = resolve_user_plan(doc.get('user_id'))
            if not plan.get('auto_position_management', True):
                # Plan doesn't allow automated exits (e.g. Free) — never watch
                # this strategy for auto-fire; user must close manually. Not
                # added to candidate_ids, so the eviction loop below drops it
                # from the hot-tick registry if it was watched before (e.g.
                # the user's plan was downgraded since the last warm refresh).
                continue
            candidate_ids.add(strategy_id)
            try:
                await self._warm_refresh_one_paper_strategy(db, strategy_id, doc)
            except Exception as exc:
                log.warning('[SIMULATOR RISK MONITOR] paper strategy warm refresh error strategy=%s: %s', strategy_id, exc)

        for strategy_id in list(self.registry.paper_baskets_by_strategy.keys()) + [
            str(leg.get('strategy_id') or '') for leg in self.registry.paper_leg_by_token.values()
        ]:
            if strategy_id and strategy_id not in candidate_ids:
                self.registry.replace_paper_strategy_slice(strategy_id, [], None)
                self.registry.paper_alert_configs.pop(strategy_id, None)

    async def _warm_refresh_one_paper_strategy(self, db: MongoData, strategy_id: str, doc: dict) -> None:
        from api import _enrich_pt_strategy_positions  # type: ignore  (lazy: api.py imports this module's package at startup)

        enriched = await asyncio.to_thread(_enrich_pt_strategy_positions, doc)
        underlying = str(enriched.get('instrument') or '').strip().upper()
        execution_mode = normalize_execution_mode(doc.get('execution_mode'))

        legs_with_risk: list[dict] = []
        basket_legs: list[dict] = []
        for index, pos in enumerate(enriched.get('positions') or []):
            if not isinstance(pos, dict) or pos.get('exited'):
                continue
            side = 'SELL' if str(pos.get('type') or '').strip().upper() == 'SELL' else 'BUY'
            option_type = 'PE' if str(pos.get('option_type') or '').strip().upper().startswith('P') else 'CE'
            sl_value = _safe_float(pos.get('sl_value'))
            tp_value = _safe_float(pos.get('tp_value'))
            leg = {
                'strategy_id': strategy_id,
                'strategy_name': str(enriched.get('strategy_name') or ''),
                'execution_mode': execution_mode,
                'position_index': index,
                'underlying': underlying,
                'leg_id': f'paper:{strategy_id}:{index}',
                'position': side,
                'option': option_type,
                'strike': pos.get('strike'),
                'expiry_date': str(pos.get('expiry') or ''),
                'entry_price': pos.get('entry_price'),
                'quantity': pos.get('quantity'),
                'token': str(pos.get('token') or '').strip(),
                'ltp': pos.get('current_ltp'),
                # Flat (not nested) so the basket-level CHECK print can show every leg's
                # own SL/TP inline, even legs with none configured (sl_value stays 0).
                'sl_mode': pos.get('sl_mode'), 'sl_value': sl_value,
                'tp_mode': pos.get('tp_mode'), 'tp_value': tp_value,
            }
            if sl_value > 0 or tp_value > 0:
                legs_with_risk.append({
                    **leg,
                    'risk': {
                        'sl_mode': pos.get('sl_mode'), 'sl_value': sl_value,
                        'tp_mode': pos.get('tp_mode'), 'tp_value': tp_value,
                    },
                })
            basket_legs.append(leg)

        # Two independent basket-level features can each need this strategy's
        # basket watched — the MTM Stoploss/Target (Position Configuration
        # panel, alert_status) and the payoff chart's spot-price-band marker
        # (sl_upper/sl_lower, sl_marker_status) — same independence the live
        # broker-position feature keeps between its own 'status'/'alert_status'
        # fields on simulator_portfolio_triggers, just both on this one doc.
        has_alert = enriched.get('alert_status') == 'active'
        has_sl_marker = enriched.get('sl_marker_status') == 'active' and (
            enriched.get('sl_upper') is not None or enriched.get('sl_lower') is not None
        )
        needs_basket = (has_alert or has_sl_marker) and basket_legs

        if needs_basket and has_sl_marker:
            try:
                from features.execution_socket import _fetch_dhan_index_quotes
                quotes = await asyncio.to_thread(_fetch_dhan_index_quotes, db, {underlying})
                spot_price = _safe_float((quotes.get(underlying) or {}).get('spot_price'))
            except Exception as exc:
                log.debug('[SIMULATOR RISK MONITOR] paper strategy spot price error strategy=%s: %s', strategy_id, exc)
                spot_price = 0.0
            portfolio_risk = {'sl_upper': enriched.get('sl_upper'), 'sl_lower': enriched.get('sl_lower')}
            for leg in basket_legs:
                leg['spot_price'] = spot_price
                leg['portfolio_risk'] = portfolio_risk

        self.registry.replace_paper_strategy_slice(strategy_id, legs_with_risk, basket_legs if needs_basket else None)
        if has_alert:
            self.registry.paper_alert_configs[strategy_id] = {
                'alert_trading_mode': enriched.get('alert_trading_mode'),
                'alert_stoploss': enriched.get('alert_stoploss'),
                'alert_target': enriched.get('alert_target'),
                'alert_trailing_stop': enriched.get('alert_trailing_stop'),
                'alert_peak_mtm': enriched.get('alert_peak_mtm'),
                'alert_legs_snapshot': [
                    {'entry_price': leg['entry_price'], 'quantity': leg['quantity']} for leg in basket_legs
                ],
            }
        else:
            self.registry.paper_alert_configs.pop(strategy_id, None)

        # Load upper/lower adjustment docs for this paper strategy
        for key in [k for k in self.registry.adjustments if k[0] == strategy_id]:
            del self.registry.adjustments[key]
        if has_sl_marker:
            try:
                adj_by_condition = await asyncio.to_thread(
                    self._fetch_adjustment_docs_paper, db, strategy_id,
                )
                for condition, doc in adj_by_condition.items():
                    self.registry.adjustments[(strategy_id, '', condition)] = doc
            except Exception as exc:
                log.debug('[SIMULATOR RISK MONITOR] paper adjustment load error strategy=%s: %s', strategy_id, exc)

    @staticmethod
    def _fetch_alert_configs(db: MongoData, broker_id: str, underlyings: set[str]) -> dict[str, dict]:
        """One batched query for every underlying this broker currently has
        open legs in — never one query per underlying (same convention as
        the rest of this module)."""
        if not underlyings:
            return {}
        raw_db = db._db
        result: dict[str, dict] = {}
        for doc in raw_db['simulator_portfolio_triggers'].find({
            'broker_id': broker_id,
            'underlying': {'$in': list(underlyings)},
            'alert_status': 'active',
        }):
            underlying = str(doc.get('underlying') or '').strip()
            if underlying:
                result[underlying] = doc
        return result

    @staticmethod
    def _fetch_adjustment_docs_broker(db: MongoData, broker_id: str, underlyings: set[str]) -> dict[tuple[str, str], dict]:
        """Load simulator_adjustments for all (underlying, condition) pairs this broker has open."""
        if not underlyings:
            return {}
        result: dict[tuple[str, str], dict] = {}
        for doc in db._db['simulator_adjustments'].find({
            'broker_id': broker_id,
            'underlying': {'$in': list(underlyings)},
            'status': {'$ne': False},
        }):
            underlying = str(doc.get('underlying') or '').strip()
            condition = str(doc.get('trigger_condition') or '').strip()
            if underlying and condition:
                result[(underlying, condition)] = doc
        return result

    @staticmethod
    def _fetch_adjustment_docs_paper(db: MongoData, strategy_id: str) -> dict[str, dict]:
        """Load simulator_adjustments for a paper strategy — keyed by trigger_condition."""
        result: dict[str, dict] = {}
        for doc in db._db['simulator_adjustments'].find({'strategy_id': strategy_id, 'status': {'$ne': False}}):
            condition = str(doc.get('trigger_condition') or '').strip()
            if condition:
                result[condition] = doc
        return result

    def _resolve_trading_mode(self, broker_id: str, underlying: str) -> str:
        """'alert_only' -> Telegram-notify on a hit instead of firing a real
        order, regardless of AUTO_FIRE_ENABLED (that switch is specifically
        about real-money order risk; a notification carries none). Default
        'auto' preserves today's behavior for legs/baskets with no Position
        Configuration trading_mode saved yet."""
        cfg = self.registry.alert_configs.get((broker_id, underlying)) or {}
        return str(cfg.get('alert_trading_mode') or 'auto')

    def _resolve_paper_trading_mode(self, strategy_id: str) -> str:
        cfg = self.registry.paper_alert_configs.get(strategy_id) or {}
        return str(cfg.get('alert_trading_mode') or 'auto')

    # ── Hot tick: pure in-memory, no I/O ─────────────────────────────────────

    def _hot_tick(self, db: MongoData) -> None:
        from features.dhan_ticker import dhan_ticker_manager

        ltp_map = dhan_ticker_manager.ltp_map or {}
        in_flight = self.registry.in_flight
        fired_legs: list[dict] = []
        now_str = _now_iso()

        if not self.registry.leg_by_token and not self.registry.baskets_by_key:
            print(f'[SIMULATOR RISK MONITOR] {now_str} — nothing to watch (no active SL/Target on an open leg yet)', flush=True)

        for token, leg in list(self.registry.leg_by_token.items()):
            leg_id = str(leg.get('leg_id') or '')
            leg_label = _format_leg_label(leg)
            if token in in_flight:
                print(f'[SIMULATOR RISK MONITOR] {now_str} leg_id={leg_id} {leg.get("underlying")} {leg_label} — fire in flight, skipping', flush=True)
                continue
            ltp, ltp_is_live = _resolve_live_value(ltp_map, token, leg.get('ltp'))
            risk = leg.get('risk') or {}
            entry = _safe_float(leg.get('entry_price'))
            side = str(leg.get('position') or '').upper()
            broker_label = self.registry.label_for(str(leg.get('broker_id') or ''))

            hit_reason, sl_price, tp_price = self._check_leg_risk(entry, side, ltp, risk)
            # Distance is "how many points of room are left", signed so a BUY
            # and a SELL both read the same way: positive = still safe/still
            # short of target, zero/negative = already there. The % alongside
            # it is "remaining room as a % of the ORIGINAL entry->SL (or
            # entry->TP) budget" — NOT a % of current LTP. A % of current LTP
            # looked plausible but doesn't relate back to the configured
            # sl_value/tp_value at all (e.g. a 50%-of-entry SL doesn't read
            # back as "~50%" against LTP once price has already moved part
            # way there) — this one does: it reads ~100% right after entry,
            # heads to 0% exactly as SL fires, and a TP that price has moved
            # away from (rather than toward) reads above 100%.
            # Signed "moved toward SL/TP" (not abs(entry-ltp)) — positive
            # means the price actually moved in that direction since entry,
            # negative means it moved the other way (room_left then exceeds
            # 100%, correctly). Spelled out here so the % can be
            # hand-verified straight off this one line.
            moved_toward_sl = (entry - ltp) if side == 'BUY' else (ltp - entry)
            moved_toward_tp = (ltp - entry) if side == 'BUY' else (entry - ltp)

            sl_part = 'sl=none'
            if sl_price is not None:
                sl_dist = (ltp - sl_price) if side == 'BUY' else (sl_price - ltp)
                sl_budget = abs(entry - sl_price)
                sl_dist_pct = (sl_dist / sl_budget * 100.0) if sl_budget > 0 else 0.0
                sl_part = (
                    f'sl={risk.get("sl_mode")}:{risk.get("sl_value")} entry={entry:.2f} sl_price={sl_price:.2f} '
                    f'(budget={sl_budget:.2f}pts moved_toward_sl={moved_toward_sl:.2f}pts room_left={sl_dist_pct:.2f}%)'
                )
            tp_part = 'tp=none'
            if tp_price is not None:
                tp_dist = (tp_price - ltp) if side == 'BUY' else (ltp - tp_price)
                tp_budget = abs(tp_price - entry)
                tp_dist_pct = (tp_dist / tp_budget * 100.0) if tp_budget > 0 else 0.0
                tp_part = (
                    f'tp={risk.get("tp_mode")}:{risk.get("tp_value")} entry={entry:.2f} tp_price={tp_price:.2f} '
                    f'(budget={tp_budget:.2f}pts moved_toward_tp={moved_toward_tp:.2f}pts room_left={tp_dist_pct:.2f}%)'
                )

            print(
                f'[SIMULATOR RISK MONITOR] {now_str} CHECK leg_id={leg_id} broker={broker_label} '
                f'{leg.get("underlying")} {leg_label} '
                f'entry={entry:.2f} ltp={ltp:.2f}{"" if ltp_is_live else "(cached)"}  {sl_part}  {tp_part}'
                + ('' if ltp > 0 else '  [no ltp available — neither live tick nor cached]'),
                flush=True,
            )
            if ltp <= 0:
                continue
            if hit_reason:
                pnl = (ltp - entry) * _leg_signed_qty(leg)
                label = 'STOPLOSS' if hit_reason == 'stoploss' else 'TARGET'
                breached_price = sl_price if hit_reason == 'stoploss' else tp_price
                trading_mode = self._resolve_trading_mode(str(leg.get('broker_id') or ''), str(leg.get('underlying') or ''))
                print(
                    '[SIMULATOR RISK MONITOR] '
                    + ('\U0001F534' if hit_reason == 'stoploss' else '\U0001F7E2')
                    + f' {label} HIT'
                    + (' — ALERT ONLY mode, Telegram notified, no order placed' if trading_mode == 'alert_only'
                       else '' if AUTO_FIRE_ENABLED else ' — auto-fire DISABLED, no order placed')
                    + f'\n    Broker     : {broker_label}'
                    + f'\n    Leg ID     : {leg_id}'
                    + f'\n    Instrument : {leg.get("underlying")} {leg_label}'
                    + f'\n    Entry      : {entry:.2f}   Current LTP: {ltp:.2f}   Leg P&L: {pnl:.2f}'
                    + f'\n    {label} setting: {risk.get("sl_mode") if hit_reason == "stoploss" else risk.get("tp_mode")} '
                    + f'{risk.get("sl_value") if hit_reason == "stoploss" else risk.get("tp_value")}'
                    + f'  ->  trigger price {breached_price:.2f}',
                    flush=True,
                )
                if trading_mode == 'alert_only':
                    notify_user(
                        f'PT_LEG_{label}',
                        f'{leg.get("underlying")} {leg_label} {label} hit (entry {entry:.2f}, LTP {ltp:.2f}, P&L {pnl:.2f})',
                        {'trade_id': leg_id, 'leg_id': leg_id, 'broker': broker_label},
                    )
                elif AUTO_FIRE_ENABLED:
                    fired_legs.append({**leg, '_ltp': ltp, '_reason': hit_reason, '_scope': 'leg'})

        for (broker_id, underlying), legs in list(self.registry.baskets_by_key.items()):
            broker_label = self.registry.label_for(broker_id)
            if any(str(leg.get('token') or '') in in_flight for leg in legs):
                # A leg in this basket already has a fire in flight —
                # skip the whole basket this tick rather than summing a
                # net PnL that mixes a settling leg with live ones.
                print(f'[SIMULATOR RISK MONITOR] {now_str} basket broker={broker_label} {underlying} — fire in flight, skipping', flush=True)
                continue
            # net_pnl is shown for context only — the saved marker is a band
            # on the underlying's own spot price (drawn on the payoff
            # chart's price axis, see PaperTradeNew.tsx's slSavedUpper/
            # slSavedLower + priceToX), NOT a P&L threshold. Comparing P&L
            # (tens/hundreds) against a 5-digit index price would make
            # "stoploss" fire almost immediately on any negative P&L —
            # that was the bug behind the first version of this check.
            net_pnl = 0.0
            missing_ltp = False
            any_cached_ltp = False
            for leg in legs:
                token = str(leg.get('token') or '').strip()
                ltp, ltp_is_live = _resolve_live_value(ltp_map, token, leg.get('ltp'))
                any_cached_ltp = any_cached_ltp or not ltp_is_live
                if ltp <= 0:
                    missing_ltp = True
                    break
                net_pnl += (ltp - _safe_float(leg.get('entry_price'))) * _leg_signed_qty(leg)
            # Spot price comes ONLY from the cached warm-refresh value here —
            # NOT dhan_ticker_manager.spot_map. That ticker dict was a second,
            # independent source of "current spot" disagreeing with the one
            # canonical source (features.execution_socket
            # ._fetch_dhan_index_quotes, reused by api.py's
            # /live-greeks-chain and by _fetch_dhan_broker_option_positions
            # — see those for the full story); the cached value here already
            # traces back to that same canonical source via this module's own
            # warm refresh, so there is nothing live-but-different left to
            # prefer it over.
            spot_price = _safe_float(legs[0].get('spot_price') if legs else 0)
            spot_is_live = False
            portfolio_risk = (legs[0].get('portfolio_risk') or {}) if legs else {}

            # This is the "upper and lower stoploss checking status" — spot
            # vs each saved boundary, both in points and as a % of spot, same
            # shape as the per-leg sl/tp line above. dist > 0 on both sides
            # means spot is still inside the band (safe); a hit means spot
            # has reached/crossed whichever side it's closest to.
            lower_part = 'sl_lower=none'
            sl_lower = portfolio_risk.get('sl_lower')
            if sl_lower is not None and spot_price > 0:
                lower_val = _safe_float(sl_lower)
                lower_dist = spot_price - lower_val
                lower_dist_pct = (lower_dist / spot_price * 100.0)
                lower_part = f'sl_lower={lower_val:.2f} (dist={lower_dist:.2f}, {lower_dist_pct:.2f}%)'
            upper_part = 'sl_upper=none'
            sl_upper = portfolio_risk.get('sl_upper')
            if sl_upper is not None and spot_price > 0:
                upper_val = _safe_float(sl_upper)
                upper_dist = upper_val - spot_price
                upper_dist_pct = (upper_dist / spot_price * 100.0)
                upper_part = f'sl_upper={upper_val:.2f} (dist={upper_dist:.2f}, {upper_dist_pct:.2f}%)'

            legs_desc = ', '.join(f'{_format_leg_label(l)} [leg_id={l.get("leg_id")}]' for l in legs)
            print(
                f'[SIMULATOR RISK MONITOR] {now_str} CHECK basket broker={broker_label} {underlying} '
                f'legs=[{legs_desc}] spot={spot_price:.2f}{"" if spot_is_live else "(cached)"} '
                f'net_pnl={net_pnl:.2f}{"" if not any_cached_ltp else " (uses cached ltp for at least one leg)"}'
                f'  {lower_part}  {upper_part}'
                + ('  [missing ltp — no live tick or cached value for at least one leg]' if missing_ltp else '')
                + ('  [missing spot — no live tick or cached value]' if spot_price <= 0 else ''),
                flush=True,
            )
            if missing_ltp:
                continue

            if spot_price > 0:
                hit_reason = self._check_basket_risk(spot_price, portfolio_risk)
                if hit_reason:
                    # hit_reason is 'lower_breach' or 'upper_breach'
                    fired_condition = '<=' if hit_reason == 'lower_breach' else '>='
                    reset_condition  = '>=' if hit_reason == 'lower_breach' else '<='
                    adj_key  = (broker_id, underlying, fired_condition)
                    adj_doc  = self.registry.adjustments.get(adj_key)
                    trading_mode = self._resolve_trading_mode(broker_id, underlying)
                    label = 'LOWER BREACH' if hit_reason == 'lower_breach' else 'UPPER BREACH'

                    print(
                        '[SIMULATOR RISK MONITOR] '
                        + '\U0001F534'
                        + f' {label} HIT'
                        + (' — ALERT ONLY mode, Telegram notified, no order placed' if trading_mode == 'alert_only'
                           else '' if AUTO_FIRE_ENABLED else ' — auto-fire DISABLED, no order placed')
                        + f'\n    Broker     : {broker_label}'
                        + f'\n    Underlying : {underlying}'
                        + f'\n    Spot price : {spot_price:.2f}   SL Lower: {portfolio_risk.get("sl_lower")}   SL Upper: {portfolio_risk.get("sl_upper")}'
                        + f'\n    Adjustment positions: {len((adj_doc or {}).get("positions") or [])}',
                        flush=True,
                    )

                    if trading_mode == 'alert_only':
                        notify_user(
                            f'PT_ADJ_{label.replace(" ", "_")}',
                            f'{underlying} {label} hit (spot {spot_price:.2f}) — '
                            + (f'{len(adj_doc["positions"])} adjustment orders would execute' if adj_doc else 'no adjustment orders configured'),
                            {'trade_id': f'{broker_id}:{underlying}', 'leg_id': '', 'broker': broker_label},
                        )
                    elif AUTO_FIRE_ENABLED:
                        if adj_key not in self.registry.adjustment_in_flight:
                            asyncio.create_task(self._fire_broker_adjustment(
                                db, broker_id, underlying, broker_label,
                                fired_condition, reset_condition, adj_doc,
                            ))

            # ── "Position Configuration" panel's MTM Stoploss/Target/Trail SL ──
            # Independent of the spot-price-band check above — different
            # signal (basket net P&L, not underlying spot price), saved via a
            # different UI action (Add/Update Alert, not the payoff-chart
            # marker), so it's checked and fired separately even though both
            # live on the same simulator_portfolio_triggers doc.
            alert_config = self.registry.alert_configs.get((broker_id, underlying))
            if alert_config:
                mtm_reason = self._check_mtm_alert_config(
                    f'broker={broker_label} {underlying}', net_pnl, alert_config,
                    lambda peak, _b=broker_id, _u=underlying: asyncio.create_task(self._persist_peak_mtm(db, _b, _u, peak)),
                )['hit_reason']
                if mtm_reason:
                    label = 'MTM STOPLOSS' if mtm_reason == 'mtm_stoploss' else 'MTM TARGET'
                    legs_desc = ', '.join(f'{_format_leg_label(l)} [leg_id={l.get("leg_id")}]' for l in legs)
                    trading_mode = self._resolve_trading_mode(broker_id, underlying)
                    print(
                        '[SIMULATOR RISK MONITOR] '
                        + ('\U0001F534' if mtm_reason == 'mtm_stoploss' else '\U0001F7E2')
                        + f' {label} HIT'
                        + (' — ALERT ONLY mode, Telegram notified, no order placed' if trading_mode == 'alert_only'
                           else '' if AUTO_FIRE_ENABLED else ' — auto-fire DISABLED, no order placed')
                        + f'\n    Broker     : {broker_label}'
                        + f'\n    Underlying : {underlying}   Legs ({len(legs)}): {legs_desc}'
                        + f'\n    Net P&L    : {net_pnl:.2f}',
                        flush=True,
                    )
                    if trading_mode == 'alert_only':
                        notify_user(
                            f'PT_{label.replace(" ", "_")}',
                            f'{underlying} {label} hit (net P&L {net_pnl:.2f}) — legs: {legs_desc}',
                            {'trade_id': f'{broker_id}:{underlying}', 'leg_id': '', 'broker': broker_label},
                        )
                    elif AUTO_FIRE_ENABLED:
                        for leg in legs:
                            fired_legs.append({**leg, '_reason': mtm_reason, '_scope': 'mtm_basket', '_net_pnl': net_pnl})

            # ── Hedge Time Control — auto place/exit the basket's protective
            # BUY CE + BUY PE at the configured entry_time/exit_time. Always
            # safe to call every tick: the date+in-flight guards inside make
            # it a no-op except in the actual entry/exit minute windows.
            if alert_config:
                asyncio.create_task(self._check_hedge_time_control(db, broker_id, underlying, broker_label, alert_config))

        # ── Saved/virtual strategies (simulator_strategy) ──────────────────
        # Same per-leg check as broker legs above (_check_leg_risk is generic
        # over the leg dict shape, not what kind of leg it is) — just a
        # different fire action (paper exit, no broker call) on a hit.
        for token, leg in list(self.registry.paper_leg_by_token.items()):
            if token in in_flight:
                continue
            if leg.get('execution_mode') != 'advanced' and self._tick % REGULAR_MODE_CHECK_TICKS != 0:
                # Regular-mode strategies only get SL/Target evaluated every
                # ~30s (REGULAR_MODE_CHECK_TICKS hot ticks); Advanced-mode
                # stays on the full HOT_TICK_SECONDS cadence below.
                continue
            ltp, ltp_is_live = _resolve_live_value(ltp_map, token, leg.get('ltp'))
            if ltp <= 0:
                continue
            risk = leg.get('risk') or {}
            entry = _safe_float(leg.get('entry_price'))
            side = str(leg.get('position') or '').upper()
            hit_reason, sl_price, tp_price = self._check_leg_risk(entry, side, ltp, risk)
            leg_trading_mode = self._resolve_paper_trading_mode(str(leg.get('strategy_id') or ''))
            print(
                f'[SIMULATOR RISK MONITOR] {now_str} CHECK paper_leg strategy={leg.get("strategy_id")} '
                f'idx={leg.get("position_index")} {leg.get("underlying")} trade_mode={leg_trading_mode} {_format_leg_label(leg)} '
                f'entry={entry:.2f} ltp={ltp:.2f}{"" if ltp_is_live else "(cached)"} '
                f'sl={risk.get("sl_mode")}:{risk.get("sl_value")} tp={risk.get("tp_mode")}:{risk.get("tp_value")}',
                flush=True,
            )
            if hit_reason:
                label = 'STOPLOSS' if hit_reason == 'stoploss' else 'TARGET'
                breached_price = sl_price if hit_reason == 'stoploss' else tp_price
                trading_mode = leg_trading_mode
                print(
                    '[SIMULATOR RISK MONITOR] '
                    + ('\U0001F534' if hit_reason == 'stoploss' else '\U0001F7E2')
                    + f' PAPER {label} HIT'
                    + (' — ALERT ONLY mode, Telegram notified, no exit recorded' if trading_mode == 'alert_only'
                       else '' if PAPER_AUTO_FIRE_ENABLED else ' — auto-fire DISABLED, no exit recorded')
                    + f'\n    Strategy   : {leg.get("strategy_id")} (position #{leg.get("position_index")})'
                    + f'\n    Instrument : {leg.get("underlying")} {_format_leg_label(leg)}'
                    + f'\n    Entry      : {entry:.2f}   Current LTP: {ltp:.2f}'
                    + (f'  ->  trigger price {breached_price:.2f}' if breached_price is not None else ''),
                    flush=True,
                )
                if trading_mode == 'alert_only':
                    notify_user(
                        f'PT_PAPER_LEG_{label}',
                        f'{leg.get("underlying")} {_format_leg_label(leg)} {label} hit (entry {entry:.2f}, LTP {ltp:.2f})',
                        {'trade_id': str(leg.get('strategy_id') or ''), 'leg_id': str(leg.get('leg_id') or '')},
                    )
                elif PAPER_AUTO_FIRE_ENABLED:
                    fired_legs.append({**leg, '_ltp': ltp, '_reason': hit_reason, '_scope': 'paper_leg'})

        for strategy_id, legs in list(self.registry.paper_baskets_by_strategy.items()):
            if any(str(leg.get('token') or '') in in_flight for leg in legs):
                continue
            basket_mode = legs[0].get('execution_mode') if legs else 'advanced'
            if basket_mode != 'advanced' and self._tick % REGULAR_MODE_CHECK_TICKS != 0:
                continue
            net_pnl = 0.0
            missing_ltp = False
            # Captured so a basket-scope fire below can record each leg's
            # actual exit price — _fire_paper_exit reads leg['_ltp'], same
            # key the per-leg STOPLOSS/TARGET hit path already sets.
            leg_ltp_by_token: dict[str, float] = {}
            for leg in legs:
                token = str(leg.get('token') or '').strip()
                ltp, _ = _resolve_live_value(ltp_map, token, leg.get('ltp'))
                leg_ltp_by_token[token] = ltp
                if ltp <= 0:
                    missing_ltp = True
                    break
                net_pnl += (ltp - _safe_float(leg.get('entry_price'))) * _leg_signed_qty(leg)
            legs_desc = ', '.join(f'{_format_leg_label(l)} [#{l.get("position_index")}]' for l in legs)
            # portfolio_risk/spot_price are only attached during warm refresh
            # when the payoff chart's marker (sl_marker_status) is active —
            # absent whenever this basket is only here for the MTM alert
            # below, same "a basket can have just one of the two configured"
            # as the live broker-position equivalent.
            portfolio_risk = (legs[0].get('portfolio_risk') or {}) if legs else {}
            spot_price = _safe_float(legs[0].get('spot_price') if legs else 0)
            underlying = str(legs[0].get('underlying') or '') if legs else ''
            strategy_name = str(legs[0].get('strategy_name') or '') if legs else ''
            paper_trading_mode = self._resolve_paper_trading_mode(strategy_id)

            # One readable block per strategy per tick instead of a dense single
            # line — strategy/instrument/trade-mode header, every leg's own
            # strike/entry/ltp/SL/TP, the spot-price adjustment band, then the
            # basket-level MTM SL/Target — everything in one place to read.
            lower_part = 'lower=none'
            sl_lower = portfolio_risk.get('sl_lower')
            if sl_lower is not None and spot_price > 0:
                lower_val = _safe_float(sl_lower)
                lower_dist = spot_price - lower_val
                lower_part = f'lower={lower_val:.2f} (room {lower_dist:.2f}pts / {lower_dist / spot_price * 100.0:.2f}%)'
            upper_part = 'upper=none'
            sl_upper = portfolio_risk.get('sl_upper')
            if sl_upper is not None and spot_price > 0:
                upper_val = _safe_float(sl_upper)
                upper_dist = upper_val - spot_price
                upper_part = f'upper={upper_val:.2f} (room {upper_dist:.2f}pts / {upper_dist / spot_price * 100.0:.2f}%)'

            leg_lines = []
            for l in legs:
                l_sl_value = _safe_float(l.get('sl_value'))
                l_tp_value = _safe_float(l.get('tp_value'))
                l_ltp, _ = _resolve_live_value(ltp_map, str(l.get('token') or '').strip(), l.get('ltp'))
                sl_text = f'{l.get("sl_mode")}:{l_sl_value}' if l_sl_value > 0 else 'none'
                tp_text = f'{l.get("tp_mode")}:{l_tp_value}' if l_tp_value > 0 else 'none'
                leg_lines.append(
                    f'      #{l.get("position_index")} {_format_leg_label(l)}  '
                    f'entry={_safe_float(l.get("entry_price")):.2f}  ltp={l_ltp:.2f}  '
                    f'leg_SL={sl_text}  leg_TP={tp_text}'
                )

            alert_config = self.registry.paper_alert_configs.get(strategy_id)
            mtm_part = 'Overall MTM     : not configured'
            mtm_check: dict | None = None
            if alert_config:
                mtm_check = self._check_mtm_alert_config(
                    f'strategy={strategy_id}', net_pnl, alert_config,
                    lambda peak, _sid=strategy_id: asyncio.create_task(self._persist_peak_mtm_strategy(db, _sid, peak)),
                    verbose=False,
                )
                sl_t = mtm_check['sl_threshold']
                tgt_t = mtm_check['target_threshold']
                mtm_part = (
                    f'Overall MTM     : net_pnl={net_pnl:.2f}  '
                    f'SL={f"-{sl_t:.2f}" if sl_t is not None else "none"}  '
                    f'Target={f"{tgt_t:.2f}" if tgt_t is not None else "none"}  '
                    f'peak={mtm_check["peak_mtm"]:.2f}'
                )

            print(
                f'[SIMULATOR RISK MONITOR] {now_str} ── PAPER STRATEGY {strategy_name or strategy_id} ── '
                f'underlying={underlying}  trade_mode={paper_trading_mode}'
                + ('  [missing ltp]' if missing_ltp else '') + '\n'
                + '\n'.join(leg_lines) + '\n'
                + f'      Adjustment Band : spot={spot_price:.2f}  {lower_part}  {upper_part}\n'
                + f'      {mtm_part}',
                flush=True,
            )
            if missing_ltp:
                continue

            if portfolio_risk and spot_price > 0:
                hit_reason = self._check_basket_risk(spot_price, portfolio_risk)
                if hit_reason:
                    fired_condition = '<=' if hit_reason == 'lower_breach' else '>='
                    reset_condition  = '>=' if hit_reason == 'lower_breach' else '<='
                    adj_key = (strategy_id, '', fired_condition)
                    adj_doc = self.registry.adjustments.get(adj_key)
                    label = 'PAPER LOWER BREACH' if hit_reason == 'lower_breach' else 'PAPER UPPER BREACH'

                    print(
                        '[SIMULATOR RISK MONITOR] '
                        + '\U0001F534'
                        + f' {label} HIT'
                        + (' — ALERT ONLY mode, Telegram notified' if paper_trading_mode == 'alert_only'
                           else '' if PAPER_AUTO_FIRE_ENABLED else ' — auto-fire DISABLED')
                        + f'\n    Strategy   : {strategy_id}'
                        + f'\n    Spot price : {spot_price:.2f}   SL Lower: {portfolio_risk.get("sl_lower")}   SL Upper: {portfolio_risk.get("sl_upper")}'
                        + f'\n    Adjustment positions: {len((adj_doc or {}).get("positions") or [])}',
                        flush=True,
                    )
                    if paper_trading_mode == 'alert_only':
                        notify_user(
                            f'PT_{label.replace(" ", "_")}',
                            f'strategy {strategy_id} {label} hit (spot {spot_price:.2f}) — '
                            + (f'{len(adj_doc["positions"])} adjustment orders would apply' if adj_doc else 'no adjustment configured'),
                            {'trade_id': strategy_id, 'leg_id': ''},
                        )
                    elif PAPER_AUTO_FIRE_ENABLED:
                        if adj_key not in self.registry.adjustment_in_flight:
                            asyncio.create_task(self._fire_paper_adjustment(
                                db, strategy_id, legs, leg_ltp_by_token,
                                fired_condition, reset_condition, adj_doc,
                            ))

            if not alert_config:
                continue
            mtm_reason = mtm_check['hit_reason'] if mtm_check else ''
            if mtm_reason:
                label = 'PAPER MTM STOPLOSS' if mtm_reason == 'mtm_stoploss' else 'PAPER MTM TARGET'
                print(
                    '[SIMULATOR RISK MONITOR] '
                    + ('\U0001F534' if mtm_reason == 'mtm_stoploss' else '\U0001F7E2')
                    + f' {label} HIT'
                    + (' — ALERT ONLY mode, Telegram notified, no exit recorded' if paper_trading_mode == 'alert_only'
                       else '' if PAPER_AUTO_FIRE_ENABLED else ' — auto-fire DISABLED, no exit recorded')
                    + f'\n    Strategy   : {strategy_id}'
                    + f'\n    Legs ({len(legs)}): {legs_desc}'
                    + f'\n    Net P&L    : {net_pnl:.2f}',
                    flush=True,
                )
                if paper_trading_mode == 'alert_only':
                    notify_user(
                        f'PT_{label.replace(" ", "_")}',
                        f'strategy {strategy_id} {label} hit (net P&L {net_pnl:.2f}) — legs: {legs_desc}',
                        {'trade_id': strategy_id, 'leg_id': ''},
                    )
                elif PAPER_AUTO_FIRE_ENABLED:
                    for leg in legs:
                        leg_ltp = leg_ltp_by_token.get(str(leg.get('token') or '').strip(), 0.0)
                        fired_legs.append({**leg, '_ltp': leg_ltp, '_reason': mtm_reason, '_scope': 'paper_basket', '_net_pnl': net_pnl})

        if fired_legs:
            # A leg can carry both its own per-leg trigger and belong to a
            # basket trigger that hits in the same tick — dedupe to one exit
            # attempt per physical leg (token), or the broker would get two
            # opposite-side orders for the same already-closing position.
            deduped: dict[str, dict] = {}
            for leg in fired_legs:
                deduped.setdefault(str(leg.get('token') or leg.get('leg_id') or id(leg)), leg)
            for leg in deduped.values():
                token = str(leg.get('token') or '').strip()
                if token:
                    in_flight.add(token)
            asyncio.create_task(self._fire_exit(db, list(deduped.values())))

    @staticmethod
    def _check_leg_risk(entry: float, side: str, ltp: float, risk: dict) -> tuple[str, float | None, float | None]:
        """
        Always computes and returns both prices (so the caller can log them
        every tick, not just on a hit) — returns (hit_reason, sl_price,
        tp_price); sl_price/tp_price are None when that leg has no SL/TP
        configured, hit_reason is '' when neither is currently breached.
        """
        sl_price = None
        sl_mode = str(risk.get('sl_mode') or '')
        sl_value = _safe_float(risk.get('sl_value'))
        if sl_mode and sl_value > 0:
            sl_price = _leg_risk_price(entry, side, sl_mode, sl_value, 'sl')

        tp_price = None
        tp_mode = str(risk.get('tp_mode') or '')
        tp_value = _safe_float(risk.get('tp_value'))
        if tp_mode and tp_value > 0:
            tp_price = _leg_risk_price(entry, side, tp_mode, tp_value, 'tp')

        if sl_price is not None:
            sl_hit = ltp <= sl_price if side == 'BUY' else ltp >= sl_price
            if sl_hit:
                return 'stoploss', sl_price, tp_price
        if tp_price is not None:
            tp_hit = ltp >= tp_price if side == 'BUY' else ltp <= tp_price
            if tp_hit:
                return 'target', sl_price, tp_price
        return '', sl_price, tp_price

    @staticmethod
    def _check_basket_risk(spot_price: float, portfolio_risk: dict) -> str:
        """
        sl_lower/sl_upper are a band on the underlying's own spot price (the
        payoff chart's draggable markers, PaperTradeNew.tsx slSavedUpper/
        slSavedLower) — condition "<=" on the lower marker, ">=" on the
        upper one — NOT a P&L threshold.
        Returns 'lower_breach' / 'upper_breach' to distinguish which side fired,
        so the caller knows which adjustment basket to execute and which side to reset.
        """
        sl_lower = portfolio_risk.get('sl_lower')
        if sl_lower is not None and spot_price <= _safe_float(sl_lower):
            return 'lower_breach'
        sl_upper = portfolio_risk.get('sl_upper')
        if sl_upper is not None and spot_price >= _safe_float(sl_upper):
            return 'upper_breach'
        return ''

    @staticmethod
    def _trail_sl_threshold(base_sl: float, for_every: float, trail_by: float, peak_mtm: float) -> float:
        """
        Same formula as features/position_manager.py::update_overall_trail_sl
        (the algo_trades engine's own basket-level trailing SL) —
        reimplemented here rather than imported, since trading_core/algo_trades
        stays untouched by this module. base_sl/the returned threshold are
        both positive numbers meaning "exit when net P&L <= -threshold";
        peak_mtm ratchets it down (tighter) as profit climbs, never up.
        """
        if for_every <= 0 or peak_mtm <= 0:
            return base_sl
        steps = int(peak_mtm / for_every)
        return max(0.0, round(base_sl - steps * trail_by, 2))

    def _resolve_mtm_threshold(self, toggle: dict, premium_basis: float) -> float | None:
        """value/unit -> a rupee MTM threshold. 'percent' is % of the basket's
        total entry premium (Σ|entry_price×quantity| at the time the alert
        config was saved, see alert_legs_snapshot) — a new convention, since
        nothing else in this codebase already defines basket SL/Target as a
        percentage (PaperTrade.tsx's equivalent global SL/Target never
        actually computed against its own percent setting either)."""
        if not toggle.get('enabled'):
            return None
        value = _safe_float(toggle.get('value'))
        if value <= 0:
            return None
        if str(toggle.get('unit') or '') == 'percent':
            return (value / 100.0) * premium_basis if premium_basis > 0 else None
        return value

    def _check_mtm_alert_config(
        self, label: str, net_pnl: float, alert_config: dict, on_new_peak, verbose: bool = True,
    ) -> dict:
        """
        Stoploss/Target from the "Position Configuration" panel — a basket
        net-P&L threshold, independent of the spot-price-band check. Mutates
        alert_config['alert_peak_mtm'] in place (so the next tick this same
        cycle already sees the new high) and calls on_new_peak(peak_mtm) —
        a fire-and-forget persist closure the caller provides, since a
        broker-position basket persists to simulator_portfolio_triggers
        (keyed by broker_id+underlying) while a saved-strategy basket
        persists to simulator_strategy (keyed by strategy_id) — same
        check, two different homes for the one rare write it triggers.
        `label` is purely for the CHECK log line below (broker label or
        strategy name) — this function has no broker-specific concept left
        in it now that the persist call is the caller's job. verbose=False
        skips that line — the paper-strategy caller folds these numbers into
        its own single consolidated per-strategy block instead.
        Returns hit_reason plus every number the caller might want to show:
        {'hit_reason', 'sl_threshold', 'target_threshold', 'premium_basis', 'peak_mtm'}.
        """
        premium_basis = sum(
            abs(_safe_float(s.get('entry_price')) * _safe_int(s.get('quantity')))
            for s in (alert_config.get('alert_legs_snapshot') or [])
        )

        stoploss_cfg = alert_config.get('alert_stoploss') or {}
        target_cfg = alert_config.get('alert_target') or {}
        trailing_cfg = alert_config.get('alert_trailing_stop') or {}

        base_sl = self._resolve_mtm_threshold(stoploss_cfg, premium_basis)
        target_threshold = self._resolve_mtm_threshold(target_cfg, premium_basis)

        sl_threshold = base_sl
        if base_sl is not None and trailing_cfg.get('enabled'):
            peak_mtm = _safe_float(alert_config.get('alert_peak_mtm'))
            if net_pnl > peak_mtm:
                peak_mtm = net_pnl
                alert_config['alert_peak_mtm'] = peak_mtm
                on_new_peak(peak_mtm)
            for_every = _safe_float(trailing_cfg.get('x'))
            trail_by = _safe_float(trailing_cfg.get('y'))
            sl_threshold = self._trail_sl_threshold(base_sl, for_every, trail_by, peak_mtm)

        if verbose:
            print(
                f'[SIMULATOR RISK MONITOR] CHECK mtm {label} '
                f'net_pnl={net_pnl:.2f} premium_basis={premium_basis:.2f} '
                f'sl_threshold={sl_threshold if sl_threshold is not None else "none"} '
                f'target_threshold={target_threshold if target_threshold is not None else "none"} '
                f'peak_mtm={_safe_float(alert_config.get("alert_peak_mtm")):.2f}',
                flush=True,
            )

        hit_reason = ''
        if sl_threshold is not None and net_pnl <= -sl_threshold:
            hit_reason = 'mtm_stoploss'
        elif target_threshold is not None and net_pnl >= target_threshold:
            hit_reason = 'mtm_target'
        return {
            'hit_reason': hit_reason,
            'sl_threshold': sl_threshold,
            'target_threshold': target_threshold,
            'premium_basis': premium_basis,
            'peak_mtm': _safe_float(alert_config.get('alert_peak_mtm')),
        }

    async def manual_check_paper_strategy(self, db: MongoData, strategy_id: str) -> dict:
        """
        On-demand, single-pass equivalent of the hot tick's paper_leg/paper_basket
        checks — scoped to ONE strategy, for PaperTradeNew.tsx's "Manual Trigger"
        button. Runs every scenario the background monitor runs (per-leg SL/TP,
        basket adjustment upper/lower band, basket MTM SL/Target) exactly once
        against a freshly-warmed-refresh of this strategy, and — unlike the hot
        tick — awaits every fire directly rather than asyncio.create_task, so a
        hit's real exit/adjustment + real Telegram notify (notify_user/
        notify_admin, already inside _fire_paper_exit/_fire_paper_adjustment)
        complete before this returns. Used to verify those alerts actually fire
        without waiting for the next tick or faking a price move.
        """
        from bson import ObjectId
        from features.dhan_ticker import dhan_ticker_manager

        raw_db = db._db
        doc = await asyncio.to_thread(raw_db['simulator_strategy'].find_one, {'_id': ObjectId(strategy_id)})
        if not doc:
            return {'status': 'error', 'message': 'strategy not found'}

        await self._warm_refresh_one_paper_strategy(db, strategy_id, doc)

        ltp_map = dhan_ticker_manager.ltp_map or {}
        now_str = _now_iso()
        paper_trading_mode = self._resolve_paper_trading_mode(strategy_id)
        scenarios: list[dict[str, Any]] = []
        fire_legs: list[dict] = []

        print(f'[SIMULATOR RISK MONITOR] {now_str} ▶▶ MANUAL CHECK requested strategy={strategy_id} trade_mode={paper_trading_mode}', flush=True)

        # 1. Per-leg STOPLOSS/TARGET — one scenario per leg with its own sl/tp set.
        for leg in self.registry.paper_leg_by_token.values():
            if str(leg.get('strategy_id') or '') != strategy_id:
                continue
            token = str(leg.get('token') or '').strip()
            ltp, ltp_is_live = _resolve_live_value(ltp_map, token, leg.get('ltp'))
            risk = leg.get('risk') or {}
            entry = _safe_float(leg.get('entry_price'))
            side = str(leg.get('position') or '').upper()
            hit_reason, sl_price, tp_price = self._check_leg_risk(entry, side, ltp, risk)
            print(
                f'[SIMULATOR RISK MONITOR] {now_str} CHECK paper_leg (manual) strategy={strategy_id} '
                f'idx={leg.get("position_index")} {leg.get("underlying")} trade_mode={paper_trading_mode} {_format_leg_label(leg)} '
                f'entry={entry:.2f} ltp={ltp:.2f}{"" if ltp_is_live else "(cached)"} '
                f'sl={risk.get("sl_mode")}:{risk.get("sl_value")} tp={risk.get("tp_mode")}:{risk.get("tp_value")}',
                flush=True,
            )
            scenario: dict[str, Any] = {
                'scenario': 'per_leg_sl_tp', 'leg': _format_leg_label(leg),
                'position_index': leg.get('position_index'), 'entry': entry,
                'ltp': ltp, 'ltp_is_live': ltp_is_live,
                'sl_price': sl_price, 'tp_price': tp_price,
                'hit': hit_reason or None, 'action': 'no_hit',
            }
            if hit_reason:
                label = 'STOPLOSS' if hit_reason == 'stoploss' else 'TARGET'
                breached_price = sl_price if hit_reason == 'stoploss' else tp_price
                print(
                    '[SIMULATOR RISK MONITOR] '
                    + ('\U0001F534' if hit_reason == 'stoploss' else '\U0001F7E2')
                    + f' PAPER {label} HIT (manual check)'
                    + (' — ALERT ONLY mode, Telegram notified, no exit recorded' if paper_trading_mode == 'alert_only' else '')
                    + f'\n    Strategy   : {strategy_id} (position #{leg.get("position_index")})'
                    + f'\n    Instrument : {leg.get("underlying")} {_format_leg_label(leg)}'
                    + f'\n    Entry      : {entry:.2f}   Current LTP: {ltp:.2f}'
                    + (f'  ->  trigger price {breached_price:.2f}' if breached_price is not None else ''),
                    flush=True,
                )
            if hit_reason and ltp > 0:
                if paper_trading_mode == 'alert_only':
                    notify_user(
                        f'PT_PAPER_LEG_{hit_reason.upper()}',
                        f'(manual check) {leg.get("underlying")} {_format_leg_label(leg)} {hit_reason} hit '
                        f'(entry {entry:.2f}, LTP {ltp:.2f})',
                        {'trade_id': strategy_id, 'leg_id': str(leg.get('leg_id') or '')},
                    )
                    scenario['action'] = 'alert_only_notified'
                else:
                    fire_legs.append({**leg, '_ltp': ltp, '_reason': hit_reason, '_scope': 'paper_leg'})
                    scenario['action'] = 'fired_exit'
            scenarios.append(scenario)

        # 2. Basket adjustment band (upper/lower) + basket MTM SL/Target.
        legs = self.registry.paper_baskets_by_strategy.get(strategy_id) or []
        if not legs:
            print(f'[SIMULATOR RISK MONITOR] {now_str} CHECK paper_basket (manual) strategy={strategy_id} — no open basket to check', flush=True)
            scenarios.append({'scenario': 'basket_adjustment_band', 'hit': None, 'action': 'no_open_basket'})
            scenarios.append({'scenario': 'basket_mtm_sl_target', 'hit': None, 'action': 'no_open_basket'})
        else:
            net_pnl = 0.0
            leg_ltp_by_token: dict[str, float] = {}
            for leg in legs:
                token = str(leg.get('token') or '').strip()
                ltp, _ = _resolve_live_value(ltp_map, token, leg.get('ltp'))
                leg_ltp_by_token[token] = ltp
                if ltp > 0:
                    net_pnl += (ltp - _safe_float(leg.get('entry_price'))) * _leg_signed_qty(leg)
            legs_desc = ', '.join(f'{_format_leg_label(l)} [#{l.get("position_index")}]' for l in legs)

            portfolio_risk = legs[0].get('portfolio_risk') or {}
            spot_price = _safe_float(legs[0].get('spot_price'))
            print(
                f'[SIMULATOR RISK MONITOR] {now_str} CHECK paper_basket (manual) strategy={strategy_id} '
                f'legs=[{legs_desc}] net_pnl={net_pnl:.2f}'
                + (f' spot={spot_price:.2f} sl_lower={portfolio_risk.get("sl_lower")} sl_upper={portfolio_risk.get("sl_upper")}' if portfolio_risk else ''),
                flush=True,
            )
            if not portfolio_risk or spot_price <= 0:
                scenarios.append({'scenario': 'basket_adjustment_band', 'hit': None, 'action': 'not_configured'})
            else:
                hit_reason = self._check_basket_risk(spot_price, portfolio_risk)
                scenario = {
                    'scenario': 'basket_adjustment_band', 'spot_price': spot_price,
                    'sl_lower': portfolio_risk.get('sl_lower'), 'sl_upper': portfolio_risk.get('sl_upper'),
                    'hit': hit_reason or None, 'action': 'no_hit',
                }
                if hit_reason:
                    label = 'PAPER LOWER BREACH' if hit_reason == 'lower_breach' else 'PAPER UPPER BREACH'
                    print(
                        '[SIMULATOR RISK MONITOR] \U0001F534 ' + label + ' HIT (manual check)'
                        + (' — ALERT ONLY mode, Telegram notified' if paper_trading_mode == 'alert_only' else '')
                        + f'\n    Strategy   : {strategy_id}'
                        + f'\n    Spot price : {spot_price:.2f}   SL Lower: {portfolio_risk.get("sl_lower")}   SL Upper: {portfolio_risk.get("sl_upper")}',
                        flush=True,
                    )
                    fired_condition = '<=' if hit_reason == 'lower_breach' else '>='
                    reset_condition = '>=' if hit_reason == 'lower_breach' else '<='
                    adj_key = (strategy_id, '', fired_condition)
                    adj_doc = self.registry.adjustments.get(adj_key)
                    if paper_trading_mode == 'alert_only':
                        notify_user(
                            f'PT_{"PAPER_LOWER_BREACH" if hit_reason == "lower_breach" else "PAPER_UPPER_BREACH"}',
                            f'(manual check) strategy {strategy_id} {hit_reason} hit (spot {spot_price:.2f})',
                            {'trade_id': strategy_id, 'leg_id': ''},
                        )
                        scenario['action'] = 'alert_only_notified'
                    elif adj_key in self.registry.adjustment_in_flight:
                        scenario['action'] = 'already_in_flight'
                    else:
                        await self._fire_paper_adjustment(
                            db, strategy_id, legs, leg_ltp_by_token, fired_condition, reset_condition, adj_doc,
                        )
                        scenario['action'] = 'fired_adjustment'
                scenarios.append(scenario)

            alert_config = self.registry.paper_alert_configs.get(strategy_id)
            if not alert_config:
                scenarios.append({'scenario': 'basket_mtm_sl_target', 'hit': None, 'action': 'not_configured'})
            else:
                mtm_check = self._check_mtm_alert_config(
                    f'strategy={strategy_id} (manual)', net_pnl, alert_config,
                    lambda peak, _sid=strategy_id: asyncio.create_task(self._persist_peak_mtm_strategy(db, _sid, peak)),
                    verbose=True,
                )
                mtm_hit = mtm_check['hit_reason']
                scenario = {
                    'scenario': 'basket_mtm_sl_target', 'net_pnl': net_pnl,
                    'sl_threshold': mtm_check['sl_threshold'], 'target_threshold': mtm_check['target_threshold'],
                    'hit': mtm_hit or None, 'action': 'no_hit',
                }
                if mtm_hit:
                    label = 'PAPER MTM STOPLOSS' if mtm_hit == 'mtm_stoploss' else 'PAPER MTM TARGET'
                    print(
                        '[SIMULATOR RISK MONITOR] '
                        + ('\U0001F534' if mtm_hit == 'mtm_stoploss' else '\U0001F7E2')
                        + f' {label} HIT (manual check)'
                        + (' — ALERT ONLY mode, Telegram notified, no exit recorded' if paper_trading_mode == 'alert_only' else '')
                        + f'\n    Strategy   : {strategy_id}'
                        + f'\n    Legs ({len(legs)}): {legs_desc}'
                        + f'\n    Net P&L    : {net_pnl:.2f}',
                        flush=True,
                    )
                    if paper_trading_mode == 'alert_only':
                        notify_user(
                            f'PT_{"PAPER_MTM_STOPLOSS" if mtm_hit == "mtm_stoploss" else "PAPER_MTM_TARGET"}',
                            f'(manual check) strategy {strategy_id} {mtm_hit} hit (net P&L {net_pnl:.2f})',
                            {'trade_id': strategy_id, 'leg_id': ''},
                        )
                        scenario['action'] = 'alert_only_notified'
                    else:
                        for leg in legs:
                            leg_ltp = leg_ltp_by_token.get(str(leg.get('token') or '').strip(), 0.0)
                            fire_legs.append({**leg, '_ltp': leg_ltp, '_reason': mtm_hit, '_scope': 'paper_basket', '_net_pnl': net_pnl})
                        scenario['action'] = 'fired_exit_all_legs'
                scenarios.append(scenario)

        if fire_legs:
            deduped: dict[str, dict] = {}
            for leg in fire_legs:
                deduped.setdefault(str(leg.get('token') or leg.get('leg_id') or id(leg)), leg)
            for leg in deduped.values():
                token = str(leg.get('token') or '').strip()
                if token:
                    self.registry.in_flight.add(token)
            try:
                await self._fire_exit(db, list(deduped.values()))
            finally:
                for leg in deduped.values():
                    self.registry.in_flight.discard(str(leg.get('token') or ''))

        fired_count = sum(1 for s in scenarios if str(s.get('action') or '').startswith('fired'))
        print(
            f'[SIMULATOR RISK MONITOR] {now_str} ◀◀ MANUAL CHECK done strategy={strategy_id} '
            f'scenarios={len(scenarios)} fired={fired_count}', flush=True,
        )
        return {
            'status': 'success', 'strategy_id': strategy_id, 'checked_at': now_str,
            'trading_mode': paper_trading_mode, 'scenarios': scenarios,
        }

    async def force_fire_adjustment(self, db: MongoData, strategy_id: str, adjustment_id: str) -> dict:
        """
        Webhook-triggered equivalent of manual_check_paper_strategy's basket-adjustment
        branch (see lines above), minus the price-band check — the webhook hit (a
        TradingView alert, or a manual curl) *is* the trigger signal, so this fires
        unconditionally instead of re-checking spot against sl_lower/sl_upper. Used by
        POST/GET /webhook/tv/alert/{webhook_id} (api.py). Same paper-only guarantee as
        every other fire path here: see the PAPER_AUTO_FIRE_ENABLED comment up top.
        """
        from bson import ObjectId
        from features.dhan_ticker import dhan_ticker_manager

        raw_db = db._db
        try:
            adj_doc = await asyncio.to_thread(raw_db['simulator_adjustments'].find_one, {'_id': ObjectId(adjustment_id)})
        except Exception:
            return {'status': 'error', 'message': 'Invalid adjustment id'}
        if not adj_doc or str(adj_doc.get('strategy_id') or '') != strategy_id:
            return {'status': 'error', 'message': 'Adjustment not found for this strategy'}
        if adj_doc.get('status') is False:
            return {'status': 'error', 'message': 'Adjustment already fired or inactive'}

        try:
            strategy_doc = await asyncio.to_thread(raw_db['simulator_strategy'].find_one, {'_id': ObjectId(strategy_id)})
        except Exception:
            return {'status': 'error', 'message': 'Invalid strategy id'}
        if not strategy_doc:
            return {'status': 'error', 'message': 'Strategy not found'}

        await self._warm_refresh_one_paper_strategy(db, strategy_id, strategy_doc)
        # No guard on empty legs here — a basket with every leg already exited has none
        # (needs_basket comes back False in _warm_refresh_one_paper_strategy, so the
        # registry has nothing for this strategy_id), but the adjustment's NEW-tagged
        # positions must still fire and open fresh legs; only the EXIT-tagged ones need
        # a live open leg to match against, and _fire_paper_adjustment already no-ops
        # those when nothing matches (see its exit_plan loop).
        legs = self.registry.paper_baskets_by_strategy.get(strategy_id) or []

        ltp_map = dict(dhan_ticker_manager.ltp_map or {})
        leg_tokens = [str(leg.get('token') or '').strip() for leg in legs if str(leg.get('token') or '').strip()]
        # A webhook hit (TradingView alert, or a manual curl) can land while no
        # browser/UI session is open — those legs' tokens may never have been
        # WS-subscribed, leaving ltp_map empty for them. The tick-driven monitor
        # gets away with relying on ltp_map alone because some UI/session is
        # almost always keeping it hot; the webhook path can't assume that, so
        # it backfills with a REST quote (WS-first, REST-fallback) the same way
        # the option chain does for the same reason.
        missing_tokens = [t for t in leg_tokens if not ltp_map.get(t)]
        if missing_tokens:
            try:
                from features.broker_gateway import get_broker_rest_quotes
                # Also subscribes any never-seen tokens to the WS for next time.
                rest_quotes = await asyncio.to_thread(get_broker_rest_quotes, missing_tokens, raw_db, None)
                for tok, q in (rest_quotes or {}).items():
                    rest_ltp = _safe_float(q.get('ltp'))
                    if rest_ltp > 0:
                        ltp_map[tok] = rest_ltp
            except Exception as exc:
                log.debug('[SIMULATOR RISK MONITOR] webhook force-fire REST quote fallback error strategy=%s: %s', strategy_id, exc)

        leg_ltp_by_token: dict[str, float] = {}
        for leg in legs:
            token = str(leg.get('token') or '').strip()
            ltp, _ = _resolve_live_value(ltp_map, token, leg.get('ltp'))
            leg_ltp_by_token[token] = ltp

        fired_condition = str(adj_doc.get('trigger_condition') or '<=')
        reset_condition = '>=' if fired_condition == '<=' else '<='
        adj_key = (strategy_id, '', fired_condition)
        if adj_key in self.registry.adjustment_in_flight:
            return {'status': 'error', 'message': 'Adjustment already in flight'}

        now_str = _now_iso()
        print(
            f'[SIMULATOR RISK MONITOR] {now_str} ▶▶ WEBHOOK FORCE-FIRE strategy={strategy_id} '
            f'adjustment={adjustment_id} condition={fired_condition}', flush=True,
        )
        fire_result = await self._fire_paper_adjustment(db, strategy_id, legs, leg_ltp_by_token, fired_condition, reset_condition, adj_doc)
        # _fire_paper_adjustment aborts silently (no exception) when it can't get a live LTP
        # for a leg — without checking its result, this would always report "success" even
        # when nothing was actually applied (the adjustment stays armed for retry, by design).
        if fire_result.get('status') != 'success':
            return {'status': 'error', 'message': fire_result.get('message') or 'Adjustment did not fire'}
        return {
            'status': 'success', 'strategy_id': strategy_id, 'adjustment_id': adjustment_id,
            'fired_condition': fired_condition, 'exits': fire_result.get('exits', 0), 'new_adds': fire_result.get('new_adds', 0),
        }

    async def _persist_peak_mtm(self, db: MongoData, broker_id: str, underlying: str, peak_mtm: float) -> None:
        try:
            await asyncio.to_thread(
                db._db['simulator_portfolio_triggers'].update_one,
                {'broker_id': broker_id, 'underlying': underlying},
                {'$set': {'alert_peak_mtm': peak_mtm}},
            )
        except Exception as exc:
            log.debug('[SIMULATOR RISK MONITOR] peak_mtm persist error broker=%s underlying=%s: %s', broker_id, underlying, exc)

    async def _persist_peak_mtm_strategy(self, db: MongoData, strategy_id: str, peak_mtm: float) -> None:
        try:
            from bson import ObjectId
            await asyncio.to_thread(
                db._db['simulator_strategy'].update_one,
                {'_id': ObjectId(strategy_id)},
                {'$set': {'alert_peak_mtm': peak_mtm}},
            )
        except Exception as exc:
            log.debug('[SIMULATOR RISK MONITOR] paper peak_mtm persist error strategy=%s: %s', strategy_id, exc)

    async def _persist_hedge_state(
        self, db: MongoData, broker_id: str, underlying: str, active_date: str, hedge_legs: list[dict],
    ) -> None:
        try:
            await asyncio.to_thread(
                db._db['simulator_portfolio_triggers'].update_one,
                {'broker_id': broker_id, 'underlying': underlying},
                {'$set': {'alert_hedge_active_date': active_date, 'alert_hedge_legs': hedge_legs}},
            )
        except Exception as exc:
            log.debug('[SIMULATOR RISK MONITOR] hedge state persist error broker=%s underlying=%s: %s', broker_id, underlying, exc)

    # ── Hedge Time Control: rare event (twice a day at most), real broker call ──

    async def _check_hedge_time_control(
        self, db: MongoData, broker_id: str, underlying: str, broker_label: str, alert_config: dict,
    ) -> None:
        hedge_time = alert_config.get('alert_hedge_time_control') or {}
        hedge_strike = alert_config.get('alert_hedge_strike_type') or {}
        if not hedge_time.get('enabled') or not hedge_strike.get('enabled'):
            return

        key = (broker_id, underlying)
        if key in self.registry.hedge_in_flight:
            return

        now = datetime.now(IST)
        today = now.strftime('%Y-%m-%d')
        now_hm = now.strftime('%H:%M')
        entry_time = str(hedge_time.get('entry_time') or '09:15')
        exit_time = str(hedge_time.get('exit_time') or '15:30')
        active_date = str(alert_config.get('alert_hedge_active_date') or '')
        hedge_legs = alert_config.get('alert_hedge_legs') or []
        trading_mode = self._resolve_trading_mode(broker_id, underlying)

        is_entry_due = active_date != today and entry_time <= now_hm < exit_time
        is_exit_due = active_date == today and now_hm >= exit_time and hedge_legs

        if not is_entry_due and not is_exit_due:
            return

        self.registry.hedge_in_flight.add(key)
        try:
            if is_entry_due:
                await self._enter_hedge(db, broker_id, underlying, broker_label, hedge_strike, trading_mode, today)
            elif is_exit_due:
                await self._exit_hedge(db, broker_id, underlying, broker_label, hedge_legs, trading_mode, active_date)
        finally:
            self.registry.hedge_in_flight.discard(key)

    async def _enter_hedge(
        self, db: MongoData, broker_id: str, underlying: str, broker_label: str,
        hedge_strike: dict, trading_mode: str, today: str,
    ) -> None:
        mode = str(hedge_strike.get('mode') or 'delta')
        value = _safe_float(hedge_strike.get('value'))
        strike_drop = str(hedge_strike.get('strike') or 'ATM')

        if trading_mode == 'alert_only':
            print(f'[SIMULATOR RISK MONITOR] \U0001F514 HEDGE ENTRY DUE — ALERT ONLY mode, Telegram notified, no order placed broker={broker_label} {underlying}', flush=True)
            notify_user(
                'PT_HEDGE_ENTRY_DUE',
                f'{underlying} hedge entry time reached — would place BUY CE + BUY PE (mode={mode} value={value}).',
                {'trade_id': f'{broker_id}:{underlying}', 'leg_id': ''},
            )
            # Non-empty sentinel (not real legs — alert_only never places an order) so
            # is_exit_due's `and hedge_legs` truthiness check still fires the matching
            # exit-time notification later instead of silently never re-checking.
            await self._persist_hedge_state(db, broker_id, underlying, today, [{'option_type': 'ALERT_ONLY_MARKER'}])
            return

        from api import ManualOrderLeg, get_live_greeks_chain, _place_manual_order_via_order_service  # type: ignore

        try:
            chain_payload = await get_live_greeks_chain(underlying, expiry='')
        except Exception as exc:
            log.error('[SIMULATOR RISK MONITOR] hedge entry chain fetch error broker=%s %s: %s', broker_label, underlying, exc)
            return
        chain = chain_payload.get('chain') or {}
        atm_strike = _safe_float(chain_payload.get('atm_strike'))
        strike_interval = _safe_float(chain_payload.get('strike_interval'))
        expiry = str(chain_payload.get('expiry') or '')
        lot_size = int(chain_payload.get('lot_size') or 0) or 1

        ce_row = resolve_hedge_strike(chain.get('CE') or [], 'CE', mode, value, strike_drop, atm_strike, strike_interval)
        pe_row = resolve_hedge_strike(chain.get('PE') or [], 'PE', mode, value, strike_drop, atm_strike, strike_interval)
        if not ce_row or not pe_row:
            log.error('[SIMULATOR RISK MONITOR] hedge entry strike resolution failed broker=%s %s mode=%s value=%s — leaving unset, will retry next tick', broker_label, underlying, mode, value)
            return

        order_legs = [
            # MPP, not MARKET — see _fire_exit_for_broker's comment; SEBI no
            # longer allows MARKET orders via API.
            ManualOrderLeg(
                underlying=underlying, expiry=expiry, strike=_safe_float(row.get('strike')),
                option_type=opt, side='BUY', quantity=lot_size, order_type='MPP', price=_safe_float(row.get('ltp')), product='MIS',
            )
            for opt, row in (('CE', ce_row), ('PE', pe_row))
        ]
        print(
            f'[SIMULATOR RISK MONITOR] \U0001F6E1️ HEDGE ENTRY firing broker={broker_label} {underlying} '
            f'CE={ce_row.get("strike")} PE={pe_row.get("strike")} qty={lot_size} mode={mode}', flush=True,
        )
        try:
            result = await _place_manual_order_via_order_service(broker_id, order_legs)
        except Exception as exc:
            self.last_error = f'hedge entry error broker={broker_id} {underlying}: {exc}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
            return

        leg_results = result.get('results') or []
        placed_hedge_legs = [
            {'token': str(row.get('token') or ''), 'symbol': str(row.get('symbol') or ''), 'option_type': opt,
             'strike': _safe_float(row.get('strike')), 'expiry': expiry, 'quantity': lot_size}
            for (opt, row), leg_result in zip((('CE', ce_row), ('PE', pe_row)), leg_results)
            if isinstance(leg_result, dict) and leg_result.get('status') == 'success'
        ]
        if len(placed_hedge_legs) < 2:
            self.last_error = f'hedge entry partial/failed broker={broker_id} {underlying}: {result}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
        if placed_hedge_legs:
            await self._persist_hedge_state(db, broker_id, underlying, today, placed_hedge_legs)
            self.last_fire = {'broker_id': broker_id, 'reason': 'hedge_entry', 'legs': len(placed_hedge_legs), 'at': _now_iso(), 'result': result}

    async def _exit_hedge(
        self, db: MongoData, broker_id: str, underlying: str, broker_label: str,
        hedge_legs: list[dict], trading_mode: str, active_date: str,
    ) -> None:
        if trading_mode == 'alert_only':
            print(f'[SIMULATOR RISK MONITOR] \U0001F514 HEDGE EXIT DUE — ALERT ONLY mode, Telegram notified, no order placed broker={broker_label} {underlying}', flush=True)
            notify_user(
                'PT_HEDGE_EXIT_DUE',
                f'{underlying} hedge exit time reached — would square off {len(hedge_legs)} hedge leg(s).',
                {'trade_id': f'{broker_id}:{underlying}', 'leg_id': ''},
            )
            await self._persist_hedge_state(db, broker_id, underlying, active_date, [])
            return

        from api import ManualOrderLeg, get_live_greeks_chain, _place_manual_order_via_order_service  # type: ignore

        # hedge_legs (persisted at entry) carries no live price — re-fetch the chain so
        # MPP has a sane fallback price if its own live-quote lookup ever misses.
        ltp_by_key: dict[tuple[str, float], float] = {}
        try:
            chain_payload = await get_live_greeks_chain(underlying, expiry='')
            for opt in ('CE', 'PE'):
                for row in (chain_payload.get('chain') or {}).get(opt) or []:
                    ltp_by_key[(opt, _safe_float(row.get('strike')))] = _safe_float(row.get('ltp'))
        except Exception as exc:
            log.debug('[SIMULATOR RISK MONITOR] hedge exit chain fetch error broker=%s %s: %s', broker_label, underlying, exc)

        order_legs = [
            # MPP, not MARKET — see _fire_exit_for_broker's comment; SEBI no
            # longer allows MARKET orders via API.
            ManualOrderLeg(
                underlying=underlying, expiry=str(leg.get('expiry') or ''), strike=_safe_float(leg.get('strike')),
                option_type=str(leg.get('option_type') or ''), side='SELL',
                quantity=_safe_int(leg.get('quantity')), order_type='MPP',
                price=ltp_by_key.get((str(leg.get('option_type') or ''), _safe_float(leg.get('strike'))), 0.0),
                product='MIS',
            )
            for leg in hedge_legs
        ]
        print(f'[SIMULATOR RISK MONITOR] \U0001F6E1️ HEDGE EXIT firing broker={broker_label} {underlying} legs={len(order_legs)}', flush=True)
        try:
            result = await _place_manual_order_via_order_service(broker_id, order_legs)
        except Exception as exc:
            self.last_error = f'hedge exit error broker={broker_id} {underlying}: {exc}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
            return

        leg_results = result.get('results') or []
        remaining_legs = [
            leg for leg, leg_result in zip(hedge_legs, leg_results)
            if not (isinstance(leg_result, dict) and leg_result.get('status') == 'success')
        ]
        if remaining_legs:
            self.last_error = f'hedge exit partial/failed broker={broker_id} {underlying}: {result}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
        if len(remaining_legs) != len(hedge_legs):
            await self._persist_hedge_state(db, broker_id, underlying, active_date, remaining_legs)
            self.last_fire = {'broker_id': broker_id, 'reason': 'hedge_exit', 'legs': len(hedge_legs) - len(remaining_legs), 'at': _now_iso(), 'result': result}

    # ── Adjustment order polling: check broker for terminal status ───────────

    async def _poll_executing_adjustments(self, db: MongoData) -> None:
        """
        Called from warm refresh — for each broker_id that has OPEN simulator_adjustment_orders,
        poll broker for status. On completion of ALL orders in a fire group, call
        _complete_broker_adjustment to do the actual breach clearing.
        Completely separate from live_order_manager / broker_orders / algo_trades.
        """
        from features.simulator_adjustment_tracker import poll_and_update, delete_for_adjustment

        # Find distinct broker_ids with OPEN tracking rows
        raw_db = db._db
        try:
            open_broker_ids: list[str] = raw_db['simulator_adjustment_orders'].distinct(
                'broker_id', {'status': 'OPEN'}
            )
        except Exception as exc:
            log.debug('[SIMULATOR RISK MONITOR] adj poll: distinct broker_ids error: %s', exc)
            return

        for broker_id in open_broker_ids:
            try:
                completed_groups = await asyncio.to_thread(poll_and_update, db, broker_id)
                for adj_doc_id, _bid, trigger_cond in completed_groups:
                    print(
                        f'[SIMULATOR RISK MONITOR] \u2705 Adjustment orders ALL terminal adj={adj_doc_id} condition={trigger_cond} — proceeding to breach clear',
                        flush=True,
                    )
                    # Look up the adjustment doc to get underlying + reset_condition
                    try:
                        from bson import ObjectId
                        adj_doc = await asyncio.to_thread(
                            raw_db['simulator_adjustments'].find_one, {'_id': ObjectId(adj_doc_id)},
                        )
                    except Exception:
                        adj_doc = None
                    if not adj_doc:
                        continue
                    underlying = str(adj_doc.get('underlying') or '').strip()
                    reset_condition = '>=' if trigger_cond == '<=' else '<='
                    broker_label = self.registry.label_for(broker_id)
                    await self._complete_broker_adjustment(db, broker_id, underlying, broker_label, trigger_cond, reset_condition, adj_doc_id)
                    await asyncio.to_thread(delete_for_adjustment, db, adj_doc_id)
            except Exception as exc:
                log.warning('[SIMULATOR RISK MONITOR] adj poll error broker=%s: %s', broker_id, exc)

    async def _complete_broker_adjustment(
        self, db: MongoData, broker_id: str, underlying: str, broker_label: str,
        fired_condition: str, reset_condition: str, adj_doc_id: str,
    ) -> None:
        """
        Called after all adjustment orders are confirmed terminal — does the actual
        breach-point clearing. Same clearing logic that was previously done immediately
        after placing; now deferred until completion is confirmed.
        """
        now_str = _now_iso()
        raw_db = db._db

        # Opposite side was already reset at placement time (_fire_broker_adjustment Step A).
        # Only need to clear THIS side now that orders are confirmed terminal.

        # Clear THIS side breach point + adjustment doc
        fired_field = 'sl_lower' if fired_condition == '<=' else 'sl_upper'
        await asyncio.to_thread(
            raw_db['simulator_portfolio_triggers'].update_one,
            {'broker_id': broker_id, 'underlying': underlying},
            {'$unset': {fired_field: ''}, '$set': {'updated_at': now_str, 'order_status': 'complete'}},
        )
        try:
            from bson import ObjectId
            await asyncio.to_thread(
                raw_db['simulator_adjustments'].update_one,
                {'_id': ObjectId(adj_doc_id)},
                {'$set': {'order_status': 'complete', 'completed_at': now_str}},
            )
        except Exception:
            pass
        self.registry.adjustments.pop((broker_id, underlying, fired_condition), None)

        # If BOTH sides are cleared → retire sl_marker_status
        doc = await asyncio.to_thread(
            raw_db['simulator_portfolio_triggers'].find_one,
            {'broker_id': broker_id, 'underlying': underlying},
        )
        if doc and doc.get('sl_upper') is None and doc.get('sl_lower') is None:
            await asyncio.to_thread(
                raw_db['simulator_portfolio_triggers'].update_one,
                {'broker_id': broker_id, 'underlying': underlying},
                {'$set': {'status': 'fired', 'fired_reason': f'adjustment_{fired_condition}', 'updated_at': now_str}},
            )
            self.registry.baskets_by_key.pop((broker_id, underlying), None)
        print(f'[SIMULATOR RISK MONITOR] \u2705 Breach cleared adj={adj_doc_id} condition={fired_condition} broker={broker_label} {underlying}', flush=True)

    # ── Adjustment fire: execute only the side's basket, reset the other side ──

    async def _fire_broker_adjustment(
        self, db: MongoData, broker_id: str, underlying: str, broker_label: str,
        fired_condition: str, reset_condition: str, adj_doc: dict | None,
    ) -> None:
        """
        Execute the adjustment basket for the breached side (fired_condition),
        then:
          - Immediately clear the OPPOSITE side's breach point and its adjustment doc
          - After orders settle: clear this side's breach point and adjustment doc
          - Leave alert_status (Overall SL/TP) completely untouched
        """
        adj_key = (broker_id, underlying, fired_condition)
        self.registry.adjustment_in_flight.add(adj_key)
        try:
            positions = (adj_doc or {}).get('positions') or []
            if positions and AUTO_FIRE_ENABLED:
                # _place_manual_order_via_order_service (not simulator_place_manual_order/
                # _simulator_place_manual_order_core in-process) — this box isn't the one
                # whitelisted with Dhan for live order placement, only algo.order's box is.
                # Routing through algo.order's internal gateway means the actual broker call
                # happens on ITS box regardless of this monitor firing here.
                from api import ManualOrderLeg, _place_manual_order_via_order_service  # type: ignore
                order_legs = []
                for p in positions:
                    tag = str(p.get('tag') or 'EXIT')
                    side = str(p.get('side') or 'BUY').upper()
                    # EXIT tag: place the opposite side to close; NEW tag: place as-is
                    order_side = ('SELL' if side == 'BUY' else 'BUY') if tag == 'EXIT' else side
                    order_legs.append(ManualOrderLeg(
                        underlying=underlying,
                        expiry=str(p.get('expiry') or ''),
                        strike=_safe_float(p.get('strike')),
                        option_type=str(p.get('option_type') or ''),
                        side=order_side,
                        quantity=abs(_safe_int(p.get('qty') or p.get('lots') or 0)),
                        order_type='MPP',
                        price=_safe_float(p.get('entry_price')),
                        product='MIS',
                    ))
                if order_legs:
                    try:
                        result = await _place_manual_order_via_order_service(broker_id, order_legs)
                        self.last_fire = {
                            'broker_id': broker_id, 'reason': f'adjustment_{fired_condition}',
                            'legs': len(order_legs), 'at': _now_iso(), 'result': result,
                        }
                        print(f'[SIMULATOR RISK MONITOR] \U0001F7E1 ADJUSTMENT orders placed broker={broker_label} {underlying} condition={fired_condition} legs={len(order_legs)} result={result}', flush=True)
                        adj_doc_id = str((adj_doc or {}).get('_id') or '')
                        now_str = _now_iso()
                        raw_db = db._db

                        # ── Step A: Reset OPPOSITE side IMMEDIATELY after placing ──
                        # Must happen now — not after order completion. If we waited,
                        # the opposite side could breach during the settle window and
                        # both sides would double-fire.
                        opposite_field = 'sl_upper' if reset_condition == '>=' else 'sl_lower'
                        await asyncio.to_thread(
                            raw_db['simulator_portfolio_triggers'].update_one,
                            {'broker_id': broker_id, 'underlying': underlying},
                            {'$unset': {opposite_field: ''}, '$set': {'updated_at': now_str}},
                        )
                        await asyncio.to_thread(
                            raw_db['simulator_adjustments'].delete_many,
                            {'broker_id': broker_id, 'underlying': underlying, 'trigger_condition': reset_condition},
                        )
                        self.registry.adjustments.pop((broker_id, underlying, reset_condition), None)
                        print(f'[SIMULATOR RISK MONITOR] \U0001F504 Opposite side reset immediately condition={reset_condition} broker={broker_label} {underlying}', flush=True)

                        # ── Step B: Save order IDs, defer own-side clear ──────────
                        # sl_upper/sl_lower for THIS side stays active until all orders
                        # are terminal — _complete_broker_adjustment clears it.
                        from features.simulator_adjustment_tracker import save_adjustment_order_results
                        if adj_doc_id:
                            leg_results = result.get('results') or []
                            saved = await asyncio.to_thread(
                                save_adjustment_order_results,
                                db, adj_doc_id, broker_id, underlying, fired_condition,
                                leg_results, positions,
                            )
                            print(f'[SIMULATOR RISK MONITOR] \U0001F4BE Saved {saved} order records for tracking adj={adj_doc_id}', flush=True)
                            from bson import ObjectId as _ObjId
                            await asyncio.to_thread(
                                raw_db['simulator_adjustments'].update_one,
                                {'_id': _ObjId(adj_doc_id)},
                                {'$set': {'order_status': 'executing', 'executing_since': now_str}},
                            )
                            # Warm refresh → _poll_executing_adjustments will clear own side
                            # once all orders reach terminal status.
                            return
                    except Exception as exc:
                        self.last_error = f'adjustment fire error broker={broker_id}: {exc}'
                        log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
                        return

            now_str = _now_iso()
            raw_db = db._db

            # 1. Immediately reset the OPPOSITE side's breach point + adjustment doc
            opposite_field = 'sl_upper' if reset_condition == '>=' else 'sl_lower'
            await asyncio.to_thread(
                raw_db['simulator_portfolio_triggers'].update_one,
                {'broker_id': broker_id, 'underlying': underlying},
                {'$unset': {opposite_field: ''}, '$set': {'updated_at': now_str}},
            )
            await asyncio.to_thread(
                raw_db['simulator_adjustments'].delete_many,
                {'broker_id': broker_id, 'underlying': underlying, 'trigger_condition': reset_condition},
            )
            self.registry.adjustments.pop((broker_id, underlying, reset_condition), None)
            print(f'[SIMULATOR RISK MONITOR] \U0001F504 Reset opposite side condition={reset_condition} broker={broker_label} {underlying}', flush=True)

            # 2. Clear THIS side's breach point + adjustment doc (orders are now placed/settled)
            fired_field = 'sl_lower' if fired_condition == '<=' else 'sl_upper'
            await asyncio.to_thread(
                raw_db['simulator_portfolio_triggers'].update_one,
                {'broker_id': broker_id, 'underlying': underlying},
                {'$unset': {fired_field: ''}, '$set': {'updated_at': now_str}},
            )
            await asyncio.to_thread(
                raw_db['simulator_adjustments'].delete_many,
                {'broker_id': broker_id, 'underlying': underlying, 'trigger_condition': fired_condition},
            )
            self.registry.adjustments.pop(adj_key, None)
            # Also evict from baskets_by_key if BOTH sides are now gone (no more sl_marker to watch)
            doc = await asyncio.to_thread(
                raw_db['simulator_portfolio_triggers'].find_one,
                {'broker_id': broker_id, 'underlying': underlying},
            )
            if doc and doc.get('sl_upper') is None and doc.get('sl_lower') is None:
                # No more band markers — update sl_marker_status so warm refresh stops watching
                await asyncio.to_thread(
                    raw_db['simulator_portfolio_triggers'].update_one,
                    {'broker_id': broker_id, 'underlying': underlying},
                    {'$set': {'status': 'fired', 'fired_reason': f'adjustment_{fired_condition}', 'updated_at': now_str}},
                )
                self.registry.baskets_by_key.pop((broker_id, underlying), None)
            print(f'[SIMULATOR RISK MONITOR] \u2705 Adjustment complete, cleared side={fired_condition} broker={broker_label} {underlying}', flush=True)

        finally:
            self.registry.adjustment_in_flight.discard(adj_key)

    async def _fire_paper_adjustment(
        self, db: MongoData, strategy_id: str, basket_legs: list[dict],
        leg_ltp_by_token: dict[str, float],
        fired_condition: str, reset_condition: str, adj_doc: dict | None,
    ) -> dict:
        """
        Paper strategy equivalent: apply adjustment positions (EXIT → mark matching
        position exited, NEW → add new position to the strategy doc), then reset
        the opposite side and clear this side — same logic as broker adjustment.
        Overall SL/TP (alert_status) is left completely untouched.
        """
        adj_key = (strategy_id, '', fired_condition)
        self.registry.adjustment_in_flight.add(adj_key)
        try:
            from bson import ObjectId
            raw_db = db._db
            now_str = _now_iso()
            exit_count = 0
            new_adds_count = 0

            adj_positions = (adj_doc or {}).get('positions') or []
            if adj_positions:
                doc = await asyncio.to_thread(raw_db['simulator_strategy'].find_one, {'_id': ObjectId(strategy_id)})
                if doc:
                    positions = list(doc.get('positions') or [])
                    new_positions_to_add: list[dict] = []

                    # Resolve every EXIT-tagged instruction's matching open leg + live LTP
                    # FIRST, before mutating anything. A matched leg with no live LTP is a
                    # hard stop, not a silent fall-back to entry_price — that would record a
                    # fake exit at zero P&L and hide exactly the kind of LTP outage Telegram
                    # already pages admin about elsewhere (ltp_fetch_error). Abort the whole
                    # fire and retry next tick (sl_upper/sl_lower stay armed) rather than ever
                    # apply a guessed price — this is real-money-shaped P&L bookkeeping.
                    # basket_legs (this tick's enriched leg list, token already resolved by
                    # _enrich_pt_strategy_positions) is the only place a usable token exists —
                    # the raw simulator_strategy.positions doc fetched above never persists
                    # one (resolution is a runtime enrichment step), so matched_pos['token']
                    # is reliably empty and must not be used for the ltp lookup.
                    enriched_by_key: dict[tuple[float, str], dict] = {
                        (round(_safe_float(bl.get('strike')), 2), _normalize_option_type(bl.get('option'))): bl
                        for bl in basket_legs
                    }

                    exit_plan: list[tuple[dict, float]] = []
                    for ap in adj_positions:
                        if str(ap.get('tag') or 'EXIT') != 'EXIT':
                            continue
                        # Find the matching open leg by strike + option_type only — ap['side']
                        # here is the UI's reverse/closing side (what you'd place to flatten
                        # the leg), not the original leg's side, so it can't be compared
                        # against pos['type'] directly. One open leg per (strike, option_type)
                        # is the existing assumption basket-wide already.
                        ap_strike = _safe_float(ap.get('strike'))
                        ap_option = _normalize_option_type(ap.get('option_type'))
                        matched_pos = None
                        for pos in positions:
                            if pos.get('exited'):
                                continue
                            pos_strike = _safe_float(pos.get('strike'))
                            pos_option = _normalize_option_type(pos.get('option_type') or pos.get('option'))
                            if abs(pos_strike - ap_strike) < 0.01 and pos_option == ap_option:
                                matched_pos = pos
                                break
                        if matched_pos is None:
                            continue
                        enriched_leg = enriched_by_key.get((round(ap_strike, 2), ap_option)) or {}
                        token = str(enriched_leg.get('token') or matched_pos.get('token') or '').strip()
                        ltp = (
                            leg_ltp_by_token.get(token)
                            or _safe_float(enriched_leg.get('ltp'))
                            or _safe_float(matched_pos.get('current_ltp'))
                        )
                        if ltp <= 0:
                            err_msg = (
                                f'strategy {strategy_id} adjustment condition={fired_condition} ABORTED — '
                                f'no live LTP for token={token} strike={ap_strike:.2f} {ap_option}; '
                                f'refusing to fall back to entry_price as the exit fill. Will retry next tick.'
                            )
                            log.error('[SIMULATOR RISK MONITOR] %s', err_msg)
                            notify_admin('PAPER_ADJUSTMENT_LTP_MISSING', err_msg, {'trade_id': strategy_id})
                            print(f'[SIMULATOR RISK MONITOR] ⚠️ {err_msg}', flush=True)
                            return {'status': 'error', 'message': err_msg}
                        exit_plan.append((matched_pos, ltp))

                    for matched_pos, ltp in exit_plan:
                        entry = _safe_float(matched_pos.get('entry_price'))
                        qty = _safe_float(matched_pos.get('quantity'))
                        is_sell = _normalize_side(matched_pos.get('type')) == 'SELL'
                        pnl = (entry - ltp) * qty if is_sell else (ltp - entry) * qty
                        matched_pos['exited'] = True
                        matched_pos['exit_price'] = round(ltp, 2)
                        matched_pos['exit_time'] = now_str
                        matched_pos['pnl'] = round(pnl, 2)

                    # basket_legs is empty whenever every existing leg is already exited
                    # (force_fire_adjustment's "no open legs" case) — fall back to the
                    # strategy doc's own instrument field rather than leaving this blank,
                    # since a blank underlying would stop the NEW leg's token from
                    # resolving below.
                    underlying = (
                        str(basket_legs[0].get('underlying') or '').strip().upper() if basket_legs
                        else str(doc.get('instrument') or '').strip().upper()
                    )
                    from features.dhan_ticker import dhan_ticker_manager
                    ws_ltp_map = dhan_ticker_manager.ltp_map or {}

                    for ap in adj_positions:
                        tag = str(ap.get('tag') or 'EXIT')
                        if tag == 'NEW':
                            # Add new position to the strategy — stored in the same
                            # 'Buy'/'Sell' + 'Call'/'Put' word form simulator_strategy.positions
                            # already uses everywhere else (see the 4 exited legs above), not the
                            # 'B'/'S' + 'CE'/'PE' abbreviations the alert-builder UI saves with.
                            new_side = _normalize_side(ap.get('side')) or 'BUY'
                            new_option = _normalize_option_type(ap.get('option_type'))

                            # entry_price must be the LIVE price right now, never ap['entry_price']
                            # (a snapshot from whenever this adjustment was configured — could be
                            # hours/days stale by the time it actually fires). Resolve this strike's
                            # token fresh (it isn't an existing leg yet, so it has no cached token)
                            # and read its current LTP, same live-price-or-abort policy the EXIT
                            # side above uses — no fallback to a stale/guessed price.
                            ltp = 0.0
                            try:
                                from api import _resolve_pt_position_token  # type: ignore
                                token = _resolve_pt_position_token(
                                    {'strike': ap.get('strike'), 'option_type': ap.get('option_type'), 'expiry': ap.get('expiry')},
                                    underlying,
                                )
                            except Exception:
                                token = ''
                            if token:
                                ltp = _safe_float(ws_ltp_map.get(token))
                                if ltp <= 0:
                                    try:
                                        from features.broker_gateway import get_broker_rest_quotes
                                        rest_quotes = await asyncio.to_thread(
                                            get_broker_rest_quotes, [token], raw_db, {token: 'NSE_FNO'},
                                        )
                                        ltp = _safe_float((rest_quotes.get(token) or {}).get('ltp'))
                                    except Exception:
                                        ltp = 0.0
                            if ltp <= 0:
                                err_msg = (
                                    f'strategy {strategy_id} adjustment condition={fired_condition} ABORTED — '
                                    f'no live LTP for NEW leg strike={_safe_float(ap.get("strike")):.2f} {new_option} '
                                    f'token={token or "unresolved"}; refusing to use the stale configured entry_price. '
                                    f'Will retry next tick.'
                                )
                                log.error('[SIMULATOR RISK MONITOR] %s', err_msg)
                                notify_admin('PAPER_ADJUSTMENT_LTP_MISSING', err_msg, {'trade_id': strategy_id})
                                print(f'[SIMULATOR RISK MONITOR] ⚠️ {err_msg}', flush=True)
                                return {'status': 'error', 'message': err_msg}

                            new_positions_to_add.append({
                                'type': 'Sell' if new_side == 'SELL' else 'Buy',
                                'option_type': 'Put' if new_option == 'PUT' else 'Call',
                                'strike': _safe_float(ap.get('strike')),
                                'expiry': str(ap.get('expiry') or ''),
                                'quantity': abs(_safe_int(ap.get('qty') or ap.get('lots') or 0)),
                                'entry_price': round(ltp, 2),
                                'entry_time': now_str,
                                'exited': False,
                                'pnl': 0,
                            })

                    positions.extend(new_positions_to_add)
                    await asyncio.to_thread(
                        raw_db['simulator_strategy'].update_one,
                        {'_id': ObjectId(strategy_id)},
                        {'$set': {'positions': positions, 'updated_at': now_str}},
                    )
                    exit_count = len(exit_plan)
                    new_adds_count = len(new_positions_to_add)
                    print(
                        f'[SIMULATOR RISK MONITOR] \U0001F7E1 PAPER ADJUSTMENT applied strategy={strategy_id} '
                        f'condition={fired_condition} exits={exit_count} '
                        f'new_adds={new_adds_count}', flush=True,
                    )
                    notify_user(
                        'PT_PAPER_ADJUSTMENT_APPLIED',
                        f'strategy {strategy_id} adjustment applied — {exit_count} exit(s), {len(new_positions_to_add)} new position(s)',
                        {'trade_id': strategy_id, 'leg_id': ''},
                    )

            # 1. Reset opposite side
            opposite_field = 'sl_upper' if reset_condition == '>=' else 'sl_lower'
            await asyncio.to_thread(
                raw_db['simulator_strategy'].update_one,
                {'_id': ObjectId(strategy_id)},
                {'$unset': {opposite_field: ''}, '$set': {'updated_at': now_str}},
            )
            # Disable rather than delete — simulator_adjustments keeps every past
            # adjustment as history; _fetch_adjustment_docs_paper only ever loads
            # status != False, so a disabled doc can't be picked up and re-fired.
            await asyncio.to_thread(
                raw_db['simulator_adjustments'].update_many,
                {'strategy_id': strategy_id, 'trigger_condition': reset_condition, 'status': {'$ne': False}},
                {'$set': {'status': False, 'disabled_at': now_str, 'disabled_reason': f'opposite_of_{fired_condition}_fire'}},
            )
            self.registry.adjustments.pop((strategy_id, '', reset_condition), None)

            # 2. Clear this side
            fired_field = 'sl_lower' if fired_condition == '<=' else 'sl_upper'
            await asyncio.to_thread(
                raw_db['simulator_strategy'].update_one,
                {'_id': ObjectId(strategy_id)},
                {'$unset': {fired_field: ''}, '$set': {'updated_at': now_str}},
            )
            await asyncio.to_thread(
                raw_db['simulator_adjustments'].update_many,
                {'strategy_id': strategy_id, 'trigger_condition': fired_condition, 'status': {'$ne': False}},
                {'$set': {'status': False, 'disabled_at': now_str, 'disabled_reason': 'fired'}},
            )
            self.registry.adjustments.pop(adj_key, None)
            # If both sides gone, retire sl_marker_status so warm refresh stops watching
            doc = await asyncio.to_thread(raw_db['simulator_strategy'].find_one, {'_id': ObjectId(strategy_id)})
            if doc and doc.get('sl_upper') is None and doc.get('sl_lower') is None:
                await asyncio.to_thread(
                    raw_db['simulator_strategy'].update_one,
                    {'_id': ObjectId(strategy_id)},
                    {'$set': {'sl_marker_status': 'fired', 'sl_marker_fired_reason': f'adjustment_{fired_condition}', 'updated_at': now_str}},
                )
                self.registry.paper_baskets_by_strategy.pop(strategy_id, None)
            print(f'[SIMULATOR RISK MONITOR] \u2705 Paper adjustment complete, cleared side={fired_condition} strategy={strategy_id}', flush=True)
            return {'status': 'success', 'exits': exit_count, 'new_adds': new_adds_count}

        except Exception as exc:
            self.last_error = f'paper adjustment error strategy={strategy_id}: {exc}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
            notify_admin('RISK_MONITOR_PAPER_ADJUSTMENT_ERROR', self.last_error, {'trade_id': strategy_id})
            return {'status': 'error', 'message': self.last_error}
        finally:
            self.registry.adjustment_in_flight.discard(adj_key)

    # ── Fire: rare event, real broker call ───────────────────────────────────

    async def _fire_exit(self, db: MongoData, legs: list[dict]) -> None:
        # Branch by scope FIRST, not just by broker_id — a paper leg/basket
        # (saved strategy, no real broker_id at all) must never fall through
        # to the real-broker path below by accident (it would group under
        # broker_id='' and attempt a real order with an invalid broker).
        paper_scopes = ('paper_leg', 'paper_basket', 'paper_basket_marker')
        broker_legs_in = [leg for leg in legs if str(leg.get('_scope') or '') not in paper_scopes]
        paper_legs_in = [leg for leg in legs if str(leg.get('_scope') or '') in paper_scopes]

        # One order request per broker_id — a basket hit can span legs that
        # all share the same broker_id (it's keyed that way), but guard
        # against mixed input anyway since this list is assembled per-cycle.
        by_broker: dict[str, list[dict]] = {}
        for leg in broker_legs_in:
            by_broker.setdefault(str(leg.get('broker_id') or ''), []).append(leg)

        for broker_id, broker_legs in by_broker.items():
            try:
                await self._fire_exit_for_broker(db, broker_id, broker_legs)
            finally:
                # Always clear in_flight for this batch, success or failure —
                # placed legs are gone from the caches anyway (see
                # _mark_fired's eviction); failed/un-attempted ones simply
                # become eligible for the next hot tick to retry.
                for leg in broker_legs:
                    self.registry.in_flight.discard(str(leg.get('token') or ''))

        # One update_one per strategy_id — no real broker call, paper exit
        # only (mark the saved position(s) exited with the live LTP).
        by_strategy: dict[str, list[dict]] = {}
        for leg in paper_legs_in:
            by_strategy.setdefault(str(leg.get('strategy_id') or ''), []).append(leg)

        for strategy_id, strategy_legs in by_strategy.items():
            try:
                await self._fire_paper_exit(db, strategy_id, strategy_legs)
            finally:
                for leg in strategy_legs:
                    self.registry.in_flight.discard(str(leg.get('token') or ''))

    async def _fire_exit_for_broker(self, db: MongoData, broker_id: str, broker_legs: list[dict]) -> None:
        from api import ManualOrderLeg, _place_manual_order_via_order_service  # type: ignore

        order_legs = [
            ManualOrderLeg(
                underlying=str(leg.get('underlying') or ''),
                expiry=str(leg.get('expiry_date') or ''),
                strike=_safe_float(leg.get('strike')),
                option_type=str(leg.get('option') or ''),
                side='SELL' if str(leg.get('position') or '').upper() == 'BUY' else 'BUY',
                quantity=abs(_safe_int(leg.get('quantity'))),
                # SEBI no longer allows MARKET orders through the API — "MPP" is
                # simulator_place_manual_order's own NSE-compliant substitute: a
                # LIMIT order priced off Dhan's live bid/ask + protection band
                # (_resolve_mpp_price), close enough to a market fill to still
                # behave like a real stop-loss exit. price is just the fallback
                # if that live quote lookup ever misses.
                order_type='MPP',
                price=_safe_float(leg.get('_ltp') or leg.get('ltp')),
                product='MIS',
            )
            for leg in broker_legs
        ]
        log.info(
            '[SIMULATOR RISK MONITOR] firing exit broker=%s reason=%s legs=%s',
            broker_id, broker_legs[0].get('_reason'), [o.model_dump() for o in order_legs],
        )
        try:
            # The internal /internal/place-order gateway (algo.order) already sends
            # its own Telegram notification for the order's success/error/partial
            # result — only the unexpected-exception backstop below needs one of
            # its own.
            result = await _place_manual_order_via_order_service(broker_id, order_legs)
        except Exception as exc:
            self.last_error = f'fire_exit error broker={broker_id}: {exc}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
            notify_admin('RISK_MONITOR_FIRE_EXIT_ERROR', self.last_error, {'trade_id': broker_id})
            return

        if result.get('status') == 'error' and not result.get('results'):
            # Rejected before any per-leg attempt (e.g. broker creds
            # missing) — nothing placed, leave every trigger active.
            self.last_error = f'fire_exit rejected broker={broker_id}: {result}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
            return

        # Per-leg results come back in the same order as order_legs/
        # broker_legs (asyncio.gather over body.orders preserves input
        # order) — only mark+evict the legs that actually placed; a
        # "partial" result must leave the failed legs active so the next
        # hot tick retries just those instead of silently dropping them.
        leg_results = result.get('results') or []
        placed_legs = [
            leg for leg, leg_result in zip(broker_legs, leg_results)
            if isinstance(leg_result, dict) and leg_result.get('status') == 'success'
        ]
        failed_legs = [
            leg for leg, leg_result in zip(broker_legs, leg_results)
            if not (isinstance(leg_result, dict) and leg_result.get('status') == 'success')
        ]
        if failed_legs:
            self.last_error = f'fire_exit partial broker={broker_id} failed={len(failed_legs)}: {result}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
        if placed_legs:
            await self._mark_fired(db, broker_id, placed_legs)
            self.last_fire = {
                'broker_id': broker_id,
                'reason': placed_legs[0].get('_reason'),
                'legs': len(placed_legs),
                'at': _now_iso(),
                'result': result,
            }

    async def _mark_fired(self, db: MongoData, broker_id: str, legs: list[dict]) -> None:
        # update_one per leg (not update_many) so a batch that mixes leg-scope
        # and basket-scope hits — or different reasons/underlyings — records
        # an accurate fired_reason per doc instead of overwriting them all
        # with whichever leg happened to be first. Fire events are rare, so
        # the extra round trips here don't matter for the hot-path budget.
        raw_db = db._db
        now_str = _now_iso()
        basket_keys_to_evict: set[tuple[str, str]] = set()

        for leg in legs:
            reason = leg.get('_reason')
            leg_id = str(leg.get('leg_id') or '')
            if leg_id:
                await asyncio.to_thread(
                    raw_db['simulator_triggers'].update_one,
                    {'broker_id': broker_id, 'leg_id': leg_id, 'status': 'active'},
                    {'$set': {'status': 'fired', 'fired_reason': reason, 'updated_at': now_str}},
                )
            if leg.get('_scope') == 'mtm_basket':
                underlying = str(leg.get('underlying') or '')
                await asyncio.to_thread(
                    raw_db['simulator_portfolio_triggers'].update_one,
                    {'broker_id': broker_id, 'underlying': underlying, 'alert_status': 'active'},
                    {'$set': {
                        'alert_status': 'fired', 'alert_fired_reason': reason, 'alert_updated_at': now_str,
                        'status': 'fired', 'fired_reason': reason, 'updated_at': now_str,
                    }},
                )
                basket_keys_to_evict.add((broker_id, underlying))
            self.registry.leg_by_token.pop(str(leg.get('token') or ''), None)

        for key in basket_keys_to_evict:
            self.registry.baskets_by_key.pop(key, None)
            self.registry.alert_configs.pop(key, None)

    async def _fire_paper_exit(self, db: MongoData, strategy_id: str, legs: list[dict]) -> None:
        """
        Saved-strategy counterpart to _fire_exit_for_broker — no broker call
        at all. Marks the hit position(s) exited directly on the
        simulator_strategy doc (positions[i].exited/exit_price/exit_time/
        pnl, by array index — same "array index is identity" convention the
        rest of this feature uses, since a saved position has no stable
        leg_id). A 'paper_basket' fire touches every open position in the
        strategy (all in `legs`) and also retires the basket alert_status so
        it can't refire next tick; a 'paper_leg' fire only clears that one
        position's own sl_value/tp_value so its siblings stay armed.
        """
        from bson import ObjectId
        raw_db = db._db
        now_str = _now_iso()

        try:
            doc = await asyncio.to_thread(raw_db['simulator_strategy'].find_one, {'_id': ObjectId(strategy_id)})
        except Exception as exc:
            self.last_error = f'paper fire_exit lookup error strategy={strategy_id}: {exc}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
            return
        if not doc:
            self.last_error = f'paper fire_exit error: strategy {strategy_id} not found'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
            return

        positions = list(doc.get('positions') or [])
        legs_by_index = {leg.get('position_index'): leg for leg in legs}
        # Two independent basket features can each fire this same strategy —
        # the MTM alert ('paper_basket') and the payoff chart's spot-price-
        # band marker ('paper_basket_marker') — tracked via separate status
        # fields (alert_status vs sl_marker_status) when SAVING (so editing
        # one doesn't clobber the other), but linked here on FIRE: whichever
        # one actually hits retires BOTH, so alert_only mode (where nothing
        # really exits to make the other check moot on its own) can't fire
        # both separately for what's really one basket-level event.
        is_alert_basket_fire = any(leg.get('_scope') == 'paper_basket' for leg in legs)
        is_marker_basket_fire = any(leg.get('_scope') == 'paper_basket_marker' for leg in legs)
        is_basket_fire = is_alert_basket_fire or is_marker_basket_fire

        for index, leg in legs_by_index.items():
            if index is None or not isinstance(index, int) or index >= len(positions):
                continue
            pos = positions[index]
            if not isinstance(pos, dict) or pos.get('exited'):
                continue
            ltp = _safe_float(leg.get('_ltp'))
            entry = _safe_float(pos.get('entry_price'))
            qty = _safe_float(pos.get('quantity'))
            is_sell = str(pos.get('type') or '').strip().upper() == 'SELL'
            pnl = (entry - ltp) * qty if is_sell else (ltp - entry) * qty
            pos['exited'] = True
            pos['exit_price'] = round(ltp, 2)
            pos['exit_time'] = now_str
            pos['pnl'] = round(pnl, 2)
            if not is_basket_fire:
                pos['sl_value'] = 0
                pos['tp_value'] = 0

        # A 'paper_leg' fire on the LAST open leg empties the basket out from
        # under alert_status/sl_marker_status without either ever being told —
        # they're only retired by an actual basket-scope fire above. Left
        # 'active' on an empty basket, they'd silently reattach to whatever
        # positions get added to this strategy next. Closing the last leg by
        # any path at all should retire both, same as a real basket fire does.
        all_exited = bool(positions) and all(not isinstance(p, dict) or p.get('exited') for p in positions)

        update: dict[str, Any] = {'positions': positions}
        unset: dict[str, str] = {}
        if is_basket_fire or all_exited:
            reason = legs[0].get('_reason') if is_basket_fire else 'all_legs_exited'
            update['alert_status'] = 'fired'
            update['alert_fired_reason'] = reason
            update['alert_updated_at'] = now_str
            update['sl_marker_status'] = 'fired'
            update['sl_marker_fired_reason'] = reason
            update['sl_marker_updated_at'] = now_str
        if all_exited:
            unset['sl_upper'] = ''
            unset['sl_lower'] = ''
            # Previously never persisted here (only unset the SL markers above) —
            # _sim_advanced_strategies' advanced-slot count filters on this field,
            # so a strategy fully exited via this path (rather than expiry
            # squareoff, which did persist it) kept occupying its Advanced slot
            # forever. status=2 (closed) mirrors it for the same reason.
            update['all_exited'] = True
            update['status'] = 2

        mongo_update: dict[str, Any] = {'$set': update}
        if unset:
            mongo_update['$unset'] = unset
        try:
            await asyncio.to_thread(
                raw_db['simulator_strategy'].update_one, {'_id': ObjectId(strategy_id)}, mongo_update,
            )
            if all_exited:
                # No open legs left for either side's adjustment to apply to —
                # disable rather than delete so they stay in history (same
                # status=False convention _fire_paper_adjustment uses on a fire).
                await asyncio.to_thread(
                    raw_db['simulator_adjustments'].update_many,
                    {'strategy_id': strategy_id, 'status': {'$ne': False}},
                    {'$set': {'status': False, 'disabled_at': now_str, 'disabled_reason': 'all_legs_exited'}},
                )
        except Exception as exc:
            self.last_error = f'paper fire_exit write error strategy={strategy_id}: {exc}'
            log.error('[SIMULATOR RISK MONITOR] %s', self.last_error)
            return

        self.last_fire = {
            'strategy_id': strategy_id,
            'reason': legs[0].get('_reason'),
            'legs': len(legs),
            'at': now_str,
            'paper': True,
        }
        notify_user(
            'PT_PAPER_POSITION_EXITED',
            f'strategy {strategy_id} — {len(legs)} position(s) exited (reason: {legs[0].get("_reason")})'
            + (' — all legs now closed, alerts/adjustments disabled' if all_exited else ''),
            {'trade_id': strategy_id, 'leg_id': ''},
        )
        for leg in legs:
            self.registry.paper_leg_by_token.pop(str(leg.get('token') or ''), None)
        if is_basket_fire or all_exited:
            self.registry.paper_baskets_by_strategy.pop(strategy_id, None)
            self.registry.paper_alert_configs.pop(strategy_id, None)
            for key in [k for k in self.registry.adjustments if k[0] == strategy_id]:
                self.registry.adjustments.pop(key, None)


    # ── Expiry auto square-off ─────────────────────────────────────────────────

    async def run_startup_expiry_catchup(self) -> None:
        """
        Called once at server startup (see api.py's _auto_expiry_squareoff_catchup).
        Exits any open paper-strategy positions whose expiry date has already passed,
        OR whose expiry is today and 15:29 IST has already passed — at entry price
        (market is closed / the squareoff window was missed while the server was down).
        """
        db = MongoData()
        try:
            await self._auto_squareoff_expired_legs(db, is_startup=True)
        finally:
            try:
                db.close()
            except Exception:
                pass

    async def _auto_squareoff_expired_legs(self, db: MongoData, is_startup: bool = False) -> None:
        """
        Find and exit open paper-strategy positions whose legs have expired.

        is_startup=False (warm-refresh path): runs every ~5 s; at/after 15:29 IST
            exits today's expired legs at LTP (fallback: entry price).
        is_startup=True (server-startup catch-up path): exits legs with
            expiry < today OR (expiry == today and >= 15:29) at entry price —
            because the live 15:29 window was missed while the server was down.
        """
        now = datetime.now(IST)
        today = now.strftime('%Y-%m-%d')
        now_hm = now.strftime('%H:%M')

        if not is_startup and now_hm < EXPIRY_SQUAREOFF_TIME:
            return

        raw_db = db._db
        try:
            docs = list(raw_db['simulator_strategy'].find(
                {'all_exited': {'$ne': True}},
                {'_id': 1, 'instrument': 1, 'strategy_name': 1, 'positions': 1, 'user_id': 1},
            ))
        except Exception as exc:
            log.warning('[EXPIRY SQUAREOFF] DB query error: %s', exc)
            return

        if not docs:
            return

        ltp_map: dict[str, float] = {}
        if not is_startup:
            try:
                from features.broker_gateway import broker_ticker_manager
                ltp_map = dict(broker_ticker_manager.ltp_map or {})
            except Exception:
                pass

        for doc in docs:
            strategy_id = str(doc.get('_id') or '')
            plan = resolve_user_plan(doc.get('user_id'))
            if not plan.get('auto_position_management', True):
                # Same "Free plan = manual close only" rule as the SL/Target
                # watch above — expiry-day squareoff is also an automated
                # exit, so it's skipped here too; the user closes it by hand.
                continue
            positions = doc.get('positions') or []
            indices_to_exit: list[int] = []
            for i, pos in enumerate(positions):
                if not isinstance(pos, dict) or pos.get('exited'):
                    continue
                expiry = str(pos.get('expiry') or '').strip()[:10]
                if not expiry:
                    continue
                if is_startup:
                    eligible = expiry < today or (expiry == today and now_hm >= EXPIRY_SQUAREOFF_TIME)
                else:
                    eligible = expiry == today
                if eligible:
                    indices_to_exit.append(i)

            if not indices_to_exit:
                continue

            try:
                await self._squareoff_expired_paper_positions(
                    db, strategy_id, doc, indices_to_exit, ltp_map, is_startup,
                )
            except Exception as exc:
                log.warning('[EXPIRY SQUAREOFF] strategy=%s error: %s', strategy_id, exc)

    async def _squareoff_expired_paper_positions(
        self,
        db: MongoData,
        strategy_id: str,
        doc: dict,
        indices_to_exit: list[int],
        ltp_map: dict[str, float],
        use_entry_price: bool,
    ) -> None:
        """
        Writes the expiry-exit directly onto the simulator_strategy document.
        Mirrors _fire_paper_exit's Mongo update shape so the frontend sees the
        same exited/exit_price/exit_time/pnl fields it already knows how to render.
        exit_reason is stored per-position (unlike the strategy-level
        alert_fired_reason) so every leg clearly shows why it was closed.
        """
        from bson import ObjectId

        raw_db = db._db
        now_str = _now_iso()
        positions = list(doc.get('positions') or [])
        strategy_name = str(doc.get('strategy_name') or strategy_id)
        exited_count = 0

        for idx in indices_to_exit:
            if idx >= len(positions):
                continue
            pos = positions[idx]
            if not isinstance(pos, dict) or pos.get('exited'):
                continue

            entry_price = _safe_float(pos.get('entry_price'))
            token = str(pos.get('token') or '').strip()

            if use_entry_price:
                exit_price = entry_price
            else:
                ltp = _safe_float(ltp_map.get(token)) if token else 0.0
                exit_price = ltp if ltp > 0 else entry_price

            qty = _safe_float(pos.get('quantity'))
            is_sell = str(pos.get('type') or '').strip().upper() == 'SELL'
            pnl = (entry_price - exit_price) * qty if is_sell else (exit_price - entry_price) * qty

            pos['exited'] = True
            pos['exit_price'] = round(exit_price, 2)
            pos['exit_time'] = now_str
            pos['pnl'] = round(pnl, 2)
            pos['exit_reason'] = 'due to expiry alert'
            exited_count += 1

            print(
                f'[EXPIRY SQUAREOFF] strategy={strategy_name} position[{idx}] '
                f'{pos.get("option_type")} {pos.get("strike")} expiry={pos.get("expiry")} '
                f'exit_price={exit_price:.2f} entry_price={entry_price:.2f} pnl={pnl:.2f}'
                + (' (startup catch-up)' if use_entry_price else ' (real-time 15:29)'),
                flush=True,
            )

        if exited_count == 0:
            return

        all_exited = bool(positions) and all(
            not isinstance(p, dict) or p.get('exited') for p in positions
        )

        update: dict[str, Any] = {
            'positions': positions,
            'alert_fired_reason': 'due to expiry alert',
            'alert_updated_at': now_str,
        }
        unset: dict[str, str] = {}
        if all_exited:
            update['alert_status'] = 'fired'
            update['sl_marker_status'] = 'fired'
            update['sl_marker_fired_reason'] = 'due to expiry alert'
            update['sl_marker_updated_at'] = now_str
            update['all_exited'] = True
            # 2 = closed — see the same status flip in _fire_paper_exit.
            update['status'] = 2
            unset['sl_upper'] = ''
            unset['sl_lower'] = ''

        mongo_update: dict[str, Any] = {'$set': update}
        if unset:
            mongo_update['$unset'] = unset

        try:
            await asyncio.to_thread(
                raw_db['simulator_strategy'].update_one,
                {'_id': ObjectId(strategy_id)},
                mongo_update,
            )
        except Exception as exc:
            log.error('[EXPIRY SQUAREOFF] write error strategy=%s: %s', strategy_id, exc)
            return

        mode = 'startup catch-up, exit_price=entry_price' if use_entry_price else 'real-time, exit_price=ltp_or_entry'
        print(
            f'[EXPIRY SQUAREOFF] ✅ {strategy_name} — {exited_count} position(s) exited '
            f'({mode}) all_exited={all_exited}',
            flush=True,
        )
        notify_user(
            'PT_EXPIRY_SQUAREOFF',
            f'{strategy_name}: {exited_count} position(s) auto-exited due to expiry'
            + (' (server restart catch-up, exit price = entry price)' if use_entry_price else ''),
            {'trade_id': strategy_id, 'leg_id': ''},
        )
        if all_exited:
            self.registry.paper_baskets_by_strategy.pop(strategy_id, None)
            self.registry.paper_alert_configs.pop(strategy_id, None)
            for key in [k for k in self.registry.adjustments if k[0] == strategy_id]:
                self.registry.adjustments.pop(key, None)
            for token, leg in list(self.registry.paper_leg_by_token.items()):
                if str(leg.get('strategy_id') or '') == strategy_id:
                    self.registry.paper_leg_by_token.pop(token, None)


simulator_risk_monitor = SimulatorRiskMonitor()
