"""
strategy_engine.py
------------------
Core orchestrator for the Mini Strangle backtesting engine.

Engine lifecycle:
  1. Emit "started"
  2. Determine first_open_date and first_expiry using expiry_type + 12-day rule
  3. Load all backtest timestamps (start_date → end_date, daily cutoff = position_end_time)
  4. Loop through timestamps:
       a. Manage daily hedge entry / exit
       b. EXPIRY EXIT: on expiry day at position_exit_time_on_expiry → close all
       c. Determine next expiry after close → will reopen same day at position_start_time
       d. Handle re-entry delay countdown after adjustments
       e. OPEN: at position_start_time on valid day when no position is active
       f. MONITOR: normal tick processing (PnL, risk, adjustment checks)
  5. On backtest_end_date at position_end_time → close any open positions, emit "stopped"

Expiry logic:
  current_week  → use nearest available expiry, open immediately on start_date
  next_week     → 12-day rule:
                    days_to_next_week > 12 → open on start_date with next_week expiry
                    days_to_next_week ≤ 12 → wait until current_week expiry day,
                                             use 2-weeks-ahead expiry
  monthly_expiry → use last expiry of current month

Re-entry delay:
  reentry_delay=0  → new positions open at the same tick as adjustment trigger
  reentry_delay=N  → skip N ticks, open at the (N+1)th tick

Daily hedge lifecycle:
  headging_entry_time (e.g. 15:28) → open hedge BUY positions every day
  headging_exit_time  (e.g. 09:16) → close previous day's hedges next morning
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from .models import MiniStrangleRequest
from .option_chain_manager import OptionChainManager
from .strike_selector import StrikeSelector
from .adjustment_engine import AdjustmentEngine, AdjustmentLevels
from .position_manager import PositionManager
from .risk_manager import RiskManager, RiskConfig
from .streaming_controller import StreamingController
from .backtest_reporter import BacktestReporter

logger = logging.getLogger(__name__)


class StrategyEngine:

    def __init__(self, request: MiniStrangleRequest, stream: StreamingController) -> None:
        self.req = request
        self.stream = stream

        self.ocm = OptionChainManager()
        self.selector = StrikeSelector()
        self.adj_engine = AdjustmentEngine()

        self.positions: Optional[PositionManager] = None
        self.risk: Optional[RiskManager] = None
        self.adj_levels: Optional[AdjustmentLevels] = None
        self.adj_shift: int = 1

        # Active expiry being traded
        self._current_expiry: str = ""
        # Earliest date on which positions may be opened (for next_week 12-day rule)
        self._first_open_date: str = ""

        # Control flags
        self._stop_event = asyncio.Event()
        self._positions_open: bool = False
        self._ce_adjusted: bool = False
        self._pe_adjusted: bool = False

        # Re-entry state
        self._reentry_pending: bool = False
        self._reentry_countdown: int = 0
        self._event_reentry_pending: bool = False
        self._event_reentry_reason: str = ""
        self._event_reentry_target_ts: str = ""
        self._event_reentry_expiry: str = ""

        # Hedge tracking
        self._hedge_open_date: str = ""

        # Last processed state (used for final close + summary)
        self._last_chain: list[dict] = []
        self._last_ts: str = ""

        # Monitor event control
        self._post_event_monitor: bool = False   # send monitor on next tick after any event
        self._peak_pnl: float = 0.0              # for running_drawdown calculation
        self._last_minute_pnl_key: str = ""      # dedupe minute_pnl event per HH:MM

        # Realised net PnL tracking (all close events, charges deducted)
        self._cumulative_pnl: float = 0.0
        self._closed_cycles: list[dict] = []
        self._expiry_closed_cycles: list[dict] = []
        self._realized_charges_total: float = 0.0
        self._expiry_realized_charges_total: float = 0.0
        self._expiry_realized_gross_total: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        logger.info(
            f"Engine starting | lot={self.req.lot}×{self.req.lot_size}="
            f"{self.req.lot * self.req.lot_size} | "
            f"{self.req.backtest_start_date} → {self.req.backtest_end_date}"
        )
        await self.stream.send("started", {"status": "started", "message": "Start Backtesting"})

        try:
            timestamps = self.ocm.get_backtest_timestamps(
                self.req.backtest_start_date,
                self.req.backtest_end_date,
                self.req.position_end_time,
                self.req.timeframe,
            )
            if not timestamps:
                await self.stream.send(
                    "error",
                    {
                        "message": (
                            f"No data found between {self.req.backtest_start_date} "
                            f"and {self.req.backtest_end_date}"
                        )
                    },
                )
                return

            # Determine first open date and first expiry
            self._first_open_date, self._current_expiry = self._init_expiry_plan(timestamps)
            logger.info(
                f"Expiry plan | first_open_date={self._first_open_date} | "
                f"first_expiry={self._current_expiry} | expiry_type={self.req.expiry_type}"
            )

            i = 0
            while i < len(timestamps) and not self._stop_event.is_set():
                ts = timestamps[i]
                ts_date = self.ocm._extract_date(ts)
                ts_time = self.ocm._extract_time(ts)

                chain = self.ocm.fetch_chain(ts)
                if not chain:
                    i += 1
                    continue

                # Track last valid state for final close + summary
                self._last_chain = chain
                self._last_ts    = ts

                # ── 1. Expiry exit ─────────────────────────────────────
                if (
                    self._positions_open
                    and self._current_expiry
                    and ts_date == self._current_expiry
                    and ts_time == self.req.position_exit_time_on_expiry
                ):
                    await self._manage_daily_hedges(chain, ts)
                    await self._handle_expiry_exit(chain, ts)
                    i += 1
                    continue

                # ── 2. Re-entry delay countdown ────────────────────────
                if self._reentry_pending:
                    if self._reentry_countdown > 0:
                        self._reentry_countdown -= 1
                        i += 1
                        continue
                    # Countdown finished → open new positions
                    expiry_chain = self._fetch_active_expiry_chain(ts)
                    if not expiry_chain:
                        i += 1
                        continue
                    await self._open_positions(expiry_chain, ts)
                    self._reentry_pending = False
                    i += 1
                    continue

                # ── 3. Event-hit re-entry scheduler ───────────────────
                if self._event_reentry_pending:
                    if self._should_execute_event_reentry(ts):
                        self._reset_expiry_tracking(self._event_reentry_expiry)
                        expiry_chain = self._fetch_active_expiry_chain(ts)
                        if not expiry_chain:
                            i += 1
                            continue
                        await self._open_positions(expiry_chain, ts)
                        self._clear_event_reentry()
                    i += 1
                    continue

                # ── 4. Open positions ──────────────────────────────────
                if (
                    not self._positions_open
                    and ts_date >= self._first_open_date
                    and ts_time == self.req.position_start_time
                ):
                    expiry_chain = self._fetch_active_expiry_chain(ts)
                    if not expiry_chain:
                        i += 1
                        continue
                    await self._open_positions(expiry_chain, ts)
                    i += 1
                    continue

                # ── 5. Normal monitoring tick ──────────────────────────
                if self._positions_open:
                    expiry_chain = self._fetch_active_expiry_chain(ts)
                    if not expiry_chain:
                        i += 1
                        continue
                    # minute_pnl fires inside _process_tick BEFORE hedge events
                    stop_reason = await self._process_tick(expiry_chain, ts)
                    # Hedge lifecycle runs AFTER minute_pnl so hedge events carry current LTPs
                    await self._manage_daily_hedges(expiry_chain, ts)

                    if stop_reason == "adjustment_level_hit":
                        await self._handle_adjustment_reentry(expiry_chain, ts)

                    elif stop_reason:
                        # Close all positions, then either continue via scheduled re-entry or stop.
                        await self._close_for_risk_exit(expiry_chain, ts, stop_reason)
                        should_continue = await self._handle_event_hit_reentry(stop_reason, ts)
                        if not should_continue:
                            self._stop_event.set()
                            break

                await asyncio.sleep(0)
                i += 1

        except asyncio.CancelledError:
            logger.info("Engine cancelled externally")
        except Exception as exc:
            logger.exception(f"Unhandled engine error: {exc}")
            await self.stream.send("error", {"message": str(exc)})
        finally:
            await self._finalise()
            logger.info("Engine shut down cleanly")

    async def _finalise(self) -> None:
        """
        Called once when the backtest ends (naturally or via stop/SL/target).
        1. Close any remaining open positions at last known market price.
        2. Generate and send backtest_summary.
        3. Send stopped.
        """
        exit_ts = self._last_ts or f"{self.req.backtest_end_date}T{self.req.position_end_time}:00"

        if self.positions:
            # Close remaining sell positions at last market price
            ce_pos = self.positions.get_active_sell("CE")
            pe_pos = self.positions.get_active_sell("PE")
            if ce_pos or pe_pos:
                closed_positions = [p for p in (ce_pos, pe_pos) if p]
                ce_exit = self._resolve_exit_price(self._last_chain, ce_pos, "CE")
                pe_exit = self._resolve_exit_price(self._last_chain, pe_pos, "PE")
                self.positions.close_all_sells(ce_exit, pe_exit, exit_ts)

                # Close hedges together with sells at backtest end
                if self.positions.has_active_hedges():
                    ce_h = self.positions.get_active_hedge("CE")
                    pe_h = self.positions.get_active_hedge("PE")
                    closed_positions.extend([p for p in (ce_h, pe_h) if p])
                    ce_h_exit = self._resolve_exit_price(self._last_chain, ce_h, "CE")
                    pe_h_exit = self._resolve_exit_price(self._last_chain, pe_h, "PE")
                    self.positions.close_all_hedges(ce_h_exit, pe_h_exit, exit_ts)

                cycle_pnl, cumulative_pnl, cycle_charges = self._record_cycle(
                    closed_positions,
                    reason="backtest_end",
                    ts=exit_ts,
                )

                summary = self.positions.summary()
                await self._emit(
                    "positions_closed",
                    {
                        "reason": "backtest_end",
                        "closed_positions": summary["sell_closed_positions"],
                        "closed_hedges": summary["hedge_closed_positions"],
                        "cycle_pnl": cycle_pnl,
                        "cycle_charges": cycle_charges,
                        "cumulative_pnl": cumulative_pnl,
                        "cumulative_pnl_pct": self._cumulative_pct(),
                        "timestamp": exit_ts,
                    },
                )

            # Close any remaining standalone hedges (no sells open)
            elif self.positions.has_active_hedges():
                ce_h = self.positions.get_active_hedge("CE")
                pe_h = self.positions.get_active_hedge("PE")
                closed_positions = [p for p in (ce_h, pe_h) if p]
                ce_h_exit = self._resolve_exit_price(self._last_chain, ce_h, "CE")
                pe_h_exit = self._resolve_exit_price(self._last_chain, pe_h, "PE")
                self.positions.close_all_hedges(ce_h_exit, pe_h_exit, exit_ts)
                self._record_cycle(closed_positions, reason="hedge_final_exit", ts=exit_ts)

            # Generate and send backtest_summary
            summary_data = BacktestReporter(
                sell_closed=self.positions.sell_closed_positions,
                hedge_closed=self.positions.hedge_closed_positions,
                margin_used=self.positions.margin_used,
                cycle_history=self._closed_cycles,
                final_cumulative_pnl=self._cumulative_pnl,
            ).generate()

            await self.stream.send("backtest_summary", summary_data)

        await self.stream.send("stopped", {"status": "stopped", "message": "Stopped"})
        self.stream.stop()
        self.ocm.close()

    def stop(self) -> None:
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Risk exit handler
    # ------------------------------------------------------------------

    async def _close_for_risk_exit(
        self, chain: list[dict], ts: str, reason: str
    ) -> None:
        """
        Close all sells + hedges when SL / target / trailing-SL is hit.
        Emits the named risk event then positions_closed, both carrying
        cycle_pnl (this trade only) and cumulative_pnl (all trades so far).
        """
        if not self.positions:
            return

        ce_pos = self.positions.get_active_sell("CE")
        pe_pos = self.positions.get_active_sell("PE")
        closed_positions = [p for p in (ce_pos, pe_pos) if p]

        ce_exit = self._resolve_exit_price(chain, ce_pos, "CE")
        pe_exit = self._resolve_exit_price(chain, pe_pos, "PE")
        self.positions.close_all_sells(ce_exit, pe_exit, ts)

        if self.positions.has_active_hedges():
            ce_h = self.positions.get_active_hedge("CE")
            pe_h = self.positions.get_active_hedge("PE")
            closed_positions.extend([p for p in (ce_h, pe_h) if p])
            ce_h_exit = self._resolve_exit_price(chain, ce_h, "CE")
            pe_h_exit = self._resolve_exit_price(chain, pe_h, "PE")
            self.positions.close_all_hedges(ce_h_exit, pe_h_exit, ts)

        cycle_pnl, cumulative_pnl, cycle_charges = self._record_cycle(
            closed_positions,
            reason=reason,
            ts=ts,
        )

        self._positions_open = False
        self.adj_levels = None

        # Named risk event carries both PnL views
        await self._emit(
            reason,
            {
                "cycle_pnl":      cycle_pnl,
                "cumulative_pnl": cumulative_pnl,
                "cycle_charges":  cycle_charges,
                "timestamp":      ts,
            },
        )

        summary = self.positions.summary()
        await self._emit(
            "positions_closed",
            {
                "reason":           reason,
                "closed_positions": summary["sell_closed_positions"],
                "closed_hedges":    summary["hedge_closed_positions"],
                "cycle_pnl":        cycle_pnl,
                "cumulative_pnl":   cumulative_pnl,
                "cumulative_pnl_pct": self._cumulative_pct(),
                "timestamp":        ts,
            },
        )
        logger.info(
            f"Risk exit | reason={reason} | cycle_pnl={cycle_pnl:.2f} "
            f"| cumulative_pnl={cumulative_pnl:.2f} | ts={ts}"
        )

    # ------------------------------------------------------------------
    # Event emit helper
    # ------------------------------------------------------------------

    _NO_POST_MONITOR = {"monitor", "started", "stopped", "error"}

    async def _emit(self, event_type: str, data: dict) -> None:
        """
        Send one event via the stream.
        Any meaningful event (not monitor/started/stopped/error) schedules
        a monitor snapshot on the very next tick.
        """
        await self.stream.send(event_type, data)
        if event_type not in self._NO_POST_MONITOR:
            self._post_event_monitor = True

    def _running_drawdown(self, current_pnl: float) -> float:
        """Track peak PnL and return current drawdown from that peak."""
        if current_pnl > self._peak_pnl:
            self._peak_pnl = current_pnl
        return round(max(0.0, self._peak_pnl - current_pnl), 2)

    def _record_cycle(self, closed_positions: list, reason: str, ts: str) -> tuple[float, float, float]:
        """
        Realise one closure cycle and update cumulative net PnL.

        cycle_pnl = gross cycle pnl - cycle charges.
        cumulative_pnl = running sum of cycle_pnl.
        """
        if not closed_positions:
            return 0.0, self._cumulative_pnl, 0.0

        gross_pnl = round(sum(pos.pnl for pos in closed_positions), 2)
        charges = round(BacktestReporter.calculate_charges(closed_positions), 2)
        cycle_pnl = round(gross_pnl - charges, 2)

        self._cumulative_pnl = round(self._cumulative_pnl + cycle_pnl, 2)
        self._realized_charges_total = round(self._realized_charges_total + charges, 2)
        self._expiry_realized_charges_total = round(self._expiry_realized_charges_total + charges, 2)
        self._expiry_realized_gross_total = round(self._expiry_realized_gross_total + gross_pnl, 2)
        self._closed_cycles.append(
            {
                "timestamp": ts,
                "reason": reason,
                "gross_pnl": gross_pnl,
                "charges": charges,
                "net_pnl": cycle_pnl,
                "cumulative_pnl": self._cumulative_pnl,
            }
        )
        self._expiry_closed_cycles.append(
            {
                "timestamp": ts,
                "reason": reason,
                "gross_pnl": gross_pnl,
                "charges": charges,
                "net_pnl": cycle_pnl,
                "expiry": self._current_expiry,
            }
        )
        return cycle_pnl, self._cumulative_pnl, charges

    def _cumulative_pct(self) -> float:
        """cumulative_pnl as % of margin_used (0.0 if no positions yet)."""
        margin = self.positions.margin_used if self.positions else 0.0
        if not margin:
            return 0.0
        return round(self._cumulative_pnl / margin * 100, 4)

    def _realized_charges(self) -> float:
        """Charges already locked in closed cycles."""
        return self._realized_charges_total

    def _expiry_realized_charges(self) -> float:
        """Charges already locked in the current expiry's closed cycles."""
        return self._expiry_realized_charges_total

    def _expiry_realized_gross_pnl(self) -> float:
        """Gross PnL already locked in the current expiry's closed cycles."""
        return self._expiry_realized_gross_total

    @staticmethod
    def _estimate_charges(positions: list) -> float:
        """
        Fast-path charges estimation for live positions using current_price as exit_price.
        Avoids allocating temporary namespaces on every tick.
        """
        total = 0.0
        for pos in positions:
            entry_turnover = pos.entry_price * pos.quantity
            exit_turnover = pos.current_price * pos.quantity
            turnover = entry_turnover + exit_turnover

            brokerage = 40.0
            stt = entry_turnover * 0.0005
            exchange = turnover * 0.00053
            sebi = turnover * 0.000001
            gst = (brokerage + exchange) * 0.18

            total += brokerage + stt + exchange + sebi + gst

        return round(total, 2)

    def _estimated_active_charges(self) -> float:
        """
        Estimate charges for active positions if they were closed at current prices now.
        This keeps minute_pnl comparable to final backtest_summary.net_pnl.
        """
        if not self.positions:
            return 0.0

        active_positions = self.positions.sell_positions + self.positions.hedge_positions
        if not active_positions:
            return 0.0

        return self._estimate_charges(active_positions)

    def _estimated_active_expiry_charges(self) -> float:
        """
        Estimate charges for active positions belonging to the current expiry only.
        """
        if not self.positions:
            return 0.0

        active_positions = [
            pos
            for pos in (self.positions.sell_positions + self.positions.hedge_positions)
            if pos.expiry == self._current_expiry
        ]
        if not active_positions:
            return 0.0

        return self._estimate_charges(active_positions)

    def _current_expiry_open_gross_pnl(self) -> float:
        """Gross mark-to-market PnL for open positions in the current expiry."""
        if not self.positions:
            return 0.0
        open_positions = [
            pos
            for pos in (self.positions.sell_positions + self.positions.hedge_positions)
            if pos.expiry == self._current_expiry
        ]
        return round(sum(pos.pnl for pos in open_positions), 2)

    def _current_expiry_overall_pnl(self) -> dict:
        """
        Build current expiry level pnl snapshot.
        Includes closed and open positions, hedges, and charges for this expiry only.
        """
        realized_gross = self._expiry_realized_gross_pnl()
        open_gross = self._current_expiry_open_gross_pnl()
        gross_pnl = round(realized_gross + open_gross, 2)

        realized_charges = self._expiry_realized_charges()
        estimated_open_charges = self._estimated_active_expiry_charges()
        total_charges = round(realized_charges + estimated_open_charges, 2)
        net_pnl = round(gross_pnl - total_charges, 2)

        return {
            "expiry": self._current_expiry,
            "gross_pnl": gross_pnl,
            "net_pnl": net_pnl,
            "closed_gross_pnl": realized_gross,
            "open_gross_pnl": open_gross,
            "realized_charges": realized_charges,
            "estimated_open_charges": estimated_open_charges,
            "total_charges": total_charges,
        }

    def _reset_expiry_tracking(self, new_expiry: str) -> None:
        """Start fresh expiry-level tracking when the engine moves to a new expiry."""
        self._current_expiry = new_expiry
        self._expiry_closed_cycles = []
        self._expiry_realized_charges_total = 0.0
        self._expiry_realized_gross_total = 0.0

    def _clear_event_reentry(self) -> None:
        self._event_reentry_pending = False
        self._event_reentry_reason = ""
        self._event_reentry_target_ts = ""
        self._event_reentry_expiry = ""

    def _should_execute_event_reentry(self, ts: str) -> bool:
        """Execute on the first timestamp at or after the scheduled target."""
        return bool(self._event_reentry_target_ts) and ts >= self._event_reentry_target_ts

    def _resolve_next_week_expiry(self, ts: str) -> str:
        """Always pick the next expiry strictly after the current active expiry."""
        expiries = self.ocm.get_available_expiries(ts)
        future = [expiry for expiry in expiries if expiry > self._current_expiry]
        return future[0] if future else ""

    def _build_target_ts(self, target_date: str, target_time: str) -> str:
        return f"{target_date}T{target_time}:00"

    def _fetch_active_expiry_chain(self, ts: str) -> list[dict]:
        """
        Return chain data only for the active expiry.
        Using mixed-expiry fallback data can produce incorrect live PnL jumps.
        """
        if not self._current_expiry:
            return []
        return self.ocm.fetch_chain_for_expiry(ts, self._current_expiry)

    def _lookup_position_price(self, chain: list[dict], pos, option_type: str) -> Optional[float]:
        """
        Safe live price lookup for an open position.
        If a premium is missing, keep the previous valid price instead of forcing zero.
        """
        if not pos:
            return None

        if option_type == "CE":
            price = self.ocm.get_ce_premium(chain, pos.strike)
        else:
            price = self.ocm.get_pe_premium(chain, pos.strike)

        if price <= 0 and pos.current_price > 0:
            logger.warning(
                "Missing %s premium for strike=%s expiry=%s at live tick; keeping last price %.2f",
                option_type,
                pos.strike,
                pos.expiry,
                pos.current_price,
            )
            return None
        return price

    def _resolve_exit_price(self, chain: list[dict], pos, option_type: str) -> float:
        """
        Safe close price lookup.
        Never close an option leg at synthetic zero if the market quote is missing.
        """
        if not pos:
            return 0.0

        live_price = self._lookup_position_price(chain, pos, option_type)
        if live_price is not None:
            return live_price

        fallback = pos.current_price or pos.entry_price
        logger.warning(
            "Using fallback close price %.2f for %s strike=%s expiry=%s",
            fallback,
            option_type,
            pos.strike,
            pos.expiry,
        )
        return fallback

    def _target_within_backtest(self, target_ts: str) -> bool:
        """Ensure the scheduled re-entry still lies inside the backtest window."""
        if not target_ts:
            return False
        target_date = self.ocm._extract_date(target_ts)
        target_time = self.ocm._extract_time(target_ts)
        if target_date > self.req.backtest_end_date:
            return False
        if target_date == self.req.backtest_end_date and target_time > self.req.position_end_time:
            return False
        return True

    async def _schedule_event_reentry(
        self,
        reason: str,
        event_hit_position_status: int,
        scheduled_ts: str,
        target_expiry: str,
    ) -> None:
        self._event_reentry_pending = True
        self._event_reentry_reason = reason
        self._event_reentry_target_ts = scheduled_ts
        self._event_reentry_expiry = target_expiry

        await self._emit(
            "event_reentry_scheduled",
            {
                "reason": reason,
                "event_hit_position_status": event_hit_position_status,
                "event_hit_entry_condition": self.req.event_hit_entry_condition,
                "scheduled_for": scheduled_ts,
                "target_expiry": target_expiry,
                "timestamp": scheduled_ts,
            },
        )

    async def _handle_event_hit_reentry(self, reason: str, ts: str) -> bool:
        """
        Decide whether a risk event should stop the backtest or schedule a fresh cycle.
        Returns True when the engine should continue running.
        """
        status = self.req.event_hit_position_status
        target_expiry = self._resolve_next_week_expiry(ts)
        if not target_expiry:
            logger.info("Risk event continuation skipped: no future expiry available")
            return False

        event_date = self.ocm._extract_date(ts)
        event_time = self.ocm._extract_time(ts)
        scheduled_ts = ""

        if status == 0:
            if self.req.backtest_end_date < self._current_expiry:
                logger.info(
                    "Risk event continuation stopped | end_date=%s before current_expiry=%s",
                    self.req.backtest_end_date,
                    self._current_expiry,
                )
                return False

            scheduled_date = self._current_expiry
            scheduled_time = self.req.position_start_time
            # Practical fallback: if the expiry-day start time has already passed,
            # reopen on the next trading day at the normal start time.
            if scheduled_date == event_date and event_time >= scheduled_time:
                scheduled_date = self.ocm._calendar.next_trading_day(scheduled_date)
            scheduled_ts = self._build_target_ts(scheduled_date, scheduled_time)

        elif status == 1:
            wait_days = max(1, int(self.req.event_hit_entry_condition or 1))
            scheduled_date = self.ocm._calendar.add_trading_days(event_date, wait_days)
            scheduled_ts = self._build_target_ts(scheduled_date, self.req.position_start_time)

        elif status == 2:
            wait_minutes = max(1, int(self.req.event_hit_entry_condition or 1))
            event_dt = datetime.fromisoformat(ts)
            candidate_dt = event_dt + timedelta(minutes=wait_minutes)
            market_close_dt = datetime.fromisoformat(
                self._build_target_ts(event_date, self.req.position_end_time)
            )

            if candidate_dt.date().isoformat() == event_date and candidate_dt <= market_close_dt:
                scheduled_ts = candidate_dt.replace(second=0, microsecond=0).isoformat(timespec="seconds")
            else:
                next_date = self.ocm._calendar.next_trading_day(event_date)
                scheduled_ts = self._build_target_ts(next_date, self.req.position_start_time)
        else:
            logger.warning("Unknown event_hit_position_status=%s; stopping backtest", status)
            return False

        if not self._target_within_backtest(scheduled_ts):
            logger.info(
                "Risk event continuation stopped | scheduled_for=%s outside backtest range",
                scheduled_ts,
            )
            return False

        await self._schedule_event_reentry(reason, status, scheduled_ts, target_expiry)
        logger.info(
            "Risk event continuation scheduled | reason=%s | mode=%s | scheduled_for=%s | expiry=%s",
            reason,
            status,
            scheduled_ts,
            target_expiry,
        )
        return True

    # ------------------------------------------------------------------
    # Expiry plan initialisation
    # ------------------------------------------------------------------

    def _init_expiry_plan(self, timestamps: list[str]) -> tuple[str, str]:
        """
        Determine (first_open_date, first_expiry) based on expiry_type.

        current_week  → open on backtest_start_date, use nearest expiry
        next_week     → apply 12-day rule (see below)
        monthly_expiry→ open on backtest_start_date, use last expiry of current month

        next_week 12-day rule:
          - days_to_next_week_expiry > 12  → open on start_date, use next_week expiry
          - days_to_next_week_expiry ≤ 12  → wait until current_week expiry day,
                                             use 2-weeks-ahead expiry
        """
        start_date = self.req.backtest_start_date

        # Find the first timestamp on or after start_date to get available expiries
        first_ts = next(
            (ts for ts in timestamps if self.ocm._extract_date(ts) >= start_date),
            timestamps[0] if timestamps else None,
        )
        if not first_ts:
            return start_date, ""

        expiries = self.ocm.get_available_expiries(first_ts)
        if not expiries:
            logger.warning("No expiries found in option chain — using empty expiry")
            return start_date, ""

        expiry_type = self.req.expiry_type
        start = date.fromisoformat(start_date)

        if expiry_type == "current_week":
            # Nearest available expiry, open immediately
            return start_date, expiries[0]

        elif expiry_type == "next_week":
            if len(expiries) < 2:
                return start_date, expiries[0]

            current_week_exp = expiries[0]  # nearest
            next_week_exp    = expiries[1]  # second

            days_to_next = (date.fromisoformat(next_week_exp) - start).days

            if days_to_next > 12:
                # Case 1: open on start_date with next_week expiry
                logger.info(f"next_week 12-day rule: Case 1 | days={days_to_next} > 12 | expiry={next_week_exp}")
                return start_date, next_week_exp
            else:
                # Case 2: wait until current_week expiry day, use 2 weeks ahead
                two_weeks_exp = expiries[2] if len(expiries) >= 3 else expiries[-1]
                logger.info(
                    f"next_week 12-day rule: Case 2 | days={days_to_next} ≤ 12 | "
                    f"wait until={current_week_exp} | expiry={two_weeks_exp}"
                )
                return current_week_exp, two_weeks_exp

        elif expiry_type == "monthly_expiry":
            # Use last expiry of the current month
            current_month = start.month
            monthly = [e for e in expiries if date.fromisoformat(e).month == current_month]
            first_expiry = monthly[-1] if monthly else expiries[-1]
            return start_date, first_expiry

        # Fallback
        return start_date, expiries[0]

    def _resolve_next_expiry(self, ts: str) -> str:
        """
        After closing on the current expiry, determine the next expiry.
        Called immediately after expiry exit on the same day.
        """
        expiries = self.ocm.get_available_expiries(ts)
        if not expiries:
            return self._current_expiry

        # All expiries strictly after the current one
        future = [e for e in expiries if e > self._current_expiry]

        if self.req.expiry_type == "current_week":
            # Next nearest expiry
            return future[0] if future else expiries[-1]

        elif self.req.expiry_type == "next_week":
            # The next week from today — apply 12-day rule again
            today = date.fromisoformat(self.ocm._extract_date(ts))
            for exp in future:
                if (date.fromisoformat(exp) - today).days > 12:
                    return exp
            return future[-1] if future else expiries[-1]

        elif self.req.expiry_type == "monthly_expiry":
            # Last expiry of the next month
            current_exp_month = date.fromisoformat(self._current_expiry).month
            next_month = current_exp_month % 12 + 1
            next_monthly = [e for e in future if date.fromisoformat(e).month == next_month]
            return next_monthly[-1] if next_monthly else (future[0] if future else expiries[-1])

        return future[0] if future else expiries[-1]

    # ------------------------------------------------------------------
    # Expiry exit handler
    # ------------------------------------------------------------------

    async def _handle_expiry_exit(self, chain: list[dict], ts: str) -> None:
        """
        Close ALL active sell + hedge positions at position_exit_time_on_expiry.
        Then determine next expiry so positions reopen at position_start_time same day.
        """
        # Fetch expiry-filtered chain for accurate exit prices
        expiry_chain = self.ocm.fetch_chain_for_expiry(ts, self._current_expiry) or chain

        ce_pos = self.positions.get_active_sell("CE")
        pe_pos = self.positions.get_active_sell("PE")
        closed_positions = [p for p in (ce_pos, pe_pos) if p]

        ce_exit = self._resolve_exit_price(expiry_chain, ce_pos, "CE")
        pe_exit = self._resolve_exit_price(expiry_chain, pe_pos, "PE")
        self.positions.close_all_sells(ce_exit, pe_exit, ts)

        # Close hedges too
        if self.positions.has_active_hedges():
            ce_h = self.positions.get_active_hedge("CE")
            pe_h = self.positions.get_active_hedge("PE")
            closed_positions.extend([p for p in (ce_h, pe_h) if p])
            ce_h_exit = self._resolve_exit_price(chain, ce_h, "CE")
            pe_h_exit = self._resolve_exit_price(chain, pe_h, "PE")
            self.positions.close_all_hedges(ce_h_exit, pe_h_exit, ts)

        cycle_pnl, cumulative_pnl, cycle_charges = self._record_cycle(
            closed_positions,
            reason="expiry_exit",
            ts=ts,
        )

        self._positions_open = False
        self.adj_levels = None

        summary = self.positions.summary()
        await self._emit(
            "positions_closed",
            {
                "reason": "expiry_exit",
                "expiry": self._current_expiry,
                "closed_positions": summary["sell_closed_positions"],
                "closed_hedges": summary["hedge_closed_positions"],
                "cycle_pnl": cycle_pnl,
                "cycle_charges": cycle_charges,
                "cumulative_pnl": cumulative_pnl,
                "cumulative_pnl_pct": self._cumulative_pct(),
                "timestamp": ts,
            },
        )
        logger.info(f"Expiry exit complete | expiry={self._current_expiry} | ts={ts}")

        # Determine next expiry — new positions will open same day at position_start_time
        prev_expiry = self._current_expiry
        self._reset_expiry_tracking(self._resolve_next_expiry(ts))
        logger.info(f"Next expiry: {prev_expiry} → {self._current_expiry}")

        await self._emit(
            "next_expiry_scheduled",
            {
                "previous_expiry": prev_expiry,
                "next_expiry": self._current_expiry,
                "reopen_at": self.req.position_start_time,
                "timestamp": ts,
            },
        )

    # ------------------------------------------------------------------
    # Tick processor
    # ------------------------------------------------------------------

    async def _process_tick(self, chain: list[dict], ts: str) -> Optional[str]:
        ts_time = self.ocm._extract_time(ts)

        spot    = self.ocm.get_spot_price(chain)
        atm     = self.ocm.get_atm_strike(chain, spot)
        strikes = self.ocm.get_sorted_strikes(chain)
        fifth_prem = self.ocm.get_5th_otm_premium(chain, atm, strikes)

        adj_config = self.adj_engine.get_config(fifth_prem)
        self.adj_shift = adj_config.shift

        ce_pos = self.positions.get_active_sell("CE")
        pe_pos = self.positions.get_active_sell("PE")

        # Payoff graph markers: use sell closest to ATM per side.
        # When multiple sells exist (after adjustments), the one nearest ATM
        # determines the upper/lower trigger lines shown on the chart.
        _ce_sells = [p for p in self.positions.sell_positions if p.option_type == "CE" and p.position_status == 1]
        _pe_sells = [p for p in self.positions.sell_positions if p.option_type == "PE" and p.position_status == 1]
        ce_adj = min(_ce_sells, key=lambda p: abs(p.strike - atm)) if _ce_sells else ce_pos
        pe_adj = min(_pe_sells, key=lambda p: abs(p.strike - atm)) if _pe_sells else pe_pos
        if ce_adj and pe_adj:
            self.adj_levels = self.adj_engine.calculate_levels(
                ce_adj.strike, pe_adj.strike, strikes, fifth_prem, spot
            )

        ce_ltp = self._lookup_position_price(chain, ce_pos, "CE")
        pe_ltp = self._lookup_position_price(chain, pe_pos, "PE")

        ce_hedge = self.positions.get_active_hedge("CE")
        pe_hedge = self.positions.get_active_hedge("PE")
        ce_hedge_ltp = self._lookup_position_price(chain, ce_hedge, "CE")
        pe_hedge_ltp = self._lookup_position_price(chain, pe_hedge, "PE")

        self.positions.update_prices(ce_ltp, pe_ltp, ce_hedge_ltp, pe_hedge_ltp)

        total_pnl        = self.positions.total_pnl      # active + closed (drawdown use)
        realized_charges = self._realized_charges()
        estimated_active_charges = self._estimated_active_charges()
        expiry_snapshot = self._current_expiry_overall_pnl()
        # pnl = current expiry gross (active sells + active hedges + closed hedge cycles this expiry)
        expiry_pnl       = expiry_snapshot["gross_pnl"]
        net_pnl_with_charges = expiry_snapshot["net_pnl"]
        total_charges    = expiry_snapshot["total_charges"]
        expiry_risk_pnl = expiry_snapshot["gross_pnl"]   # SL/Target/TSL use gross PnL (without charges)
        overall_pnl_pct = self.positions.overall_pnl_pct
        drawdown        = self._running_drawdown(total_pnl)
        active_count    = (
            len(self.positions.sell_positions) +
            len(self.positions.hedge_positions)
        )

        # Emit compact minute-level PnL event (timestamp + pnl only).
        # This is separate from monitor and does not change existing monitor/log logic.
        minute_key = ts[:16] if len(ts) >= 16 else ts
        if minute_key != self._last_minute_pnl_key:
            self._last_minute_pnl_key = minute_key
            await self.stream.send(
                "minute_pnl",
                {
                    "timestamp": ts,
                    "pnl": expiry_pnl,
                    "pnl_without_charges": expiry_pnl,
                    "total_charges": total_charges,
                    "realized_charges": expiry_snapshot["realized_charges"],
                    "estimated_active_charges": expiry_snapshot["estimated_open_charges"],
                    "pnl_with_charges": net_pnl_with_charges,
                    "cumulative_pnl": round(self._cumulative_pnl, 2),
                    "current_expiry": self._current_expiry,
                    "expiry_gross_pnl": expiry_snapshot["gross_pnl"],
                    "expiry_net_pnl": expiry_snapshot["net_pnl"],
                    "expiry_closed_gross_pnl": expiry_snapshot["closed_gross_pnl"],
                    "expiry_open_gross_pnl": expiry_snapshot["open_gross_pnl"],
                    "expiry_total_charges": expiry_snapshot["total_charges"],
                    "expiry_realized_charges": expiry_snapshot["realized_charges"],
                    "expiry_estimated_open_charges": expiry_snapshot["estimated_open_charges"],
                    # ── Active position snapshot ──────────────────────────
                    "lot": self.req.lot,
                    "lot_size": self.req.lot_size,
                    "quantity": self.positions.quantity if self.positions else 0,
                    "ce_strike": ce_pos.strike if ce_pos else "",
                    "ce_entry_price": ce_pos.entry_price if ce_pos else "",
                    "ce_ltp": round(ce_ltp, 2) if ce_ltp is not None else (round(ce_pos.current_price, 2) if ce_pos else ""),
                    "ce_pnl": round(ce_pos.pnl, 2) if ce_pos else "",
                    "pe_strike": pe_pos.strike if pe_pos else "",
                    "pe_entry_price": pe_pos.entry_price if pe_pos else "",
                    "pe_ltp": round(pe_ltp, 2) if pe_ltp is not None else (round(pe_pos.current_price, 2) if pe_pos else ""),
                    "pe_pnl": round(pe_pos.pnl, 2) if pe_pos else "",
                    "ce_hedge_strike": ce_hedge.strike if ce_hedge else "",
                    "ce_hedge_entry_price": ce_hedge.entry_price if ce_hedge else "",
                    "ce_hedge_ltp": round(ce_hedge_ltp, 2) if ce_hedge_ltp is not None else (round(ce_hedge.current_price, 2) if ce_hedge else ""),
                    "ce_hedge_pnl": round(ce_hedge.pnl, 2) if ce_hedge else "",
                    "pe_hedge_strike": pe_hedge.strike if pe_hedge else "",
                    "pe_hedge_entry_price": pe_hedge.entry_price if pe_hedge else "",
                    "pe_hedge_ltp": round(pe_hedge_ltp, 2) if pe_hedge_ltp is not None else (round(pe_hedge.current_price, 2) if pe_hedge else ""),
                    "pe_hedge_pnl": round(pe_hedge.pnl, 2) if pe_hedge else "",
                },
            )

        # ── Monitor emit rules ─────────────────────────────────────────
        # Send monitor at: position_start_time, position_end_time,
        # OR the tick immediately after any meaningful event.
        emit_monitor = (
            ts_time == self.req.position_start_time
            or ts_time == self.req.position_end_time
            or self._post_event_monitor
        )
        self._post_event_monitor = False  # reset — events fired below may re-set it

        if emit_monitor:
            await self.stream.send(
                "monitor",
                {
                    "timestamp": ts,
                    "spot": spot,
                    "atm": atm,
                    "current_expiry": self._current_expiry,
                    "fifth_otm_premium": round(fifth_prem, 2),
                    "adjustment_shift": self.adj_shift,
                    "volatility": adj_config.volatility,
                    "current_pnl": round(total_pnl, 2),
                    "current_pnl_with_charges": net_pnl_with_charges,
                    "current_pnl_pct": round(overall_pnl_pct, 4),
                    "total_charges": total_charges,
                    "cumulative_pnl": round(self._cumulative_pnl, 2),
                    "cumulative_pnl_pct": self._cumulative_pct(),
                    "expiry_gross_pnl": expiry_snapshot["gross_pnl"],
                    "expiry_net_pnl": expiry_snapshot["net_pnl"],
                    "expiry_closed_gross_pnl": expiry_snapshot["closed_gross_pnl"],
                    "expiry_open_gross_pnl": expiry_snapshot["open_gross_pnl"],
                    "expiry_total_charges": expiry_snapshot["total_charges"],
                    "running_drawdown": drawdown,
                    "active_positions_count": active_count,
                    "positions": self.positions.summary(),
                    "adjustment_levels": self.adj_levels.to_dict() if self.adj_levels else {},
                    "risk": self.risk.status(expiry_risk_pnl),
                },
            )

        # ── Risk exits ─────────────────────────────────────────────────
        if self.risk.check_stoploss(expiry_risk_pnl):
            return "stoploss_hit"
        if self.risk.check_target(expiry_risk_pnl):
            return "target_hit"
        if self.risk.check_trailing_sl(expiry_risk_pnl):
            return "trailing_sl_hit"

        # ── OTM distance adjustment check ──────────────────────────────
        otm_hit = await self._check_otm_adjustments(chain, atm, strikes, ts)

        if otm_hit and self.req.adjustment_triggered_waiting == 0:
            # Instant mode: OTM condition alone triggers adjustment
            await self._emit(
                "adjustment_triggered",
                {
                    "side": "otm_immediate",
                    "message": "OTM condition met — immediate adjustment triggered",
                    "spot_price": spot,
                    "upper_adjustment_price": round(self.adj_levels.upper_price, 2) if self.adj_levels else 0,
                    "lower_adjustment_price": round(self.adj_levels.lower_price, 2) if self.adj_levels else 0,
                    "timestamp": ts,
                },
            )
            return "adjustment_level_hit"

        # ── Spot price vs adjustment levels (waiting=1 mode) ───────────
        if self.adj_levels:
            if await self._check_spot_vs_levels(spot, ts):
                return "adjustment_level_hit"

        return None

    # ------------------------------------------------------------------
    # Open positions
    # ------------------------------------------------------------------

    async def _open_positions(self, chain: list[dict], ts: str) -> None:
        try:
            spot    = self.ocm.get_spot_price(chain)
            atm     = self.ocm.get_atm_strike(chain, spot)
            strikes = self.ocm.get_sorted_strikes(chain)
            fifth_prem = self.ocm.get_5th_otm_premium(chain, atm, strikes)

            selected = self.selector.select_strikes(
                chain, atm, strikes, fifth_prem, self.ocm,
                current_pnl=self._cumulative_pnl,
            )

            if self.positions is None:
                self.positions = PositionManager(
                    lot=self.req.lot,
                    lot_size=self.req.lot_size,
                    expiry=self._current_expiry,
                )
            else:
                self.positions.expiry = self._current_expiry

            self.positions.open_sell("CE", selected.ce_strike, selected.ce_entry_price, ts)
            self.positions.open_sell("PE", selected.pe_strike, selected.pe_entry_price, ts)

            initial_premium = (
                (selected.ce_entry_price + selected.pe_entry_price) * self.positions.quantity
            )

            self.risk = RiskManager(
                config=RiskConfig(
                    stoploss_status=self.req.stoploss_status,
                    stoploss_type=self.req.stoploss_type,
                    stoploss_value=self.req.stoploss_value,
                    target_status=self.req.target_status,
                    target_type=self.req.target_type,
                    target_value=self.req.target_value,
                    trailing_sl_status=self.req.trailing_sl_status,
                    trailing_sl_x=self.req.trailing_sl_x,
                    trailing_sl_y=self.req.trailing_sl_y,
                ),
                initial_premium=initial_premium,
            )

            self.adj_levels = self.adj_engine.calculate_levels(
                selected.ce_strike, selected.pe_strike, strikes, fifth_prem, spot
            )
            self.adj_shift = self.adj_levels.shift
            self._positions_open = True
            self._ce_adjusted    = False
            self._pe_adjusted    = False

            await self._emit(
                "positions_opened",
                {
                    "timestamp": ts,
                    "expiry": self._current_expiry,
                    "spot": spot,
                    "atm": atm,
                    "ce_strike": selected.ce_strike,
                    "pe_strike": selected.pe_strike,
                    "ce_entry_price": selected.ce_entry_price,
                    "pe_entry_price": selected.pe_entry_price,
                    "fifth_otm_premium": round(fifth_prem, 2),
                    "strike_number": selected.strike_number,
                    "lot": self.req.lot,
                    "lot_size": self.req.lot_size,
                    "quantity": self.positions.quantity,
                    "initial_premium_collected": round(initial_premium, 2),
                    "margin_used": self.positions.margin_used,
                    "adjustment_levels": self.adj_levels.to_dict(),
                },
            )
            # Fresh expiry cycle must visibly start from zero.
            await self.stream.send(
                "minute_pnl",
                {
                    "timestamp": ts,
                    "pnl": 0.0,
                    "pnl_without_charges": 0.0,
                    "total_charges": round(self._realized_charges(), 2),
                    "realized_charges": round(self._realized_charges(), 2),
                    "estimated_active_charges": 0.0,
                    "pnl_with_charges": 0.0,
                    "cumulative_pnl": round(self._cumulative_pnl, 2),
                    "current_expiry": self._current_expiry,
                    "expiry_gross_pnl": 0.0,
                    "expiry_net_pnl": 0.0,
                    "expiry_closed_gross_pnl": 0.0,
                    "expiry_open_gross_pnl": 0.0,
                    "expiry_total_charges": 0.0,
                    "expiry_realized_charges": 0.0,
                    "expiry_estimated_open_charges": 0.0,
                },
            )
            logger.info(
                f"Positions opened | expiry={self._current_expiry} | "
                f"CE {selected.ce_strike}@{selected.ce_entry_price:.2f} | "
                f"PE {selected.pe_strike}@{selected.pe_entry_price:.2f}"
            )

        except Exception as exc:
            logger.exception(f"Failed to open positions: {exc}")
            await self.stream.send("error", {"message": f"Position open failed: {exc}"})

    # ------------------------------------------------------------------
    # Adjustment re-entry handler
    # ------------------------------------------------------------------

    async def _handle_adjustment_reentry(self, chain: list[dict], ts: str) -> None:
        ce_pos  = self.positions.get_active_sell("CE")
        pe_pos  = self.positions.get_active_sell("PE")
        ce_exit = self._resolve_exit_price(chain, ce_pos, "CE")
        pe_exit = self._resolve_exit_price(chain, pe_pos, "PE")

        closed_positions = [p for p in (ce_pos, pe_pos) if p]
        self.positions.close_all_sells(ce_exit, pe_exit, ts)
        cycle_pnl, cumulative_pnl, cycle_charges = self._record_cycle(
            closed_positions,
            reason="adjustment_reentry",
            ts=ts,
        )

        self._positions_open = False
        self.adj_levels      = None

        summary = self.positions.summary()
        await self._emit(
            "positions_closed",
            {
                "reason": "adjustment_reentry",
                "closed_positions": summary["sell_closed_positions"],
                "closed_hedges": summary["hedge_closed_positions"],
                "cycle_pnl": cycle_pnl,
                "cycle_charges": cycle_charges,
                "cumulative_pnl": cumulative_pnl,
                "cumulative_pnl_pct": self._cumulative_pct(),
                "timestamp": ts,
            },
        )

        if self.req.reentry_delay == 0:
            expiry_chain = self.ocm.fetch_chain_for_expiry(ts, self._current_expiry)
            await self._open_positions(expiry_chain or chain, ts)
        else:
            self._reentry_pending   = True
            self._reentry_countdown = self.req.reentry_delay - 1
            logger.info(f"Re-entry pending: waiting {self.req.reentry_delay} tick(s)")
            await self._emit(
                "reentry_scheduled",
                {"reentry_delay": self.req.reentry_delay, "timestamp": ts},
            )

    # ------------------------------------------------------------------
    # Spot price vs adjustment level hit
    # ------------------------------------------------------------------

    async def _check_spot_vs_levels(self, spot: float, ts: str) -> bool:
        upper = self.adj_levels.upper_price
        lower = self.adj_levels.lower_price

        if spot >= upper:
            side, message = "upper", "Spot price hit upper adjustment level"
        elif spot <= lower:
            side, message = "lower", "Spot price hit lower adjustment level"
        else:
            return False

        logger.warning(f"ADJUSTMENT HIT | side={side} | spot={spot} | upper={upper} | lower={lower}")
        await self._emit(
            "adjustment_triggered",
            {
                "side": side,
                "message": message,
                "spot_price": spot,
                "upper_adjustment_price": round(upper, 2),
                "lower_adjustment_price": round(lower, 2),
                "timestamp": ts,
            },
        )
        return True

    # ------------------------------------------------------------------
    # OTM distance adjustment check
    # ------------------------------------------------------------------

    async def _check_otm_adjustments(
        self, chain: list[dict], atm: float, strikes: list[float], ts: str
    ) -> bool:
        """
        Returns True if an OTM adjustment condition was newly detected this tick.

        adjustment_triggered_waiting=0 → caller will immediately fire adjustment_triggered
        adjustment_triggered_waiting=1 → caller waits for spot to reach price level
        """
        triggered = False
        for opt_type in ("CE", "PE"):
            pos = self.positions.get_active_sell(opt_type)
            if not pos:
                continue
            already = self._ce_adjusted if opt_type == "CE" else self._pe_adjusted
            if already:
                continue
            if self.adj_engine.is_triggered(pos.strike, atm, strikes, opt_type, self.adj_shift):
                if opt_type == "CE":
                    self._ce_adjusted = True
                else:
                    self._pe_adjusted = True
                triggered = True
                await self._emit(
                    "otm_adjustment_triggered",
                    {
                        "option_type": opt_type,
                        "sold_strike": pos.strike,
                        "current_atm": atm,
                        "required_shift": self.adj_shift,
                        "waiting_for_price": bool(self.req.adjustment_triggered_waiting),
                        "timestamp": ts,
                    },
                )
        return triggered

    # ------------------------------------------------------------------
    # Daily hedge lifecycle
    # ------------------------------------------------------------------

    async def _manage_daily_hedges(self, chain: list[dict], ts: str) -> None:
        if not self.req.headging_status or self.req.headging_type is None:
            return

        current_date = self.ocm._extract_date(ts)
        current_time = self.ocm._extract_time(ts)
        entry_time = self.req.headging_entry_time or "15:28"
        exit_time  = self.req.headging_exit_time  or "09:16"

        # ── Exit: close prev-day hedges at headging_exit_time ──────────
        if (
            current_time == exit_time
            and self.positions is not None
            and self.positions.has_active_hedges()
            and self._hedge_open_date != current_date
        ):
            ce_h = self.positions.get_active_hedge("CE")
            pe_h = self.positions.get_active_hedge("PE")
            # Capture PnL before closing (exit price not yet set)
            ce_hedge_pnl_locked = round(ce_h.pnl, 2) if ce_h else 0.0
            pe_hedge_pnl_locked = round(pe_h.pnl, 2) if pe_h else 0.0
            ce_exit = self._resolve_exit_price(chain, ce_h, "CE")
            pe_exit = self._resolve_exit_price(chain, pe_h, "PE")
            closed_positions = [p for p in (ce_h, pe_h) if p]
            self.positions.close_all_hedges(ce_exit, pe_exit, ts)
            cycle_pnl, cumulative_pnl, cycle_charges = self._record_cycle(
                closed_positions,
                reason="hedge_closed",
                ts=ts,
            )
            logger.info(f"Hedge exited | ts={ts}")
            await self._emit(
                "hedge_closed",
                {
                    "closed_hedges": self.positions.summary()["hedge_closed_positions"],
                    "cycle_pnl": cycle_pnl,
                    "cycle_charges": cycle_charges,
                    "cumulative_pnl": cumulative_pnl,
                    "cumulative_pnl_pct": self._cumulative_pct(),
                    "timestamp": ts,
                    # Carry locked-in hedge PnL forward until next hedge opens
                    "ce_hedge_pnl": ce_hedge_pnl_locked,
                    "pe_hedge_pnl": pe_hedge_pnl_locked,
                },
            )

        # ── Entry: open hedge at headging_entry_time each day ──────────
        if (
            current_time == entry_time
            and self._hedge_open_date != current_date
            and self.positions is not None
        ):
            # Use same-expiry chain so hedge strikes come from the correct expiry
            hedge_chain = self._fetch_active_expiry_chain(ts) or chain
            await self._open_hedge_positions(hedge_chain, ts)
            self._hedge_open_date = current_date

    async def _open_hedge_positions(self, chain: list[dict], ts: str) -> None:
        spot    = self.ocm.get_spot_price(chain)
        atm     = self.ocm.get_atm_strike(chain, spot)
        strikes = self.ocm.get_sorted_strikes(chain)

        ce_hedge_strike = ce_hedge_price = pe_hedge_strike = pe_hedge_price = 0.0

        if self.req.headging_type == 2:  # Closest premium
            target = float(self.req.headging_value or 0)
            ce_hedge_strike, ce_hedge_price = self.ocm.get_closest_premium_ce(chain, target)
            pe_hedge_strike, pe_hedge_price = self.ocm.get_closest_premium_pe(chain, target)

        elif self.req.headging_type == 3:  # Strike shift
            sorted_strikes = sorted(strikes)
            atm_idx = min(range(len(sorted_strikes)), key=lambda i: abs(sorted_strikes[i] - atm))
            shift = int(self.req.headging_value or 0)
            ce_idx = max(0, min(len(sorted_strikes) - 1, atm_idx - shift))
            pe_idx = max(0, min(len(sorted_strikes) - 1, atm_idx + shift))
            ce_hedge_strike = sorted_strikes[ce_idx]
            pe_hedge_strike = sorted_strikes[pe_idx]
            ce_hedge_price  = self.ocm.get_ce_premium(chain, ce_hedge_strike)
            pe_hedge_price  = self.ocm.get_pe_premium(chain, pe_hedge_strike)

        elif self.req.headging_type == 1:  # Delta — ATM approximation
            ce_hedge_strike = pe_hedge_strike = atm
            ce_hedge_price  = self.ocm.get_ce_premium(chain, atm)
            pe_hedge_price  = self.ocm.get_pe_premium(chain, atm)

        if ce_hedge_strike and pe_hedge_strike:
            self.positions.open_hedge("CE", ce_hedge_strike, ce_hedge_price, ts)
            self.positions.open_hedge("PE", pe_hedge_strike, pe_hedge_price, ts)
            logger.info(
                f"Hedge opened | CE {ce_hedge_strike}@{ce_hedge_price:.2f} | "
                f"PE {pe_hedge_strike}@{pe_hedge_price:.2f}"
            )
            await self._emit(
                "hedge_opened",
                {
                    "ce_hedge_strike":       ce_hedge_strike,
                    "ce_hedge_entry_price":  ce_hedge_price,
                    "ce_hedge_price":        ce_hedge_price,
                    "ce_hedge_pnl":          0.0,   # reset on new hedge
                    "pe_hedge_strike":       pe_hedge_strike,
                    "pe_hedge_entry_price":  pe_hedge_price,
                    "pe_hedge_price":        pe_hedge_price,
                    "pe_hedge_pnl":          0.0,   # reset on new hedge
                    "timestamp": ts,
                },
            )
