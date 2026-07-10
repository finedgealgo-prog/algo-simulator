"""
backtest_reporter.py
--------------------
Generates the final backtest_summary event after all positions are closed.

PnL consistency model:
  - cycle_pnl      = gross cycle pnl - cycle charges
  - cumulative_pnl = running sum of cycle_pnl
  - final net_pnl  = final cumulative_pnl

Validation:
  net_pnl must equal (total_profit + total_loss - total_charges).
"""

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ── Charge constants ──────────────────────────────────────────────────────────
_BROKERAGE_PER_ORDER = 20.0      # ₹20 flat per executed order
_STT_RATE            = 0.0005    # 0.05% on sell premium × qty (F&O sell side)
_EXCHANGE_RATE       = 0.00053   # NSE transaction charge
_SEBI_RATE           = 0.000001  # ₹10 per crore
_GST_RATE            = 0.18      # 18% on brokerage + exchange fee


@dataclass
class TradeSummary:
    entry_time: str
    exit_time: str
    gross_pnl: float
    charges: float


class BacktestReporter:

    def __init__(
        self,
        sell_closed: list,
        hedge_closed: list,
        margin_used: float,
        cycle_history: Optional[list[dict]] = None,
        final_cumulative_pnl: Optional[float] = None,
    ) -> None:
        self._sells = sell_closed
        self._hedges = hedge_closed
        self._margin = margin_used
        self._cycle_history = cycle_history or []
        self._final_cumulative_pnl = final_cumulative_pnl

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def generate(self) -> dict:
        """Return the full backtest_summary payload."""
        all_closed = self._sells + self._hedges
        if not all_closed and not self._cycle_history:
            return self._empty_summary()

        if self._cycle_history:
            trades = self._trades_from_cycle_history(self._cycle_history)
        else:
            # Fallback path for older engine flows: derive cycles from closed positions.
            trades = self._group_trades(all_closed)

        total_trades = len(trades)
        winning = [t for t in trades if t.gross_pnl > 0]
        losing = [t for t in trades if t.gross_pnl <= 0]

        win_count = len(winning)
        lose_count = len(losing)
        win_rate = round(win_count / total_trades * 100, 2) if total_trades else 0.0

        total_profit = round(sum(t.gross_pnl for t in winning), 2)
        total_loss = round(sum(t.gross_pnl for t in losing), 2)
        total_charges = round(sum(t.charges for t in trades), 2)

        computed_net_pnl = round(total_profit + total_loss - total_charges, 2)

        if self._final_cumulative_pnl is not None:
            net_pnl = round(self._final_cumulative_pnl, 2)
        else:
            net_pnl = computed_net_pnl

        pnl_consistency_ok = abs(net_pnl - computed_net_pnl) <= 0.01
        if not pnl_consistency_ok:
            logger.error(
                "PnL mismatch in backtest_summary | net_pnl=%s | computed=%s",
                net_pnl,
                computed_net_pnl,
            )

        max_profit = round(max((t.gross_pnl for t in trades), default=0.0), 2)
        max_loss = round(min((t.gross_pnl for t in trades), default=0.0), 2)

        avg_profit = round(total_profit / win_count, 2) if win_count else 0.0
        avg_loss = round(total_loss / lose_count, 2) if lose_count else 0.0
        risk_reward = round(abs(avg_profit / avg_loss), 4) if avg_loss else 0.0

        max_drawdown = round(self._calc_max_drawdown(trades), 2)
        overall_pnl_pct = round(net_pnl / self._margin * 100, 4) if self._margin else 0.0
        expiry_breakdown = self._build_expiry_breakdown(all_closed)

        summary = {
            "total_trades": total_trades,
            "total_winning_trades": win_count,
            "total_losing_trades": lose_count,
            "win_rate": win_rate,
            "total_profit": total_profit,
            "total_loss": total_loss,
            "total_charges": total_charges,
            "net_pnl": net_pnl,
            "cumulative_pnl": net_pnl,
            "computed_net_pnl": computed_net_pnl,
            "pnl_consistency_ok": pnl_consistency_ok,
            "overall_pnl_pct": overall_pnl_pct,
            "max_profit": max_profit,
            "max_loss": max_loss,
            "max_drawdown": max_drawdown,
            "risk_reward_ratio": risk_reward,
            "average_profit_per_trade": avg_profit,
            "average_loss_per_trade": avg_loss,
            "total_expiries_traded": len(expiry_breakdown),
            "expiry_breakdown": expiry_breakdown,
        }

        logger.info(
            "[backtest_summary] trades=%s | win=%s | net_pnl=%s | consistency=%s",
            total_trades,
            win_count,
            net_pnl,
            pnl_consistency_ok,
        )
        return summary

    def _build_expiry_breakdown(self, positions: list) -> list[dict]:
        """
        Group all closed positions by expiry so the final summary can show
        expiry-wise gross pnl, charges, and net pnl contribution.
        """
        expiry_groups: dict[str, list] = defaultdict(list)
        for pos in positions:
            expiry = getattr(pos, "expiry", "") or "unknown"
            expiry_groups[expiry].append(pos)

        breakdown: list[dict] = []
        for expiry, grouped_positions in sorted(expiry_groups.items()):
            gross_pnl = round(sum(pos.pnl for pos in grouped_positions), 2)
            charges = round(self.calculate_charges(grouped_positions), 2)
            net_pnl = round(gross_pnl - charges, 2)

            sell_positions = [pos for pos in grouped_positions if pos.entry_position_type == "SELL"]
            trade_groups: dict[str, list] = defaultdict(list)
            for pos in sell_positions:
                trade_groups[pos.entry_time].append(pos)

            trade_pnls = [round(sum(pos.pnl for pos in grouped), 2) for grouped in trade_groups.values()]
            winning_trades = sum(1 for pnl in trade_pnls if pnl > 0)
            losing_trades = sum(1 for pnl in trade_pnls if pnl <= 0)

            first_entry_time = min((pos.entry_time for pos in grouped_positions), default="")
            last_exit_time = max((pos.exit_time for pos in grouped_positions if pos.exit_time), default="")

            breakdown.append(
                {
                    "expiry": expiry,
                    "trade_cycles": len(trade_groups),
                    "winning_trades": winning_trades,
                    "losing_trades": losing_trades,
                    "gross_pnl": gross_pnl,
                    "total_charges": charges,
                    "net_pnl": net_pnl,
                    "first_entry_time": first_entry_time,
                    "last_exit_time": last_exit_time,
                }
            )

        return breakdown

    # ------------------------------------------------------------------
    # Trade grouping
    # ------------------------------------------------------------------

    def _trades_from_cycle_history(self, cycle_history: list[dict]) -> list[TradeSummary]:
        trades: list[TradeSummary] = []
        for i, cycle in enumerate(cycle_history, start=1):
            ts = cycle.get("timestamp") or f"cycle_{i}"
            trades.append(
                TradeSummary(
                    entry_time=ts,
                    exit_time=ts,
                    gross_pnl=round(float(cycle.get("gross_pnl", 0.0)), 2),
                    charges=round(float(cycle.get("charges", 0.0)), 2),
                )
            )
        return trades

    def _group_trades(self, positions: list) -> list[TradeSummary]:
        """
        Fallback grouping by entry_time when explicit cycle history is unavailable.
        Includes both sell and hedge legs for full-strategy gross pnl.
        """
        groups: dict[str, list] = defaultdict(list)
        for pos in positions:
            groups[pos.entry_time].append(pos)

        trades = []
        for entry_time, grouped_positions in sorted(groups.items()):
            gross_pnl = sum(p.pnl for p in grouped_positions)
            charges = self.calculate_charges(grouped_positions)
            exit_times = [p.exit_time for p in grouped_positions if p.exit_time]
            exit_time = max(exit_times) if exit_times else entry_time
            trades.append(
                TradeSummary(
                    entry_time=entry_time,
                    exit_time=exit_time,
                    gross_pnl=round(gross_pnl, 2),
                    charges=round(charges, 2),
                )
            )
        return trades

    # ------------------------------------------------------------------
    # Max drawdown
    # ------------------------------------------------------------------

    def _calc_max_drawdown(self, trades: list[TradeSummary]) -> float:
        """
        Maximum peak-to-trough decline in cumulative NET pnl across all trades.
        """
        peak = 0.0
        max_dd = 0.0
        cumulative = 0.0

        for trade in trades:
            cumulative += (trade.gross_pnl - trade.charges)
            if cumulative > peak:
                peak = cumulative
            drawdown = peak - cumulative
            if drawdown > max_dd:
                max_dd = drawdown

        return max_dd

    # ------------------------------------------------------------------
    # Charges calculation
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_charges(positions: list) -> float:
        """
        Approximate NSE F&O charges across the supplied closed positions.
        """
        total = 0.0
        for pos in positions:
            entry_turnover = pos.entry_price * pos.quantity
            exit_turnover = pos.exit_price * pos.quantity
            turnover = entry_turnover + exit_turnover

            brokerage = 2 * _BROKERAGE_PER_ORDER  # open + close
            stt = entry_turnover * _STT_RATE      # STT on sell side (entry)
            exchange = turnover * _EXCHANGE_RATE
            sebi = turnover * _SEBI_RATE
            gst = (brokerage + exchange) * _GST_RATE

            total += brokerage + stt + exchange + sebi + gst

        return total

    # ------------------------------------------------------------------
    # Empty summary (no trades executed)
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_summary() -> dict:
        return {
            "total_trades": 0,
            "total_winning_trades": 0,
            "total_losing_trades": 0,
            "win_rate": 0.0,
            "total_profit": 0.0,
            "total_loss": 0.0,
            "total_charges": 0.0,
            "net_pnl": 0.0,
            "cumulative_pnl": 0.0,
            "computed_net_pnl": 0.0,
            "pnl_consistency_ok": True,
            "overall_pnl_pct": 0.0,
            "max_profit": 0.0,
            "max_loss": 0.0,
            "max_drawdown": 0.0,
            "risk_reward_ratio": 0.0,
            "average_profit_per_trade": 0.0,
            "average_loss_per_trade": 0.0,
        }
