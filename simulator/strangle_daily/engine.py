"""
engine.py
---------
Day-by-day backtest loop for the NIFTY Intraday Short Strangle ("Strangle Daily").

One entry per trading day at `entry_time`:
  1. Read spot + India VIX -> pick base OTM count (4 or 5).
  2. If today is the weekly expiry day, trade next week's expiry instead
     and use `expiry_exit_time` as the cutoff; otherwise use `exit_time`.
  3. Sell CE + PE legs (premium > premium_threshold bumps that leg 1 strike farther OTM).
  4. Monitor combined MTM until target / stoploss / time cutoff.
  5. No re-entry after exit for that day.
"""

import logging

from .data_loader import StrangleDataLoader
from .models import (
    DayTrade,
    LegResult,
    StrangleDailyRequest,
    StrangleDailyResult,
    StrangleDailySummary,
)
from .strike_selector import base_otm_count, select_ce_leg, select_pe_leg

logger = logging.getLogger(__name__)


def run_backtest(request: StrangleDailyRequest) -> StrangleDailyResult:
    loader = StrangleDataLoader(underlying=request.underlying)
    trades: list[DayTrade] = []
    skipped_days: list[dict] = []

    try:
        trading_days = loader.get_trading_days(request.backtest_start_date, request.backtest_end_date)

        qty = request.lot * request.lot_size

        for day in trading_days:
            trade = _run_single_day(loader, request, day, qty)
            if trade is None:
                skipped_days.append({"date": day, "reason": "no_option_chain_data_at_entry"})
                continue
            trades.append(trade)

    finally:
        loader.close()

    summary = _compute_summary(trades)
    return StrangleDailyResult(summary=summary, trades=trades, skipped_days=skipped_days)


def _run_single_day(
    loader: StrangleDataLoader, request: StrangleDailyRequest, day: str, qty: int
) -> DayTrade | None:
    if not loader.load_day(day):
        return None

    entry_ts = loader.nearest_timestamp_at_or_after(request.entry_time)
    if entry_ts is None:
        return None

    available_expiries = loader.get_available_expiries(entry_ts)
    if not available_expiries:
        return None

    nearest_expiry = available_expiries[0]
    is_expiry_day = nearest_expiry == day
    selected_expiry = (
        available_expiries[1] if is_expiry_day and len(available_expiries) > 1 else nearest_expiry
    )

    chain = loader.fetch_chain_for_expiry(entry_ts, selected_expiry)
    if not chain:
        return None

    spot_entry = loader.get_spot_price(chain)
    strikes = loader.get_sorted_strikes(chain)
    if not strikes or not spot_entry:
        return None

    atm = loader.get_atm_strike(chain, spot_entry)
    vix = loader.get_vix_at_or_before(entry_ts)
    otm = base_otm_count(vix, request.vix_threshold, request.low_vix_otm, request.high_vix_otm)

    ce_leg = select_ce_leg(loader, chain, atm, strikes, otm, request.premium_threshold)
    pe_leg = select_pe_leg(loader, chain, atm, strikes, otm, request.premium_threshold)

    exit_cutoff = request.expiry_exit_time if is_expiry_day else request.exit_time

    monitor_ts = [
        ts
        for ts in loader.timestamps_for_day()
        if ts > entry_ts and loader.extract_time(ts) <= exit_cutoff
    ]

    exit_reason = "no_data_after_entry"
    exit_ts = entry_ts
    ce_exit_price, pe_exit_price = ce_leg.premium, pe_leg.premium
    exit_chain = chain

    for ts in monitor_ts:
        t_chain = loader.fetch_chain_for_expiry(ts, selected_expiry)
        if not t_chain:
            continue
        ce_ltp = loader.get_ce_premium(t_chain, ce_leg.strike)
        pe_ltp = loader.get_pe_premium(t_chain, pe_leg.strike)
        combined_pnl = ((ce_leg.premium - ce_ltp) + (pe_leg.premium - pe_ltp)) * qty

        exit_ts, ce_exit_price, pe_exit_price, exit_chain = ts, ce_ltp, pe_ltp, t_chain
        exit_reason = "time_exit"

        if combined_pnl >= request.profit_target:
            exit_reason = "target"
            break
        if combined_pnl <= -request.stop_loss:
            exit_reason = "stoploss"
            break

    exit_spot = loader.get_spot_price(exit_chain) or spot_entry
    ce_exit_iv = loader.get_ce_iv(exit_chain, ce_leg.strike)
    pe_exit_iv = loader.get_pe_iv(exit_chain, pe_leg.strike)

    ce_pnl = round((ce_leg.premium - ce_exit_price) * qty, 2)
    pe_pnl = round((pe_leg.premium - pe_exit_price) * qty, 2)

    legs = [
        LegResult(
            option_type="CE",
            strike=ce_leg.strike,
            otm_number=ce_leg.otm_number,
            premium_adjusted=ce_leg.premium_adjusted,
            entry_time=entry_ts,
            entry_price=ce_leg.premium,
            entry_spot=spot_entry,
            entry_iv=ce_leg.iv,
            exit_time=exit_ts,
            exit_price=ce_exit_price,
            exit_spot=exit_spot,
            exit_iv=ce_exit_iv,
            pnl=ce_pnl,
        ),
        LegResult(
            option_type="PE",
            strike=pe_leg.strike,
            otm_number=pe_leg.otm_number,
            premium_adjusted=pe_leg.premium_adjusted,
            entry_time=entry_ts,
            entry_price=pe_leg.premium,
            entry_spot=spot_entry,
            entry_iv=pe_leg.iv,
            exit_time=exit_ts,
            exit_price=pe_exit_price,
            exit_spot=exit_spot,
            exit_iv=pe_exit_iv,
            pnl=pe_pnl,
        ),
    ]

    return DayTrade(
        date=day,
        expiry=selected_expiry,
        is_expiry_day=is_expiry_day,
        vix_at_entry=vix,
        exit_reason=exit_reason,
        legs=legs,
        trade_pnl=round(ce_pnl + pe_pnl, 2),
    )


def _compute_summary(trades: list[DayTrade]) -> StrangleDailySummary:
    if not trades:
        return StrangleDailySummary()

    pnls = [t.trade_pnl for t in trades]
    winning = [p for p in pnls if p > 0]
    losing = [p for p in pnls if p <= 0]

    total_profit = round(sum(winning), 2)
    total_loss = round(sum(losing), 2)
    net_pnl = round(total_profit + total_loss, 2)
    win_rate = round(len(winning) / len(pnls) * 100, 2) if pnls else 0.0
    avg_profit = round(total_profit / len(winning), 2) if winning else 0.0
    avg_loss = round(total_loss / len(losing), 2) if losing else 0.0
    risk_reward = round(abs(avg_profit / avg_loss), 4) if avg_loss else 0.0

    peak = 0.0
    cumulative = 0.0
    max_dd = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_dd = max(max_dd, peak - cumulative)

    return StrangleDailySummary(
        total_trades=len(pnls),
        winning_trades=len(winning),
        losing_trades=len(losing),
        win_rate=win_rate,
        total_profit=total_profit,
        total_loss=total_loss,
        net_pnl=net_pnl,
        max_profit=round(max(pnls), 2),
        max_loss=round(min(pnls), 2),
        max_drawdown=round(max_dd, 2),
        average_profit_per_trade=avg_profit,
        average_loss_per_trade=avg_loss,
        risk_reward_ratio=risk_reward,
    )
