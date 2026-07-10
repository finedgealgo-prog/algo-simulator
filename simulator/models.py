from pydantic import BaseModel, Field
from typing import Optional


class MiniStrangleRequest(BaseModel):
    lot: int = Field(..., description="Number of lots: 1/2/3")
    lot_size: int = Field(default=50, description="Lot size (50=NIFTY, 75=BANKNIFTY)")
    timeframe: str = Field(..., description="Candle timeframe: 1m/5m/10m")
    strategy_type: str = Field(default="mini_strangle")

    expiry_type: str = Field(
        ..., description="current_week | next_week | monthly_expiry"
    )

    stoploss_status: int = Field(..., description="0=disabled, 1=enabled")
    stoploss_type: Optional[int] = Field(None, description="1=points, 2=percentage")
    stoploss_value: Optional[float] = None

    target_status: int = Field(..., description="0=disabled, 1=enabled")
    target_type: Optional[int] = Field(None, description="1=points, 2=percentage")
    target_value: Optional[float] = None

    trailing_sl_status: int = Field(..., description="0=disabled, 1=enabled")
    trailing_sl_x: Optional[float] = Field(
        None, description="Activate trailing when profit >= X points"
    )
    trailing_sl_y: Optional[float] = Field(
        None, description="Lock in Y points of profit once trailing activated"
    )

    trading_mode: int = Field(
        ..., description="1=manual, 2=semi_automatic, 3=automatic"
    )

    headging_status: int = Field(..., description="0=disabled, 1=enabled")
    headging_type: Optional[int] = Field(
        None, description="1=delta, 2=closest_premium, 3=strike"
    )
    headging_value: Optional[float] = Field(
        None,
        description=(
            "Type 1/2: numeric value (e.g. premium ₹10). "
            "Type 3: 0=ATM, 1=1ITM, 2=2ITM, -1=1OTM, -2=2OTM"
        ),
    )
    headging_entry_time: Optional[str] = Field(
        None, description="HH:MM — time each day to open hedge (e.g. 15:28)"
    )
    headging_exit_time: Optional[str] = Field(
        None, description="HH:MM — time NEXT day to close hedge (e.g. 09:16)"
    )

    # ── Backtest date range ───────────────────────────────────────────
    backtest_start_date: str = Field(
        ..., description="Backtest start date e.g. 2025-10-01"
    )
    backtest_end_date: str = Field(
        ..., description="Backtest end date e.g. 2025-10-10"
    )

    # ── Daily time controls ───────────────────────────────────────────
    position_start_time: str = Field(
        ..., description="Time to open new positions each day e.g. 09:20"
    )
    position_end_time: str = Field(
        ..., description="Daily monitoring cutoff time e.g. 15:29"
    )
    position_exit_time_on_expiry: str = Field(
        ..., description="Time to close all positions on expiry day e.g. 09:16"
    )

    # ── Adjustment trigger behaviour ──────────────────────────────────
    adjustment_triggered_waiting: int = Field(
        default=0,
        description=(
            "0 = instant adjustment when OTM condition is met (no price confirmation). "
            "1 = wait for spot to reach the predefined adjustment price level first."
        ),
    )

    # ── Re-entry delay ────────────────────────────────────────────────
    reentry_delay: int = Field(
        default=0,
        description="Ticks to wait before re-entering after adjustment. 0=instant.",
    )

    # ── Event-hit continuation behaviour ──────────────────────────────
    event_hit_position_status: int = Field(
        default=0,
        description=(
            "0 = expiry-based continuation, "
            "1 = trading-day based re-entry, "
            "2 = minute-delay based re-entry after SL/Target/TSL."
        ),
    )
    event_hit_entry_condition: Optional[int] = Field(
        default=None,
        description=(
            "Used when event_hit_position_status is 1 or 2. "
            "For 1: number of trading days to wait. "
            "For 2: number of minutes to wait."
        ),
    )
