"""
models.py — NIFTY Intraday Short Strangle ("Strangle Daily") request/response shapes.

Rules encoded here come straight from the functional spec:
  - Entry 09:30, sell 4th OTM CE+PE (5th OTM if India VIX > 17).
  - Any selected leg premium > 100 -> move that leg one strike farther OTM.
  - On the weekly expiry day, trade next week's expiry instead of the current one,
    and exit by 14:59 instead of 15:15.
  - Combined MTM target +2000 / stoploss -1000, one entry per day, no re-entry.
"""

from pydantic import BaseModel, Field
from typing import Optional


class StrangleDailyRequest(BaseModel):
    backtest_start_date: str = Field(..., description="YYYY-MM-DD")
    backtest_end_date: str = Field(..., description="YYYY-MM-DD")

    underlying: str = Field(default="NIFTY", description="Underlying index/symbol")

    lot: int = Field(default=1, description="Number of lots")
    lot_size: int = Field(default=75, description="NIFTY lot size")

    entry_time: str = Field(default="09:30", description="HH:MM daily entry time")
    exit_time: str = Field(default="15:15", description="HH:MM exit time on normal days")
    expiry_exit_time: str = Field(default="14:59", description="HH:MM exit time on weekly expiry day")

    vix_threshold: float = Field(default=17.0, description="India VIX cutoff: <=this -> 4th OTM, >this -> 5th OTM")
    low_vix_otm: int = Field(default=4, description="OTM count used when VIX <= vix_threshold")
    high_vix_otm: int = Field(default=5, description="OTM count used when VIX > vix_threshold")

    premium_threshold: float = Field(default=100.0, description="If selected leg premium > this, move 1 strike farther OTM")

    profit_target: float = Field(default=2000.0, description="Combined MTM profit to exit")
    stop_loss: float = Field(default=1000.0, description="Combined MTM loss to exit")


class LegResult(BaseModel):
    option_type: str            # "CE" or "PE"
    strike: float
    otm_number: int             # 4, 5, 6 ... whichever OTM was actually sold
    premium_adjusted: bool      # True if bumped one strike farther due to premium_threshold

    entry_time: str
    entry_price: float
    entry_spot: float
    entry_iv: Optional[float] = None

    exit_time: str
    exit_price: float
    exit_spot: float
    exit_iv: Optional[float] = None

    pnl: float


class DayTrade(BaseModel):
    date: str
    expiry: str
    is_expiry_day: bool
    vix_at_entry: Optional[float] = None
    exit_reason: str            # "target" | "stoploss" | "time_exit" | "no_data"
    legs: list[LegResult] = Field(default_factory=list)
    trade_pnl: float = 0.0


class StrangleDailySummary(BaseModel):
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    total_profit: float = 0.0
    total_loss: float = 0.0
    net_pnl: float = 0.0
    max_profit: float = 0.0
    max_loss: float = 0.0
    max_drawdown: float = 0.0
    average_profit_per_trade: float = 0.0
    average_loss_per_trade: float = 0.0
    risk_reward_ratio: float = 0.0


class StrangleDailyResult(BaseModel):
    summary: StrangleDailySummary
    trades: list[DayTrade] = Field(default_factory=list)
    skipped_days: list[dict] = Field(default_factory=list)
