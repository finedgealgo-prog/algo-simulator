"""
risk_manager.py
---------------
Monitors stop-loss, target, and trailing stop-loss conditions each tick.

Trailing SL logic:
  - Activates once profit reaches trailing_sl_x points.
  - Once active, locks in (current_max_profit - trailing_sl_y) as the floor.
  - The floor rises as profit rises; it never falls.
  - Triggers when current PnL drops below the floor.
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    stoploss_status: int
    stoploss_type: Optional[int]    # 1=points, 2=percentage
    stoploss_value: Optional[float]

    target_status: int
    target_type: Optional[int]      # 1=points, 2=percentage
    target_value: Optional[float]

    trailing_sl_status: int
    trailing_sl_x: Optional[float]  # activate trailing when profit >= X
    trailing_sl_y: Optional[float]  # trail distance (lock in best_profit - Y)


class RiskManager:
    def __init__(self, config: RiskConfig, initial_premium: float):
        self.config = config
        self.initial_premium = initial_premium  # total premium received (CE+PE) × qty
        self._best_profit: float = 0.0
        self._trailing_active: bool = False
        self._trailing_floor: Optional[float] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_points(self, value_type: Optional[int], value: float) -> float:
        """Convert percentage to absolute points when value_type == 2."""
        if value_type == 2:
            return self.initial_premium * value / 100.0
        return value

    # ------------------------------------------------------------------
    # Per-tick checks  (return True = exit condition met)
    # ------------------------------------------------------------------

    def check_stoploss(self, current_pnl: float) -> bool:
        if not self.config.stoploss_status or self.config.stoploss_value is None:
            return False
        sl = self._to_points(self.config.stoploss_type, self.config.stoploss_value)
        if current_pnl <= -sl:
            logger.warning(
                f"STOPLOSS triggered | PnL={current_pnl:.2f}  SL limit={-sl:.2f}"
            )
            return True
        return False

    def check_target(self, current_pnl: float) -> bool:
        if not self.config.target_status or self.config.target_value is None:
            return False
        tgt = self._to_points(self.config.target_type, self.config.target_value)
        if current_pnl >= tgt:
            logger.info(
                f"TARGET reached | PnL={current_pnl:.2f}  Target={tgt:.2f}"
            )
            return True
        return False

    def check_trailing_sl(self, current_pnl: float) -> bool:
        if not self.config.trailing_sl_status:
            return False
        x = self.config.trailing_sl_x or 0.0
        y = self.config.trailing_sl_y or 0.0

        # Track the best profit seen so far
        if current_pnl > self._best_profit:
            self._best_profit = current_pnl

        # Activate once profit crosses X
        if not self._trailing_active and self._best_profit >= x:
            self._trailing_active = True
            self._trailing_floor = x - y
            logger.info(
                f"Trailing SL ACTIVATED | best_profit={self._best_profit:.2f}  "
                f"initial floor={self._trailing_floor:.2f}"
            )

        if self._trailing_active:
            # Raise floor as profit rises
            new_floor = self._best_profit - y
            if new_floor > (self._trailing_floor or 0.0):
                self._trailing_floor = new_floor
                logger.debug(f"Trailing SL floor updated → {self._trailing_floor:.2f}")

            if current_pnl <= (self._trailing_floor or 0.0):
                logger.warning(
                    f"TRAILING SL triggered | PnL={current_pnl:.2f}  "
                    f"floor={self._trailing_floor:.2f}"
                )
                return True

        return False

    # ------------------------------------------------------------------
    # Status snapshot
    # ------------------------------------------------------------------

    def status(self, current_pnl: float) -> dict:
        return {
            "current_pnl": round(current_pnl, 2),
            "stoploss_active": bool(self.config.stoploss_status),
            "target_active": bool(self.config.target_status),
            "trailing_active": bool(self.config.trailing_sl_status),
            "trailing_activated": self._trailing_active,
            "trailing_floor": (
                round(self._trailing_floor, 2) if self._trailing_floor is not None else None
            ),
            "best_profit": round(self._best_profit, 2),
        }
