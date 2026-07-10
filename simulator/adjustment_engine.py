"""
adjustment_engine.py
--------------------
Calculates adjustment trigger levels with points and percentage output.

Adjustment Shift Table (based on 5th OTM premium):
┌────────────────────────┬────────────┬────────────┐
│ 5th OTM Premium        │ Shift      │ Volatility │
├────────────────────────┼────────────┼────────────┤
│ ≤ 190                  │ 1 OTM      │ Normal     │
│ 190 – 300              │ 2 OTM      │ Medium     │
│ > 300                  │ 3 OTM      │ High       │
└────────────────────────┴────────────┴────────────┘

Trigger Rule:
  When a sold strike comes within [shift] OTMs of ATM → adjustment fires.

Output includes:
  upper_price         → spot price where CE adjustment triggers
  lower_price         → spot price where PE adjustment triggers
  upper_price_points  → upper_price - spot_price
  lower_price_points  → spot_price - lower_price
  upper_price_pct     → ((upper_price - spot_price) / spot_price) × 100
  lower_price_pct     → ((lower_price - spot_price) / spot_price) × 100
"""

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AdjustmentConfig:
    shift: int
    volatility: str  # Normal | Medium | High


@dataclass
class AdjustmentLevels:
    upper_price: float
    lower_price: float
    shift: int
    volatility: str
    # Extended output — populated by calculate_levels()
    upper_price_points: float = 0.0
    lower_price_points: float = 0.0
    upper_price_pct: float = 0.0
    lower_price_pct: float = 0.0

    def to_dict(self) -> dict:
        return {
            "upper_price": round(self.upper_price, 2),
            "lower_price": round(self.lower_price, 2),
            "upper_price_points": round(self.upper_price_points, 2),
            "lower_price_points": round(self.lower_price_points, 2),
            "upper_price_pct": round(self.upper_price_pct, 4),
            "lower_price_pct": round(self.lower_price_pct, 4),
            "shift": self.shift,
            "volatility": self.volatility,
        }


class AdjustmentEngine:
    """Computes adjustment configuration and monitors trigger conditions."""

    def get_config(self, fifth_otm_premium: float) -> AdjustmentConfig:
        if fifth_otm_premium <= 190:
            return AdjustmentConfig(shift=1, volatility="Normal")
        if fifth_otm_premium <= 300:
            return AdjustmentConfig(shift=2, volatility="Medium")
        return AdjustmentConfig(shift=3, volatility="High")

    def calculate_levels(
        self,
        ce_strike: float,
        pe_strike: float,
        strikes: list[float],
        fifth_otm_premium: float,
        spot: float,
    ) -> AdjustmentLevels:
        """
        Derive payoff chart vertical line prices + points + percentage.

        upper_price = ce_strike - (shift × step)   [spot rises to here → CE adjustment]
        lower_price = pe_strike + (shift × step)   [spot falls to here → PE adjustment]
        """
        config = self.get_config(fifth_otm_premium)
        step = self._strike_step(sorted(strikes))

        upper_price = ce_strike - (config.shift * step)
        lower_price = pe_strike + (config.shift * step)

        # Points
        upper_pts = upper_price - spot
        lower_pts = spot - lower_price

        # Percentage (relative to current spot)
        upper_pct = ((upper_price - spot) / spot * 100) if spot else 0.0
        lower_pct = ((lower_price - spot) / spot * 100) if spot else 0.0

        levels = AdjustmentLevels(
            upper_price=upper_price,
            lower_price=lower_price,
            shift=config.shift,
            volatility=config.volatility,
            upper_price_points=upper_pts,
            lower_price_points=lower_pts,
            upper_price_pct=upper_pct,
            lower_price_pct=lower_pct,
        )

        logger.debug(
            f"Adj levels → upper={upper_price:.2f} (+{upper_pts:.0f}pts / +{upper_pct:.2f}%)  "
            f"lower={lower_price:.2f} (-{lower_pts:.0f}pts / {lower_pct:.2f}%)  "
            f"shift={config.shift}  vol={config.volatility}"
        )
        return levels

    def is_triggered(
        self,
        sold_strike: float,
        atm: float,
        strikes: list[float],
        option_type: str,  # "CE" or "PE"
        required_shift: int,
    ) -> bool:
        """
        Returns True when the sold strike has moved within [required_shift] OTMs of ATM.
        """
        step = self._strike_step(sorted(strikes))
        if option_type == "CE":
            otm_distance = (sold_strike - atm) / step
        else:
            otm_distance = (atm - sold_strike) / step
        return otm_distance <= required_shift

    @staticmethod
    def _strike_step(sorted_strikes: list[float]) -> float:
        if len(sorted_strikes) < 2:
            return 50.0
        return min(
            sorted_strikes[i + 1] - sorted_strikes[i]
            for i in range(len(sorted_strikes) - 1)
        )
