"""
position_manager.py
-------------------
Manages active and closed positions for one Mini Strangle session.

Active positions  → sell_positions, hedge_positions          (position_status=1)
Closed positions  → sell_closed_positions, hedge_closed_positions  (position_status=2)

Positions are NEVER deleted — they move from active → closed.

PnL formulas:
  SELL: pnl = (entry_price - exit_price)  × quantity
  BUY:  pnl = (exit_price  - entry_price) × quantity
  pnl_pct = (pnl / (entry_price × quantity)) × 100

Margin:
  margin_used    = 1,80,000 × lot
  overall_pnl_pct = (total_pnl / margin_used) × 100
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

MARGIN_PER_LOT = 180_000  # 1.8 Lakh per lot


def _pnl(entry_position_type: str, entry_price: float, current_price: float, qty: int) -> float:
    if entry_position_type == "SELL":
        return (entry_price - current_price) * qty
    return (current_price - entry_price) * qty   # BUY


def _pnl_pct(pnl: float, entry_price: float, qty: int) -> float:
    cost = entry_price * qty
    return (pnl / cost * 100) if cost else 0.0


@dataclass
class Position:
    """Represents one option leg — active or closed."""

    entry_time: str
    entry_position_type: str   # "SELL" or "BUY"
    lot: int
    expiry: str
    option_type: str           # "CE" or "PE"
    strike: float
    entry_price: float
    current_price: float
    quantity: int              # lot × lot_size

    # Computed each tick
    pnl: float = 0.0
    pnl_pct: float = 0.0

    # Status
    position_status: int = 1   # 1=active, 2=closed

    # Exit fields — populated when closed
    exit_price: float = 0.0
    exit_time: str = ""
    exit_position_type: str = ""  # opposite of entry_position_type

    # ------------------------------------------------------------------

    def update(self, current_price: float) -> None:
        """Update live price and recompute PnL. entry_price never changes."""
        self.current_price = current_price
        self.pnl = _pnl(self.entry_position_type, self.entry_price, current_price, self.quantity)
        self.pnl_pct = _pnl_pct(self.pnl, self.entry_price, self.quantity)

    def close(self, exit_price: float, exit_time: str) -> None:
        """Freeze final PnL at exit price and mark as closed."""
        self.exit_price = exit_price
        self.exit_time = exit_time
        self.exit_position_type = "BUY" if self.entry_position_type == "SELL" else "SELL"
        self.current_price = exit_price
        self.pnl = _pnl(self.entry_position_type, self.entry_price, exit_price, self.quantity)
        self.pnl_pct = _pnl_pct(self.pnl, self.entry_price, self.quantity)
        self.position_status = 2

    def to_dict(self) -> dict:
        d = {
            "entry_time": self.entry_time,
            "entry_position_type": self.entry_position_type,
            "lot": self.lot,
            "expiry": self.expiry,
            "option_type": self.option_type,
            "strike": self.strike,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "quantity": self.quantity,
            "pnl": round(self.pnl, 2),
            "pnl_pct": round(self.pnl_pct, 4),
            "position_status": self.position_status,
        }
        # Include exit fields only when closed
        if self.position_status == 2:
            d["exit_price"] = self.exit_price
            d["exit_time"] = self.exit_time
            d["exit_position_type"] = self.exit_position_type
        return d


class PositionManager:
    def __init__(self, lot: int, lot_size: int, expiry: str = ""):
        self.lot = lot
        self.lot_size = lot_size
        self.quantity = lot * lot_size
        self.expiry = expiry

        # Active
        self.sell_positions: list[Position] = []
        self.hedge_positions: list[Position] = []

        # Closed history
        self.sell_closed_positions: list[Position] = []
        self.hedge_closed_positions: list[Position] = []

        # Margin
        self.margin_used: float = MARGIN_PER_LOT * lot

        # Running totals
        self.active_pnl: float = 0.0        # unrealized PnL of open positions only
        self.total_pnl: float = 0.0         # active + all closed (used for drawdown)
        self.overall_pnl_pct: float = 0.0

    # ------------------------------------------------------------------
    # Open
    # ------------------------------------------------------------------

    def open_sell(self, option_type: str, strike: float, entry_price: float, entry_time: str) -> Position:
        pos = Position(
            entry_time=entry_time,
            entry_position_type="SELL",
            lot=self.lot,
            expiry=self.expiry,
            option_type=option_type,
            strike=strike,
            entry_price=entry_price,
            current_price=entry_price,
            quantity=self.quantity,
        )
        self.sell_positions.append(pos)
        logger.info(f"SELL OPEN  {option_type} | strike={strike} | entry={entry_price:.2f} | qty={self.quantity}")
        return pos

    def open_hedge(self, option_type: str, strike: float, entry_price: float, entry_time: str) -> Position:
        pos = Position(
            entry_time=entry_time,
            entry_position_type="BUY",
            lot=self.lot,
            expiry=self.expiry,
            option_type=option_type,
            strike=strike,
            entry_price=entry_price,
            current_price=entry_price,
            quantity=self.quantity,
        )
        self.hedge_positions.append(pos)
        logger.info(f"HEDGE BUY  {option_type} | strike={strike} | entry={entry_price:.2f} | qty={self.quantity}")
        return pos

    # ------------------------------------------------------------------
    # Close
    # ------------------------------------------------------------------

    def close_sell(self, option_type: str, exit_price: float, exit_time: str) -> Optional[Position]:
        """Close active SELL position for given option_type. Move to closed list."""
        pos = self.get_active_sell(option_type)
        if not pos:
            return None
        pos.close(exit_price, exit_time)
        self.sell_positions.remove(pos)
        self.sell_closed_positions.append(pos)
        logger.info(f"SELL CLOSE {option_type} | strike={pos.strike} | exit={exit_price:.2f} | pnl={pos.pnl:.2f}")
        self._recalc_totals()
        return pos

    def close_all_sells(self, ce_exit_price: float, pe_exit_price: float, exit_time: str) -> None:
        """Close all active SELL positions."""
        self.close_sell("CE", ce_exit_price, exit_time)
        self.close_sell("PE", pe_exit_price, exit_time)

    def close_hedge(self, option_type: str, exit_price: float, exit_time: str) -> Optional[Position]:
        """Close active HEDGE (BUY) position for given option_type."""
        pos = self.get_active_hedge(option_type)
        if not pos:
            return None
        pos.close(exit_price, exit_time)
        self.hedge_positions.remove(pos)
        self.hedge_closed_positions.append(pos)
        logger.info(f"HEDGE CLOSE {option_type} | strike={pos.strike} | exit={exit_price:.2f} | pnl={pos.pnl:.2f}")
        self._recalc_totals()
        return pos

    def close_all_hedges(self, ce_exit_price: float, pe_exit_price: float, exit_time: str) -> None:
        """Close all active HEDGE positions."""
        self.close_hedge("CE", ce_exit_price, exit_time)
        self.close_hedge("PE", pe_exit_price, exit_time)

    def close_all(self, ce_exit: float = 0.0, pe_exit: float = 0.0, exit_time: str = "") -> None:
        """Close every open position (sells + hedges)."""
        self.close_all_sells(ce_exit, pe_exit, exit_time)
        self.close_all_hedges(ce_exit, pe_exit, exit_time)

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def get_active_sell(self, option_type: str) -> Optional[Position]:
        for p in self.sell_positions:
            if p.option_type == option_type and p.position_status == 1:
                return p
        return None

    def get_active_hedge(self, option_type: str) -> Optional[Position]:
        for p in self.hedge_positions:
            if p.option_type == option_type and p.position_status == 1:
                return p
        return None

    def has_active_sells(self) -> bool:
        return any(p.position_status == 1 for p in self.sell_positions)

    def has_active_hedges(self) -> bool:
        return any(p.position_status == 1 for p in self.hedge_positions)

    # ------------------------------------------------------------------
    # Price update (active SELL + HEDGE positions)
    # ------------------------------------------------------------------

    def update_prices(
        self,
        ce_sell_price: Optional[float],
        pe_sell_price: Optional[float],
        ce_hedge_price: Optional[float] = None,
        pe_hedge_price: Optional[float] = None,
    ) -> None:
        for pos in self.sell_positions:
            if pos.position_status != 1:
                continue
            if pos.option_type == "CE" and ce_sell_price is not None:
                pos.update(ce_sell_price)
            elif pos.option_type == "PE" and pe_sell_price is not None:
                pos.update(pe_sell_price)

        for pos in self.hedge_positions:
            if pos.position_status != 1:
                continue
            if pos.option_type == "CE" and ce_hedge_price is not None:
                pos.update(ce_hedge_price)
            elif pos.option_type == "PE" and pe_hedge_price is not None:
                pos.update(pe_hedge_price)
        self._recalc_totals()

    def _recalc_totals(self) -> None:
        # active_pnl: only currently open positions (unrealized PnL shown in minute_pnl)
        active_pnl = (
            sum(p.pnl for p in self.sell_positions)
            + sum(p.pnl for p in self.hedge_positions)
        )
        self.active_pnl = active_pnl

        # total_pnl: all positions (active + closed) — used internally for drawdown
        all_pnl = active_pnl + (
            sum(p.pnl for p in self.sell_closed_positions)
            + sum(p.pnl for p in self.hedge_closed_positions)
        )
        self.total_pnl = all_pnl
        self.overall_pnl_pct = (active_pnl / self.margin_used * 100) if self.margin_used else 0.0

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> dict:
        return {
            "sell_positions": [p.to_dict() for p in self.sell_positions],
            "hedge_positions": [p.to_dict() for p in self.hedge_positions],
            "sell_closed_positions": [p.to_dict() for p in self.sell_closed_positions],
            "hedge_closed_positions": [p.to_dict() for p in self.hedge_closed_positions],
        }
