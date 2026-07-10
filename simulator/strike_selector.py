"""
strike_selector.py
------------------
Selects which OTM strike to SELL for CE and PE legs.

Strike Selection Table (based on 5th OTM premium):
┌─────────────────────┬────────────────────────────────────────────┐
│ 5th OTM Premium     │ Strike to Sell                             │
├─────────────────────┼────────────────────────────────────────────┤
│ < 100               │ 4th OTM (or 5th if current PnL > 0)       │
│ 100 – 160           │ 5th OTM                                    │
│ 160 – 270           │ 6th OTM                                    │
│ 270 – 370           │ 7th OTM                                    │
│ 370 – 470           │ 8th OTM                                    │
│ > 470               │ 9th OTM                                    │
└─────────────────────┴────────────────────────────────────────────┘
"""

import logging
from dataclasses import dataclass

from .option_chain_manager import OptionChainManager

logger = logging.getLogger(__name__)


@dataclass
class SelectedStrikes:
    ce_strike: float
    pe_strike: float
    ce_entry_price: float
    pe_entry_price: float
    fifth_otm_premium: float
    strike_number: int  # which OTM was selected (4, 5, 6 …)


class StrikeSelector:
    """Determines the correct OTM index to sell and returns the strikes."""

    def get_strike_number(
        self, fifth_otm_premium: float, current_pnl: float = 0.0
    ) -> int:
        if fifth_otm_premium < 100:
            return 5 if current_pnl > 0 else 4
        if fifth_otm_premium < 160:
            return 5
        if fifth_otm_premium < 270:
            return 6
        if fifth_otm_premium < 370:
            return 7
        if fifth_otm_premium < 470:
            return 8
        return 9

    def select_strikes(
        self,
        chain: list[dict],
        atm: float,
        strikes: list[float],
        fifth_otm_premium: float,
        ocm: OptionChainManager,
        current_pnl: float = 0.0,
    ) -> SelectedStrikes:
        n = self.get_strike_number(fifth_otm_premium, current_pnl)

        ce_strike = ocm.get_nth_otm_ce(strikes, atm, n)
        pe_strike = ocm.get_nth_otm_pe(strikes, atm, n)

        ce_price = ocm.get_ce_premium(chain, ce_strike)
        pe_price = ocm.get_pe_premium(chain, pe_strike)

        logger.info(
            f"Strike #{n} selected | 5th OTM premium={fifth_otm_premium:.2f} | "
            f"SELL CE {ce_strike} @ {ce_price:.2f} | SELL PE {pe_strike} @ {pe_price:.2f}"
        )

        return SelectedStrikes(
            ce_strike=ce_strike,
            pe_strike=pe_strike,
            ce_entry_price=ce_price,
            pe_entry_price=pe_price,
            fifth_otm_premium=fifth_otm_premium,
            strike_number=n,
        )
