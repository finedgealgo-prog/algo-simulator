"""
strike_selector.py
-------------------
Strike selection for the NIFTY Intraday Short Strangle spec:

  1. base OTM count = 4 if India VIX <= vix_threshold else 5.
  2. Price the base-OTM leg. If its premium > premium_threshold, move that
     SAME leg one strike farther OTM (independently for CE and PE).
"""

from dataclasses import dataclass

from .data_loader import StrangleDataLoader


@dataclass
class SelectedLeg:
    strike: float
    premium: float
    otm_number: int
    premium_adjusted: bool
    iv: float | None


def base_otm_count(vix: float | None, vix_threshold: float, low_vix_otm: int, high_vix_otm: int) -> int:
    if vix is not None and vix > vix_threshold:
        return high_vix_otm
    return low_vix_otm


def select_ce_leg(
    loader: StrangleDataLoader,
    chain: list[dict],
    atm: float,
    strikes: list[float],
    otm_count: int,
    premium_threshold: float,
) -> SelectedLeg:
    strike = loader.get_nth_otm_ce(strikes, atm, otm_count)
    premium = loader.get_ce_premium(chain, strike)
    adjusted = False

    if premium > premium_threshold:
        farther_strike = loader.get_nth_otm_ce(strikes, atm, otm_count + 1)
        if farther_strike != strike:
            strike = farther_strike
            premium = loader.get_ce_premium(chain, strike)
            otm_count += 1
            adjusted = True

    return SelectedLeg(
        strike=strike,
        premium=premium,
        otm_number=otm_count,
        premium_adjusted=adjusted,
        iv=loader.get_ce_iv(chain, strike),
    )


def select_pe_leg(
    loader: StrangleDataLoader,
    chain: list[dict],
    atm: float,
    strikes: list[float],
    otm_count: int,
    premium_threshold: float,
) -> SelectedLeg:
    strike = loader.get_nth_otm_pe(strikes, atm, otm_count)
    premium = loader.get_pe_premium(chain, strike)
    adjusted = False

    if premium > premium_threshold:
        farther_strike = loader.get_nth_otm_pe(strikes, atm, otm_count + 1)
        if farther_strike != strike:
            strike = farther_strike
            premium = loader.get_pe_premium(chain, strike)
            otm_count += 1
            adjusted = True

    return SelectedLeg(
        strike=strike,
        premium=premium,
        otm_number=otm_count,
        premium_adjusted=adjusted,
        iv=loader.get_pe_iv(chain, strike),
    )
