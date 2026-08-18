"""
crypto_fast_option_chain_api.py
---------------------------------
HTTP API for the Mongo-backed crypto historical chain replay (BTC/ETH),
same response contract as fast_option_chain_api.py's
/simulator/fast-paper-trade/* endpoints, mounted instead under
/simulator/crypto-fast-paper-trade/* and reading
crypto_option_chain_historical_data via crypto_chain_snapshot.py.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from simulator.crypto_chain_snapshot import (
    get_expiry_dates,
    get_historical_chain,
    get_latest_date,
    get_trading_days,
)

router = APIRouter(prefix="/simulator/crypto-fast-paper-trade")


@router.get("/historical-chain/{instrument}")
def crypto_historical_chain(instrument: str,
                             timestamp: str = Query(..., description="ISO timestamp, e.g. 2026-08-01T09:16:00"),
                             expiry: str = Query(default="")):
    if len(timestamp.strip()) < 10:
        raise HTTPException(status_code=400, detail="timestamp is required, e.g. 2026-08-01T09:16:00")
    return get_historical_chain(instrument, timestamp, expiry)


@router.get("/historical-chain-latest-date/{instrument}")
def crypto_historical_chain_latest_date(instrument: str):
    return get_latest_date(instrument)


@router.get("/expiry-dates/{instrument}")
def crypto_expiry_dates(instrument: str):
    return get_expiry_dates(instrument)


@router.get("/trading-days/{instrument}")
def crypto_trading_days(instrument: str):
    return get_trading_days(instrument)
