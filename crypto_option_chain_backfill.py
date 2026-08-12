"""
crypto_option_chain_backfill.py
─────────────────────────────────
Backfills a historical crypto option chain into MongoDB collection
`crypto_option_chain_historical_data`, one row per (underlying, expiry,
strike, type, timestamp) — same shape as NSE's option_chain_historical_data
(algo.trade/option_chain_backfill.py), minus `rho` and with `date`/`time`
split out alongside `timestamp` to match the sample doc this was built from.

Data sources
────────────
1. Option premiums: a LOCAL trade-tick CSV (columns: product_symbol, price,
   size, timestamp, buyer_role) — e.g. Delta Exchange's own downloadable
   monthly trade dump. Delta's public REST history
   (/v2/history/candles?symbol=<option_symbol>) returns EMPTY for expired
   option contracts (verified live) — there is no way to re-fetch past
   option prices from Delta's API once a contract has expired, so the CSV
   is the only source for this half of the data. Ticks are resampled here
   into 1-minute last-price bars per contract.
2. Spot price: Delta Exchange's own history, fetched fresh via
   /v2/history/candles?symbol=.DEXBTUSD (BTC) / .DEETHUSD (ETH) — this
   endpoint DOES retain full history (unlike expired option symbols).
   Delta caps each call at ~4000 rows regardless of the requested window,
   and (verified live) silently returns the TAIL of the window when it's
   larger than that — so this script always paginates in <4000-minute
   slices rather than trusting a single wide call.
3. IV: solved via Black-Scholes bisection from (spot, strike, TTE, premium).
   Greeks: closed-form Black-Scholes off that solved IV.
   r=0, q=0 — this codebase has no established risk-free-rate/carry
   convention for crypto (the NSE backfill uses NSE-specific constants
   that don't apply here); flag this if a funding-rate-based forward
   turns out to matter for a particular strategy.
4. Open interest: NOT populated (left 0). Neither the trade-tick CSV nor
   Delta's historical candle endpoint carries historical OI — only live
   tickers do (see delta_exchange_client.list_products/_leg_row), which is
   no use for a backfill.

Expiry time assumption: Delta settles BTC/ETH options at 12:00:00 UTC on
the expiry date (Delta's documented settlement time) — used for the
time-to-expiry input to Black-Scholes.

Usage (run with cwd=algo.simulator/ so `simulator.*` / `features.*` resolve
the same way every other script in this package does):

    python3 crypto_option_chain_backfill.py --csv /path/to/BTC_2026-07.csv

    Dry run by default — parses the CSV, fetches spot, computes IV/Greeks,
    prints a summary, and does NOT touch MongoDB. Add --write to actually
    upsert into crypto_option_chain_historical_data (whichever Mongo
    features.mongo_data.MONGO_URI currently points at — local by default
    in this checkout; flip MONGO_LIVE_DB_CONNECT there first if the live
    VPS DB is the actual target).
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
from scipy.special import ndtr

sys.path.insert(0, ".")  # so `simulator.*` / `features.*` resolve when run from elsewhere too

_SYMBOL_RE = re.compile(r"^([CP])-([A-Z0-9]+)-(\d+(?:\.\d+)?)-(\d{2})(\d{2})(\d{2})$")
_SETTLE_HOUR_UTC = 12  # Delta's documented BTC/ETH option settlement time
_R = 0.0
_Q = 0.0
_MIN_T = 1 / (365 * 24 * 60)
_DELTA_SPOT_SYMBOL = {"BTC": ".DEXBTUSD", "ETH": ".DEETHUSD"}
_CANDLE_PAGE_MINUTES = 3800  # stays under Delta's observed ~4000-row cap per call


def _parse_symbol(symbol: str, underlying: str) -> dict | None:
    m = _SYMBOL_RE.match(symbol)
    if not m:
        return None
    cp, ul, strike, dd, mm, yy = m.groups()
    if ul != underlying:
        return None
    return {
        "type": "CE" if cp == "C" else "PE",
        "strike": float(strike),
        "expiry": f"20{yy}-{mm}-{dd}",
    }


# ── vectorized Black-Scholes (S, K, T, sigma all arrays; r=q=0) ─────────────

def _bs_price_vec(S, K, T, sigma, is_call):
    sqT = np.sqrt(T)
    d1 = (np.log(S / K) + 0.5 * sigma ** 2 * T) / (sigma * sqT)
    d2 = d1 - sigma * sqT
    ce = S * ndtr(d1) - K * ndtr(d2)
    pe = K * ndtr(-d2) - S * ndtr(-d1)
    return np.where(is_call, ce, pe)


def _calc_iv_vec(close, S, K, T, is_call):
    n = len(close)
    ivs = np.zeros(n)
    valid = (close > 0) & (S > 0) & (K > 0) & (T > 0)
    if not valid.any():
        return ivs
    c, s, k, t, ic = close[valid], S[valid], K[valid], T[valid], is_call[valid]
    lo = np.full(valid.sum(), 1e-5)
    hi = np.full(valid.sum(), 20.0)
    for _ in range(60):
        mid = (lo + hi) * 0.5
        p = _bs_price_vec(s, k, t, mid, ic)
        lo = np.where(p < c, mid, lo)
        hi = np.where(p < c, hi, mid)
    ivs[valid] = (lo + hi) * 0.5
    return ivs


def _calc_greeks_vec(S, K, T, sigma, is_call):
    sqT = np.sqrt(T)
    d1 = (np.log(S / K) + 0.5 * sigma ** 2 * T) / (sigma * sqT)
    d2 = d1 - sigma * sqT
    nd1 = np.exp(-0.5 * d1 ** 2) / np.sqrt(2.0 * np.pi)
    Nd1 = ndtr(d1)
    gamma = nd1 / (S * sigma * sqT)
    vega = S * nd1 * sqT / 100.0
    delta = np.where(is_call, Nd1, Nd1 - 1.0)
    theta = np.where(
        is_call,
        (-(S * nd1 * sigma) / (2 * sqT)) / 365.0,
        (-(S * nd1 * sigma) / (2 * sqT)) / 365.0,
    )
    return (np.round(delta, 4), np.round(gamma, 6),
            np.round(theta, 4), np.round(vega, 4))


def _tte(expiry: str, at_minute: str) -> float:
    exp = datetime.strptime(expiry, "%Y-%m-%d").replace(hour=_SETTLE_HOUR_UTC, tzinfo=timezone.utc)
    ref = datetime.strptime(at_minute, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return max(_MIN_T, (exp - ref).total_seconds() / (365.0 * 86400))


# ── pass 1: resample raw trade ticks into 1-min last-price bars ────────────

def _build_minute_bars(csv_path: str, underlying: str, chunksize: int) -> dict[tuple[str, str], dict]:
    bars: dict[tuple[str, str], dict] = {}
    total_rows = 0
    t0 = time.time()
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        total_rows += len(chunk)
        # format="mixed": most rows carry microseconds ("...11:58:02.123456") but some
        # don't ("...11:58:02") — a single fixed strptime format 400s out on those.
        chunk["minute"] = pd.to_datetime(chunk["timestamp"], format="mixed").dt.floor("min").dt.strftime("%Y-%m-%dT%H:%M:%S")
        grouped = chunk.groupby(["product_symbol", "minute"], sort=False).agg(
            close=("price", "last"), volume=("size", "sum"),
        ).reset_index()
        for row in grouped.itertuples(index=False):
            key = (row.product_symbol, row.minute)
            existing = bars.get(key)
            if existing is None:
                bars[key] = {"close": row.close, "volume": row.volume}
            else:
                existing["close"] = row.close  # later chunk = later in time -> overwrite
                existing["volume"] += row.volume
        print(f"  …{total_rows:,} rows read, {len(bars):,} (symbol,minute) bars so far "
              f"({time.time() - t0:.0f}s)", file=sys.stderr)
    return bars


# ── pass 2: fetch spot history from Delta, paginated ────────────────────────

def _fetch_spot_series(underlying: str, start_dt: datetime, end_dt: datetime) -> dict[str, float]:
    from simulator.delta_exchange_client import fetch_candles

    spot_symbol = _DELTA_SPOT_SYMBOL.get(underlying)
    if not spot_symbol:
        raise ValueError(f"No Delta spot-index symbol known for {underlying}")

    series: dict[str, float] = {}
    cursor = start_dt
    page_span = timedelta(minutes=_CANDLE_PAGE_MINUTES)
    while cursor < end_dt:
        page_end = min(cursor + page_span, end_dt)
        bars = fetch_candles(underlying, "1", int(cursor.timestamp()), int(page_end.timestamp()))
        for b in bars:
            ts = datetime.fromtimestamp(b["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            series[ts] = b["close"]
        print(f"  …spot page {cursor:%Y-%m-%d %H:%M} → {page_end:%Y-%m-%d %H:%M}: "
              f"{len(bars)} candles (total {len(series):,})", file=sys.stderr)
        cursor = page_end
    return series


def _spot_lookup(spot_series: dict[str, float]):
    sorted_ts = sorted(spot_series.keys())
    sorted_close = [spot_series[t] for t in sorted_ts]
    import bisect

    def _lookup(minute: str) -> float:
        idx = bisect.bisect_right(sorted_ts, minute) - 1
        if idx < 0:
            return sorted_close[0] if sorted_close else 0.0
        return sorted_close[idx]

    return _lookup


# ── main ─────────────────────────────────────────────────────────────────

def run(csv_path: str, underlying: str, write: bool, chunksize: int, collection: str) -> None:
    print(f"[1/4] Reading + resampling ticks from {csv_path} …", file=sys.stderr)
    bars = _build_minute_bars(csv_path, underlying, chunksize)

    print(f"[2/4] Parsing {len(bars):,} bars into option legs …", file=sys.stderr)
    records = []
    minutes = set()
    for (symbol, minute), bar in bars.items():
        parsed = _parse_symbol(symbol, underlying)
        if not parsed:
            continue
        records.append({
            "minute": minute, "strike": parsed["strike"], "type": parsed["type"],
            "expiry": parsed["expiry"], "close": bar["close"], "volume": bar["volume"],
        })
        minutes.add(minute)
    if not records:
        print(f"No {underlying} option symbols matched in {csv_path} — nothing to do.", file=sys.stderr)
        return

    start_dt = datetime.strptime(min(minutes), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(max(minutes), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc) + timedelta(minutes=1)
    print(f"[3/4] Fetching {underlying} spot history {start_dt} → {end_dt} from Delta …", file=sys.stderr)
    spot_series = _fetch_spot_series(underlying, start_dt, end_dt)
    if not spot_series:
        print("Delta returned no spot candles for this range — aborting.", file=sys.stderr)
        return
    spot_at = _spot_lookup(spot_series)

    print(f"[4/4] Computing IV/Greeks for {len(records):,} rows …", file=sys.stderr)
    closes = np.array([r["close"] for r in records], dtype=np.float64)
    strikes = np.array([r["strike"] for r in records], dtype=np.float64)
    is_call = np.array([r["type"] == "CE" for r in records])
    spots = np.array([spot_at(r["minute"]) for r in records], dtype=np.float64)
    ttes = np.array([_tte(r["expiry"], r["minute"]) for r in records], dtype=np.float64)

    ivs = _calc_iv_vec(closes, spots, strikes, ttes, is_call)
    iv_valid = ivs > 0
    deltas = np.zeros(len(records)); gammas = np.zeros(len(records))
    thetas = np.zeros(len(records)); vegas = np.zeros(len(records))
    if iv_valid.any():
        d, g, th, ve = _calc_greeks_vec(spots[iv_valid], strikes[iv_valid], ttes[iv_valid], ivs[iv_valid], is_call[iv_valid])
        deltas[iv_valid], gammas[iv_valid], thetas[iv_valid], vegas[iv_valid] = d, g, th, ve

    docs = []
    for i, r in enumerate(records):
        docs.append({
            "timestamp": r["minute"],
            "date": r["minute"][:10],
            "time": r["minute"][11:16],
            "underlying": underlying,
            "expiry": r["expiry"],
            "strike": r["strike"],
            "type": r["type"],
            "close": float(r["close"]),
            "oi": 0,
            "spot_price": float(spots[i]),
            "iv": round(float(ivs[i]) * 100, 4) if ivs[i] else 0,
            "delta": float(deltas[i]),
            "gamma": float(gammas[i]),
            "theta": float(thetas[i]),
            "vega": float(vegas[i]),
        })

    print(f"\nSummary: {len(docs):,} rows | {len(minutes):,} distinct minutes | "
          f"{len({(r['strike'], r['expiry']) for r in records}):,} distinct (strike, expiry) | "
          f"date range {min(minutes)[:10]} → {max(minutes)[:10]}")
    print("Sample doc:", docs[len(docs) // 2])

    if not write:
        print("\nDRY RUN — nothing written. Re-run with --write to upsert into "
              f"MongoDB collection '{collection}'.")
        return

    from pymongo import UpdateOne
    from features.mongo_data import MongoData

    db = MongoData()
    try:
        col = db._db[collection]
        # Without this, every UpdateOne below filters on a collection with no index for
        # (underlying, expiry, strike, type, timestamp) -> full collection scan per upsert,
        # getting quadratically slower as the collection grows. Confirmed live: a first run
        # without this index was still under 12k/1.3M docs written after several minutes.
        col.create_index(
            [("underlying", 1), ("expiry", 1), ("strike", 1), ("type", 1), ("timestamp", 1)],
            unique=True, name="crypto_chain_upsert_key",
        )
        ops = []
        written = 0
        for doc in docs:
            ops.append(UpdateOne(
                {"underlying": doc["underlying"], "expiry": doc["expiry"], "strike": doc["strike"],
                 "type": doc["type"], "timestamp": doc["timestamp"]},
                {"$set": doc},
                upsert=True,
            ))
            if len(ops) >= 2000:
                col.bulk_write(ops, ordered=False)
                written += len(ops)
                print(f"  …written {written:,}/{len(docs):,}", file=sys.stderr)
                ops = []
        if ops:
            col.bulk_write(ops, ordered=False)
            written += len(ops)
        print(f"\nDone — {written:,} rows upserted into '{collection}'.")
    finally:
        db.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True, help="Path to the raw trade-tick CSV")
    ap.add_argument("--underlying", default="BTC", choices=["BTC", "ETH"])
    ap.add_argument("--collection", default="crypto_option_chain_historical_data")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--write", action="store_true", help="Actually upsert into MongoDB (default: dry run)")
    args = ap.parse_args()
    run(args.csv, args.underlying, args.write, args.chunksize, args.collection)


if __name__ == "__main__":
    main()
