"""
crypto_option_chain_algotest_backfill.py
──────────────────────────────────────────
Backfills a historical crypto option chain into MongoDB collection
`crypto_option_chain_historical_data_new`, one row per (underlying, expiry,
strike, type, timestamp) — same shape as crypto_option_chain_backfill.py's
output (which writes `crypto_option_chain_historical_data`), except this
source is prices.algotest.in's `/option-chain` endpoint instead of a local
Delta trade-tick CSV.

Why this exists: crypto_option_chain_backfill.py only stores a strike/minute
if that exact contract actually traded that minute, so illiquid ITM strikes
have large gaps. algotest.in's endpoint returns a FULL computed strike
ladder (close/IV/greeks for every listed strike, every expiry) for a given
minute regardless of whether that strike traded — so backfilling from it
gives complete coverage per minute instead of trade-driven gaps.

Auth: algotest.in's endpoint sits behind a logged-in session (JWT cookie +
CSRF header), both of which expire (the JWT `exp` claim is a few hours from
login). Never hardcode these into the script — pass them via env vars
(ALGOTEST_COOKIE, ALGOTEST_CSRF_TOKEN) or --cookie/--csrf-token, re-pulled
from your browser's devtools (Network tab -> a /option-chain request ->
copy as cURL) whenever they expire. A 401/403 mid-run stops the script
immediately with that hint rather than burning the rest of the range.

Requests are sequential, with a default 250ms gap between them (~4/sec) so
this doesn't look like scraping/bot traffic against algotest's servers and
risk the session getting flagged or rate-limited. Tune with --sleep (0 for
max speed if you know it's fine, higher to be gentler on a big range).

Usage (run with cwd=algo.simulator/ so `features.*` resolves the same way
every other script in this package does):

    export ALGOTEST_COOKIE='_fbp=...; access_token_cookie=...; csrf_access_token=...'
    export ALGOTEST_CSRF_TOKEN='<csrf_access_token value>'
    python3 crypto_option_chain_algotest_backfill.py \
        --underlying BTC --start 2026-07-14T00:00 --end 2026-07-14T06:00

    Dry run by default — fetches, parses, prints a summary, does NOT touch
    MongoDB. Add --write to actually upsert into
    crypto_option_chain_historical_data_new (whichever Mongo
    features.mongo_data.MONGO_URI currently points at — local by default in
    this checkout).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

import requests

sys.path.insert(0, ".")  # so `features.*` resolves the same way every other script in this package does

_API_URL = "https://prices.algotest.in/option-chain"
_UNDERLYING_MAP = {"BTC": "DELTA_BTCUSD", "ETH": "DELTA_ETHUSD"}

_GREEK_KEYS = ("delta", "gamma", "theta", "vega")


def _session(cookie: str, csrf_token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,ta;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "origin": "https://algotest.in",
        "referer": "https://algotest.in/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "cookie": cookie,
        "x-csrf-token-access": csrf_token,
    })
    return s


def _minute_range(start: datetime, end: datetime, step_minutes: int):
    t = start
    while t <= end:
        yield t
        t += timedelta(minutes=step_minutes)


def _fetch_candle(session: requests.Session, api_underlying: str, candle: datetime,
                   timeout: float, retries: int) -> dict | None:
    candle_str = candle.strftime("%Y-%m-%dT%H:%M")
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                _API_URL,
                params={"underlying": api_underlying, "candle": candle_str},
                timeout=timeout,
            )
        except requests.RequestException as exc:
            last_err = exc
            time.sleep(min(2 * attempt, 10))
            continue

        if resp.status_code in (401, 403):
            raise SystemExit(
                f"\n[AUTH FAILED] {resp.status_code} on candle {candle_str} — "
                "algotest.in session cookie/CSRF token has expired. Re-copy the "
                "cURL from a fresh browser session and set ALGOTEST_COOKIE / "
                "ALGOTEST_CSRF_TOKEN again."
            )
        if resp.status_code == 429:
            wait = min(5 * attempt, 30)
            print(f"  [rate limited] candle {candle_str} — retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if not resp.ok:
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
            time.sleep(min(2 * attempt, 10))
            continue

        try:
            return resp.json()
        except ValueError as exc:
            last_err = exc
            time.sleep(min(2 * attempt, 10))
            continue

    print(f"  [skip] candle {candle_str} — failed after {retries} attempts ({last_err})", file=sys.stderr)
    return None


def _underlying_label(api_underlying: str) -> str:
    return api_underlying.removeprefix("DELTA_").removesuffix("USD")


def _spot_price(payload: dict) -> float:
    cash = payload.get("cash") or {}
    if cash.get("close") is not None:
        return float(cash["close"])
    perp = payload.get("perpetual_future") or {}
    return float(perp.get("close") or 0.0)


def _docs_from_payload(payload: dict, underlying_label: str) -> list[dict]:
    candle = payload.get("candle")
    if not candle:
        return []
    ts = candle.replace(" ", "T")[:19]
    date, time_str = ts[:10], ts[11:16]
    spot = _spot_price(payload)

    docs = []
    for expiry, leg in (payload.get("options") or {}).items():
        strikes = leg.get("strike") or []
        for side, type_code in (("call", "CE"), ("put", "PE")):
            closes = leg.get(f"{side}_close") or []
            ois = leg.get(f"{side}_open_interest") or []
            ivs = leg.get(f"{side}_implied_vol") or []
            greeks = {g: (leg.get(f"{side}_{g}") or []) for g in _GREEK_KEYS}
            for i, strike in enumerate(strikes):
                close = closes[i] if i < len(closes) else None
                if close is None:
                    continue  # no quote for this strike/side at this minute — nothing to store
                docs.append({
                    "timestamp": ts,
                    "date": date,
                    "time": time_str,
                    "underlying": underlying_label,
                    "expiry": expiry,
                    "strike": float(strike),
                    "type": type_code,
                    "close": float(close),
                    "oi": int(ois[i]) if i < len(ois) and ois[i] is not None else 0,
                    "spot_price": spot,
                    "iv": round(float(ivs[i]) * 100, 4) if i < len(ivs) and ivs[i] is not None else 0.0,
                    "delta": float(greeks["delta"][i]) if i < len(greeks["delta"]) and greeks["delta"][i] is not None else 0.0,
                    "gamma": float(greeks["gamma"][i]) if i < len(greeks["gamma"]) and greeks["gamma"][i] is not None else 0.0,
                    "theta": float(greeks["theta"][i]) if i < len(greeks["theta"]) and greeks["theta"][i] is not None else 0.0,
                    "vega": float(greeks["vega"][i]) if i < len(greeks["vega"]) and greeks["vega"][i] is not None else 0.0,
                })
    return docs


def run(api_underlying: str, start: datetime, end: datetime, step_minutes: int,
        collection: str, write: bool, cookie: str, csrf_token: str,
        timeout: float, retries: int, sleep: float, flush_every: int) -> None:
    underlying_label = _underlying_label(api_underlying)
    session = _session(cookie, csrf_token)

    col = None
    if write:
        from pymongo import UpdateOne
        from features.mongo_data import MongoData

        db = MongoData()
        col = db._db[collection]
        # Same rationale as crypto_option_chain_backfill.py: without this index every
        # upsert below is a full collection scan on the filter fields.
        col.create_index(
            [("underlying", 1), ("expiry", 1), ("strike", 1), ("type", 1), ("timestamp", 1)],
            unique=True, name="crypto_chain_upsert_key",
        )

    total_candles = int((end - start).total_seconds() // 60 // step_minutes) + 1
    print(f"Backfilling {underlying_label} {start} → {end} step={step_minutes}m "
          f"({total_candles} candles) into '{collection}' "
          f"({'WRITE' if write else 'DRY RUN'})", file=sys.stderr)

    pending: list = []
    written = 0
    empty_candles = 0
    done = 0

    for candle in _minute_range(start, end, step_minutes):
        payload = _fetch_candle(session, api_underlying, candle, timeout, retries)
        done += 1
        if payload is None:
            continue

        docs = _docs_from_payload(payload, underlying_label)
        if not docs:
            empty_candles += 1
        else:
            if write:
                from pymongo import UpdateOne
                for doc in docs:
                    pending.append(UpdateOne(
                        {"underlying": doc["underlying"], "expiry": doc["expiry"], "strike": doc["strike"],
                         "type": doc["type"], "timestamp": doc["timestamp"]},
                        {"$set": doc},
                        upsert=True,
                    ))
                if len(pending) >= flush_every:
                    col.bulk_write(pending, ordered=False)
                    written += len(pending)
                    pending = []
            else:
                written += len(docs)

        if done % 20 == 0 or done == total_candles:
            print(f"  …{done}/{total_candles} candles "
                  f"({'written' if write else 'would write'} {written:,} rows, "
                  f"{empty_candles} empty)", file=sys.stderr)

        if sleep:
            time.sleep(sleep)

    if write and pending:
        col.bulk_write(pending, ordered=False)
        written += len(pending)

    if write:
        db.close()
        print(f"\nDone — {written:,} rows upserted into '{collection}'.")
    else:
        print(f"\nDRY RUN — {written:,} rows would have been written "
              f"({empty_candles} candles had no data). Re-run with --write to upsert into "
              f"MongoDB collection '{collection}'.")


def _parse_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--underlying", default="BTC", choices=["BTC", "ETH"])
    ap.add_argument("--start", required=True, type=_parse_dt, help="YYYY-MM-DDTHH:MM (UTC)")
    ap.add_argument("--end", required=True, type=_parse_dt, help="YYYY-MM-DDTHH:MM (UTC)")
    ap.add_argument("--step-minutes", type=int, default=1)
    ap.add_argument("--collection", default="crypto_option_chain_historical_data_new")
    ap.add_argument("--write", action="store_true", help="Actually upsert into MongoDB (default: dry run)")
    ap.add_argument("--cookie", default=os.environ.get("ALGOTEST_COOKIE"),
                     help="Full Cookie header value from a logged-in algotest.in session (or set ALGOTEST_COOKIE)")
    ap.add_argument("--csrf-token", default=os.environ.get("ALGOTEST_CSRF_TOKEN"),
                     help="csrf_access_token cookie value, also sent as x-csrf-token-access (or set ALGOTEST_CSRF_TOKEN)")
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--sleep", type=float, default=0.25,
                     help="Delay (seconds) between requests — kept >0 by default so this doesn't "
                          "look like scraping traffic to algotest.in. 0 = max speed.")
    ap.add_argument("--flush-every", type=int, default=2000, help="Rows per Mongo bulk_write batch")
    args = ap.parse_args()

    if not args.cookie or not args.csrf_token:
        raise SystemExit("Missing session credentials — set ALGOTEST_COOKIE and ALGOTEST_CSRF_TOKEN "
                          "(or pass --cookie/--csrf-token). Copy a fresh /option-chain request as cURL "
                          "from your browser's devtools Network tab to get current values.")

    run(
        _UNDERLYING_MAP[args.underlying], args.start, args.end, args.step_minutes,
        args.collection, args.write, args.cookie, args.csrf_token,
        args.timeout, args.retries, args.sleep, args.flush_every,
    )


if __name__ == "__main__":
    main()
