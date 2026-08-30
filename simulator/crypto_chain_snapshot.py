"""
crypto_chain_snapshot.py
-------------------------
Mongo-backed twin of shared/fast_backtest/chain_snapshot.py's historical
option-chain replay logic, for crypto (BTC/ETH). Reads from two collections:

- "option_chain" (_LIVE_COLLECTION) — the same rolling collection the NSE
  live collector writes into (crypto_live_option_chain_collector.py's own
  docstring: "same collection/schema the NSE collector uses"). Holds
  whatever the collector has captured recently (observed: only the current
  day) with `expiry` as raw DD-MM-YYYY.
- "crypto_option_chain_historical_data" (_ARCHIVE_COLLECTION) — a longer-term
  backfill (written by crypto_option_chain_backfill.py --write) with `expiry`
  already normalized to YYYY-MM-DD, but only as fresh as whenever that batch
  job last ran (observed gap: nothing after a stale cutoff date, well short
  of "today").

Both collections use the same doc shape otherwise (no `token`/`symbol`
field, so those are synthesized here the same way chain_snapshot.py does
for its Parquet source, and `close` — not `ltp` — is the premium field).
Every lookup below tries the live collection first (it's the one with
today's data) and falls back to the archive (it's the one with older dates
the rolling live collection has already aged out) — the two are unioned for
the day/expiry listings so a caller sees the full available range either way.

Same response contract as chain_snapshot.get_historical_chain/
get_latest_date/get_trading_days/get_expiry_dates, except get_trading_days
here returns every calendar date with data (crypto trades 24/7 — there's
no weekend/holiday concept to filter out).
"""

from __future__ import annotations

from collections import Counter

from features.mongo_data import MongoData

_LIVE_COLLECTION = "option_chain"
_ARCHIVE_COLLECTION = "crypto_option_chain_historical_data"
_index_ensured: set[str] = set()


def _col(collection: str):
    col = MongoData()._db[collection]
    if collection not in _index_ensured:
        try:
            col.create_index(
                [("underlying", 1), ("date", 1), ("timestamp", 1)],
                background=True,
                name="crypto_chain_underlying_date_timestamp",
            )
        except Exception:
            pass
        _index_ensured.add(collection)
    return col


def _normalize_expiry(raw: object) -> str:
    """"31-08-2026" (option_chain's raw DD-MM-YYYY) -> "2026-08-31" —
    crypto_option_chain_historical_data already stores YYYY-MM-DD, and every
    date comparison in this module (and every caller, e.g. CryptoTradeNew.tsx's
    own normalizeGroupExpiry) assumes that ISO form."""
    s = str(raw or "").strip()[:10]
    parts = s.split("-")
    if len(parts) == 3 and len(parts[0]) == 2 and len(parts[2]) == 4:
        return f"{parts[2]}-{parts[1]}-{parts[0]}"
    return s


def _safe_float(v) -> float:
    try:
        f = float(v)
        return f if f == f else 0.0  # NaN guard
    except (TypeError, ValueError):
        return 0.0


def _pivot_timestamp(collection: str, underlying: str, req_date: str, requested_ts: str) -> str | None:
    col = _col(collection)
    at_or_before = col.find_one(
        {"underlying": underlying, "date": req_date, "timestamp": {"$lte": requested_ts}},
        {"_id": 0, "timestamp": 1},
        sort=[("timestamp", -1)],
    )
    if at_or_before:
        return at_or_before["timestamp"]
    after = col.find_one(
        {"underlying": underlying, "date": req_date, "timestamp": {"$gte": requested_ts}},
        {"_id": 0, "timestamp": 1},
        sort=[("timestamp", 1)],
    )
    return after["timestamp"] if after else None


def _previous_close(collection: str, underlying: str, req_date: str) -> float:
    """Last spot tick of the most recent earlier date with data, searched in
    `collection` first and the other collection second (the live collection
    may only hold today, with no "earlier date" of its own to find)."""
    for col_name in (collection, _ARCHIVE_COLLECTION if collection != _ARCHIVE_COLLECTION else _LIVE_COLLECTION):
        col = _col(col_name)
        prev_doc = col.find_one(
            {"underlying": underlying, "date": {"$lt": req_date}},
            {"_id": 0, "date": 1},
            sort=[("date", -1)],
        )
        if not prev_doc:
            continue
        last_tick = col.find_one(
            {"underlying": underlying, "date": prev_doc["date"]},
            {"_id": 0, "spot_price": 1},
            sort=[("timestamp", -1)],
        )
        if last_tick:
            return _safe_float(last_tick.get("spot_price"))
    return 0.0


def _get_historical_chain_from(collection: str, normalize_expiry: bool, normalized: str, req_date: str, norm_ts: str, expiry: str) -> dict:
    pivot_ts = _pivot_timestamp(collection, normalized, req_date, norm_ts)
    if pivot_ts is None:
        return {"status": "error", "message": f"No historical data for {normalized} on {req_date}."}

    rows = list(_col(collection).find({"underlying": normalized, "timestamp": pivot_ts}, {"_id": 0}))
    if not rows:
        return {"status": "error", "message": f"No option chain rows for {normalized} at {pivot_ts}."}
    if normalize_expiry:
        for r in rows:
            r["expiry"] = _normalize_expiry(r.get("expiry"))

    spot_price = _safe_float(rows[0].get("spot_price"))
    expiries_sorted = sorted({r["expiry"] for r in rows if r.get("expiry")})
    if not expiries_sorted:
        return {"status": "error", "message": f"No option chain rows for {normalized} at {pivot_ts}."}

    req_expiry = (expiry or "").strip()[:10]
    if req_expiry and req_expiry in expiries_sorted:
        live_expiry = req_expiry
    else:
        future = [e for e in expiries_sorted if e >= req_date]
        live_expiry = future[0] if future else expiries_sorted[-1]

    chains_out: dict[str, dict] = {}
    for exp in expiries_sorted:
        built = _build_expiry_chain([r for r in rows if r.get("expiry") == exp], exp, normalized, spot_price)
        if built is not None:
            chains_out[exp] = built

    if live_expiry not in chains_out:
        return {"status": "error", "message": f"No option chain rows for {normalized} {live_expiry} at {pivot_ts}."}

    previous_close = _previous_close(collection, normalized, req_date)
    change_pct = round((spot_price - previous_close) / previous_close * 100, 2) if previous_close else 0.0
    change_points = round(spot_price - previous_close, 2) if previous_close else 0.0

    live = chains_out[live_expiry]
    return {
        "status": "success",
        "instrument": normalized,
        "expiry": live_expiry,
        "expiries": expiries_sorted,
        "spot_price": round(spot_price, 2),
        "pricing_spot": round(spot_price, 2),
        "previous_close": round(previous_close, 2),
        "change_pct": change_pct,
        "change_points": change_points,
        "atm_strike": live["atm_strike"],
        "strike_interval": live["strike_interval"],
        "india_vix": 0.0,
        "lot_size": 1,
        "chain": live["chain"],
        "chains": chains_out,
        "timestamp": pivot_ts,
    }


def get_historical_chain(instrument: str, timestamp: str, expiry: str = "") -> dict:
    normalized = instrument.strip().upper()
    norm_ts = timestamp.strip().replace(" ", "T").rstrip("Z")
    req_date = norm_ts[:10]

    result = _get_historical_chain_from(_LIVE_COLLECTION, True, normalized, req_date, norm_ts, expiry)
    if result.get("status") == "success":
        return result
    return _get_historical_chain_from(_ARCHIVE_COLLECTION, False, normalized, req_date, norm_ts, expiry)


def _build_expiry_chain(rows: list[dict], expiry: str, normalized: str, spot_price: float) -> dict | None:
    if not rows:
        return None

    chain_out: dict[str, list[dict]] = {"CE": [], "PE": []}
    all_strikes: set[float] = set()
    for r in rows:
        opt_type = r.get("type")
        if opt_type not in ("CE", "PE"):
            continue
        strike = _safe_float(r.get("strike"))
        all_strikes.add(strike)
        strike_label = int(strike) if strike == int(strike) else strike
        symbol = f"{normalized}{expiry.replace('-', '')}{strike_label}{opt_type}"
        chain_out[opt_type].append({
            "strike": strike_label,
            "ltp": _safe_float(r.get("close")),
            "iv": _safe_float(r.get("iv")),
            "delta": _safe_float(r.get("delta")),
            "gamma": _safe_float(r.get("gamma")),
            "theta": _safe_float(r.get("theta")),
            "vega": _safe_float(r.get("vega")),
            "oi": int(_safe_float(r.get("oi"))),
            "token": symbol,
            "symbol": symbol,
        })
    if not chain_out["CE"] and not chain_out["PE"]:
        return None

    chain_out["CE"].sort(key=lambda x: float(x["strike"]))
    chain_out["PE"].sort(key=lambda x: float(x["strike"]))

    strikes_sorted = sorted(all_strikes)
    strike_interval = 0.0
    if len(strikes_sorted) >= 2:
        diffs = [strikes_sorted[i + 1] - strikes_sorted[i] for i in range(len(strikes_sorted) - 1)]
        strike_interval = float(Counter(diffs).most_common(1)[0][0])

    atm_strike = 0.0
    if strikes_sorted and spot_price > 0:
        atm_strike = min(strikes_sorted, key=lambda s: abs(s - spot_price))
    elif strikes_sorted:
        atm_strike = strikes_sorted[len(strikes_sorted) // 2]

    return {
        "expiry": expiry,
        "atm_strike": int(atm_strike) if atm_strike == int(atm_strike) else atm_strike,
        "strike_interval": int(strike_interval) if strike_interval == int(strike_interval) else strike_interval,
        "chain": chain_out,
    }


def get_expiry_dates(instrument: str) -> dict:
    normalized = instrument.strip().upper()
    live_expiries = {_normalize_expiry(e) for e in _col(_LIVE_COLLECTION).distinct("expiry", {"underlying": normalized})}
    archive_expiries = set(_col(_ARCHIVE_COLLECTION).distinct("expiry", {"underlying": normalized}))
    expiries = live_expiries | archive_expiries
    if not expiries:
        return {"status": "error", "message": f"No historical data found for {normalized}."}
    return {"status": "success", "instrument": normalized, "expiry_dates": sorted(expiries)}


def get_trading_days(instrument: str) -> dict:
    normalized = instrument.strip().upper()
    days = set(_col(_LIVE_COLLECTION).distinct("date", {"underlying": normalized}))
    days |= set(_col(_ARCHIVE_COLLECTION).distinct("date", {"underlying": normalized}))
    if not days:
        return {"status": "error", "message": f"No historical data found for {normalized}."}
    return {"status": "success", "instrument": normalized, "trading_days": sorted(days)}


def get_latest_date(instrument: str) -> dict:
    normalized = instrument.strip().upper()
    candidates: list[dict] = []
    for col_name in (_LIVE_COLLECTION, _ARCHIVE_COLLECTION):
        latest_doc = _col(col_name).find_one({"underlying": normalized}, {"_id": 0, "date": 1}, sort=[("date", -1)])
        if latest_doc:
            candidates.append({"collection": col_name, "date": latest_doc["date"]})
    if not candidates:
        return {"status": "error", "message": f"No historical data found for {normalized}."}
    best = max(candidates, key=lambda c: c["date"])
    earliest_tick = _col(best["collection"]).find_one(
        {"underlying": normalized, "date": best["date"]},
        {"_id": 0, "timestamp": 1},
        sort=[("timestamp", 1)],
    )
    if not earliest_tick:
        return {"status": "error", "message": f"No historical data found for {normalized}."}
    return {"status": "success", "date": best["date"], "timestamp": earliest_tick["timestamp"]}
