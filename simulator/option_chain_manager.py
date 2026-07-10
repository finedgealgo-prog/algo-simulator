"""
option_chain_manager.py
-----------------------
Handles all MongoDB option chain queries.

Actual document schema:
{
  "timestamp":  "2025-10-01T09:16:00",
  "underlying": "NIFTY",
  "expiry":     "2025-10-07",
  "strike":     23900,
  "type":       "CE",          ← "CE" or "PE"
  "close":      771.25,        ← current option price
  "oi":         11700,
  "iv":         0.15370882,
  "delta":      0.94088984,
  "spot_price": 24638.1
}

One document = one option (one CE or PE at one strike).

Performance design:
  - preload_range() bulk-fetches all documents for a date range in ONE
    MongoDB query and builds an in-memory dict keyed by timestamp.
  - All fetch_chain / fetch_chain_for_expiry / get_available_expiries
    calls are O(1) dict lookups — zero DB round-trips during the loop.
"""

import logging
from bisect import bisect_left
from typing import Optional

from pymongo import MongoClient

from .market_calendar import MarketCalendar

logger = logging.getLogger(__name__)


class OptionChainManager:

    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        self._client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self._collection = self._client["stock_data"]["option_chain"]
        self._calendar = MarketCalendar(mongo_uri)

        # Full-range in-memory cache: { timestamp -> [doc, ...] }
        self._cache: dict[str, list[dict]] = {}
        self._expiry_cache: dict[str, dict[str, list[dict]]] = {}
        self._expiries_cache: dict[str, tuple[str, ...]] = {}
        self._chain_meta: dict[int, dict] = {}
        self._cache_loaded: bool = False

    # ------------------------------------------------------------------
    # Bulk preload — called ONCE per backtest
    # ------------------------------------------------------------------

    def preload_range(self, start_date: str, end_date: str) -> None:
        """
        Fetch ALL option chain documents for [start_date, end_date] in a
        single MongoDB query and store them in self._cache keyed by timestamp.

        This converts potentially millions of per-tick DB round-trips into
        one bulk read, making the backtest loop fully in-memory.
        """
        logger.info(f"Preloading option chain data: {start_date} → {end_date} ...")

        start_ts = f"{start_date}T00:00:00"
        end_ts   = f"{end_date}T23:59:59"

        # One single find() across the entire date range
        cursor = self._collection.find(
            {"timestamp": {"$gte": start_ts, "$lte": end_ts}},
            {
                "_id": 0,
                "timestamp": 1,
                "expiry": 1,
                "strike": 1,
                "type": 1,
                "close": 1,
                "spot_price": 1,
            },
            batch_size=10_000,
        ).sort([("timestamp", 1), ("expiry", 1), ("strike", 1), ("type", 1)])

        new_cache: dict[str, list[dict]] = {}
        new_expiry_cache: dict[str, dict[str, list[dict]]] = {}
        new_expiries_cache: dict[str, tuple[str, ...]] = {}
        new_chain_meta: dict[int, dict] = {}

        for raw_doc in cursor:
            ts = str(raw_doc["timestamp"])
            expiry = str(raw_doc.get("expiry") or "")

            # Keep only the fields the engine actually needs.
            doc = {
                "timestamp": ts,
                "expiry": expiry,
                "strike": float(raw_doc.get("strike") or 0.0),
                "type": raw_doc.get("type"),
                "close": float(raw_doc.get("close") or 0.0),
                "spot_price": float(raw_doc.get("spot_price") or 0.0),
            }

            ts_docs = new_cache.setdefault(ts, [])
            ts_docs.append(doc)

            expiry_docs = new_expiry_cache.setdefault(ts, {}).setdefault(expiry, [])
            expiry_docs.append(doc)

        for ts, docs in new_cache.items():
            expiry_map = new_expiry_cache.get(ts, {})
            new_expiries_cache[ts] = tuple(sorted(expiry_map.keys()))
            new_chain_meta[id(docs)] = self._build_chain_meta(docs)
            for expiry_docs in expiry_map.values():
                new_chain_meta[id(expiry_docs)] = self._build_chain_meta(expiry_docs)

        self._cache = new_cache
        self._expiry_cache = new_expiry_cache
        self._expiries_cache = new_expiries_cache
        self._chain_meta = new_chain_meta
        self._cache_loaded = True
        logger.info(
            f"Preload complete: {sum(len(v) for v in new_cache.values())} documents "
            f"across {len(new_cache)} timestamps."
        )

    @staticmethod
    def _build_chain_meta(docs: list[dict]) -> dict:
        spot_price = 0.0
        strikes: set[float] = set()
        ce_prices: dict[float, float] = {}
        pe_prices: dict[float, float] = {}
        ce_quotes: list[tuple[float, float]] = []
        pe_quotes: list[tuple[float, float]] = []

        for doc in docs:
            strike = float(doc.get("strike", 0.0))
            close = float(doc.get("close", 0.0))
            option_type = doc.get("type")

            if not spot_price and doc.get("spot_price"):
                spot_price = float(doc["spot_price"])

            strikes.add(strike)
            if option_type == "CE":
                ce_prices[strike] = close
                ce_quotes.append((strike, close))
            elif option_type == "PE":
                pe_prices[strike] = close
                pe_quotes.append((strike, close))

        return {
            "spot_price": spot_price,
            "strikes": tuple(sorted(strikes)),
            "ce_prices": ce_prices,
            "pe_prices": pe_prices,
            "ce_quotes": tuple(ce_quotes),
            "pe_quotes": tuple(pe_quotes),
        }

    def _meta(self, chain: list[dict]) -> dict:
        return self._chain_meta.get(id(chain), {})

    # ------------------------------------------------------------------
    # Backtest timestamp helpers
    # ------------------------------------------------------------------

    def get_backtest_timestamps(
        self, start_date: str, end_date: str, daily_cutoff: str, timeframe: str = "1m"
    ) -> list[str]:
        """
        Preload the entire date range (one shot), then return all valid
        trading timestamps filtered by trading day calendar and daily_cutoff.

        Args:
            start_date:   "YYYY-MM-DD"
            end_date:     "YYYY-MM-DD"
            daily_cutoff: "HH:MM"  — position_end_time, no ticks after this
            timeframe:    "1m" | "5m" | "10m" ... minute step used by the UI
        """
        resolved_start = self._calendar.resolve_start_date(start_date)
        step_minutes = self._parse_timeframe_minutes(timeframe)

        trading_days: set[str] = set(
            self._calendar.get_trading_days(resolved_start, end_date)
        )

        # Bulk load everything upfront — SINGLE DB query
        self.preload_range(resolved_start, end_date)

        result = []
        for ts in sorted(self._cache.keys()):
            ts_date = self._extract_date(ts)
            ts_time = self._extract_time(ts)

            if ts_date < resolved_start or ts_date > end_date:
                continue
            if ts_date not in trading_days:
                continue  # holiday or weekend — skip entire day
            if ts_time > daily_cutoff:
                continue  # after position_end_time — skip tick
            if step_minutes > 1 and not self._matches_timeframe(ts, step_minutes):
                continue
            result.append(ts)

        logger.info(
            f"Backtest timestamps: {len(result)} ticks across "
            f"{len(trading_days)} trading days | {resolved_start} → {end_date} | cutoff={daily_cutoff}"
        )
        return result

    @staticmethod
    def _parse_timeframe_minutes(timeframe: str) -> int:
        tf = (timeframe or "1m").strip().lower()
        if tf.endswith("m"):
            tf = tf[:-1]
        try:
            minutes = int(tf)
            return minutes if minutes > 0 else 1
        except ValueError:
            return 1

    @staticmethod
    def _matches_timeframe(ts: str, step_minutes: int) -> bool:
        try:
            minute = int(ts[-5:-3]) if ts[-6] == ":" else int(ts.split(":")[1])
        except Exception:
            return True
        return minute % step_minutes == 0

    @staticmethod
    def _extract_time(ts: str) -> str:
        """Extract HH:MM from a timestamp string."""
        sep = "T" if "T" in ts else " "
        return ts.split(sep)[-1][:5]  # "HH:MM"

    @staticmethod
    def _extract_date(ts: str) -> str:
        """Extract YYYY-MM-DD from a timestamp string."""
        sep = "T" if "T" in ts else " "
        return ts.split(sep)[0]  # "YYYY-MM-DD"

    # ------------------------------------------------------------------
    # Chain fetching — pure in-memory O(1) dict lookups
    # ------------------------------------------------------------------

    def fetch_chain(self, timestamp: str) -> list[dict]:
        """Return all documents for a given timestamp (all expiries)."""
        docs = self._cache.get(timestamp, [])
        if not docs:
            logger.warning(f"No data for timestamp={timestamp}")
        return docs

    def fetch_chain_for_expiry(self, timestamp: str, expiry: str) -> list[dict]:
        """Return chain documents filtered by a specific expiry date."""
        docs = self._expiry_cache.get(timestamp, {}).get(expiry, [])
        if not docs:
            logger.warning(f"No data for timestamp={timestamp} expiry={expiry}")
        return docs

    def get_available_expiries(self, timestamp: str) -> list[str]:
        """Return all distinct expiry dates available at this timestamp, sorted."""
        return list(self._expiries_cache.get(timestamp, ()))

    # ------------------------------------------------------------------
    # Field extraction
    # ------------------------------------------------------------------

    def get_spot_price(self, chain: list[dict]) -> float:
        spot = float(self._meta(chain).get("spot_price") or 0.0)
        if spot:
            return spot
        raise ValueError("spot_price not found in chain")

    def get_expiry(self, chain: list[dict]) -> str:
        for doc in chain:
            if doc.get("expiry"):
                return str(doc["expiry"])
        return ""

    def get_sorted_strikes(self, chain: list[dict]) -> list[float]:
        return list(self._meta(chain).get("strikes", ()))

    def get_atm_strike(self, chain: list[dict], spot: float) -> float:
        strikes = self.get_sorted_strikes(chain)
        if not strikes:
            return 0.0
        idx = bisect_left(strikes, spot)
        if idx <= 0:
            return strikes[0]
        if idx >= len(strikes):
            return strikes[-1]
        before = strikes[idx - 1]
        after = strikes[idx]
        return before if abs(before - spot) <= abs(after - spot) else after

    def get_strike_step(self, strikes: list[float]) -> float:
        if len(strikes) < 2:
            return 50.0
        return min(strikes[i + 1] - strikes[i] for i in range(len(strikes) - 1))

    # ------------------------------------------------------------------
    # Price lookup  (one doc per CE/PE per strike)
    # ------------------------------------------------------------------

    def get_ce_premium(self, chain: list[dict], strike: float) -> float:
        price = self._meta(chain).get("ce_prices", {}).get(float(strike))
        if price is not None:
            return float(price)
        logger.debug(f"CE premium not found for strike={strike}")
        return 0.0

    def get_pe_premium(self, chain: list[dict], strike: float) -> float:
        price = self._meta(chain).get("pe_prices", {}).get(float(strike))
        if price is not None:
            return float(price)
        logger.debug(f"PE premium not found for strike={strike}")
        return 0.0

    # ------------------------------------------------------------------
    # Closest-premium strike selection  (hedge_type = 2)
    # ------------------------------------------------------------------

    def get_closest_premium_ce(
        self, chain: list[dict], target_premium: float
    ) -> tuple[float, float]:
        """
        Return (strike, close_price) for the CE option whose close price
        is closest to target_premium.
        """
        ce_quotes = self._meta(chain).get("ce_quotes", ())
        if not ce_quotes:
            return (0.0, 0.0)
        strike, close = min(ce_quotes, key=lambda item: abs(item[1] - target_premium))
        return (float(strike), float(close))

    def get_closest_premium_pe(
        self, chain: list[dict], target_premium: float
    ) -> tuple[float, float]:
        """
        Return (strike, close_price) for the PE option whose close price
        is closest to target_premium.
        """
        pe_quotes = self._meta(chain).get("pe_quotes", ())
        if not pe_quotes:
            return (0.0, 0.0)
        strike, close = min(pe_quotes, key=lambda item: abs(item[1] - target_premium))
        return (float(strike), float(close))

    # ------------------------------------------------------------------
    # OTM strike traversal
    # ------------------------------------------------------------------

    def get_nth_otm_ce(self, strikes: list[float], atm: float, n: int) -> float:
        above = [s for s in strikes if s > atm]
        return above[n - 1] if len(above) >= n else (above[-1] if above else atm)

    def get_nth_otm_pe(self, strikes: list[float], atm: float, n: int) -> float:
        below = sorted((s for s in strikes if s < atm), reverse=True)
        return below[n - 1] if len(below) >= n else (below[-1] if below else atm)

    def get_5th_otm_premium(
        self, chain: list[dict], atm: float, strikes: list[float]
    ) -> float:
        ce_strike = self.get_nth_otm_ce(strikes, atm, 5)
        pe_strike = self.get_nth_otm_pe(strikes, atm, 5)
        ce_prem = self.get_ce_premium(chain, ce_strike)
        pe_prem = self.get_pe_premium(chain, pe_strike)
        # Use max so that if EITHER side falls in a higher-vol band,
        # both sides get the wider/safer strike.
        dominant = max(ce_prem, pe_prem)
        logger.debug(
            f"5th OTM → CE {ce_strike}@{ce_prem:.2f}  PE {pe_strike}@{pe_prem:.2f}  dominant={dominant:.2f}"
        )
        return dominant

    def close(self) -> None:
        self._calendar.close()
        self._client.close()
