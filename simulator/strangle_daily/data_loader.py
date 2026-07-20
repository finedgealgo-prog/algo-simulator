"""
data_loader.py
--------------
MongoDB access for the Strangle Daily backtest — reads from the same
production collections every other backtest engine uses (see
iron_condor_v2_backtest.py):

  stock_data.option_chain_historical_data   per-minute strike/type/close/iv
  stock_data.option_chain_index_spot        per-minute underlying spot
  stock_data.india_vix                      per-minute India VIX close

Document schema (option_chain_historical_data):
{
  "underlying": "NIFTY",
  "timestamp":  "2025-10-01T09:16:00",
  "expiry":     "2025-10-07",
  "strike":     23900,
  "type":       "CE",          <- "CE" or "PE"
  "close":      771.25,        <- option premium
  "iv":         0.15370882
}

Document schema (option_chain_index_spot):
{ "underlying": "NIFTY", "timestamp": "2025-10-01T09:16:00", "spot_price": 24638.1 }

Document schema (india_vix): { "timestamp": "...", "close": 15.2 }

Loaded one trading day at a time (same rationale as MongoData.load_day) so
RAM stays flat regardless of the backtest date range.
"""

import logging

from features.mongo_data import MONGO_URI, MongoData

from ..market_calendar import MarketCalendar

logger = logging.getLogger(__name__)

OC_COL = "option_chain_historical_data"
SPOT_COL = "option_chain_index_spot"
VIX_COL = "india_vix"

CHAIN_PROJECTION = {
    "_id": 0,
    "timestamp": 1,
    "expiry": 1,
    "strike": 1,
    "type": 1,
    "close": 1,
    "iv": 1,
}
SPOT_PROJECTION = {"_id": 0, "timestamp": 1, "spot_price": 1}
VIX_PROJECTION = {"_id": 0, "timestamp": 1, "close": 1}


class StrangleDataLoader:

    def __init__(self, underlying: str = "NIFTY"):
        self.underlying = underlying.upper()
        self._db = MongoData()
        self.calendar = MarketCalendar(MONGO_URI)

        # Current trading day's data only — reloaded by load_day().
        self._day: str | None = None
        self._by_ts: dict[str, list[dict]] = {}
        self._expiry_by_ts: dict[str, dict[str, list[dict]]] = {}
        self._expiries_by_ts: dict[str, tuple[str, ...]] = {}
        self._chain_meta: dict[int, dict] = {}
        self._vix_ticks: list[tuple[str, float]] = []

    # ------------------------------------------------------------------
    # Trading days / timestamps
    # ------------------------------------------------------------------

    def get_trading_days(self, start_date: str, end_date: str) -> list[str]:
        resolved_start = self.calendar.resolve_start_date(start_date)
        return self.calendar.get_trading_days(resolved_start, end_date)

    @staticmethod
    def extract_date(ts: str) -> str:
        sep = "T" if "T" in ts else " "
        return ts.split(sep)[0]

    @staticmethod
    def extract_time(ts: str) -> str:
        sep = "T" if "T" in ts else " "
        return ts.split(sep)[-1][:5]

    def timestamps_for_day(self) -> list[str]:
        """All option-chain timestamps for the currently loaded day, sorted."""
        return sorted(self._by_ts.keys())

    def nearest_timestamp_at_or_after(self, time_str: str) -> str | None:
        """Earliest loaded-day timestamp whose HH:MM is >= time_str (guards
        against a missing exact-minute candle)."""
        candidates = sorted(ts for ts in self._by_ts if self.extract_time(ts) >= time_str)
        return candidates[0] if candidates else None

    # ------------------------------------------------------------------
    # Per-day load
    # ------------------------------------------------------------------

    def load_day(self, day: str) -> bool:
        """Load one trading day's option chain + spot + VIX. Returns False
        if there is no option-chain data for this underlying on this day."""
        ts_start, ts_end = f"{day}T00:00:00", f"{day}T23:59:59"

        chain_rows = list(
            self._db._db[OC_COL]
            .find(
                {"underlying": self.underlying, "timestamp": {"$gte": ts_start, "$lte": ts_end}},
                CHAIN_PROJECTION,
                comment=self._db._comment("strangle_daily.load_day", collection=OC_COL, underlying=self.underlying, date=day),
            )
            .sort([("timestamp", 1), ("expiry", 1), ("strike", 1), ("type", 1)])
        )

        self._day = day

        if not chain_rows:
            self._by_ts, self._expiry_by_ts, self._expiries_by_ts, self._chain_meta = {}, {}, {}, {}
            self._vix_ticks = []
            return False

        spot_map = self._load_spot_map(ts_start, ts_end)

        by_ts: dict[str, list[dict]] = {}
        expiry_by_ts: dict[str, dict[str, list[dict]]] = {}

        for raw in chain_rows:
            ts = str(raw["timestamp"])
            expiry = str(raw.get("expiry") or "")
            doc = {
                "timestamp": ts,
                "expiry": expiry,
                "strike": float(raw.get("strike") or 0.0),
                "type": raw.get("type"),
                "close": float(raw.get("close") or 0.0),
                "iv": raw.get("iv"),
                "spot_price": spot_map.get(ts[:16], 0.0),
            }
            by_ts.setdefault(ts, []).append(doc)
            expiry_by_ts.setdefault(ts, {}).setdefault(expiry, []).append(doc)

        chain_meta: dict[int, dict] = {}
        for ts, docs in by_ts.items():
            chain_meta[id(docs)] = self._build_meta(docs)
            for expiry_docs in expiry_by_ts.get(ts, {}).values():
                chain_meta[id(expiry_docs)] = self._build_meta(expiry_docs)

        self._by_ts = by_ts
        self._expiry_by_ts = expiry_by_ts
        self._expiries_by_ts = {ts: tuple(sorted(m.keys())) for ts, m in expiry_by_ts.items()}
        self._chain_meta = chain_meta
        self._vix_ticks = self._load_vix(day)
        return True

    def _load_spot_map(self, ts_start: str, ts_end: str) -> dict[str, float]:
        """{timestamp_minute -> spot_price} e.g. {"2025-11-03T09:16": 25710.8}"""
        docs = self._db._db[SPOT_COL].find(
            {"underlying": self.underlying, "timestamp": {"$gte": ts_start, "$lte": ts_end}},
            SPOT_PROJECTION,
            comment=self._db._comment("strangle_daily.load_spot_map", collection=SPOT_COL, underlying=self.underlying),
        )
        return {str(d["timestamp"])[:16]: float(d["spot_price"]) for d in docs if d.get("spot_price")}

    def _load_vix(self, day: str) -> list[tuple[str, float]]:
        docs = self._db._db[VIX_COL].find(
            {"timestamp": {"$gte": f"{day}T00:00:00", "$lte": f"{day}T23:59:59"}},
            VIX_PROJECTION,
            comment=self._db._comment("strangle_daily.load_vix", collection=VIX_COL, date=day),
        ).sort([("timestamp", 1)])
        return [(str(d["timestamp"]), float(d["close"])) for d in docs if d.get("close") is not None]

    @staticmethod
    def _build_meta(docs: list[dict]) -> dict:
        spot_price = 0.0
        strikes: set[float] = set()
        ce_prices: dict[float, float] = {}
        pe_prices: dict[float, float] = {}
        ce_iv: dict[float, float] = {}
        pe_iv: dict[float, float] = {}

        for doc in docs:
            strike = float(doc.get("strike", 0.0))
            close = float(doc.get("close", 0.0))
            iv = doc.get("iv")
            option_type = doc.get("type")

            if not spot_price and doc.get("spot_price"):
                spot_price = float(doc["spot_price"])

            strikes.add(strike)
            if option_type == "CE":
                ce_prices[strike] = close
                if iv is not None:
                    ce_iv[strike] = float(iv)
            elif option_type == "PE":
                pe_prices[strike] = close
                if iv is not None:
                    pe_iv[strike] = float(iv)

        return {
            "spot_price": spot_price,
            "strikes": tuple(sorted(strikes)),
            "ce_prices": ce_prices,
            "pe_prices": pe_prices,
            "ce_iv": ce_iv,
            "pe_iv": pe_iv,
        }

    def _meta(self, chain: list[dict]) -> dict:
        return self._chain_meta.get(id(chain), {})

    # ------------------------------------------------------------------
    # Chain / VIX lookups
    # ------------------------------------------------------------------

    def fetch_chain_for_expiry(self, timestamp: str, expiry: str) -> list[dict]:
        return self._expiry_by_ts.get(timestamp, {}).get(expiry, [])

    def get_available_expiries(self, timestamp: str) -> list[str]:
        return list(self._expiries_by_ts.get(timestamp, ()))

    def get_spot_price(self, chain: list[dict]) -> float:
        return float(self._meta(chain).get("spot_price") or 0.0)

    def get_sorted_strikes(self, chain: list[dict]) -> list[float]:
        return list(self._meta(chain).get("strikes", ()))

    def get_atm_strike(self, chain: list[dict], spot: float) -> float:
        from bisect import bisect_left

        strikes = self.get_sorted_strikes(chain)
        if not strikes:
            return 0.0
        idx = bisect_left(strikes, spot)
        if idx <= 0:
            return strikes[0]
        if idx >= len(strikes):
            return strikes[-1]
        before, after = strikes[idx - 1], strikes[idx]
        return before if abs(before - spot) <= abs(after - spot) else after

    def get_nth_otm_ce(self, strikes: list[float], atm: float, n: int) -> float:
        above = [s for s in strikes if s > atm]
        return above[n - 1] if len(above) >= n else (above[-1] if above else atm)

    def get_nth_otm_pe(self, strikes: list[float], atm: float, n: int) -> float:
        below = sorted((s for s in strikes if s < atm), reverse=True)
        return below[n - 1] if len(below) >= n else (below[-1] if below else atm)

    def get_ce_premium(self, chain: list[dict], strike: float) -> float:
        return float(self._meta(chain).get("ce_prices", {}).get(float(strike)) or 0.0)

    def get_pe_premium(self, chain: list[dict], strike: float) -> float:
        return float(self._meta(chain).get("pe_prices", {}).get(float(strike)) or 0.0)

    def get_ce_iv(self, chain: list[dict], strike: float) -> float | None:
        return self._meta(chain).get("ce_iv", {}).get(float(strike))

    def get_pe_iv(self, chain: list[dict], strike: float) -> float | None:
        return self._meta(chain).get("pe_iv", {}).get(float(strike))

    def get_vix_at_or_before(self, timestamp: str) -> float | None:
        """Latest India VIX close at/before `timestamp` on the loaded day."""
        if not self._vix_ticks:
            return None
        best: float | None = None
        for ts, close in self._vix_ticks:
            if ts <= timestamp:
                best = close
            else:
                break
        return best if best is not None else self._vix_ticks[0][1]

    def close(self) -> None:
        self.calendar.close()
