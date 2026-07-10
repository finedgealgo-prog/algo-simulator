"""
market_calendar.py
------------------
Determines valid NSE trading days for backtesting.

MongoDB collection: stock_data.market_holidays
Document format:   { "date": "2025-10-02" }

Contains: all Saturdays, Sundays, and NSE market holidays.
Any date NOT present in this collection is a valid trading day.
"""

import logging
from datetime import date, timedelta

from pymongo import MongoClient

logger = logging.getLogger(__name__)


class MarketCalendar:

    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        self._client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self._collection = self._client["stock_data"]["market_holidays"]
        self._holiday_cache: set[str] | None = None  # loaded once per instance

    # ------------------------------------------------------------------
    # Holiday set (cached)
    # ------------------------------------------------------------------

    def _get_holidays(self) -> set[str]:
        """
        Load all holiday dates from MongoDB into a set.
        Result is cached so MongoDB is queried only once per engine run.
        """
        if self._holiday_cache is None:
            docs = self._collection.find({}, {"_id": 0, "date": 1})
            self._holiday_cache = {str(doc["date"]) for doc in docs if "date" in doc}
            logger.info(f"MarketCalendar: loaded {len(self._holiday_cache)} holiday dates")
        return self._holiday_cache

    def is_holiday(self, date_str: str) -> bool:
        """Return True if the given YYYY-MM-DD date is a holiday/weekend."""
        return date_str in self._get_holidays()

    def is_trading_day(self, date_str: str) -> bool:
        """Return True if the given YYYY-MM-DD date is a valid trading day."""
        return date_str not in self._get_holidays()

    # ------------------------------------------------------------------
    # Trading day range
    # ------------------------------------------------------------------

    def get_trading_days(self, start_date: str, end_date: str) -> list[str]:
        """
        Return all valid trading days (YYYY-MM-DD) between start_date
        and end_date inclusive, excluding all holidays and weekends.

        Args:
            start_date: "YYYY-MM-DD"
            end_date:   "YYYY-MM-DD"

        Returns:
            Sorted list of valid trading day strings.

        Example:
            start = "2025-10-01"  end = "2025-10-10"
            holidays = {"2025-10-02"}   (Gandhi Jayanti)
            weekends = {"2025-10-04", "2025-10-05"}

            → ["2025-10-01", "2025-10-03", "2025-10-06",
               "2025-10-07", "2025-10-08", "2025-10-09", "2025-10-10"]
        """
        holidays = self._get_holidays()

        start = date.fromisoformat(start_date)
        end   = date.fromisoformat(end_date)

        trading_days: list[str] = []
        current = start

        while current <= end:
            ds = current.isoformat()  # "YYYY-MM-DD"
            if ds not in holidays:
                trading_days.append(ds)
            current += timedelta(days=1)

        logger.info(
            f"Trading days: {len(trading_days)} valid days "
            f"between {start_date} and {end_date}"
        )
        return trading_days

    def next_trading_day(self, from_date: str) -> str:
        """
        Return the next valid trading day AFTER from_date.

        Example:
            from_date = "2025-10-01"
            → skips 2025-10-02 (Gandhi Jayanti), 2025-10-04/05 (weekend)
            → returns "2025-10-03"
        """
        holidays = self._get_holidays()
        current = date.fromisoformat(from_date) + timedelta(days=1)

        for _ in range(30):  # safety cap — no market closure > 30 days
            if current.isoformat() not in holidays:
                return current.isoformat()
            current += timedelta(days=1)

        raise RuntimeError(f"No trading day found within 30 days after {from_date}")

    def add_trading_days(self, from_date: str, days: int) -> str:
        """
        Move forward by N valid trading days from from_date.

        Example:
            from_date = "2025-10-01", days = 2
            -> returns the 2nd trading day after 2025-10-01
        """
        if days <= 0:
            return from_date if self.is_trading_day(from_date) else self.next_trading_day(from_date)

        current = from_date
        for _ in range(days):
            current = self.next_trading_day(current)
        return current

    def resolve_start_date(self, date_str: str) -> str:
        """
        If date_str is a holiday, return the next valid trading day.
        Otherwise return date_str unchanged.

        Used to handle cases where backtest_start_time falls on a holiday.
        """
        if self.is_trading_day(date_str):
            return date_str
        resolved = self.next_trading_day(date_str)
        logger.warning(
            f"Start date {date_str} is a holiday → moved to {resolved}"
        )
        return resolved

    def close(self) -> None:
        self._client.close()
