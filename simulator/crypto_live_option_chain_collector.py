"""
crypto_live_option_chain_collector.py
--------------------------------------
BTC/ETH counterpart of algo.scanner's live_option_chain_collector.py — same
per-minute snapshot into stock_data.option_chain, but sourced from Delta
Exchange (delta_exchange_ws.py's live WS cache, delta_exchange_client.py's
REST fallback) instead of Dhan's active_option_tokens/quote feed, and
covering every listed expiry (and every strike within each) for each
underlying — NSE already has its own separate collector/monitor, this one
is crypto-only.

Delta trades 24/7, so unlike the NSE collector this has no market-hours
auto start/stop — see api.py's _auto_start_crypto_live_collector, which
starts this unconditionally on server boot.
"""

import logging
import threading
from datetime import datetime, timedelta

from pymongo import MongoClient

from features.mongo_data import MONGO_URI
from features.delta_exchange_client import fetch_option_chain_rest
from features.delta_exchange_ws import delta_ticker_manager

logger = logging.getLogger(__name__)


class CryptoLiveOptionChainCollector:
    """Builds a BTC/ETH option chain snapshot — every listed expiry, every
    strike within it — once a minute and stores it into
    stock_data.option_chain — same collection/schema the NSE collector
    writes into, underlying=BTC/ETH distinguishing these rows from
    NIFTY/BANKNIFTY/etc."""

    def __init__(
        self,
        underlyings: tuple[str, ...] = ("BTC", "ETH"),
        mongo_uri: str = MONGO_URI,
    ):
        self._underlyings = tuple(u.strip().upper() for u in underlyings)
        self._client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self._out_collection = self._client["stock_data"]["option_chain"]

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_written_minute: str | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> dict:
        if self._thread and self._thread.is_alive():
            return {"status": "already_running", "underlyings": list(self._underlyings)}

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_minute_loop, daemon=True, name="crypto_live_option_chain_collector",
        )
        self._thread.start()
        logger.info("[CRYPTO LIVE COLLECTOR] started underlyings=%s", self._underlyings)
        return {"status": "started", "underlyings": list(self._underlyings)}

    def stop(self) -> dict:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("[CRYPTO LIVE COLLECTOR] stopped")
        return {"status": "stopped", "last_written_minute": self._last_written_minute}

    def status(self) -> dict:
        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "underlyings": list(self._underlyings),
            "last_written_minute": self._last_written_minute,
        }

    # ------------------------------------------------------------------
    # Minute-boundary snapshot loop
    # ------------------------------------------------------------------

    def _run_minute_loop(self) -> None:
        while not self._stop_event.is_set():
            now = datetime.now()
            next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            sleep_for = (next_minute - now).total_seconds()
            if self._stop_event.wait(timeout=max(sleep_for, 0.1)):
                break
            self._take_snapshot(next_minute.strftime("%Y-%m-%dT%H:%M:00"))

    def _fetch_chain(self, underlying: str, expiry: str) -> dict | None:
        """Chain for one underlying+expiry — same live-WS-first,
        REST-fallback path GET /simulator/crypto/option-chain uses."""
        resolved_expiry = delta_ticker_manager.ensure_subscribed(underlying, expiry)
        if not resolved_expiry:
            return None

        snapshot = delta_ticker_manager.get_chain_snapshot(underlying, resolved_expiry)
        if snapshot is not None:
            return snapshot

        try:
            return fetch_option_chain_rest(underlying, resolved_expiry)
        except Exception:
            logger.exception("[CRYPTO LIVE COLLECTOR] REST fallback failed for %s %s", underlying, expiry)
            return None

    def _build_snapshot_docs(self, minute_ts: str) -> list[dict]:
        docs = []
        for underlying in self._underlyings:
            expiries = delta_ticker_manager.get_expiries(underlying)
            if not expiries:
                logger.warning("[CRYPTO LIVE COLLECTOR] no live expiries for %s", underlying)
                continue

            for expiry in expiries:
                snapshot = self._fetch_chain(underlying, expiry)
                if not snapshot:
                    continue

                spot_price = float(snapshot.get("spot_price") or 0.0)
                resolved_expiry = snapshot.get("expiry") or expiry
                chain = snapshot.get("chain") or {}
                for opt_type, rows in chain.items():
                    for row in rows:
                        # mark_raw (Delta's own unscaled per-1-BTC/ETH quote), NOT ltp
                        # (mark_price * DELTA_CONTRACT_VALUE — see delta_exchange_
                        # client._leg_row). A strategy leg's own entry_trade.price is
                        # always recorded in Delta's raw-points convention (see algo-
                        # admin's utils/strategyLegPnl.ts, deltaPnl.ts) — storing the
                        # scaled ltp here instead made every historical close ~1000x
                        # (BTC) / ~100x (ETH) off from entry_trade.price, so any MTM
                        # chart comparing the two (close - entry_price) produced a
                        # bogus, dominant PnL number that swamped the real per-minute
                        # movement. mark_raw keeps this collection unit-consistent
                        # with entry/exit prices everywhere else in the app.
                        close = row.get("mark_raw")
                        if not close:
                            continue  # Delta itself has no quote for this strike right now — skip rather than fabricate
                        docs.append({
                            "timestamp":   minute_ts,
                            "date":        minute_ts[:10],
                            "time":        minute_ts[11:16],
                            "underlying":  underlying,
                            "expiry":      resolved_expiry,
                            "strike":      row.get("strike"),
                            "type":        opt_type,
                            "security_id": row.get("symbol"),
                            "close":       float(close),
                            "oi":          float(row.get("oi") or 0),
                            "spot_price":  spot_price,
                            "bid":         float(row.get("bid") or 0.0),
                            "ask":         float(row.get("ask") or 0.0),
                            "iv":          float(row.get("iv") or 0.0),
                            "delta":       float(row.get("delta") or 0.0),
                            "gamma":       float(row.get("gamma") or 0.0),
                            "theta":       float(row.get("theta") or 0.0),
                            "vega":        float(row.get("vega") or 0.0),
                        })
        return docs

    def _take_snapshot(self, minute_ts: str) -> None:
        if minute_ts == self._last_written_minute:
            return  # guards against a duplicate candle if the loop ever double-fires

        docs = self._build_snapshot_docs(minute_ts)
        if not docs:
            return

        try:
            self._out_collection.insert_many(docs, ordered=False)
            self._last_written_minute = minute_ts
            logger.info("[CRYPTO LIVE COLLECTOR] stored %d snapshot docs for %s", len(docs), minute_ts)
        except Exception:
            logger.exception("[CRYPTO LIVE COLLECTOR] snapshot insert failed for %s", minute_ts)

    def snapshot_now(self) -> dict:
        """Manual one-shot trigger — immediately inserts whatever real chain
        data is available right now, no background thread, no waiting for
        the next minute boundary. Covers every expiry/strike, same as the
        periodic loop. Bypasses the _last_written_minute dedup guard on
        purpose, same as the NSE collector's snapshot_now."""
        minute_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
        docs = self._build_snapshot_docs(minute_ts)
        if not docs:
            return {
                "status": "no_data",
                "message": "No live chain data available yet for BTC/ETH — try again shortly.",
                "timestamp": minute_ts,
            }

        try:
            self._out_collection.insert_many(docs, ordered=False)
        except Exception as exc:
            return {"status": "error", "message": str(exc)}

        logger.info("[CRYPTO LIVE COLLECTOR] snapshot_now stored %d docs for %s", len(docs), minute_ts)
        return {"status": "inserted", "timestamp": minute_ts, "docs_inserted": len(docs)}

    def close(self) -> None:
        self.stop()
        self._client.close()


crypto_collector = CryptoLiveOptionChainCollector()
