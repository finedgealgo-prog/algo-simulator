"""
live_option_chain_collector.py
-------------------------------
Full-chain option snapshot collector — writes into stock_data.option_chain,
the same collection/schema OptionChainManager (option_chain_manager.py)
already reads for backtesting. Built from the exd.txt spec: never poll the
Historical API during market hours; instead build minute snapshots from the
same live-priced chain every UI chain view in this app already uses.

Two generations of this collector:
  1. Original: read dhan_ticker_manager.ltp_map directly, per contract — only
     ever caught a contract with a fresh WS tick THIS session. Broke
     entirely once algo.simulator moved to "central-tick mode" (see
     simulator_main.py — it holds no Dhan WS of its own), and even after
     fixing that (reading broker_ticker_manager, the process-agnostic proxy
     every other live-price consumer here uses), coverage was still capped
     at whatever fraction of ~3500+ contracts happened to trade that minute
     — commonly under 1%, since real NIFTY options volume concentrates on
     the current weekly and most far-dated/far-OTM strikes may never trade
     in a session at all.
  2. Current: calls features.live_option_chain.fetch_full_chain(..., strike_
     window=30) once per (underlying, expiry) pair instead of reading ticks
     per-token. That function already does WS ltp_map-first / Dhan REST
     "/marketfeed/quote"+depth-fallback for whatever's missing, rate-gated
     through the same shared Dhan quote gate every other caller in this app
     uses (broker_gateway.py's dhan_quote_post_blocking) — the exact path
     PaperTradeNew's live chain panels, alert checks, etc. already rely on.
     strike_window=30 (30 strikes each side of ATM, matching
     live_greeks_chain_socket.py's UI_CHAIN_STRIKE_WINDOW) rather than 0
     (every listed strike): confirmed by direct comparison that's where
     real liquidity actually lives — 0 measured ~10 real docs out of 3946
     contracts (most of the listed range is deep ITM/OTM strikes NSE itself
     has no live quote for, at any minute, on any broker); the same expiry
     through the 30-window got real ltp on 100% of strikes (61/61 CE,
     61/61 PE). IV/Greeks come back already computed from each row's real
     LTP — no separate Black-Scholes step needed here.

A strike this still skips: one where Dhan itself has no quote at all (ltp=0
from both WS and REST) — never fabricated. Should now be rare within the
window, since REST covers anything WS hasn't ticked.

Known limitation: true intra-minute open/high/low (price movement within
the 60s window) isn't tracked — each stored document is a last-quote
"close" snapshot, not a full OHLC candle.
"""

import logging
import threading
from datetime import datetime, timedelta

from pymongo import MongoClient

from features.mongo_data import MONGO_URI
from features.broker_gateway import broker_ticker_manager

logger = logging.getLogger(__name__)


class LiveOptionChainCollector:
    """Builds a full option-chain snapshot (every expiry, every strike) once
    a minute via fetch_full_chain and stores it into stock_data.option_chain."""

    def __init__(self, underlyings: tuple[str, ...] = ("NIFTY",), mongo_uri: str = MONGO_URI):
        self._underlyings = tuple(u.strip().upper() for u in underlyings)
        self._client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self._out_collection = self._client["stock_data"]["option_chain"]
        self._tokens_collection = self._client["stock_data"]["active_option_tokens"]

        self._contracts: dict[str, dict] = {}   # security_id -> {instrument, expiry, strike, option_type}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_written_minute: str | None = None
        # algo.websocket owns the real broker WS and keeps every future expiry's
        # chain warm for its own live chain-view traffic — see _build_snapshot_docs.
        self._chain_api_base = "http://localhost:8003"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> dict:
        if self._thread and self._thread.is_alive():
            return {"status": "already_running", "contracts": len(self._contracts)}

        self._load_contracts()
        if not self._contracts:
            return {"status": "error", "message": "No active_option_tokens found for " + str(self._underlyings)}

        self._warm_chain_feed()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_minute_loop, daemon=True, name="live_option_chain_collector",
        )
        self._thread.start()
        logger.info(
            "[LIVE COLLECTOR] started underlyings=%s contracts=%d",
            self._underlyings, len(self._contracts),
        )
        return {"status": "started", "underlyings": list(self._underlyings), "contracts": len(self._contracts)}

    def stop(self) -> dict:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("[LIVE COLLECTOR] stopped")
        return {"status": "stopped", "last_written_minute": self._last_written_minute}

    def status(self) -> dict:
        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "underlyings": list(self._underlyings),
            "contracts": len(self._contracts),
            "last_written_minute": self._last_written_minute,
        }

    # ------------------------------------------------------------------
    # Pre-market contract-list initialization
    # ------------------------------------------------------------------

    def _load_contracts(self) -> None:
        """Build the full expiry x strike x CE/PE contract list in memory,
        exactly the "before market opens" step exd.txt asks for."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        contracts: dict[str, dict] = {}
        for doc in self._tokens_collection.find(
            {
                "broker": "dhan",
                "instrument": {"$in": list(self._underlyings)},
                "option_type": {"$in": ["CE", "PE"]},
                "expiry": {"$gte": today},
            },
            {"_id": 0, "token": 1, "instrument": 1, "expiry": 1, "strike": 1, "option_type": 1},
        ):
            token = str(doc.get("token") or "").strip()
            if not token:
                continue
            contracts[token] = {
                "instrument":  str(doc.get("instrument") or "").strip().upper(),
                "expiry":      str(doc.get("expiry") or "")[:10],
                "strike":      float(doc.get("strike") or 0.0),
                "option_type": str(doc.get("option_type") or "").strip().upper(),
            }
        with self._lock:
            self._contracts = contracts
        logger.info("[LIVE COLLECTOR] loaded %d contracts for %s", len(contracts), self._underlyings)

    def _warm_chain_feed(self) -> None:
        """Subscribe every loaded contract on Dhan's WebSocket chain-feed pool
        (never the Historical API) so ltp/oi/bid/ask stay live in memory."""
        with self._lock:
            security_ids = list(self._contracts.keys())
        if security_ids:
            broker_ticker_manager.warm_chain_tokens(security_ids, exchange="NSE_FNO")

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

    def _build_snapshot_docs(self, minute_ts: str) -> list[dict]:
        """One doc per strike with a real quote right now, for EVERY expiry —
        shared by the background minute-loop (_take_snapshot) and the one-shot
        manual trigger (snapshot_now).

        Calls algo.websocket's own GET /live-greeks-chain/{underlying}
        endpoint (port 8003) over HTTP — NOT features.live_option_chain.
        fetch_full_chain in-process. Those look equivalent (same function,
        same strike_window) but are not: fetch_full_chain's own result cache
        and warm-chain state are plain module-level dicts, so they live
        separately in whichever process calls them. algo.websocket has been
        running since server start, prewarming and continuously refreshing
        EVERY future expiry's chain for its own live chain-view traffic
        (prewarm_chain_rest, live_greeks_chain_socket.py) — genuinely warm.
        algo.simulator calling fetch_full_chain directly hits its OWN cold,
        separate cache/warm-state instead, and — empirically, comparing
        several real runs against direct /live-greeks-chain checks for the
        same expiry at the same time — misses expiries the warm process
        already has data for. Going through the HTTP endpoint instead reuses
        algo.websocket's actual warm state rather than re-deriving a colder
        copy of it here.

        strike_window=30 (matching live_greeks_chain_socket.py's
        UI_CHAIN_STRIKE_WINDOW, which is exactly what this endpoint always
        applies) rather than the full unwindowed listed range: confirmed by
        direct comparison that's where real liquidity actually lives — the
        full range (~3946 contracts across every expiry) measured ~10 real
        quotes; the same expiries through this endpoint's 30-window get
        real ltp on close to 100% of strikes for the near expiries. The
        far-listed strikes outside that window were never going to have a
        quote at any minute, on any broker — asking for them just multiplied
        REST calls (and 429 risk) against Dhan for nothing."""
        import requests

        with self._lock:
            contracts = dict(self._contracts)
        if not contracts:
            return []

        pairs = sorted({(m["instrument"], m["expiry"]) for m in contracts.values()})
        docs = []
        for underlying, expiry in pairs:
            try:
                resp = requests.get(
                    f"{self._chain_api_base}/live-greeks-chain/{underlying}",
                    params={"expiry": expiry},
                    timeout=15,
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception:
                logger.exception(
                    "[LIVE COLLECTOR] /live-greeks-chain fetch failed underlying=%s expiry=%s", underlying, expiry,
                )
                continue

            spot_price = float(payload.get("spot_price") or 0.0)
            chain = payload.get("chain") or {}
            for opt_type, rows in chain.items():
                for row in rows:
                    close = row.get("ltp") or 0
                    if not close:
                        continue  # Dhan itself has no quote for this contract — skip rather than fabricate
                    docs.append({
                        "timestamp":   minute_ts,
                        "date":        minute_ts[:10],
                        "time":        minute_ts[11:16],
                        "underlying":  underlying,
                        "expiry":      expiry,
                        "strike":      row.get("strike"),
                        "type":        opt_type,
                        "security_id": row.get("token"),
                        "close":       float(close),
                        "oi":          int(row.get("oi") or 0),
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
            logger.info("[LIVE COLLECTOR] stored %d snapshot docs for %s", len(docs), minute_ts)
        except Exception:
            logger.exception("[LIVE COLLECTOR] snapshot insert failed for %s", minute_ts)

    def snapshot_now(self) -> dict:
        """Manual one-shot trigger: loads active_option_tokens if this is the
        first call (same as start()), warms the chain feed, and immediately
        inserts whatever real ticks are available right now — no background
        thread, no waiting for the next minute boundary. Bypasses the
        _last_written_minute dedup guard on purpose, so hitting this twice in
        the same minute (e.g. while manually checking Mongo) still inserts
        again rather than silently no-op'ing like the loop would."""
        if not self._contracts:
            self._load_contracts()
        if not self._contracts:
            return {"status": "error", "message": "No active_option_tokens found for " + str(self._underlyings)}

        self._warm_chain_feed()
        minute_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
        docs = self._build_snapshot_docs(minute_ts)
        if not docs:
            return {
                "status": "no_data",
                "message": "No live ticks available yet for any contract — try again shortly.",
                "timestamp": minute_ts,
                "contracts": len(self._contracts),
            }

        try:
            self._out_collection.insert_many(docs, ordered=False)
        except Exception as exc:
            return {"status": "error", "message": str(exc)}

        logger.info("[LIVE COLLECTOR] snapshot_now stored %d docs for %s", len(docs), minute_ts)
        return {
            "status": "inserted",
            "timestamp": minute_ts,
            "docs_inserted": len(docs),
            "contracts_loaded": len(self._contracts),
        }

    def close(self) -> None:
        self.stop()
        self._client.close()


collector = LiveOptionChainCollector()
