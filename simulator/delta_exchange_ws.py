"""
delta_exchange_ws.py
──────────────────────
Delta Exchange's own public websocket (wss://socket.india.delta.exchange,
channel "v2/ticker") — completely separate connection from the existing
Dhan/Kite ticker (features/dhan_ticker.py) and from algo.websocket's
/ws/internal-ticks hub. Delta is a different exchange with its own symbol
set (BTC/ETH crypto options, not NSE F&O), so it gets its own connection,
its own subscription set, and its own in-memory cache — nothing here touches
features/broker_gateway.py or its ticker_manager.

No auth needed: v2/ticker is a public channel.

Lazy-started: the first request for a chain (delta_exchange_router.py) calls
ensure_subscribed(), which starts this connection on first use rather than at
process startup — keeps this feature purely additive (no simulator_main.py
startup wiring needed) and avoids opening a crypto WS connection on every
process boot when nobody has asked for crypto data yet.
"""

from __future__ import annotations

import json
import logging
import threading
import time

import websocket

from simulator.delta_exchange_client import build_chain_from_tickers, list_expiries

log = logging.getLogger(__name__)

WS_URL = "wss://socket.india.delta.exchange"
_STALE_AFTER_SECONDS = 8.0   # snapshot older than this is treated as cold — caller falls back to REST
_RECONNECT_DELAY_SECONDS = 3.0


class _DeltaTickerManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ws_app: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._started = False
        self._connected = False

        # symbol -> latest raw ticker dict
        self._ticker_cache: dict[str, dict] = {}
        self._ticker_seen_at: dict[str, float] = {}

        # (underlying, resolved_expiry) -> symbols subscribed for that pair. Keyed per-expiry
        # (not just per-underlying) so multiple expiries of the same underlying can be watched
        # concurrently without each new ensure_subscribed() call for a different expiry
        # clobbering the previous one's entry — CryptoTradeNew.tsx's live-chain hook routinely
        # wants several at once now (main table + payoff-chart overlay + Add/Edit picker +
        # prewarm, see useLiveChainSocketMulti), and a single dict slot per underlying meant
        # only the MOST RECENTLY resolved expiry's snapshot was ever servable — every other
        # concurrently-watched expiry's get_chain_snapshot() call permanently missed (wrong
        # "expiry" on the shared slot) and fell back to REST, while ensure_subscribed() kept
        # re-overwriting that one shared slot back and forth between whichever expiries were
        # being polled that tick. Grows as new (underlying, expiry) pairs are requested; never
        # shrinks — option chains are cheap to keep warm.
        self._subscriptions: dict[tuple[str, str], set[str]] = {}

        # underlying -> (resolved_at, expiries) — ensure_subscribed() is
        # called on every poll from the router (every few seconds per open
        # browser tab); without this, "nearest expiry" resolution would hit
        # Delta's REST /v2/products on every single poll just to confirm
        # nothing changed. Expiries only ever change once a day (contracts
        # expiring), so a wide TTL is safe.
        self._expiry_cache: dict[str, tuple[float, list[str]]] = {}
        _EXPIRY_CACHE_TTL = 30.0
        self._EXPIRY_CACHE_TTL = _EXPIRY_CACHE_TTL

    # ── connection lifecycle ────────────────────────────────────────────────

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._thread = threading.Thread(target=self._run_forever, daemon=True, name="delta_ws")
            self._thread.start()
            log.info("[DeltaWS] connection thread started")

    def _run_forever(self) -> None:
        while True:
            try:
                self._ws_app = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                log.exception("[DeltaWS] run_forever error")
            self._connected = False
            time.sleep(_RECONNECT_DELAY_SECONDS)

    def _on_open(self, ws) -> None:
        self._connected = True
        log.info("[DeltaWS] connected")
        # Re-send every subscription accumulated so far (covers reconnects).
        with self._lock:
            all_symbols = sorted({s for symbols in self._subscriptions.values() for s in symbols})
        if all_symbols:
            self._send_subscribe(all_symbols)

    def _on_message(self, ws, message: str) -> None:
        try:
            data = json.loads(message)
        except Exception:
            return
        if data.get("type") != "v2/ticker":
            return
        symbol = data.get("symbol")
        if not symbol:
            return
        now = time.monotonic()
        with self._lock:
            self._ticker_cache[symbol] = data
            self._ticker_seen_at[symbol] = now

    def _on_error(self, ws, error) -> None:
        log.warning("[DeltaWS] error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        self._connected = False
        log.info("[DeltaWS] closed code=%s msg=%s", code, msg)

    def _send_subscribe(self, symbols: list[str]) -> None:
        if not self._ws_app or not self._connected:
            return
        try:
            self._ws_app.send(json.dumps({
                "type": "subscribe",
                "payload": {"channels": [{"name": "v2/ticker", "symbols": symbols}]},
            }))
        except Exception:
            log.warning("[DeltaWS] subscribe send failed for %s", symbols)

    # ── subscription management ─────────────────────────────────────────────

    def ensure_subscribed(self, underlying: str, expiry: str = "") -> str:
        """Starts the connection if needed, resolves the nearest expiry if
        none given, and subscribes to that expiry's option symbols plus the
        underlying's perpetual (for spot) — additively, alongside whichever
        other expiries of this same underlying are already being watched (see
        _subscriptions' own comment). Returns the resolved expiry. Cheap to
        call on every request — no-ops once this exact (underlying, expiry)
        pair is already subscribed."""
        self.start()

        expiries = self._get_expiries_cached(underlying)
        if not expiries:
            return expiry
        # A concrete (non-nearest) expiry that's no longer in Delta's own live list has
        # since expired — without this check, a caller that had this exact expiry cached
        # in _subscriptions from before it expired hits the "already subscribed" early
        # return below FOREVER (_subscriptions never shrinks/re-validates on its own, see
        # its own comment), permanently serving that dead expiry's now-stale/empty
        # snapshot instead of ever rolling over. This is what actually produced a
        # requested_expiry equal to yesterday's date with an empty CE/PE chain — the
        # frontend's picker/overlay/prewarm subscription had locked onto that date and
        # kept getting served straight out of this cache-hit branch, well past its real
        # expiry. Falling back to nearest here (same as "" would resolve to) self-heals
        # it, same as a fresh instrument switch already does client-side.
        if expiry and expiry not in expiries:
            expiry = ""
        resolved_expiry = expiry or expiries[0]
        key = (underlying, resolved_expiry)
        if key in self._subscriptions:
            return resolved_expiry

        from simulator.delta_exchange_client import list_products
        products = list_products(underlying)
        suffix = resolved_expiry[0:2] + resolved_expiry[3:5] + resolved_expiry[8:10]
        symbols = [p["symbol"] for p in products if p.get("symbol", "").endswith(suffix)]
        symbols.append(f"{underlying}USD")  # perpetual, for a live spot tick

        with self._lock:
            self._subscriptions[key] = set(symbols)
        self._send_subscribe(symbols)
        return resolved_expiry

    def _get_expiries_cached(self, underlying: str) -> list[str]:
        now = time.monotonic()
        cached = self._expiry_cache.get(underlying)
        if cached and (now - cached[0]) < self._EXPIRY_CACHE_TTL:
            return cached[1]
        expiries = list_expiries(underlying)
        if expiries:
            self._expiry_cache[underlying] = (now, expiries)
            return expiries
        return cached[1] if cached else []

    def get_expiries(self, underlying: str) -> list[str]:
        """Public wrapper the router uses to fill the 'expiries' field —
        shares the same 30s cache ensure_subscribed() uses internally."""
        return self._get_expiries_cached(underlying)

    # ── reads ────────────────────────────────────────────────────────────────

    def get_chain_snapshot(self, underlying: str, expiry: str) -> dict | None:
        """Returns a chain built from cached WS ticks if we have a fresh
        (< _STALE_AFTER_SECONDS old) tick for every subscribed symbol of this
        (underlying, expiry); otherwise None so the caller falls back to REST."""
        symbols = self._subscriptions.get((underlying, expiry))
        if not symbols:
            return None

        now = time.monotonic()
        with self._lock:
            tickers = []
            for symbol in symbols:
                seen_at = self._ticker_seen_at.get(symbol)
                if seen_at is None or (now - seen_at) > _STALE_AFTER_SECONDS:
                    return None
                tickers.append(self._ticker_cache[symbol])

        payload = build_chain_from_tickers(underlying, expiry, tickers)
        payload["expiries"] = []  # filled in by the router from a cheap cached list, not re-fetched here
        payload["source"] = "ws"
        return payload

    def get_ticker(self, symbol: str) -> dict | None:
        """Single-symbol read of the live cache — unlike get_chain_snapshot
        above, this doesn't require the whole chain to be fresh, just this
        one symbol. Used by delta_live_quote_socket.py, which pushes
        individual (possibly cross-expiry) legs/perpetuals a browser session
        subscribed to, not a whole chain snapshot. Thread-safe (same lock
        _on_message writes under)."""
        with self._lock:
            return self._ticker_cache.get(symbol)

    def get_status(self) -> dict:
        """The actual "is the crypto socket connected" answer — everything
        else crypto-side (chart live ticks, delta_alert_checker.py, the
        option chain) reads through this same in-process cache, so this one
        upstream connection being down silently stales all of them at once.
        Exposed via delta_exchange_router.py's /ws-status for the admin
        Monitors page (see delta_exchange_router.py's own comment on why
        this is "restartOnly" there — no clean stop, just lazy-start +
        self-healing reconnect)."""
        with self._lock:
            cached_symbols = len(self._ticker_cache)
        return {
            "started": self._started,
            "connected": self._connected,
            "subscribed_pairs": len(self._subscriptions),
            "cached_symbols": cached_symbols,
        }


delta_ticker_manager = _DeltaTickerManager()
