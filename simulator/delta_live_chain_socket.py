"""
delta_live_chain_socket.py
────────────────────────────
WebSocket push for the crypto (Delta Exchange) option chain — same protocol
NSE's live_greeks_chain_socket.py multiplexed endpoint speaks
({action:"subscribe"|"unsubscribe", instrument, expiry} in, {type:"chain",
instrument, expiry, chain:{CE,PE}, ...} pushed out), so CryptoTradeNew.tsx's
useLiveChainSocketMulti — already built generically against that protocol —
just needs pointing at this service's origin instead of algo.websocket's, no
frontend protocol changes.

Before this existed, CryptoTradeNew.tsx ran with its WS path disabled
(LIVE_CHAIN_WS_DISABLED, copied verbatim from PaperTradeNew.tsx, where it's a
workaround for a *different*, NSE-specific socket bug) and fell back to
useLiveChainSocket.ts's REST-poll-every-3s path — meaning every open crypto
builder tab hit GET /simulator/crypto/rest-option-chain/{instrument} on its
own independent 3s timer, with zero sharing between tabs/users watching the
same chain.

Backed by delta_ticker_manager (delta_exchange_ws.py) — that manager already
keeps a live upstream websocket to Delta's own public feed
(wss://socket.india.delta.exchange) continuously updating an in-memory
per-symbol tick cache, so every push here is a free in-memory read, not a
network call. One broadcaster task per (underlying, expiry) is shared across
every client watching that chain, same _*Hub pattern as
live_greeks_chain_socket.py's _GreeksChainHub — N clients cost one
get_chain_snapshot() read per push interval, not N REST calls each.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from simulator.delta_exchange_client import fetch_option_chain_rest
from simulator.delta_exchange_router import _iso_to_ddmmyyyy, _to_strategy_payload
from simulator.delta_exchange_ws import delta_ticker_manager

log = logging.getLogger(__name__)

delta_live_chain_socket_router = APIRouter()

PUSH_INTERVAL_SECONDS = 2.0  # matches live_greeks_chain_socket.py's NSE cadence
SUPPORTED_UNDERLYINGS = ("BTC", "ETH")


def _build_chain_payload(underlying: str, expiry: str) -> dict | None:
    # `expiry` arrives here exactly as the frontend's subscribe message sent it — ISO
    # (YYYY-MM-DD) or "" for nearest, see _to_strategy_payload's own docstring for why
    # every expiry this app hands the frontend is ISO. delta_ticker_manager/
    # fetch_option_chain_rest below are Delta-native calls that only understand Delta's
    # own DD-MM-YYYY — get_delta_rest_option_chain (the REST sibling of this same data)
    # already converts before calling either; this WS path was missing the same
    # conversion, so any concrete (non-"") expiry silently resolved to a garbage
    # suffix/expiry that matched no real option symbols. Beyond just wrong/empty chains,
    # this fed a bad "expiry" back through ensure_subscribed on every call for a specific
    # expiry (main/overlay/picker/prewarm all request one), which — before
    # delta_ticker_manager's _subscriptions was made per-(underlying, expiry) — kept
    # clobbering whichever OTHER expiry of the same underlying was concurrently
    # subscribed, and even now would just never resolve to real symbols at all.
    delta_expiry = _iso_to_ddmmyyyy(expiry) if expiry else ""
    resolved_expiry = delta_ticker_manager.ensure_subscribed(underlying, delta_expiry)
    if not resolved_expiry:
        return None
    expiries = delta_ticker_manager.get_expiries(underlying)
    snapshot = delta_ticker_manager.get_chain_snapshot(underlying, resolved_expiry)
    if snapshot is None:
        # Cold (just subscribed, or a tick went stale) — one REST round trip to
        # prime it. Same fallback the REST endpoint itself uses (see
        # delta_exchange_router.py's get_delta_rest_option_chain).
        try:
            snapshot = fetch_option_chain_rest(underlying, resolved_expiry)
            expiries = snapshot.get("expiries") or expiries
        except Exception as exc:
            log.warning("[DeltaChainHub] REST fallback failed for %s: %s", underlying, exc)
            return None
    payload = _to_strategy_payload(underlying, snapshot, expiries)
    # requested_expiry = the exact subscribe-key expiry this push's broadcaster task is
    # registered under ("" for nearest, or whatever concrete ISO expiry was asked for) —
    # NOT payload["expiry"], which is always the resolved concrete date even for a ""
    # (nearest) task. The frontend's useLiveChainSocket.ts used to guess which of its
    # subscription keys a push belonged to purely from (instrument, resolved expiry),
    # which silently misrouted every push here into ANY "" (nearest) subscriber for the
    # same instrument too — harmless with one watched expiry per instrument, but once a
    # second concurrent expiry (e.g. CryptoTradeNew.tsx's "prewarm") is also subscribed,
    # its pushes kept overwriting the nearest subscriber's chain with the wrong expiry's
    # data and back, which is what caused the subscribe/unsubscribe thrashing between the
    # two expiries. Stamping the untranslated subscribe-key expiry here lets the frontend
    # match exactly instead of guessing.
    payload["requested_expiry"] = expiry
    return payload


class _DeltaChainHub:
    """Per-(underlying, expiry) broadcaster — mirrors live_greeks_chain_socket.py's
    _GreeksChainHub so this file stays a drop-in sibling, not a divergent design."""

    def __init__(self) -> None:
        self._clients: dict[tuple[str, str], set[WebSocket]] = {}
        self._tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._latest_payloads: dict[tuple[str, str], tuple[float, dict]] = {}
        self._lock = asyncio.Lock()

    async def register(self, key: tuple[str, str], ws: WebSocket) -> None:
        async with self._lock:
            existing = self._clients.setdefault(key, set())
            had_clients_already = bool(existing)
            existing.add(ws)
            needs_new_task = key not in self._tasks or self._tasks[key].done()
            if needs_new_task:
                self._tasks[key] = asyncio.create_task(self._broadcaster_loop(key))
        if had_clients_already and not needs_new_task:
            asyncio.create_task(self._send_immediate(key, ws))

    async def unregister(self, key: tuple[str, str], ws: WebSocket) -> None:
        async with self._lock:
            clients = self._clients.get(key)
            if not clients:
                return
            clients.discard(ws)
            if not clients:
                self._clients.pop(key, None)
                task = self._tasks.pop(key, None)
                if task:
                    task.cancel()

    async def _send_immediate(self, key: tuple[str, str], ws: WebSocket) -> None:
        underlying, expiry = key
        cached = self._latest_payloads.get(key)
        if cached and (time.monotonic() - cached[0]) < PUSH_INTERVAL_SECONDS + 0.5:
            try:
                await ws.send_text(json.dumps(cached[1]))
            except Exception:
                pass
            return
        try:
            payload = await asyncio.to_thread(_build_chain_payload, underlying, expiry)
            if payload:
                self._latest_payloads[key] = (time.monotonic(), payload)
                await ws.send_text(json.dumps(payload))
        except Exception:
            pass

    async def _broadcaster_loop(self, key: tuple[str, str]) -> None:
        underlying, expiry = key
        try:
            while True:
                async with self._lock:
                    clients = list(self._clients.get(key) or [])
                if not clients:
                    return
                try:
                    payload = await asyncio.to_thread(_build_chain_payload, underlying, expiry)
                    if payload:
                        self._latest_payloads[key] = (time.monotonic(), payload)
                        msg = json.dumps(payload)
                        dead: list[WebSocket] = []
                        for ws in clients:
                            try:
                                await ws.send_text(msg)
                            except Exception:
                                dead.append(ws)
                        if dead:
                            async with self._lock:
                                live = self._clients.get(key)
                                if live:
                                    for ws in dead:
                                        live.discard(ws)
                except Exception as exc:
                    log.warning("[DeltaChainHub] broadcast error key=%s: %s", key, exc)
                await asyncio.sleep(PUSH_INTERVAL_SECONDS)
        finally:
            pass


_hub = _DeltaChainHub()


@delta_live_chain_socket_router.websocket("/ws/live-greeks-chain")
async def delta_live_chain_socket_multi(websocket: WebSocket) -> None:
    """Multiplexed: one connection, many (instrument, expiry) pairs, added/dropped via
    {action:"subscribe"|"unsubscribe", instrument, expiry} — exact same message shape
    live_greeks_chain_socket.py's NSE sibling speaks, since useLiveChainSocketMulti on
    the frontend is shared code between both pages."""
    await websocket.accept()
    held: set[tuple[str, str]] = set()
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            action = str(msg.get("action") or "").strip().lower()
            underlying = str(msg.get("instrument") or "").strip().upper()
            expiry = str(msg.get("expiry") or "").strip()
            if not underlying or underlying not in SUPPORTED_UNDERLYINGS:
                continue
            key = (underlying, expiry)
            if action == "subscribe":
                if key not in held:
                    held.add(key)
                    await _hub.register(key, websocket)
            elif action == "unsubscribe":
                if key in held:
                    held.discard(key)
                    await _hub.unregister(key, websocket)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        for key in list(held):
            try:
                await _hub.unregister(key, websocket)
            except Exception:
                pass
