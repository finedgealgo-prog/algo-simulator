"""
analyse_entity_socket.py
──────────────────────────
Socket push for the crypto Analyse view (AnalyseCryptoPaperTrade.tsx's
fetchExternalEntity — the "Analyse" button flow off FastForward2.tsx /
strategy, group, and portfolio rows alike). Was a plain 30s REST poll per
open tab (see that function's own comment: "on a plain 30s ... interval"),
independently re-hitting GET /algo/strategy-trade-history/... on its own
timer with zero sharing between tabs — same shape of problem
delta_live_chain_socket.py's own docstring describes for the option-chain
REST-poll it replaced.

Same `_*Hub` shared-broadcaster-per-key pattern as that file's
_DeltaChainHub (and live_greeks_chain_socket.py's NSE _GreeksChainHub):
ONE background task per (entity_type, entity_id, status) key, computed
once per PUSH_INTERVAL_SECONDS and fanned out to every client watching
that same key — N tabs analysing the SAME strategy/group/portfolio cost
ONE upstream fetch per interval, not N.

The underlying trade-history data itself is NOT recomputed here — this
module owns only the connection/broadcast lifecycle. Each tick makes ONE
plain HTTP call to the old system's existing GET /algo/strategy-trade-
history/(portfolio|group)?/{id} route (algo.trade/api.py), which already
correctly proxies to algo-2_0 for an engine="algo-2_0" strategy — reusing
that endpoint rather than re-implementing its (already reused-by-old-
system-too) engine-aware backfill/reconstruction logic a third time in a
third codebase.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import requests
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

log = logging.getLogger(__name__)

analyse_entity_socket_router = APIRouter()

PUSH_INTERVAL_SECONDS = 5.0
ALGO_TRADE_API_BASE = os.getenv("ALGO_TRADE_API_BASE_URL", "http://localhost:8010/algo")

_EntityKey = tuple[str, str, str]  # (entity_type, entity_id, status)


def _path_for(entity_type: str, entity_id: str) -> str:
    if entity_type == "portfolio":
        return f"strategy-trade-history/portfolio/{entity_id}"
    if entity_type == "group":
        return f"strategy-trade-history/group/{entity_id}"
    return f"strategy-trade-history/{entity_id}"


def _fetch_payload(entity_type: str, entity_id: str, status: str) -> dict:
    """Blocking (plain `requests`) — run via asyncio.to_thread by every
    caller below, same convention algo.trade's own cross-service proxy to
    algo-2_0 already uses (this whole codebase is otherwise sync-Mongo
    everywhere, no reason for this one HTTP call to be the odd async one)."""
    try:
        resp = requests.get(
            f"{ALGO_TRADE_API_BASE}/{_path_for(entity_type, entity_id)}",
            params={"status": status},
            timeout=10.0,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        log.exception("[AnalyseEntityHub] fetch failed entity_type=%s entity_id=%s", entity_type, entity_id)
        return {"error": "fetch_failed"}


class _AnalyseEntityHub:
    """Per-(entity_type, entity_id, status) broadcaster — same shape as
    delta_live_chain_socket.py's _DeltaChainHub, kept a drop-in sibling
    rather than a divergent design."""

    def __init__(self) -> None:
        self._clients: dict[_EntityKey, set[WebSocket]] = {}
        self._tasks: dict[_EntityKey, asyncio.Task] = {}
        self._latest_payloads: dict[_EntityKey, tuple[float, dict]] = {}
        self._lock = asyncio.Lock()

    async def register(self, key: _EntityKey, ws: WebSocket) -> None:
        async with self._lock:
            existing = self._clients.setdefault(key, set())
            had_clients_already = bool(existing)
            existing.add(ws)
            needs_new_task = key not in self._tasks or self._tasks[key].done()
            if needs_new_task:
                self._tasks[key] = asyncio.create_task(self._broadcaster_loop(key), name=f"analyse-entity-{key}")
        if had_clients_already and not needs_new_task:
            asyncio.create_task(self._send_immediate(key, ws))

    async def unregister(self, key: _EntityKey, ws: WebSocket) -> None:
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
                self._latest_payloads.pop(key, None)

    async def _send_immediate(self, key: _EntityKey, ws: WebSocket) -> None:
        cached = self._latest_payloads.get(key)
        if cached and (time.monotonic() - cached[0]) < PUSH_INTERVAL_SECONDS + 0.5:
            try:
                await ws.send_text(json.dumps(cached[1]))
            except Exception:
                pass
            return
        entity_type, entity_id, status = key
        try:
            payload = await asyncio.to_thread(_fetch_payload, entity_type, entity_id, status)
            self._latest_payloads[key] = (time.monotonic(), payload)
            await ws.send_text(json.dumps(payload))
        except Exception:
            pass

    async def _broadcaster_loop(self, key: _EntityKey) -> None:
        entity_type, entity_id, status = key
        try:
            while True:
                async with self._lock:
                    clients = list(self._clients.get(key) or [])
                if not clients:
                    return
                try:
                    payload = await asyncio.to_thread(_fetch_payload, entity_type, entity_id, status)
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
                except Exception:
                    log.exception("[AnalyseEntityHub] broadcast error key=%s", key)
                await asyncio.sleep(PUSH_INTERVAL_SECONDS)
        finally:
            pass


_hub = _AnalyseEntityHub()


@analyse_entity_socket_router.websocket("/simulator/crypto/ws/analyse-entity")
async def analyse_entity_socket(websocket: WebSocket) -> None:
    """Multiplexed, same subscribe/unsubscribe shape as delta_live_chain_
    socket.py's own multiplexed endpoint: {action:"subscribe"|
    "unsubscribe", entityType, entityId, status} in, the SAME JSON shape
    GET /algo/strategy-trade-history/... already returns pushed straight
    back out (no reshaping — fetchExternalEntity's own leg-mapper stays
    unchanged, only its data SOURCE moves from a REST poll to this push)."""
    await websocket.accept()
    held: set[_EntityKey] = set()
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            action = str(msg.get("action") or "subscribe").strip().lower()
            entity_type = str(msg.get("entityType") or "strategy").strip().lower()
            entity_id = str(msg.get("entityId") or "").strip()
            status = str(msg.get("status") or "algo-backtest").strip()
            if not entity_id:
                continue
            key = (entity_type, entity_id, status)
            if action == "unsubscribe":
                if key in held:
                    held.discard(key)
                    await _hub.unregister(key, websocket)
                continue
            if key not in held:
                held.add(key)
                await _hub.register(key, websocket)
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("[AnalyseEntitySocket] connection error")
    finally:
        for key in list(held):
            try:
                await _hub.unregister(key, websocket)
            except Exception:
                pass
