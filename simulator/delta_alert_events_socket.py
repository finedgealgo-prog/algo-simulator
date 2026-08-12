"""
delta_alert_events_socket.py
──────────────────────────────
Push channel that tells the crypto full-chart page (CryptoFullChartWorkspace.
tsx) the instant one of its BTC/ETH price/trendline/indicator alerts has
actually fired — the crypto analog of shared/features/alert_events_socket.py.

Deliberately a SEPARATE websocket/channel/room registry from that NSE one,
not a shared "chart-alerts" channel repointed at a different price source —
same reasoning as delta_live_quote_socket.py being its own hub instead of
reusing the shared /ws/live-quotes: this runs 24/7 (see delta_alert_checker.
py, no NSE market-hours auto-stop), so its lifecycle, its own room key
("crypto-chart-alerts" instead of "chart-alerts"), and its own connected-
sockets registry needed to be independent of whatever the NSE alert checker/
its websocket happen to be doing (running, stopped for the night, restarted).

Reuses execution_socket.py's generic per-user-channel room registry and JWT-
as-first-message auth handshake (same helpers alert_events_socket.py itself
reuses) rather than reimplementing connection management a third time — those
helpers are already parameterized by channel name, this just uses its own.

mark_delta_alert_fired() is the sync half: delta_alert_checker.py's
_fire_alert runs on a worker thread (via asyncio.to_thread), so it can't
await a broadcast directly — it just appends to PENDING_CRYPTO_ALERT_EVENTS
(plain dict/list mutation, GIL-safe), flushed by whichever connected socket
polls next.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from features.execution_socket import (
    _broadcast_user_channel_message,
    _build_message,
    _register_user_websocket,
    _unregister_user_websocket,
    _ws_authenticate,
)

log = logging.getLogger(__name__)

CRYPTO_ALERT_EVENTS_CHANNEL = "crypto-chart-alerts"
POLL_TIMEOUT_SECONDS = 1.0

# user_id -> list of fired-alert payload dicts queued since the last flush.
PENDING_CRYPTO_ALERT_EVENTS: dict[str, list[dict]] = {}

delta_alert_events_socket_router = APIRouter(prefix="/simulator/crypto")


def mark_delta_alert_fired(user_id: str, payload: dict) -> None:
    """Sync — called from delta_alert_checker.py's _fire_alert (worker thread)."""
    uid = str(user_id or "").strip()
    if not uid:
        return
    PENDING_CRYPTO_ALERT_EVENTS.setdefault(uid, []).append(payload)


async def _flush_pending_alert_events(user_id: str) -> None:
    events = PENDING_CRYPTO_ALERT_EVENTS.pop(user_id, None)
    if not events:
        return
    message = _build_message("alert_fired", "Crypto chart alert triggered", {"events": events})
    await _broadcast_user_channel_message(user_id, CRYPTO_ALERT_EVENTS_CHANNEL, message)


@delta_alert_events_socket_router.websocket("/ws/alert-events")
async def delta_alert_events_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    user_id = await _ws_authenticate(websocket)
    if not user_id:
        return

    _register_user_websocket(CRYPTO_ALERT_EVENTS_CHANNEL, user_id, websocket)
    await websocket.send_text(_build_message(
        "connection_established",
        "Crypto alert events websocket connected",
        {"channel": CRYPTO_ALERT_EVENTS_CHANNEL, "user_id": user_id},
    ))
    # Anything that fired between the alert being armed and this socket
    # actually connecting (e.g. page refresh mid-fire) is still delivered
    # on the very first poll below rather than lost.
    await _flush_pending_alert_events(user_id)

    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=POLL_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                pass
            await _flush_pending_alert_events(user_id)
    except WebSocketDisconnect:
        return
    except Exception:
        log.exception("[delta_alert_events_socket] connection error for user %s", user_id)
    finally:
        _unregister_user_websocket(websocket)
