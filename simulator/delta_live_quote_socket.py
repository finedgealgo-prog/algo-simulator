"""
delta_live_quote_socket.py
────────────────────────────
Browser-facing WebSocket that streams live LTP for Delta Exchange crypto
option/perpetual symbols — the crypto analog of shared/features/
live_quote_socket.py's /ws/live-quotes hub, deliberately built as a close
protocol mirror (same "replace"/"subscribe"/"unsubscribe" actions, same
{"type": "ltp_update", "data": [{"token", "ltp"}], "server_time"} push shape)
so the existing frontend hook (useLiveQuoteSocket.ts) can be pointed at this
endpoint unchanged.

What's NOT ported from the NSE hub, on purpose — none of these have a crypto
equivalent worth building for v1:
  - "resolve" / "attach_instrument" / "watch_instruments" — chart/watchlist
    concepts tied to NSE's active_option_tokens + instrument_ref_manager
    admission control; the crypto page's own option-chain REST endpoints
    already hand the frontend a symbol string directly, nothing to resolve
    server-side.
  - "auth" / MTM broadcast — no crypto paper-trade strategy MTM push yet.
  - Broker-connection admission control (CHART_TOKEN_ADMISSION_CAP) — this
    hub subscribes crypto symbols into delta_ticker_manager's in-process
    cache, not a capped broker WS connection; a handful of legs per session
    is the natural ceiling here, nothing to protect against.

Data source: delta_exchange_ws.py's delta_ticker_manager — the same
process-local singleton the REST /option-chain and /rest-option-chain/
{instrument} endpoints already read. This is WHY this endpoint has to live
in algo.simulator (port 8001) rather than algo.websocket: the ticker manager
is an in-process cache + background thread, not shared across processes.

Symbol shapes handled (see _parse_delta_symbol):
  - Option: "{C|P}-{underlying}-{strike}-{DDMMYY}", e.g. "C-BTC-65000-090826"
    -> parsed into (underlying, expiry); ensure_subscribed() is called so
    delta_ticker_manager actually starts/keeps receiving ticks for that
    underlying+expiry's whole chain (cheap/idempotent — see its docstring).
    ltp is scaled by DELTA_CONTRACT_VALUE the same way delta_exchange_client.
    _leg_row does for the REST chain endpoints, since the frontend's P&L math
    expects that convention, not Delta's raw per-1-unit quote.
  - Perpetual future: "{underlying}USD", e.g. "BTCUSD" — ensure_subscribed()
    already adds the bare perpetual symbol as a side effect of subscribing
    to any expiry for that underlying (see delta_exchange_ws.ensure_subscribed),
    so the same ensure_subscribed(underlying, "") call covers this case too.
    Its mark_price is NOT contract-value-scaled — see DELTA_CONTRACT_VALUE's
    comment in delta_exchange_client.py: fetch_perpetual_ticker's mark_price
    is already the real spot-equivalent USD quote.

No auth — public market-data ticks only, nothing user-specific (unlike the
NSE hub's opt-in MTM broadcast, which this doesn't build).
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from simulator.delta_exchange_client import DELTA_CONTRACT_VALUE
from simulator.delta_exchange_ws import delta_ticker_manager

log = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30))
EMIT_INTERVAL_SECONDS = 0.5

delta_live_quote_socket_router = APIRouter(prefix="/simulator/crypto")


def _now_iso() -> str:
    return datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")


def _parse_delta_symbol(symbol: str) -> dict | None:
    """
    "{C|P}-{underlying}-{strike}-{DDMMYY}" (option), "{underlying}USD"
    (perpetual future), or "{underlying}-SPOT" (underlying index price) ->
    {"kind": "option"/"perpetual"/"spot", "underlying": str, "expiry":
    "DD-MM-YYYY" or ""}. Returns None for anything unrecognized.

    Same DDMMYY suffix convention delta_exchange_client.list_expiries already
    decodes (dd, mm, 20+yy) — kept local rather than imported since that
    function parses a whole product list's symbols, not one arbitrary token.

    "-SPOT" is a synthetic client-facing token (CryptoFullChartWorkspace.tsx)
    — Delta itself has no live-streamed spot symbol to subscribe to; it's
    resolved by reading the *same* "{underlying}USD" perpetual ticker cache
    the "perpetual" kind reads (ensure_subscribed keeps that ticker live
    either way), just picking its spot_price field over mark_price. See
    _resolve_ltp for why "perpetual" itself can't just be switched to
    spot_price instead: open futures positions' live P&L (CryptoTradeNew.tsx)
    reads "{underlying}USD" too and needs the real mark price for that.
    """
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return None

    parts = symbol.split("-")
    if len(parts) == 4 and parts[0] in ("C", "P"):
        _side, underlying, strike, ddmmyy = parts
        if underlying and strike.isdigit() and len(ddmmyy) == 6 and ddmmyy.isdigit():
            dd, mm, yy = ddmmyy[0:2], ddmmyy[2:4], ddmmyy[4:6]
            return {"kind": "option", "underlying": underlying, "expiry": f"{dd}-{mm}-20{yy}"}
        return None

    if len(parts) == 2 and parts[1] == "SPOT":
        underlying = parts[0]
        if underlying:
            return {"kind": "spot", "underlying": underlying, "expiry": ""}
        return None

    if len(parts) == 1 and symbol.endswith("USD"):
        underlying = symbol[:-3]
        if underlying:
            return {"kind": "perpetual", "underlying": underlying, "expiry": ""}

    return None


def _resolve_ltp(token: str, meta: dict) -> float:
    """Reads delta_ticker_manager's live cache for one symbol and applies the
    same USD scaling _leg_row (delta_exchange_client.py) applies for the REST
    chain endpoints, so this socket's numbers agree with the REST-fetched
    chain the frontend already renders. Returns 0.0 if no tick has landed yet
    (just-subscribed, or Delta hasn't pushed one for this symbol)."""
    if meta["kind"] == "spot":
        # Reads the perpetual's own ticker payload (it carries both
        # mark_price and spot_price) rather than "token" (the "-SPOT"
        # synthetic string, which was never subscribed to Delta itself).
        ticker = delta_ticker_manager.get_ticker(f"{meta['underlying']}USD")
        if not ticker:
            return 0.0
        return float(ticker.get("spot_price") or 0)

    ticker = delta_ticker_manager.get_ticker(token)
    if not ticker:
        return 0.0
    mark_price = float(ticker.get("mark_price") or 0)
    if mark_price <= 0:
        return 0.0
    if meta["kind"] == "option":
        contract_value = DELTA_CONTRACT_VALUE.get(meta["underlying"], 1.0)
        return mark_price * contract_value
    return mark_price  # perpetual — already real USD, no scaling (see module docstring)


@dataclass
class _DeltaLiveQuoteSession:
    websocket: WebSocket
    session_id: str
    subscribed_tokens: set[str] = field(default_factory=set)
    last_sent: dict[str, float] = field(default_factory=dict)
    closed: bool = False
    task: asyncio.Task | None = None


class _DeltaLiveQuoteHub:
    def __init__(self) -> None:
        self._sessions: dict[str, _DeltaLiveQuoteSession] = {}
        self._lock = asyncio.Lock()
        # token -> parsed {"kind", "underlying", "expiry"} — resolved once per
        # symbol (not per session, not per tick) since parsing + the
        # ensure_subscribed() call it triggers are the only non-trivial work
        # in this whole hub; every other session subscribing to the same
        # token afterward just reuses this entry.
        self._token_meta: dict[str, dict] = {}

    async def register(self, websocket: WebSocket) -> _DeltaLiveQuoteSession:
        await websocket.accept()
        session = _DeltaLiveQuoteSession(websocket=websocket, session_id=uuid.uuid4().hex)
        async with self._lock:
            self._sessions[session.session_id] = session
        session.task = asyncio.create_task(self._emit_loop(session))
        await session.websocket.send_text(json.dumps({
            "type": "message",
            "data": {"message": "delta live quote socket connected", "session_id": session.session_id},
            "server_time": _now_iso(),
        }))
        return session

    async def unregister(self, session: _DeltaLiveQuoteSession) -> None:
        session.closed = True
        if session.task and not session.task.done():
            session.task.cancel()
            try:
                await session.task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.debug("delta live quote task close error session=%s: %s", session.session_id, exc)
        async with self._lock:
            self._sessions.pop(session.session_id, None)

    async def _ensure_tokens_live(self, tokens: list[str]) -> None:
        """For each not-yet-seen token, parse it and kick delta_ticker_manager
        into actually subscribing that underlying/expiry (idempotent, cheap
        on repeat calls — see ensure_subscribed's docstring). Runs in a
        thread since ensure_subscribed can hit Delta's REST /v2/products on
        a cold underlying+expiry."""
        new_tokens = [t for t in tokens if t not in self._token_meta]
        if not new_tokens:
            return
        for token in new_tokens:
            meta = _parse_delta_symbol(token)
            if meta is None:
                log.debug("delta live quote: unrecognized symbol %s", token)
                continue
            self._token_meta[token] = meta
            await asyncio.to_thread(delta_ticker_manager.ensure_subscribed, meta["underlying"], meta["expiry"])

    async def handle_client_message(self, session: _DeltaLiveQuoteSession, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message or "{}")
        except Exception:
            return
        action = str(payload.get("action") or "").strip().lower()
        tokens = [str(t or "").strip() for t in (payload.get("tokens") or []) if str(t or "").strip()]

        new_tokens: list[str] = []
        if action == "unsubscribe":
            for token in tokens:
                session.subscribed_tokens.discard(token)
                session.last_sent.pop(token, None)
            return
        elif action == "subscribe":
            new_tokens = [t for t in tokens if t not in session.subscribed_tokens]
            session.subscribed_tokens.update(new_tokens)
        elif action == "replace":
            # Same "resend the full current set" semantics as the NSE hub's
            # "replace" — the frontend's basket changes as a whole on every
            # add/remove/expiry-change, not a client-side diff.
            new_tokens = [t for t in tokens if t not in session.subscribed_tokens]
            removed_tokens = session.subscribed_tokens - set(tokens)
            session.subscribed_tokens = (session.subscribed_tokens - removed_tokens) | set(new_tokens)
            for token in removed_tokens:
                session.last_sent.pop(token, None)
        else:
            return

        if new_tokens:
            await self._ensure_tokens_live(new_tokens)

    async def _emit_loop(self, session: _DeltaLiveQuoteSession) -> None:
        try:
            while not session.closed:
                if session.subscribed_tokens:
                    changed = self._collect_changed_ltp(session)
                    if changed:
                        await session.websocket.send_text(json.dumps({
                            "type": "ltp_update",
                            "data": changed,
                            "server_time": _now_iso(),
                        }))
                await asyncio.sleep(EMIT_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("delta live quote emit loop error session=%s: %s", session.session_id, exc)

    def _collect_changed_ltp(self, session: _DeltaLiveQuoteSession) -> list[dict]:
        """Pure dict lookups, no I/O — same "diff since last emit" shape as
        the NSE hub's _collect_changed_ltp. Crypto's per-session symbol count
        is naturally tiny (a handful of legs), so this stays a plain loop."""
        changed: list[dict] = []
        for token in session.subscribed_tokens:
            meta = self._token_meta.get(token)
            if meta is None:
                continue  # unrecognized symbol — never resolved, nothing to push
            ltp = _resolve_ltp(token, meta)
            if ltp <= 0 or session.last_sent.get(token) == ltp:
                continue
            session.last_sent[token] = ltp
            changed.append({"token": token, "ltp": ltp})
        return changed

    def get_status(self) -> dict:
        return {"connections": len(self._sessions)}


delta_live_quote_hub = _DeltaLiveQuoteHub()


@delta_live_quote_socket_router.get("/live-quotes/status")
async def delta_live_quote_status():
    return delta_live_quote_hub.get_status()


@delta_live_quote_socket_router.websocket("/ws/live-quotes")
async def delta_live_quote_socket(websocket: WebSocket):
    session = await delta_live_quote_hub.register(websocket)
    try:
        while True:
            raw_message = await websocket.receive_text()
            await delta_live_quote_hub.handle_client_message(session, raw_message)
    except WebSocketDisconnect:
        pass
    finally:
        await delta_live_quote_hub.unregister(session)
