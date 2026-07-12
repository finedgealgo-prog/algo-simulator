"""
Simulator-only entrypoint for the split backend.

`api.py` here is a trimmed copy of the original monolith: every /algo,
/trade, /broker, /live, /monitor, /mock route plus the scanner/signal_builder/
common imports and mounts have been removed from the source itself (not just
hidden), and the scanner/signal_builder/common packages are not present in
this folder at all. What's left only serves the "Simulator" frontend pages
(Paper Trade, Portfolio, Positions, Chart) — the /simulator/* REST API.

Central-tick mode
─────────────────
Like algo.trade, this process does NOT open its own broker WS. It connects to
algo.websocket's /ws/internal-ticks via CentralTickClient, keeping a local
ltp_map updated in-process for paper-trade SL/TP checks (simulator_risk_monitor
reads ltp_map directly — no network hop). The shared /ws/live-quotes hub now
lives only in algo.websocket — see _DROP_ROUTE_PREFIXES note below.

Run (from /media/ashok-innoppl/7CD60970D6092C48/algo-backend/algo.simulator):
    uvicorn simulator_main:app --reload --port 8001
"""

import asyncio
import logging
import threading

import requests

from api import app  # noqa: E402

log = logging.getLogger(__name__)

# Skip: span/redis startup (algo-only)
# Skip: alert_checker (depends on removed scanner package)
# Skip: _auto_start_ticker (central-tick mode; algo.websocket owns broker WS)
SKIP_STARTUP_FUNCS = {
    "_span_params_startup",
    "_redis_prewarm",
    "_auto_start_alert_checker",
    "_auto_start_ticker",
}

_DROP_ROUTE_PREFIXES = ("/ws/live-quotes", "/live-quotes")

app.router.on_startup = [
    f for f in app.router.on_startup
    if f.__name__ not in SKIP_STARTUP_FUNCS
]
app.router.routes = [
    r for r in app.router.routes
    if not str(getattr(r, "path", "")).startswith(_DROP_ROUTE_PREFIXES)
]


# ── Central tick client startup ───────────────────────────────────────────────

def _start_central_ticker() -> None:
    from features.central_tick_client import CentralTickClient
    from features.broker_gateway import broker_ticker_manager
    from features.mongo_data import MongoData

    client = CentralTickClient("http://localhost:8003")
    broker_ticker_manager.set_central_client(client)

    db = MongoData()
    try:
        client.start(db._db)
    finally:
        db.close()

    log.info("[simulator_main] CentralTickClient started — broker WS owned by algo.websocket")


async def _wait_for_websocket_ready(
    url: str = "http://localhost:8003/health",
    max_wait: float = 30.0,
    poll_interval: float = 0.3,
) -> bool:
    """
    Poll algo.websocket's /health instead of a blind fixed sleep — connects
    the moment it's actually ready (often well under 3s) instead of always
    waiting the full guess window, and keeps polling past 3s if it's slow
    (Mongo/broker-token validation) instead of firing one connect attempt
    that fails and falls into CentralTickClient's own (slower) backoff.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + max_wait
    while loop.time() < deadline:
        try:
            resp = await asyncio.to_thread(requests.get, url, timeout=1.0)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                return True
        except Exception:
            pass
        await asyncio.sleep(poll_interval)
    return False


@app.on_event("startup")
async def _auto_start_central_ticker() -> None:
    async def _bg() -> None:
        ready = await _wait_for_websocket_ready()
        if ready:
            log.info("[simulator_main] algo.websocket /health ok — starting CentralTickClient")
        else:
            log.warning(
                "[simulator_main] algo.websocket not ready after 30s — starting "
                "CentralTickClient anyway (it retries internally on disconnect)"
            )
        try:
            threading.Thread(target=_start_central_ticker, daemon=True).start()
        except Exception:
            log.exception("[STARTUP] CentralTickClient start failed")
    asyncio.create_task(_bg())


# ── Chart domain (TradingView chart-state/alerts/symbol search+history) ──────
# chart_api.py is symlinked in from ../shared/chart_api.py, same as
# algo.scanner does — now importable here too because its data layer
# (features/chart_data.py) no longer depends on the scanner-only `scanner`
# package. Deliberately NOT calling start_chart_background_loops() here:
# that starts the price/trendline + indicator alert-checker polling loop,
# which algo.scanner's process already runs — running it a second time here
# would double-evaluate every alert and fire each webhook twice. This mount
# is REST-only (chart-state/alerts CRUD, symbol_search, symbol_historical_chart),
# so the algo-admin frontend's chart page works whether it's pointed at
# algo.scanner (8002) or algo.simulator (8001).
from chart_api import router as chart_router  # noqa: E402

app.include_router(chart_router)
