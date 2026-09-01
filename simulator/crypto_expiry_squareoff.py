"""
crypto_expiry_squareoff.py
────────────────────────────
Auto-closes crypto_simulator_strategy (Delta Exchange BTC/ETH paper-trade)
positions once for the day, right before their expiry settles — the crypto
analog of api.py's NSE expiry square-off (_auto_expiry_squareoff_catchup →
simulator_risk_monitor.run_startup_expiry_catchup /
_auto_squareoff_expired_legs, cutoff EXPIRY_SQUAREOFF_TIME="15:29" IST).

Genuine once-a-day cron, not a continuous poll: Delta's daily BTC/ETH
options settle at a real, fixed time — 12:00 UTC (17:30 IST, see
delta_exchange_router._is_expiry_settled for the exact formula) — so there's
nothing to gain from checking more than once a day. DAILY_TRIGGER_UTC fires
one minute BEFORE that, at 11:59 UTC, deliberately — grabbing each leg's
live mark price a minute early, while Delta is still actively quoting it,
is more reliable than trying right at/after settlement once the contract
may already be in the process of being delisted from live quotes.

Eligibility is a plain date compare (`expiry <= today`, UTC), not
_is_expiry_settled — this only ever runs once daily at 11:59 UTC, so
"expiry is today" already means "one minute from settling" without needing
the exact 12:00 cutoff, and folds in the safety net for free: any older
leg that was somehow still open from a previous day (server was down, a
prior run failed, ...) matches the same `<= today` check.

No polling at all — this repo has no cron/APScheduler dependency (checked;
none exists anywhere in it), so rather than pull one in for a single daily
job, the loop computes the exact number of seconds until the next
DAILY_TRIGGER_UTC and does one `asyncio.sleep()` for that whole duration,
same technique a plain `while True: sleep(...)` cron substitute always
uses. Nothing wakes up in between — no per-tick DB/CPU cost the way a
polling dedup loop (e.g. shared/features/market_hours_scheduler.py's
Mon-Fri/NSE-hours one, wrong fit here anyway since crypto settles every
single day) would have.

A process restart lands mid-sleep-computation, not mid-sleep — the very
first thing the loop does on (re)start is compute "how long until the next
11:59 UTC" from the current wall clock, so a restart right after today's
11:59 has already passed just computes tomorrow's 11:59 and sleeps until
then. That's a real gap versus the old polling version (which caught up
immediately on any restart, even one right after settlement) — the
DB-level safety net (`expiry <= today`, not just `== today`, in
_squareoff_expired_crypto_legs) is what covers a leg missed by a same-day
restart; it just waits for the *next* day's 11:59 to sweep it up instead
of firing right away.

Started unconditionally at process boot (see api.py's
_auto_start_crypto_expiry_squareoff), same as delta_alert_checker's loops —
never registered with market_hours_scheduler's NSE auto-stop.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger(__name__)

# HH:MM UTC — one minute before Delta's real 12:00 UTC settlement (see
# module docstring for why 1 minute early, not exactly at/after 12:00).
DAILY_TRIGGER_UTC = (11, 59)


def _seconds_until_next_trigger(now: datetime) -> float:
    trigger = now.replace(hour=DAILY_TRIGGER_UTC[0], minute=DAILY_TRIGGER_UTC[1], second=0, microsecond=0)
    if trigger <= now:
        trigger += timedelta(days=1)
    return (trigger - now).total_seconds()


async def _squareoff_expired_crypto_legs() -> None:
    """
    Finds every open (not yet exited) crypto_simulator_strategy leg whose
    expiry is today or earlier (UTC date) and closes it at Delta's current
    mark price, falling back to entry_price if no live tick is cached for
    that symbol. One doc's failure never blocks the rest — see the per-doc
    try/except below, mirroring
    simulator_risk_monitor._auto_squareoff_expired_legs' same isolation.

    Called once/day by run_crypto_expiry_squareoff_loop's 11:59 UTC trigger
    — see module docstring for why eligibility is a plain date compare
    rather than delta_exchange_router._is_expiry_settled (that function's
    exact-12:00-UTC cutoff would reject a still-11:59 today's-expiry leg).
    """
    from bson import ObjectId

    from features.delta_exchange_client import DELTA_CONTRACT_VALUE
    from features.delta_exchange_ws import delta_ticker_manager
    from features.sim_plans import resolve_user_plan
    from features.telegram_notifier import notify_user_for
    from simulator.crypto_paper_trade_router import (
        CRYPTO_STRATEGY_COLLECTION,
        _shared_mongo,
    )
    from simulator.delta_exchange_router import _iso_to_ddmmyyyy

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    raw_db = _shared_mongo._db
    try:
        docs = list(raw_db[CRYPTO_STRATEGY_COLLECTION].find(
            {"status": {"$ne": 2}},
            {"_id": 1, "instrument": 1, "strategy_name": 1, "positions": 1, "user_id": 1},
        ))
    except Exception as exc:
        log.warning("[CRYPTO EXPIRY SQUAREOFF] DB query error: %s", exc)
        return

    if not docs:
        return

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for doc in docs:
        strategy_id = str(doc.get("_id") or "")
        strategy_name = str(doc.get("strategy_name") or strategy_id)
        try:
            plan = resolve_user_plan(doc.get("user_id"))
            if not plan.get("auto_position_management", True):
                # Same "Free plan = manual close only" rule the NSE monitor
                # applies to its own SL/Target + expiry-day auto-exits.
                continue

            underlying = str(doc.get("instrument") or "").strip().upper()
            contract_value = DELTA_CONTRACT_VALUE.get(underlying, 1.0)
            positions = list(doc.get("positions") or [])
            eligible_idx: list[int] = []
            for i, pos in enumerate(positions):
                if not isinstance(pos, dict) or pos.get("exited"):
                    continue
                expiry = str(pos.get("expiry") or "")[:10]
                if not expiry:
                    continue  # perpetual futures/legs with no expiry never square off here
                if expiry <= today_str:
                    eligible_idx.append(i)
            if not eligible_idx:
                continue

            exited_count = 0
            for idx in eligible_idx:
                pos = positions[idx]
                entry_price = float(pos.get("entry_price") or 0)
                token = str(pos.get("token") or "").strip()

                exit_price = entry_price
                if token:
                    try:
                        delta_ticker_manager.ensure_subscribed(underlying, _iso_to_ddmmyyyy(str(pos.get("expiry") or "")))
                        ticker = delta_ticker_manager.get_ticker(token)
                        mark_price = float((ticker or {}).get("mark_price") or 0)
                        if mark_price > 0:
                            exit_price = mark_price
                    except Exception:
                        pass  # keep entry_price fallback — never let a ticker miss block the close

                qty = float(pos.get("quantity") or 0)
                is_sell = str(pos.get("type") or "").strip().upper() == "SELL"
                diff = (entry_price - exit_price) if is_sell else (exit_price - entry_price)
                # contract_value here is the one real deviation from the NSE
                # formula (always 1 there, so invisible) — entry_price/
                # exit_price are stored raw (per-1-underlying-unit) points,
                # same convention CryptoTradeNew.tsx's legToPositionPayload
                # saves them in, so the real $ P&L needs this scale applied.
                pnl = diff * qty * contract_value

                pos["exited"] = True
                pos["exit_price"] = round(exit_price, 2)
                pos["exit_time"] = now_str
                pos["pnl"] = round(pnl, 2)
                pos["exit_reason"] = "expiry settled"
                exited_count += 1

            if exited_count == 0:
                continue

            all_exited = bool(positions) and all(
                not isinstance(p, dict) or p.get("exited") for p in positions
            )
            update: dict[str, Any] = {"positions": positions}
            if all_exited:
                update["status"] = 2
                update["all_exited"] = True

            await asyncio.to_thread(
                raw_db[CRYPTO_STRATEGY_COLLECTION].update_one,
                {"_id": ObjectId(strategy_id)},
                {"$set": update},
            )
            log.info(
                "[CRYPTO EXPIRY SQUAREOFF] %s — %d position(s) exited, all_exited=%s",
                strategy_name, exited_count, all_exited,
            )
            try:
                notify_user_for(
                    doc.get("user_id"),
                    "CRYPTO_EXPIRY_SQUAREOFF",
                    f"{strategy_name}: {exited_count} position(s) auto-exited — expiry settled",
                    {"trade_id": strategy_id, "leg_id": ""},
                )
            except Exception:
                log.exception("[CRYPTO EXPIRY SQUAREOFF] notify failed strategy=%s", strategy_id)
        except Exception as exc:
            log.warning("[CRYPTO EXPIRY SQUAREOFF] strategy=%s error: %s", strategy_id, exc)


async def run_crypto_expiry_squareoff_loop() -> None:
    """
    Runs forever. No polling — computes the exact gap to the next
    DAILY_TRIGGER_UTC (11:59) and sleeps for it in one shot, fires
    _squareoff_expired_crypto_legs() the instant it wakes, then repeats for
    the following day. See module docstring for the restart-timing
    trade-off this makes versus a polling dedup loop, and for why the DB
    query's own `expiry <= today` (not `== today`) is what actually covers
    it.
    """
    while True:
        try:
            await asyncio.sleep(_seconds_until_next_trigger(datetime.now(timezone.utc)))
            log.info("[CRYPTO EXPIRY SQUAREOFF] daily trigger firing")
            await _squareoff_expired_crypto_legs()
        except Exception:
            log.exception("[CRYPTO EXPIRY SQUAREOFF] loop tick failed")
            # Don't spin — if _seconds_until_next_trigger or the square-off
            # itself threw, back off a bit before recomputing the next gap
            # so a persistent failure can't turn this into a busy loop.
            await asyncio.sleep(60)
