"""
crypto_live_chain_parquet_export.py
-------------------------------------
BTC/ETH counterpart of algo.scanner's live_chain_parquet_export.py — same
idea (export a day's stock_data.option_chain rows to the shared Hive-
partitioned "_atm_index" Parquet architecture, then clear them out of
Mongo), applied to crypto_live_option_chain_collector.py's BTC/ETH rows
instead of the NSE collector's. Writes into the SAME shared/parquet_data/
optimized/ root — BTC/ETH land under their own symbol=BTC/symbol=ETH
subtrees, so this needs no coordination with the NSE exporter beyond both
reading/writing the same Mongo collection (stock_data.option_chain,
filtered by underlying).

Differences from the NSE exporter, matching crypto_export_optimized_
parquet.py's already-established crypto conventions:
  - STRIKE_BUCKET_SIZE=5000, not 1000 — BTC/ETH trade at a much higher
    price scale with wider strike gaps; 1000 would fragment into mostly-
    empty buckets (see crypto_export_optimized_parquet.py's docstring).
  - atm_strike: the nearest ACTUAL listed strike to spot per timestamp, not
    a fixed-grid round() — crypto strike gaps aren't evenly spaced (BTC
    gaps alternate 200/400 within the same chain), so rounding to a fixed
    interval can land on a strike that was never listed.

Why "export yesterday", not "export today" like the NSE version: NSE's
15:40 IST market-close hook only ever fires once the trading day is truly
over, so exporting+clearing "today" there is always safe. Crypto has no
close — there's no minute-boundary safe to call "today is over". Clearing
today's still-forming day out from under live queries would be wrong, so
the daily auto job (api.py's _auto_crypto_chain_parquet_export_scheduler)
always targets the PREVIOUS UTC calendar day, once it's unambiguously
finished. The manual "Convert Now" button still targets today (delete_
after=False, same as NSE) — read-only, safe to run anytime.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
from pymongo import MongoClient

from features.mongo_data import MONGO_URI

log = logging.getLogger(__name__)

DB_NAME = "stock_data"
SOURCE_COLLECTION = "option_chain"
OUT_ROOT = Path(__file__).resolve().parents[2] / "shared" / "parquet_data" / "optimized"

STRIKE_BUCKET_SIZE = 5000
DELETE_BATCH_SIZE = 20_000

PROJECTION = {
    "_id": 1, "timestamp": 1, "underlying": 1, "expiry": 1, "strike": 1,
    "type": 1, "security_id": 1, "close": 1, "oi": 1, "spot_price": 1,
    "bid": 1, "ask": 1, "iv": 1, "delta": 1, "gamma": 1, "theta": 1, "vega": 1,
}


def _fetch_day(coll, underlying: str, day: str) -> tuple[pl.DataFrame, list]:
    docs = list(coll.find({"underlying": underlying, "date": day}, PROJECTION))
    if not docs:
        return pl.DataFrame(), []
    ids = [d.pop("_id") for d in docs]
    return pl.DataFrame(docs), ids


def _transform(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        # Strip a trailing UTC 'Z' marker if present — the writer
        # (crypto_live_option_chain_collector.py) now stamps it, but rows
        # written before that fix (or not yet backfilled by the migration
        # script) won't have it; strict %S-only parsing must accept both.
        pl.col("timestamp").str.strip_suffix("Z").str.to_datetime("%Y-%m-%dT%H:%M:%S"),
        pl.col("expiry").str.to_date(),
        pl.col("type").alias("option_type"),
        pl.col("strike").round(0).cast(pl.Int32),
        pl.col("close").cast(pl.Float32),
        pl.col("oi").fill_null(0).cast(pl.Int64),
        pl.col("iv").cast(pl.Float32),
        pl.col("delta").cast(pl.Float32),
        pl.col("gamma").cast(pl.Float32),
        pl.col("theta").cast(pl.Float32),
        pl.col("vega").cast(pl.Float32),
        pl.lit(0.0).cast(pl.Float32).alias("rho"),  # not computed here — kept for schema parity
        pl.col("spot_price").cast(pl.Float32),
        pl.col("bid").cast(pl.Float32),
        pl.col("ask").cast(pl.Float32),
        pl.col("security_id").cast(pl.Utf8),
    ).drop("type")

    df = df.with_columns(
        pl.col("timestamp").dt.date().alias("trade_date"),
        (pl.col("strike") // STRIKE_BUCKET_SIZE * STRIKE_BUCKET_SIZE).cast(pl.Int32).alias("strike_bucket"),
    )

    # Nearest ACTUAL listed strike to spot, per timestamp — see module docstring.
    atm = (
        df.select(["timestamp", "strike", "spot_price"])
        .with_columns((pl.col("strike") - pl.col("spot_price")).abs().alias("_dist"))
        .sort("_dist")
        .group_by("timestamp")
        .agg(pl.col("strike").first().alias("atm_strike"))
    )
    df = df.join(atm, on="timestamp", how="left")

    return df.with_columns(pl.col("option_type").cast(pl.Categorical)).sort(
        ["expiry", "strike_bucket", "timestamp", "strike", "option_type"]
    )


def _write_partitions(df: pl.DataFrame, underlying: str, year: int, month: int, day: str) -> dict:
    base = OUT_ROOT / f"symbol={underlying}" / f"year={year:04d}" / f"month={month:02d}"
    written_files = 0
    written_rows = 0
    written_bytes = 0
    t0 = time.time()

    for (expiry, bucket), part in df.group_by(["expiry", "strike_bucket"], maintain_order=True):
        target_dir = base / f"expiry={expiry.isoformat()}" / f"strike_bucket={bucket}"
        target_dir.mkdir(parents=True, exist_ok=True)
        # Day-named file, not part-000 — this runs once a day per underlying, part-000
        # would let tomorrow's export silently overwrite today's (see module docstring).
        target_file = target_dir / f"part-{day}.parquet"
        part_to_write = part.drop([c for c in ("underlying", "expiry", "strike_bucket") if c in part.columns])
        part_to_write.write_parquet(target_file, compression="zstd", statistics=True, row_group_size=100_000)
        written_files += 1
        written_rows += part.height
        written_bytes += target_file.stat().st_size

    return {"files": written_files, "rows": written_rows, "bytes": written_bytes, "write_seconds": round(time.time() - t0, 2)}


def _write_atm_index(df: pl.DataFrame, underlying: str, year: int, month: int, day: str) -> Path:
    index_df = (
        df.select(["timestamp", "trade_date", "spot_price", "atm_strike"])
        .unique(subset=["timestamp"])
        .sort("timestamp")
    )
    target_dir = OUT_ROOT / "_atm_index" / f"symbol={underlying}" / f"year={year:04d}" / f"month={month:02d}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"atm_index_{day}.parquet"
    index_df.write_parquet(target_file, compression="zstd", statistics=True)
    return target_file


def _delete_day(coll, ids: list) -> int:
    deleted = 0
    for i in range(0, len(ids), DELETE_BATCH_SIZE):
        chunk = ids[i:i + DELETE_BATCH_SIZE]
        result = coll.delete_many({"_id": {"$in": chunk}})
        deleted += result.deleted_count
    return deleted


def export_today(underlying: str, delete_after: bool = False, day: str | None = None) -> dict:
    """Export one underlying's rows for `day` (default: today, explicit UTC
    — not this box's system clock, see features.nse_market_hours) to
    Parquet. delete_after=True also batch-
    deletes the exported Mongo rows once the Parquet write has actually
    succeeded — only ever pass True from export_and_clear_yesterday (the
    daily auto path); the Admin 'Convert Now' button always calls this with
    delete_after=False, same convention as the NSE exporter."""
    underlying = underlying.strip().upper()
    day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    year, month = int(day[:4]), int(day[5:7])

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        coll = client[DB_NAME][SOURCE_COLLECTION]
        t0 = time.time()
        raw, ids = _fetch_day(coll, underlying, day)
        if raw.is_empty():
            log.info("[CRYPTO LIVE CHAIN EXPORT] %s %s: no rows found, nothing to export", underlying, day)
            return {"underlying": underlying, "day": day, "rows": 0, "files": 0, "deleted": 0, "ok": True}

        df = _transform(raw)
        stats = _write_partitions(df, underlying, year, month, day)
        index_path = _write_atm_index(df, underlying, year, month, day)
        log.info(
            "[CRYPTO LIVE CHAIN EXPORT] %s %s: %d rows -> %d partition files, %.1f MB (%.2fs) + atm index %s",
            underlying, day, stats["rows"], stats["files"], stats["bytes"] / 1_048_576,
            time.time() - t0, index_path,
        )

        deleted = 0
        if delete_after:
            deleted = _delete_day(coll, ids)
            log.info("[CRYPTO LIVE CHAIN EXPORT] %s %s: deleted %d rows from Mongo after export", underlying, day, deleted)

        return {"underlying": underlying, "day": day, "rows": stats["rows"], "files": stats["files"], "deleted": deleted, "ok": True}
    except Exception as exc:
        log.exception("[CRYPTO LIVE CHAIN EXPORT] %s %s: export failed", underlying, day)
        return {"underlying": underlying, "day": day, "rows": 0, "files": 0, "deleted": 0, "ok": False, "error": str(exc)}
    finally:
        client.close()


def export_and_clear_yesterday() -> dict:
    """Called once a day (api.py's _auto_crypto_chain_parquet_export_scheduler)
    to archive+clear the previous UTC calendar day's BTC/ETH rows — never
    today's, see module docstring for why. Exports+clears each underlying
    independently — one underlying's export failure never blocks or
    deletes another underlying's data."""
    from simulator.crypto_live_option_chain_collector import crypto_collector

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    underlyings = crypto_collector.status().get("underlyings") or []
    results = [export_today(u, delete_after=True, day=yesterday) for u in underlyings]
    ok = sum(1 for r in results if r["ok"])
    log.info("[CRYPTO LIVE CHAIN EXPORT] daily export+clear (%s) done: %d/%d underlyings ok", yesterday, ok, len(results))
    return {"day": yesterday, "results": results, "ok_count": ok, "total": len(results)}
