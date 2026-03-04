"""
Shadow Trader — API Shared: Gold S3 Reader
==========================================
Reads the signals_cache/latest.parquet written by the Gold Databricks
notebook using PyArrow + boto3. No Spark or Delta dependency required.
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

GOLD_BUCKET = os.environ["GOLD_S3_BUCKET"]
CACHE_KEY = "signals_cache/latest.parquet"


def _get_s3_client():
    return boto3.client("s3")


def fetch_latest_signals() -> list[dict[str, Any]]:
    """
    Download and parse signals_cache/latest.parquet from the Gold S3 bucket.
    Returns a list of dicts, one per ticker.
    """
    s3 = _get_s3_client()

    try:
        obj = s3.get_object(Bucket=GOLD_BUCKET, Key=CACHE_KEY)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        records = table.to_pydict()

        # Transpose from column-dict to list-of-row-dicts
        n = len(next(iter(records.values())))
        rows = []
        for i in range(n):
            row = {k: _serialize(v[i]) for k, v in records.items()}
            rows.append(row)

        logger.info("Fetched %d signal records from cache.", len(rows))
        return rows

    except s3.exceptions.NoSuchKey:
        logger.warning("Signals cache not found at s3://%s/%s", GOLD_BUCKET, CACHE_KEY)
        return []

    except Exception as exc:
        logger.exception("Failed to read signals cache: %s", exc)
        raise


def fetch_signal_for_ticker(ticker: str) -> dict[str, Any] | None:
    """Return the latest signal row for a specific ticker, or None if not found."""
    signals = fetch_latest_signals()
    for row in signals:
        if str(row.get("ticker", "")).upper() == ticker.upper():
            return row
    return None


def _serialize(value: Any) -> Any:
    """Convert non-JSON-serializable types (timestamps, numpy scalars, etc.)."""
    if hasattr(value, "isoformat"):          # datetime / Timestamp
        return value.isoformat()
    if hasattr(value, "item"):               # numpy scalar
        return value.item()
    return value
