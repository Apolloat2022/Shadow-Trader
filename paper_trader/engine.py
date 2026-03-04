"""
Shadow Trader — Paper Trading Engine: Lambda Handler
=====================================================
Lambda entry point. Reads the latest Gold signals from S3 signals_cache
and executes paper trades via the executor module.

Triggered by: EventBridge schedule (after Gold notebook completes)
              or manually via Lambda console / API.
"""

from __future__ import annotations

import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from executor import execute_signals
from portfolio import get_trade_history

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Re-use the same S3 reader logic from the API layer
import io
import boto3
import pyarrow.parquet as pq

GOLD_BUCKET = os.environ["GOLD_S3_BUCKET"]
CACHE_KEY   = "signals_cache/latest.parquet"


def _fetch_signals() -> list[dict]:
    """Read signals_cache/latest.parquet from the Gold S3 bucket."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=GOLD_BUCKET, Key=CACHE_KEY)
    buf = io.BytesIO(obj["Body"].read())
    table = pq.read_table(buf)
    records = table.to_pydict()
    n = len(next(iter(records.values())))
    return [{k: v[i] for k, v in records.items()} for i in range(n)]


def lambda_handler(event: dict, context) -> dict:
    """
    Paper Trading Engine entry point.

    Event payload (optional — overrides defaults):
    {
        "dry_run": true,       // true = log only, don't write to DynamoDB
        "tickers": ["BTC"]     // limit execution to specific tickers
    }
    """
    dry_run       = event.get("dry_run", False)
    ticker_filter = {t.upper() for t in event.get("tickers", [])}

    logger.info("Paper Trading Engine started | dry_run=%s | filter=%s", dry_run, ticker_filter or "ALL")

    try:
        # 1. Load latest signals from Gold cache
        signals = _fetch_signals()
        logger.info("Loaded %d signals from Gold cache.", len(signals))

        # 2. Apply optional ticker filter
        if ticker_filter:
            signals = [s for s in signals if s.get("ticker", "").upper() in ticker_filter]
            logger.info("After filter: %d signals to process.", len(signals))

        # 3. Validate we have signals
        if not signals:
            logger.warning("No signals available — skipping execution.")
            return {"statusCode": 200, "body": json.dumps({"message": "no_signals"})}

        # 4. Execute trades (or dry-run log only)
        if dry_run:
            logger.info("[DRY RUN] Would process signals: %s",
                        [(s["ticker"], s.get("signal_composite")) for s in signals])
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "mode": "dry_run",
                    "signals": [(s["ticker"], s.get("signal_composite")) for s in signals],
                }),
            }

        result = execute_signals(signals)

        logger.info(
            "Execution complete: %d trades | portfolio_value=$%.2f",
            result["trades_executed"],
            result["portfolio_snapshot"]["total_value"],
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result, default=str),
        }

    except Exception as exc:
        logger.exception("Paper Trading Engine failed: %s", exc)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(exc)}),
        }
