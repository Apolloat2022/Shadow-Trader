"""
Shadow Trader — API Route: /portfolio and /trades
===================================================
Handles:
  GET /portfolio        → current virtual portfolio state
  GET /trades           → recent trade history (latest 50)
"""

from __future__ import annotations

import logging
import os

import boto3
from boto3.dynamodb.conditions import Key

from shared.response import not_found, ok, server_error

logger = logging.getLogger(__name__)

PORTFOLIO_TABLE = os.environ.get("DYNAMODB_PORTFOLIO_TABLE", "")
TRADES_TABLE    = os.environ.get("DYNAMODB_TRADES_TABLE", "")
SESSION_ID      = "default"

_dynamodb = None


def _db():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def _deserialise(obj):
    """Convert DynamoDB Decimal types to native Python floats/ints."""
    from decimal import Decimal
    if isinstance(obj, dict):
        return {k: _deserialise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deserialise(v) for v in obj]
    if isinstance(obj, Decimal):
        f = float(obj)
        return int(f) if f == int(f) else f
    return obj


def handle_portfolio() -> dict:
    """GET /portfolio — return the current virtual portfolio."""
    if not PORTFOLIO_TABLE:
        return ok({"message": "portfolio table not configured"})
    try:
        table = _db().Table(PORTFOLIO_TABLE)
        resp = table.get_item(Key={"session_id": SESSION_ID})
        item = resp.get("Item")
        if item is None:
            return not_found("portfolio (not initialised — run the Paper Trading Engine first)")
        return ok(_deserialise(item))
    except Exception as exc:
        logger.exception("Error fetching portfolio: %s", exc)
        return server_error()


def handle_trades(query: dict) -> dict:
    """GET /trades — return recent trade history."""
    if not TRADES_TABLE:
        return ok([])
    try:
        limit = min(int(query.get("limit", "50")), 100)
        table = _db().Table(TRADES_TABLE)
        resp = table.query(
            IndexName="session-timestamp-index",
            KeyConditionExpression=Key("session_id").eq(SESSION_ID),
            ScanIndexForward=False,
            Limit=limit,
        )
        items = [_deserialise(item) for item in resp.get("Items", [])]
        return ok(items, message=f"{len(items)} trade(s) returned")
    except Exception as exc:
        logger.exception("Error fetching trades: %s", exc)
        return server_error()
