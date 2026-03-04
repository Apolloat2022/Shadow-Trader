"""
Shadow Trader — Paper Trader: Portfolio State Manager
======================================================
Manages virtual portfolio state in DynamoDB.

Tables:
  shadow-trader-portfolio   — current cash + positions (one item per session)
  shadow-trader-trades      — append-only trade log
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)

# ── DynamoDB tables (set via environment variables) ───────────────────────────
PORTFOLIO_TABLE = os.environ["DYNAMODB_PORTFOLIO_TABLE"]
TRADES_TABLE    = os.environ["DYNAMODB_TRADES_TABLE"]

INITIAL_CASH    = Decimal(str(os.environ.get("INITIAL_CASH_USD", "100000")))
POSITION_SIZE_PCT = Decimal(str(os.environ.get("POSITION_SIZE_PCT", "0.10")))  # 10%

SESSION_ID = "default"  # Single virtual portfolio — extend for multi-user later

_dynamodb = None


def _get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def get_portfolio() -> dict[str, Any]:
    """
    Fetch or initialise the virtual portfolio from DynamoDB.
    Returns a dict with: cash, positions {ticker: {qty, avg_cost}}, total_trades.
    """
    table = _get_dynamodb().Table(PORTFOLIO_TABLE)
    resp = table.get_item(Key={"session_id": SESSION_ID})
    item = resp.get("Item")

    if item is None:
        # First run — seed the portfolio
        item = {
            "session_id": SESSION_ID,
            "cash": INITIAL_CASH,
            "positions": {},
            "total_trades": 0,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        table.put_item(Item=item)
        logger.info("Portfolio initialised with $%s cash.", INITIAL_CASH)

    return _deserialise(item)


def save_portfolio(portfolio: dict[str, Any]) -> None:
    """Persist the updated portfolio state back to DynamoDB."""
    table = _get_dynamodb().Table(PORTFOLIO_TABLE)
    portfolio["updated_at"] = _now_iso()

    table.put_item(Item=_serialise(portfolio))
    logger.info(
        "Portfolio saved: cash=$%.2f, positions=%s",
        float(portfolio["cash"]),
        list(portfolio["positions"].keys()),
    )


def record_trade(
    ticker: str,
    action: str,         # "BUY" | "SELL"
    quantity: float,
    price: float,
    signal: str,
    pnl: float = 0.0,
) -> str:
    """Append a trade record to the trades log table. Returns the trade_id."""
    trade_id = str(uuid.uuid4())
    table = _get_dynamodb().Table(TRADES_TABLE)

    item = {
        "trade_id":   trade_id,
        "session_id": SESSION_ID,
        "ticker":     ticker,
        "action":     action,
        "quantity":   Decimal(str(round(quantity, 8))),
        "price":      Decimal(str(round(price, 8))),
        "notional":   Decimal(str(round(quantity * price, 2))),
        "signal":     signal,
        "pnl":        Decimal(str(round(pnl, 2))),
        "timestamp":  _now_iso(),
    }

    table.put_item(Item=item)
    logger.info("Trade recorded: %s %s x%.4f @ $%.4f (PnL: $%.2f)", action, ticker, quantity, price, pnl)
    return trade_id


def get_trade_history(limit: int = 50) -> list[dict]:
    """Retrieve the most recent trades from the trades table."""
    table = _get_dynamodb().Table(TRADES_TABLE)
    resp = table.query(
        IndexName="session-timestamp-index",
        KeyConditionExpression=Key("session_id").eq(SESSION_ID),
        ScanIndexForward=False,  # descending by timestamp
        Limit=limit,
    )
    return [_deserialise(item) for item in resp.get("Items", [])]


# ── Serialisation helpers (DynamoDB uses Decimal, not float) ─────────────────

def _serialise(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    if isinstance(obj, float):
        return Decimal(str(round(obj, 8)))
    if isinstance(obj, list):
        return [_serialise(v) for v in obj]
    return obj


def _deserialise(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _deserialise(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        f = float(obj)
        return int(f) if f == int(f) else f
    if isinstance(obj, list):
        return [_deserialise(v) for v in obj]
    return obj


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
