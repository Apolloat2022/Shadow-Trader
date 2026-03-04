"""
Shadow Trader — API Route: /signals
====================================
Handles:
  GET /signals/latest          → all tickers' latest signals
  GET /signals/{ticker}        → single ticker latest signal
  GET /signals/{ticker}/history → placeholder for future time-series endpoint
"""

from __future__ import annotations

import logging
from typing import Any

from shared.gold_reader import fetch_latest_signals, fetch_signal_for_ticker
from shared.response import not_found, ok, server_error

logger = logging.getLogger(__name__)

# Fields surfaced in the "summary" view (lighter payload)
SUMMARY_FIELDS = [
    "ticker", "timestamp", "close",
    "rsi_14", "macd_line", "macd_signal",
    "signal_golden_cross", "signal_macd", "signal_rsi", "signal_bb",
    "signal_composite",
]


def handle_signals_latest(query: dict[str, str]) -> dict:
    """
    GET /signals/latest
    Optional query params:
      ?composite=BUY|SELL|HOLD   — filter by composite signal
      ?full=true                 — return all indicator columns
    """
    try:
        signals = fetch_latest_signals()
        if not signals:
            return ok([], message="no signals available yet")

        full = query.get("full", "false").lower() == "true"
        composite_filter = query.get("composite", "").upper()

        if composite_filter in ("BUY", "SELL", "HOLD"):
            signals = [s for s in signals if s.get("signal_composite") == composite_filter]

        if not full:
            signals = [_project(s, SUMMARY_FIELDS) for s in signals]

        return ok(signals, message=f"{len(signals)} ticker(s) returned")

    except Exception as exc:
        logger.exception("Error in /signals/latest: %s", exc)
        return server_error()


def handle_ticker_signal(ticker: str, query: dict[str, str]) -> dict:
    """
    GET /signals/{ticker}
    Returns the latest full signal row for a specific ticker.
    """
    try:
        ticker = ticker.upper()
        row = fetch_signal_for_ticker(ticker)
        if row is None:
            return not_found(f"ticker '{ticker}'")
        return ok(row)

    except Exception as exc:
        logger.exception("Error in /signals/%s: %s", ticker, exc)
        return server_error()


def _project(row: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    return {k: row[k] for k in fields if k in row}
