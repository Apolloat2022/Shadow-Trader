"""
Shadow Trader — API Lambda: Main Router
========================================
Single Lambda function that handles all API Gateway HTTP requests.
Routes requests to the appropriate handler based on path + method.

Supported routes:
  GET /health
  GET /signals/latest
  GET /signals/{ticker}
"""

from __future__ import annotations

import logging
import os
import sys

# ── Make bundled packages (Lambda Layer) importable ──────────────────────────
# When deployed, Lambda unzips the layer to /opt/python — already on path.
# During local testing, add the api/ directory itself.
sys.path.insert(0, os.path.dirname(__file__))

from routes.health import handle_health
from routes.signals import handle_signals_latest, handle_ticker_signal
from routes.portfolio import handle_portfolio, handle_trades
from shared.response import bad_request, not_found, server_error

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:
    """
    Main API Gateway Lambda handler.
    Dispatches to sub-handlers based on HTTP method + path.
    """
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET").upper()
    raw_path = event.get("rawPath", "/")
    query = event.get("queryStringParameters") or {}

    logger.info("API %s %s | query=%s", method, raw_path, query)

    # Handle CORS pre-flight
    if method == "OPTIONS":
        return {
            "statusCode": 204,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
            },
            "body": "",
        }

    if method != "GET":
        return bad_request(f"Method {method} not allowed")

    # ── Route dispatch ────────────────────────────────────────────────────────
    try:
        # GET /health
        if raw_path in ("/health", "/health/"):
            return handle_health()

        # GET /portfolio
        if raw_path in ("/portfolio", "/portfolio/"):
            return handle_portfolio()

        # GET /trades
        if raw_path in ("/trades", "/trades/"):
            return handle_trades(query)

        # GET /signals/latest
        if raw_path in ("/signals/latest", "/signals/latest/"):
            return handle_signals_latest(query)

        # GET /signals/{ticker}  — e.g. /signals/BTC
        parts = [p for p in raw_path.strip("/").split("/") if p]
        if len(parts) == 2 and parts[0] == "signals":
            return handle_ticker_signal(parts[1], query)

        return not_found(f"route '{raw_path}'")

    except Exception as exc:
        logger.exception("Unhandled error: %s", exc)
        return server_error()
