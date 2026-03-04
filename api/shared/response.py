"""
Shadow Trader — API Shared: Response Helpers
=============================================
Utilities to build standardised API Gateway HTTP response objects.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def _default_serializer(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def ok(data: Any, message: str = "success") -> dict:
    """Return a 200 OK response envelope."""
    return _build(200, {"status": "success", "message": message, "data": data})


def created(data: Any) -> dict:
    return _build(201, {"status": "created", "data": data})


def not_found(resource: str) -> dict:
    return _build(404, {"status": "error", "message": f"{resource} not found"})


def bad_request(message: str) -> dict:
    return _build(400, {"status": "error", "message": message})


def server_error(message: str = "Internal server error") -> dict:
    return _build(500, {"status": "error", "message": message})


def _build(status_code: int, body: dict) -> dict:
    body["ts"] = datetime.now(tz=timezone.utc).isoformat()
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
        },
        "body": json.dumps(body, default=_default_serializer),
    }
