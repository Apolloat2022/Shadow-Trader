"""
Shadow Trader — API Route: /health
====================================
Lightweight health-check endpoint for API Gateway and load-balancer monitoring.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from shared.response import ok


def handle_health() -> dict:
    """GET /health — returns service status and basic config."""
    return ok(
        data={
            "service": "shadow-trader-api",
            "version": os.environ.get("SERVICE_VERSION", "1.0.0"),
            "gold_bucket": os.environ.get("GOLD_S3_BUCKET", "unset"),
            "utc_time": datetime.now(tz=timezone.utc).isoformat(),
        },
        message="healthy",
    )
