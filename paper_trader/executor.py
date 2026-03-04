"""
Shadow Trader — Paper Trader: Trade Executor
=============================================
Applies BUY / SELL / HOLD signals against the virtual portfolio.

Strategy rules (long-only, single position per ticker):
  BUY   → If no position: buy with POSITION_SIZE_PCT % of available cash.
  SELL  → If holding: sell entire position, realise P&L.
  HOLD  → No action.

Position sizing: Fixed fractional (% of current cash balance).
"""

from __future__ import annotations

import logging
from typing import Any

from portfolio import POSITION_SIZE_PCT, get_portfolio, record_trade, save_portfolio

logger = logging.getLogger(__name__)


def execute_signals(signals: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Process a list of signal dicts (one per ticker) against the portfolio.

    Args:
        signals: List of Gold-layer signal rows, each containing at minimum:
                 ticker, close (current price), signal_composite.

    Returns:
        Summary dict with trades executed and updated portfolio snapshot.
    """
    portfolio = get_portfolio()
    trades_executed = []
    skipped = []

    for signal in signals:
        ticker    = signal.get("ticker", "").upper()
        composite = signal.get("signal_composite", "HOLD").upper()
        price     = float(signal.get("close", 0))

        if not ticker or price <= 0:
            logger.warning("Skipping invalid signal: %s", signal)
            skipped.append({"ticker": ticker, "reason": "invalid_price_or_ticker"})
            continue

        logger.info("Processing signal: %s → %s @ $%.4f", ticker, composite, price)

        if composite == "BUY":
            result = _execute_buy(portfolio, ticker, price)
        elif composite == "SELL":
            result = _execute_sell(portfolio, ticker, price)
        else:
            result = {"action": "HOLD", "ticker": ticker, "reason": "signal_is_hold"}

        if result.get("action") in ("BUY", "SELL"):
            trades_executed.append(result)
        else:
            skipped.append(result)

    # Persist updated state once, after all signals are processed
    save_portfolio(portfolio)

    # Snapshot for response
    total_value = _calc_portfolio_value(portfolio, signals)

    return {
        "trades_executed": len(trades_executed),
        "trades": trades_executed,
        "skipped": skipped,
        "portfolio_snapshot": {
            "cash":          round(portfolio["cash"], 2),
            "total_value":   round(total_value, 2),
            "positions":     portfolio["positions"],
            "total_trades":  portfolio.get("total_trades", 0),
        },
    }


# ── Internal trade logic ──────────────────────────────────────────────────────

def _execute_buy(portfolio: dict, ticker: str, price: float) -> dict:
    """Buy POSITION_SIZE_PCT of available cash worth of ticker (if not already held)."""
    positions = portfolio.setdefault("positions", {})

    if ticker in positions:
        return {"action": "SKIP", "ticker": ticker, "reason": "already_holding"}

    cash = float(portfolio["cash"])
    spend = cash * float(POSITION_SIZE_PCT)

    if spend < 1.0:
        return {"action": "SKIP", "ticker": ticker, "reason": "insufficient_cash"}

    quantity = spend / price

    # Update portfolio state
    positions[ticker] = {"qty": round(quantity, 8), "avg_cost": round(price, 8)}
    portfolio["cash"] = round(cash - spend, 2)
    portfolio["total_trades"] = portfolio.get("total_trades", 0) + 1

    trade_id = record_trade(
        ticker=ticker, action="BUY",
        quantity=quantity, price=price,
        signal="BUY", pnl=0.0,
    )

    logger.info("BUY %s: qty=%.4f, cost=$%.2f, remaining_cash=$%.2f", ticker, quantity, spend, portfolio["cash"])
    return {
        "action":    "BUY",
        "ticker":    ticker,
        "quantity":  round(quantity, 4),
        "price":     price,
        "notional":  round(spend, 2),
        "trade_id":  trade_id,
    }


def _execute_sell(portfolio: dict, ticker: str, price: float) -> dict:
    """Sell entire holding of ticker and realise P&L."""
    positions = portfolio.get("positions", {})

    if ticker not in positions:
        return {"action": "SKIP", "ticker": ticker, "reason": "no_position_to_sell"}

    position  = positions.pop(ticker)
    qty       = float(position["qty"])
    avg_cost  = float(position["avg_cost"])
    proceeds  = qty * price
    cost_basis = qty * avg_cost
    pnl       = proceeds - cost_basis

    portfolio["cash"] = float(portfolio["cash"]) + proceeds
    portfolio["total_trades"] = portfolio.get("total_trades", 0) + 1

    trade_id = record_trade(
        ticker=ticker, action="SELL",
        quantity=qty, price=price,
        signal="SELL", pnl=round(pnl, 2),
    )

    logger.info(
        "SELL %s: qty=%.4f, proceeds=$%.2f, pnl=$%.2f",
        ticker, qty, proceeds, pnl,
    )
    return {
        "action":    "SELL",
        "ticker":    ticker,
        "quantity":  round(qty, 4),
        "price":     price,
        "proceeds":  round(proceeds, 2),
        "pnl":       round(pnl, 2),
        "pnl_pct":   round((pnl / cost_basis) * 100, 2) if cost_basis else 0,
        "trade_id":  trade_id,
    }


def _calc_portfolio_value(portfolio: dict, signals: list[dict]) -> float:
    """Mark-to-market: cash + current market value of all positions."""
    price_map = {s["ticker"].upper(): float(s.get("close", 0)) for s in signals}
    positions_value = sum(
        float(pos["qty"]) * price_map.get(ticker, float(pos["avg_cost"]))
        for ticker, pos in portfolio.get("positions", {}).items()
    )
    return float(portfolio["cash"]) + positions_value
