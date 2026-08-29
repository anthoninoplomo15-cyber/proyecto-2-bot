"""Analizador de flujo para Proyecto 2 (solo simulacion).

Este modulo consulta operaciones publicas de Kalshi con solicitudes GET.
No carga credenciales y no contiene funciones para crear o cancelar ordenes.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import requests


BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
FLOW_WINDOWS = (60, 90, 120)

# Reglas iniciales para probar; se ajustaran despues de 100 simulaciones.
MIN_TRADES = int(os.getenv("PAPER_MIN_TRADES", "8"))
MIN_CONTRACTS = float(os.getenv("PAPER_MIN_CONTRACTS", "20"))
MIN_DOMINANCE = float(os.getenv("PAPER_MIN_DOMINANCE", "0.65"))

MAX_TRADE_COST = 2.00
TARGET_PROFIT = 0.20
STOP_LOSS = 0.10


def _number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _unix_time(value):
    if not value:
        return 0.0

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        ).timestamp()
    except (TypeError, ValueError):
        return 0.0


def get_recent_trades(
    ticker,
    lookback_seconds=max(FLOW_WINDOWS),
):
    """Obtiene transacciones publicas y excluye block trades."""

    response = requests.get(
        BASE_URL + "/markets/trades",
        params={
            "ticker": ticker,
            "min_ts": int(
                time.time() - lookback_seconds
            ),
            "limit": 1000,
            "is_block_trade": "false",
        },
        timeout=10,
    )

    response.raise_for_status()
    return response.json().get("trades", [])


def _trade_values(trade):
    side = (
        trade.get("taker_side")
        or trade.get("taker_outcome_side")
        or ""
    ).lower()

    if side not in {"yes", "no"}:
        return None

    contracts = _number(
        trade.get("count_fp")
    )

    if side == "yes":
        price_key = "yes_price_dollars"
    else:
        price_key = "no_price_dollars"

    price = _number(
        trade.get(price_key)
    )

    if contracts <= 0 or not 0 < price < 1:
        return None

    return {
        "side": side,
        "contracts": contracts,
        "dollars": contracts * price,
        "timestamp": _unix_time(
            trade.get("created_time")
        ),
    }


def analyze_flow(
    ticker,
    trades=None,
    now=None,
):
    """Exige confirmacion en 2 de las 3 ventanas."""

    if now is None:
        now = time.time()

    if trades is None:
        raw_trades = get_recent_trades(
            ticker
        )
    else:
        raw_trades = trades

    parsed = []

    for trade in raw_trades:
        item = _trade_values(trade)

        if item:
            parsed.append(item)

    windows = {}
    votes = {
        "yes": 0,
        "no": 0,
    }

    for seconds in FLOW_WINDOWS:
        recent = [
            item
            for item in parsed
            if 0 <= now - item["timestamp"] <= seconds
        ]

        yes_contracts = sum(
            item["contracts"]
            for item in recent
            if item["side"] == "yes"
        )

        no_contracts = sum(
            item["contracts"]
            for item in recent
            if item["side"] == "no"
        )

        total_contracts = (
            yes_contracts
            + no_contracts
        )

        if yes_contracts > no_contracts:
            side = "yes"
        elif no_contracts > yes_contracts:
            side = "no"
        else:
            side = None

        if total_contracts:
            dominance = (
                max(
                    yes_contracts,
                    no_contracts,
                )
                / total_contracts
            )
        else:
            dominance = 0.0

        confirmed = (
            side is not None
            and len(recent) >= MIN_TRADES
            and total_contracts >= MIN_CONTRACTS
            and dominance >= MIN_DOMINANCE
        )

        if confirmed:
            votes[side] += 1

        windows[str(seconds)] = {
            "trades": len(recent),
            "yes_contracts": round(
                yes_contracts,
                2,
            ),
            "no_contracts": round(
                no_contracts,
                2,
            ),
            "total_dollars": round(
                sum(
                    item["dollars"]
                    for item in recent
                ),
                2,
            ),
            "dominance": round(
                dominance,
                3,
            ),
            "side": side,
            "confirmed": confirmed,
        }

    winner = max(
        votes,
        key=votes.get,
    )

    confirmed_windows = votes[winner]

    if confirmed_windows < 2:
        return {
            "ticker": ticker,
            "action": "WAIT",
            "side": None,
            "reason": (
                "Falta confirmacion "
                "en 2 de 3 ventanas"
            ),
            "windows": windows,
        }

    return {
        "ticker": ticker,
        "action": (
            "PAPER_BUY_"
            + winner.upper()
        ),
        "side": winner,
        "reason": (
            f"Flujo {winner.upper()} "
            f"confirmado en "
            f"{confirmed_windows} ventanas"
        ),
        "windows": windows,
    }


def build_paper_plan(
    signal,
    yes_bid,
    yes_ask,
):
    """Calcula una entrada ficticia; nunca envia una orden."""

    if signal.get("action") == "WAIT":
        return {
            "action": "WAIT",
            "reason": signal.get("reason"),
        }

    yes_bid = _number(
        yes_bid,
        -1,
    )

    yes_ask = _number(
        yes_ask,
        -1,
    )

    valid_prices = (
        0 < yes_bid < 1
        and 0 < yes_ask < 1
        and yes_bid <= yes_ask
    )

    if not valid_prices:
        return {
            "action": "WAIT",
            "reason": "Precio bid/ask invalido",
        }

    side = signal["side"]

    if side == "yes":
        entry_price = yes_ask
    else:
        entry_price = 1 - yes_bid

    contracts = int(
        MAX_TRADE_COST
        / entry_price
    )

    if contracts < 1:
        return {
            "action": "WAIT",
            "reason": (
                "La entrada supera "
                "el limite de $2"
            ),
        }

    cost = round(
        contracts * entry_price,
        2,
    )

    target_price = (
        entry_price
        + TARGET_PROFIT / contracts
    )

    stop_price = max(
        0.01,
        entry_price
        - STOP_LOSS / contracts,
    )

    if target_price > 0.99:
        return {
            "action": "WAIT",
            "reason": (
                "El objetivo no cabe "
                "antes de $1.00"
            ),
        }

    return {
        "action": signal["action"],
        "ticker": signal["ticker"],
        "side": side,
        "contracts": contracts,
        "entry_price": round(
            entry_price,
            4,
        ),
        "cost": cost,
        "target_price": round(
            target_price,
            4,
        ),
        "stop_price": round(
            stop_price,
            4,
        ),
        "target_profit": TARGET_PROFIT,
        "stop_loss": STOP_LOSS,
        "fees_included": False,
        "real_order": False,
    }
