"""Analizador de flujo para Proyecto 2.

Este archivo funciona solamente en simulacion.
Consulta datos publicos usando solicitudes GET.
No contiene funciones para enviar o cancelar ordenes.
"""

from __future__ import annotations

import math
import os
import time
from datetime import datetime

import requests


BASE_URL = (
    "https://external-api.kalshi.com"
    "/trade-api/v2"
)

FLOW_WINDOWS = (
    60,
    90,
    120,
)

# Estas reglas se revisaran despues
# de completar 100 operaciones simuladas.
MIN_TRADES = int(
    os.getenv(
        "PAPER_MIN_TRADES",
        "8",
    )
)

MIN_CONTRACTS = float(
    os.getenv(
        "PAPER_MIN_CONTRACTS",
        "20",
    )
)

MIN_DOMINANCE = float(
    os.getenv(
        "PAPER_MIN_DOMINANCE",
        "0.65",
    )
)

MAX_TRADE_COST = 2.00
TARGET_PROFIT = 0.20
STOP_LOSS = 0.10

# Tarifa general para una orden
# que toma liquidez inmediatamente.
TAKER_FEE_RATE = 0.07


def _number(
    value,
    default=0.0,
):
    try:
        return float(value)

    except (
        TypeError,
        ValueError,
    ):
        return default


def taker_fee(
    contracts,
    price,
):
    """Calcula la tarifa y redondea hacia arriba."""

    raw_fee = (
        TAKER_FEE_RATE
        * contracts
        * price
        * (1 - price)
    )

    return (
        math.ceil(
            (
                raw_fee
                - 1e-12
            )
            * 100
        )
        / 100
    )


def net_pnl(
    contracts,
    entry_price,
    exit_price,
    entry_fee,
):
    """Calcula ganancia o perdida despues de tarifas."""

    exit_fee = taker_fee(
        contracts,
        exit_price,
    )

    pnl = (
        contracts
        * (
            exit_price
            - entry_price
        )
        - entry_fee
        - exit_fee
    )

    return (
        round(
            pnl,
            4,
        ),
        exit_fee,
    )


def _unix_time(
    value,
):
    if not value:
        return 0.0

    try:
        return datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        ).timestamp()

    except (
        TypeError,
        ValueError,
    ):
        return 0.0


def get_recent_trades(
    ticker,
    lookback_seconds=max(
        FLOW_WINDOWS
    ),
):
    """Obtiene operaciones publicas y excluye block trades."""

    response = requests.get(
        BASE_URL
        + "/markets/trades",
        params={
            "ticker": ticker,
            "min_ts": int(
                time.time()
                - lookback_seconds
            ),
            "limit": 1000,
            "is_block_trade": (
                "false"
            ),
        },
        timeout=10,
    )

    response.raise_for_status()

    return response.json().get(
        "trades",
        [],
    )


def _trade_values(
    trade,
):
    side = (
        trade.get(
            "taker_side"
        )
        or trade.get(
            "taker_outcome_side"
        )
        or ""
    ).lower()

    if side not in {
        "yes",
        "no",
    }:
        return None

    contracts = _number(
        trade.get(
            "count_fp"
        )
    )

    if side == "yes":
        price_key = (
            "yes_price_dollars"
        )
    else:
        price_key = (
            "no_price_dollars"
        )

    price = _number(
        trade.get(
            price_key
        )
    )

    if (
        contracts <= 0
        or not 0 < price < 1
    ):
        return None

    return {
        "side": side,
        "contracts": contracts,
        "dollars": (
            contracts
            * price
        ),
        "timestamp": (
            _unix_time(
                trade.get(
                    "created_time"
                )
            )
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
        raw_trades = (
            get_recent_trades(
                ticker
            )
        )
    else:
        raw_trades = trades

    parsed = []

    for trade in raw_trades:
        item = _trade_values(
            trade
        )

        if item:
            parsed.append(
                item
            )

    windows = {}

    votes = {
        "yes": 0,
        "no": 0,
    }

    for seconds in FLOW_WINDOWS:
        recent = [
            item
            for item in parsed
            if (
                0
                <= now
                - item["timestamp"]
                <= seconds
            )
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

        if (
            yes_contracts
            > no_contracts
        ):
            side = "yes"

        elif (
            no_contracts
            > yes_contracts
        ):
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
            and len(recent)
            >= MIN_TRADES
            and total_contracts
            >= MIN_CONTRACTS
            and dominance
            >= MIN_DOMINANCE
        )

        if confirmed:
            votes[side] += 1

        windows[str(seconds)] = {
            "trades": len(
                recent
            ),
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
            "confirmed": (
                confirmed
            ),
        }

    winner = max(
        votes,
        key=votes.get,
    )

    confirmed_windows = (
        votes[winner]
    )

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
    """Crea un plan ficticio; nunca envia una orden."""

    if (
        signal.get("action")
        == "WAIT"
    ):
        return {
            "action": "WAIT",
            "reason": (
                signal.get(
                    "reason"
                )
            ),
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
            "reason": (
                "Precio bid/ask invalido"
            ),
        }

    side = signal["side"]

    if side == "yes":
        entry_price = yes_ask
        current_exit_price = yes_bid

    else:
        entry_price = (
            1 - yes_bid
        )

        current_exit_price = (
            1 - yes_ask
        )

    if current_exit_price <= 0.01:
        return {
            "action": "WAIT",
            "reason": (
                "No hay espacio "
                "para colocar el stop"
            ),
        }

    max_contracts = int(
        MAX_TRADE_COST
        / entry_price
    )

    contracts = 0
    entry_fee = 0.0

    # Busca la mayor cantidad que:
    # 1. No supere $2 incluyendo tarifa.
    # 2. Permita respetar el stop.
    # 3. Incluya el spread actual.
    for candidate in range(
        max_contracts,
        0,
        -1,
    ):
        candidate_fee = (
            taker_fee(
                candidate,
                entry_price,
            )
        )

        total_entry = (
            candidate
            * entry_price
            + candidate_fee
        )

        adverse_test_price = max(
            0.01,
            current_exit_price
            - 0.01,
        )

        adverse_pnl, unused_fee = (
            net_pnl(
                candidate,
                entry_price,
                adverse_test_price,
                candidate_fee,
            )
        )

        if (
            total_entry
            <= MAX_TRADE_COST
            and adverse_pnl
            >= -STOP_LOSS
        ):
            contracts = candidate
            entry_fee = candidate_fee
            break

    if contracts < 1:
        return {
            "action": "WAIT",
            "reason": (
                "Tarifa y spread no "
                "permiten respetar "
                "el stop de $0.10"
            ),
        }

    position_cost = round(
        contracts
        * entry_price,
        2,
    )

    total_entry_cost = round(
        position_cost
        + entry_fee,
        2,
    )

    # Busca un objetivo que deje
    # al menos $0.20 netos.
    target_price = None
    target_pnl = None
    target_exit_fee = None

    first_target_cent = (
        int(
            math.floor(
                entry_price
                * 100
            )
        )
        + 1
    )

    for cents in range(
        first_target_cent,
       
