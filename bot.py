"""Analizador de referencia real y flujo para Proyecto 2, version 5.

Este archivo funciona solamente en simulacion.
Consulta datos publicos usando solicitudes GET.
No contiene funciones para enviar o cancelar ordenes.
"""

from __future__ import annotations

import math
import os
import re
import time
from datetime import datetime

import requests


BASE_URL = (
    "https://external-api.kalshi.com"
    "/trade-api/v2"
)

FLOW_WINDOWS = (
    30,
    60,
    90,
)

REFERENCE_WINDOWS = (
    30,
    60,
    90,
)

# La referencia debe mantenerse del mismo lado del objetivo y la distancia
# debe ser mayor que el movimiento normal reciente. Son filtros de simulacion,
# no una garantia de resultado.
MIN_REFERENCE_CONFIRMATIONS = 2
MIN_REFERENCE_DISTANCE_PCT = 0.0005
REFERENCE_RANGE_MULTIPLIER = 1.25
MAX_REFERENCE_AGE_SECONDS = 30

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
        "50",
    )
)

MIN_DOMINANCE = float(
    os.getenv(
        "PAPER_MIN_DOMINANCE",
        "0.75",
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


def _timestamp_seconds(value):
    """Convierte ISO, segundos, milisegundos o microsegundos a Unix."""

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0.0

        try:
            numeric = float(stripped)
        except ValueError:
            return _unix_time(stripped)
    else:
        numeric = _number(value)

    if numeric <= 0:
        return 0.0
    if numeric > 1e17:
        return numeric / 1e9
    if numeric > 1e14:
        return numeric / 1e6
    if numeric > 1e11:
        return numeric / 1e3
    return numeric


def _price_from_text(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if number > 0 else None

    text = str(value).strip()
    if not text:
        return None

    matches = re.findall(
        r"(?:\$\s*)?([0-9][0-9,]*(?:\.[0-9]+)?)",
        text,
    )
    for match in matches:
        try:
            number = float(match.replace(",", ""))
        except ValueError:
            continue
        if number > 0:
            return number
    return None


def _nested_target(value):
    if isinstance(value, dict):
        priority = (
            "target_price",
            "target",
            "strike",
            "value",
            "price",
        )
        for key in priority:
            if key in value:
                number = _nested_target(value.get(key))
                if number:
                    return number
        for nested in value.values():
            number = _nested_target(nested)
            if number:
                return number
    elif isinstance(value, (list, tuple)):
        for nested in value:
            number = _nested_target(nested)
            if number:
                return number
    else:
        return _price_from_text(value)
    return None


def extract_target_price(market):
    """Extrae el precio objetivo publicado por el mercado."""

    for key in (
        "floor_strike",
        "functional_strike",
        "custom_strike",
        "target_price",
        "strike",
    ):
        number = _nested_target(market.get(key))
        if number:
            return round(number, 8)

    # En los mercados de 15 minutos el titulo suele incluir "$... target".
    for key in (
        "title",
        "subtitle",
        "yes_sub_title",
        "rules_primary",
    ):
        text = str(market.get(key) or "")
        dollar = re.search(
            r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
            text,
        )
        if dollar:
            return round(
                float(dollar.group(1).replace(",", "")),
                8,
            )

        target = re.search(
            r"(?:target|objetivo|price)\D{0,20}"
            r"([0-9][0-9,]*(?:\.[0-9]+)?)",
            text,
            flags=re.IGNORECASE,
        )
        if target:
            return round(
                float(target.group(1).replace(",", "")),
                8,
            )
    return None


def get_event_live_data(event_ticker):
    """Obtiene el grafico oficial asociado al evento, sin crear ordenes."""

    if not event_ticker:
        raise ValueError("Falta event_ticker")

    response = requests.get(
        BASE_URL
        + "/live_data/events/"
        + str(event_ticker),
        params={"range": "15min"},
        timeout=10,
    )
    response.raise_for_status()
    return response.json().get("live_data", {})


_REFERENCE_VALUE_KEYS = (
    "value",
    "price",
    "v",
    "index_value",
    "indexValue",
    "close",
    "rate",
    "y",
)

_REFERENCE_TIME_KEYS = (
    "source_ts_ms",
    "timestamp_ms",
    "timestamp",
    "time",
    "ts",
    "t",
    "x",
)


def _plausible_reference_price(value, target):
    number = _number(value, -1)
    if number <= 0 or target <= 0:
        return None
    if target * 0.05 <= number <= target * 20:
        return number
    return None


def _point_from_mapping(item, target):
    timestamp = 0.0
    for key in _REFERENCE_TIME_KEYS:
        if key in item:
            timestamp = _timestamp_seconds(item.get(key))
            if timestamp:
                break

    value = None
    for key in _REFERENCE_VALUE_KEYS:
        if key in item:
            value = _plausible_reference_price(
                item.get(key),
                target,
            )
            if value is not None:
                break

    if timestamp and value is not None:
        return timestamp, value
    return None


def _point_from_sequence(item, target):
    if len(item) < 2 or isinstance(item[0], (dict, list, tuple)):
        return None

    timestamp = _timestamp_seconds(item[0])
    if not timestamp:
        return None

    # Para arreglos tipo [timestamp, open, high, low, close], usa el ultimo
    # numero que parezca un precio del activo.
    for value in reversed(item[1:]):
        if isinstance(value, (dict, list, tuple)):
            continue
        price = _plausible_reference_price(value, target)
        if price is not None:
            return timestamp, price
    return None


def _extract_reference_points(details, target, now):
    points = []

    def visit(value):
        if isinstance(value, dict):
            point = _point_from_mapping(value, target)
            if point:
                points.append(point)
            for nested in value.values():
                visit(nested)
        elif isinstance(value, (list, tuple)):
            point = _point_from_sequence(value, target)
            if point:
                points.append(point)
            for nested in value:
                visit(nested)

    visit(details)

    deduplicated = {}
    for timestamp, price in points:
        if now - 3600 <= timestamp <= now + 60:
            deduplicated[round(timestamp, 3)] = float(price)

    return sorted(
        (
            {"timestamp": timestamp, "price": price}
            for timestamp, price in deduplicated.items()
        ),
        key=lambda point: point["timestamp"],
    )


def _nearest_reference_point(points, target_time, tolerance=15):
    if not points:
        return None
    best = min(
        points,
        key=lambda point: abs(point["timestamp"] - target_time),
    )
    if abs(best["timestamp"] - target_time) > tolerance:
        return None
    return best


def analyze_reference(market, live_data=None, now=None):
    """Compara la referencia real con el objetivo y el ruido reciente."""

    if now is None:
        now = time.time()

    target = extract_target_price(market)
    if not target:
        return {
            "eligible": False,
            "side": None,
            "reason": "Precio objetivo no disponible",
        }

    event_ticker = market.get("event_ticker") or market.get("ticker")
    if live_data is None:
        live_data = get_event_live_data(event_ticker)

    details = live_data.get("details", live_data)
    points = _extract_reference_points(details, target, now)
    if not points:
        return {
            "eligible": False,
            "side": None,
            "target": target,
            "reason": "Referencia real sin datos utilizables",
            "source": live_data.get("type"),
        }

    latest = points[-1]
    latest_age = max(0.0, now - latest["timestamp"])
    if latest_age > MAX_REFERENCE_AGE_SECONDS:
        return {
            "eligible": False,
            "side": None,
            "current": round(latest["price"], 8),
            "target": target,
            "latest_age_seconds": round(latest_age, 2),
            "points": len(points),
            "reason": "Referencia real atrasada",
            "source": live_data.get("type"),
        }

    current = latest["price"]
    distance = current - target
    side = "yes" if distance > 0 else "no" if distance < 0 else None
    current_time = latest["timestamp"]
    recent = [
        point
        for point in points
        if current_time - max(REFERENCE_WINDOWS) <= point["timestamp"] <= current_time
    ]
    recent_prices = [point["price"] for point in recent]
    recent_range = (
        max(recent_prices) - min(recent_prices)
        if len(recent_prices) >= 2
        else 0.0
    )
    minimum_buffer = target * MIN_REFERENCE_DISTANCE_PCT
    required_buffer = max(
        minimum_buffer,
        recent_range * REFERENCE_RANGE_MULTIPLIER,
    )
    distance_ratio = (
        abs(distance) / required_buffer
        if required_buffer > 0
        else 0.0
    )

    windows = {}
    confirmed_windows = 0
    for seconds in REFERENCE_WINDOWS:
        sample = _nearest_reference_point(
            points,
            current_time - seconds,
        )
        if not sample:
            windows[str(seconds)] = {
                "price": None,
                "side": None,
                "distance": None,
                "confirmed": False,
            }
            continue

        sample_distance = sample["price"] - target
        sample_side = (
            "yes"
            if sample_distance > 0
            else "no"
            if sample_distance < 0
            else None
        )
        confirmed = (
            side is not None
            and sample_side == side
            and abs(sample_distance) >= required_buffer * 0.5
        )
        if confirmed:
            confirmed_windows += 1
        windows[str(seconds)] = {
            "price": round(sample["price"], 8),
            "side": sample_side,
            "distance": round(sample_distance, 8),
            "confirmed": confirmed,
        }

    eligible = (
        side is not None
        and abs(distance) >= required_buffer
        and confirmed_windows >= MIN_REFERENCE_CONFIRMATIONS
    )

    if abs(distance) < required_buffer:
        reason = (
            "Distancia menor que el ruido reciente "
            f"({distance_ratio:.2f}x)"
        )
    elif confirmed_windows < MIN_REFERENCE_CONFIRMATIONS:
        reason = (
            "Referencia confirma "
            f"{confirmed_windows}/3 ventanas"
        )
    else:
        reason = (
            f"Referencia {side.upper()} estable "
            f"{confirmed_windows}/3 · margen {distance_ratio:.2f}x"
        )

    return {
        "eligible": eligible,
        "side": side,
        "reason": reason,
        "event_ticker": event_ticker,
        "current": round(current, 8),
        "target": round(target, 8),
        "distance": round(distance, 8),
        "distance_pct": round(abs(distance) / target, 8),
        "recent_range": round(recent_range, 8),
        "required_buffer": round(required_buffer, 8),
        "distance_ratio": round(distance_ratio, 4),
        "confirmed_windows": confirmed_windows,
        "required_confirmations": MIN_REFERENCE_CONFIRMATIONS,
        "latest_age_seconds": round(latest_age, 2),
        "points": len(points),
        "windows": windows,
        "source": live_data.get("type"),
    }

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

    spread = round(
        yes_ask - yes_bid,
        4,
    )

    if spread > 0.01:
        return {
            "action": "WAIT",
            "reason": (
                "Spread mayor de $0.01"
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
        100,
    ):
        price = (
            cents / 100
        )

        pnl, exit_fee = net_pnl(
            contracts,
            entry_price,
            price,
            entry_fee,
        )

        if pnl >= TARGET_PROFIT:
            target_price = price
            target_pnl = pnl
            target_exit_fee = (
                exit_fee
            )
            break

    if target_price is None:
        return {
            "action": "WAIT",
            "reason": (
                "El objetivo no cabe "
                "antes de $1.00"
            ),
        }

    # Busca el stop mas bajo
    # que no exceda $0.10 de perdida.
    stop_price = (
        current_exit_price
    )

    stop_pnl, stop_exit_fee = (
        net_pnl(
            contracts,
            entry_price,
            current_exit_price,
            entry_fee,
        )
    )

    first_stop_cent = min(
        99,
        int(
            math.floor(
                current_exit_price
                * 100
            )
        ),
    )

    for cents in range(
        first_stop_cent,
        0,
        -1,
    ):
        price = (
            cents / 100
        )

        pnl, exit_fee = net_pnl(
            contracts,
            entry_price,
            price,
            entry_fee,
        )

        if pnl < -STOP_LOSS:
            break

        stop_price = price
        stop_pnl = pnl
        stop_exit_fee = exit_fee

    return {
        "action": (
            signal["action"]
        ),
        "ticker": (
            signal["ticker"]
        ),
        "side": side,
        "contracts": contracts,
        "entry_price": round(
            entry_price,
            4,
        ),
        "position_cost": (
            position_cost
        ),
        "entry_fee": (
            entry_fee
        ),
        "cost": (
            total_entry_cost
        ),
        "target_price": round(
            target_price,
            4,
        ),
        "stop_price": round(
            stop_price,
            4,
        ),
        "target_profit": round(
            target_pnl,
            4,
        ),
        "target_exit_fee": (
            target_exit_fee
        ),
        "stop_loss": round(
            abs(stop_pnl),
            4,
        ),
        "stop_exit_fee": (
            stop_exit_fee
        ),
        "fees_included": True,
        "spread": spread,
        "real_order": False,
    }
