"""Motor matematico PAPER de Proyecto 2, version 6.

La estrategia compra el primer lado perdedor cuyo ask ejecutable llegue a
40 centavos o menos. Arriesga como maximo $1, incluyendo la tarifa de entrada,
no usa stop loss y solo vende cuando el P&L neto llega a +$0.05.

Este modulo no contiene endpoints para crear, cancelar o modificar ordenes.
"""

import math


FEE_RATE = 0.07
ENTRY_TRIGGER = 0.40
MAX_TOTAL_COST = 1.00
NET_PROFIT_TARGET = 0.05
CONTRACT_STEP = 0.01
PRICE_STEP = 0.01


def _number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def taker_fee(contracts, price):
    """Tarifa taker estimada, redondeada hacia arriba al centavo."""
    quantity = _number(contracts)
    contract_price = _number(price)
    if (
        quantity is None
        or contract_price is None
        or quantity <= 0
        or not 0 < contract_price < 1
    ):
        return 0.0
    raw_fee = FEE_RATE * quantity * contract_price * (1 - contract_price)
    return math.ceil((raw_fee - 1e-12) * 100) / 100


def entry_cost(contracts, price):
    quantity = _number(contracts)
    contract_price = _number(price)
    if quantity is None or contract_price is None:
        return None
    return round(quantity * contract_price + taker_fee(quantity, contract_price), 4)


def net_pnl(contracts, entry_price, exit_price):
    """P&L neto despues de la tarifa de entrada y la tarifa de salida."""
    quantity = _number(contracts)
    buy_price = _number(entry_price)
    sell_price = _number(exit_price)
    if (
        quantity is None
        or buy_price is None
        or sell_price is None
        or quantity <= 0
        or not 0 < buy_price < 1
        or not 0 < sell_price < 1
    ):
        return None
    cost = entry_cost(quantity, buy_price)
    proceeds = quantity * sell_price - taker_fee(quantity, sell_price)
    return round(proceeds - cost, 4)


def affordable_contracts(entry_price, max_total=MAX_TOTAL_COST):
    """Mayor cantidad en pasos de 0.01 cuyo costo total no supera el limite."""
    price = _number(entry_price)
    limit = _number(max_total)
    if price is None or limit is None or not 0 < price < 1 or limit <= 0:
        return None

    max_units = math.floor((limit / price) / CONTRACT_STEP + 1e-9)
    for units in range(max_units, 0, -1):
        quantity = round(units * CONTRACT_STEP, 2)
        cost = entry_cost(quantity, price)
        if cost is not None and cost <= limit + 1e-9:
            return quantity
    return None


def first_target_price(
    contracts,
    entry_price,
    target_net=NET_PROFIT_TARGET,
):
    """Primer precio de salida en centavos que alcanza la meta neta."""
    quantity = _number(contracts)
    buy_price = _number(entry_price)
    target = _number(target_net)
    if quantity is None or buy_price is None or target is None:
        return None

    first_cent = max(1, math.ceil((buy_price - 1e-12) / PRICE_STEP))
    for cents in range(first_cent, 100):
        exit_price = round(cents * PRICE_STEP, 2)
        pnl = net_pnl(quantity, buy_price, exit_price)
        if pnl is not None and pnl + 1e-9 >= target:
            return exit_price
    return None


def build_loser_plan(
    side,
    entry_price,
    trigger=ENTRY_TRIGGER,
    max_total=MAX_TOTAL_COST,
    target_net=NET_PROFIT_TARGET,
):
    """Crea un plan PAPER si el ask del lado ya llego a 40 centavos."""
    normalized_side = str(side or "").lower()
    price = _number(entry_price)
    trigger_price = _number(trigger)
    if normalized_side not in {"yes", "no"}:
        return {"action": "WAIT", "reason": "Lado invalido"}
    if price is None or not 0 < price < 1:
        return {"action": "WAIT", "reason": "Ask ejecutable no disponible"}
    if trigger_price is None or price > trigger_price + 1e-9:
        return {
            "action": "WAIT",
            "side": normalized_side,
            "entry_price": round(price, 4),
            "reason": "Todavia no llega a 40 centavos",
        }

    quantity = affordable_contracts(price, max_total=max_total)
    if quantity is None:
        return {"action": "WAIT", "reason": "No cabe dentro del limite de $1"}

    cost = entry_cost(quantity, price)
    target_price = first_target_price(quantity, price, target_net=target_net)
    if target_price is None:
        return {"action": "WAIT", "reason": "No existe salida valida para la meta"}

    return {
        "action": "PAPER_BUY_" + normalized_side.upper(),
        "side": normalized_side,
        "contracts": quantity,
        "entry_price": round(price, 4),
        "entry_fee": taker_fee(quantity, price),
        "cost": cost,
        "target_net": round(float(target_net), 4),
        "estimated_target_price": target_price,
        "stop_loss": None,
        "hold_if_no_target": True,
        "reason": "Primer lado perdedor en 40 centavos o menos",
    }
