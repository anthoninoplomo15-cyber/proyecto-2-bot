"""Motor matematico PAPER de Proyecto 2, version 9.

La estrategia compra el primer lado cuyo flujo ejecutado alcanza el limite de
su criptomoneda durante los primeros cinco minutos de cada intervalo. Arriesga
como maximo $1, incluyendo la tarifa de entrada, no usa stop loss y activa un
trailing de 2 centavos cuando el valor de venta neto supera $1.10.

Este modulo no contiene endpoints para crear, cancelar o modificar ordenes.
"""

import math


FEE_RATE = 0.07
MAX_TOTAL_COST = 1.00
TRAIL_ARM_NET_PROCEEDS = 1.10
TRAIL_DROP = 0.02
CONTRACT_STEP = 0.01
PRICE_STEP = 0.01
DEFAULT_FLOW_THRESHOLD = 1_000.00
FLOW_THRESHOLDS = {
    "KXBTC15M": 10_000.00,
    "KXETH15M": 5_000.00,
    "KXSOL15M": 2_000.00,
    "KXXRP15M": 2_000.00,
}
# Se conserva este nombre para compatibilidad con cualquier importacion vieja.
FLOW_THRESHOLD = DEFAULT_FLOW_THRESHOLD


def flow_threshold_for_series(series_ticker):
    """Limite de flujo para una serie o un ticker de mercado de Kalshi."""
    normalized = str(series_ticker or "").strip().upper()
    for series, threshold in FLOW_THRESHOLDS.items():
        if normalized == series or normalized.startswith(series + "-"):
            return threshold
    return DEFAULT_FLOW_THRESHOLD


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


def net_proceeds(contracts, exit_price):
    """Valor recibido si se vende ahora, despues de la tarifa de salida."""
    quantity = _number(contracts)
    sell_price = _number(exit_price)
    if (
        quantity is None
        or sell_price is None
        or quantity <= 0
        or not 0 < sell_price < 1
    ):
        return None
    return round(quantity * sell_price - taker_fee(quantity, sell_price), 4)


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


def first_arm_price(
    contracts,
    arm_net_proceeds=TRAIL_ARM_NET_PROCEEDS,
):
    """Primer bid en centavos cuyo valor neto activa el trailing."""
    quantity = _number(contracts)
    arm_value = _number(arm_net_proceeds)
    if quantity is None or arm_value is None:
        return None

    for cents in range(1, 100):
        exit_price = round(cents * PRICE_STEP, 2)
        proceeds = net_proceeds(quantity, exit_price)
        if proceeds is not None and proceeds + 1e-9 >= arm_value:
            return exit_price
    return None


def build_entry_plan(
    side,
    entry_price,
    max_total=MAX_TOTAL_COST,
    arm_net_proceeds=TRAIL_ARM_NET_PROCEEDS,
    trail_drop=TRAIL_DROP,
    flow_threshold=FLOW_THRESHOLD,
):
    """Crea un plan PAPER para el primer lado que cruza el flujo requerido."""
    normalized_side = str(side or "").lower()
    price = _number(entry_price)
    threshold = _number(flow_threshold)
    if normalized_side not in {"yes", "no"}:
        return {"action": "WAIT", "reason": "Lado invalido"}
    if price is None or not 0 < price < 1:
        return {"action": "WAIT", "reason": "Ask ejecutable no disponible"}
    if threshold is None or threshold <= 0:
        threshold = DEFAULT_FLOW_THRESHOLD

    quantity = affordable_contracts(price, max_total=max_total)
    if quantity is None:
        return {"action": "WAIT", "reason": "No cabe dentro del limite de $1"}

    cost = entry_cost(quantity, price)
    arm_price = first_arm_price(quantity, arm_net_proceeds=arm_net_proceeds)

    return {
        "action": "PAPER_BUY_" + normalized_side.upper(),
        "side": normalized_side,
        "contracts": quantity,
        "entry_price": round(price, 4),
        "entry_fee": taker_fee(quantity, price),
        "cost": cost,
        "trail_arm_net_proceeds": round(float(arm_net_proceeds), 4),
        "trail_drop": round(float(trail_drop), 4),
        "flow_threshold": round(threshold, 2),
        "estimated_arm_price": arm_price,
        "stop_loss": None,
        "hold_if_never_armed": True,
        "reason": f"Primer lado en alcanzar ${threshold:,.0f} de flujo ejecutado",
    }
