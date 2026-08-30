import base64
import datetime as dt
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from flask import Flask, jsonify, render_template_string

from bot import (
    KALSHI_YES_THRESHOLD,
    MAX_SPREAD,
    MIN_CONFIRMING_VOTES,
    ORDERBOOK_BID_THRESHOLD,
    TAKER_BUY_THRESHOLD,
    build_entry_plan,
    omega_signal,
)


app = Flask(__name__)

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY", "").replace("\\n", "\n").strip()

# Proyecto 2 permanece 100% PAPER. Este archivo no tiene ninguna llamada para
# crear, modificar o cancelar ordenes y nunca mueve dinero real.
SETTINGS = {
    "mode": "PAPER",
    "live_trading": False,
    "test_version": 10,
    "test_bankroll": 14.00,
    "max_total_cost_per_crypto": 1.00,
    "entry_mode": "omega_impulse_three_of_four",
    "minimum_confirming_votes": MIN_CONFIRMING_VOTES,
    "taker_buy_threshold": TAKER_BUY_THRESHOLD,
    "orderbook_bid_threshold": ORDERBOOK_BID_THRESHOLD,
    "kalshi_yes_threshold": KALSHI_YES_THRESHOLD,
    "maximum_spread": MAX_SPREAD,
    "flow_measure": "executed_taker_outcome_notional",
    "trail_arm_net_proceeds": 1.05,
    "trail_drop": 0.02,
    "stop_loss": None,
    "hold_to_settlement_if_never_armed": True,
    "max_open_trades": 14,
    "continuous_operation": True,
    "intervals_per_day": 96,
    "maximum_daily_opportunities": 1344,
    "entry_window_seconds_remaining": [840, 905],
    "poll_seconds": 3,
}

# Endpoint oficial exclusivo para datos publicos de mercado. Evita depender de
# funciones de cuenta y es mas apropiado para un servicio PAPER en la nube.
BINANCE_BASE_URL = "https://data-api.binance.vision"
BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
BINANCE_CACHE_SECONDS = 10
INTERVAL_SECONDS = 15 * 60
FLOW_WINDOW_SECONDS = 5 * 60

SERIES = [
    "KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M",
    "KXDOGE15M", "KXBNB15M", "KXADA15M", "KXLINK15M",
    "KXAVAX15M", "KXLTC15M", "KXBCH15M", "KXDOT15M",
    "KXHYPE15M", "KXSUI15M",
]

BINANCE_SYMBOLS = {
    "KXBTC15M": "BTCUSDT",
    "KXETH15M": "ETHUSDT",
    "KXSOL15M": "SOLUSDT",
    "KXXRP15M": "XRPUSDT",
    "KXDOGE15M": "DOGEUSDT",
    "KXBNB15M": "BNBUSDT",
    "KXADA15M": "ADAUSDT",
    "KXLINK15M": "LINKUSDT",
    "KXAVAX15M": "AVAXUSDT",
    "KXLTC15M": "LTCUSDT",
    "KXBCH15M": "BCHUSDT",
    "KXDOT15M": "DOTUSDT",
    "KXHYPE15M": "HYPEUSDT",
    "KXSUI15M": "SUIUSDT",
}
BINANCE_FUTURES_SERIES = {"KXHYPE15M"}

CONNECTION_CACHE = {"updated": 0.0, "value": None}
BINANCE_CACHE = {}


def load_private_key():
    if not PRIVATE_KEY_PEM:
        raise ValueError("Falta KALSHI_PRIVATE_KEY")
    return serialization.load_pem_private_key(
        PRIVATE_KEY_PEM.encode("utf-8"), password=None
    )


def auth_headers(method, endpoint):
    timestamp = str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000))
    full_path = urlparse(BASE_URL + endpoint).path
    message = f"{timestamp}{method.upper()}{full_path}".encode("utf-8")
    signature = load_private_key().sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
    }


def get_balance_status():
    if not API_KEY_ID or not PRIVATE_KEY_PEM:
        return {"connected": False, "message": "Credenciales pendientes en Render"}

    now = time.monotonic()
    cached = CONNECTION_CACHE["value"]
    if cached and now - CONNECTION_CACHE["updated"] < 60:
        return cached

    try:
        endpoint = "/portfolio/balance"
        response = requests.get(
            BASE_URL + endpoint,
            headers=auth_headers("GET", endpoint),
            timeout=12,
        )
        response.raise_for_status()
        value = {"connected": True, "message": "API conectada en modo lectura"}
    except Exception as exc:
        value = {
            "connected": False,
            "message": f"No se pudo verificar la API: {type(exc).__name__}",
        }

    CONNECTION_CACHE["updated"] = now
    CONNECTION_CACHE["value"] = value
    return value


def dollars(market, dollar_key, cents_key):
    value = market.get(dollar_key)
    if value not in (None, ""):
        try:
            return round(float(value), 4)
        except (TypeError, ValueError):
            pass
    value = market.get(cents_key)
    if value not in (None, ""):
        try:
            return round(float(value) / 100, 4)
        except (TypeError, ValueError):
            pass
    return None


def opposite_price(price):
    if price is None:
        return None
    return round(1 - float(price), 4)


def empty_binance_signal(series_ticker, message="Datos Binance no disponibles"):
    return {
        "binance_available": False,
        "binance_symbol": BINANCE_SYMBOLS.get(series_ticker),
        "binance_market": (
            "futures" if series_ticker in BINANCE_FUTURES_SERIES else "spot"
        ),
        "binance_price": None,
        "momentum_1m_pct": None,
        "momentum_5m_pct": None,
        "taker_buy_share": None,
        "orderbook_bid_share": None,
        "binance_message": message,
    }


def fetch_binance_signal(series_ticker):
    """Obtiene momentum, presion taker y libro con una cache corta."""
    symbol = BINANCE_SYMBOLS.get(series_ticker)
    if not symbol:
        return empty_binance_signal(series_ticker, "Par Binance no configurado")

    futures_market = series_ticker in BINANCE_FUTURES_SERIES
    market_name = "futures" if futures_market else "spot"
    base_url = BINANCE_FUTURES_BASE_URL if futures_market else BINANCE_BASE_URL
    klines_path = "/fapi/v1/klines" if futures_market else "/api/v3/klines"
    depth_path = "/fapi/v1/depth" if futures_market else "/api/v3/depth"
    cache_key = market_name + ":" + symbol

    now = time.monotonic()
    cached = BINANCE_CACHE.get(cache_key)
    if cached and now - cached["updated"] < BINANCE_CACHE_SECONDS:
        return dict(cached["value"])

    try:
        klines_response = requests.get(
            base_url + klines_path,
            params={"symbol": symbol, "interval": "1m", "limit": 7},
            timeout=7,
        )
        klines_response.raise_for_status()
        klines = klines_response.json()
        if not isinstance(klines, list) or len(klines) < 7:
            raise ValueError("Velas Binance incompletas")

        depth_response = requests.get(
            base_url + depth_path,
            params={"symbol": symbol, "limit": 20},
            timeout=7,
        )
        depth_response.raise_for_status()
        depth = depth_response.json()

        closes = [float(candle[4]) for candle in klines]
        current_price = closes[-1]
        momentum_1m = current_price / closes[-2] - 1
        momentum_5m = current_price / closes[-6] - 1

        recent_klines = klines[-2:]
        quote_volume = sum(float(candle[7]) for candle in recent_klines)
        taker_buy_quote = sum(float(candle[10]) for candle in recent_klines)
        taker_buy_share = (
            taker_buy_quote / quote_volume if quote_volume > 0 else None
        )

        bids = depth.get("bids", [])[:10]
        asks = depth.get("asks", [])[:10]
        bid_notional = sum(float(price) * float(size) for price, size in bids)
        ask_notional = sum(float(price) * float(size) for price, size in asks)
        book_total = bid_notional + ask_notional
        orderbook_bid_share = (
            bid_notional / book_total if book_total > 0 else None
        )

        if taker_buy_share is None or orderbook_bid_share is None:
            raise ValueError("Volumen Binance incompleto")

        value = {
            "binance_available": True,
            "binance_symbol": symbol,
            "binance_market": market_name,
            "binance_price": round(current_price, 8),
            "momentum_1m_pct": round(momentum_1m * 100, 5),
            "momentum_5m_pct": round(momentum_5m * 100, 5),
            "taker_buy_share": round(taker_buy_share, 6),
            "orderbook_bid_share": round(orderbook_bid_share, 6),
            "binance_message": "Señal Binance disponible",
        }
    except (requests.exceptions.RequestException, TypeError, ValueError, ZeroDivisionError):
        value = empty_binance_signal(series_ticker)

    BINANCE_CACHE[cache_key] = {"updated": now, "value": dict(value)}
    return value


def parse_trade_time(trade):
    """Devuelve la hora Unix mas precisa disponible para ordenar ejecuciones."""
    value = trade.get("ts_ms")
    if value not in (None, ""):
        try:
            return float(value) / 1000
        except (TypeError, ValueError):
            pass

    value = trade.get("ts")
    if value not in (None, ""):
        try:
            return float(value)
        except (TypeError, ValueError):
            pass

    value = str(trade.get("created_time") or "").strip()
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def interval_times(close_time):
    value = str(close_time or "").strip()
    if not value:
        return None
    try:
        close_at = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if close_at.tzinfo is None:
        close_at = close_at.replace(tzinfo=dt.timezone.utc)
    close_ts = close_at.timestamp()
    start_ts = close_ts - INTERVAL_SECONDS
    return start_ts, start_ts + FLOW_WINDOW_SECONDS


def empty_flow(available, window_closed=False):
    starting_value = 0.0 if available and not window_closed else None
    return {
        "flow_available": available,
        "flow_window_closed": window_closed,
        "yes_flow": starting_value,
        "no_flow": starting_value,
        "flow_trade_count": 0,
        "kalshi_yes_share": None,
    }


def fetch_trade_flow(ticker, close_time):
    """Suma el dinero ejecutado por el taker de cada lado en los primeros 5 min."""
    window = interval_times(close_time)
    if not ticker or window is None:
        return empty_flow(False)

    start_ts, deadline_ts = window
    now_ts = dt.datetime.now(dt.timezone.utc).timestamp()
    if now_ts < start_ts:
        return empty_flow(True)
    if now_ts >= deadline_ts:
        # La ventana ya cerro. Nunca se abre una entrada nueva despues de 5 min.
        return empty_flow(True, window_closed=True)

    trades = []
    cursor = None
    seen_cursors = set()
    while True:
        params = {
            "ticker": ticker,
            "min_ts": max(0, int(start_ts) - 1),
            "max_ts": int(now_ts) + 1,
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor
        response = requests.get(
            BASE_URL + "/markets/trades",
            params=params,
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        trades.extend(payload.get("trades", []))
        cursor = str(payload.get("cursor") or "")
        if not cursor:
            break
        if cursor in seen_cursors:
            raise ValueError("Cursor de trades repetido")
        seen_cursors.add(cursor)

    normalized = []
    for trade in trades:
        executed_at = parse_trade_time(trade)
        if (
            executed_at is None
            or executed_at < start_ts
            or executed_at > now_ts
            or executed_at >= deadline_ts
        ):
            continue
        side = str(
            trade.get("taker_outcome_side") or trade.get("taker_side") or ""
        ).lower()
        if side not in {"yes", "no"}:
            continue
        try:
            count = float(trade.get("count_fp", trade.get("count")))
            price = float(trade.get(f"{side}_price_dollars"))
        except (TypeError, ValueError):
            continue
        if (
            not math.isfinite(count)
            or not math.isfinite(price)
            or count <= 0
            or not 0 < price < 1
        ):
            continue
        amount = count * price
        normalized.append(
            (executed_at, str(trade.get("trade_id") or ""), side, amount)
        )

    totals = {"yes": 0.0, "no": 0.0}
    for _executed_at, _trade_id, side, amount in normalized:
        totals[side] += amount
    total_flow = totals["yes"] + totals["no"]
    yes_share = totals["yes"] / total_flow if total_flow > 0 else None

    return {
        "flow_available": True,
        "flow_window_closed": False,
        "yes_flow": round(totals["yes"], 2),
        "no_flow": round(totals["no"], 2),
        "flow_trade_count": len(normalized),
        "kalshi_yes_share": (
            None if yes_share is None else round(yes_share, 6)
        ),
    }


def scan_one_series(series_ticker):
    response = requests.get(
        BASE_URL + "/markets",
        params={"series_ticker": series_ticker, "status": "open", "limit": 5},
        timeout=8,
    )
    response.raise_for_status()
    markets = [
        {
            "series": series_ticker,
            "ticker": market.get("ticker"),
            "title": market.get("title") or market.get("subtitle") or series_ticker,
            "close_time": market.get("close_time"),
            "yes_bid": dollars(market, "yes_bid_dollars", "yes_bid"),
            "yes_ask": dollars(market, "yes_ask_dollars", "yes_ask"),
            "volume": market.get("volume_fp", market.get("volume", 0)),
        }
        for market in response.json().get("markets", [])
    ]
    markets.sort(key=lambda item: item.get("close_time") or "")
    selected = markets[:1]
    for market in selected:
        try:
            market.update(
                fetch_trade_flow(
                    market.get("ticker"),
                    market.get("close_time"),
                )
            )
        except (requests.exceptions.RequestException, TypeError, ValueError):
            # Falla cerrada: sin flujo verificable no existe entrada.
            market.update(empty_flow(False))
        market.update(fetch_binance_signal(series_ticker))
    return selected


def scan_markets():
    found = []
    with ThreadPoolExecutor(max_workers=7) as pool:
        jobs = {pool.submit(scan_one_series, ticker): ticker for ticker in SERIES}
        for job in as_completed(jobs):
            try:
                found.extend(job.result())
            except requests.exceptions.RequestException:
                continue
    found.sort(key=lambda item: (item.get("close_time") or "", item.get("series") or ""))
    return found


def add_strategy_plans(markets):
    enriched = []
    for market in markets:
        item = dict(market)
        yes_bid = item.get("yes_bid")
        yes_ask = item.get("yes_ask")
        no_bid = opposite_price(yes_ask)
        no_ask = opposite_price(yes_bid)
        item["no_bid"] = no_bid
        item["no_ask"] = no_ask
        spread = None
        if yes_bid is not None and yes_ask is not None:
            spread = max(0.0, round(float(yes_ask) - float(yes_bid), 4))
        signal = omega_signal(
            item.get("momentum_1m_pct"),
            item.get("momentum_5m_pct"),
            item.get("taker_buy_share"),
            item.get("orderbook_bid_share"),
            item.get("kalshi_yes_share"),
            spread,
        )
        item.update(signal)
        item["yes_plan"] = build_entry_plan(
            "yes", yes_ask, omega_votes=signal["omega_yes_votes"]
        )
        item["no_plan"] = build_entry_plan(
            "no", no_ask, omega_votes=signal["omega_no_votes"]
        )
        omega_side = signal["omega_side"]
        item["selected_plan"] = (
            item["yes_plan"] if omega_side == "yes"
            else item["no_plan"] if omega_side == "no"
            else None
        )
        enriched.append(item)
    return enriched


@app.get("/health")
def health():
    return {
        "ok": True,
        "project": "Proyecto 2",
        "mode": "PAPER",
        "version": 10,
        "live_trading": False,
    }


@app.get("/api/status")
def api_status():
    markets = add_strategy_plans(scan_markets())
    return jsonify(
        {
            "settings": SETTINGS,
            "kalshi": get_balance_status(),
            "markets": markets,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
    )


@app.get("/api/market/<ticker>")
def market_status(ticker):
    safe_ticker = ticker.replace("-", "")
    if not safe_ticker.isalnum():
        return jsonify({"error": "Ticker invalido"}), 400

    try:
        response = requests.get(BASE_URL + "/markets/" + ticker, timeout=8)
        response.raise_for_status()
        market = response.json().get("market", {})
        yes_bid = dollars(market, "yes_bid_dollars", "yes_bid")
        yes_ask = dollars(market, "yes_ask_dollars", "yes_ask")
        return jsonify(
            {
                "ticker": market.get("ticker"),
                "status": market.get("status"),
                "result": market.get("result"),
                "close_time": market.get("close_time"),
                "yes_bid": yes_bid,
                "yes_ask": yes_ask,
                "no_bid": opposite_price(yes_ask),
                "no_ask": opposite_price(yes_bid),
            }
        )
    except requests.exceptions.RequestException:
        return jsonify({"error": "Mercado no disponible"}), 503


@app.get("/")
def home():
    return render_template_string(HTML)


HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Proyecto 2 · Versión 10 OMEGA</title>
  <style>
    :root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:#07111f;
    color:#eef4ff;font-family:system-ui,Arial}.wrap{max-width:1400px;margin:auto;padding:18px}
    h1{margin:0 0 4px}h2{margin-top:24px}.tag{display:inline-block;background:#1f6b3d;
    padding:6px 10px;border-radius:999px;font-weight:800}.grid{display:grid;
    grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px;margin:16px 0}
    .card{background:#111f33;border:1px solid #263b57;border-radius:14px;padding:14px}
    .label{color:#96abc8;font-size:12px}.value{font-size:21px;font-weight:800;
    margin-top:3px}.safe,.positive{color:#6ee7a2}.warn{color:#ffd166}.negative{color:#ff9fa8}
    .note{background:#13243a;border-left:4px solid #4da3ff;padding:12px;border-radius:9px;
    margin:14px 0;color:#c7d7ed;line-height:1.45}.risk{border-left-color:#ff9fa8}
    .controls{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:14px 0}
    .button{border:0;border-radius:10px;padding:11px 16px;font-weight:800;cursor:pointer;
    background:#2f81f7;color:white}.button.pause{background:#a66a12}.button.secondary{background:#31445f}
    .button:disabled{opacity:.5;cursor:not-allowed}.pill{display:inline-block;padding:4px 8px;
    border-radius:999px;font-size:12px;font-weight:800}.yes{background:#165b37;color:#82f0ad}
    .no{background:#67272d;color:#ff9fa8}.wait{background:#3c4655;color:#d6deea}
    table{width:100%;border-collapse:collapse;background:#111f33;border-radius:14px;
    overflow:hidden}th,td{text-align:left;padding:11px 9px;border-bottom:1px solid #263b57;
    font-size:13px}th{color:#96abc8}.foot{color:#96abc8;margin-top:14px;font-size:12px}
    .table-wrap{overflow-x:auto;border-radius:14px}.muted{color:#96abc8}
  </style>
</head>
<body><div class="wrap">
  <h1>Proyecto 2 · Versión 10 OMEGA Impulso</h1>
  <div class="tag">MODO PRUEBA · 14 CRIPTOS · 3 DE 4 SEÑALES</div>

  <div class="grid">
    <div class="card"><div class="label">Kalshi API</div>
      <div id="api" class="value warn">Verificando…</div></div>
    <div class="card"><div class="label">Operaciones cerradas</div>
      <div id="paper-count" class="value">0</div></div>
    <div class="card"><div class="label">Abiertas</div>
      <div id="paper-open" class="value">0 / 14</div></div>
    <div class="card"><div class="label">Cobertura diaria</div>
      <div class="value">14 × 96 intervalos</div></div>
    <div class="card"><div class="label">Saldo disponible</div>
      <div id="paper-cash" class="value">$14.00</div></div>
    <div class="card"><div class="label">Riesgo abierto</div>
      <div id="paper-risk" class="value">$0.00</div></div>
    <div class="card"><div class="label">Ganancia realizada</div>
      <div id="paper-pnl" class="value">$0.00</div></div>
    <div class="card"><div class="label">Operaciones positivas</div>
      <div id="paper-wins" class="value">0 / 0 · 0%</div></div>
    <div class="card"><div class="label">Entrada</div>
      <div class="value">OMEGA 3 de 4</div></div>
    <div class="card"><div class="label">Máximo por cripto</div>
      <div class="value">$1 con fee</div></div>
    <div class="card"><div class="label">Activar seguimiento</div>
      <div class="value">$1.05 netos</div></div>
    <div class="card"><div class="label">Retroceso permitido</div>
      <div class="value">2¢ desde el máximo</div></div>
    <div class="card"><div class="label">Stop loss</div>
      <div class="value negative">Ninguno</div></div>
    <div class="card"><div class="label">Ventana de entrada</div>
      <div class="value">Primer minuto</div></div>
  </div>

  <div class="note"><strong>Regla automática de esta prueba.</strong> Durante el
  primer minuto de cada contrato, OMEGA revisa cuatro señales para cada cripto:
  momentum de 1 y 5 minutos en la misma dirección; presión de compras o ventas
  taker de al menos 58%; libro de órdenes inclinado al menos 55%; y flujo de
  Kalshi inclinado al menos 60%. Compra UP o DOWN solo cuando 3 de las 4 señales
  coinciden y el spread ejecutable no supera 5¢. Si no coinciden, no entra. Usa
  hasta $1 por criptomoneda, incluyendo la tarifa.
  No vende antes de que el valor recibido al <em>bid</em>, después de la tarifa de
  salida, llegue a $1.05. Desde ese momento sigue el valor neto más alto y vende
  cuando retrocede 2¢. Si nunca llega a $1.05, conserva la posición hasta el
  resultado. Solo usa una vez cada cripto por intervalo y vigila las 14
  criptomonedas durante los 96 intervalos del día.</div>

  <div class="note risk"><strong>Riesgo importante.</strong> No tener stop loss
  permite perder casi todo el dólar. El seguimiento de 2¢ no protege la posición
  antes de llegar a $1.05 netos y una caída rápida puede simular una salida por
  debajo del nivel esperado. Una señal OMEGA 3/4 tampoco garantiza el resultado.
  Esta prueba mide por separado el impulso y el cierre; no demuestra que sea
  rentable ni coloca dinero real. Mantén esta pestaña abierta para que registre.</div>

  <div class="controls">
    <button id="paper-toggle" class="button" onclick="togglePaper()">
      Iniciar prueba automática
    </button>
    <button id="paper-download" class="button secondary" onclick="downloadPaperCsv()">
      Descargar resultados CSV
    </button>
    <span id="paper-state" class="muted">Pausada · no abre posiciones</span>
  </div>

  <h2>Mercados cripto de 15 minutos</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>Cripto</th><th>UP</th><th>DOWN</th><th>Momentum</th><th>Presión</th><th>Libro</th><th>Kalshi</th><th>Votos</th><th>Tiempo</th><th>Estado</th><th>Plan</th></tr></thead>
    <tbody id="rows"><tr><td colspan="11">Buscando mercados…</td></tr></tbody>
  </table></div>

  <h2>Operaciones simuladas abiertas</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>Cripto</th><th>Lado</th><th>Entrada</th><th>Bid actual</th><th>Valor neto</th><th>P&L neto</th><th>Seguimiento</th><th>Tiempo</th></tr></thead>
    <tbody id="open-rows"><tr><td colspan="8">Ninguna</td></tr></tbody>
  </table></div>

  <h2>Últimos resultados</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>Cripto</th><th>Lado</th><th>Entrada</th><th>Salida</th><th>Neto</th><th>Máximo neto</th><th>Motivo</th></tr></thead>
    <tbody id="closed-rows"><tr><td colspan="7">Todavía no hay resultados</td></tr></tbody>
  </table></div>

  <div id="updated" class="foot"></div>
</div>

<script>
const PAPER_KEY='proyecto2_paper_v10_omega_impulso_all';
const START_BANKROLL=14.00;
const MAX_OPEN=14;
const TRAIL_ARM_NET_PROCEEDS=1.05;
const TRAIL_DROP=0.02;
const ENTRY_MIN_SECONDS=840;
const ENTRY_MAX_SECONDS=905;
let refreshing=false;

function roundNumber(value,digits=4){
  const power=10**digits;
  return Math.round((Number(value)+Number.EPSILON)*power)/power;
}

function money(value){
  const number=Number(value)||0;
  if(number>0){return '+$'+number.toFixed(2);}
  if(number<0){return '-$'+Math.abs(number).toFixed(2);}
  return '$0.00';
}

function dollarsText(value){
  if(value==null||value===''){return '—';}
  const number=Number(value);
  return Number.isFinite(number)?'$'+number.toFixed(2):'—';
}

function flowText(value){
  if(value==null||value===''){return '—';}
  const number=Number(value);
  if(!Number.isFinite(number)){return '—';}
  return '$'+number.toLocaleString('en-US',{minimumFractionDigits:0,maximumFractionDigits:2});
}

function priceText(value){
  if(value==null||value===''){return '—';}
  const number=Number(value);
  return Number.isFinite(number)?Math.round(number*100)+'¢':'—';
}

function percentText(value){
  if(value==null||value===''){return '—';}
  const number=Number(value);
  return Number.isFinite(number)?(number*100).toFixed(0)+'%':'—';
}

function momentumText(oneMinute,fiveMinutes){
  const one=Number(oneMinute);
  const five=Number(fiveMinutes);
  if(!Number.isFinite(one)||!Number.isFinite(five)){return '—';}
  const signed=value=>(value>0?'+':'')+value.toFixed(3)+'%';
  return signed(one)+' / '+signed(five);
}

function directionalShare(value,yesLabel,noLabel){
  const share=Number(value);
  if(!Number.isFinite(share)){return '—';}
  return share>=0.5
    ?yesLabel+' '+percentText(share)
    :noLabel+' '+percentText(1-share);
}

function sideText(side){
  return side==='yes'?'UP':side==='no'?'DOWN':'—';
}

function cryptoName(series){
  return String(series||'').replace('KX','').replace('15M','');
}

function secondsLeft(market){
  const close=Date.parse(market.close_time);
  return Number.isFinite(close)?(close-Date.now())/1000:null;
}

function countdown(value){
  if(!Number.isFinite(value)){return '—';}
  const safe=Math.max(0,Math.floor(value));
  return String(Math.floor(safe/60)).padStart(2,'0')+':'
    +String(safe%60).padStart(2,'0');
}

function validEntryTime(market){
  const remaining=secondsLeft(market);
  return Number.isFinite(remaining)&&remaining>ENTRY_MIN_SECONDS
    &&remaining<=ENTRY_MAX_SECONDS;
}

function newPaperState(){
  return {version:'10-omega-impulso-all',active:false,cash:START_BANKROLL,open:[],closed:[],seen:[]};
}

function loadPaper(){
  try{
    const saved=JSON.parse(localStorage.getItem(PAPER_KEY));
    if(!saved||saved.version!=='10-omega-impulso-all'||!Array.isArray(saved.open)
      ||!Array.isArray(saved.closed)||!Array.isArray(saved.seen)){
      return newPaperState();
    }
    return {...newPaperState(),...saved,cash:Number(saved.cash),seen:saved.seen.slice(-5000)};
  }catch(error){return newPaperState();}
}

let paper=loadPaper();

function savePaper(){localStorage.setItem(PAPER_KEY,JSON.stringify(paper));}

function takerFee(contracts,price){
  const raw=0.07*Number(contracts)*Number(price)*(1-Number(price));
  return Math.ceil((raw-1e-12)*100)/100;
}

function exitPrice(market,side){
  if(side==='yes'){
    return market.yes_bid==null?null:Number(market.yes_bid);
  }
  if(market.no_bid!=null){return Number(market.no_bid);}
  return market.yes_ask==null?null:1-Number(market.yes_ask);
}

function exitResult(position,price){
  const fee=takerFee(position.contracts,price);
  const proceeds=position.contracts*price-fee;
  return {fee,pnl:roundNumber(proceeds-position.cost),proceeds:roundNumber(proceeds)};
}

function closeAtPrice(position,price,reason){
  const result=exitResult(position,price);
  paper.cash=roundNumber(paper.cash+result.proceeds);
  paper.closed.push({...position,exitPrice:price,exitFee:result.fee,
    netProceeds:result.proceeds,pnl:result.pnl,reason,closedAt:new Date().toISOString(),
    peakNetProceeds:Math.max(Number(position.peakNetProceeds??result.proceeds),result.proceeds),
    peakPnl:Math.max(Number(position.peakPnl??result.pnl),result.pnl),
    lowestPnl:Math.min(Number(position.lowestPnl??result.pnl),result.pnl)});
}

function settlePosition(position,result){
  const won=position.side===String(result).toLowerCase();
  const proceeds=won?position.contracts:0;
  const pnl=roundNumber(proceeds-position.cost);
  paper.cash=roundNumber(paper.cash+proceeds);
  paper.closed.push({...position,exitPrice:won?1:0,exitFee:0,netProceeds:proceeds,pnl,
    reason:won?'RESULTADO GANADOR':'RESULTADO PERDEDOR',
    closedAt:new Date().toISOString(),
    peakNetProceeds:Math.max(Number(position.peakNetProceeds??proceeds),proceeds),
    peakPnl:Math.max(Number(position.peakPnl??pnl),pnl),
    lowestPnl:Math.min(Number(position.lowestPnl??pnl),pnl)});
}

async function getMissingMarket(ticker){
  try{
    const response=await fetch('/api/market/'+encodeURIComponent(ticker),
      {cache:'no-store'});
    return response.ok?await response.json():null;
  }catch(error){return null;}
}

async function updateOpenPositions(markets){
  const current=new Map(markets.map(market=>[market.ticker,market]));
  const remaining=[];

  for(const original of paper.open){
    let market=current.get(original.ticker)||null;
    if(!market){market=await getMissingMarket(original.ticker);}

    if(market&&['yes','no'].includes(String(market.result).toLowerCase())){
      settlePosition(original,market.result);
      continue;
    }

    const price=market?exitPrice(market,original.side):null;
    if(price==null||!Number.isFinite(price)||price<=0||price>=1){
      remaining.push(original);
      continue;
    }

    const result=exitResult(original,price);
    const wasArmed=Boolean(original.trailArmed);
    const trailArmed=wasArmed||result.proceeds+1e-9>=TRAIL_ARM_NET_PROCEEDS;
    const previousPeak=Number(original.peakNetProceeds);
    const peakNetProceeds=trailArmed
      ?Math.max(Number.isFinite(previousPeak)?previousPeak:result.proceeds,result.proceeds)
      :null;
    const position={...original,lastExitPrice:price,lastPnl:result.pnl,
      lastNetProceeds:result.proceeds,trailArmed,peakNetProceeds,
      peakPnl:original.peakPnl==null?result.pnl:Math.max(original.peakPnl,result.pnl),
      lowestPnl:original.lowestPnl==null?result.pnl:Math.min(original.lowestPnl,result.pnl)};

    if(trailArmed&&result.proceeds<=peakNetProceeds-TRAIL_DROP+1e-9){
      closeAtPrice(position,price,'RETROCESO DE 2¢ DESDE EL MÁXIMO');
    }else{
      // Antes de $1.05 no existe salida. Despues, solo sale al retroceder 2 centavos.
      remaining.push(position);
    }
  }

  paper.open=remaining;
}

function omegaPlan(market){
  if(!market.binance_available||!['yes','no'].includes(market.omega_side)){
    return null;
  }
  const plan=market.selected_plan;
  return plan&&plan.action&&plan.action!=='WAIT'?plan:null;
}

function openCandidates(markets){
  if(!paper.active){return;}

  const candidates=markets
    .filter(market=>market.ticker&&!paper.seen.includes(market.ticker)
      &&validEntryTime(market)&&omegaPlan(market))
    .sort((a,b)=>secondsLeft(b)-secondsLeft(a));

  for(const market of candidates){
    const plan=omegaPlan(market);
    if(!plan||paper.open.length>=MAX_OPEN){continue;}

    if(Number(plan.cost)>paper.cash+1e-9){continue;}

    // Solo queda usado cuando la entrada simulada realmente se abre.
    paper.seen.push(market.ticker);
    const remaining=secondsLeft(market);
    paper.cash=roundNumber(paper.cash-Number(plan.cost));
    paper.open.push({
      ticker:market.ticker,
      series:market.series,
      side:String(plan.side).toLowerCase(),
      contracts:Number(plan.contracts),
      entryPrice:Number(plan.entry_price),
      entryFee:Number(plan.entry_fee),
      cost:Number(plan.cost),
      trailArmNetProceeds:Number(plan.trail_arm_net_proceeds),
      trailDrop:Number(plan.trail_drop),
      estimatedArmPrice:plan.estimated_arm_price==null?null:Number(plan.estimated_arm_price),
      closeTime:market.close_time,
      secondsLeftAtEntry:roundNumber(remaining,2),
      entryYesBid:Number(market.yes_bid),
      entryYesAsk:Number(market.yes_ask),
      entryNoBid:Number(market.no_bid),
      entryNoAsk:Number(market.no_ask),
      entryYesFlow:Number(market.yes_flow),
      entryNoFlow:Number(market.no_flow),
      flowTradeCount:Number(market.flow_trade_count),
      kalshiYesShare:market.kalshi_yes_share==null?null:Number(market.kalshi_yes_share),
      binanceSymbol:market.binance_symbol,
      binanceMarket:market.binance_market,
      binancePrice:market.binance_price==null?null:Number(market.binance_price),
      momentum1mPct:Number(market.momentum_1m_pct),
      momentum5mPct:Number(market.momentum_5m_pct),
      takerBuyShare:Number(market.taker_buy_share),
      orderbookBidShare:Number(market.orderbook_bid_share),
      omegaSide:String(market.omega_side),
      omegaVotes:Number(market.omega_votes),
      omegaYesVotes:Number(market.omega_yes_votes),
      omegaNoVotes:Number(market.omega_no_votes),
      omegaVoteDetails:market.omega_vote_details,
      spread:Number(market.spread),
      lastExitPrice:null,
      lastPnl:null,
      lastNetProceeds:null,
      trailArmed:false,
      peakNetProceeds:null,
      peakPnl:null,
      lowestPnl:null,
      openedAt:new Date().toISOString(),
    });
  }
}

function togglePaper(){
  paper.active=!paper.active;
  savePaper();
  renderPaper();
}

function csvCell(value){
  const text=value==null?'':String(value);
  return '"'+text.replaceAll('"','""')+'"';
}

function downloadPaperCsv(){
  if(!paper.closed.length){return;}
  const headers=['opened_at','closed_at','interval_close','series','ticker','side',
    'contracts','entry_price','exit_price','entry_fee','exit_fee','total_entry_cost',
    'net_proceeds_at_exit','pnl_net','reason','trail_arm_net_proceeds','trail_drop','estimated_arm_price',
    'seconds_left_at_entry','entry_yes_bid','entry_yes_ask','entry_no_bid','entry_no_ask',
    'entry_yes_flow','entry_no_flow','flow_trade_count','kalshi_yes_share',
    'binance_symbol','binance_market','binance_price','momentum_1m_pct','momentum_5m_pct',
    'taker_buy_share','orderbook_bid_share','omega_side','omega_votes',
    'omega_yes_votes','omega_no_votes','spread','trail_armed',
    'peak_net_proceeds','peak_pnl','lowest_pnl'];
  const rows=paper.closed.map(trade=>[
    trade.openedAt,trade.closedAt,trade.closeTime,trade.series,trade.ticker,trade.side,
    trade.contracts,trade.entryPrice,trade.exitPrice,trade.entryFee,trade.exitFee,
    trade.cost,trade.netProceeds,trade.pnl,trade.reason,trade.trailArmNetProceeds,trade.trailDrop,
    trade.estimatedArmPrice,trade.secondsLeftAtEntry,trade.entryYesBid,
    trade.entryYesAsk,trade.entryNoBid,trade.entryNoAsk,trade.entryYesFlow,
    trade.entryNoFlow,trade.flowTradeCount,trade.kalshiYesShare,
    trade.binanceSymbol,trade.binanceMarket,trade.binancePrice,trade.momentum1mPct,trade.momentum5mPct,
    trade.takerBuyShare,trade.orderbookBidShare,trade.omegaSide,trade.omegaVotes,
    trade.omegaYesVotes,trade.omegaNoVotes,trade.spread,trade.trailArmed,
    trade.peakNetProceeds,trade.peakPnl,trade.lowestPnl,
  ]);
  const csv=[headers,...rows].map(row=>row.map(csvCell).join(','))
    .join(String.fromCharCode(13,10));
  const blob=new Blob(['\ufeff'+csv],{type:'text/csv;charset=utf-8'});
  const link=document.createElement('a');
  link.href=URL.createObjectURL(blob);
  link.download='proyecto2_v10_omega_impulso_'+new Date().toISOString().slice(0,10)+'.csv';
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(()=>URL.revokeObjectURL(link.href),1000);
}

function renderPaper(){
  const realized=paper.closed.reduce((sum,trade)=>sum+Number(trade.pnl||0),0);
  const positives=paper.closed.filter(trade=>Number(trade.pnl||0)>0).length;
  const positiveRate=paper.closed.length?positives/paper.closed.length*100:0;
  const risk=paper.open.reduce((sum,trade)=>sum+Number(trade.cost||0),0);

  document.getElementById('paper-count').textContent=paper.closed.length;
  document.getElementById('paper-open').textContent=paper.open.length+' / '+MAX_OPEN;
  document.getElementById('paper-cash').textContent='$'+paper.cash.toFixed(2);
  document.getElementById('paper-risk').textContent='$'+risk.toFixed(2);
  document.getElementById('paper-wins').textContent=positives+' / '+paper.closed.length
    +' · '+positiveRate.toFixed(1)+'%';
  const pnl=document.getElementById('paper-pnl');
  pnl.textContent=money(realized);
  pnl.className='value '+(realized>0?'positive':realized<0?'negative':'');

  const button=document.getElementById('paper-toggle');
  button.textContent=paper.active?'Pausar nuevas entradas':'Iniciar prueba automática';
  button.className='button '+(paper.active?'pause':'');
  button.disabled=false;
  document.getElementById('paper-download').disabled=!paper.closed.length;
  document.getElementById('paper-state').textContent=paper.active
    ?'Activa 24/7 · vigila 14 criptos en todos los intervalos'
    :'Pausada · las posiciones abiertas sí continúan vigiladas';

  document.getElementById('open-rows').innerHTML=paper.open.map(position=>`<tr>
    <td>${cryptoName(position.series)}</td><td>${sideText(position.side)}</td>
    <td>${priceText(position.entryPrice)}</td><td>${priceText(position.lastExitPrice)}</td>
    <td>${dollarsText(position.lastNetProceeds)}</td>
    <td class="${Number(position.lastPnl)>=0?'positive':'negative'}">${position.lastPnl==null?'—':money(position.lastPnl)}</td>
    <td>${position.trailArmed?'ACTIVO · máx. '+dollarsText(position.peakNetProceeds):'Esperando $1.05'}</td>
    <td>${countdown((Date.parse(position.closeTime)-Date.now())/1000)}</td></tr>`).join('')
    ||'<tr><td colspan="8">Ninguna</td></tr>';

  document.getElementById('closed-rows').innerHTML=paper.closed.slice(-12).reverse()
    .map(trade=>`<tr><td>${cryptoName(trade.series)}</td><td>${sideText(trade.side)}</td>
    <td>${priceText(trade.entryPrice)}</td><td>${priceText(trade.exitPrice)}</td>
    <td class="${trade.pnl>=0?'positive':'negative'}">${money(trade.pnl)}</td>
    <td>${dollarsText(trade.peakNetProceeds)}</td><td>${trade.reason}</td></tr>`)
    .join('')||'<tr><td colspan="7">Todavía no hay resultados</td></tr>';
}

function renderMarkets(markets){
  document.getElementById('rows').innerHTML=markets.map(market=>{
    const remaining=secondsLeft(market);
    const plan=omegaPlan(market);
    const openPosition=paper.open.find(position=>position.ticker===market.ticker);
    const used=paper.seen.includes(market.ticker);
    const voteText=market.omega_side
      ?sideText(market.omega_side)+' '+Number(market.omega_votes)+'/4'
      :'UP '+Number(market.omega_yes_votes||0)+' · DOWN '+Number(market.omega_no_votes||0);
    let css='wait';
    let label='ESPERANDO OMEGA';

    if(openPosition){
      css=openPosition.side==='yes'?'yes':'no';
      label='ABIERTA '+sideText(openPosition.side);
    }else if(used){
      label='INTERVALO USADO';
    }else if(!validEntryTime(market)){
      label=remaining>ENTRY_MAX_SECONDS?'AÚN NO COMIENZA':'SIN APUESTA';
    }else if(!market.binance_available){
      label='BINANCE NO DISP.';
    }else if(plan){
      css=plan.side==='yes'?'yes':'no';
      label=(paper.active?'ENTRADA ':'SEÑAL ')+sideText(plan.side);
    }

    const planText=plan
      ?Number(market.omega_votes)+'/4 · '+Number(plan.contracts).toFixed(2)
        +' contratos · $'+Number(plan.cost).toFixed(2)
        +' · activa aprox. '+priceText(plan.estimated_arm_price)
      :String(market.omega_reason||market.binance_message||'Sin señal OMEGA');
    return `<tr><td>${cryptoName(market.series)}</td>
      <td>${priceText(market.yes_ask)}</td><td>${priceText(market.no_ask)}</td>
      <td>${momentumText(market.momentum_1m_pct,market.momentum_5m_pct)}</td>
      <td>${directionalShare(market.taker_buy_share,'BUY','SELL')}</td>
      <td>${directionalShare(market.orderbook_bid_share,'BID','ASK')}</td>
      <td>${directionalShare(market.kalshi_yes_share,'UP','DOWN')}</td>
      <td>${voteText}</td><td>${countdown(remaining)}</td>
      <td><span class="pill ${css}">${label}</span></td>
      <td>${planText}</td></tr>`;
  }).join('')||'<tr><td colspan="11">No hay mercados abiertos ahora</td></tr>';
}

async function refresh(){
  if(refreshing){return;}
  refreshing=true;
  try{
    const response=await fetch('/api/status',{cache:'no-store'});
    if(!response.ok){throw new Error('status');}
    const data=await response.json();
    const api=document.getElementById('api');
    api.textContent=data.kalshi.connected?'Conectada':'Pendiente';
    api.className='value '+(data.kalshi.connected?'safe':'warn');

    await updateOpenPositions(data.markets);
    openCandidates(data.markets);
    savePaper();
    renderMarkets(data.markets);
    renderPaper();
    document.getElementById('updated').textContent='Actualizado: '
      +new Date(data.updated_at).toLocaleString()
      +' · entrada al ask, salida al bid y tarifas incluidas';
  }catch(error){
    document.getElementById('api').textContent='Sin conexión';
  }finally{refreshing=false;}
}

renderPaper();
refresh();
setInterval(refresh,3000);
</script>
</body></html>
"""


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
