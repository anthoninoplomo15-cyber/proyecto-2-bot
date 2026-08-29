import base64
import datetime as dt
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from flask import Flask, jsonify, render_template_string

from bot import analyze_flow, build_paper_plan


app = Flask(__name__)

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY", "").replace("\\n", "\n").strip()

# Proyecto 2 siempre comienza en simulacion.
# No contiene llamadas para colocar ordenes.
SETTINGS = {
    "mode": "PAPER",
    "live_trading": False,
    "test_bankroll": 10.00,
    "max_cost_per_trade": 2.00,
    "profit_target": 0.20,
    "stop_loss": 0.10,
    "max_open_trades": 3,
    "flow_window_seconds": 90,
}

SERIES = [
    "KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M",
    "KXDOGE15M", "KXBNB15M", "KXADA15M", "KXLINK15M",
    "KXAVAX15M", "KXLTC15M", "KXBCH15M", "KXDOT15M",
    "KXHYPE15M", "KXSUI15M",
]

FLOW_CACHE_SECONDS = 45
FLOW_CACHE = {"updated": 0.0, "markets": {}}


def load_private_key():
    if not PRIVATE_KEY_PEM:
        raise ValueError("Falta KALSHI_PRIVATE_KEY")

    return serialization.load_pem_private_key(
        PRIVATE_KEY_PEM.encode("utf-8"),
        password=None,
    )


def auth_headers(method, endpoint):
    timestamp = str(
        int(
            dt.datetime.now(
                dt.timezone.utc
            ).timestamp() * 1000
        )
    )

    full_path = urlparse(
        BASE_URL + endpoint
    ).path

    message = (
        f"{timestamp}"
        f"{method.upper()}"
        f"{full_path}"
    ).encode("utf-8")

    signature = load_private_key().sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(
                hashes.SHA256()
            ),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": (
            base64.b64encode(
                signature
            ).decode("utf-8")
        ),
    }


def get_balance_status():
    if not API_KEY_ID or not PRIVATE_KEY_PEM:
        return {
            "connected": False,
            "message": "Credenciales pendientes en Render",
        }

    try:
        endpoint = "/portfolio/balance"

        response = requests.get(
            BASE_URL + endpoint,
            headers=auth_headers(
                "GET",
                endpoint,
            ),
            timeout=12,
        )

        response.raise_for_status()

        return {
            "connected": True,
            "message": "API conectada en modo lectura/prueba",
        }

    except Exception as exc:
        return {
            "connected": False,
            "message": (
                "No se pudo verificar la API: "
                f"{type(exc).__name__}"
            ),
        }


def dollars(
    market,
    dollar_key,
    cents_key,
):
    value = market.get(
        dollar_key
    )

    if value not in (None, ""):
        try:
            return round(
                float(value),
                4,
            )

        except (
            TypeError,
            ValueError,
        ):
            pass

    value = market.get(
        cents_key
    )

    if value not in (None, ""):
        try:
            return round(
                float(value) / 100,
                4,
            )

        except (
            TypeError,
            ValueError,
        ):
            pass

    return None


def scan_one_series(
    series_ticker,
):
    response = requests.get(
        BASE_URL + "/markets",
        params={
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 5,
        },
        timeout=8,
    )

    response.raise_for_status()

    markets = [
        {
            "series": series_ticker,
            "ticker": market.get(
                "ticker"
            ),
            "title": (
                market.get("title")
                or market.get("subtitle")
                or series_ticker
            ),
            "close_time": market.get(
                "close_time"
            ),
            "yes_bid": dollars(
                market,
                "yes_bid_dollars",
                "yes_bid",
            ),
            "yes_ask": dollars(
                market,
                "yes_ask_dollars",
                "yes_ask",
            ),
            "volume": market.get(
                "volume_fp",
                market.get(
                    "volume",
                    0,
                ),
            ),
        }
        for market
        in response.json().get(
            "markets",
            [],
        )
    ]

    markets.sort(
        key=lambda item: (
            item.get("close_time")
            or ""
        )
    )

    return markets[:1]


def scan_markets():
    found = []

    with ThreadPoolExecutor(
        max_workers=7
    ) as pool:
        jobs = {
            pool.submit(
                scan_one_series,
                ticker,
            ): ticker
            for ticker in SERIES
        }

        for job in as_completed(
            jobs
        ):
            try:
                found.extend(
                    job.result()
                )

            except requests.RequestException:
                continue

    found.sort(
        key=lambda item: (
            item.get("close_time")
            or ""
        )
    )

    return found


def analyze_one_market(
    market,
):
    ticker = market.get(
        "ticker"
    )

    try:
        signal = analyze_flow(
            ticker
        )

        plan = build_paper_plan(
            signal,
            market.get("yes_bid"),
            market.get("yes_ask"),
        )

    except requests.RequestException:
        signal = {
            "ticker": ticker,
            "action": "WAIT",
            "side": None,
            "reason": (
                "Datos de flujo temporalmente "
                "no disponibles"
            ),
            "windows": {},
        }

        plan = {
            "action": "WAIT",
            "reason": signal["reason"],
        }

    return ticker, {
        "signal": signal,
        "plan": plan,
    }


def add_paper_analysis(
    markets,
):
    now = time.monotonic()

    cache_is_fresh = (
        FLOW_CACHE["markets"]
        and now
        - FLOW_CACHE["updated"]
        < FLOW_CACHE_SECONDS
    )

    if not cache_is_fresh:
        analysis = {}

        unique = {
            market.get("ticker"): market
            for market in markets
            if market.get("ticker")
        }

        with ThreadPoolExecutor(
            max_workers=7
        ) as pool:
            jobs = [
                pool.submit(
                    analyze_one_market,
                    market,
                )
                for market
                in unique.values()
            ]

            for job in as_completed(
                jobs
            ):
                try:
                    ticker, result = (
                        job.result()
                    )

                    analysis[ticker] = (
                        result
                    )

                except Exception:
                    continue

        FLOW_CACHE["markets"] = (
            analysis
        )

        FLOW_CACHE["updated"] = (
            now
        )

    enriched = []

    for market in markets:
        item = dict(
            market
        )

        result = FLOW_CACHE[
            "markets"
        ].get(
            item.get("ticker"),
            {},
        )

        item["paper_signal"] = result.get(
            "signal",
            {
                "action": "WAIT",
                "side": None,
                "reason": "Analizando flujo",
                "windows": {},
            },
        )

        item["paper_plan"] = result.get(
            "plan",
            {
                "action": "WAIT",
                "reason": "Analizando flujo",
            },
        )

        enriched.append(
            item
        )

    return enriched


@app.get("/health")
def health():
    return {
        "ok": True,
        "project": "Proyecto 2",
        "mode": "PAPER",
    }


@app.get("/api/status")
def api_status():
    markets = scan_markets()

    return jsonify(
        {
            "settings": SETTINGS,
            "kalshi": get_balance_status(),
            "markets": add_paper_analysis(
                markets
            ),
            "updated_at": (
                dt.datetime.now(
                    dt.timezone.utc
                ).isoformat()
            ),
        }
    )


@app.get("/api/market/<ticker>")
def market_status(ticker):
    safe_ticker = ticker.replace(
        "-",
        "",
    )

    if not safe_ticker.isalnum():
        return jsonify(
            {
                "error": "Ticker invalido"
            }
        ), 400

    try:
        response = requests.get(
            BASE_URL
            + "/markets/"
            + ticker,
            timeout=8,
        )

        response.raise_for_status()

        market = response.json().get(
            "market",
            {},
        )

        return jsonify(
            {
                "ticker": market.get(
                    "ticker"
                ),
                "status": market.get(
                    "status"
                ),
                "result": market.get(
                    "result"
                ),
                "close_time": market.get(
                    "close_time"
                ),
                "yes_bid": dollars(
                    market,
                    "yes_bid_dollars",
                    "yes_bid",
                ),
                "yes_ask": dollars(
                    market,
                    "yes_ask_dollars",
                    "yes_ask",
                ),
            }
        )

    except requests.RequestException:
        return jsonify(
            {
                "error": "Mercado no disponible"
            }
        ), 503


@app.get("/")
def home():
    return render_template_string(
        HTML
    )


HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta
    name="viewport"
    content="width=device-width,initial-scale=1"
  >
  <title>Proyecto 2</title>

  <style>
    :root {
      color-scheme: dark;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      background: #07111f;
      color: #eef4ff;
      font-family: system-ui, Arial;
    }

    .wrap {
      max-width: 1000px;
      margin: auto;
      padding: 18px;
    }

    h1 {
      margin: 0 0 4px;
    }

    h2 {
      margin-top: 24px;
    }

    .tag {
      display: inline-block;
      background: #1f6b3d;
      padding: 6px 10px;
      border-radius: 999px;
      font-weight: 800;
    }

    .grid {
      display: grid;
      grid-template-columns:
        repeat(
          auto-fit,
          minmax(140px, 1fr)
        );
      gap: 10px;
      margin: 16px 0;
    }

    .card {
      background: #111f33;
      border: 1px solid #263b57;
      border-radius: 14px;
      padding: 14px;
    }

    .label {
      color: #96abc8;
      font-size: 12px;
    }

    .value {
      font-size: 21px;
      font-weight: 800;
      margin-top: 3px;
    }

    .safe,
    .positive {
      color: #6ee7a2;
    }

    .warn {
      color: #ffd166;
    }

    .negative {
      color: #ff9fa8;
    }

    .note {
      background: #13243a;
      border-left: 4px solid #4da3ff;
      padding: 12px;
      border-radius: 9px;
      margin: 14px 0;
      color: #c7d7ed;
    }

    .controls {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      margin: 14px 0;
    }

    .button {
      border: 0;
      border-radius: 10px;
      padding: 11px 16px;
      font-weight: 800;
      cursor: pointer;
      background: #2f81f7;
      color: white;
    }

    .button.pause {
      background: #a66a12;
    }

    .pill {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
    }

    .yes {
      background: #165b37;
      color: #82f0ad;
    }

    .no {
      background: #67272d;
      color: #ff9fa8;
    }

    .wait {
      background: #3c4655;
      color: #d6deea;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      background: #111f33;
      border-radius: 14px;
      overflow: hidden;
    }

    th,
    td {
      text-align: left;
      padding: 11px 9px;
      border-bottom:
        1px solid #263b57;
      font-size: 13px;
    }

    th {
      color: #96abc8;
    }

    .foot {
      color: #96abc8;
      margin-top: 14px;
      font-size: 12px;
    }

    .table-wrap {
      overflow-x: auto;
      border-radius: 14px;
    }

    .muted {
      color: #96abc8;
    }
  </style>
</head>

<body>
<div class="wrap">
  <h1>Proyecto 2</h1>

  <div class="tag">
    MODO PRUEBA · SIN ÓRDENES REALES
  </div>

  <div class="grid">
    <div class="card">
      <div class="label">
        Kalshi API
      </div>

      <div
        id="api"
        class="value warn"
      >
        Verificando…
      </div>
    </div>

    <div class="card">
      <div class="label">
        Pruebas terminadas
      </div>

      <div
        id="paper-count"
        class="value"
      >
        0 / 100
      </div>
    </div>

    <div class="card">
      <div class="label">
        Abiertas
      </div>

      <div
        id="paper-open"
        class="value"
      >
        0 / 3
      </div>
    </div>

    <div class="card">
      <div class="label">
        Saldo disponible
      </div>

      <div
        id="paper-cash"
        class="value"
      >
        $10.00
      </div>
    </div>

    <div class="card">
      <div class="label">
        Ganancia neta
      </div>

      <div
        id="paper-pnl"
        class="value"
      >
        $0.00
      </div>
    </div>

    <div class="card">
      <div class="label">
        Máximo por operación
      </div>

      <div class="value">
        $2.00
      </div>
    </div>

    <div class="card">
      <div class="label">
        Objetivo / stop
      </div>

      <div class="value">
        +$0.20 / −$0.10
      </div>
    </div>

    <div class="card">
      <div class="label">
        Ventanas de flujo
      </div>

      <div class="value">
        60 / 90 / 120 s
      </div>
    </div>
  </div>

  <div class="note">
    <strong>
      Simulación controlada.
    </strong>

    Las entradas, salidas, spread
    y tarifas se calculan con dinero
    ficticio. Para registrar pruebas,
    deja esta pestaña abierta y la
    computadora encendida.
  </div>

  <div class="controls">
    <button
      id="paper-toggle"
      class="button"
      onclick="togglePaper()"
    >
      Iniciar prueba automática
    </button>

    <span
      id="paper-state"
      class="muted"
    >
      Pausada · no abre posiciones
    </span>
  </div>

  <h2>
    Mercados cripto de 15 minutos
  </h2>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Serie</th>
          <th>YES compra</th>
          <th>Flujo</th>
          <th>Plan simulado</th>
        </tr>
      </thead>

      <tbody id="rows">
        <tr>
          <td colspan="4">
            Buscando mercados…
          </td>
        </tr>
      </tbody>
    </table>
  </div>

  <h2>
    Operaciones simuladas abiertas
  </h2>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Serie</th>
          <th>Lado</th>
          <th>Entrada</th>
          <th>Objetivo</th>
          <th>Stop</th>
        </tr>
      </thead>

      <tbody id="open-rows">
        <tr>
          <td colspan="5">
            Ninguna
          </td>
        </tr>
      </tbody>
    </table>
  </div>

  <h2>
    Últimos resultados
  </h2>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Serie</th>
          <th>Lado</th>
          <th>Salida</th>
          <th>Resultado neto</th>
          <th>Motivo</th>
        </tr>
      </thead>

      <tbody id="closed-rows">
        <tr>
          <td colspan="5">
            Todavía no hay resultados
          </td>
        </tr>
      </tbody>
    </table>
  </div>

  <div
    id="updated"
    class="foot"
  ></div>
</div>

<script>
const PAPER_KEY =
  "proyecto2_paper_v1";

const START_BANKROLL = 10.00;
const MAX_OPEN = 3;
const TEST_GOAL = 100;

let refreshing = false;


function roundNumber(
  value,
  digits = 4,
) {
  const power =
    10 ** digits;

  return Math.round(
    (
      Number(value)
      + Number.EPSILON
    )
    * power
  ) / power;
}


function money(value) {
  const number =
    Number(value) || 0;

  if (number > 0) {
    return (
      "+$"
      + number.toFixed(2)
    );
  }

  if (number < 0) {
    return (
      "-$"
      + Math.abs(
          number
        ).toFixed(2)
    );
  }

  return "$0.00";
}


function newPaperState() {
  return {
    version: 1,
    active: false,
    cash: START_BANKROLL,
    open: [],
    closed: [],
    seen: [],
  };
}


function loadPaper() {
  try {
    const saved =
      JSON.parse(
        localStorage.getItem(
          PAPER_KEY
        )
      );

    if (
      !saved
      || !Array.isArray(
        saved.open
      )
      || !Array.isArray(
        saved.closed
      )
      || !Array.isArray(
        saved.seen
      )
    ) {
      return newPaperState();
    }

    return {
      ...newPaperState(),
      ...saved,
      cash: Number(
        saved.cash
      ),
    };

  } catch (error) {
    return newPaperState();
  }
}


let paper = loadPaper();


function savePaper() {
  localStorage.setItem(
    PAPER_KEY,
    JSON.stringify(
      paper
    )
  );
}


function takerFee(
  contracts,
  price,
) {
  const raw =
    0.07
    * Number(contracts)
    * Number(price)
    * (
      1
      - Number(price)
    );

  return (
    Math.ceil(
      (
        raw
        - 1e-12
      )
      * 100
    )
    / 100
  );
}


function exitPrice(
  market,
  side,
) {
  if (side === "yes") {
    if (
      market.yes_bid == null
    ) {
      return null;
    }

    return Number(
      market.yes_bid
    );
  }

  if (
    market.yes_ask == null
  ) {
    return null;
  }

  return (
    1
    - Number(
        market.yes_ask
      )
  );
}


function exitResult(
  position,
  price,
) {
  const fee =
    takerFee(
      position.contracts,
      price,
    );

  const proceeds =
    position.contracts
    * price
    - fee;

  return {
    fee: fee,
    pnl: roundNumber(
      proceeds
      - position.cost
    ),
    proceeds: roundNumber(
      proceeds
    ),
  };
}


function closeAtPrice(
  position,
  price,
  reason,
) {
  const result =
    exitResult(
      position,
      price,
    );

  paper.cash =
    roundNumber(
      paper.cash
      + result.proceeds
    );

  paper.closed.push(
    {
      ...position,
      exitPrice: price,
      exitFee: result.fee,
      pnl: result.pnl,
      reason: reason,
      closedAt:
        new Date().toISOString(),
    }
  );
}


function settlePosition(
  position,
  result,
) {
  const won =
    position.side
    === String(
      result
    ).toLowerCase();

  const proceeds =
    won
      ? position.contracts
      : 0;

  const pnl =
    roundNumber(
      proceeds
      - position.cost
    );

  paper.cash =
    roundNumber(
      paper.cash
      + proceeds
    );

  paper.closed.push(
    {
      ...position,
      exitPrice:
        won ? 1 : 0,
      exitFee: 0,
      pnl: pnl,
      reason: "RESULTADO",
      closedAt:
        new Date().toISOString(),
    }
  );
}


async function getMissingMarket(
  ticker,
) {
  try {
    const response =
      await fetch(
        "/api/market/"
        + encodeURIComponent(
            ticker
          ),
        {
          cache: "no-store",
        },
      );

    if (!response.ok) {
      return null;
    }

    return await response.json();

  } catch (error) {
    return null;
  }
}


async function updateOpenPositions(
  markets,
) {
  const current =
    new Map(
      markets.map(
        market => [
          market.ticker,
          market,
        ]
      )
    );

  const remaining = [];

  for (
    const position
    of paper.open
  ) {
    let market =
      current.get(
        position.ticker
      ) || null;

    if (!market) {
      market =
        await getMissingMarket(
          position.ticker
        );
    }

    const result =
      String(
        market?.result
      ).toLowerCase();

    if (
      market
      && [
        "yes",
        "no",
      ].includes(result)
    ) {
      settlePosition(
        position,
        result,
      );

      continue;
    }

    const price =
      market
        ? exitPrice(
            market,
            position.side,
          )
        : null;

    if (
      price == null
      || !Number.isFinite(
        price
      )
      || price <= 0
      || price >= 1
    ) {
      remaining.push(
        position
      );

      continue;
    }

    const secondsLeft =
      (
        Date.parse(
          position.closeTime
        )
        - Date.now()
      )
      / 1000;

    if (
      price
      >= position.targetPrice
    ) {
      closeAtPrice(
        position,
        price,
        "OBJETIVO",
      );

    } else if (
      price
      <= position.stopPrice
    ) {
      closeAtPrice(
        position,
        price,
        "STOP",
      );

    } else if (
      secondsLeft <= 30
    ) {
      closeAtPrice(
        position,
        price,
        "TIEMPO",
      );

    } else {
      remaining.push(
        position
      );
    }
  }

  paper.open =
    remaining;
}


function flowScore(market) {
  const windows =
    Object.values(
      market.paper_signal
        ?.windows
      || {}
    );

  const confirmed =
    windows.filter(
      window =>
        window.confirmed
    );

  const dominance =
    confirmed.reduce(
      (
        sum,
        window,
      ) =>
        sum
        + Number(
            window.dominance
            || 0
          ),
      0,
    );

  return (
    confirmed.length
    * 100
    + dominance
  );
}


function validEntryTime(
  market,
) {
  const close =
    Date.parse(
      market.close_time
    );

  if (
    !Number.isFinite(
      close
    )
  ) {
    return false;
  }

  const secondsLeft =
    (
      close
      - Date.now()
    )
    / 1000;

  return (
    secondsLeft > 180
    && secondsLeft < 780
  );
}


function openCandidates(
  markets,
) {
  if (
    !paper.active
    || paper.closed.length
       + paper.open.length
       >= TEST_GOAL
  ) {
    return;
  }

  const candidates =
    markets.filter(
      market => {
        const plan =
          market.paper_plan
          || {};

        return (
          plan.action
          && plan.action !== "WAIT"
          && !paper.seen.includes(
            market.ticker
          )
          && validEntryTime(
            market
          )
        );
      }
    ).sort(
      (
        first,
        second,
      ) =>
        flowScore(second)
        - flowScore(first)
    );

  for (
    const market
    of candidates
  ) {
    if (
      paper.open.length
      >= MAX_OPEN
      || paper.closed.length
         + paper.open.length
         >= TEST_GOAL
    ) {
      break;
    }

    const plan =
      market.paper_plan;

    if (
      Number(plan.cost)
      > paper.cash
    ) {
      continue;
    }

    paper.cash =
      roundNumber(
        paper.cash
        - Number(
            plan.cost
          )
      );

    paper.open.push(
      {
        ticker:
          market.ticker,
        series:
          market.series,
        side:
          plan.side,
        contracts:
          Number(
            plan.contracts
          ),
        entryPrice:
          Number(
            plan.entry_price
          ),
        entryFee:
          Number(
            plan.entry_fee
          ),
        cost:
          Number(
            plan.cost
          ),
        targetPrice:
          Number(
            plan.target_price
          ),
        stopPrice:
          Number(
            plan.stop_price
          ),
        closeTime:
          market.close_time,
        openedAt:
          new Date().toISOString(),
      }
    );

    paper.seen.push(
      market.ticker
    );
  }
}


function togglePaper() {
  if (
    paper.closed.length
    >= TEST_GOAL
  ) {
    paper.active = false;

  } else {
    paper.active =
      !paper.active;
  }

  savePaper();
  renderPaper();
}


function renderPaper() {
  const realized =
    paper.closed.reduce(
      (
        sum,
        trade,
      ) =>
        sum
        + Number(
            trade.pnl
            || 0
          ),
      0,
    );

  document.getElementById(
    "paper-count"
  ).textContent =
    paper.closed.length
    + " / "
    + TEST_GOAL;

  document.getElementById(
    "paper-open"
  ).textContent =
    paper.open.length
    + " / "
    + MAX_OPEN;

  document.getElementById(
    "paper-cash"
  ).textContent =
    "$"
    + paper.cash.toFixed(2);

  const pnl =
    document.getElementById(
      "paper-pnl"
    );

  pnl.textContent =
    money(
      realized
    );

  pnl.className =
    "value "
    + (
      realized > 0
        ? "positive"
        : realized < 0
          ? "negative"
          : ""
    );

  const button =
    document.getElementById(
      "paper-toggle"
    );

  button.textContent =
    paper.active
      ? "Pausar nuevas entradas"
      : "Iniciar prueba automática";

  button.className =
    "button "
    + (
      paper.active
        ? "pause"
        : ""
    );

  document.getElementById(
    "paper-state"
  ).textContent =
    paper.closed.length
    >= TEST_GOAL
      ? "Prueba de 100 terminada"
      : paper.active
        ? "Activa · buscando máximo 3 posiciones"
        : "Pausada · no abre posiciones";

  const openRows =
    paper.open.map(
      position => `
        <tr>
          <td>
            ${
              position.series
                .replace("KX", "")
                .replace("15M", "")
            }
          </td>

          <td>
            ${position.side.toUpperCase()}
          </td>

          <td>
            $${position.entryPrice.toFixed(2)}
          </td>

          <td>
            $${position.targetPrice.toFixed(2)}
          </td>

          <td>
            $${position.stopPrice.toFixed(2)}
          </td>
        </tr>
      `
    ).join("");

  document.getElementById(
    "open-rows"
  ).innerHTML =
    openRows
    || `
      <tr>
        <td colspan="5">
          Ninguna
        </td>
      </tr>
    `;

  const closedRows =
    paper.closed
      .slice(-10)
      .reverse()
      .map(
        trade => `
          <tr>
            <td>
              ${
                trade.series
                  .replace("KX", "")
                  .replace("15M", "")
              }
            </td>

            <td>
              ${trade.side.toUpperCase()}
            </td>

            <td>
              $${Number(
                trade.exitPrice
              ).toFixed(2)}
            </td>

            <td
              class="${
                trade.pnl >= 0
                  ? "positive"
                  : "negative"
              }"
            >
              ${money(trade.pnl)}
            </td>

            <td>
              ${trade.reason}
            </td>
          </tr>
        `
      ).join("");

  document.getElementById(
    "closed-rows"
  ).innerHTML =
    closedRows
    || `
      <tr>
        <td colspan="5">
          Todavía no hay resultados
        </td>
      </tr>
    `;
}


function renderMarkets(
  markets,
) {
  const rows =
    markets.map(
      market => {
        const action =
          market.paper_signal
            ?.action
          || "WAIT";

        const css =
          action
          === "PAPER_BUY_YES"
            ? "yes"
            : action
              === "PAPER_BUY_NO"
              ? "no"
              : "wait";

        const label =
          action
          === "PAPER_BUY_YES"
            ? "PRUEBA YES"
            : action
              === "PAPER_BUY_NO"
              ? "PRUEBA NO"
              : "ESPERAR";

        const plan =
          market.paper_plan
          || {};

        const planText =
          plan.action
          && plan.action !== "WAIT"
            ? (
              plan.contracts
              + " contratos · $"
              + Number(
                  plan.cost
                ).toFixed(2)
            )
            : "—";

        const name =
          market.series
            .replace("KX", "")
            .replace("15M", "");

        const price =
          market.yes_ask == null
            ? "—"
            : (
              "$"
              + Number(
                  market.yes_ask
                ).toFixed(2)
            );

        return `
          <tr>
            <td>${name}</td>
            <td>${price}</td>

            <td>
              <span
                class="pill ${css}"
                title="${
                  market.paper_signal
                    ?.reason
                  || "Analizando"
                }"
              >
                ${label}
              </span>
            </td>

            <td>${planText}</td>
          </tr>
        `;
      }
    ).join("");

  document.getElementById(
    "rows"
  ).innerHTML =
    rows
    || `
      <tr>
        <td colspan="4">
          No hay mercados abiertos ahora
        </td>
      </tr>
    `;
}


async function refresh() {
  if (refreshing) {
    return;
  }

  refreshing = true;

  try {
    const response =
      await fetch(
        "/api/status",
        {
          cache: "no-store",
        },
      );

    if (!response.ok) {
      throw new Error(
        "status"
      );
    }

    const data =
      await response.json();

    const api =
      document.getElementById(
        "api"
      );

    api.textContent =
      data.kalshi.connected
        ? "Conectada"
        : "Pendiente";

    api.className =
      "value "
      + (
        data.kalshi.connected
          ? "safe"
          : "warn"
      );

    await updateOpenPositions(
      data.markets
    );

    openCandidates(
      data.markets
    );

    if (
      paper.closed.length
      >= TEST_GOAL
    ) {
      paper.active = false;
    }

    savePaper();

    renderMarkets(
      data.markets
    );

    renderPaper();

    document.getElementById(
      "updated"
    ).textContent =
      "Actualizado: "
      + new Date(
          data.updated_at
        ).toLocaleString()
      + " · costos incluyen tarifas";

  } catch (error) {
    document.getElementById(
      "api"
    ).textContent =
      "Sin conexión";

  } finally {
    refreshing = false;
  }
}


renderPaper();
refresh();

setInterval(
  refresh,
  15000,
);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    port = int(
        os.getenv(
            "PORT",
            "10000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
            )
