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
API_KEY_ID = os.getenv(
    "KALSHI_API_KEY_ID",
    "",
).strip()

PRIVATE_KEY_PEM = os.getenv(
    "KALSHI_PRIVATE_KEY",
    "",
).replace("\\n", "\n").strip()


# Proyecto 2 siempre comienza en simulacion.
# No crea, cancela, deposita ni retira dinero.
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
    "KXBTC15M",
    "KXETH15M",
    "KXSOL15M",
    "KXXRP15M",
    "KXDOGE15M",
    "KXBNB15M",
    "KXADA15M",
    "KXLINK15M",
    "KXAVAX15M",
    "KXLTC15M",
    "KXBCH15M",
    "KXDOT15M",
    "KXHYPE15M",
    "KXSUI15M",
]


# El flujo se vuelve a medir cada 45 segundos.
FLOW_CACHE_SECONDS = 45

FLOW_CACHE = {
    "updated": 0.0,
    "markets": {},
}


def load_private_key():
    if not PRIVATE_KEY_PEM:
        raise ValueError(
            "Falta KALSHI_PRIVATE_KEY"
        )

    return serialization.load_pem_private_key(
        PRIVATE_KEY_PEM.encode("utf-8"),
        password=None,
    )


def auth_headers(method, endpoint):
    timestamp = str(
        int(
            dt.datetime.now(
                dt.timezone.utc
            ).timestamp()
            * 1000
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
            salt_length=(
                padding.PSS.DIGEST_LENGTH
            ),
        ),
        hashes.SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": (
            API_KEY_ID
        ),
        "KALSHI-ACCESS-TIMESTAMP": (
            timestamp
        ),
        "KALSHI-ACCESS-SIGNATURE": (
            base64.b64encode(
                signature
            ).decode("utf-8")
        ),
    }


def get_balance_status():
    if (
        not API_KEY_ID
        or not PRIVATE_KEY_PEM
    ):
        return {
            "connected": False,
            "message": (
                "Credenciales pendientes "
                "en Render"
            ),
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
            "message": (
                "API conectada en modo "
                "lectura/prueba"
            ),
        }

    except Exception as exc:
        return {
            "connected": False,
            "message": (
                "No se pudo verificar "
                f"la API: {type(exc).__name__}"
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
            "series_ticker": (
                series_ticker
            ),
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
                or market.get(
                    "subtitle"
                )
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
        for market in response.json().get(
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
            market.get(
                "yes_bid"
            ),
            market.get(
                "yes_ask"
            ),
        )

    except requests.RequestException:
        signal = {
            "ticker": ticker,
            "action": "WAIT",
            "side": None,
            "reason": (
                "Datos de flujo "
                "temporalmente no disponibles"
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

        item["paper_signal"] = (
            result.get(
                "signal",
                {
                    "action": "WAIT",
                    "side": None,
                    "reason": (
                        "Analizando flujo"
                    ),
                    "windows": {},
                },
            )
        )

        item["paper_plan"] = (
            result.get(
                "plan",
                {
                    "action": "WAIT",
                    "reason": (
                        "Analizando flujo"
                    ),
                },
            )
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
            "kalshi": (
                get_balance_status()
            ),
            "markets": (
                add_paper_analysis(
                    markets
                )
            ),
            "updated_at": (
                dt.datetime.now(
                    dt.timezone.utc
                ).isoformat()
            ),
        }
    )


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

    body {
      margin: 0;
      background: #07111f;
      color: #eef4ff;
      font-family: system-ui, Arial;
    }

    .wrap {
      max-width: 900px;
      margin: auto;
      padding: 18px;
    }

    h1 {
      margin: 0 0 4px;
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
          minmax(145px, 1fr)
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

    .safe {
      color: #6ee7a2;
    }

    .warn {
      color: #ffd166;
    }

    .note {
      background: #13243a;
      border-left:
        4px solid #4da3ff;
      padding: 12px;
      border-radius: 9px;
      margin: 14px 0;
      color: #c7d7ed;
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
        Fondo de prueba
      </div>

      <div class="value">
        $10.00
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
        Operaciones simultáneas
      </div>

      <div class="value">
        3
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
      Simulación solamente.
    </strong>

    Una señal indica un candidato
    de prueba; no envía órdenes.
    Revisaremos 100 operaciones
    simuladas antes de considerar
    cualquier cambio.
  </div>

  <h2>
    Mercados cripto de 15 minutos
  </h2>

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

  <div
    id="updated"
    class="foot"
  ></div>
</div>

<script>
async function refresh() {
  try {
    const response = await fetch(
      "/api/status"
    );

    const data = await response.json();

    const api = document.getElementById(
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

    const rows = data.markets.map(
      market => {
        const action =
          market.paper_signal?.action
          || "WAIT";

        const className =
          action === "PAPER_BUY_YES"
            ? "yes"
            : action === "PAPER_BUY_NO"
              ? "no"
              : "wait";

        const label =
          action === "PAPER_BUY_YES"
            ? "PRUEBA YES"
            : action === "PAPER_BUY_NO"
              ? "PRUEBA NO"
              : "ESPERAR";

        const paperPlan =
          market.paper_plan
          || {};

        const plan =
          paperPlan.action
          && paperPlan.action !== "WAIT"
            ? (
              paperPlan.contracts
              + " contratos · $"
              + paperPlan.cost.toFixed(2)
            )
            : "—";

        const reason =
          market.paper_signal?.reason
          || "Analizando flujo";

        const name =
          market.series
            .replace("KX", "")
            .replace("15M", "");

        const price =
          market.yes_ask == null
            ? "—"
            : "$"
              + market.yes_ask.toFixed(2);

        return `
          <tr>
            <td>${name}</td>
            <td>${price}</td>
            <td>
              <span
                class="pill ${className}"
                title="${reason}"
              >
                ${label}
              </span>
            </td>
            <td>${plan}</td>
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

    document.getElementById(
      "updated"
    ).textContent =
      "Actualizado: "
      + new Date(
        data.updated_at
      ).toLocaleString();

  } catch (error) {
    document.getElementById(
      "api"
    ).textContent =
      "Sin conexión";
  }
}

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
