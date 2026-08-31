import os
import re
import csv
import io
import sqlite3
import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

NY = ZoneInfo("America/New_York")
DB_PATH = os.getenv("DB_PATH", "whales.db")

# Market direction is read from liquid U.S. ETFs.
# Nasdaq's quote endpoint is the primary source because it can include
# pre-market / after-hours quotes. CBOE is a no-key delayed fallback.
DEFAULT_MARKETS = [
    {"symbol": "QQQ", "name": "Nasdaq 100 · QQQ"},
    {"symbol": "SPY", "name": "S&P 500 · SPY"},
    {"symbol": "DIA", "name": "Dow Jones · DIA"},
    {"symbol": "IWM", "name": "Russell 2000 · IWM"},
    {"symbol": "VTI", "name": "U.S. Total Market · VTI"},
]

EXCHANGES = {
    "binance", "coinbase", "coinbase institutional", "kraken", "gemini",
    "bitfinex", "bitstamp", "okx", "okex", "bybit", "kucoin", "crypto.com",
    "gate.io", "gateio", "bitget", "htx", "huobi", "mexc", "deribit", "bittrex",
    "poloniex", "upbit", "bithumb", "bitpanda",
}

PUBLIC_TELEGRAM_CHANNELS = {
    "whale_alert_io": "https://t.me/s/whale_alert_io",
    "whalebotalerts": "https://t.me/s/whalebotalerts",
}

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 "
        "Mobile/15E148 Safari/604.1"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
}

MARKET_CACHE = None
MARKET_CACHE_AT = None
MARKET_CACHE_SECONDS = 60
MARKET_LOCK = asyncio.Lock()
MONITOR_TASK = None

# Public Kalshi market data does not require account credentials.  The terminal
# polls completed trades because the authenticated WebSocket would require the
# user's API key.  Two seconds still keeps this panel inside the requested
# 1-3-second refresh window without storing account secrets on Render.
KALSHI_API_BASE = os.getenv(
    "KALSHI_API_BASE",
    "https://external-api.kalshi.com/trade-api/v2",
).rstrip("/")
KALSHI_SERIES_TICKER = os.getenv("KALSHI_SERIES_TICKER", "KXBTC15M")
KALSHI_FLOW_POLL_SECONDS = max(1.0, float(os.getenv("KALSHI_FLOW_POLL_SECONDS", "2")))
KALSHI_MARKET_REFRESH_SECONDS = max(
    2.0,
    float(os.getenv("KALSHI_MARKET_REFRESH_SECONDS", "5")),
)
KALSHI_MAX_TRADE_PAGES = max(1, int(os.getenv("KALSHI_MAX_TRADE_PAGES", "100")))
KALSHI_FLOW_LOCK = asyncio.Lock()
KALSHI_FLOW_TASK = None
KALSHI_FLOW_STATE = {
    "market": None,
    "trades": {},
    "last_market_check": None,
    "last_poll": None,
    "last_success": None,
    "error": None,
    "truncated": False,
}

# Read-only sports flow.  This panel never submits orders and never reads the
# user's Kalshi account.  It discovers public sports events, then totals the
# completed trades initiated on each YES/NO side during a rolling time window.
SPORTS_FLOW_POLL_SECONDS = max(
    10.0,
    float(os.getenv("SPORTS_FLOW_POLL_SECONDS", "15")),
)
SPORTS_DISCOVERY_SECONDS = max(
    60.0,
    float(os.getenv("SPORTS_DISCOVERY_SECONDS", "300")),
)
SPORTS_FLOW_WINDOW_MINUTES = max(
    1,
    int(os.getenv("SPORTS_FLOW_WINDOW_MINUTES", "15")),
)
SPORTS_MAX_MARKETS = max(6, int(os.getenv("SPORTS_MAX_MARKETS", "72")))
SPORTS_MARKETS_PER_EVENT = max(
    1,
    int(os.getenv("SPORTS_MARKETS_PER_EVENT", "3")),
)
SPORTS_MARKETS_PER_SPORT = max(
    1,
    int(os.getenv("SPORTS_MARKETS_PER_SPORT", "12")),
)
SPORTS_EVENT_TICKERS_PER_SPORT = max(
    10,
    int(os.getenv("SPORTS_EVENT_TICKERS_PER_SPORT", "120")),
)
SPORTS_MAX_MILESTONE_PAGES = max(
    1,
    int(os.getenv("SPORTS_MAX_MILESTONE_PAGES", "2")),
)
SPORTS_MAX_EVENT_PAGES = max(
    1,
    int(os.getenv("SPORTS_MAX_EVENT_PAGES", "20")),
)
SPORTS_MAX_TRADE_PAGES = max(
    1,
    int(os.getenv("SPORTS_MAX_TRADE_PAGES", "5")),
)
SPORTS_REQUEST_CONCURRENCY = max(
    1,
    int(os.getenv("SPORTS_REQUEST_CONCURRENCY", "6")),
)

SPORT_LABELS = {
    "soccer": "Soccer",
    "football": "Fútbol americano",
    "basketball": "Básquetbol",
    "baseball": "Béisbol",
    "boxing": "Boxeo",
    "mma": "UFC / MMA",
}

SPORT_MILESTONE_TYPES = {
    "soccer": "soccer_tournament_multi_leg",
    "football": "football_game",
    "basketball": "basketball_game",
    "baseball": "baseball_game",
    "boxing": "boxing_match",
    "mma": "mma_match",
}
SPORTS_EVENT_LOOKBACK_HOURS = max(
    1,
    int(os.getenv("SPORTS_EVENT_LOOKBACK_HOURS", "6")),
)
SPORTS_EVENT_HORIZON_DAYS = max(
    1,
    int(os.getenv("SPORTS_EVENT_HORIZON_DAYS", "7")),
)
SPORTS_COMBAT_HORIZON_DAYS = max(
    SPORTS_EVENT_HORIZON_DAYS,
    int(os.getenv("SPORTS_COMBAT_HORIZON_DAYS", "60")),
)

SPORTS_FLOW_LOCK = asyncio.Lock()
SPORTS_FLOW_TASK = None
SPORTS_FLOW_STATE = {
    "markets": {},
    "flows": {},
    "last_discovery": None,
    "last_poll": None,
    "last_success": None,
    "error": None,
    "discovery_error": None,
}


def parse_api_datetime(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _first_datetime(item, *keys):
    for key in keys:
        dt = parse_api_datetime(item.get(key))
        if dt is not None:
            return dt
    return None


def _float_value(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def db():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS whale_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            source TEXT NOT NULL,
            external_id TEXT,
            btc REAL NOT NULL,
            direction TEXT NOT NULL,
            raw TEXT
        )
    """)

    cols = {r["name"] for r in con.execute("PRAGMA table_info(whale_events)").fetchall()}
    if "external_id" not in cols:
        con.execute("ALTER TABLE whale_events ADD COLUMN external_id TEXT")

    con.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_whale_source_external
        ON whale_events(source, external_id)
        WHERE external_id IS NOT NULL
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_whale_ts ON whale_events(ts)")
    con.commit()
    return con


def is_exchange(text: str) -> bool:
    t = (text or "").lower().replace("#", "")
    return any(name in t for name in EXCHANGES)


def clean_endpoint(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    # WhaleBot adds balance/age text after the receiver. Stop before it.
    text = re.split(
        r"\b(?:\d+\s+(?:seconds?|minutes?|hours?)\s+ago|a minute ago|sender['’]s balance|receiver['’]s balance|blockchain:)\b",
        text,
        maxsplit=1,
        flags=re.I,
    )[0]
    return text.strip(" .,-")


def classify_transfer(raw: str):
    """
    inflow  = entra BTC a un exchange  -> suma
    outflow = sale BTC de un exchange -> resta
    neutral = exchange->exchange, unknown->unknown, o no clasificable
    """
    low = (raw or "").lower().replace("➡️", " to ").replace("→", " to ")

    amount = None
    m = re.search(r"([\d,]+(?:\.\d+)?)\s*[$#]?\s*btc\b", low, flags=re.I)
    if m:
        try:
            amount = float(m.group(1).replace(",", ""))
        except ValueError:
            amount = None

    if amount is None:
        return None, "neutral"

    # Works for both "transferred" and the WhaleBot typo "transfered".
    m2 = re.search(r"\bfrom\s+(.+?)\s+\bto\s+(.+)$", low, flags=re.I | re.S)
    if not m2:
        return amount, "neutral"

    src = clean_endpoint(m2.group(1))
    dst = clean_endpoint(m2.group(2))
    src_ex = is_exchange(src)
    dst_ex = is_exchange(dst)

    if (not src_ex) and dst_ex:
        return amount, "inflow"
    if src_ex and (not dst_ex):
        return amount, "outflow"
    return amount, "neutral"


class WhaleEvent(BaseModel):
    btc: float
    direction: str
    source: str = "manual"
    raw: str = ""


class ParseEvent(BaseModel):
    raw: str
    source: str = "manual_parse"


def whale_windows_ny(now_ny: datetime | None = None):
    now_ny = now_ny or datetime.now(NY)
    today_0930 = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    today_1600 = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)

    if now_ny >= today_1600:
        overnight_start = today_1600
        overnight_end = now_ny
        overnight_active = True
        regular_start = today_0930
        regular_end = today_1600
        regular_active = False
    elif now_ny >= today_0930:
        overnight_start = today_1600 - timedelta(days=1)
        overnight_end = today_0930
        overnight_active = False
        regular_start = today_0930
        regular_end = now_ny
        regular_active = True
    else:
        overnight_start = today_1600 - timedelta(days=1)
        overnight_end = now_ny
        overnight_active = True
        regular_start = today_0930 - timedelta(days=1)
        regular_end = today_1600 - timedelta(days=1)
        regular_active = False

    return {
        "overnight": {
            "label": "4:00 PM → 9:30 AM",
            "start": overnight_start,
            "end": overnight_end,
            "active": overnight_active,
        },
        "regular": {
            "label": "9:30 AM → 4:00 PM",
            "start": regular_start,
            "end": regular_end,
            "active": regular_active,
        },
    }


def backfill_cutoff_ny(now_ny: datetime | None = None) -> datetime:
    windows = whale_windows_ny(now_ny)
    return min(w["start"] for w in windows.values())


def summarize_whale_rows(rows, window):
    inflow = sum(r["btc"] for r in rows if r["direction"] == "inflow")
    outflow = sum(r["btc"] for r in rows if r["direction"] == "outflow")
    neutral_count = sum(1 for r in rows if r["direction"] == "neutral")
    net = inflow - outflow

    if net < 0:
        signal = "bullish"
        color = "green"
    elif net > 0:
        signal = "bearish"
        color = "red"
    else:
        signal = "neutral"
        color = "gray"

    return {
        "label": window["label"],
        "active": window["active"],
        "status": "EN VIVO" if window["active"] else "CERRADO",
        "window_start": window["start"].isoformat(),
        "window_end": window["end"].isoformat(),
        "inflow_btc": round(inflow, 3),
        "outflow_btc": round(outflow, 3),
        "net_btc": round(net, 3),
        "signal": signal,
        "color": color,
        "count": len(rows),
        "neutral_count": neutral_count,
        "last_update": rows[0]["ts"] if rows else None,
        "events": [dict(r) for r in rows[:20]],
    }


def insert_whale(ts: datetime, source: str, external_id: str | None,
                 btc: float, direction: str, raw: str):
    con = db()
    try:
        con.execute(
            """
            INSERT OR IGNORE INTO whale_events(ts,source,external_id,btc,direction,raw)
            VALUES(?,?,?,?,?,?)
            """,
            (
                ts.astimezone(timezone.utc).isoformat(),
                source,
                external_id,
                btc,
                direction,
                (raw or "")[:4000],
            ),
        )
        con.commit()
    finally:
        con.close()


async def fetch_public_channel_page(
    client: httpx.AsyncClient,
    channel: str,
    before: int | None = None,
):
    url = PUBLIC_TELEGRAM_CHANNELS[channel]
    params = {"before": str(before)} if before else None
    r = await client.get(url, params=params, timeout=20)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    posts = []

    for msg in soup.select(".tgme_widget_message"):
        data_post = msg.get("data-post") or ""
        if "/" not in data_post:
            continue

        try:
            msg_id = int(data_post.rsplit("/", 1)[1])
        except Exception:
            continue

        text_el = msg.select_one(".tgme_widget_message_text")
        if not text_el:
            continue

        raw = text_el.get_text(" ", strip=True)
        time_el = msg.select_one("time")
        dt = None

        if time_el and time_el.get("datetime"):
            try:
                dt = datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00"))
            except Exception:
                dt = None

        if dt is None:
            dt = datetime.now(timezone.utc)
        elif dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        posts.append({
            "id": msg_id,
            "ts": dt.astimezone(timezone.utc),
            "raw": raw,
        })

    posts.sort(key=lambda x: x["id"])
    return posts


async def sync_public_channels(backfill: bool = False):
    cutoff = backfill_cutoff_ny().astimezone(timezone.utc)
    pages = 12 if backfill else 1

    async with httpx.AsyncClient(
        headers=HTTP_HEADERS,
        follow_redirects=True,
    ) as client:
        for channel in PUBLIC_TELEGRAM_CHANNELS:
            before = None
            for _ in range(pages):
                try:
                    posts = await fetch_public_channel_page(client, channel, before)
                except Exception as e:
                    print(f"[Telegram público] Error {channel}: {type(e).__name__}: {e}", flush=True)
                    break

                if not posts:
                    print(f"[Telegram público] {channel}: 0 mensajes encontrados", flush=True)
                    break

                inserted_candidates = 0
                for post in posts:
                    if post["ts"] < cutoff:
                        continue
                    btc, direction = classify_transfer(post["raw"])
                    if btc is None:
                        continue
                    inserted_candidates += 1
                    insert_whale(
                        post["ts"],
                        channel,
                        f"{channel}/{post['id']}",
                        btc,
                        direction,
                        post["raw"],
                    )

                print(
                    f"[Telegram público] {channel}: {len(posts)} mensajes, "
                    f"{inserted_candidates} BTC candidatos",
                    flush=True,
                )

                if not backfill:
                    break

                oldest = min(p["ts"] for p in posts)
                min_id = min(p["id"] for p in posts)
                if oldest <= cutoff:
                    break
                before = min_id
                await asyncio.sleep(0.35)


async def public_telegram_monitor():
    try:
        await sync_public_channels(backfill=True)
    except Exception as e:
        print("[Telegram público] Error de backfill:", repr(e), flush=True)

    while True:
        try:
            await sync_public_channels(backfill=False)
        except Exception as e:
            print("[Telegram público] Error de monitor:", repr(e), flush=True)
        await asyncio.sleep(45)


async def kalshi_get_json(client: httpx.AsyncClient, path: str, params=None):
    """Read one public Kalshi REST endpoint, with a small 429 retry."""
    url = f"{KALSHI_API_BASE}/{path.lstrip('/')}"
    for attempt in range(2):
        response = await client.get(url, params=params, timeout=15)
        if response.status_code != 429 or attempt == 1:
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Kalshi devolvió una respuesta inesperada")
            return payload

        retry_after = _float_value(response.headers.get("Retry-After"), 1.0)
        await asyncio.sleep(min(max(retry_after, 0.25), 2.0))

    raise RuntimeError("Kalshi no respondió")


def normalize_kalshi_market(market):
    opened = _first_datetime(market, "open_time")
    closes = _first_datetime(
        market,
        "close_time",
        "expiration_time",
        "expected_expiration_time",
    )
    if opened is None and closes is not None:
        opened = closes - timedelta(minutes=15)

    return {
        "ticker": market.get("ticker"),
        "event_ticker": market.get("event_ticker"),
        "series_ticker": market.get("series_ticker") or KALSHI_SERIES_TICKER,
        "title": market.get("title") or "BTC 15 min",
        "subtitle": market.get("subtitle") or "",
        "open_time": opened.isoformat() if opened else None,
        "close_time": closes.isoformat() if closes else None,
        "target_price": market.get("floor_strike"),
        "status": market.get("status") or "open",
    }


async def fetch_active_kalshi_market(client: httpx.AsyncClient):
    """Find the live BTC 15-minute market without hard-coding its rotating ticker."""
    now = datetime.now(timezone.utc)
    cursor = None
    candidates = []

    for _ in range(5):
        params = {
            "series_ticker": KALSHI_SERIES_TICKER,
            "status": "open",
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor

        payload = await kalshi_get_json(client, "/markets", params=params)
        for raw_market in payload.get("markets") or []:
            market = normalize_kalshi_market(raw_market)
            if not market["ticker"]:
                continue
            opened = parse_api_datetime(market["open_time"])
            closes = parse_api_datetime(market["close_time"])
            if opened and opened > now + timedelta(seconds=5):
                continue
            if closes and closes <= now:
                continue
            candidates.append(market)

        cursor = payload.get("cursor") or None
        if not cursor:
            break

    if not candidates:
        return None

    def sort_key(item):
        closes = parse_api_datetime(item["close_time"])
        return closes or datetime.max.replace(tzinfo=timezone.utc)

    return min(candidates, key=sort_key)


def normalize_kalshi_trade(raw_trade, market):
    side = str(
        raw_trade.get("taker_outcome_side")
        or raw_trade.get("taker_side")
        or ""
    ).lower()
    if side not in {"yes", "no"}:
        return None

    count = _float_value(raw_trade.get("count_fp"), None)
    if count is None:
        count = _float_value(raw_trade.get("count"), 0.0)
    if count <= 0:
        return None

    price = _float_value(raw_trade.get(f"{side}_price_dollars"), None)
    if price is None:
        legacy_price = _float_value(raw_trade.get(f"{side}_price"), None)
        price = legacy_price / 100.0 if legacy_price is not None else None
    if price is None or price < 0:
        return None

    created = parse_api_datetime(raw_trade.get("created_time"))
    opened = parse_api_datetime(market.get("open_time"))
    closes = parse_api_datetime(market.get("close_time"))
    if created and opened and created < opened:
        return None
    if created and closes and created > closes + timedelta(seconds=2):
        return None

    trade_id = raw_trade.get("trade_id")
    if not trade_id:
        trade_id = "|".join([
            str(raw_trade.get("ticker") or market.get("ticker") or ""),
            str(raw_trade.get("created_time") or ""),
            side,
            str(count),
            str(price),
        ])

    return {
        "trade_id": str(trade_id),
        "side": side,
        "count": count,
        "price": price,
        "dollars": count * price,
        "created_time": created.isoformat() if created else None,
        "is_block_trade": bool(raw_trade.get("is_block_trade")),
    }


async def fetch_kalshi_trade_updates(client: httpx.AsyncClient, market, seen_ids):
    """Backfill a new contract, then stop pagination once a known trade appears."""
    cursor = None
    new_trades = []
    truncated = False
    opened = parse_api_datetime(market.get("open_time"))
    min_ts = int(opened.timestamp()) - 1 if opened else None

    for page_number in range(KALSHI_MAX_TRADE_PAGES):
        params = {"ticker": market["ticker"], "limit": 1000}
        if min_ts is not None:
            params["min_ts"] = min_ts
        if cursor:
            params["cursor"] = cursor

        payload = await kalshi_get_json(client, "/markets/trades", params=params)
        page = payload.get("trades") or []
        reached_known_trade = False

        for raw_trade in page:
            raw_id = raw_trade.get("trade_id")
            if raw_id is not None and str(raw_id) in seen_ids:
                reached_known_trade = True
                continue
            trade = normalize_kalshi_trade(raw_trade, market)
            if trade and trade["trade_id"] not in seen_ids:
                new_trades.append(trade)

        cursor = payload.get("cursor") or None
        if reached_known_trade or not cursor:
            break

        if page_number + 1 >= KALSHI_MAX_TRADE_PAGES:
            truncated = True
            break
        await asyncio.sleep(0.06)

    return new_trades, truncated


def kalshi_flow_payload(now=None):
    now = now or datetime.now(timezone.utc)
    market = KALSHI_FLOW_STATE["market"]
    trades = list(KALSHI_FLOW_STATE["trades"].values())

    yes_dollars = sum(t["dollars"] for t in trades if t["side"] == "yes")
    no_dollars = sum(t["dollars"] for t in trades if t["side"] == "no")
    yes_contracts = sum(t["count"] for t in trades if t["side"] == "yes")
    no_contracts = sum(t["count"] for t in trades if t["side"] == "no")
    total_dollars = yes_dollars + no_dollars

    yes_pct = (yes_dollars / total_dollars * 100.0) if total_dollars else 0.0
    no_pct = (no_dollars / total_dollars * 100.0) if total_dollars else 0.0
    if yes_dollars > no_dollars:
        dominant = "yes"
    elif no_dollars > yes_dollars:
        dominant = "no"
    else:
        dominant = "neutral"

    closes = parse_api_datetime(market.get("close_time")) if market else None
    remaining = max(0, int((closes - now).total_seconds())) if closes else None
    last_trade = max(
        (t["created_time"] for t in trades if t.get("created_time")),
        default=None,
    )
    last_success = KALSHI_FLOW_STATE["last_success"]
    stale_seconds = (
        max(0.0, (now - last_success).total_seconds())
        if last_success is not None
        else None
    )

    return {
        "as_of": now.isoformat(),
        "source": "Kalshi public trades REST",
        "series_ticker": KALSHI_SERIES_TICKER,
        "poll_interval_seconds": KALSHI_FLOW_POLL_SECONDS,
        "status": "EN VIVO" if market and (closes is None or closes > now) else "BUSCANDO",
        "market": market,
        "remaining_seconds": remaining,
        "yes_dollars": round(yes_dollars, 2),
        "no_dollars": round(no_dollars, 2),
        "total_dollars": round(total_dollars, 2),
        "yes_pct": round(yes_pct, 1),
        "no_pct": round(no_pct, 1),
        "dominant": dominant,
        "trade_count": len(trades),
        "yes_contracts": round(yes_contracts, 2),
        "no_contracts": round(no_contracts, 2),
        "block_trade_count": sum(1 for t in trades if t["is_block_trade"]),
        "last_trade": last_trade,
        "last_success": last_success.isoformat() if last_success else None,
        "stale_seconds": round(stale_seconds, 1) if stale_seconds is not None else None,
        "page_limit_reached": bool(KALSHI_FLOW_STATE["truncated"]),
        "error": KALSHI_FLOW_STATE["error"],
        "definition": "Dólares del lado que tomó liquidez: contratos por precio pagado.",
    }


async def refresh_kalshi_flow():
    async with KALSHI_FLOW_LOCK:
        now = datetime.now(timezone.utc)
        KALSHI_FLOW_STATE["last_poll"] = now
        errors = []
        any_success = False

        async with httpx.AsyncClient(
            headers={**HTTP_HEADERS, "Accept": "application/json"},
            follow_redirects=True,
        ) as client:
            current_market = KALSHI_FLOW_STATE["market"]
            current_close = (
                parse_api_datetime(current_market.get("close_time"))
                if current_market
                else None
            )
            last_market_check = KALSHI_FLOW_STATE["last_market_check"]
            needs_market_check = (
                current_market is None
                or last_market_check is None
                or (now - last_market_check).total_seconds() >= KALSHI_MARKET_REFRESH_SECONDS
                or (current_close is not None and current_close <= now + timedelta(seconds=1))
            )

            if needs_market_check:
                try:
                    found_market = await fetch_active_kalshi_market(client)
                    KALSHI_FLOW_STATE["last_market_check"] = now
                    any_success = True

                    if found_market:
                        if (
                            current_market is None
                            or current_market.get("ticker") != found_market.get("ticker")
                        ):
                            KALSHI_FLOW_STATE["trades"] = {}
                            KALSHI_FLOW_STATE["truncated"] = False
                        KALSHI_FLOW_STATE["market"] = found_market
                    elif current_market is None or (
                        current_close is not None and current_close <= now
                    ):
                        KALSHI_FLOW_STATE["market"] = None
                        KALSHI_FLOW_STATE["trades"] = {}
                except Exception as exc:
                    errors.append(f"mercado: {type(exc).__name__}: {exc}")

            market = KALSHI_FLOW_STATE["market"]
            closes = parse_api_datetime(market.get("close_time")) if market else None
            if market and (closes is None or closes > now):
                try:
                    seen_ids = set(KALSHI_FLOW_STATE["trades"])
                    updates, truncated = await fetch_kalshi_trade_updates(
                        client,
                        market,
                        seen_ids,
                    )
                    for trade in updates:
                        KALSHI_FLOW_STATE["trades"][trade["trade_id"]] = trade
                    KALSHI_FLOW_STATE["truncated"] = (
                        KALSHI_FLOW_STATE["truncated"] or truncated
                    )
                    any_success = True
                except Exception as exc:
                    errors.append(f"operaciones: {type(exc).__name__}: {exc}")

        if any_success:
            KALSHI_FLOW_STATE["last_success"] = datetime.now(timezone.utc)
        KALSHI_FLOW_STATE["error"] = " | ".join(errors) or None
        return kalshi_flow_payload()


async def kalshi_flow_monitor():
    while True:
        try:
            await refresh_kalshi_flow()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            KALSHI_FLOW_STATE["error"] = f"monitor: {type(exc).__name__}: {exc}"
            print("[Kalshi flujo]", KALSHI_FLOW_STATE["error"], flush=True)
        await asyncio.sleep(KALSHI_FLOW_POLL_SECONDS)


def _sports_search_text(*values):
    return " ".join(str(value or "") for value in values).lower()


def classify_requested_sport(item):
    """Return one of the user-selected sports, or None for every other sport."""
    text = _sports_search_text(
        item.get("category"),
        item.get("type"),
        item.get("ticker"),
        item.get("series_ticker"),
        item.get("event_ticker"),
        item.get("title"),
        item.get("sub_title"),
        item.get("tags"),
        item.get("competition"),
        item.get("notification_message"),
        item.get("details"),
        item.get("product_metadata"),
        item.get("source_id"),
        item.get("source_ids"),
    )

    mma_words = (
        "mma_match", "kxmmap", "kamma", " ufc", "ufc ", "mma", "mixed martial",
        "fight night", "bellator", "professional fighters league", " pfl ",
        "one championship",
    )
    if any(word in text for word in mma_words):
        return "mma"

    boxing_words = (
        "boxing_match", "boxing", "boxeo", "boxer", "pugilist", "kxboxing",
        "bare knuckle", "bkfc",
    )
    if any(word in text for word in boxing_words):
        return "boxing"

    football_words = (
        "football_game", "american football", "pro football", "professional football",
        "college football", " ncaaf", "ncaaf", " nfl", "nfl ", " cfl", "cfl ",
        " ufl", "ufl ", "super bowl", "touchdown", "gridiron",
    )
    if any(word in text for word in football_words):
        return "football"

    if any(word in text for word in (
        "basketball", "nba", "wnba", "ncaab", "hoops", "euroleague",
    )):
        return "basketball"

    if any(word in text for word in (
        "baseball", "mlb", "college world series", "world baseball classic",
    )):
        return "baseball"

    soccer_words = (
        "soccer", "fútbol", "futbol", "premier league", "champions league",
        "europa league", "conference league", "la liga", "bundesliga",
        "serie a", "ligue 1", "major league soccer", " mls ", "fifa",
        "uefa", "concacaf", "conmebol", "copa libertadores",
        "copa sudamericana", "soccer_tournament", "eredivisie",
        "primeira liga", "liga mx", "nwsl", "women's super league",
        "fa cup", "copa del rey",
    )
    if any(word in text for word in soccer_words):
        return "soccer"

    return None


def _milestone_datetime(item):
    return _first_datetime(item, "start_date", "end_date") or datetime.max.replace(
        tzinfo=timezone.utc
    )


async def fetch_requested_sports_milestones(client: httpx.AsyncClient):
    """Discover each requested sport separately so busy sports cannot hide others."""
    now = datetime.now(timezone.utc)
    minimum_start = (
        now - timedelta(hours=SPORTS_EVENT_LOOKBACK_HOURS)
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    async def fetch_one_type(sport, milestone_type):
        cursor = None
        found = []
        horizon_days = (
            SPORTS_COMBAT_HORIZON_DAYS
            if sport in {"boxing", "mma"}
            else SPORTS_EVENT_HORIZON_DAYS
        )
        maximum_start = now + timedelta(days=horizon_days)

        for _ in range(SPORTS_MAX_MILESTONE_PAGES):
            params = {
                "limit": 500,
                "category": "Sports",
                "type": milestone_type,
                "minimum_start_date": minimum_start,
            }
            if cursor:
                params["cursor"] = cursor

            payload = await kalshi_get_json(client, "/milestones", params=params)
            for raw in payload.get("milestones") or []:
                starts = _first_datetime(raw, "start_date", "end_date")
                if starts is not None and starts > maximum_start:
                    continue
                item = dict(raw)
                item["sport"] = sport
                found.append(item)

            cursor = payload.get("cursor") or None
            if not cursor:
                break
        return found

    batches = await asyncio.gather(*(
        fetch_one_type(sport, milestone_type)
        for sport, milestone_type in SPORT_MILESTONE_TYPES.items()
    ))
    unique = {}
    for batch in batches:
        for item in batch:
            key = str(item.get("id") or "").strip() or "|".join([
                str(item.get("type") or ""),
                str(item.get("start_date") or ""),
                str(item.get("title") or ""),
            ])
            unique[key] = item
    return sorted(unique.values(), key=_milestone_datetime)


def _series_ticker_from_event(event_ticker):
    return str(event_ticker or "").split("-", 1)[0].strip()


def _is_primary_game_series(series_meta, series_ticker):
    meta = series_meta or {}
    scope = str(meta.get("series_scope") or "").strip().lower()
    title = str(meta.get("series_title") or "").strip().lower()
    if scope in {"game", "match"}:
        return True
    if scope and any(word in scope for word in (
        "future", "season", "award", "draft", "champion", "special",
        "prop", "spread", "total", "score", "round", "knockout",
    )):
        return False
    if not scope and any(word in title for word in (
        " game", "game ", " match", "fight winner", "mma fight",
    )):
        return True
    ticker = str(series_ticker or "").upper()
    return (
        ("GAME" in ticker or "FIGHT" in ticker or ticker == "KXBOXING")
        and not any(word in ticker for word in (
            "SPECIAL", "TOTAL", "SPREAD", "SCORE", "TD", "GOAL", "ROUND",
        ))
    )


def build_sports_event_index(milestones, series_index):
    """Map primary event tickers to sport metadata, keeping nearby games first."""
    by_sport = {sport: [] for sport in SPORT_LABELS}
    for milestone in milestones:
        sport = milestone.get("sport")
        if sport in by_sport:
            by_sport[sport].append(milestone)

    event_index = {}
    for sport, items in by_sport.items():
        sport_tickers = []
        for milestone in sorted(items, key=_milestone_datetime):
            tickers = (
                milestone.get("primary_event_tickers")
                or milestone.get("related_event_tickers")
                or []
            )
            for ticker in tickers:
                ticker = str(ticker or "").strip()
                if not ticker or ticker in event_index or ticker in sport_tickers:
                    continue
                series_ticker = _series_ticker_from_event(ticker)
                series_meta = series_index.get(series_ticker) or {}
                if series_meta.get("sport") not in {None, sport}:
                    continue
                if not _is_primary_game_series(series_meta, series_ticker):
                    continue
                sport_tickers.append(ticker)
                event_index[ticker] = {
                    "sport": sport,
                    "sport_label": SPORT_LABELS[sport],
                    "series_scope": series_meta.get("series_scope") or "",
                    "series_title": series_meta.get("series_title") or "",
                    "milestone_id": milestone.get("id"),
                    "milestone_title": milestone.get("title") or "",
                    "start_time": milestone.get("start_date"),
                    "end_time": milestone.get("end_date"),
                }
                if len(sport_tickers) >= SPORTS_EVENT_TICKERS_PER_SPORT:
                    break
            if len(sport_tickers) >= SPORTS_EVENT_TICKERS_PER_SPORT:
                break

    return event_index


async def fetch_open_sports_events(client: httpx.AsyncClient, event_index):
    tickers = list(event_index)
    semaphore = asyncio.Semaphore(SPORTS_REQUEST_CONCURRENCY)

    async def fetch_chunk(chunk):
        async with semaphore:
            payload = await kalshi_get_json(
                client,
                "/events",
                params={
                    "tickers": ",".join(chunk),
                    "status": "open",
                    "with_nested_markets": True,
                    "limit": 200,
                },
            )
        found = []
        for raw_event in payload.get("events") or []:
            ticker = str(raw_event.get("event_ticker") or "")
            metadata = event_index.get(ticker)
            if not metadata:
                continue
            event = dict(raw_event)
            event["sport_metadata"] = metadata
            found.append(event)
        return found

    batches = await asyncio.gather(*(
        fetch_chunk(tickers[start:start + 20])
        for start in range(0, len(tickers), 20)
    ))
    return [event for batch in batches for event in batch]


async def fetch_requested_sports_series(client: httpx.AsyncClient):
    """Build a sport lookup from Kalshi's complete public Sports series list."""
    payload = await kalshi_get_json(
        client,
        "/series",
        params={
            "category": "Sports",
            "include_product_metadata": True,
        },
    )
    series_index = {}
    for raw_series in payload.get("series") or []:
        sport = classify_requested_sport(raw_series)
        ticker = str(raw_series.get("ticker") or "").strip()
        if not sport or not ticker:
            continue
        product_metadata = raw_series.get("product_metadata") or {}
        series_index[ticker] = {
            "sport": sport,
            "sport_label": SPORT_LABELS[sport],
            "series_title": raw_series.get("title") or "",
            "series_scope": product_metadata.get("scope") or "",
        }
    return series_index


def _event_start_time(event):
    direct = _first_datetime(event, "strike_date")
    if direct is not None:
        return direct.isoformat()
    possible = []
    for market in event.get("markets") or []:
        value = _first_datetime(
            market,
            "occurrence_datetime",
            "open_time",
            "close_time",
        )
        if value is not None:
            possible.append(value)
    return min(possible).isoformat() if possible else None


async def fetch_open_sports_catalog(client: httpx.AsyncClient):
    """Scan open events directly; this is more reliable than milestone-only discovery."""
    series_index = await fetch_requested_sports_series(client)
    events = {}
    cursor = None

    for _ in range(SPORTS_MAX_EVENT_PAGES):
        params = {
            "status": "open",
            "with_nested_markets": True,
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor
        payload = await kalshi_get_json(client, "/events", params=params)

        for raw_event in payload.get("events") or []:
            event_ticker = str(raw_event.get("event_ticker") or "").strip()
            if not event_ticker:
                continue
            series_ticker = str(raw_event.get("series_ticker") or "").strip()
            series_meta = series_index.get(series_ticker)
            sport = series_meta.get("sport") if series_meta else None

            if not sport:
                probe = dict(raw_event)
                probe["details"] = [
                    {
                        "ticker": market.get("ticker"),
                        "title": market.get("title"),
                        "subtitle": market.get("subtitle"),
                        "yes_sub_title": market.get("yes_sub_title"),
                        "no_sub_title": market.get("no_sub_title"),
                    }
                    for market in (raw_event.get("markets") or [])[:6]
                ]
                sport = classify_requested_sport(probe)
            if sport not in SPORT_LABELS:
                continue

            event = dict(raw_event)
            event["sport_metadata"] = {
                "sport": sport,
                "sport_label": SPORT_LABELS[sport],
                "series_scope": (series_meta or {}).get("series_scope") or "",
                "series_title": (series_meta or {}).get("series_title") or "",
                "milestone_id": None,
                "milestone_title": (
                    (series_meta or {}).get("series_title")
                    or raw_event.get("title")
                    or ""
                ),
                "start_time": _event_start_time(raw_event),
                "end_time": None,
            }
            events[event_ticker] = event

        cursor = payload.get("cursor") or None
        if not cursor:
            break

    return list(events.values())


def _kalshi_market_price(market, side, quote):
    value = _float_value(market.get(f"{side}_{quote}_dollars"), None)
    if value is not None:
        return value
    cents = _float_value(market.get(f"{side}_{quote}"), None)
    return cents / 100.0 if cents is not None else None


def _sports_market_priority(market):
    text = _sports_search_text(
        market.get("title"),
        market.get("subtitle"),
        market.get("yes_sub_title"),
        market.get("no_sub_title"),
    )
    main_words = (" win", "winner", "moneyline", "victoria", "advance", "qualify")
    prop_words = (
        "points", "goals", "runs", "rebounds", "assists", "strikeouts",
        "total", "spread", "margin", "first score", "first goal", "player",
    )
    primary = 1 if market.get("primary_participant_key") else 0
    if any(word in text for word in main_words):
        primary += 1
    if any(word in text for word in prop_words):
        primary -= 1
    return primary


def normalize_sports_market(event, raw_market):
    ticker = str(raw_market.get("ticker") or "").strip()
    if not ticker:
        return None

    now = datetime.now(timezone.utc)
    closes = _first_datetime(
        raw_market,
        "close_time",
        "expiration_time",
        "expected_expiration_time",
    )
    status = str(raw_market.get("status") or "open").lower()
    if status in {"closed", "settled", "finalized"}:
        return None
    if closes is not None and closes <= now:
        return None

    metadata = event.get("sport_metadata") or {}
    sport = metadata.get("sport")
    series_scope = str(metadata.get("series_scope") or "").strip()
    scope_text = series_scope.lower()
    if any(word in scope_text for word in (
        "future", "season", "award", "draft", "champion", "league leader",
        "playoff seed", "next team", "division winner", "conference winner",
    )):
        return None

    event_start_value = (
        metadata.get("start_time")
        or raw_market.get("occurrence_datetime")
        or event.get("strike_date")
    )
    event_start_dt = parse_api_datetime(event_start_value)
    horizon_days = (
        SPORTS_COMBAT_HORIZON_DAYS
        if sport in {"boxing", "mma"}
        else SPORTS_EVENT_HORIZON_DAYS
    )
    maximum_time = now + timedelta(days=horizon_days)
    if event_start_dt is not None and event_start_dt > maximum_time:
        return None
    if event_start_dt is None and closes is not None and closes > maximum_time:
        return None

    volume = _float_value(raw_market.get("volume_fp"), 0.0)
    volume_24h = _float_value(raw_market.get("volume_24h_fp"), 0.0)
    open_interest = _float_value(raw_market.get("open_interest_fp"), 0.0)
    live_end = closes
    if event_start_dt is not None:
        expected_end = event_start_dt + timedelta(hours=8)
        live_end = min(live_end, expected_end) if live_end else expected_end
    is_live = bool(
        event_start_dt is not None
        and event_start_dt - timedelta(minutes=20) <= now
        and (live_end is None or now <= live_end)
    )
    has_volume = volume > 0 or volume_24h > 0 or open_interest > 0
    if not has_volume and not is_live:
        return None

    yes_label = (
        raw_market.get("yes_sub_title")
        or raw_market.get("title")
        or "SÍ"
    )
    no_label = raw_market.get("no_sub_title") or "NO"
    market_title = (
        raw_market.get("title")
        or raw_market.get("subtitle")
        or str(yes_label)
    )
    event_title = (
        event.get("title")
        or event.get("sub_title")
        or metadata.get("milestone_title")
        or "Evento deportivo"
    )

    return {
        "ticker": ticker,
        "event_ticker": event.get("event_ticker"),
        "series_ticker": event.get("series_ticker"),
        "sport": sport,
        "sport_label": metadata.get("sport_label"),
        "series_scope": series_scope,
        "event_title": event_title,
        "market_title": market_title,
        "yes_label": str(yes_label),
        "no_label": str(no_label),
        "yes_bid": _kalshi_market_price(raw_market, "yes", "bid"),
        "yes_ask": _kalshi_market_price(raw_market, "yes", "ask"),
        "no_bid": _kalshi_market_price(raw_market, "no", "bid"),
        "no_ask": _kalshi_market_price(raw_market, "no", "ask"),
        "last_price": _float_value(raw_market.get("last_price_dollars"), None),
        "volume": volume,
        "volume_24h": volume_24h,
        "open_interest": open_interest,
        "open_time": raw_market.get("open_time"),
        "close_time": closes.isoformat() if closes else None,
        "event_start": event_start_dt.isoformat() if event_start_dt else None,
        "is_live": is_live,
        "has_volume": has_volume,
        "milestone_title": metadata.get("milestone_title") or "",
        "priority": _sports_market_priority(raw_market),
        "status": status,
    }


def select_sports_markets(events):
    candidates = []
    for event in events:
        event_markets = []
        for raw_market in event.get("markets") or []:
            market = normalize_sports_market(event, raw_market)
            if market and market.get("sport") in SPORT_LABELS:
                event_markets.append(market)
        event_markets.sort(
            key=lambda item: (
                1 if item.get("is_live") else 0,
                item.get("volume_24h", 0),
                item.get("volume", 0),
                item.get("priority", 0),
            ),
            reverse=True,
        )
        candidates.extend(event_markets[:SPORTS_MARKETS_PER_EVENT])

    by_sport = {sport: [] for sport in SPORT_LABELS}
    for market in candidates:
        by_sport[market["sport"]].append(market)
    for items in by_sport.values():
        items.sort(
            key=lambda item: (
                1 if item.get("is_live") else 0,
                item.get("volume_24h", 0),
                item.get("volume", 0),
                item.get("priority", 0),
            ),
            reverse=True,
        )

    selected = []
    selected_tickers = set()
    for sport in SPORT_LABELS:
        for market in by_sport[sport][:SPORTS_MARKETS_PER_SPORT]:
            selected.append(market)
            selected_tickers.add(market["ticker"])

    remaining = [
        market for market in candidates
        if market["ticker"] not in selected_tickers
    ]
    remaining.sort(
        key=lambda item: (
            1 if item.get("is_live") else 0,
            item.get("volume_24h", 0),
            item.get("volume", 0),
        ),
        reverse=True,
    )
    for market in remaining:
        if len(selected) >= SPORTS_MAX_MARKETS:
            break
        selected.append(market)
        selected_tickers.add(market["ticker"])

    return selected[:SPORTS_MAX_MARKETS]


async def fetch_one_sports_flow(client, market, cutoff, semaphore):
    trades = {}
    cursor = None
    truncated = False
    error = None

    try:
        async with semaphore:
            for page_number in range(SPORTS_MAX_TRADE_PAGES):
                params = {
                    "ticker": market["ticker"],
                    "limit": 1000,
                    "min_ts": int(cutoff.timestamp()),
                }
                if cursor:
                    params["cursor"] = cursor
                payload = await kalshi_get_json(
                    client,
                    "/markets/trades",
                    params=params,
                )
                for raw_trade in payload.get("trades") or []:
                    trade = normalize_kalshi_trade(raw_trade, market)
                    if not trade:
                        continue
                    created = parse_api_datetime(trade.get("created_time"))
                    if created is not None and created < cutoff:
                        continue
                    trades[trade["trade_id"]] = trade

                cursor = payload.get("cursor") or None
                if not cursor:
                    break
                if page_number + 1 >= SPORTS_MAX_TRADE_PAGES:
                    truncated = True
                    break
                await asyncio.sleep(0.03)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    values = list(trades.values())
    yes_dollars = sum(t["dollars"] for t in values if t["side"] == "yes")
    no_dollars = sum(t["dollars"] for t in values if t["side"] == "no")
    total_dollars = yes_dollars + no_dollars
    yes_pct = yes_dollars / total_dollars * 100.0 if total_dollars else 0.0
    no_pct = no_dollars / total_dollars * 100.0 if total_dollars else 0.0
    if yes_dollars > no_dollars:
        dominant = "yes"
    elif no_dollars > yes_dollars:
        dominant = "no"
    else:
        dominant = "neutral"

    largest_trade = max((t["dollars"] for t in values), default=0.0)
    last_trade = max(
        (t["created_time"] for t in values if t.get("created_time")),
        default=None,
    )
    return market["ticker"], {
        "yes_dollars": round(yes_dollars, 2),
        "no_dollars": round(no_dollars, 2),
        "total_dollars": round(total_dollars, 2),
        "yes_pct": round(yes_pct, 1),
        "no_pct": round(no_pct, 1),
        "dominant": dominant,
        "trade_count": len(values),
        "block_trade_count": sum(1 for t in values if t["is_block_trade"]),
        "large_trade_count": sum(1 for t in values if t["dollars"] >= 10000),
        "largest_trade_dollars": round(largest_trade, 2),
        "last_trade": last_trade,
        "page_limit_reached": truncated,
        "error": error,
    }


def sports_flow_payload(now=None):
    now = now or datetime.now(timezone.utc)
    items = []
    for ticker, market in SPORTS_FLOW_STATE["markets"].items():
        flow = SPORTS_FLOW_STATE["flows"].get(ticker) or {
            "yes_dollars": 0.0,
            "no_dollars": 0.0,
            "total_dollars": 0.0,
            "yes_pct": 0.0,
            "no_pct": 0.0,
            "dominant": "neutral",
            "trade_count": 0,
            "block_trade_count": 0,
            "large_trade_count": 0,
            "largest_trade_dollars": 0.0,
            "last_trade": None,
            "page_limit_reached": False,
            "error": None,
        }
        items.append({**market, **flow})

    items.sort(
        key=lambda item: (
            item.get("total_dollars", 0),
            item.get("volume_24h", 0),
            item.get("volume", 0),
        ),
        reverse=True,
    )
    last_success = SPORTS_FLOW_STATE["last_success"]
    stale_seconds = (
        max(0.0, (now - last_success).total_seconds())
        if last_success is not None
        else None
    )
    sport_summaries = []
    for sport, label in SPORT_LABELS.items():
        sport_items = [item for item in items if item.get("sport") == sport]
        sport_summaries.append({
            "key": sport,
            "label": label,
            "market_count": len(sport_items),
            "event_count": len({
                item.get("event_ticker") for item in sport_items
                if item.get("event_ticker")
            }),
            "live_market_count": sum(1 for item in sport_items if item.get("is_live")),
            "flow_dollars": round(sum(
                item.get("total_dollars", 0) for item in sport_items
            ), 2),
            "volume_contracts": round(sum(
                item.get("volume", 0) for item in sport_items
            ), 2),
            "volume_24h_contracts": round(sum(
                item.get("volume_24h", 0) for item in sport_items
            ), 2),
        })

    total_flow = round(sum(item.get("total_dollars", 0) for item in items), 2)
    total_volume = round(sum(item.get("volume", 0) for item in items), 2)
    total_volume_24h = round(sum(item.get("volume_24h", 0) for item in items), 2)

    return {
        "as_of": now.isoformat(),
        "source": "Kalshi public sports events and completed trades",
        "status": "EN VIVO" if items else "BUSCANDO",
        "window_minutes": SPORTS_FLOW_WINDOW_MINUTES,
        "poll_interval_seconds": SPORTS_FLOW_POLL_SECONDS,
        "sports": sport_summaries,
        "event_count": len({item.get("event_ticker") for item in items}),
        "market_count": len(items),
        "live_market_count": sum(1 for item in items if item.get("is_live")),
        "flow_dollars": total_flow,
        "volume_contracts": total_volume,
        "volume_24h_contracts": total_volume_24h,
        "markets": items,
        "last_discovery": (
            SPORTS_FLOW_STATE["last_discovery"].isoformat()
            if SPORTS_FLOW_STATE["last_discovery"]
            else None
        ),
        "last_success": last_success.isoformat() if last_success else None,
        "stale_seconds": round(stale_seconds, 1) if stale_seconds is not None else None,
        "error": SPORTS_FLOW_STATE["error"],
        "discovery_error": SPORTS_FLOW_STATE["discovery_error"],
        "definition": (
            "Flujo agresor ejecutado en la ventana: contratos por precio pagado "
            "en el lado YES o NO. Solo lectura."
        ),
    }


async def refresh_sports_flow():
    async with SPORTS_FLOW_LOCK:
        now = datetime.now(timezone.utc)
        SPORTS_FLOW_STATE["last_poll"] = now
        errors = []
        any_success = False

        async with httpx.AsyncClient(
            headers={**HTTP_HEADERS, "Accept": "application/json"},
            follow_redirects=True,
        ) as client:
            last_discovery = SPORTS_FLOW_STATE["last_discovery"]
            needs_discovery = (
                last_discovery is None
                or (now - last_discovery).total_seconds() >= SPORTS_DISCOVERY_SECONDS
            )
            if needs_discovery:
                SPORTS_FLOW_STATE["last_discovery"] = now
                try:
                    schedule_error = None
                    try:
                        series_index = await fetch_requested_sports_series(client)
                        milestones = await fetch_requested_sports_milestones(client)
                        event_index = build_sports_event_index(
                            milestones,
                            series_index,
                        )
                        events = await fetch_open_sports_events(client, event_index)
                    except Exception as exc:
                        schedule_error = exc
                        events = []

                    # The typed milestone schedule excludes season futures and
                    # keeps football, soccer, boxing and MMA separate. Scan the
                    # broad catalog only as a temporary API fallback.
                    if not events:
                        try:
                            events = await fetch_open_sports_catalog(client)
                        except Exception:
                            if schedule_error is not None:
                                raise schedule_error
                            raise

                    selected = select_sports_markets(events)
                    new_markets = {item["ticker"]: item for item in selected}
                    SPORTS_FLOW_STATE["markets"] = new_markets
                    SPORTS_FLOW_STATE["flows"] = {
                        ticker: flow
                        for ticker, flow in SPORTS_FLOW_STATE["flows"].items()
                        if ticker in new_markets
                    }
                    SPORTS_FLOW_STATE["discovery_error"] = None
                    any_success = True
                except Exception as exc:
                    message = f"descubrimiento: {type(exc).__name__}: {exc}"
                    SPORTS_FLOW_STATE["discovery_error"] = message
                    errors.append(message)

            active_markets = {}
            for ticker, market in SPORTS_FLOW_STATE["markets"].items():
                closes = parse_api_datetime(market.get("close_time"))
                if closes is None or closes > now:
                    active_markets[ticker] = market
            SPORTS_FLOW_STATE["markets"] = active_markets

            markets = list(active_markets.values())
            if markets:
                cutoff = now - timedelta(minutes=SPORTS_FLOW_WINDOW_MINUTES)
                semaphore = asyncio.Semaphore(SPORTS_REQUEST_CONCURRENCY)
                results = await asyncio.gather(*(
                    fetch_one_sports_flow(client, market, cutoff, semaphore)
                    for market in markets
                ))
                for ticker, flow in results:
                    SPORTS_FLOW_STATE["flows"][ticker] = flow
                    if flow.get("error"):
                        errors.append(f"{ticker}: {flow['error']}")
                    else:
                        any_success = True

        if any_success:
            SPORTS_FLOW_STATE["last_success"] = datetime.now(timezone.utc)
        SPORTS_FLOW_STATE["error"] = " | ".join(errors[:5]) or None
        return sports_flow_payload()


async def sports_flow_monitor():
    while True:
        try:
            await refresh_sports_flow()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            SPORTS_FLOW_STATE["error"] = f"monitor: {type(exc).__name__}: {exc}"
            print("[Kalshi deportes]", SPORTS_FLOW_STATE["error"], flush=True)
        await asyncio.sleep(SPORTS_FLOW_POLL_SECONDS)


def market_state(now_ny: datetime):
    if now_ny.weekday() >= 5:
        return "CERRADO"
    hm = now_ny.hour * 60 + now_ny.minute
    if hm < 4 * 60:
        return "CERRADO"
    if hm < 9 * 60 + 30:
        return "PRE-MARKET"
    if hm < 16 * 60:
        return "ABIERTO"
    if hm < 20 * 60:
        return "AFTER-HOURS"
    return "CERRADO"


def _clean_market_number(value):
    """Parse values such as '$123.45', '+0.34', '-0.25%' or 'N/A'."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.upper() in {"N/A", "N/D", "--", "-"}:
        return None
    text = text.replace("$", "").replace("%", "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def _nasdaq_status(value: str | None) -> str:
    value = (value or "").strip().lower()
    if "pre" in value:
        return "PRE-MARKET"
    if "after" in value or "post" in value:
        return "AFTER-HOURS"
    if "open" in value:
        return "ABIERTO"
    if "closed" in value or "close" in value:
        return "CERRADO"
    return market_state(datetime.now(NY))


async def fetch_nasdaq_market(client: httpx.AsyncClient, item):
    """Current/extended-hours ETF quote from Nasdaq's public web endpoint."""
    symbol = item["symbol"]
    url = f"https://api.nasdaq.com/api/quote/{symbol}/info"
    headers = {
        **HTTP_HEADERS,
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.nasdaq.com/market-activity/etf/{symbol.lower()}",
        "Origin": "https://www.nasdaq.com",
    }
    r = await client.get(
        url,
        params={"assetclass": "etf"},
        headers=headers,
        timeout=15,
    )
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data") or {}
    primary = data.get("primaryData") or {}

    price = _clean_market_number(primary.get("lastSalePrice"))
    change = _clean_market_number(primary.get("netChange"))
    change_pct = _clean_market_number(primary.get("percentageChange"))
    if price is None:
        raise ValueError(f"Nasdaq sin precio para {symbol}")

    previous_close = None
    if change is not None:
        previous_close = price - change

    return {
        "symbol": symbol,
        "name": item["name"],
        "price": price,
        "previous_close": previous_close,
        "open": None,
        "change_pct": change_pct,
        "since_open_pct": None,
        "status": _nasdaq_status(data.get("marketStatus")),
        "source": "Nasdaq quote",
        "error": None,
        "last_trade": primary.get("lastTradeTimestamp"),
        "is_real_time": primary.get("isRealTime"),
    }


async def fetch_cboe_market(client: httpx.AsyncClient, item, primary_error: str = ""):
    """No-key delayed fallback for the same ETF ticker."""
    symbol = item["symbol"]
    url = f"https://cdn.cboe.com/api/global/delayed_quotes/quotes/{symbol}.json"
    r = await client.get(
        url,
        headers={**HTTP_HEADERS, "Accept": "application/json"},
        timeout=15,
    )
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data") or {}

    price = _clean_market_number(data.get("current_price"))
    previous_close = _clean_market_number(data.get("prev_day_close"))
    session_open = _clean_market_number(data.get("open"))
    change_pct = _clean_market_number(data.get("price_change_percent"))
    if price is None:
        raise ValueError(f"CBOE sin precio para {symbol}")

    if change_pct is None and previous_close not in (None, 0):
        change_pct = (price - previous_close) / previous_close * 100

    pct_open = None
    if session_open not in (None, 0):
        pct_open = (price - session_open) / session_open * 100

    return {
        "symbol": symbol,
        "name": item["name"],
        "price": price,
        "previous_close": previous_close,
        "open": session_open,
        "change_pct": change_pct,
        "since_open_pct": pct_open,
        "status": market_state(datetime.now(NY)),
        "source": "CBOE delayed fallback",
        "error": None,
        "fallback_reason": primary_error[:300] if primary_error else None,
        "last_trade": data.get("last_trade_time"),
        "is_real_time": False,
    }


async def fetch_one_market(client: httpx.AsyncClient, item):
    nasdaq_error = ""
    try:
        return await fetch_nasdaq_market(client, item)
    except Exception as e:
        nasdaq_error = f"{type(e).__name__}: {e}"

    try:
        return await fetch_cboe_market(client, item, nasdaq_error)
    except Exception as e:
        return {
            "symbol": item["symbol"],
            "name": item["name"],
            "price": None,
            "previous_close": None,
            "open": None,
            "change_pct": None,
            "since_open_pct": None,
            "status": market_state(datetime.now(NY)),
            "source": None,
            "error": f"Nasdaq: {nasdaq_error} | CBOE: {type(e).__name__}: {e}",
        }


DASHBOARD_HTML = r'''<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Terminal Ballenas + Mercados</title>
<style>
:root{color-scheme:dark;--bg:#07090d;--panel:#10141c;--panel2:#151b25;--line:#283142;--text:#f5f7fb;--muted:#9aa6b5;--green:#33d17a;--red:#ff5c68;--yellow:#f6c85f;--blue:#6aa9ff}
*{box-sizing:border-box}body{margin:0;background:linear-gradient(180deg,#07090d,#0b1018);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;min-height:100vh}.wrap{max-width:1100px;margin:auto;padding:18px}.top{display:flex;gap:12px;align-items:flex-start;justify-content:space-between;margin-bottom:14px}.title{font-size:23px;font-weight:800;margin:0}.sub{color:var(--muted);font-size:13px;margin-top:5px}.pill{border:1px solid var(--line);border-radius:999px;padding:7px 10px;font-size:12px;color:var(--muted);white-space:nowrap}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.card{background:rgba(16,20,28,.95);border:1px solid var(--line);border-radius:16px;padding:14px;box-shadow:0 10px 30px rgba(0,0,0,.18)}.card h2{font-size:15px;margin:0 0 10px}.session-head{display:flex;justify-content:space-between;align-items:center;gap:8px}.status{font-size:11px;border-radius:999px;padding:4px 8px;background:#202735;color:#c8d2e0}.status.live{background:rgba(51,209,122,.15);color:var(--green)}.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:12px}.metric{background:var(--panel2);border-radius:12px;padding:10px}.metric .k{font-size:11px;color:var(--muted)}.metric .v{font-weight:800;font-size:17px;margin-top:4px;overflow-wrap:anywhere}.metric .p{font-size:12px;font-weight:700;margin-top:2px}.green{color:var(--green)}.red{color:var(--red)}.gray{color:#c4ccd7}.flow-card{border-color:#344158;background:linear-gradient(145deg,rgba(15,23,34,.98),rgba(16,20,28,.98))}.flow-topline{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}.flow-contract{font-size:12px;color:var(--muted);margin-top:3px;overflow-wrap:anywhere}.countdown{font-variant-numeric:tabular-nums;font-size:20px;font-weight:900;letter-spacing:.5px;text-align:right}.flow-track{height:10px;display:flex;overflow:hidden;border-radius:999px;background:#222a37;margin-top:12px}.flow-yes{background:var(--green);transition:width .25s ease}.flow-no{background:var(--red);transition:width .25s ease}.flow-foot{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-top:9px}.warn{font-size:12px;color:var(--yellow);background:rgba(246,200,95,.08);border:1px solid rgba(246,200,95,.25);border-radius:10px;padding:8px;margin-top:8px;overflow-wrap:anywhere}.markets{margin-top:12px}.market{display:grid;grid-template-columns:minmax(120px,1.4fr) .9fr .7fr .7fr;gap:8px;align-items:center;padding:10px 0;border-top:1px solid var(--line)}.market:first-of-type{border-top:0}.mname{font-weight:700}.msym,.source,.small{font-size:11px;color:var(--muted)}.num{text-align:right;font-variant-numeric:tabular-nums}.err{font-size:12px;color:#ff9aa2;background:rgba(255,92,104,.09);border:1px solid rgba(255,92,104,.25);border-radius:10px;padding:8px;margin-top:7px;overflow-wrap:anywhere}.events{margin-top:12px}.event{border-top:1px solid var(--line);padding:9px 0}.event:first-child{border-top:0}.event-line{display:flex;gap:8px;justify-content:space-between;align-items:flex-start}.event-text{font-size:12px;color:#dfe6ef;line-height:1.35;margin-top:3px}.empty{color:var(--muted);font-size:13px;padding:8px 0}.footer{color:var(--muted);font-size:11px;text-align:center;padding:18px 0}.full{grid-column:1/-1}@media(max-width:720px){.grid{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(3,1fr)}.market{grid-template-columns:1.4fr .9fr .7fr}.market .openCol{display:none}.top{align-items:flex-start}.title{font-size:20px}}@media(max-width:430px){.wrap{padding:12px}.metrics{gap:6px}.metric{padding:9px 7px}.metric .v{font-size:15px}.pill{font-size:10px;padding:6px 8px}.market{font-size:13px}.countdown{font-size:17px}}
</style>
<style>
.sports-card{border-color:#3a405a;background:linear-gradient(145deg,rgba(18,22,35,.98),rgba(16,20,28,.98))}.sports-title-row{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}.sports-meta{font-size:11px;color:var(--muted);margin-top:4px}.sport-tabs{display:flex;gap:7px;overflow-x:auto;padding:11px 0 4px;scrollbar-width:none}.sport-tabs::-webkit-scrollbar{display:none}.sport-tab{border:1px solid var(--line);background:#151b25;color:#cbd5e2;border-radius:999px;padding:7px 10px;font-size:11px;font-weight:750;white-space:nowrap;cursor:pointer}.sport-tab.active{color:#07100b;background:var(--green);border-color:var(--green)}.sports-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:10px}.sport-market{background:var(--panel2);border:1px solid #2a3446;border-radius:13px;padding:11px;min-width:0}.sport-market-head{display:flex;justify-content:space-between;align-items:flex-start;gap:8px}.sport-badge{font-size:10px;font-weight:800;color:#cbd8ee;background:#252f42;border-radius:999px;padding:4px 7px;white-space:nowrap}.sport-event-title{font-size:14px;font-weight:800;line-height:1.25;overflow-wrap:anywhere}.sport-market-title{font-size:11px;color:var(--muted);margin-top:4px;line-height:1.3;overflow-wrap:anywhere}.sport-sides{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-top:10px}.sport-side{border-radius:10px;padding:9px;background:#111722;border:1px solid transparent;min-width:0}.sport-side.dominant-yes{border-color:rgba(51,209,122,.55);background:rgba(51,209,122,.07)}.sport-side.dominant-no{border-color:rgba(255,92,104,.55);background:rgba(255,92,104,.07)}.sport-side-name{font-size:11px;color:#dbe4ef;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.sport-side-money{font-size:18px;font-weight:900;margin-top:3px;font-variant-numeric:tabular-nums}.sport-side-detail{font-size:11px;color:var(--muted);margin-top:2px}.sport-flow-track{height:7px;display:flex;overflow:hidden;border-radius:999px;background:#232b39;margin-top:9px}.sport-foot{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;font-size:10px;color:var(--muted);margin-top:8px}.sports-note{font-size:11px;color:var(--muted);margin-top:10px;line-height:1.35}.sport-market .warn,.sport-market .err{font-size:10px;padding:6px;margin-top:7px}@media(max-width:720px){.sports-list{grid-template-columns:1fr}.sport-tabs{margin-right:-14px;padding-right:14px}}@media(max-width:430px){.sport-market{padding:10px}.sport-side-money{font-size:16px}.sports-title-row{align-items:flex-start}}
</style>
<style>
.sports-toggle{width:36px;height:36px;display:grid;place-items:center;border:1px solid var(--line);border-radius:10px;background:#171e2a;color:#e8edf5;font-size:19px;font-weight:900;cursor:pointer;flex:0 0 auto}.sports-toggle:active{transform:scale(.96)}.sports-collapsible[hidden]{display:none}.sports-summary{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px;margin-top:11px}.sports-summary-item{background:#121925;border:1px solid #29354a;border-radius:11px;padding:9px;min-width:0}.sports-summary-k{font-size:10px;color:var(--muted)}.sports-summary-v{font-size:15px;font-weight:900;margin-top:3px;font-variant-numeric:tabular-nums}.sports-summary-p{font-size:10px;color:var(--muted);margin-top:2px}.sport-live{color:#07100b;background:var(--green);border-color:var(--green)}.sport-volume{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-top:8px;padding-top:8px;border-top:1px solid #253044;font-size:10px;color:#b9c6d7}.sport-no-recent{font-size:10px;color:var(--yellow);margin-top:7px}@media(max-width:520px){.sports-summary{grid-template-columns:1fr 1fr}.sports-summary-item:last-child{grid-column:1/-1}}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div><h1 class="title">Terminal Ballenas + Mercados</h1><div class="sub">Actualización automática · hora de Nueva York</div></div>
    <div id="clock" class="pill">Cargando…</div>
  </div>

  <div class="grid">
    <section class="card" id="overnightCard"><div class="session-head"><h2>Ballenas · 4:00 PM → 9:30 AM</h2><span class="status" id="overnightStatus">—</span></div><div id="overnightBody" class="empty">Cargando…</div></section>
    <section class="card" id="regularCard"><div class="session-head"><h2>Ballenas · 9:30 AM → 4:00 PM</h2><span class="status" id="regularStatus">—</span></div><div id="regularBody" class="empty">Cargando…</div></section>
    <section class="card full flow-card" id="kalshiFlowCard"><div class="flow-topline"><div><div class="session-head"><h2 style="margin:0">Flujo de dinero Kalshi · BTC 15 min</h2><span class="status" id="kalshiFlowStatus">—</span></div><div class="flow-contract" id="kalshiContract">Buscando el contrato activo…</div></div><div><div class="small" style="text-align:right">TIEMPO RESTANTE</div><div class="countdown" id="kalshiCountdown">--:--</div></div></div><div id="kalshiFlowBody" class="empty">Cargando operaciones públicas…</div></section>
    <section class="card full sports-card" id="sportsFlowCard"><div class="sports-title-row"><div><div class="session-head"><h2 style="margin:0">Flujo deportivo Kalshi</h2><span class="status" id="sportsFlowStatus">—</span></div><div class="sports-meta" id="sportsFlowMeta">Soccer · fútbol americano · básquetbol · béisbol · boxeo · UFC/MMA</div></div><button type="button" class="sports-toggle" id="sportsToggle" aria-expanded="true" aria-controls="sportsCollapsible" title="Cerrar deportes">▲</button></div><div class="sports-collapsible" id="sportsCollapsible"><div class="sports-summary" id="sportsSummary"></div><div class="sport-tabs" id="sportsTabs"></div><div id="sportsFlowBody" class="empty">Buscando partidos y peleas abiertos con volumen…</div></div></section>
    <section class="card full markets"><h2>Mercados</h2><div id="marketBody" class="empty">Cargando…</div></section>
    <section class="card full events"><h2>Últimas alertas BTC</h2><div id="eventBody" class="empty">Cargando…</div></section>
  </div>
  <div class="footer">Si una fuente externa falla, la terminal lo muestra aquí en vez de dejar guiones silenciosos.</div>
</div>
<script>
const fmt = (n, d=2) => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const pct = n => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—' : `${Number(n)>=0?'+':''}${Number(n).toFixed(2)}%`;
const usd = n => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('en-US',{style:'currency',currency:'USD',minimumFractionDigits:0,maximumFractionDigits:0});
const usdFlow = n => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('en-US',{style:'currency',currency:'USD',minimumFractionDigits:2,maximumFractionDigits:2});
const compactUsd = n => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('en-US',{style:'currency',currency:'USD',notation:'compact',maximumFractionDigits:1});
const contracts = n => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('en-US',{notation:'compact',maximumFractionDigits:1});
const cents = n => (n === null || n === undefined || Number.isNaN(Number(n))) ? '—' : Math.round(Number(n)*100)+'¢';
const cls = n => Number(n)>0?'green':Number(n)<0?'red':'gray';
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c]));
let kalshiCloseAt = null;
let sportsPayload = null;
let selectedSport = 'all';
let sportsOpen = localStorage.getItem('ballenaSportsOpen') !== 'closed';

function renderSession(key, x){
  const st=document.getElementById(key+'Status');
  st.textContent=x.status || '—'; st.className='status '+(x.active?'live':'');
  const body=document.getElementById(key+'Body');
  body.className='';
  body.innerHTML=`<div class="metrics">
    <div class="metric"><div class="k">ENTRADAS</div><div class="v red">${fmt(x.inflow_btc,0)} BTC</div></div>
    <div class="metric"><div class="k">SALIDAS</div><div class="v green">${fmt(x.outflow_btc,0)} BTC</div></div>
    <div class="metric"><div class="k">NETO</div><div class="v ${cls(-Number(x.net_btc))}">${fmt(x.net_btc,0)} BTC</div></div>
  </div><div class="small" style="margin-top:10px">${x.count||0} alertas · señal: <b>${esc(x.signal||'neutral')}</b>${x.last_update?' · última '+new Date(x.last_update).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}):''}</div>`;
}

function renderKalshiFlow(d){
  const st=document.getElementById('kalshiFlowStatus');
  st.textContent=d.status||'—'; st.className='status '+(d.status==='EN VIVO'?'live':'');
  const contract=document.getElementById('kalshiContract');
  const body=document.getElementById('kalshiFlowBody');
  const m=d.market;

  if(!m){
    kalshiCloseAt=null;
    document.getElementById('kalshiCountdown').textContent='--:--';
    contract.textContent='Buscando el contrato activo '+(d.series_ticker||'KXBTC15M')+'…';
    body.innerHTML='<div class="empty">Kalshi está cambiando al próximo contrato de 15 minutos.</div>'+(d.error?'<div class="err">'+esc(d.error)+'</div>':'');
    return;
  }

  kalshiCloseAt=m.close_time ? new Date(m.close_time).getTime() : null;
  const target=(m.target_price!==null&&m.target_price!==undefined)?' · objetivo $'+fmt(m.target_price,2):'';
  contract.textContent=(m.title||'BTC 15 min')+target+' · '+(m.ticker||'');
  const dominant=d.dominant==='yes'?'🟢 YES (SUBE)':d.dominant==='no'?'🔴 NO (BAJA)':'⚪ EMPATE';
  const dominantClass=d.dominant==='yes'?'green':d.dominant==='no'?'red':'gray';
  const last=d.last_trade?' · última '+new Date(d.last_trade).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'}):'';
  const blocks=d.block_trade_count?` · ${fmt(d.block_trade_count,0)} en bloque`:'';
  const warning=d.error?'<div class="warn">Datos conservados; hubo un retraso: '+esc(d.error)+'</div>':'';
  const pageWarning=d.page_limit_reached?'<div class="warn">Este contrato superó el límite de páginas configurado; el total puede estar incompleto.</div>':'';

  body.className='';
  body.innerHTML=`<div class="metrics">
    <div class="metric"><div class="k">FLUJO YES · SUBE</div><div class="v green">${usd(d.yes_dollars)}</div><div class="p green">${fmt(d.yes_pct,1)}%</div></div>
    <div class="metric"><div class="k">FLUJO NO · BAJA</div><div class="v red">${usd(d.no_dollars)}</div><div class="p red">${fmt(d.no_pct,1)}%</div></div>
    <div class="metric"><div class="k">DOMINANTE</div><div class="v ${dominantClass}">${dominant}</div><div class="p">Diferencia ${usd(Math.abs(Number(d.yes_dollars)-Number(d.no_dollars)))}</div></div>
  </div>
  <div class="flow-track" aria-label="Distribución del flujo agresor"><div class="flow-yes" style="width:${Math.max(0,Math.min(100,Number(d.yes_pct)||0))}%"></div><div class="flow-no" style="width:${Math.max(0,Math.min(100,Number(d.no_pct)||0))}%"></div></div>
  <div class="flow-foot small"><span>${fmt(d.trade_count,0)} operaciones públicas${blocks}${last}</span><span>Total agresor: <b>${usd(d.total_dollars)}</b></span></div>
  <div class="small" style="margin-top:7px">Dólares pagados por el lado que tomó liquidez (contratos × precio). No mide cuántas personas operaron.</div>${warning}${pageWarning}`;
  updateKalshiCountdown();
}

function updateKalshiCountdown(){
  const el=document.getElementById('kalshiCountdown');
  if(!kalshiCloseAt){el.textContent='--:--';return;}
  const total=Math.max(0,Math.ceil((kalshiCloseAt-Date.now())/1000));
  const min=Math.floor(total/60); const sec=total%60;
  el.textContent=String(min).padStart(2,'0')+':'+String(sec).padStart(2,'0');
}

function renderSportsTabs(d){
  const tabs=document.getElementById('sportsTabs');
  const choices=[{key:'all',label:'Todos',market_count:d.market_count||0,flow_dollars:d.flow_dollars||0,volume_contracts:d.volume_contracts||0},...(d.sports||[])];
  tabs.innerHTML=choices.map(item=>`<button type="button" class="sport-tab ${selectedSport===item.key?'active':''}" data-sport="${esc(item.key)}" title="${fmt(item.market_count||0,0)} mercados · ${contracts(item.volume_contracts||0)} contratos">${esc(item.label)} · ${compactUsd(item.flow_dollars||0)}</button>`).join('');
  tabs.querySelectorAll('[data-sport]').forEach(button=>button.addEventListener('click',()=>{
    selectedSport=button.dataset.sport||'all';
    if(sportsPayload) renderSportsFlow(sportsPayload);
  }));
}

function renderSportsSummary(d){
  const summary=document.getElementById('sportsSummary');
  summary.innerHTML=`
    <div class="sports-summary-item"><div class="sports-summary-k">FLUJO RECIENTE</div><div class="sports-summary-v">${usdFlow(d.flow_dollars||0)}</div><div class="sports-summary-p">últimos ${fmt(d.window_minutes||15,0)} minutos</div></div>
    <div class="sports-summary-item"><div class="sports-summary-k">VOLUMEN KALSHI</div><div class="sports-summary-v">${contracts(d.volume_contracts||0)}</div><div class="sports-summary-p">contratos acumulados</div></div>
    <div class="sports-summary-item"><div class="sports-summary-k">VOLUMEN 24 HORAS</div><div class="sports-summary-v">${contracts(d.volume_24h_contracts||0)}</div><div class="sports-summary-p">contratos negociados</div></div>`;
}

function applySportsOpenState(){
  const content=document.getElementById('sportsCollapsible');
  const button=document.getElementById('sportsToggle');
  content.hidden=!sportsOpen;
  button.textContent=sportsOpen?'▲':'▼';
  button.setAttribute('aria-expanded',sportsOpen?'true':'false');
  button.title=sportsOpen?'Cerrar deportes':'Abrir deportes';
}

document.getElementById('sportsToggle').addEventListener('click',()=>{
  sportsOpen=!sportsOpen;
  localStorage.setItem('ballenaSportsOpen',sportsOpen?'open':'closed');
  applySportsOpenState();
});
applySportsOpenState();

function sportMarketCard(m,windowMinutes){
  const dominantYes=m.dominant==='yes';
  const dominantNo=m.dominant==='no';
  const yesWidth=Math.max(0,Math.min(100,Number(m.yes_pct)||0));
  const noWidth=Math.max(0,Math.min(100,Number(m.no_pct)||0));
  const when=m.event_start||m.close_time;
  const eventTime=when&&!Number.isNaN(new Date(when).getTime())
    ?new Date(when).toLocaleString([], {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'})
    :'Hora pendiente';
  const last=m.last_trade&&!Number.isNaN(new Date(m.last_trade).getTime())
    ?' · última '+new Date(m.last_trade).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'})
    :'';
  const large=m.large_trade_count
    ?`<span class="warn">${fmt(m.large_trade_count,0)} flujo${Number(m.large_trade_count)===1?'':'s'} ≥ $10K · mayor ${usdFlow(m.largest_trade_dollars)}</span>`
    :'';
  const warning=m.error?`<div class="err">Retraso en este mercado: ${esc(m.error)}</div>`:'';
  const pageWarning=m.page_limit_reached?'<div class="warn">El flujo puede estar incompleto por exceso de operaciones.</div>':'';
  const noRecent=Number(m.total_dollars||0)<=0
    ?`<div class="sport-no-recent">Sin operaciones ejecutadas en los últimos ${fmt(windowMinutes,0)} min; el volumen acumulado sí aparece abajo.</div>`
    :'';
  const badgeClass=m.is_live?'sport-badge sport-live':'sport-badge';
  const badgeText=m.is_live?'AHORA · '+(m.sport_label||'Deporte'):(m.sport_label||'Deporte');
  return `<article class="sport-market">
    <div class="sport-market-head"><div style="min-width:0"><div class="sport-event-title">${esc(m.event_title||'Evento deportivo')}</div><div class="sport-market-title">${esc(m.market_title||m.ticker||'Mercado')}</div></div><span class="${badgeClass}">${esc(badgeText)}</span></div>
    <div class="sport-sides">
      <div class="sport-side ${dominantYes?'dominant-yes':''}"><div class="sport-side-name" title="${esc(m.yes_label||'SÍ')}">YES · ${esc(m.yes_label||'SÍ')}</div><div class="sport-side-money green">${usdFlow(m.yes_dollars)}</div><div class="sport-side-detail">${fmt(m.yes_pct,1)}% · precio ${cents(m.yes_ask??m.yes_bid)}</div></div>
      <div class="sport-side ${dominantNo?'dominant-no':''}"><div class="sport-side-name" title="${esc(m.no_label||'NO')}">NO · ${esc(m.no_label||'NO')}</div><div class="sport-side-money red">${usdFlow(m.no_dollars)}</div><div class="sport-side-detail">${fmt(m.no_pct,1)}% · precio ${cents(m.no_ask??m.no_bid)}</div></div>
    </div>
    <div class="sport-flow-track"><div class="flow-yes" style="width:${yesWidth}%"></div><div class="flow-no" style="width:${noWidth}%"></div></div>
    <div class="sport-foot"><span>${fmt(m.trade_count,0)} operaciones · flujo ${usdFlow(m.total_dollars)}${last}</span><span>${esc(eventTime)} · ${windowMinutes} min</span></div>
    <div class="sport-volume"><span>Volumen total: <b>${contracts(m.volume||0)}</b> contratos</span><span>24 h: <b>${contracts(m.volume_24h||0)}</b></span></div>${noRecent}${large}${warning}${pageWarning}
  </article>`;
}

function renderSportsFlow(d){
  sportsPayload=d;
  const st=document.getElementById('sportsFlowStatus');
  st.textContent=d.status||'—'; st.className='status '+(d.status==='EN VIVO'?'live':'');
  document.getElementById('sportsFlowMeta').textContent=`Partidos y peleas abiertos con volumen · ${d.event_count||0} eventos · ${d.market_count||0} mercados`;
  renderSportsSummary(d);
  renderSportsTabs(d);

  const body=document.getElementById('sportsFlowBody');
  const markets=(d.markets||[]).filter(m=>selectedSport==='all'||m.sport===selectedSport);
  if(!markets.length){
    const label=selectedSport==='all'?'los deportes seleccionados':(d.sports||[]).find(x=>x.key===selectedSport)?.label||selectedSport;
    body.className='';
    body.innerHTML=`<div class="empty">Ahora mismo no encontré mercados abiertos de ${esc(label)}. La terminal seguirá buscando automáticamente.</div>${d.error?'<div class="err">'+esc(d.error)+'</div>':''}`;
    return;
  }

  body.className='';
  const warning=d.error?'<div class="warn">Algunos datos tuvieron retraso: '+esc(d.error)+'</div>':'';
  body.innerHTML=`<div class="sports-list">${markets.map(m=>sportMarketCard(m,d.window_minutes||15)).join('')}</div><div class="sports-note">“Flujo” son dólares ejecutados durante los últimos ${fmt(d.window_minutes||15,0)} minutos. “Volumen” son contratos acumulados que Kalshi muestra para el mercado; por eso puede existir mucho volumen aunque el flujo reciente sea $0.</div>${warning}`;
}

function renderEvents(data){
  const active = data.active_window==='overnight' ? data.overnight : data.regular;
  const all=[...(data.overnight?.events||[]),...(data.regular?.events||[])];
  const uniq=[]; const seen=new Set();
  all.sort((a,b)=>String(b.ts).localeCompare(String(a.ts))).forEach(e=>{const k=e.source+'|'+e.external_id+'|'+e.ts;if(!seen.has(k)){seen.add(k);uniq.push(e)}});
  const el=document.getElementById('eventBody');
  if(!uniq.length){el.innerHTML='<div class="empty">Todavía no hay alertas BTC guardadas para las sesiones visibles. La terminal seguirá intentando leer los canales públicos.</div>';return;}
  el.innerHTML=uniq.slice(0,18).map(e=>`<div class="event"><div class="event-line"><div><b>${fmt(e.btc,0)} BTC</b> · ${esc(e.direction)}</div><div class="small">${new Date(e.ts).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}</div></div><div class="event-text">${esc(e.raw)}</div><div class="source">${esc(e.source)}</div></div>`).join('');
}

async function loadWhales(){
  try{
    const r=await fetch('/api/whales',{cache:'no-store'}); if(!r.ok) throw new Error('HTTP '+r.status);
    const d=await r.json(); renderSession('overnight',d.overnight); renderSession('regular',d.regular); renderEvents(d);
    document.getElementById('clock').textContent=new Date(d.as_of).toLocaleString([], {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'});
  }catch(e){
    document.getElementById('overnightBody').innerHTML='<div class="err">Error en /api/whales: '+esc(e.message)+'</div>';
    document.getElementById('regularBody').innerHTML='<div class="err">Error en /api/whales: '+esc(e.message)+'</div>';
  }
}

async function loadMarkets(){
  const el=document.getElementById('marketBody');
  try{
    const r=await fetch('/api/markets',{cache:'no-store'}); if(!r.ok) throw new Error('HTTP '+r.status);
    const d=await r.json();
    el.innerHTML=(d.markets||[]).map(m=>`<div class="market"><div><div class="mname">${esc(m.name)}</div><div class="msym">${esc(m.symbol)} · ${esc(m.status||'')}</div></div><div class="num"><b>${fmt(m.price,2)}</b><div class="source">${esc(m.source||'sin fuente')}</div></div><div class="num ${cls(m.change_pct)}">${pct(m.change_pct)}</div><div class="num openCol ${cls(m.since_open_pct)}">${pct(m.since_open_pct)}</div>${m.error?'<div class="err full">'+esc(m.error)+'</div>':''}</div>`).join('') || '<div class="empty">Sin mercados.</div>';
  }catch(e){el.innerHTML='<div class="err">Error en /api/markets: '+esc(e.message)+'</div>'}
}

async function loadKalshiFlow(){
  try{
    const r=await fetch('/api/kalshi-flow',{cache:'no-store'}); if(!r.ok) throw new Error('HTTP '+r.status);
    renderKalshiFlow(await r.json());
  }catch(e){
    document.getElementById('kalshiFlowStatus').textContent='ERROR';
    document.getElementById('kalshiFlowBody').innerHTML='<div class="err">Error en /api/kalshi-flow: '+esc(e.message)+'</div>';
  }
}

async function loadSportsFlow(){
  try{
    const r=await fetch('/api/sports-flow',{cache:'no-store'}); if(!r.ok) throw new Error('HTTP '+r.status);
    renderSportsFlow(await r.json());
  }catch(e){
    document.getElementById('sportsFlowStatus').textContent='ERROR';
    document.getElementById('sportsFlowBody').innerHTML='<div class="err">Error en /api/sports-flow: '+esc(e.message)+'</div>';
  }
}

loadWhales(); loadMarkets(); loadKalshiFlow(); loadSportsFlow();
setInterval(loadWhales,20000);
setInterval(loadMarkets,60000);
setInterval(loadKalshiFlow,2000);
setInterval(loadSportsFlow,15000);
setInterval(updateKalshiCountdown,250);
</script>
</body>
</html>'''


@asynccontextmanager
async def lifespan(app: FastAPI):
    global MONITOR_TASK, KALSHI_FLOW_TASK, SPORTS_FLOW_TASK
    db().close()
    MONITOR_TASK = asyncio.create_task(public_telegram_monitor())
    KALSHI_FLOW_TASK = asyncio.create_task(kalshi_flow_monitor())
    SPORTS_FLOW_TASK = asyncio.create_task(sports_flow_monitor())
    yield
    for task in (MONITOR_TASK, KALSHI_FLOW_TASK, SPORTS_FLOW_TASK):
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


app = FastAPI(title="Terminal Ballenas + Mercados", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def home():
    # Serve the dashboard embedded in this file so Render does not depend on static/index.html.
    return HTMLResponse(
        content=DASHBOARD_HTML,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.get("/api/whales")
async def whales():
    now_ny = datetime.now(NY)
    windows = whale_windows_ny(now_ny)
    earliest = min(w["start"] for w in windows.values())
    latest = max(w["end"] for w in windows.values())

    con = db()
    try:
        rows = con.execute(
            """
            SELECT * FROM whale_events
            WHERE ts >= ? AND ts < ?
            ORDER BY ts DESC, id DESC
            LIMIT 3000
            """,
            (
                earliest.astimezone(timezone.utc).isoformat(),
                latest.astimezone(timezone.utc).isoformat(),
            ),
        ).fetchall()
    finally:
        con.close()

    def rows_for_window(window):
        start_utc = window["start"].astimezone(timezone.utc)
        end_utc = window["end"].astimezone(timezone.utc)
        selected = []
        for row in rows:
            try:
                ts = datetime.fromisoformat(row["ts"].replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if start_utc <= ts < end_utc:
                selected.append(row)
        return selected

    overnight = summarize_whale_rows(rows_for_window(windows["overnight"]), windows["overnight"])
    regular = summarize_whale_rows(rows_for_window(windows["regular"]), windows["regular"])
    active = overnight if overnight["active"] else regular

    return {
        "as_of": now_ny.isoformat(),
        "timezone": "America/New_York",
        "overnight": overnight,
        "regular": regular,
        "active_window": "overnight" if overnight["active"] else "regular",
        # Legacy fields kept so an older frontend can still read this endpoint.
        "inflow_btc": active["inflow_btc"],
        "outflow_btc": active["outflow_btc"],
        "net_btc": active["net_btc"],
        "signal": active["signal"],
        "count": active["count"],
        "window_start": active["window_start"],
        "last_update": active["last_update"],
        "events": active["events"],
    }


@app.get("/api/markets")
async def markets():
    global MARKET_CACHE, MARKET_CACHE_AT
    now = datetime.now(timezone.utc)

    if MARKET_CACHE is not None and MARKET_CACHE_AT is not None:
        age = (now - MARKET_CACHE_AT).total_seconds()
        if age < MARKET_CACHE_SECONDS:
            return {**MARKET_CACHE, "cached": True, "cache_age_seconds": round(age, 1)}

    async with MARKET_LOCK:
        now = datetime.now(timezone.utc)
        if MARKET_CACHE is not None and MARKET_CACHE_AT is not None:
            age = (now - MARKET_CACHE_AT).total_seconds()
            if age < MARKET_CACHE_SECONDS:
                return {**MARKET_CACHE, "cached": True, "cache_age_seconds": round(age, 1)}

        async with httpx.AsyncClient(headers=HTTP_HEADERS, follow_redirects=True) as client:
            results = await asyncio.gather(*(fetch_one_market(client, m) for m in DEFAULT_MARKETS))

        payload = {
            "as_of": datetime.now(NY).isoformat(),
            "markets": results,
            "cached": False,
            "cache_age_seconds": 0,
        }
        MARKET_CACHE = payload
        MARKET_CACHE_AT = datetime.now(timezone.utc)
        return payload


@app.get("/api/kalshi-flow")
async def kalshi_flow():
    # The background task refreshes every two seconds.  This first-load fallback
    # makes the endpoint useful immediately after a cold Render start as well.
    if KALSHI_FLOW_STATE["last_poll"] is None:
        await refresh_kalshi_flow()
    async with KALSHI_FLOW_LOCK:
        return kalshi_flow_payload()


@app.get("/api/sports-flow")
async def sports_flow():
    # Cold-start fallback; after this, the background monitor refreshes the
    # read-only sports totals every SPORTS_FLOW_POLL_SECONDS.
    if SPORTS_FLOW_STATE["last_poll"] is None:
        await refresh_sports_flow()
    async with SPORTS_FLOW_LOCK:
        return sports_flow_payload()


@app.get("/api/health")
async def health():
    con = db()
    try:
        total = con.execute("SELECT COUNT(*) AS n FROM whale_events").fetchone()["n"]
        last = con.execute("SELECT ts,source,btc,direction FROM whale_events ORDER BY ts DESC,id DESC LIMIT 1").fetchone()
        by_source = [dict(r) for r in con.execute(
            "SELECT source,COUNT(*) AS count FROM whale_events GROUP BY source ORDER BY count DESC"
        ).fetchall()]
    finally:
        con.close()

    return {
        "ok": True,
        "time_ny": datetime.now(NY).isoformat(),
        "db_path": DB_PATH,
        "whale_rows": total,
        "last_whale": dict(last) if last else None,
        "by_source": by_source,
        "market_cache_at": MARKET_CACHE_AT.isoformat() if MARKET_CACHE_AT else None,
        "kalshi_flow": kalshi_flow_payload(),
        "sports_flow": sports_flow_payload(),
    }


@app.post("/api/refresh")
async def refresh():
    global MARKET_CACHE, MARKET_CACHE_AT
    MARKET_CACHE = None
    MARKET_CACHE_AT = None
    KALSHI_FLOW_STATE["last_market_check"] = None
    SPORTS_FLOW_STATE["last_discovery"] = None
    await sync_public_channels(backfill=True)
    kalshi = await refresh_kalshi_flow()
    sports = await refresh_sports_flow()
    return {
        "ok": True,
        "message": "Telegram, Kalshi BTC y deportes resincronizados; caché de mercados limpiada.",
        "kalshi_flow": kalshi,
        "sports_flow": sports,
    }


@app.post("/api/whales/add")
async def add_whale(event: WhaleEvent):
    if event.direction not in {"inflow", "outflow", "neutral"}:
        raise HTTPException(400, "direction debe ser inflow, outflow o neutral")
    insert_whale(
        datetime.now(timezone.utc),
        event.source,
        None,
        event.btc,
        event.direction,
        event.raw,
    )
    return {"ok": True}


@app.post("/api/whales/parse")
async def parse_whale(event: ParseEvent):
    btc, direction = classify_transfer(event.raw)
    if btc is None:
        raise HTTPException(400, "No pude detectar una cantidad BTC en el texto.")
    insert_whale(
        datetime.now(timezone.utc),
        event.source,
        None,
        btc,
        direction,
        event.raw,
    )
    return {"ok": True, "btc": btc, "direction": direction}


@app.post("/api/whales/reset")
async def reset_whales():
    con = db()
    try:
        con.execute("DELETE FROM whale_events")
        con.commit()
    finally:
        con.close()
    return {"ok": True}
