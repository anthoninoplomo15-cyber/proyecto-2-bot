import os, time, threading, math
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, render_template
import requests

app = Flask(__name__)

BINANCE_HOSTS = (
    "https://api.binance.us",
    # .com often 451 from cloud hosts; keep as last resort only
    "https://api.binance.com",
)
KALSHI_MARKETS = "https://api.elections.kalshi.com/trade-api/v2/markets"
WHALE_USD = 75000.0  # real large prints; not fake "whale flow"

lock = threading.Lock()
STATE = {"data": None, "error": None, "updated": None}
ACTIVITY = []  # recent real events for desk log





def _et_now():
    try:
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        return datetime.now(timezone.utc) - timedelta(hours=4)


def open_risk_active(now=None):
    """True only for [09:15, 10:00) America/New_York."""
    now = now or _et_now()
    mins = now.hour * 60 + now.minute
    return (9 * 60 + 15) <= mins < (10 * 60)


def fetch_es_open_wick():
    """ES/SPY open wick around cash open. Honest N/A when no data."""
    now = _et_now()
    out = {
        "open_risk": open_risk_active(now),
        "es_label": "N/A",
        "es_detail": "Fuera de ventana 9:15–10:00 ET",
        "time_et": now.strftime("%H:%M ET"),
        "symbol": None,
    }
    if not out["open_risk"]:
        return out

    headers = {"User-Agent": "Mozilla/5.0 (compatible; omega-desk/1.0)"}
    for sym in ("ES=F", "SPY"):
        try:
            r = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                params={"interval": "1m", "range": "1d"},
                headers=headers,
                timeout=6,
            )
            if r.status_code != 200:
                continue
            res = ((r.json().get("chart") or {}).get("result") or [None])[0]
            if not res:
                continue
            ts = res.get("timestamp") or []
            q = ((res.get("indicators") or {}).get("quote") or [None])[0] or {}
            opens, highs, lows, closes = q.get("open"), q.get("high"), q.get("low"), q.get("close")
            if not ts or not opens:
                continue

            bars = []
            for i, t in enumerate(ts):
                try:
                    dt = datetime.fromtimestamp(t, tz=ZoneInfo("America/New_York"))
                except Exception:
                    dt = datetime.fromtimestamp(t, tz=timezone.utc) - timedelta(hours=4)
                if dt.hour != 9 or not (25 <= dt.minute <= 45):
                    continue
                o, h, l, c = opens[i], highs[i], lows[i], closes[i]
                if None in (o, h, l, c):
                    continue
                bars.append({"dt": dt, "o": float(o), "h": float(h), "l": float(l), "c": float(c)})

            if not bars:
                if now.hour == 9 and now.minute < 30:
                    out.update(es_label="WAITING", es_detail="Esperando open 9:30 ET", symbol=sym)
                    return out
                continue

            open_bars = [b for b in bars if b["dt"].minute >= 30]
            pre_bars = [b for b in bars if b["dt"].minute < 30]
            if not open_bars:
                out.update(es_label="WAITING", es_detail="Sin barra >=9:30 aun", symbol=sym)
                return out

            first = open_bars[0]
            rng = (first["h"] - first["l"]) or 1e-9
            lower = min(first["o"], first["c"]) - first["l"]
            rebound = False
            if pre_bars:
                pre_low = min(b["l"] for b in pre_bars)
                if first["l"] <= pre_low * 1.001 and (first["c"] - first["l"]) / rng > 0.55:
                    rebound = True
            body_up = first["c"] > first["o"]
            wick_up = rebound or (body_up and lower / rng > 0.35 and (first["c"] - first["l"]) / rng > 0.5)
            last_c = open_bars[-1]["c"]
            upper = first["h"] - max(first["o"], first["c"])
            trend_down = ((first["c"] < first["o"] and upper / rng < 0.25)
                          or (last_c < first["o"] * 0.999 and not wick_up))

            if wick_up:
                out.update(
                    es_label="WICK_UP",
                    es_detail=f"{sym}: rebote/wick alcista en open — cuidado chase UP",
                    symbol=sym,
                )
            elif trend_down:
                out.update(
                    es_label="TREND_DOWN",
                    es_detail=f"{sym}: continuacion bajista sin wick alcista fuerte",
                    symbol=sym,
                )
            else:
                out.update(
                    es_label="MIXED",
                    es_detail=f"{sym}: open mixto — sin sesgo claro",
                    symbol=sym,
                )
            return out
        except Exception:
            continue

    out.update(es_label="N/A", es_detail="Sin data ES/SPY (Yahoo)")
    return out


# Market data: OKX first (works on cloud hosts), then Binance.US. Never hang.
_HTTP_TIMEOUT = (1.2, 2.5)
_FEED = {"name": None}


def _http_get(url, params=None):
    r = requests.get(
        url,
        params=params or {},
        timeout=_HTTP_TIMEOUT,
        headers={"User-Agent": "omega-desk/1.2", "Accept": "application/json"},
    )
    r.raise_for_status()
    return r.json()


def _okx_klines(limit=120):
    j = _http_get(
        "https://www.okx.com/api/v5/market/candles",
        {"instId": "BTC-USDT", "bar": "1m", "limit": str(limit)},
    )
    rows = j.get("data") or []
    # OKX: newest first → oldest first; normalize to Binance-like kline rows
    out = []
    for row in reversed(rows):
        # [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        out.append([int(row[0]), row[1], row[2], row[3], row[4], row[5]])
    if not out:
        raise RuntimeError("okx empty klines")
    return out


def _okx_price():
    j = _http_get(
        "https://www.okx.com/api/v5/market/ticker",
        {"instId": "BTC-USDT"},
    )
    return float(j["data"][0]["last"])


def _okx_depth(limit=20):
    j = _http_get(
        "https://www.okx.com/api/v5/market/books",
        {"instId": "BTC-USDT", "sz": str(limit)},
    )
    book = (j.get("data") or [{}])[0]
    bids = [[b[0], b[1]] for b in (book.get("bids") or [])[:limit]]
    asks = [[a[0], a[1]] for a in (book.get("asks") or [])[:limit]]
    return {"bids": bids, "asks": asks}


def _okx_trades(limit=500):
    j = _http_get(
        "https://www.okx.com/api/v5/market/trades",
        {"instId": "BTC-USDT", "limit": str(min(limit, 500))},
    )
    trades = []
    for t in j.get("data") or []:
        # side = taker side; buy taker ⇒ not buyer-maker
        side = (t.get("side") or "").lower()
        trades.append(
            {
                "price": t.get("px"),
                "qty": t.get("sz"),
                "isBuyerMaker": side == "sell",
                "time": int(t.get("ts") or 0),
            }
        )
    return trades


def _binance_us_get(path, params):
    r = requests.get(
        "https://api.binance.us" + path,
        params=params,
        timeout=_HTTP_TIMEOUT,
        headers={"User-Agent": "omega-desk/1.2"},
    )
    if r.status_code == 451:
        raise RuntimeError("binance.us blocked 451")
    r.raise_for_status()
    return r.json()


def fetch_klines(limit=120):
    errors = []
    for name, fn in (
        ("okx", lambda: _okx_klines(limit)),
        ("binance.us", lambda: _binance_us_get(
            "/api/v3/klines",
            {"symbol": "BTCUSDT", "interval": "1m", "limit": limit},
        )),
    ):
        try:
            rows = fn()
            _FEED["name"] = name
            return rows
        except Exception as e:
            errors.append(f"{name}:{e}")
    raise RuntimeError("klines failed: " + " | ".join(errors))


def fetch_price():
    errors = []
    for name, fn in (
        ("okx", _okx_price),
        ("binance.us", lambda: float(
            _binance_us_get("/api/v3/ticker/price", {"symbol": "BTCUSDT"})["price"]
        )),
        ("coinbase", fetch_coinbase_btc),
    ):
        try:
            px = fn()
            if _FEED["name"] is None:
                _FEED["name"] = name
            return px
        except Exception as e:
            errors.append(f"{name}:{e}")
    raise RuntimeError("price failed: " + " | ".join(errors))


def fetch_depth(limit=20):
    errors = []
    for name, fn in (
        ("okx", lambda: _okx_depth(limit)),
        ("binance.us", lambda: _binance_us_get(
            "/api/v3/depth", {"symbol": "BTCUSDT", "limit": limit}
        )),
    ):
        try:
            return fn()
        except Exception as e:
            errors.append(f"{name}:{e}")
    raise RuntimeError("depth failed: " + " | ".join(errors))


def fetch_trades(limit=500):
    errors = []
    for name, fn in (
        ("okx", lambda: _okx_trades(limit)),
        ("binance.us", lambda: _binance_us_get(
            "/api/v3/trades", {"symbol": "BTCUSDT", "limit": limit}
        )),
    ):
        try:
            return fn()
        except Exception as e:
            errors.append(f"{name}:{e}")
    raise RuntimeError("trades failed: " + " | ".join(errors))



def kalshi_fee_cents_per_contract(p):
    """Kalshi-style fee ≈ 0.07 * P * (1-P) dollars → cents per contract."""
    try:
        p = float(p)
    except (TypeError, ValueError):
        return 0.0
    p = max(0.01, min(0.99, p))
    return 7.0 * p * (1.0 - p)


def fetch_kalshi_book_near_ask(ticker, favor_yes, ask_prob, band=0.03, min_contracts=25.0):
    """Liquidity near the ask for the favored side. YES buys lift NO bids (and vice versa)."""
    out = {
        "book_ok": False,
        "book_contracts_near": None,
        "book_label": "BOOK ?",
    }
    if not ticker or ask_prob is None:
        return out
    try:
        r = requests.get(
            f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}/orderbook",
            timeout=(1.2, 2.5),
            headers={"User-Agent": "omega-desk/1.3", "Accept": "application/json"},
        )
        r.raise_for_status()
        ob = (r.json() or {}).get("orderbook_fp") or {}
        levels = ob.get("no_dollars" if favor_yes else "yes_dollars") or []
        target = 1.0 - float(ask_prob)
        near = 0.0
        for row in levels:
            if not row or len(row) < 2:
                continue
            px, sz = float(row[0]), float(row[1])
            if abs(px - target) <= band:
                near += sz
        out["book_contracts_near"] = round(near, 0)
        ok = near >= min_contracts
        out["book_ok"] = ok
        out["book_label"] = "BOOK OK" if ok else "BOOK THIN"
        return out
    except Exception as e:
        out["book_label"] = "BOOK ?"
        out["book_error"] = str(e)[:80]
        return out


def fetch_kalshi_btc15m():
    r = requests.get(
        KALSHI_MARKETS,
        params={"series_ticker": "KXBTC15M", "status": "open", "limit": 20},
        timeout=(2.0, 4.0),
    )
    r.raise_for_status()
    markets = r.json().get("markets") or []
    if not markets:
        return None
    now = datetime.now(timezone.utc)

    def close_dt(m):
        return datetime.fromisoformat(str(m.get("close_time")).replace("Z", "+00:00"))

    markets = sorted(markets, key=close_dt)
    m = markets[0]
    ct = close_dt(m)
    rem = max(0, int((ct - now).total_seconds()))

    def money(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    return {
        "ticker": m.get("ticker"),
        "strike": money(m.get("floor_strike")),
        "yes_ask": money(m.get("yes_ask_dollars") or m.get("yes_ask")),
        "no_ask": money(m.get("no_ask_dollars") or m.get("no_ask")),
        "yes_bid": money(m.get("yes_bid_dollars") or m.get("yes_bid")),
        "no_bid": money(m.get("no_bid_dollars") or m.get("no_bid")),
        "close_time": ct.isoformat(),
        "seconds_remaining": rem,
    }


def ema(values, period):
    if len(values) < period:
        return sum(values) / len(values)
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return 50.0
    gains, losses = [], []
    for a, b in zip(values[-period - 1 : -1], values[-period:]):
        d = b - a
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains) / period
    al = sum(losses) / period
    if al == 0:
        return 100.0
    return 100 - (100 / (1 + ag / al))


def atr(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        )
    if not trs:
        return 0
    return sum(trs[-period:]) / min(period, len(trs))


def vwap(klines):
    pv = 0
    vol = 0
    last_c = 0.0
    for k in klines:
        h, l, c, v = map(float, [k[2], k[3], k[4], k[5]])
        last_c = c
        typical = (h + l + c) / 3
        pv += typical * v
        vol += v
    return pv / vol if vol else last_c


def pct(a, b):
    return ((a - b) / b * 100) if b else 0


def window_countdown():
    """Seconds left in the current UTC 15-minute bucket."""
    now = datetime.now(timezone.utc)
    minute = (now.minute // 15) * 15
    start = now.replace(minute=minute, second=0, microsecond=0)
    end = start + timedelta(minutes=15)
    return max(0, int((end - now).total_seconds())), end.isoformat()


def book_imbalance(depth):
    bids = depth.get("bids") or []
    asks = depth.get("asks") or []
    bid_notional = sum(float(p) * float(q) for p, q in bids[:10])
    ask_notional = sum(float(p) * float(q) for p, q in asks[:10])
    tot = bid_notional + ask_notional
    if tot <= 0:
        return 0.0, bid_notional, ask_notional
    # +1 = all bid, -1 = all ask
    imb = (bid_notional - ask_notional) / tot
    return imb, bid_notional, ask_notional


def aggressor_and_whales(trades):
    buy = 0.0
    sell = 0.0
    whales = []
    for t in trades:
        px = float(t["price"])
        qty = float(t["qty"])
        notional = px * qty
        # isBuyerMaker True => seller was aggressor (market sell)
        if t.get("isBuyerMaker"):
            sell += notional
            side = "SELL"
        else:
            buy += notional
            side = "BUY"
        if notional >= WHALE_USD:
            whales.append(
                {
                    "side": side,
                    "usd": round(notional, 0),
                    "price": px,
                    "qty": qty,
                    "time": t.get("time"),
                }
            )
    tot = buy + sell
    flow = (buy - sell) / tot if tot else 0.0
    whales.sort(key=lambda w: w["usd"], reverse=True)
    whale_buy = sum(w["usd"] for w in whales if w["side"] == "BUY")
    whale_sell = sum(w["usd"] for w in whales if w["side"] == "SELL")
    return {
        "buy_usd": round(buy, 0),
        "sell_usd": round(sell, 0),
        "flow": round(flow, 3),  # + buy heavy
        "whale_count": len(whales),
        "whale_buy_usd": round(whale_buy, 0),
        "whale_sell_usd": round(whale_sell, 0),
        "whale_net_usd": round(whale_buy - whale_sell, 0),
        "whales": whales[:8],
        "min_whale_usd": WHALE_USD,
    }



def timeline_phase(rem_s: int) -> dict:
    """Map seconds left in 15m window to the photo-style phases (elapsed view)."""
    elapsed = max(0, 900 - int(rem_s or 0))
    if elapsed < 180:
        name, tip = "0–3 DATA", "Recopilando precio, libro, flujo, whales"
    elif elapsed < 360:
        name, tip = "3–6 ANALYZE", "Agentes Spotter/Prior/Book/Flow/Whale"
    elif elapsed < 480:
        name, tip = "6–8 CONSENSUS", "Buscando acuerdo entre señales"
    elif elapsed < 720:
        name, tip = "8–12 CONFIRM", "Esperando confirmación / evitar ruido"
    else:
        name, tip = "12–15 EXECUTE", "Ventana tardía: solo setups muy limpios"
    return {"elapsed_s": elapsed, "name": name, "tip": tip, "rem_s": int(rem_s or 0)}



def fetch_coinbase_btc():
    r = requests.get(
        "https://api.coinbase.com/v2/prices/BTC-USD/spot",
        timeout=(2.0, 4.0),
    )
    r.raise_for_status()
    return float(r.json()["data"]["amount"])


def analyze_fallback(err: str):
    """Minimal desk payload when Binance is unreachable from Render."""
    price = fetch_coinbase_btc()
    rem15, window_end = window_countdown()
    open_session = fetch_es_open_wick()
    decision_aid = {
        "side": "UP",
        "fair_cents": 50.0,
        "kalshi_ask_cents": None,
        "edge_cents": 0.0,
        "label": "JUSTO",
        "operate": False,
        "light": "red",
        "line": f"Fallback Coinbase — Binance falló: {err}",
    }
    kalshi = None
    try:
        kalshi = fetch_kalshi_btc15m()
    except Exception as e:
        kalshi = {"error": str(e)}
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "window": {"seconds_remaining": rem15, "ends_at": window_end},
        "candle15": {"open": price, "high": price, "low": price, "close": price, "volume": 0, "change_pct": 0},
        "indicators": {"ema3": price, "ema9": price, "ema21": price, "rsi": 50.0, "vwap": price, "atr14_1m": 0, "volume_ratio": 1},
        "book": {"imbalance": 0.0, "bid_usd_top10": 0, "ask_usd_top10": 0},
        "flow": {
            "buy_usd": 0, "sell_usd": 0, "flow": 0.0, "whale_count": 0,
            "whale_buy_usd": 0, "whale_sell_usd": 0, "whale_net_usd": 0,
            "whales": [], "min_whale_usd": WHALE_USD,
        },
        "kalshi_btc15m": kalshi,
        "agents": {
            "spotter": 0, "prior": 0, "book": 0, "flow": 0, "whale": 0,
            "edge": 0, "kelly_confidence": 1, "taker": "WAIT", "closer": "HOLD",
        },
        "signal": "WAIT",
        "side": "WAIT",
        "confidence": 1.0,
        "levels": {"long_tp": price, "long_sl": price, "short_tp": price, "short_sl": price},
        "notes": {
            "whales": "Fallback mode — Binance blocked/hung from host",
            "kalshi": "Public KXBTC15M odds; terminal does not place orders",
            "ui": "Desk-style layout; no fake swarm PnL",
        },
        "timeline": timeline_phase((kalshi or {}).get("seconds_remaining") if isinstance(kalshi, dict) else rem15),
        "probability": 1.0,
        "next_15m": "WAIT",
        "whale_flow_usd": 0,
        "swarm": {"agents_live": 7, "mode": "SIGNALS ONLY · FALLBACK", "human": "YOU decide entries — no auto orders"},
        "decision_aid": decision_aid,
        "open_session": open_session,
        "fallback": True,
        "fallback_error": err,
        "feed": "coinbase-fallback",
    }



def compute_regime(closes, atr14, lookback=30):
    """TREND vs CHOP from path efficiency (no extra API)."""
    if not closes or len(closes) < 8:
        return {"label": "MIXED", "score": 0.0, "er": 0.0, "tip": "Pocos datos"}
    n = min(lookback, len(closes))
    c = closes[-n:]
    net = abs(c[-1] - c[0])
    path = sum(abs(c[i] - c[i - 1]) for i in range(1, len(c))) or 1e-9
    er = net / path
    band = max(c) - min(c)
    a = float(atr14 or 0)
    # CHOP: little net progress vs path, or tiny band vs ATR
    if er <= 0.22 or (a > 0 and band < 4.0 * a):
        return {
            "label": "CHOP",
            "score": round(-min(1.0, max(0.2, (0.35 - er) * 2.5)), 3),
            "er": round(er, 3),
            "tip": "Rango/vaivén — no forzar entradas",
        }
    if er >= 0.32 and (a <= 0 or band >= 5.0 * a):
        return {
            "label": "TREND",
            "score": round(min(1.0, er * 2), 3),
            "er": round(er, 3),
            "tip": "Dirección clara — setup tiene más sentido",
        }
    return {
        "label": "MIXED",
        "score": 0.0,
        "er": round(er, 3),
        "tip": "Ni trend limpio ni chop extremo",
    }


def analyze():
    _FEED["name"] = None
    ks = fetch_klines(120)
    price = fetch_price()
    depth = fetch_depth(20)
    trades = fetch_trades(500)
    kalshi = None
    try:
        kalshi = fetch_kalshi_btc15m()
    except Exception as e:
        kalshi = {"error": str(e)}

    closes = [float(k[4]) for k in ks]
    highs = [float(k[2]) for k in ks]
    lows = [float(k[3]) for k in ks]
    volumes = [float(k[5]) for k in ks]

    last15 = ks[-15:]
    o = float(last15[0][1])
    h = max(float(k[2]) for k in last15)
    l = min(float(k[3]) for k in last15)
    c = price
    vol15 = sum(float(k[5]) for k in last15)

    e3 = ema(closes, 3)
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    rr = rsi(closes)
    vw = vwap(ks[-60:])
    a = atr(highs, lows, closes)
    avgvol = sum(volumes[-31:-1]) / 30
    vol_ratio = volumes[-1] / avgvol if avgvol else 1

    imb, bid_n, ask_n = book_imbalance(depth)
    flow = aggressor_and_whales(trades)
    rem15, window_end = window_countdown()

    spotter = max(-1, min(1, pct(price, closes[-5]) / 0.20))
    prior = 0
    prior += 0.35 if e3 > e9 else -0.35
    prior += 0.25 if price > e21 else -0.25
    prior += 0.20 if price > vw else -0.20
    prior += 0.20 if rr > 50 else -0.20

    # real book + flow agents (replaces fake whale card)
    book_score = max(-1, min(1, imb * 2))
    flow_score = max(-1, min(1, flow["flow"] * 2))
    whale_score = 0.0
    if flow["whale_count"]:
        net = flow["whale_net_usd"]
        whale_score = max(-1, min(1, net / (3 * WHALE_USD)))

    edge = 0.30 * spotter + 0.25 * prior + 0.20 * book_score + 0.15 * flow_score + 0.10 * whale_score
    if vol_ratio > 1.5:
        edge *= 1.08
    edge = max(-1, min(1, edge))

    confidence = min(99, max(1, 50 + abs(edge) * 45))

    bullish = e3 > e9 and price > vw and price > e21 and rr >= 52
    bearish = e3 < e9 and price < vw and price < e21 and rr <= 48
    book_ok_long = imb >= 0.05 or flow["flow"] >= 0.05
    book_ok_short = imb <= -0.05 or flow["flow"] <= -0.05

    if bullish and edge >= 0.28 and book_ok_long:
        signal, side = "ENTER LONG", "UP"
    elif bearish and edge <= -0.28 and book_ok_short:
        signal, side = "ENTER SHORT", "DOWN"
    else:
        signal, side = "WAIT", "WAIT"

    regime = compute_regime(closes, a)
    if regime["label"] == "CHOP" and signal != "WAIT":
        signal, side = "WAIT", "WAIT"
        regime["tip"] = "CHOP: agentes anulados — NO ENTRAR"

    # CLOSER: need 2-of-3 so it is not stuck on EXIT LONG forever in chop.
    long_votes = (1 if e3 < e9 else 0) + (1 if price < vw else 0) + (1 if rr < 45 else 0)
    short_votes = (1 if e3 > e9 else 0) + (1 if price > vw else 0) + (1 if rr > 55 else 0)
    if long_votes >= 2 and long_votes > short_votes:
        exit_signal = "EXIT LONG"
    elif short_votes >= 2 and short_votes > long_votes:
        exit_signal = "EXIT SHORT"
    elif long_votes >= 2 and short_votes >= 2:
        exit_signal = "HOLD"  # conflicting — chop, don't nag one side
    else:
        exit_signal = "HOLD"

    if a:
        long_tp, long_sl = price + 1.2 * a, price - 0.8 * a
        short_tp, short_sl = price - 1.2 * a, price + 0.8 * a
    else:
        long_tp = long_sl = short_tp = short_sl = price

    # Kalshi alignment hint (spot vs strike)
    kalshi_hint = None
    gap_pct = None
    if isinstance(kalshi, dict) and kalshi.get("strike"):
        strike = kalshi["strike"]
        gap_pct = (price - strike) / strike * 100
        if price > strike:
            kalshi_hint = "spot ABOVE strike → favor YES/UP"
        elif price < strike:
            kalshi_hint = "spot BELOW strike → favor NO/DOWN"
        else:
            kalshi_hint = "spot ≈ strike"
        kalshi["gap_pct"] = round(gap_pct, 4)
        kalshi["hint"] = kalshi_hint

    # Decision aid: Kalshi cheap vs expensive + green/red semaphore
    open_session = fetch_es_open_wick()
    EDGE_MIN = 3.0
    EDGE_OPEN = 6.0  # tighter during OPEN RISK window

    def _norm_ask(x):
        if x is None:
            return None
        try:
            v = float(x)
        except (TypeError, ValueError):
            return None
        return v / 100.0 if v > 1 else v

    decision_aid = {
        "side": "UP",
        "fair_cents": 50.0,
        "kalshi_ask_cents": None,
        "edge_cents": 0.0,
        "label": "JUSTO",
        "operate": False,
        "light": "red",
        "line": "Sin datos Kalshi — WAIT / no entrar",
    }

    kalshi_ok = (
        isinstance(kalshi, dict)
        and not kalshi.get("error")
        and kalshi.get("strike")
        and gap_pct is not None
    )
    if kalshi_ok:
        fair_yes = 0.5 + math.tanh(gap_pct / 0.08) * 0.45
        fair_yes = max(0.05, min(0.95, fair_yes))
        favor_yes = fair_yes >= 0.5
        side_da = "UP" if favor_yes else "DOWN"
        fair_cents = fair_yes * 100 if favor_yes else (1 - fair_yes) * 100
        yes_n = _norm_ask(kalshi.get("yes_ask"))
        no_n = _norm_ask(kalshi.get("no_ask"))
        ask_prob = yes_n if favor_yes else no_n
        if ask_prob is None:
            decision_aid["line"] = "Kalshi sin ask — WAIT / no entrar"
            decision_aid["side"] = side_da
            decision_aid["fair_cents"] = round(fair_cents, 1)
        else:
            kalshi_ask_cents = ask_prob * 100
            edge_cents = fair_cents - kalshi_ask_cents
            fee_cents = kalshi_fee_cents_per_contract(ask_prob)
            edge_net_cents = edge_cents - fee_cents
            if edge_cents >= 0.5:
                label = "BARATO"
            elif edge_cents <= -0.5:
                label = "CARO"
            else:
                label = "JUSTO"
            edge_need = EDGE_OPEN if open_session.get("open_risk") else EDGE_MIN
            book = fetch_kalshi_book_near_ask(
                kalshi.get("ticker"), favor_yes, ask_prob
            )
            # Green only if NET edge clears threshold AND book not thin
            operate = (
                (edge_net_cents >= edge_need)
                and bool(book.get("book_ok"))
                and regime.get("label") != "CHOP"
            )
            light = "green" if operate else "red"
            line = (
                f"{side_da} bruto {edge_cents:+.1f}¢ − fee {fee_cents:.1f}¢ "
                f"= neto {edge_net_cents:+.1f}¢ · {label} · {book.get('book_label')}"
            )
            if open_session.get("open_risk"):
                tip = f"OPEN RISK (≥{EDGE_OPEN:.0f}¢ neto)"
                if open_session.get("es_label") == "WICK_UP" and side_da == "UP":
                    tip += " · WICK_UP: no chase UP"
                line = f"{line} · {tip}"
            if regime.get("label") == "CHOP":
                line = f"{line} · REGIME CHOP"
            decision_aid = {
                "side": side_da,
                "fair_cents": round(fair_cents, 1),
                "kalshi_ask_cents": round(kalshi_ask_cents, 1),
                "edge_cents": round(edge_cents, 1),
                "fee_cents": round(fee_cents, 2),
                "edge_net_cents": round(edge_net_cents, 1),
                "book_ok": book.get("book_ok"),
                "book_label": book.get("book_label"),
                "book_contracts_near": book.get("book_contracts_near"),
                "label": label,
                "operate": operate,
                "light": light,
                "line": line,
            }
    elif isinstance(kalshi, dict) and kalshi.get("error"):
        decision_aid["line"] = f"Sin datos Kalshi ({kalshi.get('error')}) — WAIT"

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "window": {"seconds_remaining": rem15, "ends_at": window_end},
        "candle15": {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": vol15,
            "change_pct": pct(c, o),
        },
        "indicators": {
            "ema3": e3,
            "ema9": e9,
            "ema21": e21,
            "rsi": rr,
            "vwap": vw,
            "atr14_1m": a,
            "volume_ratio": vol_ratio,
        },
        "book": {
            "imbalance": round(imb, 3),
            "bid_usd_top10": round(bid_n, 0),
            "ask_usd_top10": round(ask_n, 0),
        },
        "flow": flow,
        "kalshi_btc15m": kalshi,
        "agents": {
            "spotter": round(spotter, 3),
            "prior": round(prior, 3),
            "book": round(book_score, 3),
            "flow": round(flow_score, 3),
            "whale": round(whale_score, 3),
            "edge": round(edge, 3),
            "kelly_confidence": round(confidence, 1),
            "taker": signal,
            "closer": exit_signal,
            "regime": regime.get("label"),
            "regime_score": regime.get("score"),
            "regime_er": regime.get("er"),
            "regime_tip": regime.get("tip"),
        },
        "signal": signal,
        "side": side,
        "confidence": round(confidence, 1),
        "levels": {
            "long_tp": long_tp,
            "long_sl": long_sl,
            "short_tp": short_tp,
            "short_sl": short_sl,
        },
        "notes": {
            "whales": f"Real Binance prints ≥ ${int(WHALE_USD):,} (not fake whale alerts)",
            "kalshi": "Public KXBTC15M odds; terminal does not place orders",
            "ui": "Desk-style layout; no fake swarm PnL",
        },
        "timeline": timeline_phase((kalshi or {}).get("seconds_remaining") if isinstance(kalshi, dict) else rem15),
        "probability": round(confidence, 1),
        "next_15m": side if side in {"UP", "DOWN"} else "WAIT",
        "whale_flow_usd": flow.get("whale_net_usd"),
        "swarm": {
            "agents_live": 7,
            "mode": "SIGNALS ONLY",
            "human": "YOU decide entries — no auto orders",
        },
        "decision_aid": decision_aid,
        "open_session": open_session,
        "feed": _FEED.get("name"),
    }


def loop():
    """Background refresh. Never leave the UI on blank forever."""
    with lock:
        if STATE["data"] is None and not STATE["error"]:
            STATE["error"] = "warming up…"
    while True:
        try:
            try:
                d = analyze()
            except Exception as e:
                d = analyze_fallback(str(e))
            da = d.get("decision_aid") or {}
            light_tag = "🟢" if da.get("light") == "green" else "🔴"
            line = (
                f"{d['signal']} conf={d['confidence']}% "
                f"flow={d['flow']['flow']} whales={d['flow']['whale_count']} "
                f"net={d['flow']['whale_net_usd']} "
                f"kalshi={(d.get('kalshi_btc15m') or {}).get('hint')} "
                f"{light_tag} {da.get('label') or '—'} "
                f"edge={da.get('edge_cents')}"
            )
            if d.get("fallback"):
                line = f"FALLBACK {line}"
            with lock:
                STATE["data"] = d
                STATE["error"] = None
                STATE["updated"] = time.time()
                ACTIVITY.append({"ts": d["timestamp"], "line": line})
                del ACTIVITY[:-40]
                d["activity"] = list(reversed(ACTIVITY[-12:]))
                STATE["data"] = d
        except Exception as e:
            with lock:
                STATE["error"] = str(e)
        time.sleep(5)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def data():
    _start_loop_once()
    with lock:
        d, err = STATE["data"], STATE["error"]
        updated = STATE.get("updated") or 0
    age = time.time() - float(updated or 0)
    # Refresh if empty, fallback, OR stale (loop wedged — was freezing WINDOW at 10:43).
    need = (
        d is None
        or age > 20
        or (isinstance(d, dict) and d.get("fallback") and age > 10)
    )
    if need:
        try:
            try:
                d2 = analyze()
            except Exception as e:
                d2 = analyze_fallback(str(e))
            with lock:
                STATE["data"] = d2
                STATE["error"] = None
                STATE["updated"] = time.time()
                if not d2.get("activity"):
                    d2["activity"] = [{"ts": d2["timestamp"], "line": f"sync feed={d2.get('feed')} age={age:.0f}s"}]
                    STATE["data"] = d2
                d, err = STATE["data"], STATE["error"]
        except Exception as e:
            err = err or str(e)
    return jsonify({"data": d, "error": err})


def _start_loop_once():
    if getattr(app, "_omega_loop_started", False):
        return
    app._omega_loop_started = True
    threading.Thread(target=loop, daemon=True, name="omega-loop").start()


@app.before_request
def _ensure_loop():
    _start_loop_once()


if __name__ == "__main__":
    _start_loop_once()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)

#_eager_start for gunicorn workers
_start_loop_once()
