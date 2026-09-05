import os, time, threading, math
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template
import requests

app = Flask(__name__)

BINANCE = "https://api.binance.com"
lock = threading.Lock()
STATE = {"data": None, "error": None, "updated": None}

def fetch_klines(limit=120):
    r = requests.get(
        BINANCE + "/api/v3/klines",
        params={"symbol":"BTCUSDT","interval":"1m","limit":limit},
        timeout=5
    )
    r.raise_for_status()
    return r.json()

def fetch_price():
    r = requests.get(
        BINANCE + "/api/v3/ticker/price",
        params={"symbol":"BTCUSDT"},
        timeout=5
    )
    r.raise_for_status()
    return float(r.json()["price"])

def ema(values, period):
    if len(values) < period:
        return sum(values)/len(values)
    k = 2/(period+1)
    e = sum(values[:period])/period
    for v in values[period:]:
        e = v*k + e*(1-k)
    return e

def rsi(values, period=14):
    if len(values) < period+1:
        return 50.0
    gains, losses = [], []
    for a,b in zip(values[-period-1:-1], values[-period:]):
        d = b-a
        gains.append(max(d,0))
        losses.append(max(-d,0))
    ag = sum(gains)/period
    al = sum(losses)/period
    if al == 0:
        return 100.0
    return 100 - (100/(1 + ag/al))

def atr(highs, lows, closes, period=14):
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(
            highs[i]-lows[i],
            abs(highs[i]-closes[i-1]),
            abs(lows[i]-closes[i-1])
        ))
    if not trs: return 0
    return sum(trs[-period:])/min(period,len(trs))

def vwap(klines):
    pv=0; vol=0
    for k in klines:
        h,l,c,v = map(float, [k[2],k[3],k[4],k[5]])
        typical=(h+l+c)/3
        pv += typical*v
        vol += v
    return pv/vol if vol else float(k[4])

def pct(a,b):
    return ((a-b)/b*100) if b else 0

def analyze():
    ks = fetch_klines(120)
    price = fetch_price()
    closes=[float(k[4]) for k in ks]
    highs=[float(k[2]) for k in ks]
    lows=[float(k[3]) for k in ks]
    volumes=[float(k[5]) for k in ks]

    # 15-minute structure built from the 1-minute candles.
    # Current 15m candle = the last 15 one-minute candles.
    last15 = ks[-15:]
    o=float(last15[0][1]); h=max(float(k[2]) for k in last15)
    l=min(float(k[3]) for k in last15); c=price
    vol15=sum(float(k[5]) for k in last15)

    e3=ema(closes,3); e9=ema(closes,9)
    e21=ema(closes,21)
    rr=rsi(closes)
    vw=vwap(ks[-60:])
    a=atr(highs,lows,closes)
    avgvol=sum(volumes[-31:-1])/30
    vol_ratio=volumes[-1]/avgvol if avgvol else 1

    # Agent scores - transparent and based on real market data.
    spotter = max(-1,min(1,pct(price,closes[-5])/0.20))
    prior = 0
    prior += 0.35 if e3>e9 else -0.35
    prior += 0.25 if price>e21 else -0.25
    prior += 0.20 if price>vw else -0.20
    prior += 0.20 if rr>50 else -0.20

    edge = 0.45*spotter + 0.55*prior
    if vol_ratio > 1.5:
        edge *= 1.10
    edge=max(-1,min(1,edge))

    confidence = min(99,max(1,50 + abs(edge)*45))

    # Entry requires agreement; exit is triggered by reversal / loss of structure.
    bullish = e3>e9 and price>vw and price>e21 and rr>=52
    bearish = e3<e9 and price<vw and price<e21 and rr<=48

    if bullish and edge >= .30:
        signal="ENTER LONG"
        side="UP"
    elif bearish and edge <= -.30:
        signal="ENTER SHORT"
        side="DOWN"
    else:
        signal="WAIT"
        side="WAIT"

    # Exit guidance for a position already held.
    long_exit = e3<e9 or price<vw or rr<45
    short_exit = e3>e9 or price>vw or rr>55
    exit_signal = "EXIT LONG" if long_exit else ("EXIT SHORT" if short_exit else "HOLD")

    # Levels are reference levels, not guarantees.
    entry=price
    if a:
        long_tp=price + 1.2*a
        long_sl=price - 0.8*a
        short_tp=price - 1.2*a
        short_sl=price + 0.8*a
    else:
        long_tp=long_sl=short_tp=short_sl=price

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "candle15": {"open":o,"high":h,"low":l,"close":c,"volume":vol15,
                     "change_pct":pct(c,o)},
        "indicators":{
            "ema3":e3,"ema9":e9,"ema21":e21,"rsi":rr,"vwap":vw,
            "atr14_1m":a,"volume_ratio":vol_ratio
        },
        "agents":{
            "spotter":round(spotter,3),
            "prior":round(prior,3),
            "edge":round(edge,3),
            "kelly_confidence":round(confidence,1),
            "taker":signal,
            "closer":exit_signal
        },
        "signal":signal,
        "side":side,
        "confidence":round(confidence,1),
        "levels":{
            "long_tp":long_tp,"long_sl":long_sl,
            "short_tp":short_tp,"short_sl":short_sl
        }
    }

def loop():
    while True:
        try:
            d=analyze()
            with lock:
                STATE["data"]=d
                STATE["error"]=None
                STATE["updated"]=time.time()
        except Exception as e:
            with lock: STATE["error"]=str(e)
        time.sleep(5)

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/data")
def data():
    with lock:
        return jsonify({"data":STATE["data"],"error":STATE["error"]})

if __name__=="__main__":
    threading.Thread(target=loop,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
