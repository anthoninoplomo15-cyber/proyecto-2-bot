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

from bot import build_loser_plan


app = Flask(__name__)

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY", "").replace("\\n", "\n").strip()

# Proyecto 2 permanece 100% PAPER. Este archivo no tiene ninguna llamada para
# crear, modificar o cancelar ordenes y nunca mueve dinero real.
SETTINGS = {
    "mode": "PAPER",
    "live_trading": False,
    "test_version": 6,
    "test_bankroll": 14.00,
    "max_total_cost_per_crypto": 1.00,
    "entry_trigger": 0.40,
    "net_profit_target": 0.05,
    "stop_loss": None,
    "hold_to_settlement_if_no_target": True,
    "max_open_trades": 14,
    "continuous_operation": True,
    "intervals_per_day": 96,
    "maximum_daily_opportunities": 1344,
    "entry_window_seconds_remaining": [600, 905],
    "poll_seconds": 3,
}

SERIES = [
    "KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M",
    "KXDOGE15M", "KXBNB15M", "KXADA15M", "KXLINK15M",
    "KXAVAX15M", "KXLTC15M", "KXBCH15M", "KXDOT15M",
    "KXHYPE15M", "KXSUI15M",
]

CONNECTION_CACHE = {"updated": 0.0, "value": None}


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
    return markets[:1]


def scan_markets():
    found = []
    with ThreadPoolExecutor(max_workers=7) as pool:
        jobs = {pool.submit(scan_one_series, ticker): ticker for ticker in SERIES}
        for job in as_completed(jobs):
            try:
                found.extend(job.result())
            except requests.RequestException:
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
        item["yes_plan"] = build_loser_plan("yes", yes_ask)
        item["no_plan"] = build_loser_plan("no", no_ask)
        enriched.append(item)
    return enriched


@app.get("/health")
def health():
    return {
        "ok": True,
        "project": "Proyecto 2",
        "mode": "PAPER",
        "version": 6,
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
    except requests.RequestException:
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
  <title>Proyecto 2 · Versión 6</title>
  <style>
    :root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:#07111f;
    color:#eef4ff;font-family:system-ui,Arial}.wrap{max-width:1080px;margin:auto;padding:18px}
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
  <h1>Proyecto 2 · Versión 6</h1>
  <div class="tag">MODO PRUEBA · ESTRATEGIA DEL LADO PERDEDOR</div>

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
      <div class="value">Primer lado ≤40¢</div></div>
    <div class="card"><div class="label">Máximo por cripto</div>
      <div class="value">$1 con fee</div></div>
    <div class="card"><div class="label">Objetivo neto</div>
      <div class="value">+$0.05</div></div>
    <div class="card"><div class="label">Stop loss</div>
      <div class="value negative">Ninguno</div></div>
    <div class="card"><div class="label">Ventana de entrada</div>
      <div class="value">Primeros 5 min</div></div>
  </div>

  <div class="note"><strong>Regla automática de esta prueba.</strong> Durante los
  primeros 5 minutos de cada contrato de 15 minutos, compra el primer lado cuyo
  <em>ask</em> ejecutable llegue a 40¢ o menos. Usa hasta $1 por criptomoneda,
  incluyendo la tarifa. Vende al <em>bid</em> real únicamente cuando la ganancia
  calculada después de ambas tarifas sea por lo menos +$0.05. Si nunca llega,
  conserva la posición hasta el resultado. Solo puede usar una vez cada cripto
  en cada intervalo. Vigila las 14 criptomonedas en los 96 intervalos del día,
  sin límite diario ni pausa automática por cantidad de operaciones.</div>

  <div class="note risk"><strong>Riesgo importante.</strong> No tener stop loss
  permite perder casi todo el dólar. Una pérdida de $1 consume aproximadamente
  veinte objetivos de 5¢. Esta prueba mide la idea; no demuestra que sea rentable
  ni coloca dinero real. Mantén esta pestaña abierta para que siga registrando.</div>

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
    <thead><tr><th>Cripto</th><th>UP compra</th><th>DOWN compra</th><th>Tiempo</th><th>Estado</th><th>Plan</th></tr></thead>
    <tbody id="rows"><tr><td colspan="6">Buscando mercados…</td></tr></tbody>
  </table></div>

  <h2>Operaciones simuladas abiertas</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>Cripto</th><th>Lado</th><th>Entrada</th><th>Bid actual</th><th>P&L neto</th><th>Meta estimada</th><th>Tiempo</th></tr></thead>
    <tbody id="open-rows"><tr><td colspan="7">Ninguna</td></tr></tbody>
  </table></div>

  <h2>Últimos resultados</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>Cripto</th><th>Lado</th><th>Entrada</th><th>Salida</th><th>Neto</th><th>Mejor P&L</th><th>Motivo</th></tr></thead>
    <tbody id="closed-rows"><tr><td colspan="7">Todavía no hay resultados</td></tr></tbody>
  </table></div>

  <div id="updated" class="foot"></div>
</div>

<script>
const PAPER_KEY='proyecto2_paper_v6_all_intervals';
const START_BANKROLL=14.00;
const MAX_OPEN=14;
const ENTRY_TRIGGER=0.40;
const NET_TARGET=0.05;
const ENTRY_MIN_SECONDS=600;
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

function priceText(value){
  const number=Number(value);
  return Number.isFinite(number)?Math.round(number*100)+'¢':'—';
}

function sideText(side){return side==='yes'?'UP':'DOWN';}

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
  return {version:'6-all',active:false,cash:START_BANKROLL,open:[],closed:[],seen:[]};
}

function loadPaper(){
  try{
    const saved=JSON.parse(localStorage.getItem(PAPER_KEY));
    if(!saved||saved.version!=='6-all'||!Array.isArray(saved.open)
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
    pnl:result.pnl,reason,closedAt:new Date().toISOString(),
    peakPnl:Math.max(Number(position.peakPnl??result.pnl),result.pnl),
    lowestPnl:Math.min(Number(position.lowestPnl??result.pnl),result.pnl)});
}

function settlePosition(position,result){
  const won=position.side===String(result).toLowerCase();
  const proceeds=won?position.contracts:0;
  const pnl=roundNumber(proceeds-position.cost);
  paper.cash=roundNumber(paper.cash+proceeds);
  paper.closed.push({...position,exitPrice:won?1:0,exitFee:0,pnl,
    reason:won?'RESULTADO GANADOR':'RESULTADO PERDEDOR',
    closedAt:new Date().toISOString(),
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
    const position={...original,lastExitPrice:price,lastPnl:result.pnl,
      peakPnl:original.peakPnl==null?result.pnl:Math.max(original.peakPnl,result.pnl),
      lowestPnl:original.lowestPnl==null?result.pnl:Math.min(original.lowestPnl,result.pnl)};

    if(result.pnl+1e-9>=NET_TARGET){
      closeAtPrice(position,price,'OBJETIVO NETO +$0.05');
    }else{
      // No hay stop ni salida por tiempo: se conserva hasta la meta o el resultado.
      remaining.push(position);
    }
  }

  paper.open=remaining;
}

function activePlans(market){
  return [market.yes_plan,market.no_plan].filter(plan=>
    plan&&plan.action&&plan.action!=='WAIT'&&Number(plan.entry_price)<=ENTRY_TRIGGER+1e-9
  );
}

function openCandidates(markets){
  if(!paper.active){return;}

  const candidates=markets
    .filter(market=>market.ticker&&!paper.seen.includes(market.ticker)
      &&validEntryTime(market)&&activePlans(market).length>0)
    .sort((a,b)=>secondsLeft(b)-secondsLeft(a));

  for(const market of candidates){
    const plans=activePlans(market);
    if(plans.length!==1||paper.open.length>=MAX_OPEN){continue;}

    const plan=plans[0];
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
      targetNet:Number(plan.target_net),
      estimatedTargetPrice:Number(plan.estimated_target_price),
      closeTime:market.close_time,
      secondsLeftAtEntry:roundNumber(remaining,2),
      entryYesBid:Number(market.yes_bid),
      entryYesAsk:Number(market.yes_ask),
      entryNoBid:Number(market.no_bid),
      entryNoAsk:Number(market.no_ask),
      lastExitPrice:null,
      lastPnl:null,
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
    'pnl_net','reason','entry_trigger','target_net','estimated_target_price',
    'seconds_left_at_entry','entry_yes_bid','entry_yes_ask','entry_no_bid','entry_no_ask',
    'peak_pnl','lowest_pnl'];
  const rows=paper.closed.map(trade=>[
    trade.openedAt,trade.closedAt,trade.closeTime,trade.series,trade.ticker,trade.side,
    trade.contracts,trade.entryPrice,trade.exitPrice,trade.entryFee,trade.exitFee,
    trade.cost,trade.pnl,trade.reason,ENTRY_TRIGGER,trade.targetNet,
    trade.estimatedTargetPrice,trade.secondsLeftAtEntry,trade.entryYesBid,
    trade.entryYesAsk,trade.entryNoBid,trade.entryNoAsk,trade.peakPnl,trade.lowestPnl,
  ]);
  const csv=[headers,...rows].map(row=>row.map(csvCell).join(','))
    .join(String.fromCharCode(13,10));
  const blob=new Blob(['\ufeff'+csv],{type:'text/csv;charset=utf-8'});
  const link=document.createElement('a');
  link.href=URL.createObjectURL(blob);
  link.download='proyecto2_v6_lado_perdedor_'+new Date().toISOString().slice(0,10)+'.csv';
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
    <td class="${Number(position.lastPnl)>=0?'positive':'negative'}">${position.lastPnl==null?'—':money(position.lastPnl)}</td>
    <td>${priceText(position.estimatedTargetPrice)}</td>
    <td>${countdown((Date.parse(position.closeTime)-Date.now())/1000)}</td></tr>`).join('')
    ||'<tr><td colspan="7">Ninguna</td></tr>';

  document.getElementById('closed-rows').innerHTML=paper.closed.slice(-12).reverse()
    .map(trade=>`<tr><td>${cryptoName(trade.series)}</td><td>${sideText(trade.side)}</td>
    <td>${priceText(trade.entryPrice)}</td><td>${priceText(trade.exitPrice)}</td>
    <td class="${trade.pnl>=0?'positive':'negative'}">${money(trade.pnl)}</td>
    <td>${trade.peakPnl==null?'—':money(trade.peakPnl)}</td><td>${trade.reason}</td></tr>`)
    .join('')||'<tr><td colspan="7">Todavía no hay resultados</td></tr>';
}

function renderMarkets(markets){
  document.getElementById('rows').innerHTML=markets.map(market=>{
    const remaining=secondsLeft(market);
    const plans=activePlans(market);
    const openPosition=paper.open.find(position=>position.ticker===market.ticker);
    const used=paper.seen.includes(market.ticker);
    let css='wait';
    let label='ESPERANDO 40¢';

    if(openPosition){
      css=openPosition.side==='yes'?'yes':'no';
      label='ABIERTA '+sideText(openPosition.side);
    }else if(used){
      label='INTERVALO USADO';
    }else if(!validEntryTime(market)){
      label=remaining>ENTRY_MAX_SECONDS?'AÚN NO COMIENZA':'FUERA DE VENTANA';
    }else if(plans.length===1){
      css=plans[0].side==='yes'?'yes':'no';
      label=(paper.active?'ENTRADA ':'40¢ DETECTADO ')+sideText(plans[0].side);
    }else if(plans.length>1){
      label='PRECIO AMBIGUO';
    }

    const plan=plans.length===1?plans[0]:null;
    const planText=plan
      ?Number(plan.contracts).toFixed(2)+' contratos · $'+Number(plan.cost).toFixed(2)
        +' · meta aprox. '+priceText(plan.estimated_target_price)
      :'Primero que llegue a 40¢';
    return `<tr><td>${cryptoName(market.series)}</td>
      <td>${priceText(market.yes_ask)}</td><td>${priceText(market.no_ask)}</td>
      <td>${countdown(remaining)}</td><td><span class="pill ${css}">${label}</span></td>
      <td>${planText}</td></tr>`;
  }).join('')||'<tr><td colspan="6">No hay mercados abiertos ahora</td></tr>';
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
