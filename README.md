# OMEGA 15M LIVE TERMINAL

This version is NOT connected to Kalshi and does NOT simulate trades.

It reads real BTC/USDT market data from Binance public endpoints and displays a decision terminal:
- SPOTTER
- PRIOR
- EDGE
- KELLY (confidence score)
- TAKER (entry signal)
- CLOSER (exit signal)
- EMA 3/9/21
- VWAP
- RSI 14
- volume ratio
- ATR-based reference levels

Signals:
- ENTER LONG
- ENTER SHORT
- WAIT
- EXIT LONG / EXIT SHORT

The terminal refreshes every 5 seconds.

Important: signals are technical rules, not guaranteed predictions. The system does not place trades.

Run:
pip install -r requirements.txt
python app.py

For Render/Railway, the included Procfile can be used with:
web: gunicorn app:app
