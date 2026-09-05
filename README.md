# OMEGA 15M LIVE TERMINAL

Panel de señales BTC (Binance). No opera Kalshi ni coloca órdenes.

## Local
pip install -r requirements.txt
python app.py

## Render
1. Web Service desde GitHub
2. Root: raíz del repo (app.py, Procfile, requirements.txt juntos)
3. Build: pip install -r requirements.txt
4. Start: Procfile (web: gunicorn app:app)
5. Si falla: Manual Deploy → Clear build cache & deploy

Fix: el loop de datos arranca también bajo gunicorn (antes solo con python app.py). Falta gunicorn en requirements era el otro error típico.

Nota: usa api.binance.us primero porque api.binance.com da 451 en muchos servidores de EE.UU. (Render).
