import time
import threading
import requests
import psycopg2
import os
import json
from fastapi import FastAPI

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")


def save_snapshot(market_id, yes_price, no_price, volume):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id SERIAL PRIMARY KEY,
            market_id TEXT,
            yes_price FLOAT,
            no_price FLOAT,
            volume FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        INSERT INTO market_snapshots (market_id, yes_price, no_price, volume)
        VALUES (%s, %s, %s, %s);
    """, (market_id, yes_price, no_price, volume))

    conn.commit()
    cur.close()
    conn.close()


def collect_markets():
    try:
        response = requests.get("https://gamma-api.polymarket.com/markets")
        markets = response.json()

        for market in markets:

            # 🔥 FILTRO CORRETO
            if market.get("closed") is True:
                continue

            market_id = market.get("id")
            volume = float(market.get("liquidity", 0))

            outcomes_raw = market.get("outcomes", "[]")
            prices_raw = market.get("outcomePrices", "[]")

            outcomes = json.loads(outcomes_raw)
            prices = json.loads(prices_raw)

            if len(prices) != 2:
                continue

            yes_price = float(prices[0])
            no_price = float(prices[1])

            save_snapshot(market_id, yes_price, no_price, volume)

        print("Snapshot salvo com sucesso")

    except Exception as e:
        print("Erro ao coletar:", e)


def background_collector():
    while True:
        collect_markets()
        time.sleep(60)


@app.on_event("startup")
def start_background_task():
    thread = threading.Thread(target=background_collector)
    thread.daemon = True
    thread.start()


@app.get("/")
def health():
    return {"status": "running"}