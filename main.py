import time
import threading
import requests
import psycopg2
import os
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

        for market in markets[:20]:
            market_id = market.get("id")
            volume = float(market.get("volume", 0))

            outcomes = market.get("outcomes", [])
            outcome_prices = market.get("outcomePrices", [])

            yes_price = 0
            no_price = 0

            if len(outcomes) == 2 and len(outcome_prices) == 2:
                yes_price = float(outcome_prices[0])
                no_price = float(outcome_prices[1])

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