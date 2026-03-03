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
        response = requests.get("https://clob.polymarket.com/markets")
        markets = response.json()

        for market in markets[:20]:

            if market.get("closed") is True:
                continue

            market_id = market.get("condition_id")
            volume = float(market.get("volume", 0))

            yes_price = float(market.get("best_bid", 0))
            no_price = float(market.get("best_ask", 0))

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