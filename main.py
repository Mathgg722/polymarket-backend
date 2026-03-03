from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import threading
import time
import json
from datetime import datetime, timezone
import os
import psycopg2

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# DATABASE SAFE CONNECTION
# ==============================

DATABASE_URL = os.getenv("DATABASE_URL")

conn = None
cursor = None

if DATABASE_URL:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_prices (
            market_id TEXT,
            question TEXT,
            yes_price FLOAT,
            no_price FLOAT,
            volume FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        print("Database connected successfully")

    except Exception as e:
        print("Database connection failed:", e)
else:
    print("DATABASE_URL not found")

# ==============================
# CONFIG
# ==============================

NEWS_API_KEY = "a595b3e7d7a047fda7a934162cf9c3ad"

cache = {
    "markets": [],
    "signals": [],
    "last_update": None
}

# ==============================
# FETCH MARKETS
# ==============================

def fetch_markets_data():
    try:
        url = "https://gamma-api.polymarket.com/markets?limit=50&order=volume24hr&ascending=false&active=true"
        response = requests.get(url, timeout=10)
        markets_data = response.json()

        markets = []

        for m in markets_data:
            yes_price = 50.0
            no_price = 50.0

            outcome_prices = m.get("outcomePrices")
            if outcome_prices:
                try:
                    prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                    if len(prices) >= 2:
                        yes_price = round(float(prices[0]) * 100, 2)
                        no_price = round(float(prices[1]) * 100, 2)
                except:
                    pass

            market = {
                "id": m.get("id"),
                "question": m.get("question"),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": m.get("volume"),
            }

            markets.append(market)

            # Save history if DB exists
            if cursor:
                cursor.execute("""
                    INSERT INTO market_prices (market_id, question, yes_price, no_price, volume)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    market["id"],
                    market["question"],
                    yes_price,
                    no_price,
                    market["volume"]
                ))

        if conn:
            conn.commit()

        return markets

    except Exception as e:
        print("Error fetching markets:", e)
        return []

# ==============================
# SIMPLE SIGNAL LOGIC
# ==============================

def analyze_signals(markets):
    signals = []

    for market in markets:
        yes_prob = market["yes_price"] / 100

        if yes_prob > 0.60:
            signals.append({
                "market": market["question"],
                "direction": "YES",
                "probability": market["yes_price"]
            })

        if yes_prob < 0.40:
            signals.append({
                "market": market["question"],
                "direction": "NO",
                "probability": market["no_price"]
            })

    return signals

# ==============================
# BACKGROUND LOOP
# ==============================

def update_cache():
    while True:
        markets = fetch_markets_data()
        signals = analyze_signals(markets)

        cache["markets"] = markets
        cache["signals"] = signals
        cache["last_update"] = datetime.utcnow().isoformat()

        time.sleep(900)

threading.Thread(target=update_cache, daemon=True).start()

# ==============================
# ENDPOINTS
# ==============================

@app.get("/")
def root():
    return {"status": "ok", "last_update": cache["last_update"]}

@app.get("/markets")
def get_markets():
    if not cache["markets"]:
        cache["markets"] = fetch_markets_data()
    return cache["markets"]

@app.get("/signals")
def get_signals():
    return cache["signals"]

@app.get("/history/{market_id}")
def get_history(market_id: str):
    if not cursor:
        return {"error": "Database not connected"}

    cursor.execute("""
        SELECT yes_price, no_price, volume, timestamp
        FROM market_prices
        WHERE market_id = %s
        ORDER BY timestamp DESC
        LIMIT 50
    """, (market_id,))
    
    rows = cursor.fetchall()
    
    return [
        {
            "yes_price": r[0],
            "no_price": r[1],
            "volume": r[2],
            "timestamp": r[3]
        }
        for r in rows
    ]
