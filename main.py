from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/markets")
def get_markets():
    url = "https://gamma-api.polymarket.com/markets?limit=20&order=volume24hr&ascending=false&active=true"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        markets = []
        for m in data:
            prices = m.get("outcomePrices", ["0.5", "0.5"])
            try:
                yes_price = round(float(prices[0]) * 100, 1)
                no_price = round(float(prices[1]) * 100, 1)
            except:
                yes_price = 50.0
                no_price = 50.0
            markets.append({
                "question": m.get("question"),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": m.get("volume"),
                "liquidity": m.get("liquidity"),
                "end_date": m.get("endDate"),
            })
        return markets
    except Exception as e:
        return {"error": str(e)}