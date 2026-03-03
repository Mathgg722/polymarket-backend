from fastapi import FastAPI
import requests

app = FastAPI()

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
            markets.append({
                "question": m.get("question"),
                "volume": m.get("volume"),
                "liquidity": m.get("liquidity"),
                "end_date": m.get("endDate"),
            })
        return markets
    except Exception as e:
        return {"error": str(e)}
from fastapi import FastAPI
import requests

app = FastAPI()

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
            markets.append({
                "question": m.get("question"),
                "volume": m.get("volume"),
                "liquidity": m.get("liquidity"),
                "end_date": m.get("endDate"),
            })
        return markets
    except Exception as e:
        return {"error": str(e)}