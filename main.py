from fastapi import FastAPI
import requests

app = FastAPI()

@app.get("/")
def root():
    return {"status": "Backend rodando com dados reais do Polymarket"}

@app.get("/markets")
def get_markets():
    url = "https://gamma-api.polymarket.com/markets?active=true"

    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        markets = []

        for m in data[:10]:
            markets.append({
                "question": m.get("question"),
                "volume": m.get("volume"),
                "liquidity": m.get("liquidity"),
                "end_date": m.get("endDate"),
                "active": m.get("active"),
            })

        return markets

    except Exception as e:
        return {"error": str(e)}