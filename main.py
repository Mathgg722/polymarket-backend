from fastapi import FastAPI
import requests

app = FastAPI()

GAMMA_URL = "https://gamma-api.polymarket.com/markets?active=true"


def get_open_markets():
    try:
        response = requests.get(GAMMA_URL)
        markets = response.json()

        open_markets = []

        for market in markets:
            open_markets.append({
                "question": market.get("question"),
                "slug": market.get("slug"),
                "end_date": market.get("endDate"),
            })

        return open_markets

    except Exception as e:
        print("Erro ao coletar:", e)
        return []


@app.get("/")
def home():
    return {"status": "ok"}


@app.get("/status")
def status():
    markets = get_open_markets()
    return {
        "open_markets": len(markets)
    }


@app.get("/markets")
def markets():
    return get_open_markets()