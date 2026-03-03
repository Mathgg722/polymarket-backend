from fastapi import FastAPI
import requests
import time

app = FastAPI()

BASE_URL = "https://gamma-api.polymarket.com/markets"


def get_all_active_markets():
    try:
        all_markets = []
        offset = 0
        limit = 100

        while True:
            url = f"{BASE_URL}?active=true&limit={limit}&offset={offset}"
            response = requests.get(url)
            markets = response.json()

            if not markets:
                break

            all_markets.extend(markets)
            offset += limit

            time.sleep(0.2)  # evita rate limit

        return all_markets

    except Exception as e:
        print("Erro ao coletar:", e)
        return []


@app.get("/")
def home():
    return {"status": "ok"}


@app.get("/status")
def status():
    markets = get_all_active_markets()
    return {
        "open_markets": len(markets)
    }


@app.get("/markets")
def markets():
    return get_all_active_markets()