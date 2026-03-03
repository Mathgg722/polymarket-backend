from fastapi import FastAPI
import requests

app = FastAPI()

BASE_URL = "https://gamma-api.polymarket.com/markets"


def get_all_active_markets():
    try:
        all_markets = []
        limit = 100

        # 🔒 Pegamos só até 5 páginas (máx 500 mercados)
        for page in range(5):
            offset = page * limit
            url = f"{BASE_URL}?active=true&limit={limit}&offset={offset}"
            response = requests.get(url, timeout=10)
            markets = response.json()

            if not markets:
                break

            all_markets.extend(markets)

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