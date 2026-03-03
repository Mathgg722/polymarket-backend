from fastapi import FastAPI
import requests
from datetime import datetime, timezone

app = FastAPI()

CLOB_URL = "https://clob.polymarket.com/markets?limit=1000"


def get_open_markets():
    try:
        response = requests.get(CLOB_URL)
        data = response.json()

        markets = data.get("data", [])

        now = datetime.now(timezone.utc)

        open_markets = []

        for market in markets:
            end_date = market.get("end_date_iso")

            if not end_date:
                continue

            try:
                end_dt = datetime.fromisoformat(
                    end_date.replace("Z", "+00:00")
                )
            except:
                continue

            # ✅ REGRA SIMPLES: mercado aberto = data no futuro
            if end_dt > now:
                open_markets.append({
                    "question": market.get("question"),
                    "end_date": end_date,
                    "slug": market.get("market_slug"),
                    "tokens": market.get("tokens")
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