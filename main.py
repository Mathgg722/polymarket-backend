from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import threading
import time
import json
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

NEWS_API_KEY = "a595b3e7d7a047fda7a934162cf9c3ad"
EDGE_THRESHOLD = 0.08

cache = {
    "markets": [],
    "news": [],
    "signals": [],
    "last_update": None
}

def fetch_markets_data():
    try:
        url = "https://gamma-api.polymarket.com/markets?limit=100&order=volume24hr&ascending=false&active=true"
        response = requests.get(url, timeout=10)
        markets_data = response.json()

        markets = []
        for m in markets_data:
            yes_price = 50.0
            no_price = 50.0

            # outcomePrices vem como string JSON ex: "[\"0.9995\", \"0.0005\"]"
            outcome_prices = m.get("outcomePrices")
            if outcome_prices:
                try:
                    if isinstance(outcome_prices, str):
                        prices = json.loads(outcome_prices)
                    else:
                        prices = outcome_prices
                    if len(prices) >= 2:
                        yes_price = round(float(prices[0]) * 100, 1)
                        no_price = round(float(prices[1]) * 100, 1)
                except:
                    pass

            # Fallback para lastTradePrice
            if yes_price == 50.0:
                last_trade = m.get("lastTradePrice")
                if last_trade:
                    try:
                        yes_price = round(float(last_trade) * 100, 1)
                        no_price = round(100 - yes_price, 1)
                    except:
                        pass

            markets.append({
                "id": m.get("id"),
                "question": m.get("question"),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": m.get("volume"),
                "liquidity": m.get("liquidity"),
                "end_date": m.get("endDate"),
                "volume_24hr": m.get("volume24hr"),
                "price_change_24h": m.get("oneDayPriceChange"),
            })

        return markets
    except Exception as e:
        return [{"error": str(e)}]

def fetch_news_data():
    url = f"https://newsapi.org/v2/top-headlines?language=en&pageSize=50&apiKey={NEWS_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        news = []
        for article in data.get("articles", []):
            news.append({
                "title": article.get("title"),
                "description": article.get("description"),
                "source": article.get("source", {}).get("name"),
                "published_at": article.get("publishedAt"),
                "url": article.get("url"),
            })
        return news
    except Exception as e:
        return []

def analyze_signals(markets, news):
    signals = []
    keywords_yes = ["win", "approved", "confirmed", "rises", "increases", "launches", "passes", "beats", "surge", "gains"]
    keywords_no = ["fails", "loses", "denied", "rejected", "falls", "decreases", "cancels", "drops", "miss", "crash"]

    for market in markets:
        question = market.get("question", "").lower()
        yes_price = market.get("yes_price", 50)
        no_price = market.get("no_price", 50)

        if yes_price == 50.0 and no_price == 50.0:
            continue

        for article in news:
            title = (article.get("title") or "").lower()
            description = (article.get("description") or "").lower()
            content = title + " " + description

            question_words = set(question.split())
            content_words = set(content.split())
            overlap = question_words & content_words

            if len(overlap) < 2:
                continue

            yes_score = sum(1 for k in keywords_yes if k in content)
            no_score = sum(1 for k in keywords_no if k in content)

            if yes_score == 0 and no_score == 0:
                continue

            direction = "YES" if yes_score >= no_score else "NO"
            current_prob = yes_price / 100 if direction == "YES" else no_price / 100
            adjustment = abs(yes_score - no_score) * 0.05
            estimated_prob = min(0.99, max(0.01, current_prob + adjustment))
            edge = round(abs(estimated_prob - current_prob), 3)

            if edge >= EDGE_THRESHOLD:
                signals.append({
                    "market": market.get("question"),
                    "news_title": article.get("title"),
                    "news_source": article.get("source"),
                    "direction": direction,
                    "current_probability": round(current_prob * 100, 1),
                    "estimated_probability": round(estimated_prob * 100, 1),
                    "edge": round(edge * 100, 1),
                    "confidence": round(min(0.95, 0.5 + edge), 2),
                    "generated_at": datetime.utcnow().isoformat(),
                })

    signals.sort(key=lambda x: x["edge"], reverse=True)
    return signals[:20]

def update_cache():
    while True:
        markets = fetch_markets_data()
        news = fetch_news_data()
        signals = analyze_signals(markets, news)
        cache["markets"] = markets
        cache["news"] = news
        cache["signals"] = signals
        cache["last_update"] = datetime.utcnow().isoformat()
        time.sleep(1800)

threading.Thread(target=update_cache, daemon=True).start()

@app.get("/")
def root():
    return {"status": "ok", "last_update": cache["last_update"]}

@app.get("/markets")
def get_markets():
    if not cache["markets"]:
        cache["markets"] = fetch_markets_data()
    return cache["markets"]

@app.get("/news")
def get_news():
    if not cache["news"]:
        cache["news"] = fetch_news_data()
    return cache["news"]

@app.get("/signals")
def get_signals():
    if not cache["signals"]:
        markets = cache["markets"] or fetch_markets_data()
        news = cache["news"] or fetch_news_data()
        cache["signals"] = analyze_signals(markets, news)
    return cache["signals"]

@app.get("/status")
def get_status():
    return {
        "last_update": cache["last_update"],
        "total_markets": len(cache["markets"]),
        "total_news": len(cache["news"]),
        "total_signals": len(cache["signals"]),
    }