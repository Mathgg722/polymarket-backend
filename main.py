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
EDGE_THRESHOLD = 0.05

STOPWORDS = {"will", "the", "a", "an", "in", "on", "at", "to", "for", "of", "and", "or", "is", "be", "by", "as", "it", "its", "this", "that", "with", "from", "are", "was", "were", "has", "have", "had", "not", "but", "what", "who", "how", "when", "up", "down", "out", "win", "vs"}

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
            outcome_prices = m.get("outcomePrices")
            if outcome_prices:
                try:
                    prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                    if len(prices) >= 2:
                        yes_price = round(float(prices[0]) * 100, 1)
                        no_price = round(float(prices[1]) * 100, 1)
                except:
                    pass
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

def extract_keywords(text):
    words = text.lower().replace("?", "").replace(",", "").replace(".", "").split()
    return {w for w in words if len(w) > 3 and w not in STOPWORDS}

def analyze_signals(markets, news):
    signals = []
    keywords_positive = ["win", "approved", "confirmed", "rises", "increases", "launches", "passes", "beats", "surge", "gains", "attack", "strike", "hit", "yes", "success"]
    keywords_negative = ["fails", "loses", "denied", "rejected", "falls", "decreases", "cancels", "drops", "miss", "crash", "ceasefire", "peace", "retreat", "no"]

    for market in markets:
        yes_price = market.get("yes_price", 50)
        no_price = market.get("no_price", 50)

        if yes_price == 50.0 and no_price == 50.0:
            continue
        if yes_price >= 99.0 or yes_price <= 1.0:
            continue  # mercado ja resolvido

        question = market.get("question", "")
        market_keywords = extract_keywords(question)

        for article in news:
            title = article.get("title") or ""
            description = article.get("description") or ""
            content = title + " " + description
            content_keywords = extract_keywords(content)
            content_lower = content.lower()

            overlap = market_keywords & content_keywords
            if len(overlap) < 1:
                continue

            pos_score = sum(1 for k in keywords_positive if k in content_lower)
            neg_score = sum(1 for k in keywords_negative if k in content_lower)

            if pos_score == 0 and neg_score == 0:
                continue

            direction = "YES" if pos_score >= neg_score else "NO"
            current_prob = yes_price / 100 if direction == "YES" else no_price / 100
            adjustment = abs(pos_score - neg_score) * 0.06
            estimated_prob = min(0.99, max(0.01, current_prob + adjustment))
            edge = round(abs(estimated_prob - current_prob), 3)

            if edge >= EDGE_THRESHOLD:
                signals.append({
                    "market": market.get("question"),
                    "news_title": title,
                    "news_source": article.get("source"),
                    "direction": direction,
                    "current_probability": round(current_prob * 100, 1),
                    "estimated_probability": round(estimated_prob * 100, 1),
                    "edge": round(edge * 100, 1),
                    "confidence": round(min(0.95, 0.5 + edge), 2),
                    "overlap_keywords": list(overlap),
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