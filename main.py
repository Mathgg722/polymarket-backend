from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import threading
import time
import json
from datetime import datetime, timezone

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
    "markets_short": [],
    "markets_long": [],
    "news": [],
    "signals": [],
    "top_traders": [],
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

            end_date = m.get("endDate")
            term = "unknown"
            if end_date:
                try:
                    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    days_left = (end_dt - now).days
                    if days_left <= 7:
                        term = "short"
                    elif days_left <= 90:
                        term = "medium"
                    else:
                        term = "long"
                except:
                    pass

            markets.append({
                "id": m.get("id"),
                "question": m.get("question"),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": m.get("volume"),
                "liquidity": m.get("liquidity"),
                "end_date": end_date,
                "volume_24hr": m.get("volume24hr"),
                "price_change_24h": m.get("oneDayPriceChange"),
                "price_change_1w": m.get("oneWeekPriceChange"),
                "term": term,
            })
        return markets
    except Exception as e:
        return []

def fetch_news_data():
    all_news = []

    # NewsAPI
    try:
        url = f"https://newsapi.org/v2/top-headlines?language=en&pageSize=30&apiKey={NEWS_API_KEY}"
        response = requests.get(url, timeout=10)
        data = response.json()
        for article in data.get("articles", []):
            all_news.append({
                "title": article.get("title"),
                "description": article.get("description"),
                "source": article.get("source", {}).get("name"),
                "published_at": article.get("publishedAt"),
                "url": article.get("url"),
                "type": "news"
            })
    except:
        pass

    # Google News RSS - geopolitica
    try:
        topics = ["Iran war", "Israel strike", "Fed interest rates", "Bitcoin price", "election 2028"]
        for topic in topics:
            rss_url = f"https://news.google.com/rss/search?q={topic.replace(' ', '+')}&hl=en&gl=US&ceid=US:en"
            resp = requests.get(rss_url, timeout=8)
            if resp.status_code == 200:
                import re
                items = re.findall(r'<item>(.*?)</item>', resp.text, re.DOTALL)
                for item in items[:3]:
                    title_match = re.search(r'<title>(.*?)</title>', item)
                    desc_match = re.search(r'<description>(.*?)</description>', item)
                    link_match = re.search(r'<link>(.*?)</link>', item)
                    pub_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
                    if title_match:
                        all_news.append({
                            "title": title_match.group(1).replace('<![CDATA[', '').replace(']]>', '').strip(),
                            "description": desc_match.group(1).replace('<![CDATA[', '').replace(']]>', '').strip() if desc_match else "",
                            "source": f"Google News ({topic})",
                            "published_at": pub_match.group(1) if pub_match else None,
                            "url": link_match.group(1) if link_match else None,
                            "type": "google_news"
                        })
    except:
        pass

    # Reddit - subreddits relevantes
    try:
        subreddits = ["worldnews", "geopolitics", "economics", "Polymarket"]
        headers = {"User-Agent": "PolySignalBot/1.0"}
        for sub in subreddits:
            reddit_url = f"https://www.reddit.com/r/{sub}/hot.json?limit=5"
            resp = requests.get(reddit_url, headers=headers, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                posts = data.get("data", {}).get("children", [])
                for post in posts:
                    p = post.get("data", {})
                    all_news.append({
                        "title": p.get("title"),
                        "description": p.get("selftext", "")[:200],
                        "source": f"Reddit r/{sub}",
                        "published_at": datetime.fromtimestamp(p.get("created_utc", 0)).isoformat(),
                        "url": f"https://reddit.com{p.get('permalink', '')}",
                        "type": "reddit",
                        "score": p.get("score", 0)
                    })
    except:
        pass

    return all_news

def fetch_top_traders():
    headers = {"User-Agent": "Mozilla/5.0"}
    urls = [
        "https://data-api.polymarket.com/leaderboard?window=1mo&limit=20",
        "https://data-api.polymarket.com/leaderboard?limit=20",
        "https://gamma-api.polymarket.com/leaderboard?limit=20",
        "https://data-api.polymarket.com/profiles?limit=20&sortBy=profit&sortOrder=DESC",
    ]
    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=8)
            if response.status_code == 200:
                data = response.json()
                if data:
                    traders = []
                    items = data if isinstance(data, list) else data.get("data", [])
                    for t in items[:20]:
                        traders.append({
                            "username": t.get("name") or t.get("pseudonym") or t.get("proxyWallet", "")[:12] or "Anonymous",
                            "profit": t.get("profit") or t.get("pnl") or t.get("profitAndLoss"),
                            "volume": t.get("volume"),
                            "profit_pct": t.get("profitPct") or t.get("roi"),
                            "positions_won": t.get("positionsWon") or t.get("marketsWon"),
                            "positions_lost": t.get("positionsLost") or t.get("marketsLost"),
                        })
                    if traders:
                        return traders
        except:
            continue
    return []

def extract_keywords(text):
    words = text.lower().replace("?", "").replace(",", "").replace(".", "").split()
    return {w for w in words if len(w) > 3 and w not in STOPWORDS}

def analyze_signals(markets, news):
    signals = []
    keywords_positive = ["win", "approved", "confirmed", "rises", "increases", "launches", "passes", "beats", "surge", "gains", "attack", "strike", "hit", "success", "advance"]
    keywords_negative = ["fails", "loses", "denied", "rejected", "falls", "decreases", "cancels", "drops", "miss", "crash", "ceasefire", "peace", "retreat", "collapse"]

    for market in markets:
        yes_price = market.get("yes_price", 50)
        no_price = market.get("no_price", 50)
        if yes_price == 50.0 and no_price == 50.0:
            continue
        if yes_price >= 99.0 or yes_price <= 1.0:
            continue

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
                # Kelly fracionado
                kelly = edge / current_prob
                position_pct = round(kelly * 0.25 * 100, 1)

                signals.append({
                    "market": market.get("question"),
                    "term": market.get("term"),
                    "news_title": title,
                    "news_source": article.get("source"),
                    "news_type": article.get("type"),
                    "direction": direction,
                    "current_probability": round(current_prob * 100, 1),
                    "estimated_probability": round(estimated_prob * 100, 1),
                    "edge": round(edge * 100, 1),
                    "confidence": round(min(0.95, 0.5 + edge), 2),
                    "kelly_position_pct": min(position_pct, 5.0),
                    "overlap_keywords": list(overlap),
                    "generated_at": datetime.utcnow().isoformat(),
                })

    signals.sort(key=lambda x: x["edge"], reverse=True)
    return signals[:30]

def update_cache():
    while True:
        markets = fetch_markets_data()
        news = fetch_news_data()
        signals = analyze_signals(markets, news)
        top_traders = fetch_top_traders()

        now = datetime.now(timezone.utc)
        markets_short = [m for m in markets if m.get("term") in ["short", "medium"]]
        markets_long = [m for m in markets if m.get("term") == "long"]

        cache["markets"] = markets
        cache["markets_short"] = markets_short
        cache["markets_long"] = markets_long
        cache["news"] = news
        cache["signals"] = signals
        cache["top_traders"] = top_traders
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

@app.get("/markets/short")
def get_markets_short():
    if not cache["markets"]:
        cache["markets"] = fetch_markets_data()
        cache["markets_short"] = [m for m in cache["markets"] if m.get("term") in ["short", "medium"]]
    return cache["markets_short"]

@app.get("/markets/long")
def get_markets_long():
    if not cache["markets"]:
        cache["markets"] = fetch_markets_data()
        cache["markets_long"] = [m for m in cache["markets"] if m.get("term") == "long"]
    return cache["markets_long"]

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

@app.get("/top-traders")
def get_top_traders():
    if not cache["top_traders"]:
        cache["top_traders"] = fetch_top_traders()
    return cache["top_traders"]

@app.get("/status")
def get_status():
    return {
        "last_update": cache["last_update"],
        "total_markets": len(cache["markets"]),
        "markets_short_medium": len(cache["markets_short"]),
        "markets_long": len(cache["markets_long"]),
        "total_news": len(cache["news"]),
        "total_signals": len(cache["signals"]),
        "top_traders": len(cache["top_traders"]),
    }
    import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
