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

NEWS_API_KEY = "a595b3e7d7a047fda7a934162cf9c3ad"

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/markets")
def get_markets():
    url = "https://gamma-api.polymarket.com/markets?limit=100&order=volume24hr&ascending=false&active=true"
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

@app.get("/news")
def get_news():
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
        return {"error": str(e)}

@app.get("/news/topic/{topic}")
def get_news_by_topic(topic: str):
    url = f"https://newsapi.org/v2/everything?q={topic}&language=en&sortBy=publishedAt&pageSize=20&apiKey={NEWS_API_KEY}"
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
        return {"error": str(e)}