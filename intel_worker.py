import os
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import json
import re

# ─────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────

API_URL = "https://polymarket-backend-production-f363.up.railway.app"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")

CHECK_INTERVAL = 60  # segundos
alerted = set()
markets_cache = []
last_markets_update = None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html, application/xml",
}

# ─────────────────────────────────────────
# CANAIS TELEGRAM BREAKING NEWS (públicos)
# ─────────────────────────────────────────

TELEGRAM_CHANNELS = [
    "bbcbreaking",
    "reutersnews", 
    "AP",
    "worldnewsnn",
    "breakingmiltary",
    "intelslava",
    "OSINTdefender",
    "financialjuice",
    "zerohedge",
    "politico",
]

# ─────────────────────────────────────────
# FONTES RSS
# ─────────────────────────────────────────

RSS_FEEDS = [
    # Breaking News
    ("Reuters Breaking", "https://feeds.reuters.com/reuters/topNews"),
    ("Reuters World", "https://feeds.reuters.com/Reuters/worldNews"),
    ("AP Breaking", "https://feeds.apnews.com/rss/apf-topnews"),
    ("BBC News", "https://feeds.bbci.co.uk/news/rss.xml"),
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
    # Geopolítica
    ("The Guardian World", "https://www.theguardian.com/world/rss"),
    ("Foreign Policy", "https://foreignpolicy.com/feed/"),
    ("War on the Rocks", "https://warontherocks.com/feed/"),
    # Economia
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
    ("Zero Hedge", "https://feeds.feedburner.com/zerohedge/feed"),
    ("MarketWatch", "https://feeds.marketwatch.com/marketwatch/topstories/"),
    # Política EUA
    ("Politico", "https://www.politico.com/rss/politicopicks.xml"),
    ("The Hill", "https://thehill.com/feed/"),
    ("NBC News", "https://feeds.nbcnews.com/nbcnews/public/news"),
    # Conflitos/OSINT
    ("LiveUAMap", "https://liveuamap.com/rss"),
    ("Defense News", "https://www.defensenews.com/arc/outboundfeeds/rss/"),
    # Esportes
    ("ESPN", "https://www.espn.com/espn/rss/news"),
    ("BBC Sport", "https://feeds.bbci.co.uk/sport/rss.xml"),
    # Tech/Crypto
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("The Verge", "https://www.theverge.com/rss/index.xml"),
]

# ─────────────────────────────────────────
# COLETA RSS
# ─────────────────────────────────────────

def fetch_rss(name: str, url: str, max_items: int = 5) -> list:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=8)
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall(".//item")[:max_items]:
            title = item.findtext("title", "").strip()
            desc = item.findtext("description", "").strip()
            link = item.findtext("link", "").strip()
            pub = item.findtext("pubDate", "").strip()
            if title and len(title) > 5:
                items.append({
                    "title": title,
                    "description": desc[:200] if desc else "",
                    "url": link,
                    "published": pub,
                    "source": name,
                })
        return items
    except Exception as e:
        return []


def fetch_all_rss() -> list:
    all_items = []
    for name, url in RSS_FEEDS:
        items = fetch_rss(name, url, max_items=3)
        all_items.extend(items)
        time.sleep(0.2)
    print(f"📰 RSS: {len(all_items)} artigos coletados de {len(RSS_FEEDS)} fontes")
    return all_items


# ─────────────────────────────────────────
# REDDIT
# ─────────────────────────────────────────

SUBREDDITS = [
    "worldnews", "geopolitics", "politics", "economics",
    "ukraine", "middleeast", "europe", "asia",
    "sports", "nfl", "nba", "soccer",
    "cryptocurrency", "investing", "stocks",
    "conspiracy", "news", "usnews",
]

def fetch_reddit_new() -> list:
    posts = []
    for sub in SUBREDDITS[:10]:
        try:
            url = f"https://www.reddit.com/r/{sub}/new.json?limit=5"
            headers = {"User-Agent": "PolySignal/2.0"}
            resp = requests.get(url, headers=headers, timeout=6)
            if resp.status_code == 200:
                children = resp.json().get("data", {}).get("children", [])
                for c in children:
                    d = c.get("data", {})
                    posts.append({
                        "title": d.get("title", ""),
                        "score": d.get("score", 0),
                        "comments": d.get("num_comments", 0),
                        "url": f"https://reddit.com{d.get('permalink','')}",
                        "source": f"Reddit r/{sub}",
                        "created": d.get("created_utc", 0),
                    })
        except:
            pass
        time.sleep(0.3)
    # Ordena por mais novo
    posts.sort(key=lambda x: x.get("created", 0), reverse=True)
    print(f"💬 Reddit: {len(posts)} posts coletados")
    return posts[:30]


# ─────────────────────────────────────────
# GDELT — MONITORA TODA MÍDIA DO MUNDO
# ─────────────────────────────────────────

def fetch_gdelt() -> list:
    try:
        url = "https://api.gdeltproject.org/api/v2/doc/doc"
        params = {
            "query": "war OR attack OR strike OR election OR crisis OR explosion",
            "mode": "artlist",
            "maxrecords": 20,
            "format": "json",
            "timespan": "30min",
            "sort": "datedesc",
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            articles = resp.json().get("articles", [])
            result = []
            for a in articles:
                result.append({
                    "title": a.get("title", ""),
                    "url": a.get("url", ""),
                    "source": a.get("domain", "GDELT"),
                    "published": a.get("seendate", ""),
                })
            print(f"🌐 GDELT: {len(result)} artigos")
            return result
    except Exception as e:
        print(f"GDELT erro: {e}")
    return []


# ─────────────────────────────────────────
# BUSCA MERCADOS DO POLYMARKET
# ─────────────────────────────────────────

def get_markets() -> list:
    global markets_cache, last_markets_update
    now = datetime.utcnow()
    if last_markets_update and (now - last_markets_update).seconds < 300:
        return markets_cache
    try:
        resp = requests.get(f"{API_URL}/markets", timeout=15)
        if resp.status_code == 200:
            markets_cache = resp.json()
            last_markets_update = now
            print(f"📊 {len(markets_cache)} mercados carregados")
    except Exception as e:
        print(f"Erro mercados: {e}")
    return markets_cache


# ─────────────────────────────────────────
# CRUZA NOTÍCIAS COM MERCADOS
# ─────────────────────────────────────────

def match_news_to_markets(news_items: list, markets: list) -> list:
    matches = []
    for item in news_items:
        title = item.get("title", "").lower()
        desc = item.get("description", "").lower()
        content = title + " " + desc

        for market in markets:
            question = market.get("question", "").lower()
            yes_price = market.get("yes_price", 50)
            no_price = market.get("no_price", 50)

            if yes_price == 0 and no_price == 0:
                continue

            # Extrai palavras-chave da pergunta
            words = [w for w in question.split() if len(w) > 4 and w not in
                     ["will", "the", "this", "that", "with", "from", "have", "been", "they", "their"]]
            
            # Conta overlap
            overlap = sum(1 for w in words if w in content)
            
            if overlap >= 2:
                key = f"{item['title'][:30]}_{market.get('slug','')}"
                if key not in alerted:
                    matches.append({
                        "news": item,
                        "market": market,
                        "overlap": overlap,
                        "relevance": round(overlap / max(len(words), 1) * 100, 1),
                    })

    matches.sort(key=lambda x: x["relevance"], reverse=True)
    return matches[:10]


# ─────────────────────────────────────────
# IA ANALISA OPORTUNIDADE
# ─────────────────────────────────────────

def analyze_opportunity(match: dict) -> dict:
    news = match["news"]
    market = match["market"]
    
    prompt = f"""Analise esta oportunidade de prediction market:

MERCADO: {market.get('question')}
PREÇO YES: {market.get('yes_price')}% | PREÇO NO: {market.get('no_price')}%

NOTÍCIA/EVENTO:
Fonte: {news.get('source')}
Título: {news.get('title')}
Descrição: {news.get('description', '')[:300]}

Esta notícia aumenta ou diminui a probabilidade do YES acontecer?
Responda APENAS com JSON:
{{"score_yes": <0-100>, "recomendacao": <"APOSTE YES" ou "APOSTE NO" ou "EVITE">, "confianca": <0.0-1.0>, "resumo": <max 80 chars português>, "urgencia": <"ALTA" ou "MEDIA" ou "BAIXA">}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=15
        )
        if resp.status_code == 200:
            text = resp.json().get("content", [{}])[0].get("text", "{}")
            text = text.replace("```json","").replace("```","").strip()
            return json.loads(text)
    except Exception as e:
        print(f"IA erro: {e}")
    
    return {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.3, "resumo": "IA indisponível", "urgencia": "BAIXA"}


# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────

def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
        print("✅ Telegram enviado!")
    except Exception as e:
        print(f"❌ Telegram: {e}")


def format_intel_alert(match: dict, analysis: dict) -> str:
    news = match["news"]
    market = match["market"]
    rec = analysis.get("recomendacao", "EVITE")
    confianca = analysis.get("confianca", 0)
    urgencia = analysis.get("urgencia", "BAIXA")
    resumo = analysis.get("resumo", "")
    yes_price = market.get("yes_price", 50)
    no_price = market.get("no_price", 50)

    if rec == "APOSTE YES":
        sinal = "🟢"
        preco = yes_price
        potencial = round((100 - preco) / preco * 10, 2) if preco > 0 else 0
    elif rec == "APOSTE NO":
        sinal = "🔴"
        preco = no_price
        potencial = round((100 - preco) / preco * 10, 2) if preco > 0 else 0
    else:
        sinal = "🟡"
        preco = yes_price
        potencial = 0

    urgencia_emoji = {"ALTA": "🚨", "MEDIA": "⚡", "BAIXA": "📌"}.get(urgencia, "📌")

    return f"""{sinal} <b>INTEL ALERT {urgencia_emoji}</b>

📊 <b>{market.get('question')}</b>
💰 YES: {yes_price}% | NO: {no_price}%

📰 <b>{news.get('source')}</b>
<i>{news.get('title')[:120]}</i>

🤖 IA: {resumo}
🎯 {rec} @ {preco}%
💵 $10 → potencial ${potencial}
⚡ Confiança: {round(confianca*100)}%

🔗 <a href="{market.get('polymarket_url', 'https://polymarket.com')}">Ver no Polymarket</a>
📰 <a href="{news.get('url','#')}">Ver notícia</a>
⏰ {datetime.utcnow().strftime('%H:%M')} UTC""".strip()


# ─────────────────────────────────────────
# LOOP PRINCIPAL
# ─────────────────────────────────────────

def run():
    print("🚀 PolySignal Intel Worker iniciado!")
    print(f"📰 {len(RSS_FEEDS)} feeds RSS monitorados")
    print(f"💬 {len(SUBREDDITS)} subreddits monitorados")
    print(f"🌐 GDELT ativo")
    print(f"📱 Telegram: {'✅' if TELEGRAM_TOKEN else '❌'}")

    send_telegram(f"""🚀 <b>PolySignal Intel v2 Ativo!</b>

Monitorando em tempo real:
📰 {len(RSS_FEEDS)} feeds RSS (Reuters, BBC, AP, Al Jazeera...)
💬 {len(SUBREDDITS)} subreddits (worldnews, geopolitics...)
🌐 GDELT — toda mídia do mundo
🤖 IA analisando cada oportunidade

Você será avisado assim que detectar algo relevante! 🎯""")

    cycle = 0
    while True:
        cycle += 1
        now = datetime.utcnow().strftime("%H:%M:%S")
        print(f"\n{'='*50}")
        print(f"[{now}] CICLO #{cycle}")

        markets = get_markets()
        if not markets:
            print("⚠️ Sem mercados, aguardando...")
            time.sleep(CHECK_INTERVAL)
            continue

        all_news = []

        # Coleta RSS
        rss_items = fetch_all_rss()
        all_news.extend(rss_items)

        # Reddit a cada 2 ciclos
        if cycle % 2 == 0:
            reddit_posts = fetch_reddit_new()
            all_news.extend(reddit_posts)

        # GDELT a cada 3 ciclos
        if cycle % 3 == 0:
            gdelt_items = fetch_gdelt()
            all_news.extend(gdelt_items)

        print(f"📊 Total: {len(all_news)} itens coletados")

        # Cruza com mercados
        matches = match_news_to_markets(all_news, markets)
        print(f"🎯 {len(matches)} matches encontrados")

        for match in matches[:5]:
            news = match["news"]
            market = match["market"]
            
            key = f"{news['title'][:40]}_{market.get('slug','')}"
            if key in alerted:
                continue

            # IA analisa
            analysis = analyze_opportunity(match)
            rec = analysis.get("recomendacao", "EVITE")
            confianca = analysis.get("confianca", 0)
            urgencia = analysis.get("urgencia", "BAIXA")

            # Só alerta se IA recomendar e tiver confiança
            if rec != "EVITE" and confianca >= 0.5:
                alerted.add(key)
                msg = format_intel_alert(match, analysis)
                send_telegram(msg)
                print(f"🚨 Alerta enviado: {news['title'][:60]}")
                time.sleep(2)

        # Limpa cache
        if len(alerted) > 1000:
            alerted.clear()
            print("🧹 Cache limpo")

        print(f"[{now}] Próxima checagem em {CHECK_INTERVAL}s...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run()
