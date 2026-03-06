# ============================================================
# PolySignal — Intel Worker (remarkable-flow)
# Monitora: RSS + Reddit + GDELT → cruza com mercados → Telegram
# Railway start command: python intel_worker.py
# ============================================================

import os
import json
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

API_URL         = os.environ.get("API_BASE", "https://polymarket-backend-production-f363.up.railway.app")
TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_TOKEN", "") or os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ANTHROPIC_KEY   = os.environ.get("ANTHROPIC_KEY", "")
NEWSAPI_KEY     = os.environ.get("NEWSAPI_KEY", "")
CHECK_INTERVAL  = int(os.environ.get("CHECK_INTERVAL", "60"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html, application/xml",
}

# ── RSS Feeds ─────────────────────────────────────────────────
RSS_FEEDS = [
    ("Reuters Top", "https://feeds.reuters.com/reuters/topNews"),
    ("Reuters World", "https://feeds.reuters.com/Reuters/worldNews"),
    ("AP Breaking", "https://feeds.apnews.com/rss/apf-topnews"),
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
    ("Guardian World", "https://www.theguardian.com/world/rss"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
    ("Politico", "https://www.politico.com/rss/politicopicks.xml"),
    ("The Hill", "https://thehill.com/feed/"),
    ("Defense News", "https://www.defensenews.com/arc/outboundfeeds/rss/"),
    ("ESPN", "https://www.espn.com/espn/rss/news"),
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
]

SUBREDDITS = ["worldnews", "geopolitics", "politics", "economics", "ukraine", "middleeast", "sports"]

alerted = set()
markets_cache = []
last_markets_update = None


# ── RSS ───────────────────────────────────────────────────────

def fetch_rss(name: str, url: str, max_items: int = 3) -> list:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=8)
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall(".//item")[:max_items]:
            title = (item.findtext("title") or "").strip()
            if title and len(title) > 5:
                items.append({
                    "title": title,
                    "description": (item.findtext("description") or "").strip()[:200],
                    "url": (item.findtext("link") or "").strip(),
                    "published": (item.findtext("pubDate") or "").strip(),
                    "source": name,
                })
        return items
    except Exception:
        return []


def fetch_all_rss() -> list:
    all_items = []
    for name, url in RSS_FEEDS:
        items = fetch_rss(name, url)
        all_items.extend(items)
        time.sleep(0.15)
    print(f"📰 RSS: {len(all_items)} artigos de {len(RSS_FEEDS)} fontes")
    return all_items


# ── Reddit ────────────────────────────────────────────────────

def fetch_reddit() -> list:
    posts = []
    for sub in SUBREDDITS[:6]:
        try:
            resp = requests.get(
                f"https://www.reddit.com/r/{sub}/new.json?limit=5",
                headers={"User-Agent": "PolySignal/3.0"}, timeout=6
            )
            if resp.status_code == 200:
                for c in resp.json().get("data", {}).get("children", []):
                    d = c.get("data", {})
                    posts.append({
                        "title": d.get("title", ""),
                        "score": d.get("score", 0),
                        "url": f"https://reddit.com{d.get('permalink','')}",
                        "source": f"r/{sub}",
                        "created": d.get("created_utc", 0),
                    })
        except Exception:
            pass
        time.sleep(0.3)
    posts.sort(key=lambda x: x.get("created", 0), reverse=True)
    print(f"💬 Reddit: {len(posts)} posts")
    return posts[:25]


# ── GDELT ─────────────────────────────────────────────────────

def fetch_gdelt() -> list:
    try:
        params = {
            "query": "war OR attack OR election OR crisis OR explosion OR breakthrough",
            "mode": "artlist", "maxrecords": 20, "format": "json",
            "timespan": "30min", "sort": "datedesc",
        }
        resp = requests.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params, timeout=10)
        if resp.status_code == 200:
            articles = resp.json().get("articles", [])
            result = [{"title": a.get("title",""), "url": a.get("url",""),
                       "source": a.get("domain","GDELT"), "published": a.get("seendate","")} for a in articles]
            print(f"🌐 GDELT: {len(result)} artigos")
            return result
    except Exception as e:
        print(f"[GDELT] erro: {e}")
    return []


# ── Mercados ──────────────────────────────────────────────────

def get_markets() -> list:
    global markets_cache, last_markets_update
    now = datetime.utcnow()
    if last_markets_update and (now - last_markets_update).total_seconds() < 300:
        return markets_cache
    try:
        resp = requests.get(f"{API_URL}/markets", timeout=15)
        if resp.status_code == 200:
            markets_cache = resp.json()
            last_markets_update = now
            print(f"📊 {len(markets_cache)} mercados carregados")
    except Exception as e:
        print(f"[mercados] erro: {e}")
    return markets_cache


# ── Match notícia × mercado ───────────────────────────────────

STOP_WORDS = {"will", "the", "this", "that", "with", "from", "have", "been", "they", "their", "which", "what"}

def match_news_to_markets(news_items: list, markets: list) -> list:
    matches = []
    for item in news_items:
        content = (item.get("title","") + " " + item.get("description","")).lower()
        for market in markets:
            yes_price = market.get("yes_price") or 0
            no_price = market.get("no_price") or 0
            if yes_price == 0 and no_price == 0:
                continue
            # Ignora mercados já resolvidos
            if yes_price >= 95 or yes_price <= 5:
                continue

            question = (market.get("question") or "").lower()
            words = [w for w in question.split() if len(w) > 4 and w not in STOP_WORDS]
            overlap = sum(1 for w in words if w in content)

            if overlap >= 2:
                key = f"{item['title'][:30]}_{market.get('slug','')}"
                if key not in alerted:
                    matches.append({
                        "news": item, "market": market, "overlap": overlap,
                        "relevance": round(overlap / max(len(words), 1) * 100, 1),
                    })

    matches.sort(key=lambda x: x["relevance"], reverse=True)
    return matches[:8]


# ── IA analisa ────────────────────────────────────────────────

def analyze_opportunity(match: dict) -> dict:
    news = match["news"]
    market = match["market"]
    prompt = f"""Analise esta oportunidade de prediction market:

MERCADO: {market.get('question')}
PREÇO YES: {market.get('yes_price')}% | NO: {market.get('no_price')}%

NOTÍCIA:
Fonte: {news.get('source')}
Título: {news.get('title')}
Descrição: {news.get('description','')[:300]}

Responda APENAS com JSON:
{{"score_yes": <0-100>, "recomendacao": <"APOSTE YES" ou "APOSTE NO" ou "EVITE">, "confianca": <0.0-1.0>, "resumo": <max 80 chars português>, "urgencia": <"ALTA" ou "MEDIA" ou "BAIXA">}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 200,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=15
        )
        if resp.status_code == 200:
            text = resp.json().get("content", [{}])[0].get("text", "{}")
            return json.loads(text.replace("```json","").replace("```","").strip())
    except Exception as e:
        print(f"[IA] erro: {e}")
    return {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.3, "resumo": "IA indisponível", "urgencia": "BAIXA"}


# ── Telegram ──────────────────────────────────────────────────

def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10
        )
        print("✅ Telegram enviado!")
    except Exception as e:
        print(f"❌ Telegram: {e}")


def format_alert(match: dict, analysis: dict) -> str:
    news = match["news"]
    market = match["market"]
    rec = analysis.get("recomendacao", "EVITE")
    confianca = analysis.get("confianca", 0)
    urgencia = analysis.get("urgencia", "BAIXA")
    resumo = analysis.get("resumo", "")
    yes_price = market.get("yes_price", 50)
    no_price = market.get("no_price", 50)

    if rec == "APOSTE YES":
        sinal, preco = "🟢", yes_price
        potencial = round((100 - preco) / preco * 10, 2) if preco > 0 else 0
    elif rec == "APOSTE NO":
        sinal, preco = "🔴", no_price
        potencial = round((100 - preco) / preco * 10, 2) if preco > 0 else 0
    else:
        sinal, preco, potencial = "🟡", yes_price, 0

    urg_emoji = {"ALTA": "🚨", "MEDIA": "⚡", "BAIXA": "📌"}.get(urgencia, "📌")
    poly_url = market.get("polymarket_url") or f"https://polymarket.com/event/{market.get('slug','')}"
    news_url = news.get("url", "#")

    return f"""{sinal} <b>INTEL ALERT {urg_emoji} — {urgencia}</b>

📊 <b>{market.get('question','')}</b>
💰 YES: {yes_price}% | NO: {no_price}%

📰 <b>{news.get('source','')}</b>
<i>{news.get('title','')[:120]}</i>

🤖 {resumo}
🎯 {rec} @ {preco}%
💵 $10 → potencial ${potencial}
⚡ Confiança: {round(confianca*100)}%

🔗 <a href="{poly_url}">Ver no Polymarket</a>
📰 <a href="{news_url}">Ver notícia</a>
⏰ {datetime.utcnow().strftime('%H:%M')} UTC""".strip()


# ── Loop principal ────────────────────────────────────────────

def run():
    print("🚀 PolySignal Intel Worker v3 iniciado!")
    print(f"📰 {len(RSS_FEEDS)} feeds RSS | 💬 {len(SUBREDDITS)} subreddits | 🌐 GDELT")
    print(f"📱 Telegram: {'✅' if TELEGRAM_TOKEN else '❌ NÃO CONFIGURADO'}")
    print(f"⏱  Intervalo: {CHECK_INTERVAL}s")

    send_telegram(f"""🚀 <b>PolySignal Intel v3 Ativo!</b>

📰 {len(RSS_FEEDS)} feeds RSS (Reuters, BBC, AP, Al Jazeera...)
💬 {len(SUBREDDITS)} subreddits monitorados
🌐 GDELT — toda mídia do mundo
🤖 IA Claude analisando cada match

Alertas chegam quando notícia + mercado = oportunidade real! 🎯""")

    cycle = 0
    while True:
        cycle += 1
        ts = datetime.utcnow().strftime("%H:%M:%S")
        print(f"\n{'='*50}")
        print(f"[{ts}] CICLO #{cycle}")

        markets = get_markets()
        if not markets:
            print("⚠️ Sem mercados. Aguardando...")
            time.sleep(CHECK_INTERVAL)
            continue

        all_news = []
        all_news.extend(fetch_all_rss())

        if cycle % 2 == 0:
            all_news.extend(fetch_reddit())

        if cycle % 3 == 0:
            all_news.extend(fetch_gdelt())

        print(f"📊 Total: {len(all_news)} itens | {len(markets)} mercados")

        matches = match_news_to_markets(all_news, markets)
        print(f"🎯 {len(matches)} matches encontrados")

        sent_count = 0
        for match in matches[:5]:
            news = match["news"]
            market = match["market"]
            key = f"{news['title'][:40]}_{market.get('slug','')}"
            if key in alerted:
                continue

            analysis = analyze_opportunity(match)
            rec = analysis.get("recomendacao", "EVITE")
            confianca = analysis.get("confianca", 0)

            if rec != "EVITE" and confianca >= 0.5:
                alerted.add(key)
                send_telegram(format_alert(match, analysis))
                sent_count += 1
                print(f"🚨 Alerta: {news['title'][:60]}")
                time.sleep(2)

        print(f"📱 {sent_count} alertas enviados neste ciclo")

        if len(alerted) > 1000:
            alerted.clear()
            print("🧹 Cache limpo")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run()
