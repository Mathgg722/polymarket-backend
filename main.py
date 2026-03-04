import os
import requests
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot
from datetime import datetime, timedelta

Base.metadata.create_all(bind=engine)

app = FastAPI()

from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def home():
    return {"status": "ok", "version": "2.0"}


@app.get("/status")
def status(db: Session = Depends(get_db)):
    total = db.query(Market).count()
    total_tokens = db.query(Token).count()
    total_snapshots = db.query(Snapshot).count()
    total_with_tokens = db.query(Market).join(Token).distinct().count()
    return {
        "total_markets": total,
        "markets_with_tokens": total_with_tokens,
        "total_tokens": total_tokens,
        "total_snapshots": total_snapshots,
        "last_check": datetime.utcnow().isoformat()
    }


@app.get("/markets")
def get_active_markets(db: Session = Depends(get_db)):
    # Converte end_date (roda só uma vez)
    try:
        db.execute(text("""
            ALTER TABLE markets
            ALTER COLUMN end_date TYPE TIMESTAMP
            USING CASE
                WHEN end_date IS NULL OR end_date = '' THEN NULL
                ELSE end_date::TIMESTAMP
            END
        """))
        db.commit()
    except Exception:
        db.rollback()

    now = datetime.utcnow()
    markets = (
        db.query(Market)
        .join(Token)
        .filter(
            (Market.end_date == None) |
            (Market.end_date > now)
        )
        .distinct()
        .all()
    )

    result = []
    for m in markets:
        yes_price = None
        no_price = None
        for t in m.tokens:
            if t.outcome and t.outcome.upper() == "YES":
                yes_price = round(t.price * 100, 1)
            elif t.outcome and t.outcome.upper() == "NO":
                no_price = round(t.price * 100, 1)

        result.append({
            "id": m.id,
            "question": m.question,
            "slug": m.market_slug,
            "end_date": str(m.end_date) if m.end_date else None,
            "yes_price": yes_price,
            "no_price": no_price,
        })

    return result


@app.get("/history/{token_id}")
def get_history(token_id: str, limit: int = 100, db: Session = Depends(get_db)):
    """Historico de precos de um token especifico."""
    snapshots = (
        db.query(Snapshot)
        .filter(Snapshot.token_id == token_id)
        .order_by(Snapshot.timestamp.desc())
        .limit(limit)
        .all()
    )
    return [
        {"price": round(s.price * 100, 1), "timestamp": str(s.timestamp)}
        for s in snapshots
    ]


@app.get("/anomalies")
def get_anomalies(db: Session = Depends(get_db)):
    """
    Detecta mercados com variacao de preco suspeita.
    Compara preco atual com preco de 5 min atras e 15 min atras.
    """
    now = datetime.utcnow()
    window_5m = now - timedelta(minutes=5)
    window_15m = now - timedelta(minutes=15)
    window_1h = now - timedelta(hours=1)

    anomalies = []

    tokens = db.query(Token).all()

    for token in tokens:
        current_price = token.price

        # Preco 5 minutos atras
        snap_5m = (
            db.query(Snapshot)
            .filter(
                Snapshot.token_id == token.token_id,
                Snapshot.timestamp <= window_5m
            )
            .order_by(Snapshot.timestamp.desc())
            .first()
        )

        # Preco 1 hora atras
        snap_1h = (
            db.query(Snapshot)
            .filter(
                Snapshot.token_id == token.token_id,
                Snapshot.timestamp <= window_1h
            )
            .order_by(Snapshot.timestamp.desc())
            .first()
        )

        if not snap_5m:
            continue

        price_5m_ago = snap_5m.price
        change_5m = round((current_price - price_5m_ago) * 100, 2)

        change_1h = None
        if snap_1h:
            change_1h = round((current_price - snap_1h.price) * 100, 2)

        # Anomalia: variacao maior que 5% em 5 minutos
        if abs(change_5m) >= 5.0:
            market = db.query(Market).filter(Market.id == token.market_id).first()
            anomalies.append({
                "market": market.question if market else "Unknown",
                "slug": market.market_slug if market else None,
                "outcome": token.outcome,
                "current_price": round(current_price * 100, 1),
                "price_5m_ago": round(price_5m_ago * 100, 1),
                "change_5m": change_5m,
                "change_1h": change_1h,
                "alert_level": "HIGH" if abs(change_5m) >= 10 else "MEDIUM",
                "detected_at": now.isoformat(),
            })

    # Ordena por maior variacao
    anomalies.sort(key=lambda x: abs(x["change_5m"]), reverse=True)
    return anomalies[:20]


@app.get("/market/{slug}")
def get_market_detail(slug: str, db: Session = Depends(get_db)):
    """Detalhe de um mercado com historico completo."""
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado nao encontrado"}

    tokens_detail = []
    for token in market.tokens:
        # Ultimos 50 snapshots
        snapshots = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id)
            .order_by(Snapshot.timestamp.desc())
            .limit(50)
            .all()
        )
        tokens_detail.append({
            "outcome": token.outcome,
            "current_price": round(token.price * 100, 1),
            "history": [
                {"price": round(s.price * 100, 1), "timestamp": str(s.timestamp)}
                for s in snapshots
            ]
        })

    return {
        "id": market.id,
        "question": market.question,
        "slug": market.market_slug,
        "end_date": str(market.end_date) if market.end_date else None,
        "tokens": tokens_detail
    }


@app.get("/movers")
def get_movers(db: Session = Depends(get_db)):
    """
    Top mercados com maior movimentacao na ultima hora.
    """
    now = datetime.utcnow()
    window_1h = now - timedelta(hours=1)

    movers = []
    tokens = db.query(Token).all()

    for token in tokens:
        snap_old = (
            db.query(Snapshot)
            .filter(
                Snapshot.token_id == token.token_id,
                Snapshot.timestamp <= window_1h
            )
            .order_by(Snapshot.timestamp.desc())
            .first()
        )

        if not snap_old:
            continue

        change = round((token.price - snap_old.price) * 100, 2)

        if abs(change) >= 2.0:
            market = db.query(Market).filter(Market.id == token.market_id).first()
            movers.append({
                "market": market.question if market else "Unknown",
                "slug": market.market_slug if market else None,
                "outcome": token.outcome,
                "current_price": round(token.price * 100, 1),
                "change_1h": change,
                "direction": "UP" if change > 0 else "DOWN",
            })

    movers.sort(key=lambda x: abs(x["change_1h"]), reverse=True)
    return movers[:20]


# ─────────────────────────────────────────
# SISTEMA DE TRADES
# ─────────────────────────────────────────

from models import Trade

@app.post("/trades")
def open_trade(
    market_slug: str,
    outcome: str,
    amount: float,
    notes: str = "",
    db: Session = Depends(get_db)
):
    """Registra uma nova aposta."""
    market = db.query(Market).filter(Market.market_slug == market_slug).first()
    if not market:
        return {"error": "Mercado nao encontrado"}

    # Busca preco atual do outcome
    token = db.query(Token).filter(
        Token.market_id == market.id,
        Token.outcome == outcome.upper()
    ).first()

    if not token:
        return {"error": f"Token {outcome} nao encontrado"}

    entry_price = round(token.price * 100, 2)
    shares = round(amount / entry_price * 100, 4) if entry_price > 0 else 0

    trade = Trade(
        market_slug=market_slug,
        question=market.question,
        outcome=outcome.upper(),
        amount=amount,
        entry_price=entry_price,
        shares=shares,
        notes=notes,
        status="open"
    )
    db.add(trade)
    db.commit()
    db.refresh(trade)

    return {
        "message": "Aposta registrada com sucesso!",
        "trade_id": trade.id,
        "market": market.question,
        "outcome": outcome.upper(),
        "amount": f"${amount}",
        "entry_price": f"{entry_price}%",
        "shares": shares,
        "created_at": str(trade.created_at)
    }


@app.get("/trades")
def list_trades(db: Session = Depends(get_db)):
    """Lista todas as apostas com PnL atual."""
    trades = db.query(Trade).order_by(Trade.created_at.desc()).all()
    result = []

    for t in trades:
        # Preco atual
        market = db.query(Market).filter(Market.market_slug == t.market_slug).first()
        current_price = t.entry_price
        if market:
            token = db.query(Token).filter(
                Token.market_id == market.id,
                Token.outcome == t.outcome
            ).first()
            if token:
                current_price = round(token.price * 100, 2)

        # PnL atual
        if t.status == "open":
            pnl = round((current_price - t.entry_price) / 100 * t.shares, 2)
            pnl_pct = round((current_price - t.entry_price), 1)
        else:
            pnl = t.pnl
            pnl_pct = round((t.exit_price - t.entry_price), 1) if t.exit_price else 0

        result.append({
            "id": t.id,
            "market": t.question,
            "outcome": t.outcome,
            "amount": t.amount,
            "entry_price": t.entry_price,
            "current_price": current_price,
            "shares": t.shares,
            "pnl_usd": pnl,
            "pnl_pct": pnl_pct,
            "status": t.status,
            "notes": t.notes,
            "created_at": str(t.created_at)
        })

    return result


@app.post("/trades/{trade_id}/close")
def close_trade(trade_id: int, db: Session = Depends(get_db)):
    """Fecha uma aposta pelo preco atual."""
    trade = db.query(Trade).filter(Trade.id == trade_id).first()
    if not trade:
        return {"error": "Trade nao encontrado"}

    market = db.query(Market).filter(Market.market_slug == trade.market_slug).first()
    token = db.query(Token).filter(
        Token.market_id == market.id,
        Token.outcome == trade.outcome
    ).first()

    exit_price = round(token.price * 100, 2) if token else trade.entry_price
    pnl = round((exit_price - trade.entry_price) / 100 * trade.shares, 2)

    trade.exit_price = exit_price
    trade.pnl = pnl
    trade.status = "won" if pnl > 0 else "lost"
    trade.closed_at = datetime.utcnow()
    db.commit()

    return {
        "message": "Aposta fechada!",


# ─────────────────────────────────────────
# CONFIGURAÇÕES GLOBAIS
# ─────────────────────────────────────────

NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "a595b3e7d7a047fda7a934162cf9c3ad")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
POLYMARKET_CLOB = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://polymarket.com",
}


# ─────────────────────────────────────────
# /news — NOTÍCIAS REAIS
# ─────────────────────────────────────────

@app.get("/news")
def get_news(query: str = "prediction markets politics economy"):
    """Busca notícias reais via NewsAPI + Google News RSS."""
    articles = []

    # Fonte 1: NewsAPI
    try:
        params = {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 30,
            "apiKey": NEWSAPI_KEY,
        }
        resp = requests.get("https://newsapi.org/v2/everything", params=params, timeout=8)
        if resp.status_code == 200:
            for a in resp.json().get("articles", []):
                if not a.get("title") or "[Removed]" in a.get("title", ""):
                    continue
                articles.append({
                    "title": a.get("title"),
                    "description": a.get("description") or "",
                    "source": a.get("source", {}).get("name", ""),
                    "url": a.get("url"),
                    "published_at": a.get("publishedAt"),
                    "fonte": "NewsAPI"
                })
    except Exception as e:
        print(f"Erro NewsAPI: {e}")

    # Fonte 2: Google News RSS
    try:
        import xml.etree.ElementTree as ET
        params = {"q": "polymarket OR prediction market OR geopolitics", "hl": "en", "gl": "US", "ceid": "US:en"}
        resp = requests.get("https://news.google.com/rss/search", params=params, timeout=8)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:20]:
                title = item.findtext("title", "")
                articles.append({
                    "title": title,
                    "description": item.findtext("description", "")[:200],
                    "source": "Google News",
                    "url": item.findtext("link", ""),
                    "published_at": item.findtext("pubDate", ""),
                    "fonte": "Google News"
                })
    except Exception as e:
        print(f"Erro Google News RSS: {e}")

    return {
        "total": len(articles),
        "articles": articles[:40]
    }


# ─────────────────────────────────────────
# /cleanup — LIMPEZA DE MERCADOS ANTIGOS
# ─────────────────────────────────────────

@app.post("/cleanup")
def cleanup_old_markets(db: Session = Depends(get_db)):
    """Remove mercados antigos com preco zero."""
    years_old = ["2020", "2021", "2019", "2018", "2017"]

    markets_all = db.query(Market).all()
    removed = 0

    for m in markets_all:
        slug = m.market_slug or ""
        question = m.question or ""
        is_old = any(y in slug or y in question for y in years_old)

        # Verifica se todos tokens tem preco zero
        all_zero = all(t.price == 0 for t in m.tokens) if m.tokens else True

        if is_old and all_zero:
            for token in m.tokens:
                db.query(Snapshot).filter(Snapshot.token_id == token.token_id).delete()
                db.delete(token)
            db.delete(m)
            removed += 1

    db.commit()
    total = db.query(Market).count()
    return {
        "message": "Limpeza concluída!",
        "removidos": removed,
        "total_restante": total
    }


# ─────────────────────────────────────────
# /signals — SINAIS DE OPORTUNIDADE
# ─────────────────────────────────────────

@app.get("/signals")
def get_signals(db: Session = Depends(get_db)):
    """Sinais baseados em variações de preço recentes."""
    now = datetime.utcnow()
    window_5m = now - timedelta(minutes=5)
    window_1h = now - timedelta(hours=1)

    signals = []
    tokens = db.query(Token).all()

    for token in tokens:
        current_price = token.price
        if current_price == 0:
            continue

        snap_5m = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= window_5m)
            .order_by(Snapshot.timestamp.desc())
            .first()
        )
        snap_1h = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= window_1h)
            .order_by(Snapshot.timestamp.desc())
            .first()
        )

        if not snap_5m:
            continue

        change_5m = round((current_price - snap_5m.price) * 100, 2)
        if abs(change_5m) < 3.0:
            continue

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        edge = abs(change_5m)
        confidence = min(edge / 20, 1.0)
        kelly = round(edge / 100 * 0.25 * 100, 2)
        change_1h = round((current_price - snap_1h.price) * 100, 2) if snap_1h else None

        signals.append({
            "market": market.question,
            "slug": market.market_slug,
            "outcome": token.outcome,
            "signal_type": "YES" if change_5m > 0 else "NO",
            "market_prob": round(snap_5m.price * 100, 1),
            "estimated_prob": round(current_price * 100, 1),
            "edge": edge,
            "confidence": round(confidence, 2),
            "kelly_position_pct": kelly,
            "change_1h": change_1h,
            "detected_at": now.isoformat(),
        })

    signals.sort(key=lambda x: abs(x["edge"]), reverse=True)
    return signals[:30]


# ─────────────────────────────────────────
# /leaders — TOP TRADERS
# ─────────────────────────────────────────

@app.get("/leaders")
def get_leaders():
    for window in ["all", "1mo", "1w", "1d"]:
        try:
            url = f"{DATA_API}/leaderboard?window={window}&limit=20"
            resp = requests.get(url, headers=HEADERS, timeout=8)
            if resp.status_code == 200:
                items = resp.json()
                items = items if isinstance(items, list) else items.get("data", [])
                if items:
                    leaders = []
                    for i, t in enumerate(items[:20]):
                        address = t.get("proxyWallet") or t.get("address") or ""
                        won = t.get("positionsWon") or 0
                        lost = t.get("positionsLost") or 0
                        leaders.append({
                            "rank": i + 1,
                            "username": t.get("name") or t.get("pseudonym") or address[:10] + "...",
                            "address": address,
                            "profit_usd": t.get("profit") or t.get("pnl") or 0,
                            "volume_usd": t.get("volume") or 0,
                            "win_rate": round(won / max(won + lost, 1) * 100, 1),
                            "ver_apostas": f"https://polymarket.com/profile/{address}",
                        })
                    return {"status": "ok", "leaders": leaders}
        except:
            continue

    return {
        "status": "unavailable",
        "links": {
            "leaderboard": "https://polymarket.com/leaderboard",
            "whales": "https://polymarketwhales.info",
            "atividade": "https://polymarket.com/activity",
        }
    }


@app.get("/leaders/live")
def get_live_trades():
    """Apostas recentes na blockchain."""
    try:
        url = f"{POLYMARKET_CLOB}/trades?limit=100"
        resp = requests.get(url, headers=HEADERS, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            trades_raw = data if isinstance(data, list) else data.get("data", [])
            trades = []
            wallets_seen = {}

            for tx in trades_raw:
                valor = float(tx.get("size") or tx.get("usdcSize") or 0)
                wallet = tx.get("maker") or tx.get("owner") or ""
                if not wallet:
                    continue
                wallets_seen[wallet] = wallets_seen.get(wallet, 0) + 1
                trades.append({
                    "wallet": wallet[:8] + "..." + wallet[-4:],
                    "wallet_full": wallet,
                    "valor_usd": round(valor, 2),
                    "outcome": tx.get("outcome") or tx.get("side"),
                    "preco": tx.get("price"),
                    "timestamp": tx.get("timestamp") or tx.get("matchedAt"),
                    "polymarket_url": f"https://polymarket.com/profile/{wallet}",
                })

            top_wallets = sorted(wallets_seen.items(), key=lambda x: x[1], reverse=True)[:10]
            return {
                "status": "ok",
                "total_apostas": len(trades),
                "apostas_recentes": sorted(trades, key=lambda x: x["valor_usd"], reverse=True)[:20],
                "top_carteiras": [
                    {"wallet": w[:8] + "..." + w[-4:], "wallet_full": w, "num_apostas": n,
                     "polymarket_url": f"https://polymarket.com/profile/{w}"}
                    for w, n in top_wallets
                ]
            }
    except Exception as e:
        print(f"Erro live trades: {e}")

    return {
        "status": "unavailable",
        "links": {
            "atividade": "https://polymarket.com/activity",
            "whales": "https://polymarketwhales.info",
        }
    }


@app.get("/leaders/wallet/{address}")
def get_wallet_detail(address: str):
    try:
        url = f"{DATA_API}/positions?user={address}&sizeThreshold=10&limit=50"
        resp = requests.get(url, headers=HEADERS, timeout=8)
        if resp.status_code == 200:
            items = resp.json()
            items = items if isinstance(items, list) else items.get("data", [])
            if items:
                positions = []
                total = 0
                for p in items:
                    valor = float(p.get("currentValue") or p.get("size") or 0)
                    total += valor
                    positions.append({
                        "mercado": p.get("title") or p.get("market") or p.get("question"),
                        "outcome": p.get("outcome"),
                        "valor_usd": round(valor, 2),
                        "preco_medio": p.get("avgPrice") or p.get("curPrice"),
                        "pnl": p.get("cashPnl") or p.get("pnl"),
                    })
                return {
                    "status": "ok",
                    "wallet": address[:8] + "..." + address[-4:],
                    "total_posicoes": len(positions),
                    "total_exposto_usd": round(total, 2),
                    "posicoes": positions,
                    "polymarket_url": f"https://polymarket.com/profile/{address}",
                }
    except Exception as e:
        print(f"Erro wallet: {e}")

    return {
        "status": "unavailable",
        "polymarket_url": f"https://polymarket.com/profile/{address}",
        "polygonscan_url": f"https://polygonscan.com/address/{address}",
    }


# ─────────────────────────────────────────
# /intelligence — SISTEMA DE INTELIGÊNCIA COM IA REAL
# ─────────────────────────────────────────

def fetch_news_for_query(query: str, max_results: int = 8):
    """Busca notícias para uma query específica."""
    articles = []
    try:
        params = {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": max_results,
            "apiKey": NEWSAPI_KEY,
        }
        resp = requests.get("https://newsapi.org/v2/everything", params=params, timeout=8)
        if resp.status_code == 200:
            for a in resp.json().get("articles", []):
                if not a.get("title") or "[Removed]" in a.get("title", ""):
                    continue
                articles.append({
                    "title": a.get("title", ""),
                    "description": (a.get("description") or "")[:300],
                    "source": a.get("source", {}).get("name", ""),
                    "url": a.get("url", ""),
                    "published_at": a.get("publishedAt", ""),
                })
    except Exception as e:
        print(f"Erro news query: {e}")

    # Google News RSS como complemento
    try:
        import xml.etree.ElementTree as ET
        params = {"q": query, "hl": "en", "gl": "US", "ceid": "US:en"}
        resp = requests.get("https://news.google.com/rss/search", params=params, timeout=6)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:5]:
                articles.append({
                    "title": item.findtext("title", ""),
                    "description": item.findtext("description", "")[:200],
                    "source": "Google News",
                    "url": item.findtext("link", ""),
                    "published_at": item.findtext("pubDate", ""),
                })
    except:
        pass

    return articles[:12]


def analyze_with_claude(question: str, articles: list) -> dict:
    """Usa Claude Haiku para analisar notícias e dar score."""
    import json

    if not articles:
        return {
            "score_yes": 50,
            "recomendacao": "EVITE",
            "confianca": 0.2,
            "resumo": "Sem notícias suficientes para análise confiável.",
            "sentimento": "NEUTRO",
            "fontes_relevantes": 0,
        }

    news_text = "\n".join([
        f"[{a['source']}] {a['title']} — {a['description'][:150]}"
        for a in articles[:8]
    ])

    prompt = f"""Você é um analista expert em prediction markets e geopolítica.

PERGUNTA DO MERCADO: {question}

NOTÍCIAS RECENTES:
{news_text}

Analise as notícias e responda SOMENTE com JSON válido, sem texto antes ou depois:

{{"score_yes": <0-100 probabilidade do YES acontecer>, "recomendacao": <"APOSTE YES" ou "APOSTE NO" ou "EVITE">, "confianca": <0.0-1.0>, "resumo": <explicação curta em português max 100 chars>, "sentimento": <"POSITIVO" ou "NEGATIVO" ou "NEUTRO">, "fontes_relevantes": <número de notícias relevantes>}}"""

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
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=15
        )
        if resp.status_code == 200:
            text = resp.json().get("content", [{}])[0].get("text", "{}")
            text = text.replace("```json", "").replace("```", "").strip()
            return json.loads(text)
    except Exception as e:
        print(f"Erro Claude API: {e}")

    # Fallback por palavras-chave
    all_text = " ".join([a["title"] + " " + a["description"] for a in articles]).lower()
    pos = sum(1 for w in ["confirmed","approved","wins","rises","signed","passes"] if w in all_text)
    neg = sum(1 for w in ["fails","rejected","loses","denied","cancelled","drops"] if w in all_text)
    score = 50 + (pos - neg) * 8
    score = max(10, min(90, score))
    return {
        "score_yes": score,
        "recomendacao": "APOSTE YES" if pos > neg else "APOSTE NO" if neg > pos else "EVITE",
        "confianca": min(len(articles) / 10, 0.7),
        "resumo": f"{len(articles)} notícias analisadas. {pos} positivas, {neg} negativas.",
        "sentimento": "POSITIVO" if pos > neg else "NEGATIVO" if neg > pos else "NEUTRO",
        "fontes_relevantes": len(articles),
    }


@app.get("/intelligence/{slug}")
def get_intelligence(slug: str, db: Session = Depends(get_db)):
    """Score de inteligência completo para um mercado."""
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado não encontrado"}

    yes_price = None
    no_price = None
    for token in market.tokens:
        if token.outcome and token.outcome.upper() == "YES":
            yes_price = round(token.price * 100, 1)
        elif token.outcome and token.outcome.upper() == "NO":
            no_price = round(token.price * 100, 1)

    # Keywords da pergunta
    keywords = market.question.replace("?","").replace("Will ","").replace("will ","")[:80]
    articles = fetch_news_for_query(keywords)
    analysis = analyze_with_claude(market.question, articles)

    score_yes = analysis.get("score_yes", 50)
    edge = round(score_yes - (yes_price or 50), 1)
    rec = analysis.get("recomendacao", "EVITE")
    confianca = analysis.get("confianca", 0.5)

    # Sinal visual
    if rec == "APOSTE YES" and abs(edge) >= 5 and confianca >= 0.5:
        sinal = "🟢 APOSTE YES"
        sinal_cor = "green"
    elif rec == "APOSTE NO" and abs(edge) >= 5 and confianca >= 0.5:
        sinal = "🔴 APOSTE NO"
        sinal_cor = "red"
    else:
        sinal = "🟡 EVITE"
        sinal_cor = "yellow"

    return {
        "market": market.question,
        "slug": slug,
        "yes_price_mercado": yes_price,
        "no_price_mercado": no_price,
        "score_yes_ia": score_yes,
        "edge": edge,
        "sinal": sinal,
        "sinal_cor": sinal_cor,
        "recomendacao": rec,
        "confianca": confianca,
        "resumo": analysis.get("resumo"),
        "sentimento": analysis.get("sentimento"),
        "fontes_relevantes": analysis.get("fontes_relevantes", 0),
        "noticias": articles[:5],
        "polymarket_url": f"https://polymarket.com/event/{slug}",
        "atualizado_em": datetime.utcnow().isoformat(),
    }


@app.get("/intelligence")
def get_all_intelligence(db: Session = Depends(get_db)):
    """Score de inteligência para os top 15 mercados mais relevantes."""
    now = datetime.utcnow()
    markets = (
        db.query(Market)
        .join(Token)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct()
        .limit(15)
        .all()
    )

    results = []
    for m in markets:
        yes_price = None
        for token in m.tokens:
            if token.outcome and token.outcome.upper() == "YES":
                yes_price = round(token.price * 100, 1)

        if not yes_price or yes_price == 0:
            continue

        keywords = m.question.replace("?","").replace("Will ","")[:60]
        articles = fetch_news_for_query(keywords, max_results=5)
        analysis = analyze_with_claude(m.question, articles)

        score_yes = analysis.get("score_yes", 50)
        edge = round(score_yes - yes_price, 1)
        rec = analysis.get("recomendacao", "EVITE")
        confianca = analysis.get("confianca", 0.5)

        if rec == "APOSTE YES" and abs(edge) >= 5 and confianca >= 0.5:
            sinal_cor = "green"
        elif rec == "APOSTE NO" and abs(edge) >= 5 and confianca >= 0.5:
            sinal_cor = "red"
        else:
            sinal_cor = "yellow"

        results.append({
            "market": m.question,
            "slug": m.market_slug,
            "yes_price": yes_price,
            "score_yes_ia": score_yes,
            "edge": edge,
            "sinal_cor": sinal_cor,
            "recomendacao": rec,
            "confianca": confianca,
            "resumo": analysis.get("resumo"),
            "fontes": analysis.get("fontes_relevantes", 0),
        })

    results.sort(key=lambda x: abs(x["edge"]), reverse=True)
    return {
        "total": len(results),
        "atualizados_em": now.isoformat(),
        "mercados": results
    }