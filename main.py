import os
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
    # Converte end_date (roda s├│ uma vez)
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
    Deteccao avancada de anomalias com classificacao por tipo,
    score de confianca, multiplas janelas temporais e filtro
    de mercados ja resolvidos.
    """
    now = datetime.utcnow()
    window_1m  = now - timedelta(minutes=1)
    window_5m  = now - timedelta(minutes=5)
    window_15m = now - timedelta(minutes=15)
    window_1h  = now - timedelta(hours=1)

    anomalies = []
    tokens = db.query(Token).all()

    for token in tokens:
        current_price = token.price

        # Ignora mercados ja resolvidos (>95% ou <5%)
        if current_price >= 0.95 or current_price <= 0.05:
            continue

        if current_price == 0:
            continue

        def get_snap(window):
            return (
                db.query(Snapshot)
                .filter(
                    Snapshot.token_id == token.token_id,
                    Snapshot.timestamp <= window
                )
                .order_by(Snapshot.timestamp.desc())
                .first()
            )

        snap_1m  = get_snap(window_1m)
        snap_5m  = get_snap(window_5m)
        snap_15m = get_snap(window_15m)
        snap_1h  = get_snap(window_1h)

        if not snap_5m:
            continue

        change_1m  = round((current_price - snap_1m.price) * 100, 2) if snap_1m else None
        change_5m  = round((current_price - snap_5m.price) * 100, 2)
        change_15m = round((current_price - snap_15m.price) * 100, 2) if snap_15m else None
        change_1h  = round((current_price - snap_1h.price) * 100, 2) if snap_1h else None

        if abs(change_5m) < 5.0:
            continue

        # Classifica tipo de anomalia
        if abs(change_5m) >= 20:
            tipo = "EXTREME"
            alert_level = "EXTREME"
        elif change_5m > 0 and (change_1h or 0) > 0:
            tipo = "SPIKE"
            alert_level = "HIGH"
        elif change_5m < 0 and (change_1h or 0) < 0:
            tipo = "DUMP"
            alert_level = "HIGH"
        elif change_5m > 0 and (change_1h or 0) < 0:
            tipo = "REVERSAL_UP"
            alert_level = "HIGH"
        elif change_5m < 0 and (change_1h or 0) > 0:
            tipo = "REVERSAL_DOWN"
            alert_level = "HIGH"
        else:
            tipo = "MOVE"
            alert_level = "MEDIUM"

        # Score de confianca (0-100)
        score = 0
        score += min(abs(change_5m) * 2, 40)
        score += min(abs(change_1h or 0), 20)
        if change_1m and abs(change_1m) > 2:
            score += 20
        if change_15m and abs(change_15m) > abs(change_5m):
            score += 20
        confianca_score = round(min(score, 100), 0)

        # Oportunidade
        if change_5m > 0 and current_price < 0.8:
            oportunidade = "POSSIVEL_YES"
        elif change_5m < 0 and current_price > 0.2:
            oportunidade = "POSSIVEL_NO"
        else:
            oportunidade = "AGUARDAR"

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        anomalies.append({
            "market": market.question,
            "slug": market.market_slug,
            "outcome": token.outcome,
            "tipo": tipo,
            "alert_level": alert_level,
            "oportunidade": oportunidade,
            "confianca_score": confianca_score,
            "current_price": round(current_price * 100, 1),
            "price_5m_ago": round(snap_5m.price * 100, 1),
            "change_1m": change_1m,
            "change_5m": change_5m,
            "change_15m": change_15m,
            "change_1h": change_1h,
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
            "detected_at": now.isoformat(),
        })

    anomalies.sort(key=lambda x: x["confianca_score"], reverse=True)
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


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# SISTEMA DE TRADES
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

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
        "outcome": trade.outcome,
        "entry_price": trade.entry_price,
        "exit_price": exit_price,
        "pnl_usd": pnl,
        "status": trade.status
    }


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# CONFIGURA├ç├òES GLOBAIS
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
POLYMARKET_CLOB = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://polymarket.com",
}


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# /news ÔÇö NOT├ìCIAS REAIS
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.get("/news")
def get_news(query: str = "prediction markets politics economy"):
    """Busca not├¡cias reais via NewsAPI + Google News RSS."""
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


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# /cleanup ÔÇö LIMPEZA DE MERCADOS ANTIGOS
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

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
        "message": "Limpeza conclu├¡da!",
        "removidos": removed,
        "total_restante": total
    }


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# /signals ÔÇö SINAIS DE OPORTUNIDADE
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.get("/signals")
def get_signals(db: Session = Depends(get_db)):
    """Sinais baseados em varia├º├Áes de pre├ºo recentes."""
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


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# /leaders ÔÇö TOP TRADERS
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

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


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# /intelligence ÔÇö SISTEMA DE INTELIG├èNCIA COM IA REAL
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

def fetch_news_for_query(query: str, max_results: int = 8):
    """Busca not├¡cias para uma query espec├¡fica."""
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
    """Usa Claude Haiku para analisar not├¡cias e dar score."""
    import json

    if not articles:
        return {
            "score_yes": 50,
            "recomendacao": "EVITE",
            "confianca": 0.2,
            "resumo": "Sem not├¡cias suficientes para an├ílise confi├ível.",
            "sentimento": "NEUTRO",
            "fontes_relevantes": 0,
        }

    news_text = "\n".join([
        f"[{a['source']}] {a['title']} ÔÇö {a['description'][:150]}"
        for a in articles[:8]
    ])

    prompt = f"""Voc├¬ ├® um analista expert em prediction markets e geopol├¡tica.

PERGUNTA DO MERCADO: {question}

NOT├ìCIAS RECENTES:
{news_text}

Analise as not├¡cias e responda SOMENTE com JSON v├ílido, sem texto antes ou depois:

{{"score_yes": <0-100 probabilidade do YES acontecer>, "recomendacao": <"APOSTE YES" ou "APOSTE NO" ou "EVITE">, "confianca": <0.0-1.0>, "resumo": <explica├º├úo curta em portugu├¬s max 100 chars>, "sentimento": <"POSITIVO" ou "NEGATIVO" ou "NEUTRO">, "fontes_relevantes": <n├║mero de not├¡cias relevantes>}}"""

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
        "resumo": f"{len(articles)} not├¡cias analisadas. {pos} positivas, {neg} negativas.",
        "sentimento": "POSITIVO" if pos > neg else "NEGATIVO" if neg > pos else "NEUTRO",
        "fontes_relevantes": len(articles),
    }


@app.get("/intelligence/{slug}")
def get_intelligence(slug: str, db: Session = Depends(get_db)):
    """Score de intelig├¬ncia completo para um mercado."""
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado n├úo encontrado"}

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
        sinal = "­ƒƒó APOSTE YES"
        sinal_cor = "green"
    elif rec == "APOSTE NO" and abs(edge) >= 5 and confianca >= 0.5:
        sinal = "­ƒö┤ APOSTE NO"
        sinal_cor = "red"
    else:
        sinal = "­ƒƒí EVITE"
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
    """Score de intelig├¬ncia para os top 15 mercados mais relevantes."""
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


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# /best ÔÇö MELHORES APOSTAS DO MOMENTO
# Filtro rigoroso para o teste de $100
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.get("/best")
def get_best_opportunities(db: Session = Depends(get_db)):
    """
    Retorna apenas as MELHORES anomalias para apostar.
    Crit├®rios rigorosos:
    - Score >= 50
    - Pre├ºo entre 20% e 80% (tem edge real)
    - Varia├º├úo consistente em 15min E 1h
    - Confirma com not├¡cias via IA
    Ideal para estrat├®gia de $10 por aposta.
    """
    now = datetime.utcnow()
    window_5m  = now - timedelta(minutes=5)
    window_15m = now - timedelta(minutes=15)
    window_1h  = now - timedelta(hours=1)

    candidates = []
    tokens = db.query(Token).all()

    for token in tokens:
        current_price = token.price

        # FILTRO 1: Mercado entre 20% e 80% ÔÇö tem edge real
        if current_price < 0.20 or current_price > 0.80:
            continue

        if current_price == 0:
            continue

        def get_snap(window):
            return (
                db.query(Snapshot)
                .filter(
                    Snapshot.token_id == token.token_id,
                    Snapshot.timestamp <= window
                )
                .order_by(Snapshot.timestamp.desc())
                .first()
            )

        snap_5m  = get_snap(window_5m)
        snap_15m = get_snap(window_15m)
        snap_1h  = get_snap(window_1h)

        if not snap_5m or not snap_15m:
            continue

        change_5m  = round((current_price - snap_5m.price) * 100, 2)
        change_15m = round((current_price - snap_15m.price) * 100, 2)
        change_1h  = round((current_price - snap_1h.price) * 100, 2) if snap_1h else None

        # FILTRO 2: Varia├º├úo >= 5% em 5min
        if abs(change_5m) < 5.0:
            continue

        # FILTRO 3: Tend├¬ncia consistente ÔÇö 15min na mesma dire├º├úo
        if change_5m > 0 and change_15m < 0:
            continue
        if change_5m < 0 and change_15m > 0:
            continue

        # FILTRO 4: 1h tamb├®m na mesma dire├º├úo (se dispon├¡vel)
        if change_1h is not None:
            if change_5m > 0 and change_1h < -5:
                continue
            if change_5m < 0 and change_1h > 5:
                continue

        # Calcula score
        score = 0
        score += min(abs(change_5m) * 2, 40)
        score += min(abs(change_15m) * 1.5, 25)
        score += min(abs(change_1h or 0), 20)
        # Bonus por estar em zona de valor (40-60%)
        if 0.40 <= current_price <= 0.60:
            score += 15
        confianca_score = round(min(score, 100), 0)

        # FILTRO 5: Score m├¡nimo 40
        if confianca_score < 40:
            continue

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        # FILTRO 6: Confirma com not├¡cias via IA
        keywords = market.question.replace("?","").replace("Will ","")[:60]
        articles = fetch_news_for_query(keywords, max_results=5)
        analysis = analyze_with_claude(market.question, articles)

        ai_score = analysis.get("score_yes", 50)
        rec = analysis.get("recomendacao", "EVITE")
        confianca_ia = analysis.get("confianca", 0)

        # FILTRO 7: IA deve confirmar a dire├º├úo
        if change_5m > 0 and rec == "APOSTE NO":
            continue
        if change_5m < 0 and rec == "APOSTE YES":
            continue

        # Score final combinado (mercado + IA)
        score_final = round((confianca_score * 0.6) + (confianca_ia * 100 * 0.4), 1)

        # Dire├º├úo recomendada
        direcao = "YES" if change_5m > 0 else "NO"
        preco_entrada = round(current_price * 100, 1)

        # Potencial de lucro se resolver a favor
        potencial_lucro = round((100 - preco_entrada) / preco_entrada * 10, 2) if direcao == "YES" else round(preco_entrada / (100 - preco_entrada) * 10, 2)

        candidates.append({
            "market": market.question,
            "slug": market.market_slug,
            "outcome": token.outcome,
            "direcao": direcao,
            "preco_entrada": preco_entrada,
            "potencial_lucro_10usd": potencial_lucro,
            "score_mercado": confianca_score,
            "score_ia": round(confianca_ia * 100, 0),
            "score_final": score_final,
            "change_5m": change_5m,
            "change_15m": change_15m,
            "change_1h": change_1h,
            "recomendacao_ia": rec,
            "resumo_ia": analysis.get("resumo"),
            "fontes": len(articles),
            "noticias": [a["title"] for a in articles[:3]],
            "sinal": "­ƒƒó APOSTE" if score_final >= 60 else "­ƒƒí CONSIDERE",
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
            "detectado_em": now.isoformat(),
        })

    # Ordena pelo score final
    candidates.sort(key=lambda x: x["score_final"], reverse=True)

    total = len(candidates)
    top = candidates[:5]  # M├íximo 5 apostas por vez

    return {
        "total_oportunidades": total,
        "top_apostas": top,
        "capital_necessario": len(top) * 10,
        "resumo": f"{total} oportunidades encontradas. Top {len(top)} recomendadas para $10 cada.",
        "atualizado_em": now.isoformat(),
    }


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# /performance ÔÇö DASHBOARD $100 EM 30 DIAS
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.get("/performance")
def get_performance(db: Session = Depends(get_db)):
    """
    Dashboard completo de performance das apostas.
    Mostra ROI, taxa de acerto, e se o PolySignal est├í ajudando.
    """
    trades = db.query(Trade).order_by(Trade.created_at.asc()).all()

    if not trades:
        return {
            "mensagem": "Nenhuma aposta registrada ainda.",
            "capital_inicial": 100,
            "capital_atual": 100,
            "roi_pct": 0,
            "trades": []
        }

    total_apostado = sum(t.amount for t in trades)
    total_aberto = sum(t.amount for t in trades if t.status == "open")

    # PnL de trades fechados
    pnl_fechados = sum(t.pnl or 0 for t in trades if t.status in ["won", "lost"])

    # PnL de trades abertos (calculado ao vivo)
    pnl_abertos = 0
    for t in trades:
        if t.status == "open":
            market = db.query(Market).filter(Market.market_slug == t.market_slug).first()
            if market:
                token = db.query(Token).filter(
                    Token.market_id == market.id,
                    Token.outcome == t.outcome
                ).first()
                if token:
                    current = round(token.price * 100, 2)
                    pnl_abertos += round((current - t.entry_price) / 100 * t.shares, 2)

    pnl_total = round(pnl_fechados + pnl_abertos, 2)
    capital_atual = round(100 + pnl_total, 2)
    roi = round(pnl_total / 100 * 100, 1)

    # Taxa de acerto
    fechados = [t for t in trades if t.status in ["won", "lost"]]
    ganhos = [t for t in fechados if t.status == "won"]
    taxa_acerto = round(len(ganhos) / max(len(fechados), 1) * 100, 1)

    # Maior ganho e maior perda
    pnls = [t.pnl for t in fechados if t.pnl is not None]
    maior_ganho = max(pnls) if pnls else 0
    maior_perda = min(pnls) if pnls else 0

    # Status geral
    if roi > 10:
        status = "­ƒƒó EXCELENTE ÔÇö Sistema funcionando!"
    elif roi > 0:
        status = "­ƒƒí POSITIVO ÔÇö Sistema ajudando"
    elif roi > -10:
        status = "­ƒƒá NEGATIVO ÔÇö Ajustar estrat├®gia"
    else:
        status = "­ƒö┤ ATEN├ç├âO ÔÇö Revisar sistema"

    # Lista de trades
    trades_lista = []
    for t in trades:
        market = db.query(Market).filter(Market.market_slug == t.market_slug).first()
        current_price = t.entry_price
        if market and t.status == "open":
            token = db.query(Token).filter(
                Token.market_id == market.id,
                Token.outcome == t.outcome
            ).first()
            if token:
                current_price = round(token.price * 100, 2)

        pnl = t.pnl if t.status != "open" else round((current_price - t.entry_price) / 100 * t.shares, 2)

        trades_lista.append({
            "id": t.id,
            "market": t.question,
            "outcome": t.outcome,
            "amount": t.amount,
            "entry_price": t.entry_price,
            "current_price": current_price,
            "pnl_usd": round(pnl or 0, 2),
            "pnl_pct": round((current_price - t.entry_price), 1),
            "status": t.status,
            "created_at": str(t.created_at),
        })

    return {
        "status_geral": status,
        "capital_inicial": 100,
        "capital_atual": capital_atual,
        "pnl_total_usd": pnl_total,
        "roi_pct": roi,
        "total_apostas": len(trades),
        "apostas_abertas": len([t for t in trades if t.status == "open"]),
        "apostas_fechadas": len(fechados),
        "taxa_acerto_pct": taxa_acerto,
        "maior_ganho_usd": maior_ganho,
        "maior_perda_usd": maior_perda,
        "total_apostado_usd": total_apostado,
        "trades": trades_lista,
        "atualizado_em": datetime.utcnow().isoformat(),
    }


# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# SISTEMA MULTI-FONTE DE INTELIG├èNCIA
# Reddit + Google Trends + Whales + NewsAPI
# ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

def fetch_reddit(query: str) -> list:
    """Busca posts relevantes no Reddit."""
    results = []
    subreddits = ["worldnews", "geopolitics", "politics", "economics", "sports"]
    try:
        for sub in subreddits[:3]:
            url = f"https://www.reddit.com/r/{sub}/search.json"
            params = {"q": query, "sort": "new", "limit": 5, "t": "day"}
            headers = {"User-Agent": "PolySignal/1.0"}
            resp = requests.get(url, params=params, headers=headers, timeout=6)
            if resp.status_code == 200:
                posts = resp.json().get("data", {}).get("children", [])
                for p in posts:
                    data = p.get("data", {})
                    results.append({
                        "title": data.get("title", ""),
                        "score": data.get("score", 0),
                        "comments": data.get("num_comments", 0),
                        "url": f"https://reddit.com{data.get('permalink','')}",
                        "fonte": f"Reddit r/{sub}"
                    })
    except Exception as e:
        print(f"Reddit erro: {e}")
    return results[:8]


def fetch_google_trends(query: str) -> dict:
    """Verifica se o assunto est├í em alta no Google."""
    try:
        url = "https://trends.google.com/trends/api/dailytrends"
        params = {"hl": "en-US", "tz": "-180", "geo": "US", "ns": "15"}
        resp = requests.get(url, params=params, timeout=6)
        if resp.status_code == 200:
            # Remove o prefixo de seguran├ºa do Google
            text = resp.text[6:]
            import json
            data = json.loads(text)
            trends = data.get("default", {}).get("trendingSearchesDays", [])
            query_lower = query.lower()
            for day in trends:
                for item in day.get("trendingSearches", []):
                    title = item.get("title", {}).get("query", "").lower()
                    if any(word in title for word in query_lower.split()[:3]):
                        traffic = item.get("formattedTraffic", "")
                        return {"trending": True, "traffic": traffic, "termo": title}
    except Exception as e:
        print(f"Google Trends erro: {e}")
    return {"trending": False, "traffic": "0", "termo": ""}


def fetch_whale_activity(slug: str) -> dict:
    """Detecta apostas grandes no mercado via CLOB."""
    try:
        url = f"https://clob.polymarket.com/trades?limit=50"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            trades = data if isinstance(data, list) else data.get("data", [])
            big_trades = []
            total_volume = 0
            for t in trades:
                valor = float(t.get("size") or t.get("usdcSize") or 0)
                total_volume += valor
                if valor >= 500:  # Aposta >= $500 ├® baleia
                    big_trades.append({
                        "valor": round(valor, 2),
                        "outcome": t.get("outcome") or t.get("side"),
                        "wallet": (t.get("maker") or "")[:8] + "...",
                    })
            big_trades.sort(key=lambda x: x["valor"], reverse=True)
            return {
                "total_volume_recente": round(total_volume, 2),
                "num_baleias": len(big_trades),
                "maior_aposta": big_trades[0] if big_trades else None,
                "baleias": big_trades[:3],
            }
    except Exception as e:
        print(f"Whale erro: {e}")
    return {"total_volume_recente": 0, "num_baleias": 0, "maior_aposta": None, "baleias": []}


def multi_source_analysis(question: str, slug: str, articles: list) -> dict:
    """
    Analisa uma oportunidade cruzando TODAS as fontes:
    NewsAPI + Google News + Reddit + Google Trends + Whales + IA
    """
    keywords = question.replace("?","").replace("Will ","").replace("will ","")[:60]

    # Busca em todas as fontes em paralelo
    reddit_posts = fetch_reddit(keywords)
    trends = fetch_google_trends(keywords)
    whales = fetch_whale_activity(slug)

    # Score por fonte
    score_news = min(len(articles) * 8, 30)          # M├íx 30pts
    score_reddit = min(len(reddit_posts) * 5, 20)     # M├íx 20pts
    score_trends = 20 if trends.get("trending") else 0 # 20pts se trending
    score_whales = min(whales.get("num_baleias", 0) * 10, 30)  # M├íx 30pts

    score_total = score_news + score_reddit + score_trends + score_whales

    # An├ílise IA com contexto completo
    reddit_text = "\n".join([f"- [Reddit {p['fonte']}] {p['title']} ({p['score']} upvotes)" for p in reddit_posts[:4]])
    news_text = "\n".join([f"- [{a['source']}] {a['title']}" for a in articles[:5]])
    whale_text = f"Apostas grandes detectadas: {whales.get('num_baleias', 0)} baleias, maior: ${whales.get('maior_aposta', {}).get('valor', 0) if whales.get('maior_aposta') else 0}"

    prompt = f"""Analise esta oportunidade de prediction market:

MERCADO: {question}

NOT├ìCIAS:
{news_text or 'Nenhuma not├¡cia encontrada'}

REDDIT:
{reddit_text or 'Nenhum post encontrado'}

ATIVIDADE DE BALEIAS: {whale_text}
GOOGLE TRENDS: {'EM ALTA: ' + trends.get('termo','') if trends.get('trending') else 'N├úo trending'}

Responda APENAS com JSON:
{{"score_yes": <0-100>, "recomendacao": <"APOSTE YES" ou "APOSTE NO" ou "EVITE">, "confianca": <0.0-1.0>, "resumo": <max 80 chars em portugu├¬s>, "sentimento": <"POSITIVO" ou "NEGATIVO" ou "NEUTRO">}}"""

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
            import json
            text = resp.json().get("content", [{}])[0].get("text", "{}")
            text = text.replace("```json","").replace("```","").strip()
            ai = json.loads(text)
        else:
            ai = {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.3, "resumo": "IA indispon├¡vel", "sentimento": "NEUTRO"}
    except:
        ai = {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.3, "resumo": "IA indispon├¡vel", "sentimento": "NEUTRO"}

    # Score final combinado
    score_final = round((score_total * 0.5) + (ai.get("confianca", 0) * 100 * 0.5), 1)

    return {
        "score_final": score_final,
        "score_noticias": score_news,
        "score_reddit": score_reddit,
        "score_trends": score_trends,
        "score_whales": score_whales,
        "ai": ai,
        "reddit_posts": reddit_posts[:3],
        "trending": trends,
        "whales": whales,
        "num_fontes": sum([
            1 if articles else 0,
            1 if reddit_posts else 0,
            1 if trends.get("trending") else 0,
            1 if whales.get("num_baleias", 0) > 0 else 0,
        ])
    }


@app.get("/best/v2")
def get_best_v2(db: Session = Depends(get_db)):
    """
    Vers├úo 2 do filtro de melhores apostas.
    Cruza NewsAPI + Google News + Reddit + Google Trends + Whales + IA.
    S├│ retorna oportunidades confirmadas por m├║ltiplas fontes.
    """
    now = datetime.utcnow()
    window_5m  = now - timedelta(minutes=5)
    window_15m = now - timedelta(minutes=15)
    window_1h  = now - timedelta(hours=1)

    candidates = []
    tokens = db.query(Token).all()

    for token in tokens:
        current_price = token.price

        # Pre├ºo entre 15% e 85%
        if current_price < 0.15 or current_price > 0.85:
            continue
        if current_price == 0:
            continue

        def get_snap(window):
            return (
                db.query(Snapshot)
                .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= window)
                .order_by(Snapshot.timestamp.desc())
                .first()
            )

        snap_5m  = get_snap(window_5m)
        snap_15m = get_snap(window_15m)
        snap_1h  = get_snap(window_1h)

        if not snap_5m:
            continue

        change_5m  = round((current_price - snap_5m.price) * 100, 2)
        change_15m = round((current_price - snap_15m.price) * 100, 2) if snap_15m else None
        change_1h  = round((current_price - snap_1h.price) * 100, 2) if snap_1h else None

        # Varia├º├úo m├¡nima 4%
        if abs(change_5m) < 4.0:
            continue

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        # Busca not├¡cias
        keywords = market.question.replace("?","").replace("Will ","")[:60]
        articles = fetch_news_for_query(keywords, max_results=6)

        # An├ílise multi-fonte
        analysis = multi_source_analysis(market.question, market.market_slug, articles)

        score_final = analysis["score_final"]
        ai = analysis["ai"]
        rec = ai.get("recomendacao", "EVITE")

        # S├│ oportunidades com score >= 35 e IA n├úo diz EVITE
        if score_final < 35 or rec == "EVITE":
            continue

        direcao = "YES" if change_5m > 0 else "NO"
        preco = round(current_price * 100, 1)
        potencial = round((100 - preco) / preco * 10, 2) if direcao == "YES" else round(preco / (100 - preco) * 10, 2)

        # Sinal final
        if score_final >= 65:
            sinal = "­ƒƒó APOSTE"
        elif score_final >= 45:
            sinal = "­ƒƒí CONSIDERE"
        else:
            sinal = "ÔÜ¬ FRACO"

        candidates.append({
            "market": market.question,
            "slug": market.market_slug,
            "direcao": direcao,
            "preco_entrada": preco,
            "potencial_lucro_10usd": potencial,
            "sinal": sinal,
            "score_final": score_final,
            "scores": {
                "noticias": analysis["score_noticias"],
                "reddit": analysis["score_reddit"],
                "trends": analysis["score_trends"],
                "baleias": analysis["score_whales"],
            },
            "num_fontes_confirmando": analysis["num_fontes"],
            "change_5m": change_5m,
            "change_1h": change_1h,
            "resumo_ia": ai.get("resumo"),
            "baleias": analysis["whales"].get("baleias", []),
            "trending": analysis["trending"].get("trending", False),
            "noticias_titulos": [a["title"] for a in articles[:3]],
            "reddit_posts": [p["title"] for p in analysis["reddit_posts"][:2]],
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
            "detectado_em": now.isoformat(),
        })

    candidates.sort(key=lambda x: x["score_final"], reverse=True)
    top = candidates[:5]

    return {
        "total_oportunidades": len(candidates),
        "top_apostas": top,
        "capital_necessario": len(top) * 10,
        "resumo": f"{len(candidates)} oportunidades. Top {len(top)} confirmadas por m├║ltiplas fontes.",
        "atualizado_em": now.isoformat(),
    }
# SISTEMA MULTI-FONTE DE INTELIGÃŠNCIA
# Reddit + Google Trends + Whales + NewsAPI
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def fetch_reddit(query: str) -> list:
    """Busca posts relevantes no Reddit."""
    results = []
    subreddits = ["worldnews", "geopolitics", "politics", "economics", "sports"]
    try:
        for sub in subreddits[:3]:
            url = f"https://www.reddit.com/r/{sub}/search.json"
            params = {"q": query, "sort": "new", "limit": 5, "t": "day"}
            headers = {"User-Agent": "PolySignal/1.0"}
            resp = requests.get(url, params=params, headers=headers, timeout=6)
            if resp.status_code == 200:
                posts = resp.json().get("data", {}).get("children", [])
                for p in posts:
                    data = p.get("data", {})
                    results.append({
                        "title": data.get("title", ""),
                        "score": data.get("score", 0),
                        "comments": data.get("num_comments", 0),
                        "url": f"https://reddit.com{data.get('permalink','')}",
                        "fonte": f"Reddit r/{sub}"
                    })
    except Exception as e:
        print(f"Reddit erro: {e}")
    return results[:8]


def fetch_google_trends(query: str) -> dict:
    """Verifica se o assunto estÃ¡ em alta no Google."""
    try:
        url = "https://trends.google.com/trends/api/dailytrends"
        params = {"hl": "en-US", "tz": "-180", "geo": "US", "ns": "15"}
        resp = requests.get(url, params=params, timeout=6)
        if resp.status_code == 200:
            # Remove o prefixo de seguranÃ§a do Google
            text = resp.text[6:]
            import json
            data = json.loads(text)
            trends = data.get("default", {}).get("trendingSearchesDays", [])
            query_lower = query.lower()
            for day in trends:
                for item in day.get("trendingSearches", []):
                    title = item.get("title", {}).get("query", "").lower()
                    if any(word in title for word in query_lower.split()[:3]):
                        traffic = item.get("formattedTraffic", "")
                        return {"trending": True, "traffic": traffic, "termo": title}
    except Exception as e:
        print(f"Google Trends erro: {e}")
    return {"trending": False, "traffic": "0", "termo": ""}


def fetch_whale_activity(slug: str) -> dict:
    """Detecta apostas grandes no mercado via CLOB."""
    try:
        url = f"https://clob.polymarket.com/trades?limit=50"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            trades = data if isinstance(data, list) else data.get("data", [])
            big_trades = []
            total_volume = 0
            for t in trades:
                valor = float(t.get("size") or t.get("usdcSize") or 0)
                total_volume += valor
                if valor >= 500:  # Aposta >= $500 Ã© baleia
                    big_trades.append({
                        "valor": round(valor, 2),
                        "outcome": t.get("outcome") or t.get("side"),
                        "wallet": (t.get("maker") or "")[:8] + "...",
                    })
            big_trades.sort(key=lambda x: x["valor"], reverse=True)
            return {
                "total_volume_recente": round(total_volume, 2),
                "num_baleias": len(big_trades),
                "maior_aposta": big_trades[0] if big_trades else None,
                "baleias": big_trades[:3],
            }
    except Exception as e:
        print(f"Whale erro: {e}")
    return {"total_volume_recente": 0, "num_baleias": 0, "maior_aposta": None, "baleias": []}


def multi_source_analysis(question: str, slug: str, articles: list) -> dict:
    """
    Analisa uma oportunidade cruzando TODAS as fontes:
    NewsAPI + Google News + Reddit + Google Trends + Whales + IA
    """
    keywords = question.replace("?","").replace("Will ","").replace("will ","")[:60]

    # Busca em todas as fontes em paralelo
    reddit_posts = fetch_reddit(keywords)
    trends = fetch_google_trends(keywords)
    whales = fetch_whale_activity(slug)

    # Score por fonte
    score_news = min(len(articles) * 8, 30)          # MÃ¡x 30pts
    score_reddit = min(len(reddit_posts) * 5, 20)     # MÃ¡x 20pts
    score_trends = 20 if trends.get("trending") else 0 # 20pts se trending
    score_whales = min(whales.get("num_baleias", 0) * 10, 30)  # MÃ¡x 30pts

    score_total = score_news + score_reddit + score_trends + score_whales

    # AnÃ¡lise IA com contexto completo
    reddit_text = "\n".join([f"- [Reddit {p['fonte']}] {p['title']} ({p['score']} upvotes)" for p in reddit_posts[:4]])
    news_text = "\n".join([f"- [{a['source']}] {a['title']}" for a in articles[:5]])
    whale_text = f"Apostas grandes detectadas: {whales.get('num_baleias', 0)} baleias, maior: ${whales.get('maior_aposta', {}).get('valor', 0) if whales.get('maior_aposta') else 0}"

    prompt = f"""Analise esta oportunidade de prediction market:

MERCADO: {question}

NOTÃCIAS:
{news_text or 'Nenhuma notÃ­cia encontrada'}

REDDIT:
{reddit_text or 'Nenhum post encontrado'}

ATIVIDADE DE BALEIAS: {whale_text}
GOOGLE TRENDS: {'EM ALTA: ' + trends.get('termo','') if trends.get('trending') else 'NÃ£o trending'}

Responda APENAS com JSON:
{{"score_yes": <0-100>, "recomendacao": <"APOSTE YES" ou "APOSTE NO" ou "EVITE">, "confianca": <0.0-1.0>, "resumo": <max 80 chars em portuguÃªs>, "sentimento": <"POSITIVO" ou "NEGATIVO" ou "NEUTRO">}}"""

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
            import json
            text = resp.json().get("content", [{}])[0].get("text", "{}")
            text = text.replace("```json","").replace("```","").strip()
            ai = json.loads(text)
        else:
            ai = {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.3, "resumo": "IA indisponÃ­vel", "sentimento": "NEUTRO"}
    except:
        ai = {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.3, "resumo": "IA indisponÃ­vel", "sentimento": "NEUTRO"}

    # Score final combinado
    score_final = round((score_total * 0.5) + (ai.get("confianca", 0) * 100 * 0.5), 1)

    return {
        "score_final": score_final,
        "score_noticias": score_news,
        "score_reddit": score_reddit,
        "score_trends": score_trends,
        "score_whales": score_whales,
        "ai": ai,
        "reddit_posts": reddit_posts[:3],
        "trending": trends,
        "whales": whales,
        "num_fontes": sum([
            1 if articles else 0,
            1 if reddit_posts else 0,
            1 if trends.get("trending") else 0,
            1 if whales.get("num_baleias", 0) > 0 else 0,
        ])
    }


@app.get("/best/v2")
def get_best_v2(db: Session = Depends(get_db)):
    """
    VersÃ£o 2 do filtro de melhores apostas.
    Cruza NewsAPI + Google News + Reddit + Google Trends + Whales + IA.
    SÃ³ retorna oportunidades confirmadas por mÃºltiplas fontes.
    """
    now = datetime.utcnow()
    window_5m  = now - timedelta(minutes=5)
    window_15m = now - timedelta(minutes=15)
    window_1h  = now - timedelta(hours=1)

    candidates = []
    tokens = db.query(Token).all()

    for token in tokens:
        current_price = token.price

        # PreÃ§o entre 15% e 85%
        if current_price < 0.15 or current_price > 0.85:
            continue
        if current_price == 0:
            continue

        def get_snap(window):
            return (
                db.query(Snapshot)
                .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= window)
                .order_by(Snapshot.timestamp.desc())
                .first()
            )

        snap_5m  = get_snap(window_5m)
        snap_15m = get_snap(window_15m)
        snap_1h  = get_snap(window_1h)

        if not snap_5m:
            continue

        change_5m  = round((current_price - snap_5m.price) * 100, 2)
        change_15m = round((current_price - snap_15m.price) * 100, 2) if snap_15m else None
        change_1h  = round((current_price - snap_1h.price) * 100, 2) if snap_1h else None

        # VariaÃ§Ã£o mÃ­nima 4%
        if abs(change_5m) < 4.0:
            continue

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        # Busca notÃ­cias
        keywords = market.question.replace("?","").replace("Will ","")[:60]
        articles = fetch_news_for_query(keywords, max_results=6)

        # AnÃ¡lise multi-fonte
        analysis = multi_source_analysis(market.question, market.market_slug, articles)

        score_final = analysis["score_final"]
        ai = analysis["ai"]
        rec = ai.get("recomendacao", "EVITE")

        # SÃ³ oportunidades com score >= 35 e IA nÃ£o diz EVITE
        if score_final < 35 or rec == "EVITE":
            continue

        direcao = "YES" if change_5m > 0 else "NO"
        preco = round(current_price * 100, 1)
        potencial = round((100 - preco) / preco * 10, 2) if direcao == "YES" else round(preco / (100 - preco) * 10, 2)

        # Sinal final
        if score_final >= 65:
            sinal = "ðŸŸ¢ APOSTE"
        elif score_final >= 45:
            sinal = "ðŸŸ¡ CONSIDERE"
        else:
            sinal = "âšª FRACO"

        candidates.append({
            "market": market.question,
            "slug": market.market_slug,
            "direcao": direcao,
            "preco_entrada": preco,
            "potencial_lucro_10usd": potencial,
            "sinal": sinal,
            "score_final": score_final,
            "scores": {
                "noticias": analysis["score_noticias"],
                "reddit": analysis["score_reddit"],
                "trends": analysis["score_trends"],
                "baleias": analysis["score_whales"],
            },
            "num_fontes_confirmando": analysis["num_fontes"],
            "change_5m": change_5m,
            "change_1h": change_1h,
            "resumo_ia": ai.get("resumo"),
            "baleias": analysis["whales"].get("baleias", []),
            "trending": analysis["trending"].get("trending", False),
            "noticias_titulos": [a["title"] for a in articles[:3]],
            "reddit_posts": [p["title"] for p in analysis["reddit_posts"][:2]],
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
            "detectado_em": now.isoformat(),
        })

    candidates.sort(key=lambda x: x["score_final"], reverse=True)
    top = candidates[:5]

    return {
        "total_oportunidades": len(candidates),
        "top_apostas": top,
        "capital_necessario": len(top) * 10,
        "resumo": f"{len(candidates)} oportunidades. Top {len(top)} confirmadas por mÃºltiplas fontes.",
        "atualizado_em": now.isoformat(),
    }


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# /refresh â€” ATUALIZA MERCADOS MANUALMENTE
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.post("/refresh")
def refresh_markets(db: Session = Depends(get_db)):
    """
    ForÃ§a atualizaÃ§Ã£o completa dos mercados:
    - Busca mercados novos da API do Polymarket
    - Fecha mercados expirados
    - Adiciona mercados novos
    - Atualiza preÃ§os
    """
    import json as json_lib

    BASE_URL = "https://gamma-api.polymarket.com/markets"
    now = datetime.utcnow()

    # Busca mercados ativos da API
    try:
        resp = requests.get(
            f"{BASE_URL}?limit=200&active=true&order=volume24hr&ascending=false",
            timeout=15
        )
        markets_raw = resp.json()
        if isinstance(markets_raw, dict):
            markets_raw = markets_raw.get("markets", [])
    except Exception as e:
        return {"error": f"Erro ao buscar mercados: {e}"}

    novos = 0
    atualizados = 0
    fechados = 0

    for m in markets_raw:
        try:
            slug = m.get("slug")
            question = m.get("question")
            if not slug:
                continue

            # Parse end_date
            end_date = None
            ed = m.get("endDate")
            if ed:
                try:
                    end_date = datetime.fromisoformat(ed.replace("Z", "+00:00")).replace(tzinfo=None)
                except:
                    try:
                        end_date = datetime.strptime(ed[:10], "%Y-%m-%d")
                    except:
                        pass

            # Extrai preÃ§os
            yes_price = None
            no_price = None
            tokens_data = []

            # Tenta outcomePrices
            op = m.get("outcomePrices")
            if op:
                try:
                    prices = json_lib.loads(op) if isinstance(op, str) else op
                    if len(prices) >= 2:
                        yes_price = float(prices[0])
                        no_price = float(prices[1])
                        clob_ids = m.get("clobTokenIds", "[]")
                        ids = json_lib.loads(clob_ids) if isinstance(clob_ids, str) else clob_ids
                        tokens_data = [
                            {"tokenId": ids[0] if ids else f"{slug}_YES", "outcome": "YES", "price": yes_price},
                            {"tokenId": ids[1] if len(ids) > 1 else f"{slug}_NO", "outcome": "NO", "price": no_price},
                        ]
                except:
                    pass

            # Tenta tokens direto
            if not tokens_data:
                for t in m.get("tokens", []):
                    outcome = (t.get("outcome") or "").upper()
                    price = float(t.get("price") or 0)
                    token_id = t.get("tokenId") or t.get("token_id")
                    if token_id:
                        tokens_data.append({"tokenId": token_id, "outcome": outcome, "price": price})

            if not tokens_data:
                continue

            # Upsert mercado
            market_obj = db.query(Market).filter(Market.market_slug == slug).first()
            if not market_obj:
                market_obj = Market(market_slug=slug, question=question, end_date=end_date)
                db.add(market_obj)
                db.flush()
                novos += 1
            else:
                market_obj.end_date = end_date
                atualizados += 1

            # Upsert tokens e snapshot
            for t in tokens_data:
                token_id = t["tokenId"]
                price = float(t.get("price") or 0)
                outcome = t.get("outcome", "")

                token_obj = db.query(Token).filter(Token.token_id == token_id).first()
                if not token_obj:
                    token_obj = Token(
                        token_id=token_id,
                        outcome=outcome,
                        price=price,
                        market_id=market_obj.id
                    )
                    db.add(token_obj)
                else:
                    token_obj.price = price

                db.add(Snapshot(token_id=token_id, price=price))

        except Exception as e:
            print(f"Erro refresh mercado: {e}")
            continue

    # Fecha mercados expirados
    expired = db.query(Market).filter(
        Market.end_date != None,
        Market.end_date < now
    ).all()
    for m in expired:
        # Verifica se todos tokens estÃ£o em 0% ou 100%
        all_resolved = all(
            t.price <= 0.01 or t.price >= 0.99
            for t in m.tokens
        ) if m.tokens else False
        if all_resolved:
            fechados += 1

    db.commit()

    total = db.query(Market).count()
    return {
        "message": "AtualizaÃ§Ã£o concluÃ­da!",
        "novos_mercados": novos,
        "mercados_atualizados": atualizados,
        "mercados_expirados_detectados": fechados,
        "total_mercados": total,
        "atualizado_em": now.isoformat(),
    }
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# MARKET INEFFICIENCY ENGINE
# Motor quantitativo â€” diferencial do PolySignal
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/metrics/{slug}")
def get_market_metrics(slug: str, db: Session = Depends(get_db)):
    """
    4 mÃ©tricas quantitativas por mercado:
    - volatility_score: movimento atual vs mÃ©dia histÃ³rica
    - mispricing_score: desvio do preÃ§o justo histÃ³rico
    - reversal_probability: % de vezes que reverteu apÃ³s movimento similar
    - confidence_score: convicÃ§Ã£o 0-100 baseada em dados reais
    """
    now = datetime.utcnow()

    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado nÃ£o encontrado"}

    metrics_por_token = []

    for token in market.tokens:
        current_price = token.price
        if current_price == 0:
            continue

        # â”€â”€ Coleta histÃ³rico completo do token â”€â”€
        snapshots = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id)
            .order_by(Snapshot.timestamp.desc())
            .limit(2000)
            .all()
        )

        if len(snapshots) < 10:
            continue

        prices = [s.price for s in snapshots]
        times  = [s.timestamp for s in snapshots]

        # â”€â”€ 1. VOLATILITY SCORE â”€â”€
        # Compara movimento recente vs mÃ©dia histÃ³rica
        recent_prices = prices[:12]   # ~1h
        all_changes = [abs(prices[i] - prices[i+1]) for i in range(len(prices)-1)]
        recent_changes = [abs(recent_prices[i] - recent_prices[i+1]) for i in range(len(recent_prices)-1)]

        avg_historical = sum(all_changes) / max(len(all_changes), 1)
        avg_recent     = sum(recent_changes) / max(len(recent_changes), 1)

        volatility_ratio = round(avg_recent / max(avg_historical, 0.0001), 2)
        volatility_score = min(round(volatility_ratio * 25, 1), 100)

        if volatility_ratio > 3:
            volatility_label = "EXTREMA"
        elif volatility_ratio > 2:
            volatility_label = "ALTA"
        elif volatility_ratio > 1.2:
            volatility_label = "ELEVADA"
        else:
            volatility_label = "NORMAL"

        # â”€â”€ 2. MISPRICING SCORE â”€â”€
        # Compara preÃ§o atual com mÃ©dia histÃ³rica ponderada
        if len(prices) >= 50:
            hist_mean = sum(prices[12:62]) / 50   # mÃ©dia 1h-5h atrÃ¡s
            deviation = abs(current_price - hist_mean)
            mispricing_score = min(round(deviation * 300, 1), 100)
            price_direction = "ACIMA" if current_price > hist_mean else "ABAIXO"
            hist_mean_pct = round(hist_mean * 100, 1)
        else:
            mispricing_score = 0
            price_direction = "NEUTRO"
            hist_mean_pct = round(current_price * 100, 1)

        # â”€â”€ 3. REVERSAL PROBABILITY â”€â”€
        # % de vezes que apÃ³s subida/queda similar, o preÃ§o reverteu
        reversals = 0
        total_similar = 0
        window = 6  # ~30 min

        for i in range(window, len(prices) - window):
            move = prices[i-window] - prices[i]  # movimento antes
            future = prices[i] - prices[i+window]  # movimento depois

            if abs(move) > 0.03:  # movimento > 3%
                total_similar += 1
                if (move > 0 and future < -0.01) or (move < 0 and future > 0.01):
                    reversals += 1

        reversal_probability = round(reversals / max(total_similar, 1) * 100, 1)

        # â”€â”€ 4. CONFIDENCE SCORE â”€â”€
        # ConvicÃ§Ã£o baseada em volume de evidÃªncias
        evidence_points = 0
        evidence_points += min(len(snapshots) / 100, 20)      # histÃ³rico longo
        evidence_points += min(volatility_score * 0.3, 25)    # volatilidade
        evidence_points += min(mispricing_score * 0.3, 25)    # distorÃ§Ã£o
        evidence_points += min(total_similar / 5, 15)         # padrÃµes similares
        if reversal_probability > 60:
            evidence_points += 15                              # alta prob reversÃ£o

        confidence_score = min(round(evidence_points, 1), 100)

        # â”€â”€ Edge detectado â”€â”€
        edge = None
        edge_direction = None

        if mispricing_score >= 20 and confidence_score >= 40:
            if price_direction == "ACIMA":
                edge = "POSSIVEL_QUEDA"
                edge_direction = "NO"
            else:
                edge = "POSSIVEL_SUBIDA"
                edge_direction = "YES"

        if volatility_ratio > 2 and reversal_probability > 55:
            edge = "REVERSAO_PROVAVEL"
            recent_change = prices[0] - prices[min(12, len(prices)-1)]
            edge_direction = "NO" if recent_change > 0 else "YES"

        metrics_por_token.append({
            "outcome": token.outcome,
            "current_price_pct": round(current_price * 100, 1),
            "volatility": {
                "score": volatility_score,
                "label": volatility_label,
                "ratio_vs_historico": volatility_ratio,
            },
            "mispricing": {
                "score": mispricing_score,
                "preco_historico_medio_pct": hist_mean_pct,
                "desvio_direcao": price_direction,
            },
            "reversal": {
                "probability_pct": reversal_probability,
                "padroes_similares_encontrados": total_similar,
            },
            "confidence_score": confidence_score,
            "edge": edge,
            "edge_direction": edge_direction,
            "snapshots_analisados": len(snapshots),
        })

    if not metrics_por_token:
        return {"error": "Dados insuficientes para anÃ¡lise"}

    # Score geral do mercado
    max_confidence = max(m["confidence_score"] for m in metrics_por_token)
    has_edge = any(m["edge"] for m in metrics_por_token)

    return {
        "market": market.question,
        "slug": slug,
        "has_edge": has_edge,
        "max_confidence": max_confidence,
        "tokens": metrics_por_token,
        "polymarket_url": f"https://polymarket.com/event/{slug}",
        "analisado_em": now.isoformat(),
    }


@app.get("/inefficiencies")
def get_inefficiencies(db: Session = Depends(get_db)):
    """
    TOP mercados com maior distorÃ§Ã£o estatÃ­stica agora.
    SÃ³ mostra onde existe edge real baseado em histÃ³rico.
    Este Ã© o diferencial do PolySignal.
    """
    now = datetime.utcnow()
    window_5m  = now - timedelta(minutes=5)
    window_1h  = now - timedelta(hours=1)

    results = []
    tokens = db.query(Token).all()

    for token in tokens:
        current_price = token.price
        if current_price < 0.10 or current_price > 0.90:
            continue
        if current_price == 0:
            continue

        snapshots = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id)
            .order_by(Snapshot.timestamp.desc())
            .limit(500)
            .all()
        )

        if len(snapshots) < 20:
            continue

        prices = [s.price for s in snapshots]

        # Movimento recente
        snap_5m = next((s for s in snapshots if s.timestamp <= window_5m), None)
        snap_1h = next((s for s in snapshots if s.timestamp <= window_1h), None)

        change_5m = round((current_price - snap_5m.price) * 100, 2) if snap_5m else 0
        change_1h = round((current_price - snap_1h.price) * 100, 2) if snap_1h else None

        if abs(change_5m) < 3:
            continue

        # Volatilidade histÃ³rica
        all_changes = [abs(prices[i] - prices[i+1]) for i in range(min(len(prices)-1, 200))]
        avg_hist = sum(all_changes) / max(len(all_changes), 1)
        volatility_ratio = abs(change_5m / 100) / max(avg_hist, 0.0001)

        # Mispricing
        hist_mean = sum(prices[12:62]) / min(50, len(prices[12:62])) if len(prices) > 12 else current_price
        mispricing = abs(current_price - hist_mean)
        mispricing_score = min(round(mispricing * 300, 1), 100)

        # ReversÃ£o histÃ³rica
        reversals = 0
        total_similar = 0
        for i in range(6, min(len(prices) - 6, 200)):
            move = prices[i-6] - prices[i]
            future = prices[i] - prices[i+6]
            if abs(move) > 0.03:
                total_similar += 1
                if (move > 0 and future < -0.01) or (move < 0 and future > 0.01):
                    reversals += 1

        reversal_prob = round(reversals / max(total_similar, 1) * 100, 1)

        # Score de ineficiÃªncia
        ineff_score = round(
            (volatility_ratio * 20) +
            (mispricing_score * 0.4) +
            (reversal_prob * 0.4),
            1
        )
        ineff_score = min(ineff_score, 100)

        if ineff_score < 25:
            continue

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        # Edge direction
        if change_5m > 0 and reversal_prob > 55:
            edge = "REVERSAO_QUEDA"
            apostar = "NO"
        elif change_5m < 0 and reversal_prob > 55:
            edge = "REVERSAO_SUBIDA"
            apostar = "YES"
        elif abs(change_5m) > 8:
            edge = "MOVIMENTO_EXTREMO"
            apostar = "NO" if change_5m > 0 else "YES"
        else:
            edge = "DISTORCAO"
            apostar = "YES" if current_price < hist_mean else "NO"

        # ConvicÃ§Ã£o
        if ineff_score >= 70:
            conviction = "ðŸ”´ ALTA"
        elif ineff_score >= 50:
            conviction = "ðŸŸ  MÃ‰DIA"
        elif ineff_score >= 35:
            conviction = "ðŸŸ¡ MODERADA"
        else:
            conviction = "ðŸ”µ BAIXA"

        results.append({
            "market": market.question,
            "slug": market.market_slug,
            "outcome": token.outcome,
            "current_price_pct": round(current_price * 100, 1),
            "change_5m": change_5m,
            "change_1h": change_1h,
            "ineficiencia_score": ineff_score,
            "conviction": conviction,
            "edge_tipo": edge,
            "apostar": apostar,
            "metricas": {
                "volatilidade_vs_historico": round(volatility_ratio, 2),
                "mispricing_score": mispricing_score,
                "reversal_probability_pct": reversal_prob,
                "padroes_similares": total_similar,
            },
            "snapshots_analisados": len(snapshots),
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
            "detectado_em": now.isoformat(),
        })

    results.sort(key=lambda x: x["ineficiencia_score"], reverse=True)
    top = results[:10]

    return {
        "total_ineficiencias": len(results),
        "top_10": top,
        "resumo": f"{len(results)} distorÃ§Ãµes detectadas. Top 10 com maior edge.",
        "metodologia": "Volatility Ratio + Mispricing Score + Reversal Probability",
        "atualizado_em": now.isoformat(),
    }


@app.get('/inefficiencies/v2')
def get_inefficiencies_v2():
    db = SessionLocal()
    try:
        mercados = db.query(Market).filter(Market.tokens.any()).all()
        resultados = []
        for m in mercados:
            for t in m.tokens:
                snaps = db.query(Snapshot).filter_by(token_id=t.token_id).order_by(Snapshot.timestamp.desc()).limit(200).all()
                if len(snaps) < 10:
                    continue
                preco_atual = t.price * 100
                if preco_atual < 5 or preco_atual > 95:
                    continue
                precos = [s.price * 100 for s in snaps]
                media = sum(precos) / len(precos)
                desvio = abs(preco_atual - media)
                volatilidade = max(precos) - min(precos)
                if desvio < 1.5:
                    continue
                score = round(min(100, (desvio / max(volatilidade, 1)) * 100), 1)
                resultados.append({'market': m.question, 'slug': m.market_slug, 'outcome': t.outcome, 'preco_atual': round(preco_atual, 1), 'media_historica': round(media, 1), 'desvio': round(desvio, 1), 'score': score, 'snapshots': len(snaps), 'polymarket_url': f'https://polymarket.com/event/{m.market_slug}'})
        resultados.sort(key=lambda x: x['score'], reverse=True)
        return {'total': len(resultados), 'top_10': resultados[:10], 'metodologia': 'Desvio do preco historico medio', 'atualizado_em': datetime.utcnow().isoformat()}
    finally:
        db.close()

@app.get('/backtest')
def backtest():
    db = SessionLocal()
    try:
        mercados = db.query(Market).filter(Market.tokens.any()).all()
        acertos = 0
        erros = 0
        amostra = []
        for m in mercados:
            for t in m.tokens:
                snaps = db.query(Snapshot).filter_by(token_id=t.token_id).order_by(Snapshot.timestamp.asc()).all()
                if len(snaps) < 50:
                    continue
                for i in range(10, len(snaps) - 10):
                    preco_entrada = snaps[i].price * 100
                    if preco_entrada < 10 or preco_entrada > 90:
                        continue
                    precos_ant = [s.price * 100 for s in snaps[max(0,i-10):i]]
                    media_ant = sum(precos_ant) / len(precos_ant)
                    variacao = preco_entrada - media_ant
                    if abs(variacao) < 5:
                        continue
                    preco_futuro = snaps[min(i+10, len(snaps)-1)].price * 100
                    acertou = (variacao > 0 and preco_futuro > preco_entrada) or (variacao < 0 and preco_futuro < preco_entrada)
                    if acertou: acertos += 1
                    else: erros += 1
                    if len(amostra) < 20:
                        amostra.append({'market': m.question, 'outcome': t.outcome, 'entrada': round(preco_entrada,1), 'saida': round(preco_futuro,1), 'resultado': 'ACERTO' if acertou else 'ERRO'})
        total = acertos + erros
        win_rate = round((acertos / total * 100), 1) if total > 0 else 0
        return {'total_simulados': total, 'acertos': acertos, 'erros': erros, 'win_rate_pct': win_rate, 'amostra': amostra, 'atualizado_em': datetime.utcnow().isoformat()}
    finally:
        db.close()

@app.get('/inefficiencies/v3')
def get_inefficiencies_v3():
    db = SessionLocal()
    try:
        tokens = db.query(Token).limit(200).all()
        resultados = []
        for t in tokens:
            preco_atual = t.price * 100
            if preco_atual < 5 or preco_atual > 95:
                continue
            snaps = db.query(Snapshot).filter_by(token_id=t.token_id).order_by(Snapshot.timestamp.desc()).limit(50).all()
            if len(snaps) < 10:
                continue
            precos = [s.price * 100 for s in snaps]
            media = sum(precos) / len(precos)
            desvio = abs(preco_atual - media)
            if desvio < 2:
                continue
            volatilidade = max(precos) - min(precos)
            score = round(min(100, (desvio / max(volatilidade, 0.1)) * 100), 1)
            mercado = db.query(Market).filter_by(id=t.market_id).first()
            if not mercado:
                continue
            resultados.append({'market': mercado.question, 'slug': mercado.market_slug, 'outcome': t.outcome, 'preco_atual': round(preco_atual,1), 'media_historica': round(media,1), 'desvio': round(desvio,1), 'score': score, 'polymarket_url': f'https://polymarket.com/event/{mercado.market_slug}'})
        resultados.sort(key=lambda x: x['score'], reverse=True)
        return {'total': len(resultados), 'top_10': resultados[:10], 'atualizado_em': datetime.utcnow().isoformat()}
    finally:
        db.close()

@app.get('/backtest/v2')
def backtest_v2():
    db = SessionLocal()
    try:
        tokens = db.query(Token).limit(50).all()
        acertos = 0
        erros = 0
        amostra = []
        for t in tokens:
            snaps = db.query(Snapshot).filter_by(token_id=t.token_id).order_by(Snapshot.timestamp.asc()).limit(100).all()
            if len(snaps) < 20:
                continue
            for i in range(5, len(snaps) - 5):
                preco_entrada = snaps[i].price * 100
                if preco_entrada < 10 or preco_entrada > 90:
                    continue
                precos_ant = [s.price * 100 for s in snaps[max(0,i-5):i]]
                media_ant = sum(precos_ant) / len(precos_ant)
                variacao = preco_entrada - media_ant
                if abs(variacao) < 3:
                    continue
                preco_futuro = snaps[min(i+5, len(snaps)-1)].price * 100
                acertou = (variacao > 0 and preco_futuro > preco_entrada) or (variacao < 0 and preco_futuro < preco_entrada)
                if acertou: acertos += 1
                else: erros += 1
                if len(amostra) < 15:
                    mercado = db.query(Market).filter_by(id=t.market_id).first()
                    amostra.append({'market': mercado.question if mercado else '?', 'outcome': t.outcome, 'entrada': round(preco_entrada,1), 'saida': round(preco_futuro,1), 'resultado': 'ACERTO' if acertou else 'ERRO'})
        total = acertos + erros
        win_rate = round((acertos / total * 100), 1) if total > 0 else 0
        return {'total_simulados': total, 'acertos': acertos, 'erros': erros, 'win_rate_pct': win_rate, 'amostra': amostra, 'atualizado_em': datetime.utcnow().isoformat()}
    finally:
        db.close()

@app.get('/debug/tokens')
def debug_tokens():
    db = SessionLocal()
    try:
        total_tokens = db.query(Token).count()
        tokens_sample = db.query(Token).limit(20).all()
        total_snaps = db.query(Snapshot).count()
        return {
            'total_tokens': total_tokens,
            'total_snapshots': total_snaps,
            'amostra_tokens': [{'id': t.token_id, 'outcome': t.outcome, 'price': t.price, 'price_pct': round(t.price * 100, 1)} for t in tokens_sample]
        }
    finally:
        db.close()

@app.get('/inefficiencies/v4')
def get_inefficiencies_v4():
    db = SessionLocal()
    try:
        tokens_ativos = db.query(Token).filter(Token.price > 0.05, Token.price < 0.95).all()
        resultados = []
        for t in tokens_ativos:
            snaps = db.query(Snapshot).filter_by(token_id=t.token_id).order_by(Snapshot.timestamp.desc()).limit(50).all()
            if len(snaps) < 10:
                continue
            preco_atual = t.price * 100
            precos = [s.price * 100 for s in snaps]
            media = sum(precos) / len(precos)
            desvio = abs(preco_atual - media)
            if desvio < 2:
                continue
            volatilidade = max(precos) - min(precos)
            score = round(min(100, (desvio / max(volatilidade, 0.1)) * 100), 1)
            mercado = db.query(Market).filter_by(id=t.market_id).first()
            if not mercado:
                continue
            resultados.append({'market': mercado.question, 'slug': mercado.market_slug, 'outcome': t.outcome, 'preco_atual': round(preco_atual,1), 'media_historica': round(media,1), 'desvio': round(desvio,1), 'score': score, 'polymarket_url': f'https://polymarket.com/event/{mercado.market_slug}'})
        resultados.sort(key=lambda x: x['score'], reverse=True)
        return {'total': len(resultados), 'tokens_ativos_encontrados': len(tokens_ativos), 'top_10': resultados[:10], 'atualizado_em': datetime.utcnow().isoformat()}
    finally:
        db.close()

@app.get('/backtest/v3')
def backtest_v3():
    db = SessionLocal()
    try:
        tokens_ativos = db.query(Token).filter(Token.price > 0.05, Token.price < 0.95).limit(30).all()
        acertos = 0
        erros = 0
        amostra = []
        for t in tokens_ativos:
            snaps = db.query(Snapshot).filter_by(token_id=t.token_id).order_by(Snapshot.timestamp.asc()).all()
            if len(snaps) < 20:
                continue
            for i in range(5, len(snaps) - 5):
                preco_entrada = snaps[i].price * 100
                if preco_entrada < 10 or preco_entrada > 90:
                    continue
                precos_ant = [s.price * 100 for s in snaps[max(0,i-5):i]]
                media_ant = sum(precos_ant) / len(precos_ant)
                variacao = preco_entrada - media_ant
                if abs(variacao) < 3:
                    continue
                preco_futuro = snaps[min(i+5, len(snaps)-1)].price * 100
                acertou = (variacao > 0 and preco_futuro > preco_entrada) or (variacao < 0 and preco_futuro < preco_entrada)
                if acertou: acertos += 1
                else: erros += 1
                if len(amostra) < 15:
                    mercado = db.query(Market).filter_by(id=t.market_id).first()
                    amostra.append({'market': mercado.question if mercado else '?', 'outcome': t.outcome, 'entrada': round(preco_entrada,1), 'saida': round(preco_futuro,1), 'resultado': 'ACERTO' if acertou else 'ERRO'})
        total = acertos + erros
        win_rate = round((acertos / total * 100), 1) if total > 0 else 0
        return {'total_simulados': total, 'acertos': acertos, 'erros': erros, 'win_rate_pct': win_rate, 'tokens_ativos': len(tokens_ativos), 'amostra': amostra, 'atualizado_em': datetime.utcnow().isoformat()}
    finally:
        db.close() 
@app.get("/intelligence/v3/{slug}")
def intelligence_v3(slug: str, db: Session = Depends(get_db)):
    """
    Intelligence determinística (sem Anthropic):
    - score baseado em: desvio do preço vs média recente + sentimento simples das notícias
    """
    try:
        market = db.query(Market).filter_by(market_slug=slug).first()
        if not market:
            return {"error": "market_not_found", "slug": slug}

        token_yes = db.query(Token).filter_by(market_id=market.id, outcome="YES").first()
        token_no = db.query(Token).filter_by(market_id=market.id, outcome="NO").first()

        yes_price = round((token_yes.price or 0) * 100, 1) if token_yes else 0.0
        no_price = round((token_no.price or 0) * 100, 1) if token_no else round(100 - yes_price, 1)

        # snapshots recentes do YES (se existir)
        snaps = []
        if token_yes:
            snaps = (
                db.query(Snapshot)
                .filter_by(token_id=token_yes.token_id)
                .order_by(Snapshot.timestamp.desc())
                .limit(60)
                .all()
            )

        hist = [s.price * 100 for s in snaps if s.price is not None]
        hist_mean = round(sum(hist) / len(hist), 1) if len(hist) >= 10 else None
        hist_min = round(min(hist), 1) if len(hist) >= 10 else None
        hist_max = round(max(hist), 1) if len(hist) >= 10 else None
        deviation = round(abs(yes_price - hist_mean), 1) if hist_mean is not None else None

        # Notícias do cache (mesma fonte que você já usa no /intelligence atual)
        articles = []

        # Sentimento simples por palavras-chave
        POS = ["confirmed", "wins", "approved", "deal", "ceasefire", "success", "announced", "elected", "signed", "released"]
        NEG = ["killed", "dead", "attack", "war", "explosion", "sanctions", "crisis", "fails", "rejects", "collapse"]

        pos_count = 0
        neg_count = 0
        for a in articles:
            t = (a.get("title", "") or "").lower()
            d = (a.get("description", "") or "").lower()
            text = t + " " + d
            if any(w in text for w in POS):
                pos_count += 1
            if any(w in text for w in NEG):
                neg_count += 1

        if pos_count > neg_count:
            sentimento = "POSITIVO"
        elif neg_count > pos_count:
            sentimento = "NEGATIVO"
        else:
            sentimento = "NEUTRO"

        # Score base: começa neutro
        score = 50.0

        # Ajuste por desvio do preço vs média (se o mercado “mudou muito”, aumenta score de “sinal”)
        if deviation is not None:
            # cada 5 pontos de desvio adiciona 10 pontos (cap)
            score += min(25.0, (deviation / 5.0) * 10.0)

        # Ajuste por sentimento das notícias
        if sentimento == "POSITIVO":
            score += 10.0
        elif sentimento == "NEGATIVO":
            score -= 10.0

        # Clamp 0-100
        score = max(0.0, min(100.0, score))
        score = round(score, 1)

        if score >= 65:
            recomendacao = "APOSTE YES"
            cor = "green"
        elif score <= 35:
            recomendacao = "APOSTE NO"
            cor = "red"
        else:
            recomendacao = "EVITE"
            cor = "yellow"

        razao = []
        if deviation is not None:
            razao.append(f"Preço YES={yes_price}% vs média recente={hist_mean}% (desvio {deviation} pts)")
        else:
            razao.append("Histórico insuficiente para média/ desvio")

        razao.append(f"Notícias: {pos_count} positivas, {neg_count} negativas → {sentimento}")

        return {
            "market": market.question,
            "slug": slug,
            "yes_price_mercado": yes_price,
            "no_price_mercado": no_price,
            "score_yes": score,
            "recomendacao": recomendacao,
            "sinal_cor": cor,
            "explicacao": " | ".join(razao),
            "news_count": len(articles),
            "noticias": [
                {
                    "title": a.get("title"),
                    "source": a.get("source"),
                    "url": a.get("url"),
                    "published_at": a.get("published_at"),
                }
                for a in articles[:5]
            ],
            "polymarket_url": f"https://polymarket.com/event/{slug}",
            "atualizado_em": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {"error": "intelligence_v3_failed", "detail": str(e)}
from models import Signal

@app.get("/signals/v1")
def signals_v1(limit: int = 50, db: Session = Depends(get_db)):
    rows = (
        db.query()       .order_by(())
        .limit(min(limit, 200))
        .all()
    )
    return {
        "total": len(rows),
        "signals": [
            {
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "market": r.market,
                "slug": r.slug,
                "outcome": r.outcome,
                "tipo": r.tipo,
                "change_5m": r.change_5m,
                "current_price": r.current_price,
                "confidence": r.confidence,
                "polymarket_url": r.polymarket_url,
            }
            for r in rows
        ],
    }    # ==============================
# SIGNAL SCANNER API
# ==============================

from models import Signal

@app.get("/signals/v1")
def signals_v1(limit: int = 50, db: Session = Depends(get_db)):
    rows = (
        db.query(Signal)
        .order_by(Signal.created_at.desc())
        .limit(min(limit, 200))
        .all()
    )

    return {
        "total": len(rows),
        "signals": [
            {
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "market": r.market,
                "slug": r.slug,
                "outcome": r.outcome,
                "tipo": r.tipo,
                "change_5m": r.change_5m,
                "current_price": r.current_price,
                "confidence": r.confidence,
                "polymarket_url": r.polymarket_url,
            }
            for r in rows
        ],
    }