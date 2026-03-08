# ============================================================
# PolySignal — Backend Principal
# FastAPI + PostgreSQL + Railway
# ============================================================

import os
import json
import time
import hmac
import hashlib
import base64
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text, func, desc
from sqlalchemy.orm import Session

from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot, Trade, Signal

# ── Cria tabelas ─────────────────────────────────────────────
Base.metadata.create_all(bind=engine)

app = FastAPI(title="PolySignal API", version="3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Env vars ─────────────────────────────────────────────────
NEWSAPI_KEY        = os.environ.get("NEWSAPI_KEY", "")
ANTHROPIC_KEY      = os.environ.get("ANTHROPIC_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "") or os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
POLY_API_KEY       = os.environ.get("POLY_API_KEY", "")
POLY_PASSPHRASE    = os.environ.get("POLY_PASSPHRASE", "")
POLY_SECRET        = os.environ.get("POLY_SECRET", "")
POLY_ADDRESS       = os.environ.get("POLY_ADDRESS", "") or os.environ.get("POLY_ADDRESSSS", "")

GAMMA_API    = "https://gamma-api.polymarket.com"
DATA_API     = "https://data-api.polymarket.com"
CLOB_API     = "https://clob.polymarket.com"
HEADERS      = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://polymarket.com",
}

# ── Estado global ─────────────────────────────────────────────
_LAST_ALERT_SENT_AT = None


# ─────────────────────────────────────────────────────────────
# DB SESSION
# ─────────────────────────────────────────────────────────────

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────
# ROOT
# ─────────────────────────────────────────────────────────────

@app.get("/")
def home():
    return {"status": "ok", "version": "3.0", "service": "PolySignal"}


# ─────────────────────────────────────────────────────────────
# STATUS
# ─────────────────────────────────────────────────────────────

@app.get("/status")
def status(db: Session = Depends(get_db)):
    last_snap = db.query(func.max(Snapshot.timestamp)).scalar()
    age_minutes = None
    if last_snap:
        age_minutes = round((datetime.utcnow() - last_snap).total_seconds() / 60, 1)

    return {
        "total_markets": db.query(Market).count(),
        "markets_with_tokens": db.query(Market).join(Token).distinct().count(),
        "total_tokens": db.query(Token).count(),
        "total_snapshots": db.query(Snapshot).count(),
        "total_signals": db.query(Signal).count(),
        "last_snapshot": last_snap.isoformat() if last_snap else None,
        "snapshot_age_minutes": age_minutes,
        "worker_healthy": age_minutes is not None and age_minutes < 10,
        "now": datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# MARKETS
# ─────────────────────────────────────────────────────────────

@app.get("/markets")
def get_active_markets(db: Session = Depends(get_db)):
    # Garante coluna timestamp correta
    try:
        db.execute(text("""
            ALTER TABLE markets ALTER COLUMN end_date TYPE TIMESTAMP
            USING CASE WHEN end_date IS NULL OR end_date = '' THEN NULL
            ELSE end_date::TIMESTAMP END
        """))
        db.commit()
    except Exception:
        db.rollback()

    now = datetime.utcnow()
    markets = (
        db.query(Market).join(Token)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().all()
    )

    result = []
    for m in markets:
        yes_price = no_price = None
        for t in m.tokens:
            o = (t.outcome or "").upper()
            if o == "YES":
                yes_price = round(t.price * 100, 1)
            elif o == "NO":
                no_price = round(t.price * 100, 1)
        if yes_price == 0 and no_price == 0:
            continue
        result.append({
            "id": m.id,
            "question": m.question,
            "slug": m.market_slug,
            "market_slug": m.market_slug,   # compatibilidade
            "end_date": str(m.end_date) if m.end_date else None,
            "yes_price": yes_price,
            "no_price": no_price,
            "tokens": [
                {"outcome": t.outcome, "price": round(t.price, 4), "token_id": t.token_id}
                for t in m.tokens
            ],
            "polymarket_url": f"https://polymarket.com/event/{m.market_slug}",
        })
    return result


@app.get("/market/{slug}")
def get_market_detail(slug: str, db: Session = Depends(get_db)):
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado não encontrado"}

    tokens_detail = []
    for token in market.tokens:
        snapshots = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id)
            .order_by(Snapshot.timestamp.desc())
            .limit(100).all()
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
        "tokens": tokens_detail,
        "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
    }


@app.get("/history/{token_id}")
def get_history(token_id: str, limit: int = 100, db: Session = Depends(get_db)):
    snapshots = (
        db.query(Snapshot)
        .filter(Snapshot.token_id == token_id)
        .order_by(Snapshot.timestamp.desc())
        .limit(limit).all()
    )
    return [{"price": round(s.price * 100, 1), "timestamp": str(s.timestamp)} for s in snapshots]


# ─────────────────────────────────────────────────────────────
# MOVERS & ANOMALIES
# ─────────────────────────────────────────────────────────────

@app.get("/movers")
def get_movers(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    window_1h = now - timedelta(hours=1)
    movers = []

    for token in db.query(Token).all():
        snap_old = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= window_1h)
            .order_by(Snapshot.timestamp.desc()).first()
        )
        if not snap_old:
            continue
        change = round((token.price - snap_old.price) * 100, 2)
        if abs(change) < 2.0:
            continue
        market = db.query(Market).filter(Market.id == token.market_id).first()
        movers.append({
            "market": market.question if market else "Unknown",
            "slug": market.market_slug if market else None,
            "outcome": token.outcome,
            "current_price": round(token.price * 100, 1),
            "change_1h": change,
            "direction": "UP" if change > 0 else "DOWN",
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}" if market else None,
        })

    movers.sort(key=lambda x: abs(x["change_1h"]), reverse=True)
    return movers[:20]


@app.get("/anomalies")
def get_anomalies(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    w5m  = now - timedelta(minutes=5)
    w15m = now - timedelta(minutes=15)
    w1h  = now - timedelta(hours=1)
    anomalies = []

    for token in db.query(Token).all():
        cp = token.price
        if cp >= 0.95 or cp <= 0.05 or cp == 0:
            continue

        def get_snap(w):
            return (
                db.query(Snapshot)
                .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= w)
                .order_by(Snapshot.timestamp.desc()).first()
            )

        s5m = get_snap(w5m)
        s15m = get_snap(w15m)
        s1h = get_snap(w1h)
        if not s5m:
            continue

        c5m  = round((cp - s5m.price) * 100, 2)
        c15m = round((cp - s15m.price) * 100, 2) if s15m else None
        c1h  = round((cp - s1h.price) * 100, 2) if s1h else None

        if abs(c5m) < 3.0:
            continue

        if abs(c5m) >= 20:
            tipo, level = "EXTREME", "EXTREME"
        elif c5m > 0 and (c1h or 0) > 0:
            tipo, level = "SPIKE", "HIGH"
        elif c5m < 0 and (c1h or 0) < 0:
            tipo, level = "DUMP", "HIGH"
        elif c5m > 0 and (c1h or 0) < 0:
            tipo, level = "REVERSAL_UP", "MEDIUM"
        elif c5m < 0 and (c1h or 0) > 0:
            tipo, level = "REVERSAL_DOWN", "MEDIUM"
        else:
            tipo, level = "MOVE", "LOW"

        score = min(abs(c5m) * 2 + abs(c1h or 0) + (20 if c15m and abs(c15m) > abs(c5m) else 0), 100)

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        anomalies.append({
            "market": market.question,
            "slug": market.market_slug,
            "outcome": token.outcome,
            "tipo": tipo,
            "alert_level": level,
            "confianca_score": round(score, 0),
            "current_price": round(cp * 100, 1),
            "change_5m": c5m,
            "change_15m": c15m,
            "change_1h": c1h,
            "oportunidade": "POSSIVEL_YES" if c5m > 0 and cp < 0.8 else "POSSIVEL_NO" if c5m < 0 and cp > 0.2 else "AGUARDAR",
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
            "detected_at": now.isoformat(),
        })

    anomalies.sort(key=lambda x: x["confianca_score"], reverse=True)
    return anomalies[:20]


# ─────────────────────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────────────────────

@app.post("/cleanup")
def cleanup_old_markets(db: Session = Depends(get_db)):
    old_years = ["2020", "2021", "2019", "2018", "2017"]
    removed = 0
    for m in db.query(Market).all():
        slug = m.market_slug or ""
        question = m.question or ""
        is_old = any(y in slug or y in question for y in old_years)
        all_zero = all(t.price == 0 for t in m.tokens) if m.tokens else True
        if is_old and all_zero:
            for token in m.tokens:
                db.query(Snapshot).filter(Snapshot.token_id == token.token_id).delete()
                db.delete(token)
            db.delete(m)
            removed += 1
    db.commit()
    return {"message": "Limpeza concluída!", "removidos": removed, "total_restante": db.query(Market).count()}


# ─────────────────────────────────────────────────────────────
# REFRESH
# ─────────────────────────────────────────────────────────────

@app.post("/refresh")
def refresh_markets(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    try:
        resp = requests.get(
            f"{GAMMA_API}/markets?limit=200&active=true&order=volume24hr&ascending=false",
            timeout=15
        )
        markets_raw = resp.json()
        if isinstance(markets_raw, dict):
            markets_raw = markets_raw.get("markets", [])
    except Exception as e:
        return {"error": f"Erro ao buscar mercados: {e}"}

    novos = atualizados = 0
    for m in markets_raw:
        try:
            slug = m.get("slug")
            question = m.get("question")
            if not slug:
                continue

            end_date = None
            ed = m.get("endDate")
            if ed:
                try:
                    end_date = datetime.fromisoformat(ed.replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception:
                    try:
                        end_date = datetime.strptime(ed[:10], "%Y-%m-%d")
                    except Exception:
                        pass

            tokens_data = []
            op = m.get("outcomePrices")
            if op:
                try:
                    prices = json.loads(op) if isinstance(op, str) else op
                    clob_ids = m.get("clobTokenIds", "[]")
                    ids = json.loads(clob_ids) if isinstance(clob_ids, str) else clob_ids
                    if len(prices) >= 2:
                        tokens_data = [
                            {"tokenId": ids[0] if ids else f"{slug}_YES", "outcome": "YES", "price": float(prices[0])},
                            {"tokenId": ids[1] if len(ids) > 1 else f"{slug}_NO", "outcome": "NO", "price": float(prices[1])},
                        ]
                except Exception:
                    pass

            if not tokens_data:
                for t in m.get("tokens", []):
                    tid = t.get("tokenId") or t.get("token_id")
                    if tid:
                        tokens_data.append({
                            "tokenId": tid,
                            "outcome": (t.get("outcome") or "").upper(),
                            "price": float(t.get("price") or 0),
                        })

            if not tokens_data:
                continue

            market_obj = db.query(Market).filter(Market.market_slug == slug).first()
            if not market_obj:
                market_obj = Market(market_slug=slug, question=question, end_date=end_date)
                db.add(market_obj)
                db.flush()
                novos += 1
            else:
                market_obj.end_date = end_date
                atualizados += 1

            for t in tokens_data:
                tid = t["tokenId"]
                price = float(t.get("price") or 0)
                outcome = t.get("outcome", "")
                token_obj = db.query(Token).filter(Token.token_id == tid).first()
                if not token_obj:
                    token_obj = Token(token_id=tid, outcome=outcome, price=price, market_id=market_obj.id)
                    db.add(token_obj)
                else:
                    token_obj.price = price
                db.add(Snapshot(token_id=tid, price=price, timestamp=now))

        except Exception as e:
            print(f"[refresh] erro: {e}")
            continue

    db.commit()
    return {
        "message": "Atualização concluída!",
        "novos_mercados": novos,
        "mercados_atualizados": atualizados,
        "total_mercados": db.query(Market).count(),
        "atualizado_em": now.isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# TRADES
# ─────────────────────────────────────────────────────────────

class TradeCreate(BaseModel):
    market: str                  # pode ser slug OU question
    outcome: str                 # "YES" ou "NO"
    amount: float
    entry_price: float = 0.0    # 0 = usar preço atual do token
    notes: str = ""

@app.post("/trades")
def open_trade(body: TradeCreate, db: Session = Depends(get_db)):
    # Tenta achar pelo slug primeiro, depois pela question
    market = (
        db.query(Market).filter(Market.market_slug == body.market).first()
        or db.query(Market).filter(Market.question.ilike(f"%{body.market[:40]}%")).first()
    )
    if not market:
        # Cria trade genérico mesmo sem mercado no banco (para GeoBot)
        entry_price = round(float(body.entry_price), 2) if body.entry_price > 0 else 50.0
        shares = round(body.amount / entry_price * 100, 4) if entry_price > 0 else 0
        trade = Trade(
            market_slug=body.market[:200], question=body.market[:500],
            outcome=body.outcome.upper(), amount=body.amount,
            entry_price=entry_price, shares=shares, notes=body.notes, status="open"
        )
        db.add(trade); db.commit(); db.refresh(trade)
        return {"message": "Aposta registrada (mercado não encontrado no banco)!",
                "trade_id": trade.id, "entry_price": f"{entry_price}%", "shares": shares}

    outcome_upper = body.outcome.upper()
    token = db.query(Token).filter(Token.market_id == market.id, Token.outcome == outcome_upper).first()
    entry_price = round(token.price * 100, 2) if token else round(float(body.entry_price), 2)
    if entry_price == 0:
        entry_price = round(float(body.entry_price), 2) or 50.0
    shares = round(body.amount / entry_price * 100, 4) if entry_price > 0 else 0
    trade = Trade(
        market_slug=market.market_slug, question=market.question,
        outcome=outcome_upper, amount=body.amount,
        entry_price=entry_price, shares=shares, notes=body.notes, status="open"
    )
    db.add(trade); db.commit(); db.refresh(trade)
    return {"message": "Aposta registrada!", "trade_id": trade.id,
            "entry_price": f"{entry_price}%", "shares": shares, "created_at": str(trade.created_at)}


@app.get("/trades")
def list_trades(db: Session = Depends(get_db)):
    trades = db.query(Trade).order_by(Trade.created_at.desc()).all()
    result = []
    for t in trades:
        current_price = t.entry_price
        market = db.query(Market).filter(Market.market_slug == t.market_slug).first()
        if market:
            token = db.query(Token).filter(Token.market_id == market.id, Token.outcome == t.outcome).first()
            if token:
                current_price = round(token.price * 100, 2)
        pnl = round((current_price - t.entry_price) / 100 * t.shares, 2) if t.status == "open" else (t.pnl or 0)
        result.append({
            "id": t.id, "market": t.question, "outcome": t.outcome,
            "amount": t.amount, "entry_price": t.entry_price, "current_price": current_price,
            "shares": t.shares, "pnl_usd": pnl, "pnl_pct": round(current_price - t.entry_price, 1),
            "status": t.status, "notes": t.notes, "created_at": str(t.created_at),
        })
    return result


@app.post("/trades/{trade_id}/close")
def close_trade(trade_id: int, db: Session = Depends(get_db)):
    trade = db.query(Trade).filter(Trade.id == trade_id).first()
    if not trade:
        return {"error": "Trade não encontrado"}
    market = db.query(Market).filter(Market.market_slug == trade.market_slug).first()
    token = db.query(Token).filter(Token.market_id == market.id, Token.outcome == trade.outcome).first() if market else None
    exit_price = round(token.price * 100, 2) if token else trade.entry_price
    pnl = round((exit_price - trade.entry_price) / 100 * trade.shares, 2)
    trade.exit_price = exit_price
    trade.pnl = pnl
    trade.status = "won" if pnl > 0 else "lost"
    trade.closed_at = datetime.utcnow()
    db.commit()
    return {"message": "Aposta fechada!", "exit_price": exit_price, "pnl_usd": pnl, "status": trade.status}


@app.get("/performance")
def get_performance(db: Session = Depends(get_db)):
    trades = db.query(Trade).order_by(Trade.created_at.asc()).all()
    if not trades:
        return {"capital_inicial": 100, "capital_atual": 100, "roi_pct": 0, "trades": []}

    pnl_fechados = sum(t.pnl or 0 for t in trades if t.status in ["won", "lost"])
    pnl_abertos = 0
    for t in trades:
        if t.status == "open":
            market = db.query(Market).filter(Market.market_slug == t.market_slug).first()
            if market:
                token = db.query(Token).filter(Token.market_id == market.id, Token.outcome == t.outcome).first()
                if token:
                    pnl_abertos += round((token.price * 100 - t.entry_price) / 100 * t.shares, 2)

    pnl_total = round(pnl_fechados + pnl_abertos, 2)
    capital_atual = round(100 + pnl_total, 2)
    roi = round(pnl_total, 1)
    fechados = [t for t in trades if t.status in ["won", "lost"]]
    ganhos = [t for t in fechados if t.status == "won"]
    taxa_acerto = round(len(ganhos) / max(len(fechados), 1) * 100, 1)

    return {
        "status_geral": "🟢 POSITIVO" if roi > 0 else "🔴 NEGATIVO",
        "capital_inicial": 100,
        "capital_atual": capital_atual,
        "pnl_total_usd": pnl_total,
        "roi_pct": roi,
        "total_apostas": len(trades),
        "apostas_abertas": len([t for t in trades if t.status == "open"]),
        "taxa_acerto_pct": taxa_acerto,
        "atualizado_em": datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# NEWS
# ─────────────────────────────────────────────────────────────

@app.get("/news")
def get_news(query: str = "prediction markets politics economy"):
    articles = []
    try:
        params = {"q": query, "language": "en", "sortBy": "publishedAt", "pageSize": 20, "apiKey": NEWSAPI_KEY}
        resp = requests.get("https://newsapi.org/v2/everything", params=params, timeout=8)
        if resp.status_code == 200:
            for a in resp.json().get("articles", []):
                if not a.get("title") or "[Removed]" in a.get("title", ""):
                    continue
                articles.append({
                    "title": a.get("title"), "description": (a.get("description") or "")[:200],
                    "source": a.get("source", {}).get("name", ""), "url": a.get("url"),
                    "published_at": a.get("publishedAt"), "fonte": "NewsAPI",
                })
    except Exception as e:
        print(f"[news] NewsAPI erro: {e}")

    try:
        params = {"q": "polymarket OR prediction market OR geopolitics", "hl": "en", "gl": "US", "ceid": "US:en"}
        resp = requests.get("https://news.google.com/rss/search", params=params, timeout=8)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:15]:
                articles.append({
                    "title": item.findtext("title", ""), "description": item.findtext("description", "")[:200],
                    "source": "Google News", "url": item.findtext("link", ""),
                    "published_at": item.findtext("pubDate", ""), "fonte": "Google News",
                })
    except Exception as e:
        print(f"[news] Google News erro: {e}")

    return {"total": len(articles), "articles": articles[:40]}


# ─────────────────────────────────────────────────────────────
# REDDIT — via PullPush.io (não bloqueia datacenter)
# ─────────────────────────────────────────────────────────────

SUBREDDITS_POLY  = ["Polymarket", "polymarketbets", "predictionmarkets", "polymarket_analysis", "polymarket_news"]
SUBREDDITS_GERAL = ["worldnews", "geopolitics", "politics", "economics", "ukraine", "middleeast", "CryptoCurrency", "investing"]

PULLPUSH_HEADERS = {
    "User-Agent": "PolySignal/3.0",
    "Accept": "application/json",
}

def _fetch_pullpush(subreddit: str, size: int = 10) -> list:
    """Busca posts via PullPush.io — espelho do Reddit sem bloqueio de datacenter."""
    posts = []
    try:
        url = "https://api.pullpush.io/reddit/search/submission/"
        params = {
            "subreddit": subreddit,
            "size": size,
            "sort": "desc",
            "sort_type": "created_utc",
            "filter": "id,title,selftext,score,num_comments,permalink,url,author,link_flair_text,created_utc,subreddit",
        }
        r = requests.get(url, params=params, headers=PULLPUSH_HEADERS, timeout=10)
        if r.status_code == 200:
            for item in r.json().get("data", []):
                title = (item.get("title") or "").strip()
                if not title:
                    continue
                created = item.get("created_utc", 0)
                posts.append({
                    "title": title,
                    "selftext": (item.get("selftext") or "")[:400],
                    "score": item.get("score", 0),
                    "num_comments": item.get("num_comments", 0),
                    "url": f"https://reddit.com{item.get('permalink', '')}",
                    "external_url": item.get("url", ""),
                    "source": f"r/{subreddit}",
                    "subreddit": subreddit,
                    "is_poly": subreddit in SUBREDDITS_POLY,
                    "author": item.get("author", ""),
                    "flair": item.get("link_flair_text") or "",
                    "created_utc": created,
                    "created_at": datetime.utcfromtimestamp(created).isoformat() if created else None,
                })
        else:
            print(f"[pullpush] r/{subreddit} → HTTP {r.status_code}")
    except Exception as e:
        print(f"[pullpush] r/{subreddit}: {e}")
    return posts


def _fetch_pullpush_search(query: str, size: int = 20) -> list:
    """Busca posts por palavra-chave no PullPush — ex: 'polymarket'."""
    posts = []
    try:
        url = "https://api.pullpush.io/reddit/search/submission/"
        params = {
            "q": query,
            "size": size,
            "sort": "desc",
            "sort_type": "created_utc",
            "filter": "id,title,selftext,score,num_comments,permalink,url,author,link_flair_text,created_utc,subreddit",
        }
        r = requests.get(url, params=params, headers=PULLPUSH_HEADERS, timeout=10)
        if r.status_code == 200:
            for item in r.json().get("data", []):
                title = (item.get("title") or "").strip()
                if not title:
                    continue
                sub = item.get("subreddit", "")
                created = item.get("created_utc", 0)
                posts.append({
                    "title": title,
                    "selftext": (item.get("selftext") or "")[:400],
                    "score": item.get("score", 0),
                    "num_comments": item.get("num_comments", 0),
                    "url": f"https://reddit.com{item.get('permalink', '')}",
                    "external_url": item.get("url", ""),
                    "source": f"r/{sub}",
                    "subreddit": sub,
                    "is_poly": sub in SUBREDDITS_POLY,
                    "author": item.get("author", ""),
                    "flair": item.get("link_flair_text") or "",
                    "created_utc": created,
                    "created_at": datetime.utcfromtimestamp(created).isoformat() if created else None,
                })
    except Exception as e:
        print(f"[pullpush_search] '{query}': {e}")
    return posts


@app.get("/reddit")
def get_reddit(limit: int = Query(60, ge=10, le=100)):
    """Posts recentes sobre Polymarket e contexto global via PullPush.io."""
    posts = []
    seen_urls: set = set()

    def add(new_posts):
        for p in new_posts:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                posts.append(p)

    # 1) Busca keyword "polymarket" em todo Reddit — mais abrangente
    add(_fetch_pullpush_search("polymarket", size=25))
    time.sleep(0.3)

    # 2) Subreddits Polymarket específicos
    for sub in SUBREDDITS_POLY:
        add(_fetch_pullpush(sub, size=10))
        time.sleep(0.2)

    # 3) Subreddits de contexto
    for sub in SUBREDDITS_GERAL:
        add(_fetch_pullpush(sub, size=5))
        time.sleep(0.15)

    # Polymarket primeiro, depois mais recentes
    posts.sort(key=lambda x: (not x["is_poly"], -(x.get("created_utc") or 0)))

    poly_count = sum(1 for p in posts if p["is_poly"])
    return {
        "total": len(posts),
        "polymarket_posts": poly_count,
        "general_posts": len(posts) - poly_count,
        "subreddits_monitored": SUBREDDITS_POLY + SUBREDDITS_GERAL,
        "posts": posts[:limit],
    }


# ─────────────────────────────────────────────────────────────
# NEWS DEEP ANALYSIS — IA analisa impacto em mercados
# ─────────────────────────────────────────────────────────────

@app.get("/news/analysis")
def get_news_analysis(limit: int = Query(8, ge=1, le=20), db: Session = Depends(get_db)):
    """
    Coleta notícias de TODAS as fontes (NewsAPI + Google News + Reddit + GDELT),
    cruza com mercados ativos, e usa Claude IA para análise profunda de impacto.
    Retorna análises educacionais com o que fazer em cada situação.
    """
    # 1) Coletar notícias de todas as fontes
    all_news = []

    # NewsAPI
    try:
        params = {"q": "prediction market OR polymarket OR geopolitics OR election OR war OR economy",
                  "language": "en", "sortBy": "publishedAt", "pageSize": 15, "apiKey": NEWSAPI_KEY}
        r = requests.get("https://newsapi.org/v2/everything", params=params, timeout=8)
        if r.status_code == 200:
            for a in r.json().get("articles", []):
                if a.get("title") and "[Removed]" not in a.get("title",""):
                    all_news.append({
                        "title": a["title"],
                        "description": (a.get("description") or "")[:300],
                        "source": a.get("source",{}).get("name","NewsAPI"),
                        "url": a.get("url",""),
                        "published_at": a.get("publishedAt",""),
                        "fonte_tipo": "news",
                    })
    except Exception as e:
        print(f"[analysis] NewsAPI: {e}")

    # Reddit (PullPush — keyword polymarket)
    try:
        r = requests.get("https://api.pullpush.io/reddit/search/submission/",
            params={"q": "polymarket OR prediction market", "size": 10, "sort": "desc", "sort_type": "created_utc"},
            headers={"User-Agent": "PolySignal/3.0"}, timeout=8)
        if r.status_code == 200:
            for item in r.json().get("data", []):
                title = (item.get("title") or "").strip()
                if title:
                    all_news.append({
                        "title": title,
                        "description": (item.get("selftext") or "")[:300],
                        "source": f"r/{item.get('subreddit','reddit')}",
                        "url": f"https://reddit.com{item.get('permalink','')}",
                        "published_at": datetime.utcfromtimestamp(item.get("created_utc",0)).isoformat(),
                        "fonte_tipo": "reddit",
                    })
    except Exception as e:
        print(f"[analysis] Reddit: {e}")

    # GDELT
    try:
        params = {"query": "election OR war OR attack OR economy OR crisis", "mode": "artlist",
                  "maxrecords": 10, "format": "json", "timespan": "60min", "sort": "datedesc"}
        r = requests.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params, timeout=8)
        if r.status_code == 200:
            for a in r.json().get("articles", []):
                if a.get("title"):
                    all_news.append({
                        "title": a["title"], "description": "",
                        "source": a.get("domain","GDELT"), "url": a.get("url",""),
                        "published_at": a.get("seendate",""), "fonte_tipo": "gdelt",
                    })
    except Exception as e:
        print(f"[analysis] GDELT: {e}")

    if not all_news:
        return {"total": 0, "analyses": [], "summary": "Sem notícias disponíveis no momento."}

    # 2) Buscar mercados ativos do banco
    markets = db.query(Market).join(Token).filter(
        Token.price > 0.05, Token.price < 0.95
    ).distinct().limit(200).all()

    if not markets:
        return {"total": 0, "analyses": [], "summary": "Sem mercados ativos."}

    STOP = {"will","the","this","that","with","from","have","been","they","their","which","what","does","about","after","before","into","more","some","would","could","should","when","where","there"}

    # Categorias de keywords para match semântico
    TOPIC_KEYWORDS = {
        "war": ["war","attack","strike","missile","bomb","military","troops","invasion","conflict","ceasefire"],
        "election": ["election","vote","president","minister","candidate","poll","win","lose","party"],
        "economy": ["inflation","recession","gdp","interest","rate","fed","economy","market","stock","trade"],
        "crypto": ["bitcoin","btc","ethereum","crypto","blockchain","coinbase","binance","defi","token"],
        "sports": ["nba","nfl","soccer","championship","world cup","playoffs","finals","super bowl"],
    }

    # 3) Match notícia × mercado — threshold mais baixo
    top_matches = []
    seen_markets = set()
    for news in all_news[:40]:
        content = (news["title"] + " " + news["description"]).lower()
        best_market = None
        best_overlap = 0
        for m in markets:
            if m.id in seen_markets:
                continue
            q = (m.question or "").lower()
            words = [w for w in q.split() if len(w) > 3 and w not in STOP]

            # Match direto por palavras
            overlap = sum(1 for w in words if w in content)
            relevance = overlap / max(len(words), 1)

            # Boost por categoria temática
            for cat_words in TOPIC_KEYWORDS.values():
                news_has = any(kw in content for kw in cat_words)
                market_has = any(kw in q for kw in cat_words)
                if news_has and market_has:
                    relevance += 0.15

            if overlap >= 1 and relevance > best_overlap:
                best_overlap = relevance
                best_market = m

        if best_market and best_overlap > 0:
            seen_markets.add(best_market.id)
            top_matches.append({"news": news, "market": best_market, "relevance": round(best_overlap*100,1)})

    top_matches.sort(key=lambda x: x["relevance"], reverse=True)
    top_matches = top_matches[:limit]

    if not top_matches:
        # Fallback: analisa as top notícias sem mercado específico
        top_matches = [{"news": n, "market": None, "relevance": 50} for n in all_news[:limit]]

    # 4) Claude IA — análise profunda de cada match
    analyses = []
    for match in top_matches:
        news = match["news"]
        market = match["market"]

        # Busca token YES do mercado
        yes_price = no_price = 50
        if market:
            yes_tok = db.query(Token).filter(Token.market_id == market.id, Token.outcome == "YES").first()
            no_tok  = db.query(Token).filter(Token.market_id == market.id, Token.outcome == "NO").first()
            yes_price = round((yes_tok.price if yes_tok else 0.5) * 100, 1)
            no_price  = round((no_tok.price  if no_tok  else 0.5) * 100, 1)

        market_q = market.question if market else "Mercados de prediction geral"
        market_slug = market.market_slug if market else ""

        prompt = f"""Você é professor expert em prediction markets, análise geopolítica e finanças quantitativas.

NOTÍCIA:
Fonte: {news['source']} ({news['fonte_tipo']})
Título: {news['title']}
Descrição: {news['description'][:400]}

MERCADO AFETADO: {market_q}
Preço atual: YES={yes_price}% | NO={no_price}%

Faça uma análise PROFUNDA e EDUCACIONAL. Ensine o trader como pensar sobre isso.

Responda SOMENTE com JSON válido (sem markdown):
{{
  "titulo_analise": "<título curto e direto em português, max 60 chars>",
  "impacto": "<ALTA|MEDIA|BAIXA>",
  "direcao": "<BULLISH_YES|BEARISH_YES|NEUTRO>",
  "preco_justo_yes": <número 0-100 — sua estimativa do preço correto>,
  "edge": <diferença entre preço_justo e preço_atual — pode ser negativo>,
  "acao_recomendada": "<COMPRAR YES|COMPRAR NO|AGUARDAR|EVITAR>",
  "confianca": <0-100>,
  "raciocinio": "<Explique em 2-3 frases claras POR QUE essa notícia afeta esse mercado>",
  "logica_mercado": "<1 frase: qual a lógica que conecta a notícia ao preço>",
  "o_que_fazer": "<instrução prática direta: ex: 'Comprar YES abaixo de 45% é edge positivo'>",
  "risco_principal": "<principal risco que pode invalidar a tese>",
  "prazo": "<IMEDIATO(1-2h)|CURTO(1-3 dias)|MEDIO(1-2 semanas)>",
  "categoria": "<GEOPOLITICA|ECONOMIA|ELEICOES|CRYPTO|ESPORTES|TECNOLOGIA|OUTROS>",
  "lição": "<1 frase educacional — o que aprender com essa situação para futuros trades>"
}}"""

        try:
            if not ANTHROPIC_KEY:
                raise Exception("ANTHROPIC_KEY não configurada")
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-haiku-4-5-20251001", "max_tokens": 600,
                      "messages": [{"role": "user", "content": prompt}]},
                timeout=25
            )
            if r.status_code == 200:
                text = r.json().get("content",[{}])[0].get("text","{}").strip()
                text = text.replace("```json","").replace("```","").strip()
                # Extrai só o JSON se vier texto antes/depois
                start = text.find("{")
                end = text.rfind("}") + 1
                if start >= 0 and end > start:
                    text = text[start:end]
                ia = json.loads(text)
                print(f"[analysis] ✅ Claude OK: {ia.get('titulo_analise','?')[:40]}")
            else:
                print(f"[analysis] Claude HTTP {r.status_code}: {r.text[:200]}")
                raise Exception(f"HTTP {r.status_code}")
        except Exception as e:
            print(f"[analysis] Claude erro: {e}")
            ia = {
                "titulo_analise": news["title"][:60],
                "impacto": "MEDIA", "direcao": "NEUTRO",
                "preco_justo_yes": yes_price, "edge": 0,
                "acao_recomendada": "AGUARDAR", "confianca": 30,
                "raciocinio": "Análise IA indisponível momentaneamente.",
                "logica_mercado": "—", "o_que_fazer": "Acompanhe o mercado.",
                "risco_principal": "Dados insuficientes.", "prazo": "CURTO",
                "categoria": "OUTROS", "lição": "Sempre espere mais dados antes de agir.",
            }

        analyses.append({
            # Notícia
            "news_title": news["title"],
            "news_source": news["source"],
            "news_url": news["url"],
            "news_tipo": news["fonte_tipo"],
            "news_published": news.get("published_at",""),
            # Mercado
            "market_question": market_q,
            "market_slug": market_slug,
            "market_yes_price": yes_price,
            "market_no_price": no_price,
            "polymarket_url": f"https://polymarket.com/event/{market_slug}" if market_slug else "",
            "relevance_score": match["relevance"],
            # Análise IA
            **ia,
        })
        time.sleep(0.3)  # rate limit

    # 5) Resumo geral
    bullish = sum(1 for a in analyses if a.get("direcao") == "BULLISH_YES")
    bearish = sum(1 for a in analyses if a.get("direcao") == "BEARISH_YES")
    high_impact = sum(1 for a in analyses if a.get("impacto") == "ALTA")
    buy_signals = [a for a in analyses if a.get("acao_recomendada") in ("COMPRAR YES","COMPRAR NO") and a.get("confianca",0) >= 60]

    return {
        "total": len(analyses),
        "fontes_usadas": len(all_news),
        "mercados_analisados": len(top_matches),
        "resumo": {
            "bullish": bullish,
            "bearish": bearish,
            "neutro": len(analyses) - bullish - bearish,
            "alto_impacto": high_impact,
            "oportunidades_confirmadas": len(buy_signals),
            "sentimento_geral": "BULLISH" if bullish > bearish else "BEARISH" if bearish > bullish else "NEUTRO",
        },
        "melhores_oportunidades": sorted(
            [a for a in analyses if abs(a.get("edge",0)) >= 5],
            key=lambda x: abs(x.get("edge",0)), reverse=True
        )[:3],
        "analyses": analyses,
        "gerado_em": datetime.utcnow().isoformat(),
    }


def _fetch_news(query: str, max_results: int = 8) -> list:
    articles = []
    try:
        params = {"q": query, "language": "en", "sortBy": "publishedAt", "pageSize": max_results, "apiKey": NEWSAPI_KEY}
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
        print(f"[news] erro: {e}")

    try:
        params = {"q": query, "hl": "en", "gl": "US", "ceid": "US:en"}
        resp = requests.get("https://news.google.com/rss/search", params=params, timeout=6)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:5]:
                articles.append({
                    "title": item.findtext("title", ""),
                    "description": item.findtext("description", "")[:200],
                    "source": "Google News", "url": item.findtext("link", ""),
                    "published_at": item.findtext("pubDate", ""),
                })
    except Exception:
        pass
    return articles[:12]


def _analyze_with_claude(question: str, articles: list) -> dict:
    if not articles:
        return {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.2,
                "resumo": "Sem notícias suficientes.", "sentimento": "NEUTRO", "fontes_relevantes": 0}

    news_text = "\n".join([f"[{a['source']}] {a['title']} — {a['description'][:150]}" for a in articles[:8]])
    prompt = f"""Você é analista expert em prediction markets e geopolítica.

PERGUNTA DO MERCADO: {question}

NOTÍCIAS RECENTES:
{news_text}

Analise e responda SOMENTE com JSON válido:
{{"score_yes": <0-100>, "recomendacao": <"APOSTE YES" ou "APOSTE NO" ou "EVITE">, "confianca": <0.0-1.0>, "resumo": <max 100 chars português>, "sentimento": <"POSITIVO" ou "NEGATIVO" ou "NEUTRO">, "fontes_relevantes": <número>}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 300, "messages": [{"role": "user", "content": prompt}]},
            timeout=15
        )
        if resp.status_code == 200:
            text = resp.json().get("content", [{}])[0].get("text", "{}")
            text = text.replace("```json", "").replace("```", "").strip()
            return json.loads(text)
    except Exception as e:
        print(f"[claude] erro: {e}")

    # Fallback por palavras-chave
    all_text = " ".join([a["title"] + " " + a["description"] for a in articles]).lower()
    pos = sum(1 for w in ["confirmed", "approved", "wins", "rises", "signed"] if w in all_text)
    neg = sum(1 for w in ["fails", "rejected", "loses", "denied", "cancelled"] if w in all_text)
    score = max(10, min(90, 50 + (pos - neg) * 8))
    return {
        "score_yes": score,
        "recomendacao": "APOSTE YES" if pos > neg else "APOSTE NO" if neg > pos else "EVITE",
        "confianca": min(len(articles) / 10, 0.7),
        "resumo": f"{len(articles)} notícias. {pos} positivas, {neg} negativas.",
        "sentimento": "POSITIVO" if pos > neg else "NEGATIVO" if neg > pos else "NEUTRO",
        "fontes_relevantes": len(articles),
    }


@app.get("/intelligence/{slug}")
def get_intelligence(slug: str, db: Session = Depends(get_db)):
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado não encontrado"}

    yes_price = no_price = None
    for token in market.tokens:
        o = (token.outcome or "").upper()
        if o == "YES":
            yes_price = round(token.price * 100, 1)
        elif o == "NO":
            no_price = round(token.price * 100, 1)

    keywords = market.question.replace("?", "").replace("Will ", "")[:80]
    articles = _fetch_news(keywords)
    analysis = _analyze_with_claude(market.question, articles)

    score_yes = analysis.get("score_yes", 50)
    edge = round(score_yes - (yes_price or 50), 1)
    rec = analysis.get("recomendacao", "EVITE")
    confianca = analysis.get("confianca", 0.5)

    if rec == "APOSTE YES" and abs(edge) >= 5 and confianca >= 0.5:
        sinal, sinal_cor = "🟢 APOSTE YES", "green"
    elif rec == "APOSTE NO" and abs(edge) >= 5 and confianca >= 0.5:
        sinal, sinal_cor = "🔴 APOSTE NO", "red"
    else:
        sinal, sinal_cor = "🟡 EVITE", "yellow"

    return {
        "market": market.question, "slug": slug,
        "yes_price_mercado": yes_price, "no_price_mercado": no_price,
        "score_yes_ia": score_yes, "edge": edge,
        "sinal": sinal, "sinal_cor": sinal_cor,
        "recomendacao": rec, "confianca": confianca,
        "resumo": analysis.get("resumo"),
        "sentimento": analysis.get("sentimento"),
        "fontes_relevantes": analysis.get("fontes_relevantes", 0),
        "noticias": articles[:5],
        "polymarket_url": f"https://polymarket.com/event/{slug}",
        "atualizado_em": datetime.utcnow().isoformat(),
    }


@app.get("/intelligence")
def get_all_intelligence(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    markets = (
        db.query(Market).join(Token)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(10).all()
    )
    results = []
    for m in markets:
        yes_price = next((round(t.price * 100, 1) for t in m.tokens if (t.outcome or "").upper() == "YES"), None)
        if not yes_price:
            continue
        keywords = m.question.replace("?", "").replace("Will ", "")[:60]
        articles = _fetch_news(keywords, max_results=5)
        analysis = _analyze_with_claude(m.question, articles)
        score_yes = analysis.get("score_yes", 50)
        edge = round(score_yes - yes_price, 1)
        rec = analysis.get("recomendacao", "EVITE")
        confianca = analysis.get("confianca", 0.5)
        sinal_cor = "green" if rec == "APOSTE YES" and abs(edge) >= 5 else "red" if rec == "APOSTE NO" and abs(edge) >= 5 else "yellow"
        results.append({
            "market": m.question, "slug": m.market_slug, "yes_price": yes_price,
            "score_yes_ia": score_yes, "edge": edge, "sinal_cor": sinal_cor,
            "recomendacao": rec, "confianca": confianca,
            "resumo": analysis.get("resumo"), "fontes": analysis.get("fontes_relevantes", 0),
        })

    results.sort(key=lambda x: abs(x["edge"]), reverse=True)
    return {"total": len(results), "mercados": results, "atualizados_em": now.isoformat()}


# ─────────────────────────────────────────────────────────────
# LEADERS
# ─────────────────────────────────────────────────────────────

@app.get("/leaders")
def get_leaders():
    for window in ["all", "1mo", "1w", "1d"]:
        try:
            resp = requests.get(f"{DATA_API}/leaderboard?window={window}&limit=20", headers=HEADERS, timeout=8)
            if resp.status_code == 200:
                items = resp.json()
                items = items if isinstance(items, list) else items.get("data", [])
                if items:
                    leaders = []
                    for i, t in enumerate(items[:20]):
                        addr = t.get("proxyWallet") or t.get("address") or ""
                        won = t.get("positionsWon") or 0
                        lost = t.get("positionsLost") or 0
                        leaders.append({
                            "rank": i + 1,
                            "username": t.get("name") or t.get("pseudonym") or addr[:10] + "...",
                            "address": addr,
                            "profit_usd": t.get("profit") or t.get("pnl") or 0,
                            "volume_usd": t.get("volume") or 0,
                            "win_rate": round(won / max(won + lost, 1) * 100, 1),
                            "ver_apostas": f"https://polymarket.com/profile/{addr}",
                        })
                    return {"status": "ok", "leaders": leaders}
        except Exception:
            continue
    return {"status": "unavailable", "links": {"leaderboard": "https://polymarket.com/leaderboard", "whales": "https://polymarketwhales.info"}}


@app.get("/leaders/live")
def get_live_trades():
    url = f"{CLOB_API}/trades?limit=100"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code != 200:
            return {"status": "unavailable", "status_code": resp.status_code, "body_head": resp.text[:200]}

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
            "top_carteiras": [{"wallet": w[:8] + "..." + w[-4:], "wallet_full": w, "num_apostas": n,
                               "polymarket_url": f"https://polymarket.com/profile/{w}"} for w, n in top_wallets],
        }
    except Exception as e:
        return {"status": "unavailable", "error": str(e)}


@app.get("/leaders/wallet/{address}")
def get_wallet_detail(address: str):
    try:
        resp = requests.get(f"{DATA_API}/positions?user={address}&sizeThreshold=10&limit=50", headers=HEADERS, timeout=8)
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
                        "outcome": p.get("outcome"), "valor_usd": round(valor, 2),
                        "preco_medio": p.get("avgPrice") or p.get("curPrice"), "pnl": p.get("cashPnl") or p.get("pnl"),
                    })
                return {"status": "ok", "wallet": address[:8] + "..." + address[-4:],
                        "total_posicoes": len(positions), "total_exposto_usd": round(total, 2),
                        "posicoes": positions, "polymarket_url": f"https://polymarket.com/profile/{address}"}
    except Exception as e:
        print(f"[wallet] erro: {e}")
    return {"status": "unavailable", "polymarket_url": f"https://polymarket.com/profile/{address}"}


# ─────────────────────────────────────────────────────────────
# MARKET INEFFICIENCY ENGINE
# ─────────────────────────────────────────────────────────────

@app.get("/inefficiencies")
def get_inefficiencies(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    w5m = now - timedelta(minutes=5)
    w1h = now - timedelta(hours=1)
    results = []

    for token in db.query(Token).filter(Token.price > 0.05, Token.price < 0.95).all():
        cp = token.price
        snaps = (
            db.query(Snapshot).filter(Snapshot.token_id == token.token_id)
            .order_by(Snapshot.timestamp.desc()).limit(500).all()
        )
        if len(snaps) < 8:
            continue
        prices = [s.price for s in snaps]

        s5m = next((s for s in snaps if s.timestamp <= w5m), None)
        s1h = next((s for s in snaps if s.timestamp <= w1h), None)
        c5m = round((cp - s5m.price) * 100, 2) if s5m else 0
        c1h = round((cp - s1h.price) * 100, 2) if s1h else None
        if abs(c5m) < 1.5:
            continue

        all_changes = [abs(prices[i] - prices[i+1]) for i in range(min(len(prices)-1, 200))]
        avg_hist = sum(all_changes) / max(len(all_changes), 1)
        vol_ratio = abs(c5m / 100) / max(avg_hist, 0.0001)

        hist_mean = sum(prices[12:62]) / min(50, len(prices[12:])) if len(prices) > 12 else cp
        mispricing_score = min(round(abs(cp - hist_mean) * 300, 1), 100)

        reversals = total_sim = 0
        for i in range(6, min(len(prices) - 6, 200)):
            move = prices[i-6] - prices[i]
            future = prices[i] - prices[i+6]
            if abs(move) > 0.03:
                total_sim += 1
                if (move > 0 and future < -0.01) or (move < 0 and future > 0.01):
                    reversals += 1
        reversal_prob = round(reversals / max(total_sim, 1) * 100, 1)

        ineff_score = min(round((vol_ratio * 20) + (mispricing_score * 0.4) + (reversal_prob * 0.4), 1), 100)
        if ineff_score < 15:
            continue

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        if c5m > 0 and reversal_prob > 55:
            edge, apostar = "REVERSAO_QUEDA", "NO"
        elif c5m < 0 and reversal_prob > 55:
            edge, apostar = "REVERSAO_SUBIDA", "YES"
        elif abs(c5m) > 8:
            edge, apostar = "MOVIMENTO_EXTREMO", "NO" if c5m > 0 else "YES"
        else:
            edge, apostar = "DISTORCAO", "YES" if cp < hist_mean else "NO"

        conviction = "🔴 ALTA" if ineff_score >= 70 else "🟠 MÉDIA" if ineff_score >= 50 else "🟡 MODERADA" if ineff_score >= 35 else "🔵 BAIXA"

        results.append({
            "market": market.question, "slug": market.market_slug, "outcome": token.outcome,
            "current_price_pct": round(cp * 100, 1),
            "change_5m": c5m, "change_1h": c1h,
            "ineficiencia_score": ineff_score, "conviction": conviction,
            "edge_tipo": edge, "apostar": apostar,
            "metricas": {"volatilidade_vs_historico": round(vol_ratio, 2), "mispricing_score": mispricing_score,
                         "reversal_probability_pct": reversal_prob, "padroes_similares": total_sim},
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
            "detectado_em": now.isoformat(),
        })

    results.sort(key=lambda x: x["ineficiencia_score"], reverse=True)
    return {
        "total_ineficiencias": len(results), "top_10": results[:10],
        "metodologia": "Volatility Ratio + Mispricing Score + Reversal Probability",
        "atualizado_em": now.isoformat(),
    }


@app.get("/metrics/{slug}")
def get_market_metrics(slug: str, db: Session = Depends(get_db)):
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado não encontrado"}

    metrics_por_token = []
    for token in market.tokens:
        cp = token.price
        if cp == 0:
            continue
        snaps = (
            db.query(Snapshot).filter(Snapshot.token_id == token.token_id)
            .order_by(Snapshot.timestamp.desc()).limit(2000).all()
        )
        if len(snaps) < 10:
            continue
        prices = [s.price for s in snaps]

        # Volatility
        recent = prices[:12]
        all_ch = [abs(prices[i] - prices[i+1]) for i in range(len(prices)-1)]
        rec_ch = [abs(recent[i] - recent[i+1]) for i in range(len(recent)-1)]
        avg_h = sum(all_ch) / max(len(all_ch), 1)
        avg_r = sum(rec_ch) / max(len(rec_ch), 1)
        vol_ratio = round(avg_r / max(avg_h, 0.0001), 2)
        vol_score = min(round(vol_ratio * 25, 1), 100)
        vol_label = "EXTREMA" if vol_ratio > 3 else "ALTA" if vol_ratio > 2 else "ELEVADA" if vol_ratio > 1.2 else "NORMAL"

        # Mispricing
        if len(prices) >= 50:
            hist_mean = sum(prices[12:62]) / 50
            mispricing_score = min(round(abs(cp - hist_mean) * 300, 1), 100)
            price_dir = "ACIMA" if cp > hist_mean else "ABAIXO"
            hist_mean_pct = round(hist_mean * 100, 1)
        else:
            mispricing_score, price_dir, hist_mean_pct = 0, "NEUTRO", round(cp * 100, 1)

        # Reversal
        reversals = total_sim = 0
        for i in range(6, len(prices) - 6):
            move = prices[i-6] - prices[i]
            future = prices[i] - prices[i+6]
            if abs(move) > 0.03:
                total_sim += 1
                if (move > 0 and future < -0.01) or (move < 0 and future > 0.01):
                    reversals += 1
        rev_prob = round(reversals / max(total_sim, 1) * 100, 1)

        # Confidence
        ev = min(len(snaps)/100, 20) + min(vol_score*0.3, 25) + min(mispricing_score*0.3, 25) + min(total_sim/5, 15) + (15 if rev_prob > 60 else 0)
        confidence_score = min(round(ev, 1), 100)

        edge = edge_dir = None
        if mispricing_score >= 20 and confidence_score >= 40:
            edge = "POSSIVEL_QUEDA" if price_dir == "ACIMA" else "POSSIVEL_SUBIDA"
            edge_dir = "NO" if price_dir == "ACIMA" else "YES"
        if vol_ratio > 2 and rev_prob > 55:
            edge = "REVERSAO_PROVAVEL"
            recent_change = prices[0] - prices[min(12, len(prices)-1)]
            edge_dir = "NO" if recent_change > 0 else "YES"

        metrics_por_token.append({
            "outcome": token.outcome,
            "current_price_pct": round(cp * 100, 1),
            "volatility": {"score": vol_score, "label": vol_label, "ratio_vs_historico": vol_ratio},
            "mispricing": {"score": mispricing_score, "preco_historico_medio_pct": hist_mean_pct, "desvio_direcao": price_dir},
            "reversal": {"probability_pct": rev_prob, "padroes_similares_encontrados": total_sim},
            "confidence_score": confidence_score,
            "edge": edge, "edge_direction": edge_dir,
            "snapshots_analisados": len(snaps),
        })

    if not metrics_por_token:
        return {"error": "Dados insuficientes"}

    return {
        "market": market.question, "slug": slug,
        "has_edge": any(m["edge"] for m in metrics_por_token),
        "max_confidence": max(m["confidence_score"] for m in metrics_por_token),
        "tokens": metrics_por_token,
        "polymarket_url": f"https://polymarket.com/event/{slug}",
        "analisado_em": datetime.utcnow().isoformat(),
    }


@app.get("/backtest")
def backtest(db: Session = Depends(get_db)):
    tokens = db.query(Token).limit(50).all()
    acertos = erros = 0
    amostra = []
    for t in tokens:
        snaps = db.query(Snapshot).filter(Snapshot.token_id == t.token_id).order_by(Snapshot.timestamp.asc()).limit(200).all()
        if len(snaps) < 20:
            continue
        for i in range(5, len(snaps) - 5):
            pe = snaps[i].price * 100
            if pe < 10 or pe > 90:
                continue
            media_ant = sum(s.price * 100 for s in snaps[max(0,i-5):i]) / 5
            variacao = pe - media_ant
            if abs(variacao) < 3:
                continue
            pf = snaps[min(i+5, len(snaps)-1)].price * 100
            acertou = (variacao > 0 and pf > pe) or (variacao < 0 and pf < pe)
            if acertou:
                acertos += 1
            else:
                erros += 1
            if len(amostra) < 20:
                market = db.query(Market).filter(Market.id == t.market_id).first()
                amostra.append({"market": market.question if market else "?", "outcome": t.outcome,
                                "entrada": round(pe,1), "saida": round(pf,1),
                                "resultado": "✅ ACERTO" if acertou else "❌ ERRO"})
    total = acertos + erros
    win_rate = round(acertos / total * 100, 1) if total > 0 else 0
    return {"total_simulados": total, "acertos": acertos, "erros": erros, "win_rate_pct": win_rate,
            "conclusao": "Momentum tem edge" if win_rate > 55 else "Momentum sem edge claro",
            "amostra": amostra, "atualizado_em": datetime.utcnow().isoformat()}


# ─────────────────────────────────────────────────────────────
# SIGNALS SCAN
# ─────────────────────────────────────────────────────────────

def _level_from_move(abs_move: float) -> str:
    if abs_move >= 12: return "EXTREME"
    if abs_move >= 6: return "HIGH"
    if abs_move >= 2: return "MED"
    return "LOW"


def _last_signal(db: Session, slug: str, tipo_prefix: str, cooldown_min: int):
    if cooldown_min <= 0:
        return None
    since = datetime.utcnow() - timedelta(minutes=cooldown_min)
    return (
        db.query(Signal)
        .filter(Signal.slug == slug, Signal.created_at >= since, Signal.tipo.like(f"{tipo_prefix}%"))
        .order_by(Signal.created_at.desc()).first()
    )


def _ref_price(db: Session, token_id: str, now: datetime, target_min: int = 5, max_age_min: int = 30):
    """Busca snapshot de referência com janela flexível (5m → 10m → 15m → 20m)."""
    min_ts = now - timedelta(minutes=max_age_min)
    for mins in [target_min, 10, 15, 20]:
        t = now - timedelta(minutes=mins)
        snap = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token_id, Snapshot.timestamp <= t, Snapshot.timestamp >= min_ts)
            .order_by(Snapshot.timestamp.desc()).first()
        )
        if snap and snap.price is not None:
            return float(snap.price), f"ref ≤ now-{mins}m"
    return None, "not found"


@app.api_route("/signals/scan", methods=["GET", "POST"])
def signals_scan(
    limit_markets: int = 300,
    min_move: float = 0.3,
    arb_over: float = 1.02,
    arb_under: float = 0.98,
    cooldown_minutes: int = 8,
    repeat_boost: float = 1.0,
    max_created: int = 150,
    db: Session = Depends(get_db),
):
    now = datetime.utcnow()
    created = errors = scanned = 0
    preview = []

    markets = (
        db.query(Market).join(Token, Token.market_id == Market.id)
        .group_by(Market.id).order_by(Market.id.desc()).limit(int(limit_markets)).all()
    )

    for market in markets:
        if created >= int(max_created):
            break
        scanned += 1
        try:
            yes_t = no_t = None
            for t in (market.tokens or []):
                o = (t.outcome or "").upper()
                if o == "YES": yes_t = t
                elif o == "NO": no_t = t

            if not yes_t or not no_t:
                continue

            yes = float(yes_t.price or 0)
            no = float(no_t.price or 0)

            if yes <= 0.000001 and no <= 0.000001: continue
            if yes >= 0.999 or yes <= 0.001: continue

            slug = market.market_slug or ""
            if not slug: continue

            total = yes + no

            # ── ARBITRAGE ──
            if yes > 0.001 and no > 0.001:
                if total >= float(arb_over):
                    prev = _last_signal(db, slug, "ARBITRAGE_OVER", cooldown_minutes)
                    err_pts = round((total - 1.0) * 100, 2)
                    if not (prev and abs(err_pts) < abs(float(prev.change_5m or 0)) * float(repeat_boost)):
                        for outcome, price in [("YES", yes), ("NO", no)]:
                            db.add(Signal(market=market.question or "", slug=slug, outcome=outcome,
                                          tipo="ARBITRAGE_OVER", change_5m=err_pts,
                                          current_price=round(price * 100, 2),
                                          confidence=min(1.0, (total - 1.0) / 0.08),
                                          polymarket_url=f"https://polymarket.com/event/{slug}"))
                            created += 1
                        if len(preview) < 10:
                            preview.append({"slug": slug, "tipo": "ARBITRAGE_OVER", "err_pts": err_pts})
                    continue

                if total <= float(arb_under):
                    prev = _last_signal(db, slug, "ARBITRAGE_UNDER", cooldown_minutes)
                    err_pts = round((1.0 - total) * 100, 2)
                    if not (prev and abs(err_pts) < abs(float(prev.change_5m or 0)) * float(repeat_boost)):
                        for outcome, price in [("YES", yes), ("NO", no)]:
                            db.add(Signal(market=market.question or "", slug=slug, outcome=outcome,
                                          tipo="ARBITRAGE_UNDER", change_5m=-abs(err_pts),
                                          current_price=round(price * 100, 2),
                                          confidence=min(1.0, (1.0 - total) / 0.08),
                                          polymarket_url=f"https://polymarket.com/event/{slug}"))
                            created += 1
                        if len(preview) < 10:
                            preview.append({"slug": slug, "tipo": "ARBITRAGE_UNDER", "err_pts": err_pts})
                    continue

            # ── MOVEMENT ──
            old_price, ref_label = _ref_price(db, yes_t.token_id, now, target_min=5, max_age_min=30)
            if old_price is None or old_price <= 0:
                continue

            move = round((yes - old_price) * 100, 2)
            abs_move = abs(move)
            if abs_move < float(min_move):
                continue

            base_tipo = "SPIKE" if move > 0 else "DUMP"
            lvl = _level_from_move(abs_move)
            tipo = f"{base_tipo}_{lvl}"

            prev = _last_signal(db, slug, base_tipo, cooldown_minutes)
            if prev and abs_move < abs(float(prev.change_5m or 0)) * float(repeat_boost):
                continue

            db.add(Signal(market=market.question or "", slug=slug, outcome="YES", tipo=tipo,
                          change_5m=move, current_price=round(yes * 100, 2),
                          confidence=min(1.0, abs_move / 4.0),
                          polymarket_url=f"https://polymarket.com/event/{slug}"))
            created += 1
            if len(preview) < 10:
                preview.append({"slug": slug, "tipo": tipo, "move": move, "ref": ref_label})

        except Exception:
            errors += 1
            continue

    db.commit()
    return {
        "status": "ok", "scanned_markets": scanned, "created_signals": created,
        "errors": errors, "preview": preview, "atualizado_em": now.isoformat(),
    }


@app.get("/signals")
@app.get("/signals/v1")
@app.get("/signals/v1/top")
def signals_v1_top(limit: int = 50, db: Session = Depends(get_db)):
    try:
        limit_n = min(max(int(limit), 1), 200)
        rows = db.query(Signal).order_by(Signal.created_at.desc()).limit(3000).all()
        seen = set()
        uniq = []
        for r in rows:
            if r.slug and r.slug not in seen:
                seen.add(r.slug)
                uniq.append(r)
        uniq = sorted(uniq, key=lambda r: abs(float(r.change_5m or 0)), reverse=True)[:limit_n]
        return {
            "total": len(uniq),
            "signals": [{
                "id": r.id, "created_at": r.created_at.isoformat() if r.created_at else None,
                "market": r.market, "slug": r.slug, "outcome": r.outcome, "tipo": r.tipo,
                "change_5m": float(r.change_5m or 0), "current_price": float(r.current_price or 0),
                "confidence": float(r.confidence or 0), "polymarket_url": r.polymarket_url,
            } for r in uniq],
        }
    except Exception as e:
        return {"total": 0, "signals": [], "error": str(e)}


# ─────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────

def _telegram_send(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID não configurados"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                                     "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/alerts/test")
def alerts_test():
    resp = _telegram_send("✅ <b>PolySignal v3</b> conectado e funcionando!")
    return {"status": "ok", "telegram": resp}


@app.get("/alerts/run")
def alerts_run(minutes: int = 10, limit: int = 10, dry_run: int = 0, db: Session = Depends(get_db)):
    global _LAST_ALERT_SENT_AT
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=max(int(minutes), 1))

    if _LAST_ALERT_SENT_AT and (now - _LAST_ALERT_SENT_AT).total_seconds() < 60:
        return {"status": "skip", "reason": "too_soon"}

    limit_n = min(max(int(limit), 1), 30)

    # Tenta HIGH/EXTREME primeiro
    strong = (
        db.query(Signal)
        .filter(Signal.created_at >= cutoff,
                (Signal.tipo.like("%HIGH%")) | (Signal.tipo.like("%EXTREME%")) | (Signal.tipo.like("ARBITRAGE_%")))
        .order_by(Signal.created_at.desc()).limit(800).all()
    )
    strong = sorted(strong, key=lambda r: abs(float(r.change_5m or 0)), reverse=True)[:limit_n]

    rows, mode = strong, "STRONG"
    if not rows:
        med = (
            db.query(Signal)
            .filter(Signal.created_at >= cutoff, Signal.tipo.like("%MED%"))
            .order_by(Signal.created_at.desc()).limit(800).all()
        )
        rows = sorted(med, key=lambda r: abs(float(r.change_5m or 0)), reverse=True)[:limit_n]
        mode = "MED_FALLBACK"

    if not rows:
        return {"status": "ok", "sent": 0, "message": "sem sinais na janela", "mode": mode}

    spike_n = sum(1 for r in rows if "SPIKE" in (r.tipo or ""))
    dump_n = sum(1 for r in rows if "DUMP" in (r.tipo or ""))
    arb_n = sum(1 for r in rows if (r.tipo or "").startswith("ARBITRAGE_"))
    max_abs = max(abs(float(r.change_5m or 0)) for r in rows)

    title = "🚨 <b>PolySignal</b> — sinais fortes" if mode == "STRONG" else "🟡 <b>PolySignal</b> — sinais moderados"
    lines = [title, f"📊 {spike_n} SPIKE | {dump_n} DUMP | {arb_n} ARB | Max: {max_abs:.2f} pts | Janela: {minutes}min", ""]

    for r in rows:
        change = float(r.change_5m or 0)
        price = float(r.current_price or 0)
        tipo = r.tipo or ""
        url = r.polymarket_url or f"https://polymarket.com/event/{r.slug}"
        emoji = "🟣" if tipo.startswith("ARBITRAGE_") else "🟢" if "SPIKE" in tipo else "🔴" if "DUMP" in tipo else "⚪"
        sign = "+" if change >= 0 else ""
        lines.append(f"{emoji} <b>{tipo}</b> | {sign}{change:.2f} pts @ {price:.1f}%")
        lines.append(f'<a href="{url}">{(r.market or "")[:100]}</a>')
        lines.append("")

    text_out = "\n".join(lines).strip()

    if int(dry_run) == 1:
        return {"status": "dry_run", "would_send": len(rows), "mode": mode, "text": text_out}

    telegram_resp = _telegram_send(text_out)
    ok = bool(telegram_resp.get("ok"))
    if ok:
        _LAST_ALERT_SENT_AT = now

    return {"status": "ok" if ok else "fail", "sent": len(rows) if ok else 0, "mode": mode, "telegram": telegram_resp}


# ─────────────────────────────────────────────────────────────
# DEBUG
# ─────────────────────────────────────────────────────────────

@app.get("/debug/last_snapshot")
def debug_last_snapshot(db: Session = Depends(get_db)):
    last = db.query(func.max(Snapshot.timestamp)).scalar()
    age = round((datetime.utcnow() - last).total_seconds() / 60, 1) if last else None
    return {
        "last_snapshot_timestamp": last.isoformat() if last else None,
        "age_minutes": age,
        "worker_healthy": age is not None and age < 10,
        "now": datetime.utcnow().isoformat(),
    }


@app.get("/debug/tokens")
def debug_tokens(db: Session = Depends(get_db)):
    tokens_sample = db.query(Token).limit(20).all()
    return {
        "total_tokens": db.query(Token).count(),
        "total_snapshots": db.query(Snapshot).count(),
        "total_signals": db.query(Signal).count(),
        "amostra_tokens": [{"id": t.token_id, "outcome": t.outcome, "price_pct": round(t.price * 100, 1)} for t in tokens_sample],
    }


@app.get("/debug/clob_trades")
def debug_clob_trades(limit: int = Query(5, ge=1, le=100)):
    url = f"{CLOB_API}/trades?limit={limit}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        try:
            data = resp.json()
        except Exception:
            data = None
        return {"ok": resp.status_code == 200, "status_code": resp.status_code,
                "url": url, "body_head": resp.text[:400], "json": data}
    except Exception as e:
        return {"ok": False, "url": url, "error": str(e)}


# ─────────────────────────────────────────────────────────────
# CRON TICK
# ─────────────────────────────────────────────────────────────

@app.get("/cron/tick")
def cron_tick(db: Session = Depends(get_db)):
    try:
        try:
            refresh_markets(db)
        except Exception as e:
            print(f"[cron] refresh error: {e}")

        scan_resp = None
        try:
            scan_resp = signals_scan(db=db)
        except Exception as e:
            print(f"[cron] scan error: {e}")

        last = db.query(Snapshot).order_by(desc(Snapshot.timestamp)).first()
        return {
            "status": "ok",
            "tick_now": datetime.utcnow().isoformat(),
            "last_snapshot_timestamp": last.timestamp.isoformat() if last and last.timestamp else None,
            "scan": scan_resp if scan_resp else {"status": "error"},
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ─────────────────────────────────────────────────────────────
# MULTI-SOURCE HELPERS (Reddit + Trends + Whales)
# ─────────────────────────────────────────────────────────────

def _fetch_reddit(query: str) -> list:
    results = []
    subreddits = ["worldnews", "geopolitics", "politics", "economics", "sports"]
    for sub in subreddits[:3]:
        try:
            resp = requests.get(
                f"https://www.reddit.com/r/{sub}/search.json",
                params={"q": query, "sort": "new", "limit": 5, "t": "day"},
                headers={"User-Agent": "PolySignal/3.0"}, timeout=6
            )
            if resp.status_code == 200:
                for c in resp.json().get("data", {}).get("children", []):
                    d = c.get("data", {})
                    results.append({
                        "title": d.get("title", ""), "score": d.get("score", 0),
                        "comments": d.get("num_comments", 0),
                        "url": f"https://reddit.com{d.get('permalink','')}",
                        "fonte": f"Reddit r/{sub}",
                    })
        except Exception:
            pass
        time.sleep(0.2)
    return results[:8]


def _fetch_google_trends(query: str) -> dict:
    try:
        resp = requests.get(
            "https://trends.google.com/trends/api/dailytrends",
            params={"hl": "en-US", "tz": "-180", "geo": "US", "ns": "15"}, timeout=6
        )
        if resp.status_code == 200:
            data = json.loads(resp.text[6:])
            query_lower = query.lower()
            for day in data.get("default", {}).get("trendingSearchesDays", []):
                for item in day.get("trendingSearches", []):
                    title = item.get("title", {}).get("query", "").lower()
                    if any(w in title for w in query_lower.split()[:3]):
                        return {"trending": True, "traffic": item.get("formattedTraffic", ""), "termo": title}
    except Exception:
        pass
    return {"trending": False, "traffic": "0", "termo": ""}


def _fetch_whale_activity(slug: str) -> dict:
    try:
        resp = requests.get(f"{CLOB_API}/trades?limit=50", headers=HEADERS, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            trades = data if isinstance(data, list) else data.get("data", [])
            big_trades = []
            total_volume = 0
            for t in trades:
                valor = float(t.get("size") or t.get("usdcSize") or 0)
                total_volume += valor
                if valor >= 500:
                    big_trades.append({
                        "valor": round(valor, 2),
                        "outcome": t.get("outcome") or t.get("side"),
                        "wallet": (t.get("maker") or "")[:8] + "...",
                    })
            big_trades.sort(key=lambda x: x["valor"], reverse=True)
            return {"total_volume_recente": round(total_volume, 2), "num_baleias": len(big_trades),
                    "maior_aposta": big_trades[0] if big_trades else None, "baleias": big_trades[:3]}
    except Exception:
        pass
    return {"total_volume_recente": 0, "num_baleias": 0, "maior_aposta": None, "baleias": []}


def _multi_source_analysis(question: str, slug: str, articles: list) -> dict:
    keywords = question.replace("?","").replace("Will ","").replace("will ","")[:60]
    reddit_posts = _fetch_reddit(keywords)
    trends = _fetch_google_trends(keywords)
    whales = _fetch_whale_activity(slug)

    score_news   = min(len(articles) * 8, 30)
    score_reddit = min(len(reddit_posts) * 5, 20)
    score_trends = 20 if trends.get("trending") else 0
    score_whales = min(whales.get("num_baleias", 0) * 10, 30)
    score_total  = score_news + score_reddit + score_trends + score_whales

    news_text   = "\n".join([f"- [{a['source']}] {a['title']}" for a in articles[:5]])
    reddit_text = "\n".join([f"- [{p['fonte']}] {p['title']} ({p['score']} upvotes)" for p in reddit_posts[:4]])
    whale_text  = f"Baleias: {whales.get('num_baleias',0)} | Maior: ${whales.get('maior_aposta',{}).get('valor',0) if whales.get('maior_aposta') else 0}"

    prompt = f"""Analise esta oportunidade de prediction market:

MERCADO: {question}

NOTÍCIAS:
{news_text or 'Nenhuma'}

REDDIT:
{reddit_text or 'Nenhum'}

ATIVIDADE DE BALEIAS: {whale_text}
GOOGLE TRENDS: {'EM ALTA: ' + trends.get('termo','') if trends.get('trending') else 'Não trending'}

Responda APENAS com JSON:
{{"score_yes": <0-100>, "recomendacao": <"APOSTE YES" ou "APOSTE NO" ou "EVITE">, "confianca": <0.0-1.0>, "resumo": <max 80 chars português>, "sentimento": <"POSITIVO" ou "NEGATIVO" ou "NEUTRO">}}"""

    ai = {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.3, "resumo": "IA indisponível", "sentimento": "NEUTRO"}
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
            ai = json.loads(text.replace("```json","").replace("```","").strip())
    except Exception as e:
        print(f"[multi_source] IA erro: {e}")

    score_final = round((score_total * 0.5) + (ai.get("confianca", 0) * 100 * 0.5), 1)
    return {
        "score_final": score_final,
        "score_noticias": score_news, "score_reddit": score_reddit,
        "score_trends": score_trends, "score_whales": score_whales,
        "ai": ai, "reddit_posts": reddit_posts[:3], "trending": trends, "whales": whales,
        "num_fontes": sum([1 if articles else 0, 1 if reddit_posts else 0,
                          1 if trends.get("trending") else 0, 1 if whales.get("num_baleias",0) > 0 else 0]),
    }


# ─────────────────────────────────────────────────────────────
# /best e /best/v2 — MELHORES APOSTAS
# ─────────────────────────────────────────────────────────────

@app.get("/best")
def get_best(db: Session = Depends(get_db)):
    """Top oportunidades filtradas por score + confirmação IA."""
    now = datetime.utcnow()
    w5m  = now - timedelta(minutes=5)
    w15m = now - timedelta(minutes=15)
    w1h  = now - timedelta(hours=1)
    candidates = []

    for token in db.query(Token).all():
        cp = token.price
        if cp < 0.20 or cp > 0.80 or cp == 0:
            continue

        def snap(w):
            return (db.query(Snapshot)
                    .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= w)
                    .order_by(Snapshot.timestamp.desc()).first())

        s5m, s15m, s1h = snap(w5m), snap(w15m), snap(w1h)
        if not s5m or not s15m:
            continue

        c5m  = round((cp - s5m.price) * 100, 2)
        c15m = round((cp - s15m.price) * 100, 2)
        c1h  = round((cp - s1h.price) * 100, 2) if s1h else None

        if abs(c5m) < 5.0:
            continue
        # Tendência consistente
        if c5m > 0 and c15m < 0: continue
        if c5m < 0 and c15m > 0: continue

        score = min(abs(c5m) * 2 + abs(c15m) * 1.5 + abs(c1h or 0) + (15 if 0.40 <= cp <= 0.60 else 0), 100)
        if score < 40:
            continue

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        keywords = market.question.replace("?","").replace("Will ","")[:60]
        articles = _fetch_news(keywords, max_results=5)
        analysis = _analyze_with_claude(market.question, articles)

        rec = analysis.get("recomendacao", "EVITE")
        confianca_ia = analysis.get("confianca", 0)

        # IA deve confirmar a direção
        if c5m > 0 and rec == "APOSTE NO": continue
        if c5m < 0 and rec == "APOSTE YES": continue

        score_final = round(score * 0.6 + confianca_ia * 100 * 0.4, 1)
        direcao = "YES" if c5m > 0 else "NO"
        preco = round(cp * 100, 1)
        potencial = round((100 - preco) / preco * 10, 2) if direcao == "YES" else round(preco / (100 - preco) * 10, 2)

        candidates.append({
            "market": market.question, "slug": market.market_slug,
            "outcome": token.outcome, "direcao": direcao,
            "preco_entrada": preco, "potencial_lucro_10usd": potencial,
            "score_mercado": round(score, 0), "score_ia": round(confianca_ia * 100, 0),
            "score_final": score_final,
            "change_5m": c5m, "change_15m": c15m, "change_1h": c1h,
            "recomendacao_ia": rec, "resumo_ia": analysis.get("resumo"),
            "sinal": "🟢 APOSTE" if score_final >= 60 else "🟡 CONSIDERE",
            "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
            "detectado_em": now.isoformat(),
        })

    candidates.sort(key=lambda x: x["score_final"], reverse=True)
    top = candidates[:5]
    return {
        "total_oportunidades": len(candidates), "top_apostas": top,
        "capital_necessario": len(top) * 10,
        "resumo": f"{len(candidates)} oportunidades. Top {len(top)} recomendadas.",
        "atualizado_em": now.isoformat(),
    }


@app.get("/best/v2")
def get_best_v2(db: Session = Depends(get_db)):
    """Multi-fonte: NewsAPI + Reddit + Google Trends + Baleias + IA."""
    now = datetime.utcnow()
    w5m = now - timedelta(minutes=5)
    candidates = []

    for token in db.query(Token).all():
        cp = token.price
        if cp < 0.15 or cp > 0.85 or cp == 0:
            continue

        s5m = (db.query(Snapshot)
               .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= w5m)
               .order_by(Snapshot.timestamp.desc()).first())
        if not s5m:
            continue

        c5m = round((cp - s5m.price) * 100, 2)
        if abs(c5m) < 4.0:
            continue

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        keywords = market.question.replace("?","").replace("Will ","")[:60]
        articles = _fetch_news(keywords, max_results=6)
        analysis = _multi_source_analysis(market.question, market.market_slug, articles)

        score_final = analysis["score_final"]
        ai = analysis["ai"]
        rec = ai.get("recomendacao", "EVITE")

        if score_final < 35 or rec == "EVITE":
            continue

        direcao = "YES" if c5m > 0 else "NO"
        preco = round(cp * 100, 1)
        potencial = round((100 - preco) / preco * 10, 2) if direcao == "YES" else round(preco / (100 - preco) * 10, 2)
        sinal = "🟢 APOSTE" if score_final >= 65 else "🟡 CONSIDERE" if score_final >= 45 else "⚪ FRACO"

        candidates.append({
            "market": market.question, "slug": market.market_slug, "direcao": direcao,
            "preco_entrada": preco, "potencial_lucro_10usd": potencial,
            "sinal": sinal, "score_final": score_final,
            "scores": {"noticias": analysis["score_noticias"], "reddit": analysis["score_reddit"],
                       "trends": analysis["score_trends"], "baleias": analysis["score_whales"]},
            "num_fontes_confirmando": analysis["num_fontes"],
            "change_5m": c5m,
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
        "total_oportunidades": len(candidates), "top_apostas": top,
        "capital_necessario": len(top) * 10,
        "resumo": f"{len(candidates)} oportunidades confirmadas por múltiplas fontes.",
        "atualizado_em": now.isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# VERSÕES EXTRAS — INEFFICIENCIES + BACKTEST
# ─────────────────────────────────────────────────────────────

@app.get("/inefficiencies/v2")
def get_inefficiencies_v2(db: Session = Depends(get_db)):
    """Versão simplificada: desvio do preço histórico médio."""
    resultados = []
    for m in db.query(Market).filter(Market.tokens.any()).all():
        for t in m.tokens:
            snaps = db.query(Snapshot).filter(Snapshot.token_id == t.token_id).order_by(Snapshot.timestamp.desc()).limit(200).all()
            if len(snaps) < 10: continue
            preco = t.price * 100
            if preco < 5 or preco > 95: continue
            precos = [s.price * 100 for s in snaps]
            media = sum(precos) / len(precos)
            desvio = abs(preco - media)
            if desvio < 1.5: continue
            volatilidade = max(precos) - min(precos)
            score = round(min(100, (desvio / max(volatilidade, 1)) * 100), 1)
            resultados.append({"market": m.question, "slug": m.market_slug, "outcome": t.outcome,
                               "preco_atual": round(preco,1), "media_historica": round(media,1),
                               "desvio": round(desvio,1), "score": score, "snapshots": len(snaps),
                               "polymarket_url": f"https://polymarket.com/event/{m.market_slug}"})
    resultados.sort(key=lambda x: x["score"], reverse=True)
    return {"total": len(resultados), "top_10": resultados[:10],
            "metodologia": "Desvio do preço histórico médio", "atualizado_em": datetime.utcnow().isoformat()}


@app.get("/inefficiencies/v3")
def get_inefficiencies_v3(db: Session = Depends(get_db)):
    """v3: tokens ativos com desvio > 2pts."""
    resultados = []
    for t in db.query(Token).limit(200).all():
        preco = t.price * 100
        if preco < 5 or preco > 95: continue
        snaps = db.query(Snapshot).filter(Snapshot.token_id == t.token_id).order_by(Snapshot.timestamp.desc()).limit(50).all()
        if len(snaps) < 10: continue
        precos = [s.price * 100 for s in snaps]
        media = sum(precos) / len(precos)
        desvio = abs(preco - media)
        if desvio < 2: continue
        volatilidade = max(precos) - min(precos)
        score = round(min(100, (desvio / max(volatilidade, 0.1)) * 100), 1)
        mercado = db.query(Market).filter(Market.id == t.market_id).first()
        if not mercado: continue
        resultados.append({"market": mercado.question, "slug": mercado.market_slug, "outcome": t.outcome,
                           "preco_atual": round(preco,1), "media_historica": round(media,1),
                           "desvio": round(desvio,1), "score": score,
                           "polymarket_url": f"https://polymarket.com/event/{mercado.market_slug}"})
    resultados.sort(key=lambda x: x["score"], reverse=True)
    return {"total": len(resultados), "top_10": resultados[:10], "atualizado_em": datetime.utcnow().isoformat()}


@app.get("/inefficiencies/v4")
def get_inefficiencies_v4(db: Session = Depends(get_db)):
    """v4: apenas tokens com preço ativo (5-95%), mais rápido."""
    resultados = []
    for t in db.query(Token).filter(Token.price > 0.05, Token.price < 0.95).all():
        snaps = db.query(Snapshot).filter(Snapshot.token_id == t.token_id).order_by(Snapshot.timestamp.desc()).limit(50).all()
        if len(snaps) < 10: continue
        preco = t.price * 100
        precos = [s.price * 100 for s in snaps]
        media = sum(precos) / len(precos)
        desvio = abs(preco - media)
        if desvio < 2: continue
        volatilidade = max(precos) - min(precos)
        score = round(min(100, (desvio / max(volatilidade, 0.1)) * 100), 1)
        mercado = db.query(Market).filter(Market.id == t.market_id).first()
        if not mercado: continue
        resultados.append({"market": mercado.question, "slug": mercado.market_slug, "outcome": t.outcome,
                           "preco_atual": round(preco,1), "media_historica": round(media,1),
                           "desvio": round(desvio,1), "score": score,
                           "polymarket_url": f"https://polymarket.com/event/{mercado.market_slug}"})
    resultados.sort(key=lambda x: x["score"], reverse=True)
    return {"total": len(resultados), "tokens_ativos_encontrados": len(resultados),
            "top_10": resultados[:10], "atualizado_em": datetime.utcnow().isoformat()}


@app.get("/backtest/v2")
def backtest_v2(db: Session = Depends(get_db)):
    """Backtest com janela 5 snapshots."""
    tokens = db.query(Token).limit(50).all()
    acertos = erros = 0
    amostra = []
    for t in tokens:
        snaps = db.query(Snapshot).filter(Snapshot.token_id == t.token_id).order_by(Snapshot.timestamp.asc()).limit(100).all()
        if len(snaps) < 20: continue
        for i in range(5, len(snaps) - 5):
            pe = snaps[i].price * 100
            if pe < 10 or pe > 90: continue
            media = sum(s.price * 100 for s in snaps[max(0,i-5):i]) / 5
            var = pe - media
            if abs(var) < 3: continue
            pf = snaps[min(i+5, len(snaps)-1)].price * 100
            ok = (var > 0 and pf > pe) or (var < 0 and pf < pe)
            if ok: acertos += 1
            else: erros += 1
            if len(amostra) < 15:
                m = db.query(Market).filter(Market.id == t.market_id).first()
                amostra.append({"market": m.question if m else "?", "outcome": t.outcome,
                                "entrada": round(pe,1), "saida": round(pf,1),
                                "resultado": "✅ ACERTO" if ok else "❌ ERRO"})
    total = acertos + erros
    return {"total_simulados": total, "acertos": acertos, "erros": erros,
            "win_rate_pct": round(acertos/total*100,1) if total > 0 else 0,
            "amostra": amostra, "atualizado_em": datetime.utcnow().isoformat()}


@app.get("/backtest/v3")
def backtest_v3(db: Session = Depends(get_db)):
    """Backtest v3: 30 tokens ativos, janela de 10 snapshots."""
    tokens = db.query(Token).filter(Token.price > 0.05, Token.price < 0.95).limit(30).all()
    acertos = erros = 0
    amostra = []
    for t in tokens:
        snaps = db.query(Snapshot).filter(Snapshot.token_id == t.token_id).order_by(Snapshot.timestamp.asc()).limit(200).all()
        if len(snaps) < 30: continue
        for i in range(10, len(snaps) - 10):
            pe = snaps[i].price * 100
            if pe < 10 or pe > 90: continue
            media = sum(s.price * 100 for s in snaps[max(0,i-10):i]) / 10
            var = pe - media
            if abs(var) < 4: continue
            pf = snaps[min(i+10, len(snaps)-1)].price * 100
            ok = (var > 0 and pf > pe) or (var < 0 and pf < pe)
            if ok: acertos += 1
            else: erros += 1
            if len(amostra) < 20:
                m = db.query(Market).filter(Market.id == t.market_id).first()
                amostra.append({"market": m.question if m else "?", "outcome": t.outcome,
                                "entrada": round(pe,1), "saida": round(pf,1),
                                "resultado": "✅ ACERTO" if ok else "❌ ERRO"})
    total = acertos + erros
    return {"total_simulados": total, "acertos": acertos, "erros": erros,
            "win_rate_pct": round(acertos/total*100,1) if total > 0 else 0,
            "conclusao": "Momentum tem edge" if total > 0 and acertos/total > 0.55 else "Win rate abaixo do esperado",
            "amostra": amostra, "atualizado_em": datetime.utcnow().isoformat()}


@app.get("/intelligence/v3/{slug}")
def get_intelligence_v3(slug: str, db: Session = Depends(get_db)):
    """Intelligence v3: score determinístico sem chamar IA (mais rápido, sem custo)."""
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado não encontrado"}

    yes_price = no_price = None
    for token in market.tokens:
        o = (token.outcome or "").upper()
        if o == "YES": yes_price = round(token.price * 100, 1)
        elif o == "NO": no_price = round(token.price * 100, 1)

    keywords = market.question.replace("?","").replace("Will ","")[:80]

    # Notícias (sem IA)
    articles = _fetch_news(keywords, max_results=8)
    all_text = " ".join([a["title"] + " " + a.get("description","") for a in articles]).lower()

    pos_words = ["confirmed","approved","wins","rises","signed","passes","success","breakthrough","yes","agreed"]
    neg_words = ["fails","rejected","loses","denied","cancelled","drops","collapse","refused","no","veto"]
    pos = sum(1 for w in pos_words if w in all_text)
    neg = sum(1 for w in neg_words if w in all_text)

    # Score baseado em desvio de preço + sentimento de notícias
    news_score = 50 + (pos - neg) * 8
    news_score = max(10, min(90, news_score))

    # Desvio de preço do histórico
    yes_token = next((t for t in market.tokens if (t.outcome or "").upper() == "YES"), None)
    price_deviation_score = 0
    if yes_token:
        snaps = db.query(Snapshot).filter(Snapshot.token_id == yes_token.token_id).order_by(Snapshot.timestamp.desc()).limit(50).all()
        if len(snaps) >= 10:
            hist_mean = sum(s.price * 100 for s in snaps) / len(snaps)
            deviation = (yes_price or 50) - hist_mean
            price_deviation_score = round(deviation, 1)

    score_yes = round((news_score * 0.7) + (50 + price_deviation_score * 0.3), 1)
    score_yes = max(10, min(90, score_yes))
    edge = round(score_yes - (yes_price or 50), 1)

    if abs(edge) >= 8 and len(articles) >= 3:
        rec = "APOSTE YES" if edge > 0 else "APOSTE NO"
        sinal = "🟢 APOSTE YES" if edge > 0 else "🔴 APOSTE NO"
        sinal_cor = "green" if edge > 0 else "red"
        confianca = min(0.8, len(articles) / 10 + abs(edge) / 50)
    else:
        rec = "EVITE"
        sinal = "🟡 EVITE"
        sinal_cor = "yellow"
        confianca = 0.3

    sentimento = "POSITIVO" if pos > neg else "NEGATIVO" if neg > pos else "NEUTRO"

    return {
        "market": market.question, "slug": slug,
        "yes_price_mercado": yes_price, "no_price_mercado": no_price,
        "score_yes_determinisico": score_yes, "edge": edge,
        "sinal": sinal, "sinal_cor": sinal_cor,
        "recomendacao": rec, "confianca": round(confianca, 2),
        "sentimento": sentimento,
        "resumo": f"{len(articles)} notícias. {pos} pos, {neg} neg. Desvio preço: {price_deviation_score:+.1f}pts",
        "fontes_relevantes": len(articles),
        "noticias": articles[:5],
        "polymarket_url": f"https://polymarket.com/event/{slug}",
        "atualizado_em": datetime.utcnow().isoformat(),
        "nota": "v3 = sem IA, determinístico, mais rápido",
    }
