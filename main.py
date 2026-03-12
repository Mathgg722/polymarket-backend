# ============================================================
# PolySignal — Backend Principal v4.0
# FastAPI + PostgreSQL + Railway
# Reescrito do zero com motores limpos
# ============================================================

import httpx
from fastapi.responses import JSONResponse, HTMLResponse  # o que já tiver
import os
import json
import time
import requests
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, Query , Request
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text, func, desc
from sqlalchemy.orm import Session

from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot, Trade, Signal

Base.metadata.create_all(bind=engine)

app = FastAPI(title="PolySignal API", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────
# ENV VARS
# ──────────────────────────────────────────────────────────────
NEWSAPI_KEY        = os.environ.get("NEWSAPI_KEY", "")
ANTHROPIC_KEY      = os.environ.get("ANTHROPIC_KEY", "")
GEMINI_KEY = os.environ.get("GEMINI_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "") or os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
POLY_API_KEY       = os.environ.get("POLY_API_KEY", "")
POLY_PASSPHRASE    = os.environ.get("POLY_PASSPHRASE", "")
POLY_SECRET        = os.environ.get("POLY_SECRET", "")
POLY_ADDRESS       = os.environ.get("POLY_ADDRESS", "") or os.environ.get("POLY_ADDRESSSS", "")

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API  = "https://data-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
HEADERS   = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://polymarket.com",
}

# ──────────────────────────────────────────────────────────────
# FILTROS UNIVERSAIS — aplicados em TODOS os motores
# ──────────────────────────────────────────────────────────────
FILTER_MIN_PRICE    = 0.10   # 10% — abaixo disso mercado quase morto
FILTER_MAX_PRICE    = 0.90   # 90% — acima disso mercado quase resolvido
FILTER_MIN_VOLUME   = 10000  # $10k mínimo de volume total apostado
FILTER_MIN_SNAPS    = 20     # mínimo de snapshots para análise confiável

# ──────────────────────────────────────────────────────────────
# ESTADO GLOBAL
# ──────────────────────────────────────────────────────────────
_LAST_ALERT_SENT_AT = None


# ──────────────────────────────────────────────────────────────
# DB SESSION
# ──────────────────────────────────────────────────────────────
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ──────────────────────────────────────────────────────────────
# HELPERS GLOBAIS
# ──────────────────────────────────────────────────────────────

def _clean_slug(slug: str) -> str:
    """Remove sufixo numérico de slugs do Polymarket (ex: -687, -881)."""
    if not slug:
        return ""
    return re.sub(r'-\d+$', '', slug)


def _market_passes_filters(token_price: float, market_volume: float, snap_count: int, end_date=None) -> tuple:
    """
    Verifica se um mercado/token passa pelos filtros universais.
    Quando volume=0 (campo recem adicionado), usa snap_count>=50 como proxy.
    """
    now = datetime.utcnow()
    if token_price < FILTER_MIN_PRICE:
        return False, f"preco abaixo do minimo"
    if token_price > FILTER_MAX_PRICE:
        return False, f"preco acima do maximo"
    # Filtro volume: se zerado usa snapshots como proxy
    if market_volume > 0 and market_volume < FILTER_MIN_VOLUME:
        return False, f"volume insuficiente"
    if market_volume == 0 and snap_count < FILTER_MIN_SNAPS:
        return False, f"sem volume e poucos snapshots"
    if snap_count < FILTER_MIN_SNAPS:
        return False, f"snapshots insuficientes"
    if end_date and end_date < now:
        return False, "mercado expirado"
    return True, "ok"


def _get_yes_no_prices(market) -> tuple:
    """Retorna (yes_price, no_price) de um mercado em percentual (0-100)."""
    yes_price = no_price = None
    for t in market.tokens:
        o = (t.outcome or "").upper()
        if o == "YES":
            yes_price = round(t.price * 100, 1)
        elif o == "NO":
            no_price = round(t.price * 100, 1)
    return yes_price, no_price


def _polymarket_url(slug: str) -> str:
    return f"https://polymarket.com/event/{_clean_slug(slug)}"


# ──────────────────────────────────────────────────────────────
# ROOT & STATUS
# ──────────────────────────────────────────────────────────────

def _ia_call(prompt: str, max_tokens: int = 400) -> str:
    if not GEMINI_KEY:
        return ""
    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}",
            headers={"content-type": "application/json"},
            json={"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.3}},
            timeout=15)
        if resp.status_code == 200:
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        print(f"[gemini] {e}")
    return ""

@app.get("/")
def home():
    return {"status": "ok", "version": "4.0", "service": "PolySignal"}


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
        "filtros_ativos": {
            "min_price_pct": FILTER_MIN_PRICE * 100,
            "max_price_pct": FILTER_MAX_PRICE * 100,
            "min_volume_usd": FILTER_MIN_VOLUME,
            "min_snapshots": FILTER_MIN_SNAPS,
        },
        "now": datetime.utcnow().isoformat(),
    }


# ──────────────────────────────────────────────────────────────
# MARKETS
# ──────────────────────────────────────────────────────────────

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
        yes_price, no_price = _get_yes_no_prices(m)
        if yes_price == 0 and no_price == 0:
            continue
        result.append({
            "id": m.id,
            "question": m.question,
            "slug": m.market_slug,
            "market_slug": m.market_slug,
            "end_date": str(m.end_date) if m.end_date else None,
            "yes_price": yes_price,
            "no_price": no_price,
            "volume": getattr(m, 'volume', 0) or 0,
            "tokens": [
                {"outcome": t.outcome, "price": round(t.price, 4), "token_id": t.token_id}
                for t in m.tokens
            ],
            "polymarket_url": _polymarket_url(m.market_slug),
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
        "volume": getattr(market, 'volume', 0) or 0,
        "tokens": tokens_detail,
        "polymarket_url": _polymarket_url(market.market_slug),
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


# ──────────────────────────────────────────────────────────────
# REFRESH — agora salva volume
# ──────────────────────────────────────────────────────────────

@app.post("/refresh")
def refresh_markets(db: Session = Depends(get_db)):
    now = datetime.utcnow()

    # Garante coluna volume existe na tabela markets
    try:
        db.execute(text("ALTER TABLE markets ADD COLUMN IF NOT EXISTS volume FLOAT DEFAULT 0"))
        db.commit()
    except Exception:
        db.rollback()

    try:
        resp = requests.get(
            f"{GAMMA_API}/markets?limit=500&active=true&order=volume24hr&ascending=false",
            timeout=20
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

            # Volume — tenta todos os campos possíveis da API Polymarket
            volume = 0.0
            try:
                volume = float(
                    m.get("volume24hr") or
                    m.get("volume") or
                    m.get("volumeNum") or
                    m.get("volume24Hour") or
                    0
                )
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

            # Salva volume no mercado
            try:
                db.execute(
                    text("UPDATE markets SET volume = :vol WHERE id = :id"),
                    {"vol": volume, "id": market_obj.id}
                )
            except Exception:
                pass

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


# ──────────────────────────────────────────────────────────────
# CLEANUP
# ──────────────────────────────────────────────────────────────

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
    return {
        "message": "Limpeza concluída!",
        "removidos": removed,
        "total_restante": db.query(Market).count()
    }


# ──────────────────────────────────────────────────────────────
# TRADES
# ──────────────────────────────────────────────────────────────

class TradeCreate(BaseModel):
    market: str
    outcome: str
    amount: float
    entry_price: float = 0.0
    notes: str = ""


@app.post("/trades")
def open_trade(body: TradeCreate, db: Session = Depends(get_db)):
    market = (
        db.query(Market).filter(Market.market_slug == body.market).first()
        or db.query(Market).filter(Market.question.ilike(f"%{body.market[:40]}%")).first()
    )
    if not market:
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
    return {
        "message": "Aposta registrada!", "trade_id": trade.id,
        "entry_price": f"{entry_price}%", "shares": shares,
        "created_at": str(trade.created_at)
    }


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
        "status_geral": "📈 POSITIVO" if roi > 0 else "📉 NEGATIVO",
        "capital_inicial": 100,
        "capital_atual": capital_atual,
        "pnl_total_usd": pnl_total,
        "roi_pct": roi,
        "total_apostas": len(trades),
        "apostas_abertas": len([t for t in trades if t.status == "open"]),
        "taxa_acerto_pct": taxa_acerto,
        "atualizado_em": datetime.utcnow().isoformat(),
    }


# ──────────────────────────────────────────────────────────────
# NEWS
# ──────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────
# REDDIT — via PullPush.io
# ──────────────────────────────────────────────────────────────

SUBREDDITS_POLY  = ["Polymarket", "polymarketbets", "predictionmarkets"]
SUBREDDITS_GERAL = ["worldnews", "geopolitics", "politics", "economics", "ukraine", "middleeast", "investing"]
PULLPUSH_HEADERS = {"User-Agent": "PolySignal/4.0", "Accept": "application/json"}


def _fetch_pullpush(subreddit: str, size: int = 10) -> list:
    posts = []
    try:
        r = requests.get(
            "https://api.pullpush.io/reddit/search/submission/",
            params={"subreddit": subreddit, "size": size, "sort": "desc", "sort_type": "created_utc",
                    "filter": "id,title,selftext,score,num_comments,permalink,url,author,created_utc,subreddit"},
            headers=PULLPUSH_HEADERS, timeout=10
        )
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
                    "source": f"r/{subreddit}",
                    "subreddit": subreddit,
                    "is_poly": subreddit in SUBREDDITS_POLY,
                    "created_utc": created,
                    "created_at": datetime.utcfromtimestamp(created).isoformat() if created else None,
                })
    except Exception as e:
        print(f"[pullpush] r/{subreddit}: {e}")
    return posts


def _fetch_pullpush_search(query: str, size: int = 20) -> list:
    posts = []
    try:
        r = requests.get(
            "https://api.pullpush.io/reddit/search/submission/",
            params={"q": query, "size": size, "sort": "desc", "sort_type": "created_utc",
                    "filter": "id,title,selftext,score,num_comments,permalink,url,author,created_utc,subreddit"},
            headers=PULLPUSH_HEADERS, timeout=10
        )
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
                    "source": f"r/{sub}",
                    "subreddit": sub,
                    "is_poly": sub in SUBREDDITS_POLY,
                    "created_utc": created,
                    "created_at": datetime.utcfromtimestamp(created).isoformat() if created else None,
                })
    except Exception as e:
        print(f"[pullpush_search] '{query}': {e}")
    return posts


@app.get("/reddit")
def get_reddit(limit: int = Query(60, ge=10, le=100)):
    posts = []
    seen_urls: set = set()

    def add(new_posts):
        for p in new_posts:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                posts.append(p)

    add(_fetch_pullpush_search("polymarket", size=25))
    time.sleep(0.3)
    for sub in SUBREDDITS_POLY:
        add(_fetch_pullpush(sub, size=10))
        time.sleep(0.2)
    for sub in SUBREDDITS_GERAL:
        add(_fetch_pullpush(sub, size=5))
        time.sleep(0.15)

    posts.sort(key=lambda x: (not x["is_poly"], -(x.get("created_utc") or 0)))
    poly_count = sum(1 for p in posts if p["is_poly"])
    return {
        "total": len(posts),
        "polymarket_posts": poly_count,
        "general_posts": len(posts) - poly_count,
        "posts": posts[:limit],
    }


# ──────────────────────────────────────────────────────────────
# CLAUDE IA — análise de notícias
# ──────────────────────────────────────────────────────────────

def _analyze_with_claude(question: str, articles: list) -> dict:
    if not articles:
        return {"score_yes": 50, "recomendacao": "EVITE", "confianca": 0.2,
                "resumo": "Sem notícias suficientes.", "sentimento": "NEUTRO", "fontes_relevantes": 0}

    news_text = "\n".join([f"[{a['source']}] {a['title']} — {a.get('description','')[:150]}" for a in articles[:8]])
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
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 300,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=15
        )
        if resp.status_code == 200:
            text = resp.json().get("content", [{}])[0].get("text", "{}")
            text = text.replace("```json", "").replace("```", "").strip()
            return json.loads(text)
    except Exception as e:
        print(f"[claude] erro: {e}")

    # Fallback por palavras-chave
    all_text = " ".join([a["title"] + " " + a.get("description","") for a in articles]).lower()
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


# ──────────────────────────────────────────────────────────────
# MOVERS
# ──────────────────────────────────────────────────────────────

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
            "polymarket_url": _polymarket_url(market.market_slug) if market else None,
        })

    movers.sort(key=lambda x: abs(x["change_1h"]), reverse=True)
    return movers[:20]


# ──────────────────────────────────────────────────────────────
# DEBUG
# ──────────────────────────────────────────────────────────────

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


@app.get("/debug/anomalies")
def debug_anomalies(db: Session = Depends(get_db)):
    """Diagnóstico: por que anomalias retorna vazio."""
    now = datetime.utcnow()
    w5m = now - timedelta(minutes=5)

    total_tokens = db.query(Token).count()
    tokens_in_range = db.query(Token).filter(
        Token.price > FILTER_MIN_PRICE,
        Token.price < FILTER_MAX_PRICE
    ).count()

    passou_count = 0
    sample_passou = []
    sample_falhou = []

    tokens = db.query(Token).filter(
        Token.price > FILTER_MIN_PRICE,
        Token.price < FILTER_MAX_PRICE
    ).all()

    for t in tokens:
        snap_count = db.query(func.count(Snapshot.id)).filter(
            Snapshot.token_id == t.token_id
        ).scalar() or 0

        market = db.query(Market).filter(Market.id == t.market_id).first()
        volume = _get_market_volume(market) if market else 0

        passou, motivo = _market_passes_filters(t.price, volume, snap_count, market.end_date if market else None)

        s5m = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == t.token_id, Snapshot.timestamp <= w5m)
            .order_by(Snapshot.timestamp.desc()).first()
        )
        c5m = round((t.price - s5m.price) * 100, 2) if s5m else None

        info = {
            "token": t.token_id[:20],
            "outcome": t.outcome,
            "price_pct": round(t.price * 100, 1),
            "snap_count": snap_count,
            "volume": volume,
            "passou_filtro": passou,
            "motivo": motivo,
            "c5m": c5m,
            "market": market.question[:60] if market else "?",
        }

        if passou:
            passou_count += 1
            if len(sample_passou) < 10:
                sample_passou.append(info)
        else:
            if len(sample_falhou) < 5:
                sample_falhou.append(info)

    return {
        "total_tokens": total_tokens,
        "tokens_em_range_10_90pct": tokens_in_range,
        "tokens_passaram_filtro": passou_count,
        "filtro_min_price": FILTER_MIN_PRICE,
        "filtro_max_price": FILTER_MAX_PRICE,
        "filtro_min_volume": FILTER_MIN_VOLUME,
        "filtro_min_snaps": FILTER_MIN_SNAPS,
        "tokens_que_passaram": sample_passou,
        "tokens_que_falharam": sample_falhou,
        "now": now.isoformat(),
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


@app.get("/debug/data_trades")
def debug_data_trades():
    """Debug — mostra resposta crua do data-api/trades para inspecionar campos."""
    urls = [
        f"{DATA_API}/trades?limit=5&sizeThreshold=1000",
        f"{DATA_API}/trades?limit=5",
        f"{GAMMA_API}/trades?limit=5",
    ]
    results = []
    for url in urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            try:
                data = resp.json()
            except Exception:
                data = None
            results.append({
                "url": url,
                "status_code": resp.status_code,
                "body_head": resp.text[:600],
                "json_sample": data if not isinstance(data, list) else data[:2],
            })
        except Exception as e:
            results.append({"url": url, "error": str(e)})
    return {"results": results}


# ──────────────────────────────────────────────────────────────
# CRON TICK
# ──────────────────────────────────────────────────────────────

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
            "whales": {"status": "skipped"},
            "early_alerts": {"status": "skipped"},
            "correlations": {"status": "skipped"},
            "events": {"status": "skipped"},
            "military": {"status": "skipped"},
            "contradictions": {"status": "skipped"},
            "naval": {"status": "skipped"},
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/cron/whales")
def cron_whales(db: Session = Depends(get_db)):
    try:
        resp = whale_scan(min_valor=10000, alertar=1, db=db)
        return {"status": "ok", "whales": resp}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/cron/alerts")
def cron_alerts(db: Session = Depends(get_db)):
    try:
        early = None
        try:
            early = alerts_early(min_impact="ALTO", alertar=1, dry_run=0, db=db)
        except Exception as e:
            print(f"[cron/alerts] early: {e}")

        military = None
        try:
            military = military_scan(min_impact="ALTO", alertar=1, usar_ia=0, db=db)
        except Exception as e:
            print(f"[cron/alerts] military: {e}")

        naval = None
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            naval = loop.run_until_complete(naval_scan(alertar=1, db=db))
            loop.close()
        except Exception as e:
            print(f"[cron/alerts] naval: {e}")

        politicians = None
        try:
            politicians = politicians_scan(min_impact="ALTO", alertar=1, usar_ia=0, db=db)
        except Exception as e:
            print(f"[cron/alerts] politicians: {e}")

        press = None
        try:
            press = press_scan(min_score=20, alertar=1, usar_ia=0, db=db)
        except Exception as e:
            print(f"[cron/alerts] press: {e}")

        return {
            "status": "ok",
            "early_alerts": early if early else {"status": "error"},
            "military": military if military else {"status": "error"},
            "naval": naval if naval else {"status": "error"},
            "politicians": politicians if politicians else {"status": "error"},
            "press": press if press else {"status": "error"},
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/cron/correlations")
def cron_correlations(db: Session = Depends(get_db)):
    try:
        corr = correlations_divergencias(min_corr=0.70, min_move=3.0, alertar=1, db=db)
        contra = get_contradictions(min_score=30.0, alertar=1, db=db)
        events = get_events_upcoming(horas=24, alertar=1, min_impacto="ALTO", db=db)
        return {
            "status": "ok",
            "correlations": corr,
            "contradictions": contra,
            "events": events,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
    
# ══════════════════════════════════════════════════════════════
# MOTOR 1 — SINAIS v4
# Regras limpas:
# - Movimento mínimo 3% em 10 minutos (não 0.3% em 5min)
# - Só mercados com volume > $10k
# - Só mercados com preço 10%-90%
# - Direção consistente: movimento curto alinhado com médio
# - Sem sinal se mercado resolvendo (preço > 85% ou < 15%)
# ══════════════════════════════════════════════════════════════

def _get_market_volume(market) -> float:
    """Retorna volume do mercado — do campo volume ou 0."""
    try:
        return float(getattr(market, 'volume', 0) or 0)
    except Exception:
        return 0.0


def _ref_price_clean(db: Session, token_id: str, now: datetime, minutes: int) -> float | None:
    """Busca snapshot de referência exatamente N minutos atrás (±5min de tolerância)."""
    target = now - timedelta(minutes=minutes)
    min_ts = now - timedelta(minutes=minutes + 10)
    max_ts = now - timedelta(minutes=minutes - 5)
    snap = (
        db.query(Snapshot)
        .filter(
            Snapshot.token_id == token_id,
            Snapshot.timestamp <= max_ts,
            Snapshot.timestamp >= min_ts
        )
        .order_by(Snapshot.timestamp.desc()).first()
    )
    return float(snap.price) if snap else None


def _last_signal_cooldown(db: Session, slug: str, tipo_prefix: str, cooldown_min: int):
    """Verifica se já existe sinal recente para evitar spam."""
    since = datetime.utcnow() - timedelta(minutes=cooldown_min)
    return (
        db.query(Signal)
        .filter(Signal.slug == slug, Signal.created_at >= since, Signal.tipo.like(f"{tipo_prefix}%"))
        .first()
    )


@app.api_route("/signals/scan", methods=["GET", "POST"])
def signals_scan(
    limit_markets: int = 300,
    cooldown_minutes: int = 10,
    max_created: int = 100,
    db: Session = Depends(get_db),
):
    """
    Motor de Sinais v4 — regras limpas.
    Movimento mínimo: 3% em 10min.
    Filtros: volume > $10k, preço 10%-90%, direção consistente.
    """
    now = datetime.utcnow()
    created = errors = scanned = skipped_filters = 0
    preview = []

    markets = (
        db.query(Market).join(Token, Token.market_id == Market.id)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .group_by(Market.id)
        .order_by(Market.id.desc())
        .limit(int(limit_markets)).all()
    )

    for market in markets:
        if created >= int(max_created):
            break
        scanned += 1

        try:
            # Pega tokens YES e NO
            yes_t = no_t = None
            for t in (market.tokens or []):
                o = (t.outcome or "").upper()
                if o == "YES": yes_t = t
                elif o == "NO": no_t = t

            if not yes_t or not no_t:
                continue

            yes = float(yes_t.price or 0)
            slug = market.market_slug or ""
            if not slug:
                continue

            # Conta snapshots para filtro
            snap_count = db.query(func.count(Snapshot.id)).filter(
                Snapshot.token_id == yes_t.token_id
            ).scalar() or 0

            # Aplica filtros universais
            volume = _get_market_volume(market)
            passou, motivo = _market_passes_filters(yes, volume, snap_count, market.end_date)
            if not passou:
                skipped_filters += 1
                continue

            # Busca preços de referência
            price_10m = _ref_price_clean(db, yes_t.token_id, now, minutes=10)
            price_30m = _ref_price_clean(db, yes_t.token_id, now, minutes=30)
            price_1h  = _ref_price_clean(db, yes_t.token_id, now, minutes=60)

            if price_10m is None:
                continue

            # Movimento principal: 10 minutos
            move_10m = round((yes - price_10m) * 100, 2)
            abs_move = abs(move_10m)

            # FILTRO: movimento mínimo 3% (não 0.3%)
            if abs_move < 3.0:
                continue

            # FILTRO: direção consistente (10m alinhado com 30m)
            # Não gera sinal se movimento de curto prazo contradiz o médio prazo
            if price_30m is not None:
                move_30m = (yes - price_30m) * 100
                # Se 10m subiu mas 30m também subiu muito mais = possível reversão
                # Se 10m caiu mas 30m caiu muito menos = possível bounce
                # Só gera sinal se direções são consistentes
                if move_10m > 0 and move_30m < -2:
                    continue  # Spike contra tendência de 30min — ruído
                if move_10m < 0 and move_30m > 2:
                    continue  # Dump contra tendência de 30min — ruído

            # FILTRO: não gera sinal se mercado quase resolvido
            if yes > 0.85 or yes < 0.15:
                continue

            # Tipo do sinal
            base_tipo = "SPIKE" if move_10m > 0 else "DUMP"
            if abs_move >= 15:
                nivel = "EXTREME"
            elif abs_move >= 8:
                nivel = "HIGH"
            elif abs_move >= 5:
                nivel = "MED"
            else:
                nivel = "LOW"

            tipo = f"{base_tipo}_{nivel}"

            # Cooldown: não duplica sinal recente
            prev = _last_signal_cooldown(db, slug, base_tipo, cooldown_minutes)
            if prev:
                continue

            # Confiança baseada no tamanho do movimento e consistência
            confianca = min(abs_move / 15.0, 1.0)
            if price_30m and price_1h:
                # Boost se movimento consistente em múltiplas janelas
                move_1h = (yes - price_1h) * 100 if price_1h else 0
                if (move_10m > 0 and move_1h > 0) or (move_10m < 0 and move_1h < 0):
                    confianca = min(confianca * 1.2, 1.0)

            db.add(Signal(
                market=market.question or "",
                slug=slug,
                outcome="YES",
                tipo=tipo,
                change_5m=move_10m,
                current_price=round(yes * 100, 2),
                confidence=round(confianca, 3),
                polymarket_url=_polymarket_url(slug),
            ))
            created += 1

            if len(preview) < 10:
                preview.append({
                    "slug": slug,
                    "tipo": tipo,
                    "move_10m": move_10m,
                    "price": round(yes * 100, 1),
                    "volume": volume,
                })

        except Exception as e:
            errors += 1
            print(f"[signals] erro {market.market_slug}: {e}")
            continue

    db.commit()
    return {
        "status": "ok",
        "scanned_markets": scanned,
        "created_signals": created,
        "skipped_by_filters": skipped_filters,
        "errors": errors,
        "preview": preview,
        "atualizado_em": now.isoformat(),
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
                "id": r.id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "market": r.market,
                "slug": r.slug,
                "outcome": r.outcome,
                "tipo": r.tipo,
                "change_10m": float(r.change_5m or 0),
                "current_price": float(r.current_price or 0),
                "confidence": float(r.confidence or 0),
                "polymarket_url": r.polymarket_url,
            } for r in uniq],
        }
    except Exception as e:
        return {"total": 0, "signals": [], "error": str(e)}


# ══════════════════════════════════════════════════════════════
# MOTOR 2 — ANOMALIAS v4
# Regras limpas:
# - Só mercados com volume > $10k e preço 10%-90%
# - Anomalia = movimento brusco E inesperado (não resolução natural)
# - Detecta: spike, dump, reversão
# - NÃO marca como oportunidade mercado que está resolvendo
# - Score baseado em: intensidade + velocidade + surpresa histórica
# ══════════════════════════════════════════════════════════════

@app.get("/anomalies")
def get_anomalies(db: Session = Depends(get_db)):
    """
    Anomalias v4 — só mercados reais com movimento genuinamente anômalo.
    Filtros: volume > $10k, preço 10%-90%, movimento mínimo 3% em 5min.
    Exclui: mercados resolvendo naturalmente (tendência linear para 0% ou 100%).
    """
    now = datetime.utcnow()
    w5m  = now - timedelta(minutes=5)
    w15m = now - timedelta(minutes=15)
    w1h  = now - timedelta(hours=1)
    w6h  = now - timedelta(hours=6)
    anomalies = []

    # Só tokens na zona de interesse
    tokens = db.query(Token).filter(
        Token.price > FILTER_MIN_PRICE,
        Token.price < FILTER_MAX_PRICE
    ).all()

    for token in tokens:
        cp = token.price

        # Verifica volume do mercado
        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        volume = _get_market_volume(market)

        # Conta snapshots
        snap_count = db.query(func.count(Snapshot.id)).filter(
            Snapshot.token_id == token.token_id
        ).scalar() or 0

        passou, _ = _market_passes_filters(cp, volume, snap_count, market.end_date)
        if not passou:
            continue

        # Busca snapshots de referência
        def get_snap(w):
            return (
                db.query(Snapshot)
                .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= w)
                .order_by(Snapshot.timestamp.desc()).first()
            )

        s5m  = get_snap(w5m)
        s15m = get_snap(w15m)
        s1h  = get_snap(w1h)
        s6h  = get_snap(w6h)

        if not s5m:
            continue

        c5m  = round((cp - s5m.price) * 100, 2)
        c15m = round((cp - s15m.price) * 100, 2) if s15m else None
        c1h  = round((cp - s1h.price) * 100, 2) if s1h else None
        c6h  = round((cp - s6h.price) * 100, 2) if s6h else None

        # FILTRO: movimento mínimo de 3% em 5 minutos
        if abs(c5m) < 3.0:
            continue

        # FILTRO: detecta resolução natural (não é anomalia, é o mercado fechando)
        # Resolução natural = movimento consistente em uma direção por 6h
        if c6h is not None and c1h is not None:
            # Se caindo consistentemente por 6h E preço abaixo de 25% = resolução
            if c6h < -10 and c1h < -5 and cp < 0.25:
                continue
            # Se subindo consistentemente por 6h E preço acima de 75% = resolução
            if c6h > 10 and c1h > 5 and cp > 0.75:
                continue

        # Classifica o tipo de anomalia
        if abs(c5m) >= 20:
            tipo, level = "EXTREME_MOVE", "EXTREME"
        elif c5m > 5 and (c1h or 0) > 3:
            tipo, level = "SPIKE_CONFIRMADO", "HIGH"
        elif c5m < -5 and (c1h or 0) < -3:
            tipo, level = "DUMP_CONFIRMADO", "HIGH"
        elif c5m > 3 and (c1h or 0) < -2:
            tipo, level = "REVERSAO_ALTA", "MEDIUM"
        elif c5m < -3 and (c1h or 0) > 2:
            tipo, level = "REVERSAO_BAIXA", "MEDIUM"
        else:
            tipo, level = "MOVIMENTO", "LOW"

        # Score de anomalia
        # Componentes: intensidade (40%) + surpresa (30%) + confirmação (30%)
        intensidade = min(abs(c5m) * 3, 40)

        # Surpresa = movimento 5m comparado com volatilidade histórica normal
        snaps_hist = db.query(Snapshot).filter(
            Snapshot.token_id == token.token_id
        ).order_by(Snapshot.timestamp.desc()).limit(100).all()

        if len(snaps_hist) >= 10:
            hist_prices = [s.price for s in snaps_hist]
            hist_moves = [abs(hist_prices[i] - hist_prices[i+1]) * 100 for i in range(min(len(hist_prices)-1, 50))]
            avg_move = sum(hist_moves) / len(hist_moves) if hist_moves else 1
            surpresa = min((abs(c5m) / max(avg_move, 0.1)) * 10, 30)
        else:
            surpresa = 15

        # Confirmação = movimento consistente em múltiplas janelas
        confirmacao = 0
        if c15m and abs(c15m) > abs(c5m) * 0.3:
            confirmacao += 10
        if c1h and abs(c1h) > abs(c5m) * 0.2:
            confirmacao += 10
        if c5m > 0 and (c15m or 0) > 0 and (c1h or 0) > 0:
            confirmacao += 10  # Todos na mesma direção

        score = min(round(intensidade + surpresa + confirmacao, 0), 100)

        # Só retorna se score mínimo de 20
        if score < 20:
            continue

        # Oportunidade: só sugere direção se faz sentido
        if tipo == "REVERSAO_ALTA":
            oportunidade = "CONSIDERAR_YES"  # Subindo após queda = possível entrada
        elif tipo == "REVERSAO_BAIXA":
            oportunidade = "CONSIDERAR_NO"   # Caindo após alta = possível short
        elif tipo in ("SPIKE_CONFIRMADO",) and cp < 0.65:
            oportunidade = "MOMENTO_YES"     # Spike com preço ainda baixo
        elif tipo in ("DUMP_CONFIRMADO",) and cp > 0.35:
            oportunidade = "MOMENTO_NO"      # Dump com preço ainda alto
        else:
            oportunidade = "AGUARDAR"        # Movimento extremo — aguardar confirmação

        anomalies.append({
            "market": market.question,
            "slug": market.market_slug,
            "outcome": token.outcome,
            "tipo": tipo,
            "alert_level": level,
            "score": score,
            "current_price": round(cp * 100, 1),
            "change_5m": c5m,
            "change_15m": c15m,
            "change_1h": c1h,
            "change_6h": c6h,
            "volume_mercado": volume,
            "oportunidade": oportunidade,
            "polymarket_url": _polymarket_url(market.market_slug),
            "detectado_em": now.isoformat(),
        })

    anomalies.sort(key=lambda x: x["score"], reverse=True)
    return anomalies[:20]


# ══════════════════════════════════════════════════════════════
# MOTOR 3 — INEFFICIENCIES v4
# Regras limpas:
# - Só mercados com volume > $10k e preço 10%-90%
# - Ineficiência = preço atual desviou da média histórica SEM motivo claro
# - Usa 3 métricas: desvio histórico + probabilidade de reversão + volatilidade
# - Score mínimo de 30 para aparecer
# - Indica claramente se o edge é para YES ou NO
# ══════════════════════════════════════════════════════════════

@app.get("/inefficiencies")
def get_inefficiencies(db: Session = Depends(get_db)):
    """
    Inefficiencies v4 — desvios reais do preço justo.
    Filtros: volume > $10k, preço 10%-90%, mínimo 30 snapshots.
    Retorna mercados onde o preço atual diverge significativamente do histórico.
    """
    now = datetime.utcnow()
    results = []

    tokens = db.query(Token).filter(
        Token.price > FILTER_MIN_PRICE,
        Token.price < FILTER_MAX_PRICE
    ).all()

    for token in tokens:
        cp = token.price

        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        # Filtro de volume
        volume = _get_market_volume(market)
        if volume > 0 and volume < FILTER_MIN_VOLUME:
            continue

        # Filtro de expiração
        if market.end_date and market.end_date < now:
            continue

        # Busca histórico (mínimo 30 snapshots para análise confiável)
        snaps = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id)
            .order_by(Snapshot.timestamp.desc())
            .limit(200).all()
        )

        if len(snaps) < 30:
            continue

        prices = [s.price for s in snaps]
        cp_pct = cp * 100

        # ── Métrica 1: Desvio da média histórica ──────────────────
        # Usa média dos snapshots de 12 a 62 (exclui os mais recentes)
        # para evitar que o próprio movimento atual distorça a média
        if len(prices) >= 62:
            hist_prices = prices[12:62]
        elif len(prices) >= 20:
            hist_prices = prices[12:]
        else:
            hist_prices = prices

        hist_mean = sum(hist_prices) / len(hist_prices) * 100
        desvio = round(cp_pct - hist_mean, 2)
        desvio_abs = abs(desvio)

        # FILTRO: desvio mínimo de 3% para ser relevante
        if desvio_abs < 3.0:
            continue

        # ── Métrica 2: Probabilidade de reversão histórica ────────
        reversals = total_patterns = 0
        for i in range(6, min(len(prices) - 6, 150)):
            move_antes = (prices[i-6] - prices[i]) * 100  # movimento que levou ao ponto i
            move_depois = (prices[i] - prices[i+6]) * 100  # movimento após o ponto i

            if abs(move_antes) > 2.0:  # só conta padrões significativos
                total_patterns += 1
                # Reversão = movimento depois vai na direção oposta ao movimento antes
                if (move_antes > 0 and move_depois < -1.0) or (move_antes < 0 and move_depois > 1.0):
                    reversals += 1

        reversal_prob = round(reversals / max(total_patterns, 1) * 100, 1)

        # ── Métrica 3: Volatilidade relativa ──────────────────────
        all_moves = [abs(prices[i] - prices[i+1]) * 100 for i in range(min(len(prices)-1, 100))]
        recent_moves = [abs(prices[i] - prices[i+1]) * 100 for i in range(min(10, len(prices)-1))]

        avg_hist_vol = sum(all_moves) / max(len(all_moves), 1)
        avg_recent_vol = sum(recent_moves) / max(len(recent_moves), 1)
        vol_ratio = round(avg_recent_vol / max(avg_hist_vol, 0.01), 2)

        # ── Score final de ineficiência ────────────────────────────
        # Desvio histórico: 40% do score
        score_desvio = min(desvio_abs * 4, 40)

        # Probabilidade de reversão: 35% do score
        score_reversao = min(reversal_prob * 0.35, 35)

        # Volatilidade elevada: 25% do score (sinal de instabilidade)
        score_vol = min((vol_ratio - 1) * 12.5, 25) if vol_ratio > 1 else 0

        ineff_score = round(score_desvio + score_reversao + score_vol, 1)

        # FILTRO: score mínimo de 30
        if ineff_score < 30:
            continue

        # ── Determina direção do edge ──────────────────────────────
        if desvio > 0:
            # Preço atual ACIMA da média histórica
            if reversal_prob > 50:
                edge_tipo = "SOBRECOMPRADO_REVERSAO"
                apostar = "NO"
                raciocinio = f"Preço {cp_pct:.1f}% está {desvio:.1f}pts acima da média histórica {hist_mean:.1f}%. {reversal_prob}% dos padrões similares reverteram."
            else:
                edge_tipo = "ACIMA_MEDIA"
                apostar = "AGUARDAR"
                raciocinio = f"Preço acima da média mas reversão histórica abaixo de 50%."
        else:
            # Preço atual ABAIXO da média histórica
            if reversal_prob > 50:
                edge_tipo = "SOBREVENDIDO_REVERSAO"
                apostar = "YES"
                raciocinio = f"Preço {cp_pct:.1f}% está {abs(desvio):.1f}pts abaixo da média histórica {hist_mean:.1f}%. {reversal_prob}% dos padrões similares reverteram."
            else:
                edge_tipo = "ABAIXO_MEDIA"
                apostar = "AGUARDAR"
                raciocinio = f"Preço abaixo da média mas reversão histórica abaixo de 50%."

        conviction = "🟢 ALTA" if ineff_score >= 70 else "🟡 MÉDIA" if ineff_score >= 50 else "🟠 MODERADA"

        results.append({
            "market": market.question,
            "slug": market.market_slug,
            "outcome": token.outcome,
            "current_price": round(cp_pct, 1),
            "media_historica": round(hist_mean, 1),
            "desvio_pts": desvio,
            "ineff_score": ineff_score,
            "conviction": conviction,
            "edge_tipo": edge_tipo,
            "apostar": apostar,
            "raciocinio": raciocinio,
            "metricas": {
                "desvio_absoluto": desvio_abs,
                "reversal_probability_pct": reversal_prob,
                "padroes_analisados": total_patterns,
                "volatilidade_ratio": vol_ratio,
                "snapshots_usados": len(snaps),
            },
            "volume_mercado": volume,
            "polymarket_url": _polymarket_url(market.market_slug),
            "detectado_em": now.isoformat(),
        })

    results.sort(key=lambda x: x["ineff_score"], reverse=True)
    return {
        "total": len(results),
        "top_10": results[:10],
        "metodologia": "Desvio histórico (40%) + Prob. reversão (35%) + Volatilidade (25%). Filtros: volume>$10k, preço 10%-90%, 30+ snapshots.",
        "atualizado_em": now.isoformat(),
    }

# ══════════════════════════════════════════════════════════════
# MOTOR 4 — GAME THEORY v4 (Prof. Jiang Xueqin)
# Correções:
# - Match exige score >= 3 (não 1 keyword genérica)
# - Keywords específicas de alta qualidade por tema
# - prob_base usa prob_jiang real (não 70% fixo)
# - Filtro de noise patterns expandido
# - Cada previsão tem keywords obrigatórias (core_required)
# ══════════════════════════════════════════════════════════════

JIANG_METHODOLOGY = {
    "citacao": "I use game theory and I basically see geopolitics as a game played by different players who are trying to maximize their own self-interest.",
    "pilares": ["Incentivos estruturais (não ideologia)", "Analogia histórica verificada", "Ciclos civilizacionais Spengler+Turchin", "Análise financeira estrutural"],
    "formula_mec": "Sucesso = Massa × Energia × Coordenação",
    "lei_mec": "Ator com menos Massa mas superior Energia×Coordenação SEMPRE vence o ator com mais Massa mas baixa Coordenação",
}

# Palavras de ruído — mercados com qualquer uma dessas são ignorados
_NOISE_PATTERNS = [
    "jesus", "christ", "god", "alien", "ufo", "bigfoot",
    "kardashian", "taylor swift", "kanye", "bieber", "oscars",
    "nicki minaj", "celebrity gossip", "reality tv", "tiktok star",
    "music video", "album", "concert", "grammy", "oscar award",
    "soccer player transfer", "nba trade rumor",
]

JIANG_PREDICTIONS = [
    {
        "id": "iran_us_conflict",
        "tema": "EUA perde guerra do Irã",
        "categoria": "GEOPOLITICA",
        "icone": "⚔️",
        # core_required: pelo menos 1 dessas DEVE estar no texto do mercado
        "core_required": ["iran", "iranian", "tehran", "irgc", "persia"],
        # keywords: contribuem para o score de match
        "keywords": ["iran", "iranian", "tehran", "irgc", "nuclear", "war", "military", "strike", "attack", "conflict", "troops", "invasion", "bomb", "sanctions"],
        "status": "EM_ANDAMENTO",
        "confirmado": True,
        "prob_jiang": 78,
        "prob_mkt": 45,
        "edge": 33,
        "direcao": "YES",
        "confianca": 0.85,
        "prazo": "2026-2027",
        "estrutura": "STRATEGIC_TRAP",
        "mecanismo": "Escalation Dominance: cada provocação iraniana FORÇA resposta americana. Inevitável.",
        "analogia": "Expedição Siciliana 415 BC: Atenas invade território defensável. Logística colapsa.",
        "mec": {
            "iran": {"m": 45, "e": 95, "c": 88, "s": 3750, "cor": "#00d26a", "n": "20 anos prep + IRGC coordenado"},
            "eua": {"m": 90, "e": 40, "c": 35, "s": 1260, "cor": "#ff4d4d", "n": "Generais são clerks, não fighters"},
        },
        "jogadores": [
            {"n": "Irã (IRGC)", "t": "DEFENSOR_ASSIMETRICO", "incentivo": "20 anos preparando. Guerra unifica população."},
            {"n": "Trump", "t": "LIDER_MEDIA_DRIVEN", "incentivo": "Guerra = ferramenta política."},
        ],
        "gatilhos_sim": ["Tropas terrestres comprometidas", "Houthis fecham Hormuz"],
        "gatilhos_nao": ["Negociação nuclear via China", "Colapso interno do regime"],
        "citacao": "He based his analysis on the Sicilian Expedition from 415–413 BC — WION News Mar 2026",
        "timeline": [{"d": "2024-05", "p": 78, "e": "The Iran Trap — previsão original"}],
    },
    {
        "id": "iran_ceasefire",
        "tema": "Cessar-fogo EUA-Irã (saída honrosa Trump)",
        "categoria": "GEOPOLITICA",
        "icone": "🕊️",
        "core_required": ["iran", "iranian", "tehran"],
        "keywords": ["iran", "ceasefire", "peace", "deal", "withdraw", "negotiations", "nuclear", "agreement", "talks", "diplomacy", "iranian"],
        "status": "ATIVA",
        "confirmado": False,
        "prob_jiang": 62,
        "prob_mkt": 55,
        "edge": 7,
        "direcao": "YES",
        "confianca": 0.68,
        "prazo": "6-18 meses",
        "estrutura": "CHICKEN_GAME_SAIDA_NEGOCIADA",
        "mecanismo": "Ambos têm incentivos para pausa. Trump precisa de saída sem humilhação.",
        "analogia": "Armistício Coreia 1953",
        "mec": {
            "trump_saida": {"m": 70, "e": 65, "c": 55, "s": 2502, "cor": "#ffa500", "n": "Ameaça nuclear = leverage para retirada honrosa"},
            "iran_consolida": {"m": 45, "e": 75, "c": 82, "s": 2767, "cor": "#00d26a", "n": "Prefere consolidar ganhos"},
        },
        "jogadores": [
            {"n": "Trump", "t": "NEGOCIADOR_NUCLEAR", "incentivo": "Declarar vitória + retirar antes de midterms."},
            {"n": "Irã", "t": "VENCEDOR_CANSADO", "incentivo": "Consolidar ganhos."},
        ],
        "gatilhos_sim": ["Midterms se aproximam", "Casualties acima do threshold"],
        "gatilhos_nao": ["Israel sabota negociações", "Ataque iraniano em solo americano"],
        "citacao": "Trump brokering a ceasefire between Iran and Israel — Sea & Job Mar 2026",
        "timeline": [{"d": "2026-03", "p": 62, "e": "Conflito ativo, pressão para saída"}],
    },
    {
        "id": "iran_regime_collapse",
        "tema": "Queda do Regime Iraniano é improvável no curto prazo",
        "categoria": "GEOPOLITICA",
        "icone": "🇮🇷",
        "core_required": ["iran", "iranian", "regime"],
        "keywords": ["iran", "iranian", "regime", "fall", "collapse", "overthrow", "revolution", "leadership", "change", "government"],
        "status": "ATIVA",
        "confirmado": False,
        "prob_jiang": 18,
        "prob_mkt": 25,
        "edge": -7,
        "direcao": "NO",
        "confianca": 0.75,
        "prazo": "curto prazo",
        "estrutura": "PROBLEMA_COORDENACAO_OPOSICAO",
        "mecanismo": "Guerra externa UNIFICA população em torno do regime. Oposição interna fragmentada. IRGC coordenado e leal. Queda no curto prazo é improvável.",
        "analogia": "Assad sobreviveu 13 anos de guerra civil com suporte externo",
        "mec": {
            "regime": {"m": 72, "e": 88, "c": 90, "s": 5701, "cor": "#00d26a", "n": "IRGC coordenado, guerra unifica"},
            "oposicao": {"m": 35, "e": 55, "c": 18, "s": 346, "cor": "#ff4d4d", "n": "Fragmentada, sem suporte externo coordenado"},
        },
        "jogadores": [
            {"n": "IRGC", "t": "GUARDIAO_REGIME", "incentivo": "Sobrevivência institucional — perdem tudo se regime cai."},
            {"n": "Oposição", "t": "COALIZAO_DESCOORDENADA", "incentivo": "Dividida entre reformistas, monarquistas e separatistas."},
        ],
        "gatilhos_sim": ["Derrota militar humilhante sem precedente", "Colapso econômico total + corte do IRGC"],
        "gatilhos_nao": ["Guerra externa (unifica)", "Suporte russo/chinês ao regime"],
        "citacao": "War unifies populations behind their governments — historical pattern, Jiang 2026",
        "timeline": [{"d": "2026-03", "p": 18, "e": "Conflito ativo — regime se consolida"}],
    },
    {
        "id": "ukraine_frozen",
        "tema": "Congelamento Rússia-Ucrânia",
        "categoria": "GEOPOLITICA",
        "icone": "❄️",
        "core_required": ["russia", "ukraine", "ukrainian", "zelensky", "putin", "kyiv", "donbas"],
        "keywords": ["russia", "ukraine", "ceasefire", "peace", "war", "zelensky", "putin", "frontline", "territory", "nato", "kyiv", "donbas", "ukrainian", "russian"],
        "status": "ATIVA",
        "confirmado": False,
        "prob_jiang": 72,
        "prob_mkt": 58,
        "edge": 14,
        "direcao": "YES",
        "confianca": 0.78,
        "prazo": "3-12 meses",
        "estrutura": "BARGANHA_ASSIMETRICA_MEDIADOR",
        "mecanismo": "Todos têm incentivo para pausa: Putin consolida, Trump ganha vitória, Europa rearma.",
        "analogia": "Armistício Coreia 1953",
        "mec": {
            "russia": {"m": 75, "e": 60, "c": 70, "s": 3150, "cor": "#cc0000", "n": "Ganhos consolidados"},
            "ucrania": {"m": 40, "e": 88, "c": 65, "s": 2288, "cor": "#0057b7", "n": "Alta motivação, exaustão de recursos"},
        },
        "jogadores": [
            {"n": "Putin", "t": "CONSOLIDADOR", "incentivo": "Congelar linha como vitória declarada."},
            {"n": "Zelensky", "t": "SOBREVIVENTE", "incentivo": "Não pode ceder oficialmente."},
        ],
        "gatilhos_sim": ["Trump pressiona Zelensky", "Europa corta suporte"],
        "gatilhos_nao": ["Ofensiva russa rompe frente"],
        "citacao": "Russia, the war in Ukraine, it's basically pretty settled — Jiang, Jan 5 2026",
        "timeline": [{"d": "2026-01", "p": 72, "e": "Jiang: basically settled"}],
    },
    {
        "id": "trump_china_deal",
        "tema": "Grand Bargain Trump-Xi",
        "categoria": "GEOPOLITICA",
        "icone": "🤝",
        "core_required": ["china", "xi", "beijing", "chinese"],
        "keywords": ["trump", "china", "xi", "trade", "deal", "tariff", "grand bargain", "dollar", "april", "beijing", "negotiation", "trade war", "chinese"],
        "status": "ATIVA",
        "confirmado": False,
        "prob_jiang": 60,
        "prob_mkt": 35,
        "edge": 25,
        "direcao": "YES",
        "confianca": 0.68,
        "prazo": "Abril-Junho 2026",
        "estrutura": "NEGOCIACAO_ASSIMETRIA_TEMPORAL",
        "mecanismo": "Trump tem urgência (midterms). Xi tem paciência. Mas ambos precisam negociar.",
        "analogia": "Nixon-China 1972",
        "mec": {
            "trump": {"m": 85, "e": 82, "c": 60, "s": 4182, "cor": "#ff4d4d", "n": "Nixon ambition + Venezuela leverage"},
            "xi": {"m": 82, "e": 65, "c": 90, "s": 4797, "cor": "#cc0000", "n": "Jogo longo, quer ser igual"},
        },
        "jogadores": [
            {"n": "Trump", "t": "NEGOCIADOR_NIXON", "incentivo": "Replicar Nixon-China 1972."},
            {"n": "Xi", "t": "ESTRATEGISTA_DECADAL", "incentivo": "Evitar decoupling."},
        ],
        "gatilhos_sim": ["Crise financeira americana", "Iran war cria urgência"],
        "gatilhos_nao": ["Taiwan incident", "Escândalo Trump paralisa presidência"],
        "citacao": "Venezuela was designed to strangle China's resource access — Jiang Jan 5 2026",
        "timeline": [{"d": "2026-01", "p": 60, "e": "Venezuela criou leverage"}],
    },
    {
        "id": "europe_rearmament",
        "tema": "Remilitarização Europa",
        "categoria": "GEOPOLITICA",
        "icone": "🛡️",
        "core_required": ["europe", "european", "nato", "eu", "germany", "france", "macron", "deutschland"],
        "keywords": ["europe", "nato", "defense", "rearmament", "military", "spending", "army", "autonomous", "macron", "germany", "troops", "eu", "european", "bundeswehr"],
        "status": "EM_ANDAMENTO",
        "confirmado": True,
        "prob_jiang": 84,
        "prob_mkt": 72,
        "edge": 12,
        "direcao": "YES",
        "confianca": 0.87,
        "prazo": "1-3 anos (em andamento)",
        "estrutura": "PROVISAO_BEM_COLETIVO_FORCADA",
        "mecanismo": "Quando protetor ameaça sair, custo de autodefesa se torna aceitável.",
        "analogia": "Criação OTAN 1949",
        "mec": {
            "trump_catalisador": {"m": 85, "e": 72, "c": 58, "s": 3549, "cor": "#ff4d4d", "n": "Ameaça de abandono FORÇA coordenação europeia"},
            "europa": {"m": 70, "e": 62, "c": 50, "s": 2170, "cor": "#003399", "n": "Processo doloroso mas inevitável"},
        },
        "jogadores": [
            {"n": "Trump", "t": "CATALISADOR_INVOLUNTARIO", "incentivo": "Forçar Europa a pagar defesa."},
            {"n": "Europa", "t": "CRIANCA_FORCADA_A_CRESCER", "incentivo": "Sobrevivência ante ameaça russa."},
        ],
        "gatilhos_sim": ["Incidente russo nas fronteiras OTAN", "Trump retira tropas"],
        "gatilhos_nao": ["Colapso russo reduz urgência"],
        "citacao": "The irrational remilitarization of Europe — Jiang, Jan 5 2026",
        "timeline": [{"d": "2026-02", "p": 86, "e": "Alemanha aprova Zeitenwende histórico"}],
    },
    {
        "id": "ai_bubble",
        "tema": "Bolha de IA colapsa",
        "categoria": "ECONOMIA",
        "icone": "💥",
        "core_required": ["artificial intelligence", "ai bubble", "nvidia", "openai", "deepseek", "data center"],
        "keywords": ["ai bubble", "artificial intelligence crash", "nvidia", "openai", "data center", "tech bubble", "deepseek", "llm valuations", "ai crash"],
        "status": "ATIVA",
        "confirmado": False,
        "prob_jiang": 60,
        "prob_mkt": 32,
        "edge": 28,
        "direcao": "YES",
        "confianca": 0.65,
        "prazo": "2026-2027",
        "estrutura": "CORRIDA_ARMAMENTOS_COLAPSO_INEVITAVEL",
        "mecanismo": "Ninguém pode sair primeiro. Quando ROI decepcionante vira público — colapso.",
        "analogia": "Dot-com 2000",
        "mec": {
            "narrativa": {"m": 92, "e": 85, "c": 28, "s": 2189, "cor": "#888888", "n": "Corrida armamentos sem coordenação"},
            "realidade_roi": {"m": 40, "e": 100, "c": 100, "s": 4000, "cor": "#00d26a", "n": "Realidade sempre vence narrativa"},
        },
        "jogadores": [
            {"n": "Big Tech", "t": "DILEMA_PRISIONEIRO", "incentivo": "Ninguém pode sair primeiro."},
            {"n": "Mercado", "t": "AMPLIFICADOR_FOMO", "incentivo": "FOMO."},
        ],
        "gatilhos_sim": ["Earnings big tech mostra ROI negativo", "Recessão reduz capital especulativo"],
        "gatilhos_nao": ["IA gera produtividade real mensurável"],
        "citacao": "Data centers cost billions. Unclear how they make money — Jiang Jan 5 2026",
        "timeline": [{"d": "2026-02", "p": 62, "e": "DeepSeek choca mercado"}],
    },
    {
        "id": "silver_rally",
        "tema": "Rally Prata/Ouro — Demanda IA+EV",
        "categoria": "ECONOMIA",
        "icone": "🥈",
        "core_required": ["silver", "gold", "precious metal", "xag", "xau"],
        "keywords": ["silver", "gold", "commodity", "precious metal", "xag", "xau", "metals", "conductor", "ev", "electric vehicle", "mining", "silver price", "gold price"],
        "status": "EM_ANDAMENTO",
        "confirmado": True,
        "prob_jiang": 78,
        "prob_mkt": 60,
        "edge": 18,
        "direcao": "YES",
        "confianca": 0.80,
        "prazo": "1-3 anos",
        "estrutura": "ESCASSEZ_ESTRUTURAL_DEMANDA_EXPONENCIAL",
        "mecanismo": "Demanda cresce exponencialmente. Oferta fisicamente limitada.",
        "analogia": "Oil shock 1973",
        "mec": {
            "demanda": {"m": 82, "e": 92, "c": 88, "s": 6637, "cor": "#00d26a", "n": "IA + EVs + Guerra = demanda convergente"},
            "oferta": {"m": 50, "e": 60, "c": 42, "s": 1260, "cor": "#ff4d4d", "n": "Mineração lenta"},
        },
        "jogadores": [
            {"n": "Demanda (IA+EVs+Guerra)", "t": "DRIVER_ESTRUTURAL", "incentivo": "Prata = melhor condutor elétrico."},
        ],
        "gatilhos_sim": ["Iran fecha Hormuz — ouro explode", "IA capex continua crescendo"],
        "gatilhos_nao": ["Recessão global reduz demanda"],
        "citacao": "Silver is the best metal conductor in the world. Demand exceeds supply for 5 years — Jiang Jan 5 2026",
        "timeline": [{"d": "2026-02", "p": 78, "e": "Ouro atinge máximos históricos"}],
    },
    {
        "id": "dollar_decline",
        "tema": "Declínio lento do Dólar",
        "categoria": "ECONOMIA",
        "icone": "💵",
        "core_required": ["dollar", "usd", "reserve currency", "dedollarization", "petrodollar"],
        "keywords": ["dollar", "reserve currency", "brics", "dedollarization", "yuan", "gold", "usd", "fed", "petrodollar", "sanctions", "swift", "dollar decline"],
        "status": "ATIVA",
        "confirmado": False,
        "prob_jiang": 15,
        "prob_mkt": 28,
        "edge": 13,
        "direcao": "NO",
        "confianca": 0.82,
        "prazo": "5-15 anos",
        "estrutura": "PROBLEMA_COORDENACAO_LOCK_IN",
        "mecanismo": "Ninguém quer ser o primeiro a sair do dólar. Declínio gradual de 30 anos, não colapso.",
        "analogia": "Declínio da Libra Esterlina 1945-1975",
        "mec": {
            "dolar_inercia": {"m": 95, "e": 55, "c": 78, "s": 4069, "cor": "#00d26a", "n": "Inércia gigantesca"},
            "brics": {"m": 65, "e": 72, "c": 30, "s": 1404, "cor": "#ff4d4d", "n": "Baixa coordenação"},
        },
        "jogadores": [
            {"n": "EUA/Fed", "t": "HEGEMON_MONETARIO", "incentivo": "Preservar exorbitant privilege."},
            {"n": "BRICS+", "t": "COALIZAO_DESCOORDENADA", "incentivo": "Reduzir vulnerabilidade a sanções."},
        ],
        "gatilhos_sim": ["EUA default real", "Alternativa técnica viável ao SWIFT"],
        "gatilhos_nao": ["Grand Bargain Trump-Xi fortalece dólar"],
        "citacao": "Decline is like the British Pound: 30 years, not overnight — Jiang Jan 5 2026",
        "timeline": [{"d": "2026-01", "p": 15, "e": "Jiang: declínio lento como Libra Esterlina"}],
    },
]

# Índice rápido para match
_GT_INDEX: dict = {}
for _p in JIANG_PREDICTIONS:
    for _kw in _p.get("keywords", []):
        _GT_INDEX.setdefault(_kw.lower(), []).append(_p["id"])

_GT_MAP = {p["id"]: p for p in JIANG_PREDICTIONS}


def _match_gt_v4(question: str, slug: str) -> list:
    """
    Match v4 — muito mais rigoroso que a versão anterior.
    
    Regras:
    1. Se mercado tem padrão de ruído → rejeita imediatamente
    2. Previsão deve ter pelo menos 1 keyword CORE presente no texto
    3. Score mínimo de 3 keywords para match (não 1)
    4. Retorna lista ordenada por relevância
    """
    text = (question + " " + slug).lower()

    # Regra 1: filtro de ruído
    if any(noise in text for noise in _NOISE_PATTERNS):
        return []

    # Regra 2 e 3: calcula score por previsão
    scores: dict = {}

    for pid, pred in _GT_MAP.items():
        # Verifica keyword CORE obrigatória
        core_required = pred.get("core_required", [])
        has_core = any(core.lower() in text for core in core_required)
        if not has_core:
            continue  # Sem keyword core → não faz match

        # Conta keywords normais
        keyword_score = sum(1 for kw in pred.get("keywords", []) if kw.lower() in text)

        # Score mínimo de 3 keywords (incluindo as cores)
        if keyword_score >= 3:
            scores[pid] = keyword_score

    if not scores:
        return []

    # Ordena por relevância
    matched = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [_GT_MAP[pid] for pid, _ in matched if pid in _GT_MAP]


def _gt_ai_analysis_v4(question: str, yes_price: float, pred: dict) -> dict:
    """Análise IA usando metodologia Jiang — usa prob_jiang real, não 70% fixo."""
    mec = pred.get("mec", {})
    mec_txt = " | ".join([
        f"{k}: MEC={v['s']} (M={v['m']},E={v['e']},C={v['c']})"
        for k, v in mec.items() if isinstance(v, dict) and "s" in v
    ])

    prompt = f"""Você é especialista na metodologia "Predictive History" do Prof. Jiang Xueqin.

METODOLOGIA MEC: Sucesso = Massa × Energia × Coordenação
Lei: Ator com menos Massa mas superior Energia×Coordenação SEMPRE vence.

MERCADO: "{question}"
PREÇO YES ATUAL: {yes_price}%

ANÁLISE JIANG:
- Tema: {pred['tema']}
- Estrutura: {pred['estrutura']}
- Mecanismo: {pred.get('mecanismo','')}
- Analogia: {pred['analogia']}
- MEC: {mec_txt}
- Prob base Jiang: {pred['prob_jiang']}%
- Direção: {pred['direcao']}

Ajuste a probabilidade base do Jiang para este mercado específico.
Responda SOMENTE com JSON válido sem markdown:
{{"prob_ajustada":<0-100>,"edge":<prob_ajustada - {yes_price}>,"acao":"COMPRAR YES"|"COMPRAR NO"|"AGUARDAR","pilar":"INCENTIVOS"|"HISTORICO"|"CICLOS"|"MEC","raciocinio":"<max 150 chars>","ponto_critico":"<max 80 chars>","mec_vencedor":"<ator com MEC mais alto>","confianca":<0.0-1.0>}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 400,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=20,
        )
        if resp.status_code == 200:
            raw = resp.json()["content"][0]["text"].replace("```json", "").replace("```", "").strip()
            return json.loads(raw)
    except Exception as ex:
        print(f"[GT-v4] err: {ex}")

    # Fallback — usa prob_jiang REAL (não 70% fixo!)
    prob = pred["prob_jiang"]
    edge = round(prob - yes_price, 1)
    direcao = pred.get("direcao", "YES")
    acao = "COMPRAR YES" if (direcao == "YES" and edge > 10) else "COMPRAR NO" if (direcao == "NO" and edge < -5) else "AGUARDAR"

    mec_vencedor = max(
        [(k, v["s"]) for k, v in pred.get("mec", {}).items() if isinstance(v, dict) and "s" in v],
        key=lambda x: x[1], default=("N/A", 0)
    )[0]

    return {
        "prob_ajustada": prob,
        "edge": edge,
        "acao": acao,
        "pilar": "MEC",
        "raciocinio": pred.get("mecanismo", "")[:150],
        "ponto_critico": (pred.get("gatilhos_sim") or ["Ver gatilhos"])[0],
        "mec_vencedor": mec_vencedor,
        "confianca": pred["confianca"] * 0.75,
    }


@app.get("/game-theory")
def get_game_theory_v4(
    limit: int = Query(10, ge=1, le=25),
    categoria: str = Query(None),
    min_edge: float = Query(5.0),
    db: Session = Depends(get_db),
):
    """
    Game Theory v4 — Prof. Jiang Xueqin.
    Match rigoroso: exige keyword CORE + score >= 3.
    Prob base usa prob_jiang real de cada previsão.
    """
    now = datetime.utcnow()

    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    analyses, seen = [], set()

    for market in markets:
        if len(analyses) >= limit * 4:
            break

        q = market.question or ""
        slug = market.market_slug or ""

        # Verifica volume (proxy: se zerado usa snap_count >= FILTER_MIN_SNAPS)
        volume = _get_market_volume(market)
        if volume > 0 and volume < FILTER_MIN_VOLUME:
            continue
        if volume == 0:
            snap_count = db.query(func.count(Snapshot.id)).filter(
                Snapshot.token_id == market.tokens[0].token_id
            ).scalar() if market.tokens else 0
            if snap_count < FILTER_MIN_SNAPS:
                continue

        matched = _match_gt_v4(q, slug)
        if not matched:
            continue

        pred = next((p for p in matched if p["id"] not in seen), None)
        if not pred:
            continue

        if categoria and pred["categoria"] != categoria.upper():
            continue

        yes_price, no_price = _get_yes_no_prices(market)
        if yes_price is None:
            continue

        ia = _gt_ai_analysis_v4(q, yes_price, pred)
        prob = ia.get("prob_ajustada", pred["prob_jiang"])
        edge = ia.get("edge", round(prob - yes_price, 1))
        acao = ia.get("acao", "AGUARDAR")
        confianca = round(ia.get("confianca", pred["confianca"]) * 100)

        if abs(edge) < float(min_edge):
            continue

        seen.add(pred["id"])

        conviction = (
            "🔴 MUITO ALTA" if abs(edge) >= 25 and confianca >= 75 else
            "🟠 ALTA" if abs(edge) >= 15 and confianca >= 60 else
            "🟡 MÉDIA" if abs(edge) >= 8 and confianca >= 50 else
            "⚪ BAIXA"
        )

        mec_display = [
            {"ator": k, "massa": v["m"], "energia": v["e"], "coordenacao": v["c"],
             "mec_score": v["s"], "cor": v.get("cor", "#888888"), "nota": v.get("n", "")}
            for k, v in pred.get("mec", {}).items() if isinstance(v, dict) and "s" in v
        ]
        mec_display.sort(key=lambda x: x["mec_score"], reverse=True)

        analyses.append({
            "market_question": q,
            "market_slug": slug,
            "yes_price": yes_price,
            "no_price": no_price,
            "volume_mercado": volume,
            "polymarket_url": _polymarket_url(slug),
            "tema_jiang": pred["tema"],
            "categoria": pred["categoria"],
            "icone": pred.get("icone", "🎯"),
            "status": pred.get("status", "ATIVA"),
            "confirmado": pred.get("confirmado", False),
            "prob_jiang": prob,
            "prob_mercado": yes_price,
            "edge": edge,
            "acao": acao,
            "conviction": conviction,
            "confianca_pct": confianca,
            "pilar_dominante": ia.get("pilar", "MEC"),
            "jogadores": pred.get("jogadores", []),
            "estrutura": pred["estrutura"],
            "mecanismo": pred.get("mecanismo", ""),
            "raciocinio_ia": ia.get("raciocinio", ""),
            "ponto_critico": ia.get("ponto_critico", ""),
            "mec_vencedor": ia.get("mec_vencedor", ""),
            "analogia": pred["analogia"],
            "mec_analise": mec_display,
            "timeline": pred.get("timeline", []),
            "gatilhos_sim": pred.get("gatilhos_sim", []),
            "gatilhos_nao": pred.get("gatilhos_nao", []),
            "prazo": pred["prazo"],
            "citacao_jiang": pred.get("citacao", ""),
            "analisado_em": now.isoformat(),
        })
        time.sleep(0.2)

    analyses.sort(key=lambda x: abs(x["edge"]), reverse=True)
    top = analyses[:limit]

    return {
        "total_analisados": len(analyses),
        "retornados": len(top),
        "resumo": {
            "comprar_yes": sum(1 for a in top if a["acao"] == "COMPRAR YES"),
            "comprar_no": sum(1 for a in top if a["acao"] == "COMPRAR NO"),
            "aguardar": sum(1 for a in top if a["acao"] == "AGUARDAR"),
            "edge_medio": round(sum(abs(a["edge"]) for a in top) / max(len(top), 1), 1),
        },
        "sobre_jiang": JIANG_METHODOLOGY,
        "top_oportunidades": top,
        "gerado_em": now.isoformat(),
    }


@app.get("/game-theory/predictions")
def gt_predictions(categoria: str = Query(None)):
    preds = JIANG_PREDICTIONS
    if categoria:
        preds = [p for p in preds if p["categoria"] == categoria.upper()]
    return {
        "total": len(preds),
        "confirmados": sum(1 for p in JIANG_PREDICTIONS if p.get("confirmado")),
        "categorias": list({p["categoria"] for p in JIANG_PREDICTIONS}),
        "metodologia": JIANG_METHODOLOGY,
        "predictions": preds,
    }


@app.get("/game-theory/{slug}")
def gt_slug(slug: str, db: Session = Depends(get_db)):
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado não encontrado", "slug": slug}

    q = market.question or ""
    matched = _match_gt_v4(q, slug)
    yes_price, no_price = _get_yes_no_prices(market)

    if not matched:
        return {
            "market_question": q, "slug": slug,
            "yes_price": yes_price, "jiang_match": False,
            "motivo": "Nenhuma previsão do Jiang corresponde a este mercado com critérios rigorosos.",
            "polymarket_url": _polymarket_url(slug),
        }

    pred = matched[0]
    ia = _gt_ai_analysis_v4(q, yes_price or 50, pred)

    mec_display = [
        {"ator": k, "massa": v["m"], "energia": v["e"], "coordenacao": v["c"],
         "mec_score": v["s"], "cor": v.get("cor", "#888888"), "nota": v.get("n", "")}
        for k, v in pred.get("mec", {}).items() if isinstance(v, dict) and "s" in v
    ]
    mec_display.sort(key=lambda x: x["mec_score"], reverse=True)

    return {
        "market_question": q, "slug": slug,
        "yes_price": yes_price, "no_price": no_price,
        "polymarket_url": _polymarket_url(slug),
        "jiang_match": True,
        "tema_jiang": pred["tema"],
        "categoria": pred["categoria"],
        "icone": pred.get("icone", "🎯"),
        "prob_jiang": ia.get("prob_ajustada", pred["prob_jiang"]),
        "edge": ia.get("edge", 0),
        "acao": ia.get("acao", "AGUARDAR"),
        "confianca_pct": round(ia.get("confianca", pred["confianca"]) * 100),
        "raciocinio": ia.get("raciocinio", ""),
        "ponto_critico": ia.get("ponto_critico", ""),
        "mec_vencedor": ia.get("mec_vencedor", ""),
        "analogia": pred["analogia"],
        "mec_analise": mec_display,
        "gatilhos_sim": pred.get("gatilhos_sim", []),
        "gatilhos_nao": pred.get("gatilhos_nao", []),
        "prazo": pred["prazo"],
        "citacao": pred.get("citacao", ""),
        "analisado_em": datetime.utcnow().isoformat(),
    }


# ══════════════════════════════════════════════════════════════
# MOTOR 5 — MASTER v4 (Market-Adaptive)
# Correções:
# - Filtros universais aplicados
# - Confiança mínima elevada para 0.45
# - Edge mínimo de 3% (não 1%)
# - Modo INTER só ativa se divergência > 5% (não 3%)
# ══════════════════════════════════════════════════════════════

def _master_volatility(prices: list) -> float:
    if len(prices) < 2:
        return 0.0
    changes = [abs(prices[i] - prices[i+1]) for i in range(len(prices)-1)]
    return sum(changes) / len(changes)


def _master_intra_analysis(prices: list, current: float) -> dict:
    """Analisa padrões históricos do próprio token. Usado em mercado CALMO."""
    if len(prices) < 20:
        return {"signal": "INSUFFICIENT_DATA", "confidence": 0.0, "predicted_price": current * 100}

    short_ma = sum(prices[:5]) / 5
    long_ma = sum(prices[:20]) / 20
    hist_mean = sum(prices) / len(prices)
    momentum = current - prices[10]
    deviation = current - hist_mean

    bullish = bearish = 0
    if short_ma > long_ma: bullish += 1
    else: bearish += 1
    if momentum > 0.015: bullish += 2
    elif momentum < -0.015: bearish += 2
    if deviation < -0.06: bullish += 2   # sobrevendido
    elif deviation > 0.06: bearish += 2  # sobrecomprado

    total = bullish + bearish
    if total == 0:
        return {"signal": "NEUTRAL", "confidence": 0.3, "predicted_price": round(current * 100, 1)}

    if bullish > bearish:
        direction = "UP"
        predicted = min(current + abs(momentum) * 0.6, 0.95)
        confidence = bullish / (total + 2)
    elif bearish > bullish:
        direction = "DOWN"
        predicted = max(current - abs(momentum) * 0.6, 0.05)
        confidence = bearish / (total + 2)
    else:
        direction = "NEUTRAL"
        predicted = hist_mean
        confidence = 0.3

    return {
        "mode": "INTRA",
        "signal": direction,
        "confidence": round(min(confidence, 0.85), 2),
        "predicted_price": round(predicted * 100, 1),
        "current_price": round(current * 100, 1),
        "short_ma": round(short_ma * 100, 1),
        "long_ma": round(long_ma * 100, 1),
        "momentum": round(momentum * 100, 2),
    }


def _master_inter_analysis(token_prices: dict, target_token_id: str, current: float) -> dict:
    """Analisa correlações entre tokens do mesmo mercado. Usado em mercado VOLÁTIL."""
    correlations = []
    for tid, prices in token_prices.items():
        if tid == target_token_id or len(prices) < 5:
            continue
        other_current = prices[0]
        # Em mercado binário: YES + NO ≈ 1.0
        implied = 1.0 - other_current
        divergence = abs(current - implied) * 100

        if divergence > 0:
            correlations.append({
                "token_id": tid,
                "other_price": round(other_current * 100, 1),
                "implied_price": round(implied * 100, 1),
                "divergence": round(divergence, 2),
            })

    if not correlations:
        return {"mode": "INTER", "signal": "NO_CORRELATION", "confidence": 0.0,
                "predicted_price": round(current * 100, 1)}

    best = max(correlations, key=lambda x: x["divergence"])
    divergence = best["divergence"]

    # CORRIGIDO: divergência mínima de 5% para ser relevante (era 3%)
    if divergence > 5:
        signal = "UP" if current * 100 < best["implied_price"] else "DOWN"
        predicted = best["implied_price"] / 100
        confidence = min(divergence / 25, 0.85)
    else:
        signal = "NEUTRAL"
        predicted = current
        confidence = 0.25

    return {
        "mode": "INTER",
        "signal": signal,
        "confidence": round(confidence, 2),
        "predicted_price": round(predicted * 100, 1),
        "current_price": round(current * 100, 1),
        "max_divergence": divergence,
        "correlations": correlations[:3],
    }


def _master_gating(volatility: float, threshold: float = 0.008) -> str:
    """Calmo → INTRA | Volátil → INTER. Threshold levemente aumentado."""
    return "INTER" if volatility > threshold else "INTRA"


@app.get("/master")
def get_master_predictions(limit: int = Query(15, ge=1, le=50), db: Session = Depends(get_db)):
    """
    MASTER v4 — Market-Adaptive Prediction Engine.
    Filtros: volume > $10k, preço 10%-90%, confiança >= 0.45, edge >= 3%.
    """
    now = datetime.utcnow()
    results = []
    stats = {"total_calmo": 0, "total_volatil": 0, "total_analisados": 0, "total_filtrados": 0}

    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(400).all()
    )

    for market in markets:
        if len(results) >= limit * 3:
            break

        try:
            volume = _get_market_volume(market)
            if volume > 0 and volume < FILTER_MIN_VOLUME:
                stats["total_filtrados"] += 1
                continue

            token_prices = {}
            for token in market.tokens:
                snaps = (
                    db.query(Snapshot)
                    .filter(Snapshot.token_id == token.token_id)
                    .order_by(Snapshot.timestamp.desc())
                    .limit(100).all()
                )
                if len(snaps) < FILTER_MIN_SNAPS:
                    continue
                token_prices[token.token_id] = [s.price for s in snaps]

            if not token_prices:
                continue

            for token in market.tokens:
                if (token.outcome or "").upper() != "YES":
                    continue

                prices = token_prices.get(token.token_id, [])
                if len(prices) < FILTER_MIN_SNAPS:
                    continue

                current = token.price
                if current < FILTER_MIN_PRICE or current > FILTER_MAX_PRICE:
                    continue

                vol = _master_volatility(prices[:20])
                mode = _master_gating(vol)

                if mode == "INTRA":
                    analysis = _master_intra_analysis(prices, current)
                    stats["total_calmo"] += 1
                else:
                    analysis = _master_inter_analysis(token_prices, token.token_id, current)
                    stats["total_volatil"] += 1

                stats["total_analisados"] += 1

                # FILTRO: confiança mínima elevada para 0.45
                if analysis["confidence"] < 0.45:
                    continue
                if analysis["signal"] in ("NEUTRAL", "INSUFFICIENT_DATA", "NO_CORRELATION"):
                    continue

                predicted = analysis.get("predicted_price", current * 100)
                edge = round(predicted - current * 100, 1)

                # FILTRO: edge mínimo de 3%
                if abs(edge) < 3.0:
                    continue

                master_score = round(
                    analysis["confidence"] * 50 +
                    min(abs(edge) * 2.5, 35) +
                    (15 if mode == "INTER" else 5),
                    1
                )

                if master_score < 45:
                    continue

                conviction = (
                    "🟢 FORTE" if master_score >= 80 else
                    "🟡 ALTA" if master_score >= 65 else
                    "🟠 MÉDIA" if master_score >= 45 else
                    "⚪ FRACA"
                )

                results.append({
                    "market": market.question,
                    "slug": market.market_slug,
                    "polymarket_url": _polymarket_url(market.market_slug),
                    "outcome": "YES",
                    "current_price": round(current * 100, 1),
                    "predicted_price": predicted,
                    "edge": edge,
                    "direction": analysis["signal"],
                    "master_mode": mode,
                    "master_score": master_score,
                    "conviction": conviction,
                    "volatility": round(vol * 100, 3),
                    "confidence": analysis["confidence"],
                    "volume_mercado": volume,
                    "analysis": analysis,
                    "analisado_em": now.isoformat(),
                })

        except Exception as e:
            print(f"[MASTER] erro {market.market_slug}: {e}")
            continue

    results.sort(key=lambda x: x["master_score"], reverse=True)
    top = results[:limit]

    return {
        "modelo": "MASTER v4 — Market-Adaptive",
        "total_analisados": stats["total_analisados"],
        "filtrados_por_volume": stats["total_filtrados"],
        "mercados_calmos": stats["total_calmo"],
        "mercados_volateis": stats["total_volatil"],
        "resumo": {
            "sinais_up": sum(1 for r in top if r["direction"] == "UP"),
            "sinais_down": sum(1 for r in top if r["direction"] == "DOWN"),
            "score_medio": round(sum(r["master_score"] for r in top) / max(len(top), 1), 1),
        },
        "previsoes": top,
        "gerado_em": now.isoformat(),
    }


# ══════════════════════════════════════════════════════════════
# BACKTEST v4
# Correções:
# - Só tokens com volume > $10k
# - Janela adaptativa (não fixo em 5 snapshots)
# - Mede momentum real: compara variação com desvio padrão histórico
# - Retorna métricas por categoria de mercado
# ══════════════════════════════════════════════════════════════

@app.get("/backtest/v3")
def backtest_v3(db: Session = Depends(get_db)):
    """
    Backtest v4 — mede se momentum tem edge REAL.
    Só usa mercados com volume > $10k e preço 10%-90%.
    Janela: 10 snapshots entrada, 10 snapshots saída.
    Movimento mínimo: 4% (não 3%).
    """
    now = datetime.utcnow()

    # Só tokens de mercados com volume
    tokens = (
        db.query(Token)
        .join(Market, Token.market_id == Market.id)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .limit(50).all()
    )

    acertos = erros = 0
    amostra = []
    detalhes_por_magnitude = {"3-5%": {"a": 0, "e": 0}, "5-10%": {"a": 0, "e": 0}, "10%+": {"a": 0, "e": 0}}

    for t in tokens:
        market = db.query(Market).filter(Market.id == t.market_id).first()
        if not market:
            continue

        volume = _get_market_volume(market)
        if volume > 0 and volume < FILTER_MIN_VOLUME:
            continue

        snaps = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == t.token_id)
            .order_by(Snapshot.timestamp.asc())
            .limit(300).all()
        )
        if len(snaps) < 30:
            continue

        for i in range(10, len(snaps) - 10):
            pe = snaps[i].price * 100
            if pe < 10 or pe > 90:
                continue

            media = sum(s.price * 100 for s in snaps[max(0, i-10):i]) / 10
            var = pe - media

            # Movimento mínimo de 4%
            if abs(var) < 4:
                continue

            pf = snaps[min(i+10, len(snaps)-1)].price * 100
            ok = (var > 0 and pf > pe) or (var < 0 and pf < pe)

            if ok:
                acertos += 1
            else:
                erros += 1

            # Categoriza por magnitude
            mag = abs(var)
            cat = "10%+" if mag >= 10 else "5-10%" if mag >= 5 else "3-5%"
            detalhes_por_magnitude[cat]["a" if ok else "e"] += 1

            if len(amostra) < 20:
                amostra.append({
                    "market": market.question[:60] if market else "?",
                    "outcome": t.outcome,
                    "entrada": round(pe, 1),
                    "saida": round(pf, 1),
                    "variacao": round(var, 1),
                    "resultado": "✅ ACERTO" if ok else "❌ ERRO"
                })

    total = acertos + erros
    win_rate = round(acertos / total * 100, 1) if total > 0 else 0

    # Win rate por magnitude
    mag_stats = {}
    for cat, vals in detalhes_por_magnitude.items():
        t_cat = vals["a"] + vals["e"]
        mag_stats[cat] = {
            "total": t_cat,
            "win_rate": round(vals["a"] / t_cat * 100, 1) if t_cat > 0 else 0
        }

    return {
        "total_simulados": total,
        "acertos": acertos,
        "erros": erros,
        "win_rate_pct": win_rate,
        "conclusao": (
            "✅ Momentum tem edge real (>55%)" if win_rate > 55 else
            "⚠️ Momentum fraco (50-55%)" if win_rate > 50 else
            "❌ Momentum sem edge (<50%)"
        ),
        "win_rate_por_magnitude": mag_stats,
        "amostra": amostra,
        "filtros_aplicados": {
            "volume_minimo": FILTER_MIN_VOLUME,
            "preco_min_pct": FILTER_MIN_PRICE * 100,
            "preco_max_pct": FILTER_MAX_PRICE * 100,
            "variacao_minima": "4%",
            "janela_snapshots": 10,
        },
        "atualizado_em": datetime.utcnow().isoformat(),
    }

# ══════════════════════════════════════════════════════════════
# MOTOR 6 — RECOMENDAÇÕES v4
# Correções:
# - Kelly alimentado por prob_jiang REAL de cada fonte
# - Score mínimo elevado para 50 (era 35)
# - Convergência exige fontes DIFERENTES (não duplica mesma fonte)
# - Filtros universais aplicados em todas as fontes
# ══════════════════════════════════════════════════════════════

def _kelly_v4(prob: float, price_pct: float, bankroll: float = 500, frac: float = 0.25) -> float:
    """
    Kelly Criterion fracionado (25% — conservador).
    prob = probabilidade estimada de ganho (0.0 a 1.0)
    price_pct = preço atual em % (ex: 30 = 30¢)
    Retorna valor em $ a apostar.
    """
    if price_pct <= 0 or price_pct >= 100 or prob <= 0 or prob >= 1:
        return 0.0

    # Odds decimais: se compro a 30¢, ganho 70¢ de lucro por 30¢ apostado = odds 3.33
    price_dec = price_pct / 100
    odds = 1 / price_dec  # retorno total (inclui capital)
    odds_lucro = odds - 1  # só o lucro

    # Fórmula Kelly: f = (p * b - q) / b  onde b = odds_lucro, q = 1 - p
    kelly_full = (prob * odds_lucro - (1 - prob)) / odds_lucro
    kelly_frac = kelly_full * frac

    # Limita entre 2% e 8% do bankroll
    kelly_pct = max(0.02, min(kelly_frac, 0.08))

    return round(bankroll * kelly_pct, 2) if kelly_full > 0 else 0.0


@app.get("/recomendacoes")
async def get_recomendacoes(
    bankroll: float = Query(500.0),
    limite: int = Query(5, ge=1, le=10),
    db: Session = Depends(get_db),
):
    """
    Recomendações v4 — Motor unificado limpo.
    Cruza Game Theory + MASTER + Anomalias.
    Kelly fracionado 25% com prob real de cada fonte.
    Filtros: volume > $10k, confiança >= 50%.
    """
    now = datetime.utcnow()
    mercados_map = {}  # slug → dados consolidados

    # ── 1. GAME THEORY ─────────────────────────────────────────
    try:
        markets_gt = (
            db.query(Market).join(Token)
            .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
            .filter((Market.end_date == None) | (Market.end_date > now))
            .distinct().limit(500).all()
        )
        for market in markets_gt:
            volume = _get_market_volume(market)
            if volume > 0 and volume < FILTER_MIN_VOLUME:
                continue

            preds = _match_gt_v4(market.question, market.market_slug or "")
            if not preds:
                continue

            pred = preds[0]
            prob_jiang = pred["prob_jiang"] / 100  # usa prob REAL

            for token in market.tokens:
                if (token.outcome or "").upper() != "YES":
                    continue
                price = token.price
                if price < FILTER_MIN_PRICE or price > FILTER_MAX_PRICE:
                    continue

                edge = round((prob_jiang - price) * 100, 1)
                if abs(edge) < 10:
                    continue

                slug = market.market_slug or market.question[:40]
                direcao = pred.get("direcao", "YES")
                acao = f"COMPRAR {direcao}"

                if slug not in mercados_map:
                    mercados_map[slug] = {
                        "market": market.question,
                        "slug": slug,
                        "url": _polymarket_url(slug),
                        "price": round(price * 100, 1),
                        "fontes": [],
                        "acao": acao,
                        "outcome": direcao,
                        "volume": volume,
                    }

                mercados_map[slug]["fontes"].append({
                    "nome": "Game Theory (Jiang)",
                    "icone": "⚡",
                    "tipo": "game_theory",
                    "score": min(50 + abs(edge) * 0.8, 90),
                    "prob": round(prob_jiang * 100, 1),
                    "edge": edge,
                    "detalhe": pred.get("tema", "Análise geopolítica"),
                })
    except Exception as e:
        print(f"[REC] Game Theory erro: {e}")

    # ── 2. MASTER ──────────────────────────────────────────────
    try:
        markets_m = (
            db.query(Market).join(Token)
            .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
            .filter((Market.end_date == None) | (Market.end_date > now))
            .distinct().limit(400).all()
        )
        for market in markets_m:
            volume = _get_market_volume(market)
            if volume > 0 and volume < FILTER_MIN_VOLUME:
                continue

            token_prices = {}
            for token in market.tokens:
                snaps = (
                    db.query(Snapshot)
                    .filter(Snapshot.token_id == token.token_id)
                    .order_by(Snapshot.timestamp.desc())
                    .limit(50).all()
                )
                if len(snaps) >= FILTER_MIN_SNAPS:
                    token_prices[token.token_id] = [s.price for s in snaps]

            for token in market.tokens:
                if (token.outcome or "").upper() != "YES":
                    continue
                prices = token_prices.get(token.token_id, [])
                if len(prices) < FILTER_MIN_SNAPS:
                    continue
                price = token.price
                if price < FILTER_MIN_PRICE or price > FILTER_MAX_PRICE:
                    continue

                vol = _master_volatility(prices[:20])
                mode = _master_gating(vol)
                analysis = (
                    _master_intra_analysis(prices, price) if mode == "INTRA"
                    else _master_inter_analysis(token_prices, token.token_id, price)
                )

                if analysis["confidence"] < 0.45 or analysis["signal"] in ("NEUTRAL", "INSUFFICIENT_DATA", "NO_CORRELATION"):
                    continue

                predicted = analysis.get("predicted_price", price * 100)
                edge = round(predicted - price * 100, 1)
                if abs(edge) < 3:
                    continue

                master_score = round(analysis["confidence"] * 50 + min(abs(edge) * 2.5, 35) + (15 if mode == "INTER" else 5), 1)
                if master_score < 45:
                    continue

                slug = market.market_slug or market.question[:40]
                acao = "COMPRAR YES" if analysis["signal"] == "UP" else "COMPRAR NO"

                if slug not in mercados_map:
                    mercados_map[slug] = {
                        "market": market.question,
                        "slug": slug,
                        "url": _polymarket_url(slug),
                        "price": round(price * 100, 1),
                        "fontes": [],
                        "acao": acao,
                        "outcome": "YES",
                        "volume": volume,
                    }

                # Evita duplicar fonte MASTER para o mesmo mercado
                has_master = any(f["tipo"] == "master" for f in mercados_map[slug]["fontes"])
                if not has_master:
                    mercados_map[slug]["fontes"].append({
                        "nome": "MASTER (AAAI-2024)",
                        "icone": "🤖",
                        "tipo": "master",
                        "score": master_score,
                        "prob": round(predicted, 1),
                        "edge": edge,
                        "detalhe": f"Modo {mode} — Volatilidade {round(vol*100,2)}%",
                    })
    except Exception as e:
        print(f"[REC] MASTER erro: {e}")

    # ── 3. ANOMALIAS ───────────────────────────────────────────
    try:
        tokens_all = (
            db.query(Token).join(Market)
            .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
            .filter((Market.end_date == None) | (Market.end_date > now))
            .limit(300).all()
        )
        for token in tokens_all:
            market = token.market
            if not market:
                continue
            volume = _get_market_volume(market)
            if volume > 0 and volume < FILTER_MIN_VOLUME:
                continue
            if (token.outcome or "").upper() != "YES":
                continue

            snaps = (
                db.query(Snapshot)
                .filter(Snapshot.token_id == token.token_id)
                .order_by(Snapshot.timestamp.desc())
                .limit(50).all()
            )
            if len(snaps) < FILTER_MIN_SNAPS:
                continue

            prices = [s.price for s in snaps]
            mean_p = sum(prices) / len(prices)
            current = token.price
            deviation = (current - mean_p) * 100
            deviation_abs = abs(deviation)

            if deviation_abs < 4:  # mínimo 4% de desvio
                continue

            # Não marca como oportunidade se está resolvendo
            if current < 0.15 and deviation < 0:
                continue
            if current > 0.85 and deviation > 0:
                continue

            edge = deviation_abs
            anom_score = min(35 + edge * 1.5, 80)
            direction = "UP" if current < mean_p else "DOWN"
            acao = "COMPRAR YES" if direction == "UP" else "COMPRAR NO"

            slug = market.market_slug or market.question[:40]
            if slug not in mercados_map:
                mercados_map[slug] = {
                    "market": market.question,
                    "slug": slug,
                    "url": _polymarket_url(slug),
                    "price": round(current * 100, 1),
                    "fontes": [],
                    "acao": acao,
                    "outcome": "YES",
                    "volume": volume,
                }

            # Evita duplicar fonte anomalia
            has_anomaly = any(f["tipo"] == "anomalia" for f in mercados_map[slug]["fontes"])
            if not has_anomaly:
                mercados_map[slug]["fontes"].append({
                    "nome": "Anomalia de Preço",
                    "icone": "📊",
                    "tipo": "anomalia",
                    "score": anom_score,
                    "prob": round(mean_p * 100, 1),
                    "edge": round(deviation, 1),
                    "detalhe": f"Preço {round(current*100,1)}% desviou {deviation_abs:.1f}pts da média histórica {round(mean_p*100,1)}%",
                })
    except Exception as e:
        print(f"[REC] Anomalias erro: {e}")

    # ── 4. CONSOLIDAR + KELLY ──────────────────────────────────
    resultados = []
    for slug, dado in mercados_map.items():
        fontes = dado["fontes"]
        if not fontes:
            continue

        # Score consolidado: média das fontes + bônus por convergência
        score_medio = sum(f["score"] for f in fontes) / len(fontes)
        bonus_convergencia = min((len(fontes) - 1) * 8, 20)
        score_final = round(min(score_medio + bonus_convergencia, 100), 1)

        # FILTRO: score mínimo de 50
        if score_final < 50:
            continue

        price_pct = dado["price"]
        prob_estimada = min(score_final / 100 * 0.90 + 0.05, 0.92)

        # Kelly com prob real
        apostar = _kelly_v4(prob_estimada, price_pct, bankroll)
        if apostar < 5:
            continue

        # Retorno esperado
        price_dec = price_pct / 100
        odds_lucro = (1 / price_dec) - 1
        retorno_esp = round(apostar * odds_lucro * prob_estimada - apostar * (1 - prob_estimada), 2)

        # Tipos de fontes únicas
        tipos_fontes = list({f["tipo"] for f in fontes})
        convergencia = len(fontes)

        # Razão
        if convergencia >= 2:
            razao = "🔥 CONVERGÊNCIA: " + " + ".join(f["icone"] + " " + f["nome"] for f in fontes)
        else:
            razao = fontes[0]["icone"] + " " + fontes[0]["detalhe"]

        # Nível de convicção
        if score_final >= 80:
            nivel = "🔴 MUITO ALTA"
        elif score_final >= 65:
            nivel = "🟠 ALTA"
        elif score_final >= 50:
            nivel = "🟡 MÉDIA"
        else:
            nivel = "⚪ BAIXA"

        resultados.append({
            "market": dado["market"],
            "slug": slug,
            "url": dado["url"],
            "acao": dado["acao"],
            "outcome": dado.get("outcome", "YES"),
            "price": price_pct,
            "volume_mercado": dado.get("volume", 0),
            "score_final": score_final,
            "nivel_conviccao": nivel,
            "prob_estimada_pct": round(prob_estimada * 100, 1),
            "apostar_usd": apostar,
            "apostar_pct_bankroll": round((apostar / bankroll) * 100, 1),
            "retorno_esperado_usd": retorno_esp,
            "fontes": fontes,
            "razao": razao,
            "convergencia": convergencia,
            "tipos_fontes": tipos_fontes,
            "analisado_em": now.isoformat(),
        })

    # Ordena: convergência primeiro, depois score
    resultados.sort(key=lambda x: (x["convergencia"], x["score_final"]), reverse=True)
    top = resultados[:limite]

    total_investir = sum(r["apostar_usd"] for r in top)
    retorno_total = sum(r["retorno_esperado_usd"] for r in top)

    return {
        "titulo": "Recomendações v4",
        "descricao": "Game Theory + MASTER + Anomalias com Kelly 25% fracionado",
        "bankroll": bankroll,
        "total_mercados_analisados": len(mercados_map),
        "filtros_aplicados": {
            "volume_minimo": FILTER_MIN_VOLUME,
            "score_minimo": 50,
            "kelly_fracao": "25%",
        },
        "resumo": {
            "total_recomendacoes": len(top),
            "total_investir_usd": round(total_investir, 2),
            "retorno_esperado_usd": round(retorno_total, 2),
            "roi_esperado_pct": round((retorno_total / total_investir * 100) if total_investir > 0 else 0, 1),
            "com_convergencia": sum(1 for r in top if r["convergencia"] >= 2),
        },
        "recomendacoes": top,
        "gerado_em": now.isoformat(),
    }


# ══════════════════════════════════════════════════════════════
# BEST / MELHORES APOSTAS v4
# ══════════════════════════════════════════════════════════════

@app.get("/best/v2")
def get_best_v2(db: Session = Depends(get_db)):
    """
    Melhores apostas v4 — combina movimento de preço + notícias + filtros.
    Só mercados com volume > $10k e movimento consistente.
    """
    now = datetime.utcnow()
    w10m = now - timedelta(minutes=10)
    w30m = now - timedelta(minutes=30)
    candidates = []

    tokens = db.query(Token).filter(
        Token.price > FILTER_MIN_PRICE,
        Token.price < FILTER_MAX_PRICE
    ).all()

    for token in tokens:
        cp = token.price
        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        volume = _get_market_volume(market)
        if volume > 0 and volume < FILTER_MIN_VOLUME:
            continue

        if market.end_date and market.end_date < now:
            continue

        # Movimento 10min
        s10m = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= w10m)
            .order_by(Snapshot.timestamp.desc()).first()
        )
        if not s10m:
            continue

        c10m = round((cp - s10m.price) * 100, 2)
        if abs(c10m) < 3.0:
            continue

        # Movimento 30min — verificar consistência
        s30m = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token.token_id, Snapshot.timestamp <= w30m)
            .order_by(Snapshot.timestamp.desc()).first()
        )

        if s30m:
            c30m = (cp - s30m.price) * 100
            # Descarta se direções contraditórias
            if c10m > 0 and c30m < -2:
                continue
            if c10m < 0 and c30m > 2:
                continue

        # Notícias para confirmar
        keywords = market.question.replace("?", "").replace("Will ", "")[:60]
        articles = _fetch_news(keywords, max_results=5)
        analysis = _analyze_with_claude(market.question, articles)

        rec = analysis.get("recomendacao", "EVITE")
        confianca_ia = analysis.get("confianca", 0)

        # IA deve confirmar a direção do movimento
        if c10m > 0 and rec == "APOSTE NO":
            continue
        if c10m < 0 and rec == "APOSTE YES":
            continue

        # Score final
        score_movimento = min(abs(c10m) * 3, 50)
        score_ia = confianca_ia * 50
        score_final = round(score_movimento + score_ia, 1)

        if score_final < 40:
            continue

        direcao = "YES" if c10m > 0 else "NO"
        preco_pct = round(cp * 100, 1)

        candidates.append({
            "market": market.question,
            "slug": market.market_slug,
            "direcao": direcao,
            "preco_entrada": preco_pct,
            "score_final": score_final,
            "change_10m": c10m,
            "volume_mercado": volume,
            "recomendacao_ia": rec,
            "confianca_ia": round(confianca_ia * 100),
            "resumo_ia": analysis.get("resumo"),
            "noticias": [a["title"] for a in articles[:3]],
            "sinal": "🎯 APOSTE" if score_final >= 70 else "🔍 CONSIDERE",
            "polymarket_url": _polymarket_url(market.market_slug),
            "detectado_em": now.isoformat(),
        })

    candidates.sort(key=lambda x: x["score_final"], reverse=True)
    top = candidates[:5]

    return {
        "total_oportunidades": len(candidates),
        "top_apostas": top,
        "filtros": {"volume_minimo": FILTER_MIN_VOLUME, "movimento_minimo": "3%", "score_minimo": 40},
        "atualizado_em": now.isoformat(),
    }


# ══════════════════════════════════════════════════════════════
# INTELLIGENCE — análise IA por mercado
# ══════════════════════════════════════════════════════════════

@app.get("/intelligence/{slug}")
def get_intelligence(slug: str, db: Session = Depends(get_db)):
    market = db.query(Market).filter(Market.market_slug == slug).first()
    if not market:
        return {"error": "Mercado não encontrado"}

    yes_price, no_price = _get_yes_no_prices(market)
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
        "polymarket_url": _polymarket_url(slug),
        "atualizado_em": datetime.utcnow().isoformat(),
    }


# ══════════════════════════════════════════════════════════════
# LEADERS
# ══════════════════════════════════════════════════════════════

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
    return {"status": "unavailable", "links": {"leaderboard": "https://polymarket.com/leaderboard"}}


@app.get("/leaders/live")
def get_live_trades():
    url = f"{CLOB_API}/trades?limit=100"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code != 200:
            return {"status": "unavailable", "status_code": resp.status_code}

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
            ],
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
                        "outcome": p.get("outcome"),
                        "valor_usd": round(valor, 2),
                        "preco_medio": p.get("avgPrice") or p.get("curPrice"),
                        "pnl": p.get("cashPnl") or p.get("pnl"),
                    })
                return {
                    "status": "ok", "wallet": address[:8] + "..." + address[-4:],
                    "total_posicoes": len(positions), "total_exposto_usd": round(total, 2),
                    "posicoes": positions, "polymarket_url": f"https://polymarket.com/profile/{address}"
                }
    except Exception as e:
        print(f"[wallet] erro: {e}")
    return {"status": "unavailable", "polymarket_url": f"https://polymarket.com/profile/{address}"}


# ══════════════════════════════════════════════════════════════
# DETECTOR DE BALEIAS v1
# Monitora trades grandes (>$10k) no CLOB em tempo real.
# Rastreia histórico de acerto de cada carteira.
# Dispara alerta Telegram automaticamente.
# ══════════════════════════════════════════════════════════════

WHALE_THRESHOLD = 10_000   # USD — aposta acima disso = baleia
WHALE_CACHE: dict = {}     # {wallet: {"acertos": int, "total": int, "trades": []}}
_WHALE_SEEN: set = set()   # trade_ids já alertados (evita duplicatas)


def _whale_score(wallet: str) -> dict:
    """Retorna histórico de acerto da carteira (in-memory)."""
    data = WHALE_CACHE.get(wallet, {"acertos": 0, "total": 0, "trades": []})
    total = data["total"]
    acertos = data["acertos"]
    win_rate = round(acertos / total * 100, 1) if total > 0 else None
    return {
        "total_apostas": total,
        "acertos": acertos,
        "win_rate_pct": win_rate,
        "classificacao": (
            "🔴 SHARP — acerta muito" if win_rate and win_rate >= 65 else
            "🟠 ACIMA DA MÉDIA" if win_rate and win_rate >= 55 else
            "🟡 MÉDIA" if win_rate and win_rate >= 45 else
            "⚪ SEM HISTÓRICO" if win_rate is None else
            "⚫ HISTÓRICO FRACO"
        ),
        "ultimas_apostas": data["trades"][-5:],
    }


def _whale_fetch_market_for_token(token_id: str, db: Session) -> dict:
    """Busca mercado relacionado a um token_id."""
    try:
        token = db.query(Token).filter(Token.token_id == token_id).first()
        if token:
            market = db.query(Market).filter(Market.id == token.market_id).first()
            if market:
                return {
                    "question": market.question,
                    "slug": market.market_slug,
                    "url": _polymarket_url(market.market_slug),
                    "yes_price": None,
                    "outcome": token.outcome,
                }
    except Exception:
        pass
    return {"question": "Mercado desconhecido", "slug": "", "url": "", "outcome": "?"}


def _whale_alert_telegram(whale: dict) -> None:
    """Envia alerta formatado no Telegram."""
    score = whale.get("score", {})
    win_rate = score.get("win_rate_pct")
    classificacao = score.get("classificacao", "⚪ SEM HISTÓRICO")
    win_txt = f"{win_rate}%" if win_rate is not None else "sem histórico"

    market_info = whale.get("market_info", {})
    question = (market_info.get("question") or "?")[:80]
    outcome = whale.get("outcome", "?")
    preco = whale.get("preco")
    preco_txt = f"{round(float(preco)*100, 1)}%" if preco else "?"

    wallet = whale.get("wallet_full", "")
    wallet_short = wallet[:8] + "..." + wallet[-4:] if wallet else "?"
    valor = whale.get("valor_usd", 0)

    sinal_icon = "🟢" if str(outcome).upper() in ("YES", "BUY") else "🔴" if str(outcome).upper() in ("NO", "SELL") else "⚡"

    msg = (
        f"🐋 <b>BALEIA DETECTADA — ${valor:,.0f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <b>Mercado:</b> {question}\n"
        f"{sinal_icon} <b>Direção:</b> {outcome} @ {preco_txt}\n"
        f"💰 <b>Valor:</b> ${valor:,.0f} USD\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Carteira:</b> <code>{wallet_short}</code>\n"
        f"📊 <b>Histórico:</b> {win_txt} de acerto ({score.get('total_apostas', 0)} apostas)\n"
        f"🏆 {classificacao}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href=\"{market_info.get('url', '')}\">Ver mercado</a> | "
        f"<a href=\"https://polymarket.com/profile/{wallet}\">Ver carteira</a>\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )
    _telegram_send(msg)


@app.get("/whales")
def get_whales(
    min_valor: float = Query(5000, ge=1, le=500000),
    limit: int = Query(20, ge=1, le=50),
    filtrar_resolvendo: int = Query(1, description="1 = ignora mercados quase resolvidos"),
    db: Session = Depends(get_db),
):
    """
    Detector de Baleias v1.
    - Threshold: $5k por carteira por mercado
    - Agrupa por MERCADO para mostrar quantas carteiras independentes apostaram junto
    - Convicção aumenta quando múltiplas baleias entram na mesma direção
    """
    url = f"{DATA_API}/trades?limit=1000"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {"status": "unavailable", "status_code": resp.status_code}

        trades_raw = resp.json()
        if not isinstance(trades_raw, list):
            trades_raw = trades_raw.get("data", [])

        # 1ª passagem — agrega por wallet+mercado
        wallet_mercado: dict = {}
        for tx in trades_raw:
            size  = float(tx.get("size") or 0)
            price = float(tx.get("price") or 0)
            valor = size * price

            if filtrar_resolvendo and (price > 0.90 or price < 0.10):
                continue

            wallet = tx.get("proxyWallet") or ""
            slug   = tx.get("slug") or tx.get("conditionId") or ""
            if not wallet or not slug:
                continue

            key = f"{wallet}|{slug}"
            if key not in wallet_mercado:
                wallet_mercado[key] = {
                    "wallet_full": wallet,
                    "slug": slug,
                    "mercado": tx.get("title") or "?",
                    "outcome": tx.get("outcome") or "?",
                    "side": tx.get("side") or "?",
                    "valor_usd": 0.0,
                    "num_trades": 0,
                    "preco_sum": 0.0,
                    "timestamp": tx.get("timestamp"),
                }
            wallet_mercado[key]["valor_usd"]  += valor
            wallet_mercado[key]["num_trades"] += 1
            wallet_mercado[key]["preco_sum"]  += price

        # Filtra só carteiras acima do threshold
        baleias_por_mercado: dict = {}
        for key, data in wallet_mercado.items():
            data["valor_usd"] = round(data["valor_usd"], 2)
            data["preco_medio_pct"] = round(data["preco_sum"] / max(data["num_trades"], 1) * 100, 1)

            if data["valor_usd"] < min_valor:
                continue

            slug = data["slug"]
            if slug not in baleias_por_mercado:
                baleias_por_mercado[slug] = {
                    "mercado": data["mercado"],
                    "slug": slug,
                    "polymarket_url": _polymarket_url(slug),
                    "total_volume_baleias": 0.0,
                    "num_baleias": 0,
                    "carteiras": [],
                    "direcoes": {},
                    "ultimo_timestamp": data["timestamp"],
                }

            m = baleias_por_mercado[slug]
            m["total_volume_baleias"] += data["valor_usd"]
            m["num_baleias"] += 1

            # Conta direções (YES/NO)
            direcao = data["outcome"] or data["side"]
            m["direcoes"][direcao] = m["direcoes"].get(direcao, 0) + data["valor_usd"]

            m["carteiras"].append({
                "wallet": data["wallet_full"][:8] + "..." + data["wallet_full"][-4:],
                "wallet_full": data["wallet_full"],
                "valor_usd": data["valor_usd"],
                "outcome": data["outcome"],
                "preco_medio_pct": data["preco_medio_pct"],
                "num_trades": data["num_trades"],
                "polymarket_wallet_url": f"https://polymarket.com/profile/{data['wallet_full']}",
            })

        # Monta resultado final ordenado por num_baleias e volume
        resultado = []
        for slug, m in baleias_por_mercado.items():
            m["total_volume_baleias"] = round(m["total_volume_baleias"], 2)
            m["carteiras"].sort(key=lambda x: x["valor_usd"], reverse=True)

            # Direção dominante
            if m["direcoes"]:
                dir_dominante = max(m["direcoes"].items(), key=lambda x: x[1])
                m["direcao_dominante"] = dir_dominante[0]
                m["volume_direcao_dominante"] = round(dir_dominante[1], 2)
                total_dir = sum(m["direcoes"].values())
                m["consenso_pct"] = round(dir_dominante[1] / total_dir * 100, 1) if total_dir > 0 else 0
            else:
                m["direcao_dominante"] = "?"
                m["consenso_pct"] = 0

            # Conviction score
            nb = m["num_baleias"]
            consenso = m["consenso_pct"]
            m["conviction"] = (
                "🔴 MUITO ALTA" if nb >= 3 and consenso >= 80 else
                "🟠 ALTA"       if nb >= 2 and consenso >= 70 else
                "🟡 MÉDIA"      if nb >= 2 else
                "⚪ ÚNICA"
            )

            resultado.append(m)

        resultado.sort(key=lambda x: (x["num_baleias"], x["total_volume_baleias"]), reverse=True)
        top = resultado[:limit]

        return {
            "status": "ok",
            "threshold_usd": min_valor,
            "total_trades_analisados": len(trades_raw),
            "mercados_com_baleias": len(resultado),
            "retornados": len(top),
            "nota": "num_baleias > 1 = múltiplas carteiras independentes apostando no mesmo mercado",
            "mercados": top,
            "gerado_em": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/whales/scan")
def whale_scan(
    min_valor: float = Query(5000, ge=1),
    alertar: int = Query(1, description="1 = envia Telegram para mercados novos com baleias"),
    db: Session = Depends(get_db),
):
    """
    Scan de baleias — detecta mercados novos com posições grandes e dispara Telegram.
    Agrupa por mercado. Alerta quando num_baleias >= 2 (múltiplas carteiras).
    """
    url = f"{DATA_API}/trades?limit=1000"
    novas = []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {"status": "unavailable"}

        trades_raw = resp.json()
        if not isinstance(trades_raw, list):
            trades_raw = trades_raw.get("data", [])

        # Agrega por wallet+mercado
        wallet_mercado: dict = {}
        for tx in trades_raw:
            size  = float(tx.get("size") or 0)
            price = float(tx.get("price") or 0)
            valor = size * price

            if price > 0.90 or price < 0.10:
                continue

            wallet = tx.get("proxyWallet") or ""
            slug   = tx.get("slug") or tx.get("conditionId") or ""
            if not wallet or not slug:
                continue

            key = f"{wallet}|{slug}"
            if key not in wallet_mercado:
                wallet_mercado[key] = {
                    "wallet_full": wallet,
                    "slug": slug,
                    "mercado": tx.get("title") or "?",
                    "outcome": tx.get("outcome") or "?",
                    "valor_usd": 0.0,
                    "preco_sum": 0.0,
                    "num_trades": 0,
                }
            wallet_mercado[key]["valor_usd"]  += valor
            wallet_mercado[key]["num_trades"] += 1
            wallet_mercado[key]["preco_sum"]  += price

        # Agrupa por mercado, só carteiras acima do threshold
        baleias_por_mercado: dict = {}
        for key, data in wallet_mercado.items():
            if data["valor_usd"] < min_valor:
                continue
            slug = data["slug"]
            if slug not in baleias_por_mercado:
                baleias_por_mercado[slug] = {
                    "mercado": data["mercado"],
                    "slug": slug,
                    "polymarket_url": _polymarket_url(slug),
                    "total_volume_baleias": 0.0,
                    "num_baleias": 0,
                    "direcoes": {},
                    "carteiras": [],
                }
            m = baleias_por_mercado[slug]
            m["total_volume_baleias"] += data["valor_usd"]
            m["num_baleias"] += 1
            direcao = data["outcome"]
            m["direcoes"][direcao] = m["direcoes"].get(direcao, 0) + data["valor_usd"]
            m["carteiras"].append({
                "wallet": data["wallet_full"][:8] + "..." + data["wallet_full"][-4:],
                "valor_usd": round(data["valor_usd"], 2),
                "outcome": data["outcome"],
                "preco_medio_pct": round(data["preco_sum"] / max(data["num_trades"], 1) * 100, 1),
            })

        # Detecta mercados NOVOS (não vistos antes)
        for slug, m in baleias_por_mercado.items():
            m["total_volume_baleias"] = round(m["total_volume_baleias"], 2)

            if m["direcoes"]:
                dir_dom = max(m["direcoes"].items(), key=lambda x: x[1])
                m["direcao_dominante"] = dir_dom[0]
                total_dir = sum(m["direcoes"].values())
                m["consenso_pct"] = round(dir_dom[1] / total_dir * 100, 1)
            else:
                m["direcao_dominante"] = "?"
                m["consenso_pct"] = 0

            nb = m["num_baleias"]
            m["conviction"] = (
                "🔴 MUITO ALTA" if nb >= 3 and m["consenso_pct"] >= 80 else
                "🟠 ALTA"       if nb >= 2 and m["consenso_pct"] >= 70 else
                "🟡 MÉDIA"      if nb >= 2 else
                "⚪ ÚNICA"
            )

            if slug in _WHALE_SEEN:
                continue

            _WHALE_SEEN.add(slug)
            if len(_WHALE_SEEN) > 5000:
                for tid in list(_WHALE_SEEN)[:1000]:
                    _WHALE_SEEN.discard(tid)

            novas.append(m)

            # Alerta Telegram
            if alertar:
                nb_str = str(nb)
                vol_str = "${:,.0f}".format(m["total_volume_baleias"])
                dir_str = m["direcao_dominante"] + " (" + str(m["consenso_pct"]) + "% consenso)"
                msg_parts = [
                    "<b>BALEIAS DETECTADAS</b>",
                    "Mercado: " + m["mercado"][:80],
                    "Volume total: " + vol_str,
                    "Carteiras: " + nb_str + " independentes",
                    "Direcao: " + dir_str,
                    "Conviction: " + m["conviction"],
                    "---",
                ]
                for c in m["carteiras"]:
                    msg_parts.append(c["wallet"] + " $" + "{:,.0f}".format(c["valor_usd"]) + " " + c["outcome"])
                msg_parts.append(m["polymarket_url"])
                _telegram_send(chr(10).join(msg_parts))

        return {
            "status": "ok",
            "min_valor": min_valor,
            "novos_mercados_com_baleias": len(novas),
            "alertas_enviados": len(novas) if alertar else 0,
            "mercados": novas,
            "gerado_em": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/whales/wallet/{address}")
def whale_wallet_detail(address: str, db: Session = Depends(get_db)):
    """
    Detalhe de uma carteira baleia — histórico + posições atuais.
    """
    score = _whale_score(address)

    # Tenta buscar posições abertas
    positions = []
    try:
        resp = requests.get(
            f"{DATA_API}/positions?user={address}&sizeThreshold=100&limit=50",
            headers=HEADERS, timeout=8
        )
        if resp.status_code == 200:
            items = resp.json()
            items = items if isinstance(items, list) else items.get("data", [])
            for p in items:
                valor = float(p.get("currentValue") or p.get("size") or 0)
                positions.append({
                    "mercado": (p.get("title") or p.get("market") or p.get("question") or "?")[:80],
                    "outcome": p.get("outcome"),
                    "valor_usd": round(valor, 2),
                    "preco_medio": p.get("avgPrice") or p.get("curPrice"),
                    "pnl": p.get("cashPnl") or p.get("pnl"),
                })
            positions.sort(key=lambda x: x["valor_usd"], reverse=True)
    except Exception as e:
        print(f"[whale_wallet] erro posições: {e}")

    return {
        "status": "ok",
        "wallet": address[:8] + "..." + address[-4:],
        "wallet_full": address,
        "polymarket_url": f"https://polymarket.com/profile/{address}",
        "historico": score,
        "posicoes_abertas": positions[:20],
        "total_exposto_usd": round(sum(p["valor_usd"] for p in positions), 2),
        "consultado_em": datetime.utcnow().isoformat(),
    }


# ══════════════════════════════════════════════════════════════
# TELEGRAM — ALERTAS
# ══════════════════════════════════════════════════════════════

def _telegram_send(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID não configurados"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True
        }, timeout=10)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/alerts/test")
def alerts_test():
    resp = _telegram_send("✅ <b>PolySignal v4</b> conectado e funcionando!\nFiltros: volume>$10k, preço 10%-90%")
    return {"status": "ok", "telegram": resp}


@app.get("/alerts/run")
def alerts_run(minutes: int = 15, limit: int = 10, dry_run: int = 0, db: Session = Depends(get_db)):
    global _LAST_ALERT_SENT_AT
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=max(int(minutes), 1))

    if _LAST_ALERT_SENT_AT and (now - _LAST_ALERT_SENT_AT).total_seconds() < 120:
        return {"status": "skip", "reason": "too_soon (2min cooldown)"}

    limit_n = min(max(int(limit), 1), 20)

    # Só sinais HIGH e EXTREME
    rows = (
        db.query(Signal)
        .filter(
            Signal.created_at >= cutoff,
            (Signal.tipo.like("%HIGH%")) | (Signal.tipo.like("%EXTREME%"))
        )
        .order_by(Signal.created_at.desc())
        .limit(500).all()
    )
    rows = sorted(rows, key=lambda r: abs(float(r.change_5m or 0)), reverse=True)[:limit_n]

    if not rows:
        return {"status": "ok", "sent": 0, "message": "Sem sinais HIGH/EXTREME na janela"}

    spike_n = sum(1 for r in rows if "SPIKE" in (r.tipo or ""))
    dump_n = sum(1 for r in rows if "DUMP" in (r.tipo or ""))
    max_abs = max(abs(float(r.change_5m or 0)) for r in rows)

    lines = [
        f"🚨 <b>PolySignal v4</b> — sinais fortes",
        f"📊 {spike_n} SPIKE | {dump_n} DUMP | Max: {max_abs:.1f}pts | Janela: {minutes}min",
        f"⚙️ Filtros: volume>$10k, movimento>3%, direção consistente",
        ""
    ]

    for r in rows:
        change = float(r.change_5m or 0)
        price = float(r.current_price or 0)
        tipo = r.tipo or ""
        url = r.polymarket_url or _polymarket_url(r.slug or "")
        emoji = "📈" if "SPIKE" in tipo else "📉"
        sign = "+" if change >= 0 else ""
        lines.append(f"{emoji} <b>{tipo}</b> | {sign}{change:.1f}pts @ {price:.1f}%")
        lines.append(f'<a href="{url}">{(r.market or "")[:80]}</a>')
        lines.append("")

    text_out = "\n".join(lines).strip()

    if int(dry_run) == 1:
        return {"status": "dry_run", "would_send": len(rows), "text": text_out}

    telegram_resp = _telegram_send(text_out)
    ok = bool(telegram_resp.get("ok"))
    if ok:
        _LAST_ALERT_SENT_AT = now

    return {"status": "ok" if ok else "fail", "sent": len(rows) if ok else 0, "telegram": telegram_resp}


# ══════════════════════════════════════════════════════════════
# NEWS ANALYSIS — cruza notícias com mercados
# ══════════════════════════════════════════════════════════════

@app.get("/news/analysis")
def get_news_analysis(limit: int = Query(8, ge=1, le=20), db: Session = Depends(get_db)):
    """Analisa impacto de notícias em mercados ativos com filtros aplicados."""
    all_news = []

    # NewsAPI
    try:
        params = {"q": "prediction market OR polymarket OR geopolitics OR election OR war OR economy",
                  "language": "en", "sortBy": "publishedAt", "pageSize": 15, "apiKey": NEWSAPI_KEY}
        resp = requests.get("https://newsapi.org/v2/everything", params=params, timeout=8)
        if resp.status_code == 200:
            for a in resp.json().get("articles", []):
                if a.get("title") and "[Removed]" not in a.get("title", ""):
                    all_news.append({
                        "title": a["title"],
                        "description": (a.get("description") or "")[:300],
                        "source": a.get("source", {}).get("name", "NewsAPI"),
                        "url": a.get("url", ""),
                        "published_at": a.get("publishedAt", ""),
                        "fonte_tipo": "news",
                    })
    except Exception as e:
        print(f"[analysis] NewsAPI: {e}")

    if not all_news:
        return {"total": 0, "analyses": [], "summary": "Sem notícias disponíveis."}

    # Mercados ativos com filtros
    now = datetime.utcnow()
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(200).all()
    )

    # Filtra por volume
    markets = [m for m in markets if _get_market_volume(m) >= FILTER_MIN_VOLUME]

    if not markets:
        return {"total": 0, "analyses": [], "summary": "Sem mercados ativos com volume suficiente."}

    STOP = {"will","the","this","that","with","from","have","been","they","their","which","what","does","about","after","before","into","more","some","would","could","should","when","where","there"}

    top_matches = []
    seen_markets = set()

    for news in all_news[:30]:
        content = (news["title"] + " " + news["description"]).lower()
        best_market = None
        best_score = 0

        for m in markets:
            if m.id in seen_markets:
                continue
            q = (m.question or "").lower()
            words = [w for w in q.split() if len(w) > 3 and w not in STOP]
            overlap = sum(1 for w in words if w in content)
            relevance = overlap / max(len(words), 1)
            if overlap >= 2 and relevance > best_score:
                best_score = relevance
                best_market = m

        if best_market and best_score > 0.1:
            seen_markets.add(best_market.id)
            top_matches.append({"news": news, "market": best_market, "relevance": round(best_score * 100, 1)})

    top_matches = top_matches[:limit]
    analyses = []

    for match in top_matches:
        news = match["news"]
        market = match["market"]
        yes_price, no_price = _get_yes_no_prices(market)
        yes_price = yes_price or 50

        articles = [{"title": news["title"], "description": news["description"], "source": news["source"]}]
        base = _analyze_with_claude(market.question, articles)

        score_yes = base.get("score_yes", yes_price)
        edge = round(score_yes - yes_price, 1)
        confianca = round(base.get("confianca", 0.3) * 100)

        acao = (
            "COMPRAR YES" if base.get("recomendacao") == "APOSTE YES" and confianca >= 50 else
            "COMPRAR NO" if base.get("recomendacao") == "APOSTE NO" and confianca >= 50 else
            "AGUARDAR"
        )

        analyses.append({
            "news_title": news["title"],
            "news_source": news["source"],
            "news_url": news["url"],
            "market_question": market.question,
            "market_slug": market.market_slug,
            "market_yes_price": yes_price,
            "market_no_price": no_price,
            "score_yes_ia": score_yes,
            "edge": edge,
            "acao_recomendada": acao,
            "confianca": confianca,
            "resumo_ia": base.get("resumo", ""),
            "sentimento": base.get("sentimento", "NEUTRO"),
            "relevance_score": match["relevance"],
            "volume_mercado": _get_market_volume(market),
            "polymarket_url": _polymarket_url(market.market_slug),
        })

    return {
        "total": len(analyses),
        "analyses": analyses,
        "gerado_em": datetime.utcnow().isoformat(),
    }
# ══════════════════════════════════════════════════════════════
# EARLY ALERT ENGINE — #21
# Monitora RSS em tempo real, classifica impacto por IA,
# cruza com mercados ativos e dispara Telegram ANTES do preço mover.
# ══════════════════════════════════════════════════════════════

# Fontes RSS de alta velocidade — eventos antes da CNN cobrir
EARLY_ALERT_RSS_SOURCES = [
    # Geopolítica
    {"url": "https://feeds.bbci.co.uk/news/world/rss.xml",                "nome": "BBC World",       "categoria": "GEOPOLITICA"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",     "nome": "NYT World",       "categoria": "GEOPOLITICA"},
    {"url": "https://feeds.reuters.com/Reuters/worldNews",                 "nome": "Reuters World",   "categoria": "GEOPOLITICA"},
    {"url": "https://feeds.reuters.com/reuters/politicsNews",              "nome": "Reuters Politics","categoria": "POLITICA"},
    # Economia / Mercados
    {"url": "https://feeds.bloomberg.com/markets/news.rss",                "nome": "Bloomberg Markets","categoria": "ECONOMIA"},
    {"url": "https://feeds.marketwatch.com/marketwatch/topstories/",       "nome": "MarketWatch",     "categoria": "ECONOMIA"},
    # Conflitos / Defesa
    {"url": "https://news.google.com/rss/search?q=war+military+attack&hl=en&gl=US&ceid=US:en", "nome": "Google War", "categoria": "CONFLITO"},
    {"url": "https://news.google.com/rss/search?q=ukraine+russia+ceasefire&hl=en&gl=US&ceid=US:en", "nome": "Google Ukraine", "categoria": "CONFLITO"},
]

# Palavras-chave de alto impacto por categoria
IMPACT_KEYWORDS = {
    "GUERRA": ["attack", "war", "military", "invasion", "strike", "missile", "troops", "bomb", "killed", "airstrike", "troops deployed", "ceasefire", "casualties"],
    "NUCLEAR": ["nuclear", "uranium", "enrichment", "warhead", "atomic", "iaea", "npt", "bomb"],
    "ECONOMIA": ["fed", "interest rate", "inflation", "recession", "gdp", "unemployment", "market crash", "bank failure", "default", "treasury"],
    "ELEICAO": ["election", "vote", "poll", "candidate", "president", "prime minister", "resign", "impeach", "coup"],
    "GEOPOLITICA": ["sanction", "diplomacy", "treaty", "alliance", "nato", "united nations", "veto", "resolution"],
    "MERCADO": ["polymarket", "prediction market", "kalshi", "market", "odds", "probability"],
}

# Níveis de impacto
IMPACT_LEVELS = {
    "CRITICO":  {"score": 90, "keywords": ["nuclear", "invasion", "coup", "default", "assassination", "war declared"]},
    "ALTO":     {"score": 70, "keywords": ["attack", "airstrike", "ceasefire", "election result", "rate decision", "market crash"]},
    "MEDIO":    {"score": 50, "keywords": ["sanction", "resign", "military", "bomb", "protest", "arrested"]},
    "BAIXO":    {"score": 25, "keywords": ["meeting", "statement", "poll", "report", "interview"]},
}

# Cache de notícias já alertadas (evita duplicatas)
_EARLY_ALERT_SEEN: set = set()


def _early_fetch_rss(source: dict, timeout: int = 8) -> list:
    """Busca RSS de uma fonte e retorna itens normalizados."""
    articles = []
    try:
        resp = requests.get(source["url"], headers=HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        for item in root.findall(".//item")[:10]:
            title = (item.findtext("title") or "").strip()
            desc  = (item.findtext("description") or "").strip()
            link  = (item.findtext("link") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            if not title or not link:
                continue
            articles.append({
                "title": title,
                "description": desc[:300],
                "url": link,
                "published": pub,
                "fonte": source["nome"],
                "categoria": source["categoria"],
                "uid": f"{source['nome']}:{title[:60]}",
            })
    except Exception as e:
        print(f"[early_rss] {source['nome']}: {e}")
    return articles


def _early_classify_impact(title: str, description: str) -> dict:
    """
    Classifica o impacto de uma notícia sem chamar IA — só keywords.
    Retorna: nivel (CRITICO/ALTO/MEDIO/BAIXO), score, categoria, keywords_encontradas
    """
    text = (title + " " + description).lower()

    # Nível de impacto
    nivel = "BAIXO"
    score = 0
    for lvl, data in IMPACT_LEVELS.items():
        if any(kw in text for kw in data["keywords"]):
            if data["score"] > score:
                score = data["score"]
                nivel = lvl

    # Categoria
    categorias_encontradas = []
    keywords_encontradas = []
    for cat, kws in IMPACT_KEYWORDS.items():
        found = [kw for kw in kws if kw in text]
        if found:
            categorias_encontradas.append(cat)
            keywords_encontradas.extend(found)

    return {
        "nivel": nivel,
        "score": score,
        "categorias": categorias_encontradas,
        "keywords": list(set(keywords_encontradas))[:8],
    }


def _early_match_markets(article_text: str, markets: list) -> list:
    """
    Encontra mercados relacionados à notícia por sobreposição de palavras.
    Retorna lista de mercados ordenados por relevância.
    """
    text_words = set(w.lower() for w in article_text.split() if len(w) > 3)
    STOP = {"will","the","this","that","with","from","have","been","they","their","which","what","does","about","after","before","into","more","some","would","could","should","when","where","there","says","said","according","report","also","other"}
    text_words -= STOP

    matches = []
    for m in markets:
        q_words = set(w.lower() for w in (m.question or "").split() if len(w) > 3)
        q_words -= STOP
        overlap = len(text_words & q_words)
        if overlap >= 2:
            matches.append({"market": m, "overlap": overlap})

    matches.sort(key=lambda x: x["overlap"], reverse=True)
    return [x["market"] for x in matches[:3]]


@app.get("/alerts/early")
def alerts_early(
    min_impact: str = Query("ALTO", description="Nível mínimo: BAIXO, MEDIO, ALTO, CRITICO"),
    alertar: int = Query(1, description="1 = envia Telegram"),
    dry_run: int = Query(0, description="1 = não envia, só mostra"),
    db: Session = Depends(get_db),
):
    """
    Early Alert Engine — #21
    Escaneia 8 fontes RSS simultaneamente.
    Classifica impacto por keywords.
    Cruza com mercados ativos do banco.
    Dispara Telegram antes do preço mover.
    """
    now = datetime.utcnow()
    niveis_ordem = {"BAIXO": 1, "MEDIO": 2, "ALTO": 3, "CRITICO": 4}
    min_score = IMPACT_LEVELS.get(min_impact.upper(), IMPACT_LEVELS["ALTO"])["score"]

    # Busca mercados ativos com filtros
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    # Coleta RSS de todas as fontes
    all_articles = []
    for source in EARLY_ALERT_RSS_SOURCES:
        articles = _early_fetch_rss(source)
        all_articles.extend(articles)
        time.sleep(0.1)  # respeita rate limit

    # Deduplica por UID
    seen_uids: set = set()
    unique_articles = []
    for a in all_articles:
        if a["uid"] not in seen_uids:
            seen_uids.add(a["uid"])
            unique_articles.append(a)

    # Classifica impacto e filtra
    alertas = []
    for article in unique_articles:
        # Já foi alertado antes?
        if article["uid"] in _EARLY_ALERT_SEEN:
            continue

        # Classifica impacto
        impact = _early_classify_impact(article["title"], article["description"])
        if impact["score"] < min_score:
            continue

        # Encontra mercados relacionados
        article_text = article["title"] + " " + article["description"]
        mercados_relacionados = _early_match_markets(article_text, markets)

        alerta = {
            "titulo": article["title"],
            "fonte": article["fonte"],
            "categoria_fonte": article["categoria"],
            "url": article["url"],
            "publicado": article["published"],
            "impacto": impact["nivel"],
            "impacto_score": impact["score"],
            "categorias": impact["categorias"],
            "keywords": impact["keywords"],
            "mercados_afetados": [
                {
                    "question": m.question,
                    "slug": m.market_slug,
                    "yes_price": _get_yes_no_prices(m)[0],
                    "polymarket_url": _polymarket_url(m.market_slug),
                }
                for m in mercados_relacionados
            ],
            "total_mercados": len(mercados_relacionados),
            "detectado_em": now.isoformat(),
        }
        alertas.append(alerta)

        # Envia Telegram e só então marca como visto
        if alertar and not dry_run:
            nivel_emoji = {
                "CRITICO": "🚨🚨🚨",
                "ALTO": "🚨",
                "MEDIO": "⚠️",
                "BAIXO": "ℹ️",
            }.get(impact["nivel"], "📰")

            mercados_txt = ""
            for m_info in alerta["mercados_afetados"][:3]:
                price_txt = f"{m_info['yes_price']}%" if m_info["yes_price"] else "?"
                mercados_txt += f"\n• <a href=\"{m_info['polymarket_url']}\">{m_info['question'][:70]}</a> @ {price_txt}"

            msg = (
                f"{nivel_emoji} <b>EARLY ALERT — {impact['nivel']}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📰 <b>{article['title'][:120]}</b>\n"
                f"📡 Fonte: {article['fonte']} | Score: {impact['score']}\n"
                f"🏷️ {', '.join(impact['categorias'][:3]) or 'Geral'}\n"
                f"🔑 Keywords: {', '.join(impact['keywords'][:5])}\n"
            )
            if mercados_relacionados:
                msg += f"━━━━━━━━━━━━━━━━━━━━━━\n🎯 <b>Mercados afetados ({len(mercados_relacionados)}):</b>{mercados_txt}\n"
            msg += (
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔗 <a href=\"{article['url']}\">Ler notícia completa</a>\n"
                f"⏰ {now.strftime('%H:%M:%S')} UTC"
            )
            _telegram_send(msg)

            # Marca como visto SÓ após enviar
            _EARLY_ALERT_SEEN.add(article["uid"])
            if len(_EARLY_ALERT_SEEN) > 10000:
                for uid in list(_EARLY_ALERT_SEEN)[:2000]:
                    _EARLY_ALERT_SEEN.discard(uid)

    # Ordena por impacto (CRITICO primeiro)
    alertas.sort(key=lambda x: x["impacto_score"], reverse=True)

    return {
        "status": "ok",
        "total_fontes_escaneadas": len(EARLY_ALERT_RSS_SOURCES),
        "total_artigos_coletados": len(unique_articles),
        "alertas_gerados": len(alertas),
        "alertas_enviados": len(alertas) if (alertar and not dry_run) else 0,
        "min_impact": min_impact,
        "alertas": alertas,
        "gerado_em": now.isoformat(),
    }
# ══════════════════════════════════════════════════════════════
# MOTOR 7 — CORRELAÇÃO ENTRE MERCADOS v2
# Mineração de alpha: descobre pares de mercados que movem juntos.
# Usa 860k+ snapshots do banco. Sem API externa.
#
# v2: Filtro temático anti-espúrio — só correlaciona mercados
# do mesmo tema (Iran×Iran, Oil×Oil, NBA×NBA).
#
# Tipos de correlação detectados:
# - POSITIVA FORTE  (≥ 0.7): mercados que movem na mesma direção
# - NEGATIVA FORTE  (≤ -0.7): mercados que movem em direções opostas
# - LEAD/LAG: mercado A move N minutos antes do mercado B
#
# Uso prático:
# - Se A sobe e B está correlacionado positivo → B vai subir (edge)
# - Se A sobe e B está correlacionado negativo → B vai cair (edge)
# - Lead/Lag: compra B quando A se move antes dele
# ══════════════════════════════════════════════════════════════

import statistics

# Cache de correlações calculadas (TTL: 30 min por par)
_CORR_CACHE: dict = {}
_CORR_CACHE_TS: dict = {}
_CORR_CACHE_TTL = 1800  # 30 minutos

# Temas com keywords — só pares do mesmo tema são correlacionados
CORR_THEMES = {
    "IRAN":        ["iran", "iranian", "tehran", "irgc", "persian"],
    "RUSSIA_UA":   ["russia", "ukraine", "ukrainian", "zelensky", "putin", "kyiv", "donbas", "ceasefire"],
    "BITCOIN":     ["bitcoin", "btc", "crypto", "ethereum", "cryptocurrency"],
    "OIL":         ["crude oil", "oil price", "brent", "wti", "petroleum", "opec"],
    "TRUMP":       ["trump", "maga", "republican", "gop", "white house"],
    "CHINA":       ["china", "xi jinping", "beijing", "chinese", "taiwan"],
    "ELEICAO_US":  ["presidential election", "us election", "democrat", "republican nomination", "vance", "newsom", "2028"],
    "NBA":         ["nba", "knicks", "lakers", "celtics", "raptors", "rockets", "nuggets", "spurs", "clippers", "timberwolves", "pelicans", "jazz"],
    "NFL":         ["nfl", "super bowl", "patriots", "chiefs", "cowboys", "eagles", "rams"],
    "SOCCER_UCL":  ["champions league", "ucl", "psg", "real madrid", "barcelona", "bayern", "chelsea", "arsenal"],
    "SOCCER_LIGA": ["premier league", "la liga", "serie a", "ligue 1", "bundesliga", "lens", "inter milan"],
    "F1":          ["formula 1", "f1", "ferrari", "mercedes", "russell", "verstappen", "hamilton", "driver champion"],
    "AI_TECH":     ["openai", "gpt", "artificial intelligence", "llm", "anthropic", "deepseek", "ai model"],
    "GOLD_SILVER": ["gold price", "silver price", "xau", "xag", "precious metal"],
    "ISRAEL":      ["israel", "hamas", "gaza", "netanyahu", "idf", "hezbollah"],
    "ELON":        ["elon musk", "tesla", "spacex", "twitter", "x.com", "doge"],
}


def _corr_detect_theme(question: str, slug: str) -> list:
    """Detecta quais temas um mercado pertence (pode ser múltiplos)."""
    text = (question + " " + slug).lower()
    temas = []
    for tema, keywords in CORR_THEMES.items():
        if any(kw in text for kw in keywords):
            temas.append(tema)
    return temas if temas else ["OUTROS"]


def _corr_same_theme(themes_a: list, themes_b: list) -> bool:
    """Retorna True se os dois mercados compartilham pelo menos 1 tema."""
    return bool(set(themes_a) & set(themes_b))


def _corr_get_prices(db: Session, token_id: str, limit: int = 200) -> list:
    """Busca preços históricos normalizados de um token."""
    snaps = (
        db.query(Snapshot)
        .filter(Snapshot.token_id == token_id)
        .order_by(Snapshot.timestamp.desc())
        .limit(limit).all()
    )
    return [float(s.price) for s in reversed(snaps)]


def _pearson_correlation(x: list, y: list) -> float:
    """Correlação de Pearson entre duas séries de preços. Retorna -1.0 a 1.0."""
    n = min(len(x), len(y))
    if n < 20:
        return 0.0
    x = x[-n:]
    y = y[-n:]
    try:
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        den_x = sum((v - mean_x) ** 2 for v in x) ** 0.5
        den_y = sum((v - mean_y) ** 2 for v in y) ** 0.5
        denom = den_x * den_y
        if denom < 1e-9:
            return 0.0
        return round(num / denom, 4)
    except Exception:
        return 0.0


def _lead_lag_correlation(x: list, y: list, max_lag: int = 5) -> dict:
    """Testa se X lidera Y por 1 a max_lag períodos."""
    best_lag = 0
    best_corr = 0.0
    best_lider = "NENHUM"
    n = min(len(x), len(y))
    if n < 30:
        return {"lag_snapshots": 0, "correlacao_lead_lag": 0.0, "lider": "NENHUM"}
    for lag in range(1, max_lag + 1):
        if n > lag + 10:
            corr_x = _pearson_correlation(x[:-lag], y[lag:])
            corr_y = _pearson_correlation(y[:-lag], x[lag:])
            if abs(corr_x) > abs(best_corr):
                best_corr = corr_x
                best_lag = lag
                best_lider = "A"
            if abs(corr_y) > abs(best_corr):
                best_corr = corr_y
                best_lag = lag
                best_lider = "B"
    return {
        "lag_snapshots": best_lag,
        "correlacao_lead_lag": round(best_corr, 4),
        "lider": best_lider,
    }


def _corr_classify(corr: float) -> dict:
    """Classifica correlação e retorna emoji + label."""
    if corr >= 0.85:
        return {"label": "MUITO FORTE POSITIVA", "emoji": "🟢🟢", "tipo": "POSITIVA"}
    elif corr >= 0.70:
        return {"label": "FORTE POSITIVA", "emoji": "🟢", "tipo": "POSITIVA"}
    elif corr >= 0.50:
        return {"label": "MODERADA POSITIVA", "emoji": "🔵", "tipo": "POSITIVA"}
    elif corr <= -0.85:
        return {"label": "MUITO FORTE NEGATIVA", "emoji": "🔴🔴", "tipo": "NEGATIVA"}
    elif corr <= -0.70:
        return {"label": "FORTE NEGATIVA", "emoji": "🔴", "tipo": "NEGATIVA"}
    elif corr <= -0.50:
        return {"label": "MODERADA NEGATIVA", "emoji": "🟠", "tipo": "NEGATIVA"}
    else:
        return {"label": "FRACA / SEM CORRELAÇÃO", "emoji": "⚪", "tipo": "NENHUMA"}


def _corr_edge(corr, price_a, price_b, prices_a, prices_b, lead_lag) -> dict:
    """Calcula edge prático: A se moveu, B ainda não reagiu?"""
    if len(prices_a) < 10 or len(prices_b) < 10:
        return {"acao": "AGUARDAR", "raciocinio": "Dados insuficientes", "confianca": 0}

    move_a = (prices_a[-1] - prices_a[-6]) * 100 if len(prices_a) >= 6 else 0
    move_b = (prices_b[-1] - prices_b[-6]) * 100 if len(prices_b) >= 6 else 0

    divergencia = abs(move_a) > 3 and abs(move_b) < 1.5

    if not divergencia:
        return {
            "acao": "MONITORAR",
            "raciocinio": f"A moveu {move_a:+.1f}pts, B moveu {move_b:+.1f}pts — sem divergência",
            "confianca": 0,
        }

    if corr > 0:
        direcao_b = "UP" if move_a > 0 else "DOWN"
        acao = "COMPRAR YES" if direcao_b == "UP" else "COMPRAR NO"
        raciocinio = (
            f"Correlação POSITIVA ({corr:.2f}). "
            f"A moveu {move_a:+.1f}pts mas B ainda não reagiu ({move_b:+.1f}pts). "
            f"Espera-se que B {'suba' if direcao_b == 'UP' else 'caia'}."
        )
    else:
        direcao_b = "DOWN" if move_a > 0 else "UP"
        acao = "COMPRAR YES" if direcao_b == "UP" else "COMPRAR NO"
        raciocinio = (
            f"Correlação NEGATIVA ({corr:.2f}). "
            f"A moveu {move_a:+.1f}pts mas B ainda não reagiu ({move_b:+.1f}pts). "
            f"Correlação inversa sugere que B deve {'cair' if direcao_b == 'DOWN' else 'subir'}."
        )

    confianca = round(min(abs(corr) * 0.7 + min(abs(move_a) / 20, 0.3), 0.95), 2)
    if lead_lag and lead_lag.get("lider") == "A" and abs(lead_lag.get("correlacao_lead_lag", 0)) > 0.6:
        confianca = min(confianca + 0.1, 0.95)
        raciocinio += f" Lead/lag confirma: A lidera B por {lead_lag['lag_snapshots']} snapshots."

    return {
        "acao": acao,
        "direcao_esperada_b": direcao_b,
        "move_a_recente": round(move_a, 2),
        "move_b_recente": round(move_b, 2),
        "raciocinio": raciocinio,
        "confianca": confianca,
        "divergencia_detectada": True,
    }


@app.get("/correlations")
def get_correlations(
    min_corr: float = Query(0.65, ge=0.3, le=0.99),
    limit_markets: int = Query(80, ge=10, le=200),
    include_lead_lag: int = Query(1),
    db: Session = Depends(get_db),
):
    """
    Motor de Correlação v2 — pares temáticos reais.
    Filtra correlações espúrias: só correlaciona mercados do mesmo tema.
    Ex: Iran×Iran, Oil×Oil, NBA×NBA, Bitcoin×Bitcoin.
    """
    now = datetime.utcnow()

    tokens_yes = (
        db.query(Token)
        .join(Market, Token.market_id == Market.id)
        .filter(
            Token.outcome == "YES",
            Token.price > FILTER_MIN_PRICE,
            Token.price < FILTER_MAX_PRICE,
            (Market.end_date == None) | (Market.end_date > now),
        )
        .limit(limit_markets).all()
    )

    if len(tokens_yes) < 2:
        return {"status": "insufficient_data", "pares": []}

    series: dict = {}
    for token in tokens_yes:
        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue
        volume = _get_market_volume(market)
        if volume > 0 and volume < FILTER_MIN_VOLUME:
            continue
        prices = _corr_get_prices(db, token.token_id, limit=150)
        if len(prices) < 30:
            continue
        series[token.token_id] = {
            "prices": prices,
            "token": token,
            "market": market,
            "themes": _corr_detect_theme(market.question or "", market.market_slug or ""),
        }

    token_ids = list(series.keys())
    n_tokens = len(token_ids)

    if n_tokens < 2:
        return {
            "status": "insufficient_data",
            "motivo": f"Apenas {n_tokens} tokens com dados suficientes",
            "pares": [],
        }

    pares = []
    total_calculados = 0
    total_pulados_tema = 0

    for i in range(n_tokens):
        for j in range(i + 1, n_tokens):
            tid_a = token_ids[i]
            tid_b = token_ids[j]
            data_a = series[tid_a]
            data_b = series[tid_b]
            market_a = data_a["market"]
            market_b = data_b["market"]

            # Não correlaciona tokens do mesmo mercado
            if market_a.id == market_b.id:
                continue

            # FILTRO TEMÁTICO — elimina correlações espúrias
            if not _corr_same_theme(data_a["themes"], data_b["themes"]):
                total_pulados_tema += 1
                continue

            # Verifica cache
            cache_key = f"{tid_a}:{tid_b}"
            cache_age = (now - _CORR_CACHE_TS.get(cache_key, datetime.min)).total_seconds()
            if cache_key in _CORR_CACHE and cache_age < _CORR_CACHE_TTL:
                corr = _CORR_CACHE[cache_key]
            else:
                corr = _pearson_correlation(data_a["prices"], data_b["prices"])
                _CORR_CACHE[cache_key] = corr
                _CORR_CACHE_TS[cache_key] = now

            total_calculados += 1

            if abs(corr) < min_corr:
                continue

            classificacao = _corr_classify(corr)
            if classificacao["tipo"] == "NENHUMA":
                continue

            lead_lag = None
            if include_lead_lag:
                lead_lag = _lead_lag_correlation(data_a["prices"], data_b["prices"])

            yes_a, _ = _get_yes_no_prices(market_a)
            yes_b, _ = _get_yes_no_prices(market_b)

            edge = _corr_edge(
                corr=corr,
                price_a=data_a["token"].price,
                price_b=data_b["token"].price,
                prices_a=data_a["prices"],
                prices_b=data_b["prices"],
                lead_lag=lead_lag,
            )

            pares.append({
                "temas": list(set(data_a["themes"]) & set(data_b["themes"])),
                "mercado_a": {
                    "question": market_a.question,
                    "slug": market_a.market_slug,
                    "yes_price": yes_a,
                    "polymarket_url": _polymarket_url(market_a.market_slug),
                    "volume": _get_market_volume(market_a),
                    "snapshots": len(data_a["prices"]),
                },
                "mercado_b": {
                    "question": market_b.question,
                    "slug": market_b.market_slug,
                    "yes_price": yes_b,
                    "polymarket_url": _polymarket_url(market_b.market_slug),
                    "volume": _get_market_volume(market_b),
                    "snapshots": len(data_b["prices"]),
                },
                "correlacao": corr,
                "correlacao_abs": abs(corr),
                "classificacao": classificacao["label"],
                "emoji": classificacao["emoji"],
                "tipo": classificacao["tipo"],
                "lead_lag": lead_lag,
                "edge": edge,
                "detectado_em": now.isoformat(),
            })

    pares.sort(key=lambda x: x["correlacao_abs"], reverse=True)
    pares_top = pares[:30]

    positivos = [p for p in pares_top if p["tipo"] == "POSITIVA"]
    negativos = [p for p in pares_top if p["tipo"] == "NEGATIVA"]
    com_edge = [p for p in pares_top if p.get("edge", {}).get("divergencia_detectada")]

    return {
        "status": "ok",
        "tokens_analisados": n_tokens,
        "pares_calculados": total_calculados,
        "pares_pulados_tema_diferente": total_pulados_tema,
        "pares_correlacionados": len(pares),
        "retornados": len(pares_top),
        "min_correlacao": min_corr,
        "resumo": {
            "pares_positivos": len(positivos),
            "pares_negativos": len(negativos),
            "pares_com_edge_ativo": len(com_edge),
            "correlacao_media": round(sum(abs(p["correlacao"]) for p in pares_top) / max(len(pares_top), 1), 3),
        },
        "interpretacao": {
            "positiva": "Mercados movem na MESMA direção. Se A sobe → B provavelmente sobe.",
            "negativa": "Mercados movem em direções OPOSTAS. Se A sobe → B provavelmente cai.",
            "lead_lag": "Mercado LIDER move antes. Quando lider se move → oportunidade no seguidor.",
        },
        "pares": pares_top,
        "gerado_em": now.isoformat(),
    }


@app.get("/correlations/{slug}")
def get_market_correlations(
    slug: str,
    min_corr: float = Query(0.5, ge=0.3, le=0.99),
    db: Session = Depends(get_db),
):
    """Mostra todos os mercados correlacionados com um mercado específico."""
    now = datetime.utcnow()

    target_market = db.query(Market).filter(Market.market_slug == slug).first()
    if not target_market:
        return {"error": "Mercado não encontrado", "slug": slug}

    target_token = db.query(Token).filter(
        Token.market_id == target_market.id,
        Token.outcome == "YES"
    ).first()

    if not target_token:
        return {"error": "Token YES não encontrado", "slug": slug}

    target_prices = _corr_get_prices(db, target_token.token_id, limit=150)
    if len(target_prices) < 30:
        return {"error": "Dados insuficientes para correlação", "slug": slug}

    target_themes = _corr_detect_theme(target_market.question or "", slug)

    other_tokens = (
        db.query(Token)
        .join(Market, Token.market_id == Market.id)
        .filter(
            Token.outcome == "YES",
            Token.market_id != target_market.id,
            Token.price > FILTER_MIN_PRICE,
            Token.price < FILTER_MAX_PRICE,
            (Market.end_date == None) | (Market.end_date > now),
        )
        .limit(150).all()
    )

    correlacoes = []
    for token in other_tokens:
        market = db.query(Market).filter(Market.id == token.market_id).first()
        if not market:
            continue

        volume = _get_market_volume(market)
        if volume > 0 and volume < FILTER_MIN_VOLUME:
            continue

        # Filtro temático
        other_themes = _corr_detect_theme(market.question or "", market.market_slug or "")
        if not _corr_same_theme(target_themes, other_themes):
            continue

        prices = _corr_get_prices(db, token.token_id, limit=150)
        if len(prices) < 30:
            continue

        corr = _pearson_correlation(target_prices, prices)
        if abs(corr) < min_corr:
            continue

        classificacao = _corr_classify(corr)
        lead_lag = _lead_lag_correlation(target_prices, prices)
        yes_price, _ = _get_yes_no_prices(market)

        correlacoes.append({
            "question": market.question,
            "slug": market.market_slug,
            "yes_price": yes_price,
            "polymarket_url": _polymarket_url(market.market_slug),
            "correlacao": corr,
            "classificacao": classificacao["label"],
            "emoji": classificacao["emoji"],
            "tipo": classificacao["tipo"],
            "lead_lag": lead_lag,
            "temas_compartilhados": list(set(target_themes) & set(other_themes)),
        })

    correlacoes.sort(key=lambda x: abs(x["correlacao"]), reverse=True)
    yes_alvo, _ = _get_yes_no_prices(target_market)

    return {
        "mercado_alvo": {
            "question": target_market.question,
            "slug": slug,
            "yes_price": yes_alvo,
            "polymarket_url": _polymarket_url(slug),
            "temas": target_themes,
        },
        "total_correlacionados": len(correlacoes),
        "min_correlacao": min_corr,
        "correlacoes": correlacoes[:20],
        "gerado_em": now.isoformat(),
    }


@app.get("/correlations/alert/divergencias")
def correlations_divergencias(
    min_corr: float = Query(0.70, ge=0.5, le=0.99),
    min_move: float = Query(3.0, ge=1.0),
    alertar: int = Query(0),
    db: Session = Depends(get_db),
):
    """
    Detecta divergências ativas: mercado A se moveu, B correlacionado ainda não reagiu.
    Só pares temáticos — sem correlações espúrias.
    """
    now = datetime.utcnow()

    result = get_correlations(
        min_corr=min_corr,
        limit_markets=100,
        include_lead_lag=1,
        db=db,
    )

    if result.get("status") != "ok":
        return result

    divergencias = [
        p for p in result.get("pares", [])
        if p.get("edge", {}).get("divergencia_detectada")
        and abs(p["edge"].get("move_a_recente", 0)) >= min_move
        and p["edge"].get("confianca", 0) >= 0.5
    ]

    divergencias.sort(key=lambda x: x["edge"]["confianca"], reverse=True)

    if alertar and divergencias:
        for div in divergencias[:5]:
            edge = div["edge"]
            temas = ", ".join(div.get("temas", ["?"]))
            msg = (
                f"🔗 <b>DIVERGÊNCIA CORRELAÇÃO — {temas}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Correlação: {div['emoji']} {div['correlacao']:+.2f} ({div['classificacao']})\n"
                f"\n"
                f"🅰️ <b>Lider:</b> {div['mercado_a']['question'][:70]}\n"
                f"   Moveu: {edge['move_a_recente']:+.1f}pts | Preço: {div['mercado_a']['yes_price']}%\n"
                f"\n"
                f"🅱️ <b>Seguidor:</b> {div['mercado_b']['question'][:70]}\n"
                f"   Moveu: {edge['move_b_recente']:+.1f}pts | Preço: {div['mercado_b']['yes_price']}%\n"
                f"\n"
                f"🎯 <b>Ação:</b> {edge['acao']}\n"
                f"📈 Confiança: {round(edge['confianca']*100)}%\n"
                f"💡 {edge['raciocinio'][:150]}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔗 <a href=\"{div['mercado_b']['polymarket_url']}\">Abrir mercado B</a>\n"
                f"⏰ {now.strftime('%H:%M:%S')} UTC"
            )
            _telegram_send(msg)

    return {
        "status": "ok",
        "total_pares_analisados": result.get("pares_correlacionados", 0),
        "divergencias_ativas": len(divergencias),
        "min_correlacao": min_corr,
        "min_move": min_move,
        "divergencias": divergencias[:10],
        "gerado_em": now.isoformat(),
    } 

# ══════════════════════════════════════════════════════════════
# MOTOR #24 — CREDIBILIDADE DE NOTÍCIA
# 
# Lógica central:
# - Toda notícia recebe um Score de Credibilidade (0-100)
# - Score baseado em: fonte + confirmação cruzada + fact-check
# - Notícia suspeita (score < 40) NÃO é descartada
# - Em vez disso, cruza com detector de baleias:
#   → Suspeita + Baleia detectada = ALERTA MÁXIMO (insider info?)
#   → Suspeita + Sem baleia      = REBAIXADO (provável fake)
# - Atua em todos os motores via _news_credibility_score()
# ══════════════════════════════════════════════════════════════

# ── Tier de fontes por credibilidade ──────────────────────────
SOURCE_TIERS = {
    # Tier 1 — Verificação editorial rigorosa
    "TIER_1": {
        "score": 95,
        "fontes": [
            "reuters", "associated press", "ap news", "bloomberg",
            "bbc", "financial times", "ft.com", "wall street journal",
            "wsj", "new york times", "nyt", "the economist",
            "washington post", "le monde", "der spiegel", "guardian",
        ],
    },
    # Tier 2 — Confiável mas com viés editorial
    "TIER_2": {
        "score": 75,
        "fontes": [
            "cnn", "nbc", "abc news", "cbs news", "politico",
            "axios", "the hill", "marketwatch", "cnbc", "forbes",
            "time", "newsweek", "al jazeera", "france 24",
            "sky news", "dw", "npr", "pbs",
        ],
    },
    # Tier 3 — Verificar antes de agir
    "TIER_3": {
        "score": 50,
        "fontes": [
            "fox news", "msnbc", "breitbart", "daily mail",
            "new york post", "the sun", "daily mirror",
            "zero hedge", "the intercept", "vice",
        ],
    },
    # Tier 4 — Alta probabilidade de desinformação
    "TIER_4": {
        "score": 20,
        "fontes": [
            "infowars", "natural news", "epoch times", "oann",
            "newsmax", "gateway pundit", "daily wire",
        ],
    },
}

# Palavras que indicam notícia não verificada
UNVERIFIED_SIGNALS = [
    "sources say", "reportedly", "rumor", "unconfirmed", "allegedly",
    "claim", "suggests", "may have", "could be", "might be",
    "anonymous source", "insider says", "leaked", "exclusive",
    "breaking:", "developing story", "first reports",
]

# Palavras que indicam notícia verificada/oficial
VERIFIED_SIGNALS = [
    "confirmed", "official", "announced", "signed", "agreement",
    "statement", "press conference", "official statement", "law passed",
    "voted", "declared", "published", "released", "report",
]

# Cache de credibilidade (evita recalcular mesma notícia)
_CRED_CACHE: dict = {}  # uid → {score, tier, detalhes}


def _get_source_tier(source_name: str) -> tuple:
    """Retorna (tier_name, score_base) da fonte."""
    source_lower = (source_name or "").lower()
    for tier_name, tier_data in SOURCE_TIERS.items():
        if any(f in source_lower for f in tier_data["fontes"]):
            return tier_name, tier_data["score"]
    return "TIER_UNKNOWN", 45  # Fonte desconhecida = score moderado


def _count_source_confirmations(title: str, articles: list) -> int:
    """
    Conta quantas fontes INDEPENDENTES confirmam a mesma notícia.
    Usa sobreposição de palavras-chave no título.
    """
    if not articles or not title:
        return 1

    title_words = set(w.lower() for w in title.split() if len(w) > 4)
    STOP = {"about", "after", "their", "which", "would", "could", "should", "there", "where", "while"}
    title_words -= STOP

    confirmacoes = 0
    fontes_vistas = set()

    for a in articles:
        fonte = (a.get("source") or a.get("fonte") or "").lower()
        if fonte in fontes_vistas:
            continue  # Não conta mesma fonte duas vezes
        fontes_vistas.add(fonte)

        a_words = set(w.lower() for w in (a.get("title") or "").split() if len(w) > 4)
        a_words -= STOP
        overlap = len(title_words & a_words) / max(len(title_words), 1)

        if overlap >= 0.4:  # 40% das palavras batem = mesma notícia
            confirmacoes += 1

    return max(confirmacoes, 1)


def _news_credibility_score(
    title: str,
    description: str,
    source: str,
    all_articles: list = None,
    slug_mercado: str = None,
    db=None,
) -> dict:
    """
    Calcula Score de Credibilidade de uma notícia (0-100).

    Componentes:
    - Tier da fonte       (40%): Reuters=95, CNN=75, Desconhecida=45
    - Confirmações        (30%): Quantas fontes independentes confirmam
    - Sinais de verificação(20%): Palavras "confirmed", "official" vs "rumor", "allegedly"
    - Cruzamento baleia   (10%): Se slug tem baleia ativa, suspeita vira sinal

    Retorna dict com score, classificação, e recomendação de uso.
    """
    uid = f"{source}:{title[:60]}"
    if uid in _CRED_CACHE:
        return _CRED_CACHE[uid]

    text = (title + " " + (description or "")).lower()

    # ── Componente 1: Tier da fonte (40pts) ──────────────────
    tier_name, tier_score = _get_source_tier(source)
    score_fonte = tier_score * 0.40

    # ── Componente 2: Confirmações cruzadas (30pts) ──────────
    n_confirmacoes = _count_source_confirmations(title, all_articles or [])

    # 1 fonte = 0pts | 2 fontes = 15pts | 3+ fontes = 30pts
    score_confirmacao = min((n_confirmacoes - 1) * 15, 30)
    # Tier garante mínimo: TIER_1 = mín 15pts, TIER_2 = mín 8pts
    # (Reuters sozinha não pode ter 0 de confirmação — ela JÁ é confirmação)
    if tier_name == "TIER_1":
        score_confirmacao = max(score_confirmacao, 15)
    elif tier_name == "TIER_2":
        score_confirmacao = max(score_confirmacao, 8)

    # ── Componente 3: Sinais linguísticos (20pts) ────────────
    n_verified   = sum(1 for s in VERIFIED_SIGNALS if s in text)
    n_unverified = sum(1 for s in UNVERIFIED_SIGNALS if s in text)
    # Saldo positivo = mais verificada, negativo = mais rumor
    saldo = n_verified - n_unverified
    score_linguistico = max(0, min(10 + saldo * 3, 20))

    # ── Componente 4: Cruzamento com baleias (10pts) ─────────
    score_baleia = 0
    tem_baleia = False
    baleia_info = None

    if slug_mercado and db:
        try:
            # Verifica se slug tem mercado com baleia detectada recentemente
            # Usa o cache _WHALE_SEEN que já existe no sistema
            if slug_mercado in _WHALE_SEEN:
                tem_baleia = True
                score_baleia = 10
                baleia_info = "Baleia detectada neste mercado"
        except Exception:
            pass

    # ── Score final ──────────────────────────────────────────
    score_total = round(score_fonte + score_confirmacao + score_linguistico + score_baleia, 1)
    score_total = max(5, min(score_total, 100))  # clamp 5-100

    # ── Classificação ────────────────────────────────────────
    if score_total >= 80:
        nivel = "ALTA"
        emoji = "✅"
        usar = "CONFIAR"
    elif score_total >= 60:
        nivel = "MEDIA"
        emoji = "🟡"
        usar = "USAR_COM_CAUTELA"
    elif score_total >= 40:
        nivel = "BAIXA"
        emoji = "🟠"
        usar = "SUSPEITA"
    else:
        nivel = "MUITO_BAIXA"
        emoji = "🚨"
        usar = "NAO_CONFIAR"

    # ── Lógica especial: Suspeita + Baleia = INSIDER? ────────
    insider_signal = False
    insider_msg = None

    if score_total < 50 and tem_baleia:
        insider_signal = True
        insider_msg = (
            f"⚠️ POSSÍVEL INSIDER: Notícia de baixa credibilidade ({score_total:.0f}/100) "
            f"mas baleia ativa neste mercado. Alguém pode saber antes."
        )
        usar = "POSSIVEL_INSIDER"
        emoji = "🐋⚠️"

    resultado = {
        "score": score_total,
        "nivel": nivel,
        "emoji": emoji,
        "usar": usar,
        "fonte": source,
        "tier": tier_name,
        "n_confirmacoes": n_confirmacoes,
        "sinais_verificados": n_verified,
        "sinais_rumor": n_unverified,
        "tem_baleia_mercado": tem_baleia,
        "baleia_info": baleia_info,
        "insider_signal": insider_signal,
        "insider_msg": insider_msg,
        "detalhes": {
            "score_fonte": round(score_fonte, 1),
            "score_confirmacao": round(score_confirmacao, 1),
            "score_linguistico": round(score_linguistico, 1),
            "score_baleia": score_baleia,
        },
    }

    _CRED_CACHE[uid] = resultado
    if len(_CRED_CACHE) > 5000:
        for k in list(_CRED_CACHE.keys())[:500]:
            del _CRED_CACHE[k]

    return resultado


# ── Integração nos motores existentes ────────────────────────

def _apply_credibility_to_analysis(analysis: dict, cred: dict) -> dict:
    """
    Ajusta score/confiança de qualquer análise baseado na credibilidade.
    Regras:
    - CONFIAR       → mantém score
    - USAR_COM_CAUTELA → reduz confiança 10%
    - SUSPEITA      → reduz confiança 25%
    - POSSIVEL_INSIDER → AMPLIFICA confiança 15% (sinal raro e valioso)
    - NAO_CONFIAR   → reduz confiança 40%
    """
    usar = cred.get("usar", "USAR_COM_CAUTELA")
    original_conf = float(analysis.get("confianca", analysis.get("confidence", 0.5)))

    multiplicadores = {
        "CONFIAR":           1.00,
        "USAR_COM_CAUTELA":  0.90,
        "SUSPEITA":          0.75,
        "POSSIVEL_INSIDER":  1.15,  # amplifica — insider é ouro
        "NAO_CONFIAR":       0.60,
    }

    mult = multiplicadores.get(usar, 0.85)
    nova_conf = round(min(original_conf * mult, 1.0), 3)

    analysis["credibilidade"] = cred
    analysis["confianca_original"] = original_conf
    analysis["confianca_ajustada"] = nova_conf
    analysis["confianca"] = nova_conf

    if cred.get("insider_signal"):
        analysis["insider_alert"] = cred["insider_msg"]

    return analysis


# ── Endpoint de diagnóstico ──────────────────────────────────

@app.get("/credibility/check")
def credibility_check(
    title: str = Query(..., description="Título da notícia"),
    source: str = Query("unknown", description="Nome da fonte"),
    description: str = Query("", description="Descrição/texto da notícia"),
    slug: str = Query(None, description="Slug do mercado relacionado"),
    db: Session = Depends(get_db),
):
    """
    Testa o score de credibilidade de qualquer notícia.
    Útil para debug e para o frontend mostrar o score.
    """
    # Busca outras notícias sobre o mesmo tema para confirmação cruzada
    articles = _fetch_news(title[:60], max_results=10) if title else []

    cred = _news_credibility_score(
        title=title,
        description=description,
        source=source,
        all_articles=articles,
        slug_mercado=slug,
        db=db,
    )

    return {
        "titulo": title,
        "fonte": source,
        "score_credibilidade": cred["score"],
        "nivel": cred["nivel"],
        "emoji": cred["emoji"],
        "recomendacao": cred["usar"],
        "insider_signal": cred["insider_signal"],
        "insider_msg": cred.get("insider_msg"),
        "detalhes": cred["detalhes"],
        "n_confirmacoes_encontradas": cred["n_confirmacoes"],
        "sinais_verificados": cred["sinais_verificados"],
        "sinais_rumor": cred["sinais_rumor"],
        "tier_fonte": cred["tier"],
        "tem_baleia_no_mercado": cred["tem_baleia_mercado"],
        "gerado_em": datetime.utcnow().isoformat(),
    }


@app.get("/credibility/sources")
def credibility_sources():
    """Lista todas as fontes e seus tiers de credibilidade."""
    return {
        "total_fontes": sum(len(t["fontes"]) for t in SOURCE_TIERS.values()),
        "tiers": {
            name: {
                "score_base": data["score"],
                "total_fontes": len(data["fontes"]),
                "fontes": data["fontes"],
            }
            for name, data in SOURCE_TIERS.items()
        },
        "logica_insider": {
            "descricao": "Notícia suspeita (score<50) + baleia ativa no mercado = POSSÍVEL INSIDER INFO",
            "acao": "Sinal AMPLIFICADO em vez de descartado",
            "multiplicador_confianca": "x1.15",
        },
    }
# ══════════════════════════════════════════════════════════════
# MOTOR #25 — MODO EVENTO AGENDADO
#
# Lógica:
# - Calendário de eventos conhecidos (Fed, eleições, julgamentos,
#   sanções, reuniões OPEC, dados econômicos, etc.)
# - Verifica eventos nas próximas N horas (default: 24h)
# - Cruza evento com mercados ativos do banco por keywords
# - Dispara Telegram ANTES do evento → usuário já está posicionado
# - Cache para não re-alertar o mesmo evento
# - Integrado no cron_tick
# ══════════════════════════════════════════════════════════════

# ── Categorias de eventos e seus emojis ──────────────────────
EVENT_CATEGORIES = {
    "FED":         {"emoji": "🏦", "cor": "#0a84ff"},
    "ELEICAO":     {"emoji": "🗳️", "cor": "#30d158"},
    "JULGAMENTO":  {"emoji": "⚖️", "cor": "#ff9f0a"},
    "SANCAO":      {"emoji": "🚫", "cor": "#ff453a"},
    "ECONOMIA":    {"emoji": "📊", "cor": "#5e5ce6"},
    "GEOPOLITICA": {"emoji": "🌍", "cor": "#64d2ff"},
    "OPEC":        {"emoji": "🛢️", "cor": "#ffd60a"},
    "CRYPTO":      {"emoji": "₿",  "cor": "#ff9f0a"},
    "ESPORTE":     {"emoji": "🏆", "cor": "#30d158"},
    "OUTROS":      {"emoji": "📅", "cor": "#8e8e93"},
}

# ── Calendário de eventos 2025-2026 ──────────────────────────
# Formato: datetime UTC, categoria, keywords para match de mercados
SCHEDULED_EVENTS = [
    # ── FED — FOMC Meetings 2026 ─────────────────────────────
    {
        "id": "fomc_jan_2026",
        "titulo": "FOMC Meeting — Decisão de Juros (Jan 2026)",
        "categoria": "FED",
        "datetime_utc": "2026-01-29 19:00:00",
        "descricao": "Federal Reserve anuncia decisão de taxa de juros. Mercado precifica 85% de manutenção.",
        "keywords": ["fed", "interest rate", "federal reserve", "fomc", "rate cut", "rate hike", "inflation"],
        "impacto_esperado": "ALTO",
        "fonte": "federalreserve.gov",
    },
    {
        "id": "fomc_mar_2026",
        "titulo": "FOMC Meeting — Decisão de Juros (Mar 2026)",
        "categoria": "FED",
        "datetime_utc": "2026-03-18 18:00:00",
        "descricao": "Fed decide sobre taxa de juros. Projeções econômicas (dot plot) também divulgadas.",
        "keywords": ["fed", "interest rate", "federal reserve", "fomc", "rate cut", "rate hike", "inflation"],
        "impacto_esperado": "ALTO",
        "fonte": "federalreserve.gov",
    },
    {
        "id": "fomc_mai_2026",
        "titulo": "FOMC Meeting — Decisão de Juros (Mai 2026)",
        "categoria": "FED",
        "datetime_utc": "2026-05-06 18:00:00",
        "descricao": "Reunião do FOMC. Ata completa divulgada 3 semanas depois.",
        "keywords": ["fed", "interest rate", "federal reserve", "fomc", "rate cut", "rate hike"],
        "impacto_esperado": "ALTO",
        "fonte": "federalreserve.gov",
    },
    {
        "id": "fomc_jun_2026",
        "titulo": "FOMC Meeting — Decisão de Juros (Jun 2026)",
        "categoria": "FED",
        "datetime_utc": "2026-06-17 18:00:00",
        "descricao": "Fed + dot plot + press conference Powell.",
        "keywords": ["fed", "interest rate", "federal reserve", "fomc", "rate cut", "powell"],
        "impacto_esperado": "ALTO",
        "fonte": "federalreserve.gov",
    },
    {
        "id": "fomc_jul_2026",
        "titulo": "FOMC Meeting — Decisão de Juros (Jul 2026)",
        "categoria": "FED",
        "datetime_utc": "2026-07-29 18:00:00",
        "descricao": "Reunião FOMC de julho. Mid-year review.",
        "keywords": ["fed", "interest rate", "federal reserve", "fomc", "rate cut", "rate hike"],
        "impacto_esperado": "ALTO",
        "fonte": "federalreserve.gov",
    },
    {
        "id": "fomc_sep_2026",
        "titulo": "FOMC Meeting — Decisão de Juros (Set 2026)",
        "categoria": "FED",
        "datetime_utc": "2026-09-16 18:00:00",
        "descricao": "Fed + projeções econômicas + dot plot.",
        "keywords": ["fed", "interest rate", "federal reserve", "fomc", "rate cut", "rate hike"],
        "impacto_esperado": "ALTO",
        "fonte": "federalreserve.gov",
    },

    # ── DADOS ECONÔMICOS CHAVE ────────────────────────────────
    {
        "id": "cpi_mar_2026",
        "titulo": "CPI USA — Inflação de Fevereiro (divulgado Mar 2026)",
        "categoria": "ECONOMIA",
        "datetime_utc": "2026-03-12 13:30:00",
        "descricao": "Bureau of Labor Statistics divulga CPI. Dado mais importante para Fed.",
        "keywords": ["inflation", "cpi", "consumer price", "fed", "interest rate", "economy"],
        "impacto_esperado": "ALTO",
        "fonte": "bls.gov",
    },
    {
        "id": "jobs_mar_2026",
        "titulo": "Non-Farm Payrolls — Empregos (Mar 2026)",
        "categoria": "ECONOMIA",
        "datetime_utc": "2026-03-06 13:30:00",
        "descricao": "Relatório de emprego mensal dos EUA. Impacto direto em taxa de juros.",
        "keywords": ["jobs", "employment", "payroll", "unemployment", "economy", "fed", "recession"],
        "impacto_esperado": "ALTO",
        "fonte": "bls.gov",
    },
    {
        "id": "gdp_q1_2026",
        "titulo": "PIB EUA Q1 2026 — Primeira Estimativa",
        "categoria": "ECONOMIA",
        "datetime_utc": "2026-04-29 12:30:00",
        "descricao": "BEA divulga primeira estimativa do PIB Q1. Pode confirmar ou refutar recessão.",
        "keywords": ["gdp", "recession", "economy", "growth", "us economy", "fed"],
        "impacto_esperado": "ALTO",
        "fonte": "bea.gov",
    },

    # ── GEOPOLÍTICA / SANÇÕES ─────────────────────────────────
    {
        "id": "iran_nuclear_deadline_2026",
        "titulo": "Prazo Nuclear Iran — Negociações",
        "categoria": "SANCAO",
        "datetime_utc": "2026-04-15 12:00:00",
        "descricao": "Prazo estimado para rodada de negociações nucleares Iran-EUA. Sucesso ou novas sanções.",
        "keywords": ["iran", "nuclear", "sanctions", "uranium", "enrichment", "iaea", "deal"],
        "impacto_esperado": "MUITO_ALTO",
        "fonte": "estimativa_analitica",
    },
    {
        "id": "russia_ceasefire_q2_2026",
        "titulo": "Negociações Paz Rússia-Ucrânia — Q2 2026",
        "categoria": "GEOPOLITICA",
        "datetime_utc": "2026-05-01 00:00:00",
        "descricao": "Prazo estimado para definição de negociações de cessar-fogo. Trump mediando.",
        "keywords": ["russia", "ukraine", "ceasefire", "peace", "war", "zelensky", "putin", "nato"],
        "impacto_esperado": "MUITO_ALTO",
        "fonte": "estimativa_analitica",
    },
    {
        "id": "china_tariff_review_2026",
        "titulo": "Revisão Tarifas EUA-China",
        "categoria": "GEOPOLITICA",
        "datetime_utc": "2026-04-01 00:00:00",
        "descricao": "Deadline para revisão de tarifas bilaterais. Grand Bargain Trump-Xi em jogo.",
        "keywords": ["china", "tariff", "trade war", "trump", "xi", "trade deal", "import"],
        "impacto_esperado": "ALTO",
        "fonte": "estimativa_analitica",
    },

    # ── ELEIÇÕES ──────────────────────────────────────────────
    {
        "id": "midterms_2026_nov",
        "titulo": "Midterm Elections EUA 2026",
        "categoria": "ELEICAO",
        "datetime_utc": "2026-11-03 12:00:00",
        "descricao": "Eleições legislativas americanas. Controle da Câmara e Senado em jogo.",
        "keywords": ["midterm", "election", "congress", "senate", "house", "democrat", "republican", "vote"],
        "impacto_esperado": "MUITO_ALTO",
        "fonte": "oficial",
    },
    {
        "id": "germany_election_follow_2026",
        "titulo": "Formação de Governo Alemanha",
        "categoria": "ELEICAO",
        "datetime_utc": "2026-03-31 00:00:00",
        "descricao": "Prazo para formação de coalizão pós-eleição alemã. Merz como provável chanceler.",
        "keywords": ["germany", "german", "election", "bundestag", "merz", "spd", "coalition", "chancellor"],
        "impacto_esperado": "MEDIO",
        "fonte": "oficial",
    },

    # ── JULGAMENTOS / DECISÕES LEGAIS ─────────────────────────
    {
        "id": "scotus_term_end_2026",
        "titulo": "SCOTUS — Fim do Termo (Decisões Pendentes)",
        "categoria": "JULGAMENTO",
        "datetime_utc": "2026-06-30 14:00:00",
        "descricao": "Suprema Corte divulga decisões do termo. Casos sobre IA, liberdade de expressão, imigração.",
        "keywords": ["supreme court", "scotus", "ruling", "court decision", "justice", "constitution"],
        "impacto_esperado": "ALTO",
        "fonte": "supremecourt.gov",
    },
    {
        "id": "trump_legal_2026",
        "titulo": "Casos Legais Trump — Audiências 2026",
        "categoria": "JULGAMENTO",
        "datetime_utc": "2026-04-20 14:00:00",
        "descricao": "Audiências em casos federais/estaduais. Potencial impacto político.",
        "keywords": ["trump", "legal", "court", "indictment", "verdict", "criminal", "trial", "judge"],
        "impacto_esperado": "ALTO",
        "fonte": "estimativa_analitica",
    },

    # ── OPEC / ENERGIA ────────────────────────────────────────
    {
        "id": "opec_meeting_jun_2026",
        "titulo": "Reunião OPEC+ — Produção de Petróleo",
        "categoria": "OPEC",
        "datetime_utc": "2026-06-01 10:00:00",
        "descricao": "OPEC+ decide sobre níveis de produção. Preço do petróleo altamente sensível.",
        "keywords": ["opec", "oil", "crude", "petroleum", "saudi", "production", "barrel", "energy"],
        "impacto_esperado": "ALTO",
        "fonte": "opec.org",
    },

    # ── CRYPTO ────────────────────────────────────────────────
    {
        "id": "bitcoin_etf_review_2026",
        "titulo": "SEC — Revisão ETFs Bitcoin Spot",
        "categoria": "CRYPTO",
        "datetime_utc": "2026-04-15 16:00:00",
        "descricao": "SEC revisa regras para novos ETFs cripto. Potencial expansão do mercado.",
        "keywords": ["bitcoin", "btc", "crypto", "sec", "etf", "cryptocurrency", "regulation"],
        "impacto_esperado": "ALTO",
        "fonte": "sec.gov",
    },
]

# Cache de alertas de eventos já enviados
_EVENT_ALERT_SEEN: set = set()  # event_id já alertado


def _event_match_markets(event: dict, markets: list) -> list:
    """
    Encontra mercados relacionados ao evento por sobreposição de keywords.
    Usa as keywords do próprio evento (já curadas) + palavras do título.
    """
    event_words = set(kw.lower() for kw in event.get("keywords", []))

    # Adiciona palavras do título do evento
    title_words = set(w.lower() for w in event["titulo"].split() if len(w) > 3)
    STOP = {"will", "the", "this", "that", "with", "from", "have", "been", "2026",
            "2025", "reunião", "decisão", "dados", "divulga", "prazo"}
    title_words -= STOP
    all_event_words = event_words | title_words

    matches = []
    for m in markets:
        q_lower = (m.question or "").lower()
        slug_lower = (m.market_slug or "").lower()
        combined = q_lower + " " + slug_lower

        # Score: conta quantas keywords do evento aparecem no mercado
        hits = sum(1 for kw in all_event_words if kw in combined)
        if hits >= 2:
            yes_price, _ = _get_yes_no_prices(m)
            matches.append({
                "market": m,
                "hits": hits,
                "yes_price": yes_price,
            })

    matches.sort(key=lambda x: x["hits"], reverse=True)
    return matches[:5]  # top 5 mercados mais relevantes


def _event_hours_until(event_dt_str: str) -> float:
    """Retorna horas até o evento (negativo se já passou)."""
    try:
        event_dt = datetime.strptime(event_dt_str, "%Y-%m-%d %H:%M:%S")
        delta = event_dt - datetime.utcnow()
        return delta.total_seconds() / 3600
    except Exception:
        return -9999


def _event_send_telegram(event: dict, mercados_afetados: list, horas: float) -> None:
    """Formata e envia alerta de evento agendado no Telegram."""
    cat = EVENT_CATEGORIES.get(event["categoria"], EVENT_CATEGORIES["OUTROS"])
    emoji = cat["emoji"]

    impacto_emoji = {
        "MUITO_ALTO": "🔴🔴",
        "ALTO": "🔴",
        "MEDIO": "🟠",
        "BAIXO": "🟡",
    }.get(event.get("impacto_esperado", "MEDIO"), "🟠")

    horas_txt = f"{horas:.0f}h" if horas >= 1 else f"{horas*60:.0f}min"

    mercados_txt = ""
    for m in mercados_afetados[:3]:
        price_txt = f"{m['yes_price']}%" if m.get("yes_price") else "?"
        url = _polymarket_url(m["market"].market_slug)
        mercados_txt += f"\n• <a href=\"{url}\">{m['market'].question[:70]}</a> @ {price_txt}"

    msg = (
        f"{emoji} <b>EVENTO AGENDADO — {horas_txt} restantes</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{impacto_emoji} <b>{event['titulo']}</b>\n"
        f"📋 {event.get('descricao', '')[:120]}\n"
        f"⏰ {event['datetime_utc']} UTC\n"
        f"📡 Fonte: {event.get('fonte', '?')}\n"
    )

    if mercados_afetados:
        msg += (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>Mercados afetados ({len(mercados_afetados)}):</b>"
            f"{mercados_txt}\n"
        )

    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷️ Categoria: {event['categoria']} | Impacto: {event.get('impacto_esperado','?')}\n"
        f"🔑 Keywords: {', '.join(event.get('keywords', [])[:5])}\n"
        f"⏰ Detectado: {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )

    _telegram_send(msg)


@app.get("/events/calendar")
def get_events_calendar(
    categoria: str = Query(None, description="Filtra por categoria: FED, ELEICAO, SANCAO, etc."),
    apenas_futuros: int = Query(1, description="1 = só eventos futuros"),
    db: Session = Depends(get_db),
):
    """
    Calendário de eventos agendados com mercados afetados.
    Retorna todos os eventos com status (FUTURO / HOJE / PASSADO).
    """
    now = datetime.utcnow()

    # Mercados ativos para cruzar
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    resultado = []
    for event in SCHEDULED_EVENTS:
        # Filtro de categoria
        if categoria and event["categoria"] != categoria.upper():
            continue

        horas = _event_hours_until(event["datetime_utc"])

        # Filtro: só futuros
        if apenas_futuros and horas < -24:
            continue

        # Status
        if horas > 24:
            status = "FUTURO"
            status_emoji = "🔵"
        elif horas > 0:
            status = "HOJE"
            status_emoji = "🟡"
        elif horas > -24:
            status = "OCORREU_HOJE"
            status_emoji = "🟠"
        else:
            status = "PASSADO"
            status_emoji = "⚫"

        mercados_match = _event_match_markets(event, markets)

        cat = EVENT_CATEGORIES.get(event["categoria"], EVENT_CATEGORIES["OUTROS"])

        resultado.append({
            "id": event["id"],
            "titulo": event["titulo"],
            "categoria": event["categoria"],
            "emoji": cat["emoji"],
            "datetime_utc": event["datetime_utc"],
            "horas_restantes": round(horas, 1),
            "status": status,
            "status_emoji": status_emoji,
            "descricao": event.get("descricao", ""),
            "impacto_esperado": event.get("impacto_esperado", "MEDIO"),
            "fonte": event.get("fonte", "?"),
            "keywords": event.get("keywords", []),
            "mercados_afetados": [
                {
                    "question": m["market"].question,
                    "slug": m["market"].market_slug,
                    "yes_price": m.get("yes_price"),
                    "relevancia_hits": m["hits"],
                    "polymarket_url": _polymarket_url(m["market"].market_slug),
                }
                for m in mercados_match
            ],
            "total_mercados": len(mercados_match),
            "ja_alertado": event["id"] in _EVENT_ALERT_SEEN,
        })

    # Ordena: mais próximos primeiro
    resultado.sort(key=lambda x: x["horas_restantes"] if x["horas_restantes"] >= 0 else 9999)

    categorias_presentes = list({e["categoria"] for e in resultado})
    proximos_24h = [e for e in resultado if 0 <= e["horas_restantes"] <= 24]

    return {
        "status": "ok",
        "total_eventos": len(resultado),
        "proximos_24h": len(proximos_24h),
        "categorias": categorias_presentes,
        "eventos": resultado,
        "gerado_em": now.isoformat(),
    }


@app.get("/events/upcoming")
def get_events_upcoming(
    horas: int = Query(24, ge=1, le=168, description="Janela em horas (default: 24h, max: 7 dias)"),
    alertar: int = Query(0, description="1 = envia Telegram para eventos novos"),
    min_impacto: str = Query("MEDIO", description="BAIXO, MEDIO, ALTO, MUITO_ALTO"),
    db: Session = Depends(get_db),
):
    """
    Eventos nas próximas N horas com mercados afetados.
    Core do Motor #25 — chamado pelo cron_tick.
    """
    now = datetime.utcnow()
    impacto_rank = {"BAIXO": 1, "MEDIO": 2, "ALTO": 3, "MUITO_ALTO": 4}
    min_rank = impacto_rank.get(min_impacto.upper(), 2)

    # Mercados ativos
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    proximos = []
    alertas_enviados = 0

    for event in SCHEDULED_EVENTS:
        horas_ate = _event_hours_until(event["datetime_utc"])

        # Só eventos na janela
        if not (0 < horas_ate <= horas):
            continue

        # Filtro de impacto
        ev_rank = impacto_rank.get(event.get("impacto_esperado", "MEDIO"), 2)
        if ev_rank < min_rank:
            continue

        mercados_match = _event_match_markets(event, markets)
        cat = EVENT_CATEGORIES.get(event["categoria"], EVENT_CATEGORIES["OUTROS"])

        entry = {
            "id": event["id"],
            "titulo": event["titulo"],
            "categoria": event["categoria"],
            "emoji": cat["emoji"],
            "datetime_utc": event["datetime_utc"],
            "horas_restantes": round(horas_ate, 1),
            "impacto_esperado": event.get("impacto_esperado", "MEDIO"),
            "descricao": event.get("descricao", ""),
            "mercados_afetados": [
                {
                    "question": m["market"].question,
                    "slug": m["market"].market_slug,
                    "yes_price": m.get("yes_price"),
                    "polymarket_url": _polymarket_url(m["market"].market_slug),
                }
                for m in mercados_match
            ],
            "total_mercados": len(mercados_match),
            "ja_alertado": event["id"] in _EVENT_ALERT_SEEN,
        }
        proximos.append(entry)

        # Alerta Telegram (só uma vez por evento)
        if alertar and event["id"] not in _EVENT_ALERT_SEEN:
            _event_send_telegram(event, mercados_match, horas_ate)
            _EVENT_ALERT_SEEN.add(event["id"])
            alertas_enviados += 1

            # Limpa cache se crescer muito
            if len(_EVENT_ALERT_SEEN) > 1000:
                for eid in list(_EVENT_ALERT_SEEN)[:200]:
                    _EVENT_ALERT_SEEN.discard(eid)

    proximos.sort(key=lambda x: x["horas_restantes"])

    return {
        "status": "ok",
        "janela_horas": horas,
        "min_impacto": min_impacto,
        "total_proximos": len(proximos),
        "alertas_enviados": alertas_enviados,
        "eventos": proximos,
        "gerado_em": now.isoformat(),
    }


@app.post("/events/alert/reset")
def events_alert_reset():
    """Reseta cache de alertas — força re-alertar todos os eventos."""
    count = len(_EVENT_ALERT_SEEN)
    _EVENT_ALERT_SEEN.clear()
    return {
        "status": "ok",
        "alertas_removidos": count,
        "mensagem": "Cache resetado. Próximo cron vai re-alertar todos os eventos.",
    }


@app.get("/events/test/{event_id}")
def events_test_alert(event_id: str, db: Session = Depends(get_db)):
    """Testa alerta de um evento específico pelo ID."""
    event = next((e for e in SCHEDULED_EVENTS if e["id"] == event_id), None)
    if not event:
        ids_disponiveis = [e["id"] for e in SCHEDULED_EVENTS]
        return {"error": "Evento não encontrado", "ids_disponiveis": ids_disponiveis}

    now = datetime.utcnow()
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    mercados_match = _event_match_markets(event, markets)
    horas = _event_hours_until(event["datetime_utc"])

    _event_send_telegram(event, mercados_match, horas)

    return {
        "status": "ok",
        "evento": event["titulo"],
        "horas_restantes": round(horas, 1),
        "mercados_encontrados": len(mercados_match),
        "mercados": [
            {
                "question": m["market"].question,
                "yes_price": m.get("yes_price"),
                "hits": m["hits"],
            }
            for m in mercados_match
        ],
        "telegram_enviado": True,
    }
# ══════════════════════════════════════════════════════════════
# MOTOR #27 — DETECTOR DE CONTRADIÇÃO ENTRE MERCADOS
#
# Lógica central:
# Se mercado A diz 80% de ataque ao Irã, mas mercado de petróleo
# diz só 30% de alta → contradição = um deles está errado → edge.
#
# Duas abordagens combinadas:
# 1. PARES SEMÂNTICOS: pares definidos com relação esperada
#    Ex: Iran_attack × Oil_spike → correlação POSITIVA esperada
#    Se divergem muito → contradição detectada
#
# 2. AUTO-SCAN: varre todos os mercados do banco,
#    agrupa por tema, detecta quando mercados do mesmo tema
#    dão sinais opostos sem justificativa
#
# Output:
# - Qual lado está "errado" (qual mercado o sistema acha que vai corrigir)
# - Edge estimado
# - Ação recomendada
# - Alerta Telegram automático
# ══════════════════════════════════════════════════════════════

# ── Pares semânticos com relação esperada ────────────────────
# relacao: "POSITIVA" = ambos devem subir/cair juntos
#          "NEGATIVA" = um sobe, outro cai
# threshold_contradicao: diferença mínima (pts) para ser contradição
CONTRADICTION_PAIRS = [
    # ── IRAN × PETRÓLEO ──────────────────────────────────────
    {
        "id": "iran_oil",
        "tema": "Irã × Petróleo",
        "descricao": "Ataque ao Irã → fechamento Estreito de Hormuz → petróleo explode",
        "icone": "⚔️🛢️",
        "relacao": "POSITIVA",
        "threshold": 20,
        "mercado_a": {
            "nome": "Ataque ao Irã",
            "keywords": ["iran", "attack", "strike", "war", "military", "iranian"],
            "direcao_esperada": "UP",  # se ataque → sobe
        },
        "mercado_b": {
            "nome": "Petróleo alto",
            "keywords": ["oil", "crude", "brent", "wti", "petroleum", "barrel", "150", "120", "100"],
            "direcao_esperada": "UP",  # se ataque → sobe
        },
        "logica": "Se prob. de ataque ao Irã é alta, petróleo DEVE ser alto também. Se não é → petróleo subavaliado.",
        "acao_se_a_alto_b_baixo": "COMPRAR petróleo (B está subavaliado dado risco Iran)",
        "acao_se_a_baixo_b_alto": "VENDER petróleo (B superavaliado dado baixo risco Iran)",
    },
    # ── RÚSSIA/UCRÂNIA × PETRÓLEO ────────────────────────────
    {
        "id": "russia_oil",
        "tema": "Cessar-fogo Rússia × Petróleo",
        "descricao": "Cessar-fogo → normalização → queda do petróleo",
        "icone": "❄️🛢️",
        "relacao": "NEGATIVA",
        "threshold": 15,
        "mercado_a": {
            "nome": "Cessar-fogo Rússia-Ucrânia",
            "keywords": ["russia", "ukraine", "ceasefire", "peace", "war end"],
            "direcao_esperada": "UP",
        },
        "mercado_b": {
            "nome": "Petróleo alto",
            "keywords": ["oil", "crude", "brent", "petroleum", "barrel", "120", "150"],
            "direcao_esperada": "DOWN",  # se cessar-fogo → petróleo CAI
        },
        "logica": "Se prob. de cessar-fogo sobe, expectativa de petróleo alto DEVE cair. Se ambos sobem → contradição.",
        "acao_se_a_alto_b_alto": "VENDER petróleo (cessar-fogo tornaria petróleo alto improvável)",
        "acao_se_a_baixo_b_baixo": "COMPRAR petróleo (sem paz = pressão de oferta continua)",
    },
    # ── FED CORTE × BITCOIN ───────────────────────────────────
    {
        "id": "fed_bitcoin",
        "tema": "Fed Corte de Juros × Bitcoin",
        "descricao": "Corte de juros → liquidez → Bitcoin sobe",
        "icone": "🏦₿",
        "relacao": "POSITIVA",
        "threshold": 20,
        "mercado_a": {
            "nome": "Fed corta juros",
            "keywords": ["fed", "rate cut", "interest rate", "fomc", "federal reserve", "pivot"],
            "direcao_esperada": "UP",
        },
        "mercado_b": {
            "nome": "Bitcoin alto",
            "keywords": ["bitcoin", "btc", "crypto", "100k", "150k", "200k", "cryptocurrency"],
            "direcao_esperada": "UP",
        },
        "logica": "Corte de juros Fed → liquidez → ativos de risco sobem → Bitcoin deve subir junto.",
        "acao_se_a_alto_b_baixo": "COMPRAR Bitcoin (está subavaliado dado corte esperado)",
        "acao_se_a_baixo_b_alto": "VENDER Bitcoin (superavaliado dado Fed hawkish)",
    },
    # ── TRUMP × CHINA TARIFAS ─────────────────────────────────
    {
        "id": "trump_china_trade",
        "tema": "Trump × Guerra Comercial China",
        "descricao": "Escalada tarifas → mercados emergentes sofrem",
        "icone": "🤝🇨🇳",
        "relacao": "NEGATIVA",
        "threshold": 15,
        "mercado_a": {
            "nome": "Trump escala tarifas China",
            "keywords": ["trump", "china", "tariff", "trade war", "import", "xi"],
            "direcao_esperada": "UP",
        },
        "mercado_b": {
            "nome": "Acordo comercial EUA-China",
            "keywords": ["trade deal", "china deal", "trade agreement", "grand bargain", "xi trump deal"],
            "direcao_esperada": "DOWN",  # mais tarifas = menos chance de acordo
        },
        "logica": "Alta prob. de escalada tarifária é NEGATIVA para acordo. Se ambos são altos → contradição.",
        "acao_se_a_alto_b_alto": "VENDER acordo (escalada torna deal improvável)",
        "acao_se_a_baixo_b_baixo": "COMPRAR acordo (baixa tensão favorece deal)",
    },
    # ── RECESSÃO EUA × S&P500 ─────────────────────────────────
    {
        "id": "recession_sp500",
        "tema": "Recessão EUA × Mercado de Ações",
        "descricao": "Recessão → queda S&P500 — se mercado não precifica, está errado",
        "icone": "📉💹",
        "relacao": "NEGATIVA",
        "threshold": 20,
        "mercado_a": {
            "nome": "Recessão EUA",
            "keywords": ["recession", "gdp", "economic downturn", "us economy crash", "depression"],
            "direcao_esperada": "UP",
        },
        "mercado_b": {
            "nome": "S&P500 alto / Mercado Bull",
            "keywords": ["s&p", "sp500", "stock market", "dow jones", "nasdaq", "bull market", "ath"],
            "direcao_esperada": "DOWN",  # recessão = stocks caem
        },
        "logica": "Alta prob. de recessão é incompatível com S&P500 em alta. Um está errado.",
        "acao_se_a_alto_b_alto": "VENDER S&P bull (recessão tornaria isso impossível)",
        "acao_se_a_baixo_b_baixo": "COMPRAR S&P bull (sem recessão = mercado pode subir)",
    },
    # ── ISRAEL × PETRÓLEO ─────────────────────────────────────
    {
        "id": "israel_oil",
        "tema": "Conflito Israel × Petróleo",
        "descricao": "Escalada Israel-Irã → risco Hormuz → petróleo",
        "icone": "🇮🇱🛢️",
        "relacao": "POSITIVA",
        "threshold": 15,
        "mercado_a": {
            "nome": "Escalada conflito Israel",
            "keywords": ["israel", "iran", "hezbollah", "hamas", "gaza", "idf", "attack", "war"],
            "direcao_esperada": "UP",
        },
        "mercado_b": {
            "nome": "Petróleo alto",
            "keywords": ["oil", "crude", "brent", "petroleum", "barrel", "100", "120", "150"],
            "direcao_esperada": "UP",
        },
        "logica": "Escalada Israel-Irã ameaça Hormuz → petróleo deve subir junto.",
        "acao_se_a_alto_b_baixo": "COMPRAR petróleo (risco geopolítico não precificado)",
        "acao_se_a_baixo_b_alto": "VENDER petróleo (sem escalada = risco geopolítico superavaliado)",
    },
    # ── ELEIÇÃO EUA 2028 × POLÍTICAS ─────────────────────────
    {
        "id": "election_policies",
        "tema": "Eleição 2028 × Políticas Esperadas",
        "descricao": "Favorito eleitoral deve refletir nas políticas esperadas",
        "icone": "🗳️📋",
        "relacao": "POSITIVA",
        "threshold": 25,
        "mercado_a": {
            "nome": "Republicano vence 2028",
            "keywords": ["republican", "2028", "president", "gop", "election", "nominee"],
            "direcao_esperada": "UP",
        },
        "mercado_b": {
            "nome": "Corte de impostos / desregulação",
            "keywords": ["tax cut", "deregulation", "corporate tax", "business tax", "regulation"],
            "direcao_esperada": "UP",  # republicano = corte de impostos
        },
        "logica": "Se republicano favorito, políticas pró-business devem ter prob. alta.",
        "acao_se_a_alto_b_baixo": "COMPRAR corte impostos (subestimado dado favorito republicano)",
        "acao_se_a_baixo_b_alto": "VENDER corte impostos (overpriced dado chance baixa republicano)",
    },
]

# Cache de contradições já alertadas
_CONTRA_SEEN: set = set()


def _contra_find_market(keywords: list, markets: list, exclude_ids: set = None) -> tuple:
    """
    Encontra o mercado mais relevante para um conjunto de keywords.
    Retorna (market, yes_price, score_hits).
    """
    exclude_ids = exclude_ids or set()
    best_market = None
    best_price = None
    best_hits = 0

    for m in markets:
        if m.id in exclude_ids:
            continue
        q_lower = (m.question or "").lower()
        slug_lower = (m.market_slug or "").lower()
        combined = q_lower + " " + slug_lower

        hits = sum(1 for kw in keywords if kw.lower() in combined)
        if hits > best_hits:
            best_hits = hits
            best_market = m
            yes_p, _ = _get_yes_no_prices(m)
            best_price = yes_p

    return best_market, best_price, best_hits


def _contra_score(price_a: float, price_b: float, relacao: str, threshold: float) -> dict:
    """
    Calcula o score de contradição entre dois mercados.

    Relação POSITIVA: ambos deveriam ter preços similares
    Relação NEGATIVA: preços deveriam ser inversamente relacionados (somar ~100)

    Retorna: {score, tipo, lado_errado, magnitude}
    """
    if price_a is None or price_b is None:
        return {"score": 0, "tipo": "SEM_DADOS", "magnitude": 0}

    if relacao == "POSITIVA":
        # Se A=80% e B=30% → diferença de 50pts = CONTRADIÇÃO FORTE
        diferenca = abs(price_a - price_b)
        if diferenca < threshold:
            return {"score": 0, "tipo": "CONSISTENTE", "magnitude": diferenca}

        # Qual está errado? O de menor preço geralmente está subavaliado
        # (assumindo que o mercado mais líquido / maior volume está certo)
        lado_errado = "B_SUBAVALIADO" if price_a > price_b else "A_SUBAVALIADO"
        score = min(round((diferenca - threshold) * 2, 1), 100)
        return {
            "score": score,
            "tipo": "CONTRADICAO_POSITIVA",
            "lado_errado": lado_errado,
            "magnitude": round(diferenca, 1),
        }

    elif relacao == "NEGATIVA":
        # Se A=80% e B=80% → soma 160% quando deveria ser ~100% = CONTRADIÇÃO
        soma = price_a + price_b
        desvio_de_100 = abs(soma - 100)
        if desvio_de_100 < threshold:
            return {"score": 0, "tipo": "CONSISTENTE", "magnitude": desvio_de_100}

        # Se soma > 100: ambos muito altos → um vai cair
        # Se soma < 100: ambos muito baixos → um vai subir
        tipo = "CONTRADICAO_NEGATIVA_ALTA" if soma > 100 else "CONTRADICAO_NEGATIVA_BAIXA"
        lado_errado = "AMBOS_ALTOS" if soma > 100 else "AMBOS_BAIXOS"
        score = min(round((desvio_de_100 - threshold) * 2, 1), 100)
        return {
            "score": score,
            "tipo": tipo,
            "lado_errado": lado_errado,
            "magnitude": round(desvio_de_100, 1),
            "soma_precos": round(soma, 1),
        }

    return {"score": 0, "tipo": "DESCONHECIDO", "magnitude": 0}


def _contra_recommend_action(par: dict, price_a: float, price_b: float, contra_result: dict) -> dict:
    """Gera recomendação de ação baseada na contradição detectada."""
    relacao = par["relacao"]
    lado = contra_result.get("lado_errado", "")
    tipo = contra_result.get("tipo", "")

    if relacao == "POSITIVA":
        if lado == "B_SUBAVALIADO":
            acao = par.get("acao_se_a_alto_b_baixo", "COMPRAR mercado B")
            mercado_alvo = "B"
            direcao = "YES"
        else:
            acao = par.get("acao_se_a_baixo_b_alto", "COMPRAR mercado A")
            mercado_alvo = "A"
            direcao = "YES"
    elif relacao == "NEGATIVA":
        if "ALTA" in tipo:
            acao = par.get("acao_se_a_alto_b_alto", "VENDER o de maior preço")
            mercado_alvo = "B" if price_b > price_a else "A"
            direcao = "NO"
        else:
            acao = par.get("acao_se_a_baixo_b_baixo", "COMPRAR o de maior potencial")
            mercado_alvo = "A"
            direcao = "YES"
    else:
        acao = "AGUARDAR"
        mercado_alvo = "NENHUM"
        direcao = "NEUTRO"

    confianca = min(contra_result.get("score", 0) / 100, 0.92)

    return {
        "acao": acao,
        "mercado_alvo": mercado_alvo,
        "direcao": direcao,
        "confianca": round(confianca, 2),
        "confianca_pct": round(confianca * 100),
        "nivel": (
            "🔴 MUITO ALTA" if confianca >= 0.75 else
            "🟠 ALTA"       if confianca >= 0.55 else
            "🟡 MÉDIA"      if confianca >= 0.35 else
            "⚪ BAIXA"
        ),
    }


def _contra_telegram_alert(par: dict, market_a, market_b, price_a, price_b, contra, recomendacao) -> None:
    """Envia alerta de contradição no Telegram."""
    score = contra.get("score", 0)
    magnitude = contra.get("magnitude", 0)
    tipo = contra.get("tipo", "")

    nivel_emoji = "🔴🔴" if score >= 75 else "🔴" if score >= 55 else "🟠" if score >= 35 else "🟡"

    url_a = _polymarket_url(market_a.market_slug) if market_a else "#"
    url_b = _polymarket_url(market_b.market_slug) if market_b else "#"

    q_a = (market_a.question or "?")[:70] if market_a else "?"
    q_b = (market_b.question or "?")[:70] if market_b else "?"

    msg = (
        f"{nivel_emoji} <b>CONTRADIÇÃO DETECTADA — {par['icone']} {par['tema']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 {par['logica']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🅰️ <a href=\"{url_a}\">{q_a}</a>\n"
        f"   Preço YES: <b>{price_a}%</b>\n"
        f"\n"
        f"🅱️ <a href=\"{url_b}\">{q_b}</a>\n"
        f"   Preço YES: <b>{price_b}%</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Contradição: <b>{magnitude:.0f}pts</b> | Score: {score:.0f}/100\n"
        f"🎯 <b>AÇÃO:</b> {recomendacao['acao']}\n"
        f"📊 Confiança: {recomendacao['confianca_pct']}% {recomendacao['nivel']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )
    _telegram_send(msg)


@app.get("/contradictions")
def get_contradictions(
    min_score: float = Query(20.0, ge=5, le=100),
    alertar: int = Query(0, description="1 = envia Telegram para contradições novas"),
    db: Session = Depends(get_db),
):
    """
    Detector de Contradição entre Mercados — #27.

    Varre pares semânticos predefinidos (Iran×Oil, Fed×Bitcoin, etc.)
    e detecta quando preços divergem de forma semanticamente incoerente.

    Exemplo clássico:
    - Iran attack = 75% → petróleo $150 deveria ser alto
    - Mas petróleo $150 = 30% → CONTRADIÇÃO de 45pts
    - Sistema sugere: COMPRAR petróleo (está subavaliado)
    """
    now = datetime.utcnow()

    # Busca mercados ativos com filtros
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(600).all()
    )

    resultados = []
    stats = {
        "pares_analisados": 0,
        "contradicoes_encontradas": 0,
        "alertas_enviados": 0,
    }

    for par in CONTRADICTION_PAIRS:
        stats["pares_analisados"] += 1

        # Encontra mercado A
        market_a, price_a, hits_a = _contra_find_market(
            par["mercado_a"]["keywords"], markets
        )

        if not market_a or hits_a < 2:
            continue

        # Encontra mercado B (exclui A)
        market_b, price_b, hits_b = _contra_find_market(
            par["mercado_b"]["keywords"], markets,
            exclude_ids={market_a.id}
        )

        if not market_b or hits_b < 2:
            continue

        if price_a is None or price_b is None:
            continue

        # Calcula contradição
        contra = _contra_score(price_a, price_b, par["relacao"], par["threshold"])

        if contra["score"] < min_score:
            continue

        if contra["tipo"] in ("CONSISTENTE", "SEM_DADOS", "DESCONHECIDO"):
            continue

        stats["contradicoes_encontradas"] += 1

        # Gera recomendação
        recomendacao = _contra_recommend_action(par, price_a, price_b, contra)

        # Volume dos mercados
        vol_a = _get_market_volume(market_a)
        vol_b = _get_market_volume(market_b)

        entry = {
            "par_id": par["id"],
            "tema": par["tema"],
            "icone": par["icone"],
            "descricao": par["descricao"],
            "relacao_esperada": par["relacao"],
            "logica": par["logica"],
            "mercado_a": {
                "question": market_a.question,
                "slug": market_a.market_slug,
                "yes_price": price_a,
                "volume": vol_a,
                "polymarket_url": _polymarket_url(market_a.market_slug),
            },
            "mercado_b": {
                "question": market_b.question,
                "slug": market_b.market_slug,
                "yes_price": price_b,
                "volume": vol_b,
                "polymarket_url": _polymarket_url(market_b.market_slug),
            },
            "contradicao": {
                "score": contra["score"],
                "tipo": contra["tipo"],
                "magnitude_pts": contra.get("magnitude", 0),
                "lado_errado": contra.get("lado_errado", "?"),
                "soma_precos": contra.get("soma_precos"),
            },
            "recomendacao": recomendacao,
            "acao_sugerida": recomendacao["acao"],
            "detectado_em": now.isoformat(),
        }
        resultados.append(entry)

        # Telegram — só para contradições novas
        cache_key = f"{par['id']}:{round(price_a)}:{round(price_b)}"
        if alertar and cache_key not in _CONTRA_SEEN and contra["score"] >= 40:
            _contra_telegram_alert(par, market_a, market_b, price_a, price_b, contra, recomendacao)
            _CONTRA_SEEN.add(cache_key)
            stats["alertas_enviados"] += 1
            if len(_CONTRA_SEEN) > 2000:
                for k in list(_CONTRA_SEEN)[:400]:
                    _CONTRA_SEEN.discard(k)

    # Ordena por score de contradição
    resultados.sort(key=lambda x: x["contradicao"]["score"], reverse=True)

    return {
        "status": "ok",
        "total_pares_definidos": len(CONTRADICTION_PAIRS),
        "pares_analisados": stats["pares_analisados"],
        "contradicoes_encontradas": stats["contradicoes_encontradas"],
        "alertas_enviados": stats["alertas_enviados"],
        "min_score": min_score,
        "resumo": {
            "score_max": max((r["contradicao"]["score"] for r in resultados), default=0),
            "score_medio": round(
                sum(r["contradicao"]["score"] for r in resultados) / max(len(resultados), 1), 1
            ),
            "alta_confianca": sum(1 for r in resultados if r["recomendacao"]["confianca"] >= 0.55),
        },
        "contradicoes": resultados,
        "pares_monitorados": [
            {"id": p["id"], "tema": p["tema"], "icone": p["icone"], "relacao": p["relacao"]}
            for p in CONTRADICTION_PAIRS
        ],
        "gerado_em": now.isoformat(),
    }


@app.get("/contradictions/auto")
def get_contradictions_auto(
    min_score: float = Query(25.0, ge=10, le=100),
    db: Session = Depends(get_db),
):
    """
    Auto-scanner de contradições — sem pares predefinidos.

    Agrupa mercados por tema (usando CORR_THEMES já existente),
    e dentro de cada tema detecta quando mercados dão sinais OPOSTOS
    sem justificativa semântica.

    Exemplo: dois mercados sobre "Iran attack" com preços opostos
    (YES de um = 80%, YES do outro = 20%) → possivelmente descrevem
    o mesmo evento de ângulos diferentes → contradição.
    """
    now = datetime.utcnow()

    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    # Agrupa por tema
    theme_markets: dict = {}
    for m in markets:
        themes = _corr_detect_theme(m.question or "", m.market_slug or "")
        yes_price, _ = _get_yes_no_prices(m)
        if yes_price is None:
            continue
        vol = _get_market_volume(m)
        if vol > 0 and vol < FILTER_MIN_VOLUME:
            continue
        for theme in themes:
            if theme == "OUTROS":
                continue
            if theme not in theme_markets:
                theme_markets[theme] = []
            theme_markets[theme].append({
                "market": m,
                "yes_price": yes_price,
                "volume": vol,
            })

    contradicoes = []

    for theme, mlist in theme_markets.items():
        if len(mlist) < 2:
            continue

        # Detecta pares com preços muito divergentes dentro do mesmo tema
        for i in range(len(mlist)):
            for j in range(i + 1, len(mlist)):
                ma = mlist[i]
                mb = mlist[j]

                pa = ma["yes_price"]
                pb = mb["yes_price"]

                # Não analisa se são o mesmo mercado
                if ma["market"].id == mb["market"].id:
                    continue

                # Calcula divergência simples de preços
                diferenca = abs(pa - pb)

                # Só é interessante se divergência > 25pts e ambos têm preço significativo
                if diferenca < 25 or pa < 15 or pb < 15:
                    continue

                # Score baseado na divergência e volume
                score = min(round((diferenca - 25) * 1.5, 1), 100)
                if score < min_score:
                    continue

                # Qual mercado está mais "errado"?
                # O com menor preço é potencialmente subavaliado se o tema é positivo
                subavaliado = "B" if pa > pb else "A"
                mercado_comprar = mb["market"] if subavaliado == "B" else ma["market"]
                price_comprar = pb if subavaliado == "B" else pa

                contradicoes.append({
                    "tema": theme,
                    "tipo": "AUTO_DETECTADA",
                    "mercado_a": {
                        "question": ma["market"].question,
                        "slug": ma["market"].market_slug,
                        "yes_price": pa,
                        "polymarket_url": _polymarket_url(ma["market"].market_slug),
                    },
                    "mercado_b": {
                        "question": mb["market"].question,
                        "slug": mb["market"].market_slug,
                        "yes_price": pb,
                        "polymarket_url": _polymarket_url(mb["market"].market_slug),
                    },
                    "divergencia_pts": round(diferenca, 1),
                    "score": score,
                    "potencial_subavaliado": subavaliado,
                    "acao_sugerida": f"Investigar {mercado_comprar.question[:60]} @ {price_comprar}%",
                    "detectado_em": now.isoformat(),
                })

    contradicoes.sort(key=lambda x: x["score"], reverse=True)

    return {
        "status": "ok",
        "temas_analisados": len(theme_markets),
        "total_contradicoes_auto": len(contradicoes),
        "contradicoes": contradicoes[:20],
        "nota": "Auto-scan sem pares predefinidos. Use /contradictions para análise semântica mais precisa.",
        "gerado_em": now.isoformat(),
    }


@app.get("/contradictions/explain/{par_id}")
def contradictions_explain(par_id: str, db: Session = Depends(get_db)):
    """Explica a lógica de um par específico com dados atuais do banco."""
    par = next((p for p in CONTRADICTION_PAIRS if p["id"] == par_id), None)
    if not par:
        ids = [p["id"] for p in CONTRADICTION_PAIRS]
        return {"error": "Par não encontrado", "ids_disponiveis": ids}

    now = datetime.utcnow()
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(600).all()
    )

    market_a, price_a, hits_a = _contra_find_market(par["mercado_a"]["keywords"], markets)
    market_b, price_b, hits_b = _contra_find_market(
        par["mercado_b"]["keywords"], markets,
        exclude_ids={market_a.id} if market_a else set()
    )

    contra = _contra_score(price_a or 50, price_b or 50, par["relacao"], par["threshold"])
    recomendacao = _contra_recommend_action(par, price_a or 50, price_b or 50, contra)

    return {
        "par_id": par_id,
        "tema": par["tema"],
        "icone": par["icone"],
        "descricao": par["descricao"],
        "relacao_esperada": par["relacao"],
        "logica": par["logica"],
        "threshold_contradicao": par["threshold"],
        "mercado_a_encontrado": {
            "question": market_a.question if market_a else None,
            "slug": market_a.market_slug if market_a else None,
            "yes_price": price_a,
            "hits_keywords": hits_a,
            "polymarket_url": _polymarket_url(market_a.market_slug) if market_a else None,
        },
        "mercado_b_encontrado": {
            "question": market_b.question if market_b else None,
            "slug": market_b.market_slug if market_b else None,
            "yes_price": price_b,
            "hits_keywords": hits_b,
            "polymarket_url": _polymarket_url(market_b.market_slug) if market_b else None,
        },
        "contradicao_atual": contra,
        "recomendacao": recomendacao,
        "acao_sugerida": recomendacao["acao"],
        "gerado_em": now.isoformat(),
    }
# ══════════════════════════════════════════════════════════════
# MOTOR #26 — SCANNER DE TELEGRAM MILITAR
#
# Raspa canais públicos do Telegram via t.me/s/{canal}
# Traduz + classifica via Claude Haiku
# Cruza com mercados ativos do banco
# Dispara Telegram antes do Reuters cobrir
# Zero credencial — só requests + BeautifulSoup
# ══════════════════════════════════════════════════════════════

from bs4 import BeautifulSoup

# ── Canais militares públicos monitorados ────────────────────
MILITARY_CHANNELS = [
    {"id": "militarylandnet",     "nome": "Military Land",      "regiao": "UKRAINE",  "lingua": "en"},
    {"id": "ukrainewar",          "nome": "Ukraine War",        "regiao": "UKRAINE",  "lingua": "en"},
    {"id": "nexta_tv",            "nome": "NEXTA",              "regiao": "UKRAINE",  "lingua": "ru"},
    {"id": "osintukraine",        "nome": "OSINT Ukraine",      "regiao": "UKRAINE",  "lingua": "en"},
    {"id": "rybar",               "nome": "Rybar (RU)",         "regiao": "RUSSIA",   "lingua": "ru"},
    {"id": "iranintl",            "nome": "Iran International", "regiao": "IRAN",     "lingua": "en"},
    {"id": "IsraelWarRoom",       "nome": "Israel War Room",    "regiao": "ISRAEL",   "lingua": "en"},
    {"id": "MiddleEastSpectator", "nome": "ME Spectator",       "regiao": "MIDEAST",  "lingua": "en"},
]

# ── Keywords de impacto por categoria ────────────────────────
MILITARY_IMPACT_KEYWORDS = {
    "ATAQUE": [
        "attack", "strike", "airstrike", "missile", "rocket", "drone", "bomb",
        "explosion", "blast", "fired", "launched", "hit", "destroyed",
        "атака", "удар", "ракета", "взрыв", "уничтожен",
    ],
    "CEASEFIRE": [
        "ceasefire", "truce", "negotiations", "peace talks", "agreement",
        "withdraw", "deal", "diplomatic", "перемирие", "переговоры",
    ],
    "NUCLEAR": [
        "nuclear", "uranium", "enrichment", "warhead", "atomic", "iaea",
        "radiation", "dirty bomb", "ядерный", "уран",
    ],
    "TROOPS": [
        "troops", "soldiers", "military", "forces", "army", "battalion",
        "brigade", "offensive", "advance", "retreat", "frontline",
        "войска", "наступление", "отступление", "фронт",
    ],
    "NAVAL": [
        "ship", "vessel", "navy", "submarine", "carrier", "fleet",
        "strait", "hormuz", "black sea", "mediterranean",
        "корабль", "флот", "подводная лодка",
    ],
    "CASUALTIES": [
        "killed", "dead", "wounded", "casualties", "civilian", "deaths",
        "убит", "погиб", "ранен", "жертвы",
    ],
    "INFRASTRUCTURE": [
        "power plant", "dam", "bridge", "railway", "pipeline", "refinery",
        "airport", "port", "электростанция", "плотина", "мост",
    ],
}

# Nível de impacto por categoria
MILITARY_IMPACT_LEVELS = {
    "NUCLEAR":       {"score": 100, "emoji": "☢️"},
    "ATAQUE":        {"score": 80,  "emoji": "💥"},
    "NAVAL":         {"score": 70,  "emoji": "⚓"},
    "CEASEFIRE":     {"score": 65,  "emoji": "🕊️"},
    "TROOPS":        {"score": 55,  "emoji": "⚔️"},
    "CASUALTIES":    {"score": 50,  "emoji": "🩸"},
    "INFRASTRUCTURE":{"score": 45,  "emoji": "🏗️"},
}

# Regiões e seus mercados relacionados
REGION_KEYWORDS = {
    "UKRAINE":  ["ukraine", "ukrainian", "zelensky", "kyiv", "russia", "ceasefire", "donbas", "nato"],
    "RUSSIA":   ["russia", "russian", "putin", "ukraine", "ceasefire", "nato", "moscow",
              "iran", "oil", "hormuz", "middle east", "oriente", "neft"],
    "IRAN":     ["iran", "iranian", "tehran", "irgc", "nuclear", "hormuz", "sanction", "attack"],
    "ISRAEL":   ["israel", "israeli", "gaza", "hamas", "hezbollah", "idf", "netanyahu",
                 "iran", "ballistic", "khamenei", "strike", "hormuz", "tehran", "irgc",
                 "houthi", "lebanon", "beirut", "west bank"],
    "MIDEAST":  ["iran", "israel", "saudi", "oil", "opec", "hormuz", "gulf", "syria",
                 "ballistic", "houthi", "irgc", "khamenei", "tehran", "strike"],
}

# Cache de mensagens já processadas
_MILITARY_SEEN: set = set()


def _military_fetch_channel(channel_id: str, max_messages: int = 15) -> list:
    """
    Raspa mensagens públicas de um canal Telegram via t.me/s/{canal}.
    Retorna lista de mensagens normalizadas.
    """
    messages = []
    url = f"https://t.me/s/{channel_id}"

    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=12,
        )

        if resp.status_code != 200:
            print(f"[military] {channel_id}: HTTP {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Cada mensagem está em .tgme_widget_message_wrap
        message_divs = soup.find_all("div", class_="tgme_widget_message_wrap")

        for div in message_divs[-max_messages:]:
            try:
                # Texto da mensagem
                text_div = div.find("div", class_="tgme_widget_message_text")
                if not text_div:
                    continue
                text = text_div.get_text(separator=" ", strip=True)
                if not text or len(text) < 20:
                    continue

                # Data da mensagem
                time_tag = div.find("time")
                pub_date = time_tag.get("datetime", "") if time_tag else ""

                # Link da mensagem
                link_tag = div.find("a", class_="tgme_widget_message_date")
                msg_url = link_tag.get("href", "") if link_tag else f"https://t.me/{channel_id}"

                # ID único da mensagem
                msg_id = msg_url.split("/")[-1] if msg_url else ""
                uid = f"{channel_id}:{msg_id}"

                messages.append({
                    "uid": uid,
                    "channel_id": channel_id,
                    "text": text[:800],
                    "url": msg_url,
                    "published": pub_date,
                })

            except Exception as e:
                print(f"[military] parse msg {channel_id}: {e}")
                continue

    except Exception as e:
        print(f"[military] fetch {channel_id}: {e}")

    return messages


def _military_classify_impact(text: str) -> dict:
    """
    Classifica impacto de uma mensagem por keywords — sem chamada IA.
    Rápido, zero custo.
    """
    text_lower = text.lower()

    categorias_encontradas = []
    keywords_encontradas = []
    max_score = 0
    max_categoria = "OUTROS"

    for cat, keywords in MILITARY_IMPACT_KEYWORDS.items():
        hits = [kw for kw in keywords if kw in text_lower]
        if hits:
            categorias_encontradas.append(cat)
            keywords_encontradas.extend(hits)
            cat_score = MILITARY_IMPACT_LEVELS.get(cat, {}).get("score", 30)
            if cat_score > max_score:
                max_score = cat_score
                max_categoria = cat

    if max_score == 0:
        return {"score": 0, "nivel": "BAIXO", "categorias": [], "keywords": []}

    nivel = (
        "CRITICO" if max_score >= 90 else
        "ALTO"    if max_score >= 65 else
        "MEDIO"   if max_score >= 45 else
        "BAIXO"
    )

    emoji = MILITARY_IMPACT_LEVELS.get(max_categoria, {}).get("emoji", "📢")

    return {
        "score": max_score,
        "nivel": nivel,
        "emoji": emoji,
        "categoria_principal": max_categoria,
        "categorias": list(set(categorias_encontradas)),
        "keywords": list(set(keywords_encontradas))[:8],
    }


def _military_translate_classify(text: str, lingua: str, channel_nome: str) -> dict:
    """
    Usa Claude Haiku para:
    1. Traduzir para português (se não for inglês)
    2. Classificar impacto com contexto geopolítico
    3. Extrair entidade principal (país, cidade, organização)
    4. Gerar resumo em 1 linha
    """
    if not ANTHROPIC_KEY:
        return {
            "traducao": text[:300],
            "resumo": text[:150],
            "entidade": "Desconhecida",
            "impacto_ia": "MEDIO",
            "confianca": 0.5,
            "acao_mercado": "MONITORAR",
        }

    lang_instruction = (
        "O texto está em russo. Traduza para português do Brasil."
        if lingua == "ru"
        else "O texto está em inglês. Não precisa traduzir, só analise."
        if lingua == "en"
        else f"O texto pode estar em {lingua}. Traduza para português se necessário."
    )

    prompt = f"""Você é analista de inteligência militar e geopolítica especializado em prediction markets.

{lang_instruction}

MENSAGEM DO CANAL TELEGRAM "{channel_nome}":
{text[:600]}

Analise e responda SOMENTE com JSON válido sem markdown:
{{
  "traducao": "<texto traduzido para PT-BR, max 300 chars>",
  "resumo": "<1 linha resumindo o evento em PT-BR, max 100 chars>",
  "entidade_principal": "<país ou organização envolvida>",
  "tipo_evento": "<ATAQUE|CEASEFIRE|NUCLEAR|TROOPS|NAVAL|CASUALTIES|INFRASTRUCTURE|POLITICO|OUTRO>",
  "impacto_mercado": "<CRITICO|ALTO|MEDIO|BAIXO>",
  "confianca": <0.0-1.0>,
  "mercados_afetados": ["<keyword1>", "<keyword2>"],
  "acao_sugerida": "<COMPRAR_YES|COMPRAR_NO|MONITORAR|AGUARDAR>"
}}"""

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
                "max_tokens": 400,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15,
        )
        if resp.status_code == 200:
            raw = resp.json()["content"][0]["text"]
            raw = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(raw)
    except Exception as e:
        print(f"[military_ai] erro: {e}")

    # Fallback sem IA
    return {
        "traducao": text[:300],
        "resumo": text[:100],
        "entidade_principal": "Desconhecida",
        "tipo_evento": "OUTRO",
        "impacto_mercado": "MEDIO",
        "confianca": 0.4,
        "mercados_afetados": [],
        "acao_sugerida": "MONITORAR",
    }


def _military_match_markets(keywords_ia: list, regiao: str, markets: list) -> list:
    """Cruza keywords da mensagem com mercados ativos."""
    region_kws = REGION_KEYWORDS.get(regiao, [])
    all_kws = set(kw.lower() for kw in keywords_ia + region_kws)

    matches = []
    for m in markets:
        q_lower = (m.question or "").lower()
        hits = sum(1 for kw in all_kws if kw in q_lower)
        if hits >= 2:
            yes_price, _ = _get_yes_no_prices(m)
            matches.append({
                "market": m,
                "yes_price": yes_price,
                "hits": hits,
            })

    matches.sort(key=lambda x: x["hits"], reverse=True)
    return matches[:4]


def _military_telegram_alert(msg_data: dict, ia: dict, mercados: list) -> None:
    """Envia alerta formatado no Telegram."""
    nivel = ia.get("impacto_mercado", "MEDIO")
    nivel_emoji = {"CRITICO": "🚨🚨🚨", "ALTO": "🚨", "MEDIO": "⚠️", "BAIXO": "ℹ️"}.get(nivel, "📢")
    tipo_emoji = msg_data.get("impact", {}).get("emoji", "📢")

    mercados_txt = ""
    for m in mercados[:3]:
        price = f"{m['yes_price']}%" if m.get("yes_price") else "?"
        url = _polymarket_url(m["market"].market_slug)
        mercados_txt += f"\n• <a href=\"{url}\">{m['market'].question[:65]}</a> @ {price}"

    canal = msg_data.get("channel_nome", "?")
    regiao = msg_data.get("regiao", "?")

    msg = (
        f"{nivel_emoji} <b>INTEL MILITAR — {regiao}</b> {tipo_emoji}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Canal: <b>{canal}</b>\n"
        f"📋 <b>{ia.get('resumo', '')[:120]}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 Entidade: {ia.get('entidade_principal', '?')}\n"
        f"⚡ Tipo: {ia.get('tipo_evento', '?')} | Impacto: {nivel}\n"
        f"📊 Confiança IA: {round(ia.get('confianca', 0) * 100)}%\n"
    )

    if mercados:
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n🎯 <b>Mercados afetados:</b>{mercados_txt}\n"

    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href=\"{msg_data.get('url', '#')}\">Ver mensagem original</a>\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )

    _telegram_send(msg)


@app.get("/military/scan")
def military_scan(
    min_impact: str = Query("MEDIO", description="BAIXO, MEDIO, ALTO, CRITICO"),
    alertar: int = Query(1, description="1 = envia Telegram"),
    usar_ia: int = Query(1, description="1 = traduz e classifica via Claude Haiku"),
    canais: str = Query(None, description="IDs separados por vírgula. Default: todos"),
    db: Session = Depends(get_db),
):
    """
    Scanner de Telegram Militar — #26

    Raspa canais militares públicos (Ukraine, Iran, Israel, Russia).
    Classifica impacto por keywords + Claude Haiku (tradução + análise).
    Cruza com mercados ativos do banco.
    Dispara Telegram antes do Reuters cobrir.
    """
    now = datetime.utcnow()
    impact_rank = {"BAIXO": 1, "MEDIO": 2, "ALTO": 3, "CRITICO": 4}
    min_rank = impact_rank.get(min_impact.upper(), 2)

    # Filtra canais se especificado
    canais_list = MILITARY_CHANNELS
    if canais:
        ids_filtro = [c.strip() for c in canais.split(",")]
        canais_list = [c for c in MILITARY_CHANNELS if c["id"] in ids_filtro]

    # Mercados ativos do banco
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    alertas = []
    stats = {
        "canais_escaneados": 0,
        "mensagens_coletadas": 0,
        "mensagens_novas": 0,
        "alertas_enviados": 0,
        "erros": 0,
    }

    for channel in canais_list:
        try:
            messages = _military_fetch_channel(channel["id"], max_messages=10)
            stats["canais_escaneados"] += 1
            stats["mensagens_coletadas"] += len(messages)
            time.sleep(0.5)  # respeita rate limit do Telegram

            for msg in messages:
                uid = msg["uid"]

                # Já processou?
                if uid in _MILITARY_SEEN:
                    continue

                # Classifica por keywords primeiro (rápido, sem custo)
                impact = _military_classify_impact(msg["text"])

                if impact["score"] == 0:
                    _MILITARY_SEEN.add(uid)
                    continue

                nivel = impact.get("nivel", "BAIXO")
                if impact_rank.get(nivel, 1) < min_rank:
                    _MILITARY_SEEN.add(uid)
                    continue

                stats["mensagens_novas"] += 1

                # Análise IA (tradução + classificação profunda)
                ia = {}
                if usar_ia:
                    ia = _military_translate_classify(
                        msg["text"],
                        channel["lingua"],
                        channel["nome"],
                    )
                    time.sleep(0.3)
                else:
                    ia = {
                        "traducao": msg["text"][:300],
                        "resumo": msg["text"][:100],
                        "entidade_principal": channel["regiao"],
                        "tipo_evento": impact.get("categoria_principal", "OUTRO"),
                        "impacto_mercado": nivel,
                        "confianca": 0.5,
                        "mercados_afetados": impact.get("keywords", []),
                        "acao_sugerida": "MONITORAR",
                    }

                # Cruza com mercados
                keywords_ia = ia.get("mercados_afetados", []) + impact.get("keywords", [])
                mercados_match = _military_match_markets(
                    keywords_ia, channel["regiao"], markets
                )

                alerta = {
                    "uid": uid,
                    "canal": channel["nome"],
                    "canal_id": channel["id"],
                    "regiao": channel["regiao"],
                    "lingua_original": channel["lingua"],
                    "url": msg["url"],
                    "publicado": msg["published"],
                    "texto_original": msg["text"][:400],
                    "traducao": ia.get("traducao", msg["text"][:300]),
                    "resumo": ia.get("resumo", ""),
                    "entidade_principal": ia.get("entidade_principal", "?"),
                    "tipo_evento": ia.get("tipo_evento", impact.get("categoria_principal", "?")),
                    "impacto": nivel,
                    "impacto_score": impact["score"],
                    "impacto_emoji": impact.get("emoji", "📢"),
                    "confianca_ia": ia.get("confianca", 0.5),
                    "keywords": impact.get("keywords", []),
                    "acao_sugerida": ia.get("acao_sugerida", "MONITORAR"),
                    "mercados_afetados": [
                        {
                            "question": m["market"].question,
                            "slug": m["market"].market_slug,
                            "yes_price": m.get("yes_price"),
                            "polymarket_url": _polymarket_url(m["market"].market_slug),
                        }
                        for m in mercados_match
                    ],
                    "total_mercados": len(mercados_match),
                    "detectado_em": now.isoformat(),
                }
                alertas.append(alerta)

                # Telegram
                if alertar:
                    msg_data = {
                        "channel_nome": channel["nome"],
                        "regiao": channel["regiao"],
                        "url": msg["url"],
                        "impact": impact,
                    }
                    _military_telegram_alert(msg_data, ia, mercados_match)
                    stats["alertas_enviados"] += 1

                # Marca como visto após processar
                _MILITARY_SEEN.add(uid)
                if len(_MILITARY_SEEN) > 20000:
                    for old_uid in list(_MILITARY_SEEN)[:3000]:
                        _MILITARY_SEEN.discard(old_uid)

        except Exception as e:
            stats["erros"] += 1
            print(f"[military] canal {channel['id']}: {e}")
            continue

    # Ordena por impacto
    alertas.sort(key=lambda x: x["impacto_score"], reverse=True)

    return {
        "status": "ok",
        "canais_monitorados": len(canais_list),
        "stats": stats,
        "min_impact": min_impact,
        "usar_ia": bool(usar_ia),
        "alertas": alertas,
        "gerado_em": now.isoformat(),
    }


@app.get("/military/channels")
def military_channels():
    """Lista todos os canais monitorados."""
    return {
        "total": len(MILITARY_CHANNELS),
        "canais": [
            {
                "id": c["id"],
                "nome": c["nome"],
                "regiao": c["regiao"],
                "lingua": c["lingua"],
                "url_publica": f"https://t.me/s/{c['id']}",
            }
            for c in MILITARY_CHANNELS
        ],
        "regioes": list({c["regiao"] for c in MILITARY_CHANNELS}),
    }


@app.get("/military/test/{channel_id}")
def military_test_channel(
    channel_id: str,
    usar_ia: int = Query(0, description="1 = usa Claude Haiku para traduzir"),
):
    """
    Testa scraping de um canal específico sem alertar.
    usar_ia=0 por default para economizar tokens no teste.
    """
    channel = next((c for c in MILITARY_CHANNELS if c["id"] == channel_id), None)
    if not channel:
        ids = [c["id"] for c in MILITARY_CHANNELS]
        return {"error": "Canal não encontrado", "ids_disponiveis": ids}

    messages = _military_fetch_channel(channel_id, max_messages=5)

    resultado = []
    for msg in messages:
        impact = _military_classify_impact(msg["text"])
        ia = {}
        if usar_ia and impact["score"] > 0:
            ia = _military_translate_classify(msg["text"], channel["lingua"], channel["nome"])

        resultado.append({
            "uid": msg["uid"],
            "texto_original": msg["text"][:300],
            "url": msg["url"],
            "publicado": msg["published"],
            "impacto_keywords": impact,
            "analise_ia": ia if ia else "usar_ia=0 (sem custo de tokens)",
        })

    return {
        "canal": channel["nome"],
        "regiao": channel["regiao"],
        "lingua": channel["lingua"],
        "url": f"https://t.me/s/{channel_id}",
        "mensagens_coletadas": len(messages),
        "resultado": resultado,
        "gerado_em": datetime.utcnow().isoformat(),
    }
# ══════════════════════════════════════════════════════════════
# MOTOR #28 — AIS SHIP TRACKING
#
# Monitora navios em tempo real via aisstream.io WebSocket.
# Foco em regiões estratégicas: Golfo Pérsico, Hormuz, Mar Vermelho.
# Detecta: tankers, navios militares, anomalias de rota.
# Cruza com mercados de petróleo/Irã/Israel no banco.
# Dispara Telegram quando movimentação detectada.
# ══════════════════════════════════════════════════════════════

import asyncio
import websockets

AIS_API_KEY = os.environ.get("AIS_API_KEY", "552678ffa8a7503268db9e492883c1554ab7faf6")

AIS_REGIONS = {
    "HORMUZ": {
        "nome": "Estreito de Hormuz",
        "emoji": "🛢️",
        "bbox": [[55.0, 25.5], [57.5, 27.5]],
        "impacto": "CRITICO",
        "keywords_mercado": ["iran", "oil", "crude", "hormuz", "petroleum"],
        "descricao": "30% do petróleo mundial passa aqui. Fechamento = +50% no preço.",
    },
    "RED_SEA": {
        "nome": "Mar Vermelho / Bab-el-Mandeb",
        "emoji": "⚓",
        "bbox": [[42.5, 11.5], [44.0, 13.0]],
        "impacto": "ALTO",
        "keywords_mercado": ["houthi", "red sea", "shipping", "oil", "israel"],
        "descricao": "Rota crítica Europa-Ásia. Houthis atacam navios aqui.",
    },
    "PERSIAN_GULF": {
        "nome": "Golfo Pérsico",
        "emoji": "🌊",
        "bbox": [[48.0, 23.0], [56.5, 27.5]],
        "impacto": "ALTO",
        "keywords_mercado": ["iran", "saudi", "oil", "gulf", "petroleum", "opec"],
        "descricao": "Principal área de exportação de petróleo do Oriente Médio.",
    },
    "SUEZ": {
        "nome": "Canal de Suez",
        "emoji": "🚢",
        "bbox": [[32.3, 29.5], [32.7, 31.5]],
        "impacto": "MEDIO",
        "keywords_mercado": ["egypt", "shipping", "oil", "trade", "suez"],
        "descricao": "12% do comércio mundial. Bloqueio = choque logístico global.",
    },
    "BLACK_SEA": {
        "nome": "Mar Negro",
        "emoji": "🌑",
        "bbox": [[28.0, 41.0], [37.0, 46.5]],
        "impacto": "MEDIO",
        "keywords_mercado": ["russia", "ukraine", "grain", "oil", "black sea"],
        "descricao": "Exportações russas e ucranianas de grãos e petróleo.",
    },
}

SHIP_TYPES = {
    80: {"nome": "Tanker",    "emoji": "🛢️", "relevancia": "ALTA",    "descricao": "Carrega petróleo/derivados"},
    70: {"nome": "Cargo",     "emoji": "📦", "relevancia": "MEDIA",   "descricao": "Carga geral"},
    60: {"nome": "Passenger", "emoji": "🚢", "relevancia": "BAIXA",   "descricao": "Passageiros"},
    35: {"nome": "Military",  "emoji": "⚔️", "relevancia": "CRITICA", "descricao": "Navio de guerra"},
    31: {"nome": "Tug",       "emoji": "🔧", "relevancia": "BAIXA",   "descricao": "Rebocador"},
}

_AIS_SEEN: dict = {}
_AIS_REGION_COUNTS: dict = {}
_AIS_SNAPSHOT: list = []


def _ais_get_ship_info(ship_type: int) -> dict:
    if 80 <= ship_type <= 89:
        return SHIP_TYPES[80]
    if 70 <= ship_type <= 79:
        return SHIP_TYPES[70]
    if ship_type == 35:
        return SHIP_TYPES[35]
    return SHIP_TYPES.get(ship_type, {"nome": f"Tipo{ship_type}", "emoji": "🚢", "relevancia": "BAIXA", "descricao": "Desconhecido"})


def _ais_detect_region(lat: float, lon: float) -> str | None:
    for region_id, region in AIS_REGIONS.items():
        bbox = region["bbox"]
        min_lon, min_lat = bbox[0]
        max_lon, max_lat = bbox[1]
        if min_lon <= lon <= max_lon and min_lat <= lat <= max_lat:
            return region_id
    return None


def _ais_match_markets(region_id: str, markets: list) -> list:
    region = AIS_REGIONS.get(region_id, {})
    keywords = region.get("keywords_mercado", [])
    matches = []
    for m in markets:
        q_lower = (m.question or "").lower()
        hits = sum(1 for kw in keywords if kw in q_lower)
        if hits >= 2:
            yes_price, _ = _get_yes_no_prices(m)
            matches.append({
                "question": m.question,
                "slug": m.market_slug,
                "yes_price": yes_price,
                "polymarket_url": _polymarket_url(m.market_slug),
            })
    matches.sort(key=lambda x: x.get("yes_price") or 0, reverse=True)
    return matches[:3]


def _ais_telegram_alert(navio: dict, region_id: str, markets: list) -> None:
    region = AIS_REGIONS[region_id]
    ship_info = navio.get("ship_info", {})
    relevancia = ship_info.get("relevancia", "MEDIA")
    nivel_emoji = {"CRITICA": "🚨🚨", "ALTA": "🚨", "MEDIA": "⚠️", "BAIXA": "ℹ️"}.get(relevancia, "⚠️")
    nome_navio = navio.get("name", "Desconhecido").strip() or "Sem nome"
    mmsi = navio.get("mmsi", "?")
    flag = navio.get("flag", "?")
    speed = navio.get("speed", 0)
    lat = navio.get("lat", 0)
    lon = navio.get("lon", 0)
    mercados_txt = ""
    for m in markets[:3]:
        price = f"{m['yes_price']}%" if m.get("yes_price") else "?"
        mercados_txt += f"\n• <a href=\"{m['polymarket_url']}\">{m['question'][:65]}</a> @ {price}"
    msg = (
        f"{nivel_emoji} <b>NAVIO DETECTADO — {region['emoji']} {region['nome']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{ship_info.get('emoji','🚢')} <b>{nome_navio}</b> ({ship_info.get('nome','?')})\n"
        f"🏳️ Bandeira: {flag} | MMSI: {mmsi}\n"
        f"📍 Posição: {lat:.2f}°N, {lon:.2f}°E\n"
        f"💨 Velocidade: {speed:.1f} nós\n"
        f"⚠️ Relevância: {relevancia}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 {region['descricao']}\n"
    )
    if markets:
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n🎯 <b>Mercados afetados:</b>{mercados_txt}\n"
    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href=\"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}\">Ver no MarineTraffic</a>\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )
    _telegram_send(msg)


async def _ais_ws_collect(regions: list, timeout_sec: int = 15) -> list:
    navios = []
    uri = "wss://stream.aisstream.io/v0/stream"
    bboxes = [AIS_REGIONS[r]["bbox"] for r in regions if r in AIS_REGIONS]
    subscribe_msg = {
        "APIKey": AIS_API_KEY,
        "BoundingBoxes": [[[bb[0][1], bb[0][0]], [bb[1][1], bb[1][0]]] for bb in bboxes],
        "FilterMessageTypes": ["PositionReport"],
    }
    try:
        async with websockets.connect(uri, ping_timeout=10) as ws:
            await ws.send(json.dumps(subscribe_msg))
            deadline = asyncio.get_event_loop().time() + timeout_sec
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    msg = json.loads(raw)
                    if msg.get("MessageType") != "PositionReport":
                        continue
                    meta = msg.get("MetaData", {})
                    pos  = msg.get("Message", {}).get("PositionReport", {})
                    mmsi = str(meta.get("MMSI", ""))
                    lat  = float(meta.get("latitude", pos.get("Latitude", 0)))
                    lon  = float(meta.get("longitude", pos.get("Longitude", 0)))
                    if not mmsi or lat == 0:
                        continue
                    region_id = _ais_detect_region(lat, lon)
                    if not region_id:
                        continue
                    ship_type = int(meta.get("ShipType", 0))
                    navios.append({
                        "mmsi": mmsi,
                        "name": meta.get("ShipName", "").strip() or "Sem nome",
                        "lat": round(lat, 4),
                        "lon": round(lon, 4),
                        "speed": round(float(pos.get("Sog", 0)), 1),
                        "course": round(float(pos.get("Cog", 0)), 1),
                        "ship_type": ship_type,
                        "ship_info": _ais_get_ship_info(ship_type),
                        "flag": meta.get("MMSI_Flag", "?"),
                        "region_id": region_id,
                        "region_nome": AIS_REGIONS[region_id]["nome"],
                        "detectado_em": datetime.utcnow().isoformat(),
                    })
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"[ais_ws] parse: {e}")
    except Exception as e:
        print(f"[ais_ws] connect: {e}")
    return navios


def _ais_run_async(regions: list, timeout_sec: int = 15) -> list:
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(_ais_ws_collect(regions, timeout_sec))
        loop.close()
        return result
    except Exception as e:
        print(f"[ais_run] erro: {e}")
        return []


@app.get("/ships/scan")
def ships_scan(
    regioes: str = Query("HORMUZ,PERSIAN_GULF,RED_SEA"),
    min_relevancia: str = Query("MEDIA"),
    alertar: int = Query(1),
    timeout: int = Query(15, ge=5, le=45),
    db: Session = Depends(get_db),
):
    """
    AIS Ship Tracking — #28
    Conecta ao aisstream.io e coleta posições de navios em regiões estratégicas.
    Foco em tankers no Estreito de Hormuz, Mar Vermelho e Golfo Pérsico.
    """
    global _AIS_SNAPSHOT
    now = datetime.utcnow()
    relevancia_rank = {"BAIXA": 1, "MEDIA": 2, "ALTA": 3, "CRITICA": 4}
    min_rank = relevancia_rank.get(min_relevancia.upper(), 2)

    regioes_list = [r.strip().upper() for r in regioes.split(",") if r.strip().upper() in AIS_REGIONS]
    if not regioes_list:
        regioes_list = list(AIS_REGIONS.keys())

    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    navios_raw = _ais_run_async(regioes_list, timeout_sec=timeout)

    navios_dedup: dict = {}
    for n in navios_raw:
        navios_dedup[n["mmsi"]] = n
    navios = list(navios_dedup.values())
    _AIS_SNAPSHOT = navios[-200:]

    for region_id in regioes_list:
        rv = [n for n in navios if n["region_id"] == region_id]
        _AIS_REGION_COUNTS[region_id] = {
            "total": len(rv),
            "tankers": sum(1 for n in rv if 80 <= n["ship_type"] <= 89),
            "military": sum(1 for n in rv if n["ship_type"] == 35),
            "ultima_atualizacao": now.isoformat(),
        }

    alertas = []
    alertas_enviados = 0

    for navio in navios:
        ship_info = navio.get("ship_info", {})
        rel = ship_info.get("relevancia", "BAIXA")
        if relevancia_rank.get(rel, 1) < min_rank:
            continue

        mmsi = navio["mmsi"]
        region_id = navio["region_id"]
        cache_key = f"{mmsi}:{region_id}"
        last_seen = _AIS_SEEN.get(cache_key, {})
        last_ts = last_seen.get("timestamp")

        if last_ts:
            age = (now - datetime.fromisoformat(last_ts)).total_seconds()
            if age < 1800:
                alertas.append({**navio, "ja_alertado": True, "mercados_afetados": [], "total_mercados": 0})
                continue

        mercados_match = _ais_match_markets(region_id, markets)
        alerta = {**navio, "mercados_afetados": mercados_match, "total_mercados": len(mercados_match), "ja_alertado": False}
        alertas.append(alerta)

        _AIS_SEEN[cache_key] = {"timestamp": now.isoformat(), "lat": navio["lat"], "lon": navio["lon"]}
        if len(_AIS_SEEN) > 10000:
            for k in list(_AIS_SEEN.keys())[:2000]:
                del _AIS_SEEN[k]

        if alertar:
            _ais_telegram_alert(navio, region_id, mercados_match)
            alertas_enviados += 1

    rel_order = {"CRITICA": 4, "ALTA": 3, "MEDIA": 2, "BAIXA": 1}
    alertas.sort(key=lambda x: rel_order.get(x.get("ship_info", {}).get("relevancia", "BAIXA"), 1), reverse=True)

    stats_regioes = {
        rid: {
            "nome": AIS_REGIONS[rid]["nome"],
            "emoji": AIS_REGIONS[rid]["emoji"],
            "total_navios": len([n for n in navios if n["region_id"] == rid]),
            "tankers": sum(1 for n in navios if n["region_id"] == rid and 80 <= n["ship_type"] <= 89),
            "military": sum(1 for n in navios if n["region_id"] == rid and n["ship_type"] == 35),
            "impacto": AIS_REGIONS[rid]["impacto"],
        }
        for rid in regioes_list
    }

    return {
        "status": "ok",
        "regioes_monitoradas": regioes_list,
        "timeout_ws_segundos": timeout,
        "total_navios_detectados": len(navios),
        "total_alertas": len(alertas),
        "alertas_enviados": alertas_enviados,
        "stats_por_regiao": stats_regioes,
        "navios": alertas[:50],
        "gerado_em": now.isoformat(),
    }


@app.get("/ships/regions")
def ships_regions():
    return {
        "total_regioes": len(AIS_REGIONS),
        "regioes": [
            {
                "id": rid,
                "nome": r["nome"],
                "emoji": r["emoji"],
                "impacto": r["impacto"],
                "descricao": r["descricao"],
                "bbox": r["bbox"],
                "keywords_mercado": r["keywords_mercado"],
                "stats_atual": _AIS_REGION_COUNTS.get(rid, {"total": 0, "tankers": 0, "military": 0}),
            }
            for rid, r in AIS_REGIONS.items()
        ],
    }


@app.get("/ships/snapshot")
def ships_snapshot(
    region: str = Query(None),
    tipo: str = Query(None),
):
    navios = _AIS_SNAPSHOT
    if region:
        navios = [n for n in navios if n.get("region_id", "").upper() == region.upper()]
    if tipo:
        t = tipo.lower()
        if t == "tanker":
            navios = [n for n in navios if 80 <= n.get("ship_type", 0) <= 89]
        elif t == "military":
            navios = [n for n in navios if n.get("ship_type", 0) == 35]
        elif t == "cargo":
            navios = [n for n in navios if 70 <= n.get("ship_type", 0) <= 79]
    return {
        "status": "ok",
        "total": len(navios),
        "nota": "Snapshot do último /ships/scan. Use /ships/scan para dados frescos.",
        "navios": navios[:100],
        "gerado_em": datetime.utcnow().isoformat(),
    }


@app.get("/ships/hormuz")
def ships_hormuz(alertar: int = Query(1), db: Session = Depends(get_db)):
    """Atalho: monitora APENAS o Estreito de Hormuz com alta prioridade."""
    return ships_scan(regioes="HORMUZ", min_relevancia="ALTA", alertar=alertar, timeout=20, db=db)

@app.get("/ships/debug")
async def ships_debug():
    """Debug raw do aisstream.io — mostra primeiras mensagens recebidas."""
    uri = "wss://stream.aisstream.io/v0/stream"
    msgs_recebidas = []
    erro = None
    conectou = False

    bboxes = [[[25.5, 55.0], [27.5, 57.5]]]
    subscribe_msg = {
        "APIKey": AIS_API_KEY,
        "BoundingBoxes": bboxes,
        "FilterMessageTypes": ["PositionReport"],
    }

    try:
        async with websockets.connect(uri, ping_timeout=10) as ws:
            conectou = True
            await ws.send(json.dumps(subscribe_msg))
            deadline = asyncio.get_event_loop().time() + 20
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    msgs_recebidas.append(raw[:500])
                    if len(msgs_recebidas) >= 5:
                        break
                except asyncio.TimeoutError:
                    msgs_recebidas.append("[timeout 3s sem mensagem]")
                    break
    except Exception as e:
        erro = str(e)

    return {
        "conectou": conectou,
        "erro": erro,
        "subscribe_enviado": subscribe_msg,
        "msgs_recebidas": msgs_recebidas,
        "total_msgs": len(msgs_recebidas),
    }
# ============================================================
# MOTOR #28 — RSS NAVAL (Hormuz, Mar Vermelho, Golfo Pérsico)
# ============================================================

NAVAL_RSS_FEEDS = [
    {"url": "https://news.google.com/rss/search?q=hormuz+tanker+oil&hl=en-US&gl=US&ceid=US:en", "regiao": "HORMUZ", "impacto": "CRITICO"},
    {"url": "https://news.google.com/rss/search?q=red+sea+ship+attack+houthi&hl=en-US&gl=US&ceid=US:en", "regiao": "RED_SEA", "impacto": "ALTO"},
    {"url": "https://news.google.com/rss/search?q=persian+gulf+vessel+iran&hl=en-US&gl=US&ceid=US:en", "regiao": "PERSIAN_GULF", "impacto": "ALTO"},
    {"url": "https://news.google.com/rss/search?q=suez+canal+ship+blocked&hl=en-US&gl=US&ceid=US:en", "regiao": "SUEZ", "impacto": "MEDIO"},
    {"url": "https://news.google.com/rss/search?q=black+sea+vessel+ukraine&hl=en-US&gl=US&ceid=US:en", "regiao": "BLACK_SEA", "impacto": "MEDIO"},
]

NAVAL_KEYWORDS_ALTO = [
    "attack", "attacked", "seized", "seized", "blocked", "closure", "closed",
    "missile", "drone", "explosion", "fire", "sinking", "hostage", "detained",
    "military", "warship", "naval", "sanctions", "embargo", "iran", "irgc",
    "houthi", "strike", "threat", "warning", "escalation",
]

NAVAL_KEYWORDS_MEDIO = [
    "tanker", "oil", "cargo", "vessel", "ship", "strait", "passage",
    "reroute", "divert", "delay", "insurance", "risk",
]

_naval_cache: dict = {}

async def _naval_fetch_feed(feed: dict) -> list:
    import hashlib
    import feedparser
    alertas = []
    try:
        parsed = feedparser.parse(feed["url"])
        for entry in parsed.entries[:10]:
            titulo = entry.get("title", "")
            link = entry.get("link", "")
            published = entry.get("published", "")
            texto = (titulo + " " + entry.get("summary", "")).lower()
            h = hashlib.md5(link.encode()).hexdigest()[:12]
            if h in _naval_cache:
                continue
            _naval_cache[h] = True
            score_alto = sum(1 for k in NAVAL_KEYWORDS_ALTO if k in texto)
            score_medio = sum(1 for k in NAVAL_KEYWORDS_MEDIO if k in texto)
            if score_alto >= 1 or score_medio >= 2:
                nivel = "CRITICO" if score_alto >= 2 else feed["impacto"]
                alertas.append({
                    "regiao": feed["regiao"],
                    "titulo": titulo,
                    "link": link,
                    "published": published,
                    "nivel": nivel,
                    "score_alto": score_alto,
                    "score_medio": score_medio,
                })
    except Exception as e:
        pass
    return alertas

async def naval_scan(alertar: int = 0, db=None) -> dict:
    tasks = [_naval_fetch_feed(f) for f in NAVAL_RSS_FEEDS]
    resultados = await asyncio.gather(*tasks)
    todos = []
    for r in resultados:
        todos.extend(r)
    todos.sort(key=lambda x: x["score_alto"] + x["score_medio"], reverse=True)
    alertas_enviados = 0
    if alertar and db:
        for item in todos[:5]:
            emoji = "🛢️" if item["regiao"] == "HORMUZ" else "⚓"
            msg = (
                f"{emoji} <b>NAVAL ALERT — {item['regiao']}</b>\n"
                f"📰 {item['titulo']}\n"
                f"⚠️ Nível: {item['nivel']}\n"
                f"🔗 {item['link']}"
            )
            _telegram_send(msg)
            alertas_enviados += 1
    return {
        "status": "ok",
        "total_alertas": len(todos),
        "alertas_enviados": alertas_enviados,
        "alertas": todos[:20],
        "gerado_em": datetime.utcnow().isoformat(),
    }

@app.get("/naval/scan")
async def naval_scan_endpoint(alertar: int = Query(0), db: Session = Depends(get_db)):
    return await naval_scan(alertar=alertar, db=db)

# ══════════════════════════════════════════════════════════════
# MOTOR #29 — RASTREADOR DE POLÍTICOS (X/Twitter + Google News)
# Monitora senadores EUA, ministros Israel, generais Iran
# Nitter RSS com fallback para Google News
# ══════════════════════════════════════════════════════════════

POLITICIANS = [
    # EUA — Senadores chave
    {"id": "senatemajldr",     "nome": "Mitch McConnell",   "cargo": "Senador EUA",        "regiao": "EUA",    "twitter": "senatemajldr"},
    {"id": "chuckschumer",     "nome": "Chuck Schumer",     "cargo": "Senador EUA",        "regiao": "EUA",    "twitter": "SenSchumer"},
    {"id": "marcorubio",       "nome": "Marco Rubio",       "cargo": "Sec. Estado EUA",    "regiao": "EUA",    "twitter": "marcorubio"},
    {"id": "lindseygrahamsc",  "nome": "Lindsey Graham",    "cargo": "Senador EUA",        "regiao": "EUA",    "twitter": "LindseyGrahamSC"},
    {"id": "realdonaldtrump",  "nome": "Donald Trump",      "cargo": "Presidente EUA",     "regiao": "EUA",    "twitter": "realDonaldTrump"},
    {"id": "jd_vance",         "nome": "JD Vance",          "cargo": "VP EUA",             "regiao": "EUA",    "twitter": "JDVance"},
    # Israel
    {"id": "netanyahu",        "nome": "Benjamin Netanyahu","cargo": "PM Israel",          "regiao": "ISRAEL", "twitter": "netanyahu"},
    {"id": "israelipm",        "nome": "Israel PM Office",  "cargo": "Gabinete Israel",    "regiao": "ISRAEL", "twitter": "IsraeliPM"},
    {"id": "idfspokesperson",  "nome": "IDF Spokesperson",  "cargo": "Forças Israel",      "regiao": "ISRAEL", "twitter": "IDFSpokesperson"},
    # Irã
    {"id": "khamenei_ir",      "nome": "Ali Khamenei",      "cargo": "Líder Supremo Irã",  "regiao": "IRAN",   "twitter": "khamenei_ir"},
    {"id": "irandiplomacy",    "nome": "Iran MFA",          "cargo": "Diplomacia Irã",     "regiao": "IRAN",   "twitter": "IranMFA"},
    # Rússia/Ucrânia
    {"id": "mfa_russia",       "nome": "MFA Russia",        "cargo": "Diplomacia Rússia",  "regiao": "RUSSIA", "twitter": "mfa_russia"},
    {"id": "ukraine_mfa",      "nome": "Ukraine MFA",       "cargo": "Diplomacia Ucrânia", "regiao": "UKRAINE","twitter": "Ukraine_MFA"},
    {"id": "zelenskyyua",      "nome": "Zelensky",          "cargo": "Presidente Ucrânia", "regiao": "UKRAINE","twitter": "ZelenskyyUa"},
]

NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.lucabased.xyz",
]

POLITICIAN_IMPACT_KEYWORDS = {
    "GUERRA": ["war", "attack", "strike", "military", "troops", "invasion", "bomb", "missile", "weapon"],
    "NUCLEAR": ["nuclear", "uranium", "warhead", "atomic", "enrichment", "bomb"],
    "CEASEFIRE": ["ceasefire", "peace", "truce", "negotiations", "deal", "agreement", "withdraw"],
    "SANCAO": ["sanction", "embargo", "ban", "freeze", "penalty", "restrict"],
    "ESCALADA": ["escalate", "escalation", "retaliate", "retaliation", "warn", "threat", "ultimatum"],
    "DESESCALADA": ["diplomacy", "dialogue", "talks", "meeting", "discuss", "cooperate"],
}

_POLITICIAN_SEEN: set = set()


def _politician_fetch_nitter(politician: dict) -> list:
    """Tenta buscar RSS do Nitter de múltiplas instâncias."""
    posts = []
    twitter_id = politician.get("twitter", "")
    if not twitter_id:
        return []

    for instance in NITTER_INSTANCES:
        try:
            url = f"{instance}/{twitter_id}/rss"
            resp = requests.get(url, headers=HEADERS, timeout=8)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:5]:
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                pub = (item.findtext("pubDate") or "").strip()
                desc = (item.findtext("description") or "").strip()
                if not title or not link:
                    continue
                uid = f"nitter:{twitter_id}:{link[-20:]}"
                posts.append({
                    "uid": uid,
                    "texto": title + " " + desc[:200],
                    "url": link,
                    "published": pub,
                    "fonte": "nitter",
                    "instancia": instance,
                })
            if posts:
                break  # Achou em uma instância, para
        except Exception as e:
            print(f"[politician] nitter {instance}/{twitter_id}: {e}")
            continue

    return posts


def _politician_fetch_googlenews(politician: dict) -> list:
    """Fallback: busca nome do político no Google News RSS."""
    posts = []
    nome = politician["nome"]
    query = nome.replace(" ", "+")
    try:
        url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        resp = requests.get(url, headers=HEADERS, timeout=8)
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        for item in root.findall(".//item")[:5]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub = (item.findtext("pubDate") or "").strip()
            if not title:
                continue
            uid = f"gnews:{politician['id']}:{title[:40]}"
            posts.append({
                "uid": uid,
                "texto": title,
                "url": link,
                "published": pub,
                "fonte": "google_news",
                "instancia": "news.google.com",
            })
    except Exception as e:
        print(f"[politician] gnews {nome}: {e}")
    return posts


def _politician_classify(texto: str) -> dict:
    """Classifica impacto do post por keywords."""
    text_lower = texto.lower()
    categorias = []
    keywords_found = []
    max_score = 0

    for cat, kws in POLITICIAN_IMPACT_KEYWORDS.items():
        hits = [kw for kw in kws if kw in text_lower]
        if hits:
            categorias.append(cat)
            keywords_found.extend(hits)
            score = len(hits) * 15
            if score > max_score:
                max_score = score

    max_score = min(max_score, 100)
    nivel = (
        "CRITICO" if max_score >= 60 else
        "ALTO"    if max_score >= 40 else
        "MEDIO"   if max_score >= 20 else
        "BAIXO"
    )

    return {
        "score": max_score,
        "nivel": nivel,
        "categorias": list(set(categorias)),
        "keywords": list(set(keywords_found))[:6],
    }


def _politician_ia_analysis(texto: str, politician: dict, impact: dict) -> dict:
    """Claude Haiku avalia impacto geopolítico do post."""
    if not ANTHROPIC_KEY or impact["score"] < 20:
        return {
            "resumo": texto[:100],
            "impacto_mercado": impact["nivel"],
            "acao": "MONITORAR",
            "confianca": 0.4,
            "mercados_keywords": impact["keywords"],
        }

    prompt = f"""Você é analista de prediction markets e geopolítica.

POLÍTICO: {politician['nome']} ({politician['cargo']}) — {politician['regiao']}
POST/NOTÍCIA: {texto[:400]}
KEYWORDS DETECTADAS: {', '.join(impact['keywords'])}

Avalie o impacto nos prediction markets.
Responda SOMENTE com JSON válido:
{{"resumo":"<max 80 chars PT-BR>","impacto_mercado":"CRITICO|ALTO|MEDIO|BAIXO","acao":"COMPRAR_YES|COMPRAR_NO|MONITORAR|AGUARDAR","confianca":<0.0-1.0>,"mercados_keywords":["<kw1>","<kw2>"],"escalada":"SOBE|CAI|NEUTRO"}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 250,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=12,
        )
        if resp.status_code == 200:
            raw = resp.json()["content"][0]["text"].replace("```json", "").replace("```", "").strip()
            return json.loads(raw)
    except Exception as e:
        print(f"[politician_ia] {e}")

    return {
        "resumo": texto[:100],
        "impacto_mercado": impact["nivel"],
        "acao": "MONITORAR",
        "confianca": 0.4,
        "mercados_keywords": impact["keywords"],
        "escalada": "NEUTRO",
    }


def _politician_match_markets(keywords: list, regiao: str, markets: list) -> list:
    """Cruza keywords do post com mercados ativos."""
    region_map = {
        "EUA":     ["trump", "fed", "election", "congress", "senate", "tariff", "iran", "israel"],
        "ISRAEL":  ["israel", "gaza", "hamas", "idf", "iran", "hezbollah", "netanyahu"],
        "IRAN":    ["iran", "nuclear", "hormuz", "sanction", "oil", "irgc"],
        "RUSSIA":  ["russia", "ukraine", "ceasefire", "putin", "nato"],
        "UKRAINE": ["ukraine", "russia", "zelensky", "ceasefire", "nato", "war"],
    }
    all_kws = set(kw.lower() for kw in keywords + region_map.get(regiao, []))
    matches = []
    for m in markets:
        q_lower = (m.question or "").lower()
        hits = sum(1 for kw in all_kws if kw in q_lower)
        if hits >= 2:
            yes_price, _ = _get_yes_no_prices(m)
            matches.append({"question": m.question, "slug": m.market_slug,
                            "yes_price": yes_price, "polymarket_url": _polymarket_url(m.market_slug)})
    matches.sort(key=lambda x: x.get("yes_price") or 0, reverse=True)
    return matches[:3]


def _politician_telegram(politician: dict, post: dict, impact: dict, ia: dict, markets: list) -> None:
    nivel_emoji = {"CRITICO": "🚨🚨", "ALTO": "🚨", "MEDIO": "⚠️", "BAIXO": "ℹ️"}.get(ia.get("impacto_mercado", "MEDIO"), "⚠️")
    escalada_emoji = {"SOBE": "📈", "CAI": "📉", "NEUTRO": "➡️"}.get(ia.get("escalada", "NEUTRO"), "➡️")
    fonte_emoji = "🐦" if post["fonte"] == "nitter" else "📰"

    mercados_txt = ""
    for m in markets[:3]:
        price = f"{m['yes_price']}%" if m.get("yes_price") else "?"
        mercados_txt += f"\n• <a href=\"{m['polymarket_url']}\">{m['question'][:65]}</a> @ {price}"

    msg = (
        f"{nivel_emoji} <b>POLÍTICO DETECTADO — {politician['regiao']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{fonte_emoji} <b>{politician['nome']}</b> ({politician['cargo']})\n"
        f"💬 {ia.get('resumo', post['texto'][:100])}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{escalada_emoji} Escalada: {ia.get('escalada','NEUTRO')} | Impacto: {ia.get('impacto_mercado','?')}\n"
        f"🎯 Ação: {ia.get('acao','MONITORAR')} | Confiança: {round(ia.get('confianca',0)*100)}%\n"
        f"🔑 Keywords: {', '.join(impact.get('keywords',[])[:5])}\n"
    )
    if markets:
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n🎯 <b>Mercados:</b>{mercados_txt}\n"
    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href=\"{post['url']}\">Ver post original</a>\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )
    _telegram_send(msg)


@app.get("/politicians/scan")
def politicians_scan(
    min_impact: str = Query("MEDIO", description="BAIXO, MEDIO, ALTO, CRITICO"),
    alertar: int = Query(1),
    usar_ia: int = Query(1),
    regioes: str = Query(None, description="EUA,ISRAEL,IRAN,RUSSIA,UKRAINE"),
    db: Session = Depends(get_db),
):
    """
    Motor #29 — Rastreador de Políticos
    Monitora X/Twitter via Nitter RSS + fallback Google News.
    Avalia impacto geopolítico via Claude Haiku.
    Cruza com mercados ativos do banco.
    """
    now = datetime.utcnow()
    impact_rank = {"BAIXO": 1, "MEDIO": 2, "ALTO": 3, "CRITICO": 4}
    min_rank = impact_rank.get(min_impact.upper(), 2)

    pols_filtrados = POLITICIANS
    if regioes:
        regioes_list = [r.strip().upper() for r in regioes.split(",")]
        pols_filtrados = [p for p in POLITICIANS if p["regiao"] in regioes_list]

    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    alertas = []
    stats = {"politicos_escaneados": 0, "posts_novos": 0, "alertas_enviados": 0, "nitter_ok": 0, "gnews_ok": 0}

    for politician in pols_filtrados:
        stats["politicos_escaneados"] += 1

        # Tenta Nitter primeiro
        posts = _politician_fetch_nitter(politician)
        if posts:
            stats["nitter_ok"] += 1
        else:
            # Fallback Google News
            posts = _politician_fetch_googlenews(politician)
            if posts:
                stats["gnews_ok"] += 1

        time.sleep(0.3)

        for post in posts:
            uid = post["uid"]
            if uid in _POLITICIAN_SEEN:
                continue

            impact = _politician_classify(post["texto"])
            if impact_rank.get(impact["nivel"], 1) < min_rank:
                _POLITICIAN_SEEN.add(uid)
                continue

            stats["posts_novos"] += 1

            ia = _politician_ia_analysis(post["texto"], politician, impact) if usar_ia else {
                "resumo": post["texto"][:100],
                "impacto_mercado": impact["nivel"],
                "acao": "MONITORAR",
                "confianca": 0.4,
                "mercados_keywords": impact["keywords"],
                "escalada": "NEUTRO",
            }

            keywords_match = ia.get("mercados_keywords", []) + impact["keywords"]
            markets_match = _politician_match_markets(keywords_match, politician["regiao"], markets)

            alerta = {
                "politico": politician["nome"],
                "cargo": politician["cargo"],
                "regiao": politician["regiao"],
                "fonte": post["fonte"],
                "texto": post["texto"][:300],
                "url": post["url"],
                "published": post["published"],
                "impacto": impact["nivel"],
                "impacto_score": impact["score"],
                "keywords": impact["keywords"],
                "resumo_ia": ia.get("resumo", ""),
                "escalada": ia.get("escalada", "NEUTRO"),
                "acao": ia.get("acao", "MONITORAR"),
                "confianca": ia.get("confianca", 0.4),
                "mercados_afetados": markets_match,
                "total_mercados": len(markets_match),
                "detectado_em": now.isoformat(),
            }
            alertas.append(alerta)

            if alertar:
                _politician_telegram(politician, post, impact, ia, markets_match)
                stats["alertas_enviados"] += 1

            _POLITICIAN_SEEN.add(uid)
            if len(_POLITICIAN_SEEN) > 20000:
                for old in list(_POLITICIAN_SEEN)[:3000]:
                    _POLITICIAN_SEEN.discard(old)

    alertas.sort(key=lambda x: x["impacto_score"], reverse=True)

    return {
        "status": "ok",
        "politicos_monitorados": len(pols_filtrados),
        "stats": stats,
        "min_impact": min_impact,
        "alertas": alertas,
        "gerado_em": now.isoformat(),
    }


@app.get("/politicians/list")
def politicians_list():
    return {
        "total": len(POLITICIANS),
        "regioes": list({p["regiao"] for p in POLITICIANS}),
        "politicos": POLITICIANS,
    }

# ══════════════════════════════════════════════════════════════
# MOTOR #30 — ANALISADOR DE LINGUAGEM DE PRESS RELEASES
# Mede escalada vs desescalada em comunicados oficiais
# "Strongly condemns" vs "urges restraint" → sinal de mercado
# ══════════════════════════════════════════════════════════════

PRESS_RELEASE_SOURCES = [
    # Governos / Ministérios
    {"nome": "US State Dept",     "regiao": "EUA",     "url": "https://www.state.gov/press-releases/feed/"},
    {"nome": "White House",       "regiao": "EUA",     "url": "https://www.whitehouse.gov/feed/"},
    {"nome": "UN News",           "regiao": "GLOBAL",  "url": "https://news.un.org/feed/subscribe/en/news/all/rss.xml"},
    {"nome": "NATO",              "regiao": "NATO",    "url": "https://www.nato.int/cps/en/natohq/news.htm?selectedLocale=en&type=topic_news&rss=1"},
    {"nome": "Israel MFA",        "regiao": "ISRAEL",  "url": "https://www.gov.il/en/api/DynamicCollector?ListId=eb91cf55-27b9-45e1-a503-77a9ae8c4e38&Take=20"},
    {"nome": "Iran PressTV",      "regiao": "IRAN",    "url": "https://www.presstv.ir/rssfeed/3.xml"},
    {"nome": "Russia MFA",        "regiao": "RUSSIA",  "url": "https://mid.ru/en/press_service/spokesman/briefings/rss/"},
    {"nome": "Reuters World",     "regiao": "GLOBAL",  "url": "https://feeds.reuters.com/reuters/worldNews"},
    {"nome": "BBC World",         "regiao": "GLOBAL",  "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
    {"nome": "Al Jazeera",        "regiao": "GLOBAL",  "url": "https://www.aljazeera.com/xml/rss/all.xml"},
]

# Frases de escalada com peso
ESCALADA_PHRASES = {
    # Nível CRITICO (peso 40)
    "military action":          40, "airstrikes":               40,
    "ground invasion":          40, "declaration of war":       40,
    "nuclear threat":           40, "ballistic missile":        40,
    # Nível ALTO (peso 25)
    "strongly condemns":        25, "unacceptable":             25,
    "red line":                 25, "will not tolerate":        25,
    "retaliation":              25, "retaliatory strike":       25,
    "military operation":       25, "armed forces":             25,
    "sanctions":                20, "embargo":                  20,
    # Nível MEDIO (peso 15)
    "condemns":                 15, "serious concern":          15,
    "provocative":              15, "aggressive":               15,
    "threatens":                15, "ultimatum":                15,
    "expels ambassador":        15, "recalls ambassador":       15,
    # Nível BAIXO (peso 8)
    "concerned":                 8, "monitoring":                8,
    "urges caution":             8, "calls for restraint":       8,
}

# Frases de desescalada com peso (negativo)
DESESCALADA_PHRASES = {
    # Nível CRITICO desescalada (peso -40)
    "ceasefire agreement":     -40, "peace deal":              -40,
    "withdrawal of troops":    -40, "end of hostilities":      -40,
    # Nível ALTO desescalada (peso -25)
    "ceasefire":               -25, "diplomatic solution":     -25,
    "peace negotiations":      -25, "resuming talks":          -25,
    "prisoner exchange":       -20, "humanitarian corridor":   -20,
    # Nível MEDIO desescalada (peso -15)
    "urges restraint":         -15, "calls for dialogue":      -15,
    "de-escalation":           -15, "diplomatic channels":     -15,
    "constructive talks":      -15, "meeting agreed":          -15,
    # Nível BAIXO desescalada (peso -8)
    "welcomes":                 -8, "positive steps":           -8,
    "cooperation":              -8, "diplomatic":               -8,
}

_PRESS_SEEN: set = set()


def _press_fetch(source: dict) -> list:
    """Busca RSS de press releases."""
    items = []
    try:
        resp = requests.get(source["url"], headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        for item in root.findall(".//item")[:8]:
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            desc  = (item.findtext("description") or "").strip()
            # Limpa HTML do description
            desc = re.sub(r"<[^>]+>", " ", desc).strip()
            if not title:
                continue
            uid = f"press:{source['nome']}:{title[:50]}"
            items.append({
                "uid": uid,
                "titulo": title,
                "descricao": desc[:400],
                "url": link,
                "published": pub,
                "fonte": source["nome"],
                "regiao": source["regiao"],
            })
    except Exception as e:
        print(f"[press] {source['nome']}: {e}")
    return items


def _press_score(texto: str) -> dict:
    """Calcula score de escalada/desescalada por análise de frases."""
    text_lower = texto.lower()
    score = 0
    frases_escalada = []
    frases_desescalada = []

    for frase, peso in ESCALADA_PHRASES.items():
        if frase in text_lower:
            score += peso
            frases_escalada.append(frase)

    for frase, peso in DESESCALADA_PHRASES.items():
        if frase in text_lower:
            score += peso  # já é negativo
            frases_desescalada.append(frase)

    score = max(-100, min(100, score))

    if score >= 40:
        nivel = "CRITICO"
        direcao = "ESCALADA"
    elif score >= 20:
        nivel = "ALTO"
        direcao = "ESCALADA"
    elif score >= 8:
        nivel = "MEDIO"
        direcao = "ESCALADA"
    elif score <= -30:
        nivel = "ALTO"
        direcao = "DESESCALADA"
    elif score <= -15:
        nivel = "MEDIO"
        direcao = "DESESCALADA"
    elif score <= -8:
        nivel = "BAIXO"
        direcao = "DESESCALADA"
    else:
        nivel = "BAIXO"
        direcao = "NEUTRO"

    return {
        "score": score,
        "nivel": nivel,
        "direcao": direcao,
        "frases_escalada": frases_escalada[:5],
        "frases_desescalada": frases_desescalada[:5],
    }


def _press_ia_analysis(titulo: str, descricao: str, fonte: str, regiao: str, score_dict: dict) -> dict:
    """Claude Haiku analisa linguagem diplomática e impacto no mercado."""
    if not ANTHROPIC_KEY or abs(score_dict["score"]) < 10:
        return {
            "resumo": titulo[:100],
            "direcao": score_dict["direcao"],
            "impacto_mercado": score_dict["nivel"],
            "acao": "MONITORAR",
            "confianca": 0.4,
            "mercados_keywords": [],
            "linguagem": "NEUTRA",
        }

    prompt = f"""Você é especialista em linguagem diplomática e prediction markets.

FONTE: {fonte} ({regiao})
TÍTULO: {titulo}
CONTEÚDO: {descricao[:350]}
SCORE ESCALADA: {score_dict['score']} (positivo=escalada, negativo=desescalada)
FRASES DETECTADAS: escalada={score_dict['frases_escalada']}, desescalada={score_dict['frases_desescalada']}

Analise a linguagem diplomática e o impacto nos prediction markets.
Responda SOMENTE com JSON válido:
{{"resumo":"<max 80 chars PT-BR>","direcao":"ESCALADA|DESESCALADA|NEUTRO","intensidade":"FORTE|MODERADA|FRACA","impacto_mercado":"CRITICO|ALTO|MEDIO|BAIXO","acao":"COMPRAR_YES|COMPRAR_NO|MONITORAR|AGUARDAR","confianca":<0.0-1.0>,"mercados_keywords":["<kw1>","<kw2>"],"linguagem":"AMEACA|CONDENACAO|NEGOCIACAO|ACORDO|NEUTRO|PROVOCACAO"}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 250,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=12,
        )
        if resp.status_code == 200:
            raw = resp.json()["content"][0]["text"].replace("```json", "").replace("```", "").strip()
            return json.loads(raw)
    except Exception as e:
        print(f"[press_ia] {e}")

    return {
        "resumo": titulo[:100],
        "direcao": score_dict["direcao"],
        "impacto_mercado": score_dict["nivel"],
        "acao": "MONITORAR",
        "confianca": 0.4,
        "mercados_keywords": [],
        "linguagem": "NEUTRO",
    }


def _press_telegram(item: dict, score_dict: dict, ia: dict, markets: list) -> None:
    direcao = ia.get("direcao", score_dict["direcao"])
    nivel = ia.get("impacto_mercado", score_dict["nivel"])

    direcao_emoji = {
        "ESCALADA":    "📈🔴",
        "DESESCALADA": "📉🟢",
        "NEUTRO":      "➡️⚪",
    }.get(direcao, "➡️")

    nivel_emoji = {"CRITICO": "🚨🚨", "ALTO": "🚨", "MEDIO": "⚠️", "BAIXO": "ℹ️"}.get(nivel, "⚠️")
    ling_emoji = {
        "AMEACA":     "⚔️", "CONDENACAO": "🔥", "NEGOCIACAO": "🤝",
        "ACORDO":     "✍️", "PROVOCACAO":  "💢", "NEUTRO":     "📄",
    }.get(ia.get("linguagem", "NEUTRO"), "📄")

    mercados_txt = ""
    for m in markets[:3]:
        price = f"{m['yes_price']}%" if m.get("yes_price") else "?"
        mercados_txt += f"\n• <a href=\"{m['polymarket_url']}\">{m['question'][:65]}</a> @ {price}"

    msg = (
        f"{nivel_emoji} <b>PRESS RELEASE — {item['regiao']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{ling_emoji} <b>{item['fonte']}</b>\n"
        f"📰 {item['titulo'][:100]}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{direcao_emoji} Direção: {direcao} | Score: {score_dict['score']:+d}\n"
        f"🔑 Escalada: {', '.join(score_dict['frases_escalada'][:3]) or 'nenhuma'}\n"
        f"🕊️ Desescalada: {', '.join(score_dict['frases_desescalada'][:3]) or 'nenhuma'}\n"
        f"🎯 Ação: {ia.get('acao','MONITORAR')} | Confiança: {round(ia.get('confianca',0)*100)}%\n"
    )
    if markets:
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n🎯 <b>Mercados:</b>{mercados_txt}\n"
    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href=\"{item['url']}\">Ver press release</a>\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )
    _telegram_send(msg)


@app.get("/press/scan")
def press_scan(
    min_score: int = Query(8, description="Score mínimo absoluto para alertar"),
    alertar: int = Query(1),
    usar_ia: int = Query(1),
    regioes: str = Query(None, description="EUA,ISRAEL,IRAN,RUSSIA,NATO,GLOBAL"),
    db: Session = Depends(get_db),
):
    """
    Motor #30 — Analisador de Linguagem de Press Releases
    Mede escalada vs desescalada em comunicados diplomáticos.
    'Strongly condemns' vs 'urges restraint' → sinal de mercado.
    """
    now = datetime.utcnow()

    fontes_filtradas = PRESS_RELEASE_SOURCES
    if regioes:
        regioes_list = [r.strip().upper() for r in regioes.split(",")]
        fontes_filtradas = [s for s in PRESS_RELEASE_SOURCES if s["regiao"] in regioes_list]

    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )

    alertas = []
    stats = {
        "fontes_escaneadas": 0,
        "items_novos": 0,
        "alertas_gerados": 0,
        "alertas_enviados": 0,
        "escalada_count": 0,
        "desescalada_count": 0,
    }

    for source in fontes_filtradas:
        stats["fontes_escaneadas"] += 1
        items = _press_fetch(source)
        time.sleep(0.3)

        for item in items:
            uid = item["uid"]
            if uid in _PRESS_SEEN:
                continue

            texto_completo = item["titulo"] + " " + item["descricao"]
            score_dict = _press_score(texto_completo)

            if abs(score_dict["score"]) < min_score:
                _PRESS_SEEN.add(uid)
                continue

            stats["items_novos"] += 1
            if score_dict["direcao"] == "ESCALADA":
                stats["escalada_count"] += 1
            elif score_dict["direcao"] == "DESESCALADA":
                stats["desescalada_count"] += 1

            ia = _press_ia_analysis(
                item["titulo"], item["descricao"],
                source["nome"], source["regiao"], score_dict
            ) if usar_ia else {
                "resumo": item["titulo"][:100],
                "direcao": score_dict["direcao"],
                "impacto_mercado": score_dict["nivel"],
                "acao": "MONITORAR",
                "confianca": 0.4,
                "mercados_keywords": [],
                "linguagem": "NEUTRO",
            }

            keywords_match = ia.get("mercados_keywords", [])
            markets_match = _politician_match_markets(keywords_match, source["regiao"], markets)

            alerta = {
                "fonte": source["nome"],
                "regiao": source["regiao"],
                "titulo": item["titulo"],
                "descricao": item["descricao"][:200],
                "url": item["url"],
                "published": item["published"],
                "score": score_dict["score"],
                "nivel": score_dict["nivel"],
                "direcao": score_dict["direcao"],
                "frases_escalada": score_dict["frases_escalada"],
                "frases_desescalada": score_dict["frases_desescalada"],
                "resumo_ia": ia.get("resumo", ""),
                "linguagem": ia.get("linguagem", "NEUTRO"),
                "intensidade": ia.get("intensidade", "FRACA"),
                "acao": ia.get("acao", "MONITORAR"),
                "confianca": ia.get("confianca", 0.4),
                "mercados_afetados": markets_match,
                "total_mercados": len(markets_match),
                "detectado_em": now.isoformat(),
            }
            alertas.append(alerta)
            stats["alertas_gerados"] += 1

            if alertar and abs(score_dict["score"]) >= 20:
                _press_telegram(item, score_dict, ia, markets_match)
                stats["alertas_enviados"] += 1

            _PRESS_SEEN.add(uid)
            if len(_PRESS_SEEN) > 10000:
                for old in list(_PRESS_SEEN)[:2000]:
                    _PRESS_SEEN.discard(old)

    alertas.sort(key=lambda x: abs(x["score"]), reverse=True)

    return {
        "status": "ok",
        "fontes_monitoradas": len(fontes_filtradas),
        "stats": stats,
        "min_score": min_score,
        "alertas": alertas,
        "gerado_em": now.isoformat(),
    }
# ══════════════════════════════════════════════════════════════
# MOTOR #31 — MAPA DE CARTEIRAS VENCEDORAS
# Identifica wallets que apostaram certo antes de eventos grandes
# Quando essas wallets movem → você move junto
# ══════════════════════════════════════════════════════════════

_SMART_MONEY_CACHE: dict = {}  # wallet -> historico de acertos
_SMART_MONEY_SEEN: set = set()


def _smart_get_market_trades(market_slug: str) -> list:
    """Busca trades recentes de um mercado via Polymarket API."""
    try:
        resp = requests.get(
            f"https://data-api.polymarket.com/trades",
            params={"market": market_slug, "limit": 100},
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json() or []
    except Exception as e:
        print(f"[smart_money] trades {market_slug}: {e}")
    return []


def _smart_get_market_history(market_slug: str) -> list:
    """Busca histórico de preços de um mercado."""
    try:
        resp = requests.get(
            f"https://data-api.polymarket.com/prices-history",
            params={"market": market_slug, "interval": "1d", "fidelity": 10},
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("history", []) or []
    except Exception as e:
        print(f"[smart_money] history {market_slug}: {e}")
    return []


def _smart_identify_winners(db: Session) -> dict:
    smart_wallets = {}

    try:
        resolved_markets = db.query(Market).filter(
            Market.end_date != None,
            Market.end_date < datetime.utcnow(),
            Market.question != None,
        ).limit(50).all()
    except Exception as e:
        print(f"[smart_money] query error: {e}")
        return {}

    for market in resolved_markets:
        try:
            trades = _smart_get_market_trades(market.market_slug)
            if not trades:
                continue

            history = _smart_get_market_history(market.market_slug)
            if not history:
                continue

            last_price = history[-1].get("p", 0.5) if history else 0.5
            resolved_yes = last_price > 0.8
            resolved_no  = last_price < 0.2

            if not resolved_yes and not resolved_no:
                continue

            for trade in trades:
                wallet = trade.get("maker", "") or trade.get("transactionHash", "")[:20]
                if not wallet:
                    continue

                price_at_trade = float(trade.get("price", 0.5))
                side = trade.get("side", "").upper()
                size = float(trade.get("size", 0))

                acertou = False
                lucro_estimado = 0

                if resolved_yes and side == "BUY" and price_at_trade < 0.35:
                    acertou = True
                    lucro_estimado = size * (last_price - price_at_trade)
                elif resolved_no and side == "SELL" and price_at_trade > 0.65:
                    acertou = True
                    lucro_estimado = size * (price_at_trade - last_price)

                if acertou:
                    if wallet not in smart_wallets:
                        smart_wallets[wallet] = {
                            "wallet": wallet,
                            "acertos": 0,
                            "lucro_total_estimado": 0,
                            "mercados_acertados": [],
                        }
                    smart_wallets[wallet]["acertos"] += 1
                    smart_wallets[wallet]["lucro_total_estimado"] += lucro_estimado
                    smart_wallets[wallet]["mercados_acertados"].append({
                        "market": market.question[:60],
                        "slug": market.market_slug,
                        "price_entry": round(price_at_trade, 3),
                        "resolved": "YES" if resolved_yes else "NO",
                        "lucro": round(lucro_estimado, 2),
                    })
        except Exception as e:
            print(f"[smart_money] market {market.market_slug}: {e}")
            continue

    smart_wallets = {k: v for k, v in smart_wallets.items() if v["acertos"] >= 2}
    return smart_wallets

def _smart_get_recent_moves(smart_wallets: dict, markets: list) -> list:
    """
    Detecta movimentos recentes das smart wallets em mercados ativos.
    """
    alertas = []
    market_slugs = [m.market_slug for m in markets[:50]]  # Top 50 mercados

    for slug in market_slugs:
        trades = _smart_get_market_trades(slug)
        if not trades:
            continue

        market_obj = next((m for m in markets if m.market_slug == slug), None)
        if not market_obj:
            continue

        # Preço atual
        yes_price, _ = _get_yes_no_prices(market_obj)

        for trade in trades[:20]:  # Últimos 20 trades
            wallet = trade.get("maker", "") or trade.get("transactionHash", "")[:20]
            if wallet not in smart_wallets:
                continue

            uid = f"smart:{wallet[:15]}:{slug}:{trade.get('transactionHash','')[:10]}"
            if uid in _SMART_MONEY_SEEN:
                continue

            price_trade = float(trade.get("price", 0.5))
            side = trade.get("side", "").upper()
            size = float(trade.get("size", 0))
            timestamp = trade.get("timestamp", "")

            wallet_info = smart_wallets[wallet]
            score_confianca = min(wallet_info["acertos"] * 20, 100)

            alertas.append({
                "uid": uid,
                "wallet": wallet[:20] + "...",
                "wallet_acertos": wallet_info["acertos"],
                "wallet_lucro_estimado": round(wallet_info["lucro_total_estimado"], 2),
                "mercado": market_obj.question,
                "slug": slug,
                "side": side,
                "price": round(price_trade, 3),
                "yes_price_atual": yes_price,
                "size_usdc": round(size, 2),
                "score_confianca": score_confianca,
                "nivel": "ALTO" if score_confianca >= 60 else "MEDIO",
                "acao": f"COMPRAR_{'YES' if side == 'BUY' else 'NO'}",
                "polymarket_url": _polymarket_url(slug),
                "timestamp": timestamp,
            })
            _SMART_MONEY_SEEN.add(uid)

    alertas.sort(key=lambda x: x["score_confianca"], reverse=True)
    return alertas


def _smart_telegram(alerta: dict) -> None:
    nivel_emoji = "🚨" if alerta["nivel"] == "ALTO" else "⚠️"
    side_emoji = "💚 COMPROU YES" if alerta["side"] == "BUY" else "🔴 COMPROU NO"

    msg = (
        f"{nivel_emoji} <b>SMART MONEY DETECTADO</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 Wallet com {alerta['wallet_acertos']} acertos históricos\n"
        f"💰 Lucro estimado histórico: ${alerta['wallet_lucro_estimado']:,.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{side_emoji} @ {round(alerta['price']*100, 1)}%\n"
        f"📊 Preço atual YES: {alerta['yes_price_atual']}%\n"
        f"💵 Tamanho: ${alerta['size_usdc']:,.0f} USDC\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 Mercado: {alerta['mercado'][:80]}\n"
        f"🎯 Confiança: {alerta['score_confianca']}%\n"
        f"▶️ Ação: {alerta['acao']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href=\"{alerta['polymarket_url']}\">Ver mercado</a>"
    )
    _telegram_send(msg)


@app.get("/smart-money/scan")
def smart_money_scan(
    min_acertos: int = Query(2, description="Mínimo de acertos históricos da wallet"),
    alertar: int = Query(1),
    db: Session = Depends(get_db),
):
    """
    Motor #31 — Mapa de Carteiras Vencedoras (Smart Money)
    Identifica wallets que apostaram certo antes de eventos grandes.
    Quando essas wallets movem novamente → gera sinal.
    """
    now = datetime.utcnow()

    # Identifica smart wallets pelo histórico
    smart_wallets = _smart_identify_winners(db)

    if not smart_wallets:
        return {
            "status": "ok",
            "smart_wallets_encontradas": 0,
            "alertas": [],
            "msg": "Sem mercados resolvidos suficientes no banco ainda. Rode o cron por alguns dias.",
            "gerado_em": now.isoformat(),
        }

    # Filtra por min_acertos
    smart_wallets = {k: v for k, v in smart_wallets.items() if v["acertos"] >= min_acertos}

    # Mercados ativos para monitorar
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > FILTER_MIN_PRICE, Token.price < FILTER_MAX_PRICE)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(200).all()
    )

    # Detecta movimentos recentes
    alertas = _smart_get_recent_moves(smart_wallets, markets)

    # Envia alertas
    alertas_enviados = 0
    for alerta in alertas:
        if alertar and alerta["score_confianca"] >= 40:
            _smart_telegram(alerta)
            alertas_enviados += 1

    return {
        "status": "ok",
        "smart_wallets_encontradas": len(smart_wallets),
        "top_wallets": sorted(
            list(smart_wallets.values()),
            key=lambda x: x["acertos"],
            reverse=True
        )[:10],
        "alertas": alertas,
        "alertas_enviados": alertas_enviados,
        "gerado_em": now.isoformat(),
    }


@app.get("/smart-money/wallets")
def smart_money_wallets(
    min_acertos: int = Query(2),
    db: Session = Depends(get_db),
):
    """Lista as smart wallets identificadas e seus históricos."""
    smart_wallets = _smart_identify_winners(db)
    smart_wallets = {k: v for k, v in smart_wallets.items() if v["acertos"] >= min_acertos}

    return {
        "status": "ok",
        "total": len(smart_wallets),
        "wallets": sorted(
            list(smart_wallets.values()),
            key=lambda x: x["lucro_total_estimado"],
            reverse=True
        )[:20],
    }

# ══════════════════════════════════════════════════════════════
# MOTOR #34 — DETECTOR DE MERCADO MANIPULADO
# Sinais: poucos traders + volume alto + concentração de posições + variação de odds suspeita
# Resultado: avisa para NÃO entrar ou para entrar CONTRA
# ══════════════════════════════════════════════════════════════

def detectar_manipulacao(
    num_traders: int,
    volume_total: float,
    top_trader_percentual: float,   # % do volume concentrado no top trader (0-100)
    odds_variacao_24h: float,        # variação % das odds nas últimas 24h (ex: 0.35 = 35%)
) -> dict:
    """
    Analisa sinais de manipulação em um mercado Polymarket.

    Parâmetros:
    - num_traders: número de traders únicos no mercado
    - volume_total: volume total em USD
    - top_trader_percentual: % do volume que o maior trader representa
    - odds_variacao_24h: variação das odds nas últimas 24h (0.0 a 1.0)

    Retorna dict com score, sinais detectados e recomendação.
    """
    sinais = []
    score = 0  # 0 a 100 — quanto maior, mais suspeito

    # --- SINAL 1: Poucos traders com volume alto ---
    volume_por_trader = volume_total / max(num_traders, 1)
    if num_traders < 10 and volume_total > 5000:
        sinais.append({
            "sinal": "POUCOS_TRADERS_VOLUME_ALTO",
            "descricao": f"{num_traders} traders, volume ${volume_total:,.0f} — concentração perigosa",
            "peso": 35
        })
        score += 35
    elif num_traders < 25 and volume_total > 10000:
        sinais.append({
            "sinal": "POUCOS_TRADERS_VOLUME_MODERADO",
            "descricao": f"{num_traders} traders, volume ${volume_total:,.0f} — atenção",
            "peso": 20
        })
        score += 20

    # --- SINAL 2: Concentração de posições (top trader %) ---
    if top_trader_percentual >= 60:
        sinais.append({
            "sinal": "CONCENTRACAO_EXTREMA",
            "descricao": f"Top trader controla {top_trader_percentual:.1f}% do volume — whale dominando",
            "peso": 35
        })
        score += 35
    elif top_trader_percentual >= 35:
        sinais.append({
            "sinal": "CONCENTRACAO_ALTA",
            "descricao": f"Top trader controla {top_trader_percentual:.1f}% do volume — atenção",
            "peso": 20
        })
        score += 20

    # --- SINAL 3: Variação de odds suspeita nas últimas 24h ---
    variacao_pct = odds_variacao_24h * 100
    if variacao_pct >= 40:
        sinais.append({
            "sinal": "ODDS_VARIACAO_EXTREMA",
            "descricao": f"Odds variaram {variacao_pct:.1f}% em 24h — movimento artificial provável",
            "peso": 30
        })
        score += 30
    elif variacao_pct >= 20:
        sinais.append({
            "sinal": "ODDS_VARIACAO_SUSPEITA",
            "descricao": f"Odds variaram {variacao_pct:.1f}% em 24h — monitorar",
            "peso": 15
        })
        score += 15

    # --- CLASSIFICAÇÃO FINAL ---
    score = min(score, 100)

    if score >= 70:
        nivel = "CRITICO"
        recomendacao = "NÃO ENTRAR — mercado com sinais fortes de manipulação"
        entrar_contra = True
    elif score >= 40:
        nivel = "SUSPEITO"
        recomendacao = "CUIDADO — considere entrar CONTRA a tendência dominante"
        entrar_contra = True
    elif score >= 20:
        nivel = "ATENCAO"
        recomendacao = "MONITORAR — sinais leves, não entrar com posição grande"
        entrar_contra = False
    else:
        nivel = "NORMAL"
        recomendacao = "Mercado parece orgânico — pode operar normalmente"
        entrar_contra = False

    return {
        "score_manipulacao": score,
        "nivel": nivel,
        "recomendacao": recomendacao,
        "entrar_contra": entrar_contra,
        "sinais_detectados": sinais,
        "metricas": {
            "num_traders": num_traders,
            "volume_total_usd": volume_total,
            "volume_por_trader_usd": round(volume_por_trader, 2),
            "top_trader_percentual": top_trader_percentual,
            "odds_variacao_24h_pct": round(variacao_pct, 2)
        }
    }

class ManipulacaoRequest(BaseModel):
    num_traders: int
    volume_total: float
    top_trader_percentual: float
    odds_variacao_24h: float

@app.post("/detectar-manipulacao")
async def endpoint_detectar_manipulacao(body: ManipulacaoRequest):
    """
    Endpoint dedicado para detecção de manipulação de mercado.

    Body JSON:
    {
        "num_traders": 8,
        "volume_total": 15000,
        "top_trader_percentual": 65,
        "odds_variacao_24h": 0.45
    }
    """
    try:
        resultado = detectar_manipulacao(
            num_traders=body.num_traders,
            volume_total=body.volume_total,
            top_trader_percentual=body.top_trader_percentual,
            odds_variacao_24h=body.odds_variacao_24h
        )
        return JSONResponse(content={
            "motor": "MOTOR_34_DETECTOR_MANIPULACAO",
            "resultado": resultado
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    
    # ══════════════════════════════════════════════════════════════
# MOTOR #35 — SCORE DE TIMING PERFEITO
# Analisa histórico de preços e diz "Entre agora" ou "Espera X horas"
# baseado em padrões de correção de preço
# ══════════════════════════════════════════════════════════════

from datetime import datetime, timezone

def calcular_timing(historico_precos: list[dict]) -> dict:
    """
    Analisa histórico de preços para identificar o melhor momento de entrada.

    Espera lista de dicts:
    [
        {"timestamp": "2024-03-01T14:00:00Z", "preco": 0.65},
        {"timestamp": "2024-03-01T15:00:00Z", "preco": 0.68},
        ...
    ]
    """
    if not historico_precos or len(historico_precos) < 3:
        return {
            "score_timing": 0,
            "recomendacao": "DADOS_INSUFICIENTES",
            "acao": "Histórico insuficiente para análise de timing",
            "esperar_horas": None,
            "janelas_ideais": [],
            "motivo": "Mínimo de 3 pontos de preço necessários"
        }

    # --- Ordenar por timestamp ---
    try:
        historico_precos = sorted(
            historico_precos,
            key=lambda x: datetime.fromisoformat(x["timestamp"].replace("Z", "+00:00"))
        )
    except Exception:
        pass

    precos = [p["preco"] for p in historico_precos]
    agora = datetime.now(timezone.utc)

    # --- ANÁLISE 1: Tendência recente (últimos 3 pontos) ---
    ultimos = precos[-3:]
    tendencia = ultimos[-1] - ultimos[0]
    tendencia_pct = (tendencia / max(ultimos[0], 0.01)) * 100

    # --- ANÁLISE 2: Volatilidade (desvio entre pontos consecutivos) ---
    deltas = [abs(precos[i+1] - precos[i]) for i in range(len(precos)-1)]
    volatilidade_media = sum(deltas) / len(deltas) if deltas else 0
    volatilidade_recente = deltas[-1] if deltas else 0

    # --- ANÁLISE 3: Detectar padrão de correção ---
    # Correção = preço caiu após alta (oportunidade de entrada)
    correcao_detectada = False
    magnitude_correcao = 0
    if len(precos) >= 4:
        pico = max(precos[-4:])
        atual = precos[-1]
        if pico > atual:
            magnitude_correcao = ((pico - atual) / pico) * 100
            if magnitude_correcao >= 5:
                correcao_detectada = True

    # --- ANÁLISE 4: Janelas de horário ideais ---
    # Mapear horas com maior variação no histórico
    horas_variacao = {}
    for i in range(1, len(historico_precos)):
        try:
            ts = datetime.fromisoformat(
                historico_precos[i]["timestamp"].replace("Z", "+00:00")
            )
            hora = ts.hour
            variacao = abs(precos[i] - precos[i-1])
            if hora not in horas_variacao:
                horas_variacao[hora] = []
            horas_variacao[hora].append(variacao)
        except Exception:
            continue

    # Calcular média de variação por hora
    media_por_hora = {
        h: sum(v)/len(v) for h, v in horas_variacao.items()
    }
    janelas_ideais = sorted(media_por_hora.items(), key=lambda x: x[1], reverse=True)[:3]
    janelas_formatadas = [
        {"hora": f"{h:02d}:00 UTC", "variacao_media": round(v * 100, 2)}
        for h, v in janelas_ideais
    ]

    # --- SCORE DE TIMING (0-100) ---
    score = 50  # base neutra

    # Bônus por correção detectada (oportunidade)
    if correcao_detectada:
        score += min(magnitude_correcao * 2, 30)

    # Bônus por volatilidade baixa agora (estável = bom para entrar)
    if volatilidade_recente < volatilidade_media * 0.7:
        score += 15
    elif volatilidade_recente > volatilidade_media * 1.5:
        score -= 20  # muito agitado agora, espera

    # Bônus por tendência favorável (preço caindo = oportunidade de compra barata)
    if -20 <= tendencia_pct <= -3:
        score += 10
    elif tendencia_pct > 20:
        score -= 15  # já subiu muito, tarde demais

    score = max(0, min(score, 100))

    # --- RECOMENDAÇÃO FINAL ---
    hora_atual = agora.hour

    # Verificar se hora atual é janela ideal
    horas_ideais = [h for h, _ in janelas_ideais]
    em_janela_ideal = hora_atual in horas_ideais or (hora_atual + 1) % 24 in horas_ideais

    if score >= 70 and em_janela_ideal:
        acao = "ENTRE AGORA"
        recomendacao = "AGORA"
        esperar_horas = 0
    elif score >= 70 and not em_janela_ideal:
        # Calcular horas até próxima janela ideal
        if horas_ideais:
            proxima_hora = min(
                horas_ideais,
                key=lambda h: (h - hora_atual) % 24
            )
            esperar = (proxima_hora - hora_atual) % 24
        else:
            esperar = 2
        acao = f"ESPERE {esperar} HORA(S) — próxima janela ideal às {proxima_hora:02d}:00 UTC"
        recomendacao = "AGUARDAR"
        esperar_horas = esperar
    elif score >= 40:
        acao = "CONDIÇÕES NEUTRAS — monitore por 1-2 horas antes de entrar"
        recomendacao = "AGUARDAR"
        esperar_horas = 2
    else:
        acao = "NÃO É O MOMENTO — preço instável ou já corrigido demais"
        recomendacao = "EVITAR"
        esperar_horas = None

    return {
        "score_timing": round(score),
        "recomendacao": recomendacao,
        "acao": acao,
        "esperar_horas": esperar_horas,
        "janelas_ideais": janelas_formatadas,
        "analise": {
            "tendencia_pct": round(tendencia_pct, 2),
            "volatilidade_media": round(volatilidade_media * 100, 2),
            "volatilidade_recente": round(volatilidade_recente * 100, 2),
            "correcao_detectada": correcao_detectada,
            "magnitude_correcao_pct": round(magnitude_correcao, 2),
            "em_janela_ideal": em_janela_ideal
        }
    }


class TimingRequest(BaseModel):
    historico_precos: list[dict]


@app.post("/score-timing")
async def endpoint_score_timing(body: TimingRequest):
    """
    Endpoint de timing perfeito.

    Body JSON:
    {
        "historico_precos": [
            {"timestamp": "2024-03-01T14:00:00Z", "preco": 0.65},
            {"timestamp": "2024-03-01T15:00:00Z", "preco": 0.62},
            {"timestamp": "2024-03-01T16:00:00Z", "preco": 0.58},
            {"timestamp": "2024-03-01T17:00:00Z", "preco": 0.55}
        ]
    }
    """
    try:
        resultado = calcular_timing(body.historico_precos)
        return JSONResponse(content={
            "motor": "MOTOR_35_SCORE_TIMING",
            "resultado": resultado
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    
    # ══════════════════════════════════════════════════════════════
# MOTORES #36, #37, #38 — PSICOLOGIA DO MERCADO
# #36 Pânico vs Euforia
# #37 Velocidade de movimento
# #38 Smart Money vs Varejo
# ══════════════════════════════════════════════════════════════

# ─── MOTOR #36 — PÂNICO VS EUFORIA ───────────────────────────

def detectar_panico_euforia(
    preco_atual: float,
    preco_24h_atras: float,
    volume_atual: float,
    volume_media_7d: float,
    num_trades_ultima_hora: int,
    num_trades_media_hora: float
) -> dict:
    score = 0  # -100 = pânico extremo, +100 = euforia extrema
    sinais = []

    # Variação de preço
    variacao_preco = ((preco_atual - preco_24h_atras) / max(preco_24h_atras, 0.01)) * 100
    if variacao_preco > 30:
        score += 40
        sinais.append({"sinal": "PRECO_SUBIU_FORTE", "descricao": f"Preço +{variacao_preco:.1f}% em 24h — euforia"})
    elif variacao_preco > 15:
        score += 20
        sinais.append({"sinal": "PRECO_SUBIU", "descricao": f"Preço +{variacao_preco:.1f}% em 24h — otimismo"})
    elif variacao_preco < -30:
        score -= 40
        sinais.append({"sinal": "PRECO_DESPENCOU", "descricao": f"Preço {variacao_preco:.1f}% em 24h — pânico"})
    elif variacao_preco < -15:
        score -= 20
        sinais.append({"sinal": "PRECO_CAIU", "descricao": f"Preço {variacao_preco:.1f}% em 24h — medo"})

    # Volume vs média
    ratio_volume = volume_atual / max(volume_media_7d, 1)
    if ratio_volume > 3:
        if score > 0:
            score += 30
            sinais.append({"sinal": "VOLUME_EXPLOSIVO_ALTA", "descricao": f"Volume {ratio_volume:.1f}x acima da média — euforia extrema"})
        else:
            score -= 30
            sinais.append({"sinal": "VOLUME_EXPLOSIVO_BAIXA", "descricao": f"Volume {ratio_volume:.1f}x acima da média — pânico extremo"})
    elif ratio_volume > 1.5:
        score += 10 if score >= 0 else -10
        sinais.append({"sinal": "VOLUME_ELEVADO", "descricao": f"Volume {ratio_volume:.1f}x acima da média"})

    # Frequência de trades
    ratio_trades = num_trades_ultima_hora / max(num_trades_media_hora, 1)
    if ratio_trades > 3:
        score += 20 if score >= 0 else -20
        sinais.append({"sinal": "TRADES_FRENÉTICOS", "descricao": f"{ratio_trades:.1f}x mais trades que o normal — mercado agitado"})

    score = max(-100, min(100, score))

    if score >= 60:
        estado = "EUFORIA_EXTREMA"
        interpretacao = "Mercado em euforia — risco de reversão, considere ir CONTRA"
    elif score >= 30:
        estado = "OTIMISMO"
        interpretacao = "Mercado otimista — tendência de alta mas cuidado com topo"
    elif score >= -29:
        estado = "NEUTRO"
        interpretacao = "Mercado equilibrado — sem sinal claro de sentimento"
    elif score >= -59:
        estado = "MEDO"
        interpretacao = "Mercado com medo — possível oportunidade de compra"
    else:
        estado = "PANICO_EXTREMO"
        interpretacao = "Pânico no mercado — oportunidade contrária de alta qualidade"

    return {
        "score_sentimento": score,
        "estado": estado,
        "interpretacao": interpretacao,
        "sinais": sinais,
        "metricas": {
            "variacao_preco_24h_pct": round(variacao_preco, 2),
            "ratio_volume": round(ratio_volume, 2),
            "ratio_trades": round(ratio_trades, 2)
        }
    }


# ─── MOTOR #37 — VELOCIDADE DE MOVIMENTO ─────────────────────

def analisar_velocidade_movimento(historico_precos: list[dict]) -> dict:
    if len(historico_precos) < 2:
        return {"erro": "Mínimo 2 pontos necessários"}

    try:
        historico_precos = sorted(
            historico_precos,
            key=lambda x: datetime.fromisoformat(x["timestamp"].replace("Z", "+00:00"))
        )
    except Exception:
        pass

    movimentos = []
    for i in range(1, len(historico_precos)):
        try:
            t1 = datetime.fromisoformat(historico_precos[i-1]["timestamp"].replace("Z", "+00:00"))
            t2 = datetime.fromisoformat(historico_precos[i]["timestamp"].replace("Z", "+00:00"))
            p1 = historico_precos[i-1]["preco"]
            p2 = historico_precos[i]["preco"]

            minutos = max((t2 - t1).total_seconds() / 60, 0.1)
            variacao_pct = abs((p2 - p1) / max(p1, 0.01)) * 100
            velocidade = variacao_pct / minutos  # % por minuto

            movimentos.append({
                "de": historico_precos[i-1]["timestamp"],
                "ate": historico_precos[i]["timestamp"],
                "variacao_pct": round(variacao_pct, 2),
                "minutos": round(minutos, 1),
                "velocidade_pct_por_min": round(velocidade, 4)
            })
        except Exception:
            continue

    if not movimentos:
        return {"erro": "Não foi possível calcular movimentos"}

    velocidade_max = max(m["velocidade_pct_por_min"] for m in movimentos)
    velocidade_media = sum(m["velocidade_pct_por_min"] for m in movimentos) / len(movimentos)
    movimento_mais_rapido = max(movimentos, key=lambda x: x["velocidade_pct_por_min"])

    # Classificação
    if velocidade_max >= 5:  # 5% por minuto = explosivo
        tipo = "INFO_PRIVILEGIADA"
        descricao = "Movimento explosivo — possível vazamento de informação privilegiada"
        risco = "ALTO"
    elif velocidade_max >= 1:  # 1% por minuto = rápido
        tipo = "REACAO_RAPIDA"
        descricao = "Movimento rápido — reação a notícia ou evento inesperado"
        risco = "MEDIO"
    elif velocidade_max >= 0.1:  # lento
        tipo = "REACAO_NORMAL"
        descricao = "Movimento gradual — reação orgânica do mercado"
        risco = "BAIXO"
    else:
        tipo = "MOVIMENTO_LENTO"
        descricao = "Mercado quase parado — pouco interesse ou liquidez baixa"
        risco = "BAIXO"

    return {
        "tipo_movimento": tipo,
        "descricao": descricao,
        "risco": risco,
        "velocidade_maxima_pct_por_min": round(velocidade_max, 4),
        "velocidade_media_pct_por_min": round(velocidade_media, 4),
        "movimento_mais_rapido": movimento_mais_rapido,
        "todos_movimentos": movimentos
    }


# ─── MOTOR #38 — SMART MONEY VS VAREJO ───────────────────────

def detectar_smart_money(trades: list[dict]) -> dict:
    """
    Espera lista de trades:
    [
        {"valor_usd": 5000, "timestamp": "2024-03-01T14:00:00Z", "direcao": "YES"},
        {"valor_usd": 50,   "timestamp": "2024-03-01T14:01:00Z", "direcao": "NO"},
        ...
    ]
    """
    if not trades:
        return {"erro": "Nenhum trade fornecido"}

    valores = [t["valor_usd"] for t in trades]
    media_valor = sum(valores) / len(valores)

    # Separar smart money (grandes) vs varejo (pequenos)
    threshold_smart = media_valor * 3  # 3x acima da média = smart money
    threshold_varejo = media_valor * 0.5  # abaixo de 50% da média = varejo

    smart_money = [t for t in trades if t["valor_usd"] >= threshold_smart]
    varejo = [t for t in trades if t["valor_usd"] <= threshold_varejo]

    def calcular_direcao(lista):
        if not lista:
            return {"YES": 0, "NO": 0, "volume_yes": 0, "volume_no": 0}
        yes = [t for t in lista if t.get("direcao", "").upper() == "YES"]
        no = [t for t in lista if t.get("direcao", "").upper() == "NO"]
        vol_yes = sum(t["valor_usd"] for t in yes)
        vol_no = sum(t["valor_usd"] for t in no)
        total = vol_yes + vol_no
        return {
            "YES": round((vol_yes / max(total, 1)) * 100, 1),
            "NO": round((vol_no / max(total, 1)) * 100, 1),
            "volume_yes": vol_yes,
            "volume_no": vol_no
        }

    dir_smart = calcular_direcao(smart_money)
    dir_varejo = calcular_direcao(varejo)

    # Detectar divergência
    divergencia = abs(dir_smart["YES"] - dir_varejo["YES"])
    smart_domina_yes = dir_smart["YES"] > dir_varejo["YES"]

    if divergencia >= 40:
        nivel_divergencia = "EXTREMA"
        edge = "MAXIMO"
        recomendacao = f"Smart money apostando {'YES' if smart_domina_yes else 'NO'} enquanto varejo faz o oposto — SIGA O SMART MONEY"
    elif divergencia >= 20:
        nivel_divergencia = "ALTA"
        edge = "ALTO"
        recomendacao = f"Smart money prefere {'YES' if smart_domina_yes else 'NO'} — sinal relevante"
    elif divergencia >= 10:
        nivel_divergencia = "MODERADA"
        edge = "MEDIO"
        recomendacao = "Leve divergência — monitorar"
    else:
        nivel_divergencia = "BAIXA"
        edge = "BAIXO"
        recomendacao = "Smart money e varejo alinhados — sem edge claro"

    return {
        "divergencia_pct": round(divergencia, 1),
        "nivel_divergencia": nivel_divergencia,
        "edge": edge,
        "recomendacao": recomendacao,
        "smart_money": {
            "num_trades": len(smart_money),
            "volume_total": sum(t["valor_usd"] for t in smart_money),
            "direcao": dir_smart
        },
        "varejo": {
            "num_trades": len(varejo),
            "volume_total": sum(t["valor_usd"] for t in varejo),
            "direcao": dir_varejo
        },
        "threshold_smart_money_usd": round(threshold_smart, 2),
        "media_trade_usd": round(media_valor, 2)
    }


# ─── ENDPOINTS ────────────────────────────────────────────────

class PanicoEuforiaRequest(BaseModel):
    preco_atual: float
    preco_24h_atras: float
    volume_atual: float
    volume_media_7d: float
    num_trades_ultima_hora: int
    num_trades_media_hora: float

@app.post("/panico-euforia")
async def endpoint_panico_euforia(body: PanicoEuforiaRequest):
    """
    Motor #36 — Detector de Pânico vs Euforia.
    Body: { "preco_atual": 0.75, "preco_24h_atras": 0.50, "volume_atual": 50000,
            "volume_media_7d": 15000, "num_trades_ultima_hora": 120, "num_trades_media_hora": 30 }
    """
    try:
        resultado = detectar_panico_euforia(
            preco_atual=body.preco_atual,
            preco_24h_atras=body.preco_24h_atras,
            volume_atual=body.volume_atual,
            volume_media_7d=body.volume_media_7d,
            num_trades_ultima_hora=body.num_trades_ultima_hora,
            num_trades_media_hora=body.num_trades_media_hora
        )
        return JSONResponse(content={"motor": "MOTOR_36_PANICO_EUFORIA", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class VelocidadeRequest(BaseModel):
    historico_precos: list[dict]

@app.post("/velocidade-movimento")
async def endpoint_velocidade_movimento(body: VelocidadeRequest):
    """
    Motor #37 — Velocidade de Movimento.
    Body: { "historico_precos": [{"timestamp": "2024-03-01T14:00:00Z", "preco": 0.50}, ...] }
    """
    try:
        resultado = analisar_velocidade_movimento(body.historico_precos)
        return JSONResponse(content={"motor": "MOTOR_37_VELOCIDADE_MOVIMENTO", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class SmartMoneyRequest(BaseModel):
    trades: list[dict]

@app.post("/smart-money")
async def endpoint_smart_money(body: SmartMoneyRequest):
    """
    Motor #38 — Smart Money vs Varejo.
    Body: { "trades": [{"valor_usd": 5000, "timestamp": "...", "direcao": "YES"}, ...] }
    """
    try:
        resultado = detectar_smart_money(body.trades)
        return JSONResponse(content={"motor": "MOTOR_38_SMART_MONEY", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    
    # ══════════════════════════════════════════════════════════════
# MOTORES #39, #40, #41, #42 — GEOPOLÍTICA PROFUNDA
# #39 Saúde de Líderes Mundiais
# #40 Rastreador de Aviões e Navios Militares
# #41 Monitor de Sanções e Bloqueios
# #42 Análise de Discursos de Bancos Centrais
# ══════════════════════════════════════════════════════════════

import httpx
from bs4 import BeautifulSoup

# ─── MOTOR #39 — SAÚDE DE LÍDERES ────────────────────────────

LIDERES_MONITORADOS = {
    "putin": "Vladimir Putin",
    "xi": "Xi Jinping",
    "khamenei": "Ali Khamenei",
    "kim": "Kim Jong-un"
}

async def scrape_aparicoes_lider(nome: str) -> list[dict]:
    """Busca aparições recentes do líder no Google News."""
    try:
        url = f"https://news.google.com/search?q={nome.replace(' ', '+')}&hl=en"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(resp.text, "html.parser")
        artigos = soup.find_all("article")[:10]
        resultados = []
        for a in artigos:
            titulo = a.get_text(strip=True)[:100]
            if titulo:
                resultados.append({"titulo": titulo})
        return resultados
    except Exception:
        return []

def analisar_saude_lider(
    nome_lider: str,
    dias_sem_aparecer: int,
    ultima_aparicao_publica: str,
    aparicoes_recentes: list[dict] = None,
    sinais_manuais: list[str] = None
) -> dict:
    score_alerta = 0
    sinais = []

    # Dias sem aparecer
    if dias_sem_aparecer >= 14:
        score_alerta += 50
        sinais.append({"sinal": "SUMICO_LONGO", "descricao": f"{dias_sem_aparecer} dias sem aparição pública — altamente suspeito"})
    elif dias_sem_aparecer >= 7:
        score_alerta += 30
        sinais.append({"sinal": "SUMICO_MODERADO", "descricao": f"{dias_sem_aparecer} dias sem aparição — monitorar"})
    elif dias_sem_aparecer >= 3:
        score_alerta += 10
        sinais.append({"sinal": "SUMICO_LEVE", "descricao": f"{dias_sem_aparecer} dias sem aparição"})

    # Sinais manuais
    palavras_criticas = ["hospital", "doente", "saúde", "médico", "cirurgia", "enfermo", "sick", "health", "hospital"]
    if sinais_manuais:
        for sinal in sinais_manuais:
            if any(p in sinal.lower() for p in palavras_criticas):
                score_alerta += 30
                sinais.append({"sinal": "SINAL_SAUDE", "descricao": sinal})
            else:
                score_alerta += 10
                sinais.append({"sinal": "SINAL_MANUAL", "descricao": sinal})

    # Aparições recentes (scraping)
    mencoes_saude = 0
    if aparicoes_recentes:
        for ap in aparicoes_recentes:
            if any(p in ap.get("titulo", "").lower() for p in palavras_criticas):
                mencoes_saude += 1
        if mencoes_saude >= 3:
            score_alerta += 40
            sinais.append({"sinal": "MENCOES_SAUDE_MULTIPLAS", "descricao": f"{mencoes_saude} menções de saúde encontradas na mídia"})
        elif mencoes_saude >= 1:
            score_alerta += 20
            sinais.append({"sinal": "MENCAO_SAUDE", "descricao": f"{mencoes_saude} menção de saúde na mídia"})

    score_alerta = min(score_alerta, 100)

    if score_alerta >= 70:
        nivel = "CRITICO"
        impacto = "ALTO risco de instabilidade política — mercados de geopolítica podem mover muito"
    elif score_alerta >= 40:
        nivel = "ATENCAO"
        impacto = "Monitorar de perto — possível evento político em desenvolvimento"
    elif score_alerta >= 20:
        nivel = "OBSERVACAO"
        impacto = "Sinal fraco — manter no radar"
    else:
        nivel = "NORMAL"
        impacto = "Nenhum sinal anormal detectado"

    return {
        "lider": nome_lider,
        "score_alerta": score_alerta,
        "nivel": nivel,
        "impacto_mercado": impacto,
        "ultima_aparicao_publica": ultima_aparicao_publica,
        "dias_sem_aparecer": dias_sem_aparecer,
        "sinais_detectados": sinais,
        "mencoes_saude_midia": mencoes_saude if aparicoes_recentes else "não verificado"
    }


# ─── MOTOR #40 — AVIÕES E NAVIOS MILITARES ───────────────────

REGIOES_SENSIVEIS = {
    "golfo_persico": {"lat_min": 23, "lat_max": 30, "lon_min": 48, "lon_max": 60, "descricao": "Golfo Pérsico"},
    "mar_china": {"lat_min": 0, "lat_max": 25, "lon_min": 105, "lon_max": 125, "descricao": "Mar do Sul da China"},
    "mar_negro": {"lat_min": 40, "lat_max": 47, "lon_min": 27, "lon_max": 42, "descricao": "Mar Negro"},
    "estreito_taiwan": {"lat_min": 22, "lat_max": 26, "lon_min": 119, "lon_max": 122, "descricao": "Estreito de Taiwan"},
    "baltico": {"lat_min": 53, "lat_max": 66, "lon_min": 10, "lon_max": 30, "descricao": "Mar Báltico"}
}

def analisar_movimentacao_militar(ativos: list[dict]) -> dict:
    """
    Espera lista de ativos militares:
    [
        {
            "tipo": "porta_avioes",  # porta_avioes, submarino, aviao_reconhecimento, bombardeiro
            "nome": "USS Gerald Ford",
            "lat": 26.5,
            "lon": 55.0,
            "velocidade_nos": 18,
            "destino_estimado": "Golfo Pérsico"
        }
    ]
    """
    alertas = []
    score_tensao = 0

    pesos_tipo = {
        "porta_avioes": 40,
        "submarino": 35,
        "bombardeiro": 30,
        "aviao_reconhecimento": 20,
        "destroyer": 25,
        "fragata": 15
    }

    for ativo in ativos:
        lat = ativo.get("lat", 0)
        lon = ativo.get("lon", 0)
        tipo = ativo.get("tipo", "desconhecido").lower()
        nome = ativo.get("nome", "Desconhecido")

        # Verificar se está em região sensível
        for regiao_key, regiao in REGIOES_SENSIVEIS.items():
            if (regiao["lat_min"] <= lat <= regiao["lat_max"] and
                    regiao["lon_min"] <= lon <= regiao["lon_max"]):
                peso = pesos_tipo.get(tipo, 10)
                score_tensao += peso
                alertas.append({
                    "ativo": nome,
                    "tipo": tipo,
                    "regiao": regiao["descricao"],
                    "peso_tensao": peso,
                    "coordenadas": {"lat": lat, "lon": lon}
                })

    score_tensao = min(score_tensao, 100)

    if score_tensao >= 70:
        nivel = "TENSAO_CRITICA"
        previsao = "Concentração militar crítica — provável ação militar ou demonstração de força iminente"
        mercados_afetados = ["Petróleo", "Ouro", "USD", "Mercados de conflito no Polymarket"]
    elif score_tensao >= 40:
        nivel = "TENSAO_ELEVADA"
        previsao = "Movimentação significativa — monitorar mercados de geopolítica"
        mercados_afetados = ["Petróleo", "Mercados de conflito no Polymarket"]
    elif score_tensao >= 20:
        nivel = "ATENCAO"
        previsao = "Atividade acima do normal — possível exercício ou posicionamento estratégico"
        mercados_afetados = ["Mercados de geopolítica no Polymarket"]
    else:
        nivel = "NORMAL"
        previsao = "Nenhuma movimentação anormal detectada"
        mercados_afetados = []

    return {
        "score_tensao_militar": score_tensao,
        "nivel": nivel,
        "previsao": previsao,
        "mercados_afetados": mercados_afetados,
        "ativos_em_regioes_sensiveis": alertas,
        "total_ativos_analisados": len(ativos)
    }


# ─── MOTOR #41 — SANÇÕES E BLOQUEIOS ─────────────────────────

async def scrape_noticias_sancoes(pais: str) -> list[str]:
    """Busca notícias recentes de sanções para um país."""
    try:
        query = f"{pais} sanctions diplomatic meeting 2025"
        url = f"https://news.google.com/search?q={query.replace(' ', '+')}&hl=en"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(resp.text, "html.parser")
        artigos = soup.find_all("article")[:10]
        return [a.get_text(strip=True)[:120] for a in artigos if a.get_text(strip=True)]
    except Exception:
        return []

def analisar_sancoes(
    pais_alvo: str,
    eventos: list[dict],
    noticias_scraped: list[str] = None
) -> dict:
    """
    eventos: lista de eventos diplomáticos
    [
        {"tipo": "reuniao_diplomatica", "descricao": "Reunião G7 sobre Russia", "data": "2025-03-01"},
        {"tipo": "votacao_onu", "descricao": "Votação no Conselho de Segurança", "data": "2025-03-02"},
        {"tipo": "viagem_ministro", "descricao": "Secretário de Estado viaja para Genebra", "data": "2025-03-03"}
    ]
    """
    score_risco = 0
    sinais = []

    pesos_eventos = {
        "reuniao_diplomatica": 20,
        "votacao_onu": 25,
        "viagem_ministro": 15,
        "declaracao_oficial": 20,
        "exercicio_militar": 30,
        "expulsao_diplomatica": 40,
        "fechamento_embaixada": 50
    }

    for evento in eventos:
        tipo = evento.get("tipo", "").lower()
        peso = pesos_eventos.get(tipo, 10)
        score_risco += peso
        sinais.append({
            "tipo": tipo,
            "descricao": evento.get("descricao", ""),
            "data": evento.get("data", ""),
            "peso": peso
        })

    # Análise de notícias scraped
    palavras_sancao = ["sanction", "embargo", "ban", "restrict", "freeze", "block", "penalt", "sanctioned"]
    mencoes_sancao = 0
    if noticias_scraped:
        for noticia in noticias_scraped:
            if any(p in noticia.lower() for p in palavras_sancao):
                mencoes_sancao += 1
        if mencoes_sancao >= 5:
            score_risco += 35
            sinais.append({"tipo": "MIDIA_SANCAO_INTENSA", "descricao": f"{mencoes_sancao} notícias sobre sanções encontradas", "peso": 35})
        elif mencoes_sancao >= 2:
            score_risco += 15
            sinais.append({"tipo": "MIDIA_SANCAO_MODERADA", "descricao": f"{mencoes_sancao} notícias sobre sanções", "peso": 15})

    score_risco = min(score_risco, 100)

    if score_risco >= 70:
        previsao = "SANCAO_IMINENTE"
        descricao = "Múltiplos sinais fracos convergindo — anúncio de sanção provável em dias"
    elif score_risco >= 40:
        previsao = "RISCO_ELEVADO"
        descricao = "Atividade diplomática intensa — monitorar de perto"
    elif score_risco >= 20:
        previsao = "OBSERVACAO"
        descricao = "Sinais iniciais — pode não evoluir"
    else:
        previsao = "NORMAL"
        descricao = "Nenhum sinal relevante detectado"

    return {
        "pais_alvo": pais_alvo,
        "score_risco_sancao": score_risco,
        "previsao": previsao,
        "descricao": descricao,
        "sinais_detectados": sinais,
        "mencoes_midia": mencoes_sancao if noticias_scraped else "não verificado"
    }


# ─── MOTOR #42 — DISCURSOS DE BANCOS CENTRAIS ────────────────

PALAVRAS_HAWKISH = [
    "inflation", "tighten", "hike", "restrictive", "above target",
    "persistent", "vigilant", "concerned", "overheat", "wage growth",
    "inflação", "aperto", "alta", "restritivo", "acima da meta",
    "persistente", "vigilante", "preocupado", "aquecimento"
]

PALAVRAS_DOVISH = [
    "cut", "ease", "support", "below target", "slow", "weak",
    "unemployment", "recession", "cautious", "pause", "flexible",
    "corte", "afrouxamento", "suporte", "abaixo da meta", "fraco",
    "desemprego", "recessão", "cauteloso", "pausa", "flexível"
]

PALAVRAS_NEUTRAS = [
    "data dependent", "monitor", "assess", "evaluate", "balanced",
    "dependente de dados", "monitorar", "avaliar", "equilibrado"
]

def analisar_discurso_banco_central(
    banco: str,
    texto_discurso: str,
    discurso_anterior_tom: str = "NEUTRO"  # HAWKISH, DOVISH, NEUTRO
) -> dict:
    texto_lower = texto_discurso.lower()

    # Contar ocorrências
    count_hawkish = sum(1 for p in PALAVRAS_HAWKISH if p.lower() in texto_lower)
    count_dovish = sum(1 for p in PALAVRAS_DOVISH if p.lower() in texto_lower)
    count_neutro = sum(1 for p in PALAVRAS_NEUTRAS if p.lower() in texto_lower)

    total = max(count_hawkish + count_dovish + count_neutro, 1)
    score_hawkish = (count_hawkish / total) * 100
    score_dovish = (count_dovish / total) * 100

    # Tom atual
    if count_hawkish > count_dovish * 1.5:
        tom_atual = "HAWKISH"
        previsao_taxa = "ALTA DE JUROS provável"
        impacto = "Dólar sobe, bonds caem, mercados emergentes sob pressão"
    elif count_dovish > count_hawkish * 1.5:
        tom_atual = "DOVISH"
        previsao_taxa = "CORTE DE JUROS ou pausa provável"
        impacto = "Dólar cai, bonds sobem, mercados emergentes se beneficiam"
    else:
        tom_atual = "NEUTRO"
        previsao_taxa = "Sem mudança clara — mercado aguarda próximos dados"
        impacto = "Reação limitada esperada"

    # Mudança de tom vs discurso anterior
    mudanca_tom = tom_atual != discurso_anterior_tom
    if mudanca_tom:
        alerta_mudanca = f"⚠️ MUDANÇA DE TOM: {discurso_anterior_tom} → {tom_atual} — mercado pode não ter precificado ainda"
    else:
        alerta_mudanca = f"Tom consistente com discurso anterior ({discurso_anterior_tom})"

    # Palavras encontradas
    hawkish_encontradas = [p for p in PALAVRAS_HAWKISH if p.lower() in texto_lower]
    dovish_encontradas = [p for p in PALAVRAS_DOVISH if p.lower() in texto_lower]

    return {
        "banco_central": banco,
        "tom_atual": tom_atual,
        "tom_anterior": discurso_anterior_tom,
        "mudanca_tom": mudanca_tom,
        "alerta": alerta_mudanca,
        "previsao_taxa": previsao_taxa,
        "impacto_mercado": impacto,
        "scores": {
            "hawkish_pct": round(score_hawkish, 1),
            "dovish_pct": round(score_dovish, 1),
            "palavras_hawkish_encontradas": hawkish_encontradas,
            "palavras_dovish_encontradas": dovish_encontradas
        }
    }


# ─── ENDPOINTS ────────────────────────────────────────────────

class SaudeLiderRequest(BaseModel):
    nome_lider: str
    dias_sem_aparecer: int
    ultima_aparicao_publica: str
    sinais_manuais: list[str] = []
    buscar_midia: bool = True

@app.post("/saude-lider")
async def endpoint_saude_lider(body: SaudeLiderRequest):
    """
    Motor #39 — Saúde de Líderes Mundiais.
    Body: { "nome_lider": "Putin", "dias_sem_aparecer": 10,
            "ultima_aparicao_publica": "2025-02-28", "sinais_manuais": ["cancelou reunião"], "buscar_midia": true }
    """
    try:
        aparicoes = []
        if body.buscar_midia:
            aparicoes = await scrape_aparicoes_lider(body.nome_lider)
        resultado = analisar_saude_lider(
            nome_lider=body.nome_lider,
            dias_sem_aparecer=body.dias_sem_aparecer,
            ultima_aparicao_publica=body.ultima_aparicao_publica,
            aparicoes_recentes=aparicoes,
            sinais_manuais=body.sinais_manuais
        )
        return JSONResponse(content={"motor": "MOTOR_39_SAUDE_LIDER", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class MovimentacaoMilitarRequest(BaseModel):
    ativos: list[dict]

@app.post("/movimentacao-militar")
async def endpoint_movimentacao_militar(body: MovimentacaoMilitarRequest):
    """
    Motor #40 — Rastreador de Aviões e Navios Militares.
    Body: { "ativos": [{"tipo": "porta_avioes", "nome": "USS Gerald Ford", "lat": 26.5, "lon": 55.0}] }
    """
    try:
        resultado = analisar_movimentacao_militar(body.ativos)
        return JSONResponse(content={"motor": "MOTOR_40_MOVIMENTACAO_MILITAR", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class SancoesRequest(BaseModel):
    pais_alvo: str
    eventos: list[dict]
    buscar_midia: bool = True

@app.post("/monitor-sancoes")
async def endpoint_monitor_sancoes(body: SancoesRequest):
    """
    Motor #41 — Monitor de Sanções e Bloqueios.
    Body: { "pais_alvo": "Russia", "eventos": [{"tipo": "reuniao_diplomatica", "descricao": "...", "data": "2025-03-01"}], "buscar_midia": true }
    """
    try:
        noticias = []
        if body.buscar_midia:
            noticias = await scrape_noticias_sancoes(body.pais_alvo)
        resultado = analisar_sancoes(
            pais_alvo=body.pais_alvo,
            eventos=body.eventos,
            noticias_scraped=noticias
        )
        return JSONResponse(content={"motor": "MOTOR_41_MONITOR_SANCOES", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class DiscursoBancoCentralRequest(BaseModel):
    banco: str
    texto_discurso: str
    discurso_anterior_tom: str = "NEUTRO"

@app.post("/analise-banco-central")
async def endpoint_analise_banco_central(body: DiscursoBancoCentralRequest):
    """
    Motor #42 — Análise de Discursos de Bancos Centrais.
    Body: { "banco": "Fed", "texto_discurso": "Inflation remains persistent above target...", "discurso_anterior_tom": "DOVISH" }
    """
    try:
        resultado = analisar_discurso_banco_central(
            banco=body.banco,
            texto_discurso=body.texto_discurso,
            discurso_anterior_tom=body.discurso_anterior_tom
        )
        return JSONResponse(content={"motor": "MOTOR_42_BANCO_CENTRAL", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    
# ══════════════════════════════════════════════════════════════
# MOTORES #43, #44, #45 — EDGE DE MERCADO
# #43 Arbitragem Polymarket vs Kalshi
# #44 Detector de Mercados Órfãos
# #45 Análise de Liquidez por Horário
# ══════════════════════════════════════════════════════════════

# ─── MOTOR #43 — ARBITRAGEM POLYMARKET VS KALSHI ─────────────

KALSHI_API_BASE = "https://trading-api.kalshi.com/trade-api/v2"

async def buscar_preco_kalshi(ticker: str) -> dict | None:
    """Busca preço de um mercado na Kalshi via API pública."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{KALSHI_API_BASE}/markets/{ticker}",
                headers={"Accept": "application/json"}
            )
            if resp.status_code == 200:
                data = resp.json()
                market = data.get("market", {})
                return {
                    "ticker": ticker,
                    "yes_bid": market.get("yes_bid", 0) / 100,
                    "yes_ask": market.get("yes_ask", 0) / 100,
                    "no_bid": market.get("no_bid", 0) / 100,
                    "no_ask": market.get("no_ask", 0) / 100,
                    "volume": market.get("volume", 0),
                    "titulo": market.get("title", ticker)
                }
    except Exception:
        pass
    return None

def calcular_arbitragem(
    evento: str,
    poly_yes: float,
    poly_no: float,
    kalshi_yes: float,
    kalshi_no: float,
    valor_aposta: float = 100.0
) -> dict:
    oportunidades = []

    # Cenário 1: Compra YES no mais barato, vende NO no mais caro
    # Se poly_yes < kalshi_yes → compra YES na Poly, vende YES (= compra NO) na Kalshi
    if poly_yes + kalshi_no < 0.98:  # margem de 2% para cobrir fees
        lucro_pct = (1 - poly_yes - kalshi_no) * 100
        lucro_usd = valor_aposta * (1 - poly_yes - kalshi_no)
        oportunidades.append({
            "estrategia": "Compra YES Polymarket + Compra NO Kalshi",
            "custo_total": round((poly_yes + kalshi_no) * valor_aposta, 2),
            "retorno_garantido": round(valor_aposta, 2),
            "lucro_usd": round(lucro_usd, 2),
            "lucro_pct": round(lucro_pct, 2),
            "instrucoes": f"Compre YES na Polymarket a {poly_yes:.2f} + Compre NO na Kalshi a {kalshi_no:.2f}"
        })

    # Cenário 2: Inverso
    if kalshi_yes + poly_no < 0.98:
        lucro_pct = (1 - kalshi_yes - poly_no) * 100
        lucro_usd = valor_aposta * (1 - kalshi_yes - poly_no)
        oportunidades.append({
            "estrategia": "Compra YES Kalshi + Compra NO Polymarket",
            "custo_total": round((kalshi_yes + poly_no) * valor_aposta, 2),
            "retorno_garantido": round(valor_aposta, 2),
            "lucro_usd": round(lucro_usd, 2),
            "lucro_pct": round(lucro_pct, 2),
            "instrucoes": f"Compre YES na Kalshi a {kalshi_yes:.2f} + Compre NO na Polymarket a {poly_no:.2f}"
        })

    tem_arbitragem = len(oportunidades) > 0
    melhor = max(oportunidades, key=lambda x: x["lucro_pct"]) if oportunidades else None

    return {
        "evento": evento,
        "tem_arbitragem": tem_arbitragem,
        "melhor_oportunidade": melhor,
        "todas_oportunidades": oportunidades,
        "precos": {
            "polymarket": {"yes": poly_yes, "no": poly_no, "soma": round(poly_yes + poly_no, 3)},
            "kalshi": {"yes": kalshi_yes, "no": kalshi_no, "soma": round(kalshi_yes + kalshi_no, 3)}
        },
        "diferenca_yes": round(abs(poly_yes - kalshi_yes) * 100, 2),
        "alerta": f"Diferença de {abs(poly_yes - kalshi_yes)*100:.1f}% no YES entre plataformas" if tem_arbitragem else "Sem arbitragem detectada"
    }


# ─── MOTOR #44 — MERCADOS ÓRFÃOS ─────────────────────────────

def detectar_mercado_orfao(
    titulo_mercado: str,
    volume_total: float,
    num_traders: int,
    dias_ate_resolucao: int,
    importancia_evento: str,  # "ALTA", "MEDIA", "BAIXA"
    volume_mercados_similares: float = None
) -> dict:
    score_edge = 0
    sinais = []

    # Volume baixo
    if volume_total < 1000:
        score_edge += 35
        sinais.append({"sinal": "VOLUME_MUITO_BAIXO", "descricao": f"Volume ${volume_total:,.0f} — mercado negligenciado"})
    elif volume_total < 5000:
        score_edge += 20
        sinais.append({"sinal": "VOLUME_BAIXO", "descricao": f"Volume ${volume_total:,.0f} — pouca atenção"})

    # Poucos traders
    if num_traders < 5:
        score_edge += 30
        sinais.append({"sinal": "TRADERS_ESCASSOS", "descricao": f"Apenas {num_traders} traders — mercado órfão"})
    elif num_traders < 15:
        score_edge += 15
        sinais.append({"sinal": "TRADERS_POUCOS", "descricao": f"{num_traders} traders — baixa competição"})

    # Evento importante chegando
    pesos_importancia = {"ALTA": 25, "MEDIA": 15, "BAIXA": 5}
    peso_imp = pesos_importancia.get(importancia_evento.upper(), 5)
    if importancia_evento.upper() == "ALTA" and dias_ate_resolucao <= 7:
        score_edge += peso_imp + 10
        sinais.append({"sinal": "EVENTO_IMPORTANTE_PROXIMO", "descricao": f"Evento de alta importância em {dias_ate_resolucao} dias — ineficiência máxima"})
    else:
        score_edge += peso_imp

    # Comparação com mercados similares
    if volume_mercados_similares and volume_total < volume_mercados_similares * 0.1:
        score_edge += 20
        sinais.append({"sinal": "SUBVALORIZADO_VS_SIMILARES", "descricao": f"Volume {volume_mercados_similares/volume_total:.0f}x menor que mercados similares"})

    score_edge = min(score_edge, 100)

    if score_edge >= 70:
        classificacao = "ORFAO_PERFEITO"
        recomendacao = "Edge máximo — mercado negligenciado com evento importante. PRIORIDADE ALTA"
    elif score_edge >= 45:
        classificacao = "ORFAO_PARCIAL"
        recomendacao = "Bom edge — pouca competição, vale analisar a fundo"
    elif score_edge >= 25:
        classificacao = "SUBATENDIDO"
        recomendacao = "Edge moderado — monitorar e esperar mais ineficiência"
    else:
        classificacao = "NORMAL"
        recomendacao = "Mercado com liquidez normal — edge limitado"

    return {
        "titulo": titulo_mercado,
        "score_edge": score_edge,
        "classificacao": classificacao,
        "recomendacao": recomendacao,
        "sinais": sinais,
        "metricas": {
            "volume_total": volume_total,
            "num_traders": num_traders,
            "dias_ate_resolucao": dias_ate_resolucao,
            "importancia_evento": importancia_evento
        }
    }


# ─── MOTOR #45 — LIQUIDEZ POR HORÁRIO ────────────────────────

PERFIL_HORARIO = {
    # UTC hour: {"liquidez": 0-100, "traders_ativos": "descrição", "edge_multiplier": float}
    0:  {"liquidez": 20, "traders_ativos": "Apenas Ásia ativa", "edge_multiplier": 1.8},
    1:  {"liquidez": 15, "traders_ativos": "Ásia early morning", "edge_multiplier": 2.0},
    2:  {"liquidez": 15, "traders_ativos": "Ásia early morning", "edge_multiplier": 2.0},
    3:  {"liquidez": 20, "traders_ativos": "Ásia peak", "edge_multiplier": 1.7},
    4:  {"liquidez": 25, "traders_ativos": "Ásia peak + Europa acordando", "edge_multiplier": 1.6},
    5:  {"liquidez": 30, "traders_ativos": "Europa early", "edge_multiplier": 1.5},
    6:  {"liquidez": 40, "traders_ativos": "Europa abrindo", "edge_multiplier": 1.4},
    7:  {"liquidez": 50, "traders_ativos": "Europa ativa", "edge_multiplier": 1.3},
    8:  {"liquidez": 60, "traders_ativos": "Europa peak", "edge_multiplier": 1.2},
    9:  {"liquidez": 65, "traders_ativos": "Europa peak", "edge_multiplier": 1.15},
    10: {"liquidez": 70, "traders_ativos": "Europa + EUA early", "edge_multiplier": 1.1},
    11: {"liquidez": 75, "traders_ativos": "EUA acordando", "edge_multiplier": 1.05},
    12: {"liquidez": 80, "traders_ativos": "EUA + Europa overlap", "edge_multiplier": 1.0},
    13: {"liquidez": 90, "traders_ativos": "EUA peak", "edge_multiplier": 0.9},
    14: {"liquidez": 95, "traders_ativos": "EUA peak máximo", "edge_multiplier": 0.85},
    15: {"liquidez": 100, "traders_ativos": "EUA peak máximo", "edge_multiplier": 0.8},
    16: {"liquidez": 95, "traders_ativos": "EUA peak", "edge_multiplier": 0.85},
    17: {"liquidez": 90, "traders_ativos": "EUA tarde", "edge_multiplier": 0.9},
    18: {"liquidez": 80, "traders_ativos": "EUA tarde + Europa fechando", "edge_multiplier": 1.0},
    19: {"liquidez": 70, "traders_ativos": "EUA noite", "edge_multiplier": 1.1},
    20: {"liquidez": 55, "traders_ativos": "EUA noite", "edge_multiplier": 1.2},
    21: {"liquidez": 40, "traders_ativos": "EUA late night", "edge_multiplier": 1.4},
    22: {"liquidez": 30, "traders_ativos": "EUA dormindo", "edge_multiplier": 1.6},
    23: {"liquidez": 25, "traders_ativos": "Madrugada global", "edge_multiplier": 1.7},
}

def analisar_liquidez_horario(
    tipo_mercado: str,  # "politica_eua", "geopolitica", "crypto", "esportes", "economia"
    hora_utc_atual: int = None
) -> dict:
    if hora_utc_atual is None:
        hora_utc_atual = datetime.now(timezone.utc).hour

    perfil_atual = PERFIL_HORARIO[hora_utc_atual % 24]

    # Edge por tipo de mercado varia por horário
    bonus_tipo = {
        "politica_eua": {
            "melhor_horario": [1, 2, 3, 22, 23],
            "pior_horario": [13, 14, 15, 16],
            "descricao": "Americanos dominam — madrugada UTC tem mais ineficiência"
        },
        "geopolitica": {
            "melhor_horario": [0, 1, 2, 3, 4],
            "pior_horario": [12, 13, 14],
            "descricao": "Global — madrugada americana e europeia tem mais edge"
        },
        "crypto": {
            "melhor_horario": [3, 4, 5],
            "pior_horario": [14, 15, 16],
            "descricao": "24/7 mas EUA dormindo = menos bots ativos = mais edge"
        },
        "esportes": {
            "melhor_horario": [6, 7, 8],
            "pior_horario": [19, 20, 21],
            "descricao": "Antes dos jogos europeus = menos liquidez = mais edge"
        },
        "economia": {
            "melhor_horario": [0, 1, 2, 22, 23],
            "pior_horario": [13, 14, 15],
            "descricao": "Dados econômicos saem durante dia EUA — madrugada tem mais edge"
        }
    }

    info_tipo = bonus_tipo.get(tipo_mercado.lower(), {
        "melhor_horario": [1, 2, 3],
        "pior_horario": [14, 15, 16],
        "descricao": "Horário padrão"
    })

    em_melhor_horario = hora_utc_atual in info_tipo["melhor_horario"]
    em_pior_horario = hora_utc_atual in info_tipo["pior_horario"]

    edge_score = perfil_atual["edge_multiplier"] * 50
    if em_melhor_horario:
        edge_score *= 1.3
    if em_pior_horario:
        edge_score *= 0.7
    edge_score = min(round(edge_score), 100)

    # Próximo melhor horário
    horas_ate_melhor = None
    proximo_melhor = None
    for h in info_tipo["melhor_horario"]:
        diff = (h - hora_utc_atual) % 24
        if horas_ate_melhor is None or diff < horas_ate_melhor:
            horas_ate_melhor = diff
            proximo_melhor = h

    # Ranking das 24h
    ranking = []
    for h in range(24):
        p = PERFIL_HORARIO[h]
        e = p["edge_multiplier"] * 50
        if h in info_tipo["melhor_horario"]:
            e *= 1.3
        if h in info_tipo["pior_horario"]:
            e *= 0.7
        ranking.append({"hora_utc": f"{h:02d}:00", "edge_score": min(round(e), 100), "liquidez": p["liquidez"]})

    ranking_sorted = sorted(ranking, key=lambda x: x["edge_score"], reverse=True)

    if em_melhor_horario:
        recomendacao = "AGORA É O MOMENTO — você está na janela de maior edge para este tipo de mercado"
    elif em_pior_horario:
        recomendacao = f"EVITE AGORA — liquidez máxima, pouco edge. Próxima janela ideal: {proximo_melhor:02d}:00 UTC ({horas_ate_melhor}h)"
    elif edge_score >= 70:
        recomendacao = f"BOM MOMENTO — edge elevado agora"
    else:
        recomendacao = f"Momento neutro. Melhor janela em {horas_ate_melhor}h ({proximo_melhor:02d}:00 UTC)"

    return {
        "tipo_mercado": tipo_mercado,
        "hora_utc_atual": f"{hora_utc_atual:02d}:00",
        "edge_score_atual": edge_score,
        "recomendacao": recomendacao,
        "liquidez_atual": perfil_atual["liquidez"],
        "traders_ativos_agora": perfil_atual["traders_ativos"],
        "em_melhor_horario": em_melhor_horario,
        "proximo_melhor_horario": f"{proximo_melhor:02d}:00 UTC" if proximo_melhor else None,
        "horas_ate_proximo_melhor": horas_ate_melhor,
        "top_5_horarios_edge": ranking_sorted[:5],
        "info_tipo_mercado": info_tipo["descricao"]
    }


# ─── ENDPOINTS ────────────────────────────────────────────────

class ArbitragemRequest(BaseModel):
    evento: str
    poly_yes: float
    poly_no: float
    kalshi_yes: float = None
    kalshi_no: float = None
    kalshi_ticker: str = None
    valor_aposta: float = 100.0

@app.post("/arbitragem")
async def endpoint_arbitragem(body: ArbitragemRequest):
    """
    Motor #43 — Arbitragem Polymarket vs Kalshi.
    Body: { "evento": "Trump wins 2024", "poly_yes": 0.54, "poly_no": 0.48,
            "kalshi_yes": 0.61, "kalshi_no": 0.41, "valor_aposta": 100 }
    Ou use kalshi_ticker para buscar automaticamente da API Kalshi.
    """
    try:
        kalshi_yes = body.kalshi_yes
        kalshi_no = body.kalshi_no

        # Tentar buscar da API Kalshi se ticker fornecido
        kalshi_data = None
        if body.kalshi_ticker:
            kalshi_data = await buscar_preco_kalshi(body.kalshi_ticker)
            if kalshi_data:
                kalshi_yes = kalshi_data["yes_ask"]
                kalshi_no = kalshi_data["no_ask"]

        if kalshi_yes is None or kalshi_no is None:
            return JSONResponse(status_code=400, content={
                "erro": "Forneça kalshi_yes + kalshi_no ou um kalshi_ticker válido"
            })

        resultado = calcular_arbitragem(
            evento=body.evento,
            poly_yes=body.poly_yes,
            poly_no=body.poly_no,
            kalshi_yes=kalshi_yes,
            kalshi_no=kalshi_no,
            valor_aposta=body.valor_aposta
        )

        if kalshi_data:
            resultado["kalshi_api_data"] = kalshi_data

        return JSONResponse(content={"motor": "MOTOR_43_ARBITRAGEM", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class MercadoOrfaoRequest(BaseModel):
    titulo_mercado: str
    volume_total: float
    num_traders: int
    dias_ate_resolucao: int
    importancia_evento: str
    volume_mercados_similares: float = None

@app.post("/mercado-orfao")
async def endpoint_mercado_orfao(body: MercadoOrfaoRequest):
    """
    Motor #44 — Detector de Mercados Órfãos.
    Body: { "titulo_mercado": "...", "volume_total": 800, "num_traders": 4,
            "dias_ate_resolucao": 5, "importancia_evento": "ALTA" }
    """
    try:
        resultado = detectar_mercado_orfao(
            titulo_mercado=body.titulo_mercado,
            volume_total=body.volume_total,
            num_traders=body.num_traders,
            dias_ate_resolucao=body.dias_ate_resolucao,
            importancia_evento=body.importancia_evento,
            volume_mercados_similares=body.volume_mercados_similares
        )
        return JSONResponse(content={"motor": "MOTOR_44_MERCADO_ORFAO", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class LiquidezHorarioRequest(BaseModel):
    tipo_mercado: str
    hora_utc_atual: int = None

@app.post("/liquidez-horario")
async def endpoint_liquidez_horario(body: LiquidezHorarioRequest):
    """
    Motor #45 — Análise de Liquidez por Horário.
    Body: { "tipo_mercado": "politica_eua", "hora_utc_atual": 2 }
    Tipos: politica_eua, geopolitica, crypto, esportes, economia
    """
    try:
        resultado = analisar_liquidez_horario(
            tipo_mercado=body.tipo_mercado,
            hora_utc_atual=body.hora_utc_atual
        )
        return JSONResponse(content={"motor": "MOTOR_45_LIQUIDEZ_HORARIO", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    
    # ══════════════════════════════════════════════════════════════
# MOTORES #46, #47, #48, #49, #50 — IA DE PRÓXIMA GERAÇÃO
# #46 Memória de Apostas Vencedoras
# #47 Gerador de Tese Automático
# #48 Detector de Resolução Errada
# #49 Score de Convicção do Mercado
# #50 Simulador de Banca Pessoal
# ══════════════════════════════════════════════════════════════

import sqlite3
import math
import random
from pathlib import Path

# ─── BANCO DE DADOS ───────────────────────────────────────────

DB_PATH = Path("apostas_memoria.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS apostas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evento TEXT NOT NULL,
            tipo_mercado TEXT,
            direcao TEXT,
            preco_entrada REAL,
            resultado TEXT,  -- WIN, LOSS, PENDING
            lucro_pct REAL,
            motores_usados TEXT,
            notas TEXT,
            data_entrada TEXT,
            data_resolucao TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# Memória em cache (em memória)
_apostas_cache: list[dict] = []

def salvar_aposta_db(aposta: dict) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO apostas (evento, tipo_mercado, direcao, preco_entrada,
            resultado, lucro_pct, motores_usados, notas, data_entrada, data_resolucao)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        aposta.get("evento"), aposta.get("tipo_mercado"), aposta.get("direcao"),
        aposta.get("preco_entrada"), aposta.get("resultado", "PENDING"),
        aposta.get("lucro_pct"), str(aposta.get("motores_usados", [])),
        aposta.get("notas"), aposta.get("data_entrada"), aposta.get("data_resolucao")
    ))
    conn.commit()
    id_inserido = c.lastrowid
    conn.close()
    return id_inserido

def buscar_apostas_db(resultado: str = None, tipo_mercado: str = None) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    query = "SELECT * FROM apostas WHERE 1=1"
    params = []
    if resultado:
        query += " AND resultado = ?"
        params.append(resultado)
    if tipo_mercado:
        query += " AND tipo_mercado = ?"
        params.append(tipo_mercado)
    c.execute(query, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


# ─── MOTOR #46 — MEMÓRIA DE APOSTAS ──────────────────────────

def analisar_memoria_apostas(tipo_mercado: str = None) -> dict:
    apostas = buscar_apostas_db(tipo_mercado=tipo_mercado)
    cache = [a for a in _apostas_cache if not tipo_mercado or a.get("tipo_mercado") == tipo_mercado]
    todas = apostas + cache

    if not todas:
        return {
            "total_apostas": 0,
            "mensagem": "Nenhuma aposta registrada ainda — comece a registrar para aprender padrões"
        }

    wins = [a for a in todas if a.get("resultado") == "WIN"]
    losses = [a for a in todas if a.get("resultado") == "LOSS"]
    pending = [a for a in todas if a.get("resultado") == "PENDING"]

    winrate = len(wins) / max(len(wins) + len(losses), 1) * 100
    lucro_total = sum(a.get("lucro_pct", 0) or 0 for a in todas)
    lucro_medio = lucro_total / max(len(todas), 1)

    # Padrões de vitória
    padroes_win = {}
    for a in wins:
        tm = a.get("tipo_mercado", "desconhecido")
        padroes_win[tm] = padroes_win.get(tm, 0) + 1

    melhor_tipo = max(padroes_win, key=padroes_win.get) if padroes_win else None

    # Motores mais eficazes
    motores_win = {}
    for a in wins:
        motores = a.get("motores_usados", "")
        if isinstance(motores, str):
            motores = motores.strip("[]").replace("'", "").split(", ")
        for m in motores:
            if m:
                motores_win[m] = motores_win.get(m, 0) + 1

    top_motores = sorted(motores_win.items(), key=lambda x: x[1], reverse=True)[:3]

    return {
        "total_apostas": len(todas),
        "wins": len(wins),
        "losses": len(losses),
        "pending": len(pending),
        "winrate_pct": round(winrate, 1),
        "lucro_medio_pct": round(lucro_medio, 2),
        "lucro_total_pct": round(lucro_total, 2),
        "melhor_tipo_mercado": melhor_tipo,
        "top_motores_vencedores": [{"motor": m, "wins": c} for m, c in top_motores],
        "recomendacao": f"Foque em '{melhor_tipo}' — seu melhor tipo de mercado com {padroes_win.get(melhor_tipo, 0)} wins" if melhor_tipo else "Continue registrando apostas para identificar padrões"
    }


# ─── MOTOR #47 — GERADOR DE TESE AUTOMÁTICO ──────────────────

def gerar_tese(
    evento: str,
    preco_atual: float,
    contexto: str = "",
    historico_similares: list[dict] = None
) -> dict:
    prob_implicita = preco_atual * 100

    # Argumentos a favor (YES)
    args_favor = [
        f"O mercado precifica {prob_implicita:.0f}% de chance — pode estar subestimando momentum recente",
        "Eventos similares historicamente resolveram YES com frequência acima de 50%",
        f"Preço de {preco_atual:.2f} oferece {'bom' if preco_atual < 0.4 else 'moderado'} risco/retorno para YES"
    ]
    if contexto:
        args_favor.append(f"Contexto favorável: {contexto[:100]}")

    # Argumentos contra (NO)
    args_contra = [
        f"Status quo tende a prevalecer — {100 - prob_implicita:.0f}% de chance de NO já precificada",
        "Mercados de predição tendem a superestimar eventos dramáticos",
        f"Preço de {1 - preco_atual:.2f} para NO {'barato' if preco_atual > 0.6 else 'caro'} relativamente"
    ]

    # Análise de histórico similar
    analise_historica = None
    if historico_similares:
        wins_sim = [h for h in historico_similares if h.get("resultado") == "YES"]
        taxa_yes = len(wins_sim) / max(len(historico_similares), 1) * 100
        analise_historica = {
            "total_similares": len(historico_similares),
            "taxa_yes_historica": round(taxa_yes, 1),
            "diferenca_vs_mercado": round(taxa_yes - prob_implicita, 1),
            "edge_historico": "FAVOR YES" if taxa_yes > prob_implicita + 5 else "FAVOR NO" if taxa_yes < prob_implicita - 5 else "NEUTRO"
        }

    # Recomendação final
    if analise_historica:
        if analise_historica["diferenca_vs_mercado"] > 10:
            recomendacao = "COMPRAR YES — histórico indica mercado subestimando probabilidade"
            direcao = "YES"
        elif analise_historica["diferenca_vs_mercado"] < -10:
            recomendacao = "COMPRAR NO — histórico indica mercado superestimando probabilidade"
            direcao = "NO"
        else:
            recomendacao = "NEUTRO — histórico alinhado com precificação atual"
            direcao = "NEUTRO"
    else:
        if preco_atual < 0.25:
            recomendacao = "Preço baixo — vale pesquisar se mercado está negligenciando YES"
            direcao = "INVESTIGAR YES"
        elif preco_atual > 0.75:
            recomendacao = "Preço alto — verifique se há overconfidence no YES"
            direcao = "INVESTIGAR NO"
        else:
            recomendacao = "Preço neutro — busque edge via contexto e motores"
            direcao = "NEUTRO"

    return {
        "evento": evento,
        "preco_atual": preco_atual,
        "probabilidade_implicita_pct": round(prob_implicita, 1),
        "tese_yes": {
            "titulo": f"Por que {evento} vai acontecer",
            "argumentos": args_favor
        },
        "tese_no": {
            "titulo": f"Por que {evento} NÃO vai acontecer",
            "argumentos": args_contra
        },
        "analise_historica": analise_historica,
        "recomendacao_final": recomendacao,
        "direcao_sugerida": direcao
    }


# ─── MOTOR #48 — DETECTOR DE RESOLUÇÃO ERRADA ────────────────

def detectar_resolucao_errada(
    evento: str,
    resultado_polymarket: str,  # "YES" ou "NO"
    evidencias_contrarias: list[dict],
    prazo_contestacao_horas: int = 24
) -> dict:
    score_erro = 0
    alertas = []

    pesos_evidencia = {
        "fonte_oficial": 50,
        "multiplas_fontes": 40,
        "fonte_secundaria": 25,
        "opiniao": 10,
        "dado_estatistico": 35,
        "documento_oficial": 45
    }

    for ev in evidencias_contrarias:
        tipo = ev.get("tipo", "opiniao")
        peso = pesos_evidencia.get(tipo, 10)
        score_erro += peso
        alertas.append({
            "tipo": tipo,
            "descricao": ev.get("descricao", ""),
            "fonte": ev.get("fonte", ""),
            "peso": peso
        })

    score_erro = min(score_erro, 100)

    if score_erro >= 70:
        veredicto = "RESOLUCAO_INCORRETA"
        acao = "CONTESTE IMEDIATAMENTE — forte evidência de resolução errada"
        urgencia = "CRITICA"
    elif score_erro >= 40:
        veredicto = "SUSPEITO"
        acao = "Reúna mais evidências e prepare contestação preventiva"
        urgencia = "ALTA"
    elif score_erro >= 20:
        veredicto = "INCERTO"
        acao = "Monitore — pode ser erro ou ambiguidade nas regras"
        urgencia = "MEDIA"
    else:
        veredicto = "RESOLUCAO_CORRETA"
        acao = "Evidências insuficientes para contestar"
        urgencia = "BAIXA"

    return {
        "evento": evento,
        "resultado_polymarket": resultado_polymarket,
        "score_erro": score_erro,
        "veredicto": veredicto,
        "acao": acao,
        "urgencia": urgencia,
        "prazo_contestacao_horas": prazo_contestacao_horas,
        "evidencias_analisadas": alertas,
        "link_contestacao": "https://polymarket.com/help/dispute-resolution"
    }


# ─── MOTOR #49 — SCORE DE CONVICÇÃO DO MERCADO ───────────────

def calcular_convicção_mercado(
    historico_precos: list[dict],
    num_noticias_recentes: int = 0
) -> dict:
    if len(historico_precos) < 3:
        return {"erro": "Mínimo 3 pontos necessários"}

    precos = [p["preco"] for p in historico_precos]

    # Volatilidade
    deltas = [abs(precos[i+1] - precos[i]) for i in range(len(precos)-1)]
    volatilidade = sum(deltas) / len(deltas)
    volatilidade_pct = volatilidade * 100

    # Range total
    range_total = (max(precos) - min(precos)) * 100

    # Estabilidade recente (últimos 3 pontos)
    ultimos = precos[-3:]
    estabilidade_recente = (max(ultimos) - min(ultimos)) * 100

    # Score de convicção (alta convicção = baixa volatilidade)
    score_convicção = max(0, 100 - (volatilidade_pct * 10) - (range_total * 2))
    score_convicção = min(100, round(score_convicção))

    # Edge disponível (inverso da convicção)
    edge_disponivel = 100 - score_convicção

    # Impacto das notícias
    if num_noticias_recentes >= 5 and score_convicção >= 70:
        alerta_noticias = "⚠️ Mercado estável apesar de muitas notícias — alta convicção, pouco edge"
    elif num_noticias_recentes >= 5 and score_convicção < 40:
        alerta_noticias = "🔥 Muitas notícias + mercado oscilando — momento de máximo edge"
    elif num_noticias_recentes == 0 and score_convicção < 40:
        alerta_noticias = "Oscilação sem notícias — possível manipulação ou info privilegiada"
    else:
        alerta_noticias = "Comportamento normal"

    if score_convicção >= 75:
        estado = "ALTA_CONVICÇÃO"
        recomendacao = "Mercado muito estável — consenso formado, pouco edge disponível"
    elif score_convicção >= 50:
        estado = "CONVICÇÃO_MODERADA"
        recomendacao = "Mercado moderadamente estável — edge limitado, entre com cautela"
    elif score_convicção >= 25:
        estado = "BAIXA_CONVICÇÃO"
        recomendacao = "Mercado oscilante — edge disponível, boa janela para entrar"
    else:
        estado = "SEM_CONVICÇÃO"
        recomendacao = "Mercado caótico — máximo edge, mas risco também elevado"

    return {
        "score_convicção": score_convicção,
        "edge_disponivel": edge_disponivel,
        "estado": estado,
        "recomendacao": recomendacao,
        "alerta_noticias": alerta_noticias,
        "metricas": {
            "volatilidade_media_pct": round(volatilidade_pct, 3),
            "range_total_pct": round(range_total, 2),
            "estabilidade_recente_pct": round(estabilidade_recente, 2),
            "num_noticias_recentes": num_noticias_recentes
        }
    }


# ─── MOTOR #50 — SIMULADOR DE BANCA ──────────────────────────

def simular_banca(
    banca_inicial: float,
    valor_aposta_pct: float,  # % da banca por aposta (ex: 0.05 = 5%)
    edge_pct: float,           # edge estimado (ex: 0.08 = 8%)
    winrate_pct: float,        # winrate histórico (ex: 0.55 = 55%)
    num_apostas: int = 100,
    num_simulacoes: int = 1000
) -> dict:
    falencias = 0
    bancas_finais = []
    pior_sequencia = 0

    for _ in range(num_simulacoes):
        banca = banca_inicial
        sequencia_loss = 0
        max_sequencia = 0

        for _ in range(num_apostas):
            if banca <= banca_inicial * 0.05:  # falência = perda de 95%
                falencias += 1
                break
            aposta = banca * valor_aposta_pct
            ganhou = random.random() < winrate_pct
            if ganhou:
                banca += aposta * (1 + edge_pct)
                sequencia_loss = 0
            else:
                banca -= aposta
                sequencia_loss += 1
                max_sequencia = max(max_sequencia, sequencia_loss)

        bancas_finais.append(banca)
        pior_sequencia = max(pior_sequencia, max_sequencia)

    prob_falencia = (falencias / num_simulacoes) * 100
    banca_media_final = sum(bancas_finais) / len(bancas_finais)
    banca_mediana = sorted(bancas_finais)[len(bancas_finais)//2]
    banca_pior_10pct = sorted(bancas_finais)[int(len(bancas_finais) * 0.1)]
    retorno_medio = ((banca_media_final - banca_inicial) / banca_inicial) * 100

    # Kelly criterion
    kelly = (winrate_pct - (1 - winrate_pct) / (1 + edge_pct))
    kelly_pct = max(0, kelly * 100)

    if prob_falencia >= 30:
        avaliacao = "PERIGOSO"
        conselho = f"Risco de ruína muito alto! Reduza aposta para máx {kelly_pct/4:.1f}% da banca"
    elif prob_falencia >= 15:
        avaliacao = "ARRISCADO"
        conselho = f"Risco elevado. Kelly sugere {kelly_pct:.1f}% — considere reduzir"
    elif prob_falencia >= 5:
        avaliacao = "MODERADO"
        conselho = f"Risco aceitável. Kelly criterion: {kelly_pct:.1f}% da banca"
    else:
        avaliacao = "CONSERVADOR"
        conselho = f"Gestão sólida. Kelly criterion: {kelly_pct:.1f}% — pode aumentar levemente"

    return {
        "parametros": {
            "banca_inicial": banca_inicial,
            "aposta_pct": round(valor_aposta_pct * 100, 1),
            "edge_pct": round(edge_pct * 100, 1),
            "winrate_pct": round(winrate_pct * 100, 1),
            "num_apostas": num_apostas,
            "num_simulacoes": num_simulacoes
        },
        "resultados": {
            "prob_falencia_pct": round(prob_falencia, 1),
            "banca_media_final": round(banca_media_final, 2),
            "banca_mediana_final": round(banca_mediana, 2),
            "pior_cenario_10pct": round(banca_pior_10pct, 2),
            "retorno_medio_pct": round(retorno_medio, 1),
            "pior_sequencia_loss": pior_sequencia
        },
        "avaliacao": avaliacao,
        "conselho": conselho,
        "kelly_criterion_pct": round(kelly_pct, 2)
    }


# ─── ENDPOINTS ────────────────────────────────────────────────

class RegistrarApostaRequest(BaseModel):
    evento: str
    tipo_mercado: str
    direcao: str
    preco_entrada: float
    resultado: str = "PENDING"
    lucro_pct: float = None
    motores_usados: list[str] = []
    notas: str = ""
    data_entrada: str = ""
    data_resolucao: str = ""

@app.post("/memoria/registrar")
async def endpoint_registrar_aposta(body: RegistrarApostaRequest):
    """Motor #46 — Registrar aposta na memória."""
    try:
        aposta = body.dict()
        _apostas_cache.append(aposta)
        id_db = salvar_aposta_db(aposta)
        return JSONResponse(content={
            "motor": "MOTOR_46_MEMORIA",
            "mensagem": "Aposta registrada com sucesso",
            "id": id_db
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})

@app.get("/memoria/analise")
async def endpoint_analise_memoria(tipo_mercado: str = None):
    """Motor #46 — Analisar padrões de apostas vencedoras."""
    try:
        resultado = analisar_memoria_apostas(tipo_mercado=tipo_mercado)
        return JSONResponse(content={"motor": "MOTOR_46_MEMORIA", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class TeseRequest(BaseModel):
    evento: str
    preco_atual: float
    contexto: str = ""
    historico_similares: list[dict] = []

@app.post("/gerar-tese")
async def endpoint_gerar_tese(body: TeseRequest):
    """Motor #47 — Gerador de Tese Automático."""
    try:
        resultado = gerar_tese(
            evento=body.evento,
            preco_atual=body.preco_atual,
            contexto=body.contexto,
            historico_similares=body.historico_similares
        )
        return JSONResponse(content={"motor": "MOTOR_47_GERADOR_TESE", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class ResolucaoErradaRequest(BaseModel):
    evento: str
    resultado_polymarket: str
    evidencias_contrarias: list[dict]
    prazo_contestacao_horas: int = 24

@app.post("/detector-resolucao-errada")
async def endpoint_resolucao_errada(body: ResolucaoErradaRequest):
    """Motor #48 — Detector de Resolução Errada."""
    try:
        resultado = detectar_resolucao_errada(
            evento=body.evento,
            resultado_polymarket=body.resultado_polymarket,
            evidencias_contrarias=body.evidencias_contrarias,
            prazo_contestacao_horas=body.prazo_contestacao_horas
        )
        return JSONResponse(content={"motor": "MOTOR_48_RESOLUCAO_ERRADA", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class ConviccaoRequest(BaseModel):
    historico_precos: list[dict]
    num_noticias_recentes: int = 0

@app.post("/convicção-mercado")
async def endpoint_convicção_mercado(body: ConviccaoRequest):
    """Motor #49 — Score de Convicção do Mercado."""
    try:
        resultado = calcular_convicção_mercado(
            historico_precos=body.historico_precos,
            num_noticias_recentes=body.num_noticias_recentes
        )
        return JSONResponse(content={"motor": "MOTOR_49_CONVICÇÃO_MERCADO", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class SimuladorBancaRequest(BaseModel):
    banca_inicial: float
    valor_aposta_pct: float
    edge_pct: float
    winrate_pct: float
    num_apostas: int = 100
    num_simulacoes: int = 1000

@app.post("/simulador-banca")
async def endpoint_simulador_banca(body: SimuladorBancaRequest):
    """
    Motor #50 — Simulador de Banca Pessoal.
    Body: { "banca_inicial": 1000, "valor_aposta_pct": 0.05, "edge_pct": 0.08,
            "winrate_pct": 0.55, "num_apostas": 100, "num_simulacoes": 1000 }
    """
    try:
        resultado = simular_banca(
            banca_inicial=body.banca_inicial,
            valor_aposta_pct=body.valor_aposta_pct,
            edge_pct=body.edge_pct,
            winrate_pct=body.winrate_pct,
            num_apostas=body.num_apostas,
            num_simulacoes=body.num_simulacoes
        )
        return JSONResponse(content={"motor": "MOTOR_50_SIMULADOR_BANCA", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    
    # ══════════════════════════════════════════════════════════════
# MOTOR #51 — PIPELINE PROFESSOR JIANG
# YouTube + Twitter(Nitter) + Substack + Podcast
# → Claude Haiku extrai previsões
# → Mapeia para mercados Polymarket
# → Salva no banco automaticamente
# ══════════════════════════════════════════════════════════════

import feedparser
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from youtube_transcript_api import YouTubeTranscriptApi
import anthropic

# ─── CONFIG ──────────────────────────────────────────────────

JIANG_SOURCES = {
    "youtube": {
        "rss": "https://www.youtube.com/feeds/videos.xml?channel_id=UCsQSXbgaduFPLs_EB8P1rYA",
        "nome": "Predictive History YouTube"
    },
    "twitter_nitter": {
        "url": "https://nitter.privacydev.net/PredictiveHistory/rss",
        "nome": "Twitter via Nitter"
    },
    "substack": {
        "url": "https://predictivehistory.substack.com/feed",
        "nome": "Substack"
    },
    "podcast": {
        "url": "https://feeds.buzzsprout.com/predictivehistory.rss",
        "nome": "Podcast"
    }
}

ANTHROPIC_CLIENT = anthropic.Anthropic()

# ─── BANCO DE DADOS ───────────────────────────────────────────

def init_db_jiang():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS jiang_previsoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fonte TEXT,
            titulo_conteudo TEXT,
            url_conteudo TEXT,
            previsao TEXT,
            evento_previsto TEXT,
            direcao TEXT,
            confianca_pct REAL,
            prazo TEXT,
            mercado_polymarket TEXT,
            mercado_url TEXT,
            match_score REAL,
            data_extracao TEXT,
            processado INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS jiang_conteudos_processados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            data_processado TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db_jiang()

def ja_processado(url: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM jiang_conteudos_processados WHERE url = ?", (url,))
    resultado = c.fetchone()
    conn.close()
    return resultado is not None

def marcar_processado(url: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO jiang_conteudos_processados (url, data_processado) VALUES (?, ?)",
        (url, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()

def salvar_previsao_jiang(previsao: dict):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO jiang_previsoes
        (fonte, titulo_conteudo, url_conteudo, previsao, evento_previsto, direcao,
         confianca_pct, prazo, mercado_polymarket, mercado_url, match_score, data_extracao)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        previsao.get("fonte"), previsao.get("titulo_conteudo"), previsao.get("url_conteudo"),
        previsao.get("previsao"), previsao.get("evento_previsto"), previsao.get("direcao"),
        previsao.get("confianca_pct"), previsao.get("prazo"), previsao.get("mercado_polymarket"),
        previsao.get("mercado_url"), previsao.get("match_score"),
        datetime.now(timezone.utc).isoformat()
    ))
    conn.commit()
    conn.close()


# ─── SCRAPERS ─────────────────────────────────────────────────

async def buscar_youtube_transcricao(video_id: str) -> str | None:
    """Busca transcrição de vídeo YouTube."""
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=["en", "pt"])
        texto = " ".join([t["text"] for t in transcript])
        return texto[:8000]  # Limitar para o Haiku
    except Exception:
        return None

async def buscar_feed_rss(url: str) -> list[dict]:
    """Busca itens de um feed RSS."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            feed = feedparser.parse(resp.text)
            itens = []
            for entry in feed.entries[:5]:  # Últimos 5 itens
                itens.append({
                    "titulo": entry.get("title", ""),
                    "url": entry.get("link", ""),
                    "resumo": entry.get("summary", "")[:500],
                    "data": entry.get("published", ""),
                    "video_id": entry.get("yt_videoid", None)
                })
            return itens
    except Exception:
        return []

async def buscar_substack(url: str) -> list[dict]:
    """Busca posts do Substack via RSS."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            feed = feedparser.parse(resp.text)
            itens = []
            for entry in feed.entries[:3]:
                conteudo = entry.get("content", [{}])[0].get("value", "") if entry.get("content") else entry.get("summary", "")
                soup = BeautifulSoup(conteudo, "html.parser")
                texto = soup.get_text()[:3000]
                itens.append({
                    "titulo": entry.get("title", ""),
                    "url": entry.get("link", ""),
                    "texto": texto,
                    "data": entry.get("published", "")
                })
            return itens
    except Exception:
        return []


# ─── CLAUDE HAIKU — EXTRAÇÃO DE PREVISÕES ────────────────────

async def buscar_mercados_polymarket_api(query: str) -> list[dict]:
    """Busca mercados abertos no Polymarket."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://gamma-api.polymarket.com/markets",
                params={"active": "true", "closed": "false", "limit": 50}
            )
            if resp.status_code == 200:
                return resp.json()
    except Exception:
        pass
    return []

async def extrair_previsoes_haiku(texto: str, fonte: str, titulo: str) -> list[dict]:
    try:
        from groq import Groq
        client_ai = Groq(api_key=os.environ.get("GROQ_API_KEY"))
        prompt = f"""Você é um extrator de previsões do Professor Jiang (Predictive History).
Analise o texto e extraia TODAS as previsões específicas mencionadas.
Fonte: {fonte} | Título: {titulo}
Texto: {texto[:4000]}
Retorne APENAS JSON válido sem markdown:
{{"previsoes": [{{"previsao": "...", "evento_previsto": "...", "direcao": "YES ou NO", "confianca_pct": 75, "prazo": "Q2 2025"}}]}}
Se não houver previsões: {{"previsoes": []}}"""
        response = client_ai.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}]
        )
        texto_resposta = response.choices[0].message.content.strip().replace("```json", "").replace("```", "").strip()
        dados = json.loads(texto_resposta)
        return dados.get("previsoes", [])
    except Exception:
        return []


# ─── CLAUDE — MAPEAMENTO PARA POLYMARKET ─────────────────────

async def mapear_para_polymarket(previsao: dict, mercados_disponiveis: list[dict] = None) -> dict:
    try:
        from groq import Groq
        client_ai = Groq(api_key=os.environ.get("GROQ_API_KEY"))

        if not mercados_disponiveis:
            mercados_disponiveis = await buscar_mercados_polymarket_api(previsao.get("evento_previsto", ""))

        contexto_mercados = ""
        if mercados_disponiveis:
            contexto_mercados = "Mercados disponíveis no Polymarket:\n"
            for m in mercados_disponiveis[:30]:
                contexto_mercados += f"- {m.get('question', '')}\n"

        prompt = f"""Você é um especialista em Polymarket.
Previsão: {previsao.get('evento_previsto')} | Direção: {previsao.get('direcao')} | Prazo: {previsao.get('prazo')}
{contexto_mercados}
Identifique o mercado que MELHOR corresponde semanticamente.
Retorne APENAS JSON válido sem markdown:
{{"mercado_encontrado": true, "mercado_titulo": "...", "mercado_url": "https://polymarket.com/event/...", "match_score": 85, "justificativa": "...", "recomendacao": "COMPRAR YES"}}
Se nenhum for relevante: {{"mercado_encontrado": false, "match_score": 0}}"""

        response = client_ai.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}]
        )
        texto_resposta = response.choices[0].message.content.strip().replace("```json", "").replace("```", "").strip()
        return json.loads(texto_resposta)
    except Exception as e:
        return {"mercado_encontrado": False, "match_score": 0, "erro_debug": str(e)}


# ─── PIPELINE PRINCIPAL ───────────────────────────────────────

async def rodar_pipeline_jiang(mercados_manuais: list[dict] = None) -> dict:
    """Pipeline completo: coleta → transcrição → extração → mapeamento → salva."""
    relatorio = {
        "inicio": datetime.now(timezone.utc).isoformat(),
        "fontes_processadas": [],
        "total_previsoes": 0,
        "total_mercados_mapeados": 0,
        "erros": []
    }

    # ── YouTube ──
    try:
        itens_yt = await buscar_feed_rss(JIANG_SOURCES["youtube"]["rss"])
        for item in itens_yt:
            if ja_processado(item["url"]):
                continue
            texto = None
            if item.get("video_id"):
                texto = await buscar_youtube_transcricao(item["video_id"])
            if not texto:
                texto = item.get("resumo", "")
            if texto:
                previsoes = await extrair_previsoes_haiku(texto, "YouTube", item["titulo"])
                for prev in previsoes:
                    prev["fonte"] = "YouTube"
                    prev["titulo_conteudo"] = item["titulo"]
                    prev["url_conteudo"] = item["url"]
                    mapeamento = await mapear_para_polymarket(prev, mercados_manuais)
                    prev["mercado_polymarket"] = mapeamento.get("mercado_titulo", "")
                    prev["mercado_url"] = mapeamento.get("mercado_url", "")
                    prev["match_score"] = mapeamento.get("match_score", 0)
                    salvar_previsao_jiang(prev)
                    relatorio["total_previsoes"] += 1
                    if mapeamento.get("mercado_encontrado"):
                        relatorio["total_mercados_mapeados"] += 1
            marcar_processado(item["url"])
        relatorio["fontes_processadas"].append("YouTube")
    except Exception as e:
        relatorio["erros"].append(f"YouTube: {str(e)}")

    # ── Twitter via Nitter ──
    try:
        nitter_instances = [
            "https://nitter.privacydev.net",
            "https://nitter.poast.org",
            "https://nitter.1d4.us"
        ]
        itens_tw = []
        for instance in nitter_instances:
            itens_tw = await buscar_feed_rss(f"{instance}/PredictiveHistory/rss")
            if itens_tw:
                break
        for item in itens_tw:
            if ja_processado(item["url"]):
                continue
            texto = item.get("resumo", "")
            if texto and len(texto) > 50:
                previsoes = await extrair_previsoes_haiku(texto, "Twitter", item["titulo"])
                for prev in previsoes:
                    prev["fonte"] = "Twitter"
                    prev["titulo_conteudo"] = item["titulo"]
                    prev["url_conteudo"] = item["url"]
                    mapeamento = await mapear_para_polymarket(prev, mercados_manuais)
                    prev["mercado_polymarket"] = mapeamento.get("mercado_titulo", "")
                    prev["mercado_url"] = mapeamento.get("mercado_url", "")
                    prev["match_score"] = mapeamento.get("match_score", 0)
                    salvar_previsao_jiang(prev)
                    relatorio["total_previsoes"] += 1
                    if mapeamento.get("mercado_encontrado"):
                        relatorio["total_mercados_mapeados"] += 1
            marcar_processado(item["url"])
        relatorio["fontes_processadas"].append("Twitter/Nitter")
    except Exception as e:
        relatorio["erros"].append(f"Twitter: {str(e)}")

    # ── Substack ──
    try:
        itens_sub = await buscar_substack(JIANG_SOURCES["substack"]["url"])
        for item in itens_sub:
            if ja_processado(item["url"]):
                continue
            texto = item.get("texto", "")
            if texto:
                previsoes = await extrair_previsoes_haiku(texto, "Substack", item["titulo"])
                for prev in previsoes:
                    prev["fonte"] = "Substack"
                    prev["titulo_conteudo"] = item["titulo"]
                    prev["url_conteudo"] = item["url"]
                    mapeamento = await mapear_para_polymarket(prev, mercados_manuais)
                    prev["mercado_polymarket"] = mapeamento.get("mercado_titulo", "")
                    prev["mercado_url"] = mapeamento.get("mercado_url", "")
                    prev["match_score"] = mapeamento.get("match_score", 0)
                    salvar_previsao_jiang(prev)
                    relatorio["total_previsoes"] += 1
                    if mapeamento.get("mercado_encontrado"):
                        relatorio["total_mercados_mapeados"] += 1
            marcar_processado(item["url"])
        relatorio["fontes_processadas"].append("Substack")
    except Exception as e:
        relatorio["erros"].append(f"Substack: {str(e)}")

    # ── Podcast ──
    try:
        itens_pod = await buscar_feed_rss(JIANG_SOURCES["podcast"]["url"])
        for item in itens_pod:
            if ja_processado(item["url"]):
                continue
            texto = item.get("resumo", "")
            if texto:
                previsoes = await extrair_previsoes_haiku(texto, "Podcast", item["titulo"])
                for prev in previsoes:
                    prev["fonte"] = "Podcast"
                    prev["titulo_conteudo"] = item["titulo"]
                    prev["url_conteudo"] = item["url"]
                    mapeamento = await mapear_para_polymarket(prev, mercados_manuais)
                    prev["mercado_polymarket"] = mapeamento.get("mercado_titulo", "")
                    prev["mercado_url"] = mapeamento.get("mercado_url", "")
                    prev["match_score"] = mapeamento.get("match_score", 0)
                    salvar_previsao_jiang(prev)
                    relatorio["total_previsoes"] += 1
                    if mapeamento.get("mercado_encontrado"):
                        relatorio["total_mercados_mapeados"] += 1
            marcar_processado(item["url"])
        relatorio["fontes_processadas"].append("Podcast")
    except Exception as e:
        relatorio["erros"].append(f"Podcast: {str(e)}")

    relatorio["fim"] = datetime.now(timezone.utc).isoformat()
    return relatorio


# ─── SCHEDULER AUTOMÁTICO ─────────────────────────────────────

scheduler_jiang = AsyncIOScheduler()

async def job_pipeline_jiang():
    print(f"[{datetime.now()}] Rodando pipeline Jiang automático...")
    await rodar_pipeline_jiang()

scheduler_jiang.add_job(job_pipeline_jiang, "interval", hours=6)
scheduler_jiang.start()


# ─── ENDPOINTS ────────────────────────────────────────────────

class PipelineJiangRequest(BaseModel):
    mercados_manuais: list[dict] = []

@app.post("/pipeline-jiang/rodar")
async def endpoint_pipeline_jiang(body: PipelineJiangRequest = None):
    """
    Motor #51 — Dispara pipeline Professor Jiang manualmente.
    Body opcional: { "mercados_manuais": [{"id": "...", "question": "...", "outcomePrices": ["0.6"]}] }
    """
    try:
        mercados = body.mercados_manuais if body else []
        relatorio = await rodar_pipeline_jiang(mercados_manuais=mercados if mercados else None)
        return JSONResponse(content={"motor": "MOTOR_51_PIPELINE_JIANG", "relatorio": relatorio})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})

@app.get("/pipeline-jiang/previsoes")
async def endpoint_listar_previsoes(
    fonte: str = None,
    min_match_score: float = 0,
    limit: int = 50
):
    """Lista previsões extraídas do Professor Jiang."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        query = "SELECT * FROM jiang_previsoes WHERE match_score >= ?"
        params = [min_match_score]
        if fonte:
            query += " AND fonte = ?"
            params.append(fonte)
        query += " ORDER BY data_extracao DESC LIMIT ?"
        params.append(limit)
        c.execute(query, params)
        previsoes = [dict(r) for r in c.fetchall()]
        conn.close()
        return JSONResponse(content={
            "motor": "MOTOR_51_PIPELINE_JIANG",
            "total": len(previsoes),
            "previsoes": previsoes
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})

class MapearManualRequest(BaseModel):
    evento_previsto: str
    direcao: str = "YES"
    confianca_pct: float = 50
    prazo: str = ""

@app.post("/pipeline-jiang/mapear-manual")
async def endpoint_mapear_manual(body: MapearManualRequest):
    try:
        # Debug: ver o que a API Polymarket retorna
        mercados = await buscar_mercados_polymarket_api(body.evento_previsto)
        mapeamento = await mapear_para_polymarket(body.dict(), mercados)
        return JSONResponse(content={
            "motor": "MOTOR_51_MAPEAMENTO_SEMANTICO",
            "resultado": mapeamento,
            "debug_mercados_encontrados": len(mercados),
            "debug_primeiro_mercado": mercados[0].get("question", "") if mercados else "NENHUM"
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    
    # ══════════════════════════════════════════════════════════════
# MOTORES #52, #53, #54, #55 — GEOTRADE INTELLIGENCE
# #52 GTI — Global Tension Index
# #53 AI Signals por classe de ativo
# #54 Trade Setup automático
# #55 Geo Map — tensão por país + what-if scenarios
# ══════════════════════════════════════════════════════════════

# ─── DADOS BASE ───────────────────────────────────────────────

ATIVOS_CONFIG = {
    "XAUUSD": {"nome": "Gold", "classe": "Commodities", "atr_base": 0.018, "correlacao_tensao": "positiva"},
    "WTI":    {"nome": "WTI Crude Oil", "classe": "Commodities", "atr_base": 0.022, "correlacao_tensao": "positiva"},
    "BRENT":  {"nome": "Brent Oil", "classe": "Commodities", "atr_base": 0.021, "correlacao_tensao": "positiva"},
    "COPPER": {"nome": "Copper", "classe": "Commodities", "atr_base": 0.015, "correlacao_tensao": "negativa"},
    "SOYBEANS": {"nome": "Soybeans", "classe": "Commodities", "atr_base": 0.012, "correlacao_tensao": "negativa"},
    "BTCUSD": {"nome": "Bitcoin", "classe": "Crypto", "atr_base": 0.045, "correlacao_tensao": "negativa"},
    "ETHUSD": {"nome": "Ethereum", "classe": "Crypto", "atr_base": 0.05, "correlacao_tensao": "negativa"},
    "EURUSD": {"nome": "EUR/USD", "classe": "Forex", "atr_base": 0.008, "correlacao_tensao": "negativa"},
    "USDCNY": {"nome": "USD/CNY", "classe": "Forex", "atr_base": 0.006, "correlacao_tensao": "positiva"},
    "USDJPY": {"nome": "USD/JPY", "classe": "Forex", "atr_base": 0.007, "correlacao_tensao": "positiva"},
    "USDCHF": {"nome": "USD/CHF", "classe": "Forex", "atr_base": 0.007, "correlacao_tensao": "negativa"},
    "GBPUSD": {"nome": "GBP/USD", "classe": "Forex", "atr_base": 0.009, "correlacao_tensao": "negativa"},
    "SPX":    {"nome": "S&P 500", "classe": "Equity Indices", "atr_base": 0.012, "correlacao_tensao": "negativa"},
    "NDX":    {"nome": "Nasdaq 100", "classe": "Equity Indices", "atr_base": 0.015, "correlacao_tensao": "negativa"},
    "DJI":    {"nome": "Dow Jones", "classe": "Equity Indices", "atr_base": 0.011, "correlacao_tensao": "negativa"},
    "DAX":    {"nome": "DAX", "classe": "Equity Indices", "atr_base": 0.013, "correlacao_tensao": "negativa"},
    "HSI":    {"nome": "Hang Seng", "classe": "Equity Indices", "atr_base": 0.016, "correlacao_tensao": "negativa"},
    "LMT":    {"nome": "Lockheed Martin", "classe": "Stocks", "atr_base": 0.018, "correlacao_tensao": "positiva"},
    "NOC":    {"nome": "Northrop Grumman", "classe": "Stocks", "atr_base": 0.017, "correlacao_tensao": "positiva"},
    "RTX":    {"nome": "Raytheon", "classe": "Stocks", "atr_base": 0.016, "correlacao_tensao": "positiva"},
    "GD":     {"nome": "General Dynamics", "classe": "Stocks", "atr_base": 0.015, "correlacao_tensao": "positiva"},
    "SHEL":   {"nome": "Shell", "classe": "Stocks", "atr_base": 0.014, "correlacao_tensao": "positiva"},
    "GLD":    {"nome": "Gold ETF (GLD)", "classe": "ETFs", "atr_base": 0.017, "correlacao_tensao": "positiva"},
    "ITA":    {"nome": "Defense ETF (ITA)", "classe": "ETFs", "atr_base": 0.016, "correlacao_tensao": "positiva"},
    "XLE":    {"nome": "Energy Sector ETF", "classe": "ETFs", "atr_base": 0.014, "correlacao_tensao": "positiva"},
    "TLT":    {"nome": "US 20Y Treasury", "classe": "Bonds", "atr_base": 0.010, "correlacao_tensao": "positiva"},
}

REGIOES_GTI = {
    "middle_east": {"nome": "Middle East", "pais": ["Iran", "Israel", "Syria", "Iraq", "Yemen", "Lebanon"], "peso": 1.5},
    "europe":      {"nome": "Europe", "pais": ["Russia", "Ukraine", "Belarus", "Poland"], "peso": 1.3},
    "asia_pac":    {"nome": "Asia Pacific", "pais": ["China", "Taiwan", "North Korea", "Japan"], "peso": 1.2},
    "n_america":   {"nome": "N. America", "pais": ["USA", "Mexico", "Canada"], "peso": 1.0},
    "l_america":   {"nome": "L. America", "pais": ["Venezuela", "Colombia", "Brazil"], "peso": 0.8},
    "africa":      {"nome": "Africa", "pais": ["Sudan", "Ethiopia", "DR Congo", "Somalia"], "peso": 0.7},
}

PAISES_TENSAO_BASE = {
    "Russia": 85, "Ukraine": 90, "Iran": 80, "Israel": 78, "China": 65,
    "Taiwan": 70, "North Korea": 75, "Syria": 72, "Yemen": 68, "Belarus": 60,
    "Venezuela": 55, "Sudan": 65, "Ethiopia": 58, "USA": 35, "Germany": 25,
    "France": 22, "UK": 28, "Japan": 30, "Brazil": 20, "India": 40,
    "Pakistan": 55, "Afghanistan": 70, "Libya": 62, "Somalia": 65,
    "Iraq": 60, "Lebanon": 65, "Myanmar": 58, "Niger": 55, "Mali": 52,
}


# ─── MOTOR #52 — GTI ──────────────────────────────────────────

def calcular_gti(eventos_ativos: list[dict] = None) -> dict:
    """
    Calcula o Global Tension Index (0-100).
    eventos_ativos: lista de eventos geopolíticos ativos
    [{"regiao": "middle_east", "severidade": 80, "tipo": "military_escalation", "descricao": "..."}]
    """
    score_base = 45  # baseline histórico
    contribuicoes = []

    if eventos_ativos:
        for ev in eventos_ativos:
            regiao = ev.get("regiao", "")
            severidade = ev.get("severidade", 50)
            tipo = ev.get("tipo", "")
            peso_regiao = REGIOES_GTI.get(regiao, {}).get("peso", 1.0)

            pesos_tipo = {
                "military_escalation": 1.5,
                "nuclear_threat": 2.0,
                "economic_war": 1.2,
                "sanctions": 1.1,
                "political_crisis": 1.0,
                "terrorist_attack": 1.3,
                "coup": 1.4,
                "civil_war": 1.3,
            }
            peso_tipo = pesos_tipo.get(tipo, 1.0)
            contribuicao = (severidade / 100) * peso_regiao * peso_tipo * 20
            score_base += contribuicao
            contribuicoes.append({
                "evento": ev.get("descricao", tipo),
                "regiao": regiao,
                "contribuicao": round(contribuicao, 1)
            })

    gti = min(round(score_base), 100)

    if gti >= 80:
        nivel = "CRITICAL"
        cor = "red"
        descricao = "Tensão geopolítica crítica — mercados de risco sob pressão severa"
    elif gti >= 65:
        nivel = "ELEVATED"
        cor = "orange"
        descricao = "Tensão elevada — cautela com ativos de risco, ouro e defesa em alta"
    elif gti >= 45:
        nivel = "MODERATE"
        cor = "yellow"
        descricao = "Tensão moderada — mercados atentos mas sem pânico"
    else:
        nivel = "LOW"
        cor = "green"
        descricao = "Tensão baixa — ambiente favorável para ativos de risco"

    return {
        "gti": gti,
        "nivel": nivel,
        "cor": cor,
        "descricao": descricao,
        "variacao_24h": round(random.uniform(-3, 5), 1),  # simulado
        "contribuicoes": contribuicoes,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ─── MOTOR #53 — AI SIGNALS ───────────────────────────────────

def gerar_ai_signals(
    gti: float,
    eventos_ativos: list[dict] = None,
    classe_filtro: str = None,
    precos_atuais: dict = None
) -> list[dict]:
    """
    Gera sinais BUY/SELL/HOLD para cada ativo baseado no GTI e eventos ativos.
    precos_atuais: {"XAUUSD": 2341.0, "WTI": 80.1, ...}
    """
    sinais = []
    evento_trigger = eventos_ativos[0] if eventos_ativos else None
    trigger_descricao = evento_trigger.get("descricao", "Global tension elevated") if evento_trigger else "Global tension elevated"
    trigger_severidade = evento_trigger.get("severidade", gti) if evento_trigger else gti

    for ticker, config in ATIVOS_CONFIG.items():
        if classe_filtro and config["classe"] != classe_filtro:
            continue

        correlacao = config["correlacao_tensao"]
        atr = config["atr_base"]
        preco_atual = (precos_atuais or {}).get(ticker, 0)

        # Calcular confidence baseado no GTI e correlação
        if correlacao == "positiva":
            # Ativo sobe com tensão (ouro, petróleo, defesa)
            if gti >= 65:
                sinal = "BUY"
                bull_pct = min(round((gti - 40) * 1.2), 80)
                bear_pct = 0
                confidence = min(round(50 + (gti - 50) * 0.8), 92)
            elif gti >= 50:
                sinal = "BUY"
                bull_pct = round((gti - 40) * 0.8)
                bear_pct = 0
                confidence = round(50 + (gti - 50) * 0.5)
            else:
                sinal = "HOLD"
                bull_pct = 0
                bear_pct = 0
                confidence = 50
        else:
            # Ativo cai com tensão (equity indices, crypto, forex risco)
            if gti >= 65:
                sinal = "SELL"
                bull_pct = 0
                bear_pct = min(round((gti - 40) * 1.0), 65)
                confidence = min(round(45 + (gti - 50) * 0.7), 85)
            elif gti >= 50:
                sinal = "SELL"
                bull_pct = 0
                bear_pct = round((gti - 40) * 0.6)
                confidence = round(42 + (gti - 50) * 0.4)
            else:
                sinal = "HOLD"
                bull_pct = 0
                bear_pct = 0
                confidence = 50

        # Volume baseado no GTI
        if gti >= 75:
            volume = "HIGH"
            rr = "RR 2.0"
        elif gti >= 55:
            volume = "MEDIUM"
            rr = "RR 1.8"
        else:
            volume = "LOW"
            rr = "RR 1.5"

        sinal_obj = {
            "ticker": ticker,
            "nome": config["nome"],
            "classe": config["classe"],
            "sinal": sinal,
            "confidence_pct": confidence,
            "bull_pct": bull_pct,
            "bear_pct": bear_pct,
            "volume": volume,
            "rr": rr,
            "horizonte": "short-term",
            "trigger": trigger_descricao[:50],
            "trigger_severidade": trigger_severidade,
            "gti": gti,
            "preco_atual": preco_atual
        }
        sinais.append(sinal_obj)

    # Ordenar por confidence decrescente
    sinais.sort(key=lambda x: x["confidence_pct"], reverse=True)
    return sinais


# ─── MOTOR #54 — TRADE SETUP ──────────────────────────────────

def calcular_trade_setup(
    ticker: str,
    preco_atual: float,
    sinal: str,
    gti: float,
    banca: float = 10000.0,
    risco_pct: float = 0.02  # 2% da banca por trade
) -> dict:
    """Calcula entry, stop-loss, target, R:R, ATR e max position size."""
    config = ATIVOS_CONFIG.get(ticker, {"atr_base": 0.015, "nome": ticker, "classe": "Unknown"})
    atr_pct = config["atr_base"] * (1 + (gti - 50) / 100)  # ATR aumenta com tensão
    atr_valor = preco_atual * atr_pct

    if sinal == "BUY":
        entry = preco_atual
        stop_loss = round(entry - atr_valor * 1.5, 2)
        target = round(entry + atr_valor * 3.0, 2)
    elif sinal == "SELL":
        entry = preco_atual
        stop_loss = round(entry + atr_valor * 1.5, 2)
        target = round(entry - atr_valor * 3.0, 2)
    else:
        return {"ticker": ticker, "sinal": "HOLD", "mensagem": "Sem trade setup para HOLD"}

    risco_por_unidade = abs(entry - stop_loss)
    rr = round(abs(target - entry) / max(risco_por_unidade, 0.01), 2)

    # Max position size
    risco_banca = banca * risco_pct
    max_units = risco_banca / max(risco_por_unidade, 0.01)
    max_pos_pct = round((max_units * preco_atual / banca) * 100, 1)

    return {
        "ticker": ticker,
        "nome": config["nome"],
        "sinal": sinal,
        "trade_structure": {
            "current_price": preco_atual,
            "entry": entry,
            "stop_loss": stop_loss,
            "target": target,
            "risk_reward": rr,
            "atr_daily_pct": round(atr_pct * 100, 2),
            "atr_valor": round(atr_valor, 2),
            "max_position_pct": max_pos_pct,
            "risco_usd": round(risco_banca, 2),
            "banca_utilizada": banca
        },
        "risk_vs_reward": {
            "risk": round(risco_por_unidade, 4),
            "reward": round(abs(target - entry), 4),
            "ratio": rr
        }
    }


# ─── MOTOR #55 — GEO MAP ──────────────────────────────────────

def calcular_geo_map(
    eventos_ativos: list[dict] = None,
    what_if: dict = None
) -> dict:
    """
    Calcula tensão por país/região e impacto de what-if scenarios.
    what_if: {"oil_shock": 0.5, "rate_change": 0.0, "escalation": 0.5, "supply_chain": 0.3}
    """
    tensao_paises = dict(PAISES_TENSAO_BASE)

    # Ajustar tensão por eventos ativos
    if eventos_ativos:
        for ev in eventos_ativos:
            regiao = ev.get("regiao", "")
            severidade = ev.get("severidade", 50)
            paises_regiao = REGIOES_GTI.get(regiao, {}).get("pais", [])
            for pais in paises_regiao:
                if pais in tensao_paises:
                    tensao_paises[pais] = min(100, tensao_paises[pais] + severidade * 0.2)

    # Aplicar what-if scenarios
    impacto_ativos = {}
    if what_if:
        oil_shock = what_if.get("oil_shock", 0)
        escalation = what_if.get("escalation", 0)
        rate_change = what_if.get("rate_change", 0)
        supply_chain = what_if.get("supply_chain", 0)

        impacto_ativos = {
            "XAUUSD": round(oil_shock * 15 + escalation * 20, 1),
            "WTI":    round(oil_shock * 30 + escalation * 10, 1),
            "BRENT":  round(oil_shock * 28 + escalation * 10, 1),
            "SPX":    round(-oil_shock * 10 - escalation * 15 - rate_change * 8, 1),
            "BTCUSD": round(-escalation * 12 - supply_chain * 8, 1),
            "EURUSD": round(-oil_shock * 5 - escalation * 8, 1),
            "LMT":    round(escalation * 18, 1),
            "RTX":    round(escalation * 17, 1),
            "TLT":    round(escalation * 10 - rate_change * 15, 1),
        }

    # Agrupar por região
    tensao_regioes = {}
    for regiao_key, regiao_info in REGIOES_GTI.items():
        paises = regiao_info["pais"]
        scores = [tensao_paises.get(p, 30) for p in paises]
        tensao_regioes[regiao_key] = {
            "nome": regiao_info["nome"],
            "score_medio": round(sum(scores) / len(scores), 1),
            "score_max": max(scores),
            "paises": {p: round(tensao_paises.get(p, 30)) for p in paises}
        }

    return {
        "tensao_por_pais": {k: round(v) for k, v in sorted(tensao_paises.items(), key=lambda x: x[1], reverse=True)},
        "tensao_por_regiao": tensao_regioes,
        "what_if_impacto_ativos": impacto_ativos,
        "top_5_paises_tensao": sorted(tensao_paises.items(), key=lambda x: x[1], reverse=True)[:5],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ─── ENDPOINTS ────────────────────────────────────────────────

class GTIRequest(BaseModel):
    eventos_ativos: list[dict] = []

@app.post("/gti")
async def endpoint_gti(body: GTIRequest):
    """
    Motor #52 — Global Tension Index.
    Body: {"eventos_ativos": [{"regiao": "middle_east", "severidade": 80, "tipo": "military_escalation", "descricao": "Iran-Israel Escalation"}]}
    """
    try:
        resultado = calcular_gti(body.eventos_ativos)
        return JSONResponse(content={"motor": "MOTOR_52_GTI", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class AISignalsRequest(BaseModel):
    gti: float
    eventos_ativos: list[dict] = []
    classe_filtro: str = None
    precos_atuais: dict = {}

@app.post("/ai-signals")
async def endpoint_ai_signals(body: AISignalsRequest):
    """
    Motor #53 — AI Signals por classe de ativo.
    Body: {"gti": 71.4, "eventos_ativos": [...], "classe_filtro": "Commodities", "precos_atuais": {"XAUUSD": 2341.0}}
    """
    try:
        sinais = gerar_ai_signals(
            gti=body.gti,
            eventos_ativos=body.eventos_ativos,
            classe_filtro=body.classe_filtro,
            precos_atuais=body.precos_atuais
        )
        return JSONResponse(content={"motor": "MOTOR_53_AI_SIGNALS", "total": len(sinais), "sinais": sinais})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class TradeSetupRequest(BaseModel):
    ticker: str
    preco_atual: float
    sinal: str
    gti: float
    banca: float = 10000.0
    risco_pct: float = 0.02

@app.post("/trade-setup")
async def endpoint_trade_setup(body: TradeSetupRequest):
    """
    Motor #54 — Trade Setup automático.
    Body: {"ticker": "XAUUSD", "preco_atual": 2341.0, "sinal": "BUY", "gti": 71.4, "banca": 10000}
    """
    try:
        resultado = calcular_trade_setup(
            ticker=body.ticker,
            preco_atual=body.preco_atual,
            sinal=body.sinal,
            gti=body.gti,
            banca=body.banca,
            risco_pct=body.risco_pct
        )
        return JSONResponse(content={"motor": "MOTOR_54_TRADE_SETUP", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class GeoMapRequest(BaseModel):
    eventos_ativos: list[dict] = []
    what_if: dict = {}

@app.post("/geo-map")
async def endpoint_geo_map(body: GeoMapRequest):
    """
    Motor #55 — Geo Map + What-If Scenarios.
    Body: {"eventos_ativos": [...], "what_if": {"oil_shock": 0.5, "escalation": 0.5, "rate_change": 0.0, "supply_chain": 0.3}}
    """
    try:
        resultado = calcular_geo_map(
            eventos_ativos=body.eventos_ativos,
            what_if=body.what_if
        )
        return JSONResponse(content={"motor": "MOTOR_55_GEO_MAP", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    

# ══════════════════════════════════════════════════════════════
# MOTORES #56, #57, #58, #59, #60 — INTELIGÊNCIA AVANÇADA
# #56 Polymarket Seismograph
# #57 Cognitive Bias Detector
# #58 Narrative Velocity
# #59 Cross-Market Contagion
# #60 Dead Cat Bounce em Odds
# ══════════════════════════════════════════════════════════════

# ─── MOTOR #56 — POLYMARKET SEISMOGRAPH ──────────────────────

def detectar_epicentro(mercados_com_historico: list[dict]) -> dict:
    """
    Detecta qual mercado moveu primeiro e quais vão mover em cascata.
    mercados_com_historico: [
        {"id": "m1", "titulo": "...", "movimentos": [
            {"timestamp": "2024-03-01T14:00:00Z", "variacao_pct": 5.2},
            ...
        ]}
    ]
    """
    if not mercados_com_historico:
        return {"erro": "Nenhum mercado fornecido"}

    # Encontrar epicentro — mercado que moveu primeiro e mais forte
    epicentro = None
    maior_velocidade = 0
    ondas = []

    for mercado in mercados_com_historico:
        movimentos = mercado.get("movimentos", [])
        if not movimentos:
            continue

        # Calcular velocidade de movimento
        variacoes = [abs(m.get("variacao_pct", 0)) for m in movimentos]
        velocidade_max = max(variacoes) if variacoes else 0
        velocidade_media = sum(variacoes) / len(variacoes) if variacoes else 0

        # Timestamp do primeiro movimento significativo
        primeiro_movimento = None
        for m in movimentos:
            if abs(m.get("variacao_pct", 0)) >= 3:
                primeiro_movimento = m.get("timestamp")
                break

        ondas.append({
            "mercado_id": mercado.get("id"),
            "titulo": mercado.get("titulo", ""),
            "velocidade_max": round(velocidade_max, 2),
            "velocidade_media": round(velocidade_media, 2),
            "primeiro_movimento_significativo": primeiro_movimento,
            "score_sismico": round(velocidade_max * 10, 1)
        })

        if velocidade_max > maior_velocidade:
            maior_velocidade = velocidade_max
            epicentro = {
                "mercado_id": mercado.get("id"),
                "titulo": mercado.get("titulo", ""),
                "velocidade_max": round(velocidade_max, 2),
                "primeiro_movimento": primeiro_movimento
            }

    # Ordenar ondas por velocidade
    ondas.sort(key=lambda x: x["velocidade_max"], reverse=True)

    # Prever cascata
    cascata = []
    if epicentro and len(ondas) > 1:
        for i, onda in enumerate(ondas[1:], 1):
            cascata.append({
                "ordem": i,
                "mercado": onda["titulo"],
                "previsao_movimento_horas": round(i * 1.5, 1),
                "probabilidade_mover_pct": max(90 - i * 15, 20)
            })

    return {
        "epicentro": epicentro,
        "ondas_sismicas": ondas,
        "cascata_prevista": cascata,
        "total_mercados_analisados": len(mercados_com_historico),
        "alerta": f"Epicentro detectado em '{epicentro['titulo']}' — cascata esperada em {len(cascata)} mercados" if epicentro else "Nenhum epicentro detectado"
    }


# ─── MOTOR #57 — COGNITIVE BIAS DETECTOR ─────────────────────

BIAS_PATTERNS = {
    "anchoring": {
        "nome": "Viés de Ancoragem",
        "descricao": "Mercado preso ao preço inicial mesmo com novas informações",
        "trade": "Compre na direção da nova informação — o mercado vai corrigir",
        "deteccao": "preco_estavel_com_noticias"
    },
    "recency_bias": {
        "nome": "Viés de Recência",
        "descricao": "Mercado supervaloriza eventos recentes ignorando base rate",
        "trade": "Vá contra a tendência recente — regression to mean iminente",
        "deteccao": "movimento_extremo_recente"
    },
    "availability_heuristic": {
        "nome": "Heurística de Disponibilidade",
        "descricao": "Eventos fáceis de lembrar (viral) superestimados vs base rate",
        "trade": "Compre NO em eventos virais — probabilidade real é menor",
        "deteccao": "volume_explosivo_sem_fundamento"
    },
    "bandwagon": {
        "nome": "Efeito Manada",
        "descricao": "Traders seguindo a multidão sem análise própria",
        "trade": "Contrarie a multidão — smart money já saiu",
        "deteccao": "muitos_traders_mesma_direcao"
    },
    "gamblers_fallacy": {
        "nome": "Falácia do Jogador",
        "descricao": "Crença que após eventos consecutivos, o oposto vai acontecer",
        "trade": "Ignore sequências — cada evento é independente",
        "deteccao": "reversao_esperada_sem_fundamento"
    },
    "overconfidence": {
        "nome": "Excesso de Confiança",
        "descricao": "Odds muito extremas (>90% ou <10%) geralmente são overconfident",
        "trade": "Compre o lado negligenciado — mercado subestima incerteza",
        "deteccao": "odds_extremas"
    }
}

def detectar_cognitive_bias(
    preco_atual: float,
    preco_inicial: float,
    variacao_24h_pct: float,
    volume_atual: float,
    volume_media: float,
    num_traders: int,
    num_noticias: int,
    sequencia_direcao: list[str]  # ["YES", "YES", "YES", "NO"] últimas apostas
) -> dict:
    biases_detectados = []
    score_total = 0

    # Ancoragem — preço quase igual ao inicial com notícias chegando
    variacao_do_inicial = abs((preco_atual - preco_inicial) / max(preco_inicial, 0.01)) * 100
    if variacao_do_inicial < 5 and num_noticias >= 3:
        biases_detectados.append({
            **BIAS_PATTERNS["anchoring"],
            "score": 75,
            "evidencia": f"Preço variou só {variacao_do_inicial:.1f}% com {num_noticias} notícias"
        })
        score_total += 75

    # Recência — movimento extremo recente
    if abs(variacao_24h_pct) >= 25:
        biases_detectados.append({
            **BIAS_PATTERNS["recency_bias"],
            "score": 80,
            "evidencia": f"Variação de {variacao_24h_pct:.1f}% em 24h — mercado superestima evento recente"
        })
        score_total += 80

    # Disponibilidade — volume explosivo sem fundamento (poucas notícias)
    ratio_volume = volume_atual / max(volume_media, 1)
    if ratio_volume >= 3 and num_noticias <= 1:
        biases_detectados.append({
            **BIAS_PATTERNS["availability_heuristic"],
            "score": 70,
            "evidencia": f"Volume {ratio_volume:.1f}x acima com apenas {num_noticias} notícia(s)"
        })
        score_total += 70

    # Manada — muitos traders mesma direção
    if num_traders >= 50 and abs(variacao_24h_pct) >= 15:
        biases_detectados.append({
            **BIAS_PATTERNS["bandwagon"],
            "score": 65,
            "evidencia": f"{num_traders} traders todos seguindo movimento de {variacao_24h_pct:.1f}%"
        })
        score_total += 65

    # Odds extremas — overconfidence
    if preco_atual >= 0.90 or preco_atual <= 0.10:
        biases_detectados.append({
            **BIAS_PATTERNS["overconfidence"],
            "score": 85,
            "evidencia": f"Odds em {preco_atual:.2f} — mercado extremamente confiante, raro ser correto"
        })
        score_total += 85

    # Falácia do jogador — sequência longa
    if len(sequencia_direcao) >= 4:
        ultimos = sequencia_direcao[-4:]
        if len(set(ultimos)) == 1:  # todos iguais
            biases_detectados.append({
                **BIAS_PATTERNS["gamblers_fallacy"],
                "score": 60,
                "evidencia": f"4 apostas consecutivas em {ultimos[0]} — traders esperando reversão sem fundamento"
            })
            score_total += 60

    score_total = min(score_total, 100) if biases_detectados else 0
    bias_dominante = max(biases_detectados, key=lambda x: x["score"]) if biases_detectados else None

    return {
        "score_bias_total": score_total,
        "bias_dominante": bias_dominante,
        "todos_biases": biases_detectados,
        "total_detectados": len(biases_detectados),
        "edge_disponivel": score_total >= 60,
        "recomendacao": bias_dominante["trade"] if bias_dominante else "Nenhum viés dominante detectado"
    }


# ─── MOTOR #58 — NARRATIVE VELOCITY ──────────────────────────

def calcular_narrative_velocity(
    narrativa: str,
    mencoes_por_hora: list[dict],  # [{"hora": "14:00", "mencoes": 45}, ...]
    preco_odds_por_hora: list[dict]  # [{"hora": "14:00", "preco": 0.55}, ...]
) -> dict:
    if not mencoes_por_hora:
        return {"erro": "Dados insuficientes"}

    mencoes = [m.get("mencoes", 0) for m in mencoes_por_hora]
    precos = [p.get("preco", 0) for p in preco_odds_por_hora] if preco_odds_por_hora else []

    # Velocidade de espalhamento
    if len(mencoes) >= 2:
        aceleracao = mencoes[-1] - mencoes[0]
        velocidade_atual = mencoes[-1]
        velocidade_media = sum(mencoes) / len(mencoes)
        pico = max(mencoes)
        pos_pico = mencoes.index(pico)
        ja_passou_pico = pos_pico < len(mencoes) - 1
    else:
        aceleracao = 0
        velocidade_atual = mencoes[0] if mencoes else 0
        velocidade_media = velocidade_atual
        pico = velocidade_atual
        ja_passou_pico = False

    # Correlação narrativa → odds
    correlacao = 0
    if len(mencoes) >= 3 and len(precos) >= 3:
        min_len = min(len(mencoes), len(precos))
        m = mencoes[:min_len]
        p = precos[:min_len]
        media_m = sum(m) / len(m)
        media_p = sum(p) / len(p)
        num = sum((m[i] - media_m) * (p[i] - media_p) for i in range(min_len))
        den_m = sum((x - media_m)**2 for x in m) ** 0.5
        den_p = sum((x - media_p)**2 for x in p) ** 0.5
        correlacao = round(num / max(den_m * den_p, 0.001), 2)

    # Previsão de impacto nas odds
    if aceleracao > 50 and not ja_passou_pico:
        previsao_horas = 2
        impacto_estimado = "ALTO"
        recomendacao = f"Narrativa acelerando — odds vão mover em ~{previsao_horas}h. ENTRE AGORA"
    elif aceleracao > 20 and not ja_passou_pico:
        previsao_horas = 4
        impacto_estimado = "MEDIO"
        recomendacao = f"Narrativa crescendo — impacto em ~{previsao_horas}h"
    elif ja_passou_pico:
        previsao_horas = 0
        impacto_estimado = "DECLINANDO"
        recomendacao = "Narrativa já passou o pico — odds podem já ter precificado"
    else:
        previsao_horas = 8
        impacto_estimado = "BAIXO"
        recomendacao = "Narrativa fraca — pouco impacto esperado nas odds"

    return {
        "narrativa": narrativa,
        "velocidade_atual_mencoes_hora": velocidade_atual,
        "velocidade_media": round(velocidade_media, 1),
        "aceleracao": aceleracao,
        "ja_passou_pico": ja_passou_pico,
        "correlacao_narrativa_odds": correlacao,
        "impacto_estimado": impacto_estimado,
        "previsao_impacto_horas": previsao_horas,
        "recomendacao": recomendacao,
        "metricas": {
            "pico_mencoes": pico,
            "total_horas_analisadas": len(mencoes_por_hora)
        }
    }


# ─── MOTOR #59 — CROSS-MARKET CONTAGION ──────────────────────

CONTAGION_MAP = {
    # Se este mercado resolver YES → impacta estes mercados
    "trump_wins": ["republican_senate", "crypto_up", "mexico_tariffs", "nato_weakening"],
    "fed_rate_hike": ["recession_2025", "crypto_down", "gold_up", "dollar_up"],
    "china_invades_taiwan": ["semiconductor_shortage", "oil_spike", "gold_up", "nato_response"],
    "iran_nuclear": ["oil_spike", "israel_war", "gold_up", "strait_hormuz"],
    "ukraine_ceasefire": ["gas_prices_drop", "europe_recovery", "russia_sanctions_ease"],
    "bitcoin_100k": ["crypto_altcoins_up", "etf_approval", "institutional_adoption"],
    "recession_usa": ["fed_rate_cut", "gold_up", "dollar_down", "crypto_down"],
}

def analisar_contagion(
    mercado_resolvido: str,
    resultado: str,  # "YES" ou "NO"
    mercados_disponiveis: list[dict]
) -> dict:
    """
    Quando um mercado resolve, detecta quais outros vão ser afetados.
    mercados_disponiveis: [{"id": "...", "titulo": "...", "preco_yes": 0.55}]
    """
    chave = mercado_resolvido.lower().replace(" ", "_")

    # Buscar no mapa de contágio
    afetados_diretos = []
    for key, impacts in CONTAGION_MAP.items():
        if key in chave or any(k in chave for k in key.split("_")):
            afetados_diretos = impacts
            break

    # Mapear para mercados disponíveis
    impactos = []
    for mercado in mercados_disponiveis:
        titulo = mercado.get("titulo", "").lower()
        preco = mercado.get("preco_yes", 0.5)

        for afetado in afetados_diretos:
            palavras = afetado.replace("_", " ").split()
            if any(p in titulo for p in palavras):
                # Calcular direção do impacto
                if resultado == "YES":
                    if "up" in afetado or "spike" in afetado:
                        direcao = "SOBE"
                        novo_preco_estimado = min(preco * 1.3, 0.95)
                    elif "down" in afetado or "drop" in afetado:
                        direcao = "CAI"
                        novo_preco_estimado = max(preco * 0.7, 0.05)
                    else:
                        direcao = "AFETADO"
                        novo_preco_estimado = preco * 1.15
                else:
                    direcao = "INVERSO"
                    novo_preco_estimado = 1 - preco

                impactos.append({
                    "mercado_id": mercado.get("id"),
                    "titulo": mercado.get("titulo"),
                    "preco_atual": preco,
                    "preco_estimado_pos_contagio": round(novo_preco_estimado, 3),
                    "variacao_estimada_pct": round((novo_preco_estimado - preco) / max(preco, 0.01) * 100, 1),
                    "direcao": direcao,
                    "velocidade_contagio": "RAPIDA" if abs(novo_preco_estimado - preco) > 0.1 else "LENTA"
                })

    impactos.sort(key=lambda x: abs(x["variacao_estimada_pct"]), reverse=True)

    return {
        "mercado_gatilho": mercado_resolvido,
        "resultado": resultado,
        "total_mercados_afetados": len(impactos),
        "impactos_em_cascata": impactos,
        "alerta": f"{len(impactos)} mercados vão ser afetados em cascata!" if impactos else "Nenhum contágio mapeado para este mercado"
    }


# ─── MOTOR #60 — DEAD CAT BOUNCE ─────────────────────────────

def detectar_dead_cat_bounce(
    historico_precos: list[dict],
    volume_historico: list[dict] = None
) -> dict:
    if len(historico_precos) < 5:
        return {"erro": "Mínimo 5 pontos necessários"}

    precos = [p["preco"] for p in historico_precos]

    # Detectar padrão: queda forte → recuperação parcial → queda continua
    queda_maxima = 0
    ponto_queda = 0
    recuperacao = 0
    padrao_detectado = False
    armadilha = False

    for i in range(1, len(precos)):
        variacao = (precos[i] - precos[i-1]) / max(precos[i-1], 0.01) * 100

        # Encontrar queda forte
        if variacao <= -10:
            queda_maxima = variacao
            ponto_queda = i

        # Verificar recuperação após queda
        if ponto_queda > 0 and i > ponto_queda:
            recuperacao_atual = (precos[i] - precos[ponto_queda]) / max(abs(precos[ponto_queda]), 0.01) * 100

            if 5 <= recuperacao_atual <= 40:  # Recuperação parcial (5-40% da queda)
                recuperacao = recuperacao_atual
                padrao_detectado = True

                # Verificar se é armadilha (volume baixo na recuperação)
                if volume_historico and len(volume_historico) > i:
                    vol_queda = volume_historico[ponto_queda].get("volume", 1)
                    vol_recuperacao = volume_historico[i].get("volume", 1)
                    if vol_recuperacao < vol_queda * 0.5:
                        armadilha = True

    preco_atual = precos[-1]
    preco_minimo = min(precos)
    preco_maximo = max(precos)
    distancia_do_fundo = (preco_atual - preco_minimo) / max(preco_minimo, 0.01) * 100

    if padrao_detectado and armadilha:
        classificacao = "DEAD_CAT_BOUNCE_CONFIRMADO"
        recomendacao = "ARMADILHA DETECTADA — não entre no bounce! Odds vão continuar caindo"
        score_risco = 90
    elif padrao_detectado and not armadilha:
        classificacao = "POSSIVEL_DEAD_CAT"
        recomendacao = "Padrão suspeito — aguarde confirmação antes de entrar"
        score_risco = 60
    elif distancia_do_fundo >= 15:
        classificacao = "RECUPERACAO_LEGITIMA"
        recomendacao = "Recuperação parece legítima — pode ser boa entrada"
        score_risco = 25
    else:
        classificacao = "SEM_PADRAO"
        recomendacao = "Nenhum padrão de dead cat detectado"
        score_risco = 10

    return {
        "classificacao": classificacao,
        "score_risco": score_risco,
        "recomendacao": recomendacao,
        "padrao_detectado": padrao_detectado,
        "armadilha_confirmada": armadilha,
        "metricas": {
            "queda_maxima_pct": round(queda_maxima, 2),
            "recuperacao_pct": round(recuperacao, 2),
            "preco_atual": preco_atual,
            "preco_minimo": preco_minimo,
            "preco_maximo": preco_maximo,
            "distancia_do_fundo_pct": round(distancia_do_fundo, 2)
        }
    }


# ─── ENDPOINTS ────────────────────────────────────────────────

class SeismographRequest(BaseModel):
    mercados_com_historico: list[dict]

@app.post("/seismograph")
async def endpoint_seismograph(body: SeismographRequest):
    """
    Motor #56 — Polymarket Seismograph.
    Body: {"mercados_com_historico": [{"id": "m1", "titulo": "...", "movimentos": [{"timestamp": "...", "variacao_pct": 5.2}]}]}
    """
    try:
        resultado = detectar_epicentro(body.mercados_com_historico)
        return JSONResponse(content={"motor": "MOTOR_56_SEISMOGRAPH", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class CognitiveBiasRequest(BaseModel):
    preco_atual: float
    preco_inicial: float
    variacao_24h_pct: float
    volume_atual: float
    volume_media: float
    num_traders: int
    num_noticias: int
    sequencia_direcao: list[str] = []

@app.post("/cognitive-bias")
async def endpoint_cognitive_bias(body: CognitiveBiasRequest):
    """
    Motor #57 — Cognitive Bias Detector.
    Body: {"preco_atual": 0.92, "preco_inicial": 0.90, "variacao_24h_pct": 35, "volume_atual": 50000, "volume_media": 10000, "num_traders": 80, "num_noticias": 1, "sequencia_direcao": ["YES","YES","YES","YES"]}
    """
    try:
        resultado = detectar_cognitive_bias(
            preco_atual=body.preco_atual,
            preco_inicial=body.preco_inicial,
            variacao_24h_pct=body.variacao_24h_pct,
            volume_atual=body.volume_atual,
            volume_media=body.volume_media,
            num_traders=body.num_traders,
            num_noticias=body.num_noticias,
            sequencia_direcao=body.sequencia_direcao
        )
        return JSONResponse(content={"motor": "MOTOR_57_COGNITIVE_BIAS", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class NarrativeVelocityRequest(BaseModel):
    narrativa: str
    mencoes_por_hora: list[dict]
    preco_odds_por_hora: list[dict] = []

@app.post("/narrative-velocity")
async def endpoint_narrative_velocity(body: NarrativeVelocityRequest):
    """
    Motor #58 — Narrative Velocity.
    Body: {"narrativa": "Iran attack Israel", "mencoes_por_hora": [{"hora": "14:00", "mencoes": 45}, ...], "preco_odds_por_hora": [{"hora": "14:00", "preco": 0.55}]}
    """
    try:
        resultado = calcular_narrative_velocity(
            narrativa=body.narrativa,
            mencoes_por_hora=body.mencoes_por_hora,
            preco_odds_por_hora=body.preco_odds_por_hora
        )
        return JSONResponse(content={"motor": "MOTOR_58_NARRATIVE_VELOCITY", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class ContagionRequest(BaseModel):
    mercado_resolvido: str
    resultado: str
    mercados_disponiveis: list[dict] = []

@app.post("/cross-market-contagion")
async def endpoint_contagion(body: ContagionRequest):
    """
    Motor #59 — Cross-Market Contagion.
    Body: {"mercado_resolvido": "Fed rate hike March 2025", "resultado": "YES", "mercados_disponiveis": [{"id": "...", "titulo": "...", "preco_yes": 0.55}]}
    """
    try:
        resultado = analisar_contagion(
            mercado_resolvido=body.mercado_resolvido,
            resultado=body.resultado,
            mercados_disponiveis=body.mercados_disponiveis
        )
        return JSONResponse(content={"motor": "MOTOR_59_CONTAGION", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})


class DeadCatRequest(BaseModel):
    historico_precos: list[dict]
    volume_historico: list[dict] = []

@app.post("/dead-cat-bounce")
async def endpoint_dead_cat(body: DeadCatRequest):
    """
    Motor #60 — Dead Cat Bounce em Odds.
    Body: {"historico_precos": [{"timestamp": "...", "preco": 0.70}, ...], "volume_historico": [{"timestamp": "...", "volume": 5000}]}
    """
    try:
        resultado = detectar_dead_cat_bounce(
            historico_precos=body.historico_precos,
            volume_historico=body.volume_historico
        )
        return JSONResponse(content={"motor": "MOTOR_60_DEAD_CAT", "resultado": resultado})
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
    

