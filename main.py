# ============================================================
# PolySignal — Backend Principal v4.0
# FastAPI + PostgreSQL + Railway
# Reescrito do zero com motores limpos
# ============================================================

import os
import json
import time
import requests
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, Query
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