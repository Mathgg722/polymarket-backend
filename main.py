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
    Retorna (passou: bool, motivo: str)
    """
    now = datetime.utcnow()

    # Filtro 1: preço fora da zona de interesse
    if token_price < FILTER_MIN_PRICE:
        return False, f"preço {round(token_price*100,1)}% abaixo do mínimo de {FILTER_MIN_PRICE*100}%"
    if token_price > FILTER_MAX_PRICE:
        return False, f"preço {round(token_price*100,1)}% acima do máximo de {FILTER_MAX_PRICE*100}%"

    # Filtro 2: volume insuficiente
    if market_volume < FILTER_MIN_VOLUME:
        return False, f"volume ${market_volume:.0f} abaixo do mínimo de ${FILTER_MIN_VOLUME}"

    # Filtro 3: dados insuficientes
    if snap_count < FILTER_MIN_SNAPS:
        return False, f"apenas {snap_count} snapshots, mínimo é {FILTER_MIN_SNAPS}"

    # Filtro 4: mercado expirado
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

            # Volume — salva agora
            volume = 0.0
            try:
                volume = float(m.get("volume", 0) or m.get("volumeNum", 0) or 0)
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
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}

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
        if volume < FILTER_MIN_VOLUME:
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
    min_edge: float = Query(10.0),
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

        # Verifica volume
        volume = _get_market_volume(market)
        if volume < FILTER_MIN_VOLUME:
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
            if volume < FILTER_MIN_VOLUME:
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
        if volume < FILTER_MIN_VOLUME:
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
            if volume < FILTER_MIN_VOLUME:
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
            if volume < FILTER_MIN_VOLUME:
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
            if volume < FILTER_MIN_VOLUME:
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
        if volume < FILTER_MIN_VOLUME:
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