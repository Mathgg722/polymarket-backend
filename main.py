from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot
from datetime import datetime, timedelta

Base.metadata.create_all(bind=engine)

app = FastAPI()


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
        "outcome": trade.outcome,
        "entry_price": trade.entry_price,
        "exit_price": exit_price,
        "pnl_usd": pnl,
        "status": trade.status
    }


# ─────────────────────────────────────────
# SISTEMA DE LÍDERES / TOP TRADERS
# ─────────────────────────────────────────

@app.get("/leaders")
def get_leaders(db: Session = Depends(get_db)):
    """
    Busca top traders do Polymarket via API pública.
    Mostra quem está lucrando mais e em quais mercados estão apostando.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }

    leaders = []

    # Tenta buscar leaderboard por diferentes janelas de tempo
    windows = [
        ("all", "Todo período"),
        ("1mo", "Último mês"),
        ("1w", "Última semana"),
    ]

    for window, label in windows:
        try:
            url = f"https://data-api.polymarket.com/leaderboard?window={window}&limit=20"
            resp = requests.get(url, headers=headers, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                items = data if isinstance(data, list) else data.get("data", [])
                if items:
                    for t in items[:20]:
                        address = t.get("proxyWallet") or t.get("address") or ""
                        leaders.append({
                            "rank": len(leaders) + 1,
                            "username": t.get("name") or t.get("pseudonym") or address[:10] + "...",
                            "address": address,
                            "profit_usd": t.get("profit") or t.get("pnl") or 0,
                            "volume_usd": t.get("volume") or 0,
                            "positions_won": t.get("positionsWon") or t.get("marketsWon") or 0,
                            "positions_lost": t.get("positionsLost") or t.get("marketsLost") or 0,
                            "win_rate": round(
                                (t.get("positionsWon") or 0) /
                                max((t.get("positionsWon") or 0) + (t.get("positionsLost") or 0), 1) * 100, 1
                            ),
                            "window": label,
                            "polymarket_url": f"https://polymarket.com/profile/{address}" if address else None,
                        })
                    break  # Se achou dados para alguma janela, para
        except:
            continue

    # Se API oficial não retornar, busca via gamma API profiles
    if not leaders:
        try:
            url = "https://gamma-api.polymarket.com/profiles?limit=20&sortBy=profit&sortOrder=DESC"
            resp = requests.get(url, headers=headers, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                items = data if isinstance(data, list) else data.get("data", [])
                for i, t in enumerate(items[:20]):
                    address = t.get("proxyWallet") or t.get("address") or ""
                    leaders.append({
                        "rank": i + 1,
                        "username": t.get("name") or t.get("pseudonym") or address[:10] + "...",
                        "address": address,
                        "profit_usd": t.get("profit") or t.get("pnl") or 0,
                        "volume_usd": t.get("volume") or 0,
                        "win_rate": None,
                        "window": "Todo período",
                        "polymarket_url": f"https://polymarket.com/profile/{address}" if address else None,
                    })
        except:
            pass

    if not leaders:
        return {
            "status": "unavailable",
            "message": "API de traders não pública. Acesse manualmente:",
            "links": {
                "leaderboard_oficial": "https://polymarket.com/leaderboard",
                "polymarket_whales": "https://polymarketwhales.info",
                "grandes_apostas": "https://polymarket.com/activity"
            }
        }

    return {
        "status": "ok",
        "total": len(leaders),
        "leaders": leaders,
        "sources": {
            "leaderboard_oficial": "https://polymarket.com/leaderboard",
            "polymarket_whales": "https://polymarketwhales.info"
        }
    }


@app.get("/leaders/{address}/positions")
def get_leader_positions(address: str):
    """
    Busca posições abertas de um trader específico pelo endereço da carteira.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }

    positions = []

    try:
        # Busca posições via data-api
        url = f"https://data-api.polymarket.com/positions?user={address}&sizeThreshold=10&limit=50"
        resp = requests.get(url, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", [])
            for p in items:
                positions.append({
                    "market": p.get("title") or p.get("market") or p.get("question"),
                    "outcome": p.get("outcome"),
                    "size": p.get("size") or p.get("currentValue"),
                    "avg_price": p.get("avgPrice") or p.get("curPrice"),
                    "pnl": p.get("cashPnl") or p.get("pnl"),
                    "market_slug": p.get("conditionId") or p.get("marketSlug"),
                })
    except:
        pass

    if not positions:
        return {
            "status": "unavailable",
            "message": f"Veja posições deste trader em:",
            "url": f"https://polymarket.com/profile/{address}"
        }

    return {
        "address": address,
        "total_positions": len(positions),
        "positions": positions,
        "polymarket_url": f"https://polymarket.com/profile/{address}"
    }