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