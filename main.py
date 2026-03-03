from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import SessionLocal, engine
from models import Base, Market, Token
from datetime import datetime

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
    return {"status": "ok"}


@app.get("/status")
def status(db: Session = Depends(get_db)):
    total = db.query(Market).count()
    total_with_tokens = db.query(Market).join(Token).distinct().count()
    return {
        "total_markets_in_database": total,
        "markets_with_tokens": total_with_tokens,
        "last_check": datetime.utcnow().isoformat()
    }


@app.get("/markets")
def get_active_markets(db: Session = Depends(get_db)):
    # Converte end_date de String para DateTime (roda só uma vez)
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

    # Só mercados com tokens E com end_date futuro (ou null mas recentes)
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
        tokens = m.tokens
        yes_price = None
        no_price = None
        for t in tokens:
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
            "tokens": [
                {"outcome": t.outcome, "price": round(t.price * 100, 1)}
                for t in tokens
            ]
        })

    return result


@app.get("/history/{token_id}")
def get_history(token_id: str, db: Session = Depends(get_db)):
    from models import Snapshot
    snapshots = (
        db.query(Snapshot)
        .filter(Snapshot.token_id == token_id)
        .order_by(Snapshot.timestamp.desc())
        .limit(100)
        .all()
    )
    return [
        {"price": round(s.price * 100, 1), "timestamp": str(s.timestamp)}
        for s in snapshots
    ]