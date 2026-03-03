from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Base, Market
from datetime import datetime

# cria tabelas
Base.metadata.create_all(bind=engine)

app = FastAPI()


# conexão com banco
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
    return {
        "total_markets_in_database": total
    }


@app.get("/markets")
def get_active_markets(db: Session = Depends(get_db)):
    now = datetime.utcnow()

    markets = (
        db.query(Market)
        .filter(
            (Market.end_date == None) |
            (Market.end_date > now)
        )
        .all()
    )

    result = []

    for m in markets:
        result.append({
            "id": m.id,
            "question": m.question,
            "slug": m.market_slug,
            "end_date": m.end_date,
            "tokens": [
                {
                    "outcome": t.outcome,
                    "price": t.price
                } for t in m.tokens
            ]
        })

    return result