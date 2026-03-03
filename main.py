from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Base, Market
from typing import List

Base.metadata.create_all(bind=engine)

app = FastAPI()


# Dependency correta
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
    count = db.query(Market).count()
    return {"stored_markets": count}


@app.get("/markets")
def get_markets(db: Session = Depends(get_db)):
    markets = db.query(Market).all()

    result = []

    for m in markets:
        result.append({
            "id": m.id,
            "question": m.question,
            "slug": m.market_slug,
            "end_date": m.end_date,
            "volume": m.volume,
            "liquidity": m.liquidity,
            "tokens": [
                {
                    "outcome": t.outcome,
                    "price": t.price
                } for t in m.tokens
            ]
        })

    return result