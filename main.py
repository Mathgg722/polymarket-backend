from fastapi import FastAPI
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot

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
def status():
    db: Session = next(get_db())
    count = db.query(Market).count()
    return {"stored_markets": count}


@app.get("/markets")
def markets():
    db: Session = next(get_db())
    markets = db.query(Market).all()

    result = []
    for m in markets:
        result.append({
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