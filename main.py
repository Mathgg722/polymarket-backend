from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
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
    # Converte end_date de String para DateTime direto no PostgreSQL
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
        db.rollback()  # Coluna ja foi convertida, tudo certo

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
            "end_date": str(m.end_date) if m.end_date else None,
            "tokens": [
                {
                    "outcome": t.outcome,
                    "price": t.price
                } for t in m.tokens
            ]
        })

    return result