import requests
import time
from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot

Base.metadata.create_all(bind=engine)

BASE_URL = "https://gamma-api.polymarket.com/markets"


def fetch_markets():
    response = requests.get(f"{BASE_URL}?active=true&limit=200")
    return response.json()


def run():
    while True:
        print("Atualizando mercados...")
        db = SessionLocal()

        markets = fetch_markets()

        for m in markets:
            existing = db.query(Market).filter_by(market_slug=m["slug"]).first()

            if not existing:
                existing = Market(
                    market_slug=m["slug"],
                    question=m["question"],
                    end_date=m.get("endDate")
                )
                db.add(existing)
                db.commit()
                db.refresh(existing)

            for token in m.get("tokens", []):
                token_obj = db.query(Token).filter_by(
                    token_id=token["tokenId"]
                ).first()

                if not token_obj:
                    token_obj = Token(
                        token_id=token["tokenId"],
                        outcome=token["outcome"],
                        price=float(token["price"]),
                        market_id=existing.id
                    )
                    db.add(token_obj)
                else:
                    token_obj.price = float(token["price"])

                snapshot = Snapshot(
                    token_id=token["tokenId"],
                    price=float(token["price"])
                )
                db.add(snapshot)

        db.commit()
        db.close()

        print("Atualização concluída.")
        time.sleep(60)


if __name__ == "__main__":
    run()