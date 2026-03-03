import requests
import time
from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot

Base.metadata.create_all(bind=engine)

BASE_URL = "https://gamma-api.polymarket.com/markets"


def fetch_markets():
    try:
        response = requests.get(f"{BASE_URL}?limit=200")
        response.raise_for_status()
        data = response.json()

        # Algumas vezes a API retorna dict com "markets"
        if isinstance(data, dict):
            return data.get("markets", [])
        
        return data

    except Exception as e:
        print("Erro ao buscar mercados:", e)
        return []


def run():
    print("🚀 WORKER INICIOU")

    while True:
        print("🔄 Atualizando mercados...")

        db = SessionLocal()

        markets = fetch_markets()
        print("📊 Quantidade recebida:", len(markets))

        if len(markets) == 0:
            print("⚠️ Nenhum mercado recebido da API")

        for m in markets:
            try:
                slug = m.get("slug")
                question = m.get("question")
                end_date = m.get("endDate")

                if not slug:
                    continue

                existing = db.query(Market).filter_by(market_slug=slug).first()

                if not existing:
                    existing = Market(
                        market_slug=slug,
                        question=question,
                        end_date=end_date
                    )
                    db.add(existing)
                    db.flush()  # pega ID sem commit ainda

                tokens = m.get("tokens", [])

                for token in tokens:
                    token_id = token.get("tokenId")
                    outcome = token.get("outcome")
                    price = token.get("price")

                    if not token_id:
                        continue

                    token_obj = db.query(Token).filter_by(token_id=token_id).first()

                    if not token_obj:
                        token_obj = Token(
                            token_id=token_id,
                            outcome=outcome,
                            price=float(price or 0),
                            market_id=existing.id
                        )
                        db.add(token_obj)
                    else:
                        token_obj.price = float(price or 0)

                    snapshot = Snapshot(
                        token_id=token_id,
                        price=float(price or 0)
                    )
                    db.add(snapshot)

            except Exception as e:
                print("Erro ao processar mercado:", e)

        db.commit()
        db.close()

        print("✅ Atualização concluída.\n")

        time.sleep(60)


if __name__ == "__main__":
    run()