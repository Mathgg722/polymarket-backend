import requests
import time
from datetime import datetime
from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot

Base.metadata.create_all(bind=engine)

BASE_URL = "https://gamma-api.polymarket.com/markets"


def parse_end_date(end_date_str):
    """Converte string de data para objeto datetime."""
    if not end_date_str:
        return None
    try:
        # Formato: "2026-03-31T00:00:00Z"
        return datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        try:
            return datetime.strptime(end_date_str[:10], "%Y-%m-%d")
        except Exception:
            return None


def fetch_markets():
    try:
        response = requests.get(f"{BASE_URL}?limit=200")
        response.raise_for_status()
        data = response.json()

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
                end_date = parse_end_date(m.get("endDate"))  # CORRIGIDO: converte para datetime

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
                    db.flush()
                else:
                    # Atualiza end_date se mudou
                    existing.end_date = end_date

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