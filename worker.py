import requests
import time
import json
from datetime import datetime
from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot

Base.metadata.create_all(bind=engine)

BASE_URL = "https://gamma-api.polymarket.com/markets"


def parse_end_date(end_date_str):
    if not end_date_str:
        return None
    try:
        return datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        try:
            return datetime.strptime(end_date_str[:10], "%Y-%m-%d")
        except Exception:
            return None


def fetch_markets():
    try:
        response = requests.get(f"{BASE_URL}?limit=200&active=true&order=volume24hr&ascending=false")
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return data.get("markets", [])
        return data
    except Exception as e:
        print("Erro ao buscar mercados:", e)
        return []


def extract_prices(market):
    """Extrai precos YES/NO de qualquer formato da API."""
    yes_price = None
    no_price = None

    # Formato 1: campo tokens []
    tokens = market.get("tokens", [])
    if tokens:
        for t in tokens:
            outcome = (t.get("outcome") or "").upper()
            price = t.get("price")
            if outcome == "YES" and price is not None:
                yes_price = float(price)
            elif outcome == "NO" and price is not None:
                no_price = float(price)
        if yes_price is not None:
            return tokens, yes_price, no_price

    # Formato 2: outcomePrices como string JSON
    outcome_prices = market.get("outcomePrices")
    if outcome_prices:
        try:
            prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
            if len(prices) >= 2:
                yes_price = float(prices[0])
                no_price = float(prices[1])
                # Cria tokens sinteticos
                clob_ids = market.get("clobTokenIds", "[]")
                ids = json.loads(clob_ids) if isinstance(clob_ids, str) else clob_ids
                synth_tokens = [
                    {"tokenId": ids[0] if ids else f"{market.get('id')}_YES", "outcome": "YES", "price": yes_price},
                    {"tokenId": ids[1] if len(ids) > 1 else f"{market.get('id')}_NO", "outcome": "NO", "price": no_price},
                ]
                return synth_tokens, yes_price, no_price
        except:
            pass

    # Formato 3: lastTradePrice
    last_trade = market.get("lastTradePrice")
    if last_trade:
        yes_price = float(last_trade)
        no_price = round(1 - yes_price, 4)
        synth_tokens = [
            {"tokenId": f"{market.get('id')}_YES", "outcome": "YES", "price": yes_price},
            {"tokenId": f"{market.get('id')}_NO", "outcome": "NO", "price": no_price},
        ]
        return synth_tokens, yes_price, no_price

    return [], None, None


def run():
    print("🚀 WORKER INICIOU")
    first_run = True

    while True:
        print("🔄 Atualizando mercados...")

        db = SessionLocal()
        markets = fetch_markets()
        print("📊 Quantidade recebida:", len(markets))

        # Debug: mostra campos do primeiro mercado na primeira execucao
        if first_run and markets:
            print("🔍 CAMPOS DO PRIMEIRO MERCADO:", list(markets[0].keys()))
            print("🔍 EXEMPLO:", json.dumps(markets[0], indent=2)[:500])
            first_run = False

        saved_tokens = 0

        for m in markets:
            try:
                slug = m.get("slug")
                question = m.get("question")
                end_date = parse_end_date(m.get("endDate"))

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
                    existing.end_date = end_date

                tokens, yes_price, no_price = extract_prices(m)

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
                    saved_tokens += 1

            except Exception as e:
                print("Erro ao processar mercado:", e)

        db.commit()
        db.close()

        print(f"✅ Atualização concluída. Tokens salvos: {saved_tokens}\n")
        time.sleep(60)


if __name__ == "__main__":
    run()