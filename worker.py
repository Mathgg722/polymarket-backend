import time
import requests
from datetime import datetime, timedelta

from database import SessionLocal, engine
from models import Base, Market, Token, Snapshot, Signal

Base.metadata.create_all(bind=engine)

POLYMARKET_API = "https://clob.polymarket.com/markets"


def fetch_markets():
    try:
        r = requests.get(POLYMARKET_API, timeout=10)
        return r.json()
    except Exception as e:
        print("Erro ao buscar mercados:", e)
        return []


def detect_signal(db, token_id, current_price, market):
    try:
        five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)

        prev = (
            db.query(Snapshot)
            .filter(Snapshot.token_id == token_id)
            .order_by(Snapshot.timestamp.desc())
            .limit(50)
            .all()
        )

        prev_5m = None
        for s in prev:
            if s.timestamp <= five_minutes_ago:
                prev_5m = s
                break

        if not prev_5m:
            return

        prev_price = prev_5m.price * 100
        current_price = current_price * 100

        if prev_price == 0:
            return

        change = ((current_price - prev_price) / prev_price) * 100
        change = round(change, 1)

        signal_type = None

        if abs(change) >= 20:
            signal_type = "EXTREME"
        elif change >= 8:
            signal_type = "SPIKE"
        elif change <= -8:
            signal_type = "DUMP"

        if not signal_type:
            return

        confidence = min(1.0, abs(change) / 20)

        signal = Signal(
            market=market.question,
            slug=market.market_slug,
            outcome="YES",
            tipo=signal_type,
            change_5m=change,
            current_price=current_price,
            confidence=confidence,
            polymarket_url=f"https://polymarket.com/event/{market.market_slug}",
        )

        db.add(signal)

    except Exception as e:
        print("Erro detectando sinal:", e)


def run_worker():
    while True:

        print("🔄 Atualizando mercados...")

        db = SessionLocal()
        markets = fetch_markets()

        print("📊 Mercados recebidos:", len(markets))

        saved = 0

        for m in markets:

            try:
                slug = m.get("slug")
                question = m.get("question")

                existing = db.query(Market).filter_by(market_slug=slug).first()

                if not existing:
                    existing = Market(
                        market_slug=slug,
                        question=question,
                        end_date=m.get("endDate"),
                    )
                    db.add(existing)
                    db.flush()

                tokens = m.get("tokens", [])

                for t in tokens:

                    token_id = t.get("token_id")
                    outcome = t.get("outcome")
                    price = float(t.get("price") or 0)

                    token_obj = (
                        db.query(Token)
                        .filter_by(token_id=token_id)
                        .first()
                    )

                    if not token_obj:
                        token_obj = Token(
                            token_id=token_id,
                            outcome=outcome,
                            price=price,
                            market_id=existing.id,
                        )
                        db.add(token_obj)
                    else:
                        token_obj.price = price

                    snapshot = Snapshot(
                        token_id=token_id,
                        price=price,
                    )

                    db.add(snapshot)

                    detect_signal(db, token_id, price, existing)

                    saved += 1

            except Exception as e:
                print("Erro no mercado:", e)

        db.commit()
        db.close()

        print("💾 Snapshots salvos:", saved)

        time.sleep(60)


if __name__ == "__main__":
    run_worker()