from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from database import Base


class Market(Base):
    __tablename__ = "markets"

    id = Column(Integer, primary_key=True, index=True)
    market_slug = Column(String, unique=True, index=True)
    question = Column(String)
    end_date = Column(DateTime, nullable=True)

    tokens = relationship("Token", back_populates="market")


class Token(Base):
    __tablename__ = "tokens"

    id = Column(Integer, primary_key=True, index=True)
    token_id = Column(String)
    outcome = Column(String)
    price = Column(Float)

    market_id = Column(Integer, ForeignKey("markets.id"))
    market = relationship("Market", back_populates="tokens")


class Snapshot(Base):
    __tablename__ = "snapshots"

    id = Column(Integer, primary_key=True, index=True)
    token_id = Column(String)
    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    market_slug = Column(String, index=True)
    question = Column(String)
    outcome = Column(String)           # YES ou NO
    amount = Column(Float)             # valor apostado em USD
    entry_price = Column(Float)        # preco de entrada (0 a 100)
    shares = Column(Float)             # quantidade de shares compradas
    status = Column(String, default="open")   # open, closed, won, lost
    pnl = Column(Float, nullable=True)        # lucro/prejuizo ao fechar
    exit_price = Column(Float, nullable=True) # preco de saida
    notes = Column(String, nullable=True)     # observacoes
    created_at = Column(DateTime, default=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)
    from sqlalchemy import Column, Integer, String, Float, DateTime

class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    market = Column(String, nullable=False)
    slug = Column(String, nullable=False, index=True)
    outcome = Column(String, nullable=False)  # YES/NO

    tipo = Column(String, nullable=False)  # SPIKE/DUMP/REVERSAL/EXTREME
    change_5m = Column(Float, default=0.0)  # em %
    current_price = Column(Float, default=0.0)  # em %
    confidence = Column(Float, default=0.0)  # 0-1

    polymarket_url = Column(String, nullable=False)
    from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime

class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    market = Column(String, nullable=False)
    slug = Column(String, nullable=False, index=True)
    outcome = Column(String, nullable=False)  # YES/NO

    tipo = Column(String, nullable=False)  # SPIKE/DUMP/EXTREME
    change_5m = Column(Float, default=0.0)
    current_price = Column(Float, default=0.0)
    confidence = Column(Float, default=0.0)

    polymarket_url = Column(String, nullable=False)