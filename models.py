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