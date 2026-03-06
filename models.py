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
    token_id = Column(String, unique=True, index=True)
    outcome = Column(String)
    price = Column(Float, default=0.0)
    market_id = Column(Integer, ForeignKey("markets.id"))
    market = relationship("Market", back_populates="tokens")


class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(Integer, primary_key=True, index=True)
    token_id = Column(String, index=True)
    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)


class Trade(Base):
    __tablename__ = "trades"
    id = Column(Integer, primary_key=True, index=True)
    market_slug = Column(String, index=True)
    question = Column(String)
    outcome = Column(String)
    amount = Column(Float)
    entry_price = Column(Float)
    shares = Column(Float)
    status = Column(String, default="open")
    pnl = Column(Float, nullable=True)
    exit_price = Column(Float, nullable=True)
    notes = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)


class Signal(Base):
    __tablename__ = "signals"
    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    market = Column(String, nullable=False)
    slug = Column(String, nullable=False, index=True)
    outcome = Column(String, nullable=False)
    tipo = Column(String, nullable=False)
    change_5m = Column(Float, default=0.0)
    current_price = Column(Float, default=0.0)
    confidence = Column(Float, default=0.0)
    polymarket_url = Column(String, nullable=False)