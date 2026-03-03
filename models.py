from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from database import Base


class Market(Base):
    __tablename__ = "markets"

    id = Column(Integer, primary_key=True, index=True)
    market_slug = Column(String, unique=True, index=True)
    question = Column(String)
    end_date = Column(DateTime, nullable=True)  # CORRIGIDO: era String, agora DateTime

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