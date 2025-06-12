# models.py
from sqlalchemy import (
    Column, String, Date, Float, BigInteger, PrimaryKeyConstraint
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DailyBar(Base):
    __tablename__ = "daily_bar"
    symbol     = Column(String(10), nullable=False)
    trade_date = Column(Date, nullable=False)
    open       = Column(Float)
    high       = Column(Float)
    low        = Column(Float)
    close      = Column(Float)
    volume     = Column(BigInteger)
    __table_args__ = (PrimaryKeyConstraint("symbol", "trade_date"),)

class AdjFactor(Base):
    __tablename__ = "adj_factor"
    symbol          = Column(String(10), nullable=False)
    trade_date      = Column(Date, nullable=False)
    forward_factor  = Column(Float)
    back_factor     = Column(Float)
    __table_args__ = (PrimaryKeyConstraint("symbol", "trade_date"),)