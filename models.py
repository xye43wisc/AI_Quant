# models.py
from sqlalchemy import (
    Column, String, Date, Float, BigInteger, PrimaryKeyConstraint, create_engine,
    Index, Integer, DateTime
)
from sqlalchemy.orm import declarative_base
from config import settings
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
Base = declarative_base()

# --- 1. 模型列定义 (Mixins) ---
class DailyBarMixin:
    symbol     = Column(String(10), nullable=False)
    trade_date = Column(Date, nullable=False)
    open       = Column(Float)
    high       = Column(Float)
    low        = Column(Float)
    close      = Column(Float)
    volume     = Column(BigInteger)

class AdjFactorMixin:
    symbol          = Column(String(10), nullable=False)
    trade_date      = Column(Date, nullable=False)
    forward_factor  = Column(Float)
    back_factor     = Column(Float)

# --- 2. 静态模型 (与数据源无关) ---
class CleaningLog(Base):
    __tablename__ = "cleaning_log"
    symbol            = Column(String(10), nullable=False)
    source            = Column(String(20), nullable=False) # 新增 source 字段
    last_cleaned_date = Column(Date, nullable=False)
    # 将 symbol 和 source 设置为联合主键
    __table_args__ = (PrimaryKeyConstraint('symbol', 'source'),)


class SuspensionInfo(Base):
    __tablename__ = "suspension_info"
    symbol          = Column(String(10), nullable=False, primary_key=True)
    suspension_date = Column(Date, nullable=False, primary_key=True)

class DataIssueLog(Base):
    __tablename__ = 'data_issue_log'
    id = Column(Integer, primary_key=True, autoincrement=True)
    check_run_ts = Column(DateTime, default=datetime.utcnow, index=True)
    check_type = Column(String(50), nullable=False, index=True)
    symbol = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False)
    severity = Column(String(20))
    issue = Column(String(100))
    details = Column(String(500))
    __table_args__ = (Index('ix_issue_symbol_date_type', 'symbol', 'trade_date', 'check_type'),)


# --- 3. 动态模型工厂 ---
_KNOWN_SOURCES = ['akshare', 'baostock']
_MODELS_CACHE = {}

def _create_dynamic_models():
    for source in _KNOWN_SOURCES:
        bar_model = type(
            f'DailyBar_{source.capitalize()}', (DailyBarMixin, Base), 
            {'__tablename__': f'daily_bar_{source}', '__table_args__': (PrimaryKeyConstraint("symbol", "trade_date"),)}
        )
        _MODELS_CACHE[f'bar_{source}'] = bar_model

        factor_model = type(
            f'AdjFactor_{source.capitalize()}', (AdjFactorMixin, Base),
            {'__tablename__': f'adj_factor_{source}', '__table_args__': (PrimaryKeyConstraint("symbol", "trade_date"),)}
        )
        _MODELS_CACHE[f'factor_{source}'] = factor_model

def get_model(model_type: str, source: str):
    key = f'{model_type.lower()}_{source.lower()}'
    model = _MODELS_CACHE.get(key)
    if not model:
        raise ValueError(f"Model for type '{model_type}' and source '{source}' not found.")
    return model

_create_dynamic_models()

# --- 4. 数据库初始化辅助函数 ---
def create_all_tables():
    logger.info("Connecting to database to create all tables...")
    engine = create_engine(settings.DATABASE_URL)
    Base.metadata.create_all(engine)
    logger.info("All tables are created or already exist.")