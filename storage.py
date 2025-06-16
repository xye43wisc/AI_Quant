# storage.py
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session as SessionType
from sqlalchemy.dialects.postgresql import insert
from config import settings
from models import CleaningLog # 仅导入静态模型
from typing import Dict, Optional
from datetime import date

# --- 1. 单一数据库连接 ---
engine = create_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
Session = sessionmaker(bind=engine)

# --- 2. 数据操作函数 (接收 session 和 model_class) ---
def get_all_last_dates(session: SessionType, bar_model_class) -> Dict[str, str]:
    results = (
        session.query(bar_model_class.symbol, func.max(bar_model_class.trade_date))
               .group_by(bar_model_class.symbol).all()
    )
    return {symbol: date_obj.strftime("%Y%m%d") for symbol, date_obj in results}

def upsert_bars(df, symbol: str, session: SessionType, bar_model_class):
    data = [
        {'symbol': symbol, 'trade_date': row.trade_date.date(), 'open': row.open, 'high': row.high, 'low': row.low, 'close': row.close, 'volume': row.volume}
        for _, row in df.iterrows()
    ]
    if not data: return
    
    stmt = insert(bar_model_class).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'trade_date'])
    session.execute(stmt)

def upsert_factors(df, symbol: str, session: SessionType, factor_model_class):
    data = [
        {'symbol': symbol, 'trade_date': row.trade_date.date(), 'forward_factor': row.forward_factor, 'back_factor': row.back_factor}
        for _, row in df.iterrows()
    ]
    if not data: return

    stmt = insert(factor_model_class).values(data)
    stmt = stmt.on_conflict_do_update(
        index_elements=['symbol', 'trade_date'],
        set_={'forward_factor': stmt.excluded.forward_factor, 'back_factor': stmt.excluded.back_factor}
    )
    session.execute(stmt)

def get_last_cleaned_dates_for_source(session: SessionType, source: str) -> Dict[str, date]:
    """获取指定数据源的所有股票的最后清洗日期。"""
    results = session.query(CleaningLog.symbol, CleaningLog.last_cleaned_date)\
        .filter(CleaningLog.source == source).all()
    return {symbol: date_obj for symbol, date_obj in results}

def update_cleaning_log(symbol: str, source: str, latest_date: date, session: SessionType):
    """使用传入的 session 来 upsert 清洗日志，现在包含 source。"""
    stmt = insert(CleaningLog).values(
        symbol=symbol, 
        source=source, 
        last_cleaned_date=latest_date
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=['symbol', 'source'],
        set_={'last_cleaned_date': stmt.excluded.last_cleaned_date}
    )
    session.execute(stmt)