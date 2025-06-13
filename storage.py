# storage.py
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session as SessionType
from sqlalchemy.dialects.postgresql import insert
from config import settings
from models import Base, DailyBar, AdjFactor, CleaningLog # 导入 CleaningLog
from typing import Dict, List, Optional
from datetime import date

# Initialize the database connection
en= create_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
Session = sessionmaker(bind=en)
Base.metadata.create_all(en) # 这将自动创建包括 cleaning_log 在内的新表

def get_all_last_dates() -> Dict[str, str]:
    # ... (此函数保持不变)
    session = Session()
    try:
        results = (
            session.query(DailyBar.symbol, func.max(DailyBar.trade_date))
                   .group_by(DailyBar.symbol)
                   .all()
        )
        return {symbol: date_obj.strftime("%Y%m%d") for symbol, date_obj in results}
    finally:
        session.close()

def upsert_bars(df, symbol: str, session: SessionType):
    # ... (此函数保持不变)
    data = [
        {
            'symbol': symbol,
            'trade_date': row.trade_date.date(),
            'open': row.open,
            'high': row.high,
            'low': row.low,
            'close': row.close,
            'volume': row.volume
        }
        for _, row in df.iterrows()
    ]
    if not data:
        return
    
    stmt = insert(DailyBar).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'trade_date'])
    session.execute(stmt)

def upsert_factors(df, symbol: str, session: SessionType):
    # ... (此函数保持不变)
    data = [
        {
            'symbol': symbol,
            'trade_date': row.trade_date.date(),
            'forward_factor': row.forward_factor,
            'back_factor': row.back_factor
        }
        for _, row in df.iterrows()
    ]
    if not data:
        return

    stmt = insert(AdjFactor).values(data)
    stmt = stmt.on_conflict_do_update(
        index_elements=['symbol', 'trade_date'],
        set_={
            'forward_factor': stmt.excluded.forward_factor,
            'back_factor': stmt.excluded.back_factor
        }
    )
    session.execute(stmt)

# --- 新增函数 ---

def get_last_cleaned_dates() -> Dict[str, date]:
    """一次性获取所有 symbol 的最后清洗日期。"""
    session = Session()
    try:
        results = session.query(CleaningLog.symbol, CleaningLog.last_cleaned_date).all()
        return {symbol: date_obj for symbol, date_obj in results}
    finally:
        session.close()

def update_cleaning_log(symbol: str, latest_date: date, session: SessionType):
    """使用传入的 session 来 upsert 清洗日志。"""
    stmt = insert(CleaningLog).values(symbol=symbol, last_cleaned_date=latest_date)
    stmt = stmt.on_conflict_do_update(
        index_elements=['symbol'],
        set_={'last_cleaned_date': stmt.excluded.last_cleaned_date}
    )
    session.execute(stmt)