# storage.py
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session as SessionType
from sqlalchemy.dialects.postgresql import insert
from config import settings
from models import Base, DailyBar, AdjFactor
from typing import Dict, List

# Initialize the database connection
en= create_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
Session = sessionmaker(bind=en)
Base.metadata.create_all(en)

def get_all_last_dates() -> Dict[str, str]:
    """
    一次性获取数据库中所有 symbol 的最后一个交易日。
    这个版本利用了 (symbol, trade_date) 上的索引，性能最佳。
    注意：此函数要求数据库的 max_locks_per_transaction 配置足够大。
    """
    session = Session()
    try:
        results = (
            session.query(DailyBar.symbol, func.max(DailyBar.trade_date))
                   .group_by(DailyBar.symbol)
                   .all()
        )
        return {symbol: date.strftime("%Y%m%d") for symbol, date in results}
    finally:
        session.close()


def upsert_bars(df, symbol: str, session: SessionType):
    """
    使用传入的 session 批量 upsert 日线数据
    """
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
    """
    使用传入的 session 批量 upsert 复权因子数据
    """
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