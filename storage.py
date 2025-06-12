from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from config import settings
from models import Base, DailyBar, AdjFactor

# 初始化数据库连接
en= create_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
Session = sessionmaker(bind=en)
Base.metadata.create_all(en)

def upsert_bars(df, symbol: str):
    """
    使用 PostgreSQL 的 ON CONFLICT DO NOTHING 实现批量 upsert 日线数据
    """
    session = Session()
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
    stmt = insert(DailyBar).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'trade_date'])
    session.execute(stmt)
    session.commit()
    session.close()


def upsert_factors(df, symbol: str):
    """
    使用 PostgreSQL 的 ON CONFLICT DO NOTHING 实现批量 upsert 复权因子数据
    """
    session = Session()
    data = [
        {
            'symbol': symbol,
            'trade_date': row.trade_date.date(),
            'forward_factor': row.forward_factor,
            'back_factor': row.back_factor
        }
        for _, row in df.iterrows()
    ]
    stmt = insert(AdjFactor).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'trade_date'])
    session.execute(stmt)
    session.commit()
    session.close()