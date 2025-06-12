# updater.py
from datetime import date
from storage import upsert_bars, upsert_factors
from fetcher import fetch_raw, fetch_qfq_factor, fetch_hfq_factor
from config import settings
from typing import Optional


def get_last_date(symbol: str) -> Optional[str]:
    """
    返回数据库中最后一个交易日（YYYYMMDD），如果没有记录则返回 None。
    """
    from storage import Session
    from models import DailyBar

    session = Session()
    last = (
        session.query(DailyBar.trade_date)
               .filter(DailyBar.symbol == symbol)
               .order_by(DailyBar.trade_date.desc())
               .first()
    )
    session.close()
    return last[0].strftime("%Y%m%d") if last else None


def update_symbol(symbol: str, start_date: Optional[str] = None) -> None:
    """
    更新指定 symbol 的数据。
    start_date: 可选覆盖起始日期（格式 YYYYMMDD），
                若不指定则取上次更新或 DEFAULT_START_DATE。
    """
    last = get_last_date(symbol)
    first = start_date or last or settings.DEFAULT_START_DATE
    today = date.today().strftime("%Y%m%d")
    if first >= today:
        print(f"{symbol}: no new data (since {first})")
        return

    # 拉取并入库不复权日线
    df_raw = fetch_raw(symbol, start_date=first, end_date=today)
    upsert_bars(df_raw, symbol)
    # 拉取并入库复权因子
    df_q = fetch_qfq_factor(symbol)
    df_h = fetch_hfq_factor(symbol)
    df_factor = df_q.merge(df_h, on="trade_date")
    upsert_factors(df_factor, symbol)

    print(f"{symbol}: updated bars={len(df_raw)} factors={len(df_factor)} from {first} to {today}")