# updater.py
from datetime import date
from storage import upsert_bars, upsert_factors, Session
from fetcher import fetch_raw, fetch_qfq_factor, fetch_hfq_factor
from config import settings
from typing import Optional, Tuple
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

def update_symbol(
    symbol: str,
    last_date: Optional[str],
    start_date_override: Optional[str] = None
) -> None:
    """
    更新指定 symbol 的数据。
    此版本中，bar的获取与factor的获取并行，但两个factor的获取任务内部是串行的。

    :param symbol: 股票代码
    :param last_date: 数据库中该股票的最后日期（YYYYMMDD），可能为 None
    :param start_date_override: 命令行传入的强制起始日期，优先级最高
    """
    first = start_date_override or last_date or settings.DEFAULT_START_DATE
    today = date.today().strftime("%Y%m%d")

    if first >= today:
        return

    # 辅助函数，用于串行获取两种因子
    def _fetch_factors_sequentially(symbol_to_fetch: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df_q = fetch_qfq_factor(symbol_to_fetch)
        df_h = fetch_hfq_factor(symbol_to_fetch)
        return df_q, df_h

    # 使用线程池并行执行两个主要任务：获取bar 和 获取factors
    with ThreadPoolExecutor(max_workers=1) as executor:
        future_raw = executor.submit(fetch_raw, symbol, start_date=first, end_date=today)
        future_factors = executor.submit(_fetch_factors_sequentially, symbol)

        # 获取任务结果
        df_raw = future_raw.result()
        # 从 future_factors 的结果中解包出两个 dataframe
        df_q, df_h = future_factors.result()


    if df_raw.empty:
        return

    # 数据处理与入库
    session = Session()
    try:
        # 入库日线
        upsert_bars(df_raw, symbol, session)

        # 合并并入库因子
        if not df_q.empty and not df_h.empty:
            df_factor = pd.merge(df_q, df_h, on="trade_date", how="outer").dropna()

            # 筛选因子数据，仅更新需要的时间范围，减少不必要的数据库写入
            mask = (df_factor['trade_date'] >= pd.to_datetime(first)) & \
                   (df_factor['trade_date'] <= pd.to_datetime(today))
            upsert_factors(df_factor[mask], symbol, session)

        session.commit()
        print(f"{symbol}: updated bars={len(df_raw)} from {first} to {today}")
    except Exception as e:
        session.rollback()
        print(f"Failed to update {symbol}: {e}")
    finally:
        session.close()