# updater.py
from datetime import date
from storage import upsert_bars, upsert_factors
from data_sources.base_source import BaseSource
from typing import Optional
import pandas as pd
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)

def update_bar_for_symbol(
    symbol: str,
    source: BaseSource,
    session: Session,
    bar_model_class,
    last_date: Optional[str],
    start_date_override: Optional[str] = None,
    end_date_override: Optional[str] = None
):
    """为单个 symbol 更新日线数据。在循环外部处理 session commit。"""
    first = start_date_override or last_date or date(2010, 1, 1).strftime("%Y%m%d")
    end = end_date_override or date.today().strftime("%Y%m%d")

    if first >= end:
        return
    
    df_raw = source.fetch_bars(symbol, start_date=first, end_date=end)
    if not df_raw.empty:
        upsert_bars(df_raw, symbol, session, bar_model_class)
        # 此处的日志已被移除
        # logger.info(f"[{source.name}/{symbol}]: Fetched and staged {len(df_raw)} new bars from {first} to {end}.")

def update_factor_for_symbol(
    symbol: str,
    source: BaseSource,
    session: Session,
    factor_model_class
):
    """为单个 symbol 更新复权因子。在循环外部处理 session commit。"""
    df_q, df_h = source.fetch_factors(symbol)

    if df_q.empty or df_h.empty:
        return

    df_factor = pd.merge(df_q, df_h, on="trade_date", how="outer").dropna()
    if not df_factor.empty:
        upsert_factors(df_factor, symbol, session, factor_model_class)
        # 此处的日志已被移除
        # logger.info(f"[{source.name}/{symbol}]: Fetched and staged {len(df_factor)} factor records.")