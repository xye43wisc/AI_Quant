# cleaner.py
import pandas as pd
import akshare as ak
from sqlalchemy.orm import Session as SessionType
from sqlalchemy import asc, desc
from pandas import read_sql_query
from storage import update_cleaning_log, Session
from models import SuspensionInfo, get_model, CleaningLog
from typing import List, Dict, Any, Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)
_trade_date_cache = None

def get_trade_calendar() -> pd.DataFrame:
    global _trade_date_cache
    if _trade_date_cache is None:
        try:
            df = ak.tool_trade_date_hist_sina()
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            _trade_date_cache = df
        except Exception as e:
            logger.warning(f"无法获取交易日历: {e}")
            return pd.DataFrame(columns=['trade_date'])
    return _trade_date_cache

def get_data_for_cleaning(symbol: str, session: SessionType, bar_model_class, start_date: Optional[date]) -> pd.DataFrame:
    query = session.query(bar_model_class).filter(bar_model_class.symbol == symbol)
    if start_date:
        previous_day_query = session.query(bar_model_class.trade_date)\
            .filter(bar_model_class.symbol == symbol, bar_model_class.trade_date < start_date)\
            .order_by(desc(bar_model_class.trade_date)).limit(1).scalar_subquery()
        query = query.filter(bar_model_class.trade_date >= previous_day_query)
    query = query.order_by(asc(bar_model_class.trade_date))
    return read_sql_query(query.statement, query.session.bind)  # type: ignore

def get_adj_factors(symbol: str, session: SessionType, factor_model_class) -> pd.DataFrame:
    query = session.query(factor_model_class).filter(factor_model_class.symbol == symbol).order_by(asc(factor_model_class.trade_date))
    return read_sql_query(query.statement, query.session.bind) # type: ignore

def get_suspension_dates(symbol: str, session: SessionType) -> set:
    results = session.query(SuspensionInfo.suspension_date).filter(SuspensionInfo.symbol == symbol).all()
    return {r[0] for r in results}

def find_data_issues(daily_df: pd.DataFrame, factor_df: pd.DataFrame, check_from_date: Optional[date]) -> List[Dict[str, Any]]:
    if daily_df.empty:
        return []
    issues = []
    daily_df['trade_date'] = pd.to_datetime(daily_df['trade_date']).dt.date
    daily_df['pct_change'] = daily_df['close'].pct_change().abs()
    factor_dates = set(pd.to_datetime(factor_df['trade_date']).dt.date) if not factor_df.empty else set()
    target_df = daily_df[daily_df['trade_date'] >= check_from_date] if check_from_date else daily_df

    for _, row in target_df.iterrows():
        row_date_obj = row['trade_date']
        if row['low'] <= 0 or row['high'] <= 0 or row['open'] <= 0 or row['close'] <= 0 or row['low'] > row['high']:
            issues.append({'trade_date': row_date_obj, 'issue': '价格异常', 'severity': 'Error', 'details': f"O={row['open']}, H={row['high']}, L={row['low']}, C={row['close']}"})
        if pd.notna(row['pct_change']) and row['pct_change'] > 0.30:
            if row_date_obj in factor_dates:
                issues.append({'trade_date': row_date_obj, 'issue': '价格突变', 'severity': 'Warning', 'details': f"涨跌幅: {row['pct_change']:.2%}, 当日有复权事件，可能正常"})
            else:
                issues.append({'trade_date': row_date_obj, 'issue': '价格突变', 'severity': 'Error', 'details': f"涨跌幅: {row['pct_change']:.2%}"})
    return issues

def clean_symbol(symbol: str, source_name: str, session: SessionType, bar_model, factor_model, last_cleaned_date: Optional[date], full_recheck: bool) -> List[Dict[str, Any]]:
    start_date = None if full_recheck else last_cleaned_date
    daily_df = get_data_for_cleaning(symbol, session, bar_model, start_date)
    if daily_df.empty or (start_date and len(daily_df) <= 1):
        return []
    factor_df = get_adj_factors(symbol, session, factor_model)
    check_from_date = daily_df['trade_date'].min() if full_recheck or not last_cleaned_date else last_cleaned_date
    issues = find_data_issues(daily_df, factor_df, check_from_date)
    final_issues = []
    if issues:
        for issue in issues:
            issue['symbol'] = symbol
            issue['check_type'] = 'single_source'
            final_issues.append(issue)
    latest_date_in_df = daily_df['trade_date'].max()
    update_cleaning_log(symbol, source_name, latest_date_in_df, session)
    return final_issues

def worker_clean(symbol: str, source_name: str, full_recheck: bool) -> List[Dict[str, Any]]:
    session = Session()
    try:
        bar_model = get_model('bar', source_name)
        factor_model = get_model('factor', source_name)
        last_cleaned_date = None
        if not full_recheck:
            log_entry = session.query(CleaningLog).filter_by(symbol=symbol, source=source_name).first()
            if log_entry:
                last_cleaned_date = log_entry.last_cleaned_date
        issues = clean_symbol(symbol, source_name, session, bar_model, factor_model, last_cleaned_date, full_recheck) # type: ignore
        session.commit()
        return issues
    except Exception as e:
        session.rollback()
        logger.error(f"清洗股票 {symbol} (来源: {source_name}) 时出错。", exc_info=True)
        return []
    finally:
        session.close()

def cross_validate_symbol(symbol: str, session: SessionType) -> List[Dict[str, Any]]:
    ak_bar_model = get_model('bar', 'akshare')
    bk_bar_model = get_model('bar', 'baostock')
    df_ak = read_sql_query(session.query(ak_bar_model).filter(ak_bar_model.symbol == symbol).order_by(asc(ak_bar_model.trade_date)).statement, session.bind) # type: ignore
    df_bk = read_sql_query(session.query(bk_bar_model).filter(bk_bar_model.symbol == symbol).order_by(asc(bk_bar_model.trade_date)).statement, session.bind) # type: ignore
    if df_ak.empty and df_bk.empty:
        return []
    df_merged = pd.merge(df_ak, df_bk, on='trade_date', how='outer', suffixes=('_ak', '_bk')).sort_values('trade_date').reset_index(drop=True)
    df_merged['trade_date'] = pd.to_datetime(df_merged['trade_date']).dt.date
    suspension_dates = get_suspension_dates(symbol, session)
    trade_calendar_df = get_trade_calendar()
    issues = []
    for _, row in df_merged.iterrows():
        date = row['trade_date']
        ak_exists = pd.notna(row.get('close_ak'))
        bk_exists = pd.notna(row.get('close_bk'))
        if ak_exists and bk_exists:
            price_diff = abs(row['close_ak'] - row['close_bk']) / row['close_ak'] if row['close_ak'] != 0 else 0
            if price_diff > 0.001:
                issues.append({'trade_date': date, 'issue': '价格不一致', 'severity': 'Error', 'details': f"收盘价差异: ak={row['close_ak']:.2f}, bk={row['close_bk']:.2f}"})
        elif ak_exists and not bk_exists:
            if date not in suspension_dates:
                issues.append({'trade_date': date, 'issue': '单方面数据缺失', 'severity': 'Warning', 'details': 'baostock 源缺失该日数据'})
        elif not ak_exists and bk_exists:
            if date not in suspension_dates:
                issues.append({'trade_date': date, 'issue': '单方面数据缺失', 'severity': 'Warning', 'details': 'akshare 源缺失该日数据'})
    if not trade_calendar_df.empty:
        start_date = df_merged['trade_date'].min()
        end_date = df_merged['trade_date'].max()
        mask = (trade_calendar_df['trade_date'] >= start_date) & (trade_calendar_df['trade_date'] <= end_date)
        expected_dates = set(trade_calendar_df.loc[mask, 'trade_date']) # type: ignore
        actual_dates = set(df_merged['trade_date'])
        missing_dates = sorted(list(expected_dates - actual_dates))
        for date in missing_dates:
            if date not in suspension_dates:
                issues.append({'trade_date': date, 'issue': '双方数据缺失', 'severity': 'Critical', 'details': 'akshare 和 baostock 均缺失该交易日数据'})
    final_issues = []
    if issues:
        for issue in issues:
            issue['symbol'] = symbol
            issue['check_type'] = 'cross_validation'
            final_issues.append(issue)
    return final_issues

def worker_cross_validate(symbol: str) -> List[Dict[str, Any]]:
    session = Session()
    try:
        return cross_validate_symbol(symbol, session)
    except Exception as e:
        logger.error(f"交叉验证股票 {symbol} 时出错。", exc_info=True)
        return []
    finally:
        session.close()