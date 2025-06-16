# cleaner.py
import pandas as pd
import akshare as ak
from sqlalchemy.orm import Session as SessionType
from sqlalchemy import asc, desc
from pandas import read_sql_query
from storage import Session, update_cleaning_log
from models import DailyBar, AdjFactor, SuspensionInfo # 导入 SuspensionInfo
from typing import List, Dict, Any, Optional
from datetime import date

_trade_date_cache = None

def get_trade_calendar() -> pd.DataFrame:
    global _trade_date_cache
    if _trade_date_cache is None:
        print("首次运行，正在从 akshare 获取市场交易日历...")
        try:
            df = ak.tool_trade_date_hist_sina()
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            _trade_date_cache = df
            print("交易日历获取成功。")
        except Exception as e:
            print(f"获取交易日历失败: {e}")
            return pd.DataFrame(columns=['trade_date'])
    return _trade_date_cache

def get_data_for_cleaning(symbol: str, start_date: Optional[date], session: SessionType) -> pd.DataFrame:
    query = session.query(DailyBar).filter(DailyBar.symbol == symbol)
    if start_date:
        previous_day_query = session.query(DailyBar.trade_date)\
            .filter(DailyBar.symbol == symbol, DailyBar.trade_date < start_date)\
            .order_by(desc(DailyBar.trade_date)).limit(1).scalar_subquery()
        query = query.filter(DailyBar.trade_date >= previous_day_query)
    query = query.order_by(asc(DailyBar.trade_date))
    return read_sql_query(query.statement, query.session.bind) # type: ignore

def get_adj_factors(symbol: str, session: SessionType) -> pd.DataFrame:
    query = session.query(AdjFactor).filter(AdjFactor.symbol == symbol).order_by(asc(AdjFactor.trade_date))
    return read_sql_query(query.statement, query.session.bind) # type: ignore

def get_suspension_dates(symbol: str, session: SessionType) -> set:
    results = session.query(SuspensionInfo.suspension_date)\
        .filter(SuspensionInfo.symbol == symbol).all()
    return {r[0] for r in results}

def find_data_issues(daily_df: pd.DataFrame, factor_df: pd.DataFrame, suspension_dates: set, check_from_date: Optional[date]) -> List[Dict[str, Any]]:
    if daily_df.empty:
        return []

    issues = []
    daily_df['pct_change'] = daily_df['close'].pct_change().abs()
    factor_dates = set(factor_df['trade_date'])
    target_df = daily_df[daily_df['trade_date'] >= check_from_date] if check_from_date else daily_df

    for _, row in target_df.iterrows():
        row_date_str = row['trade_date'].strftime('%Y-%m-%d')
        if row['low'] <= 0 or row['high'] <= 0 or row['open'] <= 0 or row['close'] <= 0 or row['low'] > row['high']:
            issues.append({'date': row_date_str, 'issue': '价格异常', 'severity': 'Error', 'details': f"O={row['open']}, H={row['high']}, L={row['low']}, C={row['close']}"})
        if pd.notna(row['pct_change']) and row['pct_change'] > 0.30:
            if row['trade_date'] in factor_dates:
                issues.append({'date': row_date_str, 'issue': '价格突变', 'severity': 'Warning', 'details': f"涨跌幅: {row['pct_change']:.2%}, 当日有复权事件，可能正常"})
            else:
                issues.append({'date': row_date_str, 'issue': '价格突变', 'severity': 'Error', 'details': f"涨跌幅: {row['pct_change']:.2%}"})
    
    trade_calendar = get_trade_calendar()
    if not trade_calendar.empty:
        start, end = daily_df['trade_date'].min(), daily_df['trade_date'].max()
        mask = (trade_calendar['trade_date'] >= start) & (trade_calendar['trade_date'] <= end)
        expected_dates = set(trade_calendar.loc[mask, 'trade_date']) # type: ignore
        actual_dates = set(daily_df['trade_date'])
        missing_dates = sorted([d for d in (expected_dates - actual_dates) if check_from_date is None or d >= check_from_date]) # type: ignore
        for missing_date in missing_dates:
            if missing_date in suspension_dates:
                issues.append({'date': missing_date.strftime('%Y-%m-%d'), 'issue': '数据不连续', 'severity': 'Warning', 'details': '该日为停牌日，数据缺失可能正常'}) # type: ignore
            else:
                issues.append({'date': missing_date.strftime('%Y-%m-%d'), 'issue': '数据不连续', 'severity': 'Error', 'details': '该交易日数据缺失'}) # type: ignore
    return issues

def clean_symbol(symbol: str, last_cleaned_date: Optional[date], full_recheck: bool) -> List[Dict[str, Any]]:
    session = Session()
    all_issues = []
    try:
        start_date = None if full_recheck else last_cleaned_date
        daily_df = get_data_for_cleaning(symbol, start_date, session)
        factor_df = get_adj_factors(symbol, session)
        suspension_dates = get_suspension_dates(symbol, session)
        if daily_df.empty or (start_date and len(daily_df) <= 1):
            return []
        check_from_date = daily_df['trade_date'].min() if full_recheck or not last_cleaned_date else last_cleaned_date
        issues = find_data_issues(daily_df, factor_df, suspension_dates, check_from_date)
        if issues:
            for issue in issues:
                issue['symbol'] = symbol
            all_issues.extend(issues)
            print(f"--- 发现 {symbol} 的 {len(issues)} 个问题 ---")
        latest_date = daily_df['trade_date'].max()
        update_cleaning_log(symbol, latest_date, session)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"清洗 {symbol} 时发生错误: {e}")
    finally:
        session.close()
    return all_issues