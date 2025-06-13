# cleaner.py
import pandas as pd
import akshare as ak
from sqlalchemy.orm import Session as SessionType
from sqlalchemy import asc, desc
from storage import Session, update_cleaning_log
from models import DailyBar
from typing import List, Dict, Any, Optional
from datetime import date, timedelta

# 使用一个模块级缓存来存储交易日历，避免在单次运行中重复请求API
_trade_date_cache = None

def get_trade_calendar() -> pd.DataFrame:
    """
    获取A股所有历史交易日历。
    在首次调用时通过 akshare 获取并缓存在内存中。
    """
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
            # 返回一个空的DataFrame，让调用方可以安全处理
            return pd.DataFrame(columns=['trade_date'])
    return _trade_date_cache

def get_data_for_cleaning(symbol: str, start_date: Optional[date], session: SessionType) -> pd.DataFrame:
    """
    为清洗获取数据。
    如果提供了 start_date，会额外包含该日期的前一个交易日的数据，以确保价格变动计算的准确性。
    """
    query = session.query(DailyBar).filter(DailyBar.symbol == symbol)

    if start_date:
        # 为了计算第一个新数据的pct_change，需要获取start_date之前的一个数据点
        previous_day_query = session.query(DailyBar.trade_date)\
            .filter(DailyBar.symbol == symbol, DailyBar.trade_date < start_date)\
            .order_by(desc(DailyBar.trade_date)).limit(1).scalar_subquery()
        
        # 合并查询条件：日期大于等于前一个交易日
        query = query.filter(DailyBar.trade_date >= previous_day_query)

    query = query.order_by(asc(DailyBar.trade_date))
    return pd.read_sql(query.statement, query.session.bind)


def find_data_issues(df: pd.DataFrame, check_from_date: Optional[date]) -> List[Dict[str, Any]]:
    """
    分析数据并找出所有质量问题。
    只会报告 `check_from_date` (含) 之后的问题。
    """
    if df.empty:
        return []

    issues = []
    # 计算价格变动率
    df['pct_change'] = df['close'].pct_change().abs()
    
    # 如果是增量检查，只关注新日期
    target_df = df[df['trade_date'] >= check_from_date] if check_from_date else df

    # --- 规则检查 ---
    for _, row in target_df.iterrows():
        row_date_str = row['trade_date'].strftime('%Y-%m-%d')
        
        # 规则1: 价格异常 (<=0) 或 规则2: 高低价反转
        if row['low'] <= 0 or row['high'] <= 0 or row['open'] <= 0 or row['close'] <= 0 or row['low'] > row['high']:
            details = f"O={row['open']}, H={row['high']}, L={row['low']}, C={row['close']}"
            issues.append({'date': row_date_str, 'issue': '价格异常', 'details': details})

        # 规则3: 价格剧烈波动
        if pd.notna(row['pct_change']) and row['pct_change'] > 0.30: # 涨跌幅阈值30%
            issues.append({'date': row_date_str, 'issue': '价格突变', 'details': f"涨跌幅: {row['pct_change']:.2%}"})
    
    # 规则4: 日期连续性检查
    trade_calendar = get_trade_calendar()
    if not trade_calendar.empty:
        start, end = df['trade_date'].min(), df['trade_date'].max()
        mask = (trade_calendar['trade_date'] >= start) & (trade_calendar['trade_date'] <= end)
        expected_dates = set(trade_calendar.loc[mask, 'trade_date'])
        actual_dates = set(df['trade_date'])
        missing_dates = sorted([d for d in (expected_dates - actual_dates) if check_from_date is None or d >= check_from_date])

        if missing_dates:
            details = f"发现 {len(missing_dates)}个疑似缺失日。例如: {[d.strftime('%Y-%m-%d') for d in missing_dates[:3]]}"
            issues.append({'date': '时间段内', 'issue': '数据不连续', 'details': details})
            
    return issues


def clean_symbol(symbol: str, last_cleaned_date: Optional[date], full_recheck: bool):
    """
    对单个股票执行数据质量检查。
    :param symbol: 股票代码
    :param last_cleaned_date: 上次清洗的最后日期
    :param full_recheck: 是否强制全量重查
    """
    session = Session()
    try:
        start_date = None if full_recheck else last_cleaned_date
        df = get_data_for_cleaning(symbol, start_date, session)

        if df.empty or (start_date and len(df) <= 1):
            # 没有新数据或只有一个用于计算的旧数据点
            return

        # 确定需要开始报告问题的日期
        check_from_date = df['trade_date'].min() if full_recheck or not last_cleaned_date else last_cleaned_date
        
        issues = find_data_issues(df, check_from_date)

        if issues:
            print(f"--- 发现 {symbol} 的数据问题 ---")
            for issue in issues:
                print(f"  日期: {issue['date']}, 问题: {issue['issue']}, 详情: {issue['details']}")
        
        # 更新清洗日志到本次检查的最新日期
        latest_date = df['trade_date'].max()
        update_cleaning_log(symbol, latest_date, session)
        
        session.commit()

    except Exception as e:
        session.rollback()
        print(f"清洗 {symbol} 时发生错误: {e}")
    finally:
        session.close()