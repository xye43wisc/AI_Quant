# fetcher.py
import pandas as pd
from akshare import stock_zh_a_hist, stock_zh_a_daily
from config import settings

def fetch_raw(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    使用东财接口获取不复权日线数据
    返回字段: trade_date, open, high, low, close, volume
    """
    df = stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=""
    )
    # 重命名中文列到英文
    rename_map = {
        '日期': 'trade_date',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '成交量': 'volume'
    }
    df.rename(columns=rename_map, inplace=True)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df[['trade_date', 'open', 'high', 'low', 'close', 'volume']]

def determine_market(symbol: str) -> str:
    return 'sh' if symbol.startswith('6') else 'sz'

def fetch_qfq_factor(symbol: str) -> pd.DataFrame:
    market = determine_market(symbol)
    df = stock_zh_a_daily(symbol=f"{market}{symbol}", adjust="qfq-factor")
    df.rename(columns={'date':'trade_date','qfq_factor':'forward_factor'}, inplace=True)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df[['trade_date','forward_factor']]

def fetch_hfq_factor(symbol: str) -> pd.DataFrame:
    market = determine_market(symbol)
    df = stock_zh_a_daily(symbol=f"{market}{symbol}", adjust="hfq-factor")
    df.rename(columns={'date':'trade_date','hfq_factor':'back_factor'}, inplace=True)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df[['trade_date','back_factor']]