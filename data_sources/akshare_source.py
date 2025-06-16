# data_sources/akshare_source.py
import pandas as pd
from akshare import stock_zh_a_hist, stock_zh_a_daily
from typing import Tuple

from .base_source import BaseSource

class AkshareSource(BaseSource):
    """使用 Akshare 作为数据源的实现。"""

    @property
    def name(self) -> str:
        return 'akshare'

    def login(self):
        # Akshare 不需要登录
        print("Akshare source does not require login.")
        pass

    def logout(self):
        # Akshare 不需要登出
        pass

    def _determine_market(self, symbol: str) -> str:
        """根据股票代码前缀判断市场。"""
        if symbol.startswith('6'):
            return 'sh'
        elif symbol.startswith('0') or symbol.startswith('3'):
            return 'sz'
        elif symbol.startswith('8') or symbol.startswith('4') or symbol.startswith('9'):
            return 'bj'
        return 'sz'

    def fetch_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用东财接口获取不复权日线数据。"""
        df = stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )
        rename_map = {'日期': 'trade_date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'}
        df.rename(columns=rename_map, inplace=True)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        return df[['trade_date', 'open', 'high', 'low', 'close', 'volume']]

    def fetch_factors(self, symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """获取前复权和后复权因子。"""
        market = self._determine_market(symbol)
        
        # 获取前复权因子
        df_q = stock_zh_a_daily(symbol=f"{market}{symbol}", adjust="qfq-factor")
        df_q.rename(columns={'date': 'trade_date', 'qfq_factor': 'forward_factor'}, inplace=True)
        df_q['trade_date'] = pd.to_datetime(df_q['trade_date'])

        # 获取后复权因子
        df_h = stock_zh_a_daily(symbol=f"{market}{symbol}", adjust="hfq-factor")
        df_h.rename(columns={'date': 'trade_date', 'hfq_factor': 'back_factor'}, inplace=True)
        df_h['trade_date'] = pd.to_datetime(df_h['trade_date'])
        
        return df_q[['trade_date', 'forward_factor']], df_h[['trade_date', 'back_factor']]