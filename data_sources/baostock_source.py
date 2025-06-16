# data_sources/baostock_source.py
import pandas as pd
import baostock as bs
from typing import Tuple
from datetime import date
import logging
from .base_source import BaseSource

logger = logging.getLogger(__name__)

class BaostockSource(BaseSource):
    """使用 Baostock 作为数据源的实现。"""

    @property
    def name(self) -> str:
        return 'baostock'

    def login(self):
        lg = bs.login()
        if lg.error_code != '0':
            raise ConnectionError(f"Baostock login failed: {lg.error_msg}")

    def logout(self):
        bs.logout()

    def _convert_symbol_format(self, symbol: str) -> str:
        """根据股票代码前缀判断市场。"""
        if symbol.startswith('6'):
            return f"sh.{symbol}"
        elif symbol.startswith('0') or symbol.startswith('3'):
            return f"sz.{symbol}"
        elif symbol.startswith('8') or symbol.startswith('4') or symbol.startswith('9'):
            return f"bj.{symbol}"
        return f"sz.{symbol}"

    def fetch_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取并标准化日线数据。"""
        bs_symbol = self._convert_symbol_format(symbol)
        if bs_symbol.startswith('bj.'):
            return pd.DataFrame()  # Baostock 不支持北交所股票
        start_date_bs = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        end_date_bs = pd.to_datetime(end_date).strftime('%Y-%m-%d')
        
        rs = bs.query_history_k_data_plus(
            bs_symbol, "date,open,high,low,close,volume",
            start_date=start_date_bs, end_date=end_date_bs,
            frequency="d", adjustflag="3"
        )
        if rs.error_code != '0': # type: ignore
            logger.warning(f"Baostock failed to fetch bars for {symbol}: {rs.error_msg}") # type: ignore
            return pd.DataFrame()

        df = rs.get_data() # type: ignore
        
        # --- 标准化步骤 ---
        df.rename(columns={'date': 'trade_date'}, inplace=True)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('Int64')

        standard_columns = ['trade_date', 'open', 'high', 'low', 'close', 'volume']
        return df[standard_columns].dropna()

    def fetch_factors(self, symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        [最终方案] 使用 bs.query_adjust_factor() 获取因子，并转换为每日累乘因子序列。
        """
        bs_symbol = self._convert_symbol_format(symbol)
        if bs_symbol.startswith('bj.'):
            return pd.DataFrame(), pd.DataFrame()  # Baostock 不支持北交所股票
        today_str = date.today().strftime("%Y-%m-%d")

        # 步骤 1: 获取该股票的所有历史交易日，作为输出的“骨架”
        rs_days = bs.query_history_k_data_plus(bs_symbol, "date", start_date="1990-01-01", end_date=today_str)
        if rs_days.error_code != '0' or not rs_days.get_data().shape[0] > 0: # type: ignore
            logger.warning(f"Baostock failed to fetch trading days for {symbol}.")
            return pd.DataFrame(), pd.DataFrame()
        
        df_days = rs_days.get_data() # type: ignore
        df_days['trade_date'] = pd.to_datetime(df_days['date'])
        df_days = df_days.set_index('trade_date').sort_index()[['date']] # 使用'date'列确保DataFrame不为空

        # 步骤 2: 获取所有除权除息日的“单次调整比例”
        rs_factor = bs.query_adjust_factor(code=bs_symbol, start_date="1990-01-01", end_date=today_str)
        if rs_factor.error_code != '0':
            logger.warning(f"Baostock failed to fetch adjust factor for {symbol}: {rs_factor.error_msg}")
            return pd.DataFrame(), pd.DataFrame()
        
        factor_events = []
        while (rs_factor.error_code == '0') & rs_factor.next():
            factor_events.append(rs_factor.get_row_data())

        # 如果该股票从未有过除权除息，则所有因子都为1.0，直接返回
        if not factor_events:
            df_days['forward_factor'] = 1.0
            df_days['back_factor'] = 1.0
            df_final = df_days.reset_index()
            return df_final[['trade_date', 'forward_factor']], df_final[['trade_date', 'back_factor']]

        # 步骤 3: 将“单次调整比例”合并到所有交易日的“骨架”中
        df_events = pd.DataFrame(factor_events, columns=rs_factor.fields)
        df_events.rename(columns={'dividOperateDate': 'trade_date'}, inplace=True)
        df_events['trade_date'] = pd.to_datetime(df_events['trade_date'])
        df_events['foreAdjustFactor'] = pd.to_numeric(df_events['foreAdjustFactor'])
        df_events['backAdjustFactor'] = pd.to_numeric(df_events['backAdjustFactor'])
        df_events = df_events.set_index('trade_date')

        # 根据BaoStock文档，标准前复权因子是 foreAdjustFactor 的倒数
        df_days['forward_factor_discrete'] = 1 / df_events['foreAdjustFactor']
        df_days['back_factor_discrete'] = df_events['backAdjustFactor']
        df_days.fillna(1.0, inplace=True) # 非事件日的单次调整比例为1.0（即无调整）

        # 步骤 4: [核心] 使用累乘(cumprod)将“单次调整比例”转换为“每日累乘因子”
        # 前复权因子 = 从后往前累乘
        df_days = df_days.sort_index(ascending=False)
        df_days['forward_factor'] = df_days['forward_factor_discrete'].cumprod()
        
        # 后复权因子 = 从前往后累乘
        df_days = df_days.sort_index(ascending=True)
        df_days['back_factor'] = df_days['back_factor_discrete'].cumprod()
        
        # 步骤 5: 返回符合系统标准的每日因子序列
        df_final = df_days.reset_index()
        
        df_q_final = df_final[['trade_date', 'forward_factor']].copy()
        df_h_final = df_final[['trade_date', 'back_factor']].copy()
        
        return df_q_final, df_h_final