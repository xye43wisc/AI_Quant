# data_sources/base_source.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import Tuple

class BaseSource(ABC):
    """
    数据源抽象基类，定义了所有数据源必须实现的标准接口。
    """

    @abstractmethod
    def login(self):
        """登录数据源（如果需要）"""
        pass

    @abstractmethod
    def logout(self):
        """登出数据源（如果需要）"""
        pass

    @abstractmethod
    def fetch_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取日线数据 (不复权)
        :return: 包含 trade_date, open, high, low, close, volume 的 DataFrame
        """
        pass

    @abstractmethod
    def fetch_factors(self, symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        获取复权因子
        :return: (前复权因子 DataFrame, 后复权因子 DataFrame)
        """
        pass