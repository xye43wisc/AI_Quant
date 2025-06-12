# symbol_list.py
import akshare as ak


def get_all_a_share_symbols() -> list[str]:
    """
    拉取沪深京 A 股股票代码列表，使用一次性接口 stock_info_a_code_name。
    返回所有股票代码列表（不含市场前缀）。
    """
    df = ak.stock_info_a_code_name()
    # df.columns: ['code', 'name']
    return df['code'].tolist()


def get_all_a_share_code_name() -> list[tuple[str, str]]:
    """
    拉取沪深京 A 股股票代码和名称列表。
    返回包含 (code, name) 的元组列表。
    """
    df = ak.stock_info_a_code_name()
    return list(zip(df['code'], df['name']))