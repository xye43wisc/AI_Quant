# main.py
import sys
import argparse
from updater import update_symbol
from symbol_list import get_all_a_share_symbols
from config import settings

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A股数据下载与更新脚本")
    parser.add_argument("symbols", nargs="*", help="股票代码列表，或指定 'all' 代表所有A股")
    parser.add_argument("--start-date", type=str, default=None,
                        help="覆盖起始拉取日期，格式 YYYYMMDD，默认使用上次更新或配置")
    args = parser.parse_args()
    start_date = args.start_date
    if not args.symbols or args.symbols[0].lower() == "all":
        symbols = get_all_a_share_symbols()
    else:
        symbols = args.symbols
    for sym in symbols:
        update_symbol(sym, start_date)