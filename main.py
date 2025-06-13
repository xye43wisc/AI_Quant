# main.py
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from updater import update_symbol
from symbol_list import get_all_a_share_symbols
from storage import get_all_last_dates, get_last_cleaned_dates
from cleaner import clean_symbol

def run_update(args):
    """执行数据更新任务"""
    if not args.symbols or args.symbols[0].lower() == "all":
        print("正在获取所有A股代码...")
        symbols = get_all_a_share_symbols()
        print(f"已找到 {len(symbols)} 个代码。")
    else:
        symbols = args.symbols

    print("正在从数据库获取所有股票的最后更新日期...")
    all_last_dates = get_all_last_dates()
    print("完成。")

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                update_symbol, 
                sym, 
                all_last_dates.get(sym),
                args.start_date
            ): sym 
            for sym in symbols
        }
        
        for future in tqdm(as_completed(futures), total=len(symbols), desc="更新数据"):
            sym = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"处理 {sym} 时发生错误: {e}", file=sys.stderr)

def run_clean(args):
    """执行数据清洗任务"""
    if not args.symbols or args.symbols[0].lower() == "all":
        print("正在获取所有A股代码...")
        symbols = get_all_a_share_symbols()
        print(f"已找到 {len(symbols)} 个代码。")
    else:
        symbols = args.symbols

    last_cleaned_dates = {}
    if not args.full_recheck:
        print("正在获取所有股票的最后清洗日期...")
        last_cleaned_dates = get_last_cleaned_dates()
        print("完成。")
    else:
        print("检测到 --full-recheck 参数，将对所有历史数据进行全面检查。")

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                clean_symbol,
                sym,
                last_cleaned_dates.get(sym),
                args.full_recheck
            ): sym
            for sym in symbols
        }
        for future in tqdm(as_completed(futures), total=len(symbols), desc="清洗数据"):
            sym = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"清洗 {sym} 时发生错误: {e}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="A股数据管理工具")
    subparsers = parser.add_subparsers(dest="command", required=True, help="可执行的命令")

    # 更新命令解析器
    parser_update = subparsers.add_parser("update", help="下载或更新股票日线及复权数据")
    parser_update.add_argument("symbols", nargs="*", help="股票代码列表，或指定 'all' 代表所有A股", default=["all"])
    parser_update.add_argument("--start-date", type=str, default=None, help="强制指定开始日期 (格式 YYYYMMDD)")
    parser_update.add_argument("--max-workers", type=int, default=4, help="并发线程数 (默认: 4)")

    # 清洗命令解析器
    parser_clean = subparsers.add_parser("clean", help="检查数据库中的数据质量")
    parser_clean.add_argument("symbols", nargs="*", help="股票代码列表，或指定 'all' 代表所有A股", default=["all"])
    parser_clean.add_argument("--full-recheck", action="store_true", help="强制对所有历史数据进行全面重新检查")
    parser_clean.add_argument("--max-workers", type=int, default=4, help="并发线程数 (默认: 4)")

    args = parser.parse_args()

    if args.command == "update":
        run_update(args)
    elif args.command == "clean":
        run_clean(args)

if __name__ == "__main__":
    main()