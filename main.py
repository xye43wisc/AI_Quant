# main.py
import sys
import argparse
import pandas as pd
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from updater import update_symbol
from symbol_list import get_all_a_share_symbols
from storage import get_all_last_dates, get_last_cleaned_dates
from cleaner import clean_symbol
from suspension_updater import update_suspension_data

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
        futures = {executor.submit(update_symbol, sym, all_last_dates.get(sym), args.start_date): sym for sym in symbols}
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
    total_issues = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(clean_symbol, sym, last_cleaned_dates.get(sym), args.full_recheck): sym for sym in symbols}
        for future in tqdm(as_completed(futures), total=len(symbols), desc="清洗数据"):
            sym = futures[future]
            try:
                issues_from_symbol = future.result()
                if issues_from_symbol:
                    total_issues.extend(issues_from_symbol)
            except Exception as e:
                print(f"清洗 {sym} 时发生错误: {e}", file=sys.stderr)
    if total_issues:
        print(f"\n检查完成，共发现 {len(total_issues)} 个数据问题。")
        df_issues = pd.DataFrame(total_issues)
        df_issues = df_issues[['symbol', 'date', 'severity', 'issue', 'details']]
        try:
            df_issues.to_csv(args.output, index=False, encoding='utf-8-sig')
            print(f"清洗报告已保存至: {args.output}")
        except Exception as e:
            print(f"保存报告文件失败: {e}", file=sys.stderr)
    else:
        print("\n检查完成，未发现任何数据质量问题。")

def main():
    parser = argparse.ArgumentParser(description="A股数据管理工具")
    subparsers = parser.add_subparsers(dest="command", required=True, help="可执行的命令")
    parser_update = subparsers.add_parser("update", help="下载或更新股票日线及复权数据")
    parser_update.add_argument("symbols", nargs="*", help="股票代码列表，或指定 'all' 代表所有A股", default=["all"])
    parser_update.add_argument("--start-date", type=str, default=None, help="强制指定开始日期 (格式YYYYMMDD)")
    parser_update.add_argument("--max-workers", type=int, default=1, help="并发线程数 (默认: 1)")
    parser_clean = subparsers.add_parser("clean", help="检查数据库中的数据质量")
    parser_clean.add_argument("symbols", nargs="*", help="股票代码列表，或指定 'all' 代表所有A股", default=["all"])
    parser_clean.add_argument("--full-recheck", action="store_true", help="强制对所有历史数据进行全面重新检查")
    parser_clean.add_argument("--max-workers", type=int, default=10, help="并发线程数 (默认: 10)")
    default_filename = f"cleaning_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    parser_clean.add_argument("--output", type=str, default=default_filename, help=f"指定报告输出路径 (默认: {default_filename})")
    parser_suspension = subparsers.add_parser("update-suspensions", help="更新历史停复牌数据到数据库")
    parser_suspension.add_argument("--start-date", type=str, default="20100101", help="开始日期 (格式YYYYMMDD)")
    parser_suspension.add_argument("--end-date", type=str, default=date.today().strftime("%Y%m%d"), help="结束日期 (格式YYYYMMDD)")
    args = parser.parse_args()
    if args.command == "update":
        run_update(args)
    elif args.command == "clean":
        run_clean(args)
    elif args.command == "update-suspensions":
        update_suspension_data(args.start_date, args.end_date)

if __name__ == "__main__":
    main()