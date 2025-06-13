# main.py
import sys
import argparse
from updater import update_symbol
from symbol_list import get_all_a_share_symbols
from storage import get_all_last_dates
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def main():
    parser = argparse.ArgumentParser(description="A股数据下载与更新脚本")
    parser.add_argument("symbols", nargs="*", help="股票代码列表，或指定 'all' 代表所有A股")
    parser.add_argument("--start-date", type=str, default=None,
                        help="覆盖起始拉取日期，格式为 YYYYMMDD，默认使用上次更新或配置")
    parser.add_argument("--max-workers", type=int, default=1,
                        help="并发更新的最大线程数 (默认为 1)")
    args = parser.parse_args()

    # 确定要更新的股票列表
    if not args.symbols or args.symbols[0].lower() == "all":
        print("Fetching all A-share symbols...")
        symbols = get_all_a_share_symbols()
        print(f"Found {len(symbols)} symbols.")
    else:
        symbols = args.symbols

    # 优化点：一次性获取所有股票的最后更新日期
    print("Fetching last update dates for all symbols from database...")
    all_last_dates = get_all_last_dates()
    print("Done.")

    # 使用线程池并发执行更新任务
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # 为每个 symbol 创建一个 future
        futures = {
            executor.submit(
                update_symbol, 
                sym, 
                all_last_dates.get(sym), # 从字典传入最后更新日期
                args.start_date
            ): sym 
            for sym in symbols
        }
        
        # 使用 tqdm 显示进度条
        for future in tqdm(as_completed(futures), total=len(symbols), desc="Updating symbols"):
            sym = futures[future]
            try:
                future.result()  # 获取任务结果，如果任务有异常会在这里抛出
            except Exception as e:
                # 打印特定股票的错误信息
                print(f"Error processing symbol {sym}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()