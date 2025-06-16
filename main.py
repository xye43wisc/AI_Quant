# main.py
import sys
import argparse
import pandas as pd
from datetime import date, datetime
from tqdm import tqdm

from symbol_list import get_all_a_share_symbols
from storage import Session, get_all_last_dates, get_last_cleaned_dates_for_source
from models import get_model, create_all_tables
from updater import update_bar_for_symbol, update_factor_for_symbol
from cleaner import clean_symbol
from suspension_updater import update_suspension_data
from data_sources.base_source import BaseSource
from data_sources.akshare_source import AkshareSource
from data_sources.baostock_source import BaostockSource

def get_data_source(source_name: str) -> BaseSource:
    if source_name.lower() == 'akshare': return AkshareSource()
    if source_name.lower() == 'baostock': return BaostockSource()
    raise ValueError(f"Unsupported data source: {source_name}")

def get_symbols(args_symbols: list) -> list:
    if not args_symbols or args_symbols[0].lower() == "all":
        print("正在获取所有A股代码...")
        symbols = get_all_a_share_symbols()
        print(f"已找到 {len(symbols)} 个代码。")
        return symbols
    return args_symbols

def run_init_db(args):
    """初始化数据库，创建所有表"""
    try:
        create_all_tables()
    except Exception as e:
        print(f"Database initialization failed: {e}", file=sys.stderr)

def run_update_bar(args):
    source_name = args.source
    symbols = get_symbols(args.symbols)
    
    BarModel = get_model('bar', source_name)
    source = get_data_source(source_name)
    session = Session()
    
    print(f"正在从数据库 (表: {BarModel.__tablename__}) 获取最后更新日期...")
    last_dates = get_all_last_dates(session, BarModel)
    
    source.login()
    try:
        for sym in tqdm(symbols, desc=f"Updating Bars ({source_name})"):
            update_bar_for_symbol(sym, source, session, BarModel, last_dates.get(sym), args.start_date)
        session.commit()
        print("\n所有日线数据更新完成并已提交。")
    except Exception as e:
        session.rollback()
        print(f"\n发生错误，事务已回滚: {e}", file=sys.stderr)
    finally:
        source.logout()
        session.close()

def run_update_factor(args):
    source_name = args.source
    symbols = get_symbols(args.symbols)
    
    FactorModel = get_model('factor', source_name)
    source = get_data_source(source_name)
    session = Session()

    source.login()
    try:
        for sym in tqdm(symbols, desc=f"Updating Factors ({source_name})"):
            update_factor_for_symbol(sym, source, session, FactorModel)
        session.commit()
        print("\n所有复权因子更新完成并已提交。")
    except Exception as e:
        session.rollback()
        print(f"\n发生错误，事务已回滚: {e}", file=sys.stderr)
    finally:
        source.logout()
        session.close()

def run_update_suspension(args):
    print("开始更新停牌数据...")
    update_suspension_data(args.start_date, args.end_date)
    print("停牌数据更新完成。")

def run_clean(args):
    source_name = args.source
    symbols = get_symbols(args.symbols)
    
    BarModel = get_model('bar', source_name)
    FactorModel = get_model('factor', source_name)
    session = Session()
    
    try:
        last_cleaned = {}
        if not args.full_recheck:
            print(f"正在为数据源 '{source_name}' 获取所有股票的最后清洗日期...")
            # 调用新的、区分源的日志读取函数
            last_cleaned = get_last_cleaned_dates_for_source(session, source_name)
        
        total_issues = []
        for sym in tqdm(symbols, desc=f"Cleaning Data ({source_name})"):
            # 将 source_name 传递给 clean_symbol
            issues = clean_symbol(
                sym, source_name, session, BarModel, FactorModel, 
                last_cleaned.get(sym), args.full_recheck
            )
            if issues:
                total_issues.extend(issues)
        
        session.commit()
        print("\n数据清洗完成并已更新日志。")

        if total_issues:
            # ... 后续报告生成的逻辑不变 ...
            df_issues = pd.DataFrame(total_issues)[['symbol', 'date', 'severity', 'issue', 'details']]
            df_issues.to_csv(args.output, index=False, encoding='utf-8-sig')
            print(f"共发现 {len(total_issues)} 个问题，报告已保存至: {args.output}")
        else:
            print("未发现任何数据质量问题。")

    except Exception as e:
        session.rollback()
        print(f"\n清洗过程中发生错误，事务已回滚: {e}", file=sys.stderr)
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="A股数据管理工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 命令: init-db
    p_init = subparsers.add_parser("init-db", help="初始化数据库（创建所有表）")
    p_init.set_defaults(func=run_init_db)

    # 命令: update
    p_update = subparsers.add_parser("update", help="下载或更新股票数据")
    up_sub = p_update.add_subparsers(dest="target", required=True)
    
    p_bar = up_sub.add_parser("bar", help="更新日线数据")
    p_bar.add_argument("symbols", nargs="*", default=["all"])
    p_bar.add_argument("--source", type=str, default="akshare", choices=['akshare', 'baostock'])
    p_bar.add_argument("--start-date", type=str)
    p_bar.set_defaults(func=run_update_bar)

    p_factor = up_sub.add_parser("factor", help="更新复权因子")
    p_factor.add_argument("symbols", nargs="*", default=["all"])
    p_factor.add_argument("--source", type=str, default="akshare", choices=['akshare', 'baostock'])
    p_factor.set_defaults(func=run_update_factor)

    p_susp = up_sub.add_parser("suspension", help="更新历史停复牌数据")
    p_susp.add_argument("--start-date", default="20100101")
    p_susp.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    p_susp.set_defaults(func=run_update_suspension)

    # 命令: clean
    p_clean = subparsers.add_parser("clean", help="检查数据库中的数据质量")
    p_clean.add_argument("symbols", nargs="*", default=["all"])
    p_clean.add_argument("--source", type=str, required=True, choices=['akshare', 'baostock'])
    p_clean.add_argument("--full-recheck", action="store_true")
    default_fn = f"cleaning_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    p_clean.add_argument("--output", default=default_fn)
    p_clean.set_defaults(func=run_clean)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()