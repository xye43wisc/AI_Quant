# main.py
import sys
import argparse
import pandas as pd
from datetime import date, datetime
from tqdm import tqdm
import concurrent.futures
import logging

# 必须在所有其他模块导入之前最先配置日志
from logger_config import setup_logger
setup_logger()

from symbol_list import get_all_a_share_symbols
from storage import Session
from models import get_model, create_all_tables, DataIssueLog
from updater import update_bar_for_symbol, update_factor_for_symbol
from cleaner import worker_clean, worker_cross_validate
from suspension_updater import update_suspension_data
from data_sources.base_source import BaseSource
from data_sources.akshare_source import AkshareSource
from data_sources.baostock_source import BaostockSource

logger = logging.getLogger(__name__)
ISSUE_BATCH_SIZE = 2000

def get_data_source(source_name: str) -> BaseSource:
    if source_name.lower() == 'akshare': return AkshareSource()
    if source_name.lower() == 'baostock': return BaostockSource()
    raise ValueError(f"Unsupported data source: {source_name}")

def get_symbols(args_symbols: list) -> list:
    if not args_symbols or args_symbols[0].lower() == "all":
        logger.info("正在获取所有A股代码...")
        symbols = get_all_a_share_symbols()
        logger.info(f"已找到 {len(symbols)} 个代码。")
        return symbols
    return args_symbols

def run_init_db(args):
    """初始化数据库，创建所有表"""
    try:
        create_all_tables()
    except Exception as e:
        logger.critical("数据库初始化失败。", exc_info=True)

def run_update_bar(args):
    source_name = args.source
    symbols = get_symbols(args.symbols)
    BarModel = get_model('bar', source_name)
    source = get_data_source(source_name)
    session = Session()
    from storage import get_all_last_dates
    logger.info(f"正从数据库 (表: {BarModel.__tablename__}) 获取最后更新日期...")
    last_dates = get_all_last_dates(session, BarModel)
    source.login()
    try:
        count = 0
        for sym in tqdm(symbols, desc=f"Updating Bars ({source_name})"):
            try:
                update_bar_for_symbol(sym, source, session, BarModel, last_dates.get(sym), args.start_date, args.end_date)
            except Exception:
                logger.error(f"处理股票 {sym} 日线数据时失败。", exc_info=True)
            count += 1
            if count % 200 == 0:
                session.commit()
                tqdm.write(f"\n阶段性提交：已处理 {count} 个股票的数据。")
        session.commit()
        logger.info("日线数据更新完成。部分股票可能失败，详情请查看日志。")
    except Exception as e:
        session.rollback()
        logger.critical("日线更新过程中发生严重错误，事务已回滚。", exc_info=True)
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
            try:
                update_factor_for_symbol(sym, source, session, FactorModel)
            except Exception:
                logger.error(f"处理股票 {sym} 复权因子时失败。", exc_info=True)
        session.commit()
        logger.info("复权因子更新完成。部分股票可能失败，详情请查看日志。")
    except Exception as e:
        session.rollback()
        logger.critical("复权因子更新过程中发生严重错误，事务已回滚。", exc_info=True)
    finally:
        source.logout()
        session.close()

def run_update_suspension(args):
    update_suspension_data(args.start_date, args.end_date)

def run_clean(args):
    """并发检查单个数据源的数据质量，并将问题记录到数据库。"""
    source_name = args.source
    symbols = get_symbols(args.symbols)
    check_run_ts = datetime.utcnow()
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {executor.submit(worker_clean, sym, source_name, args.full_recheck): sym for sym in symbols}
        all_issues = []
        session = Session()
        try:
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(symbols), desc=f"Cleaning Data ({source_name})"):
                issues = future.result()
                if issues:
                    for issue in issues:
                        issue['check_run_ts'] = check_run_ts
                    all_issues.extend(issues)
                if len(all_issues) >= ISSUE_BATCH_SIZE:
                    session.bulk_insert_mappings(DataIssueLog, all_issues) # type: ignore
                    session.commit()
                    tqdm.write(f"\n已将 {len(all_issues)} 条问题记录分批写入数据库...")
                    all_issues.clear()
            if all_issues:
                session.bulk_insert_mappings(DataIssueLog, all_issues) # type: ignore
                session.commit()
                tqdm.write(f"\n已将最后 {len(all_issues)} 条问题记录写入数据库...")
            logger.info("数据清洗完成。")
        except Exception as e:
            session.rollback()
            logger.critical("数据清洗主进程发生错误，事务已回滚。", exc_info=True)
        finally:
            session.close()

def run_cross_validation(args):
    """并发交叉比对 akshare 和 baostock 的数据，并将问题记录到数据库。"""
    symbols = get_symbols(args.symbols)
    check_run_ts = datetime.utcnow()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {executor.submit(worker_cross_validate, sym): sym for sym in symbols}
        all_issues = []
        session = Session()
        try:
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(symbols), desc="Cross-Validating Data"):
                issues = future.result()
                if issues:
                    for issue in issues:
                        issue['check_run_ts'] = check_run_ts
                    all_issues.extend(issues)
                if len(all_issues) >= ISSUE_BATCH_SIZE:
                    session.bulk_insert_mappings(DataIssueLog, all_issues) # type: ignore
                    session.commit()
                    tqdm.write(f"\n已将 {len(all_issues)} 条问题记录分批写入数据库...")
                    all_issues.clear()
            if all_issues:
                session.bulk_insert_mappings(DataIssueLog, all_issues) # type: ignore
                session.commit()
                tqdm.write(f"\n已将最后 {len(all_issues)} 条问题记录写入数据库...")
            logger.info("交叉验证完成。")
        except Exception as e:
            session.rollback()
            logger.critical("交叉验证主进程发生错误，事务已回滚。", exc_info=True)
        finally:
            session.close()

def main():
    parser = argparse.ArgumentParser(description="A股数据管理工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Command: init-db
    p_init = subparsers.add_parser("init-db", help="初始化数据库（创建所有表）")
    p_init.set_defaults(func=run_init_db)

    # Command: update
    p_update = subparsers.add_parser("update", help="下载或更新股票数据")
    up_sub = p_update.add_subparsers(dest="target", required=True)
    p_bar = up_sub.add_parser("bar", help="更新日线数据")
    p_bar.add_argument("symbols", nargs="*", default=["all"])
    p_bar.add_argument("--source", type=str, default="akshare", choices=['akshare', 'baostock'])
    p_bar.add_argument("--start-date", type=str)
    p_bar.add_argument("--end-date", type=str, default=date.today().strftime("%Y%m%d"))
    p_bar.set_defaults(func=run_update_bar)
    p_factor = up_sub.add_parser("factor", help="更新复权因子")
    p_factor.add_argument("symbols", nargs="*", default=["all"])
    p_factor.add_argument("--source", type=str, default="akshare", choices=['akshare', 'baostock'])
    p_factor.set_defaults(func=run_update_factor)
    p_susp = up_sub.add_parser("suspension", help="更新历史停复牌数据")
    p_susp.add_argument("--start-date", default="20100101")
    p_susp.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    p_susp.set_defaults(func=run_update_suspension)

    # Command: clean
    p_clean = subparsers.add_parser("clean", help="并发检查单个数据源的数据质量")
    p_clean.add_argument("symbols", nargs="*", default=["all"])
    p_clean.add_argument("--source", type=str, required=True, choices=['akshare', 'baostock'])
    p_clean.add_argument("--full-recheck", action="store_true")
    p_clean.set_defaults(func=run_clean)

    # Command: cross-validate
    p_cv = subparsers.add_parser("cross-validate", help="并发交叉比对 akshare 和 baostock 的数据")
    p_cv.add_argument("symbols", nargs="*", default=["all"])
    p_cv.set_defaults(func=run_cross_validation)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()