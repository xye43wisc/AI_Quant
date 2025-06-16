# suspension_updater.py
import akshare as ak
import pandas as pd
from datetime import timedelta
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm
import logging

from storage import Session
from models import SuspensionInfo
from cleaner import get_trade_calendar

logger = logging.getLogger(__name__)

def update_suspension_data(start_date_str: str, end_date_str: str):
    session = Session()
    try:
        logger.info(f"开始一次性获取自 {start_date_str} 以来的所有停复牌公告...")
        
        df = ak.stock_tfp_em(date=start_date_str)
        logger.info(f"API 调用成功！共找到 {len(df)} 条历史公告。")

        if df.empty:
            logger.info("未找到任何停复牌公告。")
            return

        script_end_date = pd.to_datetime(end_date_str).date()
        trade_dates_df = get_trade_calendar()
        all_records = []

        logger.info("开始解析所有公告...")
        for _, row in tqdm(df.iterrows(), total=len(df), desc="解析公告"):
            try:
                suspension_start_dt = pd.to_datetime(row.get('停牌时间')).date() # type: ignore
                symbol = row['代码']
                suspension_duration = row.get('停牌期限')
                resumption_dt = pd.to_datetime(row.get('预计复牌时间')).date() if pd.notna(row.get('预计复牌时间')) else None # type: ignore

                suspension_end_dt = suspension_start_dt
                if suspension_duration == '连续停牌' and resumption_dt:
                    suspension_end_dt = resumption_dt - timedelta(days=1)
                
                final_end_dt = min(suspension_end_dt, script_end_date)

                if suspension_start_dt <= script_end_date:
                    period_mask = (trade_dates_df['trade_date'] >= suspension_start_dt) & \
                                  (trade_dates_df['trade_date'] <= final_end_dt)
                    
                    for d in trade_dates_df.loc[period_mask, 'trade_date']: # type: ignore
                        all_records.append({'symbol': symbol, 'suspension_date': d})
            except Exception:
                # 忽略单条解析错误
                continue
        
        if all_records:
            unique_records = [dict(t) for t in {tuple(d.items()) for d in all_records}]
            logger.info(f"共解析出 {len(unique_records)} 条在指定范围内的停牌日期记录。")
            
            logger.warning("正在清空旧的停牌数据...")
            session.query(SuspensionInfo).delete()
            session.commit()
            
            logger.info("开始批量写入新的停牌数据...")
            chunk_size = 5000
            for i in tqdm(range(0, len(unique_records), chunk_size), desc="批量写入数据库"):
                chunk = unique_records[i:i + chunk_size]
                stmt = insert(SuspensionInfo).values(chunk)
                stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'suspension_date'])
                session.execute(stmt)
            
            session.commit()
            logger.info("批量写入完成。")
        else:
            logger.info("未解析出任何在指定范围内的停牌记录。")

    except Exception as e:
        session.rollback()
        logger.error("更新停牌数据过程中发生错误。", exc_info=True)
    finally:
        session.close()
    
    logger.info("停复牌数据更新流程结束。")