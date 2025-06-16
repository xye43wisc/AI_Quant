# logger_config.py
import logging
import sys
import os
from datetime import datetime

def setup_logger():
    """配置全局日志记录器"""
    
    # --- 1. 创建 logs 目录 ---
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # --- 2. 定义日志格式 ---
    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
    )
    
    # --- 3. 获取根日志记录器 ---
    logger = logging.getLogger()
    # 清除已存在的处理器，防止重复记录
    if logger.hasHandlers():
        logger.handlers.clear()
        
    logger.setLevel(logging.INFO) # 设置最低响应级别为 INFO

    # --- 4. 配置控制台处理器 ---
    # 输出 INFO 及以上级别的信息到控制台
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    stream_handler.setLevel(logging.INFO)
    logger.addHandler(stream_handler)

    # --- 5. 配置文件处理器 ---
    # 输出所有 INFO 及以上级别的信息到文件
    log_filename = os.path.join(log_dir, f"quant_{datetime.now().strftime('%Y%m%d')}.log")
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO) 
    logger.addHandler(file_handler)

    logging.info("日志系统已启动，日志将记录到控制台及文件: %s", log_filename)