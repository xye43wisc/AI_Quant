# config.py
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # 使用新的 model_config 声明环境配置
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    DATABASE_URL: str                # e.g. postgresql+psycopg2://user:pwd@host:5432/a_share_db
    DEFAULT_START_DATE: str = "20100101"  # 默认首次拉取起始日期，格式 YYYYMMDD

settings = Settings() # type: ignore