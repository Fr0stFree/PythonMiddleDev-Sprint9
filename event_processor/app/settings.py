from dotenv import find_dotenv
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    kafka_url: str = Field(..., env='KAFKA_URL')
    kafka_topic: str = Field(..., env='KAFKA_TOPIC')
    clickhouse_url: str = Field(..., env='CLICKHOUSE_URL')

    class Config:
        env_file = find_dotenv('.env')
        env_file_encoding = 'utf-8'
        extra = 'ignore'