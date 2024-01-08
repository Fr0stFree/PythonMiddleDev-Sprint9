from typing import Literal

from dotenv import find_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    selected_broker: Literal['kafka', 'rabbitmq', 'redis'] = Field(..., env='SELECTED_BROKER')

    kafka_url: str = Field(..., env='KAFKA_URL')
    kafka_topic: str = Field(..., env='KAFKA_TOPIC')

    rabbitmq_host: str = Field(..., env='RABBITMQ_HOST')
    rabbitmq_port: int = Field(..., env='RABBITMQ_PORT')
    rabbitmq_user: str = Field(..., env='RABBITMQ_USER')
    rabbitmq_password: str = Field(..., env='RABBITMQ_PASSWORD')
    rabbitmq_exchange: str = Field(..., env='RABBITMQ_EXCHANGE')
    rabbitmq_queue_name: str = Field(..., env='RABBITMQ_QUEUE_NAME')

    redis_host: str = Field(..., env='REDIS_HOST')
    redis_port: int = Field(..., env='REDIS_PORT')
    redis_channel: str = Field(..., env='REDIS_CHANNEL')

    clickhouse_host: str = Field(..., env='CLICKHOUSE_HOST')
    clickhouse_port: int = Field(..., env='CLICKHOUSE_PORT')

    class Config:
        env_file = find_dotenv('.env')
        env_file_encoding = 'utf-8'
        extra = 'ignore'
