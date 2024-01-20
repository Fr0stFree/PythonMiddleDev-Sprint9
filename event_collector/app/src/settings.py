from typing import Literal

from dotenv import find_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_port: int = Field(8000, env='EVENT_COLLECTOR_PORT')
    app_host: str = Field('localhost', env='EVENT_COLLECTOR_HOST')
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

    mongo_username: str = Field(..., env='MONGO_USERNAME')
    mongo_password: str = Field(..., env='MONGO_PASSWORD')
    mongo_host: str = Field(..., env='MONGO_HOST')
    mongo_port: int = Field(..., env='MONGO_PORT')
    mongo_db_name: str = Field(..., env='MONGO_DB_NAME')

    class Config:
        env_file = find_dotenv('.env')
        env_file_encoding = 'utf-8'
        extra = 'ignore'
