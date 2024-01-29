import logging.config
from typing import Literal

from aiohttp import web
from belvaio_request_id.logger import RequestIdAccessLogger
from belvaio_request_id.middleware import request_id_middleware

from src.middleware import error_handler
from src.olap_events.handlers import CreateEventHandler
from src.olap_events.producers import KafkaBroker, RabbitBroker, RedisBroker, AbstractBroker
from src.olap_events.recorder import EventRecorder
from src.oltp_events.db import connect_to_mongo, create_collections, MongoCollections
from src.oltp_events.handlers import router as oltp_router
from src.settings import Settings


async def startup(app: web.Application) -> None:
    await recorder.start()
    client = await connect_to_mongo(
        settings.mongo_username, settings.mongo_password, settings.mongo_host, settings.mongo_port, settings.mongo_db_name
    )
    await create_collections(
        client, collections=[MongoCollections.LIKES, MongoCollections.REVIEWS, MongoCollections.BOOKMARKS]
    )
    app['db'] = client[settings.mongo_db_name]


async def shutdown(app: web.Application) -> None:
    await recorder.shutdown()


def get_broker(broker: Literal['kafka', 'rabbitmq', 'redis']) -> AbstractBroker:
    if broker == 'kafka':
        return KafkaBroker(broker_url=settings.kafka_url, topic=settings.kafka_topic)
    elif broker == 'rabbitmq':
        return RabbitBroker(host=settings.rabbitmq_host, port=settings.rabbitmq_port,
                            user=settings.rabbitmq_user, password=settings.rabbitmq_password,
                            exchange_name=settings.rabbitmq_exchange, queue_name=settings.rabbitmq_queue_name)
    elif broker == 'redis':
        return RedisBroker(host=settings.redis_host, port=settings.redis_port, channel_name=settings.redis_channel)


if __name__ == '__main__':
    settings = Settings()
    logging.config.dictConfig(settings.logger_config)
    recorder = EventRecorder(message_broker=get_broker(settings.selected_broker))

    app = web.Application(middlewares=[error_handler, request_id_middleware])
    app.add_routes([web.post('/event', CreateEventHandler(callback=recorder.on_event))])
    app.add_routes(oltp_router)
    app.on_startup.append(startup)
    app.on_shutdown.append(shutdown)
    web.run_app(app, port=settings.app_port, host=settings.app_host, access_log_format=settings.access_log_format,
                access_log_class=RequestIdAccessLogger)
