from typing import Literal

from aiohttp import web

from settings import Settings
from src.handlers import CreateEventHandler
from src.producers import KafkaBroker, RabbitBroker, RedisBroker, AbstractBroker
from src.recorder import EventRecorder


async def startup(app: web.Application) -> None:
    await recorder.start()


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
    app = web.Application()
    recorder = EventRecorder(message_broker=get_broker(settings.selected_broker))

    app.add_routes([web.post('/event', CreateEventHandler(callback=recorder.on_event))])
    app.on_startup.append(startup)
    app.on_shutdown.append(shutdown)
    web.run_app(app, port=settings.app_port, host=settings.app_host)
