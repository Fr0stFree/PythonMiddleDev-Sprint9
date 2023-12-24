from typing import Literal

from aiohttp import web

from src.producers import KafkaBroker, RabbitBroker
from src.handlers import CreateEventHandler
from src.recorder import EventRecorder
from settings import Settings


async def startup(app: web.Application) -> None:
    await recorder.start()


async def shutdown(app: web.Application) -> None:
    await recorder.shutdown()


def get_broker(broker: Literal['kafka', 'rabbitmq']) -> KafkaBroker | RabbitBroker:
    if broker == 'kafka':
        return KafkaBroker(broker_url=settings.kafka_url, topic=settings.kafka_topic)
    elif broker == 'rabbitmq':
        return RabbitBroker(host=settings.rabbitmq_host, port=settings.rabbitmq_port,
                            user=settings.rabbitmq_username, password=settings.rabbitmq_password,
                            exchange_name=settings.rabbitmq_exchange, queue_name=settings.rabbitmq_exchange)


if __name__ == '__main__':
    settings = Settings()
    app = web.Application()
    recorder = EventRecorder(message_broker=get_broker(settings.selected_broker))

    app.add_routes([web.post('/event', CreateEventHandler(callback=recorder.on_event))])
    app.on_startup.append(startup)
    app.on_shutdown.append(shutdown)
    web.run_app(app, port=settings.app_port, host=settings.app_host)

